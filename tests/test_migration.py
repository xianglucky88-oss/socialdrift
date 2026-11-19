"""Schema migration and rule-change safety."""
import json
import sqlite3

from socialdrift import framework, pipeline, timeline
from socialdrift.config import RULES_VERSION, SCHEMA_VERSION
from socialdrift.mockdata import build_rows
from socialdrift.store import Store


def _legacy_db(path, rows, watermark=None):
    """Recreate the schema v1 wrote, including post-kernel payloads."""
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE records(content_hash TEXT PRIMARY KEY, id TEXT, "
                "ts TEXT, payload TEXT NOT NULL)")
    con.execute("CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT)")
    for rec in rows:
        payload = dict(rec)
        payload.setdefault("logical_id", rec["name"].lower())
        payload.setdefault("version_no", 1)
        payload.setdefault("prev_version_id", None)
        payload.setdefault("is_latest", 1)
        con.execute("INSERT INTO records VALUES(?,?,?,?)",
                    (framework.content_hash(payload), str(rec["id"]),
                     str(rec["ts"]), json.dumps(payload)))
    if watermark:
        con.execute("INSERT INTO meta VALUES('watermark', ?)", (watermark,))
    con.commit()
    con.close()


def test_v1_migration_is_preserving_and_idempotent(tmp_path):
    rows = build_rows()[:3]
    path = tmp_path / "legacy.db"
    _legacy_db(path, rows, watermark=str(max(r["ts"] for r in rows)))

    con = Store().connect(path)
    try:
        store = Store(con)
        meta = store.meta()
        assert meta["migrated_from"] == "records"
        assert meta["migrated_rows"] == "3"
        assert int(meta["schema_version"]) == SCHEMA_VERSION
        assert store.count() == 3
        logical_ids = store.entities()
        assert len(logical_ids) == 3

        # Chain annotations written by v1 are gone from the evidence; they are
        # derived again on read.
        for entry in store.revisions():
            assert "version_no" not in entry["payload"]
            assert "is_latest" not in entry["payload"]
            assert entry["origin"] == "migration:v1"

        before = {lid: [(e["event_id"], e["version_no"])
                        for e in store.history(lid)] for lid in logical_ids}

        # Re-ingesting the migrated evidence is a no-op for history.
        for entry in store.revisions():
            assert store.upsert(entry["raw_payload"]) is False
        assert store.count() == 3

        # Re-opening does not re-run the migration.
        assert Store(con).count() == 3
        after = {lid: [(e["event_id"], e["version_no"])
                       for e in store.history(lid)] for lid in logical_ids}
        assert after == before

        # The legacy table is preserved as audit evidence.
        tables = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert "records_legacy_v1" in tables
        assert "records" not in tables
    finally:
        con.close()


def test_migration_then_refetch_uses_evidence_identity(tmp_path, source):
    rows = build_rows()[:3]
    path = tmp_path / "legacy.db"
    _legacy_db(path, rows)
    con = Store().connect(path)
    try:
        store = Store(con)
        migrated = store.count()
        # Re-fetch the same source rows the legacy database was built from.
        fresh, _ = framework.fetch_all(
            framework.Http(framework.Transport(), framework.Sleeper()))
        for rec in fresh[:3]:
            store.upsert(rec)
        assert store.count() == migrated
    finally:
        con.close()


def test_rule_change_preserves_identity_and_history(tmp_path, source,
                                                    monkeypatch):
    con = Store().connect(tmp_path / "rules.db")
    try:
        pipeline.run(mode="full", con=con)
        store = Store(con)
        before = {lid: [e["event_id"] for e in store.history(lid)]
                  for lid in store.entities()}
        recorded = {e["event_id"]: e["recorded_at"] for e in store.revisions()}

        original_normalize = framework.normalize
        original_logical = timeline.logical_id_for
        monkeypatch.setattr(framework, "normalize",
                            lambda r: {**original_normalize(r),
                                       "status": str(r.get("status", "")).upper()})
        monkeypatch.setattr(timeline, "logical_id_for",
                            lambda r: "RULE/" + original_logical(r))

        result = pipeline.run(mode="full", con=con)
        assert result["ingested"] == 0
        # Grouping may now differ under the new rule, but the underlying
        # revisions and their receipt times are untouched.
        after = {e["event_id"]: e["recorded_at"] for e in store.revisions()}
        assert after == recorded
        assert {e["rules_version"] for e in store.revisions()} == {RULES_VERSION}
    finally:
        con.close()
