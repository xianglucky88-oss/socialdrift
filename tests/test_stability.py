"""The same evidence must yield the same history from every time viewpoint."""
import json
import sqlite3

from socialdrift import framework, pipeline, timeline
from socialdrift.config import SPEC
from socialdrift.mockdata import build_rows
from socialdrift.store import Store

from conftest import control

INDEX = {r["name"]: r for r in build_rows()}
ALPHA = INDEX["Alpha Widget"]
SAMPLE_TIMES = ("2030-01-02T00:00:00Z", "2030-01-04T00:00:00Z",
                "2035-01-01T00:00:00Z")


def chain_fingerprint(store):
    """Arrival-order-free fingerprint of the whole stored history."""
    return {
        lid: [(e["event_id"], e["version_no"], e["kind"], e["is_latest"])
              for e in store.history(lid)]
        for lid in store.entities()
    }


def viewpoint_fingerprint(store):
    """Resolved-state fingerprint across several valid-time viewpoints."""
    return {
        lid: tuple(store.state_at(lid, at=at)["event_id"]
                   if store.state_at(lid, at=at) else None
                   for at in SAMPLE_TIMES)
        for lid in store.entities()
    }


def add_edit(source, ts="2030-01-03T00:00:00Z", value=999):
    control(source, "/source/edit",
            {"id": ALPHA["id"], "record": {"value": value, "ts": ts}})


def _run_to_disk(tmp_path, name, mode="full", reverse_pages=False, source=None):
    """Run the pipeline into a named database and return its fingerprint."""
    con = Store().connect(tmp_path / name)
    try:
        if reverse_pages:
            control(source, "/source/echo-page", {"page": 1, "served": 2})
            control(source, "/source/echo-page", {"page": 2, "served": 1})
        result = pipeline.run(mode=mode, con=con)
        store = Store(con)
        return result, chain_fingerprint(store), viewpoint_fingerprint(store)
    finally:
        con.close()


def test_out_of_order_notifications_do_not_change_history(
        tmp_path, source, run_pipeline):
    run_pipeline(mode="full")
    # A back-dated edit and a late row arrive *after* the cursor advanced.
    add_edit(source)
    control(source, "/source/append",
            {"id": "SDR-LATE", "name": "Alpha Widget", "value": 1,
             "status": "active", "ts": "2030-01-02T00:00:00Z"})
    run_pipeline(mode="full")

    forward = chain_fingerprint(run_pipeline.store)

    # Same evidence, fetched with pages served in the opposite order, into a
    # brand-new database.
    _res, _, _ = _run_to_disk(tmp_path, "reversed.db", reverse_pages=True,
                              source=source)
    reordered = _fingerprint(tmp_path / "reversed.db")

    assert forward == reordered


def _fingerprint(path):
    con = Store().connect(path)
    try:
        store = Store(con)
        return chain_fingerprint(store)
    finally:
        con.close()


def test_page_order_never_changes_history_for_the_same_evidence(
        tmp_path, source):
    """Ordering is a property of the evidence, not of the fetch sequence."""
    from socialdrift.config import SPEC

    def run_with(order, name):
        control(source, "/source/reset")
        for slot, served in enumerate(order, start=1):
            control(source, "/source/echo-page",
                    {"page": slot, "served": served})
        con = Store().connect(tmp_path / name)
        try:
            pipeline.run(mode="full", con=con)
            store = Store(con)
            evidence = tuple(sorted(
                tuple(sorted((k, repr(v)) for k, v in e["raw_payload"].items()))
                for e in store.revisions()))
            return evidence, repr(chain_fingerprint(store))
        finally:
            con.close()

    permutations = list(__import__("itertools").permutations(
        range(1, (len(build_rows()) // SPEC.page_size) + 2)))
    by_evidence = {}
    for i, order in enumerate(permutations):
        evidence, history = run_with(order, "order-%d.db" % i)
        by_evidence.setdefault(evidence, set()).add(history)

    assert len(by_evidence) > 1                    # some orders truncate data
    for histories in by_evidence.values():
        assert len(histories) == 1                 # ... but never reorder it


def test_duplicate_page_does_not_duplicate_history(tmp_path, source,
                                                   run_pipeline):
    control(source, "/source/echo-page", {"page": 2, "served": 1})
    result = run_pipeline(mode="full")
    store = run_pipeline.store
    # Page 2 re-served page 1's rows: they must collapse as duplicates, and
    # nothing may be recorded twice.
    assert result["duplicates"] >= SPEC.page_size
    assert result["ingested"] == store.count()
    for lid in store.entities():
        events = [e["event_id"] for e in store.history(lid)]
        assert len(events) == len(set(events))
        assert [e["version_no"] for e in store.history(lid)] == list(
            range(1, len(events) + 1))

    partial = chain_fingerprint(store)
    partial_states = viewpoint_fingerprint(store)

    # Once pagination is clean again, a full run recovers the evidence the
    # duplicate page hid without disturbing what was already available.
    control(source, "/source/echo-page", {"page": 2, "served": 2})
    rerun = run_pipeline(mode="full")
    assert rerun["ingested"] > 0
    full = chain_fingerprint(store)
    for lid, events in partial.items():
        assert {e[0] for e in events} <= {e[0] for e in full[lid]}
    # Past viewpoints that were already resolvable do not change.
    for lid, resolved in partial_states.items():
        for at, event_id in zip(SAMPLE_TIMES, resolved):
            now = store.state_at(lid, at=at)
            if event_id is not None:
                assert now["event_id"] == event_id


def test_partial_failure_leaves_no_partial_history(tmp_path, source):
    baseline_con = Store().connect(tmp_path / "baseline.db")
    pipeline.run(mode="full", con=baseline_con)
    baseline = chain_fingerprint(Store(baseline_con))
    baseline_con.close()

    control(source, "/source/fail-page", {"page": 1, "times": 10})
    con = Store().connect(tmp_path / "failing.db")
    try:
        with __import__("pytest").raises(RuntimeError):
            pipeline.run(mode="full", con=con)
        assert Store(con).count() == 0          # nothing half-written
        assert Store(con).receipt_count() == 0
        assert Store(con).watermark() is None
    finally:
        con.close()

    # Fixture recovers; a fresh run matches the uninterrupted baseline.
    control(source, "/source/fail-page", {"page": 1, "times": 0})
    recovered_con = Store().connect(tmp_path / "recovered.db")
    pipeline.run(mode="full", con=recovered_con)
    assert chain_fingerprint(Store(recovered_con)) == baseline
    recovered_con.close()


def test_full_rerun_is_idempotent_and_does_not_rewrite_history(run_pipeline):
    first = run_pipeline(mode="full")
    chain = chain_fingerprint(run_pipeline.store)
    receipts = run_pipeline.store.receipt_count()
    second = run_pipeline(mode="full")
    assert second["ingested"] == 0
    assert second["count"] == first["count"]
    assert chain_fingerprint(run_pipeline.store) == chain
    # Every re-observation is still journalled for audit.
    assert run_pipeline.store.receipt_count() == receipts + first["fetched"] - first["duplicates"]


def test_incremental_resume_after_failure_is_stable(tmp_path, source,
                                                   run_pipeline):
    baseline = run_pipeline(mode="full")
    before = chain_fingerprint(run_pipeline.store)
    before_views = viewpoint_fingerprint(run_pipeline.store)

    # Partial failure during an incremental run: the cursor and history must
    # be exactly as they were.
    control(source, "/source/append",
            {"id": "SDR-NEW", "name": "Beta Gadget", "value": 43,
             "status": "active", "ts": "2030-02-01T00:00:00Z"})
    control(source, "/source/fail-page", {"page": 1, "times": 10})
    with __import__("pytest").raises(RuntimeError):
        run_pipeline(mode="incremental")
    store = run_pipeline.store
    assert store.watermark() == baseline["watermark"]
    assert chain_fingerprint(store) == before
    assert viewpoint_fingerprint(store) == before_views

    # Resume: the same evidence is processed cleanly exactly once.
    control(source, "/source/fail-page", {"page": 1, "times": 0})
    resumed = run_pipeline(mode="incremental")
    after = chain_fingerprint(store)
    assert resumed["ingested"] == 1
    assert store.watermark() == "2030-02-01T00:00:00Z"
    # Historical viewpoints are unchanged by the newly arrived revision.
    assert viewpoint_fingerprint(store)["alpha widget"] == \
        before_views["alpha widget"]

    # A second resume has nothing to do and cannot grow or rewrite history.
    again = run_pipeline(mode="incremental")
    assert again["ingested"] == 0
    assert chain_fingerprint(store) == after

    # A full rerun over the same evidence must agree with the incremental path.
    full = run_pipeline(mode="full")
    assert full["ingested"] == 0
    assert chain_fingerprint(store) == after


def test_cursor_gap_is_left_to_the_existing_cursor_rule(run_pipeline, source):
    run_pipeline(mode="full")
    control(source, "/source/append",
            {"id": "SDR-GAP", "name": "Beta Gadget", "value": 42,
             "status": "active", "ts": "2030-01-01T00:00:00Z"})
    # Behind the cursor: an incremental run does not pick it up (unchanged
    # rule), and crucially does not regress the cursor or existing history.
    before = chain_fingerprint(run_pipeline.store)
    inc = run_pipeline(mode="incremental")
    assert inc["ingested"] == 0
    assert run_pipeline.store.watermark() == "2030-01-05T00:00:00Z"
    assert chain_fingerprint(run_pipeline.store) == before

    # A full run does see it; nothing already available is lost.  Version
    # numbers of later revisions legitimately shift (they are derived), but
    # every previously stored event is still present and past viewpoints
    # still resolve to the same text.
    at = "2030-01-02T00:00:00Z"
    past = {lid: run_pipeline.store.state_at(lid, at=at)
            for lid in run_pipeline.store.entities()}
    full = run_pipeline(mode="full")
    assert full["ingested"] == 1
    store = run_pipeline.store
    after = chain_fingerprint(store)
    for lid, chain in before.items():
        assert {e[0] for e in chain} <= {e[0] for e in after[lid]}
        before_state = store.state_at(lid, at=at)
        assert (before_state or {}).get("content") == (past[lid] or {}).get("content")
    beta_before, beta = before["beta gadget"], after["beta gadget"]
    assert len(beta) == len(beta_before) + 1
    # The back-dated insert is ordered by valid time, not arrival time, and
    # the chain annotations stay consistent with that order.
    assert [e[1] for e in beta] == list(range(1, len(beta) + 1))
    assert [e[3] for e in beta] == [0] * (len(beta) - 1) + [1]


def test_late_withdrawal_does_not_regress_visible_history(
        tmp_path, source, run_pipeline):
    run_pipeline(mode="full")
    # The withdrawal carries a valid time *behind* the cursor (a late
    # notification) and is therefore invisible to the cursor alone.
    control(source, "/source/withdraw",
            {"id": ALPHA["id"], "ts": "2030-02-01T00:00:00Z"})
    run_pipeline(mode="incremental")
    store = run_pipeline.store
    lid = ALPHA["name"].lower()
    assert store.state_at(lid)["visible"] is False
    assert store.state_at(lid, at="2030-01-02T00:00:00Z")["visible"] is True
    assert store.state_at(lid, at="2030-01-04T00:00:00Z")["visible"] is True
    # Other documents remain visible.
    assert len(store.visible_latest()) == len(store.entities()) - 1

    # Replaying the same withdrawal is idempotent and cannot resurrect it.
    run_pipeline(mode="full")
    assert store.state_at(lid)["visible"] is False
    assert len(store.visible_latest()) == len(store.entities()) - 1


def test_migration_preserves_history_and_dedupes_refetch(tmp_path, source):
    path = tmp_path / "legacy.db"
    old = sqlite3.connect(str(path))
    old.execute("CREATE TABLE records(content_hash TEXT PRIMARY KEY, id TEXT, "
                "ts TEXT, payload TEXT NOT NULL)")
    old.execute("CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT)")
    # Legacy rows were stored *after* the kernel ran and carry batch-local
    # chain fields, exactly like rows written by schema v1.
    for i, rec in enumerate(build_rows()[:3]):
        payload = dict(rec)
        payload.update({"logical_id": rec["name"].lower(), "version_no": 1,
                        "prev_version_id": None, "is_latest": 1})
        old.execute("INSERT INTO records VALUES(?,?,?,?)",
                    (framework.content_hash(payload), str(rec["id"]),
                     str(rec["ts"]), json.dumps(payload)))
    old.execute("INSERT INTO meta VALUES('watermark', ?)",
                (max(r["ts"] for r in build_rows()),))
    old.commit()
    old.close()

    con = Store().connect(path)
    store = Store(con)
    try:
        assert store.count() == 3
        migrated = chain_fingerprint(store)
        for lid, entries in migrated.items():
            assert len(entries) == 1          # one revision per legacy row
            assert entries[0][2] == "revision"
        assert store.meta()["migrated_from"] == "records"

        # Re-fetching the same evidence must not invent new revisions.
        source_rows, _ = framework.fetch_all(
            framework.Http(framework.Transport(), framework.Sleeper()))
        for rec in source_rows[:3]:
            store.upsert(rec)
        assert store.count() == 3
        assert chain_fingerprint(store) == migrated

        # Reopening is idempotent: migration does not run twice.
        again = Store(con)
        assert again.count() == 3
    finally:
        con.close()


def test_rule_change_does_not_rewrite_history(tmp_path, source, run_pipeline,
                                             monkeypatch):
    run_pipeline(mode="full")
    before = chain_fingerprint(run_pipeline.store)
    before_states = viewpoint_fingerprint(run_pipeline.store)

    # A rule change: case folding of status and a different logical-id rule.
    original_normalize = framework.normalize
    original_logical_id = timeline.logical_id_for
    monkeypatch.setattr(framework, "normalize",
                        lambda r: {**original_normalize(r),
                                   "status": str(r.get("status", "")).upper()})
    monkeypatch.setattr(timeline, "logical_id_for",
                        lambda r: "RULE:" + original_logical_id(r))

    result = run_pipeline(mode="full")
    store = run_pipeline.store
    # Identity is evidence-based, so no new revisions are fabricated even
    # though the derived rules now disagree with the stored ones.
    assert result["ingested"] == 0
    assert chain_fingerprint(store) == before
    assert viewpoint_fingerprint(store) == before_states

    # The rule version is recorded next to each stored derivation.
    versions = {e["rules_version"] for e in store.revisions()}
    assert versions == {"version_chain/2"}
