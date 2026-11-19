
"""Append-only bitemporal revision store.

The store keeps *evidence*, never conclusions:

* every observed source record becomes one immutable revision whose identity
  is derived from the **raw source row** (source + record id + occurrence
  timestamp + raw digest).  Normalization and version-chain rules are not
  part of identity, so a rule change cannot fabricate a new revision for an
  observation that was already received;
* a separate ``receipts`` journal records every time an observation was
  received (including duplicates), preserving arrival order for audit;
* version numbers, ``prev`` links and ``is_latest`` are **not** persisted --
  they are recomputed from the full revision set at read time, so a late
  arrival, a rerun, a migration, or a rule change cannot leave a stale
  "latest" marker that contaminates a historical query.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager

from . import timeline
from .config import DB_PATH, SPEC, SCHEMA_VERSION, RULES_VERSION

# Conclusion fields: dropped before an observation's normalized payload is
# stored, so re-derivation never changes the evidence.
_DERIVED_FIELDS = timeline.DERIVED_FIELDS

_LEGACY_TABLE = "records_legacy_v1"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS revisions(
    event_id         TEXT PRIMARY KEY,
    logical_id       TEXT NOT NULL,
    source           TEXT NOT NULL,
    source_record_id TEXT,
    valid_from       TEXT NOT NULL,
    recorded_at      TEXT NOT NULL,
    kind             TEXT NOT NULL,
    rules_version    TEXT NOT NULL,
    content_hash     TEXT NOT NULL,
    raw_digest       TEXT NOT NULL,
    raw_payload      TEXT NOT NULL,
    payload          TEXT NOT NULL,
    origin           TEXT NOT NULL DEFAULT 'fetch',
    batch_id         TEXT,
    source_order     INTEGER,
    schema_version   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS revisions_logical
    ON revisions(logical_id, valid_from);
CREATE INDEX IF NOT EXISTS revisions_entity_valid
    ON revisions(source, source_record_id, valid_from);
CREATE INDEX IF NOT EXISTS revisions_content
    ON revisions(content_hash);
CREATE TABLE IF NOT EXISTS receipts(
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT NOT NULL,
    batch_id     TEXT,
    source       TEXT,
    source_order INTEGER,
    recorded_at  TEXT NOT NULL,
    ingested     INTEGER NOT NULL,
    attempts     INTEGER
);
CREATE INDEX IF NOT EXISTS receipts_event ON receipts(event_id);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
"""


def evidence(record):
    """Stored payload: the observation with batch-local conclusions removed."""
    return {k: v for k, v in record.items() if k not in _DERIVED_FIELDS}


class Store:
    def __init__(self, con=None):
        self.con = con
        if con is not None:
            self._init()

    # ------------------------------------------------------------------ #
    # Lifecycle / migration.
    # ------------------------------------------------------------------ #
    def connect(self, path=None):
        con = sqlite3.connect(str(path or DB_PATH))
        self.__init__(con)
        return con

    def _tables(self):
        return {r[0] for r in self.con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}

    def _init(self):
        self.con.executescript(_SCHEMA)
        if "records" in self._tables():
            self._migrate_v1()
        version = self._get_meta("schema_version")
        if version is None or int(version) < SCHEMA_VERSION:
            self._set_meta("schema_version", str(SCHEMA_VERSION))
        self._set_meta("rules_version", RULES_VERSION)
        self.con.commit()

    def _migrate_v1(self):
        """Migrate the legacy content-hash table into immutable revisions.

        The legacy table stored post-kernel payloads, so batch-local chain
        fields are stripped and identity is recomputed from the source
        columns only.  A migrated row therefore dedupes against the same
        record re-fetched later instead of inventing a second revision.  The
        legacy table is renamed (preserved) as audit evidence, and the
        migration is atomic and idempotent: the legacy table no longer exists
        afterwards, so it never runs twice.
        """
        recorded_at = timeline.now_iso()
        source = self._source()
        rows = self.con.execute(
            "SELECT content_hash,payload FROM records").fetchall()
        migrated = 0
        with self.atomic():
            for _legacy_hash, payload_json in rows:
                try:
                    payload = json.loads(payload_json)
                except (TypeError, ValueError):
                    continue
                if not isinstance(payload, dict):
                    continue
                stored = evidence(payload)
                raw = timeline.source_projection(payload)
                if not raw.get("id") and stored.get("id") is not None:
                    raw["id"] = str(stored.get("id"))
                self._insert(
                    record=stored, raw=raw, source=source,
                    source_record_id=None, valid_from=None, kind=None,
                    logical_id=stored.get("logical_id"),
                    recorded_at=recorded_at, origin="migration:v1",
                    batch_id="migration:v1", source_order=None, attempts=None,
                    log_receipt=False)
                migrated += 1
            self.con.execute(
                "ALTER TABLE records RENAME TO %s" % _LEGACY_TABLE)
            self._set_meta("migrated_from", "records")
            self._set_meta("migrated_rows", str(migrated))
            self._set_meta("migrated_at", recorded_at)

    # ------------------------------------------------------------------ #
    # Metadata.
    # ------------------------------------------------------------------ #
    def _get_meta(self, key):
        row = self.con.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
        return row[0] if row else None

    def _set_meta(self, key, value):
        self.con.execute(
            "INSERT INTO meta(k,v) VALUES(?,?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, str(value)))

    def _source(self):
        known = self._get_meta("source")
        if known:
            return known
        return str(getattr(SPEC, "source_name", "local"))

    def meta(self):
        return {k: v for k, v in self.con.execute("SELECT k,v FROM meta")}

    # ------------------------------------------------------------------ #
    # Ingestion.
    # ------------------------------------------------------------------ #
    @staticmethod
    def hash(record):
        """Content hash of the raw observation (evidence, rules excluded)."""
        return timeline.content_hash(timeline.source_projection(record))

    @contextmanager
    def atomic(self):
        """Group inserts so a partial failure leaves no partial history."""
        try:
            yield
        except Exception:
            self.con.rollback()
            raise
        else:
            self.con.commit()

    def upsert(self, record, *, raw=None, source=None, batch_id=None,
               source_order=None, recorded_at=None, origin="fetch",
               attempts=None, commit=True):
        """Record one observation.  Returns True only for a new revision.

        Re-observing the same event is idempotent for history but is still
        journalled in ``receipts`` so the arrival order stays auditable.
        """
        source = source or self._source()
        recorded_at = recorded_at or timeline.now_iso()
        raw = raw if raw is not None else record
        created = self._insert(
            record=evidence(record), raw=raw, source=source,
            source_record_id=None, valid_from=None, kind=None,
            logical_id=record.get("logical_id"), recorded_at=recorded_at,
            origin=origin, batch_id=batch_id, source_order=source_order,
            attempts=attempts, log_receipt=True)
        if commit:
            self.con.commit()
        return created

    def _insert(self, *, record, raw, source, source_record_id, valid_from,
                kind, logical_id, recorded_at, origin, batch_id, source_order,
                attempts, log_receipt):
        raw_view = timeline.source_projection(raw)
        if not raw_view.get("id") and raw.get("id") is not None:
            raw_view["id"] = str(raw.get("id"))
        ts_field = getattr(SPEC, "ts_field", "ts")
        if valid_from is None:
            # Valid time comes from the raw evidence, matching the value used
            # for identity; a normalization rule can never move a revision in
            # time.
            valid_from = timeline.norm_ts(
                raw_view.get(ts_field, record.get(ts_field)))
        if kind is None:
            kind = timeline.kind_for(record)
        if logical_id in (None, ""):
            logical_id = timeline.logical_id_for(record)
        else:
            logical_id = str(logical_id).strip().lower()
        if source_record_id in (None, ""):
            source_record_id = timeline.source_record_id_for(raw_view)
        raw_digest = timeline.digester(raw_view)
        event_id = timeline.event_id_for(source, raw_view)
        content_hash = timeline.content_hash(raw_view)

        cur = self.con.execute(
            "INSERT OR IGNORE INTO revisions("
            "event_id,logical_id,source,source_record_id,valid_from,recorded_at,"
            "kind,rules_version,content_hash,raw_digest,raw_payload,payload,"
            "origin,batch_id,source_order,schema_version) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (event_id, logical_id, source, str(source_record_id), valid_from,
             recorded_at, kind, RULES_VERSION, content_hash, raw_digest,
             json.dumps(raw_view, ensure_ascii=False, default=str),
             json.dumps(record, ensure_ascii=False, default=str),
             origin, batch_id, source_order, SCHEMA_VERSION))
        created = cur.rowcount > 0
        if log_receipt:
            self.con.execute(
                "INSERT INTO receipts(event_id,batch_id,source,source_order,"
                "recorded_at,ingested,attempts) VALUES(?,?,?,?,?,?,?)",
                (event_id, batch_id, source, source_order, recorded_at,
                 1 if created else 0, attempts))
        return created

    # ------------------------------------------------------------------ #
    # Read models.
    # ------------------------------------------------------------------ #
    def count(self):
        """Number of immutable revisions (reruns do not grow this)."""
        return self.con.execute("SELECT COUNT(*) FROM revisions").fetchone()[0]

    def entity_count(self):
        return self.con.execute(
            "SELECT COUNT(DISTINCT logical_id) FROM revisions").fetchone()[0]

    def receipt_count(self):
        return self.con.execute("SELECT COUNT(*) FROM receipts").fetchone()[0]

    def entities(self):
        return [r[0] for r in self.con.execute(
            "SELECT DISTINCT logical_id FROM revisions ORDER BY logical_id")]

    def entity_revisions(self, source, source_record_id):
        """All revisions of one *source* record, by any logical-id rule."""
        return [self._entry(row) for row in self.con.execute(
            "SELECT %s FROM revisions WHERE source=? AND source_record_id=? "
            "ORDER BY valid_from, kind, event_id" % self._COLUMNS,
            (str(source), str(source_record_id)))]

    @staticmethod
    def _entry(row):
        (event_id, logical_id, source, source_record_id, valid_from,
         recorded_at, kind, rules_version, content_hash, raw_digest,
         raw_payload, payload, origin, batch_id, source_order,
         schema_version) = row
        return {
            "event_id": event_id,
            "logical_id": logical_id,
            "source": source,
            "source_record_id": source_record_id,
            "valid_from": valid_from,
            "recorded_at": recorded_at,
            "kind": kind,
            "rules_version": rules_version,
            "content_hash": content_hash,
            "raw_digest": raw_digest,
            "raw_payload": json.loads(raw_payload),
            "payload": json.loads(payload),
            "origin": origin,
            "batch_id": batch_id,
            "source_order": source_order,
            "schema_version": schema_version,
        }

    _COLUMNS = ("event_id,logical_id,source,source_record_id,valid_from,"
                "recorded_at,kind,rules_version,content_hash,raw_digest,"
                "raw_payload,payload,origin,batch_id,source_order,"
                "schema_version")

    def revisions(self, logical_id=None, at=None, known_at=None):
        """Raw revisions, optionally bounded by valid time / receipt time.

        ``at`` and ``known_at`` are independent: ``at`` says "what was true
        then", ``known_at`` says "what had we received by then".
        """
        clauses, args = [], []
        if logical_id is not None:
            clauses.append("logical_id=?")
            args.append(logical_id)
        if at is not None:
            clauses.append("valid_from<=?")
            args.append(timeline.norm_ts(at))
        if known_at is not None:
            clauses.append("recorded_at<=?")
            args.append(timeline.norm_ts(known_at))
        sql = "SELECT %s FROM revisions" % self._COLUMNS
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY logical_id, valid_from, kind, content_hash, event_id"
        return [self._entry(row) for row in self.con.execute(sql, args)]

    def history(self, logical_id, at=None, known_at=None):
        """Full, order-independent revision chain for one logical document.

        ``is_latest`` here is relative to the requested view and is never
        persisted, so it cannot leak today's answer into a past query.
        """
        return timeline.chain(self.revisions(logical_id, at=at,
                                             known_at=known_at))

    def state_at(self, logical_id, at=None, known_at=None):
        """Resolved state at a valid-time point, optionally as known by then."""
        return timeline.resolve(self.revisions(logical_id),
                                at=at, known_at=known_at)

    def latest(self, logical_id):
        return self.state_at(logical_id)

    def latest_states(self, known_at=None):
        """Resolved state of every logical document, from one read."""
        out = {}
        for lid, entries in timeline.group(self.revisions(known_at=known_at)).items():
            state = timeline.resolve(entries)
            if state is not None:
                out[lid] = state
        return out

    def visible_latest(self):
        """Currently visible documents (retracted ones excluded)."""
        return {lid: state for lid, state in self.latest_states().items()
                if state.get("visible")}

    def audit(self, logical_id):
        """Provenance + evidence trail for one document, in receipt order."""
        receipts = [
            {"seq": r[0], "event_id": r[1], "batch_id": r[2], "source": r[3],
             "source_order": r[4], "recorded_at": r[5], "ingested": bool(r[6]),
             "attempts": r[7]}
            for r in self.con.execute(
                "SELECT r.seq,r.event_id,r.batch_id,r.source,r.source_order,"
                "r.recorded_at,r.ingested,r.attempts FROM receipts r "
                "JOIN revisions v ON v.event_id=r.event_id "
                "WHERE v.logical_id=? ORDER BY r.seq", (logical_id,))]
        return {"logical_id": logical_id,
                "revisions": self.history(logical_id),
                "receipts": receipts}

    def all_records(self):
        """Stored payloads in canonical occurrence order (compat helper)."""
        return [entry["payload"] for entry in timeline.chain(self.revisions())]

    def get(self, content_hash):
        row = self.con.execute(
            "SELECT payload FROM revisions WHERE content_hash=? "
            "ORDER BY recorded_at, event_id LIMIT 1", (content_hash,)).fetchone()
        return json.loads(row[0]) if row else None

    # ------------------------------------------------------------------ #
    # Cursor.
    # ------------------------------------------------------------------ #
    def watermark(self):
        return self._get_meta("watermark")

    def set_watermark(self, value, commit=True):
        """Advance the cursor; it never moves backwards.

        Application-level ordering is unchanged (a run still considers only
        records newer than the stored cursor); this only guarantees that a
        late or lower timestamp cannot roll the cursor back and let an
        incremental run re-emit history that was already accounted for.
        """
        value = timeline.norm_ts(value)
        current = self.watermark()
        if current is not None and timeline.norm_ts(current) >= value:
            return current
        self._set_meta("watermark", value)
        if commit:
            self.con.commit()
        return value

    def reset(self):
        self.con.execute("DELETE FROM revisions")
        self.con.execute("DELETE FROM receipts")
        self.con.execute("DELETE FROM meta")
        self.con.execute("DROP TABLE IF EXISTS %s" % _LEGACY_TABLE)
        try:
            self.con.execute("DELETE FROM sqlite_sequence")
        except sqlite3.DatabaseError:
            pass
        self.con.commit()
