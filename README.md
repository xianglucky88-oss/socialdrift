# socialdrift

A small offline data-processing service with a local HTTP fixture and SQLite
storage. It is self-contained and does not require network access.

The domain is a **timeline of edits and withdrawals**. The service keeps two
independent clocks for every observation, so reviewers can see the version
that is visible now *and* reconstruct exactly what the source said at any past
moment — without today's answer contaminating a historical query.

## Usage

```text
make run       # process the bundled source
make run_inc   # process using the incremental command-line mode
make test      # run the test suite
make mock      # start the local fixture service
```

The package exposes a Python entry point under `src/socialdrift`. The fixture
service supports the source formats used by the package, and the command-line
entry point writes results to the local SQLite database. Re-running commands is
safe for an existing database.

### Inspecting history

```text
socialdrift --history "alpha widget"            # full revision chain
socialdrift --history "alpha widget" --at 2030-01-02T00:00:00Z
socialdrift --state   "alpha widget" --at 2030-01-04T00:00:00Z
socialdrift --state   "alpha widget" --known-at 2030-02-01T00:00:00Z
socialdrift --audit   "alpha widget"            # revisions + receipt journal
socialdrift --visible                           # currently visible documents
```

`--at` is **valid time** (“what was true then”); `--known-at` is **receipt
time** (“what had we learned by then”). They compose: both filters apply to the
same read, so a past viewpoint never sees evidence that had not arrived yet.

## History model

`src/socialdrift/timeline.py` holds the primitives and `store.py` the
persistence.

* **Evidence, never conclusions.** Each observed source row becomes one
  immutable revision. Its identity (`event_id`) is derived from the raw source
  row only — source, source record id, occurrence timestamp and a digest of
  the raw payload. Normalization, kernel enrichment and version-chain rules are
  *not* part of identity, so a rule change or a migration cannot fabricate a
  new revision for evidence that was already received.
* **Two clocks.** `valid_from` is the record's own timestamp; `recorded_at`
  is when this process received it. Receipts are journalled in `receipts`
  (including duplicates and reruns), so arrival order and provenance are
  auditable even though history is not ordered by them.
* **Derived, never persisted.** `version_no`, `prev_event_id`, `is_latest`
  and visibility are recomputed from the full revision set on every read.
  A late arrival, rerun, migration or rule change can therefore never leave a
  stale “latest” marker behind.
* **Total order.** Revisions are ordered by valid time, then kind, then their
  canonical evidence — never by arrival order. Same-instant conflicts resolve
  deterministically, and a withdrawal sorts after a revision stamped with the
  same instant, so a withdrawal is not silently undone by an edit.
* **Withdrawals are retractions.** `status` in `withdrawn/deleted/removed/…`
  (or an explicit `kind: retraction`) hides the document from that valid
  instant onwards. Nothing is deleted: earlier revisions and their text stay
  recoverable.
* **Nothing rolls history back.** The revision table is append-only, the
  cursor only advances, and batches are ingested in one transaction, so a
  partial failure leaves no half-written history and a resumed run continues
  from exactly where it stopped.

### What is unchanged

Cursor semantics (incremental runs process records with `ts` strictly newer
than the watermark and never rewind the cursor), count-based duplicate
reporting, and field parsing/coercion all behave as before. Identity is now
evidence-based, so those rules govern *which work a run does*, not *what
history is allowed to exist*.

### Migration

An existing schema-v1 database (`records` table, content-hash primary key,
post-kernel payloads) is migrated automatically on first open. Chain fields
are stripped from the evidence, identity is recomputed from the source
columns so re-fetched rows dedupe instead of duplicating, and the old table is
renamed `records_legacy_v1` and preserved. The migration is atomic and
idempotent; `meta` records `migrated_from`, `migrated_rows`, `migrated_at`,
`schema_version` and the active `rules_version`.

## Verification

`make test` covers the stability properties directly:

* `tests/test_history.py` — edit-then-withdraw recovery, past-vs-present
  viewpoints, arrival-order independence, late deliveries, same-instant
  conflicts, duplicate collection, receipt-time views, append-only identity.
* `tests/test_stability.py` — out-of-order notifications, duplicate pages,
  partial failure, full rerun, incremental resume, cursor gaps, late
  withdrawals, migration, rule changes.
* `tests/test_migration.py` — schema-v1 migration and rule-change safety.

The fixture exposes scenario controls used by those tests:
`POST /source/{append,edit,withdraw,echo-page,fail-page,reset}`.
