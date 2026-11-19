"""Bitemporal history: edit/withdraw, out-of-order arrival, same-instant ties."""
import itertools

from socialdrift.store import Store

TS = "2030-01-0%dT00:00:00Z"


def row(rid="A-1", name="Alpha Widget", value=100, status="active", ts=None):
    return {"id": rid, "name": name, "value": value, "status": status,
            "ts": ts or TS % 1}


BASE = row(value=100, ts=TS % 1)
EDIT = row(value=200, ts=TS % 3)
WITHDRAW = row(value=200, status="withdrawn", ts=TS % 5)


def test_edit_then_withdraw_keeps_history_and_hides_current(store):
    for r in (BASE, EDIT, WITHDRAW):
        assert store.upsert(r, recorded_at="2030-02-01T00:00:00Z")

    chain = store.history("alpha widget")
    assert [e["version_no"] for e in chain] == [1, 2, 3]
    assert [e["kind"] for e in chain] == ["revision", "revision", "retraction"]
    assert [e["is_latest"] for e in chain] == [0, 0, 1]

    # The withdrawal hides the document today ...
    assert store.state_at("alpha widget")["visible"] is False
    assert "alpha widget" not in store.visible_latest()
    # ... but the text from before it is still recoverable verbatim.
    assert store.state_at("alpha widget", at=TS % 2)["content"]["value"] == 100
    assert store.state_at("alpha widget", at=TS % 4)["content"]["value"] == 200


def test_past_query_is_not_answered_with_todays_state(store):
    for r in (BASE, EDIT, WITHDRAW):
        store.upsert(r)
    chain = store.history("alpha widget")
    today = store.state_at("alpha widget")
    before_edit = store.state_at("alpha widget", at=TS % 2)
    during_edit = store.state_at("alpha widget", at=TS % 4)

    # A past viewpoint resolves to the revision that was true then, not to
    # today's answer with a back-dated timestamp.
    assert before_edit["event_id"] == chain[0]["event_id"]
    assert before_edit["is_latest"] == 1 and before_edit["visible"] is True
    assert during_edit["event_id"] == chain[1]["event_id"]
    assert today["event_id"] == chain[2]["event_id"]
    assert len({today["event_id"], before_edit["event_id"],
                during_edit["event_id"]}) == 3
    # is_latest is viewpoint-relative, never a persisted global flag.
    assert {before_edit["view_versions"], during_edit["view_versions"]} == {1, 2}
    assert today["is_latest"] == 1 and today["visible"] is False


def test_history_is_independent_of_arrival_order(store):
    baseline = None
    for order in itertools.permutations((BASE, EDIT, WITHDRAW)):
        fresh = Store(store.con)
        fresh.reset()
        for r in order:
            fresh.upsert(r, recorded_at="2030-02-01T00:00:00Z")
        view = [(e["event_id"], e["version_no"], e["is_latest"])
                for e in fresh.history("alpha widget")]
        if baseline is None:
            baseline = view
        assert view == baseline


def test_late_delivery_does_not_regress_latest(store):
    store.upsert(EDIT)
    store.upsert(BASE)                       # late, older valid time
    chain = store.history("alpha widget")
    assert [e["payload"]["value"] for e in chain] == [100, 200]
    assert chain[-1]["payload"]["value"] == 200
    assert store.state_at("alpha widget")["content"]["value"] == 200


def test_same_instant_conflict_is_total_and_deterministic(store):
    a = row(value=100, ts=TS % 1)
    b = row(value=300, ts=TS % 1)
    w = row(value=300, status="withdrawn", ts=TS % 1)
    orders = list(itertools.permutations((a, b, w)))
    seen = set()
    for order in orders:
        fresh = Store(store.con)
        fresh.reset()
        for r in order:
            fresh.upsert(r)
        chain = fresh.history("alpha widget")
        seen.add(tuple((e["payload"]["value"], e["kind"]) for e in chain))
        # a withdrawal stamped at the same instant wins over the revision
        assert chain[-1]["kind"] == "retraction"
        assert fresh.state_at("alpha widget")["visible"] is False
    assert len(seen) == 1


def test_duplicate_collection_does_not_duplicate_history(store):
    for i in range(3):
        assert store.upsert(BASE, batch_id="b%d" % i,
                            recorded_at="2030-02-0%dT00:00:00Z" % (i + 1)) == (i == 0)
    assert store.count() == 1
    assert store.receipt_count() == 3
    audit = store.audit("alpha widget")
    assert [r["batch_id"] for r in audit["receipts"]] == ["b0", "b1", "b2"]
    assert [r["ingested"] for r in audit["receipts"]] == [True, False, False]


def test_receipt_time_view_excludes_later_evidence(store):
    store.upsert(BASE, recorded_at="2030-02-01T00:00:00Z")
    store.upsert(EDIT, recorded_at="2030-03-01T00:00:00Z")
    early = store.history("alpha widget", known_at="2030-02-15T00:00:00Z")
    assert len(early) == 1
    assert store.state_at("alpha widget",
                          known_at="2030-02-15T00:00:00Z")["content"]["value"] == 100
    assert len(store.history("alpha widget")) == 2


def test_identity_ignores_derived_fields(store):
    enriched = dict(BASE)
    enriched.update({"logical_id": "gone", "version_no": 9, "is_latest": 1,
                     "prev_version_id": "x"})
    store.upsert(BASE)
    assert store.upsert(enriched) is False
    assert store.count() == 1


def test_chain_annotations_are_derived_not_persisted(store):
    store.upsert(BASE)
    store.upsert(EDIT)
    stored = store.revisions("alpha widget")
    for entry in stored:
        assert "version_no" not in entry["payload"]
        assert "is_latest" not in entry["payload"]
    # A fresh reader still derives a coherent chain.
    assert [e["version_no"] for e in store.history("alpha widget")] == [1, 2]


def test_cursor_never_moves_backwards(store):
    store.set_watermark(TS % 5)
    assert store.set_watermark(TS % 2) == TS % 5
    assert store.watermark() == TS % 5
