RECORD = {"id": "sample", "name": "Sample", "ts": "2030-01-01T00:00:00Z", "v": 1}


def test_store_identity_and_checkpoint(store):
    store.reset()
    assert store.upsert(RECORD) is True
    # The same evidence is not a new revision, but the receipt is journalled.
    assert store.upsert(RECORD) is False
    assert store.count() == 1
    assert store.receipt_count() == 2
    store.set_watermark("checkpoint")
    assert store.watermark() == "checkpoint"


def test_watermark_is_monotonic(store):
    store.reset()
    store.set_watermark("2030-01-02T00:00:00Z")
    store.set_watermark("2030-01-01T00:00:00Z")
    assert store.watermark() == "2030-01-02T00:00:00Z"


def test_revisions_are_immutable_evidence(store):
    store.reset()
    store.upsert(RECORD)
    first = store.revisions()[0]
    store.upsert({**RECORD, "v": 2})
    again = store.revisions()[0]
    assert again["event_id"] == first["event_id"]
    assert again["raw_payload"] == first["raw_payload"]
    assert again["recorded_at"] == first["recorded_at"]
    assert store.count() == 2
