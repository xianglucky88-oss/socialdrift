from socialdrift.config import SPEC
from socialdrift.pipeline import run
from socialdrift.store import Store


def test_pipeline_returns_a_status(con):
    Store(con).reset()
    result = run(con=con)
    assert isinstance(result, dict)
    assert result.get("engine") == SPEC.engine
    assert result["fetched"] >= result["ingested"]
    assert result["count"] == Store(con).count()


def test_full_run_is_repeatable_without_growth(con):
    Store(con).reset()
    first = run(mode="full", con=con)
    store = Store(con)
    chain = {lid: [e["event_id"] for e in store.history(lid)]
             for lid in store.entities()}
    second = run(mode="full", con=con)
    assert second["ingested"] == 0
    assert second["count"] == first["count"]
    # The re-observation is still auditable.
    assert store.receipt_count() == 2 * (first["fetched"] - first["duplicates"])
    assert {lid: [e["event_id"] for e in store.history(lid)]
            for lid in store.entities()} == chain


def test_incremental_respects_the_cursor(con, source):
    from conftest import control

    Store(con).reset()
    run(mode="full", con=con)
    store = Store(con)
    watermark = store.watermark()
    control(source, "/source/append",
            {"id": "SOC-NEW", "name": "Beta Gadget", "value": 7,
             "status": "active", "ts": "2031-01-01T00:00:00Z"})
    inc = run(mode="incremental", con=con)
    assert inc["ingested"] == 1
    assert store.watermark() == "2031-01-01T00:00:00Z"
    assert store.watermark() > watermark
