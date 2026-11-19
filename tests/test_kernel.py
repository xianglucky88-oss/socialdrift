import importlib


def test_transform_module_has_a_public_entrypoint():
    module = importlib.import_module("socialdrift.kernel")
    assert callable(getattr(module, "process", None))
    result = module.process({"id": "sample", "name": "item", "value": 7,
                             "status": "active", "ts": "t1"})
    assert isinstance(result, dict)
    assert result["logical_id"] == "item"


def test_reduce_is_deterministic_and_marks_a_withdrawal_latest():
    module = importlib.import_module("socialdrift.kernel")
    records = [
        {"id": "1", "name": "Item", "value": 1, "status": "active",
         "ts": "2030-01-01T00:00:00Z"},
        {"id": "2", "name": "Item", "value": 2, "status": "withdrawn",
         "ts": "2030-01-03T00:00:00Z"},
        {"id": "3", "name": "Item", "value": 3, "status": "active",
         "ts": "2030-01-02T00:00:00Z"},
    ]
    out = module.reduce([dict(r) for r in records])
    assert [r["id"] for r in out] == ["1", "3", "2"]
    assert [r["version_no"] for r in out] == [1, 2, 3]
    assert [r["is_latest"] for r in out] == [0, 0, 1]
    assert out[-1]["visible"] == 0
    # Reordering the input does not change the derived chain.
    shuffled = module.reduce([dict(r) for r in reversed(records)])
    assert [(r["id"], r["version_no"]) for r in shuffled] == [
        (r["id"], r["version_no"]) for r in out]
