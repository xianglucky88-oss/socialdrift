from socialdrift.framework import content_hash, dedup, normalize, parse


def test_record_identity_is_stable():
    first = {"id": "1", "v": 2}
    same = {"id": "1", "v": 2}
    other = {"id": "1", "v": 3}
    assert content_hash(first) == content_hash(same)
    values, duplicate_count = dedup([first, same, other])
    assert len(values) == 2
    assert duplicate_count == 1


def test_normalize_lowercases_status_and_keeps_ts_ordering():
    assert normalize({"status": " Active ", "ts": " 2030-01-01T00:00:00Z "}) == {
        "status": "active", "ts": "2030-01-01T00:00:00Z"}


def test_parse_coerces_declared_integer_fields():
    body = ('{"items": [{"id": "1", "name": "A", "value": "42", '
            '"status": "new", "ts": "t1"}]}')
    rows = parse(body, "json")
    assert rows[0]["value"] == 42


def test_parse_keeps_non_numeric_values_untouched():
    body = '{"items": [{"id": "1", "name": "A", "value": "n/a", "status": "x", "ts": "t"}]}'
    assert parse(body, "json")[0]["value"] == "n/a"
