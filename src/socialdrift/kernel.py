
"""Domain kernel: version chain for timeline edits / withdrawals.

``process`` and ``reduce`` are pure derivations.  The annotations produced by
``reduce`` are a *view*; they are recomputed from the full revision set at
read time and are deliberately not persisted as truth.  Ordering is total and
depends only on intrinsic data, so the same observations always produce the
same chain no matter what order they arrive in.
"""
from collections import defaultdict

from . import timeline


def process(record):
    """Enrich one observation with the identity of its logical document."""
    return {"logical_id": timeline.logical_id_for(record)}


def _order_key(record):
    return timeline.order_key({
        "valid_from": record.get("ts", ""),
        "kind": timeline.kind_for(record),
        "event_id": str(record.get("id", "")),
        "payload": record,
    })


def reduce(records):
    """Annotate each record with its position in the logical version chain.

    Results are deterministic for a given set of observations.  Same-instant
    conflicts are resolved by kind/content/id, and a withdrawal sorts after a
    revision stamped with the same instant.
    """
    by = defaultdict(list)
    for r in records:
        by[timeline.logical_id_for(r)].append(r)
    ordered = []
    for logical_id in sorted(by):
        rows = sorted(by[logical_id], key=_order_key)
        prev = None
        for i, r in enumerate(rows):
            r["logical_id"] = logical_id
            r["version_no"] = i + 1
            r["prev_version_id"] = prev
            r["prev_event_id"] = prev
            r["is_latest"] = int(i == len(rows) - 1)
            r["visible"] = int(timeline.kind_for(r) != "retraction")
            prev = r.get("id")
        ordered.extend(rows)
    return ordered
