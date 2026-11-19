"""Bitemporal revision-chain primitives.

Two independent clocks are kept for every observation:

* ``valid_from`` -- *occurrence* time.  The moment the change became true at
  the source (the record's ``ts`` field).
* ``recorded_at`` -- *receipt* time.  The moment this process first learned
  about the observation.

Ordering of revisions uses only intrinsic data (valid time, kind, canonical
evidence, event id), never the arrival order.  A set of observations therefore
resolves to the same history regardless of the order in which notifications
arrive.  Receipt order is still retained (and audited) for transaction-time
queries.

A withdrawal (``status == "withdrawn"`` etc.) is modelled as a retraction
event: it hides the entity from that valid instant onwards without deleting
any earlier revision.  Past points in time can always be reconstructed.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

# Fields that are conclusions rather than evidence.  They never participate in
# what an observation *is*.
DERIVED_FIELDS = ("logical_id", "version_no", "prev_version_id", "is_latest",
                  "prev_event_id", "visible", "last_visible_event_id",
                  "content")

_RETRACTION_KINDS = {"retraction", "retract", "retracted", "withdrawn",
                     "delete", "deleted", "removed", "tombstone"}
_RETRACTION_STATUS = {"withdrawn", "deleted", "retracted", "removed",
                      "tombstone"}


def now_iso() -> str:
    """Current UTC time in a lexicographically sortable ISO-8601 form."""
    return (datetime.now(timezone.utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"))


def norm_ts(value) -> str:
    """Normalize a timestamp for comparison without changing its ordering.

    Source timestamps in this project are ISO-8601 strings.  Accepting plain
    values (``"t1"`` in tests) keeps the old comparison rule: values are
    ordered by their string representation.
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        text = value.isoformat(timespec="microseconds").replace("+00:00", "Z")
        # Drop a zero fractional part so an equivalent ISO string and a
        # datetime compare (and dedupe) identically.
        if text.endswith(".000000Z"):
            text = text[:-len(".000000Z")] + "Z"
        return text
    return str(value).strip()


def canonical(record) -> str:
    """Canonical JSON used for hashing and tie-breaking.

    ``default=str`` keeps the digest computable for values that JSON cannot
    encode natively (e.g. a ``datetime`` supplied by a caller); the raw
    evidence is projected first, so this is only a safety net.
    """
    return json.dumps(record, sort_keys=True, ensure_ascii=False, default=str)


def content_hash(record) -> str:
    """Stable evidence hash for a record (24 hex chars, as before)."""
    return hashlib.sha256(canonical(record).encode()).hexdigest()[:24]


def digester(record) -> str:
    """Full-length digest of a canonical record."""
    return hashlib.sha256(canonical(record).encode()).hexdigest()


def source_projection(raw):
    """Rule-independent view of a source row used for observation identity.

    Only structural normalization (whitespace) is applied.  Semantic rules
    such as case folding, type coercion, timestamp reformatting or kernel
    enrichment are deliberately excluded, so changing those rules cannot
    change what an observation *is*.
    """
    out = {}
    for key, value in raw.items():
        if key in DERIVED_FIELDS:
            continue
        if isinstance(value, datetime):
            value = norm_ts(value)
        elif isinstance(value, str):
            value = value.strip()
        out[key] = value
    return out


def logical_id_for(record) -> str:
    """Identity of the logical document an observation belongs to.

    The domain rule is that a document is identified by its (normalized)
    name; an explicit ``logical_id`` wins when a caller already derived one.
    """
    value = record.get("logical_id")
    if value not in (None, ""):
        return str(value).strip().lower()
    name = record.get("name")
    if name not in (None, ""):
        return str(name).strip().lower()
    return str(record.get("id", "")).strip().lower()


def source_record_id_for(record) -> str:
    value = record.get("source_record_id")
    if value in (None, ""):
        value = record.get("id", "")
    return str(value)


def kind_for(record) -> str:
    """Classify an observation as a normal revision or a retraction."""
    kind = str(record.get("kind", "")).strip().lower()
    if kind in _RETRACTION_KINDS:
        return "retraction"
    status = str(record.get("status", "")).strip().lower()
    if status in _RETRACTION_STATUS:
        return "retraction"
    return "revision"


def event_id_for(source, raw) -> str:
    """Deterministic identity of one *observation*.

    Identity is built from the raw source payload (the evidence), not from
    normalized or kernel-enriched fields.  Changing normalization rules or
    the version-chain algorithm therefore cannot fabricate a new revision
    for an observation that was already received.
    """
    view = source_projection(raw)
    digest = digester(view)
    blob = "\x1f".join([
        "socialdrift-event/2",
        str(source),
        source_record_id_for(view),
        norm_ts(view.get("ts", "")),
        digest,
    ])
    return hashlib.sha256(blob.encode()).hexdigest()


def tiebreak(entry):
    """Deterministic, inspectable tiebreak for same-instant observations.

    Prefers the canonical raw evidence so two observations that differ only
    in arrival order are always ordered the same way, and an auditor can
    reproduce the ordering by hand.
    """
    raw = entry.get("raw_payload")
    if isinstance(raw, dict):
        return canonical(source_projection(raw))
    payload = entry.get("payload")
    if isinstance(payload, dict):
        return canonical(source_projection(payload))
    return str(entry.get("content_hash", ""))


def order_key(entry):
    """Total, arrival-order-independent order of observations of one entity.

    Same-instant observations are broken deterministically by their evidence;
    a retraction is ordered after a revision at the same instant so that a
    withdrawal is not silently undone by an edit stamped with the same source
    timestamp.
    """
    rank = 1 if entry.get("kind") == "retraction" else 0
    return (norm_ts(entry.get("valid_from", "")), rank,
            tiebreak(entry), str(entry.get("event_id", "")))


def chain(entries):
    """Return a copy of ``entries`` ordered and annotated as a version chain.

    The annotations (``version_no``, ``prev_event_id``, ``is_latest``) are
    always recomputed from the complete set of observations.  They are never
    persisted as truth, so a late arrival or a rule change cannot leave a
    stale "latest" marker behind.
    """
    ordered = sorted((dict(e) for e in entries), key=order_key)
    previous = None
    for index, entry in enumerate(ordered, start=1):
        entry["version_no"] = index
        entry["prev_event_id"] = previous
        entry["is_latest"] = int(index == len(ordered))
        entry["visible"] = entry.get("kind") != "retraction"
        previous = entry.get("event_id")
    return ordered


def resolve(entries, at=None, known_at=None):
    """Resolve a set of observations to the state covering ``at``.

    ``at`` limits valid time (what was true then).
    ``known_at`` limits receipt time (what we had learned by then).  Both
    filters are independent, which is what keeps a latest state from
    contaminating a historical query.

    ``is_latest`` in the result is relative to the requested viewpoint, never
    the global present: asking about a past instant does not return today's
    answer with a back-dated stamp.
    """
    cutoff = None if at is None else norm_ts(at)
    known_cutoff = None if known_at is None else norm_ts(known_at)
    eligible = []
    for entry in entries:
        if cutoff is not None and norm_ts(entry.get("valid_from", "")) > cutoff:
            continue
        if known_cutoff is not None and norm_ts(entry.get("recorded_at", "")) > known_cutoff:
            continue
        eligible.append(entry)
    if not eligible:
        return None
    present = max(eligible, key=order_key)
    view = chain(eligible)
    winner = next(e for e in view if e["event_id"] == present["event_id"])
    result = dict(winner)
    result["visible"] = result.get("kind") != "retraction"
    result["view_versions"] = len(view)
    if result["visible"]:
        result["last_visible_event_id"] = result.get("event_id")
        result["content"] = result.get("payload")
    else:
        prior = [e for e in view
                 if e.get("kind") != "retraction"
                 and order_key(e) < order_key(result)]
        if prior:
            last = prior[-1]
            result["last_visible_event_id"] = last.get("event_id")
            result["content"] = last.get("payload")
        else:
            result["last_visible_event_id"] = None
            result["content"] = None
    return result


def group(entries):
    grouped = {}
    for entry in entries:
        grouped.setdefault(entry.get("logical_id", ""), []).append(entry)
    return grouped
