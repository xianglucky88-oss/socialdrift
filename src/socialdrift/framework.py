"""Local transport, parsing, storage, and session helpers."""
from __future__ import annotations
import importlib
import json
import uuid

from lxml import html as _lh

from . import timeline
from .config import SPEC
from .store import Store


# --------------------------------------------------------------------------- #
# Transport helpers.
# --------------------------------------------------------------------------- #
class Transport:
    """Client for the bundled local service."""

    def __init__(self):
        import os
        import sys
        for p in (str(SPEC_ROOT()), str(SPEC_ROOT() / "src")):
            if p not in sys.path:
                sys.path.insert(0, p)
        from starlette.testclient import TestClient
        app = importlib.import_module("mockapp.main").app
        self.client = TestClient(app)
        self.cookies: dict[str, str] = {}

    def request(self, method, path, params=None, json_body=None, headers=None):
        q = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        resp = self.client.request(method, path, params=q, json=json_body,
                                   headers=headers or {}, cookies=self.cookies)
        for k, v in resp.cookies.items():
            self.cookies[k] = v
        return resp.status_code, dict(resp.headers), resp.text


def SPEC_ROOT():
    from .config import ROOT
    return ROOT


class Sleeper:
    """Records requested delays without blocking the caller."""

    def __init__(self):
        self.slept = 0.0
        self.calls = 0

    def sleep(self, seconds):
        self.slept += seconds
        self.calls += 1


class Http:
    """HTTP client with bounded retry support."""

    def __init__(self, transport: Transport, sleeper: Sleeper,
                 max_retries=4, base_backoff=0.5, politeness=0.25):
        self.t = transport
        self.sleeper = sleeper
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.politeness = politeness

    def get(self, path, params=None, headers=None):
        attempt = 0
        while True:
            self.sleeper.sleep(self.politeness)
            status, hdr, body = self.t.request("GET", path, params, headers=headers)
            if status in (429, 500, 502, 503) and attempt < self.max_retries:
                self.sleeper.sleep(self.base_backoff * (2 ** attempt))
                attempt += 1
                continue
            return status, hdr, body, attempt


# --------------------------------------------------------------------------- #
# Response parsing.
# --------------------------------------------------------------------------- #
def _coerce(name, val):
    if name in SPEC.int_fields:
        try:
            return int(val)
        except (TypeError, ValueError):
            return val
    return val


def parse(resp_body, fmt):
    cols = SPEC.columns
    if fmt == "json":
        items = json.loads(resp_body).get("items", [])
        return [{c: _coerce(c, it.get(c)) for c in cols} for it in items]
    if fmt == "html":
        doc = _lh.fromstring(resp_body)
        rows = []
        for tr in doc.xpath("//table//tr[./td]"):
            cells = [td.text_content().strip() for td in tr.xpath("./td")]
            if len(cells) != len(cols):
                continue
            rows.append({c: _coerce(c, v) for c, v in zip(cols, cells)})
        return rows
    if fmt == "text":
        out = []
        for line in resp_body.splitlines():
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split("\t")]
            d = {c: _coerce(c, parts[i]) if i < len(parts) else _coerce(c, "")
                 for i, c in enumerate(cols)}
            out.append(d)
        return out
    raise ValueError("unknown fmt: %s" % fmt)


def fetch_all(http: Http):
    """Collect records from the local source."""
    records, attempts, page = [], 0, 1
    while True:
        status, _hdr, body, att = http.get("/" + SPEC.endpoint, params={"page": page})
        attempts += att + 1
        if status != 200:
            raise RuntimeError("fetch failed status=%s page=%s" % (status, page))
        page_recs = parse(body, SPEC.fmt)
        records.extend(page_recs)
        if len(page_recs) < SPEC.page_size:
            break
        page += 1
        if page > 200:
            break
    return records, attempts


# --------------------------------------------------------------------------- #
# Record identity helpers.
# --------------------------------------------------------------------------- #
def canonical(d):
    return timeline.canonical(d)


def content_hash(d):
    return timeline.content_hash(d)


def dedup(records):
    seen, out, dup = set(), [], 0
    for r in records:
        h = timeline.content_hash(r)
        if h in seen:
            dup += 1
            continue
        seen.add(h)
        out.append(r)
    return out, dup


def normalize(r):
    d = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
    if isinstance(d.get("status"), str):
        d["status"] = d["status"].lower()
    if SPEC.ts_field in d:
        d[SPEC.ts_field] = timeline.norm_ts(d[SPEC.ts_field])
    return d


def _apply_kernel(records):
    k = importlib.import_module(".kernel", __package__)
    for r in records:
        if hasattr(k, "process"):
            r.update(k.process(r))
    # ``reduce`` is intentionally not run before storage: chain annotations
    # are batch-local conclusions and must be recomputed from all revisions.

    return records


def _order_for(pair):
    """Canonical occurrence order of one observation (never arrival order)."""
    raw, record = pair
    return timeline.order_key({
        "valid_from": record.get(SPEC.ts_field, ""),
        "kind": timeline.kind_for(record),
        "event_id": str(record.get("id", "")),
        "raw_payload": raw,
    })


def _stable_ingest(store, pairs, *, batch_id, recorded_at, mode):
    """Persist observations idempotently, in canonical occurrence order.

    ``pairs`` are ``(raw, normalized)`` tuples.  Identity is computed from
    the raw source row, so a rule change or a migration cannot create a new
    revision for evidence that was already received.  The whole batch is one
    transaction: a partial failure leaves no partial history behind, and a
    rerun can resume exactly where it stopped.
    """
    ordered = sorted(pairs, key=_order_for)
    ingested = 0
    with store.atomic():
        for source_order, (raw, record) in enumerate(ordered):
            created = store.upsert(
                record, raw=raw, batch_id=batch_id, recorded_at=recorded_at,
                source_order=source_order, origin=mode, attempts=None,
                commit=False)
            ingested += 1 if created else 0
    return ingested


# --------------------------------------------------------------------------- #
# Collection pipeline.
# --------------------------------------------------------------------------- #
def run(mode="full", con=None, recorded_at=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    sleeper = Sleeper()
    http = Http(Transport(), sleeper)
    try:
        records, attempts = fetch_all(http)
        norm = [normalize(dict(r)) for r in records]

        # Keep the raw evidence paired with the normalized view *before* any
        # rule runs.  Identity is computed from the pair's raw half, so a
        # change in normalization/kernel rules cannot alter what was observed.
        pairs = []
        seen = set()
        dup_in_batch = 0
        for raw, record in zip(records, norm):
            key = content_hash(record)
            if key in seen:
                dup_in_batch += 1
                continue
            seen.add(key)
            pairs.append((raw, record))
        norm = _apply_kernel([record for _raw, record in pairs])
        pairs = [(raw, record) for (raw, _old), record in zip(pairs, norm)]

        batch_id = "%s-%s" % (mode, uuid.uuid4().hex[:12])
        recorded_at = recorded_at or timeline.now_iso()
        wm = store.watermark()

        # Which records an incremental run is responsible for is still
        # decided by the existing cursor rule (``ts`` strictly newer than the
        # watermark), and duplicate counting/parsing are untouched.  What
        # changes is only the *effect* downstream: skipping a record can no
        # longer roll history back, because nothing is ever overwritten and
        # the cursor cannot regress.
        if mode == "incremental" and wm:
            selected = [(raw, record) for raw, record in pairs
                        if timeline.norm_ts(record.get(SPEC.ts_field))
                        > timeline.norm_ts(wm)]
        else:
            selected = pairs

        ingested = _stable_ingest(
            store, selected, batch_id=batch_id, recorded_at=recorded_at,
            mode=mode)

        # The watermark advances monotonically from evidence actually
        # observed in this run, so a late lower timestamp cannot roll it back.
        observed = [timeline.norm_ts(r.get(SPEC.ts_field, "")) for r in norm
                    if timeline.norm_ts(r.get(SPEC.ts_field, ""))]
        new_wm = max(observed + [wm or ""]) if observed else (wm or "")
        if new_wm:
            store.set_watermark(new_wm)

        return {"engine": "crawl", "mode": mode, "fetched": len(records),
                "duplicates": dup_in_batch,
                "ingested": ingested, "count": store.count(),
                "entities": store.entity_count(), "receipts": store.receipt_count(),
                "batch_id": batch_id,
                "http_attempts": attempts, "slept": round(sleeper.slept, 4),
                "watermark": store.watermark()}
    finally:
        if own:
            con.close()


# --------------------------------------------------------------------------- #
# Session pipeline.
# --------------------------------------------------------------------------- #
class Rpa:
    def __init__(self):
        self.t = Transport()
        self.sleeper = Sleeper()
        self.log = []
        self.captcha_solved = False

    def login(self):
        status, _h, _b = self.t.request("POST", "/login",
                                        json_body={"u": "operator", "p": "local"})
        if status != 200:
            raise RuntimeError("login failed status=%s" % status)
        self.log.append("login")
        return True

    def _csrf(self):
        return {"x-csrf": self.t.cookies.get("csrf", "")}

    def _query_page(self, page):
        status, _h, body = self.t.request("GET", "/query", params={"page": page},
                                          headers=self._csrf())
        if status != 200:
            raise RuntimeError("query failed status=%s" % status)
        return json.loads(body)

    def collect(self):
        out, page = [], 1
        while True:
            self.sleeper.sleep(0.1)
            data = self._query_page(page)
            out.extend(data.get("items", []))
            if data.get("captcha_required") and not self.captcha_solved:
                ok = self._solve_captcha()
                if not ok:
                    raise RuntimeError("captcha solve rejected")
                self.captcha_solved = True
                page += 1
                continue
            if not data.get("has_more"):
                break
            page += 1
            if page > 200:
                break
        self.log.append("collect:%d" % len(out))
        return out

    def _solve_captcha(self):
        status, _h, body = self.t.request("POST", "/captcha",
                                          json_body={"code": self.t.cookies.get("challenge", "")},
                                          headers=self._csrf())
        ok = status == 200 and json.loads(body).get("ok") is True
        self.log.append("captcha:%s" % ("solved" if ok else "failed"))
        return ok

    def submit(self, record):
        token = content_hash(record)
        status, _h, body = self.t.request("POST", "/submit",
                                          json_body={"token": token, "record": record},
                                          headers=self._csrf())
        data = json.loads(body)
        self.log.append("submit:%s" % ("created" if data.get("created") else "dedup"))
        return bool(data.get("created"))


def run_rpa(con=None, recorded_at=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    r = Rpa()
    try:
        logged = r.login()
        items = r.collect()
        norm = [normalize(dict(it)) for it in items]
        pairs, seen, dup = [], set(), 0
        for raw, record in zip(items, norm):
            key = content_hash(record)
            if key in seen:
                dup += 1
                continue
            seen.add(key)
            pairs.append((raw, record))
        norm = _apply_kernel([record for _raw, record in pairs])
        pairs = [(raw, record) for (raw, _old), record in zip(pairs, norm)]
        batch_id = "rpa-%s" % uuid.uuid4().hex[:12]
        recorded_at = recorded_at or timeline.now_iso()
        ingested = _stable_ingest(
            store, pairs, batch_id=batch_id, recorded_at=recorded_at,
            mode="rpa")
        created = dedup_submit = 0
        for x in norm[:2]:
            if r.submit(x):
                created += 1
            else:
                dedup_submit += 1
        for x in norm[:1]:
            if not r.submit(x):
                dedup_submit += 1
        wm = max([timeline.norm_ts(x.get(SPEC.ts_field, "")) for x in norm] + [""])
        if wm:
            store.set_watermark(wm)
        return {"engine": "rpa", "logged_in": logged, "captcha_solved": r.captcha_solved,
                "collected": len(items), "fetched": len(items), "duplicates": dup,
                "ingested": ingested, "count": store.count(),
                "entities": store.entity_count(), "receipts": store.receipt_count(),
                "batch_id": batch_id,
                "submit_created": created, "submit_dedup": dedup_submit, "log": r.log}
    finally:
        if own:
            con.close()
