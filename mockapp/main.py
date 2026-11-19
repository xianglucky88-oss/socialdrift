
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import hashlib
import json
from fastapi import FastAPI, Request, Response
from socialdrift.config import SPEC
from socialdrift.mockdata import build_rows, extra_row

app = FastAPI(title="Local source")
_ALL = build_rows()
_extra = []
_verified_sessions = set()
_accepted_tokens = set()

# Scenario controls used by the stability tests.
_fail_pages = {}       # page -> number of remaining forced 500s
_echo_pages = {}       # page -> page whose content is served instead (dup pages)

def _data():
    return _ALL + _extra

def _page_items(page):
    served = _echo_pages.get(page, page)
    lo, hi = (served - 1) * SPEC.page_size, served * SPEC.page_size
    return _data()[lo:hi]

@app.get("/items")
def items(page: int = 1):
    remaining = _fail_pages.get(page, 0)
    if remaining > 0:
        _fail_pages[page] = remaining - 1
        return Response(status_code=500)
    data = _page_items(page)
    if SPEC.fmt == "json":
        return {"items": data, "page": page}
    if SPEC.fmt == "text":
        lines = ["\t".join(str(it.get(c, "")) for c in SPEC.columns) for it in data]
        return Response(content="\n".join(lines), media_type="text/plain")
    th = "".join("<th>%s</th>" % c for c in SPEC.columns)
    trs = "".join("<tr>" + "".join("<td>%s</td>" % it.get(c, "") for c in SPEC.columns)
                  + "</tr>" for it in data)
    return Response(content="<html><table><tr>%s</tr>%s</table></html>" % (th, trs),
                    media_type="text/html")

@app.get("/refresh")
def refresh():
    _extra.append(extra_row(len(_extra) + 1))
    return {"total": len(_data())}

@app.get("/probe")
def probe():
    app.__dict__["_flaky_n"] = app.__dict__.get("_flaky_n", 0) + 1
    if app.__dict__["_flaky_n"] <= 2:
        return Response(status_code=429)
    return {"ok": True, "attempts": app.__dict__["_flaky_n"]}

@app.get("/health")
def health():
    return {"ok": True, "rows": len(_data())}

@app.post("/login")
def login(response: Response):
    sid = "SID-local"
    csrf = "CSRF-local"
    challenge = hashlib.sha256((sid + csrf).encode()).hexdigest()[:4]
    response.set_cookie("sid", sid)
    response.set_cookie("csrf", csrf)
    response.set_cookie("challenge", challenge)
    return {"role": "operator"}

@app.get("/query")
def query(request: Request, page: int = 1):
    if not request.cookies.get("sid"):
        return Response(status_code=401)
    if request.headers.get("x-csrf") != request.cookies.get("csrf"):
        return Response(status_code=403)
    items_page = _page_items(page)
    nxt = _page_items(page + 1)
    sid = request.cookies.get("sid")
    gate = (SPEC.engine == "rpa" and page == 1 and sid not in _verified_sessions
            and len(_data()) > SPEC.page_size)
    return {"items": items_page, "page": page, "has_more": bool(nxt),
            "captcha_required": bool(gate)}

@app.post("/captcha")
async def captcha(request: Request):
    body = await request.json()
    ok = body.get("code") == request.cookies.get("challenge")
    if ok:
        _verified_sessions.add(request.cookies.get("sid"))
    return {"ok": bool(ok)}

@app.post("/submit")
async def submit(request: Request):
    body = await request.json()
    token = body.get("token")
    created = token not in _accepted_tokens
    if created:
        _accepted_tokens.add(token)
    return {"created": bool(created), "token": token}


# --------------------------------------------------------------------------- #
# Scenario controls: late / out-of-order notifications, duplicate pages,
# partial failure, and isolated fixture resets.
# --------------------------------------------------------------------------- #
@app.post("/source/append")
async def source_append(request: Request):
    """Append a raw row exactly as supplied (late or out-of-order delivery)."""
    row = await request.json()
    _extra.append(dict(row))
    return {"total": len(_data()), "appended": row}


@app.post("/source/edit")
async def source_edit(request: Request):
    """Append an edited revision of an existing row."""
    body = await request.json()
    target = body.get("id")
    change = body.get("record", {})
    base = next((r for r in _data() if r.get("id") == target), None)
    if base is None:
        return Response(status_code=404)
    row = dict(base)
    row.update(change)
    row["id"] = target
    _extra.append(row)
    return {"total": len(_data()), "appended": row}


@app.post("/source/withdraw")
async def source_withdraw(request: Request):
    """Append a withdrawal (retraction) revision of an existing row."""
    body = await request.json()
    target = body.get("id")
    base = next((r for r in _data() if r.get("id") == target), None)
    if base is None:
        return Response(status_code=404)
    row = dict(base)
    row["status"] = "withdrawn"
    if body.get("ts"):
        row["ts"] = body["ts"]
    _extra.append(row)
    return {"total": len(_data()), "appended": row}


@app.post("/source/echo-page")
async def source_echo_page(request: Request):
    """Serve ``page``'s content for another page, simulating pagination drift."""
    body = await request.json()
    _echo_pages[int(body["page"])] = int(body["served"])
    return {"echo": {str(k): v for k, v in _echo_pages.items()}}


@app.post("/source/fail-page")
async def source_fail_page(request: Request):
    """Force a page to fail ``times`` times (partial-failure scenario)."""
    body = await request.json()
    page = int(body["page"])
    if int(body.get("times", 0)) <= 0:
        _fail_pages.pop(page, None)
    else:
        _fail_pages[page] = int(body["times"])
    return {"failing": {str(k): v for k, v in _fail_pages.items()}}


@app.post("/source/reset")
async def source_reset():
    _extra.clear()
    _fail_pages.clear()
    _echo_pages.clear()
    return {"total": len(_data())}
