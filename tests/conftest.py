import json

import pytest

from socialdrift import pipeline
from socialdrift.framework import Transport
from socialdrift.store import Store


@pytest.fixture()
def transport():
    return Transport()


def control(transport, path, body=None):
    status, _hdr, text = transport.request("POST", path, json_body=body or {})
    assert status == 200, (path, status, text)
    return json.loads(text)


@pytest.fixture()
def source(transport):
    """Reset the shared offline fixture to its deterministic baseline."""
    control(transport, "/source/reset")
    yield transport
    control(transport, "/source/reset")


@pytest.fixture()
def con(tmp_path):
    con = Store().connect(tmp_path / "test.db")
    try:
        yield con
    finally:
        con.close()


@pytest.fixture()
def store(con):
    return Store(con)


@pytest.fixture()
def run_pipeline(tmp_path, source):
    """Run the pipeline against an isolated database per test."""
    con = Store().connect(tmp_path / "pipeline.db")

    def _run(mode="full", **kw):
        return pipeline.run(mode=mode, con=con, **kw)

    _run.con = con
    _run.store = Store(con)
    try:
        yield _run
    finally:
        con.close()
