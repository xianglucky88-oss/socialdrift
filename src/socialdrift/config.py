
import os
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = Path(os.environ.get("SOCIALDRIFT_DB") or (ROOT / "socialdrift.db"))

# Bump when the persisted revision schema changes.  Migrations are idempotent.
SCHEMA_VERSION = 2
# Bump when normalization / identity / version-chain rules change.  It is
# recorded next to every stored derivation so a rule change never rewrites
# previously observed source evidence.
RULES_VERSION = "version_chain/2"

SPEC = SimpleNamespace(
    slug="socialdrift",
    scene="社媒时间线编辑/撤回版本链",
    fmt="json",
    engine="crawl",
    kernel="version_chain",
    endpoint="items",
    source_name="local:/items",
    columns=["id", "name", "value", "status", "ts"],
    page_size=4,
    seed=67,
    int_fields=["value"],
    ts_field="ts",
)
