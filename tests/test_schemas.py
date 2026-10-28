import json

import yaml

from src.common import schemas
from src.common.config import ROOT

CONTRACTS = ROOT / "contracts"


def _yaml(name):
    return yaml.safe_load((CONTRACTS / name).read_text(encoding="utf-8"))


def test_source_csv_matches_c1():
    spec = _yaml("source_csv.yaml")
    assert [(c["name"], c["type"]) for c in spec["columns"]] == schemas.SOURCE_COLUMNS


def test_event_schema_matches_c3():
    spec = json.loads((CONTRACTS / "event.schema.json").read_text(encoding="utf-8"))
    assert set(spec["properties"]) == {name for name, _ in schemas.EVENT_COLUMNS}
    assert spec["properties"]["event_type"]["enum"] == list(schemas.EVENT_TYPES)


def test_bronze_matches_c4():
    spec = _yaml("bronze_events.yaml")
    assert [(c["name"], c["type"]) for c in spec["columns"]] == schemas.BRONZE_COLUMNS
    assert len(schemas.BRONZE_COLUMNS) == 14
    assert spec["partition_by"] == ["event_date"]
