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


def test_collector_openapi_matches_c2():
    spec = _yaml("collector_openapi.yaml")
    post = spec["paths"]["/events"]["post"]
    body = post["requestBody"]["content"]["application/json"]["schema"]
    assert body["type"] == "array" and body["minItems"] == 1 and body["maxItems"] == 500
    assert (CONTRACTS / body["items"]["$ref"]).exists()  # lô là mảng message C3
    assert {"202", "400"} <= set(post["responses"])
    assert body["maxItems"] == yaml.safe_load((ROOT / "configs" / "pipeline.yaml").read_text(encoding="utf-8"))["collector"]["max_batch"]


def test_bronze_matches_c4():
    spec = _yaml("bronze_events.yaml")
    assert [(c["name"], c["type"]) for c in spec["columns"]] == schemas.BRONZE_COLUMNS
    assert len(schemas.BRONZE_COLUMNS) == 14
    assert spec["partition_by"] == ["event_date"]


def test_c4_has_fourteen_data_columns_and_one_explicit_date_partition():
    from src.storage.init_lake import table_contract

    spec = _yaml("bronze_events.yaml")
    assert len(spec["columns"]) == 14
    assert spec["data_column_count"] == 14
    assert spec["physical_column_count"] == 15
    assert [c["name"] for c in spec["columns"][9:]] == [
        "event_id", "kafka_partition", "kafka_offset", "produced_at", "ingested_at"
    ]
    assert "source_file" not in {c["name"] for c in spec["columns"]}
    partition = spec["partition_columns"]
    assert [(c["name"], c["type"], c["nullable"]) for c in partition] == [("event_date", "date", True)]
    assert partition[0]["derived_from"] == "to_date(event_time)"
    assert len(table_contract("bronze")["columns"]) == 15
    assert schemas.BRONZE_TABLE_COLUMNS == schemas.BRONZE_COLUMNS + [("event_date", "date")]


def test_c5_matches_shared_schema_and_explicit_ingest_partition():
    from src.storage.init_lake import table_contract

    spec = _yaml("quarantine_events.yaml")
    assert spec["data_column_count"] == 5
    assert spec["physical_column_count"] == 6
    assert [(c["name"], c["type"]) for c in spec["columns"]] == schemas.QUARANTINE_COLUMNS
    assert schemas.QUARANTINE_COLUMNS == [
        ("payload", "string"), ("error_reason", "string"),
        ("kafka_partition", "int"), ("kafka_offset", "bigint"), ("ingested_at", "timestamp")
    ]
    assert spec["partition_by"] == ["ingest_date"]
    assert [(c["name"], c["type"], c["nullable"]) for c in spec["partition_columns"]] == [
        ("ingest_date", "date", True)
    ]
    assert spec["partition_columns"][0]["derived_from"] == "to_date(ingested_at)"
    assert len(table_contract("quarantine")["columns"]) == 6
    assert schemas.QUARANTINE_TABLE_COLUMNS == schemas.QUARANTINE_COLUMNS + [("ingest_date", "date")]


def test_c4_c5_nullable_fields_are_part_of_the_interface():
    bronze = _yaml("bronze_events.yaml")["columns"]
    quarantine = _yaml("quarantine_events.yaml")["columns"]
    assert [c["name"] for c in bronze if c["nullable"]] == [
        "category_code", "brand", "user_session", "produced_at"
    ]
    assert [c["name"] for c in quarantine if c["nullable"]] == ["payload"]
    assert all(isinstance(c["nullable"], bool) for c in bronze + quarantine)


def test_c4_preserves_c1_types_and_nullable_flags():
    source = _yaml("source_csv.yaml")["columns"]
    bronze = _yaml("bronze_events.yaml")["columns"][:9]
    fields = lambda columns: [(c["name"], c["type"], c["nullable"]) for c in columns]
    assert fields(bronze) == fields(source)
    assert next(c for c in bronze if c["name"] == "category_id")["type"] == "bigint"
    assert next(c for c in bronze if c["name"] == "event_type")["allowed"] == list(schemas.EVENT_TYPES)
    assert next(c for c in bronze if c["name"] == "price")["minimum"] == 0


def test_c4_unique_key_requires_table_wide_replay_protection():
    spec = _yaml("bronze_events.yaml")
    assert spec["unique_key"] == "event_id"
    assert spec["deduplication"]["scope"] == "table"
    assert spec["deduplication"]["watermark_alone_enforces_uniqueness"] is False
    assert spec["deduplication"]["existing_event_id"] == "ignore"
    assert spec["deduplication"]["conflict_columns"] == [name for name, _ in schemas.SOURCE_COLUMNS]
    assert spec["deduplication"]["comparison"] == "null_safe_typed_values"


def test_c5_error_vocabulary_rule_precedence_and_raw_payload():
    spec = _yaml("quarantine_events.yaml")
    reasons = next(c for c in spec["columns"] if c["name"] == "error_reason")["allowed"]
    assert reasons == list(schemas.QUARANTINE_ERROR_REASONS)
    assert [(r["rule"], r["reason"]) for r in spec["error_rules"]] == [
        (1, "parse_error"), (2, "bad_event_time"), (3, "bad_event_type"),
        (4, "missing_id"), (4, "negative_price")
    ]
    assert [r["reason"] for r in spec["error_rules"]] == reasons
    assert spec["error_selection"] == "first_matching_rule"
    assert spec["payload_semantics"]["representation"] == "original_utf8_value"
    assert spec["duplicates"]["write_to_quarantine"] is False
