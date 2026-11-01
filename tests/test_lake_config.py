from src.storage.init_lake import lake_paths, table_contract


def test_bucket_override_reaches_every_storage_path(monkeypatch):
    monkeypatch.setenv("LAKE_BUCKET", "test-lake")
    paths = lake_paths()
    assert paths["raw_csv"] == "s3a://test-lake/raw/csv/"
    assert paths["raw"] == "s3a://test-lake/raw/ecom.events.raw/"
    assert paths["bronze"] == "s3a://test-lake/bronze/events/"
    assert paths["quarantine"] == "s3a://test-lake/quarantine/events/"
    assert paths["checkpoints"] == "s3a://test-lake/_checkpoints/"


def test_contract_adds_typed_partition_without_losing_nullability():
    bronze = table_contract("bronze")
    quarantine = table_contract("quarantine")
    assert bronze["columns"][-1] == {
        "name": "event_date", "type": "date", "nullable": True
    }
    assert quarantine["columns"][-1]["name"] == "ingest_date"
    columns = {c["name"]: c for c in bronze["columns"]}
    assert columns["category_id"]["type"] == "bigint"
    assert columns["event_id"]["nullable"] is False
    assert columns["brand"]["nullable"] is True


def test_partition_expansion_uses_declared_type_and_nullability(tmp_path, monkeypatch):
    from src.storage import init_lake

    contract_dir = tmp_path / 'contracts'
    contract_dir.mkdir()
    (contract_dir / 'example_events.yaml').write_text(
        'columns: [{name: id, type: bigint, nullable: false}]\n'
        'partition_by: [batch]\n'
        'partition_columns: [{name: batch, type: string, nullable: false}]\n',
        encoding='utf-8',
    )
    monkeypatch.setattr(init_lake, 'ROOT', tmp_path)
    result = init_lake.table_contract('example')
    assert result['columns'][-1] == {'name': 'batch', 'type': 'string', 'nullable': False}
