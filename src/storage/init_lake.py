"""Initialize S3A prefixes and C4/C5 Delta tables without needing source data."""
import json
import os
from urllib.parse import urlsplit, urlunsplit

import yaml

from src.common.config import ROOT, load


def lake_paths():
    lake = load()["lake"]
    bucket = os.environ.get("LAKE_BUCKET") or lake["bucket"]
    paths = {}
    for key in ("raw_csv", "raw", "bronze", "quarantine", "checkpoints"):
        uri = urlsplit(lake[key])
        if uri.scheme != "s3a" or not uri.netloc:
            raise ValueError(f"lake.{key} must be an s3a:// URI")
        paths[key] = urlunsplit(("s3a", bucket, uri.path, "", ""))
    return paths


def table_contract(name):
    contract = yaml.safe_load(
        (ROOT / "contracts" / f"{name}_events.yaml").read_text(encoding="utf-8")
    )
    declared = {c["name"]: c for c in contract["partition_columns"]}
    for partition in contract["partition_by"]:
        if partition not in {c["name"] for c in contract["columns"]}:
            column = declared[partition]
            contract["columns"].append({key: column[key] for key in ("name", "type", "nullable")})
    return contract


def contract_schema(spark, contract):
    from pyspark.sql.types import StructField, StructType

    fields = []
    for column in contract["columns"]:
        data_type = spark.createDataFrame([], f"value {column['type']}").schema[0].dataType
        fields.append(StructField(column["name"], data_type, column["nullable"]))
    return StructType(fields)


def initialize_lake(spark, paths=None):
    from delta.tables import DeltaTable

    paths = lake_paths() if paths is None else paths
    for uri in paths.values():
        path = spark._jvm.org.apache.hadoop.fs.Path(uri)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.mkdirs(path):
            raise RuntimeError(f"Cannot create lake prefix: {uri}")

    for name in ("bronze", "quarantine"):
        contract = table_contract(name)
        expected = contract_schema(spark, contract)
        # Delta 3.2 normalizes table identifiers, but not LOCATION's trailing slash.
        uri = paths[name].rstrip("/")
        (DeltaTable.createIfNotExists(spark).location(uri)
         .addColumns(expected).partitionedBy(*contract["partition_by"]).execute())
        actual = spark.read.format("delta").load(uri).schema
        partitions = DeltaTable.forPath(spark, uri).detail().first().partitionColumns
        signature = lambda schema: [(f.name, f.dataType, f.nullable) for f in schema]
        if signature(actual) != signature(expected) or partitions != contract["partition_by"]:
            raise ValueError(f"Existing table does not match {name} contract: {uri}")
        print(json.dumps({"event": "lake_table_ready", "table": name, "path": uri}))
    return paths


def main():
    from src.common.spark_session import get_spark

    spark = get_spark("init-lake")
    spark.sparkContext.setLogLevel("WARN")
    try:
        initialize_lake(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
