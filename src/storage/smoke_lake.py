"""Read C1 CSV from MinIO and round-trip Delta using a unique temporary path."""
import argparse
import json
from collections import Counter
from datetime import datetime
from uuid import uuid4

from src.common.config import ROOT
from src.common.spark_csv import read_source_csv
from src.common.spark_session import get_spark
from src.storage.init_lake import initialize_lake, lake_paths


def run_smoke(spark, *, sample=False, source=None, limit=100, keep_artifacts=False):
    if limit < 1:
        raise ValueError("limit must be positive")
    paths = initialize_lake(spark)
    run_id = uuid4().hex
    destination = paths["checkpoints"] + "smoke/" + run_id
    source = source or paths["raw_csv"] + "2019-Nov.csv"
    temporary_input = None
    frame = None
    Path = spark._jvm.org.apache.hadoop.fs.Path
    conf = spark._jsc.hadoopConfiguration()
    try:
        if sample:
            temporary_input = paths["raw_csv"] + "_smoke/" + run_id + ".csv"
            source = temporary_input
            target = Path(source)
            target.getFileSystem(conf).copyFromLocalFile(
                False, False, Path((ROOT / "data/sample/storage_smoke.csv").as_uri()), target
            )
        target = Path(source)
        if not target.getFileSystem(conf).exists(target):
            raise FileNotFoundError(f"CSV not found: {source}. Upload the dataset or use --sample.")
        frame = read_source_csv(spark, source)
        frame = (frame if sample else frame.limit(limit)).cache()
        # collect materializes every column; count alone may skip CSV parsing.
        rows = frame.orderBy("event_time").collect()
        if not rows:
            raise ValueError(f"CSV contains no data: {source}")
        if sample:
            if (len(rows) != 3 or rows[0].category_id != 2053013555631882655
                    or rows[0].event_time != datetime(2019, 11, 1)
                    or rows[1].brand is not None or rows[1].user_session is not None):
                raise AssertionError("Synthetic C1 fixture did not retain expected values")
        frame.repartition(2).write.format("delta").mode("errorifexists").save(destination)
        restored = spark.read.format("delta").load(destination)
        if restored.schema != frame.schema or Counter(restored.collect()) != Counter(rows):
            raise AssertionError("Delta round-trip changed the CSV schema or values")
        result = {"event": "storage_smoke_passed", "synthetic": sample,
                  "rows": len(rows), "source": source, "delta_path": destination,
                  "master": spark.sparkContext.master,
                  "artifacts_retained": keep_artifacts}
        print(json.dumps(result))
        return result
    finally:
        if frame is not None:
            frame.unpersist()
        # Only delete objects allocated by this invocation, never source datasets.
        for uri in (() if keep_artifacts else (destination, temporary_input)):
            if uri:
                path = Path(uri)
                path.getFileSystem(conf).delete(path, True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--sample", action="store_true", help="Use the committed synthetic fixture")
    inputs.add_argument("--source", help="CSV URI; default: configured raw/csv/2019-Nov.csv")
    parser.add_argument("--limit", type=int, default=100, help="Maximum real dataset rows to test")
    parser.add_argument("--keep-artifacts", action="store_true",
                        help="Keep test objects on MinIO instead of deleting them")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    spark = get_spark("storage-smoke")
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_smoke(spark, sample=args.sample, source=args.source, limit=args.limit,
                  keep_artifacts=args.keep_artifacts)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
