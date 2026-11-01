"""Run with spark-submit inside spark-master; uses a unique S3 prefix."""
import unittest
from datetime import datetime
from uuid import uuid4

from src.common.spark_session import get_spark
from src.storage import init_lake
from src.storage.smoke_lake import read_source_csv


class StorageChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("storage-integration")
        cls.spark.sparkContext.setLogLevel("WARN")
        cls.root = init_lake.lake_paths()["checkpoints"] + "tests-" + uuid4().hex
        cls.paths = {
            key: cls.root + "/" + suffix + "/"
            for key, suffix in {
                "raw_csv": "raw/csv", "raw": "raw/ecom.events.raw",
                "bronze": "bronze/events", "quarantine": "quarantine/events",
                "checkpoints": "_checkpoints",
            }.items()
        }

    @classmethod
    def tearDownClass(cls):
        path = cls.spark._jvm.org.apache.hadoop.fs.Path(cls.root)
        path.getFileSystem(cls.spark._jsc.hadoopConfiguration()).delete(path, True)
        cls.spark.stop()

    def test_initialize_empty_contracts_and_preserve_rows(self):
        from delta.tables import DeltaTable

        init_lake.initialize_lake(self.spark, self.paths)
        bronze = self.spark.read.format("delta").load(self.paths["bronze"])
        self.assertEqual(bronze.count(), 0)
        self.assertEqual(len(bronze.schema), 15)
        self.assertEqual(bronze.schema["category_id"].dataType.simpleString(), "bigint")
        self.assertFalse(bronze.schema["event_id"].nullable)
        self.assertTrue(bronze.schema["brand"].nullable)
        for key, partition in (("bronze", "event_date"), ("quarantine", "ingest_date")):
            detail = DeltaTable.forPath(self.spark, self.paths[key]).detail().first()
            self.assertEqual(detail.partitionColumns, [partition])
        quarantine = self.spark.read.format("delta").load(self.paths["quarantine"])
        self.assertEqual(quarantine.count(), 0)
        row = ("{}", "parse_error", 0, 42, datetime(2019, 11, 1), datetime(2019, 11, 1).date())
        self.spark.createDataFrame([row], quarantine.schema).write.format("delta").mode("append").save(self.paths["quarantine"])
        init_lake.initialize_lake(self.spark, self.paths)
        self.assertEqual(self.spark.read.format("delta").load(self.paths["quarantine"]).first().kafka_offset, 42)

    def test_reject_incompatible_existing_table(self):
        paths = dict(self.paths, bronze=self.root + "/incompatible")
        self.spark.range(1).write.format("delta").save(paths["bronze"])
        with self.assertRaisesRegex(ValueError, "contract"):
            init_lake.initialize_lake(self.spark, paths)
        self.assertEqual(self.spark.read.format("delta").load(paths["bronze"]).count(), 1)

    def test_csv_c1_exact_types_and_nulls(self):
        rows = read_source_csv(self.spark, "/opt/app/data/sample/storage_smoke.csv").orderBy("event_time").collect()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].category_id, 2053013555631882655)
        self.assertEqual(rows[0].event_time, datetime(2019, 11, 1))
        self.assertIsNone(rows[1].brand)
        self.assertIsNone(rows[1].user_session)

    def test_csv_rejects_wrong_header_and_invalid_timestamp(self):
        import tempfile
        from pathlib import Path

        source = Path("/opt/app/data/sample/storage_smoke.csv").read_text()
        for content in (source.replace("event_time,event_type", "event_type,event_time", 1),
                        source.replace("2019-11-01 00:00:00 UTC", "not-a-timestamp", 1)):
            # Store on S3 so both remote executors can read the test input.
            with tempfile.TemporaryDirectory() as directory:
                local = Path(directory) / "invalid.csv"
                local.write_text(content)
                remote = self.root + "/invalid-" + uuid4().hex + ".csv"
                destination = self.spark._jvm.org.apache.hadoop.fs.Path(remote)
                fs = destination.getFileSystem(self.spark._jsc.hadoopConfiguration())
                fs.copyFromLocalFile(False, False, self.spark._jvm.org.apache.hadoop.fs.Path(local.as_uri()), destination)
            with self.assertRaises(Exception):
                read_source_csv(self.spark, remote).collect()


if __name__ == "__main__":
    unittest.main(verbosity=2)
