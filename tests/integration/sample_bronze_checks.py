"""Spark integration checks; all fixtures and Delta outputs are retained."""
import csv
import unittest
from pathlib import Path
from uuid import uuid4

from delta.tables import DeltaTable
from src.common.spark_session import get_spark
from src.common.spark_csv import select_sample
from src.sources.make_sample import make_sample
from src.storage.init_lake import lake_paths
from src.storage.stream_to_bronze import write_sample_bronze


class SampleBronzeChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark('sample-bronze-checks')
        cls.spark.sparkContext.setLogLevel('WARN')
        run_id = uuid4().hex
        cls.directory = Path('/opt/app/data/raw') / ('sample-tests-' + run_id)
        cls.directory.mkdir()
        cls.target = lake_paths()['checkpoints'] + 'sample-tests/' + run_id
        with open('/opt/app/data/sample/storage_smoke.csv', newline='') as stream:
            rows = list(csv.reader(stream))
        # Two events share product_id, one exact row is duplicated, and one is on another day.
        cls.input = cls.directory / 'source.csv'
        with cls.input.open('x', newline='') as stream:
            csv.writer(stream).writerows(rows + [rows[1]])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_selection_preserves_bigint_and_nullable_fields(self):
        sample = select_sample(self.spark, str(self.input), rows=2, day='2019-11-01')
        rows = sample.orderBy('event_time').collect()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].category_id, 2053013555631882655)
        self.assertEqual(sample.schema['category_id'].dataType.simpleString(), 'bigint')
        self.assertIsNone(rows[1].brand)
        self.assertIsNone(rows[1].user_session)

    def test_short_sample_does_not_create_output(self):
        destination = self.directory / 'too-short.csv'
        with self.assertRaisesRegex(ValueError, 'distinct'):
            make_sample(self.spark, str(self.input), destination, rows=3, day='2019-11-01')
        self.assertFalse(destination.exists())

    def test_csv_sample_is_reusable_without_overwrite(self):
        destination = self.directory / 'sample.csv'
        make_sample(self.spark, str(self.input), destination, rows=2, day='2019-11-01')
        original = destination.read_bytes()
        make_sample(self.spark, str(self.input), destination, rows=2, day='2019-11-01')
        self.assertEqual(destination.read_bytes(), original)
        self.assertIn(b'2053013555631882655', original)
        self.assertEqual(len(original.splitlines()), 3)

    def test_delta_contract_rerun_and_conflicting_input(self):
        sample = select_sample(self.spark, str(self.input), rows=2, day='2019-11-01')
        target = self.target + '/bronze'
        first = write_sample_bronze(self.spark, sample, target, expected_rows=2)
        self.assertEqual(first['rows'], 2)
        table = DeltaTable.forPath(self.spark, target)
        frame = table.toDF()
        self.assertEqual(table.detail().first().partitionColumns, ['event_date'])
        self.assertEqual(len(frame.columns), 15)
        self.assertFalse(frame.schema['event_id'].nullable)
        self.assertEqual(frame.select('event_id').distinct().count(), 2)
        self.assertEqual(frame.first().kafka_partition, -1)
        self.assertEqual(frame.first().kafka_offset, -1)
        version = table.history(1).first().version
        second = write_sample_bronze(self.spark, sample, target, expected_rows=2)
        self.assertTrue(second['reused'])
        self.assertEqual(table.history(1).first().version, version)
        from pyspark.sql import functions as F
        conflicting = sample.withColumn('price', F.col('price') + 1)
        with self.assertRaisesRegex(ValueError, 'different'):
            write_sample_bronze(self.spark, conflicting, target, expected_rows=2)
        self.assertEqual(table.toDF().count(), 2)
        self.assertEqual(table.history(1).first().version, version)


if __name__ == '__main__':
    unittest.main(verbosity=2)
