"""Real Delta ZORDER/DRY RUN acceptance; retain every fixture and report."""
from datetime import date, datetime
import json
from pathlib import Path
from uuid import uuid4

from delta.tables import DeltaTable

from src.common.config import ROOT, load
from src.common.spark_session import get_spark
from src.storage.init_lake import contract_schema, table_contract
from src.storage import maintenance


def run():
    spark = get_spark('maintenance-checks')
    spark.sparkContext.setLogLevel('WARN')
    run_id = uuid4().hex
    path = load()['lake']['checkpoints'].rstrip('/') + '/maintenance-test-' + run_id + '/bronze/events'
    schema = contract_schema(spark, table_contract('bronze'))
    try:
        DeltaTable.createIfNotExists(spark).location(path).addColumns(schema).partitionedBy('event_date').execute()
        for day, count, parts in [(date(2019, 11, 1), 4800, 12), (date(2019, 11, 2), 1200, 3)]:
            rows = [(datetime(day.year, day.month, day.day), 'view', index % 200,
                     2053013555631882655, None, None, float(index % 80), index % 400,
                     None, f'{day}-{index}', index % 2, index, datetime(2026, 11, 1),
                     datetime(2026, 11, 1), day) for index in range(count)]
            (spark.createDataFrame(rows, schema).repartition(parts).write.format('delta')
             .mode('append').save(path))
        plan = maintenance.MaintenancePlan(table_path=path, day='2019-11-01', closed_before='2019-11-02')
        before = maintenance.active_file_snapshot(spark, plan)
        assert before['partition']['files'] >= 12
        log = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession, path)

        def other_files():
            from pyspark.sql import DataFrame
            return {row.path for row in DataFrame(log.update(False, spark._jvm.scala.Option.empty()).allFiles().toDF(), spark)
                    .where("partitionValues['event_date'] = '2019-11-02'").select('path').collect()}

        untouched = other_files()
        original = spark.read.format('delta').option('versionAsOf', before['version']).load(path)
        report = maintenance.run_maintenance(spark, plan)
        after = report['after']
        assert report['status'] == 'optimized'
        assert after['partition']['files'] < before['partition']['files']
        assert after['partition']['small_files'] <= before['partition']['small_files'] // 2
        assert other_files() == untouched
        current = spark.read.format('delta').load(path)
        assert current.schema == original.schema
        assert current.count() == original.count() == 6000
        assert current.select('event_id').distinct().count() == 6000
        assert current.exceptAll(original).limit(1).count() == 0
        assert original.exceptAll(current).limit(1).count() == 0
        assert report['vacuum']['mode'] == 'dry_run'
        assert report['vacuum']['executed'] is False
        assert report['vacuum']['preview_sql'].endswith('RETAIN 720 HOURS DRY RUN')

        # An empty date is a no-op, not an excuse to VACUUM the entire table.
        empty = maintenance.MaintenancePlan(table_path=path, day='2019-10-31', closed_before='2019-11-02')
        empty_report = maintenance.run_maintenance(spark, empty)
        assert empty_report['status'] == 'skipped_empty_partition'
        assert empty_report['vacuum']['mode'] == 'skipped'
        assert empty_report['before']['version'] == empty_report['after']['version']

        # Even DRY RUN refuses a disabled engine retention check, before rewriting.
        version = DeltaTable.forPath(spark, path).history(1).first().version
        spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')
        try:
            maintenance.run_maintenance(spark, plan)
            raise AssertionError('Disabled retention check was accepted')
        except ValueError as error:
            assert 'retentionDurationCheck' in str(error)
        finally:
            spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'true')
        assert DeltaTable.forPath(spark, path).history(1).first().version == version

        # Existing table retention is checked before OPTIMIZE, not after its commit.
        spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES "
                  "('delta.deletedFileRetentionDuration' = 'interval 60 days')").collect()
        version = DeltaTable.forPath(spark, path).history(1).first().version
        try:
            maintenance.run_maintenance(spark, plan)
            raise AssertionError('Shortening existing table retention was accepted')
        except ValueError as error:
            assert 'deletedFileRetentionDuration' in str(error)
        assert DeltaTable.forPath(spark, path).history(1).first().version == version
        assert spark.read.format('delta').option('versionAsOf', before['version']).load(path).count() == 6000
        report.update(event='maintenance_acceptance_verified', fixture_run_id=run_id,
                      rows_before=6000, rows_after=6000, unchanged_other_partition=True,
                      exact_data_preserved=True, schema_preserved=True, time_travel_verified=True,
                      empty_partition_noop_verified=True, disabled_safety_rejected=True,
                      existing_retention_guard_verified=True, artifacts_retained=True)
        output = ROOT / 'data' / 'raw' / f'maintenance-verification-{run_id}.json'
        with output.open('x', encoding='utf-8') as stream:
            json.dump(report, stream, indent=2, default=str)
        print(json.dumps({'event': report['event'], 'report': str(output), 'table_path': path,
                          'before_files': before['partition']['files'],
                          'after_files': after['partition']['files'],
                          'vacuum_executed': report['vacuum']['executed']}), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    run()
