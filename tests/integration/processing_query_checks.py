"""Real Spark SQL accounting, null-rate and pruning checks; retain fixtures."""
from datetime import date, datetime
from copy import deepcopy
import json
from uuid import uuid4

from delta.tables import DeltaTable

from src.common.config import ROOT, load
from src.common.schemas import SOURCE_COLUMNS, ddl
from src.common.spark_session import get_spark
from src.monitoring import reconcile
from src.storage.init_lake import contract_schema, initialize_lake, table_contract


def run():
    spark = get_spark('processing-query-checks')
    spark.sparkContext.setLogLevel('WARN')
    run_id = uuid4().hex
    root = load()['lake']['checkpoints'].rstrip('/') + '/processing-query-test-' + run_id
    paths = {'bronze': root + '/bronze/events', 'quarantine': root + '/quarantine/events',
             'checkpoints': root + '/checkpoint'}
    day, received = date(2019, 11, 1), datetime(2026, 11, 1)
    source_path = 'fixture:' + run_id
    try:
        initialize_lake(spark, paths)
        source_rows = [(datetime(2019, 11, 1), 'purchase' if i == 1 else 'view', i + 1,
                        2053013555631882655, None if i < 4 else 'electronics', None,
                        12.5, i + 20, None) for i in range(9)]
        other = (datetime(2019, 11, 2), 'view', 101, 2053013555631882655, None, None, 9.0, 201, None)
        source = spark.createDataFrame(source_rows + [other], ddl(SOURCE_COLUMNS))
        bronze_rows = [(*row, 'event-' + str(index), 0, index, None if index == 0 else received,
                        received, row[0].date()) for index, row in enumerate(source_rows[:2] + [other])]
        bronze_schema = contract_schema(spark, table_contract('bronze'))
        (spark.createDataFrame(bronze_rows, bronze_schema).coalesce(1).write.format('delta')
         .mode('append').partitionBy('event_date').save(paths['bronze']))
        quarantine_rows = [('{ broken JSON', 'parse_error', 0, 10, received, received.date()),
                           (None, 'parse_error', 0, 14, received, received.date()),
                           ('{"event_time":"2019-11-01T00:00:00Z","price":-1}',
                            'negative_price', 0, 11, received, received.date()),
                           ('{"event_time":"2019-11-02T00:00:00Z","event_type":"refund"}',
                            'bad_event_type', 0, 12, received, received.date())]
        (spark.createDataFrame(quarantine_rows, contract_schema(spark, table_contract('quarantine')))
         .coalesce(1).write.format('delta').mode('append').partitionBy('ingest_date').save(paths['quarantine']))
        manifest = {'source_path': source_path, 'bronze_path': paths['bronze'], 'quarantine_path': paths['quarantine'],
                    'days': [{'event_date': str(day), 'duplicates_dropped': 2,
                              'late_records_dropped': 1, 'durable_duplicates_ignored': 1}],
                    'quarantine_dates': [{'kafka_partition': 0, 'kafka_offset': offset, 'event_date': str(day)}
                                         for offset in (10, 14)]}
        versions = {name: DeltaTable.forPath(spark, paths[name]).history(1).first().version
                    for name in ('bronze', 'quarantine')}
        report = reconcile.run_verification(spark, source, day=str(day), source_path=source_path,
            bronze_path=paths['bronze'], quarantine_path=paths['quarantine'], manifest=manifest)
        result = report['results']['reconciliation'][0]
        assert list(result) == list(reconcile.REPORT_COLUMNS)
        assert result == {'event_date': day, 'rows_source': 9, 'rows_bronze': 2,
                          'rows_quarantine': 3, 'duplicates_dropped': 2, 'match': True}, result
        assert report['results']['accounting_diagnostics'][0]['late_records_dropped'] == 1
        nulls = {(row['dataset'], row['column_name']): row for row in report['results']['null_rates']}
        assert len(nulls) == 30  # C1=9, C4=15, C5=6 physical columns.
        assert nulls['source', 'category_code']['null_count'] == 4
        assert nulls['source', 'category_code']['null_ratio'] == 4 / 9
        assert nulls['bronze', 'produced_at']['null_ratio'] == 0.5
        assert nulls['quarantine', 'payload']['null_ratio'] == 1 / 3
        assert report['results']['duplicate_event_ids'] == []
        assert report['results']['event_type_totals'] == [
            {'event_date': day, 'event_type': 'purchase', 'rows': 1, 'users': 1},
            {'event_date': day, 'event_type': 'view', 'rows': 1, 'users': 1}]
        plan = '\n'.join(str(value) for row in report['results']['partition_pruning'] for value in row.values())
        assert 'PartitionFilters' in plan and '2019-11-01' in plan
        params = report['parameters']
        queries = reconcile.load_queries(ROOT / 'queries' / 'verify.sql')
        sample_queries = reconcile.load_queries(ROOT / 'queries' / 'sample_queries.sql')
        frame = spark.sql(reconcile.render_query(sample_queries['events_for_day'], **params))
        assert len(frame.collect()) == 2
        nodes, scans = [frame._jdf.queryExecution().executedPlan()], []
        while nodes:
            node = nodes.pop()
            name = node.getClass().getSimpleName()
            if name == 'AdaptiveSparkPlanExec':
                nodes.append(node.executedPlan())
            elif name.endswith('QueryStageExec'):
                nodes.append(node.plan())
            elif name == 'FileSourceScanExec':
                scans.append(int(node.metrics().apply('numFiles').value()))
            children = node.children()
            nodes.extend(children.apply(index) for index in range(children.size()))
        assert scans and sum(scans) == 1, scans  # Other date's file is not scanned.
        assert all(DeltaTable.forPath(spark, paths[name]).history(1).first().version == version
                   for name, version in versions.items())

        # A mapping must not move a timestamp-validated row from Nov2 into Nov1.
        # Compensating drops (2 -> 1) otherwise hide the extra C5 row in the balance.
        conflicting = deepcopy(manifest)
        conflicting['days'][0]['duplicates_dropped'] = 1
        conflicting['quarantine_dates'].append(
            {'kafka_partition': 0, 'kafka_offset': 12, 'event_date': str(day)})
        try:
            invalid = reconcile.run_verification(spark, source, day=str(day), source_path=source_path,
                bronze_path=paths['bronze'], quarantine_path=paths['quarantine'], manifest=conflicting)
        except ValueError as error:
            assert 'mapping conflicts' in str(error)
        else:
            raise AssertionError('Conflicting C5 date mapping accepted: ' +
                                 str(invalid['results']['reconciliation']))

        def audit_row(duplicates, late=1, durable=1, complete=True):
            spark.createDataFrame([(day, duplicates, late, durable, complete)],
                'event_date date, duplicates_dropped bigint, late_records_dropped bigint, '
                'durable_duplicates_ignored bigint, evidence_complete boolean').createOrReplaceTempView('verify_audit')

        def balance(parameters=params):
            return spark.sql(reconcile.render_query(queries['reconciliation'], **parameters)).first()

        audit_row(None, None, None, False)
        assert balance().match is False
        audit_row(3)
        assert balance().match is False
        # A compensating wrong drop count must not hide unassigned malformed C5.
        raw = spark.read.format('delta').option('versionAsOf', versions['quarantine']).load(paths['quarantine'])
        from pyspark.sql import functions as F
        (raw.withColumn('source_event_date', F.when(F.col('error_reason') != 'parse_error',
            F.to_date(F.try_to_timestamp(F.get_json_object('payload', '$.event_time')))))
            .createOrReplaceTempView('verify_quarantine'))
        audit_row(4)
        assert balance().match is False
        diagnostics = spark.sql(reconcile.render_query(queries['accounting_diagnostics'], **params)).first()
        assert diagnostics.rows_quarantine_unassigned == 2
        # Restore known source-date mapping for a deliberate duplicate C4 test.
        (spark.createDataFrame([(*row, day if row[3] in (10, 14, 11) else date(2019, 11, 2))
                               for row in quarantine_rows],
             contract_schema(spark, table_contract('quarantine')).add('source_event_date', 'date'))
         .createOrReplaceTempView('verify_quarantine'))
        spark.createDataFrame([bronze_rows[0]], bronze_schema).write.format('delta').mode('append').save(paths['bronze'])
        latest = int(DeltaTable.forPath(spark, paths['bronze']).history(1).first().version)
        audit_row(1)  # 9 = 3 C4 + 3 C5 + 1 duplicate + 1 late + 1 durable, but C4 violates uniqueness.
        assert balance({**params, 'bronze_version': latest}).match is False
        audit_row(2)
        assert balance().match is True  # Original pinned version remains reproducible.
        report.update(event='processing_query_acceptance_verified', fixture_run_id=run_id,
                      paths=paths, scanned_bronze_files=scans, partition_pruning_verified=True,
                      c5_ingest_date_not_event_date_verified=True, runner_left_versions_unchanged=True,
                      missing_audit_rejected=True, wrong_counts_rejected=True,
                      unknown_c5_date_cannot_be_hidden=True, duplicate_c4_cannot_be_hidden=True,
                      conflicting_c5_date_mapping_rejected=True,
                      snapshot_pinning_verified=True, artifacts_retained=True)
        output = ROOT / 'data' / 'raw' / f'processing-query-verification-{run_id}.json'
        with output.open('x', encoding='utf-8') as stream:
            json.dump(report, stream, indent=2, default=str)
        print(json.dumps({'event': report['event'], 'report': str(output),
                          'reconciliation': result, 'scanned_files': scans}, default=str), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    run()
