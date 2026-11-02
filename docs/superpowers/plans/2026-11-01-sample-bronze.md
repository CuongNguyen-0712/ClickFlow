# Sample 100k -> bronze

Implement the user's screenshot: explicit C1 schema through shared ddl(),
bigint category_id, sample mode selected in pipeline.yaml, Delta partitioned
by event_date, and read-back verification of exactly 100,000 rows.

Decisions: generate a bounded sample from MinIO for 2019-11-01.
The first 100k raw rows contain 27 duplicates; select 100k distinct events from
a bounded 110k-row candidate set to preserve C4's unique key. Store the CSV at the
requested data/sample/events_sample_100k.csv. Use a separate sample_events
Delta table, C4 types/nullability, Kafka metadata -1 and produced_at null.
Sample mode is a finite batch; Kafka streaming/quality routing remains the
separate upcoming task and must fail explicitly until implemented.

1. Add integration checks first: explicit types/nulls, duplicate selection,
   insufficient sample, C4 partitions/constraints, repeat without adding rows,
   refusal to replace a different existing batch. Keep all test artifacts.
2. Share the C1 Spark reader; implement Spark sample creation and configuration.
3. Implement sample dispatch, deterministic IDs, safe append into empty Delta,
   and full value/schema/row count read-back verification. Reuse a matching
   existing batch; reject conflicting tables. Never overwrite or delete.
4. Run integration checks and existing pytest, generate the real sample, run
   sample ingestion twice, inspect table metadata, and document evidence.

Preserve prior-turn changes to smoke_lake.py and runbook.md. No delete commands,
no automatic cleanup. Dataset remains on D and in MinIO. No Kafka startup.

## Completed verification

- Integration suite: 4 tests passed; existing pytest suite: 5 passed.
- Real sample: 100,000 distinct rows, 13,343,184 bytes. Regeneration reused
  identical existing CSV under the unchanged Spark source layout.
- Real Delta: 100,000 rows and distinct IDs, 15 typed C4 columns, bigint IDs,
  event_date partition 2019-11-01. Repeated ingestion reused version 1.
- Read-only verification after Docker restart confirmed the same table and
  counts; main bronze/events remains empty. All artifacts retained.
- Review found no important sink/schema defects. Sampling documentation
  clarifies candidate order depends on Spark's input partition layout; the
  materialized CSV is the fixed input for repeatable sample ingestion.
- Report: data/raw/sample-bronze-verification.json. Runbook contains commands
  and evidence. Kafka streaming remains the separate later task.
