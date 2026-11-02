# Kafka -> bronze + quarantine with restartable checkpoints

Implement screenshot task: Spark readStream Kafka C3, strict typing and rules
1-4, C4/C5 partitioned Delta outputs, 10s trigger, maxOffsetsPerTrigger,
MinIO checkpoint, StreamingQueryListener metrics, proven stop/restart.

Architecture: one query, foreachBatch, validated normalized C3 plus original
payload. Insert-only MERGE bronze by event_id and quarantine by Kafka
partition/offset. Within a batch choose deterministic first event per ID.
Persisted keys protect retries even after one sink committed and the other
failed, and replay outside any watermark. Watermark remains an event-time
progress marker; durable sink dedup preserves late valid unique events.
Only one writer per target pair; no cross-table atomic visibility guarantee.
Bind checkpoint to topic/brokers/output paths/schema versions, and bind each
sink pair to one topic to protect C5 offset identity. Fail on mismatch.

1. Independently implement pure C3 validation/classifier + tests in
   quality_rules.py; expose classify_payload and classify_records(frame).
2. Independently implement Prometheus metrics + StreamingQueryListener and
   metrics tests. No Spark import required for generic metrics tests.
3. Implement streaming source/config/checkpoint binding/idempotent sink,
   preserving sample mode. Default stream.source=kafka; make stream runs it.
4. Start Kafka x3, create configured topic without replacing existing data,
   run focused Spark and real Kafka tests using retained unique topic/prefixes.
5. Test two driver processes across stop/restart, duplicate IDs with changed
   metadata, all error reasons and payload retention. Inject failure after
   bronze commit before quarantine to prove batch retry convergence. Check
   listener metrics, full pytest and regressions, then independent review.

No deletion commands, automatic test cleanup, topic removal or overwrites.
Keep Kafka topics, Delta outputs, checkpoints, logs and test reports.
Group contract approval remains pending as recorded; no invented sign-offs.

## Completed verification

- 187 unit tests passed, including 161 classifier cases, configuration and
  Prometheus/real Spark listener callbacks. Compose validation and diff check pass.
- Real Kafka topic ecom.events.raw exists with 6 partitions/RF3 and ISR3.
  Test topic clickflow-stream-test-767d9cd4c267 uses its own retained S3 paths.
- Four fresh Spark drivers: fault after bronze commit (1/0 rows), resume same
  query ID/new run ID (1/2), append/replay/conflict (2/7, all 5 reasons),
  committed-checkpoint restart (2/7, Delta versions remain 2/2).
- Quarantine payload whitespace and null tombstone retained; bigint exact,
  first bronze row/metadata preserved on retries; metrics HTTP scraped.
- Independent review found an unbound-checkpoint adoption hole. A retained
  Spark regression failed against old code, then passed after preflight guard
  before table initialization; final review has no important findings.
- Default continuous CLI served metrics 8002, stopped via exact-PID SIGTERM
  with exit0, then default drain resumed same checkpoint/query ID with exit0.
- Sample mode regression still verifies 100000 rows, reused=true.
- Runtime report: data/raw/kafka-stream-verification.json. Runbook documents
  single-writer/two-commit semantics, retained-topic lifecycle, commands,
  checkpoint binding, monitoring and reproducible integration stages.

No deletion commands or cleanup. Test artifacts and all checkpoint/data remain.
This is functional/recovery verification, not a full-dataset throughput claim.
