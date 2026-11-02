# Rule 5: stateful event_id dedup with a one-hour watermark

Implement Spark dropDuplicatesWithinWatermark(['event_id']) after quality
rules 1-4, with withWatermark('event_time','1 hour'). Invalid records must
still reach C5, including null event_time and timestamps outside source bounds.

First prove a single-query layout: valid branch -> watermark/dedup, union
with invalid branch -> existing foreachBatch. This preserves original two-sink
retry semantics while keeping invalid timestamps out of watermark calculation.
If Spark 3.5 rejects this layout, use independently checkpointed valid/error
queries without allowing valid dedup to swallow errors.

Stateful checkpoints are incompatible with the old stateless plan. Use a fresh
default checkpoint prefix; checkpoint identity records the processing layout
and watermark, while existing sink/source identity remains compatible.
Never delete or overwrite existing checkpoints/data to force adoption.

1. Implement a deterministic duplicate injector and count manifest + pure tests.
2. Add progress-based duplicates_dropped and late-record counters, handling
   repeated listener events and exposing state metrics in logs/tests.
3. Add valid-only stateful dedup and checkpoint-layout guard, keeping durable
   C4 MERGE as table-wide protection after state eviction.
4. Prove file/Kafka streaming, malformed/null-time bypass, same-ID resends,
   changed duplicate timestamps within 1h, restart state recovery, state expiry
   and late-data behavior. Compare dropped counters to injector manifests.
5. Run unit/CI-equivalent checks and review; update current operational docs.

Counters use successful state progress, not foreachBatch input scans. Prometheus
resets per driver, so retained stage reports sum counts for cross-driver manifest
comparison. Duplicates do not enter quarantine. Late records have their own count.
Keep all fixtures, reports, topics, state and previous checkpoint prefixes.

Verified outcome (2026-11-01):

- Spark 3.5.3 accepts the valid-stateful/invalid-bypass single-query union.
  Prototype drops two duplicates while invalid timestamps do not advance state.
- Real checkpoint checks reject stateless checkpoint adoption and changed layouts.
  The default uses kafka_to_delta_watermark_v1; existing checkpoints are retained.
- Kafka manifest case watermark-dedup-test-386af898f66d passes seed, recovery,
  expiry/boundary and changed-timestamp-within-hour stages. Acceptance report
  reconciles Kafka offsets: 39 = 6 C4 + 4 C5 + 26 state duplicate drops +
  2 late drops + 1 durable duplicate ignored. Expected and observed drops are 26.
- A new driver restores state; same ID at 03:01 then 03:30 is dropped by state,
  not the durable guard. Errors retain original payloads and bypass state.
- Stateful partial-commit regression kafka-stream-test-d6f3f3347503 passes
  fault/resume/append/verify: C4/C5 1/0 -> 1/2 -> 2/7, then unchanged;
  final Delta versions remain 2/2. Wrong-topic checkpoint reuse is rejected.
- Full suite: 240 passed with PySpark. Local CI-equivalent Python 3.11:
  238 passed, 2 optional PySpark callbacks skipped. Compose validation passes.
  No remote CI run, commit or push was performed.
- Independent review found no remaining important concerns after the two
  additional coverage checks above. Operational docs include retained evidence.
- No deletion commands were used; test topics, checkpoints and data are retained.
