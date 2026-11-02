# Rules 1–4, counters, tests

The classifier and 161 pure validation tests were implemented with Kafka
streaming. Complete the screenshot task without duplicating that work.

1. Add tests proving primary C5 reasons map to four numbered rules, including
   aggregation of missing_id and negative_price into rule 4; zero counters
   must be exported and invalid count inputs must not partly mutate metrics.
2. Add a Spark-free quality_rule_counts helper and committed-quarantine
   Prometheus rule counters. Keep first-matching reason semantics and existing
   per-reason counters; retries must not increment committed-rule counts.
3. Run a retained UUID Spark/MinIO fixture covering all rules, valid nullable
   optional fields, exact bigint IDs, payload preservation and replay twice.
4. Run full pytest and Compose validation, plus Python 3.11 dependency install
   and unit checks matching GitHub CI. Report local workflow-equivalent checks
   accurately; do not claim a remote Actions run without evidence.
5. Document counters and test coverage, request review, record results.

No delete commands, automatic artifact cleanup or topic/table overwrites.
Contract approval remains pending; the classifier policy is unchanged.

## Completed technical verification

- New tests observed RED (9 failures for missing aggregation/series/validation),
  then GREEN. Full Spark-container suite: 196 passed, including 168 quality
  classifier/aggregation tests. No classifier policy rewrite was needed.
- Retained Spark/MinIO fixture: 9 inputs -> 1 bronze + 8 quarantine. Reasons
  2 parse_error, 2 bad_event_time, 1 bad_event_type, 2 missing_id, 1 negative_price;
  grouped counters 1..4 = 2/2/1/3. Retry adds zero rows/counts. Exact bigint,
  nullable optionals, raw payload, HTTP metric values all verified.
- Report: data/raw/quality-rules-verification-70767c47e8a245d784237b2d4331b3cf.json.
- Python 3.11 Linux CI-equivalent run installed all pinned requirements;
  pytest 194 passed/2 optional Spark callback skips. Compose validation passed.
  Retained container clickflow-quality-ci-20261101 exited0, log in
  data/raw/quality-rules-ci-python311.log. No remote Actions run claimed.
- Independent review found no blocking correctness issues. Documentation
  describes primary-reason counting, rule4 aggregation and driver resets.
- No deletion commands or automatic artifact cleanup performed.
