# C4/C5 review candidate

User task: settle C4's 14 payload columns plus event_date, uniqueness of
event_id, C5's original payload/error_reason/ingest_date, and Processing
handoff. Completion includes green test_schemas.py and actual team approval.

Preserve existing C4 (15 physical fields) and C5 (6 physical fields), field
order, types and nullability. Add explicit partition declarations, shared
C5 schema/error vocabulary, contract version/governance metadata, timestamp
semantics, rule precedence and replay/uniqueness expectations. The 1-hour
watermark is not a table-wide unique constraint. Physical schemas stay
compatible with existing MinIO tables; this task does not implement Kafka
streaming or quality routing.

1. Extend schema tests first for both contracts, nullability, explicit
   partition fields, column counts and error vocabulary/precedence.
2. Update YAML contracts, shared schemas and partition expansion in init_lake.
3. Write the Processing handoff/review record and read-only example queries.
4. Run schema tests and full pytest, verify real Delta schemas without
   writes/deletes, request independent review, record results.

Governance: version 1.0.0 review candidate, team approval pending with no
invented approvals. Required reviewer roles: Sources, Ingestion, Storage,
Infra and the Processing consumer. No external messages or deletion commands.

## Verification and remaining acceptance

Technical work complete: 9 schema tests and 12 total pytest tests passed.
The new partition-expansion test first exposed the old hardcoded date/nullable
behavior and passed after reading declared partition definitions. Independent
review's conflict-comparison ambiguity was fixed and regression checked.

Read-only Spark validation confirmed all existing Delta field signatures
and partitions: bronze/events 15 fields/0 rows, quarantine/events 6/0,
bronze/sample_events 15/100000 unique rows. All four handoff SQL queries ran.
Report: data/raw/c4-c5-contract-verification-2026-11-01.json.

No remaining important review findings. Actual team approval is still pending
and is explicitly recorded in both YAMLs and docs/contracts-c4-c5-handoff.md;
the task cannot honestly be marked fully Done until that acceptance exists.
