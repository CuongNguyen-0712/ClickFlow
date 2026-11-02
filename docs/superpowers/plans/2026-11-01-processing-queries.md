# Processing SQL handoff implementation plan

> **For agentic workers:** Implement inline with superpowers:executing-plans, TDD, and one independent final review.

**Goal:** Run daily source/C4/C5 reconciliation and null profiles, demonstrate partition-pruned Spark SQL for 2019-11-01, and add evidence to the C4 handoff.

**Architecture:** Named read-only SQL statements in queries/verify.sql and sample_queries.sql consume a typed source view, attributed C5 view and explicit audit evidence. A small Spark runner replaces the reconcile.py stub, pins Delta versions, renders validated day/path/version parameters, writes the required six-column CSV and a JSON report. No Delta mutation, maintenance, deletion, schema migration or approval-state changes.

**Spec:** User screenshot: reconcile rows by event_date, null ratio for every column, sample SQL on s3a://lake/bronze/events with partition pruning, output event_date,rows_source,rows_bronze,rows_quarantine,duplicates_dropped,match; run for 2019-11-01 and include in C4 handoff.

## Decisions

- Work in existing clean feature/BIG-39-verify-and-sample-queries branch. No commit/push.
- C5 ingest_date is not event_date. Attribute valid payload dates only for errors whose timestamp passed validation; parse_error/bad_event_time need an explicit Kafka partition/offset→source date manifest. Unknown dates prevent a successful match.
- State duplicate counts are external evidence, never a subtraction of table counts. Keep state duplicates, late drops and durable ignored duplicates separate. Six-column match includes all three in its balance; diagnostics show hidden exclusions explicitly.
- Missing audit evidence yields duplicates_dropped=NULL and match=false. Manifest binds exact source/bronze/quarantine paths and validates integer counts, dates and unique Kafka keys. Sample mode has known zero stream drops and no C5 stage, so it uses an empty typed quarantine view.
- Source input is a closed C1 CSV cohort. For Kafka replay/injection, source cohort must include actual emitted rows; a monthly original CSV and an arbitrary Kafka metric total are different scopes.
- Pin C4/C5 versions before querying. This is a reproducible pair, not a cross-table transaction; run after successful completed batches on a closed cohort.
- Verify real sample100k against its own Delta table, execute default production-path sample queries as well, and report the main table's actual state rather than populate it for a false success.

## Tasks

- [x] Add failing tests for named query rendering, strict manifest scope/count/date validation, missing evidence and sample semantics.
- [x] Implement query bundle and runner: daily balance, all C1/C4/C5 null rates, diagnostics and duplicates, pruned examples and EXPLAIN; CSV + exclusive JSON reports; strict mismatch exit2.
- [x] Run real Spark fixture with two event dates, attributed/unattributed C5, nonzero duplicates/late/durable counts, wrong counts and missing evidence. Prove selected-partition file pruning and table versions unchanged.
- [x] Run actual 2019-11-01 sample100k, main-path queries and appropriate complete suite / Compose checks.
- [x] Add Make targets and README/runbook/C4 handoff evidence. Independent read-only review and fix important issues with reproducing tests.

## Progress

- Recon: verify.sql and reconcile.py are stubs; sample_queries.sql has four old examples, including unbounded scans. All previous task changes are in HEAD; no deletion is authorized for this task.
- RED→GREEN pure suite: 49 new tests failed on missing functionality, then all332 passed with PySpark3.5.3 on Python3.11.
- Real fixture da4bce68281f4c1199f0263243a715dd passed:9=2C4+3C5+2state+1late+1durable; all30 null profile rows correct;1 scanned file from2 dates; unknown evidence/dates and compensating duplicate counts fail; source/C4/C5 versions unchanged by runner. Test intentionally appends a duplicate only to its fixture afterward to prove pinned-version reproducibility.
- Actual sample CSV:100000,100000,0,0,true. Full2019-Nov.csv scan found1445360 source rows on2019-11-01; main C4/C5 currently0/0 with unknown drop evidence, correctly matchfalse/exit2. Reports processing-{sample,production}-59ff2843b8cd46558002a589c422fedd.{json,csv}.
- Independent review Important: mapped_event_date currently overrides validated C5 payload dates for all reasons. Added reproducer with Nov2 bad_event_type remapped to Nov1 and state drops2→1, which would falsely balance9 rows; run RED before restricting mapping to timestamp/schema-error cases.
- Deferred minor: make verify cannot forward an audit manifest (direct CLI supports it); conservative missing-evidence exit2 is documented.
- Deferred minor: a CSV-write failure after JSON serialization can append a second JSON error object, so that rare failure report may be invalid JSON; process still fails and Spark logs remain.
- Review scope rulings: daily duplicate/date profiles are not global C4 integrity checks; external counter authenticity and immutable cohort are caller contracts. These limits are explicitly documented rather than silently treated as proven.
- Review fix RED confirmed: bad mapping yields 9 source =2 C4+4 C5+1 state+1late+1durable, matchtrue despite a Nov2 row. Fix restricts manifest attribution to parse_error/bad_event_time and rejects conflicts against timestamp-validated payload dates before reporting; rerun regression and complete suite.
- Final GREEN: fixture b62428e85048490c975b1e46b6c23c16 passes all previous checks plus conflicting_c5_date_mapping_rejected=true. Exactly1 selected-day file scanned; pinning and no runner table mutations verified. Full suite332passed/0skipped, Compose config and git diff whitespace pass.
- Authorized task complete: both SQL bundles ran on2019-11-01, real sample/production CSV outcomes and final regression evidence are in C4 handoff/runbook. Contract group approval metadata remains pending, correctly unchanged. No physical deletion, commit, push or external messages performed.
