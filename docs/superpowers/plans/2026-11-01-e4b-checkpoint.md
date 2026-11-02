# E4b checkpoint failover implementation plan

> **For agentic workers:** Use superpowers:executing-plans, inline execution.

**Goal:** Kill an owned Spark driver during ingestion, resume the production stream from its unchanged checkpoint, and demonstrate lossless, duplicate-free C4 results on every affected date.

**Architecture:** A native controller runs isolated Spark driver subprocesses and observes checkpoint objects through MinIO. A benchmark-only callback blocks after a real Delta commit and before the Spark batch commit. Recovery uses the production entrypoint without the callback; a separate Spark verification job compares producer receipts and runs the existing daily reconciliation SQL.

**Tech stack:** Spark 3.5.3, Delta 3.2.1, Kafka, MinIO, Python, Bash, Pillow/FFmpeg.

**Spec:** User E4b screenshot: actual driver kill, checkpoint restart, no missing/duplicate rows, logs, reconciliation, short video.

## Constraints and review focus

- No deletion, no existing topic/checkpoint/table/result modification, no service restart.
- Kill only the controller-owned SparkSubmit process group after validating PID, PGID and command identity.
- Replay one distinct 20k-row cohort from the real 100k sample, shift timestamps across two November dates within a one-hour span; preserve source CSV and delivery receipts.
- Distinguish attempted replay duplicates from distinct Kafka source records in reconciliation.
- Require committed offsets, unchanged query/checkpoint identity, identical pre-kill C4 rows, and complete source-to-sink equality; counts alone are insufficient.
- Record actual terminal output and timing; video is explicitly an accelerated terminal replay.

## Task 1: Reliable harness boundaries

Files: `tests/test_e4_failover.py`, `src/monitoring/e4b_failover.py`, `src/monitoring/terminal_recording.py`.

- [x] Write and run failing tests for cohort dates/IDs, strict offset parsing, unsafe kill targets, invalid receipts and future output in recordings.
- [x] Implement the pure helpers; rerun focused tests.

## Task 2: Actual crash and recovery

Files: `src/monitoring/e4b_driver.py`, `src/monitoring/e4b_verify.py`, `benchmarks/e4_failover.sh`, benchmark requirements, Makefile.

- [x] Implement the post-Delta/pre-checkpoint gate, owned process supervision, continuous publisher and checkpoint observations.
- [x] Implement exact C1/C4 comparison, historical row preservation, two-day `reconcile.py` reports and fail-closed completion.
- [x] Add recording/video rendering, invocation and reproduction documentation.
- [x] Run unit suite and independent read-only review; resolve findings before running the actual kill.

## Task 3: Retained evidence

- [x] Execute E4b on the healthy local stack without competing Spark applications.
- [x] Inspect before/after logs, receipts, checkpoints, reports and decoded video.
- [x] Record actual results and limitations in README and the case report; rerun relevant checks after fixes.

## Execution notes

- 18 new boundary tests initially failed; implementation brought the suite to 396 passing tests. Independent read-only review found no critical/important issues.
- First startup attempt stopped before publishing: logger `event` argument collision. Watched regression fail, fixed helper, 397 passing tests; retained failed case with explicit status.
- Second startup attempt delivered 1,000 seed rows then rejected the real two-source union offset log. Read the actual checkpoint; added failing mirrored-offset regression, required both entries to agree/count once. Suite: 399 passing tests. Failed case retained and labeled.
- Ruling: recovery invokes the production spark-submit entrypoint directly rather than GNU Make inside Docker, where Make is unavailable. The runbook states this explicitly and documents custom config support for `make stream`.
- Successful case: `benchmarks/results/e4b-1016f257a6024eb085ee425572dbdaea`. Actual SIGKILL exit -9 during publishing, checkpoint held at 1000 while C4 held 6000; replay ignored exactly 5000 attempts. Final exact 20000 records, zero missing/extra/duplicate/field mismatches, all 6000 historical rows unchanged. Daily matches: 10027 (Nov1), 9973 (Nov2).
- Independent read-only evidence review found no blocker and independently checked implementation/source hashes, receipt uniqueness and cast/stdout equality. FFmpeg decoded all 100 frames of the 25-second H.264 terminal replay; saved validation report and original cast. Final suite 399 pass, 14 upstream deprecation warnings.
