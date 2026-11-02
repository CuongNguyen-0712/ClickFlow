# Delta maintenance implementation plan

> **For agentic workers:** Use superpowers:executing-plans; implement inline with TDD and a final independent review.

**Goal:** Compact a declared closed C4 partition with ZORDER, report active file changes for E5, and preview safe table-wide VACUUM.

**Architecture:** Keep the existing Spark 3.5.3 / Delta 3.2.1 / S3A stack. A validated maintenance plan requires a target date and an exclusive closed-before date, rejects open/future days and unsafe retention. OPTIMIZE is partition scoped; VACUUM is table scoped, defaults to DRY RUN and requires explicit execution opt-in after user approval.

**Spec:** User screenshot: src/storage/maintenance.py, daily closed-partition OPTIMIZE ZORDER BY (user_id, product_id), safe subsequent VACUUM, before/after file logs, measurable small-file reduction for E5.

## Constraints and decisions

- Existing feature/BIG-38-delta-maintenance-optimize-vacuum workspace is already isolated; keep user changes and do not commit/push.
- User authorizes non-deletion operations. Never execute physical VACUUM or cleanup without asking; preserve fixtures and previous data.
- The closed-before date is an operator declaration that replay/backfill/writers no longer write earlier dates. Wall-clock age or watermark alone cannot prove closure.
- User clarified time travel 30 days, stream recovery 7 days: default file retention/time-travel requirement is 720 hours and maximum stream lag is 168 hours. Retention must also exceed maximum transaction and stream-lag windows, and respect existing table deleted-file retention. Log retention must cover required time travel.
- Use pinned Delta snapshot AddFile metadata for active file/byte statistics; do not count obsolete files still retained physically. Track table and target partition independently.
- DRY RUN does not reclaim disk space. Never disable retention safety, lower table properties, or add scheduled physical deletion.
- Daily invocation is an external scheduler's responsibility; provide exact Make/CLI recipes rather than installing host scheduled tasks.

## Task 1: Maintenance implementation and guards

- [x] Write pure tests proving closed-date, URI, retention and table-property guards, plus explicit DRY RUN versus execute SQL behavior; run RED.
- [x] Implement validated MaintenancePlan, config resolution, and SQL command generation. Run GREEN.
- [x] Add metadata snapshots, partition-scoped OPTIMIZE, post-OPTIMIZE VACUUM preview/explicit execution, structured logs and exclusive JSON reports.

## Task 2: Real Spark/MinIO verification

- [x] Create a retained C4-schema fixture with many small files in one closed date and a separate untouched partition. Run maintenance, prove file reduction, exact row/schema preservation, original version time travel, unchanged other-partition file paths and preview-only VACUUM.
- [x] Verify absent/open partitions and unsafe table retention fail before OPTIMIZE. Verify empty-partition skip does not run VACUUM. Keep all artifacts.
- [x] Run full Python 3.11/PySpark unit suite and Compose validation.

## Task 3: Operations and review

- [x] Add maintenance config, Make targets, daily invocation/closure/retention guidance and E5 evidence.
- [x] Independent code review; fix important issues with reproducing tests, then verify.
- [x] Present exact target, retention and preview candidates before requesting authorization for physical VACUUM. Do not interpret missing authorization as consent.

## Progress

- Recon: maintenance.py is a docstring stub; E5 benchmark is a placeholder. Existing feature branch is clean. Spark containers already include Delta 3.2.1.
- User decision: time travel 30 days, stream recovery 7 days. Only DRY RUN is authorized until the user explicitly approves physical VACUUM.
- RED: 39 new unit cases failed on missing maintenance functionality, original 240 passed. Real MinIO fixture also reached missing MaintenancePlan after writing its 15 small files; all artifacts retained.
- Unit GREEN: 283 passed on Python 3.11 with PySpark. First runtime pass exposed Scala default-argument handling: DeltaLog.update requires explicit false/None via Py4J. Confirmed against pinned 3.2.1 source and corrected both metadata reads; rerun the real regression.
- Integration GREEN: maintenance-verification-0f53e3542bfd432480e866918b7198d3.json confirms target active/small files 12→1, total table 15→4, rows 6000→6000, exact rows/schema, other partition paths and original-version reads preserved. VACUUM DRY RUN720 found0 candidates; no physical VACUUM. Empty partition skips work; disabled safety and shortening60day existing retention are rejected before OPTIMIZE.
- Final review: no Critical/Important findings. Deferred minor: Spark startup failure can leave the pre-reserved report empty; startup diagnostics remain in spark-submit logs.
- Review scope: physical VACUUM unexecuted pending user approval; fresh fixtures prove immediate time travel, not 30 days of elapsed retention. Closed-before is an operator declaration, and configured168h stream lag is not a seven-day outage simulation. These limits are explicit in operations documentation.
- Final full suite: 283 passed, 0 skipped (maintenance-unit-final.log), pip check and Compose config pass. CLI on sample100k completed with report maintenance-sample-100k-d08972cae2584e75a4a9764b2d11c211.json, version1→2, files1→1 (already a single file), VACUUM720DRYRUN0 candidates. Read-only exact sample comparison is the remaining validation before requesting physical VACUUM approval.
- Sample readback passed: 100000 rows and unique event IDs, schema exact, both exceptAll directions empty, v1 still readable. Evidence: matching -readback.json and maintenance-sample-readback.log. Implementation and authorized checks are complete; only physical VACUUM remains pending user permission. No cleanup, deletion, commit or push performed.
- User subsequently approved physical VACUUM once on s3a://lake/bronze/sample_events at720hours. Started the existing CLI with --execute-vacuum and that explicit path/retention; report maintenance-vacuum-approved-dd384b6b5a8341d1b961340404243e6f.json. No authorization for cleanup or other table deletion is inferred.
- Approved CLI finished exit0: OPTIMIZE version2→3 then exactly one physical VACUUM RETAIN720. Fresh preview0 candidates; Delta reports Deleted0 files/directories. No other deletion executed.
- Final readback exit0: 100000 rows and unique IDs, schema unchanged, both exceptAll directions with version1 empty, version1 remains readable. Evidence: maintenance-vacuum-approved-dd384b6b5a8341d1b961340404243e6f-readback.json and maintenance-vacuum-approved-readback.log. All task steps complete within the authorized scope; no remaining approval needed for this run. No commit/push performed.
