# Storage chapters writing plan

**Goal:** Complete TV3's chapter2 theory and chapter4 storage design in Vietnamese,
with primary references and traceable E3/E4b/E5 measurements.

**Scope:** Chapter2 reserves Kafka theory for TV2; chapter4 covers the storage
subsection. Existing source/config/contracts/results are evidence, not edited.
Use the project's report/chNN convention and one original architecture figure.

- [x] Read contracts, writer/dedup/maintenance/reconciliation and allthree benchmark reports.
- [x] Verify theory against official Delta/Spark sources, pinned Spark3.5.3/Delta3.2.1 behavior.
- [x] Write ch02_data_lake_delta.md, ch04_storage.md, references_storage.md and diagram source/export.
- [x] Link chapter files from outline; add merge notes and evidence ledger with exact metric definitions.
- [x] Check links, cited numbers, schema counts and image; independent read-only technical review.

Critical checks: no cross-table atomicity claim; stable-ID single-writer MERGE
provides convergence; watermark is not global uniqueness; late drops distinct
from duplicate drops; produced_at→ingested_at ends at batch start, not commit;
E3 pacing misses retained; E4b shifted20k vs E5 original100k; active vs physical
bytes; C4/C5 approval stillpending; rawConnect config not asserted live-tested.

Validation:60local file/anchor links and76 numeric table values checked against
original CSV/JSON; PNG1800x1100 viewed. Independent read-only review found no
important factual issue. Removed ambiguous C4→C5 sequence arrow from PNG to
match Mermaid data flow, clarified bothsink callback order and source-day mapping
for every parse_error/bad_event_time row. No source/config/contract/benchmark edits
or runtime jobs were needed for this writing task.

