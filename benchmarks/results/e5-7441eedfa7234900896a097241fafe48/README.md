# E5 startup attempt — not a completed benchmark

Extraction succeeded: 100,000 original records, 50,000 per 2019-11-01/02.
See `workload.json` and `cohort.csv`. Before any variant writes or timed queries,
Hadoop S3A rejected the upload's unqualified local source path while comparing
it to its enumerated `file:///` FileStatus URI. The driver stopped; exit1.

The upload source was corrected to an explicit file URI, matching the existing
`smoke_lake.py` implementation. This attempt is retained for diagnosis and is
not used as E5 performance evidence. Subsequent runs use a new isolated scope.
