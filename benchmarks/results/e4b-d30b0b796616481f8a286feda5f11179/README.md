# E4b checkpoint-format attempt — not a successful failover

The producer acknowledged 1,000 seed records. The controller then rejected the
real checkpoint format: production's valid/invalid union writes two identical
logical Kafka source offset entries, while the first observer expected one.
The owned first driver was stopped gracefully (exit 0); no SIGKILL/recovery/daily
verification took place. See `terminal.log`, `deliveries.jsonl` and
`before-driver.log`. This video's contents describe the failed attempt only.

The observer was corrected after a failing regression test: mirrored source
offsets are counted once, and mismatched source entries still fail closed.
All local artifacts, isolated topic, tables, checkpoint and stopped container
are retained without deletion. A subsequent case uses new resources.
