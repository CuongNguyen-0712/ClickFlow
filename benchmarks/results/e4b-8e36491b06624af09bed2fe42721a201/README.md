# E4b startup attempt — not a successful failover

This retained attempt stopped before publishing any Kafka records. The native
controller's logging helper had an argument collision with the production
`kafka_stream_started` event. The owned first driver was stopped gracefully
(exit 0); no SIGKILL/recovery/daily verification took place.

See `terminal.log` and `before-driver.log`. Its video records this failed attempt,
not a passing experiment. The source CSV, isolated empty tables/checkpoint/topic
and stopped container are retained without deletion. A regression test was added
before fixing the logger; the subsequent case uses a new isolated resource scope.
