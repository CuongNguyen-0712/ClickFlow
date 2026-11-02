"""Driver-owned Prometheus metrics, with no dependency on PySpark.

Sink counters describe rows actually inserted into Delta. Spark progress can
count source rows repeatedly when foreachBatch performs multiple actions; those
observations are operational gauges, never the committed-row audit.
"""

import logging
import math
from collections.abc import Mapping
from datetime import datetime, timezone

from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

from src.storage.quality_rules import quality_rule_counts


LOGGER = logging.getLogger(__name__)


class StreamMetrics:
    """One query's metrics and optional HTTP server, owned by the driver.

    Counters reset when the driver restarts. Duplicate and conflict counters
    describe attempted rows ignored and conflicting IDs observed during
    successfully handled batches; they are not persistent Delta audit totals.
    """

    def __init__(self, query_name="clickflow-kafka-to-delta", port=None):
        self.query_name = query_name
        self.registry = CollectorRegistry()
        self.server = None
        self.thread = None

        def counter(name, description, labels=()):
            return Counter("clickflow_stream_" + name, description, labels, registry=self.registry)

        def gauge(name, description):
            return Gauge("clickflow_stream_" + name, description, registry=self.registry)

        self._sink_rows = counter("sink_rows_total", "Rows actually inserted into a Delta sink", ("destination",))
        self._quarantine_errors = counter("quarantine_errors_total", "Inserted quarantine rows by error reason", ("reason",))
        self._quality_rules = counter("quality_rule_violations_total", "Inserted quarantine rows by first-matching rule", ("rule",))
        for rule in range(1, 5):
            self._quality_rules.labels(rule=str(rule))
        self._duplicates = counter("duplicates_ignored_total", "Attempted duplicate rows ignored in handled batches")
        self._conflicts = counter("conflicting_event_ids_total", "Conflicting event IDs observed in handled batches")
        self._batches = counter("batches_total", "Spark progress events observed")
        self._failures = counter("failures_total", "Query terminations with an exception")
        self._running = gauge("running", "Whether the query is running")
        self._input_rate = gauge("input_rows_per_second", "Spark-reported source input rows per second")
        self._processing_rate = gauge("processed_rows_per_second", "Spark-reported processing rows per second")
        self._batch_id = gauge("batch_id", "Latest observed Spark batch ID")
        self._batch_duration = gauge("batch_duration_milliseconds", "Latest Spark triggerExecution duration in milliseconds")
        self._input_rows = gauge("input_rows", "Latest Spark-reported input rows, which may include repeated scans")
        self._watermark = gauge("event_watermark_seconds", "Latest available event-time watermark as Unix seconds")
        if port is not None:
            self.server, self.thread = start_http_server(port, addr="0.0.0.0", registry=self.registry)

    def record_sink(self, destination, inserted, errors=None):
        """Record only the insert counts returned after a successful sink commit."""
        rules = quality_rule_counts({} if errors is None else errors) if destination == 'quarantine' else {}
        self._sink_rows.labels(destination=destination).inc(inserted)
        if destination == "quarantine" and errors:
            for reason, count in errors.items():
                self._quarantine_errors.labels(reason=reason).inc(count)
        for rule, count in rules.items():
            self._quality_rules.labels(rule=str(rule)).inc(count)

    def record_ignored(self, duplicates, conflicts):
        """Record attempted ignored rows and conflict IDs after batch success."""
        self._duplicates.inc(duplicates)
        self._conflicts.inc(conflicts)

    @staticmethod
    def _set_number(gauge, value, field):
        if value is None:
            return
        try:
            number = float(value)
            if not math.isfinite(number) or number < 0:
                raise ValueError("expected a finite nonnegative number")
            gauge.set(number)
        except (TypeError, ValueError, OverflowError):
            LOGGER.warning("Ignoring malformed streaming progress field %s", field)

    def record_progress(self, progress):
        """Update Spark observations; malformed fields cannot fail the query."""
        if not isinstance(progress, Mapping):
            LOGGER.warning("Ignoring malformed streaming progress payload")
            return
        self._batches.inc()
        for field, metric in (
            ("batchId", self._batch_id),
            ("numInputRows", self._input_rows),
            ("inputRowsPerSecond", self._input_rate),
            ("processedRowsPerSecond", self._processing_rate),
        ):
            self._set_number(metric, progress.get(field), field)
        duration = progress.get("durationMs")
        if isinstance(duration, Mapping):
            self._set_number(self._batch_duration, duration.get("triggerExecution"), "durationMs.triggerExecution")
        event_time = progress.get("eventTime")
        if isinstance(event_time, Mapping) and event_time.get("watermark"):
            try:
                timestamp = datetime.fromisoformat(event_time["watermark"].replace("Z", "+00:00"))
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                self._watermark.set(timestamp.timestamp())
            except (TypeError, ValueError, AttributeError, OverflowError):
                LOGGER.warning("Ignoring malformed streaming event-time watermark")

    def query_started(self):
        self._running.set(1)

    def query_terminated(self, failed=False):
        self._running.set(0)
        if failed:
            self._failures.inc()

    def close(self):
        """Stop this driver's metrics endpoint, safely allowing repeated calls."""
        server, thread = self.server, self.thread
        if server is None:
            return
        self.server = None
        self.thread = None
        try:
            server.shutdown()
        finally:
            server.server_close()
            thread.join(timeout=5)
