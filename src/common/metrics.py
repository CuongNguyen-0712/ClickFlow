"""Driver-owned Prometheus metrics, with no dependency on PySpark.

Sink counters describe rows actually inserted into Delta. Spark progress can
count source rows repeatedly when foreachBatch performs multiple actions; those
input observations are operational gauges, never the committed-row audit.
Stateful drop counters accumulate Spark's per-batch operator counts separately.
"""

import json
import logging
import math
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

from src.storage.quality_rules import quality_rule_counts


LOGGER = logging.getLogger(__name__)


class StreamMetrics:
    """One query's metrics and optional HTTP server, owned by the driver.

    Counters reset when the driver restarts. Ignored-duplicate and conflict
    counters describe attempted rows ignored and conflicting IDs observed during
    successfully handled batches. Stateful drop counters describe Spark progress;
    neither set is a persistent Delta audit total.
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
        self._duplicates_dropped = counter("duplicates_dropped_total", "Rows dropped by Spark stateful deduplication")
        self._late_records_dropped = counter("late_records_dropped_total", "Rows dropped by Spark stateful operators after the watermark")
        self._progress_run = None
        self._last_state_batch = -1
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

    @staticmethod
    def _progress_count(value, field):
        """Accept only finite nonnegative integer counts representable by Prometheus."""
        try:
            number = Decimal(str(value))
            if (not number.is_finite() or number < 0 or number != number.to_integral_value()
                    or not math.isfinite(float(number))):
                raise ValueError("expected a finite nonnegative integer")
            return int(number)
        except (InvalidOperation, TypeError, ValueError, OverflowError):
            LOGGER.warning("Ignoring malformed streaming progress field %s", field)
            return None

    def _record_state_drops(self, progress):
        """Count each batch once in the current run, using constant memory.

        Spark supplies ordered batch IDs and stable query/run IDs. Repeated or
        older notifications in that run cannot add counts again. Without a
        complete valid identity each call is an independent observation.
        """
        query_id, run_id = progress.get("id"), progress.get("runId")
        query_id = query_id if isinstance(query_id, str) and query_id else None
        run_id = run_id if isinstance(run_id, str) and run_id else None
        batch_id = None
        if "batchId" in progress:
            batch_id = self._progress_count(progress["batchId"], "batchId")
        if query_id is not None and run_id is not None and batch_id is not None:
            run = (query_id, run_id)
            if run == self._progress_run and batch_id <= self._last_state_batch:
                return
            self._progress_run = run
            self._last_state_batch = batch_id

        duplicates, late = 0, 0
        operators = progress.get("stateOperators", [])
        if not isinstance(operators, (list, tuple)):
            LOGGER.warning("Ignoring malformed streaming progress field stateOperators")
            operators = []
        for operator in operators:
            if not isinstance(operator, Mapping):
                LOGGER.warning("Ignoring malformed streaming progress state operator")
                continue
            custom = operator.get("customMetrics", {})
            if not isinstance(custom, Mapping):
                LOGGER.warning("Ignoring malformed streaming progress field customMetrics")
            elif "numDroppedDuplicateRows" in custom:
                duplicates += self._progress_count(
                    custom["numDroppedDuplicateRows"], "customMetrics.numDroppedDuplicateRows"
                ) or 0
            if "numRowsDroppedByWatermark" in operator:
                late += self._progress_count(
                    operator["numRowsDroppedByWatermark"], "numRowsDroppedByWatermark"
                ) or 0
        self._duplicates_dropped.inc(duplicates)
        self._late_records_dropped.inc(late)
        LOGGER.info(json.dumps({
            "event": "dedup_progress", "query_id": query_id, "run_id": run_id,
            "batch_id": batch_id, "duplicates_dropped": duplicates, "late_records_dropped": late,
        }))

    def record_progress(self, progress):
        """Update Spark observations; malformed fields cannot fail the query."""
        if not isinstance(progress, Mapping):
            LOGGER.warning("Ignoring malformed streaming progress payload")
            return
        self._batches.inc()
        self._record_state_drops(progress)
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
