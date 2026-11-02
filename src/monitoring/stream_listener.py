"""Spark 3.5 streaming listener; common.metrics intentionally stays Spark-free."""

import json
import logging

from pyspark.sql.streaming import StreamingQueryListener


LOGGER = logging.getLogger(__name__)


class StreamMetricsListener(StreamingQueryListener):
    """Observe one named query without allowing instrumentation to fail it."""

    def __init__(self, metrics):
        super().__init__()
        self.metrics = metrics
        self._runs = set()

    def onQueryStarted(self, event):
        try:
            if event.name == self.metrics.query_name:
                self._runs.add(str(event.runId))
                self.metrics.query_started()
        except Exception:
            LOGGER.exception("Unable to record streaming query start metrics")

    def onQueryProgress(self, event):
        try:
            progress = json.loads(event.progress.json)
            if progress.get("name") == self.metrics.query_name:
                self.metrics.record_progress(progress)
        except Exception:
            LOGGER.exception("Unable to record streaming query progress metrics")

    def onQueryTerminated(self, event):
        try:
            run_id = str(event.runId)
            if run_id in self._runs:
                self._runs.discard(run_id)
                self.metrics.query_terminated(failed=event.exception is not None)
        except Exception:
            LOGGER.exception("Unable to record streaming query termination metrics")

    def onQueryIdle(self, event):
        """Spark 3.5 idle notifications leave the last progress gauges intact."""


def attach_listener(spark, metrics):
    """Attach before starting the query and return the removable listener."""
    listener = StreamMetricsListener(metrics)
    spark.streams.addListener(listener)
    return listener
