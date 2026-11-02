"""Prometheus observations must remain separate from committed sink audit rows."""

import builtins
import importlib
import json
from types import SimpleNamespace
from urllib.request import urlopen
from uuid import UUID

import pytest
from prometheus_client import generate_latest

from src.common import metrics as metrics_module


def make_metrics(**kwargs):
    assert hasattr(metrics_module, "StreamMetrics"), "StreamMetrics is not implemented"
    return metrics_module.StreamMetrics(**kwargs)


def sample(metrics, name, labels=None):
    return metrics.registry.get_sample_value("clickflow_stream_" + name, labels or {})


def test_committed_sink_counts_are_separate_from_observed_progress():
    metrics = make_metrics()
    metrics.record_sink("bronze", 7)
    metrics.record_sink("quarantine", 3, errors={"invalid_json": 2, "invalid_price": 1})
    metrics.record_sink("bronze", 0)
    metrics.record_ignored(duplicates=4, conflicts=2)
    metrics.record_progress({"batchId": 0, "numInputRows": 60})

    assert sample(metrics, "sink_rows_total", {"destination": "bronze"}) == 7
    assert sample(metrics, "sink_rows_total", {"destination": "quarantine"}) == 3
    assert sample(metrics, "quarantine_errors_total", {"reason": "invalid_json"}) == 2
    assert sample(metrics, "quarantine_errors_total", {"reason": "invalid_price"}) == 1
    assert sample(metrics, "duplicates_ignored_total") == 4
    assert sample(metrics, "conflicting_event_ids_total") == 2
    assert sample(metrics, "input_rows") == 60
    assert b'clickflow_stream_sink_rows_total{destination="bronze"} 7.0' in generate_latest(metrics.registry)


def test_instances_have_independent_collectors_and_no_default_process_metrics():
    first = make_metrics()
    second = make_metrics()
    first.record_sink("bronze", 2)
    second.record_sink("bronze", 5)
    assert sample(first, "sink_rows_total", {"destination": "bronze"}) == 2
    assert sample(second, "sink_rows_total", {"destination": "bronze"}) == 5
    assert b"process_cpu_seconds" not in generate_latest(first.registry)


def test_progress_exports_spark_rates_batch_duration_watermark_and_lifecycle():
    metrics = make_metrics()
    metrics.query_started()
    metrics.record_progress({
        "batchId": 4,
        "numInputRows": 25,
        "inputRowsPerSecond": 12.5,
        "processedRowsPerSecond": 20.0,
        "durationMs": {"triggerExecution": 1250},
        "eventTime": {"watermark": "2020-01-01T00:00:00.000Z"},
    })
    assert sample(metrics, "running") == 1
    assert sample(metrics, "input_rows_per_second") == 12.5
    assert sample(metrics, "processed_rows_per_second") == 20
    assert sample(metrics, "batch_id") == 4
    assert sample(metrics, "batch_duration_milliseconds") == 1250
    assert sample(metrics, "input_rows") == 25
    assert sample(metrics, "batches_total") == 1
    assert sample(metrics, "event_watermark_seconds") == 1577836800
    metrics.query_terminated(failed=True)
    assert sample(metrics, "running") == 0
    assert sample(metrics, "failures_total") == 1
    metrics.query_started()
    metrics.query_terminated()
    assert sample(metrics, "failures_total") == 1


def test_optional_fields_and_bad_progress_do_not_interrupt_observations(caplog):
    metrics = make_metrics()
    metrics.record_progress({"batchId": 0, "numInputRows": 3})
    metrics.record_progress({
        "batchId": 1, "numInputRows": "bad", "inputRowsPerSecond": float("nan"),
        "durationMs": None, "eventTime": {"watermark": "invalid"},
    })
    metrics.record_progress(None)
    assert sample(metrics, "input_rows") == 3
    assert sample(metrics, "input_rows_per_second") == 0
    assert sample(metrics, "batch_id") == 1
    assert sample(metrics, "batches_total") == 2
    assert caplog.records


def test_http_endpoint_serves_the_owned_registry_and_close_is_idempotent():
    metrics = make_metrics(port=0)
    try:
        metrics.record_sink("bronze", 9)
        with urlopen("http://127.0.0.1:%s/metrics" % metrics.server.server_port, timeout=3) as response:
            assert b'clickflow_stream_sink_rows_total{destination="bronze"} 9.0' in response.read()
        thread = metrics.thread
    finally:
        metrics.close()
    metrics.close()
    assert not thread.is_alive()


def test_common_metrics_import_does_not_require_pyspark(monkeypatch):
    real_import = builtins.__import__

    def without_spark(name, *args, **kwargs):
        if name == "pyspark" or name.startswith("pyspark."):
            raise ImportError("PySpark is deliberately unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_spark)
    module = importlib.reload(metrics_module)
    assert hasattr(module, "StreamMetrics"), "StreamMetrics is not implemented"
    assert b"clickflow_stream_running" in generate_latest(module.StreamMetrics().registry)


def test_listener_uses_spark_35_callbacks_and_contains_bad_event_data(caplog):
    pytest.importorskip("pyspark")
    assert importlib.util.find_spec("src.monitoring.stream_listener"), "Listener is not implemented"
    from pyspark.sql.streaming.listener import (
        QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent,
        StreamingQueryListener,
    )
    from src.monitoring.stream_listener import attach_listener

    metrics = make_metrics(query_name="target")
    attached = []
    spark = SimpleNamespace(streams=SimpleNamespace(addListener=attached.append))
    listener = attach_listener(spark, metrics)
    assert isinstance(listener, StreamingQueryListener)
    assert attached == [listener]
    query_id = UUID("00000000-0000-0000-0000-000000000001")
    run_id = UUID("00000000-0000-0000-0000-000000000002")
    listener.onQueryStarted(QueryStartedEvent(query_id, run_id, "target", "2020-01-01T00:00:00Z"))
    assert sample(metrics, "running") == 1
    listener.onQueryProgress(QueryProgressEvent.fromJson({"progress": {
        "id": str(query_id), "runId": str(run_id), "name": "target",
        "timestamp": "2020-01-01T00:00:00.000Z", "batchId": 0,
        "batchDuration": 100, "numInputRows": 3,
        "inputRowsPerSecond": 30.0, "processedRowsPerSecond": 30.0,
        "durationMs": {"triggerExecution": 100}, "eventTime": {},
        "stateOperators": [], "sources": [], "observedMetrics": {},
        "sink": {"description": "ForeachBatchSink", "numOutputRows": -1, "metrics": {}},
    }}))
    listener.onQueryIdle(QueryIdleEvent(query_id, run_id, "2020-01-01T00:00:01Z"))
    listener.onQueryProgress(QueryProgressEvent(SimpleNamespace(json="malformed")))
    listener.onQueryStarted(None)
    listener.onQueryTerminated(None)
    assert sample(metrics, "input_rows") == 3
    assert sample(metrics, "batches_total") == 1
    assert caplog.records
    listener.onQueryTerminated(QueryTerminatedEvent(query_id, run_id, "failed", None))
    assert sample(metrics, "running") == 0
    assert sample(metrics, "failures_total") == 1


def test_listener_ignores_other_queries_and_success_does_not_count_failure():
    pytest.importorskip("pyspark")
    assert importlib.util.find_spec("src.monitoring.stream_listener"), "Listener is not implemented"
    from pyspark.sql.streaming.listener import QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent
    from src.monitoring.stream_listener import attach_listener

    metrics = make_metrics(query_name="target")
    listener = attach_listener(SimpleNamespace(streams=SimpleNamespace(addListener=lambda _: None)), metrics)
    query_id = UUID(int=1)
    run_id = UUID(int=2)
    listener.onQueryStarted(QueryStartedEvent(query_id, run_id, "other", "2020-01-01T00:00:00Z"))
    listener.onQueryProgress(QueryProgressEvent(SimpleNamespace(json=json.dumps({
        "name": "other", "batchId": 0, "numInputRows": 10,
    }))))
    listener.onQueryTerminated(QueryTerminatedEvent(query_id, run_id, "failure", None))
    assert sample(metrics, "running") == 0
    assert sample(metrics, "batches_total") == 0
    assert sample(metrics, "failures_total") == 0
    listener.onQueryStarted(QueryStartedEvent(query_id, run_id, "target", "2020-01-01T00:00:00Z"))
    listener.onQueryTerminated(QueryTerminatedEvent(query_id, run_id, None, None))
    assert sample(metrics, "running") == 0
    assert sample(metrics, "failures_total") == 0
