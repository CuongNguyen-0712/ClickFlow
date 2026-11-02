"""E4b-only driver: pause after the real sink callback, before batch checkpoint commit."""
import argparse
import json
import os
from pathlib import Path
import signal
from threading import Event

import yaml

from src.common.spark_session import get_spark
from src.monitoring.e4b_failover import save_json


def main():
    from delta.tables import DeltaTable
    from src.storage import stream_sink
    from src.storage.stream_to_bronze import run_pipeline

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    directory = args.config.parent
    config = yaml.safe_load(args.config.read_text())
    stop = Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    original = stream_sink.process_batch
    spark = get_spark('e4b-first-driver')
    spark.sparkContext.setLogLevel('WARN')

    def with_fault_gate(batch, batch_id, settings, metrics):
        def inserted():
            return int(metrics.registry.get_sample_value('clickflow_stream_sink_rows_total',
                                                          {'destination': 'bronze'}) or 0)
        before = inserted()
        original(batch, batch_id, settings, metrics)
        after = inserted()
        if (directory / 'arm-fault').exists() and after > before and not (directory / 'gate.json').exists():
            # Do not recompute the stateful batch after original() unpersists it.
            gate = {'batch_id': batch_id, 'python_pid': os.getpid(), 'inserted_in_batch': after-before,
                    'driver_inserted_rows': after,
                    'bronze_version': int(DeltaTable.forPath(spark, settings['bronze']).history(1).first().version)}
            save_json(directory / 'gate.json', gate)
            print(json.dumps({'event': 'e4b_delta_committed_before_checkpoint', **gate}), flush=True)
            stop.wait()  # Normal benchmark execution kills this process group with SIGKILL.

    stream_sink.process_batch = with_fault_gate
    try:
        run_pipeline(spark, config, stop_requested=stop)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
