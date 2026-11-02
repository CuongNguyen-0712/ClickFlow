#!/usr/bin/env bash
# Full E3:18 measured trials; retain data/checkpoints/topics and copy result artifacts.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
export MSYS_NO_PATHCONV=1
run_id="e3-$(docker compose exec -T spark-master python3 -c 'import uuid; print(uuid.uuid4().hex)' | tr -d '\r')"
result_dir="benchmarks/results/$run_id"
stage_dir="/opt/app/data/raw/$run_id"
mkdir -p "$result_dir"
docker compose exec -T --user root spark-master python3 -m pip install \
  --disable-pip-version-check --target /tmp/clickflow-e3-deps -r /dev/stdin < benchmarks/requirements.txt
set +e
docker compose exec -T \
  -e PYTHONPATH=/tmp/clickflow-e3-deps:/opt/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  -e MPLCONFIGDIR=/tmp/clickflow-e3-mpl \
  spark-master /opt/spark/bin/spark-submit /opt/app/src/monitoring/e3_benchmark.py \
  --output "$stage_dir" "$@" 2>&1 | tee "$result_dir/driver.log"
status=${PIPESTATUS[0]}
set -e
# Copy even on failure, so attempted runs and diagnostics remain reviewable.
docker compose cp "spark-master:$stage_dir/." "$result_dir/"
printf 'E3 artifacts: %s\n' "$result_dir"
exit "$status"
