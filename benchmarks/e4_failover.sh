#!/usr/bin/env bash
# E4b: actual Spark driver SIGKILL + unchanged checkpoint recovery; retain artifacts.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
export MSYS_NO_PATHCONV=1
master="$(docker compose ps -q spark-master | tr -d '\r')"
[[ -n "$master" ]] || { echo 'Start the stack with make up-stream first.' >&2; exit 1; }
run_id="e4b-$(docker compose exec -T spark-master python3 -c 'import uuid; print(uuid.uuid4().hex)' | tr -d '\r')"
if command -v cygpath >/dev/null 2>&1; then
  repo="$(cygpath -m "$PWD")"
else
  repo="$(pwd -P)"
fi
result_dir="benchmarks/results/$run_id"
mkdir -p "$result_dir"
# Separate process namespace; shared master network allows existing executor routing.
# No --rm: retain the controller container, all topics and all MinIO/checkpoint objects.
set +e
cat benchmarks/requirements.txt benchmarks/e4b-requirements.txt | docker run -i \
  --name "clickflow-$run_id" --network "container:$master" --user root \
  --env-file .env \
  -e PYTHONPATH=/tmp/clickflow-e4b-deps:/opt/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  -e PYSPARK_PYTHON=python3 -e MPLCONFIGDIR=/tmp/clickflow-e4b-mpl \
  -v "$repo/src:/opt/app/src:ro" -v "$repo/configs:/opt/app/configs:ro" \
  -v "$repo/contracts:/opt/app/contracts:ro" -v "$repo/queries:/opt/app/queries:ro" \
  -v "$repo/data/sample:/opt/app/data/sample:ro" \
  -v "$repo/benchmarks/results:/opt/app/benchmarks/results" \
  -v "$repo/configs/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro" \
  --entrypoint /bin/bash ecom/spark:3.5.3 -c '
    set -euo pipefail
    export AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
    python3 -m pip install --disable-pip-version-check --target /tmp/clickflow-e4b-deps -r /dev/stdin
    exec python3 -u -m src.monitoring.terminal_recording "$@"
  ' -- --directory "/opt/app/$result_dir" "$@" 2>&1 | tee "$result_dir/bootstrap.log"
status=${PIPESTATUS[1]}
set -e
printf 'E4b artifacts: %s\n' "$result_dir"
exit "$status"
