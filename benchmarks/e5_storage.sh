#!/usr/bin/env bash
# E5: isolated real100k/2day comparison; preserve all data and Delta history.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
export MSYS_NO_PATHCONV=1
master="$(docker compose ps -q spark-master | tr -d '\r')"
[[ -n "$master" ]] || { echo 'Start storage with make up-storage first.' >&2; exit 1; }
run_id="e5-$(docker compose exec -T spark-master python3 -c 'import uuid; print(uuid.uuid4().hex)' | tr -d '\r')"
if command -v cygpath >/dev/null 2>&1; then repo="$(cygpath -m "$PWD")"; else repo="$(pwd -P)"; fi
result_dir="benchmarks/results/$run_id"
mkdir -p "$result_dir"
set +e
docker run -i --name "clickflow-$run_id" --network "container:$master" --user root \
  --env-file .env \
  -e PYTHONPATH=/tmp/clickflow-e5-deps:/opt/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  -e PYSPARK_PYTHON=python3 -e MPLCONFIGDIR=/tmp/clickflow-e5-mpl \
  -v "$repo/src:/opt/app/src:ro" -v "$repo/configs:/opt/app/configs:ro" \
  -v "$repo/contracts:/opt/app/contracts:ro" \
  -v "$repo/data/raw/2019-Nov.csv:/opt/app/data/raw/2019-Nov.csv:ro" \
  -v "$repo/$result_dir:/opt/app/$result_dir" \
  -v "$repo/configs/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro" \
  --entrypoint /bin/bash ecom/spark:3.5.3 -c '
    set -euo pipefail
    export AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
    python3 -m pip install --disable-pip-version-check --target /tmp/clickflow-e5-deps -r /dev/stdin
    exec /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/src/monitoring/e5_storage.py "$@"
  ' -- --directory "/opt/app/$result_dir" "$@" < benchmarks/requirements.txt 2>&1 | tee "$result_dir/driver.log"
status=${PIPESTATUS[0]}
set -e
printf 'E5 artifacts: %s\n' "$result_dir"
exit "$status"
