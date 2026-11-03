"""Chạy các lệnh dự án không cần GNU Make: `python run.py <lệnh>` (Windows PowerShell/cmd đều được).

Makefile gọi vào đây nên hai cách dùng cho cùng kết quả. DAY, SPEED lấy từ biến môi trường.
"""
import glob
import os
import subprocess
import sys

COMPOSE = ["docker", "compose"]
# -T: không cấp TTY, chạy được cả trong CI/pipe
SPARK_SUBMIT = COMPOSE + ["exec", "-T", "spark-master", "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077"]
DAY = os.environ.get("DAY", "2019-11-01")
SPEED = os.environ.get("SPEED", "100")

# shortcut: alias `local` tạo lại mỗi lần vì container minio không giữ config mc qua recreate
UPLOAD_SH = ("mc alias set -q local http://127.0.0.1:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD"
             " && mc cp /upload/*.csv local/$LAKE_BUCKET/raw/csv/")

TARGETS = {
    "up":       [COMPOSE + ["--profile", "monitoring", "up", "-d", "--build"]],
    "up-core":  [COMPOSE + ["up", "-d", "--build"]],
    "up-storage": [COMPOSE + ["up", "-d", "--build", "--wait",
                              "minio", "minio-init", "spark-master", "spark-worker-1", "spark-worker-2"]],
    "down":     [COMPOSE + ["--profile", "monitoring", "down"]],
    "init-lake":          [SPARK_SUBMIT + ["/opt/app/src/storage/init_lake.py"]],
    "smoke-storage":      [SPARK_SUBMIT + ["/opt/app/src/storage/smoke_lake.py", "--sample"]],
    "smoke-storage-real": [SPARK_SUBMIT + ["/opt/app/src/storage/smoke_lake.py"]],
    "test-storage":       [SPARK_SUBMIT + ["/opt/app/tests/integration/storage_checks.py"]],
    "topics":   [[sys.executable, "-m", "src.ingestion.create_topics"]],
    "download": [[sys.executable, "-m", "src.sources.download"]],
    "upload":   [COMPOSE + ["exec", "-T", "minio", "sh", "-c", UPLOAD_SH]],
    "sample":   [SPARK_SUBMIT + ["/opt/app/src/sources/make_sample.py"]],
    "replay":   [[sys.executable, "-m", "src.sources.simulator", "--day", DAY, "--speed", SPEED]],
    "stream":   [SPARK_SUBMIT + ["/opt/app/src/storage/stream_to_bronze.py"]],
    "stream-sample":      [SPARK_SUBMIT + ["/opt/app/src/storage/stream_to_bronze.py", "--source", "sample"]],
    "test-sample-bronze": [SPARK_SUBMIT + ["/opt/app/tests/integration/sample_bronze_checks.py"]],
    "sink":     [["curl", "-s", "-X", "POST", "-H", "Content-Type: application/json",
                  "--data", "@configs/connect/s3-sink.json", "http://localhost:8083/connectors"]],
    "verify":   [SPARK_SUBMIT + ["/opt/app/src/monitoring/reconcile.py"]],
    "bench":    [["bash", f] for f in sorted(glob.glob("benchmarks/e*.sh"))],
    "test":     [[sys.executable, "-m", "pytest", "-q"]],
}

if len(sys.argv) != 2 or sys.argv[1] not in TARGETS:
    sys.exit("usage: python run.py <" + "|".join(TARGETS) + ">")

for cmd in TARGETS[sys.argv[1]]:
    print("+", " ".join(cmd), flush=True)
    rc = subprocess.call(cmd)
    if rc:
        sys.exit(rc)
