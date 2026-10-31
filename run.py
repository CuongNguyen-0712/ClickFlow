"""Chạy các lệnh dự án không cần GNU Make: `python run.py <lệnh>` (Windows PowerShell/cmd đều được).

Makefile gọi vào đây nên hai cách dùng cho cùng kết quả. DAY, SPEED lấy từ biến môi trường.
"""
import glob
import os
import subprocess
import sys

COMPOSE = ["docker", "compose"]
SPARK_SUBMIT = COMPOSE + ["exec", "spark-master", "/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077"]
DAY = os.environ.get("DAY", "2019-11-01")
SPEED = os.environ.get("SPEED", "100")

# shortcut: alias `local` tạo lại mỗi lần vì container minio không giữ config mc qua recreate
UPLOAD_SH = ("mc alias set -q local http://127.0.0.1:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD"
             " && mc cp /upload/*.csv local/$LAKE_BUCKET/raw/csv/")

TARGETS = {
    "up":       [COMPOSE + ["--profile", "monitoring", "up", "-d", "--build"]],
    "up-core":  [COMPOSE + ["up", "-d", "--build"]],
    "down":     [COMPOSE + ["--profile", "monitoring", "down"]],
    "topics":   [[sys.executable, "-m", "src.ingestion.create_topics"]],
    "download": [[sys.executable, "-m", "src.sources.download"]],
    "upload":   [COMPOSE + ["exec", "minio", "sh", "-c", UPLOAD_SH]],
    "sample":   [[sys.executable, "-m", "src.sources.make_sample"]],
    "replay":   [[sys.executable, "-m", "src.sources.simulator", "--day", DAY, "--speed", SPEED]],
    "stream":   [SPARK_SUBMIT + ["/opt/app/src/storage/stream_to_bronze.py"]],
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
