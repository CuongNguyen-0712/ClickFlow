"""Tải các file trong pipeline.yaml → simulator.files từ Kaggle về data/raw/ bằng Kaggle CLI 2.x.

Dataset public nên CLI 2.x tải được không cần token (đã thử 31/10/2026); token ~/.kaggle/kaggle.json
hoặc KAGGLE_USERNAME / KAGGLE_KEY chỉ cần khi Kaggle đòi đăng nhập. Kaggle API mới chỉ nhận CLI >= 1.8;
CLI 2.x nhận dataset là tham số vị trí, không còn cờ -d.
Idempotent: file đã có thì bỏ qua. Sau đó `make upload` đẩy lên lake/raw/csv/.
"""
import subprocess
import sys
from pathlib import Path

from src.common.config import ROOT, load

cfg = load()
dataset = cfg["simulator"]["kaggle_dataset"]
out = ROOT / "data" / "raw"
out.mkdir(parents=True, exist_ok=True)

for name in cfg["simulator"]["files"]:
    target = out / name
    if target.exists():
        print(f"skip {name}: {target.stat().st_size / 1e9:.2f} GB da co")
        continue
    print(f"download {name} tu {dataset} ...")
    # Kaggle CLI tự giải nén khi có --unzip; file zip trung gian ~2 GB bị xoá sau khi giải nén
    r = subprocess.run(
        ["kaggle", "datasets", "download", dataset, "-f", name, "-p", str(out), "--unzip"],
        check=False,
    )
    if r.returncode != 0 or not target.exists():
        sys.exit(f"download {name} that bai (exit {r.returncode}); neu Kaggle doi dang nhap thi them token (xem README)")
    print(f"ok {name}: {target.stat().st_size / 1e9:.2f} GB")
