"""Tải các file trong pipeline.yaml → simulator.files từ Kaggle về data/raw/ bằng Kaggle CLI 2.x.

Dataset public nên CLI 2.x tải được không cần token (đã thử 31/10/2026); token ~/.kaggle/kaggle.json
hoặc KAGGLE_USERNAME / KAGGLE_KEY chỉ cần khi Kaggle đòi đăng nhập. Kaggle API mới chỉ nhận CLI >= 1.8;
CLI 2.x nhận dataset là tham số vị trí, không còn cờ -d.
Idempotent: CSV đã có thì bỏ qua; chỉ có <file>.zip (tải tay chưa giải nén) thì giải nén, không tải lại.
"""
import subprocess
import sys
import zipfile
from pathlib import Path

from src.common.config import ROOT, load

cfg = load()
dataset = cfg["simulator"]["kaggle_dataset"]
out = ROOT / "data" / "raw"
out.mkdir(parents=True, exist_ok=True)


def gb(p: Path) -> str:
    return f"{p.stat().st_size / 1e9:.2f} GB"


for name in cfg["simulator"]["files"]:
    target = out / name
    if target.exists():
        print(f"skip {name}: {gb(target)} da co")
        continue
    zip_path = out / f"{name}.zip"
    if zip_path.exists():
        print(f"unzip {zip_path.name} ({gb(zip_path)}) ...", flush=True)
        with zipfile.ZipFile(zip_path) as z:
            z.extract(name, out)
        zip_path.unlink()
    else:
        print(f"download {name} tu {dataset} ...", flush=True)
        # --unzip: CLI tự giải nén và xoá zip trung gian
        r = subprocess.run(
            ["kaggle", "datasets", "download", dataset, "-f", name, "-p", str(out), "--unzip"],
            check=False,
        )
        if r.returncode != 0:
            sys.exit(f"download {name} that bai (exit {r.returncode}); neu Kaggle doi dang nhap thi them token (xem README)")
    if not target.exists():
        sys.exit(f"{name} khong thay trong {out} sau khi tai/giai nen")
    print(f"ok {name}: {gb(target)}")
