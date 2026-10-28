from functools import lru_cache
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]  # repo root trên host, /opt/app trong container Spark


@lru_cache
def load(name="pipeline.yaml"):
    return yaml.safe_load((ROOT / "configs" / name).read_text(encoding="utf-8"))
