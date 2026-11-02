"""Inject reproducible exact duplicates for deduplication acceptance checks.

Corrupt-record injection for quarantine testing remains future work. This module
changes neither event contents nor timestamps and has no Spark dependency.
"""

from collections.abc import Iterable, Mapping
from copy import deepcopy
import math
from numbers import Real
import random
from typing import Any


def inject_duplicates(
    events: Iterable[Mapping[str, Any]],
    duplicate_ratio: float = 1.0,
    seed: int = 42,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return shuffled originals plus ``floor(input_count * ratio)`` copies.

    Extras are sampled with replacement, so ratios above one are supported.
    Every returned record is independently deep-copied, including originals.
    A local random generator controls both selection and shuffling; calls with
    the same ordered input and seed reproduce the output without changing the
    process-global random state.

    Records must be mappings with nonblank string ``event_id`` values. Other
    values are preserved without schema validation. The manifest's expected
    dropped count assumes all emitted events pass quality rules and are within
    the deduplication watermark; it includes duplicates present in the input.
    """
    if isinstance(duplicate_ratio, bool) or not isinstance(duplicate_ratio, Real):
        raise TypeError("duplicate_ratio must be a finite nonnegative number")
    if not math.isfinite(duplicate_ratio) or duplicate_ratio < 0:
        raise ValueError("duplicate_ratio must be a finite nonnegative number")

    originals = []
    for index, event in enumerate(events):
        if not isinstance(event, Mapping):
            raise TypeError(f"event at index {index} must be a mapping")
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError(f"event at index {index} needs a nonblank string event_id")
        originals.append(deepcopy(dict(event)))

    original_count = len(originals)
    injected_count = math.floor(original_count * duplicate_ratio)
    rng = random.Random(seed)
    records = originals + [deepcopy(rng.choice(originals)) for _ in range(injected_count)]
    rng.shuffle(records)
    unique_count = len({record["event_id"] for record in originals})
    manifest = {
        "original_records": original_count,
        "input_duplicate_records": original_count - unique_count,
        "injected_duplicates": injected_count,
        "emitted_records": len(records),
        "unique_event_ids": unique_count,
        "expected_duplicates_dropped": len(records) - unique_count,
        "seed": seed,
        "duplicate_ratio": duplicate_ratio,
    }
    return records, manifest
