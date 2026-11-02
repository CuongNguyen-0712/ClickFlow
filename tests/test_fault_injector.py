"""Duplicate injection must preserve events and predict deduplication counts."""

import builtins
from collections import Counter, UserDict
from copy import deepcopy
import importlib
import random

import pytest

from src.sources import fault_injector


def inject(events, **kwargs):
    assert hasattr(fault_injector, "inject_duplicates"), "Duplicate injector is not implemented"
    return fault_injector.inject_duplicates(events, **kwargs)


@pytest.fixture
def events():
    return [
        {"event_id": "a", "event_time": "2020-01-01T00:00:00Z", "price": 1.25,
         "details": {"items": ["first"]}},
        {"event_id": "b", "event_time": "2020-01-01T00:00:01Z", "price": 0,
         "details": {"items": ["second"]}},
        {"event_id": "c", "event_time": "2020-01-01T00:00:02Z", "price": None,
         "details": {"items": []}},
    ]


@pytest.mark.parametrize("ratio,extra_count", [(0, 0), (0.5, 1), (1.0, 3), (2.5, 7)])
def test_preserves_originals_and_emits_floor_ratio_exact_copies(events, ratio, extra_count):
    records, manifest = inject(events, duplicate_ratio=ratio, seed=17)
    assert len(records) == 3 + extra_count
    assert set(record["event_id"] for record in records) == {"a", "b", "c"}
    by_id = {event["event_id"]: event for event in events}
    assert all(record == by_id[record["event_id"]] for record in records)
    assert sum(count - 1 for count in Counter(r["event_id"] for r in records).values()) == extra_count
    assert manifest == {
        "original_records": 3,
        "input_duplicate_records": 0,
        "injected_duplicates": extra_count,
        "emitted_records": 3 + extra_count,
        "unique_event_ids": 3,
        "expected_duplicates_dropped": extra_count,
        "seed": 17,
        "duplicate_ratio": ratio,
    }


def test_seed_reproduces_selection_and_shuffled_order(events):
    first = inject(events, duplicate_ratio=5, seed=42)
    assert first == inject(events, duplicate_ratio=5, seed=42)
    assert first[0] != inject(events, duplicate_ratio=5, seed=99)[0]


def test_copies_every_record_without_mutating_or_aliasing_nested_inputs(events):
    before = deepcopy(events)
    records, _ = inject(events, duplicate_ratio=2)
    assert events == before
    repeated_id = Counter(record["event_id"] for record in records).most_common(1)[0][0]
    same_id = [record for record in records if record["event_id"] == repeated_id]
    assert len(same_id) >= 2
    original_items = deepcopy(same_id[1]["details"]["items"])
    same_id[0]["details"]["items"].append("changed")
    assert same_id[1]["details"]["items"] == original_items
    assert events == before


def test_manifest_counts_duplicates_already_present_in_input():
    repeated = {"event_id": "a", "event_time": "2020-01-01T00:00:00Z"}
    records, manifest = inject([repeated, repeated, {"event_id": "b"}], duplicate_ratio=1)
    assert len(records) == 6
    assert manifest["original_records"] == 3
    assert manifest["input_duplicate_records"] == 1
    assert manifest["injected_duplicates"] == 3
    assert manifest["unique_event_ids"] == 2
    assert manifest["expected_duplicates_dropped"] == 4


def test_empty_input_returns_empty_records_and_zero_counts():
    records, manifest = inject([], duplicate_ratio=2)
    assert records == []
    assert manifest == {
        "original_records": 0, "input_duplicate_records": 0,
        "injected_duplicates": 0, "emitted_records": 0, "unique_event_ids": 0,
        "expected_duplicates_dropped": 0, "seed": 42, "duplicate_ratio": 2,
    }


@pytest.mark.parametrize("ratio", [-0.01, True, False, float("nan"), float("inf"),
                                 float("-inf"), "1", None])
def test_rejects_invalid_ratio_even_for_empty_input(ratio):
    with pytest.raises((TypeError, ValueError), match="duplicate_ratio"):
        inject([], duplicate_ratio=ratio)


@pytest.mark.parametrize("event", [None, [], "event", {}, {"event_id": None},
                                  {"event_id": ""}, {"event_id": "  "},
                                  {"event_id": 123}, {"event_id": True}])
def test_rejects_non_mapping_events_or_missing_nonblank_string_id(event):
    with pytest.raises((TypeError, ValueError), match="event"):
        inject([event])


def test_accepts_mapping_events_and_preserves_id_whitespace():
    records, _ = inject([UserDict({"event_id": " a ", "value": [None, True]})])
    assert records == [{"event_id": " a ", "value": [None, True]}] * 2
    assert all(type(record) is dict for record in records)


def test_does_not_change_global_random_state(events):
    before = random.getstate()
    inject(events, seed=10)
    assert random.getstate() == before


def test_import_and_execution_do_not_require_spark(monkeypatch):
    real_import = builtins.__import__

    def without_spark(name, *args, **kwargs):
        if name == "pyspark" or name.startswith("pyspark."):
            raise ImportError("PySpark is deliberately unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_spark)
    importlib.reload(fault_injector)
    assert inject([{"event_id": "a"}])[1]["expected_duplicates_dropped"] == 1
