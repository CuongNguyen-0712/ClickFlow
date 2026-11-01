"""C3 validation and C5 first-matching-rule behavior, without Spark."""

import importlib
import json
import subprocess
import sys

import pytest

from src.common.schemas import EVENT_COLUMNS


def valid_event(**overrides):
    event = {
        "event_id": "stable-csv-row-hash",
        "event_time": "2019-11-01T00:00:00Z",
        "event_type": "view",
        "product_id": 100,
        "category_id": 2053013555631882655,
        "price": 12.5,
        "user_id": 200,
        "source_file": "2019-Nov.csv",
        "produced_at": "2026-11-01T01:02:03+07:00",
    }
    event.update(overrides)
    return event


def classify(payload):
    # Resolve the API inside the test so the initial missing feature is a failure.
    module = importlib.import_module("src.storage.quality_rules")
    assert hasattr(module, "classify_payload"), "C3 classifier is not implemented"
    return module.classify_payload(payload)


def assert_reason(event, reason):
    normalized, actual = classify(json.dumps(event))
    assert actual == reason
    if reason == "parse_error":
        assert normalized is None


def test_valid_event_preserves_19_digit_category_and_normalizes_optional_fields():
    normalized, reason = classify(json.dumps(valid_event()))
    assert reason is None
    parsed = json.loads(normalized)
    assert parsed["category_id"] == 2053013555631882655
    assert type(parsed["category_id"]) is int
    assert parsed["category_code"] is None
    assert parsed["brand"] is None
    assert parsed["user_session"] is None
    assert parsed["produced_at"] == "2026-10-31T18:02:03.000000Z"
    assert set(parsed) == {name for name, _ in EVENT_COLUMNS}


@pytest.mark.parametrize("payload", [None, "", "{", "null", "[]", "true", "1", '"string"'])
def test_malformed_json_tombstone_and_nonobject_roots_are_parse_errors(payload):
    assert classify(payload) == (None, "parse_error")


def test_unknown_properties_are_parse_errors_even_with_invalid_domain_values():
    assert_reason(valid_event(extra="unexpected", event_time="bad"), "parse_error")


def test_duplicate_json_keys_are_parse_errors():
    payload = json.dumps(valid_event())[:-1] + ', "category_id": 1}'
    assert classify(payload) == (None, "parse_error")


@pytest.mark.parametrize("field", ["event_id", "category_id", "price", "source_file", "produced_at"])
@pytest.mark.parametrize("missing", [True, False], ids=["missing", "null"])
def test_required_nondomain_fields_are_parse_errors(field, missing):
    event = valid_event()
    if missing:
        event.pop(field)
    else:
        event[field] = None
    assert_reason(event, "parse_error")


@pytest.mark.parametrize("field", ["product_id", "category_id", "user_id"])
@pytest.mark.parametrize("value", [True, False, 1.0, "1", [], {}, 2**63, -(2**63) - 1])
def test_integer_fields_reject_coercion_and_int64_overflow(field, value):
    assert_reason(valid_event(**{field: value}), "parse_error")


@pytest.mark.parametrize("field", ["product_id", "category_id", "user_id"])
@pytest.mark.parametrize("value", [-(2**63), 0, 2**63 - 1])
def test_integer_fields_accept_exact_int64_boundaries(field, value):
    normalized, reason = classify(json.dumps(valid_event(**{field: value})))
    assert reason is None
    assert json.loads(normalized)[field] == value


@pytest.mark.parametrize("field", ["event_id", "source_file", "produced_at", "event_time", "event_type", "category_code", "brand", "user_session"])
@pytest.mark.parametrize("value", [42, True, [], {}])
def test_string_fields_reject_other_json_types(field, value):
    assert_reason(valid_event(**{field: value}), "parse_error")


@pytest.mark.parametrize("value", [True, "12.5", [], {}, float("nan"), float("inf"), -float("inf"), 10**400])
def test_price_rejects_non_numeric_nonfinite_and_double_overflow(value):
    assert_reason(valid_event(price=value), "parse_error")


@pytest.mark.parametrize("value", [0, 0.0, 1, 1.5, 1e308])
def test_finite_nonnegative_prices_are_valid(value):
    normalized, reason = classify(json.dumps(valid_event(price=value)))
    assert reason is None
    assert json.loads(normalized)["price"] == value


@pytest.mark.parametrize("token,reason", [
    ("-1e-400", "negative_price"), ("1e-400", None), ("-0.0", None),
    ("1e400", "parse_error"),
])
def test_price_sign_is_checked_before_double_underflow(token, reason):
    payload = json.dumps(valid_event()).replace('"price": 12.5', '"price": ' + token)
    assert classify(payload)[1] == reason


@pytest.mark.parametrize("value", [None, "", "unparseable", "2019-11-01", "2019-11-01T00:00:00", "2019-11-31T00:00:00Z", "2019-10-31T23:59:59.999999Z", "2019-12-01T00:00:00Z"])
def test_event_time_invalid_or_outside_source_month_is_bad_event_time(value):
    assert_reason(valid_event(event_time=value), "bad_event_time")


def test_missing_event_time_is_bad_event_time():
    event = valid_event()
    event.pop("event_time")
    assert_reason(event, "bad_event_time")


@pytest.mark.parametrize("timestamp,source_file,expected", [
    ("2019-11-01T07:00:00+07:00", "2019-Nov.csv", "2019-11-01T00:00:00.000000Z"),
    ("2019-10-31T23:00:00-01:00", "2019-Nov.csv", "2019-11-01T00:00:00.000000Z"),
    ("2019-11-30T23:59:59.999999Z", "2019-Nov.csv", "2019-11-30T23:59:59.999999Z"),
    ("2020-02-29T23:59:59Z", "2020-Feb.csv", "2020-02-29T23:59:59.000000Z"),
    ("2019-12-31T23:59:59Z", "2019-Dec.csv", "2019-12-31T23:59:59.000000Z"),
    ("2019-11-01t00:00:00z", "2019-Nov.csv", "2019-11-01T00:00:00.000000Z"),
])
def test_utc_month_boundaries_and_leap_month(timestamp, source_file, expected):
    normalized, reason = classify(json.dumps(valid_event(event_time=timestamp, source_file=source_file)))
    assert reason is None
    assert json.loads(normalized)["event_time"] == expected


@pytest.mark.parametrize("timestamp", ["2020-03-01T00:00:00Z", "2020-02-01T00:00:00+00:01", "2019-02-29T00:00:00Z"])
def test_leap_month_boundaries_reject_outside_instants(timestamp):
    assert_reason(valid_event(event_time=timestamp, source_file="2020-Feb.csv"), "bad_event_time")


@pytest.mark.parametrize("source", ["2019-Unknown.csv", "2019-11.csv", "2019-nov.csv", "2019-Nov.csv.gz", "0000-Nov.csv", "raw/2019-Nov.csv"])
def test_source_filename_requires_supported_year_and_english_month(source):
    assert_reason(valid_event(source_file=source), "parse_error")


@pytest.mark.parametrize("value", ["bad", "2026-11-01", "2026-11-01T00:00:00", "2026-02-30T00:00:00Z", "2026-11-01 00:00:00Z", "2026-11-01T00:00:00+0700"])
def test_produced_at_requires_rfc3339_with_timezone(value):
    assert_reason(valid_event(produced_at=value), "parse_error")


@pytest.mark.parametrize("value", [None, "", "click", "VIEW"])
def test_invalid_event_type_is_bad_event_type(value):
    assert_reason(valid_event(event_type=value), "bad_event_type")


def test_missing_event_type_is_bad_event_type():
    event = valid_event()
    event.pop("event_type")
    assert_reason(event, "bad_event_type")


@pytest.mark.parametrize("event_type", ["view", "cart", "purchase"])
def test_allowed_event_types(event_type):
    assert classify(json.dumps(valid_event(event_type=event_type)))[1] is None


@pytest.mark.parametrize("field", ["product_id", "user_id"])
@pytest.mark.parametrize("missing", [True, False], ids=["missing", "null"])
def test_missing_or_null_identity_is_missing_id(field, missing):
    event = valid_event()
    if missing:
        event.pop(field)
    else:
        event[field] = None
    assert_reason(event, "missing_id")


@pytest.mark.parametrize("changes,expected", [
    ({"price": -1}, "negative_price"),
    ({"event_time": "bad", "event_type": "bad", "product_id": None, "price": -1}, "bad_event_time"),
    ({"event_type": "bad", "product_id": None, "price": -1}, "bad_event_type"),
    ({"product_id": None, "price": -1}, "missing_id"),
    ({"event_time": "bad", "price": "-1"}, "parse_error"),
    ({"event_time": "bad", "event_type": 1}, "parse_error"),
    ({"event_type": "bad", "user_id": True}, "parse_error"),
    ({"product_id": None, "produced_at": "bad"}, "parse_error"),
])
def test_first_matching_rule_precedence(changes, expected):
    assert_reason(valid_event(**changes), expected)


@pytest.mark.parametrize("session", [None, "123e4567-e89b-12d3-a456-426614174000", "123E4567-E89B-12D3-A456-426614174000"])
def test_optional_strings_and_uuid_are_preserved(session):
    normalized, reason = classify(json.dumps(valid_event(category_code="", brand="Apple", user_session=session)))
    assert reason is None
    assert json.loads(normalized)["user_session"] == session
    assert json.loads(normalized)["category_code"] == ""
    assert json.loads(normalized)["brand"] == "Apple"


@pytest.mark.parametrize("session", ["", "bad", "123e4567e89b12d3a456426614174000", "{123e4567-e89b-12d3-a456-426614174000}"])
def test_user_session_requires_canonical_uuid_string(session):
    assert_reason(valid_event(user_session=session), "parse_error")


def test_import_does_not_import_pyspark():
    result = subprocess.run(
        [sys.executable, "-c", "import sys; import src.storage.quality_rules; assert 'pyspark' not in sys.modules"],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
