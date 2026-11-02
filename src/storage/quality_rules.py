"""Validate C3 before Spark coercion and select the first C5 error reason.

The pure classifier emits separate normalized JSON for typed parsing. Kafka's
original payload is never replaced, including malformed JSON and tombstones.
Duplicate JSON keys are rejected rather than silently choosing one value.
"""

from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, DecimalException
import json
import math
import re

from src.common.schemas import EVENT_COLUMNS, EVENT_TYPES, ddl


_FIELDS = frozenset(name for name, _ in EVENT_COLUMNS)
_MONTHS = {name: index for index, name in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"),
    start=1,
)}
_SOURCE_FILE = re.compile(r"([0-9]{4})-([A-Z][a-z]{2})\.csv")
_RFC3339 = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])"
)
_UUID = re.compile(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

_ERROR_RULES = {
    'parse_error': 1,
    'bad_event_time': 2,
    'bad_event_type': 3,
    'missing_id': 4,
    'negative_price': 4,
}


def quality_rule_counts(error_counts):
    """Aggregate first-matching C5 reasons into four rule counters.

    The caller supplies newly committed quarantine rows by reason, not every
    rule a row could violate. No worker-side increments or mutable global counts.
    """
    if not isinstance(error_counts, Mapping):
        raise ValueError('Quality counts must be a mapping of C5 reasons to counts')
    counts = {1: 0, 2: 0, 3: 0, 4: 0}
    for reason, count in error_counts.items():
        if reason not in _ERROR_RULES:
            raise ValueError(f'Unknown C5 error reason: {reason}')
        if type(count) is not int or count < 0:
            raise ValueError('Quality counts must be nonnegative integers')
        counts[_ERROR_RULES[reason]] += count
    return counts


def _object_without_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_non_json_number(value):
    raise ValueError("non-JSON numeric constant")


def _timestamp(value):
    """Parse RFC3339 to a UTC datetime representable by Spark/Python.

    Leap seconds cannot be represented by Spark timestamps and are invalid here.
    Fractional seconds use Spark's microsecond precision.
    """
    if not isinstance(value, str) or _RFC3339.fullmatch(value) is None:
        return None
    try:
        canonical = value[:10] + "T" + value[11:]
        if canonical[-1] in "Zz":
            canonical = canonical[:-1] + "+00:00"
        return datetime.fromisoformat(canonical).astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def _utc_string(value):
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def classify_payload(payload: str | None) -> tuple[str | None, str | None]:
    """Return ``(normalized_json, error_reason)`` using C5 rule precedence.

    Structural/type errors return no normalized JSON. Domain failures retain
    safely typed values, with invalid/missing event_time represented as null.
    Optional C3 fields are always explicit, including when absent in the input.
    """
    if not isinstance(payload, str):
        return None, "parse_error"
    try:
        event = json.loads(
            payload,
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_non_json_number,
            parse_float=Decimal,
        )
    except (ValueError, RecursionError, DecimalException):
        return None, "parse_error"
    if not isinstance(event, dict) or event.keys() - _FIELDS:
        return None, "parse_error"

    # Rule 1 checks every type first; rules 2–4 own only their stated exceptions.
    for name in ("event_id", "source_file", "produced_at"):
        if not isinstance(event.get(name), str):
            return None, "parse_error"
    for name in ("event_time", "event_type", "category_code", "brand", "user_session"):
        if event.get(name) is not None and not isinstance(event[name], str):
            return None, "parse_error"
    for name in ("product_id", "category_id", "user_id"):
        value = event.get(name)
        if value is None and name != "category_id":
            continue
        # bool is an int subclass; JSON floating-point IDs must not be coerced.
        if type(value) is not int or not _INT64_MIN <= value <= _INT64_MAX:
            return None, "parse_error"
    price = event.get("price")
    if type(price) not in (int, Decimal):
        return None, "parse_error"
    negative_price = price < 0
    try:
        price = float(price)
    except (ValueError, OverflowError):
        return None, "parse_error"
    if not math.isfinite(price):
        return None, "parse_error"
    session = event.get("user_session")
    if session is not None and _UUID.fullmatch(session) is None:
        return None, "parse_error"
    source = _SOURCE_FILE.fullmatch(event["source_file"])
    if source is None or source[2] not in _MONTHS or int(source[1]) == 0:
        return None, "parse_error"
    produced_at = _timestamp(event["produced_at"])
    if produced_at is None:
        return None, "parse_error"

    event_time = _timestamp(event.get("event_time"))
    if event_time is None or (event_time.year, event_time.month) != (int(source[1]), _MONTHS[source[2]]):
        reason = "bad_event_time"
    elif event.get("event_type") not in EVENT_TYPES:
        reason = "bad_event_type"
    elif event.get("product_id") is None or event.get("user_id") is None:
        reason = "missing_id"
    elif negative_price:
        reason = "negative_price"
    else:
        reason = None

    normalized = {name: event.get(name) for name, _ in EVENT_COLUMNS}
    normalized["event_time"] = _utc_string(event_time) if event_time is not None else None
    normalized["produced_at"] = _utc_string(produced_at)
    normalized["price"] = price
    return json.dumps(normalized, ensure_ascii=True, allow_nan=False), reason


def classify_records(frame):
    """Add typed C3 fields and C5 reason to Kafka records, retaining raw payload.

    Input columns are payload, kafka_partition, kafka_offset, and ingested_at.
    The Spark session must use UTC so date partitions use the contract timezone.
    Spark imports are deliberately lazy; classify_payload is stdlib-only.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    result_type = StructType([
        StructField("normalized_json", StringType(), True),
        StructField("error_reason", StringType(), True),
    ])
    validate = F.udf(classify_payload, result_type)
    classified = frame.select("payload", "kafka_partition", "kafka_offset", "ingested_at").withColumn(
        "_quality", validate(F.col("payload"))
    )
    parsed = classified.withColumn(
        "_event", F.from_json(
            F.col("_quality.normalized_json"), ddl(EVENT_COLUMNS),
            {"timeZone": "UTC", "mode": "FAILFAST"},
        )
    )
    return parsed.select(
        "payload", "kafka_partition", "kafka_offset", "ingested_at", "_event.*",
        F.col("_quality.error_reason").alias("error_reason"),
    ).withColumn("event_date", F.to_date("event_time")).withColumn(
        "ingest_date", F.to_date("ingested_at")
    )
