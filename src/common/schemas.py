"""Schema dùng chung. Nguồn sự thật là contracts/; tests/test_schemas.py giữ hai bên khớp nhau.

Kiểu ghi theo Spark SQL DDL nên dùng thẳng được: from_json(col("value"), ddl(EVENT_COLUMNS)).
"""

SOURCE_COLUMNS = [  # C1: 9 cột CSV gốc
    ("event_time", "timestamp"),
    ("event_type", "string"),
    ("product_id", "bigint"),
    ("category_id", "bigint"),
    ("category_code", "string"),
    ("brand", "string"),
    ("price", "double"),
    ("user_id", "bigint"),
    ("user_session", "string"),
]

EVENT_COLUMNS = SOURCE_COLUMNS + [  # C3: message Kafka, 12 trường
    ("event_id", "string"),
    ("source_file", "string"),
    ("produced_at", "timestamp"),
]

BRONZE_COLUMNS = SOURCE_COLUMNS + [  # C4: 14 cột (chưa tính cột phân vùng event_date)
    ("event_id", "string"),
    ("kafka_partition", "int"),
    ("kafka_offset", "bigint"),
    ("produced_at", "timestamp"),
    ("ingested_at", "timestamp"),
]

EVENT_TYPES = ("view", "cart", "purchase")


def ddl(columns):
    return ", ".join(f"{name} {type_}" for name, type_ in columns)
