"""Collector API (C2): POST /events nhận lô ≤ 500 sự kiện, validate bằng Pydantic, đẩy vào Kafka qua aiokafka.

Hiện chỉ có /health để `make up` chạy được; POST /events, /metrics làm ở task Collector API (TV2, tuần 3).
"""
from fastapi import FastAPI

app = FastAPI(title="Collector API")


@app.get("/health")
def health():
    return {"status": "ok"}
