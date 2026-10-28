# Build context = thư mục gốc repo
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ src/
COPY configs/ configs/
COPY contracts/ contracts/
EXPOSE 8000
CMD ["uvicorn", "src.ingestion.collector_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
