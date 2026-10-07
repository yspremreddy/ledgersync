FROM python:3.13.2-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY ingestion/requirements.txt /app/ingestion/requirements.txt
RUN pip install --no-cache-dir --requirement /app/ingestion/requirements.txt
COPY ingestion /app/ingestion
COPY tests /app/tests

USER 10001:10001
CMD ["python", "-m", "ingestion.service"]
