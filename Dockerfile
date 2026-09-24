FROM python:3.13.2-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY infra/check_cdc.py infra/debezium/connector.json /app/
USER 10001:10001
CMD ["python", "--version"]
