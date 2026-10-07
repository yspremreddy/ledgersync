"""Consume Debezium records and durably persist them before committing Kafka offsets."""

from dataclasses import dataclass
from datetime import datetime, timezone
import base64
import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
import psycopg

from ingestion.events import KafkaPosition, MalformedDebeziumEvent, parse_debezium_event
from ingestion.storage import PostgresStore


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        output: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        output.update(getattr(record, "event_fields", {}))
        if record.exc_info:
            output["exception"] = self.formatException(record.exc_info)
        return json.dumps(output, separators=(",", ":"), default=str)


def configure_logging(level: str) -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("ledgersync.ingestion")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level.upper())
    logger.propagate = False
    return logger


def log(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    logger.log(level, message, extra={"event_fields": fields})


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set.")
    return value


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_group_id: str
    kafka_topics: tuple[str, ...]
    postgres_options: dict[str, Any]
    log_level: str

    @classmethod
    def from_environment(cls) -> "Settings":
        topics = tuple(
            topic.strip()
            for topic in required_environment("KAFKA_TOPICS").split(",")
            if topic.strip()
        )
        if not topics:
            raise RuntimeError("KAFKA_TOPICS must contain at least one topic.")
        return cls(
            kafka_bootstrap_servers=required_environment("KAFKA_BOOTSTRAP_SERVERS"),
            kafka_group_id=required_environment("KAFKA_GROUP_ID"),
            kafka_topics=topics,
            postgres_options={
                "host": required_environment("PGHOST"),
                "port": int(os.environ.get("PGPORT", "5432")),
                "dbname": required_environment("PGDATABASE"),
                "user": required_environment("PGUSER"),
                "password": required_environment("PGPASSWORD"),
                "connect_timeout": int(os.environ.get("PGCONNECT_TIMEOUT", "10")),
                "application_name": "ledgersync-ingestion",
            },
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )


def kafka_timestamp(message: Message) -> datetime | None:
    _, timestamp_ms = message.timestamp()
    if timestamp_ms is None or timestamp_ms < 0:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def kafka_exception_is_retriable(exc: KafkaException) -> bool:
    return bool(exc.args and hasattr(exc.args[0], "retriable") and exc.args[0].retriable())


def persist_with_retry(
    store: PostgresStore,
    position: KafkaPosition,
    consumer_group: str,
    event: Any,
    stop: threading.Event,
    logger: logging.Logger,
) -> bool:
    delay = 1.0
    while not stop.is_set():
        try:
            return store.persist(position, consumer_group, event)
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            store.close()
            log(
                logger,
                logging.WARNING,
                "postgres_persist_retry",
                topic=position.topic,
                partition=position.partition,
                offset=position.offset,
                retry_seconds=delay,
                error=str(exc),
            )
            stop.wait(delay)
            delay = min(delay * 2, 30.0)
    raise RuntimeError("Shutdown requested before PostgreSQL persistence completed.")


def persist_dead_letter_with_retry(
    store: PostgresStore,
    position: KafkaPosition,
    consumer_group: str,
    message: Message,
    error: MalformedDebeziumEvent,
    stop: threading.Event,
    logger: logging.Logger,
) -> bool:
    headers = [
        {
            "name": name,
            "value_base64": None if value is None else base64.b64encode(value).decode("ascii"),
        }
        for name, value in (message.headers() or [])
    ]
    delay = 1.0
    while not stop.is_set():
        try:
            return store.persist_dead_letter(
                position,
                consumer_group,
                message.key(),
                message.value(),
                headers,
                error,
            )
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            store.close()
            log(
                logger,
                logging.WARNING,
                "postgres_dead_letter_retry",
                topic=position.topic,
                partition=position.partition,
                offset=position.offset,
                retry_seconds=delay,
                error=str(exc),
            )
            stop.wait(delay)
            delay = min(delay * 2, 30.0)
    raise RuntimeError("Shutdown requested before dead-letter persistence completed.")


def commit_with_retry(
    consumer: Consumer,
    message: Message,
    stop: threading.Event,
    logger: logging.Logger,
) -> None:
    delay = 1.0
    while not stop.is_set():
        try:
            results = consumer.commit(message=message, asynchronous=False) or []
            errors = [result.error for result in results if result.error is not None]
            if errors:
                raise KafkaException(errors[0])
            return
        except KafkaException as exc:
            if not kafka_exception_is_retriable(exc):
                raise
            log(
                logger,
                logging.WARNING,
                "kafka_commit_retry",
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
                retry_seconds=delay,
                error=str(exc),
            )
            stop.wait(delay)
            delay = min(delay * 2, 30.0)
    raise RuntimeError("Shutdown requested before Kafka offset commit completed.")


def run(settings: Settings) -> None:
    logger = configure_logging(settings.log_level)
    stop = threading.Event()

    def request_stop(signum: int, _frame: Any) -> None:
        log(logger, logging.INFO, "shutdown_requested", signal=signum)
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_group_id,
            "client.id": "ledgersync-ingestion",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "session.timeout.ms": 10000,
            "max.poll.interval.ms": 900000,
        }
    )
    store = PostgresStore(settings.postgres_options)
    consumer.subscribe(list(settings.kafka_topics))
    log(
        logger,
        logging.INFO,
        "consumer_started",
        group_id=settings.kafka_group_id,
        topics=list(settings.kafka_topics),
    )

    try:
        while not stop.is_set():
            message = consumer.poll(1.0)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                if error.retriable():
                    log(logger, logging.WARNING, "kafka_poll_retry", error=str(error))
                    stop.wait(1.0)
                    continue
                raise KafkaException(error)

            position = KafkaPosition(
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
                timestamp=kafka_timestamp(message),
            )
            try:
                event = parse_debezium_event(message.value(), message.key())
            except MalformedDebeziumEvent as exc:
                inserted = persist_dead_letter_with_retry(
                    store,
                    position,
                    settings.kafka_group_id,
                    message,
                    exc,
                    stop,
                    logger,
                )
                commit_with_retry(consumer, message, stop, logger)
                log(
                    logger,
                    logging.ERROR,
                    "dead_letter_persisted" if inserted else "dead_letter_deduplicated",
                    topic=position.topic,
                    partition=position.partition,
                    offset=position.offset,
                    error_class=type(exc).__name__,
                    error=str(exc),
                )
                continue

            inserted = persist_with_retry(
                store,
                position,
                settings.kafka_group_id,
                event,
                stop,
                logger,
            )
            commit_with_retry(consumer, message, stop, logger)
            log(
                logger,
                logging.INFO,
                "event_persisted" if inserted else "event_deduplicated",
                topic=position.topic,
                partition=position.partition,
                offset=position.offset,
                operation=event.operation.value,
                source_database=event.source_database,
                source_schema=event.source_schema,
                source_table=event.source_table,
            )
    finally:
        store.close()
        consumer.close()
        log(logger, logging.INFO, "consumer_stopped")


def main() -> int:
    try:
        run(Settings.from_environment())
        return 0
    except Exception:
        logger = configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
        logger.exception("ingestion_service_failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
