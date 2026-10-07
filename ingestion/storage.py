"""PostgreSQL persistence for raw CDC records."""

from collections.abc import Callable
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from ingestion.events import DebeziumEvent, KafkaPosition


INSERT_EVENT_SQL = """
INSERT INTO ingestion.raw_cdc_events (
    kafka_topic,
    kafka_partition,
    kafka_offset,
    consumer_group,
    kafka_timestamp,
    source_database,
    source_schema,
    source_table,
    operation,
    event_timestamp,
    source_timestamp,
    source_lsn,
    source_tx_id,
    source_metadata,
    transaction_metadata,
    record_key,
    before_payload,
    after_payload,
    payload
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
RETURNING kafka_offset
"""

INSERT_DEAD_LETTER_SQL = """
INSERT INTO ingestion.cdc_dead_letters (
    kafka_topic,
    kafka_partition,
    kafka_offset,
    consumer_group,
    kafka_timestamp,
    record_key,
    record_value,
    record_headers,
    error_class,
    failure_reason
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
RETURNING kafka_offset
"""


class PostgresStore:
    def __init__(
        self,
        connection_options: dict[str, Any],
        connect: Callable[..., psycopg.Connection[Any]] = psycopg.connect,
    ) -> None:
        self._connection_options = connection_options
        self._connect = connect
        self._connection: psycopg.Connection[Any] | None = None

    def _get_connection(self) -> psycopg.Connection[Any]:
        if self._connection is None or self._connection.closed:
            self._connection = self._connect(**self._connection_options, autocommit=True)
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def persist(
        self,
        position: KafkaPosition,
        consumer_group: str,
        event: DebeziumEvent,
    ) -> bool:
        connection = self._get_connection()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(
                        INSERT_EVENT_SQL,
                        (
                            position.topic,
                            position.partition,
                            position.offset,
                            consumer_group,
                            position.timestamp,
                            event.source_database,
                            event.source_schema,
                            event.source_table,
                            event.operation.value,
                            event.event_timestamp,
                            event.source_timestamp,
                            event.source_lsn,
                            event.source_tx_id,
                            Jsonb(event.source_metadata),
                            Jsonb(event.transaction_metadata),
                            Jsonb(event.record_key),
                            Jsonb(event.before),
                            Jsonb(event.after),
                            Jsonb(event.raw_event),
                        ),
                    )
                    return cursor.fetchone() is not None
        except Exception:
            if connection.broken:
                self.close()
            raise

    def persist_dead_letter(
        self,
        position: KafkaPosition,
        consumer_group: str,
        record_key: bytes | None,
        record_value: bytes | None,
        record_headers: list[dict[str, str | None]],
        error: Exception,
    ) -> bool:
        connection = self._get_connection()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(
                        INSERT_DEAD_LETTER_SQL,
                        (
                            position.topic,
                            position.partition,
                            position.offset,
                            consumer_group,
                            position.timestamp,
                            record_key,
                            record_value,
                            Jsonb(record_headers),
                            type(error).__name__,
                            str(error),
                        ),
                    )
                    return cursor.fetchone() is not None
        except Exception:
            if connection.broken:
                self.close()
            raise
