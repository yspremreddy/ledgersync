"""Parse schemaless or schema-wrapped Debezium change events."""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
import json
from typing import Any


class MalformedDebeziumEvent(ValueError):
    """Raised when a Kafka record is not a usable Debezium change event."""


class Operation(StrEnum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    SNAPSHOT = "SNAPSHOT"


OPERATION_MAP = {
    "c": Operation.INSERT,
    "u": Operation.UPDATE,
    "d": Operation.DELETE,
    "r": Operation.SNAPSHOT,
}


@dataclass(frozen=True)
class KafkaPosition:
    topic: str
    partition: int
    offset: int
    timestamp: datetime | None = None

    @property
    def deduplication_key(self) -> tuple[str, int, int]:
        return (self.topic, self.partition, self.offset)


@dataclass(frozen=True)
class DebeziumEvent:
    operation: Operation
    source_database: str
    source_schema: str
    source_table: str
    event_timestamp: datetime
    source_timestamp: datetime | None
    source_lsn: str | None
    source_tx_id: str | None
    source_metadata: dict[str, Any]
    transaction_metadata: Any
    record_key: Any
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    raw_event: dict[str, Any]


def _decode_json(data: bytes | None, label: str, *, required: bool) -> Any:
    if data is None:
        if required:
            raise MalformedDebeziumEvent(f"Debezium {label} is null.")
        return None
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalformedDebeziumEvent(f"Debezium {label} is not valid UTF-8 JSON.") from exc


def _timestamp(value: Any, label: str, *, required: bool) -> datetime | None:
    if value is None and not required:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MalformedDebeziumEvent(f"Debezium {label} must be a numeric epoch millisecond value.")
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _required_text(mapping: dict[str, Any], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value:
        raise MalformedDebeziumEvent(f"Debezium source.{key} is missing or invalid.")
    return value


def parse_debezium_event(value: bytes | None, key: bytes | None) -> DebeziumEvent:
    raw_event = _decode_json(value, "value", required=True)
    if not isinstance(raw_event, dict):
        raise MalformedDebeziumEvent("Debezium value must be a JSON object.")

    envelope = raw_event.get("payload") if "payload" in raw_event else raw_event
    if not isinstance(envelope, dict):
        raise MalformedDebeziumEvent("Debezium payload must be a JSON object.")

    source = envelope.get("source")
    if not isinstance(source, dict):
        raise MalformedDebeziumEvent("Debezium source metadata is missing or invalid.")

    op_code = envelope.get("op")
    if op_code not in OPERATION_MAP:
        raise MalformedDebeziumEvent(f"Unsupported Debezium operation: {op_code!r}.")

    before = envelope.get("before")
    after = envelope.get("after")
    if before is not None and not isinstance(before, dict):
        raise MalformedDebeziumEvent("Debezium before payload must be an object or null.")
    if after is not None and not isinstance(after, dict):
        raise MalformedDebeziumEvent("Debezium after payload must be an object or null.")
    if op_code in ("c", "u", "r") and after is None:
        raise MalformedDebeziumEvent(f"Debezium operation {op_code!r} requires an after payload.")
    if op_code == "d" and before is None:
        raise MalformedDebeziumEvent("Debezium delete requires a before payload.")

    event_ts_ms = envelope.get("ts_ms", source.get("ts_ms"))
    source_ts_ms = source.get("ts_ms")

    return DebeziumEvent(
        operation=OPERATION_MAP[op_code],
        source_database=_required_text(source, "db"),
        source_schema=_required_text(source, "schema"),
        source_table=_required_text(source, "table"),
        event_timestamp=_timestamp(event_ts_ms, "ts_ms", required=True),
        source_timestamp=_timestamp(source_ts_ms, "source.ts_ms", required=False),
        source_lsn=None if source.get("lsn") is None else str(source["lsn"]),
        source_tx_id=None if source.get("txId") is None else str(source["txId"]),
        source_metadata=source,
        transaction_metadata=envelope.get("transaction"),
        record_key=_decode_json(key, "key", required=False),
        before=before,
        after=after,
        raw_event=raw_event,
    )
