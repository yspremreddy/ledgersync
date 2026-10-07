import json
import unittest

from ingestion.events import (
    KafkaPosition,
    MalformedDebeziumEvent,
    Operation,
    parse_debezium_event,
)


def event_bytes(operation, before, after):
    return json.dumps(
        {
            "before": before,
            "after": after,
            "source": {
                "db": "ledger_source",
                "schema": "public",
                "table": "cdc_smoke",
                "ts_ms": 1790793122789,
                "txId": 758,
                "lsn": 31130224,
            },
            "transaction": {"id": "758:1", "total_order": 1},
            "op": operation,
            "ts_ms": 1790793122885,
        }
    ).encode("utf-8")


class DebeziumEventTests(unittest.TestCase):
    def test_insert_parsing(self):
        event = parse_debezium_event(
            event_bytes("c", None, {"marker": "insert-marker"}),
            b'{"marker":"insert-marker"}',
        )
        self.assertEqual(event.operation, Operation.INSERT)
        self.assertEqual(event.after["marker"], "insert-marker")
        self.assertEqual(event.record_key, {"marker": "insert-marker"})
        self.assertEqual(event.source_lsn, "31130224")
        self.assertEqual(event.source_tx_id, "758")

    def test_update_parsing(self):
        event = parse_debezium_event(
            event_bytes(
                "u",
                {"marker": "before"},
                {"marker": "after"},
            ),
            b'{"marker":"after"}',
        )
        self.assertEqual(event.operation, Operation.UPDATE)
        self.assertEqual(event.before, {"marker": "before"})
        self.assertEqual(event.after, {"marker": "after"})

    def test_delete_parsing(self):
        event = parse_debezium_event(
            event_bytes("d", {"marker": "deleted"}, None),
            b'{"marker":"deleted"}',
        )
        self.assertEqual(event.operation, Operation.DELETE)
        self.assertEqual(event.before, {"marker": "deleted"})
        self.assertIsNone(event.after)

    def test_kafka_position_is_stable_deduplication_key(self):
        first = KafkaPosition("topic", 2, 41)
        retry = KafkaPosition("topic", 2, 41)
        next_message = KafkaPosition("topic", 2, 42)
        self.assertEqual(first.deduplication_key, retry.deduplication_key)
        self.assertNotEqual(first.deduplication_key, next_message.deduplication_key)

    def test_malformed_event_is_rejected(self):
        malformed_values = (
            b"not-json",
            b"{}",
            event_bytes("c", None, None),
            event_bytes("x", None, {"marker": "unsupported"}),
        )
        for value in malformed_values:
            with self.subTest(value=value):
                with self.assertRaises(MalformedDebeziumEvent):
                    parse_debezium_event(value, None)


if __name__ == "__main__":
    unittest.main()
