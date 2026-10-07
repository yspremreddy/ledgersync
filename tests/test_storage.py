import unittest

from ingestion.events import KafkaPosition, parse_debezium_event
from ingestion.storage import INSERT_DEAD_LETTER_SQL, INSERT_EVENT_SQL, PostgresStore
from tests.test_events import event_bytes


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, sql, parameters):
        self.connection.executed_sql = sql
        self.connection.executed_parameters = parameters
        position = tuple(parameters[:3])
        table = "dead_letter" if sql == INSERT_DEAD_LETTER_SQL else "raw"
        positions = self.connection.positions.setdefault(table, set())
        if position in positions:
            self.result = None
        else:
            positions.add(position)
            self.result = (parameters[2],)

    def fetchone(self):
        return self.result


class FakeTransaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class FakeConnection:
    closed = False
    broken = False

    def __init__(self):
        self.positions = {}
        self.executed_sql = None
        self.executed_parameters = None

    def transaction(self):
        return FakeTransaction()

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class PostgresStoreTests(unittest.TestCase):
    def test_duplicate_kafka_position_is_deduplicated(self):
        connection = FakeConnection()
        store = PostgresStore({}, connect=lambda **options: connection)
        position = KafkaPosition("topic", 0, 7)
        event = parse_debezium_event(
            event_bytes("c", None, {"marker": "idempotent"}),
            b'{"marker":"idempotent"}',
        )

        self.assertTrue(store.persist(position, "group", event))
        self.assertFalse(store.persist(position, "group", event))
        self.assertEqual(connection.positions["raw"], {("topic", 0, 7)})
        self.assertIn("ON CONFLICT", connection.executed_sql)
        self.assertEqual(connection.executed_sql, INSERT_EVENT_SQL)

    def test_malformed_record_is_preserved_and_deduplicated(self):
        connection = FakeConnection()
        store = PostgresStore({}, connect=lambda **options: connection)
        position = KafkaPosition("topic", 1, 9)
        error = ValueError("missing source metadata")

        self.assertTrue(
            store.persist_dead_letter(
                position,
                "group",
                b"original-key",
                b"not-json",
                [{"name": "trace", "value_base64": "YWJj"}],
                error,
            )
        )
        self.assertFalse(
            store.persist_dead_letter(
                position,
                "group",
                b"original-key",
                b"not-json",
                [{"name": "trace", "value_base64": "YWJj"}],
                error,
            )
        )
        self.assertEqual(connection.positions["dead_letter"], {("topic", 1, 9)})
        self.assertEqual(connection.executed_sql, INSERT_DEAD_LETTER_SQL)
        self.assertEqual(connection.executed_parameters[5], b"original-key")
        self.assertEqual(connection.executed_parameters[6], b"not-json")
        self.assertEqual(connection.executed_parameters[8], "ValueError")
        self.assertEqual(connection.executed_parameters[9], "missing source metadata")


if __name__ == "__main__":
    unittest.main()
