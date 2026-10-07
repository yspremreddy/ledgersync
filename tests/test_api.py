import unittest
from datetime import datetime, timezone
from decimal import Decimal

from app.main import (
    health,
    ledger_record,
    quality_checks,
    reconciliation_results,
    reconciliation_runs,
    reliability_metrics,
    transaction,
)


NOW = datetime(2026, 10, 7, tzinfo=timezone.utc)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query, parameters=None):
        self.connection.executions.append((query, parameters))
        self.rows = self.connection.responses.pop(0)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.executions = []

    def cursor(self):
        return FakeCursor(self)


def run_row():
    return {
        "run_id": 9,
        "left_source_system": "left",
        "right_source_system": "right",
        "started_at": NOW,
        "completed_at": NOW,
        "status": "SUCCEEDED",
        "total_result_count": 1,
        "match_count": 1,
        "missing_record_count": 0,
        "amount_mismatch_count": 0,
        "status_mismatch_count": 0,
        "duplicate_record_count": 0,
        "error_message": None,
    }


def result_row():
    return {
        "run_id": 9,
        "left_source_system": "left",
        "right_source_system": "right",
        "transaction_id": "tx-1",
        "currency": "USD",
        "result_type": "MATCH",
        "left_record_count": 1,
        "right_record_count": 1,
        "left_amount": Decimal("12.3400"),
        "right_amount": Decimal("12.3400"),
        "left_status": "POSTED",
        "right_status": "POSTED",
        "left_evidence": [],
        "right_evidence": [],
        "reconciled_at": NOW,
    }


def ledger_row():
    return {
        "source_system": "left",
        "source_record_id": "record-1",
        "transaction_id": "tx-1",
        "amount": Decimal("12.3400"),
        "currency": "USD",
        "status": "POSTED",
        "is_deleted": False,
        "source_event_timestamp": NOW,
        "source_database": "ledger_source",
        "source_schema": "public",
        "source_table": "ledger_entries",
        "last_kafka_topic": "ledger-topic",
        "last_kafka_partition": 0,
        "last_kafka_offset": 10,
        "last_source_lsn": "0/123",
        "last_cdc_event_timestamp": NOW,
        "source_payload": {"source_record_id": "record-1"},
        "refreshed_at": NOW,
    }


class ApiTests(unittest.TestCase):
    def assert_read_only_sql(self, connection):
        for query, _ in connection.executions:
            self.assertEqual(query.lstrip().split(maxsplit=1)[0].upper(), "SELECT")

    def test_health_checks_database(self):
        connection = FakeConnection([{"healthy": 1}])
        response = health(connection)
        self.assertEqual(
            response.model_dump(mode="json"), {"status": "ok", "database": "ok"}
        )
        self.assert_read_only_sql(connection)

    def test_runs_filter_and_page_are_parameterized(self):
        connection = FakeConnection([{"total": 1}], [run_row()])
        response = reconciliation_runs(
            connection,
            limit=10,
            offset=0,
            left_source_system="left",
            right_source_system=None,
            run_status="SUCCEEDED",
        )
        self.assertEqual(response.total, 1)
        self.assertEqual(response.items[0].status, "SUCCEEDED")
        self.assertEqual(connection.executions[1][1], ["left", "SUCCEEDED", 10, 0])
        self.assertNotIn("'left'", connection.executions[1][0])
        self.assertIn("left_source_system = %s", connection.executions[1][0])
        self.assert_read_only_sql(connection)

    def test_results_filter_and_page_are_parameterized(self):
        connection = FakeConnection([{"total": 1}], [result_row()])
        response = reconciliation_results(
            connection,
            limit=50,
            offset=0,
            run_id=None,
            left_source_system=None,
            right_source_system=None,
            result_type="MATCH",
            transaction_id=None,
            currency="usd",
        )
        self.assertEqual(response.total, 1)
        self.assertEqual(response.items[0].result_type, "MATCH")
        self.assertEqual(connection.executions[1][1], ["MATCH", "USD", 50, 0])
        self.assert_read_only_sql(connection)

    def test_ledger_lookup_uses_both_path_parameters(self):
        connection = FakeConnection([ledger_row()])
        response = ledger_record(connection, "left", "record-1")
        self.assertEqual(response["source_record_id"], "record-1")
        self.assertEqual(connection.executions[0][1], ("left", "record-1"))
        self.assert_read_only_sql(connection)

    def test_transaction_returns_ledger_and_reconciliation_evidence(self):
        connection = FakeConnection([ledger_row()], [result_row()])
        response = transaction(connection, "tx-1")
        self.assertEqual(len(response.ledger_records), 1)
        self.assertEqual(len(response.reconciliation_results), 1)
        self.assertEqual(connection.executions[0][1], ("tx-1",))
        self.assertEqual(connection.executions[1][1], ("tx-1",))
        self.assert_read_only_sql(connection)

    def test_quality_checks_filter_and_page_are_parameterized(self):
        quality_row = {
            "executed_at": NOW,
            "check_name": "invalid_non_positive_amounts",
            "status": "FAIL",
            "affected_row_count": 2,
            "details": {"description": "fixture"},
        }
        connection = FakeConnection([{"total": 1}], [quality_row])
        response = quality_checks(
            connection,
            limit=25,
            offset=0,
            check_name="invalid_non_positive_amounts",
            check_status="FAIL",
            executed_after=NOW,
        )
        self.assertEqual(response.total, 1)
        self.assertEqual(response.items[0].affected_row_count, 2)
        self.assertEqual(
            connection.executions[1][1],
            ["invalid_non_positive_amounts", "FAIL", NOW, 25, 0],
        )
        self.assertIn("check_name = %s", connection.executions[1][0])
        self.assert_read_only_sql(connection)

    def test_metrics_returns_all_reliability_sections(self):
        summary = {
            "observed_at": NOW,
            "raw_cdc_event_count": 18,
            "dlq_count": 1,
            "current_ledger_record_count": 9,
            "deleted_record_count": 1,
            "latest_reconciliation_run_id": 9,
            "latest_reconciliation_run_status": "SUCCEEDED",
            "latest_reconciliation_run_started_at": NOW,
            "latest_reconciliation_run_completed_at": NOW,
        }
        result_counts = [
            {"result_type": "MATCH", "result_count": 1},
            {"result_type": "MISSING_RECORD", "result_count": 1},
        ]
        checkpoint_lag = [
            {
                "kafka_topic": "ledger-topic",
                "kafka_partition": 0,
                "raw_max_offset": 10,
                "checkpoint_offset": 10,
                "lag": 0,
                "checkpoint_updated_at": NOW,
            }
        ]
        connection = FakeConnection([summary], result_counts, checkpoint_lag)
        response = reliability_metrics(connection)
        self.assertEqual(response.raw_cdc_event_count, 18)
        self.assertEqual(response.reconciliation_result_counts["MATCH"], 1)
        self.assertEqual(response.latest_reconciliation_run.run_id, 9)
        self.assertEqual(response.cdc_checkpoint_lag[0].lag, 0)
        self.assert_read_only_sql(connection)


if __name__ == "__main__":
    unittest.main()
