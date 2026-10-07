"""Read-only FastAPI endpoints for LedgerSync investigation."""

from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import Depends, FastAPI, HTTPException, Path, Query, status
import psycopg

from app.database import get_connection
from app.models import (
    HealthResponse,
    LedgerRecord,
    QualityCheckPage,
    ReconciliationResult,
    ReliabilityMetrics,
    ResultPage,
    RunPage,
    TransactionInvestigation,
)


app = FastAPI(
    title="LedgerSync Investigation API",
    version="0.2.0",
    description=(
        "Read-only access to ledger, reconciliation, quality, and reliability data."
    ),
)

Connection = Annotated[psycopg.Connection, Depends(get_connection)]
PageLimit = Annotated[int, Query(ge=1, le=200)]
PageOffset = Annotated[int, Query(ge=0)]

RUN_COLUMNS = """
run_id, left_source_system, right_source_system, started_at, completed_at,
status, total_result_count, match_count, missing_record_count,
amount_mismatch_count, status_mismatch_count, duplicate_record_count,
error_message
"""

RESULT_COLUMNS = """
run_id, left_source_system, right_source_system, transaction_id, currency,
result_type, left_record_count, right_record_count, left_amount, right_amount,
left_status, right_status, left_evidence, right_evidence, reconciled_at
"""

LEDGER_COLUMNS = """
source_system, source_record_id, transaction_id, amount, currency, status,
is_deleted, source_event_timestamp, source_database, source_schema,
source_table, last_kafka_topic, last_kafka_partition, last_kafka_offset,
last_source_lsn, last_cdc_event_timestamp, source_payload, refreshed_at
"""

QUALITY_CHECK_COLUMNS = """
executed_at, check_name, status, affected_row_count, details
"""


def _where_clause(filters: list[tuple[str, Any]]) -> tuple[str, list[Any]]:
    active = [(clause, value) for clause, value in filters if value is not None]
    if not active:
        return "", []
    return " WHERE " + " AND ".join(item[0] for item in active), [
        item[1] for item in active
    ]


def _fetch_page(
    connection: psycopg.Connection,
    table: str,
    columns: str,
    filters: list[tuple[str, Any]],
    order_by: str,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    where_sql, parameters = _where_clause(filters)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT count(*) AS total FROM {table}{where_sql}", parameters)
        total = cursor.fetchone()["total"]
        cursor.execute(
            f"SELECT {columns} FROM {table}{where_sql} "
            f"ORDER BY {order_by} LIMIT %s OFFSET %s",
            [*parameters, limit, offset],
        )
        return list(cursor.fetchall()), total


@app.get("/health", response_model=HealthResponse)
def health(connection: Connection) -> HealthResponse:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 AS healthy")
        if cursor.fetchone()["healthy"] != 1:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="database unavailable",
            )
    return HealthResponse(status="ok", database="ok")


@app.get("/reconciliation/runs", response_model=RunPage)
def reconciliation_runs(
    connection: Connection,
    limit: PageLimit = 50,
    offset: PageOffset = 0,
    left_source_system: str | None = None,
    right_source_system: str | None = None,
    run_status: Literal["RUNNING", "SUCCEEDED", "FAILED"] | None = Query(
        default=None, alias="status"
    ),
) -> RunPage:
    items, total = _fetch_page(
        connection,
        "reconciliation.runs",
        RUN_COLUMNS,
        [
            ("left_source_system = %s", left_source_system),
            ("right_source_system = %s", right_source_system),
            ("status = %s", run_status),
        ],
        "run_id DESC",
        limit,
        offset,
    )
    return RunPage(items=items, total=total, limit=limit, offset=offset)


@app.get("/reconciliation/results", response_model=ResultPage)
def reconciliation_results(
    connection: Connection,
    limit: PageLimit = 50,
    offset: PageOffset = 0,
    run_id: int | None = Query(default=None, ge=1),
    left_source_system: str | None = None,
    right_source_system: str | None = None,
    result_type: Literal[
        "MATCH",
        "MISSING_RECORD",
        "AMOUNT_MISMATCH",
        "STATUS_MISMATCH",
        "DUPLICATE_RECORD",
    ]
    | None = None,
    transaction_id: str | None = None,
    currency: str | None = Query(default=None, min_length=3, max_length=3),
) -> ResultPage:
    items, total = _fetch_page(
        connection,
        "reconciliation.results",
        RESULT_COLUMNS,
        [
            ("run_id = %s", run_id),
            ("left_source_system = %s", left_source_system),
            ("right_source_system = %s", right_source_system),
            ("result_type = %s", result_type),
            ("transaction_id = %s", transaction_id),
            ("currency = %s", currency.upper() if currency else None),
        ],
        "reconciled_at DESC, transaction_id, currency",
        limit,
        offset,
    )
    return ResultPage(items=items, total=total, limit=limit, offset=offset)


@app.get(
    "/ledger/{source_system}/{source_record_id}", response_model=LedgerRecord
)
def ledger_record(
    connection: Connection,
    source_system: Annotated[str, Path(min_length=1)],
    source_record_id: Annotated[str, Path(min_length=1)],
) -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {LEDGER_COLUMNS} FROM reconciliation.ledger_current "
            "WHERE source_system = %s AND source_record_id = %s",
            (source_system, source_record_id),
        )
        record = cursor.fetchone()
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")
    return record


@app.get("/transactions/{transaction_id}", response_model=TransactionInvestigation)
def transaction(
    connection: Connection,
    transaction_id: Annotated[str, Path(min_length=1)],
) -> TransactionInvestigation:
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {LEDGER_COLUMNS} FROM reconciliation.ledger_current "
            "WHERE transaction_id = %s ORDER BY source_system, source_record_id",
            (transaction_id,),
        )
        ledger_records = list(cursor.fetchall())
        cursor.execute(
            f"SELECT {RESULT_COLUMNS} FROM reconciliation.results "
            "WHERE transaction_id = %s "
            "ORDER BY left_source_system, right_source_system, currency",
            (transaction_id,),
        )
        results = list(cursor.fetchall())

    if not ledger_records and not results:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")
    return TransactionInvestigation(
        transaction_id=transaction_id,
        ledger_records=ledger_records,
        reconciliation_results=results,
    )


@app.get("/quality/checks", response_model=QualityCheckPage)
def quality_checks(
    connection: Connection,
    limit: PageLimit = 50,
    offset: PageOffset = 0,
    check_name: str | None = None,
    check_status: Literal["PASS", "FAIL"] | None = Query(
        default=None, alias="status"
    ),
    executed_after: datetime | None = None,
) -> QualityCheckPage:
    items, total = _fetch_page(
        connection,
        "quality.check_results",
        QUALITY_CHECK_COLUMNS,
        [
            ("check_name = %s", check_name),
            ("status = %s", check_status),
            ("executed_at >= %s", executed_after),
        ],
        "executed_at DESC, check_name",
        limit,
        offset,
    )
    return QualityCheckPage(items=items, total=total, limit=limit, offset=offset)


@app.get("/metrics", response_model=ReliabilityMetrics)
def reliability_metrics(connection: Connection) -> ReliabilityMetrics:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM quality.reliability_metrics")
        summary = cursor.fetchone()
        cursor.execute(
            "SELECT result_type, result_count "
            "FROM quality.reconciliation_result_counts ORDER BY result_type"
        )
        result_counts = {
            row["result_type"]: row["result_count"] for row in cursor.fetchall()
        }
        cursor.execute(
            "SELECT kafka_topic, kafka_partition, raw_max_offset, "
            "checkpoint_offset, lag, checkpoint_updated_at "
            "FROM quality.cdc_checkpoint_lag "
            "ORDER BY kafka_topic, kafka_partition"
        )
        checkpoint_lag = list(cursor.fetchall())

    latest_run = None
    if summary["latest_reconciliation_run_id"] is not None:
        latest_run = {
            "run_id": summary["latest_reconciliation_run_id"],
            "status": summary["latest_reconciliation_run_status"],
            "started_at": summary["latest_reconciliation_run_started_at"],
            "completed_at": summary["latest_reconciliation_run_completed_at"],
        }

    return ReliabilityMetrics(
        observed_at=summary["observed_at"],
        raw_cdc_event_count=summary["raw_cdc_event_count"],
        dlq_count=summary["dlq_count"],
        current_ledger_record_count=summary["current_ledger_record_count"],
        deleted_record_count=summary["deleted_record_count"],
        reconciliation_result_counts=result_counts,
        latest_reconciliation_run=latest_run,
        cdc_checkpoint_lag=checkpoint_lag,
    )
