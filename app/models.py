"""Response models for the investigation API."""

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel


ResultType = Literal[
    "MATCH",
    "MISSING_RECORD",
    "AMOUNT_MISMATCH",
    "STATUS_MISMATCH",
    "DUPLICATE_RECORD",
]
RunStatus = Literal["RUNNING", "SUCCEEDED", "FAILED"]
QualityStatus = Literal["PASS", "FAIL"]


class HealthResponse(BaseModel):
    status: Literal["ok"]
    database: Literal["ok"]


class ReconciliationRun(BaseModel):
    run_id: int
    left_source_system: str
    right_source_system: str
    started_at: datetime
    completed_at: datetime | None
    status: RunStatus
    total_result_count: int
    match_count: int
    missing_record_count: int
    amount_mismatch_count: int
    status_mismatch_count: int
    duplicate_record_count: int
    error_message: str | None


class ReconciliationResult(BaseModel):
    run_id: int
    left_source_system: str
    right_source_system: str
    transaction_id: str
    currency: str
    result_type: ResultType
    left_record_count: int
    right_record_count: int
    left_amount: Decimal | None
    right_amount: Decimal | None
    left_status: str | None
    right_status: str | None
    left_evidence: list[dict[str, Any]]
    right_evidence: list[dict[str, Any]]
    reconciled_at: datetime


class LedgerRecord(BaseModel):
    source_system: str
    source_record_id: str
    transaction_id: str
    amount: Decimal | None
    currency: str | None
    status: str | None
    is_deleted: bool
    source_event_timestamp: datetime
    source_database: str
    source_schema: str
    source_table: str
    last_kafka_topic: str
    last_kafka_partition: int
    last_kafka_offset: int
    last_source_lsn: str | None
    last_cdc_event_timestamp: datetime
    source_payload: dict[str, Any]
    refreshed_at: datetime


class RunPage(BaseModel):
    items: list[ReconciliationRun]
    total: int
    limit: int
    offset: int


class ResultPage(BaseModel):
    items: list[ReconciliationResult]
    total: int
    limit: int
    offset: int


class TransactionInvestigation(BaseModel):
    transaction_id: str
    ledger_records: list[LedgerRecord]
    reconciliation_results: list[ReconciliationResult]


class QualityCheckResult(BaseModel):
    executed_at: datetime
    check_name: str
    status: QualityStatus
    affected_row_count: int
    details: dict[str, Any]


class QualityCheckPage(BaseModel):
    items: list[QualityCheckResult]
    total: int
    limit: int
    offset: int


class LatestReconciliationRunMetric(BaseModel):
    run_id: int
    status: RunStatus
    started_at: datetime
    completed_at: datetime | None


class CdcCheckpointLagMetric(BaseModel):
    kafka_topic: str
    kafka_partition: int
    raw_max_offset: int | None
    checkpoint_offset: int | None
    lag: int
    checkpoint_updated_at: datetime | None


class ReliabilityMetrics(BaseModel):
    observed_at: datetime
    raw_cdc_event_count: int
    dlq_count: int
    current_ledger_record_count: int
    deleted_record_count: int
    reconciliation_result_counts: dict[str, int]
    latest_reconciliation_run: LatestReconciliationRunMetric | None
    cdc_checkpoint_lag: list[CdcCheckpointLagMetric]
