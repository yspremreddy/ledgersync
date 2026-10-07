"""LedgerSync portfolio dashboard backed exclusively by the FastAPI API."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Callable

import streamlit as st

from api_client import ApiClient, ApiError


API_BASE_URL = os.getenv("LEDGERSYNC_API_BASE_URL", "http://api:18000")
DEMO_TRANSACTION_ID = os.getenv(
    "LEDGERSYNC_DEMO_TRANSACTION_ID", "final-e2e-20261007-mvp-final-tx-match"
)
RESULT_TYPES = (
    "MATCH",
    "MISSING_RECORD",
    "AMOUNT_MISMATCH",
    "STATUS_MISMATCH",
    "DUPLICATE_RECORD",
)

st.set_page_config(
    page_title="LedgerSync",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded",
)


def api_client() -> ApiClient:
    timeout = float(os.getenv("LEDGERSYNC_API_TIMEOUT_SECONDS", "5"))
    return ApiClient(API_BASE_URL, timeout=timeout)


def load(label: str, request: Callable[[], dict[str, Any]]) -> dict[str, Any] | None:
    try:
        return request()
    except ApiError as exc:
        st.error(f"{label}: {exc}")
        return None


def friendly_time(value: str | None) -> str:
    if not value:
        return "—"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    except ValueError:
        return value


def compact_rows(rows: list[dict[str, Any]], excluded: set[str]) -> list[dict[str, Any]]:
    return [{key: value for key, value in row.items() if key not in excluded} for row in rows]


def overview(client: ApiClient) -> None:
    st.subheader("Operational overview")
    health = load("Health check failed", client.health)
    metrics = load("Metrics could not be loaded", client.metrics)

    if health:
        st.success("API and PostgreSQL are healthy")
    else:
        st.warning("Live service status is unavailable")

    if not metrics:
        return

    top = st.columns(4)
    top[0].metric("Raw CDC events", metrics["raw_cdc_event_count"])
    top[1].metric("DLQ events", metrics["dlq_count"])
    top[2].metric("Current ledger records", metrics["current_ledger_record_count"])
    top[3].metric("Deleted records", metrics["deleted_record_count"])

    result_counts = metrics.get("reconciliation_result_counts", {})
    st.caption("Latest reconciliation result set")
    classification_columns = st.columns(5)
    for column, result_type in zip(classification_columns, RESULT_TYPES):
        column.metric(result_type.replace("_", " ").title(), result_counts.get(result_type, 0))

    lag_rows = metrics.get("cdc_checkpoint_lag", [])
    total_lag = sum(row.get("lag", 0) for row in lag_rows)
    latest_run = metrics.get("latest_reconciliation_run")
    lower = st.columns(2)
    lower[0].metric("Total checkpoint lag", total_lag)
    with lower[1].container(border=True):
        st.markdown("**Latest reconciliation run**")
        if latest_run:
            st.write(f"Run **#{latest_run['run_id']}** · {latest_run['status']}")
            st.caption(f"Completed {friendly_time(latest_run.get('completed_at'))}")
        else:
            st.write("No reconciliation run is available.")

    if lag_rows:
        with st.expander("CDC checkpoint details"):
            st.dataframe(lag_rows, width="stretch", hide_index=True)


def reconciliation(client: ApiClient) -> None:
    st.subheader("Reconciliation")
    selection = st.selectbox(
        "Classification",
        ("ALL", *RESULT_TYPES),
        format_func=lambda value: value.replace("_", " ").title(),
    )
    results = load(
        "Reconciliation results could not be loaded",
        lambda: client.reconciliation_results(
            None if selection == "ALL" else selection, limit=100
        ),
    )
    if results:
        st.caption(f"Showing {len(results['items'])} of {results['total']} results")
        visible_results = compact_rows(
            results["items"], {"left_evidence", "right_evidence"}
        )
        st.dataframe(visible_results, width="stretch", hide_index=True)
        evidence_rows = [
            row
            for row in results["items"]
            if row.get("left_evidence") or row.get("right_evidence")
        ]
        if evidence_rows:
            with st.expander("Source evidence"):
                for row in evidence_rows[:10]:
                    st.markdown(
                        f"**{row['transaction_id']} · {row['currency']} · "
                        f"{row['result_type']}**"
                    )
                    st.json(
                        {
                            "left": row.get("left_evidence", []),
                            "right": row.get("right_evidence", []),
                        },
                        expanded=False,
                    )

    st.markdown("#### Recent runs")
    runs = load("Reconciliation runs could not be loaded", client.reconciliation_runs)
    if runs:
        display_rows = []
        for row in runs["items"]:
            display_rows.append(
                {
                    "run_id": row["run_id"],
                    "status": row["status"],
                    "completed_at": friendly_time(row.get("completed_at")),
                    "source_pair": (
                        f"{row['left_source_system']} → {row['right_source_system']}"
                    ),
                    "total": row["total_result_count"],
                    "match": row["match_count"],
                    "missing": row["missing_record_count"],
                    "amount_mismatch": row["amount_mismatch_count"],
                    "status_mismatch": row["status_mismatch_count"],
                    "duplicate": row["duplicate_record_count"],
                }
            )
        st.dataframe(display_rows, width="stretch", hide_index=True)


def transaction_investigation(client: ApiClient) -> None:
    st.subheader("Transaction investigation")
    st.write("Inspect current ledger records and reconciliation evidence by transaction ID.")
    with st.form("transaction-search"):
        transaction_id = st.text_input("Transaction ID", value=DEMO_TRANSACTION_ID)
        submitted = st.form_submit_button("Investigate", type="primary")

    if not submitted:
        st.info("Enter a transaction ID and select Investigate.")
        return
    if not transaction_id.strip():
        st.warning("Transaction ID is required.")
        return

    data = load(
        "Transaction lookup failed",
        lambda: client.transaction(transaction_id.strip()),
    )
    if not data:
        return

    ledger_records = data.get("ledger_records", [])
    reconciliation_results = data.get("reconciliation_results", [])
    st.success(
        f"Found {len(ledger_records)} ledger record(s) and "
        f"{len(reconciliation_results)} reconciliation result(s)."
    )
    st.markdown("#### Ledger records")
    st.dataframe(
        compact_rows(ledger_records, {"source_payload"}),
        width="stretch",
        hide_index=True,
    )
    with st.expander("Ledger source payloads"):
        for row in ledger_records:
            st.markdown(f"**{row['source_system']} / {row['source_record_id']}**")
            st.json(row.get("source_payload", {}), expanded=False)

    st.markdown("#### Reconciliation result")
    if reconciliation_results:
        st.dataframe(
            compact_rows(
                reconciliation_results, {"left_evidence", "right_evidence"}
            ),
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("No reconciliation result currently references this transaction.")


def data_quality(client: ApiClient) -> None:
    st.subheader("Data quality")
    checks = load("Quality checks could not be loaded", client.quality_checks)
    if not checks or not checks["items"]:
        st.info("No quality check executions are available.")
        return

    newest_execution = checks["items"][0]["executed_at"]
    latest = [row for row in checks["items"] if row["executed_at"] == newest_execution]
    failures = [row for row in latest if row["status"] == "FAIL"]
    if failures:
        st.error(f"{len(failures)} of {len(latest)} latest checks failed")
    else:
        st.success(f"All {len(latest)} latest checks passed")
    st.caption(f"Latest execution: {friendly_time(newest_execution)}")

    visible = []
    for row in latest:
        visible.append(
            {
                "status": "🔴 FAIL" if row["status"] == "FAIL" else "🟢 PASS",
                "check_name": row["check_name"],
                "affected_rows": row["affected_row_count"],
                "executed_at": friendly_time(row["executed_at"]),
            }
        )
    st.dataframe(visible, width="stretch", hide_index=True)

    for row in failures:
        with st.expander(f"Failure details · {row['check_name']}"):
            st.json(row.get("details", {}), expanded=True)


def service_status(client: ApiClient) -> None:
    st.subheader("Service status")
    health = load("Health check failed", client.health)
    if health:
        columns = st.columns(2)
        columns[0].success(f"API: {health['status'].upper()}")
        columns[1].success(f"PostgreSQL: {health['database'].upper()}")
    else:
        st.error("API or database health could not be confirmed.")
    st.caption(f"API endpoint: {API_BASE_URL}")
    if st.button("Refresh status", type="primary"):
        st.rerun()


st.title("LedgerSync")
st.caption("Durable CDC · Current state · Reconciliation · Reliability")

client = api_client()
with st.sidebar:
    st.markdown("### Investigation console")
    st.write("Read-only visibility into the LedgerSync demonstration pipeline.")
    st.caption("All data is retrieved through FastAPI.")

tabs = st.tabs(
    ["Overview", "Reconciliation", "Transaction", "Data quality", "Service status"]
)
with tabs[0]:
    overview(client)
with tabs[1]:
    reconciliation(client)
with tabs[2]:
    transaction_investigation(client)
with tabs[3]:
    data_quality(client)
with tabs[4]:
    service_status(client)
