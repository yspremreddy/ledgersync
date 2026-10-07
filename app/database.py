"""PostgreSQL connection dependency for the read-only API."""

from collections.abc import Iterator
import logging
import os

from fastapi import HTTPException, status
import psycopg
from psycopg.rows import dict_row


LOGGER = logging.getLogger(__name__)


def _required_environment(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


def connection_options() -> dict[str, object]:
    statement_timeout = int(os.getenv("PGSTATEMENT_TIMEOUT_MS", "5000"))
    if statement_timeout <= 0:
        raise RuntimeError("PGSTATEMENT_TIMEOUT_MS must be a positive integer")

    return {
        "host": _required_environment("PGHOST"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": _required_environment("PGDATABASE"),
        "user": _required_environment("PGUSER"),
        "password": _required_environment("PGPASSWORD"),
        "connect_timeout": int(os.getenv("PGCONNECT_TIMEOUT", "5")),
        "options": (
            "-c default_transaction_read_only=on "
            f"-c statement_timeout={statement_timeout}"
        ),
        "row_factory": dict_row,
        "autocommit": True,
    }


def get_connection() -> Iterator[psycopg.Connection]:
    try:
        with psycopg.connect(**connection_options()) as connection:
            yield connection
    except (psycopg.Error, OSError, RuntimeError, ValueError):
        LOGGER.exception("database connection or query failed")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="database unavailable",
        ) from None
