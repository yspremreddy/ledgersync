#!/usr/bin/env bash
# The official PostgreSQL entrypoint sources non-executable init scripts.
# Keep shell options inside a subshell rather than changing its parent shell.
(
set -Eeuo pipefail
psql --username "$POSTGRES_USER" --dbname postgres --set ON_ERROR_STOP=1 \
  --set cdc_password="$CDC_PASSWORD" <<'SQL'
CREATE DATABASE ledger_source;
CREATE DATABASE ledgersync;
CREATE ROLE ledger_cdc WITH LOGIN REPLICATION PASSWORD :'cdc_password';
GRANT CONNECT ON DATABASE ledger_source TO ledger_cdc;
SQL
)
