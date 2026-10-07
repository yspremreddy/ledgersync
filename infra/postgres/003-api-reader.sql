\set ON_ERROR_STOP on

-- Create or rotate a least-privileged API login without storing its password
-- in tracked SQL. psql supplies api_password from the container environment.
SELECT format('CREATE ROLE ledger_api LOGIN PASSWORD %L', :'api_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ledger_api')
\gexec
ALTER ROLE ledger_api WITH LOGIN PASSWORD :'api_password';
ALTER ROLE ledger_api SET default_transaction_read_only = on;

GRANT CONNECT ON DATABASE ledgersync TO ledger_api;
GRANT USAGE ON SCHEMA reconciliation TO ledger_api;
GRANT SELECT ON ALL TABLES IN SCHEMA reconciliation TO ledger_api;
ALTER DEFAULT PRIVILEGES FOR ROLE ledgersync_admin IN SCHEMA reconciliation
    GRANT SELECT ON TABLES TO ledger_api;
