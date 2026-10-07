\set ON_ERROR_STOP on

-- Create or rotate the dedicated ingestion login without exposing its password
-- in tracked SQL. psql supplies ingest_password from the container environment.
SELECT format('CREATE ROLE ledger_ingest LOGIN PASSWORD %L', :'ingest_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ledger_ingest')
\gexec
ALTER ROLE ledger_ingest WITH LOGIN PASSWORD :'ingest_password';

CREATE SCHEMA IF NOT EXISTS ingestion AUTHORIZATION ledgersync_admin;

CREATE TABLE IF NOT EXISTS ingestion.raw_cdc_events (
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    consumer_group text NOT NULL,
    kafka_timestamp timestamptz,
    source_database text NOT NULL,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    operation text NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE', 'SNAPSHOT')),
    event_timestamp timestamptz NOT NULL,
    source_timestamp timestamptz,
    source_lsn text,
    source_tx_id text,
    source_metadata jsonb NOT NULL,
    transaction_metadata jsonb,
    record_key jsonb,
    before_payload jsonb,
    after_payload jsonb,
    payload jsonb NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS raw_cdc_events_source_time_idx
    ON ingestion.raw_cdc_events
    (source_database, source_schema, source_table, event_timestamp);

CREATE TABLE IF NOT EXISTS ingestion.cdc_dead_letters (
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    consumer_group text NOT NULL,
    kafka_timestamp timestamptz,
    record_key bytea,
    record_value bytea,
    record_headers jsonb NOT NULL DEFAULT '[]'::jsonb,
    error_class text NOT NULL,
    failure_reason text NOT NULL,
    failed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS cdc_dead_letters_failed_at_idx
    ON ingestion.cdc_dead_letters (failed_at);

GRANT USAGE ON SCHEMA ingestion TO ledger_ingest;
GRANT SELECT, INSERT ON ingestion.raw_cdc_events TO ledger_ingest;
GRANT SELECT, INSERT ON ingestion.cdc_dead_letters TO ledger_ingest;
