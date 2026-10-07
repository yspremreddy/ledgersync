\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS quality AUTHORIZATION ledgersync_admin;

CREATE TABLE IF NOT EXISTS quality.supported_currencies (
    currency text PRIMARY KEY CHECK (currency ~ '^[A-Z]{3}$'),
    enabled boolean NOT NULL DEFAULT true
);

INSERT INTO quality.supported_currencies (currency)
VALUES ('EUR'), ('GBP'), ('INR'), ('USD')
ON CONFLICT (currency) DO NOTHING;

CREATE TABLE IF NOT EXISTS quality.check_results (
    executed_at timestamptz NOT NULL,
    check_name text NOT NULL,
    status text NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    affected_row_count bigint NOT NULL CHECK (affected_row_count >= 0),
    details jsonb NOT NULL,
    PRIMARY KEY (executed_at, check_name)
);

CREATE INDEX IF NOT EXISTS quality_check_results_name_time_idx
    ON quality.check_results (check_name, executed_at DESC);

CREATE OR REPLACE FUNCTION quality.run_checks()
RETURNS timestamptz
LANGUAGE plpgsql
AS $$
DECLARE
    execution_time timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('ledgersync.quality.run'));
    execution_time := clock_timestamp();

    INSERT INTO quality.check_results (
        executed_at,
        check_name,
        status,
        affected_row_count,
        details
    )
    WITH duplicate_groups AS (
        SELECT count(*) AS record_count
        FROM reconciliation.ledger_current
        WHERE NOT is_deleted
        GROUP BY source_system, transaction_id, currency
        HAVING count(*) > 1
    ),
    checks AS (
        SELECT
            'duplicate_source_records'::text AS check_name,
            COALESCE((SELECT sum(record_count) FROM duplicate_groups), 0)::bigint
                AS affected_row_count,
            jsonb_build_object(
                'description',
                'Active records sharing source system, transaction ID, and currency.'
            ) AS details
        UNION ALL
        SELECT
            'missing_required_transaction_fields',
            count(*)::bigint,
            jsonb_build_object(
                'description',
                'Active records with missing or blank canonical transaction fields.'
            )
        FROM reconciliation.ledger_current
        WHERE NOT is_deleted
          AND (
              btrim(source_system) = ''
              OR btrim(source_record_id) = ''
              OR btrim(transaction_id) = ''
              OR amount IS NULL
              OR currency IS NULL
              OR btrim(currency) = ''
              OR status IS NULL
              OR btrim(status) = ''
          )
        UNION ALL
        SELECT
            'invalid_non_positive_amounts',
            count(*)::bigint,
            jsonb_build_object(
                'description',
                'Active records whose amount is absent, zero, or negative.'
            )
        FROM reconciliation.ledger_current
        WHERE NOT is_deleted
          AND (amount IS NULL OR amount <= 0)
        UNION ALL
        SELECT
            'unsupported_invalid_currency',
            count(*)::bigint,
            jsonb_build_object(
                'description',
                'Active records without an enabled supported three-letter currency.'
            )
        FROM reconciliation.ledger_current AS ledger
        LEFT JOIN quality.supported_currencies AS supported
          ON supported.currency = ledger.currency
         AND supported.enabled
        WHERE NOT ledger.is_deleted
          AND (
              ledger.currency IS NULL
              OR ledger.currency !~ '^[A-Z]{3}$'
              OR supported.currency IS NULL
          )
        UNION ALL
        SELECT
            'reconciliation_result_anomalies',
            count(*)::bigint,
            jsonb_build_object(
                'description',
                'Current results inconsistent with their classification or successful run.'
            )
        FROM reconciliation.results AS result
        JOIN reconciliation.runs AS run USING (run_id)
        WHERE run.status <> 'SUCCEEDED'
           OR run.left_source_system <> result.left_source_system
           OR run.right_source_system <> result.right_source_system
           OR NOT CASE result.result_type
               WHEN 'MATCH' THEN
                   result.left_record_count = 1
                   AND result.right_record_count = 1
                   AND result.left_amount IS NOT DISTINCT FROM result.right_amount
                   AND result.left_status IS NOT DISTINCT FROM result.right_status
               WHEN 'MISSING_RECORD' THEN
                   (result.left_record_count = 0 AND result.right_record_count = 1)
                   OR (result.left_record_count = 1 AND result.right_record_count = 0)
               WHEN 'AMOUNT_MISMATCH' THEN
                   result.left_record_count = 1
                   AND result.right_record_count = 1
                   AND result.left_amount IS DISTINCT FROM result.right_amount
               WHEN 'STATUS_MISMATCH' THEN
                   result.left_record_count = 1
                   AND result.right_record_count = 1
                   AND result.left_amount IS NOT DISTINCT FROM result.right_amount
                   AND result.left_status IS DISTINCT FROM result.right_status
               WHEN 'DUPLICATE_RECORD' THEN
                   result.left_record_count > 1 OR result.right_record_count > 1
               ELSE false
           END
        UNION ALL
        SELECT
            'stale_unprocessed_cdc_events',
            count(*)::bigint,
            jsonb_build_object(
                'description',
                'Raw events older than five minutes and beyond the reconciliation checkpoint.',
                'stale_after_seconds', 300
            )
        FROM ingestion.raw_cdc_events AS raw
        LEFT JOIN reconciliation.cdc_checkpoints AS checkpoint
          ON checkpoint.kafka_topic = raw.kafka_topic
         AND checkpoint.kafka_partition = raw.kafka_partition
        WHERE raw.kafka_offset > COALESCE(checkpoint.last_kafka_offset, -1)
          AND raw.ingested_at < clock_timestamp() - interval '5 minutes'
    )
    SELECT
        execution_time,
        check_name,
        CASE WHEN affected_row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        affected_row_count,
        details
    FROM checks;

    RETURN execution_time;
END;
$$;

CREATE OR REPLACE VIEW quality.reconciliation_result_counts AS
WITH classifications(result_type) AS (
    VALUES
        ('MATCH'::text),
        ('MISSING_RECORD'),
        ('AMOUNT_MISMATCH'),
        ('STATUS_MISMATCH'),
        ('DUPLICATE_RECORD')
)
SELECT
    classification.result_type,
    count(result.result_type)::bigint AS result_count
FROM classifications AS classification
LEFT JOIN reconciliation.results AS result
  ON result.result_type = classification.result_type
GROUP BY classification.result_type;

CREATE OR REPLACE VIEW quality.cdc_checkpoint_lag AS
WITH positions AS (
    SELECT kafka_topic, kafka_partition
    FROM ingestion.raw_cdc_events
    UNION
    SELECT kafka_topic, kafka_partition
    FROM reconciliation.cdc_checkpoints
),
raw_offsets AS (
    SELECT kafka_topic, kafka_partition, max(kafka_offset) AS raw_max_offset
    FROM ingestion.raw_cdc_events
    GROUP BY kafka_topic, kafka_partition
)
SELECT
    position.kafka_topic,
    position.kafka_partition,
    raw.raw_max_offset,
    checkpoint.last_kafka_offset AS checkpoint_offset,
    CASE
        WHEN raw.raw_max_offset IS NULL THEN 0
        WHEN checkpoint.last_kafka_offset IS NULL THEN raw.raw_max_offset + 1
        ELSE GREATEST(raw.raw_max_offset - checkpoint.last_kafka_offset, 0)
    END::bigint AS lag,
    checkpoint.updated_at AS checkpoint_updated_at
FROM positions AS position
LEFT JOIN raw_offsets AS raw USING (kafka_topic, kafka_partition)
LEFT JOIN reconciliation.cdc_checkpoints AS checkpoint
  USING (kafka_topic, kafka_partition);

CREATE OR REPLACE VIEW quality.reliability_metrics AS
SELECT
    clock_timestamp() AS observed_at,
    (SELECT count(*) FROM ingestion.raw_cdc_events)::bigint
        AS raw_cdc_event_count,
    (SELECT count(*) FROM ingestion.cdc_dead_letters)::bigint
        AS dlq_count,
    (SELECT count(*) FROM reconciliation.ledger_current)::bigint
        AS current_ledger_record_count,
    (
        SELECT count(*)
        FROM reconciliation.ledger_current
        WHERE is_deleted
    )::bigint AS deleted_record_count,
    latest.run_id AS latest_reconciliation_run_id,
    latest.status AS latest_reconciliation_run_status,
    latest.started_at AS latest_reconciliation_run_started_at,
    latest.completed_at AS latest_reconciliation_run_completed_at
FROM (SELECT 1) AS singleton
LEFT JOIN LATERAL (
    SELECT run_id, status, started_at, completed_at
    FROM reconciliation.runs
    ORDER BY run_id DESC
    LIMIT 1
) AS latest ON true;

REVOKE EXECUTE ON FUNCTION quality.run_checks() FROM PUBLIC;

GRANT USAGE ON SCHEMA quality TO ledger_api;
GRANT SELECT ON ALL TABLES IN SCHEMA quality TO ledger_api;
ALTER DEFAULT PRIVILEGES FOR ROLE ledgersync_admin IN SCHEMA quality
    GRANT SELECT ON TABLES TO ledger_api;
