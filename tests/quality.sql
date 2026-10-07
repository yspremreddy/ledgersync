\set ON_ERROR_STOP on

BEGIN;

DELETE FROM quality.check_results;
DELETE FROM reconciliation.results;
DELETE FROM reconciliation.runs;
DELETE FROM reconciliation.cdc_checkpoints;
DELETE FROM reconciliation.ledger_current;
DELETE FROM ingestion.raw_cdc_events;
DELETE FROM ingestion.cdc_dead_letters;

CREATE TEMP TABLE quality_test_executions (
    scenario text PRIMARY KEY,
    executed_at timestamptz NOT NULL
) ON COMMIT DROP;

INSERT INTO quality_test_executions
SELECT 'pass', quality.run_checks();

DO $$
DECLARE
    pass_time timestamptz;
BEGIN
    SELECT executed_at INTO pass_time
    FROM quality_test_executions
    WHERE scenario = 'pass';

    IF (
        SELECT count(*)
        FROM quality.check_results
        WHERE executed_at = pass_time
          AND status = 'PASS'
          AND affected_row_count = 0
    ) <> 6 THEN
        RAISE EXCEPTION 'Expected all six checks to pass for an empty fixture.';
    END IF;
END;
$$;

INSERT INTO reconciliation.ledger_current (
    source_system,
    source_record_id,
    transaction_id,
    amount,
    currency,
    status,
    is_deleted,
    source_event_timestamp,
    source_database,
    source_schema,
    source_table,
    last_kafka_topic,
    last_kafka_partition,
    last_kafka_offset,
    last_source_lsn,
    last_cdc_event_timestamp,
    source_payload
)
SELECT
    fixture.source_system,
    fixture.source_record_id,
    fixture.transaction_id,
    fixture.amount,
    fixture.currency,
    fixture.status,
    fixture.is_deleted,
    clock_timestamp() - interval '10 minutes',
    'quality_fixture',
    'public',
    'ledger_entries',
    'quality.fixture',
    0,
    fixture.kafka_offset,
    fixture.kafka_offset::text,
    clock_timestamp() - interval '10 minutes',
    jsonb_build_object('fixture', fixture.source_record_id)
FROM (
    VALUES
        ('fixture-left', 'duplicate-1', 'tx-duplicate', 10.00::numeric, 'USD', 'POSTED', false, 1::bigint),
        ('fixture-left', 'duplicate-2', 'tx-duplicate', 10.00::numeric, 'USD', 'POSTED', false, 2::bigint),
        ('fixture-left', 'missing-field', '   ', 10.00::numeric, 'USD', 'POSTED', false, 3::bigint),
        ('fixture-left', 'invalid-amount', 'tx-amount', 0.00::numeric, 'USD', 'POSTED', false, 4::bigint),
        ('fixture-left', 'invalid-currency', 'tx-currency', 10.00::numeric, 'ZZZ', 'POSTED', false, 5::bigint),
        ('fixture-left', 'deleted', 'tx-deleted', NULL::numeric, NULL::text, NULL::text, true, 6::bigint)
) AS fixture (
    source_system,
    source_record_id,
    transaction_id,
    amount,
    currency,
    status,
    is_deleted,
    kafka_offset
);

WITH inserted_run AS (
    INSERT INTO reconciliation.runs (
        left_source_system,
        right_source_system,
        started_at,
        completed_at,
        status,
        total_result_count,
        match_count
    ) VALUES (
        'fixture-left',
        'fixture-right',
        clock_timestamp() - interval '1 minute',
        clock_timestamp(),
        'SUCCEEDED',
        1,
        1
    )
    RETURNING run_id
)
INSERT INTO reconciliation.results (
    run_id,
    left_source_system,
    right_source_system,
    transaction_id,
    currency,
    result_type,
    left_record_count,
    right_record_count,
    left_amount,
    right_amount,
    left_status,
    right_status,
    left_evidence,
    right_evidence
)
SELECT
    run_id,
    'fixture-left',
    'fixture-right',
    'tx-anomaly',
    'USD',
    'MATCH',
    1,
    1,
    10.00,
    11.00,
    'POSTED',
    'POSTED',
    '[]'::jsonb,
    '[]'::jsonb
FROM inserted_run;

INSERT INTO ingestion.raw_cdc_events (
    kafka_topic,
    kafka_partition,
    kafka_offset,
    consumer_group,
    source_database,
    source_schema,
    source_table,
    operation,
    event_timestamp,
    source_metadata,
    payload,
    ingested_at
) VALUES (
    'quality.fixture',
    0,
    5,
    'quality-fixture',
    'quality_fixture',
    'public',
    'ledger_entries',
    'INSERT',
    clock_timestamp() - interval '10 minutes',
    '{}'::jsonb,
    '{}'::jsonb,
    clock_timestamp() - interval '10 minutes'
);

INSERT INTO reconciliation.cdc_checkpoints (
    kafka_topic,
    kafka_partition,
    last_kafka_offset
) VALUES ('quality.fixture', 0, 4);

INSERT INTO ingestion.cdc_dead_letters (
    kafka_topic,
    kafka_partition,
    kafka_offset,
    consumer_group,
    error_class,
    failure_reason
) VALUES (
    'quality.fixture.dlq',
    0,
    1,
    'quality-fixture',
    'FixtureError',
    'quality fixture'
);

INSERT INTO quality_test_executions
SELECT 'fail', quality.run_checks();

DO $$
DECLARE
    fail_time timestamptz;
    mismatch_count integer;
BEGIN
    SELECT executed_at INTO fail_time
    FROM quality_test_executions
    WHERE scenario = 'fail';

    WITH expected(check_name, affected_row_count) AS (
        VALUES
            ('duplicate_source_records'::text, 2::bigint),
            ('missing_required_transaction_fields', 1),
            ('invalid_non_positive_amounts', 1),
            ('unsupported_invalid_currency', 1),
            ('reconciliation_result_anomalies', 1),
            ('stale_unprocessed_cdc_events', 1)
    ),
    actual AS (
        SELECT check_name, affected_row_count
        FROM quality.check_results
        WHERE executed_at = fail_time
          AND status = 'FAIL'
    ),
    differences AS (
        (SELECT * FROM expected EXCEPT SELECT * FROM actual)
        UNION ALL
        (SELECT * FROM actual EXCEPT SELECT * FROM expected)
    )
    SELECT count(*) INTO mismatch_count FROM differences;

    IF mismatch_count <> 0 THEN
        RAISE EXCEPTION 'Quality failure results did not match the expected fixture.';
    END IF;

    IF (SELECT count(DISTINCT executed_at) FROM quality.check_results) <> 2 THEN
        RAISE EXCEPTION 'Rerun did not retain two distinct check executions.';
    END IF;
END;
$$;

DO $$
DECLARE
    metric record;
    latest_run bigint;
BEGIN
    SELECT * INTO metric FROM quality.reliability_metrics;
    SELECT max(run_id) INTO latest_run FROM reconciliation.runs;

    IF metric.raw_cdc_event_count <> 1
       OR metric.dlq_count <> 1
       OR metric.current_ledger_record_count <> 6
       OR metric.deleted_record_count <> 1
       OR metric.latest_reconciliation_run_id <> latest_run
       OR metric.latest_reconciliation_run_status <> 'SUCCEEDED' THEN
        RAISE EXCEPTION 'Reliability summary did not match the deterministic fixture.';
    END IF;

    IF (
        SELECT result_count
        FROM quality.reconciliation_result_counts
        WHERE result_type = 'MATCH'
    ) <> 1 THEN
        RAISE EXCEPTION 'MATCH metric did not equal one.';
    END IF;

    IF (
        SELECT lag
        FROM quality.cdc_checkpoint_lag
        WHERE kafka_topic = 'quality.fixture'
          AND kafka_partition = 0
    ) <> 1 THEN
        RAISE EXCEPTION 'CDC checkpoint lag metric did not equal one.';
    END IF;
END;
$$;

SELECT
    result.check_name,
    result.status,
    result.affected_row_count
FROM quality.check_results AS result
JOIN quality_test_executions AS execution
  ON execution.executed_at = result.executed_at
WHERE execution.scenario = 'fail'
ORDER BY result.check_name;

SELECT * FROM quality.reliability_metrics;
SELECT * FROM quality.reconciliation_result_counts ORDER BY result_type;
SELECT * FROM quality.cdc_checkpoint_lag ORDER BY kafka_topic, kafka_partition;

ROLLBACK;
