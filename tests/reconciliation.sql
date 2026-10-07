\set ON_ERROR_STOP on

BEGIN;

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
    source_timestamp,
    source_lsn,
    source_metadata,
    after_payload,
    payload
)
SELECT
    'test.reconciliation',
    0,
    fixture.kafka_offset,
    'reconciliation-test',
    'test_source',
    'public',
    'ledger_fixture',
    'INSERT',
    '2026-01-01 00:00:00+00'::timestamptz
        + fixture.kafka_offset * interval '1 second',
    '2026-01-01 00:00:00+00'::timestamptz
        + fixture.kafka_offset * interval '1 second',
    fixture.kafka_offset::text,
    jsonb_build_object('test_fixture', true),
    jsonb_build_object(
        'source_system', fixture.source_system,
        'source_record_id', fixture.source_record_id,
        'transaction_id', fixture.transaction_id,
        'amount', fixture.amount,
        'currency', 'USD',
        'status', fixture.status
    ),
    jsonb_build_object('test_fixture', true)
FROM (
    VALUES
        (1001::bigint, 'test-left',  'left-match',     'tx-match',   10.00::numeric, 'SETTLED'),
        (1002::bigint, 'test-right', 'right-match',    'tx-match',   10.00::numeric, 'SETTLED'),
        (1003::bigint, 'test-left',  'left-missing',   'tx-missing', 20.00::numeric, 'SETTLED'),
        (1004::bigint, 'test-left',  'left-amount',    'tx-amount',  30.00::numeric, 'SETTLED'),
        (1005::bigint, 'test-right', 'right-amount',   'tx-amount',  31.00::numeric, 'SETTLED'),
        (1006::bigint, 'test-left',  'left-status',    'tx-status',  40.00::numeric, 'SETTLED'),
        (1007::bigint, 'test-right', 'right-status',   'tx-status',  40.00::numeric, 'PENDING'),
        (1008::bigint, 'test-left',  'left-duplicate-a','tx-duplicate', 50.00::numeric, 'SETTLED'),
        (1009::bigint, 'test-left',  'left-duplicate-b','tx-duplicate', 50.00::numeric, 'SETTLED'),
        (1010::bigint, 'test-right', 'right-duplicate','tx-duplicate', 50.00::numeric, 'SETTLED')
) AS fixture(
    kafka_offset,
    source_system,
    source_record_id,
    transaction_id,
    amount,
    status
);

SELECT reconciliation.refresh_ledger_current();
SELECT reconciliation.run_reconciliation('test-left', 'test-right');

DO $$
DECLARE
    normalized_count integer;
    result_count integer;
BEGIN
    SELECT count(*) INTO normalized_count
    FROM reconciliation.ledger_current
    WHERE source_system IN ('test-left', 'test-right');
    IF normalized_count <> 10 THEN
        RAISE EXCEPTION 'expected 10 normalized records, found %', normalized_count;
    END IF;

    SELECT count(*) INTO result_count
    FROM reconciliation.results
    WHERE left_source_system = 'test-left'
      AND right_source_system = 'test-right';
    IF result_count <> 5 THEN
        RAISE EXCEPTION 'expected 5 reconciliation results, found %', result_count;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.results
        WHERE left_source_system = 'test-left'
          AND right_source_system = 'test-right'
          AND transaction_id = 'tx-match'
          AND result_type = 'MATCH'
    ) THEN
        RAISE EXCEPTION 'MATCH case failed';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.results
        WHERE left_source_system = 'test-left'
          AND right_source_system = 'test-right'
          AND transaction_id = 'tx-missing'
          AND result_type = 'MISSING_RECORD'
          AND left_record_count = 1
          AND right_record_count = 0
    ) THEN
        RAISE EXCEPTION 'MISSING_RECORD case failed';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.results
        WHERE left_source_system = 'test-left'
          AND right_source_system = 'test-right'
          AND transaction_id = 'tx-amount'
          AND result_type = 'AMOUNT_MISMATCH'
          AND left_amount = 30.00
          AND right_amount = 31.00
    ) THEN
        RAISE EXCEPTION 'AMOUNT_MISMATCH case failed';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.results
        WHERE left_source_system = 'test-left'
          AND right_source_system = 'test-right'
          AND transaction_id = 'tx-status'
          AND result_type = 'STATUS_MISMATCH'
          AND left_status = 'SETTLED'
          AND right_status = 'PENDING'
    ) THEN
        RAISE EXCEPTION 'STATUS_MISMATCH case failed';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.results
        WHERE left_source_system = 'test-left'
          AND right_source_system = 'test-right'
          AND transaction_id = 'tx-duplicate'
          AND result_type = 'DUPLICATE_RECORD'
          AND left_record_count = 2
          AND right_record_count = 1
          AND jsonb_array_length(left_evidence) = 2
          AND jsonb_array_length(right_evidence) = 1
    ) THEN
        RAISE EXCEPTION 'DUPLICATE_RECORD case failed';
    END IF;
END;
$$;

-- A second run must replace the same five current results, not append rows.
SELECT reconciliation.refresh_ledger_current();
SELECT reconciliation.run_reconciliation('test-left', 'test-right');

DO $$
DECLARE
    result_count integer;
    duplicate_key_count integer;
BEGIN
    SELECT count(*) INTO result_count
    FROM reconciliation.results
    WHERE left_source_system = 'test-left'
      AND right_source_system = 'test-right';

    SELECT count(*) INTO duplicate_key_count
    FROM (
        SELECT transaction_id, currency
        FROM reconciliation.results
        WHERE left_source_system = 'test-left'
          AND right_source_system = 'test-right'
        GROUP BY transaction_id, currency
        HAVING count(*) > 1
    ) AS duplicate_keys;

    IF result_count <> 5 OR duplicate_key_count <> 0 THEN
        RAISE EXCEPTION
            'rerun was not idempotent: results %, duplicate keys %',
            result_count,
            duplicate_key_count;
    END IF;
END;
$$;

SELECT transaction_id, result_type, left_record_count, right_record_count
FROM reconciliation.results
WHERE left_source_system = 'test-left'
  AND right_source_system = 'test-right'
ORDER BY transaction_id;

ROLLBACK;
