\set ON_ERROR_STOP on

BEGIN;

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
    'USD',
    fixture.status,
    false,
    '2026-02-01 00:00:00+00'::timestamptz
        + fixture.kafka_offset * interval '1 second',
    'history_test',
    'public',
    'ledger_entries',
    'test.reconciliation.history',
    0,
    fixture.kafka_offset,
    fixture.kafka_offset::text,
    '2026-02-01 00:00:00+00'::timestamptz
        + fixture.kafka_offset * interval '1 second',
    jsonb_build_object(
        'source_system', fixture.source_system,
        'source_record_id', fixture.source_record_id,
        'transaction_id', fixture.transaction_id,
        'amount', fixture.amount,
        'currency', 'USD',
        'status', fixture.status
    )
FROM (
    VALUES
        ('history-left',  'left-match',      'tx-match',     10.00::numeric, 'SETTLED', 1::bigint),
        ('history-right', 'right-match',     'tx-match',     10.00::numeric, 'SETTLED', 2::bigint),
        ('history-left',  'left-missing',    'tx-missing',   20.00::numeric, 'SETTLED', 3::bigint),
        ('history-left',  'left-amount',     'tx-amount',    30.00::numeric, 'SETTLED', 4::bigint),
        ('history-right', 'right-amount',    'tx-amount',    31.00::numeric, 'SETTLED', 5::bigint),
        ('history-left',  'left-status',     'tx-status',    40.00::numeric, 'SETTLED', 6::bigint),
        ('history-right', 'right-status',    'tx-status',    40.00::numeric, 'PENDING', 7::bigint),
        ('history-left',  'left-duplicate-a','tx-duplicate', 50.00::numeric, 'SETTLED', 8::bigint),
        ('history-left',  'left-duplicate-b','tx-duplicate', 50.00::numeric, 'SETTLED', 9::bigint),
        ('history-right', 'right-duplicate', 'tx-duplicate', 50.00::numeric, 'SETTLED', 10::bigint)
) AS fixture(
    source_system,
    source_record_id,
    transaction_id,
    amount,
    status,
    kafka_offset
);

CREATE TEMPORARY TABLE history_test_runs (
    run_name text PRIMARY KEY,
    run_id bigint NOT NULL
) ON COMMIT DROP;

DO $$
DECLARE
    returned_count bigint;
    first_run_id bigint;
BEGIN
    returned_count := reconciliation.run_reconciliation(
        'history-left',
        'history-right'
    );
    IF returned_count <> 5 THEN
        RAISE EXCEPTION 'first run expected 5 results, found %', returned_count;
    END IF;

    SELECT run_id INTO first_run_id
    FROM reconciliation.runs
    WHERE left_source_system = 'history-left'
      AND right_source_system = 'history-right'
    ORDER BY run_id DESC
    LIMIT 1;

    INSERT INTO history_test_runs VALUES ('first', first_run_id);

    IF (
        SELECT count(*) FROM reconciliation.runs
        WHERE left_source_system = 'history-left'
          AND right_source_system = 'history-right'
    ) <> 1 THEN
        RAISE EXCEPTION 'successful execution did not create exactly one run';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.runs
        WHERE run_id = first_run_id
          AND status = 'SUCCEEDED'
          AND started_at IS NOT NULL
          AND completed_at IS NOT NULL
          AND total_result_count = 5
          AND match_count = 1
          AND missing_record_count = 1
          AND amount_mismatch_count = 1
          AND status_mismatch_count = 1
          AND duplicate_record_count = 1
    ) THEN
        RAISE EXCEPTION 'stored run counts do not match classifications';
    END IF;

    IF (
        SELECT count(*) FROM reconciliation.results
        WHERE run_id = first_run_id
          AND left_source_system = 'history-left'
          AND right_source_system = 'history-right'
    ) <> 5 THEN
        RAISE EXCEPTION 'results are not associated with the first run_id';
    END IF;
END;
$$;

DO $$
DECLARE
    returned_count bigint;
    first_run_id bigint;
    second_run_id bigint;
BEGIN
    SELECT run_id INTO first_run_id
    FROM history_test_runs WHERE run_name = 'first';

    returned_count := reconciliation.run_reconciliation(
        'history-left',
        'history-right'
    );
    IF returned_count <> 5 THEN
        RAISE EXCEPTION 'second run expected 5 results, found %', returned_count;
    END IF;

    SELECT run_id INTO second_run_id
    FROM reconciliation.runs
    WHERE left_source_system = 'history-left'
      AND right_source_system = 'history-right'
    ORDER BY run_id DESC
    LIMIT 1;

    INSERT INTO history_test_runs VALUES ('second', second_run_id);

    IF second_run_id = first_run_id OR (
        SELECT count(*) FROM reconciliation.runs
        WHERE left_source_system = 'history-left'
          AND right_source_system = 'history-right'
    ) <> 2 THEN
        RAISE EXCEPTION 'repeat execution did not create a distinct run';
    END IF;

    IF (
        SELECT count(*) FROM reconciliation.results
        WHERE left_source_system = 'history-left'
          AND right_source_system = 'history-right'
    ) <> 5 OR EXISTS (
        SELECT transaction_id, currency
        FROM reconciliation.results
        WHERE left_source_system = 'history-left'
          AND right_source_system = 'history-right'
        GROUP BY transaction_id, currency
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'repeat execution created duplicate current results';
    END IF;

    IF EXISTS (
        SELECT 1 FROM reconciliation.results
        WHERE left_source_system = 'history-left'
          AND right_source_system = 'history-right'
          AND run_id <> second_run_id
    ) THEN
        RAISE EXCEPTION 'current results were not replaced with the second run_id';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION reconciliation.history_test_force_failure()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.left_source_system = 'history-left'
       AND NEW.right_source_system = 'history-right' THEN
        RAISE EXCEPTION 'forced reconciliation history test failure';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER history_test_force_failure
BEFORE INSERT ON reconciliation.results
FOR EACH ROW
EXECUTE FUNCTION reconciliation.history_test_force_failure();

DO $$
DECLARE
    returned_count bigint;
    second_run_id bigint;
    failed_run_id bigint;
BEGIN
    SELECT run_id INTO second_run_id
    FROM history_test_runs WHERE run_name = 'second';

    returned_count := reconciliation.run_reconciliation(
        'history-left',
        'history-right'
    );
    IF returned_count <> -1 THEN
        RAISE EXCEPTION 'failed run expected -1, found %', returned_count;
    END IF;

    SELECT run_id INTO failed_run_id
    FROM reconciliation.runs
    WHERE left_source_system = 'history-left'
      AND right_source_system = 'history-right'
    ORDER BY run_id DESC
    LIMIT 1;

    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.runs
        WHERE run_id = failed_run_id
          AND status = 'FAILED'
          AND completed_at IS NOT NULL
          AND total_result_count = 0
          AND match_count = 0
          AND missing_record_count = 0
          AND amount_mismatch_count = 0
          AND status_mismatch_count = 0
          AND duplicate_record_count = 0
          AND error_message = 'forced reconciliation history test failure'
    ) THEN
        RAISE EXCEPTION 'failed run was not recorded correctly';
    END IF;

    IF (
        SELECT count(*) FROM reconciliation.results
        WHERE left_source_system = 'history-left'
          AND right_source_system = 'history-right'
          AND run_id = second_run_id
    ) <> 5 OR EXISTS (
        SELECT 1 FROM reconciliation.results
        WHERE run_id = failed_run_id
    ) THEN
        RAISE EXCEPTION 'failed run left partial results or lost prior results';
    END IF;
END;
$$;

DROP TRIGGER history_test_force_failure ON reconciliation.results;
DROP FUNCTION reconciliation.history_test_force_failure();

SELECT
    run_id,
    status,
    total_result_count,
    match_count,
    missing_record_count,
    amount_mismatch_count,
    status_mismatch_count,
    duplicate_record_count
FROM reconciliation.runs
WHERE left_source_system = 'history-left'
  AND right_source_system = 'history-right'
ORDER BY run_id;

ROLLBACK;
