\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS reconciliation AUTHORIZATION ledgersync_admin;

CREATE TABLE IF NOT EXISTS reconciliation.ledger_current (
    source_system text NOT NULL,
    source_record_id text NOT NULL,
    transaction_id text NOT NULL,
    amount numeric(20, 4),
    currency text,
    status text,
    is_deleted boolean NOT NULL,
    source_event_timestamp timestamptz NOT NULL,
    source_database text NOT NULL,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    last_kafka_topic text NOT NULL,
    last_kafka_partition integer NOT NULL,
    last_kafka_offset bigint NOT NULL,
    last_source_lsn text,
    last_cdc_event_timestamp timestamptz NOT NULL,
    source_payload jsonb NOT NULL,
    refreshed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_system, source_record_id),
    CHECK (source_system <> ''),
    CHECK (source_record_id <> ''),
    CHECK (transaction_id <> ''),
    CHECK (currency IS NULL OR currency ~ '^[A-Z]{3}$'),
    CHECK (is_deleted OR (amount IS NOT NULL AND currency IS NOT NULL AND status IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS ledger_current_match_idx
    ON reconciliation.ledger_current
    (source_system, transaction_id, currency)
    WHERE NOT is_deleted;

CREATE TABLE IF NOT EXISTS reconciliation.cdc_checkpoints (
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL,
    last_kafka_offset bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (kafka_topic, kafka_partition)
);

CREATE TABLE IF NOT EXISTS reconciliation.runs (
    run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    left_source_system text NOT NULL,
    right_source_system text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    total_result_count integer NOT NULL DEFAULT 0,
    match_count integer NOT NULL DEFAULT 0,
    missing_record_count integer NOT NULL DEFAULT 0,
    amount_mismatch_count integer NOT NULL DEFAULT 0,
    status_mismatch_count integer NOT NULL DEFAULT 0,
    duplicate_record_count integer NOT NULL DEFAULT 0,
    error_message text,
    CHECK (left_source_system <> ''),
    CHECK (right_source_system <> ''),
    CHECK (left_source_system <> right_source_system),
    CHECK (
        (status = 'RUNNING' AND completed_at IS NULL)
        OR (status IN ('SUCCEEDED', 'FAILED') AND completed_at IS NOT NULL)
    ),
    CHECK (
        total_result_count = match_count
            + missing_record_count
            + amount_mismatch_count
            + status_mismatch_count
            + duplicate_record_count
    )
);

CREATE INDEX IF NOT EXISTS reconciliation_runs_source_time_idx
    ON reconciliation.runs
    (left_source_system, right_source_system, started_at DESC);

CREATE TABLE IF NOT EXISTS reconciliation.results (
    run_id bigint NOT NULL REFERENCES reconciliation.runs (run_id),
    left_source_system text NOT NULL,
    right_source_system text NOT NULL,
    transaction_id text NOT NULL,
    currency text NOT NULL,
    result_type text NOT NULL CHECK (
        result_type IN (
            'MATCH',
            'MISSING_RECORD',
            'AMOUNT_MISMATCH',
            'STATUS_MISMATCH',
            'DUPLICATE_RECORD'
        )
    ),
    left_record_count integer NOT NULL,
    right_record_count integer NOT NULL,
    left_amount numeric(20, 4),
    right_amount numeric(20, 4),
    left_status text,
    right_status text,
    left_evidence jsonb NOT NULL,
    right_evidence jsonb NOT NULL,
    reconciled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        left_source_system,
        right_source_system,
        transaction_id,
        currency
    )
);

CREATE INDEX IF NOT EXISTS reconciliation_results_type_idx
    ON reconciliation.results (result_type, reconciled_at);

-- Upgrade existing current results by recording one successful baseline run
-- per source pair before making run_id mandatory.
ALTER TABLE reconciliation.results
    ADD COLUMN IF NOT EXISTS run_id bigint;

DO $$
DECLARE
    source_pair record;
    baseline_run_id bigint;
BEGIN
    FOR source_pair IN
        SELECT DISTINCT left_source_system, right_source_system
        FROM reconciliation.results
        WHERE run_id IS NULL
    LOOP
        INSERT INTO reconciliation.runs (
            left_source_system,
            right_source_system,
            started_at,
            completed_at,
            status,
            total_result_count,
            match_count,
            missing_record_count,
            amount_mismatch_count,
            status_mismatch_count,
            duplicate_record_count
        )
        SELECT
            source_pair.left_source_system,
            source_pair.right_source_system,
            min(reconciled_at),
            max(reconciled_at),
            'SUCCEEDED',
            count(*)::integer,
            count(*) FILTER (WHERE result_type = 'MATCH')::integer,
            count(*) FILTER (WHERE result_type = 'MISSING_RECORD')::integer,
            count(*) FILTER (WHERE result_type = 'AMOUNT_MISMATCH')::integer,
            count(*) FILTER (WHERE result_type = 'STATUS_MISMATCH')::integer,
            count(*) FILTER (WHERE result_type = 'DUPLICATE_RECORD')::integer
        FROM reconciliation.results
        WHERE left_source_system = source_pair.left_source_system
          AND right_source_system = source_pair.right_source_system
        RETURNING run_id INTO baseline_run_id;

        UPDATE reconciliation.results
        SET run_id = baseline_run_id
        WHERE left_source_system = source_pair.left_source_system
          AND right_source_system = source_pair.right_source_system
          AND run_id IS NULL;
    END LOOP;
END;
$$;

ALTER TABLE reconciliation.results
    ALTER COLUMN run_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'reconciliation.results'::regclass
          AND conname = 'results_run_id_fkey'
    ) THEN
        ALTER TABLE reconciliation.results
            ADD CONSTRAINT results_run_id_fkey
            FOREIGN KEY (run_id) REFERENCES reconciliation.runs (run_id);
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION reconciliation.process_new_cdc_events()
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    processed_count bigint;
BEGIN
    -- Serialize processors. Current-state changes and checkpoint advancement
    -- commit together, so retries after a rollback safely see the same events.
    PERFORM pg_advisory_xact_lock(hashtext('ledgersync.reconciliation.cdc'));

    WITH new_events AS MATERIALIZED (
        SELECT raw.*
        FROM ingestion.raw_cdc_events AS raw
        LEFT JOIN reconciliation.cdc_checkpoints AS checkpoint
          ON checkpoint.kafka_topic = raw.kafka_topic
         AND checkpoint.kafka_partition = raw.kafka_partition
        WHERE raw.kafka_offset > COALESCE(checkpoint.last_kafka_offset, -1)
    ),
    candidate_events AS (
        SELECT
            raw.*,
            CASE
                WHEN jsonb_typeof(raw.record_key) = 'object'
                    THEN raw.record_key
                ELSE '{}'::jsonb
            END
            || CASE
                WHEN jsonb_typeof(raw.after_payload) = 'object'
                    THEN raw.after_payload
                WHEN jsonb_typeof(raw.before_payload) = 'object'
                    THEN raw.before_payload
                ELSE '{}'::jsonb
            END
                AS record_data
        FROM new_events AS raw
    ),
    canonical_events AS (
        SELECT
            candidate.*,
            candidate.record_data->>'source_system' AS normalized_source_system,
            candidate.record_data->>'source_record_id' AS normalized_source_record_id,
            candidate.record_data->>'transaction_id' AS normalized_transaction_id,
            NULLIF(candidate.record_data->>'amount', '')::numeric(20, 4)
                AS normalized_amount,
            upper(NULLIF(candidate.record_data->>'currency', '')) AS normalized_currency,
            upper(NULLIF(candidate.record_data->>'status', '')) AS normalized_status
        FROM candidate_events AS candidate
        WHERE candidate.record_data ?& ARRAY[
            'source_system',
            'source_record_id',
            'transaction_id'
        ]
        AND (
            candidate.operation = 'DELETE'
            OR candidate.record_data ?& ARRAY['amount', 'currency', 'status']
        )
    ),
    latest_events AS (
        SELECT
            canonical.*,
            row_number() OVER (
                PARTITION BY
                    canonical.normalized_source_system,
                    canonical.normalized_source_record_id
                ORDER BY
                    canonical.event_timestamp DESC,
                    canonical.kafka_topic DESC,
                    canonical.kafka_partition DESC,
                    canonical.kafka_offset DESC
            ) AS event_rank
        FROM canonical_events AS canonical
        WHERE canonical.normalized_source_system <> ''
          AND canonical.normalized_source_record_id <> ''
          AND canonical.normalized_transaction_id <> ''
    ),
    applied_events AS (
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
            latest.normalized_source_system,
            latest.normalized_source_record_id,
            latest.normalized_transaction_id,
            latest.normalized_amount,
            latest.normalized_currency,
            latest.normalized_status,
            latest.operation = 'DELETE',
            COALESCE(
                NULLIF(latest.record_data->>'updated_at', '')::timestamptz,
                latest.source_timestamp,
                latest.event_timestamp
            ),
            latest.source_database,
            latest.source_schema,
            latest.source_table,
            latest.kafka_topic,
            latest.kafka_partition,
            latest.kafka_offset,
            latest.source_lsn,
            latest.event_timestamp,
            latest.record_data
        FROM latest_events AS latest
        WHERE latest.event_rank = 1
        ON CONFLICT (source_system, source_record_id) DO UPDATE
        SET
            transaction_id = EXCLUDED.transaction_id,
            amount = EXCLUDED.amount,
            currency = EXCLUDED.currency,
            status = EXCLUDED.status,
            is_deleted = EXCLUDED.is_deleted,
            source_event_timestamp = EXCLUDED.source_event_timestamp,
            source_database = EXCLUDED.source_database,
            source_schema = EXCLUDED.source_schema,
            source_table = EXCLUDED.source_table,
            last_kafka_topic = EXCLUDED.last_kafka_topic,
            last_kafka_partition = EXCLUDED.last_kafka_partition,
            last_kafka_offset = EXCLUDED.last_kafka_offset,
            last_source_lsn = EXCLUDED.last_source_lsn,
            last_cdc_event_timestamp = EXCLUDED.last_cdc_event_timestamp,
            source_payload = EXCLUDED.source_payload,
            refreshed_at = clock_timestamp()
        WHERE (
            reconciliation.ledger_current.last_cdc_event_timestamp,
            reconciliation.ledger_current.last_kafka_topic,
            reconciliation.ledger_current.last_kafka_partition,
            reconciliation.ledger_current.last_kafka_offset
        ) < (
            EXCLUDED.last_cdc_event_timestamp,
            EXCLUDED.last_kafka_topic,
            EXCLUDED.last_kafka_partition,
            EXCLUDED.last_kafka_offset
        )
        RETURNING 1
    ),
    advanced_checkpoints AS (
        INSERT INTO reconciliation.cdc_checkpoints (
            kafka_topic,
            kafka_partition,
            last_kafka_offset,
            updated_at
        )
        SELECT
            kafka_topic,
            kafka_partition,
            max(kafka_offset),
            clock_timestamp()
        FROM new_events
        GROUP BY kafka_topic, kafka_partition
        ON CONFLICT (kafka_topic, kafka_partition) DO UPDATE
        SET
            last_kafka_offset = GREATEST(
                reconciliation.cdc_checkpoints.last_kafka_offset,
                EXCLUDED.last_kafka_offset
            ),
            updated_at = clock_timestamp()
        RETURNING 1
    )
    SELECT count(*) INTO processed_count
    FROM new_events;

    RETURN processed_count;
END;
$$;

-- Backward-compatible entry point: refresh now means apply only unseen CDC.
CREATE OR REPLACE FUNCTION reconciliation.refresh_ledger_current()
RETURNS bigint
LANGUAGE sql
AS $$
    SELECT reconciliation.process_new_cdc_events();
$$;

CREATE OR REPLACE FUNCTION reconciliation.run_reconciliation(
    requested_left_source_system text,
    requested_right_source_system text
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    result_count bigint;
    reconciliation_run_id bigint;
    match_results integer;
    missing_results integer;
    amount_mismatch_results integer;
    status_mismatch_results integer;
    duplicate_results integer;
    failure_message text;
BEGIN
    IF requested_left_source_system IS NULL
       OR requested_right_source_system IS NULL
       OR requested_left_source_system = ''
       OR requested_right_source_system = ''
       OR requested_left_source_system = requested_right_source_system THEN
        RAISE EXCEPTION 'Reconciliation requires two distinct, non-empty source systems.';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtext(
            'ledgersync.reconciliation.run:'
            || requested_left_source_system
            || ':'
            || requested_right_source_system
        )
    );

    INSERT INTO reconciliation.runs (
        left_source_system,
        right_source_system,
        status
    ) VALUES (
        requested_left_source_system,
        requested_right_source_system,
        'RUNNING'
    )
    RETURNING run_id INTO reconciliation_run_id;

    BEGIN
        DELETE FROM reconciliation.results
        WHERE left_source_system = requested_left_source_system
          AND right_source_system = requested_right_source_system;

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
    WITH active_records AS (
        SELECT *
        FROM reconciliation.ledger_current
        WHERE NOT is_deleted
          AND source_system IN (
              requested_left_source_system,
              requested_right_source_system
          )
    ),
    matching_keys AS (
        SELECT DISTINCT transaction_id, currency
        FROM active_records
    ),
    summaries AS (
        SELECT
            key.transaction_id,
            key.currency,
            count(*) FILTER (
                WHERE record.source_system = requested_left_source_system
            )::integer AS left_record_count,
            count(*) FILTER (
                WHERE record.source_system = requested_right_source_system
            )::integer AS right_record_count,
            max(record.amount) FILTER (
                WHERE record.source_system = requested_left_source_system
            ) AS left_amount,
            max(record.amount) FILTER (
                WHERE record.source_system = requested_right_source_system
            ) AS right_amount,
            max(record.status) FILTER (
                WHERE record.source_system = requested_left_source_system
            ) AS left_status,
            max(record.status) FILTER (
                WHERE record.source_system = requested_right_source_system
            ) AS right_status,
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'source_record_id', record.source_record_id,
                        'amount', record.amount,
                        'currency', record.currency,
                        'status', record.status,
                        'source_event_timestamp', record.source_event_timestamp,
                        'kafka_topic', record.last_kafka_topic,
                        'kafka_partition', record.last_kafka_partition,
                        'kafka_offset', record.last_kafka_offset,
                        'source_lsn', record.last_source_lsn,
                        'source_payload', record.source_payload
                    ) ORDER BY record.source_record_id
                ) FILTER (
                    WHERE record.source_system = requested_left_source_system
                ),
                '[]'::jsonb
            ) AS left_evidence,
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'source_record_id', record.source_record_id,
                        'amount', record.amount,
                        'currency', record.currency,
                        'status', record.status,
                        'source_event_timestamp', record.source_event_timestamp,
                        'kafka_topic', record.last_kafka_topic,
                        'kafka_partition', record.last_kafka_partition,
                        'kafka_offset', record.last_kafka_offset,
                        'source_lsn', record.last_source_lsn,
                        'source_payload', record.source_payload
                    ) ORDER BY record.source_record_id
                ) FILTER (
                    WHERE record.source_system = requested_right_source_system
                ),
                '[]'::jsonb
            ) AS right_evidence
        FROM matching_keys AS key
        JOIN active_records AS record
          ON record.transaction_id = key.transaction_id
         AND record.currency = key.currency
        GROUP BY key.transaction_id, key.currency
    )
        SELECT
        reconciliation_run_id,
        requested_left_source_system,
        requested_right_source_system,
        summary.transaction_id,
        summary.currency,
        CASE
            WHEN summary.left_record_count > 1
              OR summary.right_record_count > 1
                THEN 'DUPLICATE_RECORD'
            WHEN summary.left_record_count = 0
              OR summary.right_record_count = 0
                THEN 'MISSING_RECORD'
            WHEN summary.left_amount <> summary.right_amount
                THEN 'AMOUNT_MISMATCH'
            WHEN summary.left_status <> summary.right_status
                THEN 'STATUS_MISMATCH'
            ELSE 'MATCH'
        END,
        summary.left_record_count,
        summary.right_record_count,
        summary.left_amount,
        summary.right_amount,
        summary.left_status,
        summary.right_status,
        summary.left_evidence,
        summary.right_evidence
        FROM summaries AS summary;

        GET DIAGNOSTICS result_count = ROW_COUNT;

        SELECT
            count(*) FILTER (WHERE result_type = 'MATCH')::integer,
            count(*) FILTER (WHERE result_type = 'MISSING_RECORD')::integer,
            count(*) FILTER (WHERE result_type = 'AMOUNT_MISMATCH')::integer,
            count(*) FILTER (WHERE result_type = 'STATUS_MISMATCH')::integer,
            count(*) FILTER (WHERE result_type = 'DUPLICATE_RECORD')::integer
        INTO
            match_results,
            missing_results,
            amount_mismatch_results,
            status_mismatch_results,
            duplicate_results
        FROM reconciliation.results
        WHERE run_id = reconciliation_run_id;

        UPDATE reconciliation.runs
        SET
            completed_at = clock_timestamp(),
            status = 'SUCCEEDED',
            total_result_count = result_count,
            match_count = match_results,
            missing_record_count = missing_results,
            amount_mismatch_count = amount_mismatch_results,
            status_mismatch_count = status_mismatch_results,
            duplicate_record_count = duplicate_results
        WHERE run_id = reconciliation_run_id;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS failure_message = MESSAGE_TEXT;

        -- The exception block rolls back the attempted result replacement to
        -- its implicit savepoint, preserving the preceding successful result
        -- set. The run row was created outside that savepoint and is retained.
        UPDATE reconciliation.runs
        SET
            completed_at = clock_timestamp(),
            status = 'FAILED',
            error_message = failure_message
        WHERE run_id = reconciliation_run_id;

        RETURN -1;
    END;

    RETURN result_count;
END;
$$;
