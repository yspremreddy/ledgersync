\set ON_ERROR_STOP on

BEGIN;

-- Establish a known checkpoint baseline without changing durable state after
-- the test transaction rolls back.
INSERT INTO reconciliation.cdc_checkpoints (
    kafka_topic,
    kafka_partition,
    last_kafka_offset
)
SELECT kafka_topic, kafka_partition, max(kafka_offset)
FROM ingestion.raw_cdc_events
GROUP BY kafka_topic, kafka_partition
ON CONFLICT (kafka_topic, kafka_partition) DO UPDATE
SET last_kafka_offset = EXCLUDED.last_kafka_offset;

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
    record_key,
    before_payload,
    after_payload,
    payload
) VALUES (
    'test.incremental.reconciliation',
    0,
    0,
    'incremental-test',
    'test_source',
    'public',
    'ledger_entries',
    'INSERT',
    '2026-01-01 00:00:01+00',
    '2026-01-01 00:00:01+00',
    '1',
    '{"test_fixture":true}',
    '{"source_system":"incremental-left","source_record_id":"left-1"}',
    'null',
    '{"source_system":"incremental-left","source_record_id":"left-1","transaction_id":"tx-lifecycle","amount":"10.0000","currency":"USD","status":"PENDING","updated_at":"2026-01-01T00:00:01Z"}',
    '{"test_fixture":true}'
);

DO $$
DECLARE applied bigint;
BEGIN
    applied := reconciliation.process_new_cdc_events();
    IF applied <> 1 THEN
        RAISE EXCEPTION 'INSERT pass expected 1 event, found %', applied;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.ledger_current
        WHERE source_system = 'incremental-left'
          AND source_record_id = 'left-1'
          AND amount = 10.0000
          AND status = 'PENDING'
          AND NOT is_deleted
          AND last_kafka_offset = 0
    ) THEN
        RAISE EXCEPTION 'incremental INSERT was not applied';
    END IF;
END;
$$;

INSERT INTO ingestion.raw_cdc_events (
    kafka_topic, kafka_partition, kafka_offset, consumer_group,
    source_database, source_schema, source_table, operation,
    event_timestamp, source_timestamp, source_lsn, source_metadata,
    record_key, before_payload, after_payload, payload
) VALUES (
    'test.incremental.reconciliation', 0, 1, 'incremental-test',
    'test_source', 'public', 'ledger_entries', 'UPDATE',
    '2026-01-01 00:00:02+00', '2026-01-01 00:00:02+00', '2',
    '{"test_fixture":true}',
    '{"source_system":"incremental-left","source_record_id":"left-1"}',
    '{"source_system":"incremental-left","source_record_id":"left-1","transaction_id":"tx-lifecycle","amount":"10.0000","currency":"USD","status":"PENDING","updated_at":"2026-01-01T00:00:01Z"}',
    '{"source_system":"incremental-left","source_record_id":"left-1","transaction_id":"tx-lifecycle","amount":"12.5000","currency":"USD","status":"POSTED","updated_at":"2026-01-01T00:00:02Z"}',
    '{"test_fixture":true}'
);

DO $$
DECLARE applied bigint;
BEGIN
    applied := reconciliation.process_new_cdc_events();
    IF applied <> 1 THEN
        RAISE EXCEPTION 'UPDATE pass expected 1 event, found %', applied;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.ledger_current
        WHERE source_system = 'incremental-left'
          AND source_record_id = 'left-1'
          AND amount = 12.5000
          AND status = 'POSTED'
          AND NOT is_deleted
          AND last_kafka_offset = 1
    ) THEN
        RAISE EXCEPTION 'incremental UPDATE was not applied';
    END IF;
END;
$$;

INSERT INTO ingestion.raw_cdc_events (
    kafka_topic, kafka_partition, kafka_offset, consumer_group,
    source_database, source_schema, source_table, operation,
    event_timestamp, source_timestamp, source_lsn, source_metadata,
    record_key, before_payload, after_payload, payload
) VALUES (
    'test.incremental.reconciliation', 0, 2, 'incremental-test',
    'test_source', 'public', 'ledger_entries', 'DELETE',
    '2026-01-01 00:00:03+00', '2026-01-01 00:00:03+00', '3',
    '{"test_fixture":true}',
    '{"source_system":"incremental-left","source_record_id":"left-1"}',
    '{"source_system":"incremental-left","source_record_id":"left-1","transaction_id":"tx-lifecycle","amount":"12.5000","currency":"USD","status":"POSTED","updated_at":"2026-01-01T00:00:02Z"}',
    'null',
    '{"test_fixture":true}'
);

DO $$
DECLARE applied bigint;
BEGIN
    applied := reconciliation.process_new_cdc_events();
    IF applied <> 1 THEN
        RAISE EXCEPTION 'DELETE pass expected 1 event, found %', applied;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.ledger_current
        WHERE source_system = 'incremental-left'
          AND source_record_id = 'left-1'
          AND is_deleted
          AND amount = 12.5000
          AND last_kafka_offset = 2
          AND last_source_lsn = '3'
    ) THEN
        RAISE EXCEPTION 'incremental DELETE was not applied';
    END IF;

    applied := reconciliation.process_new_cdc_events();
    IF applied <> 0 THEN
        RAISE EXCEPTION 'idempotent rerun expected 0 events, found %', applied;
    END IF;
END;
$$;

-- Add exactly one event after the processor has caught up.
INSERT INTO ingestion.raw_cdc_events (
    kafka_topic, kafka_partition, kafka_offset, consumer_group,
    source_database, source_schema, source_table, operation,
    event_timestamp, source_timestamp, source_lsn, source_metadata,
    record_key, before_payload, after_payload, payload
) VALUES (
    'test.incremental.reconciliation', 0, 3, 'incremental-test',
    'test_source', 'public', 'ledger_entries', 'INSERT',
    '2026-01-01 00:00:04+00', '2026-01-01 00:00:04+00', '4',
    '{"test_fixture":true}',
    '{"source_system":"incremental-right","source_record_id":"right-1"}',
    'null',
    '{"source_system":"incremental-right","source_record_id":"right-1","transaction_id":"tx-lifecycle","amount":"12.5000","currency":"USD","status":"POSTED","updated_at":"2026-01-01T00:00:04Z"}',
    '{"test_fixture":true}'
);

DO $$
DECLARE applied bigint;
BEGIN
    applied := reconciliation.process_new_cdc_events();
    IF applied <> 1 THEN
        RAISE EXCEPTION 'new-event pass expected 1 event, found %', applied;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.cdc_checkpoints
        WHERE kafka_topic = 'test.incremental.reconciliation'
          AND kafka_partition = 0
          AND last_kafka_offset = 3
    ) THEN
        RAISE EXCEPTION 'checkpoint did not advance to offset 3';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.ledger_current
        WHERE source_system = 'incremental-left'
          AND source_record_id = 'left-1'
          AND is_deleted
          AND last_kafka_offset = 2
    ) OR NOT EXISTS (
        SELECT 1 FROM reconciliation.ledger_current
        WHERE source_system = 'incremental-right'
          AND source_record_id = 'right-1'
          AND NOT is_deleted
          AND last_kafka_offset = 3
    ) THEN
        RAISE EXCEPTION 'only-new-event assertion failed';
    END IF;
END;
$$;

SELECT reconciliation.run_reconciliation(
    'incremental-left',
    'incremental-right'
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM reconciliation.results
        WHERE left_source_system = 'incremental-left'
          AND right_source_system = 'incremental-right'
          AND transaction_id = 'tx-lifecycle'
          AND result_type = 'MISSING_RECORD'
          AND left_record_count = 0
          AND right_record_count = 1
    ) THEN
        RAISE EXCEPTION 'existing reconciliation behavior changed';
    END IF;

    IF (
        SELECT count(*) FROM ingestion.raw_cdc_events
        WHERE kafka_topic = 'test.incremental.reconciliation'
    ) <> 4 THEN
        RAISE EXCEPTION 'raw CDC history was modified unexpectedly';
    END IF;
END;
$$;

SELECT source_system, source_record_id, is_deleted, last_kafka_offset
FROM reconciliation.ledger_current
WHERE source_system IN ('incremental-left', 'incremental-right')
ORDER BY source_system;

ROLLBACK;
