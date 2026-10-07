\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS public.ledger_entries (
    source_record_id text NOT NULL CHECK (source_record_id <> ''),
    source_system text NOT NULL CHECK (source_system <> ''),
    transaction_id text NOT NULL CHECK (transaction_id <> ''),
    amount numeric(20, 4) NOT NULL,
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status text NOT NULL CHECK (status <> ''),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_system, source_record_id)
);

CREATE INDEX IF NOT EXISTS ledger_entries_match_idx
    ON public.ledger_entries (source_system, transaction_id, currency);

CREATE OR REPLACE FUNCTION public.set_ledger_entry_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS ledger_entries_set_updated_at ON public.ledger_entries;
CREATE TRIGGER ledger_entries_set_updated_at
BEFORE UPDATE ON public.ledger_entries
FOR EACH ROW
EXECUTE FUNCTION public.set_ledger_entry_updated_at();

-- Full row images keep UPDATE/DELETE source evidence complete.
ALTER TABLE public.ledger_entries REPLICA IDENTITY FULL;

GRANT USAGE ON SCHEMA public TO ledger_cdc;
GRANT SELECT ON public.ledger_entries TO ledger_cdc;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication
        WHERE pubname = 'ledgersync_smoke_publication'
    ) THEN
        CREATE PUBLICATION ledgersync_smoke_publication;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'ledgersync_smoke_publication'
          AND schemaname = 'public'
          AND tablename = 'ledger_entries'
    ) THEN
        ALTER PUBLICATION ledgersync_smoke_publication
            ADD TABLE public.ledger_entries;
    END IF;
END
$$;
