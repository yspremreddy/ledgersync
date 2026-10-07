-- Run as the local administrator in ledger_source. Safe to run repeatedly.
CREATE TABLE IF NOT EXISTS public.cdc_smoke (
    marker text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

GRANT USAGE ON SCHEMA public TO ledger_cdc;
GRANT SELECT ON public.cdc_smoke TO ledger_cdc;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'ledgersync_smoke_publication'
    ) THEN
        CREATE PUBLICATION ledgersync_smoke_publication;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = 'ledgersync_smoke_publication'
          AND schemaname = 'public'
          AND tablename = 'cdc_smoke'
    ) THEN
        ALTER PUBLICATION ledgersync_smoke_publication
            ADD TABLE public.cdc_smoke;
    END IF;
END
$$;
