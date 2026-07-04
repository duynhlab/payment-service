-- Reconciliation persistence: each pass is a `reconciliation_runs` row; every
-- payment<->provider mismatch it finds is a `reconciliation_discrepancies` row.
-- v1 is detect-only — these tables are a record + report surface, never a source
-- of automated money movement.

CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id                   BIGSERIAL PRIMARY KEY,
    status               TEXT NOT NULL DEFAULT 'running'
                         CHECK (status IN ('running', 'completed', 'failed')),
    transactions_scanned INTEGER NOT NULL DEFAULT 0,
    discrepancies_found  INTEGER NOT NULL DEFAULT 0,
    started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at          TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS reconciliation_discrepancies (
    id                    BIGSERIAL PRIMARY KEY,
    run_id                BIGINT NOT NULL REFERENCES reconciliation_runs(id) ON DELETE CASCADE,
    provider_payment_id   TEXT NOT NULL,
    class                 TEXT NOT NULL
                          CHECK (class IN ('missing_internal', 'missing_provider', 'amount_mismatch', 'status_mismatch')),
    internal_amount_minor BIGINT NOT NULL DEFAULT 0,   -- 0 when the internal side is absent
    provider_amount_minor BIGINT NOT NULL DEFAULT 0,   -- 0 when the provider side is absent
    internal_status       TEXT NOT NULL DEFAULT '',
    provider_status       TEXT NOT NULL DEFAULT '',
    detail                TEXT NOT NULL DEFAULT '',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_recon_discrepancies_run ON reconciliation_discrepancies (run_id);

COMMENT ON TABLE reconciliation_runs IS 'One payment<->provider reconciliation pass (detect-only in v1).';
COMMENT ON TABLE reconciliation_discrepancies IS 'Mismatches found in a run: missing_internal, missing_provider, amount_mismatch, status_mismatch.';
