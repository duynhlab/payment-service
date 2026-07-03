-- V2__refund_idempotency.sql
-- Make refund creation idempotent at the row level. Without this, a crash
-- after the refund INSERT but before the idempotency key is marked finished
-- lets a takeover re-insert a SECOND refund for the same request (the amount
-- guard only caps the cumulative sum, not duplicate inserts). A partial unique
-- index on the client key dedupes the insert itself — the narrowest window.

ALTER TABLE refunds ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_refunds_idempotency_key
    ON refunds (idempotency_key) WHERE idempotency_key IS NOT NULL;

COMMENT ON COLUMN refunds.idempotency_key IS
    'Client Idempotency-Key (user-scoped) that created this refund; dedupes the insert on crash-recovery retry. NULL for legacy rows.';
