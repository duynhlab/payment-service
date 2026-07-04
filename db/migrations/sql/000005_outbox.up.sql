-- V5__outbox.sql
-- Transactional outbox. A domain event is written in the SAME transaction as
-- the state change + ledger posting that produced it, so the event and the
-- money movement commit atomically — no dual-write gap. A relay publishes
-- unpublished rows and stamps published_at (at-least-once delivery).

CREATE TABLE IF NOT EXISTS payment_outbox (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type   TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- The relay only ever scans unpublished rows; a partial index keeps that lookup
-- cheap as the published backlog grows.
CREATE INDEX IF NOT EXISTS idx_payment_outbox_unpublished
    ON payment_outbox (id) WHERE published_at IS NULL;
