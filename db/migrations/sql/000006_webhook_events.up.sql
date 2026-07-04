-- V6__webhook_events.sql
-- Dedup ledger for inbound provider webhooks. Delivery is at-least-once and out
-- of order, so the event_id primary key makes reprocessing a no-op: the receiver
-- INSERTs ON CONFLICT DO NOTHING and treats a conflict as "already handled".
-- payment_id correlates the event to a local payment (NULL = orphaned: a webhook
-- for a payment this service does not know, parked for reconciliation).

CREATE TABLE IF NOT EXISTS webhook_events (
    event_id            TEXT PRIMARY KEY,
    event_type          TEXT        NOT NULL,
    provider_payment_id TEXT,
    payment_id          BIGINT      REFERENCES payments(id),
    status              TEXT        NOT NULL CHECK (status IN ('processed', 'orphaned')),
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_webhook_events_payment ON webhook_events (payment_id);

-- The receiver correlates every inbound webhook by provider_payment_id, so index
-- it on payments to keep that lookup off a sequential scan in the request path.
CREATE INDEX IF NOT EXISTS idx_payments_provider_payment_id
    ON payments (provider_payment_id) WHERE provider_payment_id IS NOT NULL;
