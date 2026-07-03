-- V1__init_schema.sql
-- Core payment tables (RFC-0010 P1): payments, refunds, idempotency_keys.
-- Amounts are ALWAYS integer minor units (2000 = $20.00) — never floats.
-- Ledger + outbox tables arrive in P2.

CREATE TABLE IF NOT EXISTS payments (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id             BIGINT      NOT NULL,
    order_id            BIGINT,
    amount_minor        BIGINT      NOT NULL CHECK (amount_minor > 0),
    currency            CHAR(3)     NOT NULL DEFAULT 'USD',
    status              TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','authorized','captured','failed','voided','expired','refunded')),
    capture_method      TEXT        NOT NULL DEFAULT 'manual'
                        CHECK (capture_method IN ('manual','automatic')),
    payment_method      TEXT        NOT NULL,             -- opaque test token (tok_*); never PAN-like data
    provider_payment_id TEXT,                             -- shared identifier for reconciliation
    decline_code        TEXT,                             -- provider decline reason when status=failed
    authorized_at       TIMESTAMPTZ,
    expires_at          TIMESTAMPTZ,                      -- auth-hold TTL (authorized only)
    captured_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One payment per order (RFC decision; a second creator gets PAYMENT_EXISTS).
CREATE UNIQUE INDEX IF NOT EXISTS uq_payments_order_id
    ON payments (order_id) WHERE order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_payments_user_created
    ON payments (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_payments_status_expires
    ON payments (status, expires_at) WHERE status = 'authorized';

CREATE TABLE IF NOT EXISTS refunds (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id     BIGINT      NOT NULL REFERENCES payments(id),
    amount_minor   BIGINT      NOT NULL CHECK (amount_minor > 0),
    status         TEXT        NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','succeeded','failed')),
    provider_refund_id TEXT,
    reason         TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_refunds_payment ON refunds (payment_id);

-- Brandur-style idempotency keys with recovery points (RFC-0010 §Idempotency).
-- The UNIQUE index is the race-free claim: INSERT ... ON CONFLICT DO NOTHING,
-- rows-affected decides the winner.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id        BIGINT      NOT NULL,
    idem_key       TEXT        NOT NULL,
    request_method TEXT        NOT NULL,
    request_path   TEXT        NOT NULL,
    request_hash   TEXT        NOT NULL,                  -- SHA-256 of canonical body
    locked_at      TIMESTAMPTZ NOT NULL DEFAULT now(),    -- stale (>90s) => takeover
    recovery_point TEXT        NOT NULL DEFAULT 'started'
                   CHECK (recovery_point IN ('started','provider_called','finished')),
    payment_id     BIGINT REFERENCES payments(id),        -- in-progress subject (recovery re-entry)
    response_code  INT,                                   -- cached result; NULL until finished
    response_body  JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, idem_key)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_created ON idempotency_keys (created_at); -- 24h reaper scan
