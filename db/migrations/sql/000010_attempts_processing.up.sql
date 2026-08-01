-- RFC-0021 phase 6: give an ambiguous provider answer somewhere to live.
--
-- Two things are added, and they answer different questions:
--
--   payments.status = 'processing'   WHAT is true of this intent right now:
--                                    an operation was attempted and its
--                                    outcome is not known.
--   payment_attempts.outcome_class   WHY, per provider round-trip.
--
-- The rule this exists to enforce (RFC-0021): an UNKNOWN outcome never
-- auto-triggers the semantic opposite operation. Before it, a capture whose
-- response was lost was reversed on the spot — so a captured charge could end
-- up with the money taken and the books saying `authorized`.
--
-- Expand-only: no existing row changes, and nothing writes the new state until
-- the operation paths adopt it in the next slice.

ALTER TABLE payments
    DROP CONSTRAINT IF EXISTS payments_status_check;

ALTER TABLE payments
    ADD CONSTRAINT payments_status_check CHECK (status IN (
        'pending', 'processing', 'authorized', 'captured',
        'failed', 'voided', 'expired', 'refunded'
    ));

ALTER TABLE refunds
    DROP CONSTRAINT IF EXISTS refunds_status_check;

ALTER TABLE refunds
    ADD CONSTRAINT refunds_status_check CHECK (status IN (
        'pending', 'processing', 'succeeded', 'failed'
    ));

COMMENT ON COLUMN payments.status IS
    'Intent state. `processing` means an operation was attempted and the provider outcome is UNKNOWN — never a verdict, and never resolved by guessing (RFC-0021 phase 6).';

-- One row per provider round-trip. Without it there is no way to answer "how
-- many times did we call the provider for this intent, and what did each call
-- say" — which is the substrate `processing` needs to be resolvable rather than
-- a dead end.
CREATE TABLE IF NOT EXISTS payment_attempts (
    id            BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id    BIGINT      NOT NULL REFERENCES payments(id) ON DELETE CASCADE,

    operation     TEXT        NOT NULL
                  CHECK (operation IN ('authorize', 'capture', 'void', 'refund')),

    -- The four classes the RFC names. BUSINESS_DECLINE and RETRYABLE_FAILURE are
    -- both decided; UNKNOWN is the only one that leaves an intent open.
    outcome_class TEXT        NOT NULL
                  CHECK (outcome_class IN ('SUCCESS', 'BUSINESS_DECLINE', 'RETRYABLE_FAILURE', 'UNKNOWN')),

    -- What the provider said, verbatim enough to reconcile against: its
    -- reference where one came back, and its own code/status where it gave one.
    provider_ref    TEXT,
    provider_status TEXT,

    -- refund_id ties a refund attempt to its refund row; NULL for the
    -- intent-level operations.
    refund_id     BIGINT      REFERENCES refunds(id) ON DELETE CASCADE,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Set when an UNKNOWN attempt is later resolved. An unresolved UNKNOWN is
    -- the operational worklist: it is what the backlog gauge counts.
    resolved_at   TIMESTAMPTZ
);

-- At most one SUCCESS capture per payment, enforced by the database rather than
-- by hope: a double capture is the one provider mistake we can never refund our
-- way out of cleanly.
CREATE UNIQUE INDEX IF NOT EXISTS uq_payment_attempts_one_success_capture
    ON payment_attempts (payment_id)
    WHERE operation = 'capture' AND outcome_class = 'SUCCESS';

-- The reconciler reads open UNKNOWNs oldest-first; the gauge counts them.
CREATE INDEX IF NOT EXISTS idx_payment_attempts_open_unknown
    ON payment_attempts (created_at)
    WHERE outcome_class = 'UNKNOWN' AND resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_payment_attempts_payment
    ON payment_attempts (payment_id, created_at DESC);

COMMENT ON TABLE payment_attempts IS
    'One row per provider round-trip (RFC-0021 phase 6). outcome_class UNKNOWN + resolved_at IS NULL is the open-doubt worklist.';
