-- RFC-0021 phase 6: give a parked operation a way back out.
--
-- `processing` records that a provider round-trip's outcome is unknown. The only
-- safe way to learn what actually happened is to ask the provider the SAME
-- question again — the identical operation under the identical idempotency key,
-- so the provider replays its original answer instead of performing the work a
-- second time. Re-driving without the original key is not a resolution; it is a
-- second charge.
--
-- Capture and void keys are derived from the payment id, and a refund carries
-- its own key on `refunds`. The charge key is the one that is NOT derivable: it
-- is built from the caller's Idempotency-Key, which the payment row does not
-- keep. Recording the key on the attempt makes resolution uniform across all
-- four operations, and doubles as the evidence a provider dispute asks for.
--
-- Expand-only: nullable, no existing row changes.

ALTER TABLE payment_attempts
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

COMMENT ON COLUMN payment_attempts.idempotency_key IS
    'The provider idempotency key this round-trip used. Resolution re-drives the same operation under the same key, so the provider replays rather than repeats (RFC-0021 phase 6).';
