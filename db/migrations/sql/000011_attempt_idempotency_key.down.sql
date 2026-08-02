-- Reverse 000011. Dropping the column loses the record of which key each
-- round-trip used, which makes an already-parked attempt unresolvable — so this
-- down path is safe only while no attempt is open.

ALTER TABLE payment_attempts
    DROP COLUMN IF EXISTS idempotency_key;
