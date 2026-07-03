ALTER TABLE idempotency_keys
    ADD COLUMN IF NOT EXISTS recovery_point TEXT NOT NULL DEFAULT 'started';
