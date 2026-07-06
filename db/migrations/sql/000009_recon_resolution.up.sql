-- Reconciliation auto-heal (ADR-012): record what a run did about each
-- discrepancy, not just that it found one. `resolution` starts at 'detected'
-- (the detect-only default, unchanged when RECON_HEAL_ENABLED is off); a heal
-- pass moves the healable class to 'healed'/'failed' and marks the rest
-- 'skipped'. `resolved_at` stamps when heal acted.
ALTER TABLE reconciliation_discrepancies
    ADD COLUMN resolution  TEXT NOT NULL DEFAULT 'detected'
               CHECK (resolution IN ('detected', 'healed', 'skipped', 'failed')),
    ADD COLUMN resolved_at TIMESTAMPTZ;

COMMENT ON COLUMN reconciliation_discrepancies.resolution IS
    'What the run did: detected (no heal), healed, skipped (heal ran, not this class), failed (heal errored). See ADR-012.';
