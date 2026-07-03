-- V3__drop_recovery_point.sql
-- Remove the write-only recovery_point column. Crash-recovery re-entry keys off
-- payment_id (checkpointed) plus the finished flag (response_code IS NOT NULL)
-- and provider-side idempotency replay; recovery_point was recorded but never
-- read for control flow — dead state that advertised a phase machine that was
-- never wired.

ALTER TABLE idempotency_keys DROP COLUMN IF EXISTS recovery_point;
