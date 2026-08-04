-- Reverse 000012. Dropping the watermark makes the next pass unbounded again:
-- correct, just expensive, which is the safe direction for a rollback.

DROP TABLE IF EXISTS reconciliation_watermark;
