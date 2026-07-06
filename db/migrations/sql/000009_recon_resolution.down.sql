ALTER TABLE reconciliation_discrepancies
    DROP COLUMN IF EXISTS resolution,
    DROP COLUMN IF EXISTS resolved_at;
