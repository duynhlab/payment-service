-- RFC-0021 phase 6: stop reconciliation from re-reading all of history.
--
-- v1 compared EVERY payment that has a provider reference against the
-- provider's entire ledger, on every pass. The cost of checking today's
-- payments therefore grew with every payment ever made, and the repository's
-- own comment said so.
--
-- The watermark is where the last completed pass finished. The next pass starts
-- there (minus a lookback, so a mismatch that was merely in-flight gets
-- re-judged) and ends short of now (a settlement lag, so a payment the provider
-- has not seen yet is not called missing). It advances ONLY on a completed run:
-- a failed pass leaves it alone and the next one re-covers the same ground.
--
-- Single row by construction: the primary key is a boolean CHECKed true, so a
-- second row is impossible rather than merely unexpected. A watermark with two
-- values would silently halve the window one of them thinks it owns.

CREATE TABLE IF NOT EXISTS reconciliation_watermark (
    id           BOOLEAN     PRIMARY KEY DEFAULT TRUE CHECK (id),
    through_time TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE reconciliation_watermark IS
    'Where the last COMPLETED reconciliation pass finished (RFC-0021 phase 6). Single row by construction; advanced monotonically, and only by a run that completed.';

COMMENT ON COLUMN reconciliation_watermark.through_time IS
    'Exclusive upper bound of the last completed window. The next window starts here minus a lookback.';
