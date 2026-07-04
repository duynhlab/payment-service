-- V4__ledger.sql
-- Append-only double-entry ledger. Every money movement posts a balanced
-- transaction (sum of debits = sum of credits); entries are immutable —
-- corrections are new reversing transactions, never edits.
--
-- Immutability is enforced by triggers rather than REVOKE. Triggers fire even
-- for a superuser (the local app role), whereas grants do not. This is
-- defense-in-depth, not an absolute barrier: a superuser can still REPLACE the
-- function or drop the trigger. True tamper-resistance is a deployment
-- property — in the cluster the ledger objects are owned by a migration role
-- and the app role holds INSERT/SELECT only.

CREATE TABLE IF NOT EXISTS ledger_accounts (
    id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL CHECK (type IN ('asset', 'liability', 'revenue'))
);

-- Fixed chart of accounts: a customer-funds asset and merchant revenue. A
-- provider-clearing account (to model funds in transit before the provider
-- confirms a capture) arrives with the reconciliation phase that will use it.
INSERT INTO ledger_accounts (name, type) VALUES
    ('customer_funds',   'asset'),
    ('merchant_revenue', 'revenue')
ON CONFLICT (name) DO NOTHING;

CREATE TABLE IF NOT EXISTS ledger_transactions (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id   BIGINT      NOT NULL REFERENCES payments(id),
    kind         TEXT        NOT NULL CHECK (kind IN ('capture', 'refund', 'reversal')),
    external_ref TEXT,                                   -- provider payment/refund id
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ledger_txn_payment ON ledger_transactions (payment_id);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id BIGINT NOT NULL REFERENCES ledger_transactions(id),
    account_id     BIGINT NOT NULL REFERENCES ledger_accounts(id),
    direction      TEXT   NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount_minor   BIGINT NOT NULL CHECK (amount_minor > 0)
);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_txn ON ledger_entries (transaction_id);
CREATE INDEX IF NOT EXISTS idx_ledger_entries_account ON ledger_entries (account_id);

-- Append-only: block UPDATE/DELETE/TRUNCATE on the whole ledger — entries, the
-- transactions that give them meaning (kind/external_ref), and the chart of
-- accounts. Row triggers do NOT fire on TRUNCATE, so each table also gets a
-- statement-level TRUNCATE guard; otherwise the entire audit trail could be
-- erased without tripping any control. Corrections are reversing transactions.
CREATE OR REPLACE FUNCTION ledger_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger is append-only (attempted % on %)', TG_OP, TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_ledger_entries_append_only ON ledger_entries;
CREATE TRIGGER trg_ledger_entries_append_only
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
DROP TRIGGER IF EXISTS trg_ledger_entries_no_truncate ON ledger_entries;
CREATE TRIGGER trg_ledger_entries_no_truncate
    BEFORE TRUNCATE ON ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_append_only();

DROP TRIGGER IF EXISTS trg_ledger_txn_append_only ON ledger_transactions;
CREATE TRIGGER trg_ledger_txn_append_only
    BEFORE UPDATE OR DELETE ON ledger_transactions
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
DROP TRIGGER IF EXISTS trg_ledger_txn_no_truncate ON ledger_transactions;
CREATE TRIGGER trg_ledger_txn_no_truncate
    BEFORE TRUNCATE ON ledger_transactions
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_append_only();

-- The chart of accounts is fixed reference data: INSERT (seed) is allowed,
-- mutation is not.
DROP TRIGGER IF EXISTS trg_ledger_accounts_append_only ON ledger_accounts;
CREATE TRIGGER trg_ledger_accounts_append_only
    BEFORE UPDATE OR DELETE ON ledger_accounts
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
DROP TRIGGER IF EXISTS trg_ledger_accounts_no_truncate ON ledger_accounts;
CREATE TRIGGER trg_ledger_accounts_no_truncate
    BEFORE TRUNCATE ON ledger_accounts
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_append_only();
