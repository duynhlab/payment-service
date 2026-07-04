-- DROP TABLE removes each table's triggers, so the function has no dependents
-- by the time we drop it. Order respects the FKs (entries → transactions,
-- entries → accounts).
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS ledger_transactions;
DROP TABLE IF EXISTS ledger_accounts;
DROP FUNCTION IF EXISTS ledger_append_only();
