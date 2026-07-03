// Ledger persistence: an append-only double-entry ledger. Every settled money
// movement posts a balanced transaction (Σdebit = Σcredit); entries are
// immutable (a DB trigger blocks UPDATE/DELETE), so a correction is a new
// reversing transaction rather than an edit. Posting rides the payment CAS —
// it commits in the same transaction as the state change and only when that
// change actually flips a row — so it inherits the service's idempotency.
package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// Fixed chart of accounts (seeded by migration 000004).
const (
	acctCustomerFunds   = "customer_funds"
	acctMerchantRevenue = "merchant_revenue"
)

// Ledger transaction kinds.
const (
	ledgerCapture  = "capture"
	ledgerRefund   = "refund"
	ledgerReversal = "reversal"
)

// Entry directions (double-entry legs).
const (
	dirDebit  = "debit"
	dirCredit = "credit"
)

// ledgerEntry is one leg of a balanced posting.
type ledgerEntry struct {
	account   string
	direction string // "debit" | "credit"
	amount    int64  // minor units, > 0
}

// dbExec is satisfied by both *pgxpool.Pool and pgx.Tx, letting postLedger
// compose into an outer transaction (capture/refund) or stand alone.
type dbExec interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// postLedger writes one balanced transaction and its entries via q. It enforces
// the double-entry invariant (≥2 legs, Σdebit = Σcredit, every amount > 0)
// before touching the DB, and rejects an unknown account name (the entry insert
// resolves account_id by name, so a miss would silently drop a leg).
func postLedger(ctx context.Context, q dbExec, kind string, paymentID int64, externalRef string, entries []ledgerEntry) error {
	var debit, credit int64
	for _, e := range entries {
		if e.amount <= 0 {
			return fmt.Errorf("%w: non-positive amount on %s leg", domain.ErrLedgerImbalance, e.account)
		}
		switch e.direction {
		case dirDebit:
			debit += e.amount
		case dirCredit:
			credit += e.amount
		default:
			return fmt.Errorf("%w: bad direction %q", domain.ErrLedgerImbalance, e.direction)
		}
	}
	if len(entries) < 2 || debit != credit {
		return fmt.Errorf("%w: %d legs, debit=%d credit=%d", domain.ErrLedgerImbalance, len(entries), debit, credit)
	}

	var txID int64
	if err := q.QueryRow(ctx,
		`INSERT INTO ledger_transactions (payment_id, kind, external_ref)
		 VALUES ($1, $2, NULLIF($3,'')) RETURNING id`,
		paymentID, kind, externalRef).Scan(&txID); err != nil {
		return fmt.Errorf("post ledger txn: %w", err)
	}
	for _, e := range entries {
		tag, err := q.Exec(ctx,
			`INSERT INTO ledger_entries (transaction_id, account_id, direction, amount_minor)
			 SELECT $1, a.id, $2, $3 FROM ledger_accounts a WHERE a.name = $4`,
			txID, e.direction, e.amount, e.account)
		if err != nil {
			return fmt.Errorf("post ledger entry: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("post ledger entry: unknown account %q", e.account)
		}
	}
	return nil
}

// captureEntries moves funds from the customer to merchant revenue.
func captureEntries(amount int64) []ledgerEntry {
	return []ledgerEntry{
		{acctCustomerFunds, dirDebit, amount},
		{acctMerchantRevenue, dirCredit, amount},
	}
}

// reverseCaptureEntries mirror a capture for the given amount — used both for a
// refund (money returned to the customer) and for compensating a failed
// provider capture. Only the transaction kind distinguishes the two.
func reverseCaptureEntries(amount int64) []ledgerEntry {
	return []ledgerEntry{
		{acctMerchantRevenue, dirDebit, amount},
		{acctCustomerFunds, dirCredit, amount},
	}
}

// LedgerRepository exposes read-side ledger queries (balances, imbalance
// guard). Its production consumer is the reconciliation job (a later phase);
// today it backs the integration tests. Posting happens inside the payment
// transactions via postLedger.
type LedgerRepository struct {
	pool *pgxpool.Pool
}

// NewLedgerRepository wires the read-side ledger repository onto a pool.
func NewLedgerRepository(pool *pgxpool.Pool) *LedgerRepository {
	return &LedgerRepository{pool: pool}
}

// Balance returns an account's net balance (Σdebit − Σcredit) in minor units.
func (r *LedgerRepository) Balance(ctx context.Context, account string) (int64, error) {
	var bal int64
	err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE WHEN e.direction = 'debit' THEN e.amount_minor
		                         ELSE -e.amount_minor END), 0)
		FROM ledger_entries e
		JOIN ledger_accounts a ON a.id = e.account_id
		WHERE a.name = $1`, account).Scan(&bal)
	if err != nil {
		return 0, fmt.Errorf("ledger balance: %w", err)
	}
	return bal, nil
}

// Imbalance returns the number of ledger transactions whose entries do not net
// to zero — the internal double-entry invariant guard. It must always be 0.
//
// This checks the ledger against ITSELF only. It does NOT detect drift between
// the ledger and the provider: a capture that committed to the ledger but was
// never confirmed at the provider is a perfectly balanced transaction and reads
// as 0 here. Catching that is the reconciliation job's responsibility (a later
// phase), which compares ledger transactions to provider state.
func (r *LedgerRepository) Imbalance(ctx context.Context) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT transaction_id
			FROM ledger_entries
			GROUP BY transaction_id
			HAVING SUM(CASE WHEN direction = 'debit' THEN amount_minor
			                ELSE -amount_minor END) <> 0
		) unbalanced`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("ledger imbalance: %w", err)
	}
	return n, nil
}
