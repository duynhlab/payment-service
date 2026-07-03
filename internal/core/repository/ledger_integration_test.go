//go:build integration

// Integration tests for the double-entry ledger against a real Postgres: the
// balanced capture/refund/reversal postings, the append-only trigger, and the
// system-wide imbalance guard. Run with:
//
//	go test -tags=integration ./internal/core/repository/...
package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// authorizeCaptured drives a fresh payment to captured through the real repo
// methods, returning it.
func authorizeCaptured(t *testing.T, repo *PaymentRepository, amount int64) *domain.Payment {
	t.Helper()
	ctx := context.Background()
	p := createPending(t, repo, 7, nil, amount)
	if err := repo.TransitionStatus(ctx, p.ID, domain.StatusPending, domain.StatusAuthorized,
		map[string]any{"provider_payment_id": "mp_cap", "authorized_at": time.Now(), "expires_at": time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("authorize: %v", err)
	}
	if err := repo.CaptureWithLedger(ctx, p.ID, time.Now()); err != nil {
		t.Fatalf("capture: %v", err)
	}
	return p
}

func TestLedger_Integration(t *testing.T) {
	pool := newTestDB(t)
	repo := NewPaymentRepository(pool)
	ledger := NewLedgerRepository(pool)
	ctx := context.Background()

	t.Run("capture posts a balanced pair", func(t *testing.T) {
		p := authorizeCaptured(t, repo, 2000)

		customer, err := ledger.Balance(ctx, acctCustomerFunds)
		if err != nil {
			t.Fatal(err)
		}
		merchant, err := ledger.Balance(ctx, acctMerchantRevenue)
		if err != nil {
			t.Fatal(err)
		}
		// customer_funds debited (+2000), merchant_revenue credited (-2000).
		if customer != 2000 || merchant != -2000 {
			t.Fatalf("capture balances: customer=%d merchant=%d", customer, merchant)
		}
		if n, _ := ledger.Imbalance(ctx); n != 0 {
			t.Fatalf("imbalance after capture: %d", n)
		}
		_ = p
	})

	t.Run("capture ledger is idempotent under stale re-capture", func(t *testing.T) {
		p := authorizeCaptured(t, repo, 500)
		// A second capture attempt is stale (already captured) → posts nothing.
		if err := repo.CaptureWithLedger(ctx, p.ID, time.Now()); !errors.Is(err, domain.ErrStaleTransition) {
			t.Fatalf("re-capture want stale, got %v", err)
		}
		var count int
		if err := pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM ledger_transactions WHERE payment_id=$1 AND kind='capture'`,
			p.ID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("want exactly one capture txn, got %d", count)
		}
	})

	t.Run("reversal nets the capture back to zero", func(t *testing.T) {
		p := authorizeCaptured(t, repo, 1500)
		before, _ := ledger.Balance(ctx, acctMerchantRevenue)
		if err := repo.ReverseCapture(ctx, p.ID); err != nil {
			t.Fatalf("reverse: %v", err)
		}
		after, _ := ledger.Balance(ctx, acctMerchantRevenue)
		if after-before != 1500 { // credit -1500 undone by debit +1500
			t.Fatalf("reversal did not net capture: before=%d after=%d", before, after)
		}
		got, err := repo.FindByID(ctx, p.ID, 0)
		if err != nil || got.Status != domain.StatusAuthorized {
			t.Fatalf("reversed payment must be authorized, got %+v err=%v", got, err)
		}
		if n, _ := ledger.Imbalance(ctx); n != 0 {
			t.Fatalf("imbalance after reversal: %d", n)
		}
	})

	t.Run("succeeded refund posts the reverse legs", func(t *testing.T) {
		p := authorizeCaptured(t, repo, 3000)
		before, _ := ledger.Balance(ctx, acctCustomerFunds)

		ref, err := repo.CreateRefund(ctx, p.ID, 1200, "partial", "idem-refund-1")
		if err != nil {
			t.Fatalf("create refund: %v", err)
		}
		if err := repo.SettleRefund(ctx, ref.ID, domain.RefundSucceeded, "re_1"); err != nil {
			t.Fatalf("settle refund: %v", err)
		}
		after, _ := ledger.Balance(ctx, acctCustomerFunds)
		// customer_funds credited by 1200 (money returned) → balance drops by 1200.
		if before-after != 1200 {
			t.Fatalf("refund ledger: before=%d after=%d", before, after)
		}
		if n, _ := ledger.Imbalance(ctx); n != 0 {
			t.Fatalf("imbalance after refund: %d", n)
		}
	})

	t.Run("failed refund posts nothing", func(t *testing.T) {
		p := authorizeCaptured(t, repo, 800)
		ref, err := repo.CreateRefund(ctx, p.ID, 800, "full", "idem-refund-fail")
		if err != nil {
			t.Fatalf("create refund: %v", err)
		}
		before, _ := ledger.Balance(ctx, acctCustomerFunds)
		if err := repo.SettleRefund(ctx, ref.ID, domain.RefundFailed, ""); err != nil {
			t.Fatalf("settle failed refund: %v", err)
		}
		after, _ := ledger.Balance(ctx, acctCustomerFunds)
		if before != after {
			t.Fatalf("failed refund must not post ledger: before=%d after=%d", before, after)
		}
	})

	t.Run("the whole ledger is append-only", func(t *testing.T) {
		authorizeCaptured(t, repo, 100)
		// UPDATE / DELETE blocked on entries and transactions; the chart of
		// accounts is immutable too. TRUNCATE (which row triggers miss) must be
		// blocked on all three, or the audit trail could be erased silently.
		mutations := []string{
			`UPDATE ledger_entries SET amount_minor = 1 WHERE id = (SELECT id FROM ledger_entries LIMIT 1)`,
			`DELETE FROM ledger_entries WHERE id = (SELECT id FROM ledger_entries LIMIT 1)`,
			`UPDATE ledger_transactions SET kind = 'reversal' WHERE id = (SELECT id FROM ledger_transactions LIMIT 1)`,
			`DELETE FROM ledger_transactions WHERE id = (SELECT id FROM ledger_transactions LIMIT 1)`,
			`UPDATE ledger_accounts SET name = 'x' WHERE name = 'customer_funds'`,
			`TRUNCATE ledger_entries`,
			`TRUNCATE ledger_transactions`,
			`TRUNCATE ledger_accounts`,
		}
		for _, stmt := range mutations {
			if _, err := pool.Exec(ctx, stmt); err == nil {
				t.Fatalf("mutation must be blocked: %s", stmt)
			}
		}
	})

	t.Run("postLedger rejects invalid postings", func(t *testing.T) {
		p := authorizeCaptured(t, repo, 100)
		cases := map[string][]ledgerEntry{
			"unbalanced":    {{acctCustomerFunds, "debit", 100}, {acctMerchantRevenue, "credit", 90}},
			"non-positive":  {{acctCustomerFunds, "debit", 0}, {acctMerchantRevenue, "credit", 0}},
			"bad direction": {{acctCustomerFunds, "sideways", 100}, {acctMerchantRevenue, "credit", 100}},
			"single leg":    {{acctCustomerFunds, "debit", 100}},
		}
		for name, entries := range cases {
			t.Run(name, func(t *testing.T) {
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				if err := postLedger(ctx, tx, ledgerCapture, p.ID, "", entries); !errors.Is(err, domain.ErrLedgerImbalance) {
					t.Fatalf("want ErrLedgerImbalance, got %v", err)
				}
			})
		}
	})

	t.Run("postLedger rejects an unknown account", func(t *testing.T) {
		p := authorizeCaptured(t, repo, 100)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		bad := []ledgerEntry{
			{"nonexistent", "debit", 100},
			{acctMerchantRevenue, "credit", 100},
		}
		if err := postLedger(ctx, tx, ledgerCapture, p.ID, "", bad); err == nil {
			t.Fatal("unknown account must fail the posting")
		}
	})
}
