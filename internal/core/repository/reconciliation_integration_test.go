//go:build integration

// Integration test for reconciliation: it applies the real migrations (incl.
// 000008), seeds payments, runs the reconciler against a fake provider ledger
// that produces every discrepancy class, and asserts the run + discrepancy rows.

package repository

import (
	"context"
	"testing"

	"github.com/duynhlab/payment-service/internal/core/provider"
	logicv1 "github.com/duynhlab/payment-service/internal/logic/v1"
)

// fakeLedger serves canned provider transaction pages.
type fakeLedger struct{ pages []*provider.TransactionsPage }

func (f *fakeLedger) GetTransactions(_ context.Context, page, _ int) (*provider.TransactionsPage, error) {
	if page-1 < len(f.pages) {
		return f.pages[page-1], nil
	}
	return &provider.TransactionsPage{}, nil
}

func TestReconciliation_Integration(t *testing.T) {
	pool := newTestDB(t)
	ctx := context.Background()

	insert := func(pid string, amount int64, status string) {
		if _, err := pool.Exec(ctx,
			`INSERT INTO payments (user_id, amount_minor, payment_method, status, provider_payment_id)
			 VALUES ($1, $2, $3, $4, $5)`,
			1, amount, "tok_visa", status, pid); err != nil {
			t.Fatalf("insert payment %s: %v", pid, err)
		}
	}
	insert("mp_1", 1000, "captured")   // exact match → no discrepancy
	insert("mp_2", 2000, "captured")   // provider amount will differ → amount_mismatch
	insert("mp_3", 3000, "authorized") // provider status will differ → status_mismatch
	insert("mp_4", 4000, "captured")   // no provider txn → missing_provider

	ledger := &fakeLedger{pages: []*provider.TransactionsPage{{
		Total: 4,
		Transactions: []provider.Transaction{
			{ProviderPaymentID: "mp_1", AmountMinor: 1000, Status: provider.TxnCaptured},
			{ProviderPaymentID: "mp_2", AmountMinor: 2001, Status: provider.TxnCaptured},
			{ProviderPaymentID: "mp_3", AmountMinor: 3000, Status: provider.TxnCaptured},
			{ProviderPaymentID: "mp_9", AmountMinor: 6000, Status: provider.TxnCaptured}, // no payment → missing_internal
		},
	}}}

	runID, found, err := logicv1.NewReconciler(NewReconciliationRepository(pool), ledger).Run(ctx, 100)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if found != 4 {
		t.Fatalf("found = %d, want 4", found)
	}

	var status string
	var scanned, dfound int
	if err := pool.QueryRow(ctx,
		`SELECT status, transactions_scanned, discrepancies_found FROM reconciliation_runs WHERE id = $1`, runID).
		Scan(&status, &scanned, &dfound); err != nil {
		t.Fatalf("run row: %v", err)
	}
	if status != "completed" || scanned != 4 || dfound != 4 {
		t.Fatalf("run = {%s, scanned %d, found %d}, want {completed, 4, 4}", status, scanned, dfound)
	}

	classes := map[string]string{}
	rows, err := pool.Query(ctx,
		`SELECT provider_payment_id, class FROM reconciliation_discrepancies WHERE run_id = $1`, runID)
	if err != nil {
		t.Fatalf("query discrepancies: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var pid, class string
		if err := rows.Scan(&pid, &class); err != nil {
			t.Fatalf("scan discrepancy: %v", err)
		}
		classes[pid] = class
	}
	want := map[string]string{
		"mp_2": "amount_mismatch",
		"mp_3": "status_mismatch",
		"mp_9": "missing_internal",
		"mp_4": "missing_provider",
	}
	for pid, wc := range want {
		if classes[pid] != wc {
			t.Errorf("%s: class = %q, want %q", pid, classes[pid], wc)
		}
	}
	if _, ok := classes["mp_1"]; ok {
		t.Errorf("mp_1 matched exactly — must have no discrepancy")
	}
}
