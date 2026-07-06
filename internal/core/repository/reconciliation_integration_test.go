//go:build integration

// Integration test for reconciliation: it applies the real migrations (incl.
// 000008), seeds payments, runs the reconciler against a fake provider ledger
// that produces every discrepancy class, and asserts the run + discrepancy rows.

package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
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

	// Read side (the internal API's report): GetRun + ListDiscrepancies.
	repo := NewReconciliationRepository(pool)
	run, err := repo.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != "completed" || run.DiscrepanciesFound != 4 || run.FinishedAt == nil {
		t.Fatalf("GetRun = %+v, want completed/4/finished", run)
	}
	ds, err := repo.ListDiscrepancies(ctx, runID, 100, 0)
	if err != nil {
		t.Fatalf("ListDiscrepancies: %v", err)
	}
	if len(ds) != 4 {
		t.Fatalf("ListDiscrepancies len = %d, want 4", len(ds))
	}
	// Pagination: a limit returns a page in id order; offset skips.
	page1, err := repo.ListDiscrepancies(ctx, runID, 2, 0)
	if err != nil || len(page1) != 2 {
		t.Fatalf("page1 = (%d, %v), want 2 rows", len(page1), err)
	}
	page2, err := repo.ListDiscrepancies(ctx, runID, 2, 2)
	if err != nil || len(page2) != 2 {
		t.Fatalf("page2 = (%d, %v), want 2 rows", len(page2), err)
	}
	if page1[0].ProviderPaymentID == page2[0].ProviderPaymentID {
		t.Fatalf("offset did not advance the page: %q repeated", page1[0].ProviderPaymentID)
	}

	// The not-found contract is what the handler's 404 depends on: a missing row
	// must surface as domain.ErrNotFound, not a raw pgx error (which would 500).
	if _, err := repo.GetRun(ctx, runID+999); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("GetRun(unknown) = %v, want domain.ErrNotFound", err)
	}

	// An in-progress run (no finished_at) must survive the reaper — the safety
	// invariant that stops the reaper deleting a live run mid-pass.
	runningID, err := repo.CreateRun(ctx)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Reaper: ttl=0 removes finished runs (discrepancies cascade); a long ttl
	// keeps a just-finished run.
	kept, err := repo.ReapRuns(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("ReapRuns(24h): %v", err)
	}
	if kept != 0 {
		t.Fatalf("24h retention must keep the fresh run, reaped %d", kept)
	}
	removed, err := repo.ReapRuns(ctx, 0)
	if err != nil || removed == 0 {
		t.Fatalf("ReapRuns(0) = (%d, %v), want >=1 removed", removed, err)
	}
	if _, err := repo.GetRun(ctx, runID); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("finished run must be gone after reap, GetRun = %v", err)
	}
	if gone, _ := repo.ListDiscrepancies(ctx, runID, 100, 0); len(gone) != 0 {
		t.Errorf("discrepancies must cascade-delete with the run, still see %d", len(gone))
	}
	if _, err := repo.GetRun(ctx, runningID); err != nil {
		t.Errorf("in-progress run (no finished_at) must survive the reaper, GetRun = %v", err)
	}
}
