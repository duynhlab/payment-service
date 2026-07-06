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

// TestReconciliationHeal_Integration exercises ADR-012 against a real Postgres:
// a seeded crash-window drift (internal authorized / provider captured) converges
// through the real capture path when a healer is wired, the row + ledger end
// consistent, and a re-run is a no-op (the drift is gone).
func TestReconciliationHeal_Integration(t *testing.T) {
	pool := newTestDB(t)
	ctx := context.Background()
	reconRepo := NewReconciliationRepository(pool)
	payRepo := NewPaymentRepository(pool)
	ledgerRepo := NewLedgerRepository(pool)

	// mp_heal: the lost-capture-response window — internal authorized while the
	// provider already captured. mp_amt: a benign amount_mismatch heal must skip.
	var healID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO payments (user_id, amount_minor, payment_method, status, provider_payment_id)
		 VALUES (1, 1000, 'tok_visa', 'authorized', 'mp_heal') RETURNING id`).Scan(&healID); err != nil {
		t.Fatalf("seed authorized payment: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO payments (user_id, amount_minor, payment_method, status, provider_payment_id)
		 VALUES (1, 2000, 'tok_visa', 'captured', 'mp_amt')`); err != nil {
		t.Fatalf("seed captured payment: %v", err)
	}

	ledger := &fakeLedger{pages: []*provider.TransactionsPage{{
		Total: 2,
		Transactions: []provider.Transaction{
			{ProviderPaymentID: "mp_heal", AmountMinor: 1000, Status: provider.TxnCaptured},
			{ProviderPaymentID: "mp_amt", AmountMinor: 2001, Status: provider.TxnCaptured}, // amount drift
		},
	}}}

	healer := logicv1.NewCaptureHealer(payRepo, time.Now)
	runID, found, err := logicv1.NewReconciler(reconRepo, ledger, logicv1.WithHealer(healer)).Run(ctx, 100)
	if err != nil {
		t.Fatalf("run with heal: %v", err)
	}
	if found != 2 {
		t.Fatalf("found = %d, want 2 (status_mismatch + amount_mismatch)", found)
	}

	// The crash-window payment converged: healed + timestamped; the amount drift
	// was left for a human (skipped).
	res := map[string]string{}
	var healedAt *time.Time
	rows, err := pool.Query(ctx,
		`SELECT provider_payment_id, resolution, resolved_at FROM reconciliation_discrepancies WHERE run_id = $1`, runID)
	if err != nil {
		t.Fatalf("query resolutions: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var pid, resolution string
		var at *time.Time
		if err := rows.Scan(&pid, &resolution, &at); err != nil {
			t.Fatalf("scan resolution: %v", err)
		}
		res[pid] = resolution
		if pid == "mp_heal" {
			healedAt = at
		}
	}
	if res["mp_heal"] != "healed" || res["mp_amt"] != "skipped" {
		t.Fatalf("resolutions = %v, want mp_heal=healed mp_amt=skipped", res)
	}
	if healedAt == nil {
		t.Fatal("healed discrepancy must have resolved_at stamped")
	}

	// The payment row converged to captured and the ledger is balanced.
	got, err := payRepo.FindByID(ctx, healID, 0)
	if err != nil {
		t.Fatalf("find healed payment: %v", err)
	}
	if got.Status != domain.StatusCaptured {
		t.Fatalf("healed payment status = %q, want captured", got.Status)
	}
	if n, _ := ledgerRepo.Imbalance(ctx); n != 0 {
		t.Fatalf("ledger imbalance after heal: %d, want 0", n)
	}

	// Idempotent: with mp_heal now captured, the drift is gone — a re-run finds
	// only the amount drift and never re-captures (no double posting).
	_, found2, err := logicv1.NewReconciler(reconRepo, ledger, logicv1.WithHealer(healer)).Run(ctx, 100)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if found2 != 1 {
		t.Fatalf("second run found = %d, want 1 (only the amount drift remains)", found2)
	}
	if n, _ := ledgerRepo.Imbalance(ctx); n != 0 {
		t.Fatalf("ledger imbalance after re-run: %d, want 0", n)
	}
	var captureTxns int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM ledger_transactions WHERE payment_id = $1 AND kind = 'capture'`, healID).
		Scan(&captureTxns); err != nil {
		t.Fatalf("count capture txns: %v", err)
	}
	if captureTxns != 1 {
		t.Fatalf("heal must post exactly one capture txn, got %d", captureTxns)
	}
}
