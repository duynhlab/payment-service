//go:build integration

package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// TestProtectedReaders_Integration proves the operator projections over the
// real schema (RFC-0023 slice A): the cross-customer payment list, the full
// attempt history, the ledger lineage summary, and the first reconciliation
// readers.
func TestProtectedReaders_Integration(t *testing.T) {
	pool := newTestDB(t)
	payments := NewPaymentRepository(pool)
	attempts := NewAttemptRepository(pool)
	ledger := NewLedgerRepository(pool)
	recon := NewReconReadRepository(pool)
	ctx := context.Background()

	// Two owners through the real write paths.
	a := createPending(t, payments, "pr-u1", nil, 2000)
	b := createPending(t, payments, "pr-u2", nil, 3000)

	// Drive a into captured with a ledger row.
	if err := payments.TransitionStatus(ctx, a.ID, domain.StatusPending, domain.StatusAuthorized,
		map[string]any{"provider_payment_id": "prov-a", "authorized_at": time.Now()}); err != nil {
		t.Fatalf("authorize a: %v", err)
	}
	if err := payments.CaptureWithLedger(ctx, a.ID, time.Now()); err != nil {
		t.Fatalf("capture a: %v", err)
	}

	// One resolved and one open attempt on a.
	openID, err := attempts.Record(ctx, domain.Attempt{
		PaymentID: a.ID, Operation: domain.AttemptAuthorize, Outcome: domain.OutcomeUnknown,
	})
	if err != nil {
		t.Fatalf("record open attempt: %v", err)
	}
	doneID, err := attempts.Record(ctx, domain.Attempt{
		PaymentID: a.ID, Operation: domain.AttemptCapture, Outcome: domain.OutcomeUnknown,
	})
	if err != nil {
		t.Fatalf("record second attempt: %v", err)
	}
	if err := attempts.Resolve(ctx, doneID, time.Now()); err != nil {
		t.Fatalf("resolve attempt: %v", err)
	}

	t.Run("cross-customer list with status filter and paging", func(t *testing.T) {
		items, total, err := payments.ListAll(ctx, "", 50, 0)
		if err != nil || total < 2 {
			t.Fatalf("list all = (total %d, %v)", total, err)
		}
		owners := map[string]bool{}
		for _, p := range items {
			owners[p.UserID] = true
		}
		if !owners["pr-u1"] || !owners["pr-u2"] {
			t.Fatalf("list is owner-scoped somehow: %v", owners)
		}

		captured, capTotal, err := payments.ListAll(ctx, "captured", 50, 0)
		if err != nil || capTotal < 1 {
			t.Fatalf("captured filter = (total %d, %v)", capTotal, err)
		}
		for _, p := range captured {
			if p.Status != domain.StatusCaptured {
				t.Fatalf("filter leak: %+v", p)
			}
		}

		p1, _, _ := payments.ListAll(ctx, "", 1, 0)
		p2, _, err := payments.ListAll(ctx, "", 1, 1)
		if err != nil || len(p1) != 1 || len(p2) != 1 || p1[0].ID == p2[0].ID {
			t.Fatalf("paging broken: %v vs %v (%v)", p1, p2, err)
		}
	})

	t.Run("full attempt history, oldest first", func(t *testing.T) {
		hist, err := attempts.ListForPayment(ctx, a.ID)
		if err != nil || len(hist) != 2 {
			t.Fatalf("history = (%d, %v), want 2 (open AND resolved)", len(hist), err)
		}
		if hist[0].ID != openID || hist[1].ID != doneID {
			t.Fatalf("order: want oldest first [%d %d], got %+v", openID, doneID, hist)
		}
		// b has none.
		if none, err := attempts.ListForPayment(ctx, b.ID); err != nil || len(none) != 0 {
			t.Fatalf("b history = (%d, %v), want 0", len(none), err)
		}
	})

	t.Run("ledger lineage summary", func(t *testing.T) {
		txns, err := ledger.TransactionsForPayment(ctx, a.ID)
		if err != nil || len(txns) != 1 {
			t.Fatalf("ledger = (%d, %v), want 1 capture", len(txns), err)
		}
		if txns[0].Kind != "capture" || txns[0].AmountMinor != 2000 {
			t.Fatalf("capture txn = %+v, want kind=capture amount=2000", txns[0])
		}
	})

	t.Run("reconciliation readers", func(t *testing.T) {
		// Seed a run + one discrepancy directly (the writer is the recon
		// engine; the readers are what this PR adds).
		var runID int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO reconciliation_runs (status, transactions_scanned, discrepancies_found, finished_at)
			VALUES ('completed', 5, 1, now()) RETURNING id`).Scan(&runID); err != nil {
			t.Fatalf("seed run: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO reconciliation_discrepancies
			  (run_id, provider_payment_id, class, internal_amount_minor, provider_amount_minor, detail)
			VALUES ($1, 'prov-a', 'amount_mismatch', 2000, 1900, 'provider settled less')`, runID); err != nil {
			t.Fatalf("seed discrepancy: %v", err)
		}

		runs, total, err := recon.ListRuns(ctx, 10, 0)
		if err != nil || total < 1 || runs[0].ID != runID {
			t.Fatalf("list runs = (%v, total %d, %v)", runs, total, err)
		}
		run, discs, err := recon.GetRun(ctx, runID)
		if err != nil || run.DiscrepanciesFound != 1 || len(discs) != 1 {
			t.Fatalf("get run = (%+v, %d discs, %v)", run, len(discs), err)
		}
		if discs[0].Class != "amount_mismatch" || discs[0].ProviderAmountMinor != 1900 {
			t.Fatalf("discrepancy = %+v", discs[0])
		}
		if _, _, err := recon.GetRun(ctx, 999999); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("missing run: want ErrNoRows, got %v", err)
		}
	})
}
