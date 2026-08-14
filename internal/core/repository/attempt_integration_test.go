//go:build integration

// Integration tests for the attempt log and the guards a parked operation depends
// on. They only mean anything against a real Postgres: every bug this file pins
// was a CAS or a SUM in SQL that a permissive in-memory fake happily allowed.
package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

func TestAttemptRepository_Integration(t *testing.T) {
	pool := newTestDB(t)
	ctx := context.Background()
	payments := NewPaymentRepository(pool)
	attempts := NewAttemptRepository(pool)

	pay, err := payments.Create(ctx, &domain.Payment{
		UserID: "1", AmountMinor: 5000, Currency: "USD",
		Status: domain.StatusPending, CaptureMethod: domain.CaptureManual,
		PaymentMethod: "tok_test",
	})
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}

	t.Run("an UNKNOWN attempt is the open worklist", func(t *testing.T) {
		id, err := attempts.Record(ctx, domain.Attempt{
			PaymentID: pay.ID, Operation: domain.AttemptCapture, Outcome: domain.OutcomeUnknown,
			IdempotencyKey: "capture:payment:1",
		})
		if err != nil {
			t.Fatalf("record: %v", err)
		}
		open, err := attempts.ListOpen(ctx, 10)
		if err != nil || len(open) != 1 || open[0].ID != id {
			t.Fatalf("ListOpen = %+v err %v, want just attempt %d", open, err, id)
		}
		// The key is what a resolution asks under, so losing it in the round-trip
		// would make the row useless for the only thing it exists for.
		if open[0].IdempotencyKey != "capture:payment:1" {
			t.Errorf("idempotency key = %q, want it read back verbatim", open[0].IdempotencyKey)
		}
		mine, err := attempts.ListOpenForPayment(ctx, pay.ID)
		if err != nil || len(mine) != 1 || mine[0].ID != id {
			t.Fatalf("ListOpenForPayment = %+v err %v, want just attempt %d", mine, err, id)
		}
		if n, _ := attempts.CountOpen(ctx); n != 1 {
			t.Errorf("CountOpen = %d, want 1", n)
		}
		age, err := attempts.OldestOpenAge(ctx)
		if err != nil || age <= 0 {
			t.Errorf("OldestOpenAge = %v err %v, want a positive age", age, err)
		}

		// Closing it removes it from the worklist. The row keeps UNKNOWN: that is
		// what the round-trip returned, and the decided answer belongs to the
		// round-trip that produced it, not to a rewritten past.
		if err := attempts.Resolve(ctx, id, time.Now()); err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if n, _ := attempts.CountOpen(ctx); n != 0 {
			t.Errorf("CountOpen after resolve = %d, want 0", n)
		}
		if age, _ := attempts.OldestOpenAge(ctx); age != 0 {
			t.Errorf("OldestOpenAge with nothing open = %v, want 0", age)
		}
	})

	t.Run("a decided attempt was never in doubt, so it cannot be resolved", func(t *testing.T) {
		id, err := attempts.Record(ctx, domain.Attempt{
			PaymentID: pay.ID, Operation: domain.AttemptVoid, Outcome: domain.OutcomeBusinessDecline,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := attempts.Resolve(ctx, id, time.Now()); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("resolving a decided attempt = %v, want ErrNotFound", err)
		}
	})

	t.Run("resolving twice is a no-op, not a re-stamp", func(t *testing.T) {
		id, err := attempts.Record(ctx, domain.Attempt{
			PaymentID: pay.ID, Operation: domain.AttemptAuthorize, Outcome: domain.OutcomeUnknown,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := attempts.Resolve(ctx, id, time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := attempts.Resolve(ctx, id, time.Now()); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("second resolve = %v, want ErrNotFound — a racing resolver is a no-op", err)
		}
	})

	t.Run("the database refuses a second SUCCESS capture, and says so by type", func(t *testing.T) {
		p2, err := payments.Create(ctx, &domain.Payment{
			UserID: "2", AmountMinor: 7000, Currency: "USD",
			Status: domain.StatusPending, CaptureMethod: domain.CaptureManual, PaymentMethod: "tok_test",
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := attempts.Record(ctx, domain.Attempt{
			PaymentID: p2.ID, Operation: domain.AttemptCapture, Outcome: domain.OutcomeSuccess,
		}); err != nil {
			t.Fatalf("first SUCCESS capture: %v", err)
		}
		// Typed, not opaque: the caller has to be able to tell "someone else already
		// recorded this" from "the write broke", because those need different
		// reactions from anything watching for a double capture.
		if _, err := attempts.Record(ctx, domain.Attempt{
			PaymentID: p2.ID, Operation: domain.AttemptCapture, Outcome: domain.OutcomeSuccess,
		}); !errors.Is(err, domain.ErrDuplicateAttempt) {
			t.Fatalf("second SUCCESS capture = %v, want ErrDuplicateAttempt", err)
		}
		// Doubt about the same operation is not a duplicate of it.
		if _, err := attempts.Record(ctx, domain.Attempt{
			PaymentID: p2.ID, Operation: domain.AttemptCapture, Outcome: domain.OutcomeUnknown,
		}); err != nil {
			t.Fatalf("an UNKNOWN capture attempt must be allowed alongside: %v", err)
		}
	})
}

// TestProcessingRefund_KeepsItsReserveAndCanStillSettle covers the two guards a
// parked refund depends on. Both were wrong when `processing` was introduced.
//
// The reserve: `refunded_minor` sums the refunds that still lay claim to the
// captured amount. Omitting `processing` released that claim, so a full refund
// whose answer was lost let a SECOND full refund through — the provider pays the
// customer twice.
//
// The settle: SettleRefund's CAS only accepted `pending`, so a parked refund could
// never be recorded succeeded or failed. The provider pays out and no row can ever
// say so.
func TestProcessingRefund_KeepsItsReserveAndCanStillSettle(t *testing.T) {
	pool := newTestDB(t)
	ctx := context.Background()
	repo := NewPaymentRepository(pool)

	pay := capturedPayment(t, repo, "9", 5000, "ch_res")

	ref, err := repo.CreateRefund(ctx, pay.ID, 5000, "customer cancelled", "9:rk-1")
	if err != nil {
		t.Fatalf("create refund: %v", err)
	}
	if err := repo.SettleRefund(ctx, ref.ID, domain.RefundProcessing, ""); err != nil {
		t.Fatalf("park refund: %v", err)
	}

	got, err := repo.FindByID(ctx, pay.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if got.RefundedMinor != 5000 {
		t.Fatalf("refunded_minor with a processing refund = %d, want 5000 — the reserve must hold", got.RefundedMinor)
	}
	if _, err := repo.CreateRefund(ctx, pay.ID, 5000, "retry under a new key", "9:rk-2"); !errors.Is(err, domain.ErrRefundRejected) {
		t.Fatalf("second full refund = %v, want ErrRefundRejected — that money is already claimed", err)
	}
	// And a PARTIAL one, which the amount cap alone would have let through: two
	// 500-minor refunds fit inside a 5000 capture, they carry different keys, so the
	// provider replays neither and pays both. Nothing new goes out while one is in
	// doubt.
	if _, err := repo.CreateRefund(ctx, pay.ID, 500, "partial while one is in doubt", "9:rk-3"); !errors.Is(err, domain.ErrRefundRejected) {
		t.Fatalf("partial refund alongside a parked one = %v, want ErrRefundRejected", err)
	}

	if err := repo.SettleRefund(ctx, ref.ID, domain.RefundSucceeded, "rf_1"); err != nil {
		t.Fatalf("settling a parked refund = %v, want it to land: the provider already paid", err)
	}
	settled, err := repo.FindByID(ctx, pay.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != domain.StatusRefunded {
		t.Fatalf("payment status = %s, want refunded once the refunds cover the capture", settled.Status)
	}
}

// FindRefundByID is what the doubt sweep re-drives a parked refund from, so the
// two fields that make a replay a replay — the amount and the ORIGINAL key — must
// survive the round-trip. Losing the key turns the next attempt into a second
// payout.
func TestFindRefundByID_CarriesTheAmountAndTheOriginalKey(t *testing.T) {
	pool := newTestDB(t)
	ctx := context.Background()
	repo := NewPaymentRepository(pool)

	pay := capturedPayment(t, repo, "21", 4000, "ch_find")
	created, err := repo.CreateRefund(ctx, pay.ID, 1500, "partial", "21:rk-find")
	if err != nil {
		t.Fatalf("create refund: %v", err)
	}

	got, err := repo.FindRefundByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("find refund: %v", err)
	}
	if got.AmountMinor != 1500 || got.IdempotencyKey != "21:rk-find" || got.PaymentID != pay.ID {
		t.Fatalf("refund = %+v, want amount 1500, key 21:rk-find, payment %d", got, pay.ID)
	}

	if _, err := repo.FindRefundByID(ctx, created.ID+9999); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing refund = %v, want ErrNotFound", err)
	}
}

// A capture parked in `processing` must still be reversible: that is how a
// resolution records "the provider says it definitely never captured". While the
// CAS only accepted `captured`, the reversal was impossible and the books kept
// asserting revenue nobody had collected.
func TestReverseCapture_UndoesAParkedCapture(t *testing.T) {
	pool := newTestDB(t)
	ctx := context.Background()
	repo := NewPaymentRepository(pool)

	pay := capturedPayment(t, repo, "11", 2500, "ch_park")
	if err := repo.TransitionStatus(ctx, pay.ID, domain.StatusCaptured, domain.StatusProcessing, nil); err != nil {
		t.Fatal(err)
	}

	if err := repo.ReverseCapture(ctx, pay.ID); err != nil {
		t.Fatalf("reversing a parked capture = %v, want it to land", err)
	}
	got, err := repo.FindByID(ctx, pay.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusAuthorized || got.CapturedAt != nil {
		t.Fatalf("status = %s captured_at = %v, want authorized with the capture stamp cleared", got.Status, got.CapturedAt)
	}
}

// capturedPayment builds a payment that has reached `captured` through the real
// transitions, so the ledger and the row agree before a test starts bending them.
func capturedPayment(t *testing.T, repo *PaymentRepository, userID string, amount int64, providerRef string) *domain.Payment {
	t.Helper()
	ctx := context.Background()
	pay, err := repo.Create(ctx, &domain.Payment{
		UserID: userID, AmountMinor: amount, Currency: "USD",
		Status: domain.StatusPending, CaptureMethod: domain.CaptureManual, PaymentMethod: "tok_test",
	})
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}
	if err := repo.TransitionStatus(ctx, pay.ID, domain.StatusPending, domain.StatusAuthorized,
		map[string]any{"provider_payment_id": providerRef}); err != nil {
		t.Fatalf("authorize: %v", err)
	}
	if err := repo.CaptureWithLedger(ctx, pay.ID, time.Now()); err != nil {
		t.Fatalf("capture: %v", err)
	}
	return pay
}
