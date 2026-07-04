//go:build integration

// Integration tests for webhook dedup + payment correlation against a real
// Postgres. Run with: go test -tags=integration ./internal/core/repository/...
package repository

import (
	"context"
	"testing"
)

func TestWebhook_Integration(t *testing.T) {
	pool := newTestDB(t)
	repo := NewPaymentRepository(pool)
	webhooks := NewWebhookRepository(pool)
	ctx := context.Background()

	// authorizeCaptured stamps provider_payment_id = "mp_cap" on the payment.
	p := authorizeCaptured(t, repo, 2000)

	t.Run("known provider id correlates -> processed", func(t *testing.T) {
		status, isNew, err := webhooks.Record(ctx, "evt_known", "charge.captured", "mp_cap")
		if err != nil || !isNew || status != "processed" {
			t.Fatalf("want new/processed, got status=%q isNew=%v err=%v", status, isNew, err)
		}
		var paymentID int64
		if err := pool.QueryRow(ctx,
			`SELECT payment_id FROM webhook_events WHERE event_id = 'evt_known'`).Scan(&paymentID); err != nil {
			t.Fatal(err)
		}
		if paymentID != p.ID {
			t.Fatalf("want payment_id %d, got %d", p.ID, paymentID)
		}
	})

	t.Run("unknown provider id -> orphaned with null payment", func(t *testing.T) {
		status, isNew, err := webhooks.Record(ctx, "evt_orphan", "charge.captured", "mp_nope")
		if err != nil || !isNew || status != "orphaned" {
			t.Fatalf("want new/orphaned, got status=%q isNew=%v err=%v", status, isNew, err)
		}
		var nullCount int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM webhook_events WHERE event_id = 'evt_orphan' AND payment_id IS NULL`).Scan(&nullCount); err != nil {
			t.Fatal(err)
		}
		if nullCount != 1 {
			t.Fatalf("orphaned event must have null payment_id")
		}
	})

	t.Run("redelivery of the same event_id is a no-op", func(t *testing.T) {
		if _, isNew, err := webhooks.Record(ctx, "evt_dup", "charge.captured", "mp_cap"); err != nil || !isNew {
			t.Fatalf("first delivery: isNew=%v err=%v", isNew, err)
		}
		status, isNew, err := webhooks.Record(ctx, "evt_dup", "charge.captured", "mp_cap")
		if err != nil {
			t.Fatal(err)
		}
		if isNew {
			t.Fatal("redelivery must report isNew=false (dedup)")
		}
		if status != "processed" {
			t.Fatalf("redelivery must return the STORED status, got %q", status)
		}
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM webhook_events WHERE event_id = 'evt_dup'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("dedup must keep exactly one row, got %d", n)
		}
	})
}
