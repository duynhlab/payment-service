//go:build integration

// Integration tests for the transactional outbox against a real Postgres: that
// money-movement transactions enqueue their event atomically, and the relay's
// fetch/mark round-trip behaves. Run with:
//
//	go test -tags=integration ./internal/core/repository/...
package repository

import (
	"context"
	"testing"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

func TestOutbox_Integration(t *testing.T) {
	pool := newTestDB(t)
	repo := NewPaymentRepository(pool)
	outbox := NewOutboxRepository(pool)
	ctx := context.Background()

	// eventTypesFor returns the outbox event types recorded for a payment, in
	// insertion order.
	eventTypesFor := func(paymentID int64) []string {
		rows, err := pool.Query(ctx,
			`SELECT event_type FROM payment_outbox WHERE (payload->>'payment_id')::bigint = $1 ORDER BY id`,
			paymentID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var types []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			types = append(types, s)
		}
		return types
	}

	t.Run("capture enqueues payment.captured in the same tx", func(t *testing.T) {
		p := authorizeCaptured(t, repo, 2000)
		if got := eventTypesFor(p.ID); len(got) != 1 || got[0] != domain.EventPaymentCaptured {
			t.Fatalf("want [payment.captured], got %v", got)
		}
	})

	t.Run("succeeded refund enqueues payment.refunded", func(t *testing.T) {
		p := authorizeCaptured(t, repo, 3000)
		ref, err := repo.CreateRefund(ctx, p.ID, 1200, "partial", "idem-ob-refund")
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.SettleRefund(ctx, ref.ID, domain.RefundSucceeded, "re_ob"); err != nil {
			t.Fatal(err)
		}
		if got := eventTypesFor(p.ID); len(got) != 2 ||
			got[0] != domain.EventPaymentCaptured || got[1] != domain.EventPaymentRefunded {
			t.Fatalf("want [captured, refunded], got %v", got)
		}
	})

	t.Run("failed refund enqueues nothing extra", func(t *testing.T) {
		p := authorizeCaptured(t, repo, 800)
		ref, err := repo.CreateRefund(ctx, p.ID, 800, "full", "idem-ob-fail")
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.SettleRefund(ctx, ref.ID, domain.RefundFailed, ""); err != nil {
			t.Fatal(err)
		}
		if got := eventTypesFor(p.ID); len(got) != 1 {
			t.Fatalf("failed refund must not enqueue, got %v", got)
		}
	})

	t.Run("reversal enqueues payment.capture_reversed", func(t *testing.T) {
		p := authorizeCaptured(t, repo, 1500)
		if err := repo.ReverseCapture(ctx, p.ID); err != nil {
			t.Fatal(err)
		}
		if got := eventTypesFor(p.ID); len(got) != 2 || got[1] != domain.EventCaptureReversed {
			t.Fatalf("want [captured, capture_reversed], got %v", got)
		}
	})

	t.Run("fetch is FIFO and mark removes from unpublished", func(t *testing.T) {
		before, err := outbox.FetchUnpublished(ctx, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(before) == 0 {
			t.Fatal("expected unpublished events from prior subtests")
		}
		// FIFO by id.
		for i := 1; i < len(before); i++ {
			if before[i].ID <= before[i-1].ID {
				t.Fatalf("events not FIFO: %d after %d", before[i].ID, before[i-1].ID)
			}
		}
		ids := make([]int64, len(before))
		for i, e := range before {
			ids[i] = e.ID
		}
		if err := outbox.MarkPublished(ctx, ids); err != nil {
			t.Fatal(err)
		}
		after, err := outbox.FetchUnpublished(ctx, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(after) != 0 {
			t.Fatalf("all events marked published, still see %d unpublished", len(after))
		}
	})

	t.Run("reap removes published rows past retention, keeps recent + unpublished", func(t *testing.T) {
		// Prior subtest marked everything published (all with published_at ~now).
		// A fresh capture adds one unpublished row.
		p := authorizeCaptured(t, repo, 700)

		// ttl=0 deletes every published row (published_at < now), but must leave
		// the just-created unpublished one.
		removed, err := outbox.ReapPublished(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		if removed == 0 {
			t.Fatal("expected published rows to be reaped")
		}
		var unpublished, total int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FILTER (WHERE published_at IS NULL), count(*) FROM payment_outbox`).
			Scan(&unpublished, &total); err != nil {
			t.Fatal(err)
		}
		if unpublished != 1 || total != 1 {
			t.Fatalf("only the unpublished capture should remain: unpublished=%d total=%d", unpublished, total)
		}

		// A long retention keeps the unpublished row untouched.
		if _, err := outbox.ReapPublished(ctx, 24*time.Hour); err != nil {
			t.Fatal(err)
		}
		if got := eventTypesFor(p.ID); len(got) != 1 || got[0] != domain.EventPaymentCaptured {
			t.Fatalf("unpublished event must survive reap, got %v", got)
		}
	})
}
