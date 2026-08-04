//go:build integration

// Integration tests for the single-writer lease. They only mean anything against
// a real Postgres: the whole design rests on how the SERVER scopes an advisory
// lock — to a session, not to a transaction and not to a client object — and an
// in-memory double would simply agree with whatever the code assumed.
package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

func TestLease_Integration(t *testing.T) {
	pool := newTestDB(t)
	ctx := context.Background()
	leases := NewLeaseRepository(pool)

	t.Run("a held lease excludes a second holder, and frees on release", func(t *testing.T) {
		first, err := leases.TryAcquire(ctx, domain.LeaseReconciliation)
		if err != nil {
			t.Fatalf("first acquire: %v", err)
		}

		// The second attempt must fail IMMEDIATELY rather than wait. Blocking here is
		// what would let every ticker firing queue up behind one slow pass.
		if _, err := leases.TryAcquire(ctx, domain.LeaseReconciliation); !errors.Is(err, domain.ErrLeaseHeld) {
			t.Fatalf("second acquire = %v, want ErrLeaseHeld", err)
		}

		if err := first.Release(ctx); err != nil {
			t.Fatalf("release: %v", err)
		}
		second, err := leases.TryAcquire(ctx, domain.LeaseReconciliation)
		if err != nil {
			t.Fatalf("acquire after release: %v", err)
		}
		if err := second.Release(ctx); err != nil {
			t.Fatalf("release second: %v", err)
		}
	})

	// Different keys are different leases. If they were not, adding a second
	// single-writer role would silently stop the first one from ever running.
	t.Run("different keys do not exclude each other", func(t *testing.T) {
		a, err := leases.TryAcquire(ctx, domain.LeaseReconciliation)
		if err != nil {
			t.Fatal(err)
		}
		b, err := leases.TryAcquire(ctx, domain.LeaseReconciliation+1)
		if err != nil {
			t.Fatalf("a different key was blocked by an unrelated lease: %v", err)
		}
		if err := a.Release(ctx); err != nil {
			t.Fatal(err)
		}
		if err := b.Release(ctx); err != nil {
			t.Fatal(err)
		}
	})

	// TestLease_ReleaseTwiceIsAnError pins the reference-counting rule the docs
	// state: "for each completed lock request there must be a corresponding unlock
	// request". One acquire means exactly one unlock, so a second Release is a
	// broken pairing — and Postgres answers false rather than raising, which is
	// precisely the kind of failure that stays a bug if nobody checks the result.
	t.Run("releasing twice is reported, not ignored", func(t *testing.T) {
		l, err := leases.TryAcquire(ctx, domain.LeaseReconciliation)
		if err != nil {
			t.Fatal(err)
		}
		if err := l.Release(ctx); err != nil {
			t.Fatalf("first release: %v", err)
		}
		if err := l.Release(ctx); err == nil {
			t.Fatal("a second release reported success: the acquire/release pairing is unchecked")
		}
	})

	// The lease must not leak the connection it holds. A pass takes one connection
	// out of the pool for its whole duration, so a leak per pass would exhaust the
	// pool within an hour of ticking.
	t.Run("the held connection returns to the pool", func(t *testing.T) {
		before := pool.Stat().AcquiredConns()
		l, err := leases.TryAcquire(ctx, domain.LeaseReconciliation)
		if err != nil {
			t.Fatal(err)
		}
		if held := pool.Stat().AcquiredConns(); held != before+1 {
			t.Fatalf("acquired conns = %d, want %d — the lease must hold exactly one", held, before+1)
		}
		if err := l.Release(ctx); err != nil {
			t.Fatal(err)
		}
		if after := pool.Stat().AcquiredConns(); after != before {
			t.Fatalf("acquired conns = %d after release, want %d", after, before)
		}
	})
}
