package domain

import (
	"context"
	"errors"
)

// Single-writer leases. Some background roles must have exactly one runner at a
// time — not because concurrency is slow, but because two runners produce wrong
// answers: two reconciliation passes both page the provider ledger, both write
// discrepancy rows for the same charges, and both try to heal them, so the report
// double-counts and the heal path races itself.
//
// The vocabulary lives here because the KEY SPACE is shared by everything that
// takes a lease in this database. Two roles picking the same key would silently
// exclude each other, and the symptom would be a background job that mysteriously
// never runs — so the keys are declared in one place, together, on purpose.

// Lease keys. Postgres offers a single-bigint and a two-integer advisory key
// space that do not overlap; these are the single-bigint kind.
const (
	// LeaseReconciliation is held by whoever is running a reconciliation pass.
	LeaseReconciliation int64 = 21060001
)

// ErrLeaseHeld means somebody else holds the lease right now. It is NOT a
// failure: for a single-writer role, "another process is doing it" is the correct
// outcome, and the caller should stand down rather than queue or retry.
var ErrLeaseHeld = errors.New("lease is held by another process")

// Leaser hands out single-writer leases.
type Leaser interface {
	// TryAcquire takes the lease if free and reports ErrLeaseHeld if not. It never
	// blocks: waiting would queue every ticker firing behind one slow run, so a run
	// that overran its interval would build a backlog of runs all wanting to start
	// — the opposite of what a single-writer guard is for.
	TryAcquire(ctx context.Context, key int64) (Lease, error)
}

// Lease is a held lease. Release must be called on every path.
type Lease interface {
	Release(ctx context.Context) error
}
