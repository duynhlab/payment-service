package repository

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// LeaseRepository hands out single-writer leases backed by Postgres advisory
// locks (verified against the PostgreSQL 17 documentation, "Advisory Locks").
//
// Why an advisory lock rather than a lease table with an expiry: the server
// releases session-level advisory locks "at the end of the session, even if the
// client disconnects ungracefully". A crashed process therefore frees its lease
// immediately, with no TTL to tune — and a TTL is exactly the knob that is either
// too short (two writers overlap) or too long (the role stalls after every crash).
//
// Why SESSION level rather than transaction level: a reconciliation pass makes
// provider HTTP calls, so a transaction-scoped lock would mean holding a
// transaction open for the whole pass. An idle-in-transaction connection blocks
// vacuum and pins the oldest xmin, which is a far worse problem than the one being
// solved.
type LeaseRepository struct {
	pool *pgxpool.Pool
}

// NewLeaseRepository wraps a pool.
func NewLeaseRepository(pool *pgxpool.Pool) *LeaseRepository {
	return &LeaseRepository{pool: pool}
}

// lease is a held advisory lock. It owns a pooled connection for its whole life,
// because a session-level lock belongs to the SESSION that took it: hand the
// connection back and the lock is owned by a connection nobody controls, while an
// unlock issued on some other pooled connection quietly does nothing (Postgres
// returns false and logs a warning).
//
// That single held connection is the real cost of this design, and it is why the
// caller must always Release: the pool is 25 by default, so one leak is survivable
// and a loop of them is not.
type lease struct {
	conn *pgxpool.Conn
	key  int64
	// released makes a second Release report a bug instead of crashing the
	// process. pgxpool PANICS when a connection is released twice, and this object
	// is handed out through an interface — so a caller with one Release too many
	// (a stray defer, a retry loop) would take down a service whose whole job is
	// moving money. A CAS rather than a plain flag: two goroutines racing to
	// release is itself misuse worth reporting rather than a data race.
	released atomic.Bool
}

// TryAcquire takes the lease if it is free, and reports ErrLeaseHeld if it is not.
//
// Non-blocking on purpose. The blocking variant would queue every ticker firing
// behind one slow pass, so a pass that took longer than the interval would build a
// backlog of passes all wanting to run — the opposite of what a single-writer
// guard is for. A tick that cannot get the lease should simply be skipped; the
// next one is a minute away.
func (r *LeaseRepository) TryAcquire(ctx context.Context, key int64) (domain.Lease, error) {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection for lease %d: %w", key, err)
	}

	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&got); err != nil {
		conn.Release()
		return nil, fmt.Errorf("try lease %d: %w", key, err)
	}
	if !got {
		conn.Release()
		return nil, domain.ErrLeaseHeld
	}
	return &lease{conn: conn, key: key}, nil
}

// Release drops the lock and returns the connection to the pool.
//
// It unlocks exactly once, because advisory locks are reference counted: "for each
// completed lock request there must be a corresponding unlock request before the
// lock is actually released". A lease is created by exactly one successful
// acquire, so exactly one unlock is correct — and Postgres answering false means
// that pairing has been broken somewhere, which is a bug worth surfacing rather
// than a condition to tolerate.
//
// A SECOND Release is refused before the connection is touched. Not defensiveness
// for its own sake: pgxpool panics on a double connection release, so one stray
// defer would turn a bookkeeping mistake into a crashed payment service.
//
// The connection goes back to the pool either way. Leaking it to avoid reporting
// an error would trade a visible problem for an invisible one.
func (l *lease) Release(ctx context.Context) error {
	if !l.released.CompareAndSwap(false, true) {
		return fmt.Errorf("release lease %d: already released", l.key)
	}
	defer l.conn.Release()

	var released bool
	if err := l.conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, l.key).Scan(&released); err != nil {
		// The connection is closed on release, and the server frees the lock when
		// the session ends, so the lease is not stuck — only unaccounted for.
		return fmt.Errorf("release lease %d: %w", l.key, err)
	}
	if !released {
		return fmt.Errorf("release lease %d: the lock was not held by this session", l.key)
	}
	return nil
}
