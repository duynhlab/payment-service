package repository

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

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
// If the unlock itself fails, the connection is DISCARDED rather than returned to
// the pool. This is the subtle one. Advisory locks are reference counted per
// session, so a connection handed back while still holding the lock would let a
// later pass that happened to draw the same pooled connection acquire the "held"
// lease again — two writers, each certain it is alone, which is the exact failure
// this type exists to prevent. Ending the session is what frees the lock
// ("automatically cleaned up by the server at the end of the session"), so
// dropping the connection is both the fix and the cleanup.
func (l *lease) Release(ctx context.Context) error {
	if !l.released.CompareAndSwap(false, true) {
		return fmt.Errorf("release lease %d: already released", l.key)
	}

	var released bool
	if err := l.conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, l.key).Scan(&released); err != nil {
		l.discard()
		return fmt.Errorf("release lease %d: %w", l.key, err)
	}
	l.conn.Release()
	if !released {
		// The session did not hold it, so there is nothing to leak — but the
		// acquire/release pairing is broken somewhere, and silence is how that
		// stays a bug.
		return fmt.Errorf("release lease %d: the lock was not held by this session", l.key)
	}
	return nil
}

// discardGrace bounds the close of a connection we no longer trust. Short: it is
// cleanup on a failure path, and the server will end the session regardless.
const discardGrace = 3 * time.Second

// discard removes the connection from the pool for good and closes it, ending the
// session and with it any advisory lock the session still holds.
//
// It runs on a context detached from the caller's, because the canonical reason
// the unlock failed is that the caller's context was already cancelled — closing
// on that same dead context would fail too, and the connection would go back to
// the pool by finalizer with the lock still on it.
func (l *lease) discard() {
	raw := l.conn.Hijack() // takes the connection out of the pool; we own it now
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), discardGrace)
	defer cancel()
	_ = raw.Close(ctx)
}
