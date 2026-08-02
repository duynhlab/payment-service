package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// pgUniqueViolation is Postgres' SQLSTATE for a unique constraint violation.
const pgUniqueViolation = "23505"

// attemptColumns is the read projection shared by the worklist queries.
const attemptColumns = `id, payment_id, operation, outcome_class,
	COALESCE(provider_ref,''), COALESCE(provider_status,''),
	COALESCE(idempotency_key,''), refund_id, created_at`

// AttemptRepository writes and closes the per-round-trip attempt log (RFC-0021
// phase 6). An attempt records what one provider call SAID, so `outcome_class`
// is never rewritten — history does not change. `resolved_at` is the only field
// a resolution touches: it means "a later round-trip told us how this one
// actually ended", and the later round-trip is its own row.
type AttemptRepository struct {
	pool *pgxpool.Pool
}

// NewAttemptRepository wraps a pool.
func NewAttemptRepository(pool *pgxpool.Pool) *AttemptRepository {
	return &AttemptRepository{pool: pool}
}

// Record appends one attempt. It is deliberately NOT transactional with the
// state change it accompanies: an attempt that failed to record must never
// prevent the money state from being written. The converse does not hold and is
// not claimed — the attempt is written before the state change, so a crash
// between them leaves an open UNKNOWN row for a payment that was never parked.
// That direction is safe: it over-reports doubt, and the resolver's first act is
// to re-read the payment.
//
// A second SUCCESS capture for one payment is refused by
// uq_payment_attempts_one_success_capture and surfaces as
// domain.ErrDuplicateAttempt rather than an opaque driver error, so the caller
// can tell "another writer already recorded this" from "the write broke".
func (r *AttemptRepository) Record(ctx context.Context, a domain.Attempt) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx, `
		INSERT INTO payment_attempts
			(payment_id, operation, outcome_class, provider_ref, provider_status,
			 idempotency_key, refund_id)
		VALUES ($1, $2, $3, NULLIF($4,''), NULLIF($5,''), NULLIF($6,''), $7)
		RETURNING id`,
		a.PaymentID, string(a.Operation), string(a.Outcome),
		a.ProviderRef, a.ProviderStatus, a.IdempotencyKey, a.RefundID,
	).Scan(&id)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
		return 0, fmt.Errorf("record attempt for payment %d: %w", a.PaymentID, domain.ErrDuplicateAttempt)
	}
	if err != nil {
		return 0, fmt.Errorf("record attempt: %w", err)
	}
	return id, nil
}

// Resolve closes an open UNKNOWN attempt once a later round-trip has told us how
// it really ended. It stamps resolved_at and nothing else: the row keeps
// UNKNOWN because that is what this call returned, and the decided answer lives
// in the round-trip that produced it.
//
// The guards keep it honest — only an UNKNOWN row can be closed (a decided
// attempt was never in doubt), and only an unresolved one, so a racing resolver
// pass is a no-op rather than a silent re-stamp. Both cases return ErrNotFound;
// callers treat that as "someone else already closed it".
func (r *AttemptRepository) Resolve(ctx context.Context, attemptID int64, at time.Time) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE payment_attempts
		   SET resolved_at = $2
		 WHERE id = $1
		   AND outcome_class = 'UNKNOWN'
		   AND resolved_at IS NULL`,
		attemptID, at)
	if err != nil {
		return fmt.Errorf("resolve attempt %d: %w", attemptID, err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

// ListOpen returns unresolved UNKNOWN attempts oldest-first — the doubt
// worklist, in the order a human or the resolver should work it.
func (r *AttemptRepository) ListOpen(ctx context.Context, limit int) ([]domain.Attempt, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+attemptColumns+`
		  FROM payment_attempts
		 WHERE outcome_class = 'UNKNOWN' AND resolved_at IS NULL
		 ORDER BY created_at, id
		 LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list open attempts: %w", err)
	}
	return scanAttempts(rows)
}

// ListOpenForPayment returns one payment's unresolved doubt, oldest-first. This
// is the request path's query: an operation on a parked payment resolves that
// payment's own doubt before deciding what to do, so a caller never has to wait
// for a background sweep to make progress.
func (r *AttemptRepository) ListOpenForPayment(ctx context.Context, paymentID int64) ([]domain.Attempt, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+attemptColumns+`
		  FROM payment_attempts
		 WHERE payment_id = $1 AND outcome_class = 'UNKNOWN' AND resolved_at IS NULL
		 ORDER BY created_at, id`, paymentID)
	if err != nil {
		return nil, fmt.Errorf("list open attempts for payment %d: %w", paymentID, err)
	}
	return scanAttempts(rows)
}

func scanAttempts(rows pgx.Rows) ([]domain.Attempt, error) {
	defer rows.Close()

	var out []domain.Attempt
	for rows.Next() {
		var a domain.Attempt
		if err := rows.Scan(&a.ID, &a.PaymentID, &a.Operation, &a.Outcome,
			&a.ProviderRef, &a.ProviderStatus, &a.IdempotencyKey,
			&a.RefundID, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan open attempt: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// CountOpen is the gauge's query: how much unresolved doubt exists right now.
// Unwindowed on purpose — doubt about money must not age out of view.
func (r *AttemptRepository) CountOpen(ctx context.Context) (int64, error) {
	var n int64
	if err := r.pool.QueryRow(ctx, `
		SELECT count(*) FROM payment_attempts
		 WHERE outcome_class = 'UNKNOWN' AND resolved_at IS NULL`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count open attempts: %w", err)
	}
	return n, nil
}

// OldestOpenAge returns how long the oldest unresolved doubt has been open, or
// zero when there is none — the age is what makes it alertable, since a single
// open attempt is normal and an old one is not.
func (r *AttemptRepository) OldestOpenAge(ctx context.Context, now time.Time) (time.Duration, error) {
	var oldest *time.Time
	if err := r.pool.QueryRow(ctx, `
		SELECT min(created_at) FROM payment_attempts
		 WHERE outcome_class = 'UNKNOWN' AND resolved_at IS NULL`).Scan(&oldest); err != nil {
		return 0, fmt.Errorf("oldest open attempt: %w", err)
	}
	if oldest == nil {
		return 0, nil
	}
	return now.Sub(*oldest), nil
}
