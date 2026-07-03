package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// IdempotencyRepository persists idempotency keys. The UNIQUE(user_id, idem_key)
// index is the race-free claim: INSERT ... ON CONFLICT DO NOTHING, and
// rows-affected decides the winner.
type IdempotencyRepository struct {
	pool         *pgxpool.Pool
	lockTakeover time.Duration
}

// NewIdempotencyRepository wires the repository; lockTakeover is how stale an
// in-flight lock must be before a new attempt may take it over.
func NewIdempotencyRepository(pool *pgxpool.Pool, lockTakeover time.Duration) *IdempotencyRepository {
	return &IdempotencyRepository{pool: pool, lockTakeover: lockTakeover}
}

const idemColumns = `id, user_id, idem_key, request_method, request_path, request_hash,
	locked_at, recovery_point, payment_id, response_code, response_body, created_at`

func scanKey(row pgx.Row) (*domain.IdempotencyKey, error) {
	var k domain.IdempotencyKey
	err := row.Scan(&k.ID, &k.UserID, &k.Key, &k.RequestMethod, &k.RequestPath,
		&k.RequestHash, &k.LockedAt, &k.RecoveryPoint, &k.PaymentID,
		&k.ResponseCode, &k.ResponseBody, &k.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan idempotency key: %w", err)
	}
	return &k, nil
}

// Claim atomically claims (userID, key) for this request. Outcomes:
//   - fresh claim               -> (key, true, nil): caller proceeds from RecoveryPoint
//   - finished + same hash      -> (key, false, nil): caller replays the cached response
//   - finished/in-flight, other hash -> ErrKeyConflict
//   - in-flight, fresh lock     -> ErrKeyLocked
//   - in-flight, stale lock     -> (key, true, nil): TAKEOVER — caller re-drives
//     from the recorded RecoveryPoint (provider-side key makes that safe)
func (r *IdempotencyRepository) Claim(ctx context.Context, userID int64, key, method, path, hash string) (*domain.IdempotencyKey, bool, error) {
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO idempotency_keys (user_id, idem_key, request_method, request_path, request_hash)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (user_id, idem_key) DO NOTHING`,
		userID, key, method, path, hash)
	if err != nil {
		return nil, false, fmt.Errorf("claim idempotency key: %w", err)
	}

	existing, err := scanKey(r.pool.QueryRow(ctx,
		`SELECT `+idemColumns+` FROM idempotency_keys WHERE user_id = $1 AND idem_key = $2`,
		userID, key))
	if err != nil {
		return nil, false, err
	}

	if tag.RowsAffected() == 1 {
		return existing, true, nil // fresh claim — we won the index race
	}

	// A key identifies ONE request: same key on a different endpoint or with a
	// different body is a conflict, never a replay. Path+method scoping keeps
	// a create-payment key from ever answering a refund (or any future
	// endpoint whose body shape happens to collide).
	if existing.RequestHash != hash || existing.RequestPath != path || existing.RequestMethod != method {
		return nil, false, domain.ErrKeyConflict
	}
	if existing.Finished() {
		return existing, false, nil // replay
	}

	// In-flight: fresh lock waits; stale lock is taken over.
	if time.Since(existing.LockedAt) < r.lockTakeover {
		return nil, false, domain.ErrKeyLocked
	}
	took, err := scanKey(r.pool.QueryRow(ctx, `
		UPDATE idempotency_keys SET locked_at = now()
		WHERE id = $1 AND locked_at = $2 AND response_code IS NULL
		RETURNING `+idemColumns,
		existing.ID, existing.LockedAt))
	if errors.Is(err, domain.ErrNotFound) {
		return nil, false, domain.ErrKeyLocked // someone else took it over first
	}
	if err != nil {
		return nil, false, err
	}
	return took, true, nil
}

// Advance records checkpoint progress (recovery point + optional subject
// payment) so a takeover re-enters at the right phase.
func (r *IdempotencyRepository) Advance(ctx context.Context, id int64, point string, paymentID *int64) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE idempotency_keys SET recovery_point = $2, payment_id = COALESCE($3, payment_id), locked_at = now()
		WHERE id = $1`, id, point, paymentID)
	if err != nil {
		return fmt.Errorf("advance idempotency key: %w", err)
	}
	return nil
}

// Release ages the lock so an immediate same-key retry is treated as a
// takeover instead of ErrKeyLocked — used when an attempt fails transiently
// and the caller is told to retry. Only affects still-in-flight keys.
func (r *IdempotencyRepository) Release(ctx context.Context, id int64) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE idempotency_keys SET locked_at = 'epoch' WHERE id = $1 AND response_code IS NULL`, id)
	if err != nil {
		return fmt.Errorf("release idempotency key: %w", err)
	}
	return nil
}

// Finish stores the cached response and marks the key finished. It is the
// final checkpoint and the replay source for every later duplicate. The body
// binds as text with an explicit ::jsonb cast — under the simple query
// protocol (PgBouncer/PgDog pools) a raw []byte parameter is sent as a bytea
// hex literal, which jsonb rejects.
func (r *IdempotencyRepository) Finish(ctx context.Context, id int64, code int, body []byte) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE idempotency_keys SET recovery_point = $2, response_code = $3, response_body = $4::jsonb
		WHERE id = $1`, id, domain.RecoveryFinished, code, string(body))
	if err != nil {
		return fmt.Errorf("finish idempotency key: %w", err)
	}
	return nil
}

// Reap deletes keys older than ttl (Stripe's 24h window). Returns rows removed.
func (r *IdempotencyRepository) Reap(ctx context.Context, ttl time.Duration) (int64, error) {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM idempotency_keys WHERE created_at < now() - $1::interval`,
		fmt.Sprintf("%d seconds", int(ttl.Seconds())))
	if err != nil {
		return 0, fmt.Errorf("reap idempotency keys: %w", err)
	}
	return tag.RowsAffected(), nil
}
