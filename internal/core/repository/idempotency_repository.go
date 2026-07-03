package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Recovery points for the multi-phase idempotent flow (RFC-0010 §Idempotency).
// The provider call happens OUTSIDE any DB transaction; a crash between
// checkpoints is recovered by re-entering at the recorded phase, and the
// provider-side idempotency key makes the re-driven call safe to repeat.
const (
	RecoveryStarted        = "started"
	RecoveryProviderCalled = "provider_called"
	RecoveryFinished       = "finished"
)

// IdempotencyKey is one claimed request: its identity, progress, and (once
// finished) the cached response that replays verbatim.
type IdempotencyKey struct {
	ID            int64
	UserID        int64
	Key           string
	RequestMethod string
	RequestPath   string
	RequestHash   string
	LockedAt      time.Time
	RecoveryPoint string
	PaymentID     *int64
	ResponseCode  *int
	ResponseBody  []byte
	CreatedAt     time.Time
}

// Finished reports whether the key holds a cached response ready to replay.
func (k *IdempotencyKey) Finished() bool { return k.ResponseCode != nil }

// ErrKeyConflict is returned when the same key arrives with a different
// request hash — a key identifies one request, not one endpoint. Maps to
// 409 IDEMPOTENCY_CONFLICT.
var ErrKeyConflict = errors.New("idempotency key reused with a different request")

// ErrKeyLocked is returned while another attempt with the same key is
// in-flight and not yet stale. Maps to 409 + Retry-After.
var ErrKeyLocked = errors.New("idempotency key locked by an in-flight request")

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

func scanKey(row pgx.Row) (*IdempotencyKey, error) {
	var k IdempotencyKey
	err := row.Scan(&k.ID, &k.UserID, &k.Key, &k.RequestMethod, &k.RequestPath,
		&k.RequestHash, &k.LockedAt, &k.RecoveryPoint, &k.PaymentID,
		&k.ResponseCode, &k.ResponseBody, &k.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
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
func (r *IdempotencyRepository) Claim(ctx context.Context, userID int64, key, method, path, hash string) (*IdempotencyKey, bool, error) {
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

	if existing.RequestHash != hash {
		return nil, false, ErrKeyConflict
	}
	if existing.Finished() {
		return existing, false, nil // replay
	}

	// In-flight: fresh lock waits; stale lock is taken over.
	if time.Since(existing.LockedAt) < r.lockTakeover {
		return nil, false, ErrKeyLocked
	}
	took, err := scanKey(r.pool.QueryRow(ctx, `
		UPDATE idempotency_keys SET locked_at = now()
		WHERE id = $1 AND locked_at = $2 AND response_code IS NULL
		RETURNING `+idemColumns,
		existing.ID, existing.LockedAt))
	if errors.Is(err, ErrNotFound) {
		return nil, false, ErrKeyLocked // someone else took it over first
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

// Finish stores the cached response and marks the key finished. It is the
// final checkpoint and the replay source for every later duplicate. The body
// binds as text with an explicit ::jsonb cast — under the simple query
// protocol (PgBouncer/PgDog pools) a raw []byte parameter is sent as a bytea
// hex literal, which jsonb rejects.
func (r *IdempotencyRepository) Finish(ctx context.Context, id int64, code int, body []byte) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE idempotency_keys SET recovery_point = $2, response_code = $3, response_body = $4::jsonb
		WHERE id = $1`, id, RecoveryFinished, code, string(body))
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
