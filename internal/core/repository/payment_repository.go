// Package repository implements Postgres persistence for payments, refunds,
// and idempotency keys. State changes go through a compare-and-swap
// (UPDATE ... WHERE status = $expected) so concurrent transitions cannot both
// win — the domain whitelist gives the good error, the CAS gives the guarantee.
package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// errCreateRefund wraps failures from the CreateRefund transaction.
const errCreateRefund = "create refund: %w"

// PaymentRepository is the persistence port used by the logic layer.
type PaymentRepository struct {
	pool *pgxpool.Pool
}

// NewPaymentRepository wires the repository onto a pgx pool.
func NewPaymentRepository(pool *pgxpool.Pool) *PaymentRepository {
	return &PaymentRepository{pool: pool}
}

const paymentColumns = `
	id, user_id, order_id, amount_minor, currency, status, capture_method,
	payment_method, COALESCE(provider_payment_id,''), COALESCE(decline_code,''),
	authorized_at, expires_at, captured_at, created_at, updated_at,
	COALESCE((SELECT SUM(r.amount_minor) FROM refunds r
	          WHERE r.payment_id = payments.id AND r.status IN ('pending','succeeded')), 0) AS refunded_minor`

func scanPayment(row pgx.Row) (*domain.Payment, error) {
	var p domain.Payment
	err := row.Scan(&p.ID, &p.UserID, &p.OrderID, &p.AmountMinor, &p.Currency,
		&p.Status, &p.CaptureMethod, &p.PaymentMethod, &p.ProviderPaymentID,
		&p.DeclineCode, &p.AuthorizedAt, &p.ExpiresAt, &p.CapturedAt,
		&p.CreatedAt, &p.UpdatedAt, &p.RefundedMinor)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan payment: %w", err)
	}
	return &p, nil
}

// Create inserts a pending payment. A duplicate order_id surfaces as
// ErrPaymentExists.
func (r *PaymentRepository) Create(ctx context.Context, p *domain.Payment) (*domain.Payment, error) {
	row := r.pool.QueryRow(ctx, `
		INSERT INTO payments (user_id, order_id, amount_minor, currency, status, capture_method, payment_method)
		VALUES ($1, $2, $3, $4, 'pending', $5, $6)
		RETURNING `+paymentColumns,
		p.UserID, p.OrderID, p.AmountMinor, p.Currency, p.CaptureMethod, p.PaymentMethod)
	created, err := scanPayment(row)
	if err != nil && isUniqueViolation(err, "uq_payments_order_id") {
		return nil, domain.ErrPaymentExists
	}
	return created, err
}

// FindByID fetches one payment scoped to its owner (userID 0 = unscoped,
// for internal/saga callers).
func (r *PaymentRepository) FindByID(ctx context.Context, id, userID int64) (*domain.Payment, error) {
	q := `SELECT ` + paymentColumns + ` FROM payments WHERE id = $1`
	args := []any{id}
	if userID != 0 {
		q += ` AND user_id = $2`
		args = append(args, userID)
	}
	return scanPayment(r.pool.QueryRow(ctx, q, args...))
}

// FindByOrderID fetches the payment attached to an order (saga replay path).
func (r *PaymentRepository) FindByOrderID(ctx context.Context, orderID int64) (*domain.Payment, error) {
	return scanPayment(r.pool.QueryRow(ctx,
		`SELECT `+paymentColumns+` FROM payments WHERE order_id = $1`, orderID))
}

// ListByUser returns a page of the user's payments plus the total count.
func (r *PaymentRepository) ListByUser(ctx context.Context, userID int64, limit, offset int) ([]domain.Payment, int, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+paymentColumns+` FROM payments
		 WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
		userID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list payments: %w", err)
	}
	defer rows.Close()

	var items []domain.Payment
	for rows.Next() {
		p, err := scanPayment(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *p)
	}
	if rows.Err() != nil {
		return nil, 0, fmt.Errorf("list payments: %w", rows.Err())
	}

	var total int
	if err := r.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM payments WHERE user_id = $1`, userID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count payments: %w", err)
	}
	return items, total, nil
}

// TransitionStatus performs the compare-and-swap state change and stamps the
// lifecycle columns for the target state. Zero rows affected means the
// expected state was gone — ErrStaleTransition.
func (r *PaymentRepository) TransitionStatus(ctx context.Context, id int64, from, to domain.Status, set map[string]any) error {
	var q strings.Builder
	q.WriteString(`UPDATE payments SET status = $1, updated_at = now()`)
	args := []any{to}
	i := 2
	allowed := []string{"provider_payment_id", "decline_code", "authorized_at", "expires_at", "captured_at"}
	for _, col := range allowed {
		if v, ok := set[col]; ok {
			fmt.Fprintf(&q, ", %s = $%d", col, i)
			args = append(args, v)
			i++
		}
	}
	// A typoed key would otherwise silently drop a lifecycle stamp.
	if matched := i - 2; matched != len(set) {
		return fmt.Errorf("transition %s->%s: unknown column in set (allowed: %v)", from, to, allowed)
	}
	fmt.Fprintf(&q, " WHERE id = $%d AND status = $%d", i, i+1)
	args = append(args, id, from)

	tag, err := r.pool.Exec(ctx, q.String(), args...)
	if err != nil {
		return fmt.Errorf("transition %s->%s: %w", from, to, err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleTransition
	}
	return nil
}

// CaptureWithLedger flips an authorized hold to captured and posts the balanced
// capture ledger transaction (debit customer_funds / credit merchant_revenue)
// in the SAME transaction, so the row and the ledger can never disagree. The
// posting rides the CAS: zero rows affected means the hold was already
// captured/gone (re-entry or race) — ErrStaleTransition, nothing is posted, so
// the ledger stays idempotent without a uniqueness index.
func (r *PaymentRepository) CaptureWithLedger(ctx context.Context, id int64, capturedAt time.Time) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("capture: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var amount int64
	var providerRef string
	err = tx.QueryRow(ctx, `
		UPDATE payments SET status = 'captured', captured_at = $2, updated_at = now()
		WHERE id = $1 AND status = 'authorized'
		RETURNING amount_minor, COALESCE(provider_payment_id,'')`,
		id, capturedAt).Scan(&amount, &providerRef)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrStaleTransition
	}
	if err != nil {
		return fmt.Errorf("capture: %w", err)
	}
	if err := postLedger(ctx, tx, ledgerCapture, id, providerRef, captureEntries(amount)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ReverseCapture compensates a capture whose provider call failed: it flips
// captured back to authorized (clearing captured_at) and posts a balanced
// reversal ledger transaction (the capture legs mirrored) in the same tx, so
// the ledger nets back to zero without ever editing a posted entry.
func (r *PaymentRepository) ReverseCapture(ctx context.Context, id int64) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("reverse capture: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var amount int64
	var providerRef string
	err = tx.QueryRow(ctx, `
		UPDATE payments SET status = 'authorized', captured_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'captured'
		RETURNING amount_minor, COALESCE(provider_payment_id,'')`,
		id).Scan(&amount, &providerRef)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrStaleTransition
	}
	if err != nil {
		return fmt.Errorf("reverse capture: %w", err)
	}
	if err := postLedger(ctx, tx, ledgerReversal, id, providerRef, reverseCaptureEntries(amount)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ExpireStaleAuthorizations flips authorized holds whose TTL passed —
// the expiry job's single query. Returns the number of holds expired.
func (r *PaymentRepository) ExpireStaleAuthorizations(ctx context.Context, now time.Time) (int64, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE payments SET status = 'expired', updated_at = now()
		WHERE status = 'authorized' AND expires_at IS NOT NULL AND expires_at < $1`, now)
	if err != nil {
		return 0, fmt.Errorf("expire authorizations: %w", err)
	}
	return tag.RowsAffected(), nil
}

const refundColumns = `id, payment_id, amount_minor, status,
	COALESCE(provider_refund_id,''), COALESCE(reason,''), created_at, updated_at`

func scanRefund(row pgx.Row) (*domain.Refund, error) {
	var ref domain.Refund
	err := row.Scan(&ref.ID, &ref.PaymentID, &ref.AmountMinor, &ref.Status,
		&ref.ProviderRefundID, &ref.Reason, &ref.CreatedAt, &ref.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &ref, nil
}

// CreateRefund inserts a pending refund IF the accumulated pending+succeeded
// refunds (including this one) stay within the captured amount. Two safety
// properties:
//   - the payment row is locked (FOR UPDATE) before the guarded insert so
//     concurrent refunds cannot both pass the amount guard (READ COMMITTED
//     snapshots each statement independently);
//   - the insert carries the client idempotency key with a partial unique
//     index, so a crash-recovery retry adopts the existing refund instead of
//     inserting a duplicate.
//
// idemKey is the user-scoped Idempotency-Key. On the guarded insert matching
// nothing, an existing refund for the same key is adopted (recovery); a truly
// absent one means the amount/state guard rejected it.
func (r *PaymentRepository) CreateRefund(ctx context.Context, paymentID, amountMinor int64, reason, idemKey string) (*domain.Refund, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf(errCreateRefund, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize refund admission per payment; the lock also confirms existence.
	var lockedID int64
	err = tx.QueryRow(ctx,
		`SELECT id FROM payments WHERE id = $1 FOR UPDATE`, paymentID).Scan(&lockedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrRefundRejected
	}
	if err != nil {
		return nil, fmt.Errorf("create refund: lock payment: %w", err)
	}

	ref, err := scanRefund(tx.QueryRow(ctx, `
		INSERT INTO refunds (payment_id, amount_minor, reason, idempotency_key)
		SELECT p.id, $2::bigint, $3::text, $4::text FROM payments p
		WHERE p.id = $1 AND p.status IN ('captured','refunded')
		  AND $2::bigint + COALESCE((SELECT SUM(r.amount_minor) FROM refunds r
		                             WHERE r.payment_id = p.id AND r.status IN ('pending','succeeded')), 0)
		      <= p.amount_minor
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING `+refundColumns,
		paymentID, amountMinor, reason, idemKey))

	if errors.Is(err, pgx.ErrNoRows) {
		// No row inserted: either the client key already has a refund (adopt it
		// — crash-recovery) or the amount/state guard genuinely rejected it.
		existing, exErr := scanRefund(tx.QueryRow(ctx,
			`SELECT `+refundColumns+` FROM refunds WHERE idempotency_key = $1`, idemKey))
		if errors.Is(exErr, pgx.ErrNoRows) {
			return nil, domain.ErrRefundRejected
		}
		if exErr != nil {
			return nil, fmt.Errorf(errCreateRefund, exErr)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf(errCreateRefund, err)
		}
		return existing, nil
	}
	if err != nil {
		return nil, fmt.Errorf(errCreateRefund, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf(errCreateRefund, err)
	}
	return ref, nil
}

// SettleRefund marks a pending refund succeeded/failed and, when refunds now
// cover the full amount, flips the payment to refunded (derived -> stored).
func (r *PaymentRepository) SettleRefund(ctx context.Context, refundID int64, status domain.RefundStatus, providerRefundID string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("settle refund: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var paymentID, amount int64
	err = tx.QueryRow(ctx, `
		UPDATE refunds SET status = $2, provider_refund_id = NULLIF($3,''), updated_at = now()
		WHERE id = $1 AND status = 'pending' RETURNING payment_id, amount_minor`,
		refundID, status, providerRefundID).Scan(&paymentID, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("settle refund: %w", err)
	}

	if status == domain.RefundSucceeded {
		// Post the refund ledger (money returned to the customer) in the same tx
		// as the settle — it rides the pending->succeeded CAS above, so a
		// re-settle finds no pending row and never double-posts.
		if err := postLedger(ctx, tx, ledgerRefund, paymentID, providerRefundID, reverseCaptureEntries(amount)); err != nil {
			return err
		}
		// Flip captured -> refunded only when succeeded refunds reach 100%.
		if _, err := tx.Exec(ctx, `
			UPDATE payments p SET status = 'refunded', updated_at = now()
			WHERE p.id = $1 AND p.status = 'captured'
			  AND p.amount_minor <= (SELECT COALESCE(SUM(r.amount_minor),0) FROM refunds r
			                         WHERE r.payment_id = p.id AND r.status = 'succeeded')`,
			paymentID); err != nil {
			return fmt.Errorf("flip refunded: %w", err)
		}
	}
	return tx.Commit(ctx)
}
