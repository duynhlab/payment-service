// Package repository implements Postgres persistence for payments, refunds,
// and idempotency keys. State changes go through a compare-and-swap
// (UPDATE ... WHERE status = $expected) so concurrent transitions cannot both
// win — the domain whitelist gives the good error, the CAS gives the guarantee.
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

// ErrNotFound is returned when a payment/refund does not exist (or is not
// visible to the requesting user).
var ErrNotFound = errors.New("not found")

// ErrStaleTransition is returned when the CAS update matched no row: the
// payment moved to another state concurrently. Callers map it to
// 409 INVALID_TRANSITION after re-reading.
var ErrStaleTransition = errors.New("payment state changed concurrently")

// ErrPaymentExists is returned when an order already has a payment
// (unique index uq_payments_order_id).
var ErrPaymentExists = errors.New("order already has a payment")

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
		return nil, ErrNotFound
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
		return nil, ErrPaymentExists
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
	q := `UPDATE payments SET status = $1, updated_at = now()`
	args := []any{to}
	i := 2
	for _, col := range []string{"provider_payment_id", "decline_code", "authorized_at", "expires_at", "captured_at"} {
		if v, ok := set[col]; ok {
			q += fmt.Sprintf(", %s = $%d", col, i)
			args = append(args, v)
			i++
		}
	}
	q += fmt.Sprintf(" WHERE id = $%d AND status = $%d", i, i+1)
	args = append(args, id, from)

	tag, err := r.pool.Exec(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("transition %s->%s: %w", from, to, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleTransition
	}
	return nil
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

// CreateRefund inserts a pending refund IF the accumulated pending+succeeded
// refunds (including this one) stay within the captured amount. The check and
// the insert are one statement, so concurrent refunds cannot oversubscribe.
func (r *PaymentRepository) CreateRefund(ctx context.Context, paymentID, amountMinor int64, reason string) (*domain.Refund, error) {
	row := r.pool.QueryRow(ctx, `
		INSERT INTO refunds (payment_id, amount_minor, reason)
		SELECT p.id, $2, $3 FROM payments p
		WHERE p.id = $1 AND p.status IN ('captured','refunded')
		  AND $2 + COALESCE((SELECT SUM(r.amount_minor) FROM refunds r
		                     WHERE r.payment_id = p.id AND r.status IN ('pending','succeeded')), 0)
		      <= p.amount_minor
		RETURNING id, payment_id, amount_minor, status, COALESCE(provider_refund_id,''),
		          COALESCE(reason,''), created_at, updated_at`,
		paymentID, amountMinor, reason)

	var ref domain.Refund
	err := row.Scan(&ref.ID, &ref.PaymentID, &ref.AmountMinor, &ref.Status,
		&ref.ProviderRefundID, &ref.Reason, &ref.CreatedAt, &ref.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrRefundRejected
	}
	if err != nil {
		return nil, fmt.Errorf("create refund: %w", err)
	}
	return &ref, nil
}

// ErrRefundRejected means the guarded insert matched nothing: payment not
// capturable/refundable or the amount would exceed the capture.
var ErrRefundRejected = errors.New("refund rejected: not refundable or exceeds captured amount")

// SettleRefund marks a pending refund succeeded/failed and, when refunds now
// cover the full amount, flips the payment to refunded (derived -> stored).
func (r *PaymentRepository) SettleRefund(ctx context.Context, refundID int64, status domain.RefundStatus, providerRefundID string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("settle refund: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var paymentID int64
	err = tx.QueryRow(ctx, `
		UPDATE refunds SET status = $2, provider_refund_id = NULLIF($3,''), updated_at = now()
		WHERE id = $1 AND status = 'pending' RETURNING payment_id`,
		refundID, status, providerRefundID).Scan(&paymentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("settle refund: %w", err)
	}

	if status == domain.RefundSucceeded {
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
