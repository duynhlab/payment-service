// Webhook dedup persistence. Record is idempotent by event_id: a redelivered
// event conflicts on the primary key and is reported as already-seen, so the
// receiver can ack it without reprocessing (at-least-once, out-of-order safe).
package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WebhookRepository records inbound provider webhooks.
type WebhookRepository struct {
	pool *pgxpool.Pool
}

// NewWebhookRepository wires the repository onto a pool.
func NewWebhookRepository(pool *pgxpool.Pool) *WebhookRepository {
	return &WebhookRepository{pool: pool}
}

// Record inserts one webhook event, correlating it to a local payment by
// provider_payment_id (NULL → 'orphaned'). It returns the STORED status in both
// cases and isNew=false when the event_id was already present (a duplicate
// delivery), so the caller acks without side effects.
func (r *WebhookRepository) Record(ctx context.Context, eventID, eventType, providerPaymentID string) (status string, isNew bool, err error) {
	// One candidate row (VALUES) LEFT JOINed to at most one matching payment, so
	// the insert always has exactly one row to attempt. On a duplicate event_id
	// the no-op DO UPDATE lets RETURNING fire, so the stored status comes back
	// either way; (xmax = 0) is true only for a fresh insert, distinguishing new
	// from duplicate.
	err = r.pool.QueryRow(ctx, `
		INSERT INTO webhook_events (event_id, event_type, provider_payment_id, payment_id, status)
		SELECT $1, $2, NULLIF($3,''), p.id,
		       CASE WHEN p.id IS NULL THEN 'orphaned' ELSE 'processed' END
		FROM (VALUES (1)) AS v(x)
		LEFT JOIN LATERAL (
			SELECT id FROM payments WHERE provider_payment_id = NULLIF($3,'') LIMIT 1
		) p ON true
		ON CONFLICT (event_id) DO UPDATE SET status = webhook_events.status
		RETURNING status, (xmax = 0) AS is_new`,
		eventID, eventType, providerPaymentID).Scan(&status, &isNew)
	if err != nil {
		return "", false, fmt.Errorf("record webhook: %w", err)
	}
	return status, isNew, nil
}
