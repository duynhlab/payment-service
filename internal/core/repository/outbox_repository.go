// Outbox persistence: enqueue (inside a money-movement transaction) plus the
// relay's read/mark side. Writing the event in the same tx as the state change
// closes the dual-write gap; the relay then delivers at-least-once.
package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// enqueueOutbox writes one event via q — pass the enclosing tx so the event
// commits atomically with the state change. Payload is bound as $2::jsonb with
// string() because []byte into jsonb fails under the simple query protocol.
func enqueueOutbox(ctx context.Context, q dbExec, eventType string, payload []byte) error {
	if _, err := q.Exec(ctx,
		`INSERT INTO payment_outbox (event_type, payload) VALUES ($1, $2::jsonb)`,
		eventType, string(payload)); err != nil {
		return fmt.Errorf("enqueue outbox: %w", err)
	}
	return nil
}

// paymentEventPayload is the minimal JSON body shared by the money-movement
// events: which payment moved, and by how much. NOTE: this payload is written
// to logs by the relay's delivery sink — keep it to non-sensitive identifiers
// and amounts (no card-like data, no PII).
func paymentEventPayload(paymentID, amountMinor int64) []byte {
	b, _ := json.Marshal(struct {
		PaymentID   int64 `json:"payment_id"`
		AmountMinor int64 `json:"amount_minor"`
	}{paymentID, amountMinor}) // scalar struct — cannot fail
	return b
}

// OutboxRepository is the relay's read/mark side.
type OutboxRepository struct {
	pool *pgxpool.Pool
}

// NewOutboxRepository wires the outbox repository onto a pool.
func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

// FetchUnpublished returns up to limit unpublished events ordered by id
// (best-effort FIFO: id is insert-order, not commit-order, so a lower id that
// commits later is simply picked up on a later tick — never skipped, because
// liveness keys on published_at IS NULL rather than a high-water mark).
//
// Single-writer assumption: there is no row claim (FOR UPDATE SKIP LOCKED) or
// leader gate, so running the relay on multiple replicas would deliver each
// event once per replica. That is harmless for the P2 log sink (duplicate log
// lines) but MUST be fenced — claim rows or elect a leader — before a real
// broker sink ships or the service scales past one replica. Downstream
// consumers must dedupe on the event id regardless (delivery is at-least-once).
func (r *OutboxRepository) FetchUnpublished(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, event_type, payload, created_at
		FROM payment_outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch outbox: %w", err)
	}
	defer rows.Close()

	var events []domain.OutboxEvent
	for rows.Next() {
		var e domain.OutboxEvent
		if err := rows.Scan(&e.ID, &e.EventType, &e.Payload, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		events = append(events, e)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("fetch outbox: %w", rows.Err())
	}
	return events, nil
}

// MarkPublished stamps published_at on the given events. Called after delivery,
// so a crash before it leaves the events for redelivery (at-least-once).
func (r *OutboxRepository) MarkPublished(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := r.pool.Exec(ctx,
		`UPDATE payment_outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}

// ReapPublished deletes published events older than ttl, bounding table growth
// (the audit trail lives in the ledger, not here). Returns the number removed.
func (r *OutboxRepository) ReapPublished(ctx context.Context, ttl time.Duration) (int64, error) {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM payment_outbox WHERE published_at IS NOT NULL AND published_at < $1`,
		time.Now().Add(-ttl))
	if err != nil {
		return 0, fmt.Errorf("reap outbox: %w", err)
	}
	return tag.RowsAffected(), nil
}
