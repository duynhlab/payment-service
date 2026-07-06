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

// ClaimUnpublished selects up to limit unpublished events ordered by id, locking
// them `FOR UPDATE SKIP LOCKED` inside a transaction, hands them to deliver, marks
// the ids deliver returns as published, and commits — all in the same tx, so the
// row lock is held across delivery. Another relay instance (or replica) skips the
// locked rows, so the relay is safe to run on more than one replica.
//
// deliver returns the prefix of ids it actually delivered; a delivery failure
// stops the batch and the delivered prefix is still committed (at-least-once —
// downstream consumers dedupe on the event id). Returns the number marked
// published. (id is insert-order, not commit-order — a lower id that commits
// later is simply claimed on a later tick, never skipped, because liveness keys
// on published_at IS NULL.)
func (r *OutboxRepository) ClaimUnpublished(ctx context.Context, limit int, deliver func([]domain.OutboxEvent) []int64) (int64, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful commit

	rows, err := tx.Query(ctx, `
		SELECT id, event_type, payload, created_at
		FROM payment_outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, fmt.Errorf("claim outbox: %w", err)
	}
	var events []domain.OutboxEvent
	for rows.Next() {
		var e domain.OutboxEvent
		if scanErr := rows.Scan(&e.ID, &e.EventType, &e.Payload, &e.CreatedAt); scanErr != nil {
			rows.Close()
			return 0, fmt.Errorf("scan outbox: %w", scanErr)
		}
		events = append(events, e)
	}
	rows.Close() // must close before running Exec on the same tx
	if rows.Err() != nil {
		return 0, fmt.Errorf("claim outbox: %w", rows.Err())
	}
	if len(events) == 0 {
		return 0, nil
	}

	delivered := deliver(events)
	if len(delivered) > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE payment_outbox SET published_at = now() WHERE id = ANY($1)`, delivered); err != nil {
			return 0, fmt.Errorf("mark outbox published: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit outbox claim: %w", err)
	}
	return int64(len(delivered)), nil
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
