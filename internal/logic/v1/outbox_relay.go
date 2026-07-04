package v1

import (
	"context"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// markGrace bounds the MarkPublished call. It runs on a context detached from
// the tick deadline (see Relay) so a batch that spent the whole tick delivering
// can still record what it delivered.
const markGrace = 5 * time.Second

// OutboxRepo is the relay's persistence port (implemented by
// repository.OutboxRepository).
type OutboxRepo interface {
	FetchUnpublished(ctx context.Context, limit int) ([]domain.OutboxEvent, error)
	MarkPublished(ctx context.Context, ids []int64) error
	ReapPublished(ctx context.Context, ttl time.Duration) (int64, error)
}

// Publisher delivers a single outbox event to its sink. P2 ships a log sink; a
// real broker swaps in behind this interface without touching the relay.
type Publisher interface {
	Publish(ctx context.Context, e domain.OutboxEvent) error
}

// OutboxRelay drains the transactional outbox: fetch unpublished events,
// deliver each, then mark the delivered ones published. Delivery happens before
// the mark, so a crash in between redelivers rather than drops — at-least-once.
// Consumers must therefore dedupe on the event id. See OutboxRepo.FetchUnpublished
// for the single-writer assumption (fence before scaling past one replica).
type OutboxRelay struct {
	repo OutboxRepo
	pub  Publisher
}

// NewOutboxRelay wires the relay onto its port and sink.
func NewOutboxRelay(repo OutboxRepo, pub Publisher) *OutboxRelay {
	return &OutboxRelay{repo: repo, pub: pub}
}

// Relay publishes up to limit events and returns how many were delivered. A
// delivery failure stops the batch (order is preserved) and leaves the rest for
// the next tick; the events delivered before it are still marked published.
func (r *OutboxRelay) Relay(ctx context.Context, limit int) (int64, error) {
	events, err := r.repo.FetchUnpublished(ctx, limit)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, nil
	}

	published := make([]int64, 0, len(events))
	var pubErr error
	for _, e := range events {
		if err := r.pub.Publish(ctx, e); err != nil {
			pubErr = err
			break
		}
		published = append(published, e.ID)
	}

	// Mark on a context detached from the tick deadline: if delivery consumed
	// the whole tick, ctx may already be expired, but we must still record what
	// went out — otherwise a persistently slow sink re-delivers the same prefix
	// every tick and never makes progress.
	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), markGrace)
	defer cancel()
	if err := r.repo.MarkPublished(markCtx, published); err != nil {
		return int64(len(published)), err
	}
	return int64(len(published)), pubErr
}

// ReapPublished deletes published events older than ttl (delegates to the repo).
func (r *OutboxRelay) ReapPublished(ctx context.Context, ttl time.Duration) (int64, error) {
	return r.repo.ReapPublished(ctx, ttl)
}
