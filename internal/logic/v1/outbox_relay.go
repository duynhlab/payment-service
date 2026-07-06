package v1

import (
	"context"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// claimBudget bounds the whole claim (select → deliver → mark → commit). It runs
// on a context detached from the tick deadline (see Relay) so a batch that spent
// the tick delivering can still commit what it delivered instead of rolling back
// and re-delivering the same prefix forever.
const claimBudget = 30 * time.Second

// OutboxRepo is the relay's persistence port (implemented by
// repository.OutboxRepository).
type OutboxRepo interface {
	// ClaimUnpublished locks a batch FOR UPDATE SKIP LOCKED, invokes deliver, and
	// marks the returned ids published — all in one tx (multi-replica safe).
	ClaimUnpublished(ctx context.Context, limit int, deliver func([]domain.OutboxEvent) []int64) (int64, error)
	ReapPublished(ctx context.Context, ttl time.Duration) (int64, error)
}

// Publisher delivers a single outbox event to its sink. P2 ships a log sink; a
// real broker swaps in behind this interface without touching the relay.
type Publisher interface {
	Publish(ctx context.Context, e domain.OutboxEvent) error
}

// OutboxRelay drains the transactional outbox: claim a batch (FOR UPDATE SKIP
// LOCKED), deliver each, then mark the delivered ones published — all in one tx,
// so the claim is multi-replica safe (a second relay skips the locked rows).
// Delivery happens before the mark commits, so a crash in between redelivers
// rather than drops — at-least-once; consumers must dedupe on the event id.
//
// Caveat for a real broker sink: delivery runs while the claim tx (and its pool
// connection + row locks) is held, up to claimBudget. Harmless for the P2 log
// sink (instant), but a network broker would hold DB locks across network I/O —
// reconsider (claim-column + no long tx, or a shorter budget) before swapping the
// sink or raising the batch size, to avoid lock contention / pool exhaustion.
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
	// Detach from the tick deadline and bound the whole claim so a slow sink
	// can't blow the tick mid-tx and force a rollback that re-delivers forever.
	claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimBudget)
	defer cancel()

	var pubErr error
	// deliver publishes in id order, stops at the first failure, and returns the
	// delivered prefix; ClaimUnpublished marks+commits exactly that prefix.
	n, err := r.repo.ClaimUnpublished(claimCtx, limit, func(events []domain.OutboxEvent) []int64 {
		published := make([]int64, 0, len(events))
		for _, e := range events {
			if perr := r.pub.Publish(claimCtx, e); perr != nil {
				pubErr = perr
				break
			}
			published = append(published, e.ID)
		}
		return published
	})
	if err != nil {
		return n, err
	}
	return n, pubErr
}

// ReapPublished deletes published events older than ttl (delegates to the repo).
func (r *OutboxRelay) ReapPublished(ctx context.Context, ttl time.Duration) (int64, error) {
	return r.repo.ReapPublished(ctx, ttl)
}
