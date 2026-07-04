package domain

import "time"

// Domain event types written to the transactional outbox. Each corresponds to a
// settled money movement (it rides the same transaction as the ledger posting).
const (
	EventPaymentCaptured = "payment.captured"
	EventPaymentRefunded = "payment.refunded"
	EventCaptureReversed = "payment.capture_reversed"
)

// OutboxEvent is one queued domain event: an event_type plus an opaque JSON
// payload. The relay reads unpublished events, delivers them, and stamps them
// published.
type OutboxEvent struct {
	ID        int64
	EventType string
	Payload   []byte
	CreatedAt time.Time
}
