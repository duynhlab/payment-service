package domain

import "time"

// Recovery points for the multi-phase idempotent flow.
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
