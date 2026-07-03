package domain

import "time"

// IdempotencyKey is one claimed request: its identity, in-flight lock, the
// payment it created (checkpointed for crash-recovery re-entry), and — once
// finished — the cached response that replays verbatim.
type IdempotencyKey struct {
	ID            int64
	UserID        int64
	Key           string
	RequestMethod string
	RequestPath   string
	RequestHash   string
	LockedAt      time.Time
	PaymentID     *int64
	ResponseCode  *int
	ResponseBody  []byte
	CreatedAt     time.Time
}

// Finished reports whether the key holds a cached response ready to replay.
func (k *IdempotencyKey) Finished() bool { return k.ResponseCode != nil }
