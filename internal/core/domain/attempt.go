package domain

import "time"

// AttemptOperation names the provider round-trip an attempt records.
type AttemptOperation string

const (
	AttemptAuthorize AttemptOperation = "authorize"
	AttemptCapture   AttemptOperation = "capture"
	AttemptVoid      AttemptOperation = "void"
	AttemptRefund    AttemptOperation = "refund"
)

// OutcomeClass is what the provider's answer MEANT, in the only four shapes the
// caller needs to act on (RFC-0021 phase 6).
//
// The load-bearing split is decided vs undecided, not success vs failure:
// BUSINESS_DECLINE and RETRYABLE_FAILURE are both answers, and only UNKNOWN
// leaves an intent open. Code that branches on "was it a decline" instead of
// "was it decided" is the shape that produced this phase's money bugs.
type OutcomeClass string

const (
	// OutcomeSuccess — the provider did it and said so.
	OutcomeSuccess OutcomeClass = "SUCCESS"
	// OutcomeBusinessDecline — the provider refused. Decided; no money moved.
	OutcomeBusinessDecline OutcomeClass = "BUSINESS_DECLINE"
	// OutcomeRetryableFailure — the provider gave a decided error that a retry
	// may pass (rate limit, explicit ask-again). Decided about THIS attempt.
	OutcomeRetryableFailure OutcomeClass = "RETRYABLE_FAILURE"
	// OutcomeUnknown — no answer. The effect may have landed. This is the only
	// class that may leave a payment `processing`, and the only one a reconciler
	// has to resolve.
	OutcomeUnknown OutcomeClass = "UNKNOWN"
)

// Decided reports whether the class is an answer. Callers use this instead of
// testing for a decline, so a new decided class can never be mistaken for doubt.
func (o OutcomeClass) Decided() bool { return o != OutcomeUnknown }

// Attempt is one provider round-trip. Rows are append-only; ResolvedAt is the
// single field a later resolution writes.
type Attempt struct {
	ID        int64
	PaymentID int64
	Operation AttemptOperation
	Outcome   OutcomeClass
	// ProviderRef is the charge or refund this round-trip acted on, where we had
	// one to send. It is empty exactly when the round-trip was the call that would
	// have minted it — an authorize whose answer never arrived — and that absence
	// is why resolving an authorize needs the idempotency key instead.
	ProviderRef string
	// ProviderStatus is the provider's code/status verbatim, for reconciliation
	// and for the operator reading the row later.
	ProviderStatus string
	// IdempotencyKey is the provider-facing key this round-trip used. Resolution
	// re-drives the identical operation under this same key so the provider
	// replays its original answer instead of performing the work again.
	IdempotencyKey string
	// RefundID ties a refund attempt to its refund row; nil for intent-level
	// operations.
	RefundID  *int64
	CreatedAt time.Time
	// ResolvedAt is set when an UNKNOWN attempt has been settled against the
	// provider. An UNKNOWN attempt with no ResolvedAt is the open worklist.
	ResolvedAt *time.Time
}

// Open reports whether this attempt still represents unresolved doubt.
func (a Attempt) Open() bool { return a.Outcome == OutcomeUnknown && a.ResolvedAt == nil }
