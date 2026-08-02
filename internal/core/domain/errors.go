package domain

import "errors"

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

// ErrRefundRejected means the guarded insert matched nothing: payment not
// capturable/refundable or the amount would exceed the capture.
var ErrRefundRejected = errors.New("refund rejected: not refundable or exceeds captured amount")

// ErrOutcomeUnknown is returned when a provider operation's fate is not known:
// the effect may have landed, so nothing has been undone and the intent is
// parked in `processing` until the provider is asked. Callers must treat it as
// neither success nor failure — retrying with the same key is safe, and giving
// up means leaving a human a resolvable worklist item, not a lost payment.
var ErrOutcomeUnknown = errors.New("provider outcome unknown: intent parked")

// ErrRefundNotSettled is returned when a refund's provider outcome is UNKNOWN
// (timeout, transport error, unclassifiable status): the money may or may not
// have moved. The refund is parked `processing`, which still reserves its
// amount against a second refund of the same money, and the caller MUST be able
// to retry with the same idempotency key — which is why this is an error rather
// than a success carrying status "failed". Retryable.
var ErrRefundNotSettled = errors.New("refund outcome unknown: not settled")

// ErrRefundDeclined is returned when the provider DEFINITELY refused the
// refund. No money moved, so the refund is recorded `failed` and its reserve is
// released; the answer is still not a success. Not retryable.
var ErrRefundDeclined = errors.New("refund declined by provider")

// ErrKeyConflict is returned when the same key arrives with a different
// request hash — a key identifies one request, not one endpoint. Maps to
// 409 IDEMPOTENCY_CONFLICT.
var ErrKeyConflict = errors.New("idempotency key reused with a different request")

// ErrKeyLocked is returned while another attempt with the same key is
// in-flight and not yet stale. Maps to 409 + Retry-After.
var ErrKeyLocked = errors.New("idempotency key locked by an in-flight request")

// ErrLedgerImbalance is returned when a ledger posting would violate
// double-entry (legs do not net to zero, or an amount is non-positive). It is
// an internal invariant breach, never a client error.
var ErrLedgerImbalance = errors.New("ledger transaction does not balance")

// ErrDuplicateAttempt is returned when the attempt log refuses a second row the
// database has declared impossible — today only a second SUCCESS capture for one
// payment. It means another writer already recorded this outcome, not that the
// write broke, so the caller keeps its money path and says so loudly.
var ErrDuplicateAttempt = errors.New("attempt already recorded")
