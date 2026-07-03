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

// ErrKeyConflict is returned when the same key arrives with a different
// request hash — a key identifies one request, not one endpoint. Maps to
// 409 IDEMPOTENCY_CONFLICT.
var ErrKeyConflict = errors.New("idempotency key reused with a different request")

// ErrKeyLocked is returned while another attempt with the same key is
// in-flight and not yet stale. Maps to 409 + Retry-After.
var ErrKeyLocked = errors.New("idempotency key locked by an in-flight request")
