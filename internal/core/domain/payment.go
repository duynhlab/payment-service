// Package domain holds the payment service's core types: the Payment and
// Refund models and the payment state machine.
package domain

import (
	"errors"
	"fmt"
	"time"
)

// Status is a payment's stored lifecycle state. partially_refunded is
// intentionally NOT a status — it is derived from refund sums (see
// Payment.PartiallyRefunded) so it can never drift from the data.
type Status string

const (
	StatusPending    Status = "pending"
	StatusAuthorized Status = "authorized"
	StatusCaptured   Status = "captured"
	StatusFailed     Status = "failed"
	StatusVoided     Status = "voided"
	StatusExpired    Status = "expired"
	StatusRefunded   Status = "refunded"
)

// CaptureMethod selects auth-then-capture (manual, the saga's mode) or
// auth+capture in one operation (automatic).
type CaptureMethod string

const (
	CaptureManual    CaptureMethod = "manual"
	CaptureAutomatic CaptureMethod = "automatic"
)

// DefaultCurrency is applied when a request omits the currency.
const DefaultCurrency = "USD"

// allowedTransitions is the whitelist: a transition absent here is invalid,
// by construction. The DB compare-and-swap (UPDATE ... WHERE status=$expected)
// is the concurrent-safety net; this map is the business rule and the good
// error message.
var allowedTransitions = map[Status][]Status{
	StatusPending:    {StatusAuthorized, StatusFailed},
	StatusAuthorized: {StatusCaptured, StatusVoided, StatusExpired},
	StatusCaptured:   {StatusRefunded},
	// failed, voided, expired, refunded are terminal.
}

// ErrInvalidTransition is returned when a requested state change is not in the
// whitelist. Callers map it to 409 INVALID_TRANSITION.
var ErrInvalidTransition = errors.New("invalid payment state transition")

// CanTransition reports whether from -> to is an allowed move.
func CanTransition(from, to Status) bool {
	for _, next := range allowedTransitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

// Transition validates from -> to, wrapping ErrInvalidTransition with both
// states for the error message.
func Transition(from, to Status) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}

// Payment is a PaymentIntent. Amounts are integer minor units (2000 = $20.00);
// PaymentMethod is an opaque test token — PAN-like data is never accepted,
// stored, or logged (PCI discipline, even for a mock).
type Payment struct {
	ID                int64         `json:"id"`
	UserID            int64         `json:"user_id"`
	OrderID           *int64        `json:"order_id,omitempty"`
	AmountMinor       int64         `json:"amount_minor"`
	Currency          string        `json:"currency"`
	Status            Status        `json:"status"`
	CaptureMethod     CaptureMethod `json:"capture_method"`
	PaymentMethod     string        `json:"payment_method"`
	ProviderPaymentID string        `json:"provider_payment_id,omitempty"`
	DeclineCode       string        `json:"decline_code,omitempty"`
	AuthorizedAt      *time.Time    `json:"authorized_at,omitempty"`
	ExpiresAt         *time.Time    `json:"expires_at,omitempty"`
	CapturedAt        *time.Time    `json:"captured_at,omitempty"`
	RefundedMinor     int64         `json:"refunded_minor"`
	CreatedAt         time.Time     `json:"created_at"`
	UpdatedAt         time.Time     `json:"updated_at"`
}

// PartiallyRefunded derives the partial-refund flag from data: some but not
// all of the captured amount has been refunded, while the stored status is
// still captured.
func (p *Payment) PartiallyRefunded() bool {
	return p.Status == StatusCaptured && p.RefundedMinor > 0 && p.RefundedMinor < p.AmountMinor
}

// RefundStatus is a refund's lifecycle state (provider confirms async).
type RefundStatus string

const (
	RefundPending   RefundStatus = "pending"
	RefundSucceeded RefundStatus = "succeeded"
	RefundFailed    RefundStatus = "failed"
)

// Refund is a first-class object — never a mutation of its payment.
type Refund struct {
	ID               int64        `json:"id"`
	PaymentID        int64        `json:"payment_id"`
	AmountMinor      int64        `json:"amount_minor"`
	Status           RefundStatus `json:"status"`
	ProviderRefundID string       `json:"provider_refund_id,omitempty"`
	Reason           string       `json:"reason,omitempty"`
	IdempotencyKey   string       `json:"-"` // dedupe key; never exposed in API responses
	CreatedAt        time.Time    `json:"created_at"`
	UpdatedAt        time.Time    `json:"updated_at"`
}
