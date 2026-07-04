// Package provider defines the payment-provider port and its in-memory test
// double. P1 ships only the stub; the real HTTP client for mockpay (a separate
// binary with webhooks and a transactions API) lands in P2 behind the same
// interface.
package provider

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Decline codes mirror deterministic magic-amount triggers (Stripe's
// test-card philosophy, simplified to amount suffixes so failures are
// reproducible in tests and demos).
const (
	DeclineGeneric      = "generic_decline"    // amount_minor % 100 == 02
	DeclineInsufficient = "insufficient_funds" // amount_minor % 100 == 95
	DeclineProcessing   = "processing_error"   // amount_minor % 100 == 19 (transient — retry succeeds)

	errUnknownProviderPayment = "unknown provider payment %q"
)

// DeclinedError carries the provider's decline code; callers map it to
// 422 PAYMENT_DECLINED. Transient processing errors are returned as plain
// errors so retry policies treat them as retryable.
type DeclinedError struct{ Code string }

func (e *DeclinedError) Error() string { return "provider declined: " + e.Code }

// ErrTransient marks a retryable provider failure (processing_error trigger).
var ErrTransient = errors.New("provider transient processing error")

// ChargeRequest asks the provider to place (and optionally capture) a hold. The
// json tags are the wire body for POST /charges (mockpay HTTP contract).
type ChargeRequest struct {
	IdempotencyKey string `json:"idempotency_key"` // passed through — the provider replays its first answer
	AmountMinor    int64  `json:"amount_minor"`
	Currency       string `json:"currency"`
	PaymentMethod  string `json:"payment_method"` // opaque token, never PAN-like data
	AutoCapture    bool   `json:"auto_capture"`
}

// Charge is the provider's record of a hold/charge (and the POST /charges
// response body).
type Charge struct {
	ProviderPaymentID string `json:"provider_payment_id"`
	Captured          bool   `json:"captured"`
}

// RefundRequest is the POST /refunds body.
type RefundRequest struct {
	ProviderPaymentID string `json:"provider_payment_id"`
	AmountMinor       int64  `json:"amount_minor"`
	IdempotencyKey    string `json:"idempotency_key"`
}

// RefundResponse is the POST /refunds response body.
type RefundResponse struct {
	ProviderRefundID string `json:"provider_refund_id"`
}

// ErrorResponse is the mockpay error envelope (declines carry a Code).
type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

// WebhookEvent is the signed body mockpay emits and the payment receiver parses.
// EventID is the dedup key (delivery is at-least-once); Type names the movement.
type WebhookEvent struct {
	EventID           string `json:"event_id"`
	Type              string `json:"type"`
	ProviderPaymentID string `json:"provider_payment_id"`
	AmountMinor       int64  `json:"amount_minor"`
}

// Outcome classifies an amount against the deterministic magic-amount triggers.
type Outcome int

const (
	OutcomeOK Outcome = iota
	OutcomeGenericDecline
	OutcomeInsufficient
	OutcomeTransient
)

// Classify maps an amount's minor-unit suffix to its magic outcome. Shared by
// the in-memory Stub and the mockpay server so both honour identical triggers;
// each tracks its own transient-retry state separately.
func Classify(amountMinor int64) Outcome {
	switch amountMinor % 100 {
	case 2:
		return OutcomeGenericDecline
	case 95:
		return OutcomeInsufficient
	case 19:
		return OutcomeTransient
	default:
		return OutcomeOK
	}
}

// Provider is the outbound port to the payment provider.
type Provider interface {
	Charge(ctx context.Context, req ChargeRequest) (*Charge, error)
	Capture(ctx context.Context, providerPaymentID string) error
	Void(ctx context.Context, providerPaymentID string) error
	Refund(ctx context.Context, providerPaymentID string, amountMinor int64, idempotencyKey string) (providerRefundID string, err error)
}

// Stub is the in-memory Provider used in P1 and in unit tests. It honours the
// magic-amount triggers and replays answers per idempotency key, which is
// exactly the contract the recovery-point flow depends on.
type Stub struct {
	mu       sync.Mutex
	seq      int64              // guarded by mu
	byKey    map[string]*Charge // idempotency replay
	captured map[string]bool
	voided   map[string]bool // voided ids — makes Void idempotent under retry
	// transientSeen tracks which idempotency keys have already hit the
	// processing_error trigger once, so the next attempt with the same key
	// succeeds — used to test transient-then-recover retries.
	transientSeen map[string]bool
}

// NewStub returns an empty in-memory provider.
func NewStub() *Stub {
	return &Stub{
		byKey:         map[string]*Charge{},
		captured:      map[string]bool{},
		voided:        map[string]bool{},
		transientSeen: map[string]bool{},
	}
}

// Charges returns how many NEW charges the stub has minted (replays excluded).
// Tests use it to prove idempotency never double-charges.
func (s *Stub) Charges() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq
}

// Charge implements Provider with deterministic magic-amount declines and
// per-key replay.
func (s *Stub) Charge(_ context.Context, req ChargeRequest) (*Charge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if c, ok := s.byKey[req.IdempotencyKey]; ok && req.IdempotencyKey != "" {
		return c, nil // provider-side idempotent replay
	}

	switch Classify(req.AmountMinor) {
	case OutcomeGenericDecline:
		return nil, &DeclinedError{Code: DeclineGeneric}
	case OutcomeInsufficient:
		return nil, &DeclinedError{Code: DeclineInsufficient}
	case OutcomeTransient:
		if !s.transientSeen[req.IdempotencyKey] {
			s.transientSeen[req.IdempotencyKey] = true
			return nil, ErrTransient
		}
		// second attempt with the same key succeeds
	case OutcomeOK:
	}

	s.seq++
	c := &Charge{
		ProviderPaymentID: fmt.Sprintf("mp_%d", s.seq),
		Captured:          req.AutoCapture,
	}
	if req.IdempotencyKey != "" {
		s.byKey[req.IdempotencyKey] = c
	}
	s.captured[c.ProviderPaymentID] = req.AutoCapture
	return c, nil
}

// Capture marks a hold captured; capturing twice is a no-op (idempotent).
func (s *Stub) Capture(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.captured[id]; !ok {
		return fmt.Errorf(errUnknownProviderPayment, id)
	}
	s.captured[id] = true
	return nil
}

// Void releases a hold. Idempotent: voiding an already-voided id is a no-op
// (a lost 200 must be safely retryable), while a never-issued id is an error.
func (s *Stub) Void(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.voided[id] {
		return nil
	}
	if _, ok := s.captured[id]; !ok {
		return fmt.Errorf(errUnknownProviderPayment, id)
	}
	delete(s.captured, id)
	s.voided[id] = true
	return nil
}

// Refund returns a deterministic refund id; replay per idempotency key.
func (s *Stub) Refund(_ context.Context, id string, _ int64, idemKey string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.captured[id]; !ok {
		return "", fmt.Errorf(errUnknownProviderPayment, id)
	}
	return "re_" + idemKey, nil
}
