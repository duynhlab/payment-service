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
	"sync/atomic"
)

// Decline codes mirror the deterministic magic-amount triggers (RFC-0010):
// Stripe's test-card philosophy, simplified to amount suffixes so failures are
// reproducible in tests and demos.
const (
	DeclineGeneric      = "generic_decline"    // amount_minor % 100 == 02
	DeclineInsufficient = "insufficient_funds" // amount_minor % 100 == 95
	DeclineProcessing   = "processing_error"   // amount_minor % 100 == 19 (transient — retry succeeds)
)

// ErrDeclined carries the provider's decline code; callers map it to
// 422 PAYMENT_DECLINED. Transient processing errors are returned as plain
// errors so retry policies treat them as retryable.
type ErrDeclined struct{ Code string }

func (e *ErrDeclined) Error() string { return "provider declined: " + e.Code }

// ErrTransient marks a retryable provider failure (processing_error trigger).
var ErrTransient = errors.New("provider transient processing error")

// ChargeRequest asks the provider to place (and optionally capture) a hold.
type ChargeRequest struct {
	IdempotencyKey string // passed through — the provider replays its first answer
	AmountMinor    int64
	Currency       string
	PaymentMethod  string // opaque token, never PAN-like data
	AutoCapture    bool
}

// Charge is the provider's record of a hold/charge.
type Charge struct {
	ProviderPaymentID string
	Captured          bool
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
	seq      atomic.Int64
	byKey    map[string]*Charge // idempotency replay
	captured map[string]bool
	// FailProcessingOnce makes the NEXT processing_error-triggered charge
	// fail once then succeed — used to test transient retries.
	transientSeen map[string]bool
}

// NewStub returns an empty in-memory provider.
func NewStub() *Stub {
	return &Stub{byKey: map[string]*Charge{}, captured: map[string]bool{}, transientSeen: map[string]bool{}}
}

// Charge implements Provider with deterministic magic-amount declines and
// per-key replay.
func (s *Stub) Charge(_ context.Context, req ChargeRequest) (*Charge, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if c, ok := s.byKey[req.IdempotencyKey]; ok && req.IdempotencyKey != "" {
		return c, nil // provider-side idempotent replay
	}

	switch req.AmountMinor % 100 {
	case 2:
		return nil, &ErrDeclined{Code: DeclineGeneric}
	case 95:
		return nil, &ErrDeclined{Code: DeclineInsufficient}
	case 19:
		if !s.transientSeen[req.IdempotencyKey] {
			s.transientSeen[req.IdempotencyKey] = true
			return nil, ErrTransient
		}
		// second attempt with the same key succeeds
	}

	c := &Charge{
		ProviderPaymentID: fmt.Sprintf("mp_%d", s.seq.Add(1)),
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
		return fmt.Errorf("unknown provider payment %q", id)
	}
	s.captured[id] = true
	return nil
}

// Void releases a hold; voiding twice is a no-op.
func (s *Stub) Void(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.captured[id]; !ok {
		return fmt.Errorf("unknown provider payment %q", id)
	}
	delete(s.captured, id)
	return nil
}

// Refund returns a deterministic refund id; replay per idempotency key.
func (s *Stub) Refund(_ context.Context, id string, _ int64, idemKey string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.captured[id]; !ok {
		return "", fmt.Errorf("unknown provider payment %q", id)
	}
	return "re_" + idemKey, nil
}
