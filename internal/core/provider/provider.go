// Package provider defines the payment-provider port and its in-memory test
// double. P1 ships only the stub; the real HTTP client for mockpay (a separate
// binary with webhooks and a transactions API) lands in P2 behind the same
// interface.
package provider

import (
	"time"
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

	// errUnknownProviderPayment is wrapped in ErrDefinite: the provider does not
	// have this charge, and no retry invents one. The HTTP client answers 404
	// the same way, so a stub-backed test sees the classification production
	// sees.
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

// Transaction status values reported by GET /transactions — the provider's view
// of a charge, which reconciliation compares against the internal payment state.
const (
	TxnAuthorized = "authorized"
	TxnCaptured   = "captured"
	TxnVoided     = "voided"
	TxnRefunded   = "refunded"
)

// Transaction is the provider's record of one charge, matched during
// reconciliation by ProviderPaymentID (the shared identifier).
type Transaction struct {
	ProviderPaymentID string `json:"provider_payment_id"`
	AmountMinor       int64  `json:"amount_minor"`
	Status            string `json:"status"`
	// CreatedAt is when the provider first recorded the charge. It is what makes a
	// reconciliation pass boundable: matched against the internal window, it keeps
	// a charge from being reported missing on our side merely because it is older
	// than the window we asked about.
	CreatedAt time.Time `json:"created_at"`
}

// TransactionsPage is the paged GET /transactions response — the reconciliation
// job's food source.
type TransactionsPage struct {
	Transactions []Transaction `json:"transactions"`
	Page         int           `json:"page"`
	PageSize     int           `json:"page_size"`
	Total        int           `json:"total"`
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
	// OutcomeNoAnswer models the provider going silent — the case that matters
	// most and was previously untestable without stopping a container: the
	// request may have been processed, we simply never learn. Distinct from
	// OutcomeTransient, which is an explicit "ask again".
	OutcomeNoAnswer
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
	case 13:
		return OutcomeNoAnswer
	default:
		return OutcomeOK
	}
}

// ErrDefinite marks a provider answer that is FINAL even though it is not a
// business decline: the request as sent can never succeed (malformed, unknown
// charge, an amount the provider refuses to process). It exists because the
// caller's only real question about a failure is "is this decided?" — a decided
// failure is recorded and stopped, an undecided one is held open and retried.
//
// 408 and 429 are deliberately NOT definite: both mean "ask again".
var ErrDefinite = errors.New("provider answered definitively: request cannot succeed")

// ErrKeyConflict is returned when an idempotency key is reused for a different
// (operation, charge) pair. A real provider answers the same way, and both
// doubles do, so a caller that derives keys wrongly fails loudly instead of
// quietly receiving an answer that belongs to something else.
var ErrKeyConflict = errors.New("idempotency key reused for a different operation or charge")

// Operation names used in idempotency keys. Deliberately their own constants:
// deriving them from metric labels would mean renaming a dashboard label
// silently changes every in-flight idempotency key.
const (
	opCaptureKey = "capture"
	opVoidKey    = "void"
)

// CodeIdempotencyConflict is the machine code a provider returns for key reuse.
// Shared so the HTTP client maps it back to ErrKeyConflict rather than matching
// on a message.
const CodeIdempotencyConflict = "idempotency_conflict"

// mutationBinding is what an idempotency key was first used for. Only the
// identity is kept — never the answer (see mutationKeys).
type mutationBinding struct {
	operation string
	chargeID  string
}

// bindKey records that this key belongs to (op, chargeID), or reports a conflict
// when it was already used for a different one. Caller holds s.mu.
func (s *Stub) bindKey(idemKey, op, chargeID string) error {
	if idemKey == "" {
		return nil // no key: answer from state, as before keys existed
	}
	want := mutationBinding{operation: op, chargeID: chargeID}
	if prior, ok := s.mutationKeys[idemKey]; ok && prior != want {
		return fmt.Errorf("%w: %w", ErrDefinite, ErrKeyConflict)
	}
	s.mutationKeys[idemKey] = want
	return nil
}

// MutationRequest is the body of a capture/void call. It carries only the
// idempotency key: the charge id is in the path, and the amount is the
// authorized amount (partial capture is not a thing here yet).
type MutationRequest struct {
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// Provider is the outbound port to the payment provider.
type Provider interface {
	Charge(ctx context.Context, req ChargeRequest) (*Charge, error)
	// Capture and Void take an idempotency key for the same reason Charge and
	// Refund do, but against a different hazard. Both are naturally idempotent
	// by charge id — a charge can only be captured once — so the key is not
	// there to stop a second capture. It is there so a RETRY AFTER DOUBT gets
	// the provider's ORIGINAL answer replayed instead of a fresh evaluation:
	// without it, a stricter provider answers "already captured" with a 4xx,
	// which classifies as a decided FAILURE for an operation that in fact
	// SUCCEEDED. The key is deterministic per intent, so the retry carries it.
	Capture(ctx context.Context, providerPaymentID, idempotencyKey string) error
	Void(ctx context.Context, providerPaymentID, idempotencyKey string) error
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
	// mutationKeys binds an idempotency key to the (operation, charge) it was
	// first used for, so reusing it elsewhere is a detectable caller bug.
	//
	// It deliberately does NOT remember the ANSWER. Remembering one and replaying
	// it means the double can contradict its own state: a capture success
	// replayed after the hold was voided would report success for money that is
	// no longer collectable. The charge's CURRENT state is the truth, and
	// capture/void are already idempotent against it.
	mutationKeys map[string]mutationBinding
}

// NewStub returns an empty in-memory provider.
func NewStub() *Stub {
	return &Stub{
		byKey:         map[string]*Charge{},
		captured:      map[string]bool{},
		voided:        map[string]bool{},
		transientSeen: map[string]bool{},
		mutationKeys:  map[string]mutationBinding{},
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
	case OutcomeNoAnswer:
		// Silence, not a verdict: the charge may exist provider-side. Modelled as
		// a deadline so callers classify it exactly as they would a real timeout.
		return nil, fmt.Errorf("mockpay charge: %w", context.DeadlineExceeded)
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

// Capture marks a hold captured. Idempotent twice over: by charge state, and by
// idempotency key — a repeated key replays the first answer verbatim.
func (s *Stub) Capture(_ context.Context, id, idemKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.bindKey(idemKey, opCaptureKey, id); err != nil {
		return err
	}
	return s.captureLocked(id)
}

func (s *Stub) captureLocked(id string) error {
	if _, ok := s.captured[id]; !ok {
		return fmt.Errorf("%w: "+errUnknownProviderPayment, ErrDefinite, id)
	}
	s.captured[id] = true
	return nil
}

// Void releases a hold. Idempotent: voiding an already-voided id is a no-op
// (a lost 200 must be safely retryable), while a never-issued id is an error.
func (s *Stub) Void(_ context.Context, id, idemKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.bindKey(idemKey, opVoidKey, id); err != nil {
		return err
	}
	return s.voidLocked(id)
}

func (s *Stub) voidLocked(id string) error {
	if s.voided[id] {
		return nil
	}
	if _, ok := s.captured[id]; !ok {
		return fmt.Errorf("%w: "+errUnknownProviderPayment, ErrDefinite, id)
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
		return "", fmt.Errorf("%w: "+errUnknownProviderPayment, ErrDefinite, id)
	}
	return "re_" + idemKey, nil
}
