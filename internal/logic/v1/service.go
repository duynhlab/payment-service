// Package v1 implements the payment business logic: the idempotent
// authorize/capture/void/refund flows on top of the domain state machine.
// It owns the port interfaces; web/v1 translates HTTP, core implements
// persistence and the provider — strict 3-layer direction.
package v1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/duynhlab/pkg/idempotency"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

// PaymentRepo is the persistence port the logic layer needs (implemented by
// repository.PaymentRepository).
type PaymentRepo interface {
	Create(ctx context.Context, p *domain.Payment) (*domain.Payment, error)
	FindByID(ctx context.Context, id, userID int64) (*domain.Payment, error)
	FindByOrderID(ctx context.Context, orderID int64) (*domain.Payment, error)
	ListByUser(ctx context.Context, userID int64, limit, offset int) ([]domain.Payment, int, error)
	TransitionStatus(ctx context.Context, id int64, from, to domain.Status, set map[string]any) error
	CaptureWithLedger(ctx context.Context, id int64, capturedAt time.Time) error
	ReverseCapture(ctx context.Context, id int64) error
	ExpireStaleAuthorizations(ctx context.Context, now time.Time) (int64, error)
	CreateRefund(ctx context.Context, paymentID, amountMinor int64, reason, idemKey string) (*domain.Refund, error)
	SettleRefund(ctx context.Context, refundID int64, status domain.RefundStatus, providerRefundID string) error
}

// IdemRepo is the idempotency-key port (implemented by *idempotency.Repository
// from the shared pkg). The claimed subject is the payment id.
type IdemRepo interface {
	Claim(ctx context.Context, userID int64, key, method, path, hash string) (*idempotency.Record, bool, error)
	Checkpoint(ctx context.Context, id int64, subjectID *int64) error
	Release(ctx context.Context, id int64) error
	Finish(ctx context.Context, id int64, code int, body []byte) error
	Reap(ctx context.Context, ttl time.Duration) (int64, error)
}

// Service is the payment logic. AuthHoldTTL bounds authorized holds.
type Service struct {
	payments PaymentRepo
	idem     IdemRepo
	prov     provider.Provider
	holdTTL  time.Duration
	now      func() time.Time
	// attempts is the per-round-trip evidence log (RFC-0021 phase 6).
	attempts AttemptLog
}

// NewService wires the logic layer onto its ports.
func NewService(p PaymentRepo, i IdemRepo, prov provider.Provider, holdTTL time.Duration, opts ...ServiceOption) *Service {
	s := &Service{payments: p, idem: i, prov: prov, holdTTL: holdTTL, now: time.Now,
		attempts: unwiredAttemptLog{}}
	for _, o := range opts {
		o(s)
	}
	return s
}

// AttemptLog is the per-round-trip evidence log and the reason `processing` is a
// state a payment can leave. Reading it is as load-bearing as writing it: the
// open rows say WHICH operation is in doubt and under WHICH key to ask again, so
// a service without a real log can park an intent but cannot un-park one.
type AttemptLog interface {
	Record(ctx context.Context, a domain.Attempt) (int64, error)
	// ListOpenForPayment returns this payment's unresolved UNKNOWN attempts,
	// oldest-first — the questions still owed an answer.
	ListOpenForPayment(ctx context.Context, paymentID int64) ([]domain.Attempt, error)
	// Resolve stamps an attempt settled once a later round-trip answered it.
	Resolve(ctx context.Context, attemptID int64, at time.Time) error
}

// ServiceOption configures optional collaborators.
type ServiceOption func(*Service)

// WithAttempts wires the attempt log. cmd/main.go always passes the real one.
// Without it the money paths still apply the right STATE — an unknown outcome
// still parks the intent, because a park is more honest than a guess — but the
// doubt cannot be resolved, so an unwired log is a test-only configuration.
func WithAttempts(a AttemptLog) ServiceOption {
	return func(s *Service) { s.attempts = a }
}

// errAttemptLogUnwired is what an unwired log answers with. It is an error rather
// than an empty result on purpose: "no open attempts" and "I cannot tell you
// whether there are open attempts" must never look the same to a resolver.
var errAttemptLogUnwired = errors.New("attempt log not wired: doubt cannot be resolved")

// unwiredAttemptLog is the explicit default, so a service built without a log
// reads as a deliberate choice rather than a nil waiting to panic. Writes are
// dropped (the money state still applies); reads refuse.
type unwiredAttemptLog struct{}

func (unwiredAttemptLog) Record(context.Context, domain.Attempt) (int64, error) {
	return 0, errAttemptLogUnwired
}

func (unwiredAttemptLog) ListOpenForPayment(context.Context, int64) ([]domain.Attempt, error) {
	return nil, errAttemptLogUnwired
}

func (unwiredAttemptLog) Resolve(context.Context, int64, time.Time) error {
	return errAttemptLogUnwired
}

// recordAttempt appends one attempt, best-effort. An attempt row is evidence
// about the past, so failing to write it must never block the state change it
// accompanies — losing the evidence is bad, refusing to record the money's actual
// state because the evidence would not write is worse.
//
// A lost row is put on the span WITH its payment id, not just counted: the log is
// what makes an unknown outcome resolvable, so "we lost one" has to be traceable
// to which payment now cannot be resolved automatically.
//
// A duplicate is a different animal and is not counted as a loss: the row the
// database refused already exists, written by whoever got there first.
func (s *Service) recordAttempt(ctx context.Context, a domain.Attempt) error {
	_, err := s.attempts.Record(ctx, a)
	if err == nil {
		return nil
	}
	if !errors.Is(err, domain.ErrDuplicateAttempt) {
		recordAttemptWriteFailure(ctx, string(a.Operation))
	}
	trace.SpanFromContext(ctx).RecordError(err, trace.WithAttributes(
		attribute.Int64("payment.id", a.PaymentID),
		attribute.String("payment.operation", string(a.Operation)),
		attribute.String("payment.outcome_class", string(a.Outcome)),
	))
	return err
}

// chargeRef is the provider reference an authorize attempt can record. It is
// empty for an UNKNOWN outcome by definition — no answer, no reference — which
// is exactly why resolving one means asking the provider what it holds.
func chargeRef(c *provider.Charge) string {
	if c == nil {
		return ""
	}
	return c.ProviderPaymentID
}

// classifyProviderOutcome maps a provider error onto the four classes the RFC
// names. The only distinction that drives behaviour is decided vs UNKNOWN.
//
// A decided answer that is not a business decline — an unknown charge, a
// malformed request — is filed as BUSINESS_DECLINE too: the actionable fact is
// that the provider said no and a retry cannot change it, and `provider_status`
// carries which kind of no it was. Inventing a fifth class would add a state
// nothing branches on.
func classifyProviderOutcome(err error) domain.OutcomeClass {
	var declined *provider.DeclinedError
	switch {
	case err == nil:
		return domain.OutcomeSuccess
	case errors.As(err, &declined), errors.Is(err, provider.ErrDefinite):
		return domain.OutcomeBusinessDecline
	case errors.Is(err, provider.ErrTransient):
		return domain.OutcomeRetryableFailure
	default:
		// Timeout, transport failure, 5xx: the effect may have landed.
		return domain.OutcomeUnknown
	}
}

// providerStatusOf renders the provider's own verdict for the attempt row,
// without leaking anything but the code.
func providerStatusOf(err error) string {
	var declined *provider.DeclinedError
	if errors.As(err, &declined) {
		return declined.Code
	}
	switch {
	case err == nil:
		return ""
	case errors.Is(err, provider.ErrKeyConflict):
		return provider.CodeIdempotencyConflict
	case errors.Is(err, provider.ErrDefinite):
		return "definite_error"
	case errors.Is(err, provider.ErrTransient):
		return "transient"
	default:
		return "no_answer"
	}
}

// CreateIntentInput is the validated request to create a PaymentIntent. The
// json tags define the canonical shape hashed for idempotency comparison.
type CreateIntentInput struct {
	UserID        int64                `json:"user_id"`
	OrderID       *int64               `json:"order_id,omitempty"`
	AmountMinor   int64                `json:"amount_minor"`
	Currency      string               `json:"currency"`
	CaptureMethod domain.CaptureMethod `json:"capture_method"`
	PaymentMethod string               `json:"payment_method"`
}

// IntentResult is the outcome of an idempotent CreateIntent: the HTTP-ish
// status (201 created / 422 declined) plus the payment snapshot. Replayed
// results come verbatim from the idempotency cache.
type IntentResult struct {
	Code     int
	Payment  *domain.Payment
	Replayed bool
}

// mapClaimErr translates the shared idempotency package's sentinels into the
// payment domain's, so the web layer keeps one stable error vocabulary and a
// future pkg rename can never silently leak into HTTP status codes.
func mapClaimErr(err error) error {
	switch {
	case errors.Is(err, idempotency.ErrConflict):
		return domain.ErrKeyConflict
	case errors.Is(err, idempotency.ErrLocked):
		return domain.ErrKeyLocked
	default:
		return err
	}
}

// hashJSON canonicalizes a request struct (marshal → sha256 → hex) for
// same-key-different-body idempotency detection.
func hashJSON(v any) string {
	b, _ := json.Marshal(v) // struct of scalars — cannot fail
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// CreateIntent runs the recovery-point idempotent authorize flow:
//
//	claim key -> ensure payment row (checkpoint: payment_id) ->
//	provider charge OUTSIDE any tx (safe: same key passed through) ->
//	checkpoint provider_called -> apply outcome + cache response (finish).
//
// A takeover after a crash re-enters at the recorded checkpoint; a transient
// provider error leaves the key unfinished so the client retry re-drives it.
func (s *Service) CreateIntent(ctx context.Context, idemKey string, in CreateIntentInput) (*IntentResult, error) {
	key, proceed, err := s.idem.Claim(ctx, in.UserID, idemKey, "POST", "/payment/v1/private/payments", hashJSON(in))
	if err != nil {
		return nil, mapClaimErr(err)
	}
	if !proceed {
		return replayResult(key)
	}

	// Checkpoint 1: ensure the payment row exists (re-entry reuses it).
	if key.SubjectID != nil {
		pay, err := s.payments.FindByID(ctx, *key.SubjectID, 0)
		if err != nil {
			return nil, err
		}
		return s.driveCharge(ctx, key, in, pay)
	}

	pay, err := s.payments.Create(ctx, &domain.Payment{
		UserID:        in.UserID,
		OrderID:       in.OrderID,
		AmountMinor:   in.AmountMinor,
		Currency:      in.Currency,
		CaptureMethod: in.CaptureMethod,
		PaymentMethod: in.PaymentMethod,
	})
	if errors.Is(err, domain.ErrPaymentExists) && in.OrderID != nil {
		return s.adoptExistingOrderPayment(ctx, key, in, err)
	}
	if err != nil {
		return nil, err
	}
	if err := s.idem.Checkpoint(ctx, key.ID, &pay.ID); err != nil {
		return nil, err
	}
	return s.driveCharge(ctx, key, in, pay)
}

// adoptExistingOrderPayment handles the crash-recovery case where a prior
// attempt created the order's payment but died before checkpointing it on the
// key. It adopts the payment only if it is genuinely ours; a foreign owner or
// amount mismatch is a real conflict (createErr, → 409). A payment already
// past pending was charged on the first attempt, so it finishes idempotently
// by its current state and NEVER charges again.
func (s *Service) adoptExistingOrderPayment(ctx context.Context, key *idempotency.Record, in CreateIntentInput, createErr error) (*IntentResult, error) {
	existing, findErr := s.payments.FindByOrderID(ctx, *in.OrderID)
	if findErr != nil || existing.UserID != in.UserID || existing.AmountMinor != in.AmountMinor {
		return nil, createErr
	}
	// `processing` is doubt, not an outcome: finishing it as 201 would tell the
	// caller the charge succeeded when nobody knows yet. It goes through
	// driveCharge, which for a parked row RESOLVES under the original key instead
	// of charging — charging here would mint a second charge, because the provider
	// key is derived from the caller's key and this caller's is different.
	if existing.Status != domain.StatusPending && existing.Status != domain.StatusProcessing {
		code := 201
		if existing.Status == domain.StatusFailed {
			code = 422
		}
		return s.finishIntent(ctx, key.ID, code, existing.ID)
	}
	if err := s.idem.Checkpoint(ctx, key.ID, &existing.ID); err != nil {
		return nil, err
	}
	return s.driveCharge(ctx, key, in, existing)
}

// driveCharge runs the provider call and applies the resulting state
// transitions for a pending payment, caching the outcome on the key.
func (s *Service) driveCharge(ctx context.Context, key *idempotency.Record, in CreateIntentInput, pay *domain.Payment) (*IntentResult, error) {
	if pay.Status == domain.StatusProcessing {
		return s.finishParkedIntent(ctx, key, pay)
	}

	chargeKey := fmt.Sprintf("%d:%s", in.UserID, key.Key)
	// Provider call — outside any transaction; the shared idempotency key
	// makes a re-driven call replay instead of double-charging.
	charge, chErr := s.prov.Charge(ctx, provider.ChargeRequest{
		IdempotencyKey: chargeKey,
		AmountMinor:    in.AmountMinor,
		Currency:       in.Currency,
		PaymentMethod:  in.PaymentMethod,
		AutoCapture:    in.CaptureMethod == domain.CaptureAutomatic,
	})

	class := classifyProviderOutcome(chErr)
	if class != domain.OutcomeUnknown {
		// A decided round-trip is history; park() records the undecided one, because
		// there the evidence has to land before the state does.
		_ = s.recordAttempt(ctx, domain.Attempt{
			PaymentID: pay.ID, Operation: domain.AttemptAuthorize, Outcome: class,
			ProviderRef: chargeRef(charge), ProviderStatus: providerStatusOf(chErr),
			IdempotencyKey: chargeKey,
		})
	}

	switch class {
	case domain.OutcomeSuccess:
		return s.applyAuthorized(ctx, key, in, pay, charge)

	case domain.OutcomeBusinessDecline:
		// Every decided no ends the intent, not just a card decline. A definite
		// refusal that is NOT a decline — malformed request, an amount the provider
		// will not process — used to leave the row `pending`, which told the saga to
		// retry a request that can never succeed, forever.
		return s.handleDeclined(ctx, key, in, pay, declineCode(chErr))

	case domain.OutcomeUnknown:
		// The charge may exist provider-side with no reference on our side, so
		// `pending` would understate it: nothing would ever ask what happened.
		// `processing` records the doubt, and the attempt row records the key to ask
		// under — together they are what makes the escape possible.
		//
		// The lock is released either way, so a same-key retry can make progress; a
		// retry under ANY key now resolves rather than charges.
		recordAuthorization(ctx, authError, currencyLabel(in.Currency))
		recordProviderUnknown(ctx, opAuthorize)
		return nil, s.withKeyReleased(ctx, key.ID, s.park(ctx, domain.StatusPending, domain.Attempt{
			PaymentID: pay.ID, Operation: domain.AttemptAuthorize, Outcome: class,
			ProviderRef: chargeRef(charge), ProviderStatus: providerStatusOf(chErr),
			IdempotencyKey: chargeKey,
		}, chErr))

	case domain.OutcomeRetryableFailure:
		// The provider refused this attempt outright and did nothing (429): the
		// payment stays pending and the client retries.
		//
		// Release the lock so an immediate same-key retry can re-drive instead of
		// getting ErrKeyLocked until the 90s takeover window elapses.
		recordAuthorization(ctx, authError, currencyLabel(in.Currency))
		return nil, s.withKeyReleased(ctx, key.ID, chErr)
	}
	return nil, fmt.Errorf("unclassified authorize outcome: %w", chErr)
}

// finishParkedIntent resolves a payment that a previous round-trip left in doubt
// and answers with the settled outcome. This is the only door a parked intent
// goes through, and it deliberately does NOT charge: the provider key is derived
// from the CALLER's key, so charging here would mint a second charge for every
// retry that arrives under a fresh Idempotency-Key.
func (s *Service) finishParkedIntent(ctx context.Context, key *idempotency.Record, pay *domain.Payment) (*IntentResult, error) {
	settled, err := s.resolveIntentDoubt(ctx, pay)
	if err != nil {
		return nil, err
	}
	switch settled.Status {
	case domain.StatusAuthorized, domain.StatusCaptured:
		return s.finishIntent(ctx, key.ID, 201, settled.ID)
	case domain.StatusFailed:
		return s.finishIntent(ctx, key.ID, 422, settled.ID)
	case domain.StatusProcessing, domain.StatusPending, domain.StatusVoided,
		domain.StatusExpired, domain.StatusRefunded:
		// Still parked, or moved on by an operator. Either way this call has no
		// verdict to give, and nothing is cached on the key — caching here would
		// freeze the doubt into an answer no round-trip ever produced.
		return nil, s.withKeyReleased(ctx, key.ID,
			fmt.Errorf("%w: authorize", domain.ErrOutcomeUnknown))
	}
	return nil, fmt.Errorf("unhandled payment status %q after resolution", settled.Status)
}

// handleDeclined transitions the payment to failed and finishes with 422. The
// decline is counted only when THIS call applied the transition, so a
// crash-recovery re-drive (tolerated stale) never double-counts the decline.
func (s *Service) handleDeclined(ctx context.Context, key *idempotency.Record, in CreateIntentInput, pay *domain.Payment, code string) (*IntentResult, error) {
	txErr := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusPending, domain.StatusFailed,
		map[string]any{colDeclineCode: code})
	if txErr != nil && !errors.Is(txErr, domain.ErrStaleTransition) {
		return nil, txErr
	}
	if txErr == nil {
		recordAuthorization(ctx, authDeclined, currencyLabel(in.Currency))
	}
	return s.finishIntent(ctx, key.ID, 422, pay.ID)
}

// applyAuthorized applies the successful outcome through the whitelisted
// transitions, verifies the row actually landed in a successful state (never
// cache a success for a row the expiry job raced), records the authorization
// once (only the drive that applied it, not a stale re-drive), and finishes 201.
func (s *Service) applyAuthorized(ctx context.Context, key *idempotency.Record, in CreateIntentInput, pay *domain.Payment, charge *provider.Charge) (*IntentResult, error) {
	expires := s.now().Add(s.holdTTL)
	txErr := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusPending, domain.StatusAuthorized,
		map[string]any{
			colProviderPaymentID: charge.ProviderPaymentID,
			colAuthorizedAt:      s.now(),
			colExpiresAt:         expires,
		})
	if txErr != nil && !errors.Is(txErr, domain.ErrStaleTransition) {
		return nil, txErr // stale = re-entry already applied it; verified below
	}
	if in.CaptureMethod == domain.CaptureAutomatic {
		// Auto-capture posts the capture ledger via the CAS inside
		// CaptureWithLedger: a re-driven charge finds the row already captured
		// (stale) and posts nothing.
		if err := s.payments.CaptureWithLedger(ctx, pay.ID, s.now()); err != nil && !errors.Is(err, domain.ErrStaleTransition) {
			return nil, err
		}
	}
	final, err := s.payments.FindByID(ctx, pay.ID, 0)
	if err != nil {
		return nil, err
	}
	if final.Status != domain.StatusAuthorized && final.Status != domain.StatusCaptured {
		return nil, fmt.Errorf("%w: charge succeeded but payment is %s", domain.ErrInvalidTransition, final.Status)
	}
	if txErr == nil {
		recordAuthorization(ctx, authAuthorized, currencyLabel(in.Currency))
	}
	return s.finishIntent(ctx, key.ID, 201, pay.ID)
}

// finishIntent snapshots the payment, caches it on the key, and returns it.
func (s *Service) finishIntent(ctx context.Context, keyID int64, code int, paymentID int64) (*IntentResult, error) {
	pay, err := s.payments.FindByID(ctx, paymentID, 0)
	if err != nil {
		return nil, err
	}
	body, _ := json.Marshal(pay) // domain struct — cannot fail
	if err := s.idem.Finish(ctx, keyID, code, body); err != nil {
		return nil, err
	}
	return &IntentResult{Code: code, Payment: pay}, nil
}

func replayResult(key *idempotency.Record) (*IntentResult, error) {
	var pay domain.Payment
	if err := json.Unmarshal(key.ResponseBody, &pay); err != nil {
		return nil, fmt.Errorf("corrupt idempotency cache: %w", err)
	}
	return &IntentResult{Code: *key.ResponseCode, Payment: &pay, Replayed: true}, nil
}

// Capture moves an authorized hold to captured. Idempotent: capturing an
// already-captured payment returns it unchanged.
func (s *Service) Capture(ctx context.Context, paymentID, userID int64) (*domain.Payment, error) {
	pay, err := s.payments.FindByID(ctx, paymentID, userID)
	if err != nil {
		return nil, err
	}
	if pay, err = s.settleBeforeOperating(ctx, pay, opCapture); err != nil {
		return nil, err
	}
	if pay.Status == domain.StatusCaptured {
		return pay, nil // idempotent no-op
	}
	if err := domain.Transition(pay.Status, domain.StatusCaptured); err != nil {
		recordOperation(ctx, opCapture, resultRejected)
		return nil, err
	}
	// CAS FIRST, provider second: winning the row before moving money means a
	// concurrent void/expiry can never leave the provider captured while the
	// row says otherwise. The CAS and the balanced capture ledger posting
	// commit together (CaptureWithLedger), so the row and the ledger can never
	// disagree. If the provider then fails, compensate both with a reversal.
	//
	// Known gap (reconciliation phase): a crash between this commit and a
	// confirmed provider capture leaves the ledger asserting revenue the
	// provider never collected. It is internally balanced, so Imbalance() will
	// not flag it — only a provider-vs-ledger reconciliation sweep will.
	if err := s.payments.CaptureWithLedger(ctx, pay.ID, s.now()); err != nil {
		if errors.Is(err, domain.ErrStaleTransition) {
			return s.reloadAfterRace(ctx, pay.ID, domain.StatusCaptured)
		}
		return nil, err
	}
	captureKey := providerKey(providerOpCapture, pay.ID)
	capErr := s.prov.Capture(ctx, pay.ProviderPaymentID, captureKey)
	class := classifyProviderOutcome(capErr)
	attempt := domain.Attempt{
		PaymentID: pay.ID, Operation: domain.AttemptCapture, Outcome: class,
		ProviderRef: pay.ProviderPaymentID, ProviderStatus: providerStatusOf(capErr),
		IdempotencyKey: captureKey,
	}
	if class != domain.OutcomeUnknown {
		_ = s.recordAttempt(ctx, attempt)
	}
	switch class {
	case domain.OutcomeSuccess:
		recordOperation(ctx, opCapture, resultOK)
		return s.payments.FindByID(ctx, pay.ID, 0)

	case domain.OutcomeUnknown:
		// The heart of phase 6. Reversing here is the SEMANTIC OPPOSITE of the
		// operation whose fate we do not know: if the provider did capture and
		// only the answer was lost, the reversal takes the money out of our books
		// while the provider keeps it — revenue collected and then disowned, and
		// internally balanced so Imbalance() cannot see it.
		//
		// So the capture ledger entry STAYS and the intent moves to `processing`,
		// with the round-trip recorded. The next call that touches this payment
		// re-asks the provider under the same key (see resolveIntentDoubt); a
		// deliberate reversal may follow then, as a conclusion rather than a reflex.
		//
		// A background sweep over the whole worklist is the phase's next slice; until
		// it lands, the escape is a retry, not a timer.
		recordProviderUnknown(ctx, opCapture)
		recordOperation(ctx, opCapture, resultUnknown)
		return nil, s.park(ctx, domain.StatusCaptured, attempt, capErr)

	case domain.OutcomeBusinessDecline:
		// Decided no: nothing was captured. Reverse the row and post the
		// compensating reversal (append-only — never edit the capture entry).
		recordOperation(ctx, opCapture, resultDeclined)
		if rbErr := s.payments.ReverseCapture(ctx, pay.ID); rbErr != nil {
			return nil, fmt.Errorf("provider capture failed (%w) and rollback failed: %w", capErr, rbErr)
		}
		return nil, capErr

	case domain.OutcomeRetryableFailure:
		// The provider refused the request itself and did nothing with it, so the
		// hold is intact and the next attempt starts clean — the same shape as a
		// decline, but counted apart from one so a rate-limit spike never reads as
		// customers being declined.
		recordOperation(ctx, opCapture, resultError)
		if rbErr := s.payments.ReverseCapture(ctx, pay.ID); rbErr != nil {
			return nil, fmt.Errorf("provider capture failed (%w) and rollback failed: %w", capErr, rbErr)
		}
		return nil, capErr
	}
	// Unreachable: classifyProviderOutcome returns only the four classes above.
	return nil, fmt.Errorf("unclassified capture outcome: %w", capErr)
}

// Void releases an authorized hold. Idempotent on already-voided payments.
func (s *Service) Void(ctx context.Context, paymentID, userID int64) (*domain.Payment, error) {
	pay, err := s.payments.FindByID(ctx, paymentID, userID)
	if err != nil {
		return nil, err
	}
	if pay, err = s.settleBeforeOperating(ctx, pay, opVoid); err != nil {
		return nil, err
	}
	if pay.Status == domain.StatusVoided {
		return pay, nil
	}
	if err := domain.Transition(pay.Status, domain.StatusVoided); err != nil {
		recordOperation(ctx, opVoid, resultRejected)
		return nil, err
	}
	// Same ordering rationale as Capture: win the row, then touch the provider.
	if err := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusAuthorized, domain.StatusVoided, nil); err != nil {
		if errors.Is(err, domain.ErrStaleTransition) {
			return s.reloadAfterRace(ctx, pay.ID, domain.StatusVoided)
		}
		return nil, err
	}
	voidKey := providerKey(providerOpVoid, pay.ID)
	voidErr := s.prov.Void(ctx, pay.ProviderPaymentID, voidKey)
	class := classifyProviderOutcome(voidErr)
	attempt := domain.Attempt{
		PaymentID: pay.ID, Operation: domain.AttemptVoid, Outcome: class,
		ProviderRef: pay.ProviderPaymentID, ProviderStatus: providerStatusOf(voidErr),
		IdempotencyKey: voidKey,
	}
	if class != domain.OutcomeUnknown {
		_ = s.recordAttempt(ctx, attempt)
	}
	switch class {
	case domain.OutcomeSuccess:
		recordOperation(ctx, opVoid, resultOK)
		return s.payments.FindByID(ctx, pay.ID, 0)

	case domain.OutcomeUnknown:
		// Rolling back to `authorized` would assert the hold is still live. If the
		// void did land, that leaves us believing we can capture money the
		// provider has already released — the mirror image of the capture case.
		// Park it instead, and let the next call re-ask under the same key.
		recordProviderUnknown(ctx, opVoid)
		recordOperation(ctx, opVoid, resultUnknown)
		return nil, s.park(ctx, domain.StatusVoided, attempt, voidErr)

	case domain.OutcomeBusinessDecline, domain.OutcomeRetryableFailure:
		result := resultDeclined
		if class == domain.OutcomeRetryableFailure {
			result = resultError
		}
		recordOperation(ctx, opVoid, result)
		if rbErr := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusVoided, domain.StatusAuthorized, nil); rbErr != nil {
			return nil, fmt.Errorf("provider void failed (%w) and rollback failed: %w", voidErr, rbErr)
		}
		return nil, voidErr
	}
	// Unreachable: classifyProviderOutcome returns only the four classes above.
	return nil, fmt.Errorf("unclassified void outcome: %w", voidErr)
}

// reloadAfterRace re-reads after a lost CAS: if the payment reached the
// desired state anyway (concurrent duplicate), that's idempotent success;
// any other state is a real conflict. It deliberately records no operation
// metric — the CAS winner already counted this transition, so counting here
// too would double-count one operation.
func (s *Service) reloadAfterRace(ctx context.Context, id int64, want domain.Status) (*domain.Payment, error) {
	pay, err := s.payments.FindByID(ctx, id, 0)
	if err != nil {
		return nil, err
	}
	if pay.Status == want {
		return pay, nil
	}
	return nil, fmt.Errorf("%w: payment is %s", domain.ErrInvalidTransition, pay.Status)
}

// Provider-facing operation names for idempotency keys. Their own constants on
// purpose: the metric labels happen to spell the same words, and deriving keys
// from those would mean renaming a dashboard label silently changes every
// in-flight idempotency key.
const (
	providerOpCapture = "capture"
	providerOpVoid    = "void"
)

// Lifecycle columns TransitionStatus is allowed to stamp. Named because the
// forward path and the resolution path must set exactly the same ones — a typo in
// either would silently drop a stamp the other relies on.
const (
	colProviderPaymentID = "provider_payment_id"
	colAuthorizedAt      = "authorized_at"
	colExpiresAt         = "expires_at"
	colDeclineCode       = "decline_code"
)

// providerKey builds the provider-facing idempotency key for an intent-level
// mutation. Deterministic on (operation, payment) so every retry of the SAME
// logical operation carries the SAME key — which is the whole point: the
// provider replays its original answer instead of re-evaluating and telling a
// post-doubt retry "already done" as though that were a failure.
//
// The payment id is the right anchor rather than a per-attempt value: a capture
// is captured once, and all its attempts are the same operation.
func providerKey(op string, paymentID int64) string {
	return fmt.Sprintf("%s:payment:%d", op, paymentID)
}

// releaseTimeout bounds the detached key-release call. Short on purpose: it runs
// on the failure path, after the caller is already waiting.
const releaseTimeout = 3 * time.Second

// releaseKey unlocks a claimed idempotency key after a failed attempt so an
// immediate same-key retry can re-drive.
//
// It deliberately does NOT ride the request context. The canonical reason we
// are here is a provider TIMEOUT, which means ctx is already past its deadline —
// releasing on it would fail exactly when the release matters most, leaving the
// key locked while the response tells the caller to retry immediately. The
// retry would then bounce off ErrLocked until the takeover window.
func (s *Service) releaseKey(ctx context.Context, keyID int64) error {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	err := s.idem.Release(rctx, keyID)
	if err != nil {
		// Counted so the lockout is alertable, and recorded on the request's span
		// so the specific stuck key is traceable — the logic layer carries no
		// logger of its own, and a bare counter would say something is wrong
		// without saying which caller is locked out.
		recordKeyReleaseFailure(rctx)
		span := trace.SpanFromContext(rctx)
		span.RecordError(err, trace.WithAttributes(
			attribute.Int64("payment.idempotency_key_id", keyID)))
	}
	return err
}

// withKeyReleased releases the key and returns the error the caller should see.
// The provider's outcome stays the PRIMARY error — errors.Is on it must keep
// working, because that is what decides whether a retry is safe — while a failed
// release rides along as context. Both facts matter: one says what the money did,
// the other says the caller's next same-key retry will bounce until takeover.
func (s *Service) withKeyReleased(ctx context.Context, keyID int64, outcome error) error {
	if rerr := s.releaseKey(ctx, keyID); rerr != nil {
		return fmt.Errorf("%w; lock release failed: %w", outcome, rerr)
	}
	return outcome
}

// CreateRefund runs the idempotent (partial) refund flow. P1 settles
// synchronously against the provider stub; async webhook settlement is P2.
func (s *Service) CreateRefund(ctx context.Context, idemKey string, paymentID, userID, amountMinor int64, reason string) (*domain.Refund, bool, error) {
	in := struct {
		PaymentID int64  `json:"payment_id"`
		UserID    int64  `json:"user_id"`
		Amount    int64  `json:"amount"`
		Reason    string `json:"reason"`
	}{paymentID, userID, amountMinor, reason}

	key, proceed, err := s.idem.Claim(ctx, userID, idemKey, "POST",
		fmt.Sprintf("/payment/v1/internal/payments/%d/refunds", paymentID), hashJSON(in))
	if err != nil {
		return nil, false, mapClaimErr(err)
	}
	if !proceed {
		var ref domain.Refund
		if err := json.Unmarshal(key.ResponseBody, &ref); err != nil {
			return nil, false, fmt.Errorf("corrupt idempotency cache: %w", err)
		}
		return &ref, true, nil
	}

	pay, err := s.payments.FindByID(ctx, paymentID, 0)
	if err != nil {
		return nil, false, s.withKeyReleased(ctx, key.ID, err)
	}
	// The refund insert is idempotent by this scoped key: a crash-recovery
	// retry adopts the existing refund rather than creating a second one.
	scopedKey := fmt.Sprintf("%d:%s", userID, idemKey)
	ref, err := s.payments.CreateRefund(ctx, pay.ID, amountMinor, reason, scopedKey)
	if err != nil {
		if errors.Is(err, domain.ErrRefundRejected) {
			recordOperation(ctx, opRefund, resultRejected)
		}
		return nil, false, s.withKeyReleased(ctx, key.ID, err)
	}

	// Drive the refund when its fate is still open. Two statuses qualify and the
	// difference is only WHY: `pending` was never asked, `processing` was asked
	// and never answered. Both must reach the provider — for `processing` the
	// call replays under the same scoped key rather than paying twice — and
	// leaving `processing` out would strand the refund in the exact state whose
	// error message asks the caller to retry.
	//
	// An adopted refund that already settled (crash after settle, before finish)
	// must not be re-sent to the provider or re-settled — just finish.
	if ref.Status == domain.RefundPending || ref.Status == domain.RefundProcessing {
		if err := s.settlePendingRefund(ctx, pay, ref, amountMinor, scopedKey); err != nil {
			return nil, false, s.withKeyReleased(ctx, key.ID, err)
		}
	}

	// ONLY a succeeded refund is sealed. Sealing any other outcome would cache
	// it as the final answer for this key, and since the caller cannot mint a
	// different key for the same refund, the money could never be sent again.
	// This also covers the adopted-refund door above: a `failed` row reached by
	// an earlier attempt answers with an error, not a cached 201.
	if ref.Status != domain.RefundSucceeded {
		// Counted, or a refund rejected on adoption is invisible: no provider
		// call happened on this attempt, so nothing else records it.
		recordOperation(ctx, opRefund, resultRejected)
		// Keep the verdict's certainty: an adopted `failed` row was settled by a
		// DEFINITE decline (that is the only way this status is written), so the
		// caller must stop rather than retry a decision the provider already
		// made. Anything else is still open.
		outcome := fmt.Errorf("%w: refund is %s", domain.ErrRefundNotSettled, ref.Status)
		if ref.Status == domain.RefundFailed {
			outcome = fmt.Errorf("%w: refund %d already failed", domain.ErrRefundDeclined, ref.ID)
		}
		return nil, false, s.withKeyReleased(ctx, key.ID, outcome)
	}

	body, _ := json.Marshal(ref)
	if err := s.idem.Finish(ctx, key.ID, 201, body); err != nil {
		// The money moved but the answer is an error, so the caller will retry;
		// release the key or that retry bounces off ErrLocked. The retry adopts
		// this succeeded refund and seals it, without touching the provider.
		return nil, false, s.withKeyReleased(ctx, key.ID, err)
	}
	return ref, false, nil
}

// settlePendingRefund sends a pending refund to the provider, persists the
// outcome, updates ref in place, and records the operation metric. Callers must
// gate this on ref.Status == RefundPending, so a crash-recovery retry that
// adopts an already-settled refund never re-settles or re-counts.
func (s *Service) settlePendingRefund(ctx context.Context, pay *domain.Payment, ref *domain.Refund, amountMinor int64, scopedKey string) error {
	providerRefundID, provErr := s.prov.Refund(ctx, pay.ProviderPaymentID, amountMinor, scopedKey)
	class := classifyProviderOutcome(provErr)
	attemptErr := s.recordAttempt(ctx, domain.Attempt{
		PaymentID: pay.ID, Operation: domain.AttemptRefund, Outcome: class,
		RefundID: &ref.ID, ProviderRef: providerRefundID,
		ProviderStatus: providerStatusOf(provErr), IdempotencyKey: scopedKey,
	})
	// A real answer about this refund also answers any earlier round-trip that
	// went unheard. A 429 does not: it says nothing about the earlier call.
	if class == domain.OutcomeSuccess || class == domain.OutcomeBusinessDecline {
		s.closeRefundDoubt(ctx, pay.ID, ref.ID)
	}
	if provErr != nil {
		return s.refundNotSucceeded(ctx, ref, class, attemptErr, provErr)
	}
	if err := s.payments.SettleRefund(ctx, ref.ID, domain.RefundSucceeded, providerRefundID); err != nil {
		// The money MOVED but we could not record it. That is the ambiguous case
		// too — from the outside the refund is unsettled — and the same-key retry
		// replays the provider's answer and re-persists it.
		return fmt.Errorf("%w: recording the refund failed: %w", domain.ErrRefundNotSettled, err)
	}
	ref.Status = domain.RefundSucceeded
	ref.ProviderRefundID = providerRefundID
	recordOperation(ctx, opRefund, resultOK)
	return nil
}

// refundNotSucceeded turns a provider failure into the right persisted state and
// a non-nil error, because a refund that did not happen must never be answered
// as one (and so must never reach idem.Finish — see CreateRefund).
//
// The split is between a DEFINITE and an UNKNOWN outcome:
//
//   - Decided (a business decline, or any other final answer such as a malformed
//     request or an unknown charge): no money moved and a retry cannot change
//     that. Record `failed` — which releases the reserved amount — and return
//     ErrRefundDeclined (not retryable).
//   - Undecided (timeout, transport error, 5xx): we do not know whether the money
//     moved. Recording `failed` would mark an ambiguous outcome definite, so the
//     row stays `pending` and ErrRefundNotSettled asks the caller to retry with
//     the SAME idempotency key.
//
// What protects the money on that retry is the PROVIDER's idempotency key, which
// the retry re-sends: the provider replays its own answer instead of paying
// again. The pending row's held reserve is a narrower guard — it only caps the
// TOTAL refunded against the capture, so it stops a same-money retry only when
// the refund covers the whole remaining refundable amount. Sending the same key
// is what makes a partial refund safe, which is why the error says so.
func (s *Service) refundNotSucceeded(ctx context.Context, ref *domain.Refund, class domain.OutcomeClass, attemptErr, provErr error) error {
	if class == domain.OutcomeRetryableFailure {
		// The provider refused to take the request and did nothing with it, so the
		// refund was never asked. It stays `pending` — still reserving its amount —
		// and the caller retries. Parking it `processing` would claim we are unsure
		// about something the provider was explicit about.
		recordOperation(ctx, opRefund, resultError)
		return fmt.Errorf("%w: %w", domain.ErrRefundNotSettled, provErr)
	}
	if class == domain.OutcomeUnknown {
		recordOperation(ctx, opRefund, resultUnknown)
		recordProviderUnknown(ctx, opRefund)
		if attemptErr != nil {
			// Same rule as the intent-level parks: no evidence, no park. A refund
			// sitting in `processing` that no attempt row explains cannot be resolved
			// by anything, and `pending` at least keeps the reserve and reads as
			// "never asked" rather than as a question nobody can look up.
			return fmt.Errorf("%w: %w (not parked: the attempt log refused the evidence: %w)",
				domain.ErrRefundNotSettled, provErr, attemptErr)
		}
		// `processing`, not `pending`: both reserve the amount against a second
		// refund of the same money, but only one says the provider was already
		// asked. A resolver needs that distinction to know whether to ask what
		// happened or simply to send the refund for the first time.
		if err := s.payments.SettleRefund(ctx, ref.ID, domain.RefundProcessing, ""); err != nil {
			return fmt.Errorf("%w: parking the refund failed: %w", domain.ErrRefundNotSettled, err)
		}
		ref.Status = domain.RefundProcessing
		return fmt.Errorf("%w: %w", domain.ErrRefundNotSettled, provErr)
	}
	recordOperation(ctx, opRefund, resultDeclined)
	if err := s.payments.SettleRefund(ctx, ref.ID, domain.RefundFailed, ""); err != nil {
		// The verdict itself is now unpersisted, so the outcome is open again:
		// ask for a same-key retry rather than reporting a decline we did not
		// manage to record.
		return fmt.Errorf("%w: recording the decline failed: %w", domain.ErrRefundNotSettled, err)
	}
	ref.Status = domain.RefundFailed
	return fmt.Errorf("%w: %w", domain.ErrRefundDeclined, provErr)
}

// Get returns one payment scoped to its owner.
func (s *Service) Get(ctx context.Context, id, userID int64) (*domain.Payment, error) {
	return s.payments.FindByID(ctx, id, userID)
}

// GetByOrderID returns the payment attached to an order — the saga's lookup key
// for capture/void/refund (which it knows by order, not payment id).
func (s *Service) GetByOrderID(ctx context.Context, orderID int64) (*domain.Payment, error) {
	return s.payments.FindByOrderID(ctx, orderID)
}

// List returns a page of the user's payments and the total count.
func (s *Service) List(ctx context.Context, userID int64, page, pageSize int) ([]domain.Payment, int, error) {
	return s.payments.ListByUser(ctx, userID, pageSize, (page-1)*pageSize)
}

// ExpireHolds flips stale authorized holds to expired; the cron entrypoint.
func (s *Service) ExpireHolds(ctx context.Context) (int64, error) {
	return s.payments.ExpireStaleAuthorizations(ctx, s.now())
}

// ReapIdempotencyKeys removes keys past ttl (24h default).
func (s *Service) ReapIdempotencyKeys(ctx context.Context, ttl time.Duration) (int64, error) {
	return s.idem.Reap(ctx, ttl)
}
