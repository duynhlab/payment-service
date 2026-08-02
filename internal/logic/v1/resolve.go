package v1

import (
	"context"
	"errors"
	"fmt"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

// Resolution is the other half of the phase-6 rule. Parking an intent in
// `processing` is only defensible if the doubt can be settled, so the escape
// ships with the trap: without this file `processing` would be a state a payment
// enters and never leaves, which is worse than the wrong answer it replaced.
//
// The mechanism is one idea: ask the provider the SAME question again, under the
// SAME idempotency key. A provider that already performed the work replays its
// answer; one that never received it performs it now. Either way we learn what is
// true without ever performing the semantic opposite, and without performing the
// work twice. Re-driving under a DIFFERENT key is not a resolution — it is a
// second charge, which is precisely how the naive version of this loses money.
//
// Resolution runs on the request path, not only in a background sweep: the caller
// who retries an operation is the one who most needs an answer, and making them
// wait for a ticker would turn every parked payment into an incident.

// resolveIntentDoubt settles the open doubt on a parked payment and returns the
// payment as it stands afterwards. A doubt that survives the round-trip leaves
// the payment `processing` — the caller decides what to say about that.
//
// It never returns a payment in a state the caller did not ask about: every
// transition it makes is CAS-guarded from `processing`, so a racing resolver (or
// a concurrent operator action) makes this pass a no-op rather than an overwrite.
func (s *Service) resolveIntentDoubt(ctx context.Context, pay *domain.Payment) (*domain.Payment, error) {
	if pay.Status != domain.StatusProcessing {
		return pay, nil
	}
	open, err := s.attempts.ListOpenForPayment(ctx, pay.ID)
	if err != nil {
		// Wrapped as doubt, not as a plain failure: the money state IS in doubt, and
		// a caller that reads this as a decided error compensates on it.
		return nil, fmt.Errorf("%w: payment %d: reading the open attempts failed: %w",
			domain.ErrOutcomeUnknown, pay.ID, err)
	}
	if len(open) == 0 {
		// The row says "we are unsure" and nothing records what we were unsure
		// about. The only way here is a lost attempt write (counted by
		// payment.attempt.write_failures.total), so this is an operator worklist
		// item, not something to guess at. Report it as doubt so no caller reads it
		// as an outcome.
		return nil, fmt.Errorf("%w: payment %d is parked with no open attempt to resolve", domain.ErrOutcomeUnknown, pay.ID)
	}

	for _, a := range open {
		// Re-read per attempt. Each resolution can move the row, and handing the next
		// one a snapshot from before that move would make it transition from a state
		// the payment has already left.
		fresh, err := s.payments.FindByID(ctx, pay.ID, 0)
		if err != nil {
			return nil, err
		}
		if fresh.Status != domain.StatusProcessing {
			break // somebody's answer already landed; the rest is theirs to close
		}
		if err := s.resolveAttempt(ctx, fresh, a); err != nil {
			return nil, err
		}
	}
	return s.payments.FindByID(ctx, pay.ID, 0)
}

// resolveAttempt re-drives one open attempt. A refund attempt is skipped here:
// refund doubt lives on the refund row, and CreateRefund re-drives it under the
// refund's own key — the payment stays `captured` throughout, so it is never
// parked on a refund's behalf.
func (s *Service) resolveAttempt(ctx context.Context, pay *domain.Payment, a domain.Attempt) error {
	switch a.Operation {
	case domain.AttemptAuthorize:
		return s.resolveAuthorize(ctx, pay, a)
	case domain.AttemptCapture:
		return s.resolveCapture(ctx, pay, a)
	case domain.AttemptVoid:
		return s.resolveVoid(ctx, pay, a)
	case domain.AttemptRefund:
		return nil
	}
	return fmt.Errorf("resolve attempt %d: unknown operation %q", a.ID, a.Operation)
}

// resolveAuthorize replays the charge under its original key. This is the one
// operation whose key cannot be derived — it was built from the caller's
// Idempotency-Key — so an attempt row written before that key was recorded is
// unresolvable here and says so rather than charging again.
func (s *Service) resolveAuthorize(ctx context.Context, pay *domain.Payment, a domain.Attempt) error {
	if a.IdempotencyKey == "" {
		return fmt.Errorf("%w: attempt %d records no provider key, so the charge cannot be replayed without risking a second one",
			domain.ErrOutcomeUnknown, a.ID)
	}
	charge, chErr := s.prov.Charge(ctx, provider.ChargeRequest{
		IdempotencyKey: a.IdempotencyKey,
		AmountMinor:    pay.AmountMinor,
		Currency:       pay.Currency,
		PaymentMethod:  pay.PaymentMethod,
		AutoCapture:    pay.CaptureMethod == domain.CaptureAutomatic,
	})
	class := s.recordRoundTrip(ctx, a, chargeRef(charge), chErr)

	switch class {
	case domain.OutcomeSuccess:
		err := s.transitionParked(ctx, pay.ID, domain.StatusAuthorized,
			map[string]any{
				colProviderPaymentID: charge.ProviderPaymentID,
				colAuthorizedAt:      s.now(),
				colExpiresAt:         s.now().Add(s.holdTTL),
			})
		// An automatic-capture intent authorizes and captures together; the park
		// happened before either landed, so the capture leg is posted here.
		if err == nil && pay.CaptureMethod == domain.CaptureAutomatic {
			if cerr := s.payments.CaptureWithLedger(ctx, pay.ID, s.now()); cerr != nil && !errors.Is(cerr, domain.ErrStaleTransition) {
				err = cerr
			}
		}
		if err == nil {
			recordAuthorization(ctx, authAuthorized, currencyLabel(pay.Currency))
		}
		return s.closeAfterVerdict(ctx, a, err)

	case domain.OutcomeBusinessDecline:
		err := s.transitionParked(ctx, pay.ID, domain.StatusFailed,
			map[string]any{colDeclineCode: declineCode(chErr)})
		if err == nil {
			recordAuthorization(ctx, authDeclined, currencyLabel(pay.Currency))
		}
		return s.closeAfterVerdict(ctx, a, err)

	case domain.OutcomeRetryableFailure, domain.OutcomeUnknown:
		// Still no answer, and the payment stays parked. The doubt carries over to
		// the row recordRoundTrip just opened, so the next retry asks once, not twice.
		return nil
	}
	return nil
}

// resolveCapture re-asks whether the capture landed. The capture ledger leg was
// posted before the provider was called and is deliberately left in place while
// the answer is unknown, so success here is a status move only; a decided refusal
// is the one place a reversal is correct — as a conclusion, not a reflex.
func (s *Service) resolveCapture(ctx context.Context, pay *domain.Payment, a domain.Attempt) error {
	key := attemptKey(a, providerOpCapture, pay.ID)
	capErr := s.prov.Capture(ctx, pay.ProviderPaymentID, key)
	class := s.recordRoundTrip(ctx, withKey(a, key), pay.ProviderPaymentID, capErr)

	switch class {
	case domain.OutcomeSuccess:
		err := s.transitionParked(ctx, pay.ID, domain.StatusCaptured, nil)
		if err == nil {
			recordOperation(ctx, opCapture, resultOK)
		}
		return s.closeAfterVerdict(ctx, a, err)

	case domain.OutcomeBusinessDecline:
		// The provider says this capture did not happen and cannot. ReverseCapture
		// moves processing→authorized and posts the compensating ledger legs, so the
		// books stop asserting revenue nobody collected.
		err := s.payments.ReverseCapture(ctx, pay.ID)
		if errors.Is(err, domain.ErrStaleTransition) {
			err = nil // another resolver reached the same conclusion first
		}
		if err == nil {
			recordOperation(ctx, opCapture, resultDeclined)
		}
		return s.closeAfterVerdict(ctx, a, err)

	case domain.OutcomeRetryableFailure, domain.OutcomeUnknown:
		return nil
	}
	return nil
}

// resolveVoid re-asks whether the hold was released.
//
// A decided refusal returns the row to `authorized`, which is the pre-void truth
// and the safe direction: if the provider refused because it had already captured
// instead, the row is now authorized-while-the-provider-holds-captured — the one
// drift class reconciliation detects and heals (ADR-012). Claiming `voided` on a
// refusal would instead leave us believing money was released that was not.
func (s *Service) resolveVoid(ctx context.Context, pay *domain.Payment, a domain.Attempt) error {
	key := attemptKey(a, providerOpVoid, pay.ID)
	voidErr := s.prov.Void(ctx, pay.ProviderPaymentID, key)
	class := s.recordRoundTrip(ctx, withKey(a, key), pay.ProviderPaymentID, voidErr)

	switch class {
	case domain.OutcomeSuccess:
		err := s.transitionParked(ctx, pay.ID, domain.StatusVoided, nil)
		if err == nil {
			recordOperation(ctx, opVoid, resultOK)
		}
		return s.closeAfterVerdict(ctx, a, err)

	case domain.OutcomeBusinessDecline:
		err := s.transitionParked(ctx, pay.ID, domain.StatusAuthorized, nil)
		if err == nil {
			recordOperation(ctx, opVoid, resultDeclined)
		}
		return s.closeAfterVerdict(ctx, a, err)

	case domain.OutcomeRetryableFailure, domain.OutcomeUnknown:
		return nil
	}
	return nil
}

// recordRoundTrip classifies a provider answer, appends the attempt row for it,
// and counts it as a resolution. The new row is its own round-trip: the original
// attempt keeps UNKNOWN because that is genuinely what it returned, and history
// is not rewritten to look wiser than it was.
// recordRoundTrip appends the attempt row for a resolution's provider call and
// closes the attempt it re-asked. The new row is its own round-trip: the original
// keeps UNKNOWN because that is genuinely what it returned, and history is not
// rewritten to look wiser than it was.
//
// It also keeps the worklist BOUNDED. When the answer is still UNKNOWN, the new
// row inherits the question and the original is closed immediately: leaving both
// open means the next pass finds two questions, asks the provider twice, and opens
// two more, so the work per retry doubles. A provider outage — the exact condition
// this feature is for — would become a self-inflicted flood against that provider.
//
// When the answer is DECIDED the original stays open until the verdict has been
// applied (see closeAfterVerdict). Closing it here instead would mean a crash
// between the close and the transition leaves a parked payment with nothing on the
// worklist to explain it: unresolvable, which is the one outcome worse than doubt.
func (s *Service) recordRoundTrip(ctx context.Context, prior domain.Attempt, ref string, err error) domain.OutcomeClass {
	class := classifyProviderOutcome(err)
	op := prior.Operation
	recordErr := s.recordAttempt(ctx, domain.Attempt{
		PaymentID:      prior.PaymentID,
		Operation:      op,
		Outcome:        class,
		ProviderRef:    ref,
		ProviderStatus: providerStatusOf(err),
		IdempotencyKey: prior.IdempotencyKey,
		RefundID:       prior.RefundID,
	})
	if recordErr == nil && class == domain.OutcomeUnknown {
		// The doubt carried over to the row just written; this one is superseded.
		_ = s.closeAttempt(ctx, prior)
	}
	recordResolution(ctx, string(op), class)
	if class == domain.OutcomeUnknown {
		recordProviderUnknown(ctx, string(op))
	}
	return class
}

// closeAfterVerdict closes the question a decided answer settled, and is called
// only once that answer has been written to the payment. A crash before this point
// leaves the attempt open, so the next pass re-asks and reaches the same
// conclusion — the redo is free, because it asks under the same key.
func (s *Service) closeAfterVerdict(ctx context.Context, a domain.Attempt, applied error) error {
	if applied != nil {
		return applied
	}
	return s.closeAttempt(ctx, a)
}

// transitionParked applies a resolution's verdict to a parked row. A lost CAS is
// not an error: another resolver, or an operator, reached the same conclusion
// first. Returning ErrStaleTransition here would surface as a PRECONDITION
// FAILURE, which the saga reads as permanent — so a race between two resolvers
// would make the caller compensate a payment that was just settled correctly.
func (s *Service) transitionParked(ctx context.Context, paymentID int64, to domain.Status, set map[string]any) error {
	err := s.payments.TransitionStatus(ctx, paymentID, domain.StatusProcessing, to, set)
	if errors.Is(err, domain.ErrStaleTransition) {
		return nil
	}
	return err
}

// closeAttempt stamps the original attempt resolved. A lost race (another
// resolver got there first) is not an error: the doubt is closed either way, and
// the state transition above is CAS-guarded so only one pass can have applied it.
func (s *Service) closeAttempt(ctx context.Context, a domain.Attempt) error {
	err := s.attempts.Resolve(ctx, a.ID, s.now())
	if err == nil || errors.Is(err, domain.ErrNotFound) {
		return nil
	}
	return fmt.Errorf("close attempt %d: %w", a.ID, err)
}

// withKey stamps the key a re-drive actually used onto the attempt it supersedes,
// so the row recordRoundTrip writes says which key was sent — including for the
// pre-000011 rows the fallback derivation covers.
func withKey(a domain.Attempt, key string) domain.Attempt {
	a.IdempotencyKey = key
	return a
}

// attemptKey returns the key a re-drive must use: the one the original round-trip
// recorded, falling back to the deterministic intent-level derivation. The
// fallback exists for attempt rows written before the key was persisted — for
// capture and void the key is a pure function of the payment id, so deriving it
// reproduces the original exactly. Authorize has no such fallback, which is why
// it refuses instead.
func attemptKey(a domain.Attempt, op string, paymentID int64) string {
	if a.IdempotencyKey != "" {
		return a.IdempotencyKey
	}
	return providerKey(op, paymentID)
}

// declineCode is what to record as the reason a decided failure failed: the
// provider's decline code when it gave one, and otherwise its own status — a
// definite refusal that is not a business decline (malformed request, unknown
// charge) still has to leave a reason on the row.
func declineCode(err error) string {
	var declined *provider.DeclinedError
	if errors.As(err, &declined) {
		return declined.Code
	}
	return providerStatusOf(err)
}

// park records that an operation's outcome is unknown and returns the error the
// caller must see.
//
// THE EVIDENCE LANDS FIRST, and the park does not happen without it. A row in
// `processing` that no attempt explains cannot be resolved by a retry, by the
// sweep, or by anything short of manual SQL — it is a dead end, which is worse
// than the state it replaced. When the attempt log refuses the write we keep the
// pre-park state (for a capture: `captured` with its ledger leg posted) and say
// so. That state is a claim the provider has not confirmed, but it is the
// internal-ahead-of-provider drift reconciliation is built to detect, and the
// refusal is counted by payment.attempt.write_failures.total.
//
// The error stays an UNKNOWN one down every path, including a lost park CAS:
// reporting a precondition failure there is what made callers compensate an
// operation that may well have succeeded.
func (s *Service) park(ctx context.Context, from domain.Status, a domain.Attempt, cause error) error {
	unknown := fmt.Errorf("%w: %s: %w", domain.ErrOutcomeUnknown, a.Operation, cause)
	if err := s.recordAttempt(ctx, a); err != nil {
		return fmt.Errorf("%w (not parked: the attempt log refused the evidence: %w)", unknown, err)
	}
	if err := s.payments.TransitionStatus(ctx, a.PaymentID, from, domain.StatusProcessing, nil); err != nil {
		return fmt.Errorf("%w (parking it failed: %w)", unknown, err)
	}
	return unknown
}

// settleBeforeOperating gives a parked payment its answer before an operation
// acts on it, so a caller retrying is the mechanism that un-parks — no background
// sweep required for a customer to make progress. Doubt that survives the
// round-trip is reported as doubt, never as a rejection: a rejection would tell
// the saga to stop and compensate an operation that may have succeeded.
func (s *Service) settleBeforeOperating(ctx context.Context, pay *domain.Payment, op string) (*domain.Payment, error) {
	if pay.Status != domain.StatusProcessing {
		return pay, nil
	}
	settled, err := s.resolveIntentDoubt(ctx, pay)
	if err != nil {
		return nil, err
	}
	if settled.Status == domain.StatusProcessing {
		return nil, fmt.Errorf("%w: %s", domain.ErrOutcomeUnknown, op)
	}
	return settled, nil
}

// closeRefundDoubt stamps this refund's open attempts resolved once a later
// round-trip gave a real answer. Best-effort on purpose: the answer is already
// persisted on the refund row, and failing the settled refund because its audit
// annotation would not write would trade money for bookkeeping. The cost of
// losing here is a stale worklist row, which over-reports doubt — an operator who
// looks finds a settled refund, which is the safe direction to be wrong in.
func (s *Service) closeRefundDoubt(ctx context.Context, paymentID, refundID int64) {
	open, err := s.attempts.ListOpenForPayment(ctx, paymentID)
	if err != nil {
		return
	}
	for _, a := range open {
		if a.Operation == domain.AttemptRefund && a.RefundID != nil && *a.RefundID == refundID {
			_ = s.closeAttempt(ctx, a)
		}
	}
}
