package v1

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

// The tests in this file all describe the same shape: the provider PERFORMED the
// work and the answer was lost. Parking the intent is only half an answer — every
// park must have a way out, or `processing` is a worse bug than the reflexive
// reversal it replaced. Each test therefore drives a park and then drives the
// escape, and asserts the money did not move twice on the way through.

// parkedIntent builds a service whose provider loses the answer to the named
// operation, drives an intent to that park, and returns everything a test needs
// to continue. The payment is left authorized (or parked, for authorize).
func newDoubtService() (*Service, *fakePayments, *failingProvider, *recordingAttempts) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub()}
	rec := &recordingAttempts{}
	return NewService(fp, fi, prov, 168*time.Hour, WithAttempts(rec)), fp, prov, rec
}

func intentFor(order int64, amount int64) CreateIntentInput {
	in := intent(amount)
	in.OrderID = &order
	return in
}

// TestParkedIntent_FreshKeyResolvesAndNeverChargesTwice is the one that pays for
// this whole file. A parked payment is "still open", and the naive reading of that
// is "re-drive the charge" — but the provider idempotency key is derived from the
// CALLER's key, so a retry arriving under a fresh Idempotency-Key does not replay,
// it mints a second charge. One order, two charges, and the customer's card is hit
// twice. Resolution must re-drive under the ORIGINAL key instead.
func TestParkedIntent_FreshKeyResolvesAndNeverChargesTwice(t *testing.T) {
	svc, _, prov, _ := newDoubtService()
	ctx := context.Background()
	const order = int64(91)

	prov.chargeThenErr = context.DeadlineExceeded
	if _, err := svc.CreateIntent(ctx, "key-1", intentFor(order, 2000)); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("first attempt err = %v, want ErrOutcomeUnknown", err)
	}
	if got := prov.Stub.Charges(); got != 1 {
		t.Fatalf("charges after the lost answer = %d, want 1", got)
	}

	// The provider recovers, and a DIFFERENT caller key arrives for the same order.
	prov.chargeThenErr = nil
	res, err := svc.CreateIntent(ctx, "key-2", intentFor(order, 2000))
	if err != nil {
		t.Fatalf("second attempt err = %v, want the doubt resolved", err)
	}
	if res.Code != 201 || res.Payment.Status != domain.StatusAuthorized {
		t.Fatalf("code=%d status=%s, want 201/authorized", res.Code, res.Payment.Status)
	}
	if got := prov.Stub.Charges(); got != 1 {
		t.Fatalf("charges after resolution = %d, want 1 — the card must not be charged twice", got)
	}
}

// A same-key retry must also complete the park, not bounce off it. Before the
// escape existed, the retry got FailedPrecondition ("charge succeeded but payment
// is processing"), which the saga reads as a permanent rejection and compensates.
func TestParkedIntent_SameKeyRetryCompletesTheAuthorization(t *testing.T) {
	svc, _, prov, rec := newDoubtService()
	ctx := context.Background()

	prov.chargeThenErr = context.DeadlineExceeded
	if _, err := svc.CreateIntent(ctx, "key-same", intent(2000)); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("first attempt err = %v, want ErrOutcomeUnknown", err)
	}
	prov.chargeThenErr = nil

	res, err := svc.CreateIntent(ctx, "key-same", intent(2000))
	if err != nil {
		t.Fatalf("same-key retry err = %v, want the authorization completed", err)
	}
	if res.Payment.Status != domain.StatusAuthorized || res.Payment.ProviderPaymentID == "" {
		t.Fatalf("status=%s provider_ref=%q, want authorized with the reference learned",
			res.Payment.Status, res.Payment.ProviderPaymentID)
	}
	if open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID); len(open) != 0 {
		t.Fatalf("open attempts = %d, want 0 — the doubt was answered", len(open))
	}
}

// TestParkedCapture_RetryConfirmsWithoutReversing: the capture landed and the
// answer was lost. The old behaviour reversed on the spot, taking the money out of
// our books while the provider kept it. The park must survive, and the retry must
// confirm it — with the capture ledger leg intact throughout.
func TestParkedCapture_RetryConfirmsWithoutReversing(t *testing.T) {
	svc, fp, prov, rec := newDoubtService()
	ctx := context.Background()

	res, err := svc.CreateIntent(ctx, "k-cap-park", intent(2000))
	if err != nil {
		t.Fatal(err)
	}
	prov.captureThenErr = context.DeadlineExceeded
	if _, err := svc.Capture(ctx, res.Payment.ID, 7); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("capture err = %v, want ErrOutcomeUnknown", err)
	}
	parked, _ := svc.Get(ctx, res.Payment.ID, 7)
	if parked.Status != domain.StatusProcessing {
		t.Fatalf("status = %s, want processing", parked.Status)
	}
	if fp.reversals != 0 {
		t.Fatalf("reversals = %d, want 0 — an unknown outcome must never reverse", fp.reversals)
	}

	prov.captureThenErr = nil
	got, err := svc.Capture(ctx, res.Payment.ID, 7)
	if err != nil {
		t.Fatalf("retry err = %v, want the capture confirmed", err)
	}
	if got.Status != domain.StatusCaptured {
		t.Fatalf("status = %s, want captured", got.Status)
	}
	if fp.ledgerPosts != 1 || fp.reversals != 0 {
		t.Fatalf("ledger posts = %d, reversals = %d; want exactly one capture and no reversal",
			fp.ledgerPosts, fp.reversals)
	}
	if open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID); len(open) != 0 {
		t.Fatalf("open attempts = %d, want 0", len(open))
	}
}

// The other half: the provider answers definitively that the capture did NOT
// happen. Only then is a reversal correct — as a conclusion, not a reflex.
func TestParkedCapture_DefiniteRefusalReversesDeliberately(t *testing.T) {
	svc, fp, prov, _ := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-cap-def", intent(2000))
	// Round-trip 1 loses the answer (park). Round-trip 2 is the resolution, and it
	// answers definitively that the capture never happened. Round-trip 3 is the
	// capture the caller asked for, which now goes through — a resolved doubt does
	// not have to also be a failed request.
	prov.captureErrs = []error{
		context.DeadlineExceeded,
		fmt.Errorf("%w: no such charge", provider.ErrDefinite),
	}
	if _, err := svc.Capture(ctx, res.Payment.ID, 7); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("capture err = %v, want ErrOutcomeUnknown", err)
	}

	got, err := svc.Capture(ctx, res.Payment.ID, 7)
	if err != nil {
		t.Fatalf("retry err = %v, want the doubt resolved and the capture completed", err)
	}
	if got.Status != domain.StatusCaptured {
		t.Fatalf("status = %s, want captured", got.Status)
	}
	// Exactly one reversal: the deliberate one, taken only after the provider gave
	// a decided answer. Zero would mean the books still assert revenue nobody
	// collected; two would mean the reflex is back.
	if fp.reversals != 1 {
		t.Fatalf("reversals = %d, want exactly 1 — a reversal is a conclusion, not a reflex", fp.reversals)
	}
}

// A void whose answer was lost must not roll back to `authorized`: if the void did
// land, that leaves us believing we can capture money the provider has released.
func TestParkedVoid_RetryConfirmsTheRelease(t *testing.T) {
	svc, _, prov, _ := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-void-park", intent(2000))
	prov.voidThenErr = context.DeadlineExceeded
	if _, err := svc.Void(ctx, res.Payment.ID, 7); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("void err = %v, want ErrOutcomeUnknown", err)
	}
	parked, _ := svc.Get(ctx, res.Payment.ID, 7)
	if parked.Status != domain.StatusProcessing {
		t.Fatalf("status = %s, want processing (never authorized — the hold may be gone)", parked.Status)
	}

	prov.voidThenErr = nil
	got, err := svc.Void(ctx, res.Payment.ID, 7)
	if err != nil {
		t.Fatalf("retry err = %v, want the release confirmed", err)
	}
	if got.Status != domain.StatusVoided {
		t.Fatalf("status = %s, want voided", got.Status)
	}
}

// TestParkedRefund_RetrySettlesTheProviderAnswer: the refund reached the provider
// and the answer was lost, so the row is parked `processing`. The retry must be
// able to settle it — the CAS that only accepted `pending` made this refund
// permanently unsettleable, which is worse than the status it replaced: the
// provider had paid the customer and no row could ever record it.
func TestParkedRefund_RetrySettlesTheProviderAnswer(t *testing.T) {
	svc, _, prov, _ := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-ref-park", intent(2000))
	if _, err := svc.Capture(ctx, res.Payment.ID, 7); err != nil {
		t.Fatal(err)
	}

	prov.refundThenErr = context.DeadlineExceeded
	if _, _, err := svc.CreateRefund(ctx, "rk-park", res.Payment.ID, 7, 500, ""); !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("first refund err = %v, want ErrRefundNotSettled", err)
	}

	prov.refundThenErr = nil
	ref, _, err := svc.CreateRefund(ctx, "rk-park", res.Payment.ID, 7, 500, "")
	if err != nil {
		t.Fatalf("retry err = %v, want the refund settled", err)
	}
	if ref.Status != domain.RefundSucceeded || ref.ProviderRefundID == "" {
		t.Fatalf("refund = %s/%q, want succeeded with a provider reference", ref.Status, ref.ProviderRefundID)
	}
}

// A refund round-trip is a provider round-trip, so it leaves an attempt row. The
// doubt worklist is per-operation, and refunds are the operation whose parked
// state strands real money — a worklist that cannot contain them is not a
// worklist.
func TestRefund_RecordsAttemptsAndClosesItsDoubt(t *testing.T) {
	svc, _, prov, rec := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-ref-att", intent(2000))
	if _, err := svc.Capture(ctx, res.Payment.ID, 7); err != nil {
		t.Fatal(err)
	}
	prov.refundThenErr = context.DeadlineExceeded
	_, _, _ = svc.CreateRefund(ctx, "rk-att", res.Payment.ID, 7, 500, "")

	open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID)
	if len(open) != 1 || open[0].Operation != domain.AttemptRefund {
		t.Fatalf("open attempts = %+v, want one refund attempt", open)
	}
	if open[0].RefundID == nil {
		t.Fatal("refund attempt records no refund id, so nothing can tie the doubt to the money")
	}
	if open[0].IdempotencyKey == "" {
		t.Fatal("refund attempt records no key, so a resolution could not replay it")
	}

	prov.refundThenErr = nil
	if _, _, err := svc.CreateRefund(ctx, "rk-att", res.Payment.ID, 7, 500, ""); err != nil {
		t.Fatalf("retry err = %v", err)
	}
	if still, _ := rec.ListOpenForPayment(ctx, res.Payment.ID); len(still) != 0 {
		t.Fatalf("open attempts after settling = %d, want 0", len(still))
	}
}

// TestCapture_RateLimitReversesAndIsNotParked pins the line between the two kinds
// of failure. A 429 is the provider refusing the request and doing nothing with
// it — decided — so the hold is intact and the row goes back to `authorized`. Only
// an answer that never came parks. A classifier that folds both into one is how a
// lost capture response ends up reversed.
func TestCapture_RateLimitReversesAndIsNotParked(t *testing.T) {
	svc, _, prov, rec := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-429", intent(2000))
	prov.captureErr = provider.ErrTransient
	if _, err := svc.Capture(ctx, res.Payment.ID, 7); !errors.Is(err, provider.ErrTransient) {
		t.Fatalf("err = %v, want the provider's transient error", err)
	}
	got, _ := svc.Get(ctx, res.Payment.ID, 7)
	if got.Status != domain.StatusAuthorized {
		t.Fatalf("status = %s, want authorized — a refused request leaves the hold intact", got.Status)
	}
	for _, a := range rec.got {
		if a.Operation == domain.AttemptCapture && a.Outcome != domain.OutcomeRetryableFailure {
			t.Fatalf("capture attempt class = %s, want RETRYABLE_FAILURE", a.Outcome)
		}
	}
}

// A decided charge failure that is not a card decline — a malformed request, an
// amount the provider refuses — still ends the intent. Left `pending` it told the
// saga to retry a request that can never succeed, forever, while the attempt log
// recorded a decline the payment row contradicted.
func TestCreateIntent_DefiniteNonDeclineFailsTheIntent(t *testing.T) {
	svc, _, prov, _ := newDoubtService()
	ctx := context.Background()

	prov.chargeErr = fmt.Errorf("%w: malformed request", provider.ErrDefinite)
	res, err := svc.CreateIntent(ctx, "k-definite", intent(2000))
	if err != nil {
		t.Fatalf("err = %v, want the decided refusal answered, not raised", err)
	}
	if res.Code != 422 || res.Payment.Status != domain.StatusFailed {
		t.Fatalf("code=%d status=%s, want 422/failed", res.Code, res.Payment.Status)
	}
	if res.Payment.DeclineCode == "" {
		t.Fatal("no decline code recorded: the row must say why it failed")
	}
}

// A payment parked with no attempt row cannot be resolved by guessing. The only
// way into that state is a lost attempt write, so it must read as doubt — never as
// an outcome, and never as a rejection the saga would compensate on.
func TestResolve_ParkedWithoutEvidenceStaysDoubt(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(), captureThenErr: context.DeadlineExceeded}
	// The log drops writes, so the park lands with no evidence behind it.
	rec := &recordingAttempts{err: errors.New("attempts table gone")}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(rec))
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-noev", intent(2000))
	if _, err := svc.Capture(ctx, res.Payment.ID, 7); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("err = %v, want ErrOutcomeUnknown", err)
	}
	rec.err = nil // the log recovers, but the row that would have been written is gone

	prov.captureThenErr = nil
	_, err := svc.Capture(ctx, res.Payment.ID, 7)
	if !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("err = %v, want ErrOutcomeUnknown — unresolvable doubt is still doubt", err)
	}
	if errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatal("reported as a precondition failure: callers read that as permanent and compensate")
	}
}

// An unwired attempt log can park an intent but must never pretend it can resolve
// one: "no open attempts" and "I cannot tell you" have to look different, or a
// misconfigured service would silently answer doubt with a verdict.
func TestResolve_UnwiredLogRefusesRatherThanGuesses(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(), captureThenErr: context.DeadlineExceeded}
	svc := NewService(fp, fi, prov, 168*time.Hour) // no WithAttempts
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-unwired", intent(2000))
	if _, err := svc.Capture(ctx, res.Payment.ID, 7); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("err = %v, want the park to still happen", err)
	}
	prov.captureThenErr = nil
	if _, err := svc.Capture(ctx, res.Payment.ID, 7); err == nil {
		t.Fatal("resolution succeeded without an attempt log: it had no key to ask under")
	}
}
