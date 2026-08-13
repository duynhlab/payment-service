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
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("capture err = %v, want ErrOutcomeUnknown", err)
	}
	parked, _ := svc.Get(ctx, res.Payment.ID, "7")
	if parked.Status != domain.StatusProcessing {
		t.Fatalf("status = %s, want processing", parked.Status)
	}
	if fp.reversals != 0 {
		t.Fatalf("reversals = %d, want 0 — an unknown outcome must never reverse", fp.reversals)
	}

	prov.captureThenErr = nil
	got, err := svc.Capture(ctx, res.Payment.ID, "7")
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
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("capture err = %v, want ErrOutcomeUnknown", err)
	}

	got, err := svc.Capture(ctx, res.Payment.ID, "7")
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
	if _, err := svc.Void(ctx, res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("void err = %v, want ErrOutcomeUnknown", err)
	}
	parked, _ := svc.Get(ctx, res.Payment.ID, "7")
	if parked.Status != domain.StatusProcessing {
		t.Fatalf("status = %s, want processing (never authorized — the hold may be gone)", parked.Status)
	}

	prov.voidThenErr = nil
	got, err := svc.Void(ctx, res.Payment.ID, "7")
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
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}

	prov.refundThenErr = context.DeadlineExceeded
	if _, _, err := svc.CreateRefund(ctx, "rk-park", res.Payment.ID, "7", 500, ""); !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("first refund err = %v, want ErrRefundNotSettled", err)
	}

	prov.refundThenErr = nil
	ref, _, err := svc.CreateRefund(ctx, "rk-park", res.Payment.ID, "7", 500, "")
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
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	prov.refundThenErr = context.DeadlineExceeded
	_, _, _ = svc.CreateRefund(ctx, "rk-att", res.Payment.ID, "7", 500, "")

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
	if _, _, err := svc.CreateRefund(ctx, "rk-att", res.Payment.ID, "7", 500, ""); err != nil {
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
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); !errors.Is(err, provider.ErrTransient) {
		t.Fatalf("err = %v, want the provider's transient error", err)
	}
	got, _ := svc.Get(ctx, res.Payment.ID, "7")
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

// TestResolve_UnwiredLogCannotPark: a service without an attempt log can still
// answer honestly, but it must not create doubt it has no way to settle.
// "No open attempts" and "I cannot tell you whether there are any" have to look
// different, or a misconfigured service answers doubt with a verdict.
func TestResolve_UnwiredLogCannotPark(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub()}
	svc := NewService(fp, fi, prov, 168*time.Hour) // no WithAttempts
	ctx := context.Background()

	res, err := svc.CreateIntent(ctx, "k-unwired", intent(2000))
	if err != nil {
		t.Fatal(err)
	}
	prov.captureThenErr = context.DeadlineExceeded
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("err = %v, want the unknown outcome still reported", err)
	}
	got, _ := svc.Get(ctx, res.Payment.ID, "7")
	if got.Status == domain.StatusProcessing {
		t.Fatal("parked without a log: the doubt could never be resolved")
	}
}

// TestResolve_RepeatedSilenceDoesNotMultiplyTheWork is the storm guard. Each
// resolution pass records the round-trip it just made, and if that row were left
// open ALONGSIDE the one it re-asked, the next pass would find two questions, ask
// the provider twice, and open two more. Work per retry doubles — during a
// provider outage, which is the exact condition this feature exists for, so the
// feature would turn an outage into a self-inflicted flood against the provider.
func TestResolve_RepeatedSilenceDoesNotMultiplyTheWork(t *testing.T) {
	svc, fp, prov, rec := newDoubtService()
	ctx := context.Background()
	const order = int64(77)

	prov.chargeThenErr = context.DeadlineExceeded
	if _, err := svc.CreateIntent(ctx, "k-storm-0", intentFor(order, 2000)); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("park err = %v, want ErrOutcomeUnknown", err)
	}
	pay, err := fp.FindByOrderID(ctx, order)
	if err != nil {
		t.Fatal(err)
	}

	const passes = 5
	for i := 1; i <= passes; i++ {
		if _, err := svc.CreateIntent(ctx, fmt.Sprintf("k-storm-%d", i), intentFor(order, 2000)); !errors.Is(err, domain.ErrOutcomeUnknown) {
			t.Fatalf("pass %d err = %v, want ErrOutcomeUnknown", i, err)
		}
	}
	// One provider call per pass, plus the original. Doubling would be 63.
	if prov.chargeCalls != passes+1 {
		t.Fatalf("provider charge calls = %d, want %d — one question asked once per pass", prov.chargeCalls, passes+1)
	}
	open, _ := rec.ListOpenForPayment(ctx, pay.ID)
	if len(open) != 1 {
		t.Fatalf("open attempts after %d unresolved passes = %d, want exactly 1 — the doubt is one question, not many", passes, len(open))
	}
}

// A resolution that loses its CAS to another resolver must answer like a no-op,
// never like a rejection. `ErrStaleTransition` maps to FailedPrecondition, which
// the saga treats as permanent: a race between two resolvers would make the caller
// compensate a payment that had just been settled correctly.
func TestResolve_LostRaceIsNotReportedAsARejection(t *testing.T) {
	svc, fp, prov, _ := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-race", intent(2000))
	prov.captureThenErr = context.DeadlineExceeded
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("park err = %v", err)
	}
	prov.captureThenErr = nil

	// Somebody else settles the row between the resolution's read and its write.
	fp.beforeTransition = func() {
		fp.beforeTransition = nil
		_ = fp.TransitionStatus(ctx, res.Payment.ID, domain.StatusProcessing, domain.StatusCaptured, nil)
	}

	got, err := svc.Capture(ctx, res.Payment.ID, "7")
	if err != nil {
		t.Fatalf("err = %v, want the lost race treated as a no-op", err)
	}
	if errors.Is(err, domain.ErrStaleTransition) || errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("err = %v, want no rejection: the saga compensates on those", err)
	}
	if got.Status != domain.StatusCaptured {
		t.Fatalf("status = %s, want captured", got.Status)
	}
}

// TestResolve_VerdictNotWrittenKeepsTheQuestionOpen guards the crash window
// between learning the provider's answer and writing it down.
//
// Closing the attempt at the moment the answer arrives looks tidy and is wrong: if
// the write then fails, the payment stays parked with nothing on the worklist to
// explain it — unresolvable, the one outcome worse than doubt. The question stays
// open until the verdict lands, so a redo simply asks again under the same key and
// reaches the same conclusion.
func TestResolve_VerdictNotWrittenKeepsTheQuestionOpen(t *testing.T) {
	svc, fp, prov, rec := newDoubtService()
	ctx := context.Background()

	res, err := svc.CreateIntent(ctx, "k-crashwindow", intent(2000))
	if err != nil {
		t.Fatal(err)
	}
	prov.captureThenErr = context.DeadlineExceeded
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("park err = %v", err)
	}

	// The provider now answers, but the answer cannot be persisted.
	prov.captureThenErr = nil
	fp.transitionErr = errors.New("database went away")
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err == nil {
		t.Fatal("resolution reported success while its verdict was not written")
	}

	open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID)
	if len(open) == 0 {
		t.Fatal("the question was closed before the verdict landed: the payment is now parked with nothing to resolve it")
	}

	// And once the write works, the redo settles it.
	fp.transitionErr = nil
	got, err := svc.Capture(ctx, res.Payment.ID, "7")
	if err != nil {
		t.Fatalf("redo err = %v", err)
	}
	if got.Status != domain.StatusCaptured {
		t.Fatalf("status = %s, want captured", got.Status)
	}
	if still, _ := rec.ListOpenForPayment(ctx, res.Payment.ID); len(still) != 0 {
		t.Fatalf("open attempts = %d, want 0", len(still))
	}
}

// A refund whose outcome is unknown must answer the same question every other
// operation answers. The saga branches on `errors.Is(err, ErrOutcomeUnknown)` to
// tell doubt from a decided no, and a refund is not exempt from that just because
// it carries an error of its own.
func TestRefund_UnknownOutcomeIsRecognisableAsDoubt(t *testing.T) {
	svc, _, prov, _ := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-ref-doubt", intent(2000))
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	prov.refundThenErr = context.DeadlineExceeded

	_, _, err := svc.CreateRefund(ctx, "rk-doubt", res.Payment.ID, "7", 500, "")
	if !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("err = %v, want it to satisfy errors.Is(ErrOutcomeUnknown)", err)
	}
	if !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("err = %v, want the refund's own sentinel kept as well", err)
	}
}

// The sweep exists for doubt nobody retries: an abandoned checkout, a saga that
// gave up, a refund the customer is not watching. Those are exactly the ones that
// strand money, because nothing else is going to ask.
func TestSweep_SettlesDoubtNobodyIsRetrying(t *testing.T) {
	svc, _, prov, rec := newDoubtService()
	ctx := context.Background()

	res, err := svc.CreateIntent(ctx, "k-sweep", intent(2000))
	if err != nil {
		t.Fatal(err)
	}
	prov.captureThenErr = context.DeadlineExceeded
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("park err = %v", err)
	}
	prov.captureThenErr = nil

	closed, err := svc.ResolveOpenDoubt(ctx, 10)
	if err != nil {
		t.Fatalf("sweep err = %v", err)
	}
	if closed != 1 {
		t.Fatalf("closed = %d, want 1", closed)
	}
	got, _ := svc.Get(ctx, res.Payment.ID, "7")
	if got.Status != domain.StatusCaptured {
		t.Fatalf("status = %s, want captured", got.Status)
	}
	if open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID); len(open) != 0 {
		t.Fatalf("open attempts = %d, want 0", len(open))
	}
}

// A parked refund is swept under the key the refund row already holds — not a
// fresh one. A fresh key is a second payout, which is the failure this whole
// mechanism is built to avoid.
func TestSweep_ReplaysAParkedRefundUnderItsOriginalKey(t *testing.T) {
	svc, _, prov, _ := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-sweep-ref", intent(2000))
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	prov.refundThenErr = context.DeadlineExceeded
	if _, _, err := svc.CreateRefund(ctx, "rk-sweep", res.Payment.ID, "7", 500, ""); !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("park err = %v", err)
	}
	prov.refundThenErr = nil

	if _, err := svc.ResolveOpenDoubt(ctx, 10); err != nil {
		t.Fatalf("sweep err = %v", err)
	}
	unique := map[string]bool{}
	for _, k := range prov.refundKeys {
		unique[k] = true
	}
	if len(unique) != 1 {
		t.Fatalf("provider refund keys = %v, want exactly one — a second key is a second payout", prov.refundKeys)
	}
}

// An open question about a payment that is no longer parked is still a question.
// Discarding it deletes the only work item pointing at an unverified provider
// call, while the capture ledger keeps asserting revenue nobody confirmed.
func TestSweep_VerifiesAStrayAttemptInsteadOfDiscardingIt(t *testing.T) {
	svc, fp, prov, rec := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-stray", intent(2000))
	prov.captureThenErr = context.DeadlineExceeded
	// The park CAS loses, so the row stays `captured` while the question stays open.
	fp.transitionErr = domain.ErrStaleTransition
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("err = %v, want ErrOutcomeUnknown", err)
	}
	fp.transitionErr = nil
	if open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID); len(open) != 1 {
		t.Fatalf("open attempts = %d, want the question recorded", len(open))
	}

	prov.captureThenErr = nil
	before := prov.gotCaptureKey
	if _, err := svc.ResolveOpenDoubt(ctx, 10); err != nil {
		t.Fatalf("sweep err = %v", err)
	}
	if prov.gotCaptureKey == "" || before == "" {
		t.Fatal("the sweep closed the question without asking the provider anything")
	}
	if open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID); len(open) != 0 {
		t.Fatalf("open attempts = %d, want 0 once the answer is known", len(open))
	}
}

// Doubt that survives the sweep stays on the worklist. The sweep never settles a
// question by declaring an outcome — that is the entire rule — so an unresolvable
// entry escalates through the backlog gauge, not by being quietly closed.
func TestSweep_LeavesUnresolvedDoubtOnTheWorklist(t *testing.T) {
	svc, _, prov, rec := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-sweep-silent", intent(2000))
	prov.captureThenErr = context.DeadlineExceeded
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("park err = %v", err)
	}
	// The provider stays silent through the sweep as well.
	closed, err := svc.ResolveOpenDoubt(ctx, 10)
	if err != nil {
		t.Fatalf("sweep err = %v", err)
	}
	if closed != 0 {
		t.Fatalf("closed = %d, want 0 — nothing was learned", closed)
	}
	got, _ := svc.Get(ctx, res.Payment.ID, "7")
	if got.Status != domain.StatusProcessing {
		t.Fatalf("status = %s, want processing", got.Status)
	}
	open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID)
	if len(open) != 1 {
		t.Fatalf("open attempts = %d, want exactly 1 — still one question, still open", len(open))
	}
}
