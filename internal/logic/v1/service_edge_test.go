package v1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duynhlab/pkg/idempotency"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

// failingProvider wraps the stub to force specific provider outcomes.
type failingProvider struct {
	*provider.Stub
	refundErr  error
	captureErr error
	voidErr    error
	// gotCaptureKey/gotVoidKey record the provider idempotency key the service
	// passed, so a test can prove a retry reuses it.
	gotCaptureKey string
	gotVoidKey    string
	chargeErr     error
	// The *ThenErr fields model the case the whole phase exists for: the provider
	// PERFORMED the work and the answer was lost. The real effect lands on the
	// Stub, then the call reports the error — so a resolution that asks again finds
	// the work already done, and a resolution that charges again is visible as a
	// second charge.
	chargeThenErr  error
	captureThenErr error
	voidThenErr    error
	refundThenErr  error
	// refundKeys records every key a refund round-trip was sent under. A SECOND
	// distinct key for one refund is a second payout, so the set — not the count —
	// is what a test must assert on.
	refundKeys []string
	// chargeCalls counts provider charge round-trips, so a test can prove that a
	// retry costs one call and not a doubling.
	chargeCalls int
	// captureErrs answers successive capture calls in order, then falls through to
	// the Stub. Resolution and the operation that triggered it are two separate
	// round-trips, so a test that cares about the difference needs to give them
	// different answers.
	captureErrs []error
}

// nextCaptureErr pops the scripted answer for this round-trip, if any.
func (f *failingProvider) nextCaptureErr() (error, bool) {
	if len(f.captureErrs) == 0 {
		return nil, false
	}
	err := f.captureErrs[0]
	f.captureErrs = f.captureErrs[1:]
	return err, true
}

func (f *failingProvider) Refund(ctx context.Context, id string, amt int64, key string) (string, error) {
	f.refundKeys = append(f.refundKeys, key)
	if f.refundErr != nil {
		return "", f.refundErr
	}
	if f.refundThenErr != nil {
		if _, err := f.Stub.Refund(ctx, id, amt, key); err != nil {
			return "", err
		}
		return "", f.refundThenErr
	}
	return f.Stub.Refund(ctx, id, amt, key)
}

func (f *failingProvider) Charge(ctx context.Context, req provider.ChargeRequest) (*provider.Charge, error) {
	f.chargeCalls++
	if f.chargeErr != nil {
		return nil, f.chargeErr
	}
	if f.chargeThenErr != nil {
		if _, err := f.Stub.Charge(ctx, req); err != nil {
			return nil, err
		}
		return nil, f.chargeThenErr
	}
	return f.Stub.Charge(ctx, req)
}

func (f *failingProvider) Capture(ctx context.Context, id, idemKey string) error {
	f.gotCaptureKey = idemKey
	if err, scripted := f.nextCaptureErr(); scripted {
		return err
	}
	if f.captureErr != nil {
		return f.captureErr
	}
	if f.captureThenErr != nil {
		if err := f.Stub.Capture(ctx, id, idemKey); err != nil {
			return err
		}
		return f.captureThenErr
	}
	return f.Stub.Capture(ctx, id, idemKey)
}

func (f *failingProvider) Void(ctx context.Context, id, idemKey string) error {
	f.gotVoidKey = idemKey
	if f.voidErr != nil {
		return f.voidErr
	}
	if f.voidThenErr != nil {
		if err := f.Stub.Void(ctx, id, idemKey); err != nil {
			return err
		}
		return f.voidThenErr
	}
	return f.Stub.Void(ctx, id, idemKey)
}

// TestCreateRefund_ProviderUnknownNeverSealsTheKey is the regression test for
// the money bug this slice fixes: a refund whose provider outcome is UNKNOWN
// (timeout, transport error, unclassifiable status) used to be persisted as
// `failed` AND sealed into the idempotency cache as a 201. Every later retry
// then replayed that verdict without ever calling the provider again, so the
// customer's money never moved and no key existed that could re-drive it.
//
// The refund must instead stay `pending` (we do not know whether money moved,
// so the reserve stays held — refunding again on top would risk paying twice),
// the caller must see an error, and the SAME key must re-drive the provider.
func TestCreateRefund_ProviderUnknownNeverSealsTheKey(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(), refundErr: errors.New("mockpay down")}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-unk", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}

	_, _, err := svc.CreateRefund(context.Background(), "rk-unk", res.Payment.ID, "7", 500, "")
	if !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("unknown provider outcome err = %v, want ErrRefundNotSettled", err)
	}
	if len(fp.refs) != 1 {
		t.Fatalf("refund count = %d, want exactly one", len(fp.refs))
	}
	for _, r := range fp.refs {
		// `processing`, not `pending`: both hold the reserve, but only one says the
		// provider was already asked — which is what a resolver needs to know.
		if r.Status != domain.RefundProcessing {
			t.Fatalf("refund status = %s, want processing (asked, fate unknown)", r.Status)
		}
	}

	// The same key must re-drive the provider rather than replay a sealed
	// verdict. With the provider recovered, the retry settles for real.
	prov.refundErr = nil
	ref, replayed, err := svc.CreateRefund(context.Background(), "rk-unk", res.Payment.ID, "7", 500, "")
	if err != nil {
		t.Fatalf("retry after recovery: %v", err)
	}
	if replayed {
		t.Fatal("retry replayed a cached response — the failed attempt was sealed")
	}
	if ref.Status != domain.RefundSucceeded {
		t.Fatalf("retry status = %s, want succeeded", ref.Status)
	}
}

// TestCreateRefund_ProviderDeclineIsDefinite separates a DEFINITE answer from
// an unknown one: the provider said no, so the refund is `failed` and its
// reserved amount is released (no money moved, so a different refund may still
// be attempted). The caller still gets an error — a refund that did not happen
// is never a 201.
func TestCreateRefund_ProviderDeclineIsDefinite(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(),
		refundErr: &provider.DeclinedError{Code: "refund_declined"}}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-dec", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}

	_, _, err := svc.CreateRefund(context.Background(), "rk-dec", res.Payment.ID, "7", 500, "")
	if !errors.Is(err, domain.ErrRefundDeclined) {
		t.Fatalf("declined refund err = %v, want ErrRefundDeclined", err)
	}
	if len(fp.refs) != 1 {
		t.Fatalf("refund count = %d, want exactly one", len(fp.refs))
	}
	for _, r := range fp.refs {
		if r.Status != domain.RefundFailed {
			t.Fatalf("refund status = %s, want failed (definite)", r.Status)
		}
	}

	// A definite decline released the reserve, so the full amount is refundable.
	prov.refundErr = nil
	if _, _, err := svc.CreateRefund(context.Background(), "rk-dec2", res.Payment.ID, "7", 2000, ""); err != nil {
		t.Fatalf("full refund after a definite decline must pass, got %v", err)
	}
}

// TestCreateRefund_RetryAfterDeclineIsNotACachedSuccess closes the second door
// into the same bug: the first attempt already persisted `failed`, so a same-key
// retry adopts that row and skips settlement entirely. Without the seal guard it
// would answer 201 with a failed refund body — the exact lie the fresh-failure
// path was just taught not to tell.
func TestCreateRefund_RetryAfterDeclineIsNotACachedSuccess(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(),
		refundErr: &provider.DeclinedError{Code: "refund_declined"}}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-adopt", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.CreateRefund(context.Background(), "rk-adopt", res.Payment.ID, "7", 500, ""); !errors.Is(err, domain.ErrRefundDeclined) {
		t.Fatalf("first attempt err = %v, want ErrRefundDeclined", err)
	}

	// Same key, provider now healthy: the adopted row is still `failed`, so the
	// answer must stay an error rather than become a success — and it must keep
	// the DECLINE's certainty, so the saga stops instead of retrying a decision
	// the provider already made.
	prov.refundErr = nil
	ref, replayed, err := svc.CreateRefund(context.Background(), "rk-adopt", res.Payment.ID, "7", 500, "")
	if !errors.Is(err, domain.ErrRefundDeclined) {
		t.Fatalf("retry of a declined refund err = %v (ref %+v, replayed %v), want ErrRefundDeclined",
			err, ref, replayed)
	}
}

// TestCreateRefund_UnknownReleasesKeyOnADeadContext pins the reason the release
// is detached from the request context. The canonical unknown outcome IS a
// timeout, so by the time we unlock the key the request context is already dead.
// Releasing on it would fail exactly when it matters, leaving the key locked
// while the response tells the caller to retry immediately — the retry would
// bounce off ErrLocked until the takeover window (90s here, minutes in prod).
func TestCreateRefund_UnknownReleasesKeyOnADeadContext(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(), refundErr: context.DeadlineExceeded}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-dead", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}

	// A context that is already cancelled when the refund attempt gives up.
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := svc.CreateRefund(dead, "rk-dead", res.Payment.ID, "7", 500, ""); !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("err = %v, want ErrRefundNotSettled", err)
	}

	// The key must be usable again on a live context — not locked out.
	prov.refundErr = nil
	if _, _, err := svc.CreateRefund(context.Background(), "rk-dead", res.Payment.ID, "7", 500, ""); err != nil {
		t.Fatalf("same-key retry after a dead-context failure: %v (key left locked?)", err)
	}
}

// TestCreateRefund_DefiniteFailureIsNotRetried covers the failure the deployed
// provider actually produces: an unknown charge answers 404, which is decided.
// It must be recorded `failed` and stopped like a decline — filing it as unknown
// would retry something that can never succeed and hold the reserve open for a
// refund that will never happen.
func TestCreateRefund_DefiniteFailureIsNotRetried(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(),
		refundErr: fmt.Errorf("%w: mockpay refund: status 404", provider.ErrDefinite)}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-def", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.CreateRefund(context.Background(), "rk-def", res.Payment.ID, "7", 500, ""); !errors.Is(err, domain.ErrRefundDeclined) {
		t.Fatalf("definite failure err = %v, want ErrRefundDeclined (not retryable)", err)
	}
	for _, r := range fp.refs {
		if r.Status != domain.RefundFailed {
			t.Fatalf("refund status = %s, want failed (decided)", r.Status)
		}
	}
}

// TestCreateRefund_SettleFailureAfterMoneyMovedStaysOpen: the provider paid, but
// persisting that fact failed. From the outside the refund is unsettled, so the
// answer must be the retryable sentinel — a bare error would be a non-retryable
// 500 for a refund whose money already left.
func TestCreateRefund_SettleFailureAfterMoneyMovedStaysOpen(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	svc := NewService(fp, fi, provider.NewStub(), 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-settle", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	fp.settleRefundErr = errors.New("db gone")
	if _, _, err := svc.CreateRefund(context.Background(), "rk-settle", res.Payment.ID, "7", 500, ""); !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("settle failure err = %v, want ErrRefundNotSettled", err)
	}
}

// TestCreateRefund_ReleaseFailureIsSurfacedNotSwallowed: a key that cannot be
// unlocked leaves the caller locked out until the takeover window, so both facts
// have to reach the caller. The refund's own error stays the one errors.Is
// matches — that is what decides whether a retry is safe — and the release
// failure rides along as context, because "retry with the same key" is bad advice
// if that key is still locked.
func TestCreateRefund_ReleaseFailureIsSurfacedNotSwallowed(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	fi.releaseErr = errors.New("db gone")
	prov := &failingProvider{Stub: provider.NewStub(), refundErr: errors.New("mockpay down")}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-rel", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	_, _, err := svc.CreateRefund(context.Background(), "rk-rel", res.Payment.ID, "7", 500, "")
	if !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("err = %v, want the refund's own ErrRefundNotSettled", err)
	}
	if !errors.Is(err, fi.releaseErr) {
		t.Fatalf("err = %v, want the lockout surfaced alongside the outcome", err)
	}
}

// TestCreateRefund_DeclineRecordFailureReopensTheOutcome: the provider declined,
// but persisting that verdict failed — so we do not actually know the refund is
// dead. Reporting the decline would park an order on a verdict no row holds; the
// answer must be the retryable sentinel instead.
func TestCreateRefund_DeclineRecordFailureReopensTheOutcome(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(),
		refundErr: &provider.DeclinedError{Code: "refund_declined"}}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-drf", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	fp.settleRefundErr = errors.New("db gone")
	_, _, err := svc.CreateRefund(context.Background(), "rk-drf", res.Payment.ID, "7", 500, "")
	if !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("err = %v, want ErrRefundNotSettled (the decline was not recorded)", err)
	}
	if errors.Is(err, domain.ErrRefundDeclined) {
		t.Fatal("an unrecorded decline must not be reported as a decline")
	}
}

// TestCreateRefund_AdoptedRowInAnUnexpectedStateIsNotSealed guards the defensive
// arm: whatever state an adopted refund is in, only `succeeded` may be sealed.
func TestCreateRefund_AdoptedRowInAnUnexpectedStateIsNotSealed(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	svc := NewService(fp, fi, provider.NewStub(), 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-weird", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.CreateRefund(context.Background(), "rk-weird", res.Payment.ID, "7", 500, ""); err != nil {
		t.Fatal(err)
	}
	// Force a state the FSM does not produce, then replay the key past its seal.
	for _, r := range fp.refs {
		r.Status = domain.RefundStatus("in_review")
	}
	for _, k := range fi.keys {
		if k.Key == "rk-weird" {
			k.ResponseCode, k.ResponseBody = nil, nil
			k.LockedAt = time.Unix(0, 0)
		}
	}
	if _, _, err := svc.CreateRefund(context.Background(), "rk-weird", res.Payment.ID, "7", 500, ""); !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("err = %v, want ErrRefundNotSettled for an unexpected status", err)
	}
}

// TestCaptureVoid_CarryADeterministicProviderKey: the key must be the same on
// every attempt of the same logical operation, or the provider has nothing to
// replay and a post-doubt retry gets a fresh verdict.
func TestCaptureVoid_CarryADeterministicProviderKey(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub()}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-key", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("capture:payment:%d", res.Payment.ID)
	if prov.gotCaptureKey != want {
		t.Fatalf("capture key = %q, want %q", prov.gotCaptureKey, want)
	}

	res2, _ := svc.CreateIntent(context.Background(), "k-key2", intent(3000))
	if _, err := svc.Void(context.Background(), res2.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("void:payment:%d", res2.Payment.ID); prov.gotVoidKey != want {
		t.Fatalf("void key = %q, want %q", prov.gotVoidKey, want)
	}
}

// TestCreateRefund_RejectedByState covers every non-refundable state: a refund
// is only valid against a captured payment, so authorized/voided/expired/failed
// payments must all be rejected.
func TestCreateRefund_RejectedByState(t *testing.T) {
	// setup drives a fresh payment into the named state and returns its id.
	setups := map[string]func(t *testing.T, svc *Service) int64{
		"authorized (never captured)": func(t *testing.T, svc *Service) int64 {
			res, _ := svc.CreateIntent(context.Background(), "k-auth", intent(2000))
			return res.Payment.ID
		},
		"voided": func(t *testing.T, svc *Service) int64 {
			res, _ := svc.CreateIntent(context.Background(), "k-void", intent(2000))
			if _, err := svc.Void(context.Background(), res.Payment.ID, "7"); err != nil {
				t.Fatal(err)
			}
			return res.Payment.ID
		},
		"failed (declined)": func(t *testing.T, svc *Service) int64 {
			res, _ := svc.CreateIntent(context.Background(), "k-fail", intent(2002)) // ...02 => decline
			return res.Payment.ID
		},
	}
	for name, setup := range setups {
		t.Run(name, func(t *testing.T) {
			svc, _, _, _ := newTestService()
			id := setup(t, svc)
			_, _, err := svc.CreateRefund(context.Background(), "rk-"+name, id, "7", 500, "")
			if !errors.Is(err, domain.ErrRefundRejected) {
				t.Fatalf("refund on %s must be rejected, got %v", name, err)
			}
		})
	}
}

// When the provider call fails, the CAS-first ordering means the row was
// already moved; the operation must roll it back to authorized so the row never
// disagrees with the (unchanged) provider state. Assert the rollback, not just
// the error.
// A DECIDED capture failure still rolls back: the provider said no, so nothing
// was captured and the reversal is correct. Contrast with the UNKNOWN twin below.
func TestCapture_DecidedFailureRollsBackToAuthorized(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(),
		captureErr: fmt.Errorf("%w: capture down", provider.ErrDefinite)}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-cap", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err == nil || !strings.Contains(err.Error(), "capture down") {
		t.Fatalf("capture provider error must propagate, got %v", err)
	}
	got, err := svc.Get(context.Background(), res.Payment.ID, "7")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusAuthorized {
		t.Fatalf("after failed capture, status = %s, want authorized (rolled back)", got.Status)
	}
}

// Same split for void: a decided refusal rolls back, an unknown one parks.
func TestVoid_DecidedFailureRollsBackToAuthorized(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(),
		voidErr: fmt.Errorf("%w: void down", provider.ErrDefinite)}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-void", intent(2000))
	if _, err := svc.Void(context.Background(), res.Payment.ID, "7"); err == nil || !strings.Contains(err.Error(), "void down") {
		t.Fatalf("void provider error must propagate, got %v", err)
	}
	got, err := svc.Get(context.Background(), res.Payment.ID, "7")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusAuthorized {
		t.Fatalf("after failed void, status = %s, want authorized (rolled back)", got.Status)
	}
}

// TestCapture_UnknownOutcomeParksInsteadOfReversing is BUG-2's regression test.
// A lost capture answer used to trigger ReverseCapture — the semantic opposite of
// the operation whose fate was unknown. If the provider HAD captured, that took
// the money out of our books while the provider kept it: revenue collected and
// then disowned, internally balanced so Imbalance() cannot see it.
func TestCapture_UnknownOutcomeParksInsteadOfReversing(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(), captureErr: context.DeadlineExceeded}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-cu", intent(2000))
	_, err := svc.Capture(context.Background(), res.Payment.ID, "7")
	if !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("capture err = %v, want ErrOutcomeUnknown", err)
	}
	got, gerr := svc.Get(context.Background(), res.Payment.ID, "7")
	if gerr != nil {
		t.Fatal(gerr)
	}
	if got.Status != domain.StatusProcessing {
		t.Fatalf("status = %s, want processing (parked, not reversed)", got.Status)
	}
	// The capture ledger entry must still stand: no reversal was posted.
	if fp.reversals != 0 {
		t.Errorf("reversals = %d, want 0 — reversing is the opposite of the unknown operation", fp.reversals)
	}
}

// TestVoid_UnknownOutcomeParksInsteadOfRollingBack is the mirror image: rolling
// back to `authorized` would assert a live hold. If the void DID land we would
// believe we can capture money the provider has already released.
func TestVoid_UnknownOutcomeParksInsteadOfRollingBack(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(), voidErr: context.DeadlineExceeded}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	res, _ := svc.CreateIntent(context.Background(), "k-vu", intent(2000))
	if _, err := svc.Void(context.Background(), res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("void err = %v, want ErrOutcomeUnknown", err)
	}
	got, _ := svc.Get(context.Background(), res.Payment.ID, "7")
	if got.Status != domain.StatusProcessing {
		t.Fatalf("status = %s, want processing", got.Status)
	}
}

// TestAuthorize_UnknownOutcomeIsParkedNotLeftInvisible is BUG-3's regression
// test. A lost authorize answer left the row `pending` with no provider
// reference — and reconciliation selects on that reference, so the customer's
// hold was invisible to the one mechanism that could find it.
func TestAuthorize_UnknownOutcomeIsParkedNotLeftInvisible(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(), chargeErr: context.DeadlineExceeded}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(&recordingAttempts{}))

	_, err := svc.CreateIntent(context.Background(), "k-au", intent(2000))
	if !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("CreateIntent err = %v, want ErrOutcomeUnknown", err)
	}
	if len(fp.items) != 1 {
		t.Fatalf("want exactly one payment row, got %d", len(fp.items))
	}
	for _, p := range fp.items {
		if p.Status != domain.StatusProcessing {
			t.Fatalf("status = %s, want processing (findable), not pending (invisible)", p.Status)
		}
	}
}

// TestAttemptLog_RecordsOneRowPerRoundTripWithItsClass: the log is the substrate
// that makes an unknown outcome resolvable, so what it records has to be right —
// one row per provider call, classified by whether the provider DECIDED.
func TestAttemptLog_RecordsOneRowPerRoundTripWithItsClass(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		class domain.OutcomeClass
	}{
		{"success", nil, domain.OutcomeSuccess},
		{"decline", &provider.DeclinedError{Code: "card_declined"}, domain.OutcomeBusinessDecline},
		{"decided technical refusal", fmt.Errorf("%w: unknown charge", provider.ErrDefinite), domain.OutcomeBusinessDecline},
		{"explicit ask-again", provider.ErrTransient, domain.OutcomeRetryableFailure},
		{"no answer", context.DeadlineExceeded, domain.OutcomeUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fp, fi := newFakePayments(), newFakeIdem()
			rec := &recordingAttempts{}
			prov := &failingProvider{Stub: provider.NewStub(), captureErr: tc.err}
			svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(rec))

			res, _ := svc.CreateIntent(context.Background(), "k-att-"+tc.name, intent(2000))
			_, _ = svc.Capture(context.Background(), res.Payment.ID, "7")

			var captures []domain.Attempt
			for _, a := range rec.got {
				if a.Operation == domain.AttemptCapture {
					captures = append(captures, a)
				}
			}
			if len(captures) != 1 {
				t.Fatalf("capture attempts = %d, want exactly one per round-trip", len(captures))
			}
			if captures[0].Outcome != tc.class {
				t.Errorf("outcome = %s, want %s", captures[0].Outcome, tc.class)
			}
			if captures[0].PaymentID != res.Payment.ID {
				t.Errorf("payment id = %d, want %d", captures[0].PaymentID, res.Payment.ID)
			}
		})
	}
}

// TestAttemptLog_WriteFailureRefusesToPark: no evidence, no park.
//
// A `processing` row that no attempt explains is a dead end — no retry and no
// sweep can work out which operation to re-ask or under which key — and only
// manual SQL gets a payment out of it. So when the log refuses the write, the
// pre-park state stands. That state is a claim the provider has not confirmed,
// which reconciliation is built to detect; a dead end is not detectable by
// anything.
//
// The answer stays an unknown outcome either way. The provider's silence is a
// fact about the money and does not become a verdict because a table was down.
func TestAttemptLog_WriteFailureRefusesToPark(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	rec := &recordingAttempts{}
	prov := &failingProvider{Stub: provider.NewStub()}
	svc := NewService(fp, fi, prov, 168*time.Hour, WithAttempts(rec))
	ctx := context.Background()

	res, err := svc.CreateIntent(ctx, "k-attfail", intent(2000))
	if err != nil {
		t.Fatal(err)
	}
	// The log breaks only now, so the capture's own park is the one that is refused.
	rec.err = errors.New("attempts table gone")
	prov.captureErr = context.DeadlineExceeded

	capErr := mustCaptureErr(t, svc, ctx, res.Payment.ID)
	if !errors.Is(capErr, domain.ErrOutcomeUnknown) {
		t.Fatalf("err = %v, want ErrOutcomeUnknown", capErr)
	}
	got, _ := svc.Get(ctx, res.Payment.ID, "7")
	if got.Status == domain.StatusProcessing {
		t.Fatal("parked with no attempt row: nothing could ever resolve this payment")
	}
	if got.Status != domain.StatusCaptured {
		t.Fatalf("status = %s, want the pre-park state to stand", got.Status)
	}
}

// mustCaptureErr captures and requires an error, keeping the test above focused
// on WHICH error rather than on the ceremony of demanding one.
func mustCaptureErr(t *testing.T, svc *Service, ctx context.Context, paymentID int64) error {
	t.Helper()
	_, err := svc.Capture(ctx, paymentID, "7")
	if err == nil {
		t.Fatal("capture succeeded, want the unknown outcome reported")
	}
	return err
}

// recordingAttempts is an in-memory attempt log. It mirrors the PRODUCTION
// guards, not a convenient subset of them: the one-SUCCESS-capture uniqueness
// index and the resolve preconditions are enforced here too. A fake that is more
// permissive than the database is how phase 6's refund trap passed its tests.
type recordingAttempts struct {
	got []domain.Attempt
	err error
}

func (r *recordingAttempts) Record(_ context.Context, a domain.Attempt) (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	if a.Operation == domain.AttemptCapture && a.Outcome == domain.OutcomeSuccess {
		for _, e := range r.got {
			if e.PaymentID == a.PaymentID && e.Operation == domain.AttemptCapture &&
				e.Outcome == domain.OutcomeSuccess {
				return 0, domain.ErrDuplicateAttempt
			}
		}
	}
	a.ID = int64(len(r.got) + 1)
	r.got = append(r.got, a)
	return a.ID, nil
}

func (r *recordingAttempts) ListOpenForPayment(_ context.Context, paymentID int64) ([]domain.Attempt, error) {
	if r.err != nil {
		return nil, r.err
	}
	var out []domain.Attempt
	for _, a := range r.got {
		if a.PaymentID == paymentID && a.Open() {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *recordingAttempts) ListOpen(_ context.Context, limit int) ([]domain.Attempt, error) {
	if r.err != nil {
		return nil, r.err
	}
	var out []domain.Attempt
	for _, a := range r.got {
		if !a.Open() {
			continue
		}
		out = append(out, a)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (r *recordingAttempts) Resolve(_ context.Context, attemptID int64, at time.Time) error {
	if r.err != nil {
		return r.err
	}
	for i := range r.got {
		if r.got[i].ID != attemptID {
			continue
		}
		// Same preconditions as the UPDATE: only open UNKNOWN rows close.
		if r.got[i].Outcome != domain.OutcomeUnknown || r.got[i].ResolvedAt != nil {
			return domain.ErrNotFound
		}
		r.got[i].ResolvedAt = &at
		return nil
	}
	return domain.ErrNotFound
}

func TestVoid_NotFound(t *testing.T) {
	svc, _, _, _ := newTestService()
	if _, err := svc.Void(context.Background(), 999, "7"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("voiding a missing payment must return ErrNotFound, got %v", err)
	}
}

func TestConcurrentCaptureAndVoid_OnlyOneWins(t *testing.T) {
	svc, _, _, _ := newTestService()
	res, _ := svc.CreateIntent(context.Background(), "k-race", intent(2000))
	id := res.Payment.ID

	var wg sync.WaitGroup
	var capErr, voidErr error
	wg.Add(2)
	go func() { defer wg.Done(); _, capErr = svc.Capture(context.Background(), id, "7") }()
	go func() { defer wg.Done(); _, voidErr = svc.Void(context.Background(), id, "7") }()
	wg.Wait()

	// Exactly one side wins; the loser sees an invalid-transition conflict.
	if (capErr == nil) == (voidErr == nil) {
		t.Fatalf("exactly one of capture/void must win: capErr=%v voidErr=%v", capErr, voidErr)
	}
	loser := capErr
	if loser == nil {
		loser = voidErr
	}
	if !errors.Is(loser, domain.ErrInvalidTransition) {
		t.Fatalf("loser must get ErrInvalidTransition, got %v", loser)
	}
}

func TestCapture_NotFound(t *testing.T) {
	svc, _, _, _ := newTestService()
	if _, err := svc.Capture(context.Background(), 999, "7"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("capturing a missing payment must return ErrNotFound, got %v", err)
	}
}

func TestGet_ForeignUserScoped(t *testing.T) {
	svc, _, _, _ := newTestService()
	res, _ := svc.CreateIntent(context.Background(), "k-owner", intent(2000))
	if _, err := svc.Get(context.Background(), res.Payment.ID, "8"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("a foreign user must not see the payment, got %v", err)
	}
}

func TestList_Pagination(t *testing.T) {
	svc, _, _, _ := newTestService()
	for i := 0; i < 3; i++ {
		if _, err := svc.CreateIntent(context.Background(), "k-list-"+strings.Repeat("x", i+1), intent(2000)); err != nil {
			t.Fatal(err)
		}
	}
	items, total, err := svc.List(context.Background(), "7", 1, 2)
	if err != nil || total != 3 || len(items) != 2 {
		t.Fatalf("page1: err=%v total=%d len=%d", err, total, len(items))
	}
	items, _, _ = svc.List(context.Background(), "7", 2, 2)
	if len(items) != 1 {
		t.Fatalf("page2 len=%d, want 1", len(items))
	}
	// Page beyond the last still reports the true total with an empty page.
	items, total, err = svc.List(context.Background(), "7", 99, 2)
	if err != nil || total != 3 || len(items) != 0 {
		t.Fatalf("page-beyond: err=%v total=%d len=%d, want total=3 len=0", err, total, len(items))
	}
}

func TestReapIdempotencyKeys_DelegatesTTLAndCount(t *testing.T) {
	svc, _, fi, _ := newTestService()
	fi.reapCount = 4

	n, err := svc.ReapIdempotencyKeys(context.Background(), 24*time.Hour)
	if err != nil {
		t.Fatalf("ReapIdempotencyKeys: %v", err)
	}
	if n != 4 {
		t.Fatalf("returned count = %d, want 4 (from the repo)", n)
	}
	if fi.reapTTL != 24*time.Hour {
		t.Fatalf("delegated TTL = %v, want 24h", fi.reapTTL)
	}
}

func TestReplayResult_CorruptCache(t *testing.T) {
	code := 201
	if _, err := replayResult(&idempotency.Record{ResponseCode: &code, ResponseBody: []byte("{not json")}); err == nil {
		t.Fatal("corrupt cache must error")
	}
}

func TestCreateRefund_CorruptCache(t *testing.T) {
	svc, _, fi, _ := newTestService()
	res, _ := svc.CreateIntent(context.Background(), "k-cc", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.CreateRefund(context.Background(), "rk-cc", res.Payment.ID, "7", 500, ""); err != nil {
		t.Fatal(err)
	}
	fi.mu.Lock()
	for _, k := range fi.keys {
		if k.Key == "rk-cc" {
			k.ResponseBody = []byte("{corrupt")
		}
	}
	fi.mu.Unlock()
	if _, _, err := svc.CreateRefund(context.Background(), "rk-cc", res.Payment.ID, "7", 500, ""); err == nil {
		t.Fatal("corrupt refund cache must error")
	}
}

// Crash-recovery for refunds: the first attempt settled the refund but the
// key was never finished (crash before Finish). A takeover retry must adopt the
// existing refund — not create a second one, not re-charge the provider.
func TestCreateRefund_TakeoverAdoptsSettledRefund(t *testing.T) {
	fp := newFakePayments()
	ei := &erroringIdem{fakeIdem: newFakeIdem()}
	prov := provider.NewStub()
	svc := NewService(fp, ei, prov, 168*time.Hour)

	res, _ := svc.CreateIntent(context.Background(), "k-cap", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	// First refund settles, but Finish fails → key left unfinished.
	ei.finishErr = errBoom
	if _, _, err := svc.CreateRefund(context.Background(), "rk", res.Payment.ID, "7", 500, "damaged"); !errors.Is(err, errBoom) {
		t.Fatalf("setup: want finish error, got %v", err)
	}
	fp.mu.Lock()
	refundsAfterFirst := len(fp.refs)
	fp.mu.Unlock()

	// Takeover: age the lock, clear the injected error, retry the same key.
	ei.finishErr = nil
	ei.mu.Lock()
	for _, k := range ei.keys {
		if k.Key == "rk" {
			k.LockedAt = time.Unix(0, 0)
		}
	}
	ei.mu.Unlock()

	ref, replayed, err := svc.CreateRefund(context.Background(), "rk", res.Payment.ID, "7", 500, "damaged")
	if err != nil {
		t.Fatalf("takeover retry: %v", err)
	}
	if replayed {
		t.Fatalf("takeover re-drives (not a cache replay); replayed=%v", replayed)
	}
	if ref.Status != domain.RefundSucceeded {
		t.Fatalf("adopted refund status = %s, want succeeded", ref.Status)
	}
	fp.mu.Lock()
	n := len(fp.refs)
	fp.mu.Unlock()
	if n != refundsAfterFirst {
		t.Fatalf("takeover must adopt, not insert: refunds %d -> %d", refundsAfterFirst, n)
	}
}

// A new idempotency key re-requesting an order whose payment already FAILED
// must replay the 422 by adopting the existing failed payment — never a
// second provider charge.
func TestCreateIntent_AdoptFailedOrderReturns422NoRecharge(t *testing.T) {
	svc, _, _, stub := newTestService()
	order := int64(77)
	in := intent(2002) // ...02 => declines on the first attempt
	in.OrderID = &order

	first, err := svc.CreateIntent(context.Background(), "k-a", in)
	if err != nil || first.Code != 422 || first.Payment.Status != domain.StatusFailed {
		t.Fatalf("first attempt: err=%v code=%d status=%v", err, first.Code, first.Payment.Status)
	}

	in2 := intent(2002)
	in2.OrderID = &order
	second, err := svc.CreateIntent(context.Background(), "k-b", in2)
	if err != nil {
		t.Fatalf("re-request on failed order: %v", err)
	}
	if second.Code != 422 || second.Payment.ID != first.Payment.ID {
		t.Fatalf("adopt failed order: code=%d id=%d (want 422, id=%d)", second.Code, second.Payment.ID, first.Payment.ID)
	}
	if got := stub.Charges(); got != 0 {
		t.Fatalf("a declined order must never mint a charge, got %d", got)
	}
}

// The S3 guard: if the authorize CAS goes stale for a reason other than a
// benign re-entry (e.g. the expiry job flipped the row), the charge succeeded
// but the row is not authorized/captured — driveCharge must reject, never
// cache a bogus 201.
func TestCreateIntent_ChargeSucceededButRowNotAuthorizedRejects(t *testing.T) {
	ep := &erroringPayments{fakePayments: newFakePayments(), transitionErr: domain.ErrStaleTransition}
	svc := NewService(ep, newFakeIdem(), provider.NewStub(), 168*time.Hour, WithAttempts(&recordingAttempts{}))

	_, err := svc.CreateIntent(context.Background(), "k-exp", intent(2000))
	if !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("charge succeeded but row not in a success state must reject, got %v", err)
	}
}

func TestCreateIntent_ReentryUsesExistingPayment(t *testing.T) {
	// A takeover whose key already checkpointed a payment must reuse it, not
	// create a second one.
	svc, fp, fi, _ := newTestService()
	in := intent(2000)

	// Simulate a crashed first attempt: claim + create payment + checkpoint,
	// then nothing else.
	key, _, err := fi.Claim(context.Background(), in.UserID, "k-re", "POST", "/payment/v1/private/payments", hashJSON(in))
	if err != nil {
		t.Fatal(err)
	}
	pay, err := fp.Create(context.Background(), &domain.Payment{UserID: in.UserID, AmountMinor: in.AmountMinor,
		Currency: in.Currency, CaptureMethod: in.CaptureMethod, PaymentMethod: in.PaymentMethod})
	if err != nil {
		t.Fatal(err)
	}
	if err := fi.Checkpoint(context.Background(), key.ID, &pay.ID); err != nil {
		t.Fatal(err)
	}
	// Age the lock past takeover.
	fi.mu.Lock()
	fi.keys["k-re"].LockedAt = time.Now().Add(-2 * time.Minute)
	fi.mu.Unlock()

	res, err := svc.CreateIntent(context.Background(), "k-re", in)
	if err != nil {
		t.Fatalf("re-entry: %v", err)
	}
	if res.Payment.ID != pay.ID {
		t.Fatalf("re-entry created a new payment (%d), must reuse %d", res.Payment.ID, pay.ID)
	}
	fp.mu.Lock()
	n := len(fp.items)
	fp.mu.Unlock()
	if n != 1 {
		t.Fatalf("exactly one payment after re-entry, got %d", n)
	}
}
