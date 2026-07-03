package v1

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

// failingProvider wraps the stub to force specific provider outcomes.
type failingProvider struct {
	*provider.Stub
	refundErr  error
	captureErr error
	voidErr    error
}

func (f *failingProvider) Refund(ctx context.Context, id string, amt int64, key string) (string, error) {
	if f.refundErr != nil {
		return "", f.refundErr
	}
	return f.Stub.Refund(ctx, id, amt, key)
}

func (f *failingProvider) Capture(ctx context.Context, id string) error {
	if f.captureErr != nil {
		return f.captureErr
	}
	return f.Stub.Capture(ctx, id)
}

func (f *failingProvider) Void(ctx context.Context, id string) error {
	if f.voidErr != nil {
		return f.voidErr
	}
	return f.Stub.Void(ctx, id)
}

func TestCreateRefund_ProviderFailureSettlesFailed(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(), refundErr: errors.New("mockpay down")}
	svc := NewService(fp, fi, prov, 168*time.Hour)

	res, _ := svc.CreateIntent(context.Background(), "k-pf", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); err != nil {
		t.Fatal(err)
	}
	ref, _, err := svc.CreateRefund(context.Background(), "rk-pf", res.Payment.ID, 7, 500, "")
	if err != nil {
		t.Fatalf("refund with provider failure should settle failed, got err %v", err)
	}
	if ref.Status != domain.RefundFailed {
		t.Fatalf("refund status = %s, want failed", ref.Status)
	}
	// A failed refund releases its reserved amount: full refund still possible.
	if _, _, err := svc.CreateRefund(context.Background(), "rk-pf2", res.Payment.ID, 7, 2000, ""); err != nil {
		t.Fatalf("full refund after failed refund must pass, got %v", err)
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
			if _, err := svc.Void(context.Background(), res.Payment.ID, 7); err != nil {
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
			_, _, err := svc.CreateRefund(context.Background(), "rk-"+name, id, 7, 500, "")
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
func TestCapture_ProviderFailureRollsBackToAuthorized(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(), captureErr: errors.New("capture down")}
	svc := NewService(fp, fi, prov, 168*time.Hour)

	res, _ := svc.CreateIntent(context.Background(), "k-cap", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); err == nil || !strings.Contains(err.Error(), "capture down") {
		t.Fatalf("capture provider error must propagate, got %v", err)
	}
	got, err := svc.Get(context.Background(), res.Payment.ID, 7)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusAuthorized {
		t.Fatalf("after failed capture, status = %s, want authorized (rolled back)", got.Status)
	}
}

func TestVoid_ProviderFailureRollsBackToAuthorized(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(), voidErr: errors.New("void down")}
	svc := NewService(fp, fi, prov, 168*time.Hour)

	res, _ := svc.CreateIntent(context.Background(), "k-void", intent(2000))
	if _, err := svc.Void(context.Background(), res.Payment.ID, 7); err == nil || !strings.Contains(err.Error(), "void down") {
		t.Fatalf("void provider error must propagate, got %v", err)
	}
	got, err := svc.Get(context.Background(), res.Payment.ID, 7)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.StatusAuthorized {
		t.Fatalf("after failed void, status = %s, want authorized (rolled back)", got.Status)
	}
}

func TestVoid_NotFound(t *testing.T) {
	svc, _, _, _ := newTestService()
	if _, err := svc.Void(context.Background(), 999, 7); !errors.Is(err, domain.ErrNotFound) {
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
	go func() { defer wg.Done(); _, capErr = svc.Capture(context.Background(), id, 7) }()
	go func() { defer wg.Done(); _, voidErr = svc.Void(context.Background(), id, 7) }()
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
	if _, err := svc.Capture(context.Background(), 999, 7); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("capturing a missing payment must return ErrNotFound, got %v", err)
	}
}

func TestGet_ForeignUserScoped(t *testing.T) {
	svc, _, _, _ := newTestService()
	res, _ := svc.CreateIntent(context.Background(), "k-owner", intent(2000))
	if _, err := svc.Get(context.Background(), res.Payment.ID, 8); !errors.Is(err, domain.ErrNotFound) {
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
	items, total, err := svc.List(context.Background(), 7, 1, 2)
	if err != nil || total != 3 || len(items) != 2 {
		t.Fatalf("page1: err=%v total=%d len=%d", err, total, len(items))
	}
	items, _, _ = svc.List(context.Background(), 7, 2, 2)
	if len(items) != 1 {
		t.Fatalf("page2 len=%d, want 1", len(items))
	}
	// Page beyond the last still reports the true total with an empty page.
	items, total, err = svc.List(context.Background(), 7, 99, 2)
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
	if _, err := replayResult(&domain.IdempotencyKey{ResponseCode: &code, ResponseBody: []byte("{not json")}); err == nil {
		t.Fatal("corrupt cache must error")
	}
}

func TestCreateRefund_CorruptCache(t *testing.T) {
	svc, _, fi, _ := newTestService()
	res, _ := svc.CreateIntent(context.Background(), "k-cc", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.CreateRefund(context.Background(), "rk-cc", res.Payment.ID, 7, 500, ""); err != nil {
		t.Fatal(err)
	}
	fi.mu.Lock()
	for _, k := range fi.keys {
		if k.Key == "rk-cc" {
			k.ResponseBody = []byte("{corrupt")
		}
	}
	fi.mu.Unlock()
	if _, _, err := svc.CreateRefund(context.Background(), "rk-cc", res.Payment.ID, 7, 500, ""); err == nil {
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
	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); err != nil {
		t.Fatal(err)
	}
	// First refund settles, but Finish fails → key left unfinished.
	ei.finishErr = errBoom
	if _, _, err := svc.CreateRefund(context.Background(), "rk", res.Payment.ID, 7, 500, "damaged"); !errors.Is(err, errBoom) {
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

	ref, replayed, err := svc.CreateRefund(context.Background(), "rk", res.Payment.ID, 7, 500, "damaged")
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
	svc := NewService(ep, newFakeIdem(), provider.NewStub(), 168*time.Hour)

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
	if err := fi.Advance(context.Background(), key.ID, domain.RecoveryStarted, &pay.ID); err != nil {
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
