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
	"github.com/duynhlab/payment-service/internal/core/repository"
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

func TestCreateRefund_NotCapturedRejected(t *testing.T) {
	svc, _, _, _ := newTestService()
	res, _ := svc.CreateIntent(context.Background(), "k-nc", intent(2000)) // authorized only
	_, _, err := svc.CreateRefund(context.Background(), "rk-nc", res.Payment.ID, 7, 500, "")
	if !errors.Is(err, repository.ErrRefundRejected) {
		t.Fatalf("refunding an uncaptured payment must reject, got %v", err)
	}
}

func TestCaptureVoid_ProviderErrorPropagates(t *testing.T) {
	fp, fi := newFakePayments(), newFakeIdem()
	prov := &failingProvider{Stub: provider.NewStub(), captureErr: errors.New("capture down"), voidErr: errors.New("void down")}
	svc := NewService(fp, fi, prov, 168*time.Hour)

	res, _ := svc.CreateIntent(context.Background(), "k-pe", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); err == nil || !strings.Contains(err.Error(), "capture down") {
		t.Fatalf("capture provider error must propagate, got %v", err)
	}
	if _, err := svc.Void(context.Background(), res.Payment.ID, 7); err == nil || !strings.Contains(err.Error(), "void down") {
		t.Fatalf("void provider error must propagate, got %v", err)
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

func TestCapture_NotFoundAndOwnerScope(t *testing.T) {
	svc, _, _, _ := newTestService()
	if _, err := svc.Capture(context.Background(), 999, 7); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("missing payment: %v", err)
	}
	res, _ := svc.CreateIntent(context.Background(), "k-owner", intent(2000))
	if _, err := svc.Get(context.Background(), res.Payment.ID, 8); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("foreign user must not see the payment, got %v", err)
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
}

func TestReapIdempotencyKeys_Passthrough(t *testing.T) {
	svc, _, _, _ := newTestService()
	if _, err := svc.ReapIdempotencyKeys(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
}

func TestReplayResult_CorruptCache(t *testing.T) {
	code := 201
	if _, err := replayResult(&repository.IdempotencyKey{ResponseCode: &code, ResponseBody: []byte("{not json")}); err == nil {
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

func TestCreateIntent_ReentryUsesExistingPayment(t *testing.T) {
	// A takeover whose key already checkpointed a payment must reuse it, not
	// create a second one.
	svc, fp, fi, _ := newTestService()
	in := intent(2000)

	// Simulate a crashed first attempt: claim + create payment + checkpoint,
	// then nothing else.
	key, _, err := fi.Claim(context.Background(), in.UserID, "k-re", "POST", "/payment/v1/private/payments", hashInput(in))
	if err != nil {
		t.Fatal(err)
	}
	pay, err := fp.Create(context.Background(), &domain.Payment{UserID: in.UserID, AmountMinor: in.AmountMinor,
		Currency: in.Currency, CaptureMethod: in.CaptureMethod, PaymentMethod: in.PaymentMethod})
	if err != nil {
		t.Fatal(err)
	}
	if err := fi.Advance(context.Background(), key.ID, repository.RecoveryStarted, &pay.ID); err != nil {
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
