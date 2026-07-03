package v1

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
	"github.com/duynhlab/payment-service/internal/core/repository"
)

var errBoom = errors.New("boom")

// erroringIdem injects failures into individual idem operations.
type erroringIdem struct {
	*fakeIdem
	claimErr, advanceErr, finishErr, releaseErr error
	// advanceHook, when set, decides the error per recovery point (nil = ok),
	// letting a test fail only a specific checkpoint.
	advanceHook func(point string) error
}

func (e *erroringIdem) Claim(ctx context.Context, userID int64, key, method, path, hash string) (*repository.IdempotencyKey, bool, error) {
	if e.claimErr != nil {
		return nil, false, e.claimErr
	}
	return e.fakeIdem.Claim(ctx, userID, key, method, path, hash)
}

func (e *erroringIdem) Advance(ctx context.Context, id int64, point string, paymentID *int64) error {
	if e.advanceErr != nil {
		return e.advanceErr
	}
	if e.advanceHook != nil {
		if err := e.advanceHook(point); err != nil {
			return err
		}
	}
	return e.fakeIdem.Advance(ctx, id, point, paymentID)
}

func (e *erroringIdem) Release(ctx context.Context, id int64) error {
	if e.releaseErr != nil {
		return e.releaseErr
	}
	return e.fakeIdem.Release(ctx, id)
}

func (e *erroringIdem) Finish(ctx context.Context, id int64, code int, body []byte) error {
	if e.finishErr != nil {
		return e.finishErr
	}
	return e.fakeIdem.Finish(ctx, id, code, body)
}

// erroringPayments injects failures into payment persistence.
type erroringPayments struct {
	*fakePayments
	createErr, findErr, transitionErr error
	// transitionHook, when set, decides the error per (from,to) — nil = fall
	// through to the real fake — so a test can fail only the rollback CAS.
	transitionHook func(from, to domain.Status) error
}

func (e *erroringPayments) Create(ctx context.Context, p *domain.Payment) (*domain.Payment, error) {
	if e.createErr != nil {
		return nil, e.createErr
	}
	return e.fakePayments.Create(ctx, p)
}

func (e *erroringPayments) FindByID(ctx context.Context, id, userID int64) (*domain.Payment, error) {
	if e.findErr != nil {
		return nil, e.findErr
	}
	return e.fakePayments.FindByID(ctx, id, userID)
}

func (e *erroringPayments) TransitionStatus(ctx context.Context, id int64, from, to domain.Status, set map[string]any) error {
	if e.transitionErr != nil {
		return e.transitionErr
	}
	if e.transitionHook != nil {
		if err := e.transitionHook(from, to); err != nil {
			return err
		}
	}
	return e.fakePayments.TransitionStatus(ctx, id, from, to, set)
}

func TestCreateIntent_RepoErrorPropagation(t *testing.T) {
	tests := []struct {
		name string
		mut  func(ep *erroringPayments, ei *erroringIdem)
	}{
		{"claim fails", func(_ *erroringPayments, ei *erroringIdem) { ei.claimErr = errBoom }},
		{"create fails", func(ep *erroringPayments, _ *erroringIdem) { ep.createErr = errBoom }},
		{"advance fails", func(_ *erroringPayments, ei *erroringIdem) { ei.advanceErr = errBoom }},
		{"transition fails", func(ep *erroringPayments, _ *erroringIdem) { ep.transitionErr = errBoom }},
		{"finish fails", func(_ *erroringPayments, ei *erroringIdem) { ei.finishErr = errBoom }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ep := &erroringPayments{fakePayments: newFakePayments()}
			ei := &erroringIdem{fakeIdem: newFakeIdem()}
			tt.mut(ep, ei)
			svc := NewService(ep, ei, provider.NewStub(), 168*time.Hour)
			if _, err := svc.CreateIntent(context.Background(), "k-err", intent(2000)); !errors.Is(err, errBoom) {
				t.Fatalf("want errBoom, got %v", err)
			}
		})
	}
}

func TestFinishIntent_FindErrorPropagates(t *testing.T) {
	ep := &erroringPayments{fakePayments: newFakePayments()}
	ei := &erroringIdem{fakeIdem: newFakeIdem()}
	svc := NewService(ep, ei, provider.NewStub(), 168*time.Hour)

	// Let the flow run, but poison the final snapshot read.
	res, err := svc.CreateIntent(context.Background(), "k-snap", intent(2000))
	if err != nil || res == nil {
		t.Fatalf("setup: %v", err)
	}
	ep.findErr = errBoom
	if _, err := svc.Get(context.Background(), res.Payment.ID, 7); !errors.Is(err, errBoom) {
		t.Fatalf("find error must propagate, got %v", err)
	}
}

func TestCreateRefund_ClaimError(t *testing.T) {
	ei := &erroringIdem{fakeIdem: newFakeIdem(), claimErr: errBoom}
	svc := NewService(&erroringPayments{fakePayments: newFakePayments()}, ei, provider.NewStub(), 168*time.Hour)

	if _, _, err := svc.CreateRefund(context.Background(), "rk", 1, 7, 100, ""); !errors.Is(err, errBoom) {
		t.Fatalf("claim error must propagate, got %v", err)
	}
}

func TestCreateRefund_PaymentLookupError(t *testing.T) {
	svc := NewService(newFakePayments(), newFakeIdem(), provider.NewStub(), 168*time.Hour)

	if _, _, err := svc.CreateRefund(context.Background(), "rk", 999, 7, 100, ""); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("missing payment must surface ErrNotFound, got %v", err)
	}
}

func TestCreateRefund_FinishError(t *testing.T) {
	ei := &erroringIdem{fakeIdem: newFakeIdem()}
	svc := NewService(newFakePayments(), ei, provider.NewStub(), 168*time.Hour)

	res, _ := svc.CreateIntent(context.Background(), "k-rf", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); err != nil {
		t.Fatal(err)
	}
	ei.finishErr = errBoom
	if _, _, err := svc.CreateRefund(context.Background(), "rk", res.Payment.ID, 7, 100, ""); !errors.Is(err, errBoom) {
		t.Fatalf("finish error must propagate, got %v", err)
	}
}

func TestCreateIntent_TransientReleaseErrorPropagates(t *testing.T) {
	// A transient provider error tries to release the lock; if that release
	// itself fails, the wrapped error surfaces (not swallowed).
	ei := &erroringIdem{fakeIdem: newFakeIdem(), releaseErr: errBoom}
	svc := NewService(newFakePayments(), ei, provider.NewStub(), 168*time.Hour)

	_, err := svc.CreateIntent(context.Background(), "k-tr", intent(2019)) // ...19 => transient
	if !errors.Is(err, errBoom) {
		t.Fatalf("release failure must surface, got %v", err)
	}
}

func TestCreateIntent_AdoptPendingOrderCompletesCharge(t *testing.T) {
	// A prior attempt created the order's payment (pending) but crashed before
	// charging. A fresh re-request adopts that row and drives the charge to
	// completion — one payment, one charge, authorized.
	svc, fp, _, stub := newTestService()
	order := int64(61)
	if _, err := fp.Create(context.Background(), &domain.Payment{
		UserID: 7, OrderID: &order, AmountMinor: 2000, Currency: "USD",
		CaptureMethod: domain.CaptureManual, PaymentMethod: "tok_visa",
	}); err != nil {
		t.Fatal(err)
	}
	in := intent(2000)
	in.OrderID = &order

	res, err := svc.CreateIntent(context.Background(), "k-adopt-ok", in)
	if err != nil {
		t.Fatalf("adopt+charge: %v", err)
	}
	if res.Code != 201 || res.Payment.Status != domain.StatusAuthorized {
		t.Fatalf("got code=%d status=%s, want 201 authorized", res.Code, res.Payment.Status)
	}
	if got := stub.Charges(); got != 1 {
		t.Fatalf("provider charged %d times, want 1", got)
	}
	fp.mu.Lock()
	n := len(fp.items)
	fp.mu.Unlock()
	if n != 1 {
		t.Fatalf("exactly one payment for the order, got %d", n)
	}
}

func TestCreateIntent_ProviderCalledAdvanceErrorPropagates(t *testing.T) {
	// The checkpoint after a successful charge (RecoveryProviderCalled) must
	// propagate its error rather than proceed to the state transitions.
	ep := &erroringPayments{fakePayments: newFakePayments()}
	ei := &erroringIdem{fakeIdem: newFakeIdem()}
	svc := NewService(ep, ei, provider.NewStub(), 168*time.Hour)

	// Let the first Advance (RecoveryStarted) succeed, fail the second one
	// (RecoveryProviderCalled) — flip the flag after the create checkpoint.
	ei.advanceHook = func(point string) error {
		if point == repository.RecoveryProviderCalled {
			return errBoom
		}
		return nil
	}
	if _, err := svc.CreateIntent(context.Background(), "k-pc", intent(2000)); !errors.Is(err, errBoom) {
		t.Fatalf("provider-called advance error must propagate, got %v", err)
	}
}

func TestCreateIntent_AdoptAdvanceErrorPropagates(t *testing.T) {
	// Adoption of an existing pending order-payment must propagate an Advance
	// (checkpoint) failure rather than silently charging.
	ep := &erroringPayments{fakePayments: newFakePayments()}
	ei := &erroringIdem{fakeIdem: newFakeIdem()}
	svc := NewService(ep, ei, provider.NewStub(), 168*time.Hour)

	order := int64(51)
	// Seed a pending payment for the order directly (a prior crashed attempt).
	if _, err := ep.fakePayments.Create(context.Background(), &domain.Payment{
		UserID: 7, OrderID: &order, AmountMinor: 2000, Currency: "USD",
		CaptureMethod: domain.CaptureManual, PaymentMethod: "tok_visa",
	}); err != nil {
		t.Fatal(err)
	}
	ei.advanceErr = errBoom
	in := intent(2000)
	in.OrderID = &order
	if _, err := svc.CreateIntent(context.Background(), "k-adopt", in); !errors.Is(err, errBoom) {
		t.Fatalf("adopt advance error must propagate, got %v", err)
	}
}

func TestCreateIntent_ReentryFindErrorPropagates(t *testing.T) {
	// On takeover the key already has a checkpointed payment_id; a FindByID
	// failure there must propagate.
	fp := newFakePayments()
	ep := &erroringPayments{fakePayments: fp}
	fi := newFakeIdem()
	svc := NewService(ep, fi, provider.NewStub(), 168*time.Hour)

	// Seed a checkpointed, in-flight key whose lock is already stale.
	pid := int64(123)
	fi.mu.Lock()
	fi.seq++
	fi.keys["k-re"] = &repository.IdempotencyKey{
		ID: fi.seq, UserID: 7, Key: "k-re",
		RequestMethod: "POST", RequestPath: "/payment/v1/private/payments",
		RequestHash: hashJSON(intent(2000)),
		LockedAt:    time.Unix(0, 0), PaymentID: &pid,
	}
	fi.mu.Unlock()

	ep.findErr = errBoom
	if _, err := svc.CreateIntent(context.Background(), "k-re", intent(2000)); !errors.Is(err, errBoom) {
		t.Fatalf("re-entry find error must propagate, got %v", err)
	}
}

// When the provider fails AND the compensating rollback also fails, both
// errors must surface wrapped — the row is left captured/voided and the
// operator needs to see both failures.
func TestCapture_ProviderFailAndRollbackFail(t *testing.T) {
	ep := &erroringPayments{fakePayments: newFakePayments()}
	prov := &failingProvider{Stub: provider.NewStub(), captureErr: errors.New("capture down")}
	svc := NewService(ep, newFakeIdem(), prov, 168*time.Hour)

	res, _ := svc.CreateIntent(context.Background(), "k-cf", intent(2000))
	// Fail only the captured→authorized rollback.
	ep.transitionHook = func(from, to domain.Status) error {
		if from == domain.StatusCaptured && to == domain.StatusAuthorized {
			return errBoom
		}
		return nil
	}
	_, err := svc.Capture(context.Background(), res.Payment.ID, 7)
	if err == nil || !strings.Contains(err.Error(), "rollback failed") || !errors.Is(err, errBoom) {
		t.Fatalf("want wrapped rollback-failed error, got %v", err)
	}
}

func TestVoid_ProviderFailAndRollbackFail(t *testing.T) {
	ep := &erroringPayments{fakePayments: newFakePayments()}
	prov := &failingProvider{Stub: provider.NewStub(), voidErr: errors.New("void down")}
	svc := NewService(ep, newFakeIdem(), prov, 168*time.Hour)

	res, _ := svc.CreateIntent(context.Background(), "k-vf", intent(2000))
	ep.transitionHook = func(from, to domain.Status) error {
		if from == domain.StatusVoided && to == domain.StatusAuthorized {
			return errBoom
		}
		return nil
	}
	_, err := svc.Void(context.Background(), res.Payment.ID, 7)
	if err == nil || !strings.Contains(err.Error(), "rollback failed") || !errors.Is(err, errBoom) {
		t.Fatalf("want wrapped rollback-failed error, got %v", err)
	}
}

func TestReloadAfterRace_FindError(t *testing.T) {
	ep := &erroringPayments{fakePayments: newFakePayments()}
	ei := &erroringIdem{fakeIdem: newFakeIdem()}
	svc := NewService(ep, ei, provider.NewStub(), 168*time.Hour)

	res, _ := svc.CreateIntent(context.Background(), "k-rr", intent(2000))
	// Force the CAS to lose while the status is in fact unchanged (still
	// authorized): reloadAfterRace must report the conflict, not succeed.
	ep.transitionErr = repository.ErrStaleTransition
	pay, err := svc.Capture(context.Background(), res.Payment.ID, 7)
	if err == nil || pay != nil {
		t.Fatalf("stale CAS with unchanged state must conflict, got pay=%v err=%v", pay, err)
	}
	if !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("want ErrInvalidTransition, got %v", err)
	}
}
