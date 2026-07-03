package v1

import (
	"context"
	"errors"
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
	claimErr, advanceErr, finishErr error
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
	return e.fakeIdem.Advance(ctx, id, point, paymentID)
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

func TestCreateRefund_ErrorPaths(t *testing.T) {
	ep := &erroringPayments{fakePayments: newFakePayments()}
	ei := &erroringIdem{fakeIdem: newFakeIdem()}
	svc := NewService(ep, ei, provider.NewStub(), 168*time.Hour)

	// Claim error.
	ei.claimErr = errBoom
	if _, _, err := svc.CreateRefund(context.Background(), "rk-e1", 1, 7, 100, ""); !errors.Is(err, errBoom) {
		t.Fatalf("claim err: %v", err)
	}
	ei.claimErr = nil

	// Payment lookup error.
	if _, _, err := svc.CreateRefund(context.Background(), "rk-e2", 999, 7, 100, ""); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("missing payment: %v", err)
	}

	// Finish error after a successful refund.
	res, _ := svc.CreateIntent(context.Background(), "k-rf", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); err != nil {
		t.Fatal(err)
	}
	ei.finishErr = errBoom
	if _, _, err := svc.CreateRefund(context.Background(), "rk-e3", res.Payment.ID, 7, 100, ""); !errors.Is(err, errBoom) {
		t.Fatalf("finish err: %v", err)
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
