package provider_test

import (
	"context"
	"errors"
	"testing"

	"github.com/duynhlab/payment-service/internal/core/provider"
)

// The Stub must match the mock's void contract: voiding an issued hold twice is
// a no-op, and voiding a never-issued id is an error.
func TestStub_VoidIsIdempotent(t *testing.T) {
	s := provider.NewStub()
	ctx := context.Background()
	charge, err := s.Charge(ctx, provider.ChargeRequest{IdempotencyKey: "k", AmountMinor: 5000, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Void(ctx, charge.ProviderPaymentID); err != nil {
		t.Fatalf("first void: %v", err)
	}
	if err := s.Void(ctx, charge.ProviderPaymentID); err != nil {
		t.Fatalf("second void must be a no-op, got %v", err)
	}
	if err := s.Void(ctx, "mp_unknown"); err == nil {
		t.Fatal("void of unknown id must error")
	}
}

// The no-answer trigger is silence, not a verdict: it must NOT be classified as
// decided, or a caller would resolve an intent whose fate it does not know.
func TestStub_NoAnswerIsUndecided(t *testing.T) {
	s := provider.NewStub()
	_, err := s.Charge(context.Background(), provider.ChargeRequest{
		IdempotencyKey: "k-na", AmountMinor: 5013, Currency: "USD"})
	if err == nil {
		t.Fatal("the …13 trigger must not answer")
	}
	if errors.Is(err, provider.ErrDefinite) {
		t.Fatalf("silence is not a decided answer: %v", err)
	}
	var declined *provider.DeclinedError
	if errors.As(err, &declined) {
		t.Fatalf("silence is not a decline: %v", err)
	}
}

// An unknown provider payment IS decided — the stub must say so, or a
// stub-backed test would see a classification production never produces.
func TestStub_UnknownChargeIsDecided(t *testing.T) {
	s := provider.NewStub()
	for name, err := range map[string]error{
		"capture": s.Capture(context.Background(), "mp_nope"),
		"void":    s.Void(context.Background(), "mp_nope"),
	} {
		if !errors.Is(err, provider.ErrDefinite) {
			t.Errorf("%s on an unknown charge err = %v, want ErrDefinite", name, err)
		}
	}
	if _, err := s.Refund(context.Background(), "mp_nope", 100, "rk"); !errors.Is(err, provider.ErrDefinite) {
		t.Errorf("refund on an unknown charge err = %v, want ErrDefinite", err)
	}
}
