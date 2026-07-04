package provider_test

import (
	"context"
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
