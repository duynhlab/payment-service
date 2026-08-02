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
	// Distinct keys on purpose: this test is about idempotency by STATE, which is
	// what actually protects a lost 200. Reusing one key would exercise the
	// binding check instead and leave the state branch uncovered.
	if err := s.Void(ctx, charge.ProviderPaymentID, "k-void-1"); err != nil {
		t.Fatalf("first void: %v", err)
	}
	if err := s.Void(ctx, charge.ProviderPaymentID, "k-void-2"); err != nil {
		t.Fatalf("second void must be a no-op, got %v", err)
	}
	// Its own key: reusing k-void here would be a key CONFLICT (same key, other
	// charge), which is a different assertion — see TestStub_KeyReuseAcrossCharges.
	if err := s.Void(ctx, "mp_unknown", "k-void-other"); err == nil {
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
		"capture": s.Capture(context.Background(), "mp_nope", "k-cap"),
		"void":    s.Void(context.Background(), "mp_nope", "k-void"),
	} {
		if !errors.Is(err, provider.ErrDefinite) {
			t.Errorf("%s on an unknown charge err = %v, want ErrDefinite", name, err)
		}
	}
	if _, err := s.Refund(context.Background(), "mp_nope", 100, "rk"); !errors.Is(err, provider.ErrDefinite) {
		t.Errorf("refund on an unknown charge err = %v, want ErrDefinite", err)
	}
}

// TestStub_KeyBindsOneOperationToOneCharge is the whole claim of the key: it
// names ONE operation on ONE charge, so reuse anywhere else is a caller bug that
// must surface. Deleting the binding makes this test fail — unlike a "same key
// twice succeeds" assertion, which passes with or without the mechanism because
// capture and void are already idempotent by state.
func TestStub_KeyBindsOneOperationToOneCharge(t *testing.T) {
	s := provider.NewStub()
	ctx := context.Background()
	a, _ := s.Charge(ctx, provider.ChargeRequest{IdempotencyKey: "a", AmountMinor: 5000, Currency: "USD"})
	b, _ := s.Charge(ctx, provider.ChargeRequest{IdempotencyKey: "b", AmountMinor: 6000, Currency: "USD"})

	const key = "capture:payment:1"
	if err := s.Capture(ctx, a.ProviderPaymentID, key); err != nil {
		t.Fatal(err)
	}
	if err := s.Capture(ctx, a.ProviderPaymentID, key); err != nil {
		t.Fatalf("same key, same operation, same charge must stay fine: %v", err)
	}

	other := s.Capture(ctx, b.ProviderPaymentID, key)
	if !errors.Is(other, provider.ErrKeyConflict) {
		t.Errorf("key reuse on another charge = %v, want ErrKeyConflict", other)
	}
	otherOp := s.Void(ctx, a.ProviderPaymentID, key)
	if !errors.Is(otherOp, provider.ErrKeyConflict) {
		t.Errorf("key reuse for another operation = %v, want ErrKeyConflict", otherOp)
	}
	if !errors.Is(otherOp, provider.ErrDefinite) {
		t.Error("a wrong key does not become right on retry — the conflict is decided")
	}
	// The refused void really did not happen: the charge is still capturable
	// under its own key, which it would not be if the void had gone through.
	if err := s.Void(ctx, a.ProviderPaymentID, "void:payment:1"); err != nil {
		t.Fatalf("void under its own key: %v", err)
	}
}

// TestStub_NoKeyKeepsStateBasedBehaviour: a caller that sends no key must be
// unaffected by the binding — the mechanism is additive.
func TestStub_NoKeyKeepsStateBasedBehaviour(t *testing.T) {
	s := provider.NewStub()
	ctx := context.Background()
	ch, _ := s.Charge(ctx, provider.ChargeRequest{IdempotencyKey: "c", AmountMinor: 5000, Currency: "USD"})

	for i := 0; i < 2; i++ {
		if err := s.Capture(ctx, ch.ProviderPaymentID, ""); err != nil {
			t.Fatalf("keyless capture %d: %v", i, err)
		}
	}
	if err := s.Void(ctx, ch.ProviderPaymentID, ""); err != nil {
		t.Fatalf("keyless void: %v", err)
	}
}

// TestStub_StateWinsOverAnyRememberedAnswer pins the reason no answer is
// remembered. Capture under key K, then void the hold, then retry capture with
// K: an answer-replaying double would report success for money the provider no
// longer holds. The truth must come from state.
func TestStub_StateWinsOverAnyRememberedAnswer(t *testing.T) {
	s := provider.NewStub()
	ctx := context.Background()
	ch, _ := s.Charge(ctx, provider.ChargeRequest{IdempotencyKey: "c", AmountMinor: 5000, Currency: "USD"})

	const capKey = "capture:payment:9"
	if err := s.Capture(ctx, ch.ProviderPaymentID, capKey); err != nil {
		t.Fatal(err)
	}
	if err := s.Void(ctx, ch.ProviderPaymentID, "void:payment:9"); err != nil {
		t.Fatal(err)
	}
	// The hold is gone. A retry of the ORIGINAL capture must say so.
	if err := s.Capture(ctx, ch.ProviderPaymentID, capKey); !errors.Is(err, provider.ErrDefinite) {
		t.Fatalf("capture retry after void = %v, want a decided failure — the hold is released", err)
	}
}

// TestStub_KeyReuseAcrossChargesIsAConflict: a key identifies ONE operation on
// ONE charge. Reusing it elsewhere is a caller bug, and answering it with the
// other charge's verdict would hide that bug behind a plausible success.
func TestStub_KeyReuseAcrossChargesIsAConflict(t *testing.T) {
	s := provider.NewStub()
	ctx := context.Background()
	a, _ := s.Charge(ctx, provider.ChargeRequest{IdempotencyKey: "a", AmountMinor: 5000, Currency: "USD"})
	b, _ := s.Charge(ctx, provider.ChargeRequest{IdempotencyKey: "b", AmountMinor: 6000, Currency: "USD"})

	if err := s.Capture(ctx, a.ProviderPaymentID, "shared"); err != nil {
		t.Fatal(err)
	}
	err := s.Capture(ctx, b.ProviderPaymentID, "shared")
	if !errors.Is(err, provider.ErrKeyConflict) {
		t.Fatalf("key reuse across charges err = %v, want ErrKeyConflict", err)
	}
	if !errors.Is(err, provider.ErrDefinite) {
		t.Error("a key conflict is a decided answer — retrying cannot fix a wrong key")
	}
}
