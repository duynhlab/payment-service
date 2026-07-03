package v1

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

// A manual capture posts the capture ledger exactly once; a repeated capture is
// an idempotent no-op that posts nothing more — the posting rides the CAS.
func TestCapture_PostsLedgerOnceThenIdempotent(t *testing.T) {
	svc, fp, _, _ := newTestService()
	res, err := svc.CreateIntent(context.Background(), "k-cap", intent(2000))
	if err != nil {
		t.Fatalf("CreateIntent: %v", err)
	}
	if fp.ledgerPosts != 0 {
		t.Fatalf("authorize must not post ledger, got %d", fp.ledgerPosts)
	}

	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); err != nil {
		t.Fatalf("capture: %v", err)
	}
	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); err != nil {
		t.Fatalf("re-capture: %v", err)
	}
	if fp.ledgerPosts != 1 {
		t.Fatalf("capture must post ledger exactly once, got %d", fp.ledgerPosts)
	}
	if fp.reversals != 0 {
		t.Fatalf("successful capture must not reverse, got %d", fp.reversals)
	}
}

// Auto-capture posts the ledger once; an idempotent replay of the same request
// re-runs the flow but the stale CAS posts nothing more.
func TestAutoCapture_PostsLedgerOncePerReplay(t *testing.T) {
	svc, fp, _, _ := newTestService()
	in := intent(2000)
	in.CaptureMethod = domain.CaptureAutomatic

	for i := 0; i < 2; i++ {
		if _, err := svc.CreateIntent(context.Background(), "k-auto", in); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if fp.ledgerPosts != 1 {
		t.Fatalf("auto-capture must post ledger once across replays, got %d", fp.ledgerPosts)
	}
}

// When the provider capture fails, the row and the ledger are both compensated:
// one capture post followed by one reversal, leaving the payment authorized.
func TestCapture_ProviderFailPostsReversal(t *testing.T) {
	fp := newFakePayments()
	prov := &failingProvider{Stub: provider.NewStub(), captureErr: errors.New("capture down")}
	svc := NewService(fp, newFakeIdem(), prov, 168*time.Hour)

	res, err := svc.CreateIntent(context.Background(), "k-rev", intent(2000))
	if err != nil {
		t.Fatalf("CreateIntent: %v", err)
	}
	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); err == nil {
		t.Fatal("expected provider capture failure")
	}
	if fp.ledgerPosts != 1 || fp.reversals != 1 {
		t.Fatalf("want 1 post + 1 reversal, got posts=%d reversals=%d", fp.ledgerPosts, fp.reversals)
	}
	pay, err := svc.Get(context.Background(), res.Payment.ID, 7)
	if err != nil {
		t.Fatal(err)
	}
	if pay.Status != domain.StatusAuthorized {
		t.Fatalf("failed capture must leave payment authorized, got %s", pay.Status)
	}
}
