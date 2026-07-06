package v1

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

// fakeHealer records the payment ids heal attempted; converged/err are returned.
type fakeHealer struct {
	calls     []int64
	converged bool
	err       error
}

func (f *fakeHealer) HealCapture(_ context.Context, id int64) (bool, error) {
	f.calls = append(f.calls, id)
	return f.converged, f.err
}

func TestHealable(t *testing.T) {
	cases := []struct {
		name string
		d    domain.Discrepancy
		want bool
	}{
		{"crash-window: authorized vs captured", domain.Discrepancy{
			Class: domain.DiscrepancyStatusMismatch, InternalStatus: "authorized", ProviderStatus: "captured"}, true},
		{"mirror: captured vs authorized (needs provider write)", domain.Discrepancy{
			Class: domain.DiscrepancyStatusMismatch, InternalStatus: "captured", ProviderStatus: "authorized"}, false},
		{"amount_mismatch is never healed", domain.Discrepancy{
			Class: domain.DiscrepancyAmountMismatch, InternalStatus: "authorized", ProviderStatus: "captured"}, false},
		{"missing_internal is never healed", domain.Discrepancy{
			Class: domain.DiscrepancyMissingInternal, ProviderStatus: "captured"}, false},
		{"status_mismatch to a non-captured provider state", domain.Discrepancy{
			Class: domain.DiscrepancyStatusMismatch, InternalStatus: "authorized", ProviderStatus: "voided"}, false},
	}
	for _, c := range cases {
		if got := healable(c.d); got != c.want {
			t.Errorf("%s: healable = %v, want %v", c.name, got, c.want)
		}
	}
}

// reconWithDrift builds a repo+ledger where mp_heal is the healable crash window
// (internal authorized / provider captured), mp_mirror is the unhealed mirror
// (internal captured / provider authorized), and mp_amt is an amount_mismatch.
func reconWithDrift() (*fakeReconRepo, *fakeLedger) {
	repo := &fakeReconRepo{
		runID: 9,
		rows: []domain.ReconRow{
			{ID: 101, ProviderPaymentID: "mp_heal", AmountMinor: 1000, Status: domain.StatusAuthorized},
			{ID: 102, ProviderPaymentID: "mp_mirror", AmountMinor: 2000, Status: domain.StatusCaptured},
			{ID: 103, ProviderPaymentID: "mp_amt", AmountMinor: 3000, Status: domain.StatusCaptured},
		},
	}
	ledger := &fakeLedger{page: &provider.TransactionsPage{Total: 3, Transactions: []provider.Transaction{
		{ProviderPaymentID: "mp_heal", AmountMinor: 1000, Status: provider.TxnCaptured},
		{ProviderPaymentID: "mp_mirror", AmountMinor: 2000, Status: provider.TxnAuthorized},
		{ProviderPaymentID: "mp_amt", AmountMinor: 3001, Status: provider.TxnCaptured},
	}}}
	return repo, ledger
}

func TestReconciler_Heal_ConvergesOnlyCrashWindow(t *testing.T) {
	repo, ledger := reconWithDrift()
	healer := &fakeHealer{converged: true}

	_, found, err := NewReconciler(repo, ledger, WithHealer(healer)).Run(context.Background(), 100)
	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if found != 3 {
		t.Fatalf("found = %d, want 3", found)
	}
	// Only the crash-window payment id was converged.
	if len(healer.calls) != 1 || healer.calls[0] != 101 {
		t.Fatalf("healer.calls = %v, want [101]", healer.calls)
	}
	if want := map[string]domain.Resolution{
		"mp_heal":   domain.ResolutionHealed,
		"mp_mirror": domain.ResolutionSkipped,
		"mp_amt":    domain.ResolutionSkipped,
	}; !sameResolutions(repo.resolved, want) {
		t.Fatalf("resolutions = %v, want %v", repo.resolved, want)
	}
}

func TestReconciler_Heal_Disabled_IsDetectOnly(t *testing.T) {
	repo, ledger := reconWithDrift()

	if _, _, err := NewReconciler(repo, ledger).Run(context.Background(), 100); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	// No healer wired → nothing converged, nothing marked (rows keep 'detected').
	if len(repo.resolved) != 0 {
		t.Fatalf("resolved = %v, want empty (detect-only)", repo.resolved)
	}
}

func TestReconciler_Heal_MarkResolvedFailureIsNonFatal(t *testing.T) {
	repo, ledger := reconWithDrift()
	repo.resolveErr = errBoom // the audit write fails; the heal + run must still complete

	_, _, err := NewReconciler(repo, ledger, WithHealer(&fakeHealer{converged: true}), WithLogger(zap.NewNop())).
		Run(context.Background(), 100)
	if err != nil {
		t.Fatalf("a mark-resolved failure must not fail the run, got %v", err)
	}
	if repo.finished != domain.ReconRunCompleted {
		t.Fatalf("run status = %q, want completed", repo.finished)
	}
}

func TestWithLogger(t *testing.T) {
	repo, ledger := reconWithDrift()
	// A nil logger is ignored (the Nop default stays); a real one is attached.
	NewReconciler(repo, ledger, WithLogger(nil))
	NewReconciler(repo, ledger, WithLogger(zap.NewNop()))
}

func TestReconciler_Heal_FailureIsRecordedNotFatal(t *testing.T) {
	repo, ledger := reconWithDrift()
	healer := &fakeHealer{err: errBoom}

	_, _, err := NewReconciler(repo, ledger, WithHealer(healer)).Run(context.Background(), 100)
	if err != nil {
		t.Fatalf("a heal failure must not fail the run, got %v", err)
	}
	if repo.resolved["mp_heal"] != domain.ResolutionFailed {
		t.Fatalf("mp_heal resolution = %q, want failed", repo.resolved["mp_heal"])
	}
	if repo.finished != domain.ReconRunCompleted {
		t.Fatalf("run status = %q, want completed", repo.finished)
	}
}

// fakeCapturer records the capture and returns a preset error; findStatus is the
// status FindByID reports (used to resolve a stale CAS post-state).
type fakeCapturer struct {
	gotID      int64
	gotNow     time.Time
	err        error
	findStatus domain.Status
	findErr    error
}

func (f *fakeCapturer) CaptureWithLedger(_ context.Context, id int64, capturedAt time.Time) error {
	f.gotID, f.gotNow = id, capturedAt
	return f.err
}

func (f *fakeCapturer) FindByID(_ context.Context, id, _ int64) (*domain.Payment, error) {
	if f.findErr != nil {
		return nil, f.findErr
	}
	return &domain.Payment{ID: id, Status: f.findStatus}, nil
}

func TestCaptureHealer_HealCapture(t *testing.T) {
	fixed := time.Unix(1700000000, 0)

	t.Run("winning the CAS converges the row", func(t *testing.T) {
		cap := &fakeCapturer{}
		converged, err := NewCaptureHealer(cap, func() time.Time { return fixed }).HealCapture(context.Background(), 55)
		if err != nil || !converged {
			t.Fatalf("HealCapture = (%v, %v), want (true, nil)", converged, err)
		}
		if cap.gotID != 55 || !cap.gotNow.Equal(fixed) {
			t.Fatalf("captured id/now = %d/%v, want 55/%v", cap.gotID, cap.gotNow, fixed)
		}
	})

	t.Run("stale CAS but already captured counts as converged", func(t *testing.T) {
		cap := &fakeCapturer{err: domain.ErrStaleTransition, findStatus: domain.StatusCaptured}
		converged, err := NewCaptureHealer(cap, time.Now).HealCapture(context.Background(), 55)
		if err != nil || !converged {
			t.Fatalf("HealCapture = (%v, %v), want (true, nil) — idempotent re-capture", converged, err)
		}
	})

	t.Run("stale CAS from a concurrent void is NOT converged", func(t *testing.T) {
		cap := &fakeCapturer{err: domain.ErrStaleTransition, findStatus: domain.StatusVoided}
		converged, err := NewCaptureHealer(cap, time.Now).HealCapture(context.Background(), 55)
		if err != nil || converged {
			t.Fatalf("HealCapture = (%v, %v), want (false, nil) — row not captured", converged, err)
		}
	})

	t.Run("a verify read error is surfaced", func(t *testing.T) {
		cap := &fakeCapturer{err: domain.ErrStaleTransition, findErr: errors.New("db down")}
		if _, err := NewCaptureHealer(cap, time.Now).HealCapture(context.Background(), 55); err == nil {
			t.Fatal("HealCapture must surface a verify-read error")
		}
	})

	t.Run("a real capture error is surfaced", func(t *testing.T) {
		cap := &fakeCapturer{err: errors.New("db down")}
		if _, err := NewCaptureHealer(cap, time.Now).HealCapture(context.Background(), 55); err == nil {
			t.Fatal("HealCapture must surface a real capture error")
		}
	})
}

func TestReconciler_Heal_StaleRaceRecordedSkipped(t *testing.T) {
	repo, ledger := reconWithDrift()
	// Heal attempted the crash-window row but it did not converge (a concurrent
	// void/expiry won the CAS) → recorded skipped, not healed.
	healer := &fakeHealer{converged: false}

	if _, _, err := NewReconciler(repo, ledger, WithHealer(healer)).Run(context.Background(), 100); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if len(healer.calls) != 1 || healer.calls[0] != 101 {
		t.Fatalf("healer.calls = %v, want [101]", healer.calls)
	}
	if repo.resolved["mp_heal"] != domain.ResolutionSkipped {
		t.Fatalf("mp_heal resolution = %q, want skipped (did not converge)", repo.resolved["mp_heal"])
	}
}

func sameResolutions(got, want map[string]domain.Resolution) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}
