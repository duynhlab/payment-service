package v1

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

// Windowing is where reconciliation gets quietly wrong. The failure is not a
// crash: it is a pass that compares a narrow internal set against a wide provider
// answer and reports invented discrepancies, or one that steps over a stretch of
// history nobody ever checked. Both look like a healthy run.

var fixedNow = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

func windowedReconciler(repo *fakeReconRepo, ledger *fakeLedger) *Reconciler {
	return NewReconciler(repo, ledger, WithClock(func() time.Time { return fixedNow }))
}

// The first pass has no frontier to start from, so it must cover everything once.
// Its upper bound still stops short of now: a payment created seconds ago may
// legitimately not have reached the provider yet, and calling that missing would
// make the report cry wolf on healthy traffic.
func TestRecon_FirstPassIsUnboundedBelowAndLagsNow(t *testing.T) {
	repo := &fakeReconRepo{runID: 1}
	ledger := &fakeLedger{page: &provider.TransactionsPage{}}

	if _, _, err := windowedReconciler(repo, ledger).Run(context.Background(), 10); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !repo.gotWindow.From.IsZero() {
		t.Errorf("from = %v, want unbounded on the first pass", repo.gotWindow.From)
	}
	wantThrough := fixedNow.Add(-reconSettlementLag)
	if !repo.gotWindow.Through.Equal(wantThrough) {
		t.Errorf("through = %v, want %v (now less the settlement lag)", repo.gotWindow.Through, wantThrough)
	}
}

// TestRecon_BothSidesGetTheSameWindow is the one that matters most. Ask the
// provider for a different stretch than the internal query and every charge in
// the difference is reported as missing on one side — a discrepancy that is an
// artefact of the question, not a fact about the money.
func TestRecon_BothSidesGetTheSameWindow(t *testing.T) {
	mark := fixedNow.Add(-2 * time.Hour)
	repo := &fakeReconRepo{runID: 1, watermark: mark}
	ledger := &fakeLedger{page: &provider.TransactionsPage{}}

	if _, _, err := windowedReconciler(repo, ledger).Run(context.Background(), 10); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !ledger.gotFrom.Equal(repo.gotWindow.From) || !ledger.gotTo.Equal(repo.gotWindow.Through) {
		t.Fatalf("provider asked for [%v, %v) but internal for [%v, %v)",
			ledger.gotFrom, ledger.gotTo, repo.gotWindow.From, repo.gotWindow.Through)
	}
}

// The frontier is followed by a lookback, so every pass re-judges a trailing
// slice it has already seen. Without it, a `missing_provider` that was nothing
// worse than an authorize in flight would be frozen into a permanent discrepancy.
func TestRecon_WindowLooksBackPastTheFrontier(t *testing.T) {
	mark := fixedNow.Add(-30 * time.Minute)
	repo := &fakeReconRepo{runID: 1, watermark: mark}
	ledger := &fakeLedger{page: &provider.TransactionsPage{}}

	if _, _, err := windowedReconciler(repo, ledger).Run(context.Background(), 10); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := mark.Add(-reconLookback)
	if !repo.gotWindow.From.Equal(want) {
		t.Fatalf("from = %v, want %v (the frontier less a lookback)", repo.gotWindow.From, want)
	}
}

// A completed pass moves the frontier to exactly where it stopped comparing.
func TestRecon_CompletedPassAdvancesTheFrontier(t *testing.T) {
	repo := &fakeReconRepo{runID: 7}
	ledger := &fakeLedger{page: &provider.TransactionsPage{}}

	if _, _, err := windowedReconciler(repo, ledger).Run(context.Background(), 10); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !repo.advancedTo.Equal(fixedNow.Add(-reconSettlementLag)) {
		t.Fatalf("frontier moved to %v, want the window's upper bound", repo.advancedTo)
	}
}

// TestRecon_FailedPassLeavesTheFrontierAlone: the frontier means "everything
// before this has been compared". A pass that failed compared nothing, so moving
// it would step permanently over ground nobody checked — the one failure mode a
// watermark introduces that a full scan cannot have.
func TestRecon_FailedPassLeavesTheFrontierAlone(t *testing.T) {
	repo := &fakeReconRepo{runID: 3, listErr: errors.New("database went away")}
	ledger := &fakeLedger{page: &provider.TransactionsPage{}}

	if _, _, err := windowedReconciler(repo, ledger).Run(context.Background(), 10); err == nil {
		t.Fatal("run reported success on a detection failure")
	}
	if !repo.advancedTo.IsZero() {
		t.Fatalf("frontier moved to %v after a failed pass", repo.advancedTo)
	}
	if repo.finished != domain.ReconRunFailed {
		t.Errorf("run status = %s, want failed", repo.finished)
	}
}

// An unreadable frontier stops the pass before it starts. Guessing a window here
// would mean silently reconciling the wrong stretch of history.
func TestRecon_UnreadableFrontierRefusesToGuess(t *testing.T) {
	repo := &fakeReconRepo{runID: 1, watermarkErr: errors.New("watermark unreadable")}
	ledger := &fakeLedger{page: &provider.TransactionsPage{}}

	if _, _, err := windowedReconciler(repo, ledger).Run(context.Background(), 10); err == nil {
		t.Fatal("the pass ran without knowing where to start")
	}
	if ledger.gotTo != (time.Time{}) || !repo.gotWindow.Through.IsZero() {
		t.Fatal("the pass touched its sources before resolving the window")
	}
}

// A backfill covers an explicit stretch and still advances the frontier — the
// claim "everything before this has been compared" is now true of it. Monotonic
// advance is what keeps an OLDER backfill from dragging the frontier backwards
// and making the sweep re-read the same ground forever.
func TestRecon_BackfillAdvancesMonotonically(t *testing.T) {
	repo := &fakeReconRepo{runID: 1}
	ledger := &fakeLedger{page: &provider.TransactionsPage{}}
	rec := windowedReconciler(repo, ledger)
	ctx := context.Background()

	recent := domain.ReconWindow{From: fixedNow.Add(-time.Hour), Through: fixedNow.Add(-time.Minute)}
	if _, _, err := rec.RunWindow(ctx, 10, recent); err != nil {
		t.Fatalf("recent backfill: %v", err)
	}
	old := domain.ReconWindow{From: fixedNow.Add(-72 * time.Hour), Through: fixedNow.Add(-48 * time.Hour)}
	if _, _, err := rec.RunWindow(ctx, 10, old); err != nil {
		t.Fatalf("old backfill: %v", err)
	}
	if !repo.advancedTo.Equal(recent.Through) {
		t.Fatalf("frontier = %v, want it to stay at %v — an older pass must not pull it back",
			repo.advancedTo, recent.Through)
	}
}

// fakeLease records whether the pass took and returned its lease.
type fakeLease struct {
	held       bool
	releases   int
	releaseErr error
}

func (l *fakeLease) Release(context.Context) error {
	l.releases++
	return l.releaseErr
}

type fakeLeaser struct {
	lease   *fakeLease
	err     error
	gotKey  int64
	tryCall int
}

func (f *fakeLeaser) TryAcquire(_ context.Context, key int64) (domain.Lease, error) {
	f.gotKey = key
	f.tryCall++
	if f.err != nil {
		return nil, f.err
	}
	f.lease.held = true
	return f.lease, nil
}

// TestRecon_LeaseHeldStandsDownWithoutOpeningARun: standing down must leave no
// trace. Opening the run row first and then discovering the lease is taken would
// fill the runs table with passes that never happened, and the report is the thing
// an operator reads when money is missing.
func TestRecon_LeaseHeldStandsDownWithoutOpeningARun(t *testing.T) {
	repo := &fakeReconRepo{runID: 1}
	ledger := &fakeLedger{page: &provider.TransactionsPage{}}
	leaser := &fakeLeaser{err: domain.ErrLeaseHeld}
	rec := NewReconciler(repo, ledger, WithClock(func() time.Time { return fixedNow }), WithLease(leaser))

	_, _, err := rec.Run(context.Background(), 10)
	if !errors.Is(err, domain.ErrLeaseHeld) {
		t.Fatalf("err = %v, want ErrLeaseHeld reaching the caller unwrapped enough to match", err)
	}
	if repo.finishCalls != 0 || repo.gotWindow.Bounded() {
		t.Fatal("a pass that stood down still touched its sources")
	}
	if leaser.gotKey != domain.LeaseReconciliation {
		t.Errorf("lease key = %d, want the reconciliation key", leaser.gotKey)
	}
}

// The lease is returned on every path, including the failing ones. A pass holds a
// pooled connection for its whole duration, so a leak per failed pass would
// exhaust the pool within an hour of ticking.
func TestRecon_LeaseIsReleasedOnEveryPath(t *testing.T) {
	ctx := context.Background()

	t.Run("after a completed pass", func(t *testing.T) {
		l := &fakeLease{}
		rec := NewReconciler(&fakeReconRepo{runID: 1}, &fakeLedger{page: &provider.TransactionsPage{}},
			WithClock(func() time.Time { return fixedNow }), WithLease(&fakeLeaser{lease: l}))
		if _, _, err := rec.Run(ctx, 10); err != nil {
			t.Fatal(err)
		}
		if l.releases != 1 {
			t.Fatalf("releases = %d, want exactly 1", l.releases)
		}
	})

	t.Run("after a failed pass", func(t *testing.T) {
		l := &fakeLease{}
		rec := NewReconciler(&fakeReconRepo{runID: 1, listErr: errors.New("database went away")},
			&fakeLedger{page: &provider.TransactionsPage{}},
			WithClock(func() time.Time { return fixedNow }), WithLease(&fakeLeaser{lease: l}))
		if _, _, err := rec.Run(ctx, 10); err == nil {
			t.Fatal("expected the detection failure to surface")
		}
		if l.releases != 1 {
			t.Fatalf("releases = %d, want exactly 1 even when the pass failed", l.releases)
		}
	})
}
