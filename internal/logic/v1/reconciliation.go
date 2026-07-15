package v1

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

const (
	// defaultReconPageSize is used when a caller passes a non-positive page size.
	defaultReconPageSize = 100
	// maxReconTransactions bounds one pass against a provider that returns full
	// pages indefinitely (a bug, or an inflated Total from a hostile provider).
	maxReconTransactions = 1_000_000
	// finishGrace bounds the FinishRun write. It runs on a context detached from
	// the caller's (see finish) so a cancelled trigger request or a shutdown
	// can't strand the run row in 'running' forever.
	finishGrace = 5 * time.Second
)

// ProviderLedger is the provider-side food source reconciliation pages through
// (the HTTP client's GET /transactions). Segregated from the full provider port
// so only the real HTTP provider needs to implement it — the in-process Stub,
// used with no provider to reconcile against, does not.
type ProviderLedger interface {
	GetTransactions(ctx context.Context, page, pageSize int) (*provider.TransactionsPage, error)
}

// ReconRepo is reconciliation's persistence port: the internal side to compare
// (ListReconcilable) plus the run/discrepancy record it writes.
type ReconRepo interface {
	// ListReconcilable returns every payment that has a provider_payment_id — the
	// internal rows to match against the provider ledger.
	ListReconcilable(ctx context.Context) ([]domain.ReconRow, error)
	CreateRun(ctx context.Context) (int64, error)
	SaveDiscrepancies(ctx context.Context, runID int64, ds []domain.Discrepancy) error
	// MarkResolved records what a heal pass did about one discrepancy, keyed by
	// (run, provider_payment_id) — a run has at most one discrepancy per charge.
	MarkResolved(ctx context.Context, runID int64, providerPaymentID string, res domain.Resolution) error
	FinishRun(ctx context.Context, runID int64, scanned, found int, status domain.ReconRunStatus) error
}

// Healer converges an internal row that is behind a provider capture: it moves
// the payment authorized→captured by internal payment id, via the idempotent,
// CAS-guarded capture-with-ledger path. It never touches the provider (the
// provider already captured) — see ADR-012. It reports whether the row is
// genuinely captured afterwards so a lost CAS race isn't stamped as healed.
// *CaptureHealer implements it.
type Healer interface {
	HealCapture(ctx context.Context, paymentID int64) (converged bool, err error)
}

// Reconciler compares the internal payment record against the provider ledger
// and records the mismatches. Detect-only by default; when a Healer is wired
// (RECON_HEAL_ENABLED), it also converges the one healable class — ADR-012.
type Reconciler struct {
	repo   ReconRepo
	ledger ProviderLedger
	healer Healer      // nil = detect-only (the default)
	logger *zap.Logger // heal-path diagnostics; Nop unless WithLogger is set
}

// ReconcilerOption configures an optional Reconciler capability.
type ReconcilerOption func(*Reconciler)

// WithHealer enables auto-heal by wiring the money-moving convergence port. Omit
// it (or pass nil) to keep the detect-only behaviour of ADR-011.
func WithHealer(h Healer) ReconcilerOption {
	return func(r *Reconciler) { r.healer = h }
}

// WithLogger attaches a logger for the heal path (failed convergences /
// mark-resolved writes). Detection reports through Run's return value as before.
func WithLogger(l *zap.Logger) ReconcilerOption {
	return func(r *Reconciler) {
		if l != nil {
			r.logger = l
		}
	}
}

// NewReconciler wires the reconciler onto its persistence port and the provider
// ledger it pages. Detect-only unless WithHealer is passed.
func NewReconciler(repo ReconRepo, ledger ProviderLedger, opts ...ReconcilerOption) *Reconciler {
	r := &Reconciler{repo: repo, ledger: ledger, logger: zap.NewNop()}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run performs one reconciliation pass: open a run, detect discrepancies, persist
// them, and close the run. Returns the run id and the number of discrepancies.
// A detection or persistence error marks the run failed and is returned.
func (r *Reconciler) Run(ctx context.Context, pageSize int) (runID int64, found int, err error) {
	runID, err = r.repo.CreateRun(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("create reconciliation run: %w", err)
	}

	discrepancies, internal, scanned, err := r.detect(ctx, pageSize)
	if err != nil {
		r.finish(ctx, runID, scanned, 0, domain.ReconRunFailed)
		return runID, 0, fmt.Errorf("detect discrepancies: %w", err)
	}

	// Count what detection found, grouped by the bounded discrepancy class —
	// ledger-vs-provider drift is the KPI, so count at detection (before persist
	// or heal).
	byClass := make(map[domain.DiscrepancyClass]int64)
	for _, d := range discrepancies {
		byClass[d.Class]++
	}
	for class, n := range byClass {
		recordReconDiscrepancies(ctx, string(class), n)
	}

	if len(discrepancies) > 0 {
		if serr := r.repo.SaveDiscrepancies(ctx, runID, discrepancies); serr != nil {
			r.finish(ctx, runID, scanned, 0, domain.ReconRunFailed)
			return runID, 0, fmt.Errorf("save discrepancies: %w", serr)
		}
		// Heal runs after the report is persisted, so the run always records what
		// was found before any correction. No-op unless a Healer is wired.
		r.heal(ctx, runID, discrepancies, internal)
	}

	if ferr := r.repo.FinishRun(ctx, runID, scanned, len(discrepancies), domain.ReconRunCompleted); ferr != nil {
		// The caller's context may itself be why the write failed (an aborted
		// trigger request, a shutdown); retry the close detached so the run can
		// never stay 'running' — the invariant is that every run is closed.
		r.finish(ctx, runID, scanned, len(discrepancies), domain.ReconRunCompleted)
		return runID, len(discrepancies), fmt.Errorf("finish reconciliation run: %w", ferr)
	}
	return runID, len(discrepancies), nil
}

// finish closes a run on a context detached from the caller's cancellation
// (bounded by finishGrace). The failure paths reach here precisely when ctx may
// already be cancelled — a client that aborted the triggering request, or a
// shutdown mid-pass — and closing the run with the same dead context would fail
// too, stranding the row in 'running' with no terminal status.
func (r *Reconciler) finish(ctx context.Context, runID int64, scanned, found int, status domain.ReconRunStatus) {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishGrace)
	defer cancel()
	_ = r.repo.FinishRun(fctx, runID, scanned, found, status)
}

// detect loads the internal rows, pages the full provider ledger, and classifies
// the two sides. Returns the discrepancies, the internal rows keyed by
// provider_payment_id (so heal can resolve a discrepancy to its payment id), and
// the number of provider transactions scanned.
func (r *Reconciler) detect(ctx context.Context, pageSize int) ([]domain.Discrepancy, map[string]domain.ReconRow, int, error) {
	rows, err := r.repo.ListReconcilable(ctx)
	if err != nil {
		return nil, nil, 0, err
	}
	internal := make(map[string]domain.ReconRow, len(rows))
	for _, row := range rows {
		internal[row.ProviderPaymentID] = row
	}

	if pageSize <= 0 {
		pageSize = defaultReconPageSize
	}
	var txns []provider.Transaction
	for page := 1; ; page++ {
		p, perr := r.ledger.GetTransactions(ctx, page, pageSize)
		if perr != nil {
			return nil, nil, 0, perr
		}
		txns = append(txns, p.Transactions...)
		// Terminate on a short (or empty) page. The provider's Total is advisory
		// only — trusting it risks stopping early on an under-stated Total (which
		// would drop whole pages and falsely flag their payments missing_provider).
		if len(p.Transactions) < pageSize {
			break
		}
		// Hard cap against a runaway/hostile provider (full pages + inflated
		// Total): bound memory/time rather than paging forever.
		if len(txns) > maxReconTransactions {
			return nil, nil, 0, fmt.Errorf("reconciliation aborted: provider returned more than %d transactions", maxReconTransactions)
		}
	}

	return classify(internal, txns), internal, len(txns), nil
}

// heal is the ADR-012 auto-heal pass: converge the one safe drift class and
// record what it did about every discrepancy. No-op when no Healer is wired
// (detect-only, ADR-011) — the discrepancies keep their inserted 'detected'.
//
// Healable = the lost-capture-response window: status_mismatch where internal is
// authorized and the provider is captured (the provider collected the money but
// our lost-response rollback reverted the row). Converging re-drives the
// idempotent, CAS-guarded capture by internal payment id; the provider is never
// called. Every other discrepancy is recorded 'skipped'. A heal error is
// recorded 'failed' and logged (alertable); it never aborts the run.
func (r *Reconciler) heal(ctx context.Context, runID int64, discrepancies []domain.Discrepancy, internal map[string]domain.ReconRow) {
	if r.healer == nil {
		return
	}
	for _, d := range discrepancies {
		if !healable(d) {
			r.markResolved(ctx, runID, d.ProviderPaymentID, domain.ResolutionSkipped)
			continue
		}
		// Record the outcome by what actually happened to the row: healed only when
		// it converged to captured; skipped when a concurrent void/expiry won the
		// CAS (still a real discrepancy the next run re-detects); failed on error.
		converged, err := r.healer.HealCapture(ctx, internal[d.ProviderPaymentID].ID)
		var res domain.Resolution
		switch {
		case err != nil:
			res = domain.ResolutionFailed
			r.logger.Error("reconciliation heal failed",
				zap.Int64("run_id", runID), zap.String("provider_payment_id", d.ProviderPaymentID), zap.Error(err))
		case converged:
			res = domain.ResolutionHealed
		default:
			res = domain.ResolutionSkipped
		}
		r.markResolved(ctx, runID, d.ProviderPaymentID, res)
	}
}

// markResolved records a resolution best-effort: a failed write only loses the
// audit annotation, never the (already-persisted) discrepancy or the heal.
func (r *Reconciler) markResolved(ctx context.Context, runID int64, providerPaymentID string, res domain.Resolution) {
	if err := r.repo.MarkResolved(ctx, runID, providerPaymentID, res); err != nil {
		r.logger.Error("reconciliation mark-resolved failed",
			zap.Int64("run_id", runID), zap.String("provider_payment_id", providerPaymentID),
			zap.String("resolution", string(res)), zap.Error(err))
	}
}

// healable reports whether a discrepancy is the single class ADR-012 heals: the
// provider-ahead capture window (internal authorized vs provider captured). The
// mirror window (internal captured vs provider authorized) needs a provider
// write the reconciler deliberately cannot make, so it stays detect-only.
func healable(d domain.Discrepancy) bool {
	return d.Class == domain.DiscrepancyStatusMismatch &&
		d.InternalStatus == string(domain.StatusAuthorized) &&
		d.ProviderStatus == provider.TxnCaptured
}

// classify compares the internal rows (by provider_payment_id) against the
// provider transactions and returns the mismatches. It emits at most one
// discrepancy per charge; when both amount and status differ, amount wins (fix
// the amount first, then a follow-up run catches any residual status drift).
func classify(internal map[string]domain.ReconRow, txns []provider.Transaction) []domain.Discrepancy {
	var out []domain.Discrepancy
	seen := make(map[string]bool, len(txns))

	for _, tx := range txns {
		if tx.ProviderPaymentID == "" {
			continue // a provider row with no id can't be matched; skip rather than mis-flag
		}
		seen[tx.ProviderPaymentID] = true
		row, ok := internal[tx.ProviderPaymentID]
		if !ok {
			out = append(out, domain.Discrepancy{
				ProviderPaymentID: tx.ProviderPaymentID,
				Class:             domain.DiscrepancyMissingInternal,
				ProviderAmount:    tx.AmountMinor,
				ProviderStatus:    tx.Status,
				Detail:            "provider has a charge with no matching payment",
			})
			continue
		}
		if row.AmountMinor != tx.AmountMinor {
			out = append(out, domain.Discrepancy{
				ProviderPaymentID: tx.ProviderPaymentID,
				Class:             domain.DiscrepancyAmountMismatch,
				InternalAmount:    row.AmountMinor,
				ProviderAmount:    tx.AmountMinor,
				InternalStatus:    string(row.Status),
				ProviderStatus:    tx.Status,
				Detail:            fmt.Sprintf("amount differs: internal %d vs provider %d minor units", row.AmountMinor, tx.AmountMinor),
			})
			continue
		}
		if !statusReconciled(row, tx.Status) {
			out = append(out, domain.Discrepancy{
				ProviderPaymentID: tx.ProviderPaymentID,
				Class:             domain.DiscrepancyStatusMismatch,
				InternalAmount:    row.AmountMinor,
				ProviderAmount:    tx.AmountMinor,
				InternalStatus:    string(row.Status),
				ProviderStatus:    tx.Status,
				Detail:            fmt.Sprintf("status differs: internal %q vs provider %q", row.Status, tx.Status),
			})
		}
	}

	for id, row := range internal {
		if !seen[id] {
			out = append(out, domain.Discrepancy{
				ProviderPaymentID: id,
				Class:             domain.DiscrepancyMissingProvider,
				InternalAmount:    row.AmountMinor,
				InternalStatus:    string(row.Status),
				Detail:            "payment has no matching provider charge",
			})
		}
	}
	return out
}

// statusReconciled reports whether an internal row's status and the provider's
// status are an expected pairing (not drift). Beyond exact equality it accepts
// two known cross-vocabulary cases so normal operation doesn't flood the report:
//
//   - internal "expired": a hold that lapsed on our TTL is not voided at the
//     provider, so the provider still shows it authorized (or voided, if a later
//     void raced). Expected — not drift.
//   - internal "captured" with a recorded partial refund (RefundedMinor > 0):
//     the provider reports the charge "refunded" once any refund lands, while we
//     keep it "captured" until fully refunded. A captured row with NO recorded
//     refund vs a provider "refunded" is still real drift and is flagged.
//
// Comparing refund *amounts* is out of scope for v1: the provider ledger reports
// only a refunded flag, not a refunded amount, so net-refund drift can't be
// reconciled until the provider exposes it. See the reconciliation doc.
func statusReconciled(row domain.ReconRow, providerStatus string) bool {
	if string(row.Status) == providerStatus {
		return true
	}
	switch row.Status { //nolint:exhaustive // only the two cross-vocabulary cases need special handling; default covers the rest
	case domain.StatusExpired:
		return providerStatus == provider.TxnAuthorized || providerStatus == provider.TxnVoided
	case domain.StatusCaptured:
		return providerStatus == provider.TxnRefunded && row.RefundedMinor > 0
	default:
		return false
	}
}
