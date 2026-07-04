package v1

import (
	"context"
	"fmt"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

const (
	// defaultReconPageSize is used when a caller passes a non-positive page size.
	defaultReconPageSize = 100
	// maxReconTransactions bounds one pass against a provider that returns full
	// pages indefinitely (a bug, or an inflated Total from a hostile provider).
	maxReconTransactions = 1_000_000
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
	FinishRun(ctx context.Context, runID int64, scanned, found int, status domain.ReconRunStatus) error
}

// Reconciler compares the internal payment record against the provider ledger
// and records the mismatches. v1 is detect-only: it never heals.
type Reconciler struct {
	repo   ReconRepo
	ledger ProviderLedger
}

// NewReconciler wires the reconciler onto its persistence port and the provider
// ledger it pages.
func NewReconciler(repo ReconRepo, ledger ProviderLedger) *Reconciler {
	return &Reconciler{repo: repo, ledger: ledger}
}

// Run performs one reconciliation pass: open a run, detect discrepancies, persist
// them, and close the run. Returns the run id and the number of discrepancies.
// A detection or persistence error marks the run failed and is returned.
func (r *Reconciler) Run(ctx context.Context, pageSize int) (runID int64, found int, err error) {
	runID, err = r.repo.CreateRun(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("create reconciliation run: %w", err)
	}

	discrepancies, scanned, err := r.detect(ctx, pageSize)
	if err != nil {
		_ = r.repo.FinishRun(ctx, runID, scanned, 0, domain.ReconRunFailed)
		return runID, 0, fmt.Errorf("detect discrepancies: %w", err)
	}

	if len(discrepancies) > 0 {
		if serr := r.repo.SaveDiscrepancies(ctx, runID, discrepancies); serr != nil {
			_ = r.repo.FinishRun(ctx, runID, scanned, 0, domain.ReconRunFailed)
			return runID, 0, fmt.Errorf("save discrepancies: %w", serr)
		}
	}

	if ferr := r.repo.FinishRun(ctx, runID, scanned, len(discrepancies), domain.ReconRunCompleted); ferr != nil {
		return runID, len(discrepancies), fmt.Errorf("finish reconciliation run: %w", ferr)
	}
	return runID, len(discrepancies), nil
}

// detect loads the internal rows, pages the full provider ledger, and classifies
// the two sides. Returns the discrepancies and the number of provider
// transactions scanned.
func (r *Reconciler) detect(ctx context.Context, pageSize int) ([]domain.Discrepancy, int, error) {
	rows, err := r.repo.ListReconcilable(ctx)
	if err != nil {
		return nil, 0, err
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
			return nil, 0, perr
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
			return nil, 0, fmt.Errorf("reconciliation aborted: provider returned more than %d transactions", maxReconTransactions)
		}
	}

	return classify(internal, txns), len(txns), nil
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
