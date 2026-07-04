package domain

// Reconciliation compares the payment service's own record of a charge (the
// `payments` table + ledger) against the provider's record (mockpay's
// GET /transactions), matched by provider_payment_id — the shared identifier
// stored on every payment at authorize time. Two independent systems always
// drift eventually (a lost webhook, a crash between a local commit and the
// provider confirm); a reconciliation job is how a real payment platform
// detects that drift instead of discovering it from a customer complaint.
//
// v1 is **detect-only**: every mismatch is recorded and surfaced, never
// auto-corrected. Automatically moving money (posting correcting ledger entries,
// converging state) is deferred until the detector is trusted — see the
// reconciliation doc and ADR.

// DiscrepancyClass names a payment↔provider mismatch. The four classes are
// exhaustive over "does each side have the record, and do the records agree?".
type DiscrepancyClass string

const (
	// DiscrepancyMissingInternal: the provider has a charge we have no payment
	// for. Should be impossible (we create the payment before charging), so it
	// signals a bug or data loss.
	DiscrepancyMissingInternal DiscrepancyClass = "missing_internal"
	// DiscrepancyMissingProvider: we have a payment the provider never recorded.
	// Usually a transient in-flight authorize; a real gap once past the
	// settlement-lag window.
	DiscrepancyMissingProvider DiscrepancyClass = "missing_provider"
	// DiscrepancyAmountMismatch: both sides have the charge but the amounts differ.
	DiscrepancyAmountMismatch DiscrepancyClass = "amount_mismatch"
	// DiscrepancyStatusMismatch: both sides have the charge but the states differ
	// (e.g. we still show authorized while the provider shows captured — the
	// crash-between-commit-and-confirm window from ADR-007).
	DiscrepancyStatusMismatch DiscrepancyClass = "status_mismatch"
)

// ReconRunStatus is the lifecycle of a single reconciliation run.
type ReconRunStatus string

const (
	ReconRunRunning   ReconRunStatus = "running"
	ReconRunCompleted ReconRunStatus = "completed"
	ReconRunFailed    ReconRunStatus = "failed"
)

// ReconRow is the internal projection of a payment that reconciliation compares
// against the provider ledger. Only payments that already have a
// provider_payment_id participate (an unauthorized payment has no provider record
// to reconcile against yet).
type ReconRow struct {
	ProviderPaymentID string
	AmountMinor       int64
	// RefundedMinor is the sum of applied refunds. It distinguishes a benign
	// partially-refunded capture (internal stays "captured" while the provider
	// reports "refunded") from a genuine missed refund (captured, nothing
	// refunded internally, yet the provider shows refunded).
	RefundedMinor int64
	Status        Status
}

// Discrepancy is one detected payment↔provider mismatch. In v1 it is persisted
// to reconciliation_discrepancies and surfaced via the internal API + metrics,
// and nothing else — no side effect on money.
type Discrepancy struct {
	ProviderPaymentID string
	Class             DiscrepancyClass
	InternalAmount    int64  // 0 when the internal side is absent
	ProviderAmount    int64  // 0 when the provider side is absent
	InternalStatus    string // "" when the internal side is absent
	ProviderStatus    string // "" when the provider side is absent
	Detail            string // human-readable summary for the report
}
