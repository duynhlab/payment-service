package v1

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// Business metrics for payment, answering the on-call questions that matter for
// money movement:
//  1. What is the authorization decline rate?          → authorization{result,currency}
//  2. Are captures/voids/refunds failing at the provider? → operation{op,result}
//  3. Is the ledger drifting from the provider?         → reconciliation discrepancies{kind}
//
// Instruments ride the global OTel MeterProvider that obsx.SetupObservability
// installs (RFC-0014 OTLP pipeline → collector → VictoriaMetrics). Before that
// setup the global provider is a no-op, so package-init here is safe. Names are
// OTel-style; the collector renders them as payment_authorization_total,
// payment_operation_total, payment_reconciliation_discrepancies_total.
//
// Labels are bounded to enumerable domain values (RFC-0017 D-9): no ids, no
// free-form provider text, no amounts.
var (
	meter = otel.Meter("payment-service")

	authorizationCounter, _ = meter.Int64Counter("payment.authorization.total",
		metric.WithDescription("Payment authorization attempts by outcome (decline-rate KPI)"))
	operationCounter, _ = meter.Int64Counter("payment.operation.total",
		metric.WithDescription("Money-lifecycle operations (capture/void/refund) by outcome"))
	reconDiscrepancyCounter, _ = meter.Int64Counter("payment.reconciliation.discrepancies.total",
		metric.WithDescription("Ledger-vs-provider discrepancies found per reconciliation run, by kind"))
	providerUnknownCounter, _ = meter.Int64Counter("payment.provider.unknown.total",
		metric.WithDescription("Provider calls that returned no verdict, by operation — each one leaves an intent in doubt until it is resolved"))
	attemptWriteFailureCounter, _ = meter.Int64Counter("payment.attempt.write_failures.total",
		metric.WithDescription("Attempt rows that could not be written, by operation — the money state is still correct, but its evidence is missing"))
	keyReleaseFailureCounter, _ = meter.Int64Counter("payment.idempotency.release_failures.total",
		metric.WithDescription("Idempotency keys that could not be unlocked after a failed attempt; each one delays a caller's same-key retry until the takeover window"))
	attemptResolutionCounter, _ = meter.Int64Counter("payment.attempt.resolution.total",
		metric.WithDescription("Re-drives of an open UNKNOWN attempt, by operation and the class the provider answered with — `UNKNOWN` here means the doubt survived the round-trip"))
	sweepFailureCounter, _ = meter.Int64Counter("payment.doubt.sweep_failures.total",
		metric.WithDescription("Worklist entries the background sweep could not even attempt, by operation — doubt that nothing is currently working on"))
)

// recordSweepFailure counts a worklist entry the sweep could not act on at all
// (its payment or refund would not load, or it carries no key to replay under).
// Distinct from a resolution that ran and learned nothing: that one is progress,
// this one means the automatic path is not running for that row.
func recordSweepFailure(ctx context.Context, op string) {
	sweepFailureCounter.Add(ctx, 1, metric.WithAttributes(attribute.String(labelOperation, op)))
}

// ObserveDoubtBacklog registers the gauges that make unresolved doubt visible
// without anyone querying for it: how many questions are open, and how old the
// oldest one is.
//
// The AGE is the alertable one. A handful of open attempts at any moment is
// normal — every provider timeout creates one — while an attempt still open an
// hour later means the escape is not working for that payment, and money is
// sitting somewhere nobody has looked. Count alone cannot tell those apart.
func ObserveDoubtBacklog(count func(context.Context) (int64, error), oldest func(context.Context) (time.Duration, error)) error {
	open, err := meter.Int64ObservableGauge("payment.doubt.open",
		metric.WithDescription("Provider round-trips whose outcome is still unknown and unresolved"))
	if err != nil {
		return err
	}
	age, err := meter.Float64ObservableGauge("payment.doubt.oldest_age_seconds",
		metric.WithDescription("Age of the oldest unresolved provider outcome; the escalation signal, since one fresh unknown is routine and an old one is not"))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		n, cerr := count(ctx)
		d, aerr := oldest(ctx)
		if cerr != nil || aerr != nil {
			// Returning an error here drops EVERY metric this process would have
			// exported for the cycle, not just these two (the periodic reader aborts
			// the whole collection). Observe nothing and let the gauges go stale
			// instead — a stale gauge is a visible symptom, a blank scrape is a
			// mystery.
			return nil //nolint:nilerr // deliberate: one failed read must not blank the export
		}
		o.ObserveInt64(open, n)
		o.ObserveFloat64(age, d.Seconds())
		return nil
	}, open, age)
	return err
}

// recordResolution counts one attempt at closing existing doubt. It is the
// counterpart to providerUnknownCounter: doubt created versus doubt settled, and
// the two rates together are what say whether the worklist is draining. An
// outcome of UNKNOWN is counted too — a resolution that learned nothing is the
// interesting case, not an absence of data.
func recordResolution(ctx context.Context, op string, class domain.OutcomeClass) {
	attemptResolutionCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String(labelOperation, op),
		attribute.String("outcome_class", string(class)),
	))
}

// recordProviderUnknown counts a provider call that answered nothing. Separate
// from the operation counter's `unknown` result because this is the signal an
// alert watches: a rising rate means doubt is being created faster than it is
// resolved, which no per-outcome ratio makes obvious.
func recordProviderUnknown(ctx context.Context, op string) {
	providerUnknownCounter.Add(ctx, 1, metric.WithAttributes(attribute.String(labelOperation, op)))
}

// recordAttemptWriteFailure counts a lost attempt row. Deliberately visible: the
// attempt log is what makes an unknown outcome resolvable, so silently losing
// rows would leave doubt that nothing can close.
func recordAttemptWriteFailure(ctx context.Context, op string) {
	attemptWriteFailureCounter.Add(ctx, 1, metric.WithAttributes(attribute.String(labelOperation, op)))
}

// recordKeyReleaseFailure counts a key left locked after a failed attempt. The
// caller is told to retry immediately, so a rising count means those retries are
// bouncing off ErrLocked instead — invisible without this counter.
func recordKeyReleaseFailure(ctx context.Context) {
	keyReleaseFailureCounter.Add(ctx, 1)
}

// labelOperation is the attribute key every per-operation instrument shares.
const labelOperation = "operation"

// Authorization outcomes (bounded).
const (
	authAuthorized = "authorized"
	authDeclined   = "declined"
	authError      = "error"
)

// Operation names and outcomes (bounded).
const (
	// opAuthorize labels the charge round-trip. The authorization COUNTER is
	// separate (it counts terminal decisions per payment); this label is for the
	// per-round-trip signals — unknown outcomes and lost attempt rows.
	opAuthorize = "authorize"
	opCapture   = "capture"
	opVoid      = "void"
	opRefund    = "refund"

	resultOK       = "ok"
	resultRejected = "rejected"
	resultError    = "error"
	// A failed money operation splits by whether the provider DECIDED. The two
	// need opposite responses — a decline is parked, an unknown outcome is
	// retried and then resolved — so they cannot share one label.
	resultDeclined = "declined"
	resultUnknown  = "unknown"
)

// recordAuthorization counts one authorization attempt with its outcome and
// currency. Called once per real charge drive (idempotent replays return before
// the provider call, so this never double-counts a payment).
func recordAuthorization(ctx context.Context, result, currency string) {
	authorizationCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("result", result),
		attribute.String("currency", currency),
	))
}

// knownCurrencies bounds the currency label to a fixed allowlist. IsCurrency
// only checks the 3-uppercase-letter shape, so without this a client could mint
// up to 26^3 distinct label values. Any well-formed but unlisted code maps to
// "other" — the payment still stores its real currency; only the metric label
// is capped.
var knownCurrencies = map[string]struct{}{
	"USD": {}, "EUR": {}, "GBP": {}, "JPY": {}, "AUD": {},
	"CAD": {}, "CHF": {}, "CNY": {}, "SGD": {}, "VND": {},
}

func currencyLabel(c string) string {
	if _, ok := knownCurrencies[c]; ok {
		return c
	}
	return "other"
}

// recordOperation counts one money-lifecycle operation outcome. Idempotent
// no-ops (already in the target state) are not counted — only real transitions.
// resultError here means a PROVIDER failure (capture/void/refund rejected by
// mockpay); internal/persistence failures are not counted on this business
// counter — they surface via the otelpgx DB span and pool error signals.
func recordOperation(ctx context.Context, op, result string) {
	operationCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("op", op),
		attribute.String("result", result),
	))
}

// recordReconDiscrepancies counts discrepancies found in one reconciliation run,
// grouped by kind. This is a per-run DETECTION count: a standing un-healed
// discrepancy is re-counted on every scheduled run, so read it as a detection
// rate (rate()/per-run), not a cumulative sum of distinct drifts.
func recordReconDiscrepancies(ctx context.Context, kind string, n int64) {
	if n <= 0 {
		return
	}
	reconDiscrepancyCounter.Add(ctx, n, metric.WithAttributes(attribute.String("kind", kind)))
}
