package v1

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
)

// Authorization outcomes (bounded).
const (
	authAuthorized = "authorized"
	authDeclined   = "declined"
	authError      = "error"
)

// Operation names and outcomes (bounded).
const (
	opCapture = "capture"
	opVoid    = "void"
	opRefund  = "refund"

	resultOK       = "ok"
	resultRejected = "rejected"
	resultError    = "error"
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
