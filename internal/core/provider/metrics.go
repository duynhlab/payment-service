package provider

import (
	"context"
	"time"

	"github.com/duynhlab/pkg/obsx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Provider-call telemetry (RFC-0017 W2 special surface). This is the money-hop
// SLI: how long the mockpay provider takes and how it resolves. Instruments
// ride the global OTel MeterProvider that obsx.SetupObservability installs; the
// collector renders the histogram as payment_provider_request_duration_seconds.
//
// Scope: the money operations only (charge/capture/void/refund). The
// reconciliation read (GetTransactions) is deliberately not timed here — it is
// paging, not a money-hop, and would dilute the SLI.
//
// Labels are bounded enums (RFC-0017 D-9): no provider ids, amounts, or tokens.
var (
	providerMeter = otel.Meter("payment-service")

	requestDuration, _ = providerMeter.Float64Histogram(
		"payment.provider.request.duration",
		metric.WithDescription("mockpay provider call duration by operation and outcome"),
		metric.WithUnit("s"),
		// Second-scale SLO buckets. obsx installs Views only for the named HTTP
		// instruments, so without this hint an unnamed histogram gets the SDK's
		// millisecond-scale default boundaries (0,5,…,10000) and every sub-5s
		// call collapses into bucket 0 — useless quantiles. Reuse the canonical
		// SLO set (matches http.server.request.duration).
		metric.WithExplicitBucketBoundaries(obsx.DurationBuckets...))
)

// Provider operations and outcomes (bounded).
const (
	opCharge  = "charge"
	opCapture = "capture"
	opVoid    = "void"
	opRefund  = "refund"

	outcomeOK        = "ok"
	outcomeDeclined  = "declined"  // provider rejected the card (charge only)
	outcomeTransient = "transient" // 503 / unexpected status / transport error — retryable
)

// recordProviderCall records one provider-call duration with its bounded op and
// outcome. Called via defer so every return path (incl. transport errors) is
// timed; the outcome variable is read at defer time.
func recordProviderCall(ctx context.Context, op, outcome string, start time.Time) {
	requestDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
		attribute.String("op", op),
		attribute.String("outcome", outcome),
	))
}
