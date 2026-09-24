package v1

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/duynhlab/pkg/logger/slogx"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// Catalog events for the money decisions this service stores (RFC-0031 §
// Event catalog). Each is written next to the business counter for the same
// decision and only for a decided or unknown outcome: a request refused before
// the provider (an FSM rejection) or refused by it without acting (a
// retryable failure) stored no decision, so it writes no event. Amounts and
// provider text are review-class and never carried; the ids are
// correlation-only.
const (
	eventAuthorization = "payment.authorization.completed"
	eventCapture       = "payment.capture.completed"
	eventRefund        = "payment.refund.completed"
	eventDiscrepancy   = "payment.reconciliation.discrepancy.detected"

	outcomeAuthorized = "authorized"
	outcomeSucceeded  = "succeeded"
	outcomeDeclined   = "declined"
	outcomeUnknown    = "unknown"
)

func idAttr(key string, id int64) slog.Attr { return slog.String(key, strconv.FormatInt(id, 10)) }

// emitAuthorization writes payment.authorization.completed; outcome is
// authorized, declined or unknown.
func emitAuthorization(ctx context.Context, pay *domain.Payment, outcome string) {
	attrs := []slog.Attr{idAttr("payment.id", pay.ID), slog.String("outcome", outcome)}
	if pay.OrderID != nil {
		attrs = append(attrs, idAttr("order.id", *pay.OrderID))
	}
	slogx.FromContext(ctx).Event(ctx, slog.LevelInfo, eventAuthorization, "authorization completed", attrs...)
}

// emitCapture writes payment.capture.completed; outcome is succeeded,
// declined or unknown.
func emitCapture(ctx context.Context, paymentID int64, outcome string) {
	slogx.FromContext(ctx).Event(ctx, slog.LevelInfo, eventCapture, "capture completed",
		idAttr("payment.id", paymentID), slog.String("outcome", outcome))
}

// emitRefund writes payment.refund.completed; outcome is succeeded, declined
// or unknown.
func emitRefund(ctx context.Context, paymentID, refundID int64, outcome string) {
	slogx.FromContext(ctx).Event(ctx, slog.LevelInfo, eventRefund, "refund completed",
		idAttr("payment.id", paymentID), idAttr("refund.id", refundID), slog.String("outcome", outcome))
}

// emitDiscrepancies writes one payment.reconciliation.discrepancy.detected per
// class a run found, at Warn: drift between the ledger and the provider is
// something an operator acts on. The class is a bounded enum.
func emitDiscrepancies(ctx context.Context, runID int64, byClass map[domain.DiscrepancyClass]int64) {
	for class := range byClass {
		slogx.FromContext(ctx).Event(ctx, slog.LevelWarn, eventDiscrepancy, "reconciliation discrepancy detected",
			idAttr("reconciliation.run_id", runID), slog.String("discrepancy.class", string(class)))
	}
}
