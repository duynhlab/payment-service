package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/duynhlab/pkg/logger/slogx"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

func eventRecords(t *testing.T, emit func(ctx context.Context)) []map[string]any {
	t.Helper()
	buf := &bytes.Buffer{}
	ctx := slogx.WithContext(context.Background(), slogx.New(slogx.Config{Stdout: buf}))
	emit(ctx)
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("not JSON: %q", line)
		}
		out = append(out, m)
	}
	return out
}

func TestEmitAuthorization(t *testing.T) {
	order := int64(7)
	recs := eventRecords(t, func(ctx context.Context) {
		emitAuthorization(ctx, &domain.Payment{ID: 42, OrderID: &order}, outcomeAuthorized)
		emitAuthorization(ctx, &domain.Payment{ID: 43}, outcomeUnknown)
	})
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2", len(recs))
	}
	if r := recs[0]; r["event"] != eventAuthorization || r["payment.id"] != "42" || r["order.id"] != "7" ||
		r["outcome"] != "authorized" || r["level"] != "info" {
		t.Errorf("record = %v", r)
	}
	if _, has := recs[1]["order.id"]; has {
		t.Error("a payment with no order must not carry order.id")
	}
}

func TestEmitCaptureAndRefund(t *testing.T) {
	recs := eventRecords(t, func(ctx context.Context) {
		emitCapture(ctx, 42, outcomeDeclined)
		emitRefund(ctx, 42, 9, outcomeSucceeded)
	})
	if r := recs[0]; r["event"] != eventCapture || r["payment.id"] != "42" || r["outcome"] != "declined" {
		t.Errorf("capture = %v", r)
	}
	if r := recs[1]; r["event"] != eventRefund || r["refund.id"] != "9" || r["outcome"] != "succeeded" {
		t.Errorf("refund = %v", r)
	}
}

// One event per class found, at Warn: drift is something an operator acts on.
func TestEmitDiscrepancies(t *testing.T) {
	recs := eventRecords(t, func(ctx context.Context) {
		emitDiscrepancies(ctx, 5, map[domain.DiscrepancyClass]int64{"missing_in_ledger": 2, "amount_mismatch": 1})
	})
	if len(recs) != 2 {
		t.Fatalf("records = %d, want one per class", len(recs))
	}
	for _, r := range recs {
		if r["event"] != eventDiscrepancy || r["reconciliation.run_id"] != "5" || r["level"] != "warn" || r["discrepancy.class"] == nil {
			t.Errorf("record = %v", r)
		}
	}
}
