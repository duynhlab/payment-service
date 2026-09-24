package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/duynhlab/pkg/logger/slogx"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
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

// eventSeq captures the catalog events one scenario writes, in order.
func eventSeq(t *testing.T) (context.Context, func() []string) {
	t.Helper()
	buf := &bytes.Buffer{}
	ctx := slogx.WithContext(context.Background(), slogx.New(slogx.Config{Stdout: buf}))
	return ctx, func() []string {
		var out []string
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			var m map[string]any
			if line != "" && json.Unmarshal([]byte(line), &m) == nil && m["event"] != nil {
				out = append(out, fmt.Sprintf("%v:%v", m["event"], m["outcome"]))
			}
		}
		return out
	}
}

func TestEvents_OnePerStoredDecision(t *testing.T) {
	const (
		authOK     = eventAuthorization + ":authorized"
		capUnknown = eventCapture + ":unknown"
		capOK      = eventCapture + ":succeeded"
		capDecline = eventCapture + ":declined"
	)
	t.Run("authorize then capture", func(t *testing.T) {
		svc, _, _, _ := newDoubtService()
		ctx, events := eventSeq(t)
		res, err := svc.CreateIntent(ctx, "k-ev-1", intent(2000))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
			t.Fatal(err)
		}
		if got := events(); fmt.Sprint(got) != fmt.Sprint([]string{authOK, capOK}) {
			t.Errorf("events = %v", got)
		}
	})
	t.Run("unknown capture parks, then resolves", func(t *testing.T) {
		svc, _, prov, _ := newDoubtService()
		ctx, events := eventSeq(t)
		res, _ := svc.CreateIntent(ctx, "k-ev-2", intent(2000))
		prov.captureThenErr = context.DeadlineExceeded
		_, _ = svc.Capture(ctx, res.Payment.ID, "7")
		prov.captureThenErr = nil
		if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
			t.Fatal(err)
		}
		if got := events(); fmt.Sprint(got) != fmt.Sprint([]string{authOK, capUnknown, capOK}) {
			t.Errorf("events = %v, want the stored doubt then the resolved success", got)
		}
	})
	t.Run("definite refusal after a park", func(t *testing.T) {
		svc, _, prov, _ := newDoubtService()
		ctx, events := eventSeq(t)
		res, _ := svc.CreateIntent(ctx, "k-ev-3", intent(2000))
		prov.captureErrs = []error{context.DeadlineExceeded, fmt.Errorf("%w: no such charge", provider.ErrDefinite)}
		_, _ = svc.Capture(ctx, res.Payment.ID, "7")
		if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
			t.Fatal(err)
		}
		if got := events(); fmt.Sprint(got) != fmt.Sprint([]string{authOK, capUnknown, capDecline, capOK}) {
			t.Errorf("events = %v", got)
		}
	})
	t.Run("a retryable refusal stores no decision", func(t *testing.T) {
		svc, _, prov, _ := newDoubtService()
		ctx, events := eventSeq(t)
		res, _ := svc.CreateIntent(ctx, "k-ev-4", intent(2000))
		prov.captureErr = provider.ErrTransient
		_, _ = svc.Capture(ctx, res.Payment.ID, "7")
		if got := events(); fmt.Sprint(got) != fmt.Sprint([]string{authOK}) {
			t.Errorf("events = %v, want no capture event", got)
		}
	})
	t.Run("a resolver that lost the race writes nothing", func(t *testing.T) {
		svc, fp, prov, _ := newDoubtService()
		ctx, events := eventSeq(t)
		res, _ := svc.CreateIntent(ctx, "k-ev-5", intent(2000))
		prov.captureThenErr = context.DeadlineExceeded
		_, _ = svc.Capture(ctx, res.Payment.ID, "7")
		prov.captureThenErr = nil
		fp.beforeTransition = func() {
			fp.beforeTransition = nil
			_ = fp.TransitionStatus(ctx, res.Payment.ID, domain.StatusProcessing, domain.StatusCaptured, nil)
		}
		_, _ = svc.Capture(ctx, res.Payment.ID, "7")
		if got := events(); fmt.Sprint(got) != fmt.Sprint([]string{authOK, capUnknown}) {
			t.Errorf("events = %v, want no event from the losing resolver", got)
		}
	})
}
