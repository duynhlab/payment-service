package v1

import (
	"context"
	"errors"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

// The sweep's failure branches matter more than its happy path. Every one of
// them means an entry has no automatic escape and will age into the critical
// staleness alert, so each has to be counted rather than swallowed — a sweep
// that quietly skips work looks identical to a sweep with nothing to do.

// parkedCapture drives a payment to a capture park and returns it.
func parkedCapture(t *testing.T, svc *Service, prov *failingProvider, key string) *domain.Payment {
	t.Helper()
	ctx := context.Background()
	res, err := svc.CreateIntent(ctx, key, intent(2000))
	if err != nil {
		t.Fatal(err)
	}
	prov.captureThenErr = context.DeadlineExceeded
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("park err = %v, want ErrOutcomeUnknown", err)
	}
	prov.captureThenErr = nil
	return res.Payment
}

// A worklist the sweep cannot even read is not an empty worklist, and must not
// be reported as a clean pass.
func TestSweep_WorklistUnreadableIsAnError(t *testing.T) {
	svc, _, _, rec := newDoubtService()
	rec.err = errors.New("attempts table gone")

	if _, err := svc.ResolveOpenDoubt(context.Background(), 10); err == nil {
		t.Fatal("sweep reported success while it could not read the worklist")
	}
}

// A payment row that will not load leaves its entry untouched. The sweep counts
// it and moves on: one unreadable row must not stop the rest of the worklist.
func TestSweep_UnreadablePaymentSkipsTheEntryAndContinues(t *testing.T) {
	svc, fp, prov, rec := newDoubtService()
	ctx := context.Background()

	pay := parkedCapture(t, svc, prov, "k-unreadable")
	fp.findErr = errors.New("database went away")

	closed, err := svc.ResolveOpenDoubt(ctx, 10)
	if err != nil {
		t.Fatalf("sweep err = %v, want the pass to survive one bad row", err)
	}
	if closed != 0 {
		t.Fatalf("closed = %d, want 0", closed)
	}
	if open, _ := rec.ListOpenForPayment(ctx, pay.ID); len(open) != 1 {
		t.Fatalf("open attempts = %d, want the entry left intact", len(open))
	}
}

// A parked refund whose row will not load is skipped, not closed. Closing it
// would delete the only pointer to money that may already have left.
func TestSweep_UnreadableRefundLeavesTheEntryOpen(t *testing.T) {
	svc, fp, prov, rec := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-ref-unreadable", intent(2000))
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	prov.refundThenErr = context.DeadlineExceeded
	if _, _, err := svc.CreateRefund(ctx, "rk-unreadable", res.Payment.ID, "7", 500, ""); !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("park err = %v", err)
	}
	prov.refundThenErr = nil
	fp.findRefundErr = errors.New("database went away")

	if _, err := svc.ResolveOpenDoubt(ctx, 10); err != nil {
		t.Fatalf("sweep err = %v", err)
	}
	if open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID); len(open) != 1 {
		t.Fatalf("open attempts = %d, want the refund entry still open", len(open))
	}
}

// A refund somebody else already settled leaves a stale entry behind. There is
// nothing left to ask, so the sweep closes it without touching the provider.
func TestSweep_AlreadySettledRefundClosesWithoutAsking(t *testing.T) {
	svc, fp, prov, rec := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-ref-settled", intent(2000))
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	prov.refundThenErr = context.DeadlineExceeded
	if _, _, err := svc.CreateRefund(ctx, "rk-settled", res.Payment.ID, "7", 500, ""); !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("park err = %v", err)
	}
	// A caller's retry settles it while the entry is still open.
	if err := fp.SettleRefund(ctx, 1, domain.RefundSucceeded, "rf_out_of_band"); err != nil {
		t.Fatal(err)
	}
	prov.refundThenErr = nil
	before := len(prov.refundKeys)

	if _, err := svc.ResolveOpenDoubt(ctx, 10); err != nil {
		t.Fatalf("sweep err = %v", err)
	}
	if len(prov.refundKeys) != before {
		t.Fatal("the sweep re-sent a refund that was already settled")
	}
	if open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID); len(open) != 0 {
		t.Fatalf("open attempts = %d, want the stale entry closed", len(open))
	}
}

// A refund the provider definitively refuses is recorded `failed` — the reserve
// is released and the caller stops, because a decided no does not become a yes
// on retry.
func TestSweep_DefinitivelyRefusedRefundIsRecordedFailed(t *testing.T) {
	svc, fp, prov, _ := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-ref-refused", intent(2000))
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	prov.refundThenErr = context.DeadlineExceeded
	if _, _, err := svc.CreateRefund(ctx, "rk-refused", res.Payment.ID, "7", 500, ""); !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("park err = %v", err)
	}
	prov.refundThenErr = nil
	prov.refundErr = &provider.DeclinedError{Code: "refund_declined"}

	if _, err := svc.ResolveOpenDoubt(ctx, 10); err != nil {
		t.Fatalf("sweep err = %v", err)
	}
	ref, err := fp.FindRefundByID(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Status != domain.RefundFailed {
		t.Fatalf("refund status = %s, want failed", ref.Status)
	}
}

// A stray VOID question on a payment that is no longer parked is asked, not
// assumed. The sweep must not move the payment: the row already reflects a
// decision somebody else made.
func TestSweep_StrayVoidIsAskedAndTheRowIsLeftAlone(t *testing.T) {
	svc, fp, prov, rec := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-stray-void", intent(2000))
	prov.voidThenErr = context.DeadlineExceeded
	// The void's own CAS must succeed; only the PARK that follows it loses, which
	// is the state this test is about.
	var casCalls int
	fp.beforeTransition = func() {
		casCalls++
		if casCalls == 2 {
			fp.transitionErr = domain.ErrStaleTransition
		}
	}
	if _, err := svc.Void(ctx, res.Payment.ID, "7"); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("err = %v, want ErrOutcomeUnknown", err)
	}
	fp.beforeTransition, fp.transitionErr = nil, nil
	prov.voidThenErr = nil

	statusBefore, _ := svc.Get(ctx, res.Payment.ID, "7")
	if _, err := svc.ResolveOpenDoubt(ctx, 10); err != nil {
		t.Fatalf("sweep err = %v", err)
	}
	if prov.gotVoidKey == "" {
		t.Fatal("the sweep closed the question without asking the provider")
	}
	after, _ := svc.Get(ctx, res.Payment.ID, "7")
	if after.Status != statusBefore.Status {
		t.Fatalf("status moved %s -> %s; a stray answer must not overrule the row", statusBefore.Status, after.Status)
	}
	if open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID); len(open) != 0 {
		t.Fatalf("open attempts = %d, want 0 once the answer is known", len(open))
	}
}

// A stray question about a payment with no provider reference cannot be asked at
// all: there is nothing to ask about, and a key-based replay would risk creating
// the very charge we are unsure about.
func TestSweep_StrayAttemptWithNoProviderReferenceIsCounted(t *testing.T) {
	svc, fp, prov, rec := newDoubtService()
	ctx := context.Background()

	prov.chargeThenErr = context.DeadlineExceeded
	fp.transitionErr = domain.ErrStaleTransition // the park CAS loses; the row stays `pending`
	if _, err := svc.CreateIntent(ctx, "k-stray-noref", intent(2000)); !errors.Is(err, domain.ErrOutcomeUnknown) {
		t.Fatalf("err = %v, want ErrOutcomeUnknown", err)
	}
	fp.transitionErr = nil
	prov.chargeThenErr = nil
	callsBefore := prov.chargeCalls

	if _, err := svc.ResolveOpenDoubt(ctx, 10); err != nil {
		t.Fatalf("sweep err = %v", err)
	}
	if prov.chargeCalls != callsBefore {
		t.Fatal("the sweep charged again for a payment with no provider reference")
	}
	if open, _ := rec.ListOpenForPayment(ctx, 1); len(open) != 1 {
		t.Fatalf("open attempts = %d, want the entry left for a human", len(open))
	}
}

// TestObserveDoubtBacklog covers both halves of the gauge contract in one
// registration, because the OTel meter is shared across this binary and a second
// registration would leave the first callback live — making any "the gauge is
// absent" assertion meaningless.
//
// The halves: the backlog is visible without anyone querying for it, and a failed
// read does not take the rest of the export with it. Returning an error from the
// callback aborts the WHOLE collection cycle, so every other metric this process
// would have published disappears — a blank scrape is a mystery, a stale gauge is
// a symptom.
func TestObserveDoubtBacklog(t *testing.T) {
	reader := testReader
	var readFails bool

	if err := ObserveDoubtBacklog(
		func(context.Context) (int64, error) {
			if readFails {
				return 0, errors.New("database gone")
			}
			return 3, nil
		},
		func(context.Context) (time.Duration, error) { return 90 * time.Second, nil },
	); err != nil {
		t.Fatalf("register: %v", err)
	}

	got := collectGauges(t, reader)
	if got["payment.doubt.open"] != 3 {
		t.Errorf("payment.doubt.open = %v, want 3", got["payment.doubt.open"])
	}
	if got["payment.doubt.oldest_age_seconds"] != 90 {
		t.Errorf("payment.doubt.oldest_age_seconds = %v, want 90", got["payment.doubt.oldest_age_seconds"])
	}

	readFails = true
	// A counter published by the same meter stands in for "everything else".
	recordSweepFailure(context.Background(), opCapture)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect returned %v — one failed read blanked the cycle", err)
	}
	var sawCounter bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "payment.doubt.sweep_failures.total" {
				sawCounter = true
			}
		}
	}
	if !sawCounter {
		t.Error("an unrelated metric went missing when the gauge could not read")
	}
}

// collectGauges reads both doubt gauges out of one collection.
func collectGauges(t *testing.T, reader sdkmetric.Reader) map[string]float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					out[m.Name] = float64(dp.Value)
				}
			case metricdata.Gauge[float64]:
				for _, dp := range d.DataPoints {
					out[m.Name] = dp.Value
				}
			}
		}
	}
	return out
}

// Worklist rows the sweep cannot act on at all. Each is counted rather than
// closed: closing an entry the sweep never asked about deletes the only pointer
// to an unverified provider call, and counting is what makes the gap alertable.
func TestSweep_UnactionableEntriesAreLeftOpenAndCounted(t *testing.T) {
	ctx := context.Background()

	t.Run("a refund attempt with no refund id", func(t *testing.T) {
		svc, _, _, rec := newDoubtService()
		pay := capturedForSweep(t, svc, "k-noid")
		mustRecord(t, rec, domain.Attempt{
			PaymentID: pay.ID, Operation: domain.AttemptRefund,
			Outcome: domain.OutcomeUnknown, IdempotencyKey: "7:orphan",
		})
		assertStillOpen(t, ctx, svc, rec, pay.ID)
	})

	t.Run("a refund whose row records no key", func(t *testing.T) {
		svc, fp, _, rec := newDoubtService()
		pay := capturedForSweep(t, svc, "k-nokey")
		ref, err := fp.CreateRefund(ctx, pay.ID, 500, "", "") // no idempotency key
		if err != nil {
			t.Fatal(err)
		}
		if err := fp.SettleRefund(ctx, ref.ID, domain.RefundProcessing, ""); err != nil {
			t.Fatal(err)
		}
		mustRecord(t, rec, domain.Attempt{
			PaymentID: pay.ID, Operation: domain.AttemptRefund,
			Outcome: domain.OutcomeUnknown, RefundID: &ref.ID,
		})
		assertStillOpen(t, ctx, svc, rec, pay.ID)
	})

	t.Run("an authorize question about a payment that already has a reference", func(t *testing.T) {
		svc, _, prov, rec := newDoubtService()
		res, err := svc.CreateIntent(ctx, "k-authstray", intent(2000))
		if err != nil {
			t.Fatal(err)
		}
		mustRecord(t, rec, domain.Attempt{
			PaymentID: res.Payment.ID, Operation: domain.AttemptAuthorize,
			Outcome: domain.OutcomeUnknown, IdempotencyKey: "7:k-authstray",
		})
		before := prov.chargeCalls
		assertStillOpen(t, ctx, svc, rec, res.Payment.ID)
		if prov.chargeCalls != before {
			t.Fatal("the sweep charged again for a payment that already has a reference")
		}
	})
}

// A refund the provider is still silent about stays parked and stays on the
// worklist — the sweep never settles a question by declaring an outcome.
func TestSweep_SilentRefundStaysParked(t *testing.T) {
	svc, fp, prov, rec := newDoubtService()
	ctx := context.Background()

	res, _ := svc.CreateIntent(ctx, "k-ref-silent", intent(2000))
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	prov.refundThenErr = context.DeadlineExceeded
	if _, _, err := svc.CreateRefund(ctx, "rk-silent", res.Payment.ID, "7", 500, ""); !errors.Is(err, domain.ErrRefundNotSettled) {
		t.Fatalf("park err = %v", err)
	}
	// The provider is still silent when the sweep runs.
	if _, err := svc.ResolveOpenDoubt(ctx, 10); err != nil {
		t.Fatalf("sweep err = %v", err)
	}
	ref, err := fp.FindRefundByID(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Status != domain.RefundProcessing {
		t.Fatalf("refund status = %s, want it still parked", ref.Status)
	}
	if open, _ := rec.ListOpenForPayment(ctx, res.Payment.ID); len(open) != 1 {
		t.Fatalf("open attempts = %d, want exactly one — still one question", len(open))
	}
}

func capturedForSweep(t *testing.T, svc *Service, key string) *domain.Payment {
	t.Helper()
	ctx := context.Background()
	res, err := svc.CreateIntent(ctx, key, intent(2000))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Capture(ctx, res.Payment.ID, "7"); err != nil {
		t.Fatal(err)
	}
	return res.Payment
}

func mustRecord(t *testing.T, rec *recordingAttempts, a domain.Attempt) {
	t.Helper()
	if _, err := rec.Record(context.Background(), a); err != nil {
		t.Fatal(err)
	}
}

func assertStillOpen(t *testing.T, ctx context.Context, svc *Service, rec *recordingAttempts, paymentID int64) {
	t.Helper()
	if _, err := svc.ResolveOpenDoubt(ctx, 10); err != nil {
		t.Fatalf("sweep err = %v", err)
	}
	open, _ := rec.ListOpenForPayment(ctx, paymentID)
	if len(open) == 0 {
		t.Fatal("the sweep closed an entry it could not act on")
	}
}

// The reconciliation frontier's age is the "is it running at all?" signal, and it
// has to be a gauge for a specific reason: a reconciler that has STOPPED emits no
// runs, so a counter cannot tell stopped from quiet. Only something derived from
// stored state can.
func TestObserveReconciliationWatermark(t *testing.T) {
	reader := testReader
	var readFails bool

	if err := ObserveReconciliationWatermark(func(context.Context) (time.Duration, error) {
		if readFails {
			return 0, errors.New("watermark unreadable")
		}
		return 12 * time.Minute, nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	if got := collectGauges(t, reader)["payment.reconciliation.watermark_age_seconds"]; got != 720 {
		t.Errorf("watermark age = %v, want 720", got)
	}

	// Same rule as the doubt gauges: a failed read goes stale rather than taking
	// the whole export cycle down with it.
	readFails = true
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect returned %v — one failed read blanked the cycle", err)
	}
}
