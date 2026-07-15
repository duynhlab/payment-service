package v1

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collectCounter reads name into an attribute→value map keyed by one label.
func collectCounter(t *testing.T, reader sdkmetric.Reader, name, label string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want Sum[int64]", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				v, _ := dp.Attributes.Value(attribute.Key(label))
				out[v.AsString()] = dp.Value
			}
		}
	}
	return out
}

// forceReDrive puts a claimed+checkpointed key back into a re-drivable state
// (never Finished, lock gone stale) so the next CreateIntent takes the
// crash-recovery takeover path through driveCharge.
func (f *fakeIdem) forceReDrive(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if k := f.keys[key]; k != nil {
		k.ResponseCode = nil
		k.ResponseBody = nil
		k.LockedAt = time.Unix(0, 0)
	}
}

// TestAuthorizationMetric drives the three authorization outcomes, then the two
// exactly-once hazards (idempotent replay and crash-recovery re-drive), all on
// one service and one MeterProvider — the OTel global delegate is first-wins,
// so a single provider install per test binary is required, and the cumulative
// counter is asserted at each step.
func TestAuthorizationMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	svc, _, fi, _ := newTestService()
	ctx := context.Background()

	if _, err := svc.CreateIntent(ctx, "auth", intent(2000)); err != nil { // …00 authorized
		t.Fatalf("authorize: %v", err)
	}
	if _, err := svc.CreateIntent(ctx, "decl", intent(2002)); err != nil { // …02 declined
		t.Fatalf("decline: %v", err)
	}
	if _, err := svc.CreateIntent(ctx, "err", intent(2019)); err == nil { // …19 transient
		t.Fatal("expected a transient error")
	}

	got := collectCounter(t, reader, "payment.authorization.total", "result")
	for result, want := range map[string]int64{"authorized": 1, "declined": 1, "error": 1} {
		if got[result] != want {
			t.Errorf("authorization{result=%s} = %d, want %d", result, got[result], want)
		}
	}

	// Idempotent replay: CreateIntent returns the cached result before
	// driveCharge, so the counter must not increment.
	if _, err := svc.CreateIntent(ctx, "auth", intent(2000)); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got := collectCounter(t, reader, "payment.authorization.total", "result"); got["authorized"] != 1 {
		t.Errorf("authorized after replay = %d, want 1 (idempotent replay must not re-count)", got["authorized"])
	}

	// Crash-recovery takeover: a new payment authorizes (→2), then a re-drive of
	// the same key replays the charge and hits a tolerated stale transition —
	// which must NOT re-count.
	if _, err := svc.CreateIntent(ctx, "rdv", intent(2000)); err != nil {
		t.Fatalf("second authorize: %v", err)
	}
	if got := collectCounter(t, reader, "payment.authorization.total", "result"); got["authorized"] != 2 {
		t.Fatalf("authorized after second payment = %d, want 2", got["authorized"])
	}
	fi.forceReDrive("rdv")
	if _, err := svc.CreateIntent(ctx, "rdv", intent(2000)); err != nil {
		t.Fatalf("re-drive: %v", err)
	}
	if got := collectCounter(t, reader, "payment.authorization.total", "result"); got["authorized"] != 2 {
		t.Errorf("authorized after re-drive = %d, want 2 (takeover must not double-count)", got["authorized"])
	}
}

// TestCurrencyLabel_BoundedToAllowlist confirms unknown well-formed currencies
// collapse to "other" so the label domain stays bounded.
func TestCurrencyLabel_BoundedToAllowlist(t *testing.T) {
	if got := currencyLabel("USD"); got != "USD" {
		t.Errorf("currencyLabel(USD) = %q, want USD", got)
	}
	if got := currencyLabel("ZZZ"); got != "other" {
		t.Errorf("currencyLabel(ZZZ) = %q, want other", got)
	}
}
