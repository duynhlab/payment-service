package provider

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestHTTPClient_TraceparentAndDurationMetric proves the money-hop wiring: the
// otelhttp transport injects the W3C traceparent (so mockpay joins the caller's
// trace) and the provider-call histogram records the bounded op/outcome. Globals
// are set once (OTel global providers are first-wins per test binary).
func TestHTTPClient_TraceparentAndDurationMetric(t *testing.T) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	var gotTraceparent, gotBody string
	statusCh := make(chan int, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTraceparent = r.Header.Get("Traceparent")
		if b, _ := io.ReadAll(r.Body); len(b) > 0 {
			gotBody = string(b)
		}
		st := <-statusCh
		w.WriteHeader(st)
		if st == http.StatusOK {
			_, _ = w.Write([]byte(`{"provider_payment_id":"mp_1","provider_refund_id":"rf_1"}`))
		}
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL)
	ctx, span := otel.Tracer("test").Start(context.Background(), "parent")
	defer span.End()
	wantTID := span.SpanContext().TraceID().String()

	// One charge per outcome the histogram must distinguish.
	for _, st := range []int{http.StatusOK, http.StatusPaymentRequired, http.StatusServiceUnavailable} {
		statusCh <- st
		_, _ = c.Charge(ctx, ChargeRequest{IdempotencyKey: "idem-xyz", AmountMinor: 4200, Currency: "USD"})
	}
	// capture/void/refund happy paths (op labels + body integrity for refund).
	statusCh <- http.StatusOK
	_ = c.Capture(ctx, "mp_1")
	statusCh <- http.StatusOK
	_ = c.Void(ctx, "mp_1")
	statusCh <- http.StatusOK
	_, _ = c.Refund(ctx, "mp_1", 100, "refkey")

	if gotTraceparent == "" || !strings.Contains(gotTraceparent, wantTID) {
		t.Errorf("traceparent header = %q, want to contain trace id %s", gotTraceparent, wantTID)
	}
	// Body integrity: otelhttp must not corrupt/drop the request body. The last
	// body seen (refund) must carry the fields we sent.
	if !strings.Contains(gotBody, "refkey") || !strings.Contains(gotBody, "mp_1") {
		t.Errorf("request body not preserved through otelhttp transport: %q", gotBody)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	seen := map[string]bool{} // "op/outcome"
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "payment.provider.request.duration" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("%s is %T, want Histogram[float64]", m.Name, m.Data)
			}
			for _, dp := range h.DataPoints {
				op, _ := dp.Attributes.Value(attribute.Key("op"))
				out, _ := dp.Attributes.Value(attribute.Key("outcome"))
				seen[op.AsString()+"/"+out.AsString()] = true
			}
		}
	}
	for _, want := range []string{"charge/ok", "charge/declined", "charge/transient", "capture/ok", "void/ok", "refund/ok"} {
		if !seen[want] {
			t.Errorf("provider histogram missing %q (got %v)", want, seen)
		}
	}
}
