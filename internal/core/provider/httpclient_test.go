package provider_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/internal/core/provider"
	"github.com/duynhlab/payment-service/internal/mockpay"
)

// newClient spins the real mockpay handler behind httptest and points an
// HTTPClient at it, so each test exercises both the client mapping and the
// server behaviour end to end (no Docker).
func newClient(t *testing.T) *provider.HTTPClient {
	t.Helper()
	ts := httptest.NewServer(mockpay.New(zap.NewNop()).Handler())
	t.Cleanup(ts.Close)
	return provider.NewHTTPClient(ts.URL)
}

func TestHTTPClient_ChargeAutoCapture(t *testing.T) {
	c := newClient(t)
	got, err := c.Charge(context.Background(), provider.ChargeRequest{
		IdempotencyKey: "k1", AmountMinor: 5000, Currency: "USD", PaymentMethod: "tok_visa", AutoCapture: true,
	})
	if err != nil {
		t.Fatalf("charge: %v", err)
	}
	if got.ProviderPaymentID == "" || !got.Captured {
		t.Fatalf("want captured charge with id, got %+v", got)
	}
}

func TestHTTPClient_ChargeReplaysPerKey(t *testing.T) {
	c := newClient(t)
	first, err := c.Charge(context.Background(), provider.ChargeRequest{IdempotencyKey: "dup", AmountMinor: 4000, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Charge(context.Background(), provider.ChargeRequest{IdempotencyKey: "dup", AmountMinor: 4000, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	if first.ProviderPaymentID != second.ProviderPaymentID {
		t.Fatalf("same key must replay same id: %s vs %s", first.ProviderPaymentID, second.ProviderPaymentID)
	}
}

func TestHTTPClient_ChargeDeclines(t *testing.T) {
	c := newClient(t)
	tests := []struct {
		name   string
		amount int64
		code   string
	}{
		{"generic", 1002, provider.DeclineGeneric},
		{"insufficient", 1095, provider.DeclineInsufficient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := c.Charge(context.Background(), provider.ChargeRequest{
				IdempotencyKey: tt.name, AmountMinor: tt.amount, Currency: "USD",
			})
			var declined *provider.DeclinedError
			if !errors.As(err, &declined) {
				t.Fatalf("want DeclinedError, got %v", err)
			}
			if declined.Code != tt.code {
				t.Fatalf("want code %s, got %s", tt.code, declined.Code)
			}
		})
	}
}

func TestHTTPClient_ChargeTransientThenSucceeds(t *testing.T) {
	c := newClient(t)
	req := provider.ChargeRequest{IdempotencyKey: "tk", AmountMinor: 2019, Currency: "USD"}
	if _, err := c.Charge(context.Background(), req); !errors.Is(err, provider.ErrTransient) {
		t.Fatalf("first attempt must be transient, got %v", err)
	}
	got, err := c.Charge(context.Background(), req)
	if err != nil {
		t.Fatalf("retry with same key must succeed, got %v", err)
	}
	if got.ProviderPaymentID == "" {
		t.Fatal("retry should mint a charge")
	}
}

func TestHTTPClient_CaptureVoidRefund(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	charge, err := c.Charge(ctx, provider.ChargeRequest{IdempotencyKey: "cap", AmountMinor: 7000, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Capture(ctx, charge.ProviderPaymentID); err != nil {
		t.Fatalf("capture: %v", err)
	}
	refundID, err := c.Refund(ctx, charge.ProviderPaymentID, 3000, "rk")
	if err != nil {
		t.Fatalf("refund: %v", err)
	}
	if refundID == "" {
		t.Fatal("refund must return an id")
	}
	// Refund replays per key.
	again, err := c.Refund(ctx, charge.ProviderPaymentID, 3000, "rk")
	if err != nil || again != refundID {
		t.Fatalf("refund replay: got %s err=%v", again, err)
	}
}

func TestHTTPClient_VoidReleasesHold(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	charge, err := c.Charge(ctx, provider.ChargeRequest{IdempotencyKey: "v", AmountMinor: 6000, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Void(ctx, charge.ProviderPaymentID); err != nil {
		t.Fatalf("void: %v", err)
	}
	// A voided hold no longer exists — capture must fail.
	if err := c.Capture(ctx, charge.ProviderPaymentID); err == nil {
		t.Fatal("capture of a voided hold must fail")
	}
}

func TestHTTPClient_UnknownChargeErrors(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	if err := c.Capture(ctx, "mp_nope"); err == nil {
		t.Fatal("capture of unknown charge must error")
	}
	if _, err := c.Refund(ctx, "mp_nope", 100, "x"); err == nil {
		t.Fatal("refund of unknown charge must error")
	}
	if err := c.Void(ctx, "mp_nope"); err == nil {
		t.Fatal("void of unknown charge must error")
	}
}

// Void must be idempotent: a lost 200 makes the caller roll back and retry, and
// the second void must succeed (not 404) or the void can never complete.
func TestHTTPClient_VoidIsIdempotent(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	charge, err := c.Charge(ctx, provider.ChargeRequest{IdempotencyKey: "vi", AmountMinor: 6000, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Void(ctx, charge.ProviderPaymentID); err != nil {
		t.Fatalf("first void: %v", err)
	}
	if err := c.Void(ctx, charge.ProviderPaymentID); err != nil {
		t.Fatalf("second void must be a no-op, got %v", err)
	}
}

// A transport failure (provider unreachable) must surface a non-DeclinedError
// error so the caller treats it as transient and re-drives (safe via per-key
// replay) rather than as a decline.
func TestHTTPClient_TransportErrorIsRetryable(t *testing.T) {
	ts := httptest.NewServer(mockpay.New(zap.NewNop()).Handler())
	ts.Close() // nothing is listening now
	c := provider.NewHTTPClient(ts.URL)

	_, err := c.Charge(context.Background(), provider.ChargeRequest{IdempotencyKey: "down", AmountMinor: 5000, Currency: "USD"})
	if err == nil {
		t.Fatal("transport failure must error")
	}
	var declined *provider.DeclinedError
	if errors.As(err, &declined) {
		t.Fatalf("transport failure must not be a decline, got %v", err)
	}
}

// An unexpected status (500) is likewise not a decline — the caller retries.
func TestHTTPClient_UnexpectedStatusIsError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(ts.Close)
	c := provider.NewHTTPClient(ts.URL)

	_, err := c.Charge(context.Background(), provider.ChargeRequest{IdempotencyKey: "e500", AmountMinor: 5000, Currency: "USD"})
	if err == nil {
		t.Fatal("500 must error")
	}
	var declined *provider.DeclinedError
	if errors.As(err, &declined) {
		t.Fatalf("500 must not be a decline, got %v", err)
	}
}
