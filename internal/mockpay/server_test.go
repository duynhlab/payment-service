package mockpay_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/internal/core/provider"
	"github.com/duynhlab/payment-service/internal/mockpay"
)

// recordingEmitter captures the events the server emits (synchronously).
type recordingEmitter struct {
	mu     sync.Mutex
	events []provider.WebhookEvent
}

func (r *recordingEmitter) Emit(ev provider.WebhookEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recordingEmitter) snapshot() []provider.WebhookEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]provider.WebhookEvent(nil), r.events...)
}

func newServer(t *testing.T) string {
	t.Helper()
	ts := httptest.NewServer(mockpay.New(zap.NewNop(), nil).Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// post sends a JSON body (or nil) and returns status + raw body.
func post(t *testing.T, url string, body any) (int, []byte) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	resp, err := http.Post(url, "application/json", r)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func decodeCharge(t *testing.T, body []byte) provider.Charge {
	t.Helper()
	var c provider.Charge
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("decode charge: %v (%s)", err, body)
	}
	return c
}

func TestServer_Charge(t *testing.T) {
	base := newServer(t)

	t.Run("auto-capture success + replay", func(t *testing.T) {
		st, body := post(t, base+"/charges", provider.ChargeRequest{IdempotencyKey: "k1", AmountMinor: 5000, Currency: "USD", AutoCapture: true})
		if st != http.StatusOK {
			t.Fatalf("status %d", st)
		}
		c := decodeCharge(t, body)
		if c.ProviderPaymentID == "" || !c.Captured {
			t.Fatalf("bad charge %+v", c)
		}
		_, body2 := post(t, base+"/charges", provider.ChargeRequest{IdempotencyKey: "k1", AmountMinor: 5000, Currency: "USD", AutoCapture: true})
		if decodeCharge(t, body2).ProviderPaymentID != c.ProviderPaymentID {
			t.Fatal("replay must return the same id")
		}
	})

	t.Run("declines", func(t *testing.T) {
		for _, tc := range []struct {
			amount int64
			code   string
		}{{1002, provider.DeclineGeneric}, {1095, provider.DeclineInsufficient}} {
			st, body := post(t, base+"/charges", provider.ChargeRequest{AmountMinor: tc.amount, Currency: "USD"})
			if st != http.StatusPaymentRequired {
				t.Fatalf("amount %d: status %d", tc.amount, st)
			}
			var e provider.ErrorResponse
			_ = json.Unmarshal(body, &e)
			if e.Code != tc.code {
				t.Fatalf("amount %d: code %s", tc.amount, e.Code)
			}
		}
	})

	t.Run("transient then success", func(t *testing.T) {
		req := provider.ChargeRequest{IdempotencyKey: "tk", AmountMinor: 2019, Currency: "USD"}
		if st, _ := post(t, base+"/charges", req); st != http.StatusServiceUnavailable {
			t.Fatalf("first attempt status %d", st)
		}
		if st, _ := post(t, base+"/charges", req); st != http.StatusOK {
			t.Fatalf("retry status %d", st)
		}
	})

	t.Run("bad body", func(t *testing.T) {
		resp, err := http.Post(base+"/charges", "application/json", strings.NewReader("{not json"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("bad body status %d", resp.StatusCode)
		}
	})
}

func TestServer_CaptureVoidRefund(t *testing.T) {
	base := newServer(t)
	_, body := post(t, base+"/charges", provider.ChargeRequest{IdempotencyKey: "c", AmountMinor: 7000, Currency: "USD"})
	id := decodeCharge(t, body).ProviderPaymentID

	if st, _ := post(t, base+"/charges/"+id+"/capture", nil); st != http.StatusOK {
		t.Fatalf("capture status %d", st)
	}
	if st, _ := post(t, base+"/charges/mp_nope/capture", nil); st != http.StatusNotFound {
		t.Fatalf("unknown capture status %d", st)
	}

	// refund the captured charge + replay; unknown + bad body.
	st, rb := post(t, base+"/refunds", provider.RefundRequest{ProviderPaymentID: id, AmountMinor: 3000, IdempotencyKey: "rk"})
	if st != http.StatusOK {
		t.Fatalf("refund status %d", st)
	}
	var rr provider.RefundResponse
	_ = json.Unmarshal(rb, &rr)
	_, rb2 := post(t, base+"/refunds", provider.RefundRequest{ProviderPaymentID: id, AmountMinor: 3000, IdempotencyKey: "rk"})
	var rr2 provider.RefundResponse
	_ = json.Unmarshal(rb2, &rr2)
	if rr.ProviderRefundID == "" || rr.ProviderRefundID != rr2.ProviderRefundID {
		t.Fatalf("refund replay mismatch: %q vs %q", rr.ProviderRefundID, rr2.ProviderRefundID)
	}
	if st, _ := post(t, base+"/refunds", provider.RefundRequest{ProviderPaymentID: "mp_nope", AmountMinor: 1, IdempotencyKey: "x"}); st != http.StatusNotFound {
		t.Fatalf("unknown refund status %d", st)
	}
	resp, _ := http.Post(base+"/refunds", "application/json", strings.NewReader("{bad"))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad refund body status %d", resp.StatusCode)
	}
}

func TestServer_VoidIsIdempotent(t *testing.T) {
	base := newServer(t)
	_, body := post(t, base+"/charges", provider.ChargeRequest{IdempotencyKey: "v", AmountMinor: 6000, Currency: "USD"})
	id := decodeCharge(t, body).ProviderPaymentID

	if st, _ := post(t, base+"/charges/"+id+"/void", nil); st != http.StatusOK {
		t.Fatalf("first void status %d", st)
	}
	if st, _ := post(t, base+"/charges/"+id+"/void", nil); st != http.StatusOK {
		t.Fatalf("second void must be idempotent, status %d", st)
	}
	if st, _ := post(t, base+"/charges/mp_nope/void", nil); st != http.StatusNotFound {
		t.Fatalf("unknown void status %d", st)
	}
}

func TestServer_EmitsWebhooks(t *testing.T) {
	em := &recordingEmitter{}
	ts := httptest.NewServer(mockpay.New(zap.NewNop(), em).Handler())
	t.Cleanup(ts.Close)

	// auto-capture → charge.captured
	st, body := post(t, ts.URL+"/charges", provider.ChargeRequest{IdempotencyKey: "e1", AmountMinor: 5000, Currency: "USD", AutoCapture: true})
	if st != http.StatusOK {
		t.Fatalf("charge status %d", st)
	}
	id := decodeCharge(t, body).ProviderPaymentID

	// manual charge → charge.authorized
	post(t, ts.URL+"/charges", provider.ChargeRequest{IdempotencyKey: "e2", AmountMinor: 6000, Currency: "USD"})
	// refund the captured one → refund.succeeded
	post(t, ts.URL+"/refunds", provider.RefundRequest{ProviderPaymentID: id, AmountMinor: 1000, IdempotencyKey: "er"})

	events := em.snapshot()
	if len(events) != 3 {
		t.Fatalf("want 3 emitted events, got %d", len(events))
	}
	types := map[string]bool{}
	for _, e := range events {
		types[e.Type] = true
		if e.EventID == "" {
			t.Fatal("every emitted event needs an event_id")
		}
	}
	for _, want := range []string{"charge.captured", "charge.authorized", "refund.succeeded"} {
		if !types[want] {
			t.Fatalf("missing emitted type %q (got %v)", want, types)
		}
	}
}

func TestServer_Health(t *testing.T) {
	base := newServer(t)
	resp, err := http.Get(base + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status %d", resp.StatusCode)
	}
}
