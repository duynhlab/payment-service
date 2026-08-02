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
	"time"

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
		if st, _ := post(t, base+"/charges", req); st != http.StatusTooManyRequests {
			t.Fatalf("first attempt status %d, want 429 — the trigger models a refused request, not a maybe-processed one", st)
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

// A key identifies ONE operation on ONE charge. Reusing it elsewhere must
// conflict rather than borrow that operation's verdict, which would hide the
// caller's key-derivation bug behind a plausible success. Note what is NOT
// claimed: the answer is never replayed from memory — capture and void are
// idempotent against the charge's CURRENT state, and a remembered answer could
// contradict it (a capture success replayed after a void would report money that
// is no longer collectable).
func TestServer_MutationKeyBindsOneOperationToOneCharge(t *testing.T) {
	base := newServer(t)
	_, b1 := post(t, base+"/charges", provider.ChargeRequest{IdempotencyKey: "m1", AmountMinor: 7000, Currency: "USD"})
	id1 := decodeCharge(t, b1).ProviderPaymentID
	_, b2 := post(t, base+"/charges", provider.ChargeRequest{IdempotencyKey: "m2", AmountMinor: 8000, Currency: "USD"})
	id2 := decodeCharge(t, b2).ProviderPaymentID

	key := provider.MutationRequest{IdempotencyKey: "capture:payment:1"}
	if st, _ := post(t, base+"/charges/"+id1+"/capture", key); st != http.StatusOK {
		t.Fatalf("first capture status %d", st)
	}
	// Same key, same operation, same charge: still fine (idempotent by state).
	if st, _ := post(t, base+"/charges/"+id1+"/capture", key); st != http.StatusOK {
		t.Fatalf("same-key repeat status = %d, want 200", st)
	}
	// Same key, other charge → conflict.
	if st, body := post(t, base+"/charges/"+id2+"/capture", key); st != http.StatusConflict {
		t.Fatalf("key reuse on another charge = %d %s, want 409", st, body)
	}
	// Same key, same charge, OTHER operation → conflict too. Without the
	// operation in the binding this void would silently do nothing and answer 200.
	if st, body := post(t, base+"/charges/"+id1+"/void", key); st != http.StatusConflict {
		t.Fatalf("key reuse for another operation = %d %s, want 409", st, body)
	}
	// Its own key voids normally.
	if st, _ := post(t, base+"/charges/"+id1+"/void", provider.MutationRequest{IdempotencyKey: "void:payment:1"}); st != http.StatusOK {
		t.Fatalf("void with its own key status %d", st)
	}
	// And the charge really is voided — the capture key did not shield it.
	if got := transactionStatus(t, base, id1); got != "voided" {
		t.Fatalf("charge %s status = %q, want voided", id1, got)
	}
}

// A body that is present but undecodable must be a 400, not a silent
// downgrade to keyless: the situation this mechanism exists for is a request
// whose connection died mid-flight, which is exactly when a truncated body
// arrives. Answering 200 there would disable the protection precisely when it
// is needed.
func TestServer_MalformedMutationBodyIsRejected(t *testing.T) {
	base := newServer(t)
	_, b := post(t, base+"/charges", provider.ChargeRequest{IdempotencyKey: "mb", AmountMinor: 5000, Currency: "USD"})
	id := decodeCharge(t, b).ProviderPaymentID

	resp, err := http.Post(base+"/charges/"+id+"/capture", "application/json",
		strings.NewReader(`{"idempotency_key":"capture:payment`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("truncated body status = %d, want 400", resp.StatusCode)
	}

	// No body at all is a legacy caller and still works.
	resp2, err := http.Post(base+"/charges/"+id+"/capture", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("no-body capture status = %d, want 200", resp2.StatusCode)
	}
}

// The no-answer trigger must create the charge and THEN withhold the response:
// a lost answer with no effect is a different (easier) case than a lost answer
// with one. The test uses a short client timeout rather than waiting out the
// server's delay.
func TestServer_NoAnswerCreatesTheChargeThenGoesSilent(t *testing.T) {
	base := newServer(t)
	hc := &http.Client{Timeout: 900 * time.Millisecond}
	body, _ := json.Marshal(provider.ChargeRequest{IdempotencyKey: "na", AmountMinor: 5013, Currency: "USD", AutoCapture: true})

	_, err := hc.Post(base+"/charges", "application/json", bytes.NewReader(body))
	if err == nil {
		t.Fatal("the …13 trigger must not answer within the client timeout")
	}

	// The charge exists provider-side: the transactions feed lists it, which is
	// exactly how reconciliation would discover the orphan.
	resp, gerr := http.Get(base + "/transactions")
	if gerr != nil {
		t.Fatalf("transactions: %v", gerr)
	}
	defer func() { _ = resp.Body.Close() }()
	tb, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(tb), "mp_1") {
		t.Fatalf("the silent charge must still exist provider-side, got %s", tb)
	}
}

// A refund amount ending in the refund-decline suffix is refused with 402 and a
// machine code, so the client can type it as a DECIDED answer. The suffix is
// refund-only on purpose: the saga refunds the amount it charged, so sharing a
// charge-decline suffix would make "charge succeeded, refund refused"
// unreachable.
func TestServer_RefundDeclineHasItsOwnSuffix(t *testing.T) {
	base := newServer(t)
	// 5007 ends in 07 (refund decline) and NOT in a charge-decline suffix, so the
	// charge itself must succeed.
	_, body := post(t, base+"/charges", provider.ChargeRequest{IdempotencyKey: "cd", AmountMinor: 5007, Currency: "USD"})
	id := decodeCharge(t, body).ProviderPaymentID
	if id == "" {
		t.Fatal("a …07 charge must succeed — the suffix is refund-only")
	}
	if st, _ := post(t, base+"/charges/"+id+"/capture", nil); st != http.StatusOK {
		t.Fatalf("capture status %d", st)
	}

	st, rb := post(t, base+"/refunds", provider.RefundRequest{ProviderPaymentID: id, AmountMinor: 5007, IdempotencyKey: "rk-dec"})
	if st != http.StatusPaymentRequired {
		t.Fatalf("refund status = %d, want 402", st)
	}
	if !strings.Contains(string(rb), "refund_declined") {
		t.Fatalf("decline body = %s, want the refund_declined code", rb)
	}
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

// transactionStatus reads one charge's status from the provider's own ledger
// feed — the same source reconciliation reads, so an assertion here is about
// what the provider actually holds rather than what a handler replied.
func transactionStatus(t *testing.T, base, chargeID string) string {
	t.Helper()
	resp, err := http.Get(base + "/transactions")
	if err != nil {
		t.Fatalf("transactions: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var page provider.TransactionsPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		t.Fatalf("decode transactions: %v", err)
	}
	for _, tx := range page.Transactions {
		if tx.ProviderPaymentID == chargeID {
			return tx.Status
		}
	}
	return ""
}
