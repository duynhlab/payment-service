package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	logicv1 "github.com/duynhlab/payment-service/internal/logic/v1"
	"github.com/duynhlab/payment-service/internal/webhooksig"
)

const testWebhookSecret = "whsec_test"

type fakeWebhookProcessor struct {
	result     logicv1.WebhookResult
	err        error
	called     bool
	gotEventID string
}

func (f *fakeWebhookProcessor) Process(_ context.Context, eventID, _, _ string) (logicv1.WebhookResult, error) {
	f.called = true
	f.gotEventID = eventID
	return f.result, f.err
}

func newWebhookRouter(p webhookProcessor) *gin.Engine {
	r := gin.New()
	RegisterWebhookRoutes(r, NewWebhookHandler(p, testWebhookSecret))
	return r
}

// postWebhook signs body with sigTime (zero → now) and posts it.
func postWebhook(r *gin.Engine, body string, sig string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/payment/v1/public/payments/webhooks/mockpay", strings.NewReader(body))
	req.Header.Set("Mockpay-Signature", sig)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func TestWebhook_ValidSignatureProcessed(t *testing.T) {
	p := &fakeWebhookProcessor{result: logicv1.WebhookResult{Status: "processed"}}
	r := newWebhookRouter(p)
	body := `{"event_id":"evt_1","type":"charge.captured","provider_payment_id":"mp_1"}`
	sig := webhooksig.Sign(testWebhookSecret, time.Now(), []byte(body))

	rec := postWebhook(r, body, sig)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if !p.called || p.gotEventID != "evt_1" {
		t.Fatalf("processor not called with the event: called=%v id=%q", p.called, p.gotEventID)
	}
}

func TestWebhook_RejectsBadSignature(t *testing.T) {
	p := &fakeWebhookProcessor{}
	r := newWebhookRouter(p)
	body := `{"event_id":"evt_1"}`

	tests := []struct {
		name string
		sig  string
	}{
		{"wrong secret", webhooksig.Sign("other", time.Now(), []byte(body))},
		{"tampered body", webhooksig.Sign(testWebhookSecret, time.Now(), []byte(`{"event_id":"evt_x"}`))},
		{"stale", webhooksig.Sign(testWebhookSecret, time.Now().Add(-10*time.Minute), []byte(body))},
		{"missing header", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := postWebhook(r, body, tt.sig)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("want 401, got %d", rec.Code)
			}
			if p.called {
				t.Fatal("processor must not run on a bad signature")
			}
		})
	}
}

func TestWebhook_MalformedOrNoEventIDIsAcked(t *testing.T) {
	tests := map[string]string{
		"bad json":    `{not json`,
		"no event_id": `{"type":"charge.captured"}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			p := &fakeWebhookProcessor{}
			r := newWebhookRouter(p)
			sig := webhooksig.Sign(testWebhookSecret, time.Now(), []byte(body))
			rec := postWebhook(r, body, sig)
			if rec.Code != http.StatusOK {
				t.Fatalf("signed-but-unusable must ack 200, got %d", rec.Code)
			}
			if p.called {
				t.Fatal("processor must not run without an event_id")
			}
		})
	}
}

func TestWebhook_DuplicateIsAcked(t *testing.T) {
	p := &fakeWebhookProcessor{result: logicv1.WebhookResult{Status: "processed", Duplicate: true}}
	r := newWebhookRouter(p)
	body := `{"event_id":"evt_dup","type":"charge.captured"}`
	sig := webhooksig.Sign(testWebhookSecret, time.Now(), []byte(body))

	rec := postWebhook(r, body, sig)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["duplicate"] != true {
		t.Fatalf("duplicate must be surfaced: %v", resp)
	}
}

func TestWebhook_OversizedBodyRejected(t *testing.T) {
	p := &fakeWebhookProcessor{}
	r := newWebhookRouter(p)
	big := strings.Repeat("a", (1<<20)+1) // just over the 1 MiB cap
	sig := webhooksig.Sign(testWebhookSecret, time.Now(), []byte(big))

	rec := postWebhook(r, big, sig)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body must be 413, got %d", rec.Code)
	}
	if p.called {
		t.Fatal("oversized body must not reach the processor")
	}
}

func TestWebhook_ProcessorErrorIsRetryable(t *testing.T) {
	p := &fakeWebhookProcessor{err: context.DeadlineExceeded}
	r := newWebhookRouter(p)
	body := `{"event_id":"evt_err","type":"charge.captured"}`
	sig := webhooksig.Sign(testWebhookSecret, time.Now(), []byte(body))

	rec := postWebhook(r, body, sig)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("infra error must be non-2xx (retryable), got %d", rec.Code)
	}
}

// TestWebhook_DeprecatedAliasMounted locks the expand phase of the v3 path
// migration (homelab ADR-017): the pre-v3 webhook path stays mounted until
// the contract release removes it.
func TestWebhook_DeprecatedAliasMounted(t *testing.T) {
	r := newWebhookRouter(&fakeWebhookProcessor{result: logicv1.WebhookResult{Status: "processed"}})
	req := httptest.NewRequest(http.MethodPost, "/payment/v1/public/webhooks/mockpay", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code == http.StatusNotFound {
		t.Errorf("deprecated webhook alias not mounted (got 404)")
	}
}
