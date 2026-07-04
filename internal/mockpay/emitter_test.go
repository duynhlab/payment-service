package mockpay

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/internal/core/provider"
	"github.com/duynhlab/payment-service/internal/webhooksig"
)

const emitterSecret = "whsec_emit"

// fastEmitter builds an emitter with a tiny backoff and no random duplication,
// pointed at url.
func fastEmitter(url string) *WebhookEmitter {
	e := NewWebhookEmitter(url, emitterSecret, zap.NewNop())
	e.baseDelay = time.Millisecond
	e.rnd = func() float64 { return 1.0 } // never duplicate
	return e
}

// sigVerifyingServer records request count and verifies each signature; the
// first failN responses are 503, the rest 200.
func sigVerifyingServer(t *testing.T, failN int32) (*httptest.Server, *int32) {
	t.Helper()
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := webhooksig.Verify(emitterSecret, r.Header.Get("Mockpay-Signature"), body, time.Now(), 5*time.Minute); err != nil {
			t.Errorf("emitted webhook must carry a valid signature: %v", err)
		}
		n := atomic.AddInt32(&count, 1)
		if n <= failN {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)
	return ts, &count
}

func TestEmitter_RetriesUntilAcked(t *testing.T) {
	ts, count := sigVerifyingServer(t, 2) // fail twice, then succeed
	fastEmitter(ts.URL).deliver(provider.WebhookEvent{EventID: "evt_1", Type: "charge.captured"})
	if got := atomic.LoadInt32(count); got != 3 {
		t.Fatalf("want 3 attempts (2 fail + 1 ok), got %d", got)
	}
}

func TestEmitter_GivesUpAfterMaxAttempts(t *testing.T) {
	ts, count := sigVerifyingServer(t, 99) // always fail
	fastEmitter(ts.URL).deliver(provider.WebhookEvent{EventID: "evt_2"})
	if got := atomic.LoadInt32(count); got != maxWebhookAttempts {
		t.Fatalf("want %d attempts then give up, got %d", maxWebhookAttempts, got)
	}
}

func TestEmitter_DropsWhenSaturated(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)

	e := fastEmitter(ts.URL)
	// Occupy every in-flight slot so the next dispatch finds none free.
	for i := 0; i < maxInFlightWebhooks; i++ {
		e.sem <- struct{}{}
	}
	e.Emit(provider.WebhookEvent{EventID: "evt_drop"})

	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt32(&count); got != 0 {
		t.Fatalf("saturated emitter must drop, not deliver (got %d)", got)
	}
}

func TestEmitter_EmitDuplicatesWhenRandBelowProb(t *testing.T) {
	received := make(chan string, 4)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := webhooksig.Verify(emitterSecret, r.Header.Get("Mockpay-Signature"), body, time.Now(), 5*time.Minute); err != nil {
			t.Errorf("bad signature: %v", err)
		}
		received <- "hit"
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)

	e := NewWebhookEmitter(ts.URL, emitterSecret, zap.NewNop())
	e.baseDelay = time.Millisecond
	e.rnd = func() float64 { return 0.0 } // always duplicate
	e.Emit(provider.WebhookEvent{EventID: "evt_dup", Type: "charge.captured"})

	// Expect two deliveries (original + duplicate) of the same event.
	for i := 0; i < 2; i++ {
		select {
		case <-received:
		case <-time.After(2 * time.Second):
			t.Fatalf("expected 2 deliveries, got %d", i)
		}
	}
}
