package mockpay

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/internal/core/provider"
	"github.com/duynhlab/payment-service/internal/webhooksig"
)

// Emitter delivers a webhook for a money movement. The server calls it after
// charge/capture/void/refund; nil means emission is disabled (no WEBHOOK_URL).
type Emitter interface {
	Emit(ev provider.WebhookEvent)
}

const (
	maxWebhookAttempts  = 4
	webhookDupProb      = 0.10 // ~10% of events are deliberately delivered twice
	maxInFlightWebhooks = 64   // cap concurrent deliveries so a down receiver can't fan out unbounded goroutines
	fieldEventID        = "event_id"
)

// WebhookEmitter POSTs signed webhooks to a receiver. It deliberately models a
// real provider's imperfect delivery: at-least-once (retry with backoff on a
// non-2xx), occasional duplicates, and out-of-order arrival (each delivery runs
// in its own goroutine). The receiver must dedup by event_id.
type WebhookEmitter struct {
	url    string
	secret string
	client *http.Client
	logger *zap.Logger

	sem chan struct{} // bounds concurrent deliveries

	// injectable for tests
	baseDelay time.Duration
	now       func() time.Time
	rnd       func() float64
}

// NewWebhookEmitter builds an emitter targeting url, signing with secret.
func NewWebhookEmitter(url, secret string, logger *zap.Logger) *WebhookEmitter {
	return &WebhookEmitter{
		url:       url,
		secret:    secret,
		client:    &http.Client{Timeout: 5 * time.Second},
		logger:    logger,
		sem:       make(chan struct{}, maxInFlightWebhooks),
		baseDelay: 200 * time.Millisecond,
		now:       time.Now,
		rnd:       rand.Float64, //nolint:gosec // non-crypto: only picks which events to duplicate
	}
}

// Emit delivers ev asynchronously, duplicating it ~webhookDupProb of the time.
// Concurrent goroutines mean events can arrive out of order — exactly the
// at-least-once/unordered contract the receiver is built to tolerate.
func (e *WebhookEmitter) Emit(ev provider.WebhookEvent) {
	e.dispatch(ev)
	if e.rnd() < webhookDupProb {
		e.logger.Info("mockpay duplicating webhook", zap.String(fieldEventID, ev.EventID))
		e.dispatch(ev)
	}
}

// dispatch starts a bounded delivery. Acquiring the slot is non-blocking so it
// never stalls the request handler; if the in-flight cap is reached (a slow or
// down receiver) the event is dropped with a log rather than piling up
// goroutines — the receiver's dedup makes at-least-once losses tolerable.
func (e *WebhookEmitter) dispatch(ev provider.WebhookEvent) {
	select {
	case e.sem <- struct{}{}:
		go func() {
			defer func() { <-e.sem }()
			e.deliver(ev)
		}()
	default:
		e.logger.Warn("mockpay webhook dropped: delivery queue saturated", zap.String(fieldEventID, ev.EventID))
	}
}

// deliver retries with linear backoff until the receiver acks 2xx or attempts
// run out. Each attempt is freshly signed so the timestamp never goes stale.
func (e *WebhookEmitter) deliver(ev provider.WebhookEvent) {
	body, err := json.Marshal(ev)
	if err != nil {
		e.logger.Error("mockpay marshal webhook", zap.Error(err))
		return
	}
	for attempt := 1; attempt <= maxWebhookAttempts; attempt++ {
		if e.postOnce(body) {
			e.logger.Info("mockpay webhook delivered",
				zap.String(fieldEventID, ev.EventID), zap.Int("attempt", attempt))
			return
		}
		if attempt < maxWebhookAttempts {
			time.Sleep(time.Duration(attempt) * e.baseDelay)
		}
	}
	e.logger.Warn("mockpay webhook gave up", zap.String(fieldEventID, ev.EventID))
}

// postOnce signs and sends one delivery, returning true on a 2xx ack. The
// client's own timeout bounds the round trip.
func (e *WebhookEmitter) postOnce(body []byte) bool {
	sig := webhooksig.Sign(e.secret, e.now(), body)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Mockpay-Signature", sig)
	resp, err := e.client.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode/100 == 2
}
