package v1

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/internal/core/provider"
	logicv1 "github.com/duynhlab/payment-service/internal/logic/v1"
	"github.com/duynhlab/payment-service/internal/webhooksig"
)

const (
	// webhookTolerance bounds the signed-timestamp age (replay window).
	webhookTolerance = 5 * time.Minute
	// maxWebhookBytes caps the webhook body read.
	maxWebhookBytes = 1 << 20

	keyError  = "error"
	keyStatus = "status"
)

// webhookProcessor is the logic-layer slice the webhook handler calls.
type webhookProcessor interface {
	Process(ctx context.Context, eventID, eventType, providerPaymentID string) (logicv1.WebhookResult, error)
}

// WebhookHandler receives signed provider webhooks on the public edge.
type WebhookHandler struct {
	processor webhookProcessor
	secret    string
}

// NewWebhookHandler wires the handler with the shared HMAC secret.
func NewWebhookHandler(processor webhookProcessor, secret string) *WebhookHandler {
	return &WebhookHandler{processor: processor, secret: secret}
}

// RegisterWebhookRoutes mounts the public webhook route. It carries no JWT — the
// HMAC signature is the credential; Kong lets it through anonymously.
func RegisterWebhookRoutes(r *gin.Engine, h *WebhookHandler) {
	r.POST("/payment/v1/public/payments/webhooks/mockpay", h.HandleMockpay)
	// Deprecated alias — pre-v3 path kept for one release during the rollout
	// (the mockpay emitter's MOCKPAY_WEBHOOK_URL flips to the new path in the
	// same release). Remove at contract; see homelab ADR-017.
	r.POST("/payment/v1/public/webhooks/mockpay", h.HandleMockpay)
}

// HandleMockpay verifies the signature over the raw body, then records the event
// idempotently. Contract: only a signature/timestamp failure (or an infra error
// we want retried) returns non-2xx; an authentic event — even for an unknown
// payment or type — is acked 2xx so the sender stops retrying.
func (h *WebhookHandler) HandleMockpay(c *gin.Context) {
	ctx, _, log := beginRequest(c)

	// MaxBytesReader rejects an oversized body with an error (rather than
	// silently truncating, which would fail HMAC as if the signature were bad).
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxWebhookBytes)
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{keyError: "body too large"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{keyError: "unreadable body"})
		return
	}

	// Verify over the RAW bytes — re-marshaling would change them and break HMAC.
	if err := webhooksig.Verify(h.secret, c.GetHeader("Mockpay-Signature"), raw, time.Now(), webhookTolerance); err != nil {
		log.Warn("webhook signature rejected", zap.Error(err))
		c.JSON(http.StatusUnauthorized, gin.H{keyError: "invalid signature"})
		return
	}

	var ev provider.WebhookEvent
	if err := json.Unmarshal(raw, &ev); err != nil || ev.EventID == "" {
		// Validly signed but unparseable / no event_id: a sender bug. Ack so it
		// is not retried forever; nothing to dedup on.
		log.Warn("webhook malformed after valid signature", zap.Error(err))
		c.JSON(http.StatusOK, gin.H{keyStatus: "ignored"})
		return
	}

	result, err := h.processor.Process(ctx, ev.EventID, ev.Type, ev.ProviderPaymentID)
	if err != nil {
		// Infra failure — return non-2xx so the sender retries.
		log.Error("webhook processing failed", zap.String("event_id", ev.EventID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{keyError: "processing failed"})
		return
	}

	log.Info("webhook received",
		zap.String("event_id", ev.EventID),
		zap.String("event_type", ev.Type),
		zap.String(keyStatus, result.Status),
		zap.Bool("duplicate", result.Duplicate),
	)
	c.JSON(http.StatusOK, gin.H{keyStatus: result.Status, "duplicate": result.Duplicate})
}
