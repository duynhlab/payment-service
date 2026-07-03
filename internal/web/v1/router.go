// Package v1 holds the payment service web layer (Gin handlers + routing).
// It translates HTTP to the logic layer's Service API and maps its error
// sentinels to the shared httpx error envelope — no business rules here.
package v1

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
	logicv1 "github.com/duynhlab/payment-service/internal/logic/v1"
	"github.com/duynhlab/payment-service/middleware"
	"github.com/duynhlab/pkg/authmw"
	"github.com/duynhlab/pkg/httpx"
)

// errAuthRequired is the response message when a request lacks a valid user.
const errAuthRequired = "Authentication required"

// paymentLogic is the slice of the logic layer the web layer calls.
// *logicv1.Service satisfies it; kept as an interface so handlers are testable.
type paymentLogic interface {
	CreateIntent(ctx context.Context, idemKey string, in logicv1.CreateIntentInput) (*logicv1.IntentResult, error)
	Get(ctx context.Context, id, userID int64) (*domain.Payment, error)
	List(ctx context.Context, userID int64, page, pageSize int) ([]domain.Payment, int, error)
	CreateRefund(ctx context.Context, idemKey string, paymentID, userID, amountMinor int64, reason string) (*domain.Refund, bool, error)
}

// Handler is the payment web-layer handler.
type Handler struct {
	logic paymentLogic
}

// NewHandler creates a payment handler with dependency injection.
func NewHandler(logic paymentLogic) *Handler {
	return &Handler{logic: logic}
}

// RegisterRoutes mounts the payment v1 routes on the engine. Private routes
// carry the edge JWT gate; internal routes are cluster-only (NetworkPolicy is
// the fence) and run with user scope 0.
func RegisterRoutes(r *gin.Engine, h *Handler, verifier *authmw.Verifier) {
	h.mount(r, authmw.MiddlewareJWT(verifier))
}

// mount registers the routes with the given JWT middleware on the private
// group. Split from RegisterRoutes so tests can inject a fake auth middleware.
func (h *Handler) mount(r *gin.Engine, jwtMW gin.HandlerFunc) {
	private := r.Group("/payment/v1/private")
	private.Use(jwtMW)
	{
		private.POST("/payments", h.CreatePayment)
		private.GET("/payments", h.ListPayments)
		private.GET("/payments/:id", h.GetPayment)
	}

	internal := r.Group("/payment/v1/internal")
	{
		internal.POST("/payments/:id/refunds", h.CreateRefund)
	}
}

// beginRequest starts the web request span and resolves the request logger.
// The caller owns the span and must defer span.End().
func beginRequest(c *gin.Context) (context.Context, trace.Span, *zap.Logger) {
	ctx, span := middleware.StartSpan(c.Request.Context(), "http.request", trace.WithAttributes(
		attribute.String("layer", "web"),
		attribute.String("method", c.Request.Method),
		attribute.String("path", c.Request.URL.Path),
	))
	return ctx, span, middleware.GetLoggerFromGinContext(c)
}

// beginAuthed starts the request span and resolves the authenticated user id
// from the JWT claims set by authmw — never from the request body. On missing
// or non-numeric claims it writes 401, ends the span, and returns ok=false
// (the caller must return immediately). On success the caller owns the span
// and must defer span.End().
func beginAuthed(c *gin.Context, op string) (context.Context, trace.Span, *zap.Logger, int64, bool) {
	ctx, span, zapLogger := beginRequest(c)
	userID, err := strconv.ParseInt(c.GetString(authmw.CtxUserID), 10, 64)
	if err != nil || userID <= 0 {
		zapLogger.Warn(op + ": no valid user_id in context")
		httpx.RespondError(c, http.StatusUnauthorized, httpx.CodeUnauthorized, errAuthRequired)
		span.End()
		return ctx, span, zapLogger, 0, false
	}
	return ctx, span, zapLogger, userID, true
}

// requireIdempotencyKey reads the mandatory Idempotency-Key header, writing
// the 400 envelope (and returning ok=false) when it is missing.
func requireIdempotencyKey(c *gin.Context) (string, bool) {
	key := c.GetHeader("Idempotency-Key")
	if key == "" {
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeIdempotencyKeyRequired,
			"Idempotency-Key header is required")
		return "", false
	}
	return key, true
}

// pathID parses the numeric :id path param, writing the 400 envelope (and
// returning ok=false) when it is not a positive integer.
func pathID(c *gin.Context) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, "id must be a positive integer")
		return 0, false
	}
	return id, true
}

// translateError maps logic/core error sentinels to the HTTP error envelope.
// Anything unrecognized becomes an opaque 500 — internals never leak.
func translateError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		httpx.RespondError(c, http.StatusNotFound, httpx.CodeNotFound, "Payment not found")
	case errors.Is(err, domain.ErrPaymentExists):
		httpx.RespondError(c, http.StatusConflict, httpx.CodePaymentExists, "Order already has a payment")
	case errors.Is(err, domain.ErrKeyConflict):
		httpx.RespondError(c, http.StatusConflict, httpx.CodeIdempotencyConflict,
			"Idempotency-Key reused with a different request")
	case errors.Is(err, domain.ErrKeyLocked):
		c.Header("Retry-After", "1")
		httpx.RespondError(c, http.StatusConflict, httpx.CodeIdempotencyConflict, "in flight")
	case errors.Is(err, domain.ErrRefundRejected):
		httpx.RespondError(c, http.StatusConflict, httpx.CodeRefundExceedsCapture,
			"Refund rejected: not refundable or exceeds captured amount")
	case errors.Is(err, domain.ErrInvalidTransition):
		httpx.RespondError(c, http.StatusConflict, httpx.CodeInvalidTransition, "Invalid payment state transition")
	case errors.Is(err, provider.ErrTransient):
		httpx.RespondError(c, http.StatusServiceUnavailable, httpx.CodeInternal, "provider unavailable, retry")
	default:
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, "Internal server error")
	}
}

// createPaymentRequest is the POST /payments body. user_id is never accepted
// from the client — it comes from the JWT claims.
type createPaymentRequest struct {
	OrderID       *int64 `json:"order_id"`
	AmountMinor   int64  `json:"amount_minor"`
	Currency      string `json:"currency"`
	CaptureMethod string `json:"capture_method"`
	PaymentMethod string `json:"payment_method"`
}

// toInput validates the request, applies defaults (USD, manual capture), and
// builds the logic-layer input. A non-empty message means 400 VALIDATION_ERROR.
func (r *createPaymentRequest) toInput(userID int64) (logicv1.CreateIntentInput, string) {
	var in logicv1.CreateIntentInput
	if r.AmountMinor <= 0 {
		return in, "amount_minor must be a positive integer (minor units)"
	}
	if r.AmountMinor > maxAmountMinor {
		return in, "amount_minor exceeds the maximum accepted amount"
	}
	currency := r.Currency
	if currency == "" {
		currency = "USD"
	}
	if !isCurrency(currency) {
		return in, "currency must be a 3-letter uppercase code (e.g. USD)"
	}
	capture := domain.CaptureMethod(r.CaptureMethod)
	if capture == "" {
		capture = domain.CaptureManual
	}
	if capture != domain.CaptureManual && capture != domain.CaptureAutomatic {
		return in, `capture_method must be "manual" or "automatic"`
	}
	if !isTestToken(r.PaymentMethod) {
		return in, `payment_method must be an opaque "tok_" token (letters/digits/_, max 64 chars, no card-number-like digit runs)`
	}
	return logicv1.CreateIntentInput{
		UserID:        userID,
		OrderID:       r.OrderID,
		AmountMinor:   r.AmountMinor,
		Currency:      currency,
		CaptureMethod: capture,
		PaymentMethod: r.PaymentMethod,
	}, ""
}

// maxAmountMinor caps a single payment at $100M (in minor units) — a sanity
// ceiling that keeps absurd values out of the ledger, metrics, and the
// bigint refund arithmetic. Reject rather than clamp.
const (
	maxAmountMinor    int64 = 100_000_000_00 // $100M ceiling per payment (minor units)
	logFieldPaymentID       = "payment_id"
)

// isCurrency reports whether s is exactly three uppercase ASCII letters.
func isCurrency(s string) bool {
	if len(s) != 3 {
		return false
	}
	for _, r := range s {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

// isTestToken enforces PCI discipline on payment_method: a short opaque token
// (`tok_` + [A-Za-z0-9_], ≤ 64 chars) with no card-number-like digit run.
// Real payment tokens are short IDs; a pasted PAN must never be accepted,
// stored, or echoed.
func isTestToken(s string) bool {
	if !strings.HasPrefix(s, "tok_") || len(s) > 64 {
		return false
	}
	// Count TOTAL digits, not the longest contiguous run: separators like `_`
	// must not let a grouped PAN ("tok_4111_1111_1111_1111") slip through.
	// Real opaque tokens are short alnum IDs, not digit-dense.
	digits := 0
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			digits++
			if digits >= 12 { // a card number is 13–19 digits
				return false
			}
		case (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_':
			// allowed
		default:
			return false
		}
	}
	return true
}

// CreatePayment handles POST /payment/v1/private/payments — the idempotent
// authorize (and optionally capture) flow. 201 on success, 422 with the
// PAYMENT_DECLINED envelope when the provider declines.
func (h *Handler) CreatePayment(c *gin.Context) {
	ctx, span, zapLogger, userID, ok := beginAuthed(c, "CreatePayment")
	if !ok {
		return
	}
	defer span.End()

	idemKey, ok := requireIdempotencyKey(c)
	if !ok {
		return
	}

	var req createPaymentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		span.RecordError(err)
		zapLogger.Warn("CreatePayment: invalid request body", zap.Error(err))
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, "Invalid request body")
		return
	}
	in, msg := req.toInput(userID)
	if msg != "" {
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, msg)
		return
	}

	result, err := h.logic.CreateIntent(ctx, idemKey, in)
	if err != nil {
		span.RecordError(err)
		zapLogger.Error("CreatePayment: create intent failed", zap.Error(err))
		translateError(c, err)
		return
	}

	zapLogger.Info("Payment intent processed",
		zap.Int64(logFieldPaymentID, result.Payment.ID),
		zap.Int("code", result.Code),
		zap.Bool("replayed", result.Replayed),
	)
	if result.Code == http.StatusUnprocessableEntity {
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"error":   "payment declined",
			"code":    httpx.CodePaymentDeclined,
			"payment": result.Payment,
		})
		return
	}
	c.JSON(result.Code, result.Payment)
}

// GetPayment handles GET /payment/v1/private/payments/:id — owner-scoped.
func (h *Handler) GetPayment(c *gin.Context) {
	ctx, span, zapLogger, userID, ok := beginAuthed(c, "GetPayment")
	if !ok {
		return
	}
	defer span.End()

	id, ok := pathID(c)
	if !ok {
		return
	}
	span.SetAttributes(attribute.Int64("payment.id", id))

	pay, err := h.logic.Get(ctx, id, userID)
	if err != nil {
		span.RecordError(err)
		zapLogger.Warn("GetPayment: lookup failed", zap.Int64(logFieldPaymentID, id), zap.Error(err))
		translateError(c, err)
		return
	}
	c.JSON(http.StatusOK, pay)
}

// ListPayments handles GET /payment/v1/private/payments with the standard
// page/page_size query params and pagination envelope.
func (h *Handler) ListPayments(c *gin.Context) {
	ctx, span, zapLogger, userID, ok := beginAuthed(c, "ListPayments")
	if !ok {
		return
	}
	defer span.End()

	page, pageSize := httpx.ParsePage(c)
	items, total, err := h.logic.List(ctx, userID, page, pageSize)
	if err != nil {
		span.RecordError(err)
		zapLogger.Error("ListPayments: list failed", zap.Error(err))
		translateError(c, err)
		return
	}
	c.JSON(http.StatusOK, httpx.NewPaginated(items, page, pageSize, total))
}

// createRefundRequest is the POST /payments/:id/refunds body.
type createRefundRequest struct {
	AmountMinor int64  `json:"amount_minor"`
	Reason      string `json:"reason"`
}

// CreateRefund handles POST /payment/v1/internal/payments/:id/refunds — the
// idempotent (partial) refund flow. Internal audience: no JWT (the route is
// cluster-only behind NetworkPolicy), so lookups run unscoped (user 0).
func (h *Handler) CreateRefund(c *gin.Context) {
	ctx, span, zapLogger := beginRequest(c)
	defer span.End()

	idemKey, ok := requireIdempotencyKey(c)
	if !ok {
		return
	}
	paymentID, ok := pathID(c)
	if !ok {
		return
	}
	span.SetAttributes(attribute.Int64("payment.id", paymentID))

	var req createRefundRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		span.RecordError(err)
		zapLogger.Warn("CreateRefund: invalid request body", zap.Error(err))
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, "Invalid request body")
		return
	}
	if req.AmountMinor <= 0 || req.AmountMinor > maxAmountMinor {
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation,
			"amount_minor must be a positive integer within the accepted range (minor units)")
		return
	}

	ref, replayed, err := h.logic.CreateRefund(ctx, idemKey, paymentID, 0, req.AmountMinor, req.Reason)
	if err != nil {
		span.RecordError(err)
		zapLogger.Error("CreateRefund: refund failed", zap.Int64(logFieldPaymentID, paymentID), zap.Error(err))
		translateError(c, err)
		return
	}

	zapLogger.Info("Refund created",
		zap.Int64("refund_id", ref.ID),
		zap.Int64(logFieldPaymentID, paymentID),
		zap.Bool("replayed", replayed),
	)
	c.JSON(http.StatusCreated, ref)
}
