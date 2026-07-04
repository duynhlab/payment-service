// Package mockpay is a standalone mock payment provider — the real network hop
// behind the payment service's provider port. It runs as the `mockpay`
// subcommand of the payment binary (mirroring the order-worker pattern: a second
// deployment of one image), so webhooks, latency, and reconciliation are honest
// lessons against a process that can fail independently.
//
// It honours the same deterministic magic-amount triggers as the in-memory Stub
// (via provider.Classify) and replays answers per idempotency key. Signed
// webhook emission and the paged transactions API land in later slices.
package mockpay

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/internal/core/provider"
)

// msgUnknownCharge is the error message for capture/void/refund against an id
// the mock has never issued (or already voided).
const msgUnknownCharge = "unknown charge"

// maxBodyBytes caps request bodies (tiny JSON) so a client cannot grow memory
// with a giant body.
const maxBodyBytes = 1 << 20 // 1 MiB

// Server is the in-memory mock provider. Safe for concurrent requests.
type Server struct {
	logger *zap.Logger

	mu            sync.Mutex
	seq           int64                      // provider_payment_id sequence
	refundSeq     int64                      // provider_refund_id sequence
	byKey         map[string]provider.Charge // charge idempotency replay
	captured      map[string]bool            // provider_payment_id -> captured (absent = voided/unknown)
	voided        map[string]bool            // voided ids — makes void idempotent under retry
	refundsByKey  map[string]string          // refund idempotency key -> provider_refund_id
	transientSeen map[string]bool            // charge keys that already hit the transient trigger once
}

// New builds an empty mock provider.
func New(logger *zap.Logger) *Server {
	return &Server{
		logger:        logger,
		byKey:         map[string]provider.Charge{},
		captured:      map[string]bool{},
		voided:        map[string]bool{},
		refundsByKey:  map[string]string{},
		transientSeen: map[string]bool{},
	}
}

// Handler wires the routes (Go 1.22+ method+wildcard patterns).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /charges", s.handleCharge)
	mux.HandleFunc("POST /charges/{id}/capture", s.handleCapture)
	mux.HandleFunc("POST /charges/{id}/void", s.handleVoid)
	mux.HandleFunc("POST /refunds", s.handleRefund)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, provider.ErrorResponse{Error: msg, Code: code})
}

// handleCharge places (and optionally captures) a hold. Declines and transient
// failures are driven by the amount's magic suffix; answers replay per key.
func (s *Server) handleCharge(w http.ResponseWriter, r *http.Request) {
	var req provider.ChargeRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "", "invalid charge request")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if c, ok := s.byKey[req.IdempotencyKey]; ok && req.IdempotencyKey != "" {
		writeJSON(w, http.StatusOK, c) // idempotent replay
		return
	}

	switch provider.Classify(req.AmountMinor) {
	case provider.OutcomeGenericDecline:
		writeError(w, http.StatusPaymentRequired, provider.DeclineGeneric, "card declined")
		return
	case provider.OutcomeInsufficient:
		writeError(w, http.StatusPaymentRequired, provider.DeclineInsufficient, "insufficient funds")
		return
	case provider.OutcomeTransient:
		if !s.transientSeen[req.IdempotencyKey] {
			s.transientSeen[req.IdempotencyKey] = true
			writeError(w, http.StatusServiceUnavailable, provider.DeclineProcessing, "processing error, retry")
			return
		}
		// second attempt with the same key falls through to success
	case provider.OutcomeOK:
	}

	s.seq++
	c := provider.Charge{ProviderPaymentID: fmt.Sprintf("mp_%d", s.seq), Captured: req.AutoCapture}
	if req.IdempotencyKey != "" {
		s.byKey[req.IdempotencyKey] = c
	}
	s.captured[c.ProviderPaymentID] = req.AutoCapture
	s.logger.Info("charge", zap.String("id", c.ProviderPaymentID),
		zap.Int64("amount_minor", req.AmountMinor), zap.Bool("captured", c.Captured))
	writeJSON(w, http.StatusOK, c)
}

func (s *Server) handleCapture(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.captured[id]; !ok {
		writeError(w, http.StatusNotFound, "", msgUnknownCharge)
		return
	}
	s.captured[id] = true
	s.logger.Info("capture", zap.String("id", id))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleVoid(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.voided[id] {
		w.WriteHeader(http.StatusOK) // idempotent: a lost 200 must be retryable
		return
	}
	if _, ok := s.captured[id]; !ok {
		writeError(w, http.StatusNotFound, "", msgUnknownCharge)
		return
	}
	delete(s.captured, id)
	s.voided[id] = true
	s.logger.Info("void", zap.String("id", id))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleRefund(w http.ResponseWriter, r *http.Request) {
	var req provider.RefundRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "", "invalid refund request")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if id, ok := s.refundsByKey[req.IdempotencyKey]; ok && req.IdempotencyKey != "" {
		writeJSON(w, http.StatusOK, provider.RefundResponse{ProviderRefundID: id}) // replay
		return
	}
	if _, ok := s.captured[req.ProviderPaymentID]; !ok {
		writeError(w, http.StatusNotFound, "", msgUnknownCharge)
		return
	}

	s.refundSeq++
	refundID := fmt.Sprintf("re_%d", s.refundSeq)
	if req.IdempotencyKey != "" {
		s.refundsByKey[req.IdempotencyKey] = refundID
	}
	s.logger.Info("refund", zap.String("id", refundID),
		zap.String("charge", req.ProviderPaymentID), zap.Int64("amount_minor", req.AmountMinor))
	writeJSON(w, http.StatusOK, provider.RefundResponse{ProviderRefundID: refundID})
}
