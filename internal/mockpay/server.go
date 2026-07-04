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
	"sort"
	"strconv"
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
	logger  *zap.Logger
	emitter Emitter // webhook emitter; nil disables emission

	mu            sync.Mutex
	seq           int64                      // provider_payment_id sequence
	refundSeq     int64                      // provider_refund_id sequence
	eventSeq      int64                      // webhook event_id sequence
	byKey         map[string]provider.Charge // charge idempotency replay
	captured      map[string]bool            // provider_payment_id -> captured (absent = voided/unknown)
	voided        map[string]bool            // voided ids — makes void idempotent under retry
	refunded      map[string]bool            // provider_payment_id -> refunded (for GET /transactions status)
	amounts       map[string]int64           // provider_payment_id -> amount (for GET /transactions)
	refundsByKey  map[string]string          // refund idempotency key -> provider_refund_id
	transientSeen map[string]bool            // charge keys that already hit the transient trigger once
}

// New builds an empty mock provider. emitter may be nil (emission disabled).
func New(logger *zap.Logger, emitter Emitter) *Server {
	return &Server{
		logger:        logger,
		emitter:       emitter,
		byKey:         map[string]provider.Charge{},
		captured:      map[string]bool{},
		voided:        map[string]bool{},
		refunded:      map[string]bool{},
		amounts:       map[string]int64{},
		refundsByKey:  map[string]string{},
		transientSeen: map[string]bool{},
	}
}

// emit assigns a fresh event_id and hands the event to the emitter. The caller
// holds s.mu (for eventSeq); Emit itself is async and touches no server state.
func (s *Server) emit(eventType, providerPaymentID string, amount int64) {
	if s.emitter == nil {
		return
	}
	s.eventSeq++
	s.emitter.Emit(provider.WebhookEvent{
		EventID:           fmt.Sprintf("evt_%d", s.eventSeq),
		Type:              eventType,
		ProviderPaymentID: providerPaymentID,
		AmountMinor:       amount,
	})
}

// Handler wires the routes (Go 1.22+ method+wildcard patterns).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /charges", s.handleCharge)
	mux.HandleFunc("POST /charges/{id}/capture", s.handleCapture)
	mux.HandleFunc("POST /charges/{id}/void", s.handleVoid)
	mux.HandleFunc("POST /refunds", s.handleRefund)
	mux.HandleFunc("GET /transactions", s.handleTransactions)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

// transactionStatus derives a charge's provider-side status from the state maps.
// Precedence: refunded and voided are terminal over the capture flag; the caller
// holds s.mu.
func (s *Server) transactionStatus(id string) string {
	switch {
	case s.voided[id]:
		return provider.TxnVoided
	case s.refunded[id]:
		return provider.TxnRefunded
	case s.captured[id]:
		return provider.TxnCaptured
	default:
		return provider.TxnAuthorized
	}
}

// handleTransactions serves the paged provider ledger the reconciliation job
// pages through. Transactions are ordered **lexically** by provider_payment_id —
// a stable total order so a paged sweep sees every row exactly once (it is not
// chronological: `mp_10` sorts before `mp_2`). Defaults: page 1, page_size 50
// (capped at 200).
func (s *Server) handleTransactions(w http.ResponseWriter, r *http.Request) {
	const maxPage = 1_000_000 // a mock; this many pages is far beyond any test
	page := atoiDefault(r.URL.Query().Get("page"), 1, 1, maxPage)
	pageSize := atoiDefault(r.URL.Query().Get("page_size"), 50, 1, 200)

	s.mu.Lock()
	txns := make([]provider.Transaction, 0, len(s.amounts))
	for id, amt := range s.amounts {
		txns = append(txns, provider.Transaction{
			ProviderPaymentID: id,
			AmountMinor:       amt,
			Status:            s.transactionStatus(id),
		})
	}
	s.mu.Unlock()

	sort.Slice(txns, func(i, j int) bool { return txns[i].ProviderPaymentID < txns[j].ProviderPaymentID })

	total := len(txns)
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	writeJSON(w, http.StatusOK, provider.TransactionsPage{
		Transactions: txns[start:end],
		Page:         page,
		PageSize:     pageSize,
		Total:        total,
	})
}

// atoiDefault parses s as an int, clamping to [lo, hi]; returns def when empty
// or unparseable.
func atoiDefault(s string, def, lo, hi int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	if n < lo {
		return lo
	}
	if n > hi {
		return hi
	}
	return n
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
	s.amounts[c.ProviderPaymentID] = req.AmountMinor
	s.logger.Info("charge", zap.String("id", c.ProviderPaymentID),
		zap.Int64("amount_minor", req.AmountMinor), zap.Bool("captured", c.Captured))
	eventType := "charge.authorized"
	if c.Captured {
		eventType = "charge.captured"
	}
	s.emit(eventType, c.ProviderPaymentID, req.AmountMinor)
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
	s.emit("charge.captured", id, 0)
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
	s.emit("charge.voided", id, 0)
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
	s.refunded[req.ProviderPaymentID] = true
	s.logger.Info("refund", zap.String("id", refundID),
		zap.String("charge", req.ProviderPaymentID), zap.Int64("amount_minor", req.AmountMinor))
	s.emit("refund.succeeded", req.ProviderPaymentID, req.AmountMinor)
	writeJSON(w, http.StatusOK, provider.RefundResponse{ProviderRefundID: refundID})
}
