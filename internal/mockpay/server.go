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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/internal/core/provider"
)

// msgUnknownCharge is the error message for capture/void/refund against an id
// the mock has never issued (or already voided).
const msgUnknownCharge = "unknown charge"

// codeRefundDeclined is the machine code a refused refund carries, mirroring the
// charge path's decline codes so one client mapping covers both.
const codeRefundDeclined = "refund_declined"

// noAnswerDelay outlasts the payment client's 10s HTTP timeout, so the
// …13 trigger produces a real timeout rather than a modelled error.
const noAnswerDelay = 15 * time.Second

// refundDeclineSuffix is the refund-only magic amount: a refund of an amount
// ending in 07 is refused. Distinct from provider.Classify's charge suffixes
// (02/95/19) so a charge can succeed and its refund still be declined.
const refundDeclineSuffix = 7

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
	// createdAt is when the provider first saw each charge. Reconciliation reads
	// it to bound a pass to a time window: without it a caller can only ask for
	// EVERY transaction the provider has ever recorded, which is the scan that
	// grows forever.
	createdAt map[string]time.Time
	refundsByKey  map[string]string          // refund idempotency key -> provider_refund_id
	transientSeen map[string]bool            // charge keys that already hit the transient trigger once
	// mutationKeys binds an idempotency key to the (operation, charge) it was
	// first used for, so reuse elsewhere is a detectable caller bug. It does NOT
	// remember the answer: replaying a remembered success could contradict the
	// charge's later state — a capture replayed after a void would report money
	// that is no longer collectable. State is the truth here.
	mutationKeys map[string]mutationBinding
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
		createdAt:     map[string]time.Time{},
		refundsByKey:  map[string]string{},
		mutationKeys:  map[string]mutationBinding{},
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
// handleTransactions serves the reconciliation ledger, optionally bounded to a
// half-open time window [from, to). A real provider offers the same thing for the
// same reason: without it every reconciliation pass has to read the provider's
// entire history, so the cost of checking today's payments grows with every
// payment ever made.
//
// Malformed timestamps are refused rather than ignored. Silently dropping an
// unparseable `from` would widen the window to everything and answer a question
// nobody asked — and the caller would compare that against a narrow internal set
// and report every older charge as missing on our side.
func (s *Server) handleTransactions(w http.ResponseWriter, r *http.Request) {
	const maxPage = 1_000_000 // a mock; this many pages is far beyond any test
	page := atoiDefault(r.URL.Query().Get("page"), 1, 1, maxPage)
	pageSize := atoiDefault(r.URL.Query().Get("page_size"), 50, 1, 200)

	from, to, err := timeWindow(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, "", err.Error())
		return
	}

	s.mu.Lock()
	txns := make([]provider.Transaction, 0, len(s.amounts))
	for id, amt := range s.amounts {
		created := s.createdAt[id]
		if !inWindow(created, from, to) {
			continue
		}
		txns = append(txns, provider.Transaction{
			ProviderPaymentID: id,
			AmountMinor:       amt,
			Status:            s.transactionStatus(id),
			CreatedAt:         created,
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
			// 429, not 503: the trigger models a provider that REFUSED the request
			// and did nothing with it, which is a decided answer the caller may
			// safely retry. A 503 would mean "I may have done it" — undecided — and
			// the client now classifies it that way.
			writeError(w, http.StatusTooManyRequests, provider.DeclineProcessing, "processing error, retry")
			return
		}
		// second attempt with the same key falls through to success
	case provider.OutcomeNoAnswer:
		// Go silent: the charge IS created but the client never learns of it.
		// That lost-response window is what the phase-6 taxonomy exists for, and
		// it could previously only be reproduced by killing a container — which
		// also destroyed the charge, so the "provider did it, we do not know"
		// case was untestable. Here the charge really survives.
		s.mintCharge(req)
		s.noAnswer(r.Context())
		return
	case provider.OutcomeOK:
	}

	c := s.mintCharge(req)
	writeJSON(w, http.StatusOK, c)
}

// mintCharge creates the charge and emits its webhook. Split out so the
// no-answer trigger can create a charge that really exists and then stay silent
// about it — the lost-response window, reproduced faithfully. Caller holds s.mu.
func (s *Server) mintCharge(req provider.ChargeRequest) provider.Charge {
	s.seq++
	c := provider.Charge{ProviderPaymentID: fmt.Sprintf("mp_%d", s.seq), Captured: req.AutoCapture}
	if req.IdempotencyKey != "" {
		s.byKey[req.IdempotencyKey] = c
	}
	s.captured[c.ProviderPaymentID] = req.AutoCapture
	s.amounts[c.ProviderPaymentID] = req.AmountMinor
	s.createdAt[c.ProviderPaymentID] = time.Now().UTC()
	s.logger.Info("charge", zap.String("id", c.ProviderPaymentID),
		zap.Int64("amount_minor", req.AmountMinor), zap.Bool("captured", c.Captured))
	eventType := "charge.authorized"
	if c.Captured {
		eventType = "charge.captured"
	}
	s.emit(eventType, c.ProviderPaymentID, req.AmountMinor)
	return c
}

// noAnswer holds the response open past any sane client timeout, then closes
// without writing. The caller sees a transport failure with no verdict — which
// is the point: the effect landed, the answer did not.
func (s *Server) noAnswer(ctx context.Context) {
	select {
	case <-time.After(noAnswerDelay):
	case <-ctx.Done():
	}
}

func (s *Server) handleCapture(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	key, ok := mutationKey(w, r)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.bindKey(w, key, opCapture, id) {
		return
	}
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
	key, ok := mutationKey(w, r)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.bindKey(w, key, opVoid, id) {
		return
	}
	if s.voided[id] {
		w.WriteHeader(http.StatusOK) // idempotent by state: a lost 200 is retryable
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

// mutationBinding is what an idempotency key was first used for. Only the
// identity is kept — never the answer.
type mutationBinding struct {
	operation string
	chargeID  string
}

// Operation names used in idempotency-key bindings.
const (
	opCapture = "capture"
	opVoid    = "void"
)

// mutationKey reads the optional idempotency key from a capture/void body.
//
// Three cases, deliberately distinguished: no body at all is a legacy caller and
// keeps the pre-key behaviour; a well-formed body yields its key; a body that is
// present but undecodable is a 400, because silently treating it as keyless
// would disable the mechanism in exactly the situation it exists for — a request
// whose connection died mid-flight. ok=false means the response is written.
func mutationKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "", "unreadable request body")
		return "", false
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return "", true // no body: legacy caller
	}
	var req provider.MutationRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		writeError(w, http.StatusBadRequest, "", "invalid mutation request")
		return "", false
	}
	return req.IdempotencyKey, true
}

// bindKey records that this key belongs to (op, chargeID), or answers 409 when it
// was already used for a different one — borrowing another operation's verdict
// would hide the caller's key-derivation bug behind a plausible success.
// Returns false when the response is written. Caller holds s.mu.
func (s *Server) bindKey(w http.ResponseWriter, key, op, chargeID string) bool {
	if key == "" {
		return true
	}
	want := mutationBinding{operation: op, chargeID: chargeID}
	if prior, ok := s.mutationKeys[key]; ok && prior != want {
		s.logger.Warn("idempotency key reused for a different operation or charge",
			zap.String("key", key), zap.String("bound_operation", prior.operation),
			zap.String("bound_charge", prior.chargeID),
			zap.String("requested_operation", op), zap.String("requested_charge", chargeID))
		writeError(w, http.StatusConflict, provider.CodeIdempotencyConflict,
			"idempotency key reused for a different operation or charge")
		return false
	}
	s.mutationKeys[key] = want
	return true
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
	// A refund can be refused too, and the caller MUST be able to tell that
	// decided "no" from a timeout: one is recorded and parked, the other is held
	// open and retried. Without a trigger here the decline branch is unreachable
	// end to end.
	//
	// The refund trigger is its OWN suffix rather than provider.Classify's,
	// because the saga refunds the whole remaining amount — the same number it
	// charged. Reusing a charge-decline suffix would make "charge succeeded, its
	// refund refused" unreachable through the real cancellation flow, which is
	// exactly the case worth exercising.
	if req.AmountMinor%100 == refundDeclineSuffix {
		s.logger.Info("refund declined", zap.String("charge", req.ProviderPaymentID),
			zap.Int64("amount_minor", req.AmountMinor))
		writeError(w, http.StatusPaymentRequired, codeRefundDeclined, "refund declined")
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

// timeWindow parses the optional from/to query parameters as RFC 3339 instants.
// A zero time means "unbounded on that side".
func timeWindow(q url.Values) (from, to time.Time, err error) {
	for name, dst := range map[string]*time.Time{"from": &from, "to": &to} {
		raw := q.Get(name)
		if raw == "" {
			continue
		}
		t, perr := time.Parse(time.RFC3339, raw)
		if perr != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("%s must be an RFC 3339 timestamp", name)
		}
		*dst = t
	}
	if !from.IsZero() && !to.IsZero() && to.Before(from) {
		return time.Time{}, time.Time{}, errors.New("to must not precede from")
	}
	return from, to, nil
}

// inWindow reports whether created falls in the half-open range [from, to).
// Half-open on purpose: consecutive windows that share a boundary must not both
// claim the charge sitting exactly on it.
func inWindow(created, from, to time.Time) bool {
	if !from.IsZero() && created.Before(from) {
		return false
	}
	if !to.IsZero() && !created.Before(to) {
		return false
	}
	return true
}
