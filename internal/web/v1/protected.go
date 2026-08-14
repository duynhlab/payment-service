package v1

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/repository"
	"github.com/duynhlab/pkg/authmw"
	"github.com/duynhlab/pkg/httpx"
)

// Protected Backoffice reads (RFC-0023 slice A, ADR-047/050): cross-customer
// payments, the per-payment attempt + ledger lineage, and the first reader
// the detect-only reconciliation records have ever had. Read-only by
// construction — refunds and recon triggers stay on the internal audience.

// backofficeRole is the staff-realm role every protected route requires.
const backofficeRole = "backoffice_admin"

// validPaymentStatuses mirrors the payment FSM's stored vocabulary — filter
// validation only; reads never transition anything.
var validPaymentStatuses = map[string]bool{
	"pending": true, "authorized": true, "captured": true, "voided": true,
	"expired": true, "failed": true, "processing": true, "refunded": true,
}

// paymentReader is the slice of the payment repository the operator reads use.
type paymentReader interface {
	ListAll(ctx context.Context, status string, limit, offset int) ([]domain.Payment, int, error)
	FindByID(ctx context.Context, id int64, userID string) (*domain.Payment, error)
}

// attemptReader lists a payment's full round-trip history, and the
// cross-customer worklist of round-trips whose fate is still unknown.
type attemptReader interface {
	ListForPayment(ctx context.Context, paymentID int64) ([]domain.Attempt, error)
	ListOpenPaged(ctx context.Context, limit, offset int) ([]domain.Attempt, int, error)
}

// ledgerTxReader summarizes a payment's append-only ledger lineage.
type ledgerTxReader interface {
	TransactionsForPayment(ctx context.Context, paymentID int64) ([]repository.LedgerTransactionView, error)
}

// reconRunReader pages the reconciliation runs and loads one with its
// discrepancies.
type reconRunReader interface {
	ListRuns(ctx context.Context, limit, offset int) ([]repository.ReconRunView, int, error)
	GetRun(ctx context.Context, id int64) (*repository.ReconRunView, []repository.ReconDiscrepancyView, error)
}

// ProtectedHandler serves the Backoffice reads. Like ReconciliationHandler it
// talks to focused reader interfaces rather than the payment logic service —
// these are projections, not domain operations.
type ProtectedHandler struct {
	payments paymentReader
	attempts attemptReader
	ledger   ledgerTxReader
	recon    reconRunReader
}

// NewProtectedHandler wires the Backoffice read handler.
func NewProtectedHandler(payments paymentReader, attempts attemptReader, ledger ledgerTxReader, recon reconRunReader) *ProtectedHandler {
	return &ProtectedHandler{payments: payments, attempts: attempts, ledger: ledger, recon: recon}
}

// RegisterProtectedRoutes mounts the Backoffice group with the real guard
// chain. Split from mountProtected so tests can inject fakes.
func RegisterProtectedRoutes(r *gin.Engine, h *ProtectedHandler, staffVerifier *authmw.Verifier) {
	h.mountProtected(r, authmw.MiddlewareJWT(staffVerifier), authmw.MiddlewareRequireRole(backofficeRole))
}

func (h *ProtectedHandler) mountProtected(r *gin.Engine, authMW ...gin.HandlerFunc) {
	protected := r.Group("/payment/v1/protected")
	protected.Use(authMW...)
	{
		protected.GET("/payments", h.ListPayments)
		protected.GET("/payments/:id", h.GetPayment)
		protected.GET("/attempts/open", h.ListOpenAttempts)
		protected.GET("/reconciliations/runs", h.ListReconRuns)
		protected.GET("/reconciliations/runs/:id", h.GetReconRun)
	}
}

// ListPayments serves GET /payments?status=&page=&page_size=.
func (h *ProtectedHandler) ListPayments(c *gin.Context) {
	page, pageSize := httpx.ParsePage(c)

	status := c.Query("status")
	if status != "" && !validPaymentStatuses[status] {
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, "unknown status filter")
		return
	}

	items, total, err := h.payments.ListAll(c.Request.Context(), status, pageSize, httpx.Offset(page, pageSize))
	if err != nil {
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, msgInternalError)
		return
	}
	c.JSON(http.StatusOK, httpx.NewPaginated(items, page, pageSize, total))
}

// GetPayment serves GET /payments/:id — the case view: payment + full
// attempt history + ledger lineage summary.
func (h *ProtectedHandler) GetPayment(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, "id must be a positive integer")
		return
	}
	ctx := c.Request.Context()

	// userID "" = the deliberately-unscoped operator read (the same
	// convention the internal refund path already relies on).
	payment, err := h.payments.FindByID(ctx, id, "")
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, domain.ErrNotFound) {
			httpx.RespondError(c, http.StatusNotFound, httpx.CodeNotFound, "Payment not found")
			return
		}
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, msgInternalError)
		return
	}

	attempts, err := h.attempts.ListForPayment(ctx, id)
	if err != nil {
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, msgInternalError)
		return
	}
	ledger, err := h.ledger.TransactionsForPayment(ctx, id)
	if err != nil {
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, msgInternalError)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"payment":  payment,
		"attempts": attempts,
		"ledger":   ledger,
	})
}

// ListOpenAttempts serves GET /attempts/open?page=&page_size= — the operator's
// doubt worklist across all customers. Every row is a provider round-trip whose
// answer never arrived, so the money effect may or may not have landed; the
// reconciler owns resolving them, and this read is how a human sees the backlog
// it has not reached yet.
func (h *ProtectedHandler) ListOpenAttempts(c *gin.Context) {
	page, pageSize := httpx.ParsePage(c)
	items, total, err := h.attempts.ListOpenPaged(c.Request.Context(), pageSize, httpx.Offset(page, pageSize))
	if err != nil {
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, msgInternalError)
		return
	}
	c.JSON(http.StatusOK, httpx.NewPaginated(items, page, pageSize, total))
}

// ListReconRuns serves GET /reconciliations/runs?page=&page_size=.
func (h *ProtectedHandler) ListReconRuns(c *gin.Context) {
	page, pageSize := httpx.ParsePage(c)
	runs, total, err := h.recon.ListRuns(c.Request.Context(), pageSize, httpx.Offset(page, pageSize))
	if err != nil {
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, msgInternalError)
		return
	}
	c.JSON(http.StatusOK, httpx.NewPaginated(runs, page, pageSize, total))
}

// GetReconRun serves GET /reconciliations/runs/:id — run header plus its
// discrepancies, the operator's triage view.
func (h *ProtectedHandler) GetReconRun(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, "id must be a positive integer")
		return
	}
	run, discrepancies, err := h.recon.GetRun(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.RespondError(c, http.StatusNotFound, httpx.CodeNotFound, "Reconciliation run not found")
			return
		}
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, msgInternalError)
		return
	}
	c.JSON(http.StatusOK, gin.H{"run": run, "discrepancies": discrepancies})
}
