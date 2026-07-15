package v1

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/pkg/httpx"
)

// reconRunTimeout bounds a synchronously-triggered run — parity with the
// background ticker's per-job timeout, and the ceiling on what one POST can
// cost. Without it a slow pass (or a client that never reads the response)
// would hold the goroutine, the ledger snapshot, and a pool connection open
// indefinitely.
const reconRunTimeout = 30 * time.Second

// fieldRun is the JSON key wrapping the run resource in API responses.
const fieldRun = "run"

// msgInternalError is the opaque 500 message — internals never leak.
const msgInternalError = "Internal server error"

// msgRunFailed is the log/error message for a failed reconciliation pass.
const msgRunFailed = "Reconciliation run failed"

// reconRunsPath is the canonical runs collection path — used both to mount the
// routes and to build the Location header from a constant. The header must
// never be derived from the request URL: reflecting user-controlled input into
// a response header is a header-injection sink.
const reconRunsPath = "/payment/v1/internal/payments/reconciliation/runs"

// reconRunsPathDeprecated is the pre-v3 path, kept mounted as an alias for one
// release during the rollout. Remove at contract; see homelab ADR-017.
const reconRunsPathDeprecated = "/payment/v1/internal/reconciliation/runs"

// Shared JSON/log field keys.
const (
	fieldRunID         = "run_id"
	fieldDiscrepancies = "discrepancies"
)

// Discrepancy report paging bounds.
const (
	defaultDiscrepancyLimit = 100
	maxDiscrepancyLimit     = 500
)

// ReconRunner triggers one reconciliation pass. *logicv1.Reconciler satisfies
// it. The handler holds a nil runner when reconciliation is disabled (the
// in-process provider stub has no ledger to reconcile against).
type ReconRunner interface {
	Run(ctx context.Context, pageSize int) (runID int64, found int, err error)
}

// reconReader is the report side: the persisted run + its discrepancies.
// *repository.ReconciliationRepository satisfies it.
type reconReader interface {
	GetRun(ctx context.Context, id int64) (*domain.ReconRun, error)
	ListDiscrepancies(ctx context.Context, runID int64, limit, offset int) ([]domain.Discrepancy, error)
}

// ReconciliationHandler serves the internal reconciliation API: trigger a run,
// read a run's report. Internal audience — cluster-only, NetworkPolicy is the
// fence, never routed through the gateway.
type ReconciliationHandler struct {
	runner ReconRunner // nil = reconciliation disabled
	reader reconReader
	// running single-flights the trigger endpoint: one pass costs a full table
	// scan plus paging the whole provider ledger, so concurrent POSTs answer 409
	// instead of multiplying that load (the 5-minute ticker covers freshness).
	running atomic.Bool
}

// NewReconciliationHandler wires the handler. runner may be nil (stub provider);
// the trigger endpoint then answers 503 instead of running.
func NewReconciliationHandler(runner ReconRunner, reader reconReader) *ReconciliationHandler {
	return &ReconciliationHandler{runner: runner, reader: reader}
}

// RegisterReconciliationRoutes mounts the internal reconciliation routes.
func RegisterReconciliationRoutes(r *gin.Engine, h *ReconciliationHandler) {
	r.POST(reconRunsPath, h.TriggerRun)
	r.GET(reconRunsPath+"/:id", h.GetRun)
	// Deprecated aliases — same handlers on the pre-v3 path.
	r.POST(reconRunsPathDeprecated, h.TriggerRun)
	r.GET(reconRunsPathDeprecated+"/:id", h.GetRun)
}

// TriggerRun handles POST /payment/v1/internal/payments/reconciliation/runs — runs one
// reconciliation pass synchronously (a pass is seconds at this volume) and
// returns the finished run resource, 201.
func (h *ReconciliationHandler) TriggerRun(c *gin.Context) {
	ctx, span, log := beginRequest(c)

	if h.runner == nil {
		httpx.RespondError(c, http.StatusServiceUnavailable, httpx.CodeInternal,
			"Reconciliation is unavailable: no provider ledger configured")
		return
	}

	if !h.running.CompareAndSwap(false, true) {
		httpx.RespondError(c, http.StatusConflict, httpx.CodeConflict,
			"A reconciliation run is already in progress")
		return
	}
	defer h.running.Store(false)

	runCtx, cancel := context.WithTimeout(ctx, reconRunTimeout)
	defer cancel()
	// Page size 0 → the reconciler's own default; one source of truth.
	runID, found, err := h.runner.Run(runCtx, 0)
	if err != nil {
		span.RecordError(err)
		log.Error(msgRunFailed, zap.Int64(fieldRunID, runID), zap.Error(err))
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, msgRunFailed)
		return
	}

	run, err := h.reader.GetRun(ctx, runID)
	if err != nil {
		span.RecordError(err)
		log.Error("Reconciliation run lookup after trigger failed", zap.Int64(fieldRunID, runID), zap.Error(err))
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, msgInternalError)
		return
	}
	log.Info("Reconciliation run triggered", zap.Int64(fieldRunID, runID), zap.Int(fieldDiscrepancies, found))
	c.Header("Location", reconRunsPath+"/"+strconv.FormatInt(runID, 10))
	c.JSON(http.StatusCreated, gin.H{fieldRun: run})
}

// GetRun handles GET /payment/v1/internal/payments/reconciliation/runs/:id — the run
// resource plus its full discrepancy report.
func (h *ReconciliationHandler) GetRun(c *gin.Context) {
	ctx, span, log := beginRequest(c)

	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		httpx.RespondError(c, http.StatusBadRequest, httpx.CodeValidation, "run id must be a positive integer")
		return
	}

	run, err := h.reader.GetRun(ctx, id)
	if errors.Is(err, domain.ErrNotFound) {
		httpx.RespondError(c, http.StatusNotFound, httpx.CodeNotFound, "Reconciliation run not found")
		return
	}
	if err != nil {
		span.RecordError(err)
		log.Error("Reconciliation run lookup failed", zap.Int64(fieldRunID, id), zap.Error(err))
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, msgInternalError)
		return
	}

	limit, offset := discrepancyPage(c)
	discrepancies, err := h.reader.ListDiscrepancies(ctx, id, limit, offset)
	if err != nil {
		span.RecordError(err)
		log.Error("Discrepancy list failed", zap.Int64(fieldRunID, id), zap.Error(err))
		httpx.RespondError(c, http.StatusInternalServerError, httpx.CodeInternal, msgInternalError)
		return
	}

	// The run row carries the full count (discrepancies_found), so the page
	// reports total without a second COUNT query.
	c.JSON(http.StatusOK, gin.H{
		fieldRun:           run,
		fieldDiscrepancies: discrepancies,
		"pagination":       gin.H{"limit": limit, "offset": offset, "total": run.DiscrepanciesFound},
	})
}

// discrepancyPage reads the limit/offset query params for the discrepancy report,
// applying defaults and caps: limit defaults to defaultDiscrepancyLimit, is
// clamped to [1, maxDiscrepancyLimit]; offset defaults to 0 and floors at 0.
func discrepancyPage(c *gin.Context) (limit, offset int) {
	limit = defaultDiscrepancyLimit
	if v, err := strconv.Atoi(c.Query("limit")); err == nil && v > 0 {
		limit = v
	}
	if limit > maxDiscrepancyLimit {
		limit = maxDiscrepancyLimit
	}
	if v, err := strconv.Atoi(c.Query("offset")); err == nil && v > 0 {
		offset = v
	}
	return limit, offset
}
