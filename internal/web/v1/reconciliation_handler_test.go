package v1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

var errWebBoom = errors.New("boom")

type fakeRunner struct {
	runID int64
	found int
	err   error
}

func (f *fakeRunner) Run(context.Context, int) (int64, int, error) { return f.runID, f.found, f.err }

type fakeReconReader struct {
	run     *domain.ReconRun
	runErr  error
	ds      []domain.Discrepancy
	listErr error
}

func (f *fakeReconReader) GetRun(context.Context, int64) (*domain.ReconRun, error) {
	return f.run, f.runErr
}
func (f *fakeReconReader) ListDiscrepancies(context.Context, int64) ([]domain.Discrepancy, error) {
	return f.ds, f.listErr
}

func newReconRouter(runner ReconRunner, reader reconReader) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterReconciliationRoutes(r, NewReconciliationHandler(runner, reader))
	return r
}

func doRecon(r *gin.Engine, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func completedRun(id int64, found int) *domain.ReconRun {
	return &domain.ReconRun{ID: id, Status: domain.ReconRunCompleted, TransactionsScanned: 5, DiscrepanciesFound: found}
}

func TestTriggerRun_OK(t *testing.T) {
	r := newReconRouter(&fakeRunner{runID: 9, found: 2}, &fakeReconReader{run: completedRun(9, 2)})
	rec := doRecon(r, http.MethodPost, "/payment/v1/internal/reconciliation/runs")
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (%s)", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); loc != "/payment/v1/internal/reconciliation/runs/9" {
		t.Fatalf("Location = %q, want .../runs/9", loc)
	}
	var body struct {
		Run domain.ReconRun `json:"run"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Run.ID != 9 || body.Run.DiscrepanciesFound != 2 || body.Run.Status != domain.ReconRunCompleted {
		t.Fatalf("run = %+v, want id 9 / 2 found / completed", body.Run)
	}
}

func TestTriggerRun_SingleFlightIs409(t *testing.T) {
	h := NewReconciliationHandler(&fakeRunner{runID: 1}, &fakeReconReader{run: completedRun(1, 0)})
	h.running.Store(true) // a pass is in flight
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterReconciliationRoutes(r, h)
	rec := doRecon(r, http.MethodPost, "/payment/v1/internal/reconciliation/runs")
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (%s)", rec.Code, rec.Body)
	}
	// The guard releases: once the in-flight pass finishes, triggers work again.
	h.running.Store(false)
	if rec := doRecon(r, http.MethodPost, "/payment/v1/internal/reconciliation/runs"); rec.Code != http.StatusCreated {
		t.Fatalf("after release: status = %d, want 201 (%s)", rec.Code, rec.Body)
	}
}

func TestTriggerRun_DisabledIs503(t *testing.T) {
	// nil runner = reconciliation disabled (in-process stub, no provider ledger).
	r := newReconRouter(nil, &fakeReconReader{})
	rec := doRecon(r, http.MethodPost, "/payment/v1/internal/reconciliation/runs")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (%s)", rec.Code, rec.Body)
	}
}

func TestTriggerRun_RunErrorIs500(t *testing.T) {
	r := newReconRouter(&fakeRunner{err: errWebBoom}, &fakeReconReader{})
	rec := doRecon(r, http.MethodPost, "/payment/v1/internal/reconciliation/runs")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (%s)", rec.Code, rec.Body)
	}
}

func TestTriggerRun_LookupErrorIs500(t *testing.T) {
	r := newReconRouter(&fakeRunner{runID: 9}, &fakeReconReader{runErr: errWebBoom})
	rec := doRecon(r, http.MethodPost, "/payment/v1/internal/reconciliation/runs")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (%s)", rec.Code, rec.Body)
	}
}

func TestGetRun_OK(t *testing.T) {
	reader := &fakeReconReader{
		run: completedRun(4, 1),
		ds: []domain.Discrepancy{{
			ProviderPaymentID: "mp_2", Class: domain.DiscrepancyAmountMismatch,
			InternalAmount: 2000, ProviderAmount: 2001,
			InternalStatus: "captured", ProviderStatus: "captured",
		}},
	}
	rec := doRecon(newReconRouter(nil, reader), http.MethodGet, "/payment/v1/internal/reconciliation/runs/4")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (%s)", rec.Code, rec.Body)
	}
	var body struct {
		Run           domain.ReconRun      `json:"run"`
		Discrepancies []domain.Discrepancy `json:"discrepancies"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Run.ID != 4 || len(body.Discrepancies) != 1 || body.Discrepancies[0].Class != domain.DiscrepancyAmountMismatch {
		t.Fatalf("body = %+v", body)
	}
	// The report must expose minor-unit amounts under explicit field names.
	if body.Discrepancies[0].InternalAmount != 2000 || body.Discrepancies[0].ProviderAmount != 2001 {
		t.Fatalf("amounts = %+v", body.Discrepancies[0])
	}
}

func TestGetRun_Errors(t *testing.T) {
	cases := []struct {
		name   string
		path   string
		reader *fakeReconReader
		want   int
	}{
		{"bad id", "/payment/v1/internal/reconciliation/runs/abc", &fakeReconReader{}, http.StatusBadRequest},
		{"zero id", "/payment/v1/internal/reconciliation/runs/0", &fakeReconReader{}, http.StatusBadRequest},
		{"unknown run", "/payment/v1/internal/reconciliation/runs/99", &fakeReconReader{runErr: domain.ErrNotFound}, http.StatusNotFound},
		{"run lookup error", "/payment/v1/internal/reconciliation/runs/1", &fakeReconReader{runErr: errWebBoom}, http.StatusInternalServerError},
		{"discrepancy list error", "/payment/v1/internal/reconciliation/runs/1", &fakeReconReader{run: completedRun(1, 0), listErr: errWebBoom}, http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doRecon(newReconRouter(nil, tc.reader), http.MethodGet, tc.path)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.want, rec.Body)
			}
		})
	}
}
