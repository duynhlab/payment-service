package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/repository"
	"github.com/duynhlab/pkg/authmw"
)

// fakeReaders scripts all four reader ports.
type fakeReaders struct {
	payments []domain.Payment
	total    int
	got      struct {
		status        string
		limit, offset int
	}
	payment  *domain.Payment
	payErr   error
	attempts []domain.Attempt
	attErr   error
	ledger   []repository.LedgerTransactionView
	ledErr   error
	runs     []repository.ReconRunView
	runTotal int
	run      *repository.ReconRunView
	discs    []repository.ReconDiscrepancyView
	runErr   error
}

func (f *fakeReaders) ListAll(_ context.Context, status string, limit, offset int) ([]domain.Payment, int, error) {
	f.got.status, f.got.limit, f.got.offset = status, limit, offset
	return f.payments, f.total, f.payErr
}
func (f *fakeReaders) FindByID(_ context.Context, _ int64, userID string) (*domain.Payment, error) {
	if userID != "" {
		panic("operator read must be unscoped")
	}
	if f.payment == nil {
		return nil, pgx.ErrNoRows
	}
	return f.payment, f.payErr
}
func (f *fakeReaders) ListForPayment(_ context.Context, _ int64) ([]domain.Attempt, error) {
	return f.attempts, f.attErr
}
func (f *fakeReaders) TransactionsForPayment(_ context.Context, _ int64) ([]repository.LedgerTransactionView, error) {
	return f.ledger, f.ledErr
}
func (f *fakeReaders) ListRuns(_ context.Context, limit, offset int) ([]repository.ReconRunView, int, error) {
	f.got.limit, f.got.offset = limit, offset
	return f.runs, f.runTotal, f.runErr
}
func (f *fakeReaders) GetRun(_ context.Context, _ int64) (*repository.ReconRunView, []repository.ReconDiscrepancyView, error) {
	if f.run == nil {
		return nil, nil, pgx.ErrNoRows
	}
	return f.run, f.discs, f.runErr
}

func protectedEngine(t *testing.T, f *fakeReaders, roles ...string) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h := NewProtectedHandler(f, f, f, f)
	h.mountProtected(r,
		func(c *gin.Context) {
			c.Set(authmw.CtxUserID, "d0e00000-0000-4000-8000-000000000001")
			c.Set(authmw.CtxRoles, roles)
			c.Next()
		},
		authmw.MiddlewareRequireRole(backofficeRole))
	return r
}

func get(r *gin.Engine, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestProtectedPaymentsRoleGate(t *testing.T) {
	r := protectedEngine(t, &fakeReaders{}, "customer")
	for _, path := range []string{
		"/payment/v1/protected/payments",
		"/payment/v1/protected/payments/1",
		"/payment/v1/protected/reconciliations/runs",
		"/payment/v1/protected/reconciliations/runs/1",
	} {
		if w := get(r, path); w.Code != http.StatusForbidden {
			t.Fatalf("%s: want 403, got %d", path, w.Code)
		}
	}
}

func TestProtectedListPayments(t *testing.T) {
	f := &fakeReaders{payments: []domain.Payment{{ID: 9, UserID: "u-1", Status: "captured"}}, total: 33}
	r := protectedEngine(t, f, backofficeRole)

	w := get(r, "/payment/v1/protected/payments?status=captured&page=2&page_size=10")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	if f.got.status != "captured" || f.got.limit != 10 || f.got.offset != 10 {
		t.Fatalf("args not forwarded: %+v", f.got)
	}
	var resp struct {
		TotalItems int `json:"total_items"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.TotalItems != 33 {
		t.Fatalf("want 33, got %d", resp.TotalItems)
	}

	if w := get(r, "/payment/v1/protected/payments?status=bogus"); w.Code != http.StatusBadRequest {
		t.Fatalf("bogus status: want 400, got %d", w.Code)
	}
}

func TestProtectedGetPaymentCaseView(t *testing.T) {
	f := &fakeReaders{
		payment:  &domain.Payment{ID: 7, UserID: "u-1", Status: "captured"},
		attempts: []domain.Attempt{{ID: 1, PaymentID: 7}},
		ledger:   []repository.LedgerTransactionView{{ID: 1, Kind: "capture", AmountMinor: 25798}},
	}
	r := protectedEngine(t, f, backofficeRole)

	w := get(r, "/payment/v1/protected/payments/7")
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Payment  domain.Payment                     `json:"payment"`
		Attempts []domain.Attempt                   `json:"attempts"`
		Ledger   []repository.LedgerTransactionView `json:"ledger"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Payment.ID != 7 || len(resp.Attempts) != 1 || len(resp.Ledger) != 1 {
		t.Fatalf("case view incomplete: %s", w.Body.String())
	}

	if w := get(r, "/payment/v1/protected/payments/abc"); w.Code != http.StatusBadRequest {
		t.Fatalf("bad id: want 400, got %d", w.Code)
	}
	f.payment = nil
	if w := get(r, "/payment/v1/protected/payments/999"); w.Code != http.StatusNotFound {
		t.Fatalf("missing: want 404, got %d", w.Code)
	}
}

func TestProtectedGetPaymentErrorBranches(t *testing.T) {
	f := &fakeReaders{
		payment: &domain.Payment{ID: 7},
		attErr:  context.DeadlineExceeded,
	}
	r := protectedEngine(t, f, backofficeRole)
	if w := get(r, "/payment/v1/protected/payments/7"); w.Code != http.StatusInternalServerError {
		t.Fatalf("attempts err: want 500, got %d", w.Code)
	}
	f.attErr, f.ledErr = nil, context.DeadlineExceeded
	if w := get(r, "/payment/v1/protected/payments/7"); w.Code != http.StatusInternalServerError {
		t.Fatalf("ledger err: want 500, got %d", w.Code)
	}
	f2 := &fakeReaders{payErr: context.DeadlineExceeded}
	r2 := protectedEngine(t, f2, backofficeRole)
	if w := get(r2, "/payment/v1/protected/payments"); w.Code != http.StatusInternalServerError {
		t.Fatalf("list err: want 500, got %d", w.Code)
	}
}

func TestReconRuns(t *testing.T) {
	f := &fakeReaders{
		runs:     []repository.ReconRunView{{ID: 3, Status: "completed", DiscrepanciesFound: 1}},
		runTotal: 3,
		run:      &repository.ReconRunView{ID: 3, Status: "completed"},
		discs:    []repository.ReconDiscrepancyView{{ID: 1, RunID: 3, Class: "amount_mismatch"}},
	}
	r := protectedEngine(t, f, backofficeRole)

	if w := get(r, "/payment/v1/protected/reconciliations/runs?page=1&page_size=5"); w.Code != http.StatusOK {
		t.Fatalf("runs: want 200, got %d", w.Code)
	}
	w := get(r, "/payment/v1/protected/reconciliations/runs/3")
	if w.Code != http.StatusOK {
		t.Fatalf("run: want 200, got %d", w.Code)
	}
	var resp struct {
		Discrepancies []repository.ReconDiscrepancyView `json:"discrepancies"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Discrepancies) != 1 || resp.Discrepancies[0].Class != "amount_mismatch" {
		t.Fatalf("discrepancies missing: %s", w.Body.String())
	}

	f.run = nil
	if w := get(r, "/payment/v1/protected/reconciliations/runs/99"); w.Code != http.StatusNotFound {
		t.Fatalf("missing run: want 404, got %d", w.Code)
	}
	if w := get(r, "/payment/v1/protected/reconciliations/runs/abc"); w.Code != http.StatusBadRequest {
		t.Fatalf("bad id: want 400, got %d", w.Code)
	}
	f.run = &repository.ReconRunView{ID: 3}
	f.runErr = context.DeadlineExceeded
	if w := get(r, "/payment/v1/protected/reconciliations/runs/3"); w.Code != http.StatusInternalServerError {
		t.Fatalf("run err: want 500, got %d", w.Code)
	}
	if w := get(r, "/payment/v1/protected/reconciliations/runs"); w.Code != http.StatusInternalServerError {
		t.Fatalf("runs err: want 500, got %d", w.Code)
	}
}

func TestRegisterProtectedRoutesRealChain(t *testing.T) {
	verifier, err := authmw.NewVerifier(authmw.Config{
		Issuer:   "http://localhost:8081/realms/duynhlab-staff",
		Audience: "duynhlab-platform",
	})
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterProtectedRoutes(r, NewProtectedHandler(&fakeReaders{}, &fakeReaders{}, &fakeReaders{}, &fakeReaders{}), verifier)
	if w := get(r, "/payment/v1/protected/payments"); w.Code != http.StatusUnauthorized {
		t.Fatalf("tokenless: want 401 from the real chain, got %d", w.Code)
	}
}
