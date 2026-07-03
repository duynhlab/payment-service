package v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
	"github.com/duynhlab/payment-service/internal/core/repository"
	logicv1 "github.com/duynhlab/payment-service/internal/logic/v1"
	"github.com/duynhlab/pkg/authmw"
	"github.com/duynhlab/pkg/httpx"
)

func init() { gin.SetMode(gin.TestMode) }

// fakeLogic is a configurable paymentLogic double for web tests. It records
// the arguments it was called with so tests can assert the translation.
type fakeLogic struct {
	createResult *logicv1.IntentResult
	createErr    error
	gotIdemKey   string
	gotInput     logicv1.CreateIntentInput

	payment *domain.Payment
	getErr  error

	list        []domain.Payment
	total       int
	listErr     error
	gotPage     int
	gotPageSize int

	refund         *domain.Refund
	refundReplayed bool
	refundErr      error
	gotRefundKey   string
	gotPaymentID   int64
	gotUserID      int64
	gotAmount      int64
	gotReason      string
}

func (f *fakeLogic) CreateIntent(_ context.Context, idemKey string, in logicv1.CreateIntentInput) (*logicv1.IntentResult, error) {
	f.gotIdemKey, f.gotInput = idemKey, in
	return f.createResult, f.createErr
}

func (f *fakeLogic) Get(_ context.Context, _, _ int64) (*domain.Payment, error) {
	return f.payment, f.getErr
}

func (f *fakeLogic) List(_ context.Context, _ int64, page, pageSize int) ([]domain.Payment, int, error) {
	f.gotPage, f.gotPageSize = page, pageSize
	return f.list, f.total, f.listErr
}

func (f *fakeLogic) CreateRefund(_ context.Context, idemKey string, paymentID, userID, amountMinor int64, reason string) (*domain.Refund, bool, error) {
	f.gotRefundKey, f.gotPaymentID, f.gotUserID = idemKey, paymentID, userID
	f.gotAmount, f.gotReason = amountMinor, reason
	return f.refund, f.refundReplayed, f.refundErr
}

// newTestRouter mounts the handler with a fake auth middleware that injects
// userID (empty = unauthenticated) instead of verifying a real JWT.
func newTestRouter(l paymentLogic, userID string) *gin.Engine {
	r := gin.New()
	NewHandler(l).mount(r, func(c *gin.Context) {
		if userID != "" {
			c.Set(authmw.CtxUserID, userID)
		}
		c.Next()
	})
	return r
}

// do performs a request against the router and returns the recorder.
func do(r *gin.Engine, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

// decode parses the JSON response body into a generic map.
func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON body %q: %v", rec.Body.String(), err)
	}
	return body
}

func idemHeader() map[string]string {
	return map[string]string{"Idempotency-Key": "key-1", "Content-Type": "application/json"}
}

func TestCreatePayment(t *testing.T) {
	created := &domain.Payment{ID: 7, UserID: 42, AmountMinor: 2000, Currency: "USD", Status: domain.StatusAuthorized}
	declined := &domain.Payment{ID: 8, UserID: 42, AmountMinor: 2002, Currency: "USD", Status: domain.StatusFailed, DeclineCode: provider.DeclineGeneric}

	tests := []struct {
		name       string
		userID     string
		headers    map[string]string
		body       string
		fake       *fakeLogic
		wantStatus int
		wantCode   string // "" = skip code check
	}{
		{
			name:       "happy create 201",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000,"payment_method":"tok_visa"}`,
			fake:       &fakeLogic{createResult: &logicv1.IntentResult{Code: 201, Payment: created}},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "token with 11 digits accepted (below PAN threshold) 201",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000,"payment_method":"tok_12345678901"}`, // 11 digits
			fake:       &fakeLogic{createResult: &logicv1.IntentResult{Code: 201, Payment: created}},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "amount exactly at ceiling accepted 201",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":10000000000,"payment_method":"tok_visa"}`, // == maxAmountMinor
			fake:       &fakeLogic{createResult: &logicv1.IntentResult{Code: 201, Payment: created}},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "missing idempotency key 400",
			userID:     "42",
			headers:    map[string]string{"Content-Type": "application/json"},
			body:       `{"amount_minor":2000,"payment_method":"tok_visa"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeIdempotencyKeyRequired,
		},
		{
			name:       "declined 422",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":2002,"payment_method":"tok_visa"}`,
			fake:       &fakeLogic{createResult: &logicv1.IntentResult{Code: 422, Payment: declined}},
			wantStatus: http.StatusUnprocessableEntity,
			wantCode:   httpx.CodePaymentDeclined,
		},
		{
			name:       "amount zero 400",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":0,"payment_method":"tok_visa"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "amount negative 400",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":-5,"payment_method":"tok_visa"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "lowercase currency 400",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000,"currency":"usd","payment_method":"tok_visa"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "four-letter currency 400",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000,"currency":"USDX","payment_method":"tok_visa"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "bad capture method 400",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000,"capture_method":"later","payment_method":"tok_visa"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "bad token prefix 400",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000,"payment_method":"card_4242"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "underscore-split PAN rejected 400",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000,"payment_method":"tok_4111_1111_1111_1111"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "contiguous PAN rejected 400",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000,"payment_method":"tok_4111111111111111"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "over-ceiling amount 400",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":10000000001,"payment_method":"tok_visa"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "missing payment method 400",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "malformed JSON 400",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "no user in context 401",
			userID:     "",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000,"payment_method":"tok_visa"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusUnauthorized,
			wantCode:   httpx.CodeUnauthorized,
		},
		{
			name:       "non-numeric subject 401",
			userID:     "alice",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000,"payment_method":"tok_visa"}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusUnauthorized,
			wantCode:   httpx.CodeUnauthorized,
		},
		{
			name:       "logic error translated 409",
			userID:     "42",
			headers:    idemHeader(),
			body:       `{"amount_minor":2000,"order_id":9,"payment_method":"tok_visa"}`,
			fake:       &fakeLogic{createErr: repository.ErrPaymentExists},
			wantStatus: http.StatusConflict,
			wantCode:   httpx.CodePaymentExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRouter(tt.fake, tt.userID)
			rec := do(r, http.MethodPost, "/payment/v1/private/payments", tt.body, tt.headers)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantCode != "" {
				if got := decode(t, rec)["code"]; got != tt.wantCode {
					t.Errorf("code = %v, want %v", got, tt.wantCode)
				}
			}
		})
	}
}

// TestCreatePaymentDefaultsAndClaims verifies the defaults (USD, manual) and
// that user_id comes from the JWT claims, never the body.
func TestCreatePaymentDefaultsAndClaims(t *testing.T) {
	fake := &fakeLogic{createResult: &logicv1.IntentResult{
		Code:    201,
		Payment: &domain.Payment{ID: 1, UserID: 42},
	}}
	r := newTestRouter(fake, "42")

	body := `{"amount_minor":1500,"payment_method":"tok_visa","user_id":999}`
	rec := do(r, http.MethodPost, "/payment/v1/private/payments", body, idemHeader())

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %s)", rec.Code, rec.Body.String())
	}
	if fake.gotIdemKey != "key-1" {
		t.Errorf("idemKey = %q, want %q", fake.gotIdemKey, "key-1")
	}
	in := fake.gotInput
	if in.UserID != 42 {
		t.Errorf("UserID = %d, want 42 (must come from claims)", in.UserID)
	}
	if in.Currency != "USD" {
		t.Errorf("Currency = %q, want default USD", in.Currency)
	}
	if in.CaptureMethod != domain.CaptureManual {
		t.Errorf("CaptureMethod = %q, want default manual", in.CaptureMethod)
	}
	if in.AmountMinor != 1500 || in.PaymentMethod != "tok_visa" {
		t.Errorf("input = %+v, want amount 1500 / tok_visa", in)
	}
}

// TestCreatePaymentDeclinedEnvelope verifies the 422 body shape: error + code
// + the payment (with its decline_code) under "payment".
func TestCreatePaymentDeclinedEnvelope(t *testing.T) {
	fake := &fakeLogic{createResult: &logicv1.IntentResult{
		Code: 422,
		Payment: &domain.Payment{
			ID: 8, UserID: 42, Status: domain.StatusFailed, DeclineCode: provider.DeclineInsufficient,
		},
	}}
	r := newTestRouter(fake, "42")

	rec := do(r, http.MethodPost, "/payment/v1/private/payments",
		`{"amount_minor":2095,"payment_method":"tok_visa"}`, idemHeader())

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422 (body %s)", rec.Code, rec.Body.String())
	}
	body := decode(t, rec)
	if body["error"] != "payment declined" {
		t.Errorf("error = %v, want %q", body["error"], "payment declined")
	}
	if body["code"] != httpx.CodePaymentDeclined {
		t.Errorf("code = %v, want %v", body["code"], httpx.CodePaymentDeclined)
	}
	pay, ok := body["payment"].(map[string]any)
	if !ok {
		t.Fatalf("payment missing from body: %v", body)
	}
	if pay["decline_code"] != provider.DeclineInsufficient {
		t.Errorf("payment.decline_code = %v, want %v", pay["decline_code"], provider.DeclineInsufficient)
	}
}

func TestGetPayment(t *testing.T) {
	pay := &domain.Payment{ID: 7, UserID: 42, AmountMinor: 2000, Currency: "USD", Status: domain.StatusCaptured}

	tests := []struct {
		name       string
		userID     string
		target     string
		fake       *fakeLogic
		wantStatus int
		wantCode   string
	}{
		{
			name:       "found 200",
			userID:     "42",
			target:     "/payment/v1/private/payments/7",
			fake:       &fakeLogic{payment: pay},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found 404",
			userID:     "42",
			target:     "/payment/v1/private/payments/999",
			fake:       &fakeLogic{getErr: repository.ErrNotFound},
			wantStatus: http.StatusNotFound,
			wantCode:   httpx.CodeNotFound,
		},
		{
			name:       "non-numeric id 400",
			userID:     "42",
			target:     "/payment/v1/private/payments/abc",
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "unauthenticated 401",
			userID:     "",
			target:     "/payment/v1/private/payments/7",
			fake:       &fakeLogic{},
			wantStatus: http.StatusUnauthorized,
			wantCode:   httpx.CodeUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRouter(tt.fake, tt.userID)
			rec := do(r, http.MethodGet, tt.target, "", nil)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantCode != "" {
				if got := decode(t, rec)["code"]; got != tt.wantCode {
					t.Errorf("code = %v, want %v", got, tt.wantCode)
				}
			}
			if tt.wantStatus == http.StatusOK {
				if got := decode(t, rec)["id"]; got != float64(pay.ID) {
					t.Errorf("id = %v, want %v", got, pay.ID)
				}
			}
		})
	}
}

func TestListPayments(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		fake         *fakeLogic
		wantStatus   int
		wantPage     int
		wantPageSize int
	}{
		{
			name:         "defaults page 1 size 20",
			query:        "",
			fake:         &fakeLogic{list: []domain.Payment{{ID: 1}}, total: 1},
			wantStatus:   http.StatusOK,
			wantPage:     1,
			wantPageSize: 20,
		},
		{
			name:         "page_size capped at 100",
			query:        "?page=3&page_size=500",
			fake:         &fakeLogic{list: nil, total: 0},
			wantStatus:   http.StatusOK,
			wantPage:     3,
			wantPageSize: 100,
		},
		{
			name:         "invalid params fall back to defaults",
			query:        "?page=0&page_size=-1",
			fake:         &fakeLogic{list: nil, total: 0},
			wantStatus:   http.StatusOK,
			wantPage:     1,
			wantPageSize: 20,
		},
		{
			name:         "explicit page",
			query:        "?page=2&page_size=5",
			fake:         &fakeLogic{list: []domain.Payment{{ID: 6}}, total: 11},
			wantStatus:   http.StatusOK,
			wantPage:     2,
			wantPageSize: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRouter(tt.fake, "42")
			rec := do(r, http.MethodGet, "/payment/v1/private/payments"+tt.query, "", nil)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.fake.gotPage != tt.wantPage || tt.fake.gotPageSize != tt.wantPageSize {
				t.Errorf("logic called with page=%d size=%d, want page=%d size=%d",
					tt.fake.gotPage, tt.fake.gotPageSize, tt.wantPage, tt.wantPageSize)
			}
			body := decode(t, rec)
			if body["page"] != float64(tt.wantPage) || body["page_size"] != float64(tt.wantPageSize) {
				t.Errorf("envelope page=%v size=%v, want page=%d size=%d",
					body["page"], body["page_size"], tt.wantPage, tt.wantPageSize)
			}
			if _, ok := body["items"].([]any); !ok {
				t.Errorf("items must be a JSON array, got %v", body["items"])
			}
			if _, ok := body["total_items"]; !ok {
				t.Error("total_items missing from envelope")
			}
			if _, ok := body["total_pages"]; !ok {
				t.Error("total_pages missing from envelope")
			}
		})
	}
}

func TestListPaymentsError(t *testing.T) {
	r := newTestRouter(&fakeLogic{listErr: errors.New("pg down")}, "42")
	rec := do(r, http.MethodGet, "/payment/v1/private/payments", "", nil)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	body := decode(t, rec)
	if body["code"] != httpx.CodeInternal {
		t.Errorf("code = %v, want %v", body["code"], httpx.CodeInternal)
	}
	if strings.Contains(rec.Body.String(), "pg down") {
		t.Error("internal error detail leaked into the response")
	}
}

func TestCreateRefund(t *testing.T) {
	refund := &domain.Refund{ID: 3, PaymentID: 7, AmountMinor: 500, Status: domain.RefundSucceeded}

	tests := []struct {
		name       string
		target     string
		headers    map[string]string
		body       string
		fake       *fakeLogic
		wantStatus int
		wantCode   string
	}{
		{
			name:       "happy refund 201",
			target:     "/payment/v1/internal/payments/7/refunds",
			headers:    idemHeader(),
			body:       `{"amount_minor":500,"reason":"customer request"}`,
			fake:       &fakeLogic{refund: refund},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "replayed refund 201",
			target:     "/payment/v1/internal/payments/7/refunds",
			headers:    idemHeader(),
			body:       `{"amount_minor":500}`,
			fake:       &fakeLogic{refund: refund, refundReplayed: true},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "missing idempotency key 400",
			target:     "/payment/v1/internal/payments/7/refunds",
			headers:    map[string]string{"Content-Type": "application/json"},
			body:       `{"amount_minor":500}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeIdempotencyKeyRequired,
		},
		{
			name:       "amount zero 400",
			target:     "/payment/v1/internal/payments/7/refunds",
			headers:    idemHeader(),
			body:       `{"amount_minor":0}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "non-numeric payment id 400",
			target:     "/payment/v1/internal/payments/abc/refunds",
			headers:    idemHeader(),
			body:       `{"amount_minor":500}`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "malformed JSON 400",
			target:     "/payment/v1/internal/payments/7/refunds",
			headers:    idemHeader(),
			body:       `{"amount_minor":`,
			fake:       &fakeLogic{},
			wantStatus: http.StatusBadRequest,
			wantCode:   httpx.CodeValidation,
		},
		{
			name:       "refund rejected 409",
			target:     "/payment/v1/internal/payments/7/refunds",
			headers:    idemHeader(),
			body:       `{"amount_minor":999999}`,
			fake:       &fakeLogic{refundErr: repository.ErrRefundRejected},
			wantStatus: http.StatusConflict,
			wantCode:   httpx.CodeRefundExceedsCapture,
		},
		{
			name:       "payment not found 404",
			target:     "/payment/v1/internal/payments/999/refunds",
			headers:    idemHeader(),
			body:       `{"amount_minor":500}`,
			fake:       &fakeLogic{refundErr: repository.ErrNotFound},
			wantStatus: http.StatusNotFound,
			wantCode:   httpx.CodeNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No user injected: internal audience carries no JWT.
			r := newTestRouter(tt.fake, "")
			rec := do(r, http.MethodPost, tt.target, tt.body, tt.headers)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantCode != "" {
				if got := decode(t, rec)["code"]; got != tt.wantCode {
					t.Errorf("code = %v, want %v", got, tt.wantCode)
				}
			}
			if tt.wantStatus == http.StatusCreated {
				if tt.fake.gotUserID != 0 {
					t.Errorf("userID passed to logic = %d, want 0 (internal scope)", tt.fake.gotUserID)
				}
				if tt.fake.gotPaymentID != 7 {
					t.Errorf("paymentID = %d, want 7", tt.fake.gotPaymentID)
				}
				if got := decode(t, rec)["id"]; got != float64(refund.ID) {
					t.Errorf("refund id = %v, want %v", got, refund.ID)
				}
			}
		})
	}
}

func TestTranslateError(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		wantStatus     int
		wantCode       string
		wantMessage    string // "" = skip exact-message check
		wantRetryAfter string
	}{
		{
			name:       "not found",
			err:        repository.ErrNotFound,
			wantStatus: http.StatusNotFound,
			wantCode:   httpx.CodeNotFound,
		},
		{
			name:       "payment exists",
			err:        repository.ErrPaymentExists,
			wantStatus: http.StatusConflict,
			wantCode:   httpx.CodePaymentExists,
		},
		{
			name:       "key conflict",
			err:        repository.ErrKeyConflict,
			wantStatus: http.StatusConflict,
			wantCode:   httpx.CodeIdempotencyConflict,
		},
		{
			name:           "key locked",
			err:            repository.ErrKeyLocked,
			wantStatus:     http.StatusConflict,
			wantCode:       httpx.CodeIdempotencyConflict,
			wantMessage:    "in flight",
			wantRetryAfter: "1",
		},
		{
			name:       "refund rejected",
			err:        repository.ErrRefundRejected,
			wantStatus: http.StatusConflict,
			wantCode:   httpx.CodeRefundExceedsCapture,
		},
		{
			name:       "invalid transition",
			err:        domain.ErrInvalidTransition,
			wantStatus: http.StatusConflict,
			wantCode:   httpx.CodeInvalidTransition,
		},
		{
			name:        "provider transient",
			err:         provider.ErrTransient,
			wantStatus:  http.StatusServiceUnavailable,
			wantCode:    httpx.CodeInternal,
			wantMessage: "provider unavailable, retry",
		},
		{
			name:       "wrapped sentinel still matches",
			err:        fmt.Errorf("transition: %w: captured -> voided", domain.ErrInvalidTransition),
			wantStatus: http.StatusConflict,
			wantCode:   httpx.CodeInvalidTransition,
		},
		{
			name:        "unknown error is opaque 500",
			err:         errors.New("pq: SSL SYSCALL error at host db-secret-host"),
			wantStatus:  http.StatusInternalServerError,
			wantCode:    httpx.CodeInternal,
			wantMessage: "Internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)

			translateError(c, tt.err)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			body := decode(t, rec)
			if body["code"] != tt.wantCode {
				t.Errorf("code = %v, want %v", body["code"], tt.wantCode)
			}
			if tt.wantMessage != "" && body["error"] != tt.wantMessage {
				t.Errorf("error = %v, want %v", body["error"], tt.wantMessage)
			}
			if got := rec.Header().Get("Retry-After"); got != tt.wantRetryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tt.wantRetryAfter)
			}
			if tt.name == "unknown error is opaque 500" && strings.Contains(rec.Body.String(), "db-secret-host") {
				t.Error("internal error detail leaked into the response")
			}
		})
	}
}
