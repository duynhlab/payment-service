package v1

import (
	"context"
	"errors"
	"testing"

	paymentv1 "github.com/duynhlab/pkg/proto/payment/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/duynhlab/payment-service/internal/core/domain"
	logicv1 "github.com/duynhlab/payment-service/internal/logic/v1"
)

var errTest = errors.New("boom")

// fakeLogic is a configurable paymentLogic double; it records the idempotency
// key + input so tests can assert the natural-key derivation.
type fakeLogic struct {
	intent     *logicv1.IntentResult
	intentErr  error
	byOrder    *domain.Payment
	byOrderErr error
	captured   *domain.Payment
	captureErr error
	voided     *domain.Payment
	voidErr    error
	refund     *domain.Refund
	refundErr  error

	gotIdemKey  string
	gotInput    logicv1.CreateIntentInput
	gotRefundID string

	// captured call args, to lock the sagaUserID=0 + looked-up-paymentID wiring
	gotCapPayID, gotCapUserID       int64
	gotRefundPayID, gotRefundUserID int64
	gotByOrderID                    int64
}

func (f *fakeLogic) CreateIntent(_ context.Context, idemKey string, in logicv1.CreateIntentInput) (*logicv1.IntentResult, error) {
	f.gotIdemKey, f.gotInput = idemKey, in
	return f.intent, f.intentErr
}
func (f *fakeLogic) GetByOrderID(_ context.Context, orderID int64) (*domain.Payment, error) {
	f.gotByOrderID = orderID
	return f.byOrder, f.byOrderErr
}
func (f *fakeLogic) Capture(_ context.Context, paymentID, userID int64) (*domain.Payment, error) {
	f.gotCapPayID, f.gotCapUserID = paymentID, userID
	return f.captured, f.captureErr
}
func (f *fakeLogic) Void(_ context.Context, _, _ int64) (*domain.Payment, error) {
	return f.voided, f.voidErr
}
func (f *fakeLogic) CreateRefund(_ context.Context, idemKey string, paymentID, userID, _ int64, _ string) (*domain.Refund, bool, error) {
	f.gotRefundID, f.gotRefundPayID, f.gotRefundUserID = idemKey, paymentID, userID
	return f.refund, false, f.refundErr
}

func authReq() *paymentv1.AuthorizeRequest {
	return &paymentv1.AuthorizeRequest{OrderId: 42, UserId: 7, AmountMinor: 5000, Currency: "USD", PaymentMethod: "tok_visa"}
}

func TestAuthorize_OK(t *testing.T) {
	oid := int64(42)
	f := &fakeLogic{intent: &logicv1.IntentResult{Code: 201, Payment: &domain.Payment{
		ID: 1, OrderID: &oid, Status: domain.StatusAuthorized, AmountMinor: 5000, Currency: "USD"}}}
	resp, err := NewServer(f).Authorize(context.Background(), authReq())
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	if resp.GetPayment().GetStatus() != "authorized" || resp.GetPayment().GetOrderId() != 42 {
		t.Fatalf("bad payment %+v", resp.GetPayment())
	}
	if f.gotIdemKey != "order:42" {
		t.Fatalf("idem key must be order:42, got %q", f.gotIdemKey)
	}
	if f.gotInput.CaptureMethod != domain.CaptureManual {
		t.Fatalf("saga authorize must be manual capture, got %q", f.gotInput.CaptureMethod)
	}
}

func TestAuthorize_DeclineIsNotAnError(t *testing.T) {
	oid := int64(42)
	f := &fakeLogic{intent: &logicv1.IntentResult{Code: 422, Payment: &domain.Payment{
		ID: 1, OrderID: &oid, Status: domain.StatusFailed, DeclineCode: "insufficient_funds"}}}
	resp, err := NewServer(f).Authorize(context.Background(), authReq())
	if err != nil {
		t.Fatalf("decline must be a normal response, got err %v", err)
	}
	if resp.GetPayment().GetStatus() != "failed" || resp.GetPayment().GetDeclineCode() != "insufficient_funds" {
		t.Fatalf("decline must carry status+code: %+v", resp.GetPayment())
	}
}

func TestAuthorize_Validation(t *testing.T) {
	// The gRPC path must enforce the same money invariants as the HTTP path:
	// positive ids, amount within the ledger ceiling, valid currency, PCI-safe token.
	for _, r := range []*paymentv1.AuthorizeRequest{
		{OrderId: 0, UserId: 7, AmountMinor: 1, PaymentMethod: "tok_visa"},
		{OrderId: 1, UserId: 0, AmountMinor: 1, PaymentMethod: "tok_visa"},
		{OrderId: 1, UserId: 7, AmountMinor: 0, PaymentMethod: "tok_visa"},
		{OrderId: 1, UserId: 7, AmountMinor: logicv1.MaxAmountMinor + 1, PaymentMethod: "tok_visa"}, // over ceiling
		{OrderId: 1, UserId: 7, AmountMinor: 5000, Currency: "US", PaymentMethod: "tok_visa"},       // bad currency
		{OrderId: 1, UserId: 7, AmountMinor: 5000, PaymentMethod: "tok_4111111111111111"},           // PAN, not a token
		{OrderId: 1, UserId: 7, AmountMinor: 5000, PaymentMethod: ""},                               // missing token
	} {
		if _, err := NewServer(&fakeLogic{}).Authorize(context.Background(), r); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("want InvalidArgument for %+v, got %v", r, err)
		}
	}
}

func TestAuthorize_DefaultsCurrencyToUSD(t *testing.T) {
	oid := int64(42)
	f := &fakeLogic{intent: &logicv1.IntentResult{Code: 201, Payment: &domain.Payment{
		ID: 1, OrderID: &oid, Status: domain.StatusAuthorized}}}
	req := &paymentv1.AuthorizeRequest{OrderId: 42, UserId: 7, AmountMinor: 5000, Currency: "", PaymentMethod: "tok_visa"}
	if _, err := NewServer(f).Authorize(context.Background(), req); err != nil {
		t.Fatalf("authorize: %v", err)
	}
	if f.gotInput.Currency != "USD" {
		t.Fatalf("empty currency must default to USD, got %q", f.gotInput.Currency)
	}
}

func TestAuthorize_ErrorMapping(t *testing.T) {
	tests := []struct {
		err  error
		code codes.Code
	}{
		{domain.ErrKeyConflict, codes.Aborted},
		{domain.ErrKeyLocked, codes.Aborted},
		{domain.ErrPaymentExists, codes.AlreadyExists},
		{errTest, codes.Internal},
	}
	for _, tt := range tests {
		f := &fakeLogic{intentErr: tt.err}
		if _, err := NewServer(f).Authorize(context.Background(), authReq()); status.Code(err) != tt.code {
			t.Fatalf("err %v → want %v, got %v", tt.err, tt.code, status.Code(err))
		}
	}
}

func TestCapture(t *testing.T) {
	oid := int64(42)
	pay := &domain.Payment{ID: 1, OrderID: &oid, Status: domain.StatusAuthorized}
	t.Run("ok", func(t *testing.T) {
		f := &fakeLogic{byOrder: pay, captured: &domain.Payment{ID: 1, OrderID: &oid, Status: domain.StatusCaptured}}
		resp, err := NewServer(f).Capture(context.Background(), &paymentv1.CaptureRequest{OrderId: 42})
		if err != nil || resp.GetPayment().GetStatus() != "captured" {
			t.Fatalf("capture: %v %+v", err, resp.GetPayment())
		}
		if f.gotCapPayID != pay.ID || f.gotCapUserID != 0 {
			t.Fatalf("capture must use the looked-up payment id as the unscoped saga owner, got id=%d user=%d", f.gotCapPayID, f.gotCapUserID)
		}
	})
	t.Run("unknown order → NotFound", func(t *testing.T) {
		f := &fakeLogic{byOrderErr: domain.ErrNotFound}
		if _, err := NewServer(f).Capture(context.Background(), &paymentv1.CaptureRequest{OrderId: 42}); status.Code(err) != codes.NotFound {
			t.Fatalf("want NotFound, got %v", status.Code(err))
		}
	})
	t.Run("invalid transition → FailedPrecondition", func(t *testing.T) {
		f := &fakeLogic{byOrder: pay, captureErr: domain.ErrInvalidTransition}
		if _, err := NewServer(f).Capture(context.Background(), &paymentv1.CaptureRequest{OrderId: 42}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("want FailedPrecondition, got %v", status.Code(err))
		}
	})
	t.Run("bad order_id → InvalidArgument", func(t *testing.T) {
		if _, err := NewServer(&fakeLogic{}).Capture(context.Background(), &paymentv1.CaptureRequest{OrderId: 0}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("want InvalidArgument, got %v", status.Code(err))
		}
	})
}

func TestVoid(t *testing.T) {
	oid := int64(42)
	t.Run("ok", func(t *testing.T) {
		f := &fakeLogic{byOrder: &domain.Payment{ID: 1, OrderID: &oid}, voided: &domain.Payment{ID: 1, OrderID: &oid, Status: domain.StatusVoided}}
		resp, err := NewServer(f).Void(context.Background(), &paymentv1.VoidRequest{OrderId: 42})
		if err != nil || resp.GetPayment().GetStatus() != "voided" {
			t.Fatalf("void: %v %+v", err, resp.GetPayment())
		}
	})
	t.Run("unknown order → NotFound", func(t *testing.T) {
		f := &fakeLogic{byOrderErr: domain.ErrNotFound}
		if _, err := NewServer(f).Void(context.Background(), &paymentv1.VoidRequest{OrderId: 42}); status.Code(err) != codes.NotFound {
			t.Fatalf("want NotFound, got %v", status.Code(err))
		}
	})
	t.Run("invalid transition → FailedPrecondition", func(t *testing.T) {
		f := &fakeLogic{byOrder: &domain.Payment{ID: 1, OrderID: &oid}, voidErr: domain.ErrInvalidTransition}
		if _, err := NewServer(f).Void(context.Background(), &paymentv1.VoidRequest{OrderId: 42}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("want FailedPrecondition, got %v", status.Code(err))
		}
	})
}

func TestRefund(t *testing.T) {
	oid := int64(42)
	pay := &domain.Payment{ID: 9, OrderID: &oid, Status: domain.StatusCaptured}
	t.Run("ok + natural key", func(t *testing.T) {
		f := &fakeLogic{byOrder: pay, refund: &domain.Refund{ID: 3, PaymentID: 9, Status: domain.RefundSucceeded, AmountMinor: 1000}}
		resp, err := NewServer(f).Refund(context.Background(), &paymentv1.RefundRequest{OrderId: 42, AmountMinor: 1000, Reason: "x"})
		if err != nil || resp.GetRefund().GetRefundId() != 3 || resp.GetRefund().GetPaymentId() != 9 {
			t.Fatalf("refund: %v %+v", err, resp.GetRefund())
		}
		if f.gotRefundID != "refund:order:42" {
			t.Fatalf("refund key must be refund:order:42, got %q", f.gotRefundID)
		}
		if f.gotRefundPayID != pay.ID || f.gotRefundUserID != 0 {
			t.Fatalf("refund must use the looked-up payment id as the unscoped saga owner, got id=%d user=%d", f.gotRefundPayID, f.gotRefundUserID)
		}
	})
	t.Run("rejected → FailedPrecondition", func(t *testing.T) {
		f := &fakeLogic{byOrder: pay, refundErr: domain.ErrRefundRejected}
		if _, err := NewServer(f).Refund(context.Background(), &paymentv1.RefundRequest{OrderId: 42, AmountMinor: 1000}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("want FailedPrecondition, got %v", status.Code(err))
		}
	})
	t.Run("unknown order → NotFound", func(t *testing.T) {
		f := &fakeLogic{byOrderErr: domain.ErrNotFound}
		if _, err := NewServer(f).Refund(context.Background(), &paymentv1.RefundRequest{OrderId: 42, AmountMinor: 1000}); status.Code(err) != codes.NotFound {
			t.Fatalf("want NotFound, got %v", status.Code(err))
		}
	})
	t.Run("bad amount → InvalidArgument", func(t *testing.T) {
		for _, amt := range []int64{0, logicv1.MaxAmountMinor + 1} {
			if _, err := NewServer(&fakeLogic{}).Refund(context.Background(), &paymentv1.RefundRequest{OrderId: 42, AmountMinor: amt}); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("want InvalidArgument for amount %d, got %v", amt, status.Code(err))
			}
		}
	})
}

func TestGetPayment(t *testing.T) {
	oid := int64(42)
	t.Run("ok", func(t *testing.T) {
		f := &fakeLogic{byOrder: &domain.Payment{ID: 9, OrderID: &oid, Status: domain.StatusCaptured, AmountMinor: 2550, Currency: "USD", RefundedMinor: 500}}
		resp, err := NewServer(f).GetPayment(context.Background(), &paymentv1.GetPaymentRequest{OrderId: 42})
		if err != nil {
			t.Fatalf("get payment: %v", err)
		}
		if f.gotByOrderID != 42 {
			t.Fatalf("must forward order_id 42, got %d", f.gotByOrderID)
		}
		p := resp.GetPayment()
		if p.GetPaymentId() != 9 || p.GetOrderId() != 42 || p.GetStatus() != "captured" || p.GetAmountMinor() != 2550 {
			t.Fatalf("bad payment %+v", p)
		}
		if p.GetCurrency() != "USD" || p.GetRefundedMinor() != 500 {
			t.Fatalf("currency/refunded = %q/%d, want USD/500 (partial refund must be derivable)", p.GetCurrency(), p.GetRefundedMinor())
		}
	})
	t.Run("no payment → NotFound", func(t *testing.T) {
		f := &fakeLogic{byOrderErr: domain.ErrNotFound}
		if _, err := NewServer(f).GetPayment(context.Background(), &paymentv1.GetPaymentRequest{OrderId: 42}); status.Code(err) != codes.NotFound {
			t.Fatalf("want NotFound, got %v", status.Code(err))
		}
	})
	t.Run("bad order_id → InvalidArgument", func(t *testing.T) {
		if _, err := NewServer(&fakeLogic{}).GetPayment(context.Background(), &paymentv1.GetPaymentRequest{OrderId: 0}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("want InvalidArgument, got %v", status.Code(err))
		}
	})
}
