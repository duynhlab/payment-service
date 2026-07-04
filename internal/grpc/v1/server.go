// Package v1 implements the gRPC transport for payment, version 1. It is a thin
// adapter over the logic layer (mirroring internal/web/v1) so the gRPC and HTTP
// paths share the same business logic. It serves the money operations the
// order-fulfillment saga calls: Authorize (pre-pivot hold), Capture (pre-confirm),
// and Void/Refund (compensations). Every RPC is keyed by order_id — the saga's
// natural business key — and idempotent.
package v1

import (
	"context"
	"errors"
	"fmt"

	paymentv1 "github.com/duynhlab/pkg/proto/payment/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/duynhlab/payment-service/internal/core/domain"
	logicv1 "github.com/duynhlab/payment-service/internal/logic/v1"
)

// sagaUserID is the unscoped owner used for internal saga calls (the gRPC
// surface is cluster-only, NetworkPolicy-fenced; there is no end-user JWT).
const sagaUserID = 0

// errMsgAmountRange is the InvalidArgument message shared by the amount checks
// on Authorize and Refund.
const errMsgAmountRange = "amount_minor must be positive and within the accepted maximum"

// paymentLogic is the logic-layer slice the gRPC server needs.
// *logicv1.Service satisfies it.
type paymentLogic interface {
	CreateIntent(ctx context.Context, idemKey string, in logicv1.CreateIntentInput) (*logicv1.IntentResult, error)
	GetByOrderID(ctx context.Context, orderID int64) (*domain.Payment, error)
	Capture(ctx context.Context, paymentID, userID int64) (*domain.Payment, error)
	Void(ctx context.Context, paymentID, userID int64) (*domain.Payment, error)
	CreateRefund(ctx context.Context, idemKey string, paymentID, userID, amountMinor int64, reason string) (*domain.Refund, bool, error)
}

// Server implements paymentv1.PaymentServiceServer.
type Server struct {
	paymentv1.UnimplementedPaymentServiceServer

	svc paymentLogic
}

// NewServer creates a gRPC PaymentService server backed by the logic service.
func NewServer(svc paymentLogic) *Server {
	return &Server{svc: svc}
}

// Authorize places (or returns) the hold for an order. Idempotent by order_id
// (key order:<id>). A provider decline is a normal response with status="failed"
// and a decline_code — not a gRPC error.
func (s *Server) Authorize(ctx context.Context, req *paymentv1.AuthorizeRequest) (*paymentv1.AuthorizeResponse, error) {
	if req.GetOrderId() <= 0 || req.GetUserId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "order_id and user_id must be positive")
	}
	// Enforce the same money invariants as the HTTP path (shared validators in
	// the logic layer, so they can never drift between transports): positive
	// amount within the ledger ceiling, a valid currency, and a PCI-safe token.
	if req.GetAmountMinor() <= 0 || req.GetAmountMinor() > logicv1.MaxAmountMinor {
		return nil, status.Error(codes.InvalidArgument, errMsgAmountRange)
	}
	currency := req.GetCurrency()
	if currency == "" {
		currency = "USD"
	}
	if !logicv1.IsCurrency(currency) {
		return nil, status.Error(codes.InvalidArgument, "currency must be a 3-letter uppercase code")
	}
	if !logicv1.IsTestToken(req.GetPaymentMethod()) {
		return nil, status.Error(codes.InvalidArgument, `payment_method must be an opaque "tok_" token`)
	}
	orderID := req.GetOrderId()
	res, err := s.svc.CreateIntent(ctx, fmt.Sprintf("order:%d", orderID), logicv1.CreateIntentInput{
		UserID:        req.GetUserId(),
		OrderID:       &orderID,
		AmountMinor:   req.GetAmountMinor(),
		Currency:      currency,
		CaptureMethod: domain.CaptureManual, // saga holds now, captures later
		PaymentMethod: req.GetPaymentMethod(),
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return &paymentv1.AuthorizeResponse{Payment: mapPayment(res.Payment)}, nil
}

// Capture captures the authorized hold. Idempotent: an already-captured payment
// returns unchanged.
func (s *Server) Capture(ctx context.Context, req *paymentv1.CaptureRequest) (*paymentv1.CaptureResponse, error) {
	pay, err := s.lookup(ctx, req.GetOrderId())
	if err != nil {
		return nil, err
	}
	captured, err := s.svc.Capture(ctx, pay.ID, sagaUserID)
	if err != nil {
		return nil, mapErr(err)
	}
	return &paymentv1.CaptureResponse{Payment: mapPayment(captured)}, nil
}

// Void releases the authorized hold (compensation). Idempotent.
func (s *Server) Void(ctx context.Context, req *paymentv1.VoidRequest) (*paymentv1.VoidResponse, error) {
	pay, err := s.lookup(ctx, req.GetOrderId())
	if err != nil {
		return nil, err
	}
	voided, err := s.svc.Void(ctx, pay.ID, sagaUserID)
	if err != nil {
		return nil, mapErr(err)
	}
	return &paymentv1.VoidResponse{Payment: mapPayment(voided)}, nil
}

// Refund returns captured money (compensation). Idempotent by the order
// (key refund:order:<id>).
func (s *Server) Refund(ctx context.Context, req *paymentv1.RefundRequest) (*paymentv1.RefundResponse, error) {
	if req.GetAmountMinor() <= 0 || req.GetAmountMinor() > logicv1.MaxAmountMinor {
		return nil, status.Error(codes.InvalidArgument, errMsgAmountRange)
	}
	pay, err := s.lookup(ctx, req.GetOrderId())
	if err != nil {
		return nil, err
	}
	ref, _, err := s.svc.CreateRefund(ctx, fmt.Sprintf("refund:order:%d", req.GetOrderId()),
		pay.ID, sagaUserID, req.GetAmountMinor(), req.GetReason())
	if err != nil {
		return nil, mapErr(err)
	}
	return &paymentv1.RefundResponse{Refund: mapRefund(ref)}, nil
}

// lookup resolves an order_id to its payment, validating the id first.
func (s *Server) lookup(ctx context.Context, orderID int64) (*domain.Payment, error) {
	if orderID <= 0 {
		return nil, status.Error(codes.InvalidArgument, "order_id must be positive")
	}
	pay, err := s.svc.GetByOrderID(ctx, orderID)
	if err != nil {
		return nil, mapErr(err)
	}
	return pay, nil
}

func mapPayment(p *domain.Payment) *paymentv1.Payment {
	var orderID int64
	if p.OrderID != nil {
		orderID = *p.OrderID
	}
	return &paymentv1.Payment{
		PaymentId:   p.ID,
		OrderId:     orderID,
		Status:      string(p.Status),
		AmountMinor: p.AmountMinor,
		Currency:    p.Currency,
		DeclineCode: p.DeclineCode,
	}
}

func mapRefund(r *domain.Refund) *paymentv1.Refund {
	return &paymentv1.Refund{
		RefundId:    r.ID,
		PaymentId:   r.PaymentID,
		Status:      string(r.Status),
		AmountMinor: r.AmountMinor,
	}
}

// mapErr maps logic/domain errors to gRPC status codes so the saga can tell a
// business rejection (don't retry forever) from an infra error (retryable).
func mapErr(err error) error {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, "payment not found")
	case errors.Is(err, domain.ErrKeyConflict):
		return status.Error(codes.Aborted, "idempotency conflict")
	case errors.Is(err, domain.ErrKeyLocked):
		return status.Error(codes.Aborted, "operation in flight")
	case errors.Is(err, domain.ErrPaymentExists):
		return status.Error(codes.AlreadyExists, "order already has a payment")
	case errors.Is(err, domain.ErrInvalidTransition), errors.Is(err, domain.ErrStaleTransition):
		return status.Error(codes.FailedPrecondition, "invalid payment state transition")
	case errors.Is(err, domain.ErrRefundRejected):
		return status.Error(codes.FailedPrecondition, "refund rejected")
	default:
		return status.Error(codes.Internal, "payment operation failed")
	}
}
