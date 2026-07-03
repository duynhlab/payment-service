// Package v1 implements the payment business logic: the idempotent
// authorize/capture/void/refund flows on top of the domain state machine.
// It owns the port interfaces; web/v1 translates HTTP, core implements
// persistence and the provider — strict 3-layer direction.
package v1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
	"github.com/duynhlab/payment-service/internal/core/repository"
)

// PaymentRepo is the persistence port the logic layer needs (implemented by
// repository.PaymentRepository).
type PaymentRepo interface {
	Create(ctx context.Context, p *domain.Payment) (*domain.Payment, error)
	FindByID(ctx context.Context, id, userID int64) (*domain.Payment, error)
	FindByOrderID(ctx context.Context, orderID int64) (*domain.Payment, error)
	ListByUser(ctx context.Context, userID int64, limit, offset int) ([]domain.Payment, int, error)
	TransitionStatus(ctx context.Context, id int64, from, to domain.Status, set map[string]any) error
	ExpireStaleAuthorizations(ctx context.Context, now time.Time) (int64, error)
	CreateRefund(ctx context.Context, paymentID, amountMinor int64, reason string) (*domain.Refund, error)
	SettleRefund(ctx context.Context, refundID int64, status domain.RefundStatus, providerRefundID string) error
}

// IdemRepo is the idempotency-key port (implemented by
// repository.IdempotencyRepository).
type IdemRepo interface {
	Claim(ctx context.Context, userID int64, key, method, path, hash string) (*repository.IdempotencyKey, bool, error)
	Advance(ctx context.Context, id int64, point string, paymentID *int64) error
	Finish(ctx context.Context, id int64, code int, body []byte) error
	Reap(ctx context.Context, ttl time.Duration) (int64, error)
}

// Service is the payment logic. AuthHoldTTL bounds authorized holds.
type Service struct {
	payments PaymentRepo
	idem     IdemRepo
	prov     provider.Provider
	holdTTL  time.Duration
	now      func() time.Time
}

// NewService wires the logic layer onto its ports.
func NewService(p PaymentRepo, i IdemRepo, prov provider.Provider, holdTTL time.Duration) *Service {
	return &Service{payments: p, idem: i, prov: prov, holdTTL: holdTTL, now: time.Now}
}

// CreateIntentInput is the validated request to create a PaymentIntent. The
// json tags define the canonical shape hashed for idempotency comparison.
type CreateIntentInput struct {
	UserID        int64                `json:"user_id"`
	OrderID       *int64               `json:"order_id,omitempty"`
	AmountMinor   int64                `json:"amount_minor"`
	Currency      string               `json:"currency"`
	CaptureMethod domain.CaptureMethod `json:"capture_method"`
	PaymentMethod string               `json:"payment_method"`
}

// IntentResult is the outcome of an idempotent CreateIntent: the HTTP-ish
// status (201 created / 422 declined) plus the payment snapshot. Replayed
// results come verbatim from the idempotency cache.
type IntentResult struct {
	Code     int
	Payment  *domain.Payment
	Replayed bool
}

// hashInput canonicalizes the request for same-key-different-body detection.
func hashInput(in CreateIntentInput) string {
	b, _ := json.Marshal(in) // struct of scalars — cannot fail
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// CreateIntent runs the recovery-point idempotent authorize flow (RFC-0010):
//
//	claim key -> ensure payment row (checkpoint: payment_id) ->
//	provider charge OUTSIDE any tx (safe: same key passed through) ->
//	checkpoint provider_called -> apply outcome + cache response (finish).
//
// A takeover after a crash re-enters at the recorded checkpoint; a transient
// provider error leaves the key unfinished so the client retry re-drives it.
func (s *Service) CreateIntent(ctx context.Context, idemKey string, in CreateIntentInput) (*IntentResult, error) {
	key, proceed, err := s.idem.Claim(ctx, in.UserID, idemKey, "POST", "/payment/v1/private/payments", hashInput(in))
	if err != nil {
		return nil, err
	}
	if !proceed {
		return replayResult(key)
	}

	// Checkpoint 1: ensure the payment row exists (re-entry reuses it).
	var pay *domain.Payment
	if key.PaymentID != nil {
		if pay, err = s.payments.FindByID(ctx, *key.PaymentID, 0); err != nil {
			return nil, err
		}
	} else {
		pay, err = s.payments.Create(ctx, &domain.Payment{
			UserID:        in.UserID,
			OrderID:       in.OrderID,
			AmountMinor:   in.AmountMinor,
			Currency:      in.Currency,
			CaptureMethod: in.CaptureMethod,
			PaymentMethod: in.PaymentMethod,
		})
		if err != nil {
			return nil, err // incl. repository.ErrPaymentExists
		}
		if err := s.idem.Advance(ctx, key.ID, repository.RecoveryStarted, &pay.ID); err != nil {
			return nil, err
		}
	}

	// Provider call — outside any transaction; the shared idempotency key
	// makes a re-driven call replay instead of double-charging.
	charge, chErr := s.prov.Charge(ctx, provider.ChargeRequest{
		IdempotencyKey: fmt.Sprintf("%d:%s", in.UserID, idemKey),
		AmountMinor:    in.AmountMinor,
		Currency:       in.Currency,
		PaymentMethod:  in.PaymentMethod,
		AutoCapture:    in.CaptureMethod == domain.CaptureAutomatic,
	})

	var declined *provider.DeclinedError
	switch {
	case errors.As(chErr, &declined):
		if err := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusPending, domain.StatusFailed,
			map[string]any{"decline_code": declined.Code}); err != nil {
			return nil, err
		}
		return s.finishIntent(ctx, key.ID, 422, pay.ID)
	case chErr != nil:
		// Transient: key stays unfinished — the retry takes over and re-drives.
		return nil, chErr
	}

	if err := s.idem.Advance(ctx, key.ID, repository.RecoveryProviderCalled, nil); err != nil {
		return nil, err
	}

	// Apply the successful outcome through the whitelisted transitions.
	expires := s.now().Add(s.holdTTL)
	if err := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusPending, domain.StatusAuthorized,
		map[string]any{
			"provider_payment_id": charge.ProviderPaymentID,
			"authorized_at":       s.now(),
			"expires_at":          expires,
		}); err != nil && !errors.Is(err, repository.ErrStaleTransition) {
		return nil, err // stale = re-entry already applied it; continue
	}
	if in.CaptureMethod == domain.CaptureAutomatic {
		if err := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusAuthorized, domain.StatusCaptured,
			map[string]any{"captured_at": s.now()}); err != nil && !errors.Is(err, repository.ErrStaleTransition) {
			return nil, err
		}
	}
	return s.finishIntent(ctx, key.ID, 201, pay.ID)
}

// finishIntent snapshots the payment, caches it on the key, and returns it.
func (s *Service) finishIntent(ctx context.Context, keyID int64, code int, paymentID int64) (*IntentResult, error) {
	pay, err := s.payments.FindByID(ctx, paymentID, 0)
	if err != nil {
		return nil, err
	}
	body, _ := json.Marshal(pay) // domain struct — cannot fail
	if err := s.idem.Finish(ctx, keyID, code, body); err != nil {
		return nil, err
	}
	return &IntentResult{Code: code, Payment: pay}, nil
}

func replayResult(key *repository.IdempotencyKey) (*IntentResult, error) {
	var pay domain.Payment
	if err := json.Unmarshal(key.ResponseBody, &pay); err != nil {
		return nil, fmt.Errorf("corrupt idempotency cache: %w", err)
	}
	return &IntentResult{Code: *key.ResponseCode, Payment: &pay, Replayed: true}, nil
}

// Capture moves an authorized hold to captured. Idempotent: capturing an
// already-captured payment returns it unchanged.
func (s *Service) Capture(ctx context.Context, paymentID, userID int64) (*domain.Payment, error) {
	pay, err := s.payments.FindByID(ctx, paymentID, userID)
	if err != nil {
		return nil, err
	}
	if pay.Status == domain.StatusCaptured {
		return pay, nil // idempotent no-op
	}
	if err := domain.Transition(pay.Status, domain.StatusCaptured); err != nil {
		return nil, err
	}
	if err := s.prov.Capture(ctx, pay.ProviderPaymentID); err != nil {
		return nil, err
	}
	if err := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusAuthorized, domain.StatusCaptured,
		map[string]any{"captured_at": s.now()}); err != nil {
		if errors.Is(err, repository.ErrStaleTransition) {
			return s.reloadAfterRace(ctx, pay.ID, domain.StatusCaptured)
		}
		return nil, err
	}
	return s.payments.FindByID(ctx, pay.ID, 0)
}

// Void releases an authorized hold. Idempotent on already-voided payments.
func (s *Service) Void(ctx context.Context, paymentID, userID int64) (*domain.Payment, error) {
	pay, err := s.payments.FindByID(ctx, paymentID, userID)
	if err != nil {
		return nil, err
	}
	if pay.Status == domain.StatusVoided {
		return pay, nil
	}
	if err := domain.Transition(pay.Status, domain.StatusVoided); err != nil {
		return nil, err
	}
	if err := s.prov.Void(ctx, pay.ProviderPaymentID); err != nil {
		return nil, err
	}
	if err := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusAuthorized, domain.StatusVoided, nil); err != nil {
		if errors.Is(err, repository.ErrStaleTransition) {
			return s.reloadAfterRace(ctx, pay.ID, domain.StatusVoided)
		}
		return nil, err
	}
	return s.payments.FindByID(ctx, pay.ID, 0)
}

// reloadAfterRace re-reads after a lost CAS: if the payment reached the
// desired state anyway (concurrent duplicate), that's idempotent success;
// any other state is a real conflict.
func (s *Service) reloadAfterRace(ctx context.Context, id int64, want domain.Status) (*domain.Payment, error) {
	pay, err := s.payments.FindByID(ctx, id, 0)
	if err != nil {
		return nil, err
	}
	if pay.Status == want {
		return pay, nil
	}
	return nil, fmt.Errorf("%w: payment is %s", domain.ErrInvalidTransition, pay.Status)
}

// CreateRefund runs the idempotent (partial) refund flow. P1 settles
// synchronously against the provider stub; async webhook settlement is P2.
func (s *Service) CreateRefund(ctx context.Context, idemKey string, paymentID, userID, amountMinor int64, reason string) (*domain.Refund, bool, error) {
	in := struct {
		PaymentID int64  `json:"payment_id"`
		UserID    int64  `json:"user_id"`
		Amount    int64  `json:"amount"`
		Reason    string `json:"reason"`
	}{paymentID, userID, amountMinor, reason}
	b, _ := json.Marshal(in)
	sum := sha256.Sum256(b)

	key, proceed, err := s.idem.Claim(ctx, userID, idemKey, "POST",
		fmt.Sprintf("/payment/v1/internal/payments/%d/refunds", paymentID), hex.EncodeToString(sum[:]))
	if err != nil {
		return nil, false, err
	}
	if !proceed {
		var ref domain.Refund
		if err := json.Unmarshal(key.ResponseBody, &ref); err != nil {
			return nil, false, fmt.Errorf("corrupt idempotency cache: %w", err)
		}
		return &ref, true, nil
	}

	pay, err := s.payments.FindByID(ctx, paymentID, 0)
	if err != nil {
		return nil, false, err
	}
	ref, err := s.payments.CreateRefund(ctx, pay.ID, amountMinor, reason)
	if err != nil {
		return nil, false, err // incl. repository.ErrRefundRejected
	}

	providerRefundID, err := s.prov.Refund(ctx, pay.ProviderPaymentID, amountMinor,
		fmt.Sprintf("%d:%s", userID, idemKey))
	status := domain.RefundSucceeded
	if err != nil {
		status = domain.RefundFailed
	}
	if err := s.payments.SettleRefund(ctx, ref.ID, status, providerRefundID); err != nil {
		return nil, false, err
	}
	ref.Status = status
	ref.ProviderRefundID = providerRefundID

	body, _ := json.Marshal(ref)
	if err := s.idem.Finish(ctx, key.ID, 201, body); err != nil {
		return nil, false, err
	}
	return ref, false, nil
}

// Get returns one payment scoped to its owner.
func (s *Service) Get(ctx context.Context, id, userID int64) (*domain.Payment, error) {
	return s.payments.FindByID(ctx, id, userID)
}

// List returns a page of the user's payments and the total count.
func (s *Service) List(ctx context.Context, userID int64, page, pageSize int) ([]domain.Payment, int, error) {
	return s.payments.ListByUser(ctx, userID, pageSize, (page-1)*pageSize)
}

// ExpireHolds flips stale authorized holds to expired; the cron entrypoint.
func (s *Service) ExpireHolds(ctx context.Context) (int64, error) {
	return s.payments.ExpireStaleAuthorizations(ctx, s.now())
}

// ReapIdempotencyKeys removes keys past ttl (24h default).
func (s *Service) ReapIdempotencyKeys(ctx context.Context, ttl time.Duration) (int64, error) {
	return s.idem.Reap(ctx, ttl)
}
