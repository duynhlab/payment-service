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
	CreateRefund(ctx context.Context, paymentID, amountMinor int64, reason, idemKey string) (*domain.Refund, error)
	SettleRefund(ctx context.Context, refundID int64, status domain.RefundStatus, providerRefundID string) error
}

// IdemRepo is the idempotency-key port (implemented by
// repository.IdempotencyRepository).
type IdemRepo interface {
	Claim(ctx context.Context, userID int64, key, method, path, hash string) (*domain.IdempotencyKey, bool, error)
	Checkpoint(ctx context.Context, id int64, paymentID *int64) error
	Release(ctx context.Context, id int64) error
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

// hashJSON canonicalizes a request struct (marshal → sha256 → hex) for
// same-key-different-body idempotency detection.
func hashJSON(v any) string {
	b, _ := json.Marshal(v) // struct of scalars — cannot fail
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// CreateIntent runs the recovery-point idempotent authorize flow:
//
//	claim key -> ensure payment row (checkpoint: payment_id) ->
//	provider charge OUTSIDE any tx (safe: same key passed through) ->
//	checkpoint provider_called -> apply outcome + cache response (finish).
//
// A takeover after a crash re-enters at the recorded checkpoint; a transient
// provider error leaves the key unfinished so the client retry re-drives it.
func (s *Service) CreateIntent(ctx context.Context, idemKey string, in CreateIntentInput) (*IntentResult, error) {
	key, proceed, err := s.idem.Claim(ctx, in.UserID, idemKey, "POST", "/payment/v1/private/payments", hashJSON(in))
	if err != nil {
		return nil, err
	}
	if !proceed {
		return replayResult(key)
	}

	// Checkpoint 1: ensure the payment row exists (re-entry reuses it).
	if key.PaymentID != nil {
		pay, err := s.payments.FindByID(ctx, *key.PaymentID, 0)
		if err != nil {
			return nil, err
		}
		return s.driveCharge(ctx, key, in, pay)
	}

	pay, err := s.payments.Create(ctx, &domain.Payment{
		UserID:        in.UserID,
		OrderID:       in.OrderID,
		AmountMinor:   in.AmountMinor,
		Currency:      in.Currency,
		CaptureMethod: in.CaptureMethod,
		PaymentMethod: in.PaymentMethod,
	})
	if errors.Is(err, domain.ErrPaymentExists) && in.OrderID != nil {
		return s.adoptExistingOrderPayment(ctx, key, in, err)
	}
	if err != nil {
		return nil, err
	}
	if err := s.idem.Checkpoint(ctx, key.ID, &pay.ID); err != nil {
		return nil, err
	}
	return s.driveCharge(ctx, key, in, pay)
}

// adoptExistingOrderPayment handles the crash-recovery case where a prior
// attempt created the order's payment but died before checkpointing it on the
// key. It adopts the payment only if it is genuinely ours; a foreign owner or
// amount mismatch is a real conflict (createErr, → 409). A payment already
// past pending was charged on the first attempt, so it finishes idempotently
// by its current state and NEVER charges again.
func (s *Service) adoptExistingOrderPayment(ctx context.Context, key *domain.IdempotencyKey, in CreateIntentInput, createErr error) (*IntentResult, error) {
	existing, findErr := s.payments.FindByOrderID(ctx, *in.OrderID)
	if findErr != nil || existing.UserID != in.UserID || existing.AmountMinor != in.AmountMinor {
		return nil, createErr
	}
	if existing.Status != domain.StatusPending {
		code := 201
		if existing.Status == domain.StatusFailed {
			code = 422
		}
		return s.finishIntent(ctx, key.ID, code, existing.ID)
	}
	if err := s.idem.Checkpoint(ctx, key.ID, &existing.ID); err != nil {
		return nil, err
	}
	return s.driveCharge(ctx, key, in, existing)
}

// driveCharge runs the provider call and applies the resulting state
// transitions for a pending payment, caching the outcome on the key.
func (s *Service) driveCharge(ctx context.Context, key *domain.IdempotencyKey, in CreateIntentInput, pay *domain.Payment) (*IntentResult, error) {
	// Provider call — outside any transaction; the shared idempotency key
	// makes a re-driven call replay instead of double-charging.
	charge, chErr := s.prov.Charge(ctx, provider.ChargeRequest{
		IdempotencyKey: fmt.Sprintf("%d:%s", in.UserID, key.Key),
		AmountMinor:    in.AmountMinor,
		Currency:       in.Currency,
		PaymentMethod:  in.PaymentMethod,
		AutoCapture:    in.CaptureMethod == domain.CaptureAutomatic,
	})

	var declined *provider.DeclinedError
	switch {
	case errors.As(chErr, &declined):
		// A re-driven decline hits an already-failed row: stale is fine here —
		// the outcome is identical, only the cache write remains.
		if err := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusPending, domain.StatusFailed,
			map[string]any{"decline_code": declined.Code}); err != nil && !errors.Is(err, domain.ErrStaleTransition) {
			return nil, err
		}
		return s.finishIntent(ctx, key.ID, 422, pay.ID)
	case chErr != nil:
		// Transient: release the lock so an immediate same-key retry can
		// re-drive, instead of getting ErrKeyLocked until the 90s takeover
		// window elapses — the 503 tells the client to retry, so a retry must
		// be able to make progress. The payment row stays pending; the
		// re-driven charge replays or succeeds.
		if relErr := s.idem.Release(ctx, key.ID); relErr != nil {
			return nil, fmt.Errorf("provider transient (%w); lock release failed: %w", chErr, relErr)
		}
		return nil, chErr
	}

	// Apply the successful outcome through the whitelisted transitions.
	expires := s.now().Add(s.holdTTL)
	if err := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusPending, domain.StatusAuthorized,
		map[string]any{
			"provider_payment_id": charge.ProviderPaymentID,
			"authorized_at":       s.now(),
			"expires_at":          expires,
		}); err != nil && !errors.Is(err, domain.ErrStaleTransition) {
		return nil, err // stale = re-entry already applied it; verified below
	}
	if in.CaptureMethod == domain.CaptureAutomatic {
		if err := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusAuthorized, domain.StatusCaptured,
			map[string]any{"captured_at": s.now()}); err != nil && !errors.Is(err, domain.ErrStaleTransition) {
			return nil, err
		}
	}
	// A tolerated stale has two possible causes: a re-entry that already
	// applied the outcome (fine) or the expiry job racing us (not fine — the
	// charge succeeded but the row says expired). Never cache a success for a
	// row that is not actually in a successful state.
	final, err := s.payments.FindByID(ctx, pay.ID, 0)
	if err != nil {
		return nil, err
	}
	if final.Status != domain.StatusAuthorized && final.Status != domain.StatusCaptured {
		return nil, fmt.Errorf("%w: charge succeeded but payment is %s", domain.ErrInvalidTransition, final.Status)
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

func replayResult(key *domain.IdempotencyKey) (*IntentResult, error) {
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
	// CAS FIRST, provider second: winning the row before moving money means a
	// concurrent void/expiry can never leave the provider captured while the
	// row says otherwise. If the provider then fails, compensate the row back
	// (deliberately bypassing the whitelist — this is a rollback, not a
	// business transition).
	if err := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusAuthorized, domain.StatusCaptured,
		map[string]any{"captured_at": s.now()}); err != nil {
		if errors.Is(err, domain.ErrStaleTransition) {
			return s.reloadAfterRace(ctx, pay.ID, domain.StatusCaptured)
		}
		return nil, err
	}
	if err := s.prov.Capture(ctx, pay.ProviderPaymentID); err != nil {
		// Roll the row back and clear the captured_at stamp we just set.
		if rbErr := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusCaptured, domain.StatusAuthorized,
			map[string]any{"captured_at": nil}); rbErr != nil {
			return nil, fmt.Errorf("provider capture failed (%w) and rollback failed: %w", err, rbErr)
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
	// Same ordering rationale as Capture: win the row, then touch the provider.
	if err := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusAuthorized, domain.StatusVoided, nil); err != nil {
		if errors.Is(err, domain.ErrStaleTransition) {
			return s.reloadAfterRace(ctx, pay.ID, domain.StatusVoided)
		}
		return nil, err
	}
	if err := s.prov.Void(ctx, pay.ProviderPaymentID); err != nil {
		if rbErr := s.payments.TransitionStatus(ctx, pay.ID, domain.StatusVoided, domain.StatusAuthorized, nil); rbErr != nil {
			return nil, fmt.Errorf("provider void failed (%w) and rollback failed: %w", err, rbErr)
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

	key, proceed, err := s.idem.Claim(ctx, userID, idemKey, "POST",
		fmt.Sprintf("/payment/v1/internal/payments/%d/refunds", paymentID), hashJSON(in))
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
	// The refund insert is idempotent by this scoped key: a crash-recovery
	// retry adopts the existing refund rather than creating a second one.
	scopedKey := fmt.Sprintf("%d:%s", userID, idemKey)
	ref, err := s.payments.CreateRefund(ctx, pay.ID, amountMinor, reason, scopedKey)
	if err != nil {
		return nil, false, err // incl. domain.ErrRefundRejected
	}

	// An adopted refund that already settled (crash after settle, before finish)
	// must not be re-sent to the provider or re-settled — just finish.
	if ref.Status == domain.RefundPending {
		providerRefundID, provErr := s.prov.Refund(ctx, pay.ProviderPaymentID, amountMinor, scopedKey)
		status := domain.RefundSucceeded
		if provErr != nil {
			status = domain.RefundFailed
		}
		if err := s.payments.SettleRefund(ctx, ref.ID, status, providerRefundID); err != nil {
			return nil, false, err
		}
		ref.Status = status
		ref.ProviderRefundID = providerRefundID
	}

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
