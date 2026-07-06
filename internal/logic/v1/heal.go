package v1

import (
	"context"
	"errors"
	"time"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

// LedgerCapturer is the repo capability heal needs: the CAS-guarded, idempotent
// capture-with-ledger, plus a read to verify the post-state when the CAS is lost.
// *repository.PaymentRepository satisfies it. CaptureWithLedger moves a payment
// authorized→captured and posts the balanced capture ledger entry in one
// transaction, keyed by internal payment id.
type LedgerCapturer interface {
	CaptureWithLedger(ctx context.Context, id int64, capturedAt time.Time) error
	FindByID(ctx context.Context, id, userID int64) (*domain.Payment, error)
}

// CaptureHealer is the ADR-012 convergence for the lost-capture-response window:
// an internal authorized row against a provider charge the provider already
// captured. It drives only the internal side — the provider is never called, so
// the reconciler's provider port stays read-only (ADR-011).
type CaptureHealer struct {
	capturer LedgerCapturer
	now      func() time.Time
}

// NewCaptureHealer wires the healer onto the capture path and a clock.
func NewCaptureHealer(capturer LedgerCapturer, now func() time.Time) *CaptureHealer {
	return &CaptureHealer{capturer: capturer, now: now}
}

// HealCapture attempts to converge the payment to captured and reports whether it
// is genuinely captured afterwards. Winning the CAS converges it (true). Losing
// the CAS (ErrStaleTransition) is not an error but is not automatically a
// success: it means a concurrent transition beat us — an idempotent re-capture
// (still captured → true) or a void/expiry that left the row un-captured (a real,
// still-unhealed discrepancy → false). We re-read to tell them apart rather than
// stamp a heal we didn't achieve.
func (h *CaptureHealer) HealCapture(ctx context.Context, paymentID int64) (bool, error) {
	err := h.capturer.CaptureWithLedger(ctx, paymentID, h.now())
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, domain.ErrStaleTransition):
		p, ferr := h.capturer.FindByID(ctx, paymentID, 0)
		if ferr != nil {
			return false, ferr
		}
		return p.Status == domain.StatusCaptured, nil
	default:
		return false, err
	}
}
