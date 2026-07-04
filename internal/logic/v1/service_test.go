package v1

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/duynhlab/pkg/idempotency"

	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
)

// ---- in-memory fakes -------------------------------------------------------

type fakePayments struct {
	mu     sync.Mutex
	seq    int64
	items  map[int64]*domain.Payment
	byOrd  map[int64]int64
	refSeq int64
	refs   map[int64]*domain.Refund
	// ledgerPosts / reversals count successful capture / reversal postings —
	// the fake posts nothing real, but rides the same CAS, so the counters
	// prove the logic layer's ledger idempotency without a DB.
	ledgerPosts int
	reversals   int
}

func newFakePayments() *fakePayments {
	return &fakePayments{items: map[int64]*domain.Payment{}, byOrd: map[int64]int64{}, refs: map[int64]*domain.Refund{}}
}

func (f *fakePayments) Create(_ context.Context, p *domain.Payment) (*domain.Payment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p.OrderID != nil {
		if _, dup := f.byOrd[*p.OrderID]; dup {
			return nil, domain.ErrPaymentExists
		}
	}
	f.seq++
	cp := *p
	cp.ID = f.seq
	cp.Status = domain.StatusPending
	cp.CreatedAt = time.Now()
	f.items[cp.ID] = &cp
	if p.OrderID != nil {
		f.byOrd[*p.OrderID] = cp.ID
	}
	out := cp
	return &out, nil
}

func (f *fakePayments) FindByID(_ context.Context, id, userID int64) (*domain.Payment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.items[id]
	if !ok || (userID != 0 && p.UserID != userID) {
		return nil, domain.ErrNotFound
	}
	p.RefundedMinor = f.refundedLocked(id)
	out := *p
	return &out, nil
}

func (f *fakePayments) FindByOrderID(_ context.Context, orderID int64) (*domain.Payment, error) {
	f.mu.Lock()
	id, ok := f.byOrd[orderID]
	f.mu.Unlock()
	if !ok {
		return nil, domain.ErrNotFound
	}
	return f.FindByID(context.Background(), id, 0)
}

func (f *fakePayments) ListByUser(_ context.Context, userID int64, limit, offset int) ([]domain.Payment, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []domain.Payment
	for _, p := range f.items {
		if p.UserID == userID {
			all = append(all, *p)
		}
	}
	total := len(all)
	if offset > len(all) {
		return nil, total, nil
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end], total, nil
}

func (f *fakePayments) TransitionStatus(_ context.Context, id int64, from, to domain.Status, set map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.items[id]
	if !ok || p.Status != from {
		return domain.ErrStaleTransition
	}
	p.Status = to
	if v, ok := set["provider_payment_id"]; ok {
		p.ProviderPaymentID = v.(string)
	}
	if v, ok := set["decline_code"]; ok {
		p.DeclineCode = v.(string)
	}
	if v, ok := set["expires_at"]; ok {
		t := v.(time.Time)
		p.ExpiresAt = &t
	}
	return nil
}

func (f *fakePayments) CaptureWithLedger(_ context.Context, id int64, capturedAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.items[id]
	if !ok || p.Status != domain.StatusAuthorized {
		return domain.ErrStaleTransition
	}
	p.Status = domain.StatusCaptured
	p.CapturedAt = &capturedAt
	f.ledgerPosts++
	return nil
}

func (f *fakePayments) ReverseCapture(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.items[id]
	if !ok || p.Status != domain.StatusCaptured {
		return domain.ErrStaleTransition
	}
	p.Status = domain.StatusAuthorized
	p.CapturedAt = nil
	f.reversals++
	return nil
}

func (f *fakePayments) ExpireStaleAuthorizations(_ context.Context, now time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for _, p := range f.items {
		if p.Status == domain.StatusAuthorized && p.ExpiresAt != nil && p.ExpiresAt.Before(now) {
			p.Status = domain.StatusExpired
			n++
		}
	}
	return n, nil
}

func (f *fakePayments) refundedLocked(paymentID int64) int64 {
	var sum int64
	for _, r := range f.refs {
		if r.PaymentID == paymentID && r.Status != domain.RefundFailed {
			sum += r.AmountMinor
		}
	}
	return sum
}

func (f *fakePayments) CreateRefund(_ context.Context, paymentID, amountMinor int64, reason, idemKey string) (*domain.Refund, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Idempotent by key: adopt an existing refund for the same key (mirrors the
	// real partial unique index) rather than inserting a duplicate.
	if idemKey != "" {
		for _, r := range f.refs {
			if r.IdempotencyKey == idemKey {
				out := *r
				return &out, nil
			}
		}
	}
	p, ok := f.items[paymentID]
	if !ok || (p.Status != domain.StatusCaptured && p.Status != domain.StatusRefunded) {
		return nil, domain.ErrRefundRejected
	}
	if amountMinor+f.refundedLocked(paymentID) > p.AmountMinor {
		return nil, domain.ErrRefundRejected
	}
	f.refSeq++
	r := &domain.Refund{ID: f.refSeq, PaymentID: paymentID, AmountMinor: amountMinor, Status: domain.RefundPending, Reason: reason, IdempotencyKey: idemKey}
	f.refs[r.ID] = r
	out := *r
	return &out, nil
}

func (f *fakePayments) SettleRefund(_ context.Context, refundID int64, status domain.RefundStatus, providerRefundID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.refs[refundID]
	if !ok {
		return domain.ErrNotFound
	}
	r.Status = status
	r.ProviderRefundID = providerRefundID
	if status == domain.RefundSucceeded {
		p := f.items[r.PaymentID]
		if f.refundedLocked(r.PaymentID) >= p.AmountMinor && p.Status == domain.StatusCaptured {
			p.Status = domain.StatusRefunded
		}
	}
	return nil
}

type fakeIdem struct {
	mu   sync.Mutex
	seq  int64
	keys map[string]*idempotency.Record
	take time.Duration
	// reapTTL/reapCount record the last Reap call so tests can assert the
	// service delegates the configured TTL and surfaces the returned count.
	reapTTL   time.Duration
	reapCount int64
}

func newFakeIdem() *fakeIdem {
	return &fakeIdem{keys: map[string]*idempotency.Record{}, take: 90 * time.Second}
}

func (f *fakeIdem) Claim(_ context.Context, userID int64, key, method, path, hash string) (*idempotency.Record, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := key
	if k, ok := f.keys[id]; ok {
		// Mirror the real repository: a key identifies ONE request — same key
		// with a different body, path, or method is a conflict, never a replay.
		if k.RequestHash != hash || k.RequestPath != path || k.RequestMethod != method {
			return nil, false, idempotency.ErrConflict
		}
		if k.Finished() {
			cp := *k
			return &cp, false, nil
		}
		if time.Since(k.LockedAt) < f.take {
			return nil, false, idempotency.ErrLocked
		}
		k.LockedAt = time.Now()
		cp := *k
		return &cp, true, nil
	}
	f.seq++
	k := &idempotency.Record{ID: f.seq, UserID: userID, Key: key,
		RequestMethod: method, RequestPath: path, RequestHash: hash,
		LockedAt: time.Now()}
	f.keys[id] = k
	cp := *k
	return &cp, true, nil
}

func (f *fakeIdem) Checkpoint(_ context.Context, id int64, subjectID *int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range f.keys {
		if k.ID == id {
			if subjectID != nil {
				k.SubjectID = subjectID
			}
			k.LockedAt = time.Now()
		}
	}
	return nil
}

func (f *fakeIdem) Release(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range f.keys {
		if k.ID == id && !k.Finished() {
			k.LockedAt = time.Unix(0, 0)
		}
	}
	return nil
}

func (f *fakeIdem) Finish(_ context.Context, id int64, code int, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range f.keys {
		if k.ID == id {
			k.ResponseCode = &code
			k.ResponseBody = body
		}
	}
	return nil
}

func (f *fakeIdem) Reap(_ context.Context, ttl time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reapTTL = ttl
	return f.reapCount, nil
}

// ---- helpers ---------------------------------------------------------------

func newTestService() (*Service, *fakePayments, *fakeIdem, *provider.Stub) {
	fp, fi, st := newFakePayments(), newFakeIdem(), provider.NewStub()
	return NewService(fp, fi, st, 168*time.Hour), fp, fi, st
}

func intent(amount int64) CreateIntentInput {
	return CreateIntentInput{UserID: 7, AmountMinor: amount, Currency: "USD",
		CaptureMethod: domain.CaptureManual, PaymentMethod: "tok_visa"}
}

// ---- tests -----------------------------------------------------------------

func TestCreateIntent_AuthorizesManualHold(t *testing.T) {
	svc, _, _, _ := newTestService()
	res, err := svc.CreateIntent(context.Background(), "key-1", intent(2000))
	if err != nil {
		t.Fatalf("CreateIntent: %v", err)
	}
	if res.Code != 201 || res.Payment.Status != domain.StatusAuthorized {
		t.Fatalf("got code=%d status=%s, want 201 authorized", res.Code, res.Payment.Status)
	}
	if res.Payment.ExpiresAt == nil {
		t.Fatal("authorized hold must carry expires_at")
	}
}

func TestCreateIntent_AutomaticCaptures(t *testing.T) {
	svc, _, _, _ := newTestService()
	in := intent(2000)
	in.CaptureMethod = domain.CaptureAutomatic
	res, err := svc.CreateIntent(context.Background(), "key-auto", in)
	if err != nil {
		t.Fatalf("CreateIntent: %v", err)
	}
	if res.Payment.Status != domain.StatusCaptured {
		t.Fatalf("automatic capture got %s, want captured", res.Payment.Status)
	}
}

func TestCreateIntent_ReplaysFirstResponse(t *testing.T) {
	svc, fp, _, _ := newTestService()
	first, err := svc.CreateIntent(context.Background(), "key-2", intent(2000))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := svc.CreateIntent(context.Background(), "key-2", intent(2000))
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if !second.Replayed || second.Payment.ID != first.Payment.ID {
		t.Fatalf("second call must replay the first payment (got replayed=%v id=%d)", second.Replayed, second.Payment.ID)
	}
	fp.mu.Lock()
	n := len(fp.items)
	fp.mu.Unlock()
	if n != 1 {
		t.Fatalf("exactly one payment must exist, got %d", n)
	}
}

func TestCreateIntent_SameKeyDifferentBody(t *testing.T) {
	svc, _, _, _ := newTestService()
	if _, err := svc.CreateIntent(context.Background(), "key-3", intent(2000)); err != nil {
		t.Fatalf("first: %v", err)
	}
	_, err := svc.CreateIntent(context.Background(), "key-3", intent(3000))
	// The store returns idempotency.ErrConflict; the service maps it to the
	// domain sentinel so the web layer answers 409 (not 500).
	if !errors.Is(err, domain.ErrKeyConflict) {
		t.Fatalf("different body must conflict, got %v", err)
	}
}

func TestCreateIntent_DeclineCaches422(t *testing.T) {
	// Each deterministic decline trigger fails the intent with its own code,
	// caches 422, and replays on retry without re-charging.
	tests := []struct {
		name    string
		amount  int64
		decline string
	}{
		{"generic decline", 2002, provider.DeclineGeneric},         // ...02
		{"insufficient funds", 2095, provider.DeclineInsufficient}, // ...95
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, _, _ := newTestService()
			res, err := svc.CreateIntent(context.Background(), "key-4", intent(tt.amount))
			if err != nil {
				t.Fatalf("CreateIntent: %v", err)
			}
			if res.Code != 422 || res.Payment.Status != domain.StatusFailed || res.Payment.DeclineCode != tt.decline {
				t.Fatalf("got code=%d status=%s decline=%q, want 422 failed %q",
					res.Code, res.Payment.Status, res.Payment.DeclineCode, tt.decline)
			}
			// The decline replays too — a retry must NOT re-charge.
			res2, err := svc.CreateIntent(context.Background(), "key-4", intent(tt.amount))
			if err != nil || !res2.Replayed || res2.Code != 422 {
				t.Fatalf("decline replay got (%v, replayed=%v code=%d)", err, res2.Replayed, res2.Code)
			}
		})
	}
}

func TestCreateIntent_TransientReleasesLockForImmediateRetry(t *testing.T) {
	svc, _, _, _ := newTestService()
	_, err := svc.CreateIntent(context.Background(), "key-5", intent(2019)) // ...19 => transient once
	if !errors.Is(err, provider.ErrTransient) {
		t.Fatalf("want transient error, got %v", err)
	}
	// No artificial lock-aging: the transient path released the lock, so an
	// IMMEDIATE same-key retry must re-drive (not ErrKeyLocked) and succeed.
	res, err := svc.CreateIntent(context.Background(), "key-5", intent(2019))
	if err != nil {
		t.Fatalf("immediate retry after transient: %v", err)
	}
	if res.Code != 201 || res.Payment.Status != domain.StatusAuthorized {
		t.Fatalf("retry got code=%d status=%s", res.Code, res.Payment.Status)
	}
}

func TestCreateIntent_InFlightLocked(t *testing.T) {
	svc, _, fi, _ := newTestService()
	// Seed an unfinished, fresh-locked key manually.
	_, _, err := fi.Claim(context.Background(), 7, "key-6", "POST", "/payment/v1/private/payments", hashJSON(intent(2000)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.CreateIntent(context.Background(), "key-6", intent(2000))
	if !errors.Is(err, domain.ErrKeyLocked) {
		t.Fatalf("in-flight duplicate must be locked, got %v", err)
	}
}

func TestCreateIntent_OrderIdempotencyAndSquatProtection(t *testing.T) {
	svc, fp, _, stub := newTestService()
	order := int64(42)
	in := intent(2000)
	in.OrderID = &order
	first, err := svc.CreateIntent(context.Background(), "key-7", in)
	if err != nil {
		t.Fatalf("first: %v", err)
	}

	// A second request for the SAME order by the SAME user with the SAME amount
	// but a different key adopts the existing payment — idempotent by order_id
	// (the saga's crash-recovery path), and MUST NOT charge the provider again.
	in2 := intent(2000)
	in2.OrderID = &order
	second, err := svc.CreateIntent(context.Background(), "key-8", in2)
	if err != nil {
		t.Fatalf("same-order re-request must adopt, got %v", err)
	}
	if second.Payment.ID != first.Payment.ID {
		t.Fatalf("adopt must reuse payment %d, got %d", first.Payment.ID, second.Payment.ID)
	}
	if second.Payment.ProviderPaymentID != first.Payment.ProviderPaymentID {
		t.Fatalf("adopt must not re-charge: provider id changed %q -> %q",
			first.Payment.ProviderPaymentID, second.Payment.ProviderPaymentID)
	}
	if got := stub.Charges(); got != 1 {
		t.Fatalf("provider charged %d times, want exactly 1 (no double charge)", got)
	}
	fp.mu.Lock()
	n := len(fp.items)
	fp.mu.Unlock()
	if n != 1 {
		t.Fatalf("exactly one payment for the order, got %d", n)
	}

	// A DIFFERENT amount on the same order is a real conflict (squat / mismatch).
	mismatch := intent(9999)
	mismatch.OrderID = &order
	if _, err := svc.CreateIntent(context.Background(), "key-9", mismatch); !errors.Is(err, domain.ErrPaymentExists) {
		t.Fatalf("amount mismatch on existing order must reject, got %v", err)
	}

	// A DIFFERENT user squatting the same order is rejected (no cross-user adopt).
	foreign := intent(2000)
	foreign.OrderID = &order
	foreign.UserID = 99
	if _, err := svc.CreateIntent(context.Background(), "key-10", foreign); !errors.Is(err, domain.ErrPaymentExists) {
		t.Fatalf("foreign user on existing order must reject, got %v", err)
	}
}

func TestCaptureAndVoid_Lifecycle(t *testing.T) {
	svc, _, _, _ := newTestService()
	res, _ := svc.CreateIntent(context.Background(), "key-9", intent(2000))
	id := res.Payment.ID

	got, err := svc.Capture(context.Background(), id, 7)
	if err != nil || got.Status != domain.StatusCaptured {
		t.Fatalf("capture: %v status=%v", err, got)
	}
	// Idempotent re-capture.
	if _, err := svc.Capture(context.Background(), id, 7); err != nil {
		t.Fatalf("re-capture must be a no-op: %v", err)
	}
	// Void after capture is forbidden by the whitelist.
	if _, err := svc.Void(context.Background(), id, 7); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("void after capture must be invalid, got %v", err)
	}
}

func TestVoid_ReleasesHold(t *testing.T) {
	svc, _, _, _ := newTestService()
	res, _ := svc.CreateIntent(context.Background(), "key-10", intent(2000))
	got, err := svc.Void(context.Background(), res.Payment.ID, 7)
	if err != nil || got.Status != domain.StatusVoided {
		t.Fatalf("void: %v %v", err, got)
	}
	if _, err := svc.Void(context.Background(), res.Payment.ID, 7); err != nil {
		t.Fatalf("re-void must be a no-op: %v", err)
	}
}

func TestRefund_PartialThenFullFlipsStatus(t *testing.T) {
	svc, _, _, _ := newTestService()
	res, _ := svc.CreateIntent(context.Background(), "key-11", intent(2000))
	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); err != nil {
		t.Fatal(err)
	}

	ref1, replayed, err := svc.CreateRefund(context.Background(), "rk-1", res.Payment.ID, 7, 500, "damaged")
	if err != nil || replayed || ref1.Status != domain.RefundSucceeded {
		t.Fatalf("partial refund: %v replayed=%v ref=%v", err, replayed, ref1)
	}
	pay, _ := svc.Get(context.Background(), res.Payment.ID, 7)
	if !pay.PartiallyRefunded() || pay.Status != domain.StatusCaptured {
		t.Fatalf("after partial: partially=%v status=%s", pay.PartiallyRefunded(), pay.Status)
	}

	// Refund idempotency: same key replays, no double refund.
	refAgain, replayed, err := svc.CreateRefund(context.Background(), "rk-1", res.Payment.ID, 7, 500, "damaged")
	if err != nil || !replayed || refAgain.ID != ref1.ID {
		t.Fatalf("refund replay: %v replayed=%v", err, replayed)
	}

	// Over-refund must be rejected: 500 refunded, 2000 total, 1600 > remaining.
	if _, _, err := svc.CreateRefund(context.Background(), "rk-2", res.Payment.ID, 7, 1600, ""); !errors.Is(err, domain.ErrRefundRejected) {
		t.Fatalf("over-refund must reject, got %v", err)
	}

	// Refund the remainder — payment flips to refunded.
	if _, _, err := svc.CreateRefund(context.Background(), "rk-3", res.Payment.ID, 7, 1500, ""); err != nil {
		t.Fatal(err)
	}
	pay, _ = svc.Get(context.Background(), res.Payment.ID, 7)
	if pay.Status != domain.StatusRefunded || pay.PartiallyRefunded() {
		t.Fatalf("after full refund: status=%s partially=%v", pay.Status, pay.PartiallyRefunded())
	}
}

func TestExpireHolds(t *testing.T) {
	svc, fp, _, _ := newTestService()
	res, _ := svc.CreateIntent(context.Background(), "key-12", intent(2000))
	fp.mu.Lock()
	past := time.Now().Add(-time.Hour)
	fp.items[res.Payment.ID].ExpiresAt = &past
	fp.mu.Unlock()

	n, err := svc.ExpireHolds(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("expire: %v n=%d", err, n)
	}
	if _, err := svc.Capture(context.Background(), res.Payment.ID, 7); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("capturing an expired hold must be invalid, got %v", err)
	}
}

func TestReplayCache_RoundTripsJSON(t *testing.T) {
	// Guards the cache format: what finishIntent stores, replayResult must parse.
	p := &domain.Payment{ID: 1, Status: domain.StatusAuthorized, AmountMinor: 2000}
	body, _ := json.Marshal(p)
	code := 201
	res, err := replayResult(&idempotency.Record{ResponseCode: &code, ResponseBody: body})
	if err != nil || res.Payment.ID != 1 || !res.Replayed {
		t.Fatalf("replay round-trip: %v %+v", err, res)
	}
}
