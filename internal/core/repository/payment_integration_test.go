//go:build integration

// Integration tests for the payment repositories. They run a real Postgres via
// testcontainers-go and apply the service's migrations, so they exercise the
// actual SQL — the CAS transition, the guarded refund insert, and the
// idempotency claim/takeover races. Run with:
//
//	go test -tags=integration ./internal/core/repository/...
//
// Requires a reachable Docker daemon. Excluded from the default `go test ./...`
// unit run by the `integration` build tag.
package repository

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/duynhlab/payment-service/internal/core/domain"
)

func newTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("payment"),
		postgres.WithUsername("payment"),
		postgres.WithPassword("secret"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	applyMigrations(t, ctx, dsn)

	// Mirror production: the service pool runs the simple query protocol (for
	// PgBouncer/PgDog compatibility), which encodes parameters differently
	// from the extended protocol — e.g. []byte into jsonb breaks only there.
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func applyMigrations(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect for migrations: %v", err)
	}
	defer conn.Close(ctx)

	dir := filepath.Join("..", "..", "..", "db", "migrations", "sql")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	var files []string
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() && len(name) > 7 && name[len(name)-7:] == ".up.sql" {
			files = append(files, name)
		}
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
	}
}

func createPending(t *testing.T, repo *PaymentRepository, userID int64, orderID *int64, amount int64) *domain.Payment {
	t.Helper()
	p, err := repo.Create(context.Background(), &domain.Payment{
		UserID: userID, OrderID: orderID, AmountMinor: amount, Currency: "USD",
		CaptureMethod: domain.CaptureManual, PaymentMethod: "tok_visa",
	})
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}
	return p
}

func TestPaymentRepository_Integration(t *testing.T) {
	pool := newTestDB(t)
	repo := NewPaymentRepository(pool)
	ctx := context.Background()

	t.Run("create and owner-scoped find", func(t *testing.T) {
		p := createPending(t, repo, 7, nil, 2000)
		if p.Status != domain.StatusPending || p.Currency != "USD" {
			t.Fatalf("unexpected payment %+v", p)
		}
		if _, err := repo.FindByID(ctx, p.ID, 8); !errors.Is(err, ErrNotFound) {
			t.Fatalf("foreign user must not see it: %v", err)
		}
		got, err := repo.FindByID(ctx, p.ID, 7)
		if err != nil || got.ID != p.ID {
			t.Fatalf("owner find: %v", err)
		}
	})

	t.Run("unique order_id maps to ErrPaymentExists", func(t *testing.T) {
		order := int64(101)
		createPending(t, repo, 7, &order, 2000)
		if _, err := repo.Create(ctx, &domain.Payment{UserID: 7, OrderID: &order, AmountMinor: 900,
			Currency: "USD", CaptureMethod: domain.CaptureManual, PaymentMethod: "tok_visa"}); !errors.Is(err, ErrPaymentExists) {
			t.Fatalf("duplicate order payment: %v", err)
		}
		got, err := repo.FindByOrderID(ctx, order)
		if err != nil || got.AmountMinor != 2000 {
			t.Fatalf("find by order: %v %+v", err, got)
		}
	})

	t.Run("CAS transition: concurrent capture vs void, one winner", func(t *testing.T) {
		p := createPending(t, repo, 7, nil, 2000)
		if err := repo.TransitionStatus(ctx, p.ID, domain.StatusPending, domain.StatusAuthorized,
			map[string]any{"provider_payment_id": "mp_1", "authorized_at": time.Now(), "expires_at": time.Now().Add(time.Hour)}); err != nil {
			t.Fatalf("authorize: %v", err)
		}

		var wg sync.WaitGroup
		results := make([]error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			results[0] = repo.TransitionStatus(ctx, p.ID, domain.StatusAuthorized, domain.StatusCaptured,
				map[string]any{"captured_at": time.Now()})
		}()
		go func() {
			defer wg.Done()
			results[1] = repo.TransitionStatus(ctx, p.ID, domain.StatusAuthorized, domain.StatusVoided, nil)
		}()
		wg.Wait()

		wins, stales := 0, 0
		for _, err := range results {
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrStaleTransition):
				stales++
			default:
				t.Fatalf("unexpected transition error: %v", err)
			}
		}
		if wins != 1 || stales != 1 {
			t.Fatalf("want exactly one winner, got wins=%d stales=%d", wins, stales)
		}
	})

	t.Run("expiry job flips stale authorized holds", func(t *testing.T) {
		p := createPending(t, repo, 7, nil, 2000)
		past := time.Now().Add(-time.Minute)
		if err := repo.TransitionStatus(ctx, p.ID, domain.StatusPending, domain.StatusAuthorized,
			map[string]any{"provider_payment_id": "mp_2", "authorized_at": past, "expires_at": past}); err != nil {
			t.Fatal(err)
		}
		n, err := repo.ExpireStaleAuthorizations(ctx, time.Now())
		if err != nil || n < 1 {
			t.Fatalf("expire: n=%d err=%v", n, err)
		}
		got, _ := repo.FindByID(ctx, p.ID, 0)
		if got.Status != domain.StatusExpired {
			t.Fatalf("status=%s, want expired", got.Status)
		}
	})

	t.Run("guarded refunds: partial accumulate, oversubscribe rejected, full flips status", func(t *testing.T) {
		p := createPending(t, repo, 7, nil, 2000)
		mustTransition(t, repo, p.ID, domain.StatusPending, domain.StatusAuthorized,
			map[string]any{"provider_payment_id": "mp_3", "authorized_at": time.Now(), "expires_at": time.Now().Add(time.Hour)})
		mustTransition(t, repo, p.ID, domain.StatusAuthorized, domain.StatusCaptured,
			map[string]any{"captured_at": time.Now()})

		r1, err := repo.CreateRefund(ctx, p.ID, 500, "damaged")
		if err != nil {
			t.Fatalf("partial refund: %v", err)
		}
		// Pending refunds count against the cap: 1600 > 2000-500.
		if _, err := repo.CreateRefund(ctx, p.ID, 1600, ""); !errors.Is(err, ErrRefundRejected) {
			t.Fatalf("oversubscribe with pending refund: %v", err)
		}
		if err := repo.SettleRefund(ctx, r1.ID, domain.RefundSucceeded, "re_1"); err != nil {
			t.Fatalf("settle: %v", err)
		}
		got, _ := repo.FindByID(ctx, p.ID, 0)
		if got.RefundedMinor != 500 || !got.PartiallyRefunded() {
			t.Fatalf("after partial: refunded=%d partially=%v", got.RefundedMinor, got.PartiallyRefunded())
		}

		r2, err := repo.CreateRefund(ctx, p.ID, 1500, "")
		if err != nil {
			t.Fatalf("remainder refund: %v", err)
		}
		if err := repo.SettleRefund(ctx, r2.ID, domain.RefundSucceeded, "re_2"); err != nil {
			t.Fatal(err)
		}
		got, _ = repo.FindByID(ctx, p.ID, 0)
		if got.Status != domain.StatusRefunded {
			t.Fatalf("after full refund status=%s, want refunded", got.Status)
		}
		// Terminal: further refunds rejected.
		if _, err := repo.CreateRefund(ctx, p.ID, 1, ""); !errors.Is(err, ErrRefundRejected) {
			t.Fatalf("refund on refunded payment: %v", err)
		}
	})

	t.Run("concurrent refunds cannot oversubscribe (FOR UPDATE guard)", func(t *testing.T) {
		p := createPending(t, repo, 7, nil, 1000)
		mustTransition(t, repo, p.ID, domain.StatusPending, domain.StatusAuthorized,
			map[string]any{"provider_payment_id": "mp_c", "authorized_at": time.Now(), "expires_at": time.Now().Add(time.Hour)})
		mustTransition(t, repo, p.ID, domain.StatusAuthorized, domain.StatusCaptured,
			map[string]any{"captured_at": time.Now()})

		// Two refunds of 600 against a 1000 capture racing: without the row
		// lock both would snapshot SUM=0 and commit, oversubscribing to 1200.
		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func(i int) { defer wg.Done(); _, errs[i] = repo.CreateRefund(ctx, p.ID, 600, "") }(i)
		}
		wg.Wait()

		ok, rejected := 0, 0
		for _, err := range errs {
			switch {
			case err == nil:
				ok++
			case errors.Is(err, ErrRefundRejected):
				rejected++
			default:
				t.Fatalf("unexpected refund error: %v", err)
			}
		}
		if ok != 1 || rejected != 1 {
			t.Fatalf("want exactly one refund admitted, got ok=%d rejected=%d", ok, rejected)
		}
		got, _ := repo.FindByID(ctx, p.ID, 0)
		if got.RefundedMinor > got.AmountMinor {
			t.Fatalf("oversubscribed: refunded=%d > amount=%d", got.RefundedMinor, got.AmountMinor)
		}
	})

	t.Run("failed refund releases its reserved amount", func(t *testing.T) {
		p := createPending(t, repo, 7, nil, 1000)
		mustTransition(t, repo, p.ID, domain.StatusPending, domain.StatusAuthorized,
			map[string]any{"provider_payment_id": "mp_4", "authorized_at": time.Now(), "expires_at": time.Now().Add(time.Hour)})
		mustTransition(t, repo, p.ID, domain.StatusAuthorized, domain.StatusCaptured,
			map[string]any{"captured_at": time.Now()})

		r, err := repo.CreateRefund(ctx, p.ID, 1000, "")
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.SettleRefund(ctx, r.ID, domain.RefundFailed, ""); err != nil {
			t.Fatal(err)
		}
		got, _ := repo.FindByID(ctx, p.ID, 0)
		if got.Status != domain.StatusCaptured || got.RefundedMinor != 0 {
			t.Fatalf("failed refund must release: status=%s refunded=%d", got.Status, got.RefundedMinor)
		}
		if _, err := repo.CreateRefund(ctx, p.ID, 1000, ""); err != nil {
			t.Fatalf("retry refund after failure: %v", err)
		}
	})

	t.Run("list pagination", func(t *testing.T) {
		userID := int64(55)
		for i := 0; i < 3; i++ {
			createPending(t, repo, userID, nil, 1000+int64(i))
		}
		items, total, err := repo.ListByUser(ctx, userID, 2, 0)
		if err != nil || total != 3 || len(items) != 2 {
			t.Fatalf("page1: err=%v total=%d len=%d", err, total, len(items))
		}
		items, _, _ = repo.ListByUser(ctx, userID, 2, 2)
		if len(items) != 1 {
			t.Fatalf("page2 len=%d", len(items))
		}
	})
}

func mustTransition(t *testing.T, repo *PaymentRepository, id int64, from, to domain.Status, set map[string]any) {
	t.Helper()
	if err := repo.TransitionStatus(context.Background(), id, from, to, set); err != nil {
		t.Fatalf("transition %s->%s: %v", from, to, err)
	}
}

func TestIdempotencyRepository_Integration(t *testing.T) {
	pool := newTestDB(t)
	repo := NewIdempotencyRepository(pool, 90*time.Second)
	ctx := context.Background()

	t.Run("claim, finish, replay", func(t *testing.T) {
		k, proceed, err := repo.Claim(ctx, 7, "k1", "POST", "/p", "hash-a")
		if err != nil || !proceed {
			t.Fatalf("fresh claim: %v proceed=%v", err, proceed)
		}
		if err := repo.Finish(ctx, k.ID, 201, []byte(`{"id":1}`)); err != nil {
			t.Fatal(err)
		}
		k2, proceed, err := repo.Claim(ctx, 7, "k1", "POST", "/p", "hash-a")
		if err != nil || proceed || !k2.Finished() || *k2.ResponseCode != 201 {
			t.Fatalf("replay: err=%v proceed=%v key=%+v", err, proceed, k2)
		}
	})

	t.Run("same key different hash conflicts", func(t *testing.T) {
		if _, _, err := repo.Claim(ctx, 7, "k1", "POST", "/p", "hash-B"); !errors.Is(err, ErrKeyConflict) {
			t.Fatalf("want ErrKeyConflict, got %v", err)
		}
	})

	t.Run("same key different path or method conflicts", func(t *testing.T) {
		// A key identifies one request: reusing it on another endpoint (even
		// with the identical body hash) must conflict, never cross-replay.
		if _, _, err := repo.Claim(ctx, 7, "k1", "POST", "/other", "hash-a"); !errors.Is(err, ErrKeyConflict) {
			t.Fatalf("different path: want ErrKeyConflict, got %v", err)
		}
		if _, _, err := repo.Claim(ctx, 7, "k1", "DELETE", "/p", "hash-a"); !errors.Is(err, ErrKeyConflict) {
			t.Fatalf("different method: want ErrKeyConflict, got %v", err)
		}
	})

	t.Run("per-user key namespaces", func(t *testing.T) {
		_, proceed, err := repo.Claim(ctx, 8, "k1", "POST", "/p", "hash-a")
		if err != nil || !proceed {
			t.Fatalf("another user's same key must claim fresh: %v proceed=%v", err, proceed)
		}
	})

	t.Run("concurrent claims: one winner", func(t *testing.T) {
		var wg sync.WaitGroup
		proceeds := make([]bool, 8)
		errs := make([]error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, p, err := repo.Claim(ctx, 9, "k-race", "POST", "/p", "hash-r")
				proceeds[i], errs[i] = p, err
			}(i)
		}
		wg.Wait()
		winners, locked := 0, 0
		for i := range proceeds {
			switch {
			case errs[i] == nil && proceeds[i]:
				winners++
			case errors.Is(errs[i], ErrKeyLocked):
				locked++
			case errs[i] != nil:
				t.Fatalf("unexpected claim error: %v", errs[i])
			}
		}
		if winners != 1 || locked != 7 {
			t.Fatalf("want 1 winner / 7 locked, got %d / %d", winners, locked)
		}
	})

	t.Run("stale lock takeover with recovery point", func(t *testing.T) {
		k, _, err := repo.Claim(ctx, 10, "k-stale", "POST", "/p", "hash-s")
		if err != nil {
			t.Fatal(err)
		}
		// The checkpointed subject must be a real payment (FK).
		payRepo := NewPaymentRepository(pool)
		pay, err := payRepo.Create(ctx, &domain.Payment{UserID: 10, AmountMinor: 1000,
			Currency: "USD", CaptureMethod: domain.CaptureManual, PaymentMethod: "tok_visa"})
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.Advance(ctx, k.ID, RecoveryProviderCalled, &pay.ID); err != nil {
			t.Fatal(err)
		}
		// Age the lock beyond the takeover threshold.
		if _, err := pool.Exec(ctx,
			`UPDATE idempotency_keys SET locked_at = now() - interval '5 minutes' WHERE id = $1`, k.ID); err != nil {
			t.Fatal(err)
		}
		took, proceed, err := repo.Claim(ctx, 10, "k-stale", "POST", "/p", "hash-s")
		if err != nil || !proceed {
			t.Fatalf("takeover: %v proceed=%v", err, proceed)
		}
		if took.RecoveryPoint != RecoveryProviderCalled || took.PaymentID == nil || *took.PaymentID != pay.ID {
			t.Fatalf("takeover must surface checkpoint, got %+v", took)
		}
	})

	t.Run("CreateRefund surfaces a transaction begin error", func(t *testing.T) {
		// A closed pool makes Begin fail — exercises the tx error path without
		// a live fault-injection harness.
		dead, err := pgxpool.New(ctx, pool.Config().ConnString())
		if err != nil {
			t.Fatal(err)
		}
		dead.Close()
		if _, err := NewPaymentRepository(dead).CreateRefund(ctx, 1, 100, ""); err == nil {
			t.Fatal("CreateRefund on a closed pool must error")
		}
	})

	t.Run("release ages the lock so an immediate retry takes over", func(t *testing.T) {
		k, _, err := repo.Claim(ctx, 12, "k-rel", "POST", "/p", "hash-r")
		if err != nil {
			t.Fatal(err)
		}
		// Fresh lock: a second claim would normally be ErrKeyLocked.
		if _, _, err := repo.Claim(ctx, 12, "k-rel", "POST", "/p", "hash-r"); !errors.Is(err, ErrKeyLocked) {
			t.Fatalf("fresh lock should block, got %v", err)
		}
		// Release ages the lock; the next claim takes over (proceed=true).
		if err := repo.Release(ctx, k.ID); err != nil {
			t.Fatalf("release: %v", err)
		}
		_, proceed, err := repo.Claim(ctx, 12, "k-rel", "POST", "/p", "hash-r")
		if err != nil || !proceed {
			t.Fatalf("after release, retry must take over: proceed=%v err=%v", proceed, err)
		}
		// Release on a finished key is a no-op (guarded by response_code IS NULL).
		if err := repo.Finish(ctx, k.ID, 201, []byte(`{"ok":true}`)); err != nil {
			t.Fatal(err)
		}
		if err := repo.Release(ctx, k.ID); err != nil {
			t.Fatalf("release on finished key must be a harmless no-op, got %v", err)
		}
	})

	t.Run("reap removes old keys", func(t *testing.T) {
		k, _, err := repo.Claim(ctx, 11, "k-old", "POST", "/p", "h")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx,
			`UPDATE idempotency_keys SET created_at = now() - interval '2 days' WHERE id = $1`, k.ID); err != nil {
			t.Fatal(err)
		}
		n, err := repo.Reap(ctx, 24*time.Hour)
		if err != nil || n < 1 {
			t.Fatalf("reap: n=%d err=%v", n, err)
		}
	})
}
