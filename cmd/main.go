package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"

	"github.com/duynhlab/payment-service/config"
	migrations "github.com/duynhlab/payment-service/db/migrations"
	database "github.com/duynhlab/payment-service/internal/core/database"
	"github.com/duynhlab/payment-service/internal/core/domain"
	"github.com/duynhlab/payment-service/internal/core/provider"
	"github.com/duynhlab/payment-service/internal/core/repository"
	grpcv1 "github.com/duynhlab/payment-service/internal/grpc/v1"
	logicv1 "github.com/duynhlab/payment-service/internal/logic/v1"
	"github.com/duynhlab/payment-service/internal/mockpay"
	v1 "github.com/duynhlab/payment-service/internal/web/v1"
	paymentv1 "github.com/duynhlab/pkg/proto/payment/v1"

	"github.com/duynhlab/pkg/authmw"
	"github.com/duynhlab/pkg/grpcx"
	"github.com/duynhlab/pkg/httpmw"
	"github.com/duynhlab/pkg/idempotency"
	"github.com/duynhlab/pkg/logger/slogx"
	"github.com/duynhlab/pkg/migratex"
	"github.com/duynhlab/pkg/obsx"
)

// fieldStatus is the JSON key for the health/ready probe responses.
const fieldStatus = "status"

// fieldPort is the log-field key for the listen port.
const fieldPort = "port"

// Outbox relay cadence and batch size. The relay is a log sink in P2, so a
// modest interval keeps event latency low without a tight poll.
const (
	outboxRelayInterval = 10 * time.Second
	outboxRelayBatch    = 100
	reconcileInterval   = 5 * time.Minute
	reconcilePageSize   = 100
	// resolveDoubtInterval paces the doubt sweep. Every operation already resolves
	// its own payment on the request path, so this only picks up doubt nobody is
	// retrying — which is why a minute is frequent enough, and why it must not be
	// so frequent that a provider outage turns into a poll loop against a provider
	// that is already struggling.
	resolveDoubtInterval = time.Minute
	// resolveDoubtBatch bounds one sweep. Each entry is a provider round-trip, so
	// the batch is the ceiling on how much traffic one tick can generate.
	resolveDoubtBatch = 50
	// Published events are pruned after this window; the durable audit trail is
	// the ledger, so the outbox only needs a short replay buffer.
	outboxPublishedRetention = 7 * 24 * time.Hour
	// Reconciliation runs are pruned after this window (discrepancies cascade);
	// they are operational history, not the source of truth.
	reconRunRetention = 30 * 24 * time.Hour
)

// outboxLogPublisher is the P2 delivery sink: it logs each event. A real broker
// replaces it behind logicv1.Publisher with no relay change.
type outboxLogPublisher struct{ logger *slogx.Logger }

func (p outboxLogPublisher) Publish(ctx context.Context, e domain.OutboxEvent) error {
	// The payload is deny-class: the record names the event, never its body.
	p.logger.Info(ctx, "Outbox event published",
		slog.Int64("outbox_id", e.ID),
		slog.String("event_type", e.EventType),
	)
	return nil
}

func main() {
	if err := run(); err != nil {
		// Fatal startup failure — exit non-zero so init containers, Jobs, and
		// exit-code alerting see the failure instead of a clean exit.
		fmt.Fprintln(os.Stderr, "payment-service: fatal:", err)
		os.Exit(1)
	}
}

// run wires and serves the payment service, returning an error on any fatal
// startup failure. It owns all the shutdown defers, so main can os.Exit(1)
// without skipping cleanup (os.Exit in main would bypass defers).
func run() error {
	ctx := context.Background()
	cfg := config.Load()

	logger := slogx.New(slogx.Config{Level: os.Getenv("LOG_LEVEL")})
	slogx.SetDefault(logger)

	// `<binary> migrate` runs embedded schema migrations (its SQL runs and the
	// process exits). No args serves the app.
	if maybeRunSubcommand(cfg, logger) {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("configuration validation: %w", err)
	}

	logger.Info(ctx, "Service starting",
		slog.String("service.version", cfg.Service.Version),
		slog.String("deployment.environment.name", cfg.Service.Env),
		slog.String(fieldPort, cfg.Service.Port),
	)

	tp, logger := initObservability(logger)

	profilingShutdown := initProfiling(cfg, logger)
	defer profilingShutdown()

	pool, err := database.Connect(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()
	logger.Info(ctx, "Database connection pool established")

	// Local RS256 OIDC JWT verification (cached JWKS) is the only credential —
	// no gRPC fallback. The JWKS URL is derived from the Keycloak issuer unless
	// OIDC_JWKS_URL overrides it. NewVerifier does not block on an unreachable
	// JWKS — it refreshes in the background, so it is safe to build at startup.
	verifier, staffVerifier, err := buildVerifiers(cfg)
	if err != nil {
		return err
	}

	// Repositories + provider + logic. P1 runs the in-memory provider stub;
	// the real mockpay HTTP client lands in P2 behind the same interface.
	paymentRepo := repository.NewPaymentRepository(pool)
	idemRepo := idempotency.New(pool, cfg.Payment.IdempotencyLockTakeover)
	prov := selectProvider(cfg, logger)
	// The attempt log is what makes an unknown provider outcome resolvable rather
	// than permanent, so production always gets the real recorder.
	attemptRepo := repository.NewAttemptRepository(pool)
	paymentService := logicv1.NewService(paymentRepo, idemRepo, prov, cfg.Payment.AuthHoldTTL,
		logicv1.WithAttempts(attemptRepo))
	paymentHandler := v1.NewHandler(paymentService)

	reconciler, reconHandler, reconRepo := buildReconciliation(cfg, prov, pool, paymentRepo, logger)

	if err := registerBacklogGauges(attemptRepo, reconRepo); err != nil {
		return err
	}

	// Internal gRPC server (:9090) — the order-fulfillment saga's money transport.
	// A bind failure is fatal: the pod must not report healthy on HTTP while its
	// primary east-west (money) transport is silently absent.
	grpcSrv, err := startGRPC(cfg, logger, paymentService)
	if err != nil {
		return err
	}

	// Inbound webhook receiver (public route; HMAC-verified in the handler).
	webhookHandler := v1.NewWebhookHandler(
		logicv1.NewWebhookProcessor(repository.NewWebhookRepository(pool)),
		cfg.Payment.WebhookSecret,
	)

	// Outbox relay: drains events written in the money-movement transactions and
	// delivers them to the P2 log sink.
	outboxRelay := logicv1.NewOutboxRelay(repository.NewOutboxRepository(pool), outboxLogPublisher{logger: logger})

	jobsCtx, stopJobs := context.WithCancel(context.Background())
	var jobsWG sync.WaitGroup
	jobsWG.Add(1)
	go func() {
		defer jobsWG.Done()
		runBackgroundJobs(jobsCtx, paymentService, outboxRelay, reconciler, reconRepo, cfg, logger)
	}()

	// Stop the background loops and wait for the in-flight tick to finish
	// before the pool is closed — otherwise a tick landing after pool.Close()
	// acquires from a closed pool and logs a spurious error.
	stopJobsAndWait := func() {
		grpcSrv.GracefulStop() // drain in-flight RPCs before the pool closes
		stopJobs()
		jobsWG.Wait()
	}

	var isShuttingDown atomic.Bool
	srv := setupServer(cfg, obsx.ConfigFromEnv().ServiceName, logger, verifier, staffVerifier,
		httpHandlers{
			payment: paymentHandler,
			protected: v1.NewProtectedHandler(paymentRepo, attemptRepo,
				repository.NewLedgerRepository(pool), repository.NewReconReadRepository(pool)),
			webhook: webhookHandler,
			recon:   reconHandler,
		},
		&isShuttingDown)
	runGracefulShutdown(cfg, srv, tp, pool, logger, &isShuttingDown, stopJobsAndWait)
	return nil
}

// registerBacklogGauges publishes the two backlogs nobody would otherwise query:
// unresolved provider outcomes, and how far behind reconciliation is.
//
// Both are gauges derived from stored state rather than counters, for the same
// reason: a process that has STOPPED doing the work emits no events at all, so
// only something read from the database can tell "stopped" from "quiet". And in
// both the AGE is the alertable half — one fresh unknown is routine, an old one
// means money is sitting somewhere nobody has looked.
func registerBacklogGauges(attempts *repository.AttemptRepository, recon *repository.ReconciliationRepository) error {
	if err := logicv1.ObserveDoubtBacklog(attempts.CountOpen, func(ctx context.Context) (time.Duration, error) {
		return attempts.OldestOpenAge(ctx)
	}); err != nil {
		return fmt.Errorf("register doubt-backlog gauges: %w", err)
	}

	// startedAt anchors the watermark gauge before the first pass has ever landed.
	startedAt := time.Now()
	if err := logicv1.ObserveReconciliationWatermark(func(ctx context.Context) (time.Duration, error) {
		mark, merr := recon.Watermark(ctx)
		if merr != nil {
			return 0, merr
		}
		if mark.IsZero() {
			// No pass has ever completed. Reporting zero would read as perfectly
			// fresh, so report the service's own age instead: it grows until the
			// first pass lands, which is exactly the alert we want.
			return time.Since(startedAt), nil
		}
		return time.Since(mark), nil
	}); err != nil {
		return fmt.Errorf("register reconciliation watermark gauge: %w", err)
	}
	return nil
}

// runBackgroundJobs drives the periodic maintenance loops: expiring authorized
// holds whose TTL passed (every minute — an expired hold must stop being
// capturable promptly), reaping idempotency keys older than their retention
// window (hourly; the window itself is 24h, so cadence is not critical), and
// relaying the transactional outbox (every 10s — event latency). The first two
// are single-statement queries; the relay delivers to its sink.
func runBackgroundJobs(ctx context.Context, svc *logicv1.Service, relay *logicv1.OutboxRelay, recon *logicv1.Reconciler, reconRepo *repository.ReconciliationRepository, cfg *config.Config, logger *slogx.Logger) {
	expiry := time.NewTicker(time.Minute)
	reap := time.NewTicker(time.Hour)
	outbox := time.NewTicker(outboxRelayInterval)
	doubt := time.NewTicker(resolveDoubtInterval)
	defer expiry.Stop()
	defer reap.Stop()
	defer outbox.Stop()
	defer doubt.Stop()

	// Reconciliation only ticks when a provider ledger is available (recon != nil).
	// A nil channel blocks forever, so the select arm is simply never taken.
	var reconcile <-chan time.Time
	if recon != nil {
		rt := time.NewTicker(reconcileInterval)
		defer rt.Stop()
		reconcile = rt.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-reconcile:
			runJob(ctx, "Reconcile payments vs provider", logger, func(jctx context.Context) (int64, error) {
				_, found, err := recon.Run(jctx, reconcilePageSize)
				if errors.Is(err, domain.ErrLeaseHeld) {
					// Somebody else is mid-pass. Standing down is the correct outcome for
					// a single-writer role, so it must not be logged as a failure — a tick
					// that reports an error every minute is a tick nobody reads.
					return 0, nil
				}
				return int64(found), err
			})
		case <-doubt.C:
			// The request path resolves a payment the moment anyone touches it; this
			// is for the doubt nobody touches — an abandoned checkout, a saga that
			// gave up, a refund the customer is not watching.
			runJob(ctx, "Resolve unknown provider outcomes", logger, func(jctx context.Context) (int64, error) {
				return svc.ResolveOpenDoubt(jctx, resolveDoubtBatch)
			})
		case <-expiry.C:
			runJob(ctx, "Expire stale authorizations", logger, func(jctx context.Context) (int64, error) {
				return svc.ExpireHolds(jctx)
			})
		case <-reap.C:
			runJob(ctx, "Reap idempotency keys", logger, func(jctx context.Context) (int64, error) {
				return svc.ReapIdempotencyKeys(jctx, cfg.Payment.IdempotencyKeyTTL)
			})
			runJob(ctx, "Reap published outbox events", logger, func(jctx context.Context) (int64, error) {
				return relay.ReapPublished(jctx, outboxPublishedRetention)
			})
			runJob(ctx, "Reap old reconciliation runs", logger, func(jctx context.Context) (int64, error) {
				return reconRepo.ReapRuns(jctx, reconRunRetention)
			})
		case <-outbox.C:
			runJob(ctx, "Relay outbox events", logger, func(jctx context.Context) (int64, error) {
				return relay.Relay(jctx, outboxRelayBatch)
			})
		}
	}
}

// runJob executes one maintenance tick under a bounded timeout so a single
// hung query cannot stall the loop, logging the affected-row count or error.
func runJob(ctx context.Context, name string, logger *slogx.Logger, fn func(context.Context) (int64, error)) {
	jctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	n, err := fn(jctx)
	switch {
	case err != nil:
		// Include count: a job can partially succeed (e.g. the relay delivered
		// some events before the sink failed) — hiding it loses that signal.
		logger.Error(ctx, name+" failed", slog.Int64("count", n), slogx.Err(err))
	case n > 0:
		logger.Info(ctx, name+" completed", slog.Int64("count", n))
	}
}

// selectProvider returns the mockpay HTTP client when MOCKPAY_URL is set, else
// the in-memory stub (unit tests and stub-only local runs).
// buildReconciliation wires the reconciler and its internal API handler. The
// reconciler exists only when the provider exposes a ledger to page — the real
// mockpay HTTP client; the in-process stub has nothing to reconcile against, so
// the reconciler is nil (the ticker is skipped and the trigger endpoint answers
// 503). v1 is detect-only: it records drift, never heals.
//
// The handler's runner stays a nil interface when reconciliation is disabled —
// assigning a nil *Reconciler directly would make the interface non-nil (typed
// nil) and defeat the handler's disabled check.
// Returns the repo too, so the background reaper can prune old runs even when
// reconciliation itself is disabled (nil reconciler).
//
// Auto-heal (ADR-012) is wired only when RECON_HEAL_ENABLED is set: the healer
// converges the lost-capture-response window through the payment repo's
// idempotent CaptureWithLedger — the provider is never called. Default off keeps
// the detect-only behaviour of ADR-011.
func buildReconciliation(cfg *config.Config, prov provider.Provider, pool *pgxpool.Pool, capturer logicv1.LedgerCapturer, logger *slogx.Logger) (*logicv1.Reconciler, *v1.ReconciliationHandler, *repository.ReconciliationRepository) {
	ctx := context.Background()
	reconRepo := repository.NewReconciliationRepository(pool)
	ledger, ok := prov.(logicv1.ProviderLedger)
	if !ok {
		return nil, v1.NewReconciliationHandler(nil, reconRepo), reconRepo
	}
	// The lease makes the reconciler a single writer across PROCESSES, so the
	// trigger endpoint and the ticker cannot both run a pass, and neither can two
	// replicas. It does NOT make the service scalable on its own: migration 000007
	// (idempotency_keys.payment_id → subject_id) is not rolling-safe, so
	// replicaCount stays 1 regardless of this.
	opts := []logicv1.ReconcilerOption{
		logicv1.WithLease(repository.NewLeaseRepository(pool)),
	}
	if cfg.Payment.ReconHealEnabled {
		opts = append(opts, logicv1.WithHealer(logicv1.NewCaptureHealer(capturer, time.Now)))
		logger.Info(ctx, "Reconciliation auto-heal enabled (RECON_HEAL_ENABLED)")
	}
	reconciler := logicv1.NewReconciler(reconRepo, ledger, opts...)
	return reconciler, v1.NewReconciliationHandler(reconciler, reconRepo), reconRepo
}

func selectProvider(cfg *config.Config, logger *slogx.Logger) provider.Provider {
	ctx := context.Background()
	if cfg.Payment.ProviderURL != "" {
		logger.Info(ctx, "Using mockpay HTTP provider", slog.String("url", cfg.Payment.ProviderURL))
		return provider.NewHTTPClient(cfg.Payment.ProviderURL)
	}
	logger.Info(ctx, "Using in-memory provider stub")
	return provider.NewStub()
}

// maybeRunSubcommand handles the `migrate` and `mockpay` subcommands, reporting
// whether it handled one (caller then exits/returns). Both need only base
// config, so they run before cfg.Validate().
//
// `migrate` applies the versioned schema migrations (one-shot). `mockpay` runs
// the mock payment provider — a second deployment of this binary, mirroring the
// order-worker pattern. Payment has no `seed` subcommand (no demo data).
func maybeRunSubcommand(cfg *config.Config, logger *slogx.Logger) bool {
	ctx := context.Background()
	if len(os.Args) <= 1 {
		return false
	}
	switch os.Args[1] {
	case "migrate":
		if err := migratex.Run(migrations.FS, "sql", cfg.Database.BuildDSN()); err != nil {
			logger.Fatal(ctx, "Schema migration failed", slogx.Err(err))
		}
		logger.Info(ctx, "Schema migrations applied")
		return true
	case "mockpay":
		runMockpay(cfg, logger)
		return true
	default:
		return false
	}
}

// startGRPC serves the internal PaymentService on :9090 (east-west, saga-only).
// Returns the server so shutdown can GracefulStop it before the pool closes.
func startGRPC(cfg *config.Config, logger *slogx.Logger, svc *logicv1.Service) (*grpc.Server, error) {
	ctx := context.Background()
	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", ":"+cfg.GRPC.Port)
	if err != nil {
		return nil, fmt.Errorf("listen gRPC :%s: %w", cfg.GRPC.Port, err)
	}
	grpcSrv, _ := grpcx.NewServer(logger.Slog())
	paymentv1.RegisterPaymentServiceServer(grpcSrv, grpcv1.NewServer(svc))
	go func() {
		logger.Info(ctx, "Starting gRPC server", slog.String(fieldPort, cfg.GRPC.Port))
		if err := grpcSrv.Serve(lis); err != nil {
			logger.Error(ctx, "gRPC server error", slogx.Err(err))
		}
	}()
	return grpcSrv, nil
}

// runMockpay serves the mock provider until SIGTERM/SIGINT, then drains.
func runMockpay(cfg *config.Config, logger *slogx.Logger) {
	ctx := context.Background()
	// mockpay is a deployed service (a real network hop), so it gets the same
	// OTel wiring as the main binary: this installs the TracerProvider + W3C
	// propagator that let the otelhttp handler below open a server span joining
	// the caller's trace (the money-hop's far end).
	obsShutdown, logger := initObservability(logger)
	if obsShutdown != nil {
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = obsShutdown.Shutdown(sctx)
		}()
	}

	var emitter mockpay.Emitter
	switch {
	case cfg.Payment.WebhookURL == "":
		logger.Info(ctx, "mockpay webhook emission disabled (MOCKPAY_WEBHOOK_URL empty)")
	case cfg.Payment.WebhookSecret == "":
		logger.Error(ctx, "MOCKPAY_WEBHOOK_URL set but MOCKPAY_WEBHOOK_SECRET empty; emission disabled")
	default:
		emitter = mockpay.NewWebhookEmitter(cfg.Payment.WebhookURL, cfg.Payment.WebhookSecret, logger)
		logger.Info(ctx, "mockpay webhook emission enabled", slog.String("url", cfg.Payment.WebhookURL))
	}
	srv := &http.Server{
		Addr:              ":" + cfg.Service.Port,
		Handler:           otelhttp.NewHandler(mockpay.New(logger, emitter).Handler(), "mockpay"),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		logger.Info(ctx, "mockpay listening", slog.String(fieldPort, cfg.Service.Port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(ctx, "mockpay server error", slogx.Err(err))
		}
	}()

	logger.ProcessStarted(ctx, slogx.ComponentMockpay)

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-sigCtx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcome := slogx.OutcomeGraceful
	if err := srv.Shutdown(shutdownCtx); err != nil {
		outcome = slogx.OutcomeError
		logger.Error(ctx, "mockpay shutdown error", slogx.Err(err))
	}
	logger.Info(ctx, "mockpay shutdown complete")
	// Written before the deferred OTel shutdown runs, so it is exported.
	logger.ProcessStopped(ctx, slogx.ComponentMockpay, outcome)
}

// initObservability is the single OTel wiring point (RFC-0014) — traces per
// TRACING_ENABLED, OTLP metrics (the only pipeline since the P3 cutover;
// OTEL_METRICS_ENABLED defaults on, =false is a kill switch), logs behind
// OTEL_LOGS_ENABLED. The config is built once so the tracer
// scope name and the startup log reflect the values obsx actually uses.
// Returns the SDK shutdown handle (nil when setup failed) and the logger the
// caller must continue with — rebuilt with the OTLP tee on success, the
// original otherwise.
func initObservability(logger *slogx.Logger) (interface{ Shutdown(context.Context) error }, *slogx.Logger) {
	ctx := context.Background()
	otelCfg := obsx.ConfigFromEnv()
	obs, err := obsx.SetupObservability(context.Background(), otelCfg)
	if err != nil {
		logger.Warn(ctx, "Failed to initialize OpenTelemetry", slogx.Err(err))
		return nil, logger
	}
	// The facade reaches OTLP through the global logger provider obsx
	// installed; rebuilding it only wires Flush, so a Fatal record is
	// exported before the process exits.
	logger = slogx.New(slogx.Config{Level: os.Getenv("LOG_LEVEL"), Flush: obs.ForceFlush})
	slogx.SetDefault(logger)
	logger.Info(ctx, "OpenTelemetry initialized",
		slog.Bool("traces", obs.Enabled().Traces),
		slog.Bool("otlp_metrics", obs.Enabled().Metrics),
		slog.Bool("otlp_logs", obs.Enabled().Logs),
		slog.String("endpoint", otelCfg.Endpoint),
		slog.Float64("sample_rate", otelCfg.SampleRate),
	)
	return obs, logger
}

// initProfiling starts Pyroscope continuous profiling via the shared obsx helper
// and returns a cleanup func (a no-op when profiling is disabled or setup fails).
func initProfiling(cfg *config.Config, logger *slogx.Logger) func() {
	ctx := context.Background()
	if !cfg.Profiling.Enabled {
		logger.Info(ctx, "Profiling disabled (PROFILING_ENABLED=false)")
		return func() { /* profiling disabled: nothing to stop */ }
	}
	stopProfiling, err := obsx.SetupProfiling()
	if err != nil {
		logger.Warn(ctx, "Failed to initialize profiling", slogx.Err(err))
		return func() { /* setup failed: nothing to stop */ }
	}
	logger.Info(ctx, "Profiling initialized", slog.String("endpoint", cfg.Profiling.Endpoint))
	return func() {
		if err := stopProfiling(context.Background()); err != nil {
			logger.Error(ctx, "Profiling shutdown error", slogx.Err(err))
		}
	}
}

// httpHandlers groups the four route registrars setupServer mounts. They used to
// arrive as four separate parameters, which put the function at ten — past the
// seven the maintainability gate allows once the migration made the signature
// new code.
type httpHandlers struct {
	payment   *v1.Handler
	protected *v1.ProtectedHandler
	webhook   *v1.WebhookHandler
	recon     *v1.ReconciliationHandler
}

func setupServer(
	cfg *config.Config,
	otelServiceName string,
	logger *slogx.Logger,
	verifier *authmw.Verifier,
	staffVerifier *authmw.Verifier,
	h httpHandlers,
	isShuttingDown *atomic.Bool,
) *http.Server {
	// gin.New, not gin.Default: Default installs gin's own logger and
	// recovery, which print the raw path and client address past the facade.
	r := gin.New()

	r.Use(httpmw.Tracing(otelServiceName))
	r.Use(httpmw.Logging(logger.Slog()))
	r.Use(httpmw.Recovery(logger.Slog()))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{fieldStatus: "ok"})
	})
	r.GET("/ready", func(c *gin.Context) {
		if isShuttingDown.Load() {
			c.JSON(http.StatusServiceUnavailable, gin.H{fieldStatus: "shutting_down"})
			return
		}
		c.JSON(http.StatusOK, gin.H{fieldStatus: "ok"})
	})

	// Payment v1 routes — private (JWT required) + internal (cluster-only,
	// NetworkPolicy is the fence). Variant A edge naming.
	v1.RegisterRoutes(r, h.payment, verifier)
	// Protected: the Backoffice's cross-customer reads (RFC-0023),
	// staff-realm verified + role-gated (ADR-050).
	v1.RegisterProtectedRoutes(r, h.protected, staffVerifier)
	// Public webhook route — no JWT; the HMAC signature is the credential.
	v1.RegisterWebhookRoutes(r, h.webhook)
	// Internal reconciliation API — cluster-only (NetworkPolicy is the fence),
	// never routed through the gateway.
	v1.RegisterReconciliationRoutes(r, h.recon)

	return &http.Server{
		Addr:              ":" + cfg.Service.Port,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}
}

func runGracefulShutdown(
	cfg *config.Config,
	srv *http.Server,
	tp interface{ Shutdown(context.Context) error },
	pool interface{ Close() },
	logger *slogx.Logger,
	isShuttingDown *atomic.Bool,
	beforePoolClose func(),
) {
	ctx := context.Background()
	go func() {
		logger.Info(ctx, "Starting payment service", slog.String(fieldPort, cfg.Service.Port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(ctx, "Failed to start server", slogx.Err(err))
		}
	}()

	logger.ProcessStarted(ctx, slogx.ComponentAPI)

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	<-sigCtx.Done()
	logger.Info(ctx, "Shutdown signal received")

	isShuttingDown.Store(true)
	drainDelay := cfg.GetReadinessDrainDelayDuration()
	if drainDelay > 0 {
		logger.Info(ctx, "Readiness drain delay started", slog.Duration("delay", drainDelay))
		time.Sleep(drainDelay)
	}

	shutdownTimeout := cfg.GetShutdownTimeoutDuration()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	logger.Info(ctx, "Shutting down server...", slog.Duration("timeout", shutdownTimeout))

	outcome := slogx.OutcomeGraceful
	if err := srv.Shutdown(shutdownCtx); err != nil {
		outcome = slogx.OutcomeError
		logger.Error(ctx, "HTTP server shutdown error", slogx.Err(err))
	} else {
		logger.Info(ctx, "HTTP server shutdown complete")
	}

	if beforePoolClose != nil {
		beforePoolClose()
		logger.Info(ctx, "Background jobs stopped")
	}

	pool.Close()
	logger.Info(ctx, "Database pool closed")

	// process.stopped goes out BEFORE the OTel SDK shuts down: a record
	// emitted after it is dropped rather than exported.
	logger.ProcessStopped(ctx, slogx.ComponentAPI, outcome)

	// Shutdown the OTel SDK — flushes pending spans plus any OTLP
	// metrics/logs providers built behind the RFC-0014 flags.
	if tp != nil {
		if err := tp.Shutdown(shutdownCtx); err != nil {
			logger.Error(ctx, "OpenTelemetry shutdown error", slogx.Err(err))
		} else {
			logger.Info(ctx, "OpenTelemetry shutdown complete")
		}
	}

	logger.Info(ctx, "Graceful shutdown complete")
}

// buildVerifiers constructs the customer-realm verifier (private routes) and
// the staff-realm verifier (protected Backoffice group, ADR-050). Split from
// run() to keep it within the lint budget; NewVerifier does not block on an
// unreachable JWKS.
func buildVerifiers(cfg *config.Config) (*authmw.Verifier, *authmw.Verifier, error) {
	verifier, err := authmw.NewVerifier(authmw.Config{
		Issuer:   cfg.OIDCIssuer,
		Audience: cfg.OIDCAudience,
		JWKSURL:  cfg.OIDCJWKSURL,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("JWKS verifier init: %w", err)
	}
	staffVerifier, err := authmw.NewVerifier(authmw.Config{
		Issuer:   cfg.OIDCStaffIssuer,
		Audience: cfg.OIDCAudience,
		JWKSURL:  cfg.OIDCStaffJWKSURL,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("staff JWKS verifier init: %w", err)
	}
	return verifier, staffVerifier, nil
}
