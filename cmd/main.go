package main

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
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
	"github.com/duynhlab/payment-service/middleware"
	paymentv1 "github.com/duynhlab/pkg/proto/payment/v1"

	"github.com/duynhlab/pkg/authmw"
	"github.com/duynhlab/pkg/grpcx"
	"github.com/duynhlab/pkg/idempotency"
	"github.com/duynhlab/pkg/logger/zapx"
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
	// Published events are pruned after this window; the durable audit trail is
	// the ledger, so the outbox only needs a short replay buffer.
	outboxPublishedRetention = 7 * 24 * time.Hour
	// Reconciliation runs are pruned after this window (discrepancies cascade);
	// they are operational history, not the source of truth.
	reconRunRetention = 30 * 24 * time.Hour
)

// outboxLogPublisher is the P2 delivery sink: it logs each event. A real broker
// replaces it behind logicv1.Publisher with no relay change.
type outboxLogPublisher struct{ logger *zap.Logger }

func (p outboxLogPublisher) Publish(_ context.Context, e domain.OutboxEvent) error {
	p.logger.Info("Outbox event published",
		zap.Int64("outbox_id", e.ID),
		zap.String("event_type", e.EventType),
		zap.ByteString("payload", e.Payload),
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
	cfg := config.Load()

	logger, err := zapx.New(os.Getenv("LOG_LEVEL"))
	if err != nil {
		return fmt.Errorf("initialize logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	// `<binary> migrate` runs embedded schema migrations (its SQL runs and the
	// process exits). No args serves the app.
	if maybeRunSubcommand(cfg, logger) {
		return nil
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("configuration validation: %w", err)
	}

	logger.Info("Service starting",
		zap.String("service", cfg.Service.Name),
		zap.String("version", cfg.Service.Version),
		zap.String("env", cfg.Service.Env),
		zap.String(fieldPort, cfg.Service.Port),
	)

	// Initialize the OTel→Prometheus bridge FIRST (otelgrpc/otelgin metrics on
	// the scraped /metrics endpoint — the flag-off status quo). When
	// OTEL_METRICS_ENABLED=true, initObservability below installs the OTLP
	// MeterProvider as the global AFTER this, deliberately superseding the
	// bridge (RFC-0014 dual-emit: client_golang scrape stays untouched either
	// way; only the OTel-instrumented metrics switch transport).
	metricsShutdown := initMetrics(cfg, logger)
	defer metricsShutdown()

	tp := initObservability(logger)

	profilingShutdown := initProfiling(cfg, logger)
	defer profilingShutdown()

	pool, err := database.Connect(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()
	logger.Info("Database connection pool established")

	// Local RS256 JWT verification (cached JWKS) is the only credential — no
	// gRPC fallback. NewVerifier does not block on an unreachable JWKS — it
	// refreshes in the background, so a verifier is safe to build at startup.
	verifier, err := authmw.NewVerifier(cfg.JWKSURL, cfg.JWTIssuer, cfg.JWTAudience)
	if err != nil {
		return fmt.Errorf("JWKS verifier init: %w", err)
	}

	// Repositories + provider + logic. P1 runs the in-memory provider stub;
	// the real mockpay HTTP client lands in P2 behind the same interface.
	paymentRepo := repository.NewPaymentRepository(pool)
	idemRepo := idempotency.New(pool, cfg.Payment.IdempotencyLockTakeover)
	prov := selectProvider(cfg, logger)
	paymentService := logicv1.NewService(paymentRepo, idemRepo, prov, cfg.Payment.AuthHoldTTL)
	paymentHandler := v1.NewHandler(paymentService)

	reconciler, reconHandler, reconRepo := buildReconciliation(cfg, prov, pool, paymentRepo, logger)

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
	srv := setupServer(cfg, logger, verifier, paymentHandler, webhookHandler, reconHandler, &isShuttingDown)
	runGracefulShutdown(cfg, srv, tp, pool, logger, &isShuttingDown, stopJobsAndWait)
	return nil
}

// runBackgroundJobs drives the periodic maintenance loops: expiring authorized
// holds whose TTL passed (every minute — an expired hold must stop being
// capturable promptly), reaping idempotency keys older than their retention
// window (hourly; the window itself is 24h, so cadence is not critical), and
// relaying the transactional outbox (every 10s — event latency). The first two
// are single-statement queries; the relay delivers to its sink.
func runBackgroundJobs(ctx context.Context, svc *logicv1.Service, relay *logicv1.OutboxRelay, recon *logicv1.Reconciler, reconRepo *repository.ReconciliationRepository, cfg *config.Config, logger *zap.Logger) {
	expiry := time.NewTicker(time.Minute)
	reap := time.NewTicker(time.Hour)
	outbox := time.NewTicker(outboxRelayInterval)
	defer expiry.Stop()
	defer reap.Stop()
	defer outbox.Stop()

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
				return int64(found), err
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
func runJob(ctx context.Context, name string, logger *zap.Logger, fn func(context.Context) (int64, error)) {
	jctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	n, err := fn(jctx)
	switch {
	case err != nil:
		// Include count: a job can partially succeed (e.g. the relay delivered
		// some events before the sink failed) — hiding it loses that signal.
		logger.Error(name+" failed", zap.Int64("count", n), zap.Error(err))
	case n > 0:
		logger.Info(name+" completed", zap.Int64("count", n))
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
func buildReconciliation(cfg *config.Config, prov provider.Provider, pool *pgxpool.Pool, capturer logicv1.LedgerCapturer, logger *zap.Logger) (*logicv1.Reconciler, *v1.ReconciliationHandler, *repository.ReconciliationRepository) {
	reconRepo := repository.NewReconciliationRepository(pool)
	ledger, ok := prov.(logicv1.ProviderLedger)
	if !ok {
		return nil, v1.NewReconciliationHandler(nil, reconRepo), reconRepo
	}
	opts := []logicv1.ReconcilerOption{logicv1.WithLogger(logger)}
	if cfg.Payment.ReconHealEnabled {
		opts = append(opts, logicv1.WithHealer(logicv1.NewCaptureHealer(capturer, time.Now)))
		logger.Info("Reconciliation auto-heal enabled (RECON_HEAL_ENABLED)")
	}
	reconciler := logicv1.NewReconciler(reconRepo, ledger, opts...)
	return reconciler, v1.NewReconciliationHandler(reconciler, reconRepo), reconRepo
}

func selectProvider(cfg *config.Config, logger *zap.Logger) provider.Provider {
	if cfg.Payment.ProviderURL != "" {
		logger.Info("Using mockpay HTTP provider", zap.String("url", cfg.Payment.ProviderURL))
		return provider.NewHTTPClient(cfg.Payment.ProviderURL)
	}
	logger.Info("Using in-memory provider stub")
	return provider.NewStub()
}

// maybeRunSubcommand handles the `migrate` and `mockpay` subcommands, reporting
// whether it handled one (caller then exits/returns). Both need only base
// config, so they run before cfg.Validate().
//
// `migrate` applies the versioned schema migrations (one-shot). `mockpay` runs
// the mock payment provider — a second deployment of this binary, mirroring the
// order-worker pattern. Payment has no `seed` subcommand (no demo data).
func maybeRunSubcommand(cfg *config.Config, logger *zap.Logger) bool {
	if len(os.Args) <= 1 {
		return false
	}
	switch os.Args[1] {
	case "migrate":
		if err := migratex.Run(migrations.FS, "sql", cfg.Database.BuildDSN()); err != nil {
			logger.Fatal("Schema migration failed", zap.Error(err))
		}
		logger.Info("Schema migrations applied")
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
func startGRPC(cfg *config.Config, logger *zap.Logger, svc *logicv1.Service) (*grpc.Server, error) {
	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", ":"+cfg.GRPC.Port)
	if err != nil {
		return nil, fmt.Errorf("listen gRPC :%s: %w", cfg.GRPC.Port, err)
	}
	grpcSrv, _ := grpcx.NewServer()
	paymentv1.RegisterPaymentServiceServer(grpcSrv, grpcv1.NewServer(svc))
	go func() {
		logger.Info("Starting gRPC server", zap.String(fieldPort, cfg.GRPC.Port))
		if err := grpcSrv.Serve(lis); err != nil {
			logger.Error("gRPC server error", zap.Error(err))
		}
	}()
	return grpcSrv, nil
}

// runMockpay serves the mock provider until SIGTERM/SIGINT, then drains.
func runMockpay(cfg *config.Config, logger *zap.Logger) {
	var emitter mockpay.Emitter
	switch {
	case cfg.Payment.WebhookURL == "":
		logger.Info("mockpay webhook emission disabled (MOCKPAY_WEBHOOK_URL empty)")
	case cfg.Payment.WebhookSecret == "":
		logger.Error("MOCKPAY_WEBHOOK_URL set but MOCKPAY_WEBHOOK_SECRET empty; emission disabled")
	default:
		emitter = mockpay.NewWebhookEmitter(cfg.Payment.WebhookURL, cfg.Payment.WebhookSecret, logger)
		logger.Info("mockpay webhook emission enabled", zap.String("url", cfg.Payment.WebhookURL))
	}
	srv := &http.Server{
		Addr:              ":" + cfg.Service.Port,
		Handler:           mockpay.New(logger, emitter).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		logger.Info("mockpay listening", zap.String(fieldPort, cfg.Service.Port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("mockpay server error", zap.Error(err))
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("mockpay shutdown error", zap.Error(err))
	}
	logger.Info("mockpay shutdown complete")
}

// initMetrics installs the shared obsx OTel→Prometheus metrics bridge so any
// OTel-instrumented client surfaces its RED metrics on the existing /metrics
// endpoint. It returns a cleanup func (a no-op when metrics are disabled or
// setup fails).
func initMetrics(cfg *config.Config, logger *zap.Logger) func() {
	if !cfg.Metrics.Enabled {
		return func() { /* metrics disabled: no provider to shut down */ }
	}
	metricsShutdown, err := obsx.SetupMetrics()
	if err != nil {
		logger.Warn("Failed to set up metrics bridge", zap.Error(err))
		return func() { /* setup failed: no provider to shut down */ }
	}
	logger.Info("Metrics bridge initialized")
	return func() {
		if err := metricsShutdown(context.Background()); err != nil {
			logger.Error("Metrics provider shutdown error", zap.Error(err))
		}
	}
}

// initObservability is the single OTel wiring point (RFC-0014) — traces per
// TRACING_ENABLED, OTLP metrics/logs behind OTEL_METRICS_ENABLED/
// OTEL_LOGS_ENABLED (default off). The config is built once so the tracer
// scope name and the startup log reflect the values obsx actually uses.
// Returns the SDK shutdown handle (nil when setup failed).
func initObservability(logger *zap.Logger) interface{ Shutdown(context.Context) error } {
	otelCfg := obsx.ConfigFromEnv()
	middleware.SetServiceName(otelCfg.ServiceName)
	obs, err := obsx.SetupObservability(context.Background(), otelCfg)
	if err != nil {
		logger.Warn("Failed to initialize OpenTelemetry", zap.Error(err))
		return nil
	}
	logger.Info("OpenTelemetry initialized",
		zap.Bool("traces", obs.TracerProvider != nil),
		zap.Bool("otlp_metrics", obs.MeterProvider != nil),
		zap.Bool("otlp_logs", obs.LoggerProvider != nil),
		zap.String("endpoint", otelCfg.Endpoint),
		zap.Float64("sample_rate", otelCfg.SampleRate),
	)
	return obs
}

// initProfiling starts Pyroscope continuous profiling via the shared obsx helper
// and returns a cleanup func (a no-op when profiling is disabled or setup fails).
func initProfiling(cfg *config.Config, logger *zap.Logger) func() {
	if !cfg.Profiling.Enabled {
		logger.Info("Profiling disabled (PROFILING_ENABLED=false)")
		return func() { /* profiling disabled: nothing to stop */ }
	}
	stopProfiling, err := obsx.SetupProfiling()
	if err != nil {
		logger.Warn("Failed to initialize profiling", zap.Error(err))
		return func() { /* setup failed: nothing to stop */ }
	}
	logger.Info("Profiling initialized", zap.String("endpoint", cfg.Profiling.Endpoint))
	return func() {
		if err := stopProfiling(context.Background()); err != nil {
			logger.Error("Profiling shutdown error", zap.Error(err))
		}
	}
}

func setupServer(cfg *config.Config, logger *zap.Logger, verifier *authmw.Verifier, paymentHandler *v1.Handler, webhookHandler *v1.WebhookHandler, reconHandler *v1.ReconciliationHandler, isShuttingDown *atomic.Bool) *http.Server {
	r := gin.Default()

	r.Use(middleware.TracingMiddleware())
	r.Use(middleware.LoggingMiddleware(logger))
	r.Use(middleware.PrometheusMiddleware())

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
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Payment v1 routes — private (JWT required) + internal (cluster-only,
	// NetworkPolicy is the fence). Variant A edge naming.
	v1.RegisterRoutes(r, paymentHandler, verifier)
	// Public webhook route — no JWT; the HMAC signature is the credential.
	v1.RegisterWebhookRoutes(r, webhookHandler)
	// Internal reconciliation API — cluster-only (NetworkPolicy is the fence),
	// never routed through the gateway.
	v1.RegisterReconciliationRoutes(r, reconHandler)

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
	logger *zap.Logger,
	isShuttingDown *atomic.Bool,
	beforePoolClose func(),
) {
	go func() {
		logger.Info("Starting payment service", zap.String(fieldPort, cfg.Service.Port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Failed to start server", zap.Error(err))
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	<-ctx.Done()
	logger.Info("Shutdown signal received")

	isShuttingDown.Store(true)
	drainDelay := cfg.GetReadinessDrainDelayDuration()
	if drainDelay > 0 {
		logger.Info("Readiness drain delay started", zap.Duration("delay", drainDelay))
		time.Sleep(drainDelay)
	}

	shutdownTimeout := cfg.GetShutdownTimeoutDuration()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	logger.Info("Shutting down server...", zap.Duration("timeout", shutdownTimeout))

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", zap.Error(err))
	} else {
		logger.Info("HTTP server shutdown complete")
	}

	if beforePoolClose != nil {
		beforePoolClose()
		logger.Info("Background jobs stopped")
	}

	pool.Close()
	logger.Info("Database pool closed")

	// Shutdown the OTel SDK — flushes pending spans plus any OTLP
	// metrics/logs providers built behind the RFC-0014 flags.
	if tp != nil {
		if err := tp.Shutdown(shutdownCtx); err != nil {
			logger.Error("OpenTelemetry shutdown error", zap.Error(err))
		} else {
			logger.Info("OpenTelemetry shutdown complete")
		}
	}

	logger.Info("Graceful shutdown complete")
}
