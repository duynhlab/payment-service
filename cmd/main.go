package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/duynhlab/payment-service/config"
	migrations "github.com/duynhlab/payment-service/db/migrations"
	database "github.com/duynhlab/payment-service/internal/core/database"
	"github.com/duynhlab/payment-service/internal/core/provider"
	"github.com/duynhlab/payment-service/internal/core/repository"
	logicv1 "github.com/duynhlab/payment-service/internal/logic/v1"
	v1 "github.com/duynhlab/payment-service/internal/web/v1"
	"github.com/duynhlab/payment-service/middleware"
	"github.com/duynhlab/pkg/authmw"
	"github.com/duynhlab/pkg/logger/zapx"
	"github.com/duynhlab/pkg/migratex"
	"github.com/duynhlab/pkg/obsx"
)

func main() {
	cfg := config.Load()

	logger, err := zapx.New(os.Getenv("LOG_LEVEL"))
	if err != nil {
		panic("Failed to initialize logger: " + err.Error())
	}
	defer func() { _ = logger.Sync() }()

	// `<binary> migrate` runs embedded schema migrations (its SQL runs and the
	// process exits). No args serves the app.
	if maybeRunSubcommand(cfg, logger) {
		return
	}

	if err := cfg.Validate(); err != nil {
		panic("Configuration validation failed: " + err.Error())
	}

	logger.Info("Service starting",
		zap.String("service", cfg.Service.Name),
		zap.String("version", cfg.Service.Version),
		zap.String("env", cfg.Service.Env),
		zap.String("port", cfg.Service.Port),
	)

	tp := initTracing(cfg, logger)

	profilingShutdown := initProfiling(cfg, logger)
	defer profilingShutdown()

	metricsShutdown := initMetrics(cfg, logger)
	defer metricsShutdown()

	pool, err := database.Connect(context.Background(), cfg)
	if err != nil {
		logger.Error("Failed to connect to database", zap.Error(err))
		return
	}
	defer pool.Close()
	logger.Info("Database connection pool established")

	// Local RS256 JWT verification (cached JWKS) is the only credential — no
	// gRPC fallback. NewVerifier does not block on an unreachable JWKS — it
	// refreshes in the background, so a verifier is safe to build at startup.
	verifier, err := authmw.NewVerifier(cfg.JWKSURL, cfg.JWTIssuer, cfg.JWTAudience)
	if err != nil {
		logger.Error("JWKS verifier init failed", zap.Error(err))
		return
	}

	// Repositories + provider + logic. P1 runs the in-memory provider stub;
	// the real mockpay HTTP client lands in P2 behind the same interface.
	paymentRepo := repository.NewPaymentRepository(pool)
	idemRepo := repository.NewIdempotencyRepository(pool, cfg.Payment.IdempotencyLockTakeover)
	paymentService := logicv1.NewService(paymentRepo, idemRepo, provider.NewStub(), cfg.Payment.AuthHoldTTL)
	paymentHandler := v1.NewHandler(paymentService)

	var isShuttingDown atomic.Bool
	srv := setupServer(cfg, logger, verifier, paymentHandler, &isShuttingDown)
	runGracefulShutdown(cfg, srv, tp, pool, logger, &isShuttingDown)
}

// maybeRunSubcommand handles the `migrate` subcommand, reporting whether it
// handled one (caller then exits). It needs only DB config, so it runs before
// cfg.Validate().
//
// `migrate` applies the versioned schema migrations and runs in every
// environment (init container, direct DB host). Payment has no `seed`
// subcommand — the service holds no demo data.
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
	default:
		return false
	}
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

func initTracing(cfg *config.Config, logger *zap.Logger) interface{ Shutdown(context.Context) error } {
	if !cfg.Tracing.Enabled {
		logger.Info("Tracing disabled (TRACING_ENABLED=false)")
		return nil
	}
	tp, err := middleware.InitTracing(cfg)
	if err != nil {
		logger.Warn("Failed to initialize tracing", zap.Error(err))
		return nil
	}
	logger.Info("Tracing initialized",
		zap.String("endpoint", cfg.Tracing.Endpoint),
		zap.Float64("sample_rate", cfg.Tracing.SampleRate),
	)
	return tp
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

func setupServer(cfg *config.Config, logger *zap.Logger, verifier *authmw.Verifier, paymentHandler *v1.Handler, isShuttingDown *atomic.Bool) *http.Server {
	r := gin.Default()

	r.Use(middleware.TracingMiddleware())
	r.Use(middleware.LoggingMiddleware(logger))
	r.Use(middleware.PrometheusMiddleware())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	r.GET("/ready", func(c *gin.Context) {
		if isShuttingDown.Load() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "shutting_down"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Payment v1 routes — private (JWT required) + internal (cluster-only,
	// NetworkPolicy is the fence). Variant A edge naming.
	v1.RegisterRoutes(r, paymentHandler, verifier)

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
) {
	go func() {
		logger.Info("Starting payment service", zap.String("port", cfg.Service.Port))
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

	pool.Close()
	logger.Info("Database pool closed")

	if tp != nil {
		if err := tp.Shutdown(shutdownCtx); err != nil {
			logger.Error("Tracer shutdown error", zap.Error(err))
		} else {
			logger.Info("Tracer shutdown complete")
		}
	}

	logger.Info("Graceful shutdown complete")
}
