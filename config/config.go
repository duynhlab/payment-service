// Package config provides centralized configuration management for all microservices
// with validation, type safety, and clear documentation for SRE/DevOps teams.
//
// Configuration Sources (12-factor app principles):
//  1. Default values (hardcoded)
//  2. .env file (local development via godotenv)
//  3. Environment variables (Kubernetes runtime)
//  4. Helm values → deployment.yaml → env/extraEnv → container environment
//
// Usage:
//
//	import "github.com/duynhlab/payment-service/config"
//
//	func main() {
//	    cfg := config.Load()
//	    if err := cfg.Validate(); err != nil {
//	        log.Fatal(err)
//	    }
//	    // Use cfg.Service.Port, cfg.Tracing.Endpoint, etc.
//	}
package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// defaultServiceName is the fallback service name when SERVICE_NAME is not set
const defaultServiceName = "payment"

// Config holds all configuration for a microservice
type Config struct {
	Service         ServiceConfig   // Service-specific settings (port, name, version)
	Tracing         TracingConfig   // OpenTelemetry/Tempo configuration
	Profiling       ProfilingConfig // Pyroscope continuous profiling
	Logging         LoggingConfig   // Structured logging (Zap)
	Metrics         MetricsConfig   // Prometheus metrics
	Database        DatabaseConfig  // PostgreSQL database configuration
	Payment         PaymentConfig   // Payment domain settings (auth holds, idempotency, provider)
	ShutdownTimeout int             // Graceful shutdown timeout in seconds - from SHUTDOWN_TIMEOUT env (default: 10)
	// ReadinessDrainDelay: delay after failing readiness before shutting down the HTTP server.
	// This gives Kubernetes/Service routing time to stop sending new traffic.
	// From READINESS_DRAIN_DELAY env (default: 5s, max: 30s).
	ReadinessDrainDelay int
	JWKSURL             string // Auth JWKS endpoint for local JWT verification - from AUTH_JWKS_URL env
	JWTIssuer           string // Expected JWT issuer (iss) - from JWT_ISSUER env
	JWTAudience         string // Expected JWT audience (aud) - from JWT_AUDIENCE env
}

// PaymentConfig holds payment-domain settings: authorization-hold expiry,
// idempotency-key retention/lock takeover, and the (optional) mock provider URL.
type PaymentConfig struct {
	AuthHoldTTL             time.Duration // How long an authorization hold stays capturable - from AUTH_HOLD_TTL env (default: 168h = 7 days)
	IdempotencyKeyTTL       time.Duration // Retention window for stored idempotency keys - from IDEMPOTENCY_KEY_TTL env (default: 24h)
	IdempotencyLockTakeover time.Duration // Age after which an in-progress idempotency lock may be taken over - from IDEMPOTENCY_LOCK_TAKEOVER env (default: 90s)
	ProviderURL             string        // Mock payment provider base URL - from MOCKPAY_URL env (P1 uses the in-memory stub when empty)
	WebhookSecret           string        // Shared HMAC secret for mockpay webhook signatures - from MOCKPAY_WEBHOOK_SECRET env
	WebhookURL              string        // Where mockpay POSTs signed webhooks - from MOCKPAY_WEBHOOK_URL env (empty = mockpay emits nothing)
}

// ServiceConfig defines basic service configuration
type ServiceConfig struct {
	Name    string // Service name (e.g., "auth", "user") - from SERVICE_NAME env
	Port    string // HTTP server port (default: "8080") - from PORT env
	Version string // Service version (optional) - from VERSION env
	Env     string // Environment (dev/staging/production) - from ENV env
}

// TracingConfig defines OpenTelemetry tracing configuration
// Traces are sent to OpenTelemetry Collector for distributed tracing analysis
type TracingConfig struct {
	Enabled            bool    // Enable tracing (default: true) - from TRACING_ENABLED env
	Endpoint           string  // OTel Collector endpoint - from OTEL_COLLECTOR_ENDPOINT env
	SampleRate         float64 // Trace sampling rate (0.0-1.0) - from OTEL_SAMPLE_RATE env
	ServiceName        string  // Service name for traces (defaults to ServiceConfig.Name)
	MaxExportBatchSize int     // Max spans per batch (default: 512)
}

// ProfilingConfig defines Pyroscope continuous profiling configuration
type ProfilingConfig struct {
	Enabled     bool   // Enable profiling (default: true) - from PROFILING_ENABLED env
	Endpoint    string // Pyroscope endpoint - from PYROSCOPE_ENDPOINT env
	ServiceName string // Service name for profiling (defaults to ServiceConfig.Name)
}

// LoggingConfig defines structured logging configuration
type LoggingConfig struct {
	Level  string // Log level: debug, info, warn, error (default: "info") - from LOG_LEVEL env
	Format string // Log format: json, console (default: "json") - from LOG_FORMAT env
}

// MetricsConfig defines Prometheus metrics configuration
type MetricsConfig struct {
	Enabled bool   // Enable metrics (default: true) - from METRICS_ENABLED env
	Path    string // Metrics endpoint path (default: "/metrics") - from METRICS_PATH env
}

// DatabaseConfig defines PostgreSQL database configuration
// All database connections use separate environment variables (not DATABASE_URL string)
type DatabaseConfig struct {
	Host           string // Database host - from DB_HOST env
	Port           string // Database port - from DB_PORT env (default: "5432")
	Name           string // Database name - from DB_NAME env
	User           string // Database user - from DB_USER env
	Password       string // Database password - from DB_PASSWORD env
	SSLMode        string // SSL mode - from DB_SSLMODE env (default: "disable")
	MaxConnections int    // Max connections - from DB_POOL_MAX_CONNECTIONS env (default: 25)
	PoolMode       string // Pool mode - from DB_POOL_MODE env (optional)
	PoolerType     string // Pooler type - from DB_POOLER_TYPE env (optional)
}

// BuildDSN constructs PostgreSQL connection string from config
func (c *DatabaseConfig) BuildDSN() string {
	// Format: postgresql://user:password@host:port/dbname?sslmode=disable
	hostPort := net.JoinHostPort(c.Host, c.Port)
	return fmt.Sprintf("postgresql://%s:%s@%s/%s?sslmode=%s",
		c.User, c.Password, hostPort, c.Name, c.SSLMode)
}

// Load reads configuration from environment variables with defaults
// It automatically loads .env file if present (for local development)
//
// Priority: .env file < environment variables
// This means ENV vars override .env file values (production takes precedence)
func Load() *Config {
	// Load .env file if exists (for local development)
	// godotenv.Load() fails silently if .env doesn't exist - perfect for production
	_ = godotenv.Load()

	return &Config{
		Service: ServiceConfig{
			Name:    getEnv("SERVICE_NAME", defaultServiceName),
			Port:    getEnv("PORT", "8080"),
			Version: getEnv("VERSION", "dev"),
			Env:     getEnv("ENV", "development"),
		},
		Tracing: TracingConfig{
			Enabled:            getEnvBool("TRACING_ENABLED", true),
			Endpoint:           getEnv("OTEL_COLLECTOR_ENDPOINT", "otel-collector-opentelemetry-collector.monitoring.svc.cluster.local:4318"),
			SampleRate:         getEnvFloat("OTEL_SAMPLE_RATE", 0.1), // 10% default (production)
			ServiceName:        getEnv("SERVICE_NAME", defaultServiceName),
			MaxExportBatchSize: getEnvInt("OTEL_BATCH_SIZE", 512),
		},
		Profiling: ProfilingConfig{
			Enabled:     getEnvBool("PROFILING_ENABLED", true),
			Endpoint:    getEnv("PYROSCOPE_ENDPOINT", "http://pyroscope.monitoring.svc.cluster.local:4040"),
			ServiceName: getEnv("SERVICE_NAME", defaultServiceName),
		},
		Logging: LoggingConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "json"),
		},
		Metrics: MetricsConfig{
			Enabled: getEnvBool("METRICS_ENABLED", true),
			Path:    getEnv("METRICS_PATH", "/metrics"),
		},
		Database: DatabaseConfig{
			Host:           getEnv("DB_HOST", ""),
			Port:           getEnv("DB_PORT", "5432"),
			Name:           getEnv("DB_NAME", ""),
			User:           getEnv("DB_USER", ""),
			Password:       getEnv("DB_PASSWORD", ""),
			SSLMode:        getEnv("DB_SSLMODE", "disable"),
			MaxConnections: getEnvInt("DB_POOL_MAX_CONNECTIONS", 25),
			PoolMode:       getEnv("DB_POOL_MODE", ""),
			PoolerType:     getEnv("DB_POOLER_TYPE", ""),
		},
		Payment: PaymentConfig{
			AuthHoldTTL:             getEnvDuration("AUTH_HOLD_TTL", 168*time.Hour), // 7 days
			IdempotencyKeyTTL:       getEnvDuration("IDEMPOTENCY_KEY_TTL", 24*time.Hour),
			IdempotencyLockTakeover: getEnvDuration("IDEMPOTENCY_LOCK_TAKEOVER", 90*time.Second),
			ProviderURL:             getEnv("MOCKPAY_URL", ""),
			WebhookSecret:           getEnv("MOCKPAY_WEBHOOK_SECRET", ""),
			WebhookURL:              getEnv("MOCKPAY_WEBHOOK_URL", ""),
		},
		ShutdownTimeout:     getEnvDurationSeconds("SHUTDOWN_TIMEOUT", 10),
		ReadinessDrainDelay: getEnvDurationSecondsWithMax("READINESS_DRAIN_DELAY", 5, 30),
		JWKSURL:             getEnv("AUTH_JWKS_URL", "http://auth.auth.svc.cluster.local:8080/auth/v1/public/jwks"),
		JWTIssuer:           getEnv("JWT_ISSUER", "https://gateway.duynh.me"),
		JWTAudience:         getEnv("JWT_AUDIENCE", "duynhlab-platform"),
	}
}

// Validate performs comprehensive validation of all configuration fields
// Returns detailed error messages for SRE/DevOps troubleshooting
func (c *Config) Validate() error {
	var errs []string

	errs = append(errs, c.validateService()...)
	errs = append(errs, c.validateTracing()...)
	errs = append(errs, c.validateProfiling()...)
	errs = append(errs, c.validateLogging()...)
	errs = append(errs, c.validateDatabase()...)
	errs = append(errs, c.validatePayment()...)

	if len(errs) > 0 {
		return fmt.Errorf("configuration validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}

	return nil
}

func (c *Config) validateService() []string {
	var errs []string
	if c.Service.Name == "" {
		errs = append(errs, "SERVICE_NAME is required (e.g., 'payment')")
	}
	if c.Service.Port == "" {
		errs = append(errs, "PORT is required (e.g., '8080')")
	}
	if _, err := strconv.Atoi(c.Service.Port); err != nil {
		errs = append(errs, "PORT must be a valid number, got: "+c.Service.Port)
	}
	validEnvs := []string{"development", "dev", "staging", "stage", "production", "prod"}
	if !contains(validEnvs, c.Service.Env) {
		errs = append(errs, fmt.Sprintf("ENV must be one of %v, got: %s", validEnvs, c.Service.Env))
	}
	return errs
}

func (c *Config) validateTracing() []string {
	if !c.Tracing.Enabled {
		return nil
	}
	var errs []string
	if c.Tracing.Endpoint == "" {
		errs = append(errs, "OTEL_COLLECTOR_ENDPOINT is required when tracing is enabled")
	}
	if c.Tracing.SampleRate < 0 || c.Tracing.SampleRate > 1.0 {
		errs = append(errs, fmt.Sprintf("OTEL_SAMPLE_RATE must be between 0.0 and 1.0, got: %.2f", c.Tracing.SampleRate))
	}
	if c.Tracing.ServiceName == "" {
		errs = append(errs, "SERVICE_NAME is required for tracing (used in Tempo queries)")
	}
	return errs
}

func (c *Config) validateProfiling() []string {
	if !c.Profiling.Enabled {
		return nil
	}
	var errs []string
	if c.Profiling.Endpoint == "" {
		errs = append(errs, "PYROSCOPE_ENDPOINT is required when profiling is enabled")
	}
	if c.Profiling.ServiceName == "" {
		errs = append(errs, "SERVICE_NAME is required for profiling (used in Pyroscope UI)")
	}
	return errs
}

func (c *Config) validateLogging() []string {
	var errs []string
	validLogLevels := []string{"debug", "info", "warn", "error"}
	if !contains(validLogLevels, strings.ToLower(c.Logging.Level)) {
		errs = append(errs, fmt.Sprintf("LOG_LEVEL must be one of %v, got: %s", validLogLevels, c.Logging.Level))
	}
	validLogFormats := []string{"json", "console"}
	if !contains(validLogFormats, strings.ToLower(c.Logging.Format)) {
		errs = append(errs, fmt.Sprintf("LOG_FORMAT must be one of %v, got: %s", validLogFormats, c.Logging.Format))
	}
	return errs
}

func (c *Config) validateDatabase() []string {
	if c.Database.Host == "" {
		return nil
	}
	var errs []string
	if c.Database.Name == "" {
		errs = append(errs, "DB_NAME is required when DB_HOST is set")
	}
	if c.Database.User == "" {
		errs = append(errs, "DB_USER is required when DB_HOST is set")
	}
	if c.Database.Password == "" {
		errs = append(errs, "DB_PASSWORD is required when DB_HOST is set")
	}
	if c.Database.Port != "" {
		if _, err := strconv.Atoi(c.Database.Port); err != nil {
			errs = append(errs, "DB_PORT must be a valid number, got: "+c.Database.Port)
		}
	}
	// A payment service must not ship data-in-transit in cleartext in prod.
	if c.IsProduction() && (c.Database.SSLMode == "" || c.Database.SSLMode == "disable") {
		errs = append(errs, "DB_SSLMODE must not be disabled in production")
	}
	return errs
}

func (c *Config) validatePayment() []string {
	var errs []string
	if c.Payment.AuthHoldTTL <= 0 {
		errs = append(errs, "AUTH_HOLD_TTL must be a positive duration (e.g., '168h')")
	}
	if c.Payment.IdempotencyKeyTTL <= 0 {
		errs = append(errs, "IDEMPOTENCY_KEY_TTL must be a positive duration (e.g., '24h')")
	}
	if c.Payment.IdempotencyLockTakeover <= 0 {
		errs = append(errs, "IDEMPOTENCY_LOCK_TAKEOVER must be a positive duration (e.g., '90s')")
	}
	// The public webhook route is always mounted and the HMAC secret is its only
	// credential — an empty secret is fail-open, so require it unconditionally.
	if c.Payment.WebhookSecret == "" {
		errs = append(errs, "MOCKPAY_WEBHOOK_SECRET is required (public webhook receiver credential)")
	}
	return errs
}

// IsDevelopment returns true if running in development environment
func (c *Config) IsDevelopment() bool {
	env := strings.ToLower(c.Service.Env)
	return env == "development" || env == "dev"
}

// IsProduction returns true if running in production environment
func (c *Config) IsProduction() bool {
	env := strings.ToLower(c.Service.Env)
	return env == "production" || env == "prod"
}

// Helper functions for environment variable parsing

// getEnv reads an environment variable with a default fallback
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvBool reads a boolean environment variable with a default fallback
// Accepts: "true", "1", "yes" for true | "false", "0", "no" for false
func getEnvBool(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	value = strings.ToLower(value)
	return value == "true" || value == "1" || value == "yes"
}

// getEnvInt reads an integer environment variable with a default fallback
// Returns default if parsing fails
func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return intValue
}

// getEnvFloat reads a float64 environment variable with a default fallback
// Returns default if parsing fails
func getEnvFloat(key string, defaultValue float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	floatValue, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return defaultValue
	}
	return floatValue
}

// getEnvDuration reads a duration environment variable with a default fallback.
// Accepts Go duration format (e.g., "90s", "24h", "168h").
// Returns default on invalid or non-positive values (silent fallback for startup
// safety; Validate() still rejects a non-positive default explicitly set in code).
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return defaultValue
	}
	return d
}

// getEnvDurationSeconds reads a duration environment variable and returns seconds as int
// Accepts Go duration format (e.g., "10s", "30s", "1m")
// Default: 10 seconds
// Max: 60 seconds (safety limit)
// Returns default on invalid values (silent fallback for startup safety)
func getEnvDurationSeconds(key string, defaultValueSeconds int) int {
	return getEnvDurationSecondsWithMax(key, defaultValueSeconds, 60)
}

// getEnvDurationSecondsWithMax reads a duration env var and returns seconds as int.
// Accepts Go duration format (e.g., "5s", "30s", "1m").
// Returns default on invalid values (silent fallback for startup safety).
func getEnvDurationSecondsWithMax(key string, defaultValueSeconds int, maxSeconds int) int {
	timeoutStr := os.Getenv(key)
	if timeoutStr == "" {
		return defaultValueSeconds
	}

	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return defaultValueSeconds
	}

	seconds := int(timeout.Seconds())
	if seconds <= 0 || seconds > maxSeconds {
		return defaultValueSeconds
	}

	return seconds
}

// GetShutdownTimeoutDuration returns shutdown timeout as time.Duration
// Convenience method for use in main.go
func (c *Config) GetShutdownTimeoutDuration() time.Duration {
	return time.Duration(c.ShutdownTimeout) * time.Second
}

// GetReadinessDrainDelayDuration returns readiness drain delay as time.Duration.
func (c *Config) GetReadinessDrainDelayDuration() time.Duration {
	return time.Duration(c.ReadinessDrainDelay) * time.Second
}

// contains checks if a string slice contains a specific value
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if strings.EqualFold(s, item) {
			return true
		}
	}
	return false
}
