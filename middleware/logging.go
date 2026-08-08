package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/duynhlab/pkg/logger/zapx"
	"github.com/duynhlab/pkg/obsx"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const TraceIDHeader = "X-Trace-ID"
const TraceParentHeader = "traceparent"

// GetTraceID extracts trace-id from request headers or generates a new one
func GetTraceID(c *gin.Context) string {
	// Try W3C Trace Context first (traceparent header)
	if traceParent := c.GetHeader(TraceParentHeader); traceParent != "" {
		// traceparent format: version-trace_id-parent_id-flags
		// Extract trace_id (second part)
		parts := splitTraceParent(traceParent)
		if len(parts) >= 2 && parts[1] != "" {
			return parts[1]
		}
	}

	// Fallback to X-Trace-ID header
	if traceID := c.GetHeader(TraceIDHeader); traceID != "" {
		return traceID
	}

	// Generate new trace-id if not present
	return generateTraceID()
}

// splitTraceParent splits traceparent header value
func splitTraceParent(traceParent string) []string {
	// Simple split by hyphen, traceparent format: 00-<trace_id>-<parent_id>-<flags>
	parts := make([]string, 0, 4)
	start := 0
	for i := range len(traceParent) {
		if traceParent[i] == '-' {
			if start < i {
				parts = append(parts, traceParent[start:i])
			}
			start = i + 1
		}
	}
	if start < len(traceParent) {
		parts = append(parts, traceParent[start:])
	}
	return parts
}

// generateTraceID generates a trace-id using random bytes
func generateTraceID() string {
	// Generate 16 random bytes (32 hex characters)
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(b)
}

// LoggingMiddleware creates a Gin middleware for structured logging with trace-id
func LoggingMiddleware(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		// spanTraceID is the ONLY id that may reach telemetry: the active span's,
		// or empty. Deliberately not GetTraceID's value — that falls back to
		// generating one, and a fabricated id looks joinable while joining to
		// nothing, which is worse mid-incident than an absent field. Probes have
		// no span by design (TracingMiddleware skips them).
		spanTraceID := obsx.TraceIDFromContext(c.Request.Context())

		// The response header keeps its previous behaviour, generated fallback
		// included: correlate-by-header is a client contract, separate from what
		// this service puts in its own telemetry.
		headerTraceID := spanTraceID
		if headerTraceID == "" {
			headerTraceID = GetTraceID(c)
		}

		// Store trace-id in context for handlers to use
		c.Set("trace_id", headerTraceID)

		// Store logger in context for handlers to use. TraceContext binds the
		// request context so the otelzap bridge also stamps the native
		// trace_id/span_id on every OTLP log record (the string field stays for
		// stdout readability). Request-scoped logger, so binding ctx is safe.
		// The request logger always carries the trace CONTEXT so the otelzap
		// bridge stamps the native trace_id/span_id on every OTLP record. The
		// readable string field is bound only when a span exists, so no log
		// line claims a trace id the backend does not have.
		withFields := []zap.Field{obsx.TraceContext(c.Request.Context())}
		if spanTraceID != "" {
			withFields = append(withFields, zap.String("trace_id", spanTraceID))
		}
		loggerWithTrace := logger.With(withFields...)
		c.Set("logger", loggerWithTrace)

		// Add trace-id to response header
		c.Header(TraceIDHeader, headerTraceID)

		// Process request
		c.Next()

		// Calculate duration
		duration := time.Since(start)
		statusCode := c.Writer.Status()

		// Routine successful probes are traffic about the platform, not the
		// domain. TracingMiddleware already excludes them from spans and RED
		// metrics through the same shouldTrace list; excluding them here is
		// what makes that contract true for logs too. A FAILING probe is kept.
		if !shouldTrace(path) && statusCode < 400 {
			return
		}

		// 4xx is a rejected request, not a broken service: an expected business
		// rejection must not read as an infrastructure error.
		level := zapcore.InfoLevel
		switch {
		case statusCode >= 500:
			level = zapcore.ErrorLevel
		case statusCode >= 400:
			level = zapcore.WarnLevel
		}

		// loggerWithTrace, not logger: the base logger carries no trace context,
		// which left the access log uncorrelated in 9 of the 10 HTTP services
		// (telemetry audit F-1). trace_id is already bound on it.
		loggerWithTrace.Log(level, "HTTP request",
			zap.String("method", method),
			zap.String("path", path),
			zap.Int("status", statusCode),
			zap.Duration("duration", duration),
			zap.String("client_ip", c.ClientIP()),
			zap.String("user_agent", c.Request.UserAgent()),
		)
	}
}

// GetLoggerFromContext retrieves logger with trace-id from Gin context
func GetLoggerFromContext(c *gin.Context, baseLogger *zap.Logger) *zap.Logger {
	traceID, exists := c.Get("trace_id")
	if !exists {
		return baseLogger
	}
	return baseLogger.With(zap.String("trace_id", traceID.(string)))
}

// GetLoggerFromGinContext retrieves logger from Gin context (set by LoggingMiddleware)
func GetLoggerFromGinContext(c *gin.Context) *zap.Logger {
	loggerVal, exists := c.Get("logger")
	if exists {
		if l, ok := loggerVal.(*zap.Logger); ok {
			return l
		}
	}
	// Fallback: create a basic logger
	logger, _ := NewLogger()
	return logger
}

// NewLogger creates a new zap logger with JSON encoder for production
func NewLogger() (*zap.Logger, error) {
	return zapx.New("")
}

// NewDevelopmentLogger creates a new zap logger for development (console encoder)
func NewDevelopmentLogger() (*zap.Logger, error) {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	return config.Build()
}
