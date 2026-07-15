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

		// Prefer the active span's trace ID so logs carry the same trace ID
		// exported to the tracing backend; fall back to header/generated.
		traceID := obsx.TraceIDFromContext(c.Request.Context())
		if traceID == "" {
			traceID = GetTraceID(c)
		}

		// Store trace-id in context for handlers to use
		c.Set("trace_id", traceID)

		// Store logger in context for handlers to use. TraceContext binds the
		// request context so the otelzap bridge also stamps the native
		// trace_id/span_id on every OTLP log record (the string field stays for
		// stdout readability). Request-scoped logger, so binding ctx is safe.
		loggerWithTrace := logger.With(zap.String("trace_id", traceID), obsx.TraceContext(c.Request.Context()))
		c.Set("logger", loggerWithTrace)

		// Add trace-id to response header
		c.Header(TraceIDHeader, traceID)

		// Process request
		c.Next()

		// Calculate duration
		duration := time.Since(start)
		statusCode := c.Writer.Status()

		// Single log line per request; errors (4xx/5xx) are logged at error level.
		fields := []zap.Field{
			zap.String("trace_id", traceID),
			zap.String("method", method),
			zap.String("path", path),
			zap.Int("status", statusCode),
			zap.Duration("duration", duration),
			zap.String("client_ip", c.ClientIP()),
			zap.String("user_agent", c.Request.UserAgent()),
		}
		if statusCode >= 400 {
			logger.Error("HTTP request", fields...)
		} else {
			logger.Info("HTTP request", fields...)
		}
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
