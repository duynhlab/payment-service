package middleware

import (
	"context"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	tracer          trace.Tracer
	tracerOnce      sync.Once
	detectedService string
)

// defaultServiceNameFallback names the tracer scope when SetServiceName was
// never called (previously lived in the deleted resource.go).
const defaultServiceNameFallback = "unknown-service"

// SetServiceName records the service name used by TracingMiddleware and
// GetTracer for the tracer scope. The OTel SDK itself (providers, exporters,
// resource, sampler) is wired once in main() by obsx.SetupObservability
// (RFC-0014) — this package only consumes the globals it installs.
func SetServiceName(name string) {
	if name != "" {
		detectedService = name
	}
}

// shouldTrace determines if a request should be traced based on path
// Skips health checks, metrics endpoints, and static resources
func shouldTrace(path string) bool {
	skipPaths := []string{
		"/health", "/healthz", "/ready", "/readyz", "/livez",
		"/metrics", "/favicon.ico",
	}
	for _, skip := range skipPaths {
		if strings.HasPrefix(path, skip) {
			return false
		}
	}
	return true
}

// TracingMiddleware returns a Gin middleware for OpenTelemetry tracing
// Service name is automatically detected from Kubernetes metadata
//
// Usage:
//
//	r := gin.Default()
//	r.Use(middleware.TracingMiddleware())
func TracingMiddleware() gin.HandlerFunc {
	serviceName := detectedService
	if serviceName == "" {
		serviceName = defaultServiceNameFallback
	}

	// Wrap otelgin middleware with request filtering
	otelMiddleware := otelgin.Middleware(
		serviceName,
		otelgin.WithTracerProvider(otel.GetTracerProvider()),
	)

	return func(c *gin.Context) {
		// Skip tracing for health checks and metrics endpoints
		if !shouldTrace(c.Request.URL.Path) {
			c.Next()
			return
		}

		// Apply OpenTelemetry middleware
		otelMiddleware(c)
	}
}

// GetTracer returns the tracer instance with auto-detected service name
func GetTracer() trace.Tracer {
	tracerOnce.Do(func() {
		serviceName := detectedService
		if serviceName == "" {
			serviceName = defaultServiceNameFallback
		}
		tracer = otel.Tracer(serviceName)
	})
	return tracer
}

// StartSpan starts a new span with the given name
//
// Usage:
//
//	ctx, span := middleware.StartSpan(ctx, "database.query")
//	defer span.End()
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	//nolint:spancheck // span is returned to caller who is responsible for calling span.End()
	return GetTracer().Start(ctx, name, opts...)
}

// Helper Functions

// AddSpanAttributes adds attributes to the current span if it's recording
//
// Usage:
//
//	middleware.AddSpanAttributes(ctx,
//	    attribute.String("user.id", userID),
//	    attribute.Int("order.items", len(items)),
//	)
func AddSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(attrs...)
	}
}

// AddSpanEvent adds an event to the current span if it's recording
//
// Usage:
//
//	middleware.AddSpanEvent(ctx, "cache.hit",
//	    attribute.String("cache.key", key),
//	)
func AddSpanEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.AddEvent(name, trace.WithAttributes(attrs...))
	}
}

// RecordError records an error in the current span if it's recording
//
// Usage:
//
//	if err != nil {
//	    middleware.RecordError(ctx, err)
//	    return err
//	}
func RecordError(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// SetSpanStatus sets the status of the current span if it's recording
//
// Usage:
//
//	middleware.SetSpanStatus(ctx, codes.Ok, "request successful")
func SetSpanStatus(ctx context.Context, code codes.Code, description string) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetStatus(code, description)
	}
}
