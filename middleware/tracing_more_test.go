package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func TestShouldTrace(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/api/work", true},
		{"/health", false},
		{"/healthz", false},
		{"/ready", false},
		{"/readyz", false},
		{"/livez", false},
		{"/metrics", false},
		{"/favicon.ico", false},
		{"/", true},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := shouldTrace(tt.path); got != tt.want {
				t.Errorf("shouldTrace(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestTracingMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(TracingMiddleware())
	r.GET("/api/work", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	r.GET("/health", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	for _, path := range []string{"/api/work", "/health"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200", path, w.Code)
		}
	}
}

func TestStartSpan(t *testing.T) {
	ctx, span := StartSpan(context.Background(), "unit.test")
	if span == nil {
		t.Fatal("StartSpan returned a nil span")
	}
	span.End()
	if ctx == nil {
		t.Error("StartSpan returned a nil context")
	}
}

// TestSpanHelpers exercises both branches of the recording guard in
// AddSpanAttributes/AddSpanEvent/RecordError/SetSpanStatus.
func TestSpanHelpers(t *testing.T) {
	t.Run("recording span", func(t *testing.T) {
		tp := sdktrace.NewTracerProvider()
		t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
		ctx, span := tp.Tracer("test").Start(context.Background(), "s")
		defer span.End()
		if !span.IsRecording() {
			t.Fatal("expected a recording span")
		}
		AddSpanAttributes(ctx, attribute.String("k", "v"))
		AddSpanEvent(ctx, "event", attribute.Int("n", 1))
		RecordError(ctx, errors.New("boom"))
		SetSpanStatus(ctx, codes.Ok, "fine")
	})

	t.Run("no span is a no-op", func(t *testing.T) {
		ctx := context.Background() // no span → SpanFromContext is non-recording
		AddSpanAttributes(ctx, attribute.String("k", "v"))
		AddSpanEvent(ctx, "event")
		RecordError(ctx, errors.New("boom"))
		SetSpanStatus(ctx, codes.Error, "nope")
	})
}

func TestShutdown(t *testing.T) {
	orig := tracerProvider
	t.Cleanup(func() { tracerProvider = orig })

	t.Run("nil provider returns nil", func(t *testing.T) {
		tracerProvider = nil
		if err := Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown(nil provider) = %v, want nil", err)
		}
	})

	t.Run("flushes and shuts down a provider", func(t *testing.T) {
		tracerProvider = sdktrace.NewTracerProvider()
		if err := Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown() = %v, want nil", err)
		}
	})
}

func TestDetectServiceInfo(t *testing.T) {
	t.Run("from OTEL_SERVICE_NAME + resource attributes namespace", func(t *testing.T) {
		t.Setenv("OTEL_SERVICE_NAME", "order")
		t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.namespace=prod,foo=bar")
		svc, ns := detectServiceInfo()
		if svc != "order" {
			t.Errorf("service = %q, want order", svc)
		}
		if ns != "prod" {
			t.Errorf("namespace = %q, want prod", ns)
		}
	})

	t.Run("from POD_NAME pattern + POD_NAMESPACE", func(t *testing.T) {
		t.Setenv("OTEL_SERVICE_NAME", "")
		t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
		t.Setenv("POD_NAME", "order-75c98b4b9c-kdv2n")
		t.Setenv("POD_NAMESPACE", "team-a")
		svc, ns := detectServiceInfo()
		if svc != "order" {
			t.Errorf("service = %q, want order (stripped hashes)", svc)
		}
		if ns != "team-a" {
			t.Errorf("namespace = %q, want team-a", ns)
		}
	})
}

func TestGetServiceName(t *testing.T) {
	t.Run("returns service.name attribute", func(t *testing.T) {
		res := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String("order"))
		if got := GetServiceName(res); got != "order" {
			t.Errorf("GetServiceName() = %q, want order", got)
		}
	})

	t.Run("falls back to unknown when absent", func(t *testing.T) {
		res := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceVersionKey.String("1.0.0"))
		if got := GetServiceName(res); got != unknownService {
			t.Errorf("GetServiceName() = %q, want %q", got, unknownService)
		}
	})
}
