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
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

// TestSetServiceName covers both branches: a non-empty name is recorded for
// the tracer scope, and an empty name must NOT clobber a previously set one
// (main() may pass an unset config value).
func TestSetServiceName(t *testing.T) {
	orig := detectedService
	t.Cleanup(func() { detectedService = orig })

	SetServiceName("payment")
	if detectedService != "payment" {
		t.Errorf("detectedService = %q, want payment", detectedService)
	}
	SetServiceName("")
	if detectedService != "payment" {
		t.Error("SetServiceName(\"\") must not clobber the recorded name")
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
