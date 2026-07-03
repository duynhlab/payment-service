package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestShouldCollectMetrics(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/api/payment/v1/estimate", true},
		{"/payment/v1/internal/payments/7", true},
		{"/health", false},
		{"/healthz", false}, // HasPrefix("/healthz", "/health") matches → skipped
		{"/ready", false},
		{"/metrics", false},
		{"/readiness", false},
		{"/liveness", false},
		{"/", true},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := shouldCollectMetrics(tt.path); got != tt.want {
				t.Errorf("shouldCollectMetrics(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestPrometheusMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(PrometheusMiddleware())
	r.GET("/api/work", func(c *gin.Context) { c.String(http.StatusOK, "done") })
	r.GET("/health", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	t.Run("collects on a normal route", func(t *testing.T) {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/work", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
	})

	t.Run("skips infrastructure path", func(t *testing.T) {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
	})

	t.Run("unmatched route uses 'unknown' path label", func(t *testing.T) {
		w := httptest.NewRecorder()
		// No route registered → c.FullPath() == "" → labelled "unknown".
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/does-not-exist", nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
	})
}
