package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestSplitTraceParent(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"w3c traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			[]string{"00", "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", "01"}},
		{"empty", "", nil},
		{"single", "abc", []string{"abc"}},
		{"trailing hyphen", "a-b-", []string{"a", "b"}},
		{"leading hyphen", "-a-b", []string{"a", "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitTraceParent(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("splitTraceParent(%q) = %v, want %v", tt.in, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("part[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestGenerateTraceID(t *testing.T) {
	id := generateTraceID()
	if len(id) != 32 {
		t.Fatalf("generateTraceID() length = %d, want 32", len(id))
	}
	for _, c := range id {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Fatalf("generateTraceID() = %q contains non-hex char %q", id, c)
		}
	}
	if generateTraceID() == id {
		t.Error("generateTraceID() returned identical IDs on consecutive calls")
	}
}

func TestGetTraceID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name    string
		headers map[string]string
		want    string // "" means "expect a generated 32-char id"
	}{
		{"from traceparent", map[string]string{TraceParentHeader: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"}, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{"from x-trace-id", map[string]string{TraceIDHeader: "my-trace-123"}, "my-trace-123"},
		{"traceparent malformed falls back to x-trace-id", map[string]string{TraceParentHeader: "garbage", TraceIDHeader: "fallback-id"}, "fallback-id"},
		{"none -> generated", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			c.Request = req

			got := GetTraceID(c)
			if tt.want == "" {
				if len(got) != 32 {
					t.Fatalf("GetTraceID() = %q, want a generated 32-char id", got)
				}
				return
			}
			if got != tt.want {
				t.Errorf("GetTraceID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLoggingMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("sets trace id header + context and logs", func(t *testing.T) {
		r := gin.New()
		r.Use(LoggingMiddleware(zap.NewNop()))
		r.GET("/ok", func(c *gin.Context) {
			if _, ok := c.Get("trace_id"); !ok {
				t.Error("trace_id not set in context")
			}
			if _, ok := c.Get("logger"); !ok {
				t.Error("logger not set in context")
			}
			c.String(http.StatusOK, "ok")
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/ok", nil)
		req.Header.Set(TraceIDHeader, "trace-from-client")
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", w.Code)
		}
		if got := w.Header().Get(TraceIDHeader); got != "trace-from-client" {
			t.Errorf("response %s = %q, want trace-from-client", TraceIDHeader, got)
		}
	})

	t.Run("logs error branch on 4xx/5xx", func(t *testing.T) {
		r := gin.New()
		r.Use(LoggingMiddleware(zap.NewNop()))
		r.GET("/boom", func(c *gin.Context) { c.String(http.StatusInternalServerError, "boom") })

		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/boom", nil))
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", w.Code)
		}
	})
}

func TestGetLoggerFromContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	base := zap.NewNop()

	t.Run("no trace id returns base logger", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		if GetLoggerFromContext(c, base) != base {
			t.Error("expected base logger when trace_id absent")
		}
	})

	t.Run("with trace id returns derived logger", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Set("trace_id", "abc")
		if GetLoggerFromContext(c, base) == nil {
			t.Error("expected a non-nil derived logger")
		}
	})
}

func TestGetLoggerFromGinContext(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("returns logger set in context", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		want := zap.NewNop()
		c.Set("logger", want)
		if GetLoggerFromGinContext(c) != want {
			t.Error("expected the logger stored in context")
		}
	})

	t.Run("falls back to a new logger when absent", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		if GetLoggerFromGinContext(c) == nil {
			t.Error("expected a fallback logger, got nil")
		}
	})
}

func TestNewLoggers(t *testing.T) {
	if l, err := NewLogger(); err != nil || l == nil {
		t.Errorf("NewLogger() = (%v, %v), want non-nil logger and nil error", l, err)
	}
	if l, err := NewDevelopmentLogger(); err != nil || l == nil {
		t.Errorf("NewDevelopmentLogger() = (%v, %v), want non-nil logger and nil error", l, err)
	}
}

// observedLogger returns a logger whose records land in the returned sink.
func observedLogger() (*zap.Logger, *observer.ObservedLogs) {
	core, logs := observer.New(zapcore.DebugLevel)
	return zap.New(core), logs
}

// The access log must skip routine SUCCESSFUL probes and keep failing ones —
// docs/api/observability.md claims this middleware shares TracingMiddleware's
// skip list, and telemetry audit F-2 found it had none.
func TestLoggingMiddlewareSkipsSuccessfulProbesOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for _, tc := range []struct {
		name       string
		path       string
		status     int
		wantRecord bool
	}{
		{"healthy probe is silent", "/health", http.StatusOK, false},
		{"ready probe is silent", "/readyz", http.StatusOK, false},
		{"metrics scrape is silent", "/metrics", http.StatusOK, false},
		{"FAILING probe is logged", "/health", http.StatusServiceUnavailable, true},
		{"real traffic is logged", "/v1/public/things", http.StatusOK, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, logs := observedLogger()
			r := gin.New()
			r.Use(LoggingMiddleware(logger))
			r.GET(tc.path, func(c *gin.Context) { c.String(tc.status, "x") })

			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))

			got := logs.FilterMessage("HTTP request").Len()
			if tc.wantRecord && got != 1 {
				t.Errorf("%s %d: got %d access-log records, want 1", tc.path, tc.status, got)
			}
			if !tc.wantRecord && got != 0 {
				t.Errorf("%s %d: got %d access-log records, want 0", tc.path, tc.status, got)
			}
		})
	}
}

// A rejected request is not a broken service: observability.md's error-ownership
// rule says expected business rejections must not read as infrastructure errors.
func TestLoggingMiddlewareLevelByStatusClass(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for _, tc := range []struct {
		status int
		want   zapcore.Level
	}{
		{http.StatusOK, zapcore.InfoLevel},
		{http.StatusNotFound, zapcore.WarnLevel},
		{http.StatusConflict, zapcore.WarnLevel},
		{http.StatusInternalServerError, zapcore.ErrorLevel},
	} {
		logger, logs := observedLogger()
		r := gin.New()
		r.Use(LoggingMiddleware(logger))
		r.GET("/x", func(c *gin.Context) { c.String(tc.status, "x") })

		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

		rec := logs.FilterMessage("HTTP request").All()
		if len(rec) != 1 {
			t.Fatalf("status %d: got %d records, want 1", tc.status, len(rec))
		}
		if rec[0].Level != tc.want {
			t.Errorf("status %d: level = %s, want %s", tc.status, rec[0].Level, tc.want)
		}
	}
}

// Without an active span there is no trace to join, so the record must carry no
// trace_id at all rather than a generated one (telemetry audit F-1).
func TestLoggingMiddlewareOmitsTraceIDWithoutSpan(t *testing.T) {
	gin.SetMode(gin.TestMode)
	logger, logs := observedLogger()
	r := gin.New()
	r.Use(LoggingMiddleware(logger))
	r.GET("/x", func(c *gin.Context) { c.String(http.StatusOK, "x") })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

	rec := logs.FilterMessage("HTTP request").All()
	if len(rec) != 1 {
		t.Fatalf("got %d records, want 1", len(rec))
	}
	for _, f := range rec[0].Context {
		if f.Key == "trace_id" {
			t.Errorf("no span, yet the record carries trace_id=%q — a fabricated id joins to nothing", f.String)
		}
	}
	if w.Header().Get(TraceIDHeader) == "" {
		t.Errorf("missing %s response header", TraceIDHeader)
	}
}
