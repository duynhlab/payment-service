package middleware

import (
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/trace"
)

var (
	// RED method: this single histogram provides Rate, Errors, and Duration.
	// _count = request rate, _count{code=~"5.."} = error rate, _bucket = latency percentiles.
	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "request_duration_seconds",
			Help: "Duration of HTTP requests in seconds",
			// SLO-tuned: extra buckets at 200ms, 300ms, 750ms for precision around the 500ms SLO threshold.
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 2, 5, 10},
		},
		[]string{"method", "path", "code"},
	)

	requestsInFlight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "requests_in_flight",
			Help: "Number of HTTP requests currently being processed",
		},
		[]string{"method", "path"},
	)

	requestSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "request_size_bytes",
			Help:    "Size of HTTP requests in bytes",
			Buckets: []float64{100, 1000, 10000, 100000, 1000000},
		},
		[]string{"method", "path", "code"},
	)

	responseSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "response_size_bytes",
			Help:    "Size of HTTP responses in bytes",
			Buckets: []float64{100, 1000, 10000, 100000, 1000000},
		},
		[]string{"method", "path", "code"},
	)
)

// shouldCollectMetrics determines if metrics should be collected for a given path.
// Infrastructure endpoints (health checks, metrics) are excluded to prevent
// high cardinality, skewed metrics, and storage waste.
func shouldCollectMetrics(path string) bool {
	infrastructurePaths := []string{
		"/health",
		"/ready",
		"/metrics",
		"/readiness",
		"/liveness",
	}

	for _, skipPath := range infrastructurePaths {
		if strings.HasPrefix(path, skipPath) {
			return false
		}
	}

	return true
}

func PrometheusMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		method := c.Request.Method

		if !shouldCollectMetrics(c.Request.URL.Path) {
			c.Next()
			return
		}

		// Resolve route pattern before processing for consistent labels across Inc/Dec.
		// Gin resolves the route before middleware runs, so c.FullPath() is available here.
		path := c.FullPath()
		if path == "" {
			path = "unknown"
		}

		requestsInFlight.WithLabelValues(method, path).Inc()

		c.Next()

		duration := time.Since(start).Seconds()
		statusCode := strconv.Itoa(c.Writer.Status())

		// Exemplar: attach traceID so Grafana can link a latency spike directly to a Tempo trace.
		span := trace.SpanFromContext(c.Request.Context())
		if span.SpanContext().HasTraceID() {
			requestDuration.WithLabelValues(method, path, statusCode).(prometheus.ExemplarObserver).ObserveWithExemplar(
				duration, prometheus.Labels{"traceID": span.SpanContext().TraceID().String()},
			)
		} else {
			requestDuration.WithLabelValues(method, path, statusCode).Observe(duration)
		}

		requestSize.WithLabelValues(method, path, statusCode).Observe(float64(c.Request.ContentLength))
		responseSize.WithLabelValues(method, path, statusCode).Observe(float64(c.Writer.Size()))

		requestsInFlight.WithLabelValues(method, path).Dec()
	}
}
