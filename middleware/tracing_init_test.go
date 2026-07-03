package middleware

import (
	"context"
	"testing"

	"github.com/duynhlab/payment-service/config"
)

// TestInitTracing_BuildsProviderWithParentBasedSampler exercises InitTracing's
// happy path — building the OTLP exporter + TracerProvider with the ParentBased
// sampler. The OTLP-HTTP exporter is lazy (no collector connection at startup),
// so this needs no running collector.
func TestInitTracing_BuildsProviderWithParentBasedSampler(t *testing.T) {
	cfg := &config.Config{}
	cfg.Tracing.Enabled = true
	cfg.Tracing.Endpoint = "localhost:4318"
	cfg.Tracing.SampleRate = 1.0
	cfg.Tracing.MaxExportBatchSize = 512
	cfg.Service.Name = "payment-test"

	tp, err := InitTracing(cfg)
	if err != nil {
		t.Fatalf("InitTracing returned an unexpected error: %v", err)
	}
	if tp == nil {
		t.Fatal("InitTracing returned a nil TracerProvider")
	}
	if err := tp.Shutdown(context.Background()); err != nil {
		t.Errorf("TracerProvider.Shutdown returned an error: %v", err)
	}
}

// TestInitTracing_DisabledReturnsError documents the gate: tracing off is an error.
func TestInitTracing_DisabledReturnsError(t *testing.T) {
	cfg := &config.Config{}
	cfg.Tracing.Enabled = false

	if _, err := InitTracing(cfg); err == nil {
		t.Fatal("expected an error when tracing is disabled, got nil")
	}
}

// TestInitTracing_WrapsProviderWhenProfilingEnabled covers the traces-to-profiles
// branch: with profiling enabled, the global tracer provider is wrapped via
// obsx.TracerProviderWithProfiles so spans carry pyroscope.profile.id.
func TestInitTracing_WrapsProviderWhenProfilingEnabled(t *testing.T) {
	cfg := &config.Config{}
	cfg.Tracing.Enabled = true
	cfg.Tracing.Endpoint = "localhost:4318"
	cfg.Tracing.SampleRate = 1.0
	cfg.Tracing.MaxExportBatchSize = 512
	cfg.Service.Name = "payment-test"
	cfg.Profiling.Enabled = true

	tp, err := InitTracing(cfg)
	if err != nil {
		t.Fatalf("InitTracing returned an unexpected error: %v", err)
	}
	if tp == nil {
		t.Fatal("InitTracing returned a nil TracerProvider")
	}
	if err := tp.Shutdown(context.Background()); err != nil {
		t.Errorf("TracerProvider.Shutdown returned an error: %v", err)
	}
}
