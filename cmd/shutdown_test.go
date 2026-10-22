package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

// shutdownTelemetry must hand the exporter a live context bounded by its own
// timeout, independent of whatever the HTTP drain consumed (homelab#1359).
func TestShutdownTelemetry_FreshBoundedContext(t *testing.T) {
	t.Run("live context with own deadline", func(t *testing.T) {
		const timeout = 5 * time.Second
		var (
			called      bool
			entryErr    error
			hasDeadline bool
			remaining   time.Duration
		)
		// Inspect inside the fake: the helper cancels its context on return.
		err := shutdownTelemetry(func(ctx context.Context) error {
			called = true
			entryErr = ctx.Err()
			var deadline time.Time
			deadline, hasDeadline = ctx.Deadline()
			remaining = time.Until(deadline)
			return nil
		}, timeout)
		if err != nil {
			t.Fatalf("shutdownTelemetry returned %v, want nil", err)
		}
		if !called {
			t.Fatal("shutdown func was not called")
		}
		if entryErr != nil {
			t.Fatalf("context already done on entry: %v", entryErr)
		}
		if !hasDeadline {
			t.Fatal("context has no deadline")
		}
		if remaining <= 0 || remaining > timeout {
			t.Fatalf("deadline %v from now, want within (0, %v]", remaining, timeout)
		}
	})

	t.Run("exporter error is returned", func(t *testing.T) {
		want := errors.New("exporter down")
		err := shutdownTelemetry(func(context.Context) error { return want }, time.Second)
		if !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
	})
}
