package middleware

import (
	"sync"
	"testing"
)

// TestGetTracer_ConcurrentAccess exercises GetTracer's lazy initialization from
// many goroutines at once. Without the sync.Once guard this races on the
// package-level tracer var and is reported by `go test -race`.
func TestGetTracer_ConcurrentAccess(t *testing.T) {
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			if GetTracer() == nil {
				t.Error("GetTracer returned nil")
			}
		}()
	}
	wg.Wait()
}
