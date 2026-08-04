package domain

import (
	"testing"
	"time"
)

// fixedNow keeps the window cases readable: the shapes matter, not the instant.
var fixedNow = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

// A window that cannot contain anything is not a pass worth making, and the
// domain type says so rather than each caller re-deriving the rule.
func TestReconWindow_EmptyAndBounded(t *testing.T) {
	cases := map[string]struct {
		w       ReconWindow
		empty   bool
		bounded bool
	}{
		"unbounded":       {ReconWindow{}, false, false},
		"open below":      {ReconWindow{Through: fixedNow}, false, true},
		"proper":          {ReconWindow{From: fixedNow.Add(-time.Hour), Through: fixedNow}, false, true},
		"inverted":        {ReconWindow{From: fixedNow, Through: fixedNow.Add(-time.Hour)}, true, true},
		"zero width":      {ReconWindow{From: fixedNow, Through: fixedNow}, true, true},
		"open above only": {ReconWindow{From: fixedNow}, false, true},
	}
	for name, tc := range cases {
		if got := tc.w.Empty(); got != tc.empty {
			t.Errorf("%s: Empty() = %v, want %v", name, got, tc.empty)
		}
		if got := tc.w.Bounded(); got != tc.bounded {
			t.Errorf("%s: Bounded() = %v, want %v", name, got, tc.bounded)
		}
	}
}
