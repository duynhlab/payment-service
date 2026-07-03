package domain

import (
	"errors"
	"testing"
)

// TestTransitionWhitelist enumerates EVERY (from, to) pair so an accidental
// edit to the whitelist fails loudly. The forbidden set is the complement of
// the allowed set — both directions are asserted.
func TestTransitionWhitelist(t *testing.T) {
	all := []Status{StatusPending, StatusAuthorized, StatusCaptured, StatusFailed, StatusVoided, StatusExpired, StatusRefunded}

	allowed := map[Status]map[Status]bool{
		StatusPending:    {StatusAuthorized: true, StatusFailed: true},
		StatusAuthorized: {StatusCaptured: true, StatusVoided: true, StatusExpired: true},
		StatusCaptured:   {StatusRefunded: true},
	}

	for _, from := range all {
		for _, to := range all {
			want := allowed[from][to]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
			err := Transition(from, to)
			if want && err != nil {
				t.Errorf("Transition(%s, %s) unexpected error: %v", from, to, err)
			}
			if !want {
				if !errors.Is(err, ErrInvalidTransition) {
					t.Errorf("Transition(%s, %s) = %v, want ErrInvalidTransition", from, to, err)
				}
			}
		}
	}
}

// TestTransition_ForbiddenHeadlines pins the two moves the RFC calls out by
// name: captured-from-pending and refunded-from-pending must be impossible.
func TestTransition_ForbiddenHeadlines(t *testing.T) {
	for _, to := range []Status{StatusCaptured, StatusRefunded} {
		if err := Transition(StatusPending, to); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("pending -> %s must be forbidden, got %v", to, err)
		}
	}
}

func TestPartiallyRefunded(t *testing.T) {
	tests := []struct {
		name   string
		status Status
		amount int64
		ref    int64
		want   bool
	}{
		{"no refunds", StatusCaptured, 2000, 0, false},
		{"partial", StatusCaptured, 2000, 500, true},
		{"almost full", StatusCaptured, 2000, 1999, true},
		{"fully refunded status flipped", StatusRefunded, 2000, 2000, false},
		{"authorized never partial", StatusAuthorized, 2000, 500, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Payment{Status: tt.status, AmountMinor: tt.amount, RefundedMinor: tt.ref}
			if got := p.PartiallyRefunded(); got != tt.want {
				t.Errorf("PartiallyRefunded() = %v, want %v", got, tt.want)
			}
		})
	}
}
