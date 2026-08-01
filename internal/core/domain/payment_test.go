package domain

import (
	"errors"
	"testing"
	"time"
)

// TestTransitionWhitelist enumerates EVERY (from, to) pair so an accidental
// edit to the whitelist fails loudly. The forbidden set is the complement of
// the allowed set — both directions are asserted.
func TestTransitionWhitelist(t *testing.T) {
	all := []Status{StatusPending, StatusProcessing, StatusAuthorized, StatusCaptured, StatusFailed, StatusVoided, StatusExpired, StatusRefunded}

	allowed := map[Status]map[Status]bool{
		StatusPending:    {StatusAuthorized: true, StatusFailed: true, StatusProcessing: true},
		StatusAuthorized: {StatusCaptured: true, StatusVoided: true, StatusExpired: true, StatusProcessing: true},
		StatusCaptured:   {StatusRefunded: true, StatusProcessing: true},
		StatusVoided:     {StatusProcessing: true},
		// Resolution can land on any definite state the provider turns out to
		// have produced — including the post-operation ones, because the doubt
		// may be about an operation that DID land.
		StatusProcessing: {StatusAuthorized: true, StatusFailed: true, StatusCaptured: true,
			StatusVoided: true, StatusRefunded: true},
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

// TestProcessing_NeverResolvesToItself: `processing` is doubt, and doubt is not
// a destination. A path into it must come from a real operation, and a path out
// must name a definite state — otherwise an intent could be parked in doubt
// forever by a bug that keeps "resolving" it to the same place.
func TestProcessing_NeverResolvesToItself(t *testing.T) {
	if err := Transition(StatusProcessing, StatusProcessing); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("processing -> processing must be forbidden, got %v", err)
	}
	for _, terminal := range []Status{StatusFailed, StatusExpired, StatusRefunded} {
		if err := Transition(terminal, StatusProcessing); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("%s -> processing must be forbidden (terminal), got %v", terminal, err)
		}
	}
}

// TestOutcomeClass_DecidedSplitsOnDoubtNotOnFailure pins the distinction the
// whole phase rests on: a decline and a retryable failure are both ANSWERS, and
// only UNKNOWN is open. Code that asks "was it a decline" instead of "was it
// decided" is what produced the money bugs this phase fixes.
func TestOutcomeClass_DecidedSplitsOnDoubtNotOnFailure(t *testing.T) {
	decided := map[OutcomeClass]bool{
		OutcomeSuccess:          true,
		OutcomeBusinessDecline:  true,
		OutcomeRetryableFailure: true,
		OutcomeUnknown:          false,
	}
	for class, want := range decided {
		if got := class.Decided(); got != want {
			t.Errorf("%s.Decided() = %v, want %v", class, got, want)
		}
	}
}

// TestAttempt_OpenIsUnknownAndUnresolved: the open-doubt worklist is exactly
// UNKNOWN with no resolution. A resolved UNKNOWN is history, not work.
func TestAttempt_OpenIsUnknownAndUnresolved(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		a    Attempt
		want bool
	}{
		{"unresolved unknown", Attempt{Outcome: OutcomeUnknown}, true},
		{"resolved unknown", Attempt{Outcome: OutcomeUnknown, ResolvedAt: &now}, false},
		{"success", Attempt{Outcome: OutcomeSuccess}, false},
		{"decline", Attempt{Outcome: OutcomeBusinessDecline}, false},
	}
	for _, tc := range cases {
		if got := tc.a.Open(); got != tc.want {
			t.Errorf("%s: Open() = %v, want %v", tc.name, got, tc.want)
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
