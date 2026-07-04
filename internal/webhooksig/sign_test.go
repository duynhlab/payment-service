package webhooksig

import (
	"errors"
	"testing"
	"time"
)

func TestSignVerify_RoundTrip(t *testing.T) {
	secret, body := "shh", []byte(`{"event_id":"evt_1","type":"charge.captured"}`)
	now := time.Unix(1_700_000_000, 0)
	header := Sign(secret, now, body)
	if err := Verify(secret, header, body, now, 5*time.Minute); err != nil {
		t.Fatalf("valid signature must verify: %v", err)
	}
}

func TestVerify_Failures(t *testing.T) {
	secret := "shh"
	body := []byte(`{"event_id":"evt_1"}`)
	now := time.Unix(1_700_000_000, 0)
	good := Sign(secret, now, body)

	tests := []struct {
		name    string
		header  string
		body    []byte
		now     time.Time
		wantErr error
	}{
		{"tampered body", good, []byte(`{"event_id":"evt_2"}`), now, ErrSignature},
		{"wrong secret", Sign("other", now, body), body, now, ErrSignature},
		{"stale (too old)", good, body, now.Add(6 * time.Minute), ErrStale},
		{"future beyond tolerance", good, body, now.Add(-6 * time.Minute), ErrStale},
		{"malformed - no v1", "t=1700000000", body, now, ErrMalformed},
		{"malformed - empty", "", body, now, ErrMalformed},
		{"malformed - non-numeric t", "t=abc,v1=deadbeef", body, now, ErrMalformed},
		{"malformed - bad hex", "t=1700000000,v1=zz", body, now, ErrMalformed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Verify(secret, tt.header, tt.body, tt.now, 5*time.Minute); !errors.Is(err, tt.wantErr) {
				t.Fatalf("want %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestVerify_EmptySecretRejected(t *testing.T) {
	body := []byte(`{"event_id":"e"}`)
	now := time.Unix(1_700_000_000, 0)
	// A signature computed with the empty key must NOT be accepted.
	header := Sign("", now, body)
	if err := Verify("", header, body, now, 5*time.Minute); !errors.Is(err, ErrSignature) {
		t.Fatalf("empty secret must fail closed, got %v", err)
	}
}

func TestVerify_WithinToleranceBoundary(t *testing.T) {
	secret, body := "shh", []byte("x")
	now := time.Unix(1_700_000_000, 0)
	header := Sign(secret, now, body)
	// Exactly at the tolerance edge still verifies.
	if err := Verify(secret, header, body, now.Add(5*time.Minute), 5*time.Minute); err != nil {
		t.Fatalf("at boundary must verify: %v", err)
	}
}
