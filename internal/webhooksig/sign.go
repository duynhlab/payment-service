// Package webhooksig signs and verifies mockpay webhook payloads with an HMAC
// scheme modelled on Stripe's: a header
//
//	Mockpay-Signature: t=<unix-seconds>,v1=<hex HMAC-SHA256(secret, "<t>.<raw-body>")>
//
// The timestamp is inside the signed material, so an attacker cannot replay a
// captured request outside the tolerance window without invalidating v1. Both
// the signer (mockpay) and the verifier (payment) share this package so the
// wire format can never drift.
package webhooksig

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Verification failures. Signature/timestamp failures must map to a non-2xx so
// the sender retries; a stale timestamp is a distinct (also non-2xx) case.
var (
	ErrMalformed = errors.New("malformed signature header")
	ErrStale     = errors.New("signature timestamp outside tolerance")
	ErrSignature = errors.New("signature mismatch")
)

// signedPayload is the exact bytes the HMAC covers: "<unix>.<raw body>".
func signedPayload(ts string, body []byte) []byte {
	buf := make([]byte, 0, len(ts)+1+len(body))
	buf = append(buf, ts...)
	buf = append(buf, '.')
	buf = append(buf, body...)
	return buf
}

func computeMAC(secret, ts string, body []byte) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(signedPayload(ts, body))
	return mac.Sum(nil)
}

// Sign returns the header value for the given body at time t.
func Sign(secret string, t time.Time, body []byte) string {
	ts := strconv.FormatInt(t.Unix(), 10)
	return fmt.Sprintf("t=%s,v1=%s", ts, hex.EncodeToString(computeMAC(secret, ts, body)))
}

// Verify checks header against body: well-formed, within ±tolerance of now, and
// a matching HMAC (constant-time). Returns a sentinel error on failure.
//
// An empty secret is rejected outright: HMAC with a zero key is publicly
// computable, so accepting it would turn the endpoint into accept-anything. This
// is a fail-closed backstop; callers must also require the secret at startup.
func Verify(secret, header string, body []byte, now time.Time, tolerance time.Duration) error {
	if secret == "" {
		return ErrSignature
	}
	ts, sig, err := parse(header)
	if err != nil {
		return err
	}
	t, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return ErrMalformed
	}
	if d := now.Sub(time.Unix(t, 0)); d > tolerance || d < -tolerance {
		return ErrStale
	}
	provided, err := hex.DecodeString(sig)
	if err != nil {
		return ErrMalformed
	}
	if !hmac.Equal(provided, computeMAC(secret, ts, body)) {
		return ErrSignature
	}
	return nil
}

// parse pulls t and v1 out of "t=..,v1=..". Order-independent; missing either
// part is malformed.
func parse(header string) (ts, sig string, err error) {
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch k {
		case "t":
			ts = v
		case "v1":
			sig = v
		}
	}
	if ts == "" || sig == "" {
		return "", "", ErrMalformed
	}
	return ts, sig, nil
}
