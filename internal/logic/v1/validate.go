package v1

import "strings"

// MaxAmountMinor caps a single payment at $100M (in minor units) — a sanity
// ceiling that keeps absurd values out of the ledger, metrics, and the bigint
// refund arithmetic. Reject rather than clamp. Lives in the logic layer so
// every transport (HTTP + gRPC) shares one definition and the invariant can
// never drift between them.
const MaxAmountMinor int64 = 100_000_000_00 // $100M ceiling per payment (minor units)

// IsCurrency reports whether s is exactly three uppercase ASCII letters.
func IsCurrency(s string) bool {
	if len(s) != 3 {
		return false
	}
	for _, r := range s {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

// IsTestToken enforces PCI discipline on payment_method: a short opaque token
// (`tok_` + [A-Za-z0-9_], ≤ 64 chars) with no card-number-like digit run.
// Real payment tokens are short IDs; a pasted PAN must never be accepted,
// stored, or echoed.
func IsTestToken(s string) bool {
	if !strings.HasPrefix(s, "tok_") || len(s) > 64 {
		return false
	}
	// Count TOTAL digits, not the longest contiguous run: separators like `_`
	// must not let a grouped PAN ("tok_4111_1111_1111_1111") slip through.
	// Real opaque tokens are short alnum IDs, not digit-dense.
	digits := 0
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			digits++
			if digits >= 12 { // a card number is 13–19 digits
				return false
			}
		case (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_':
			// allowed
		default:
			return false
		}
	}
	return true
}
