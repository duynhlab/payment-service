package v1

import (
	"strings"
	"testing"
)

func TestIsCurrency(t *testing.T) {
	for _, s := range []string{"USD", "EUR", "JPY", "VND"} {
		if !IsCurrency(s) {
			t.Errorf("IsCurrency(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "US", "USDD", "usd", "Us1", "U$D"} {
		if IsCurrency(s) {
			t.Errorf("IsCurrency(%q) = true, want false", s)
		}
	}
}

func TestIsTestToken(t *testing.T) {
	for _, s := range []string{"tok_visa", "tok_mastercard", "tok_ABC_123"} {
		if !IsTestToken(s) {
			t.Errorf("IsTestToken(%q) = false, want true", s)
		}
	}
	for _, s := range []string{
		"",                               // empty
		"visa",                           // no tok_ prefix
		"tok_4111111111111111",           // bare PAN (16 digits)
		"tok_4111_1111_1111_1111",        // grouped PAN — separators must not hide it
		"tok_" + strings.Repeat("a", 61), // 65 chars, over the 64 limit
		"tok_with-dash",                  // disallowed char
		"tok_émoji",                      // non-ASCII
	} {
		if IsTestToken(s) {
			t.Errorf("IsTestToken(%q) = true, want false", s)
		}
	}
}
