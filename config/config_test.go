package config

import (
	"strings"
	"testing"
	"time"
)

func validPayment() PaymentConfig {
	return PaymentConfig{
		AuthHoldTTL:             time.Hour,
		IdempotencyKeyTTL:       time.Hour,
		IdempotencyLockTakeover: time.Second,
		WebhookSecret:           "whsec_x",
	}
}

func TestValidatePayment(t *testing.T) {
	t.Run("valid config has no errors", func(t *testing.T) {
		c := &Config{Payment: validPayment()}
		if errs := c.validatePayment(); len(errs) != 0 {
			t.Fatalf("want no errors, got %v", errs)
		}
	})

	t.Run("empty webhook secret is rejected", func(t *testing.T) {
		p := validPayment()
		p.WebhookSecret = ""
		c := &Config{Payment: p}
		errs := c.validatePayment()
		if len(errs) != 1 || !strings.Contains(errs[0], "MOCKPAY_WEBHOOK_SECRET") {
			t.Fatalf("want a webhook-secret error, got %v", errs)
		}
	})

	t.Run("non-positive durations are rejected", func(t *testing.T) {
		c := &Config{Payment: PaymentConfig{WebhookSecret: "x"}}
		if errs := c.validatePayment(); len(errs) != 3 {
			t.Fatalf("want 3 duration errors, got %v", errs)
		}
	})
}
