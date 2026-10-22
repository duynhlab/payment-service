package config

import (
	"testing"
	"time"
)

// DB_POOL_MAX_CONN_LIFETIME and DB_POOL_HEALTH_CHECK_PERIOD are optional: a
// valid duration is used, anything else is 0 so the pkg/dbx default applies.
func TestGetEnvOptionalDuration(t *testing.T) {
	tests := []struct {
		name, val string
		want      time.Duration
	}{
		{"unset", "", 0},
		{"seconds", "45s", 45 * time.Second},
		{"invalid", "soon", 0},
		{"negative", "-1s", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DB_POOL_MAX_CONN_LIFETIME", tt.val)
			if got := getEnvOptionalDuration("DB_POOL_MAX_CONN_LIFETIME"); got != tt.want {
				t.Errorf("getEnvOptionalDuration(%q) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}
