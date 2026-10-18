package database

import (
	"context"
	"testing"

	"github.com/duynhlab/payment-service/config"
)

// TestConnect_ParseError verifies Connect returns an error when the DSN is
// invalid. An unknown sslmode makes pgxpool.ParseConfig reject the DSN before
// any network I/O happens, with or without a password file configured.
func TestConnect_ParseError(t *testing.T) {
	cfg := &config.Config{}
	cfg.Database = config.DatabaseConfig{
		Host:           "localhost",
		Port:           "5432",
		Name:           "payment",
		User:           "payment",
		PasswordFile:   "/etc/db/secret/password",
		SSLMode:        "bogus",
		MaxConnections: 25,
	}
	if _, err := Connect(context.Background(), cfg); err == nil {
		t.Fatal("want parse error, got nil")
	}
}
