package database

import (
	"context"

	"github.com/duynhlab/pkg/dbx"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/duynhlab/payment-service/config"
)

// Connect builds the service's Postgres pool via the shared dbx helper. dbx
// wires otelpgx query tracing (bounded span names, no bind-parameter or
// connection PII) and pgxpool.* pool-stat metrics, and applies the
// transaction-mode-pooler-safe settings (simple protocol, statement/description
// caches off) required by the PgDog/PgBouncer pooler.
//
// The DSN is cfg.Database.BuildDSN() — the single source shared with the
// `migrate` subcommand, so the app and migrations connect identically.
//
// When DB_PASSWORD_FILE is set (ADR-025 pattern A), WithPasswordFile makes
// dbx read the password from that mounted file on every new connection, so a
// rotated password is used without a pod restart. The DSN password is then
// ignored: a missing or empty file fails the connection instead of falling
// back. dbx also caps connection lifetime at 30m (±3m) so the pool moves onto
// a rotated password. An empty path is a no-op (DB_PASSWORD as before).
func Connect(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, error) {
	return dbx.NewPool(ctx, cfg.Database.BuildDSN(),
		dbx.WithMaxConns(cfg.Database.MaxConnections),
		dbx.WithPasswordFile(cfg.Database.PasswordFile),
	)
}
