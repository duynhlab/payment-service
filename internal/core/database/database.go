package database

import (
	"context"
	"fmt"
	"math"

	"github.com/duynhlab/payment-service/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect establishes a database connection pool using pgx/v5 from the already
// parsed application config. The DSN is config.DatabaseConfig.BuildDSN() — the
// single source shared with the `migrate` subcommand, so the app and migrations
// connect with an identical DSN. The pool size comes from cfg.Database.MaxConnections
// (applied on the parsed config, not via a DSN query param, so the migrate DSN
// stays a plain postgres URL).
//
// Why pgx instead of lib/pq?
//   - pgx uses client-side prepared statements, compatible with PgCat/PgBouncer
//     transaction mode.
//   - lib/pq uses server-side prepared statements which cause errors with
//     connection poolers:
//     "pq: bind message supplies 1 parameters, but prepared statement "" requires 2".
//   - pgxpool provides built-in connection pooling optimized for PostgreSQL.
//
// IMPORTANT: We use SimpleProtocol mode and disable statement caching to work
// correctly with transaction-mode connection poolers (PgCat/PgBouncer). Without
// this, you may see: "prepared statement stmtcache_* does not exist".
func Connect(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.Database.BuildDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	if n := cfg.Database.MaxConnections; n > 0 && n <= math.MaxInt32 {
		poolCfg.MaxConns = int32(n)
	}

	// Configure for transaction-mode poolers (PgCat/PgBouncer):
	// - Use simple protocol to avoid server-side prepared statements
	// - Disable statement cache (prepared statements are connection-scoped)
	// - Disable description cache
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	poolCfg.ConnConfig.StatementCacheCapacity = 0
	poolCfg.ConnConfig.DescriptionCacheCapacity = 0

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close() // Clean up on failure
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}
