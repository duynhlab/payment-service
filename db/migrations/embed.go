// Package migrations embeds the SQL schema migrations applied by golang-migrate
// (via pkg/migratex) from the service's `migrate` subcommand.
package migrations

import "embed"

// FS holds the versioned up-migrations (NNNNNN_*.up.sql) under sql/.
//
//go:embed sql/*.sql
var FS embed.FS
