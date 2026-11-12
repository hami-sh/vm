// Package store is the scheduler's write-ahead record in SQLite: intent and
// identity only, one transaction per transition, read back only on startup.
// W1 owns the implementation (modernc.org/sqlite, pure Go, CGO_ENABLED=0).
package store

import _ "embed"

// Schema is applied on open. events.kind includes 'alert' (wipe-store
// higher-generation path) and 'reconcile', added in Phase 0.
//
//go:embed schema.sql
var Schema string
