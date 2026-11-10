// Package leaderelection is a small, dependency-free Go library: Lease-based leader election for Go services over Postgres or Redis: exact SQL and Lua, monotonically increasing terms, step-down before the lease expires and an in-memory store
//
// The public API is every exported identifier of this root package. Functions are pure: they do
// no I/O, keep no global mutable state and report invalid input through returned errors.
package leaderelection
