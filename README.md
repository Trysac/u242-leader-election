# leader-election

[![Go Reference](https://pkg.go.dev/badge/github.com/Trysac/u242-leader-election.svg)](https://pkg.go.dev/github.com/Trysac/u242-leader-election)
[![Go Report Card](https://goreportcard.com/badge/github.com/Trysac/u242-leader-election)](https://goreportcard.com/report/github.com/Trysac/u242-leader-election)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Services that run several replicas still need exactly one of them to run the scheduler, the outbox relay or the cache warmer. Hand-written election code often keeps leading after it has lost the lease, reuses the same term after a failover so stale leaders cannot be fenced, and is impossible to test without a real database and real waits. This package runs the campaign loop against a small `Store` interface, ships the exact Postgres statement and Redis script, and drives everything from an injected clock.

## Features

- **Store abstraction**: implement `TryAcquire` and `Release` for any backing system.
- **MemoryStore**: mutex-protected, clock-injectable, for deterministic tests.
- **PostgresStore**: sends `AcquireSQL`, `CurrentSQL` and `ReleaseSQL` through an injected `Querier`; the database clock decides expiry.
- **RedisStore**: runs `AcquireScript` and `ReleaseScript` through an injected `Evaler`; lease TTL is set in milliseconds.
- **Elector lifecycle**: renews while leader, steps down when losing the lease, and fires `OnStartedLeading`, `OnStoppedLeading` and `OnNewLeader`.
- **Monotonic terms**: a term increases whenever the holder changes or a holder re-acquires after expiry, and never decreases, even after expiry or `Release`.

## Usage

Import `github.com/Trysac/u242-leader-election` as `leaderelection`.

```go
package main
import ("context"; "time"; "log"; leaderelection "github.com/Trysac/u242-leader-election")
func main() {
    store := leaderelection.NewMemoryStore(time.Now)
    el, err := leaderelection.NewElector(leaderelection.Config{Store: store, Name: "jobs", Identity: "a", LeaseDuration: 15 * time.Second})
    if err != nil { log.Fatal(err) }
    ctx, cancel := context.WithCancel(context.Background()); defer cancel()
    _ = el.Run(ctx)
}
```

`Run` blocks until `ctx` is cancelled, renewing while leading and releasing the lease on exit if the elector was leader.

## Public API

- `type Record struct { Holder string; Term int64; ExpiresAt time.Time }`.
- `type Store interface { TryAcquire(ctx context.Context, name, candidate string, ttl time.Duration) (Record, bool, error); Release(ctx context.Context, name, candidate string) error }`.
- `func NewMemoryStore(now func() time.Time) *MemoryStore`.
- `type PostgresStore struct { DB Querier }` with `type Querier interface { QueryRow(ctx context.Context, query string, args ...any) Row }` and `type Row interface { Scan(dest ...any) error }`.
- `type RedisStore struct { Client Evaler; Prefix string }` with `type Evaler interface { Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) }`; `Prefix` defaults to `"le:"`.
- `func NewElector(cfg Config) (*Elector, error)` with `Config { Store Store; Name, Identity string; LeaseDuration, RenewInterval, RetryInterval time.Duration; Now func() time.Time; After func(time.Duration) <-chan time.Time; OnStartedLeading func(ctx context.Context); OnStoppedLeading func(); OnNewLeader func(identity string, term int64) }` and methods `Step(ctx) error`, `Run(ctx) error`, `IsLeader() bool`, `Term() int64`.
- `type Error struct { Code string; Msg string }` with codes `invalid_config` and `store`.

`NewElector` rejects an empty `Identity`, `LeaseDuration <= RenewInterval` and `RetryInterval <= 0` with an `*Error` whose `Code` is `invalid_config`. Lease defaults are `LeaseDuration` 15s, `RenewInterval` 5s and `RetryInterval` 2s. `Run` returns `ctx.Err()` after cancellation and releases the lease with a fresh context bounded to 1s if the elector was leader. Store errors are wrapped as `*Error` with `Code` `store` and do not include connection strings. The term semantics are fencing: a term increments when the holder changes or a holder re-acquires after expiry, and it never decreases.

## Adapters for real databases

### PostgreSQL

The `PostgresStore` accepts any `Querier` with a `QueryRow` method. Adapters for common drivers:

```go
// database/sql
type SQLQuerier struct { DB *sql.DB }
func (q SQLQuerier) QueryRow(ctx context.Context, query string, args ...any) leaderelection.Row {
    return q.DB.QueryRowContext(ctx, query, args...)
}

// pgx
import "github.com/jackc/pgx/v5"

type PgxQuerier struct { Pool *pgxpool.Pool }
func (q PgxQuerier) QueryRow(ctx context.Context, query string, args ...any) leaderelection.Row {
    return q.Pool.QueryRow(ctx, query, args...)
}
```

### Redis

Implement `Evaler` using your Redis client of choice (for example go-redis). `RedisStore` calls `Eval` with `AcquireScript`, keys `["le:jobs", "le:jobs:term"]` and args `("a", int64(10000))` for a 10s TTL, and computes `ExpiresAt` as `Now() + returned milliseconds`. `ReleaseScript` deletes the lease hash only when `holder == ARGV[1]`.

## Why not Kubernetes client-go leaderelection

This package does not require a Kubernetes API server or `Lease` objects, and it can be tested with a fake clock. It only provides the campaign, renewal and step-down lifecycle for one leader per name; it is not a distributed mutex and does not automatically fence application side effects.

## Documentation

See [docs/SECURITY.md](docs/SECURITY.md) for the threat model and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for design notes.
