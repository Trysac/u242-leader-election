# Project record

## Objective

Lease-based leader election for Go services over Postgres or Redis: exact SQL and Lua, monotonically increasing terms, step-down before the lease expires and an in-memory store.

## Scope

- In scope: the public API documented in [README.md](../README.md): `Record`, `Store`, `NewMemoryStore`, `PostgresStore` with `Querier` and `Row`, `RedisStore` with `Evaler`, `NewElector` with `Config`, `Elector` methods, and `Error` codes `invalid_config` and `store`.
- In scope: term fencing semantics where a term increments when the holder changes or a holder re-acquires after expiry and never decreases, even after expiry or `Release`.
- In scope: the campaign loop, renewal, step-down, callbacks and release on exit.

## Out of scope

- Kubernetes `Lease` objects, etcd, consensus protocols and priority candidates.
- Real database tests in the local gates; a conformance test tagged `//go:build conformance` may run against services in CI.
- A distributed mutex service or automatic fencing of application side effects; callers must use `Term` if they need to reject stale writes.

## Timeline

No dated events are recorded in the specification.

## Effort metrics

Measured values only; a value that has not been measured stays "pending measurement".

| Metric | Value |
|---|---|
| Tokens (input / output) | 755384 / 69180 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.0885 |

## Factory run u242 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-10 | generation | deepseek-v4-pro | yes | 2058 | 24573 | 0 | 3328 | 0.0501 |
| 2026-11-10 | fix-1 | deepseek-flash | no | 17244 | 5904 | 0 | 187136 | 0.0067 |
| 2026-11-10 | audit | deepseek-flash | no | 25505 | 20836 | 0 | 268544 | 0.0171 |
| 2026-11-10 | fix-2 | deepseek-flash | no | 16106 | 10150 | 0 | 230272 | 0.0092 |
| 2026-11-10 | docs | deepseek-flash | no | 4807 | 7717 | 0 | 384 | 0.0054 |

Total: $0.0885. Prices: deepseek line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).
