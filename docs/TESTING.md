# Testing

## Running

```sh
go test -cover .          # tests with coverage summary
go run ./scripts/check    # every gate, as CI and the factory run it
```

## Policy

The tests are Go `_test.go` files in package `leaderelection_test` beside the implementation. They exercise the public API and cover the acceptance criteria: `MemoryStore` term rules with a fake clock; `Elector` callbacks, step-down, `IsLeader` and `Term`; `PostgresStore` SQL and args through an injected `Querier`, including `sql.ErrNoRows`; `RedisStore` `Eval` args; `NewElector` validation; `Run` returning `ctx.Err()` after cancellation and releasing a held lease; and `Step` with `IsLeader` under the race detector. Edge cases include two electors on one `MemoryStore` never both reporting `IsLeader()` at the same fake instant, a `Step` after context cancellation, and a nil `OnStartedLeading`.

Tests assert observable effects: return values, returned errors through `errors.Is`, and error messages. They are deterministic: no network, no wall clock and no dependence on test order.

## Tiers

- Local tier: unit and property tests, plus conformance on small fixtures kept in the repository.
- CI tier: conformance that needs an external tool or a download. Start the file with `//go:build conformance`; `scripts/check` adds `-tags=conformance` only in CI unless `FABRICA_CI=1`. The CI `conformance` job sets `FABRICA_CI=1` and runs the full suite. Tagged tests skip cleanly when their tool is missing.

## Layout

`<file>_test.go` sits next to the file it tests. Fixtures and golden files go in `testdata/` if present. The specification does not enumerate individual test file names.
