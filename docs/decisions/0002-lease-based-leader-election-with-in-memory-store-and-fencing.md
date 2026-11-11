# Lease-based leader election with in-memory store and fencing terms

Status: accepted (generated, pending human review)

## Context

Replicated services need exactly one leader for tasks like scheduling or cache warming. Hand-written election code often mishandles lease expiry and cannot be tested deterministically.

## Decision

Implement a `Store` interface with three backends: `MemoryStore` for tests, `PostgresStore` with an atomic upsert SQL, and `RedisStore` with Lua scripts. Enforce monotonic terms that increment on holder change or expiry. The `Elector` runs a campaign loop with injectable clock and timeouts, stepping down before lease expiry.

## Consequences

Positive: deterministic testing, clear fencing semantics, reusable across database backends. Negative: requires an external store in production; Postgres and Redis implementations need proper client adapters.
