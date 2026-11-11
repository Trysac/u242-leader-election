# Architecture

This package implements lease-based leader election with a small `Store` interface. The core design decisions are:

- **Fencing tokens**: each lease holds a monotonically increasing `Term` that only increases when the holder changes or a holder re-acquires after expiry. This allows stale leaders to be detected and fenced.
- **Clock injection**: the `MemoryStore` and `Elector` accept a `Now` function, enabling deterministic tests without real waits.
- **Exact SQL and Lua**: the `PostgresStore` and `RedisStore` execute carefully crafted statements/scripts to ensure atomicity and the exact term semantics.
- **Elector state machine**: `Step` attempts acquisition and updates local state; leadership is considered valid only until the local deadline passes. Callbacks are invoked outside locks.
- **Testing strategy**: unit tests cover all acceptance criteria with fake clocks and stores; conformance tests may run against real databases in CI.
