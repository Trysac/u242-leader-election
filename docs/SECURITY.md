# Security notes

## Threat model

`github.com/Trysac/u242-leader-election` runs election over caller-supplied stores. The Postgres and Redis stores perform I/O through injected `Querier` and `Evaler` implementations, so transport security, authentication and secret handling belong to those implementations.

### Defended

- **Fencing term**: a term increases when the holder changes or a holder re-acquires after expiry and never decreases, even after expiry or `Release`. Applications can use `Term` to reject writes from a stale leader if they enforce it.
- **Scoped release**: `Release` changes nothing for a non-holder in `MemoryStore`; `ReleaseSQL` uses `WHERE name = $1 AND holder = $2`; `ReleaseScript` deletes the lease hash only when `holder == ARGV[1]`.
- **Configuration validation**: `NewElector` returns an `*Error` with `Code` `invalid_config` for an empty `Identity`, `LeaseDuration <= RenewInterval` or `RetryInterval <= 0`.
- **Error wrapping**: store errors are wrapped as `*Error` with `Code` `store` and do not include connection strings.
- **Expiry authority**: Postgres expiry is decided by `now()` in the database; Redis lease TTL is set with `PEXPIRE` in milliseconds; `MemoryStore` uses the injected clock under a mutex.

### Not defended

- TLS, authentication, authorization and network segmentation for Postgres or Redis; these are properties of the injected `Querier` or `Evaler`.
- A compromised backing store, a wrong database clock, a wrong Redis server clock, or a malicious `Querier`/`Evaler` implementation.
- Cross-process coordination through `MemoryStore`; it is in-process only.
- Consensus, Kubernetes `Lease` objects or etcd semantics.
- Automatic fencing of application side effects; the library exposes `Term` but does not block stale writes.
- Denial of service against the backing store, the database or Redis.
- Secret storage in the repository; the library does not manage credentials.

## Publishing

Releases are git tags served through the Go module proxy and checksum database. No registry token is stored in the repository. The specification does not define a dedicated security contact.
