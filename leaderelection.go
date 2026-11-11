package leaderelection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Error is the base error type for the leaderelection package.
// It has a stable machine-readable Code, a human-readable Msg and an optional cause.
type Error struct {
	Code  string
	Msg   string
	cause error
}

func (e *Error) Error() string {
	return fmt.Sprintf("leaderelection: %s: %s", e.Code, e.Msg)
}

func (e *Error) Unwrap() error {
	return e.cause
}

func newInvalidConfig(msg string) *Error {
	return &Error{Code: "invalid_config", Msg: msg}
}

func newStoreError(msg string, cause error) *Error {
	return &Error{Code: "store", Msg: msg, cause: cause}
}

// Record describes the current holder of a named lease.
type Record struct {
	Holder    string
	Term      int64
	ExpiresAt time.Time
}

// Store is the abstract interface for lease storage.
type Store interface {
	// TryAcquire attempts to acquire or renew the lease named name for candidate.
	// It returns the current lease record and whether the attempt succeeded.
	TryAcquire(ctx context.Context, name, candidate string, ttl time.Duration) (Record, bool, error)
	// Release releases the lease if it is held by candidate.
	Release(ctx context.Context, name, candidate string) error
}

// memoryEntry is the internal representation of a lease in MemoryStore.
type memoryEntry struct {
	holder    string
	term      int64
	expiresAt time.Time
}

// MemoryStore implements Store using an in-memory map protected by a mutex.
// It is intended for testing and deterministic simulations with a fake clock.
type MemoryStore struct {
	mu     sync.Mutex
	now    func() time.Time
	leases map[string]memoryEntry
}

// NewMemoryStore creates a MemoryStore. If now is nil, time.Now is used.
func NewMemoryStore(now func() time.Time) *MemoryStore {
	if now == nil {
		now = time.Now
	}
	return &MemoryStore{
		now:    now,
		leases: make(map[string]memoryEntry),
	}
}

func (s *MemoryStore) TryAcquire(ctx context.Context, name, candidate string, ttl time.Duration) (Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, false, err
	}
	if name == "" {
		return Record{}, false, newStoreError("name must not be empty", nil)
	}
	if candidate == "" {
		return Record{}, false, newStoreError("candidate must not be empty", nil)
	}
	if ttl <= 0 {
		return Record{}, false, newStoreError("ttl must be positive", nil)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	entry, exists := s.leases[name]
	if !exists || !entry.expiresAt.After(now) {
		term := int64(1)
		if exists {
			term = entry.term + 1
		}
		newExpires := now.Add(ttl)
		s.leases[name] = memoryEntry{holder: candidate, term: term, expiresAt: newExpires}
		return Record{Holder: candidate, Term: term, ExpiresAt: newExpires}, true, nil
	}

	// Lease is valid and unexpired.
	if entry.holder == candidate {
		newExpires := now.Add(ttl)
		entry.expiresAt = newExpires
		s.leases[name] = entry
		return Record{Holder: candidate, Term: entry.term, ExpiresAt: newExpires}, true, nil
	}

	// Someone else holds the lease.
	return Record{Holder: entry.holder, Term: entry.term, ExpiresAt: entry.expiresAt}, false, nil
}

func (s *MemoryStore) Release(ctx context.Context, name, candidate string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.leases[name]
	if !exists || entry.holder != candidate {
		return nil
	}
	entry.expiresAt = s.now()
	s.leases[name] = entry
	return nil
}

// Querier is the minimal database interface required by PostgresStore.
type Querier interface {
	QueryRow(ctx context.Context, query string, args ...any) Row
}

// Row is the result of a single row query.
type Row interface {
	Scan(dest ...any) error
}

// PostgresStore implements Store using PostgreSQL queries.
// It requires a Querier implementation (e.g., an adapter around *sql.DB or pgx).
type PostgresStore struct {
	DB Querier
}

// SchemaSQL creates the leader_leases table if it does not exist.
const SchemaSQL = `CREATE TABLE IF NOT EXISTS leader_leases (
	name text PRIMARY KEY,
	holder text NOT NULL,
	term bigint NOT NULL,
	expires_at timestamptz NOT NULL
)`

// AcquireSQL attempts to acquire or renew a lease atomically.
// It uses a single INSERT ... ON CONFLICT DO UPDATE statement.
const AcquireSQL = `INSERT INTO leader_leases AS l (name, holder, term, expires_at)
VALUES ($1, $2, 1, now() + $3 * interval '1 millisecond')
ON CONFLICT (name) DO UPDATE
SET holder = EXCLUDED.holder,
    term = CASE WHEN l.holder = EXCLUDED.holder AND l.expires_at > now() THEN l.term ELSE l.term + 1 END,
    expires_at = EXCLUDED.expires_at
WHERE l.holder = EXCLUDED.holder OR l.expires_at <= now()
RETURNING holder, term, expires_at`

// CurrentSQL returns the current lease record for a name.
const CurrentSQL = `SELECT holder, term, expires_at FROM leader_leases WHERE name = $1`

// ReleaseSQL releases a lease if it is held by the given candidate.
const ReleaseSQL = `UPDATE leader_leases SET expires_at = now() WHERE name = $1 AND holder = $2 RETURNING name`

func (s *PostgresStore) TryAcquire(ctx context.Context, name, candidate string, ttl time.Duration) (Record, bool, error) {
	if ttl <= 0 {
		return Record{}, false, newStoreError("ttl must be positive", nil)
	}
	ms := int64(ttl / time.Millisecond)
	row := s.DB.QueryRow(ctx, AcquireSQL, name, candidate, ms)
	var holder string
	var term int64
	var expiresAt time.Time
	err := row.Scan(&holder, &term, &expiresAt)
	if err == nil {
		return Record{Holder: holder, Term: term, ExpiresAt: expiresAt}, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		// The acquire did not succeed; fetch the current record.
		currRow := s.DB.QueryRow(ctx, CurrentSQL, name)
		if err := currRow.Scan(&holder, &term, &expiresAt); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// No row at all; treat as not acquired.
				return Record{}, false, nil
			}
			return Record{}, false, newStoreError("failed to query current lease", err)
		}
		return Record{Holder: holder, Term: term, ExpiresAt: expiresAt}, false, nil
	}
	return Record{}, false, newStoreError("acquire query failed", err)
}

func (s *PostgresStore) Release(ctx context.Context, name, candidate string) error {
	row := s.DB.QueryRow(ctx, ReleaseSQL, name, candidate)
	var dummy string
	err := row.Scan(&dummy)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return newStoreError("release query failed", err)
	}
	return nil
}

// Evaler is the minimal Redis client interface required by RedisStore.
type Evaler interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
}

// RedisStore implements Store using Redis Lua scripts for atomicity.
type RedisStore struct {
	Client Evaler
	Prefix string // default "le:"
}

// AcquireScript atomically acquires or renews a lease in Redis.
// KEYS[1] is the lease hash, KEYS[2] is the term counter.
// ARGV[1] is the candidate, ARGV[2] is the TTL in milliseconds.
const AcquireScript = `local h = redis.call('HGET', KEYS[1], 'holder')
if not h then
  local t = redis.call('INCR', KEYS[2])
  redis.call('HSET', KEYS[1], 'holder', ARGV[1], 'term', t)
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {ARGV[1], t, 1, tonumber(ARGV[2])}
elseif h == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {h, tonumber(redis.call('HGET', KEYS[1], 'term')), 1, tonumber(ARGV[2])}
else
  return {h, tonumber(redis.call('HGET', KEYS[1], 'term')), 0, redis.call('PTTL', KEYS[1])}
end`

// ReleaseScript deletes the lease hash only if the holder matches ARGV[1].
const ReleaseScript = `if redis.call('HGET', KEYS[1], 'holder') == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
else
  return 0
end`

func (s *RedisStore) prefix() string {
	if s.Prefix == "" {
		return "le:"
	}
	return s.Prefix
}

func (s *RedisStore) TryAcquire(ctx context.Context, name, candidate string, ttl time.Duration) (Record, bool, error) {
	if ttl <= 0 {
		return Record{}, false, newStoreError("ttl must be positive", nil)
	}
	ms := int64(ttl / time.Millisecond)
	keys := []string{s.prefix() + name, s.prefix() + name + ":term"}
	result, err := s.Client.Eval(ctx, AcquireScript, keys, candidate, ms)
	if err != nil {
		return Record{}, false, newStoreError("redis eval failed", err)
	}
	// The Lua script returns a Lua table, which the Redis client maps to []interface{}.
	vals, ok := result.([]interface{})
	if !ok || len(vals) != 4 {
		return Record{}, false, newStoreError("unexpected redis eval result", nil)
	}
	holder, _ := vals[0].(string)
	term, _ := vals[1].(int64)
	acquiredInt, _ := vals[2].(int64)
	ttlMS, _ := vals[3].(int64)
	acquired := acquiredInt == 1

	expiresAt := time.Now().Add(time.Duration(ttlMS) * time.Millisecond)
	return Record{Holder: holder, Term: term, ExpiresAt: expiresAt}, acquired, nil
}

func (s *RedisStore) Release(ctx context.Context, name, candidate string) error {
	keys := []string{s.prefix() + name}
	_, err := s.Client.Eval(ctx, ReleaseScript, keys, candidate)
	if err != nil {
		return newStoreError("redis eval failed", err)
	}
	return nil
}

// Config holds the configuration for an Elector.
// LeaseDuration defaults to 15s and RenewInterval to 5s when zero;
// RetryInterval must be positive.
type Config struct {
	Store            Store
	Name             string
	Identity         string
	LeaseDuration    time.Duration
	RenewInterval    time.Duration
	RetryInterval    time.Duration
	Now              func() time.Time
	After            func(time.Duration) <-chan time.Time
	OnStartedLeading func(ctx context.Context)
	OnStoppedLeading func()
	OnNewLeader      func(identity string, term int64)
}

// Elector runs the leader-election campaign for a single candidate.
type Elector struct {
	cfg Config

	mu             sync.Mutex
	leader         bool
	term           int64
	deadline       time.Time
	observedLeader string
	observedTerm   int64
	leadingCtx     context.Context
	cancelLeading  context.CancelFunc
}

// NewElector validates the configuration and returns an Elector.
func NewElector(cfg Config) (*Elector, error) {
	// Apply defaults.
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 15 * time.Second
	}
	if cfg.RenewInterval == 0 {
		cfg.RenewInterval = 5 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.After == nil {
		cfg.After = time.After
	}

	// Validate.
	if cfg.Store == nil {
		return nil, newInvalidConfig("Store is required")
	}
	if cfg.Name == "" {
		return nil, newInvalidConfig("Name is required")
	}
	if cfg.Identity == "" {
		return nil, newInvalidConfig("Identity is required")
	}
	if cfg.LeaseDuration <= 0 {
		return nil, newInvalidConfig("LeaseDuration must be positive")
	}
	if cfg.RenewInterval <= 0 {
		return nil, newInvalidConfig("RenewInterval must be positive")
	}
	if cfg.RetryInterval <= 0 {
		return nil, newInvalidConfig("RetryInterval must be positive")
	}
	if cfg.LeaseDuration <= cfg.RenewInterval {
		return nil, newInvalidConfig("LeaseDuration must be greater than RenewInterval")
	}

	return &Elector{cfg: cfg}, nil
}

// Step performs a single acquisition attempt.
// It returns an error if the underlying store fails, wrapped as *Error with Code "store".
func (e *Elector) Step(ctx context.Context) error {
	rec, acquired, err := e.cfg.Store.TryAcquire(ctx, e.cfg.Name, e.cfg.Identity, e.cfg.LeaseDuration)
	if err != nil {
		// A failed renewal only keeps leadership while the local deadline has
		// not passed.
		e.stepDownIfExpired()
		return e.storeError(err)
	}
	e.handleAcquire(rec, acquired)
	return nil
}

func (e *Elector) storeError(err error) error {
	var le *Error
	if errors.As(err, &le) && le.Code == "store" {
		return err
	}
	return newStoreError("store operation failed", err)
}

// stepDownIfExpired relinquishes leadership when the local lease deadline has
// passed, cancelling the leading context before invoking OnStoppedLeading.
func (e *Elector) stepDownIfExpired() {
	e.mu.Lock()
	if !e.leader || e.cfg.Now().Before(e.deadline) {
		e.mu.Unlock()
		return
	}
	e.leader = false
	if e.cancelLeading != nil {
		e.cancelLeading()
		e.cancelLeading = nil
	}
	e.deadline = time.Time{}
	onStopped := e.cfg.OnStoppedLeading
	e.mu.Unlock()

	if onStopped != nil {
		onStopped()
	}
}

func (e *Elector) handleAcquire(rec Record, acquired bool) {
	var callbacks []func()

	e.mu.Lock()
	now := e.cfg.Now()

	if acquired {
		if !e.leader {
			// Transition to leader. Capture the context so callbacks never read
			// the field (it may be replaced or cancelled by a concurrent Step).
			e.leader = true
			e.leadingCtx, e.cancelLeading = context.WithCancel(context.Background())
			leadingCtx := e.leadingCtx
			e.deadline = now.Add(e.cfg.LeaseDuration)
			e.term = rec.Term
			e.observedLeader = rec.Holder
			e.observedTerm = rec.Term
			callbacks = append(callbacks, func() {
				if e.cfg.OnStartedLeading != nil {
					e.cfg.OnStartedLeading(leadingCtx)
				}
			}, func() {
				if e.cfg.OnNewLeader != nil {
					e.cfg.OnNewLeader(rec.Holder, rec.Term)
				}
			})
		} else {
			// Already leader; check if term changed.
			if rec.Holder != e.observedLeader || rec.Term != e.observedTerm {
				e.observedLeader = rec.Holder
				e.observedTerm = rec.Term
				callbacks = append(callbacks, func() {
					if e.cfg.OnNewLeader != nil {
						e.cfg.OnNewLeader(rec.Holder, rec.Term)
					}
				})
			}
			e.deadline = now.Add(e.cfg.LeaseDuration)
			e.term = rec.Term
		}
	} else {
		// Not acquired.
		stepDown := false
		if e.leader {
			if !now.Before(e.deadline) || rec.Holder != e.cfg.Identity {
				stepDown = true
			}
		}
		if stepDown {
			e.leader = false
			if e.cancelLeading != nil {
				e.cancelLeading()
				e.cancelLeading = nil
			}
			e.deadline = time.Time{}
			e.term = rec.Term
			callbacks = append(callbacks, func() {
				if e.cfg.OnStoppedLeading != nil {
					e.cfg.OnStoppedLeading()
				}
			})
		}
		// Update observed and possibly fire OnNewLeader for the new leader (or same with new term).
		if rec.Holder != e.observedLeader || rec.Term != e.observedTerm {
			e.observedLeader = rec.Holder
			e.observedTerm = rec.Term
			callbacks = append(callbacks, func() {
				if e.cfg.OnNewLeader != nil {
					e.cfg.OnNewLeader(rec.Holder, rec.Term)
				}
			})
		}
	}
	e.mu.Unlock()

	for _, cb := range callbacks {
		cb()
	}
}

// Run runs the election loop until ctx is cancelled.
// It returns ctx.Err() when the context is done, releasing the lease if currently leader.
func (e *Elector) Run(ctx context.Context) error {
	for {
		e.mu.Lock()
		interval := e.cfg.RetryInterval
		if e.leader {
			interval = e.cfg.RenewInterval
		}
		e.mu.Unlock()

		select {
		case <-ctx.Done():
			e.releaseOnExit()
			return ctx.Err()
		case <-e.cfg.After(interval):
		}

		if err := e.Step(ctx); err != nil {
			if ctx.Err() != nil {
				e.releaseOnExit()
				return ctx.Err()
			}
			// A transient store error must not abandon the campaign: Step
			// keeps leadership until the local deadline and steps down after
			// it, so retry on the next tick.
			continue
		}
	}
}

func (e *Elector) releaseOnExit() {
	e.mu.Lock()
	if !e.leader {
		e.mu.Unlock()
		return
	}
	store := e.cfg.Store
	name := e.cfg.Name
	identity := e.cfg.Identity
	cancel := e.cancelLeading
	e.leader = false
	e.deadline = time.Time{}
	if cancel != nil {
		cancel()
		e.cancelLeading = nil
	}
	e.mu.Unlock()

	releaseCtx, cancelRelease := context.WithTimeout(context.Background(), time.Second)
	defer cancelRelease()
	_ = store.Release(releaseCtx, name, identity)

	if e.cfg.OnStoppedLeading != nil {
		e.cfg.OnStoppedLeading()
	}
}

// IsLeader reports whether this candidate currently holds the leadership.
func (e *Elector) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.leader
}

// Term returns the most recently observed term number.
func (e *Elector) Term() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.term
}
