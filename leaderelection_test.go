package leaderelection_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Trysac/u242-leader-election"
)

// fakeRow implements leaderelection.Row for tests.
type fakeRow struct {
	scanErr error
	values  []any
}

func (r fakeRow) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("fakeRow: expected %d dests, got %d", len(r.values), len(dest))
	}
	for i, v := range r.values {
		switch d := dest[i].(type) {
		case *string:
			*d = v.(string)
		case *int64:
			*d = v.(int64)
		case *time.Time:
			*d = v.(time.Time)
		default:
			return fmt.Errorf("fakeRow: unsupported type %T", dest[i])
		}
	}
	return nil
}

// fakeQuerier implements leaderelection.Querier.
type fakeQuerier struct {
	mu       sync.Mutex
	queries  []queryCall
	rowMap   map[string][]fakeRow // map query constant to sequence of rows
	rowIndex map[string]int
}

type queryCall struct {
	query string
	args  []any
}

func (q *fakeQuerier) QueryRow(ctx context.Context, query string, args ...any) leaderelection.Row {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queries = append(q.queries, queryCall{query: query, args: args})
	idx := q.rowIndex[query]
	q.rowIndex[query] = idx + 1
	rows := q.rowMap[query]
	if idx >= len(rows) {
		return fakeRow{scanErr: sql.ErrNoRows}
	}
	return rows[idx]
}

func newFakeQuerier() *fakeQuerier {
	return &fakeQuerier{
		queries:  make([]queryCall, 0),
		rowMap:   make(map[string][]fakeRow),
		rowIndex: make(map[string]int),
	}
}

// fakeEvaler implements leaderelection.Evaler.
type fakeEvaler struct {
	mu        sync.Mutex
	evalCalls []evalCall
	results   []evalResult
	index     int
}

type evalCall struct {
	script string
	keys   []string
	args   []any
}

type evalResult struct {
	result any
	err    error
}

func (f *fakeEvaler) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evalCalls = append(f.evalCalls, evalCall{script: script, keys: keys, args: args})
	if f.index >= len(f.results) {
		return nil, errors.New("unexpected eval call")
	}
	res := f.results[f.index]
	f.index++
	return res.result, res.err
}

func newFakeEvaler() *fakeEvaler {
	return &fakeEvaler{
		evalCalls: make([]evalCall, 0),
		results:   make([]evalResult, 0),
	}
}

// failingStore wraps a Store and can be set to return errors.
type failingStore struct {
	inner leaderelection.Store
	mu    sync.Mutex
	fail  bool
}

func (f *failingStore) TryAcquire(ctx context.Context, name, candidate string, ttl time.Duration) (leaderelection.Record, bool, error) {
	f.mu.Lock()
	fail := f.fail
	f.mu.Unlock()
	if fail {
		return leaderelection.Record{}, false, errors.New("injected failure")
	}
	return f.inner.TryAcquire(ctx, name, candidate, ttl)
}

func (f *failingStore) Release(ctx context.Context, name, candidate string) error {
	return f.inner.Release(ctx, name, candidate)
}

func (f *failingStore) setFail(v bool) {
	f.mu.Lock()
	f.fail = v
	f.mu.Unlock()
}

func TestMemoryStoreAcceptance(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	now := t0
	store := leaderelection.NewMemoryStore(func() time.Time { return now })
	ctx := context.Background()

	// First acquire by a.
	rec, ok, err := store.TryAcquire(ctx, "jobs", "a", 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire a: %v", err)
	}
	if !ok {
		t.Fatal("expected acquire true")
	}
	wantRec := leaderelection.Record{Holder: "a", Term: 1, ExpiresAt: t0.Add(10 * time.Second)}
	if rec != wantRec {
		t.Fatalf("record mismatch: got %+v, want %+v", rec, wantRec)
	}

	// b fails.
	rec, ok, err = store.TryAcquire(ctx, "jobs", "b", 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire b: %v", err)
	}
	if ok {
		t.Fatal("expected acquire false for b")
	}
	if rec != wantRec {
		t.Fatalf("record mismatch for b: got %+v, want %+v", rec, wantRec)
	}

	// Advance past expiry; b should acquire with term 2.
	now = t0.Add(10 * time.Second)
	rec, ok, err = store.TryAcquire(ctx, "jobs", "b", 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire b after expiry: %v", err)
	}
	if !ok {
		t.Fatal("expected acquire true for b after expiry")
	}
	wantRec = leaderelection.Record{Holder: "b", Term: 2, ExpiresAt: now.Add(10 * time.Second)}
	if rec != wantRec {
		t.Fatalf("record mismatch after expiry: got %+v, want %+v", rec, wantRec)
	}

	// Renewal by a at t0+5s (before initial expiry) should renew with term 1.
	now = t0.Add(5 * time.Second)
	// Reset store to initial state first (since we advanced). Better create fresh.
	store2 := leaderelection.NewMemoryStore(func() time.Time { return now })
	rec, ok, _ = store2.TryAcquire(ctx, "jobs", "a", 10*time.Second)
	if !ok || rec.Term != 1 {
		t.Fatalf("initial renew setup failed")
	}
	now = t0.Add(5 * time.Second)
	rec, ok, err = store2.TryAcquire(ctx, "jobs", "a", 10*time.Second)
	if err != nil {
		t.Fatalf("renew a: %v", err)
	}
	if !ok {
		t.Fatal("renew should succeed")
	}
	wantRec = leaderelection.Record{Holder: "a", Term: 1, ExpiresAt: now.Add(10 * time.Second)}
	if rec != wantRec {
		t.Fatalf("renew record mismatch: got %+v, want %+v", rec, wantRec)
	}

	// Release by wrong holder changes nothing.
	err = store2.Release(ctx, "jobs", "b")
	if err != nil {
		t.Fatalf("Release b: %v", err)
	}
	rec, ok, _ = store2.TryAcquire(ctx, "jobs", "b", 10*time.Second)
	if ok {
		t.Fatal("b should not acquire after its release")
	}

	// Release by a; then b can acquire at same instant.
	err = store2.Release(ctx, "jobs", "a")
	if err != nil {
		t.Fatalf("Release a: %v", err)
	}
	rec, ok, err = store2.TryAcquire(ctx, "jobs", "b", 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire b after release: %v", err)
	}
	if !ok {
		t.Fatal("b should acquire after release")
	}
	if rec.Term != 2 {
		t.Fatalf("expected term 2, got %d", rec.Term)
	}
}

func TestMemoryStoreConcurrentNoDoubleLeadership(t *testing.T) {
	var now time.Time
	store := leaderelection.NewMemoryStore(func() time.Time { return now })
	ctx := context.Background()

	e1, _ := leaderelection.NewElector(leaderelection.Config{
		Store: store, Name: "x", Identity: "a",
		LeaseDuration: 15 * time.Second, RenewInterval: 5 * time.Second, RetryInterval: 2 * time.Second,
		Now: func() time.Time { return now },
	})
	e2, _ := leaderelection.NewElector(leaderelection.Config{
		Store: store, Name: "x", Identity: "b",
		LeaseDuration: 15 * time.Second, RenewInterval: 5 * time.Second, RetryInterval: 2 * time.Second,
		Now: func() time.Time { return now },
	})

	// Interleave steps.
	if err := e1.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e2.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if e1.IsLeader() && e2.IsLeader() {
		t.Fatal("both leaders simultaneously")
	}
}

// TestElectorConcurrentStepAndIsLeader exercises Step and IsLeader/Term from
// multiple goroutines so the race detector can catch unsynchronised field
// access, and checks that no two identities lead at the same fake instant.
func TestElectorConcurrentStepAndIsLeader(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	now := t0
	nowFn := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	store := leaderelection.NewMemoryStore(nowFn)

	newElector := func(id string) *leaderelection.Elector {
		e, err := leaderelection.NewElector(leaderelection.Config{
			Store: store, Name: "jobs", Identity: id,
			LeaseDuration: 15 * time.Second, RenewInterval: 5 * time.Second, RetryInterval: 2 * time.Second,
			Now: nowFn,
		})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e1 := newElector("a")
	e2 := newElector("b")
	ctx := context.Background()

	stop := make(chan struct{})
	var watchers sync.WaitGroup
	for i := 0; i < 4; i++ {
		watchers.Add(1)
		go func() {
			defer watchers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = e1.IsLeader()
					_ = e1.Term()
					_ = e2.IsLeader()
					_ = e2.Term()
				}
			}
		}()
	}

	var steppers sync.WaitGroup
	for i := 0; i < 8; i++ {
		e := e1
		if i%2 == 1 {
			e = e2
		}
		steppers.Add(1)
		go func() {
			defer steppers.Done()
			for j := 0; j < 50; j++ {
				_ = e.Step(ctx)
			}
		}()
	}
	steppers.Wait()
	close(stop)
	watchers.Wait()

	if e1.IsLeader() && e2.IsLeader() {
		t.Fatal("both electors report leadership at the same fake instant")
	}
}

// TestStepAfterContextCancellation checks the documented edge case: a Step
// with a cancelled context returns a *Error with Code "store" wrapping
// context.Canceled and only steps down once the local deadline has passed.
func TestStepAfterContextCancellation(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	now := t0
	store := leaderelection.NewMemoryStore(func() time.Time { return now })
	var stopped int

	newElector := func(id string) *leaderelection.Elector {
		e, err := leaderelection.NewElector(leaderelection.Config{
			Store: store, Name: "jobs", Identity: id,
			LeaseDuration: 15 * time.Second, RenewInterval: 5 * time.Second, RetryInterval: 2 * time.Second,
			Now:              func() time.Time { return now },
			OnStoppedLeading: func() { stopped++ },
		})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := newElector("a")
	if err := e.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !e.IsLeader() {
		t.Fatal("expected leader")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	err := e.Step(cancelled)
	if err == nil {
		t.Fatal("expected an error from Step with a cancelled context")
	}
	var le *leaderelection.Error
	if !errors.As(err, &le) {
		t.Fatalf("expected *leaderelection.Error, got %T", err)
	}
	if le.Code != "store" {
		t.Fatalf("expected code %q, got %q", "store", le.Code)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected error to wrap context.Canceled, got %v", err)
	}
	// Before the local deadline a failed renewal keeps leadership.
	if !e.IsLeader() {
		t.Fatal("should still be leader before the deadline")
	}
	if stopped != 0 {
		t.Fatalf("OnStoppedLeading should not have fired yet, got %d", stopped)
	}

	// A fresh elector with a cancelled context never becomes leader.
	e2 := newElector("b")
	if err := e2.Step(cancelled); err == nil {
		t.Fatal("expected an error from Step with a cancelled context")
	}
	if e2.IsLeader() {
		t.Fatal("elector with a cancelled context must not become leader")
	}

	// Once the deadline has passed, the failed Step steps down.
	now = t0.Add(15 * time.Second)
	if err := e.Step(cancelled); err == nil {
		t.Fatal("expected an error from Step with a cancelled context")
	}
	if e.IsLeader() {
		t.Fatal("should not be leader after the deadline")
	}
	if stopped != 1 {
		t.Fatalf("expected one OnStoppedLeading, got %d", stopped)
	}
}

func TestElectorLifecycleAndCallbacks(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	now := t0
	store := leaderelection.NewMemoryStore(func() time.Time { return now })

	var startedCount, stoppedCount int
	var newLeaderCalls []string
	var leadingCtx context.Context

	e, err := leaderelection.NewElector(leaderelection.Config{
		Store: store, Name: "jobs", Identity: "a",
		LeaseDuration: 15 * time.Second, RenewInterval: 5 * time.Second, RetryInterval: 2 * time.Second,
		Now: func() time.Time { return now },
		OnStartedLeading: func(ctx context.Context) {
			startedCount++
			leadingCtx = ctx
		},
		OnStoppedLeading: func() {
			stoppedCount++
		},
		OnNewLeader: func(identity string, term int64) {
			newLeaderCalls = append(newLeaderCalls, fmt.Sprintf("%s:%d", identity, term))
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// First step acquires.
	if err := e.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if !e.IsLeader() {
		t.Fatal("expected leader")
	}
	if e.Term() != 1 {
		t.Fatalf("expected term 1, got %d", e.Term())
	}
	if startedCount != 1 {
		t.Fatalf("expected OnStartedLeading once, got %d", startedCount)
	}
	if leadingCtx == nil || leadingCtx.Err() != nil {
		t.Fatal("leading context should be non-nil and not done")
	}
	if len(newLeaderCalls) != 1 || newLeaderCalls[0] != "a:1" {
		t.Fatalf("expected OnNewLeader a:1 once, got %v", newLeaderCalls)
	}

	// Renewal at t0+5s should not call callbacks again.
	now = t0.Add(5 * time.Second)
	if err := e.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if startedCount != 1 || stoppedCount != 0 || len(newLeaderCalls) != 1 {
		t.Fatalf("unexpected callback counts after renewal: started=%d stopped=%d newLeader=%v", startedCount, stoppedCount, newLeaderCalls)
	}
}

func TestElectorStepDownOnErrorAfterDeadline(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	now := t0
	store := &failingStore{inner: leaderelection.NewMemoryStore(func() time.Time { return now })}
	var stoppedCount int
	var leadingCtx context.Context
	var startedCount int
	e, _ := leaderelection.NewElector(leaderelection.Config{
		Store: store, Name: "jobs", Identity: "a",
		LeaseDuration: 15 * time.Second, RenewInterval: 5 * time.Second, RetryInterval: 2 * time.Second,
		Now: func() time.Time { return now },
		OnStartedLeading: func(ctx context.Context) {
			startedCount++
			leadingCtx = ctx
		},
		OnStoppedLeading: func() { stoppedCount++ },
	})
	ctx := context.Background()

	// Acquire successfully.
	if err := e.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if !e.IsLeader() {
		t.Fatal("expected leader")
	}
	if startedCount != 1 {
		t.Fatalf("expected OnStartedLeading once, got %d", startedCount)
	}
	if leadingCtx == nil || leadingCtx.Err() != nil {
		t.Fatal("leading context should be non-nil and not done after acquiring")
	}

	// Make store fail.
	store.setFail(true)

	// At t0+14s, still leader despite errors.
	now = t0.Add(14 * time.Second)
	if err := e.Step(ctx); err == nil {
		t.Fatal("expected error from store")
	}
	if !e.IsLeader() {
		t.Fatal("should still be leader before deadline")
	}
	if stoppedCount != 0 {
		t.Fatal("stopped should not be called yet")
	}
	if leadingCtx.Err() != nil {
		t.Fatal("leading context should not be done before the deadline")
	}

	// At t0+15s, deadline passed -> step down.
	now = t0.Add(15 * time.Second)
	if err := e.Step(ctx); err == nil {
		t.Fatal("expected error from store")
	}
	if e.IsLeader() {
		t.Fatal("should not be leader after deadline")
	}
	if stoppedCount != 1 {
		t.Fatalf("expected one OnStoppedLeading, got %d", stoppedCount)
	}
	if !errors.Is(leadingCtx.Err(), context.Canceled) {
		t.Fatalf("leading context should be cancelled at the deadline, got %v", leadingCtx.Err())
	}
	if startedCount != 1 {
		t.Fatalf("expected OnStartedLeading once, got %d", startedCount)
	}
}

func TestElectorOnNewLeaderForOtherHolder(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	now := t0
	store := leaderelection.NewMemoryStore(func() time.Time { return now })
	var newLeaderCalls []string
	var stopped int
	e, _ := leaderelection.NewElector(leaderelection.Config{
		Store: store, Name: "jobs", Identity: "a",
		LeaseDuration: 15 * time.Second, RenewInterval: 5 * time.Second, RetryInterval: 2 * time.Second,
		Now:              func() time.Time { return now },
		OnStoppedLeading: func() { stopped++ },
		OnNewLeader: func(identity string, term int64) {
			newLeaderCalls = append(newLeaderCalls, fmt.Sprintf("%s:%d", identity, term))
		},
	})
	ctx := context.Background()

	// a acquires first.
	if err := e.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if len(newLeaderCalls) != 1 || newLeaderCalls[0] != "a:1" {
		t.Fatalf("expected initial call, got %v", newLeaderCalls)
	}

	// b acquires as a new leader; a should step down and observe b:2.
	// First, a's lease must expire, then b acquires.
	now = t0.Add(16 * time.Second) // after expiry, b can acquire
	rec, ok, err := store.TryAcquire(ctx, "jobs", "b", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("b should acquire: %v %v", ok, err)
	}
	_ = rec

	// Now a's Step sees other holder and steps down.
	if err := e.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if e.IsLeader() {
		t.Fatal("should not be leader")
	}
	if stopped != 1 {
		t.Fatalf("expected stopped once, got %d", stopped)
	}
	if len(newLeaderCalls) != 2 || newLeaderCalls[1] != "b:2" {
		t.Fatalf("expected b:2, got %v", newLeaderCalls)
	}
}

func TestPostgresStoreAcquireAndFallback(t *testing.T) {
	q := newFakeQuerier()
	t0 := time.Now().UTC().Round(time.Second)
	q.rowMap[leaderelection.AcquireSQL] = []fakeRow{{values: []any{"a", int64(1), t0}}}
	q.rowMap[leaderelection.CurrentSQL] = []fakeRow{{values: []any{"a", int64(1), t0}}}
	q.rowMap[leaderelection.ReleaseSQL] = []fakeRow{{values: []any{"jobs"}}}

	s := leaderelection.PostgresStore{DB: q}
	ctx := context.Background()

	// Success: acquire.
	rec, ok, err := s.TryAcquire(ctx, "jobs", "a", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire failed: %v %v", ok, err)
	}
	if rec != (leaderelection.Record{Holder: "a", Term: 1, ExpiresAt: t0}) {
		t.Fatalf("unexpected record %+v", rec)
	}
	if len(q.queries) != 1 {
		t.Fatalf("expected 1 query, got %d", len(q.queries))
	}
	call := q.queries[0]
	if call.query != leaderelection.AcquireSQL {
		t.Fatalf("query mismatch: %q", call.query)
	}
	if len(call.args) != 3 || call.args[0] != "jobs" || call.args[1] != "a" || call.args[2] != int64(10000) {
		t.Fatalf("args mismatch: %v", call.args)
	}

	// ErrNoRows on Acquire -> CurrentSQL fallback, acquired=false.
	q2 := newFakeQuerier()
	q2.rowMap[leaderelection.AcquireSQL] = []fakeRow{{scanErr: sql.ErrNoRows}}
	q2.rowMap[leaderelection.CurrentSQL] = []fakeRow{{values: []any{"b", int64(2), t0}}}
	s2 := leaderelection.PostgresStore{DB: q2}
	rec, ok, err = s2.TryAcquire(ctx, "jobs", "a", 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false")
	}
	if rec.Holder != "b" || rec.Term != 2 {
		t.Fatalf("unexpected record %+v", rec)
	}
	if len(q2.queries) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(q2.queries))
	}
	if q2.queries[1].query != leaderelection.CurrentSQL || len(q2.queries[1].args) != 1 || q2.queries[1].args[0] != "jobs" {
		t.Fatalf("fallback query mismatch: %+v", q2.queries[1])
	}

	// Release uses ReleaseSQL and ignores ErrNoRows.
	q3 := newFakeQuerier()
	q3.rowMap[leaderelection.ReleaseSQL] = []fakeRow{{scanErr: sql.ErrNoRows}}
	s3 := leaderelection.PostgresStore{DB: q3}
	if err := s3.Release(ctx, "jobs", "a"); err != nil {
		t.Fatalf("release should ignore no rows: %v", err)
	}
}

func TestRedisStoreAcquireAndRelease(t *testing.T) {
	ev := newFakeEvaler()
	ev.results = append(ev.results,
		evalResult{result: []interface{}{"a", int64(1), int64(1), int64(10000)}},
		evalResult{result: int64(1)},
	)
	s := leaderelection.RedisStore{Client: ev, Prefix: "le:"}
	ctx := context.Background()

	rec, ok, err := s.TryAcquire(ctx, "jobs", "a", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire failed: %v %v", ok, err)
	}
	if rec.Holder != "a" || rec.Term != 1 {
		t.Fatalf("unexpected record %+v", rec)
	}
	// Check eval call.
	if len(ev.evalCalls) != 1 {
		t.Fatalf("expected 1 eval, got %d", len(ev.evalCalls))
	}
	call := ev.evalCalls[0]
	if call.script != leaderelection.AcquireScript {
		t.Fatal("script mismatch")
	}
	if len(call.keys) != 2 || call.keys[0] != "le:jobs" || call.keys[1] != "le:jobs:term" {
		t.Fatalf("keys mismatch: %v", call.keys)
	}
	if len(call.args) != 2 || call.args[0] != "a" || call.args[1] != int64(10000) {
		t.Fatalf("args mismatch: %v", call.args)
	}

	if err := s.Release(ctx, "jobs", "a"); err != nil {
		t.Fatal(err)
	}
	if len(ev.evalCalls) != 2 {
		t.Fatalf("expected 2 evals, got %d", len(ev.evalCalls))
	}
	relCall := ev.evalCalls[1]
	if relCall.script != leaderelection.ReleaseScript || len(relCall.keys) != 1 || relCall.keys[0] != "le:jobs" || len(relCall.args) != 1 || relCall.args[0] != "a" {
		t.Fatalf("release call mismatch: %+v", relCall)
	}
}

func TestNewElectorValidation(t *testing.T) {
	tests := []struct {
		name     string
		cfg      leaderelection.Config
		wantCode string
	}{
		{
			name:     "nil store",
			cfg:      leaderelection.Config{Name: "x", Identity: "a"},
			wantCode: "invalid_config",
		},
		{
			name:     "empty identity",
			cfg:      leaderelection.Config{Store: leaderelection.NewMemoryStore(nil), Name: "x"},
			wantCode: "invalid_config",
		},
		{
			name: "lease <= renew",
			cfg: leaderelection.Config{
				Store: leaderelection.NewMemoryStore(nil), Name: "x", Identity: "a",
				LeaseDuration: 5 * time.Second, RenewInterval: 5 * time.Second,
			},
			wantCode: "invalid_config",
		},
		{
			name: "retry <= 0",
			cfg: leaderelection.Config{
				Store: leaderelection.NewMemoryStore(nil), Name: "x", Identity: "a",
				LeaseDuration: 15 * time.Second, RenewInterval: 5 * time.Second,
				RetryInterval: 0,
			},
			wantCode: "invalid_config",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := leaderelection.NewElector(tt.cfg)
			if err == nil {
				t.Fatal("expected error")
			}
			var le *leaderelection.Error
			if !errors.As(err, &le) {
				t.Fatalf("expected *leaderelection.Error, got %T", err)
			}
			if le.Code != tt.wantCode {
				t.Fatalf("expected code %q, got %q", tt.wantCode, le.Code)
			}
		})
	}
}

func TestRunReturnsCtxErrAndReleases(t *testing.T) {
	t0 := time.Now()
	store := leaderelection.NewMemoryStore(func() time.Time { return t0 })
	var stopped int
	// A store that records Release calls.
	var releaseCalled bool
	customStore := &recordReleaseStore{
		Store:     store,
		onRelease: func() { releaseCalled = true },
	}

	// The injected After fires immediately so Run acquires leadership before
	// the context is cancelled, exercising the release-on-exit path.
	immediate := func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time)
		close(ch)
		return ch
	}

	e, _ := leaderelection.NewElector(leaderelection.Config{
		Store: customStore, Name: "jobs", Identity: "a",
		LeaseDuration: 15 * time.Second, RenewInterval: 5 * time.Second, RetryInterval: 2 * time.Second,
		Now:              func() time.Time { return t0 },
		After:            immediate,
		OnStoppedLeading: func() { stopped++ },
	})
	ctx, cancel := context.WithCancel(context.Background())
	// Run in goroutine and cancel after a short delay.
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if !releaseCalled {
		t.Fatal("expected release to be called")
	}
	if stopped != 1 {
		t.Fatalf("expected one stopped, got %d", stopped)
	}
	if e.IsLeader() {
		t.Fatal("should not be leader after Run exit")
	}
}

// recordReleaseStore wraps Store to observe Release calls.
type recordReleaseStore struct {
	leaderelection.Store
	onRelease func()
}

func (r *recordReleaseStore) Release(ctx context.Context, name, candidate string) error {
	r.onRelease()
	return r.Store.Release(ctx, name, candidate)
}

func ExampleMemoryStore() {
	now := time.Now()
	store := leaderelection.NewMemoryStore(func() time.Time { return now })
	rec, ok, _ := store.TryAcquire(context.Background(), "jobs", "a", time.Second)
	fmt.Println(ok, rec.Holder, rec.Term)
	// Output:
	// true a 1
}
