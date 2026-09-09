package scheduler_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/scheduler"
	"github.com/Sharvary-HH/queued/internal/testutil"
)

func newStore(t *testing.T) (*queue.Store, *pgxpool.Pool) {
	t.Helper()
	pool := testutil.DB(t)
	return queue.NewStore(pool), pool
}

func mustUpsert(t *testing.T, s *queue.Store, name, expr, kind string) queue.RecurringJob {
	t.Helper()

	next, err := scheduler.NextRun(expr, time.Now().UTC())
	if err != nil {
		t.Fatalf("next run for %q: %v", expr, err)
	}
	job, err := s.UpsertRecurring(context.Background(), queue.RecurringParams{
		Name: name, Cron: expr, Kind: kind, Enabled: true, NextRunAt: next,
	})
	if err != nil {
		t.Fatalf("upsert recurring: %v", err)
	}
	return job
}

// startSchedulers runs n schedulers against the same database and stops them
// all when the test ends.
func startSchedulers(t *testing.T, store *queue.Store, n int, interval time.Duration) (stop func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	for range n {
		logs := testutil.NewLogCapture()
		s := scheduler.New(store, logs.Logger(), scheduler.Config{Interval: interval})
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Run(ctx)
		}()
	}

	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("schedulers did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

// The headline test: three instances, one database, one job per tick.
//
// A per-second schedule rather than the per-minute one the brief suggests, so
// the test observes ten ticks in ten seconds instead of one tick in sixty. The
// mechanism is identical — the scheduler never looks at the granularity, only
// at whether next_run_at has passed.
func TestThreeSchedulersProduceOneJobPerTick(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, pool := newStore(t)

	mustUpsert(t, store, "heartbeat", "* * * * * *", "noop") // every second

	startSchedulers(t, store, 3, 100*time.Millisecond)

	// Watch for a while, then check what the three of them produced between
	// them.
	const watch = 6 * time.Second
	time.Sleep(watch)

	var total, distinctKeys int
	err := pool.QueryRow(ctx, `
		SELECT count(*), count(DISTINCT idempotency_key) FROM jobs`).Scan(&total, &distinctKeys)
	if err != nil {
		t.Fatal(err)
	}

	if total == 0 {
		t.Fatal("three schedulers produced no jobs at all in 6 seconds of a per-second schedule")
	}
	// One row per tick: if any tick had fired twice there would be more rows
	// than distinct keys, and if the key were per-process there would be
	// duplicates of the same instant under different keys.
	if total != distinctKeys {
		t.Errorf("%d jobs but only %d distinct scheduled instants: a tick fired more than once",
			total, distinctKeys)
	}

	// Roughly one per second. Loose bounds on purpose — this asserts "not three
	// times as many", not the accuracy of a sleep under a loaded test binary.
	if total < 3 || total > 12 {
		t.Errorf("%d jobs in %s of a per-second schedule; expected roughly %d",
			total, watch, int(watch.Seconds()))
	}
	t.Logf("three schedulers produced %d jobs over %s, all distinct instants", total, watch)
}

// Exactly one instance may hold the advisory lock. This is the election itself,
// separate from whether the data would survive without it.
func TestOnlyOneSchedulerLeadsAtATime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, pool := newStore(t)

	mustUpsert(t, store, "heartbeat", "* * * * * *", "noop")

	var logs []*testutil.LogCapture
	schedulerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for range 3 {
		capture := testutil.NewLogCapture()
		logs = append(logs, capture)
		s := scheduler.New(store, capture.Logger(), scheduler.Config{Interval: 100 * time.Millisecond})
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Run(schedulerCtx)
		}()
	}

	time.Sleep(2 * time.Second)

	var leaders int
	for _, capture := range logs {
		if capture.Contains("became scheduler leader") {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("%d of 3 instances became leader, want exactly 1", leaders)
	}

	// Confirm at the source rather than trusting the logs.
	//
	// Scoped to this test's own database, because pg_locks reports the whole
	// cluster while advisory locks are per-database: the other scheduler tests
	// running in parallel each hold their own lock on the same id, and counting
	// those makes this look broken when it is not.
	var locks int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_locks
		WHERE locktype = 'advisory'
		  AND objid = $1
		  AND granted
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`,
		uint32(scheduler.LockID)).Scan(&locks); err != nil {
		t.Fatal(err)
	}
	if locks != 1 {
		t.Errorf("%d granted advisory locks on the scheduler id, want 1", locks)
	}

	cancel()
	wg.Wait()
}

// Leadership is an optimisation. With the election bypassed entirely — every
// instance scheduling at once, all the time — the compare-and-swap must still
// produce exactly one job per tick. If this fails, the election was carrying
// the guarantee, which is precisely what it must not be doing.
func TestNoDuplicatesWithoutLeadership(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, pool := newStore(t)

	entry := mustUpsert(t, store, "contended", "* * * * * *", "noop")

	// Drive AdvanceAndEnqueue directly from many goroutines with the same
	// expected next_run_at: every one of them believes the tick is theirs.
	const contenders = 24
	next := entry.NextRunAt.Add(time.Second)
	key := fmt.Sprintf("cron:%s:%d", entry.Name, entry.NextRunAt.UTC().Unix())

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		fired int
		errs  []error
	)
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := store.AdvanceAndEnqueue(ctx, entry.ID, entry.NextRunAt, next, key)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			if ok {
				fired++
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("concurrent advance: %v", errs[0])
	}
	if fired != 1 {
		t.Errorf("%d of %d contenders believed they scheduled the tick, want exactly 1", fired, contenders)
	}

	var jobs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Errorf("%d jobs from one tick with %d contenders, want 1", jobs, contenders)
	}
}

// A leader that goes away must be replaced without anyone intervening. This is
// the failure the advisory lock handles well: the lock is released by the
// server when the session ends, with no lease to wait out.
func TestLeadershipPassesOnWhenTheLeaderStops(t *testing.T) {
	t.Parallel()
	store, _ := newStore(t)

	mustUpsert(t, store, "heartbeat", "* * * * * *", "noop")

	firstLogs := testutil.NewLogCapture()
	firstCtx, stopFirst := context.WithCancel(context.Background())
	first := scheduler.New(store, firstLogs.Logger(), scheduler.Config{Interval: 100 * time.Millisecond})

	firstDone := make(chan struct{})
	go func() { defer close(firstDone); first.Run(firstCtx) }()

	waitUntil(t, 10*time.Second, "the first instance to lead", func() bool {
		return firstLogs.Contains("became scheduler leader")
	})

	// A second instance is running but must not be leading.
	secondLogs := testutil.NewLogCapture()
	secondCtx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	second := scheduler.New(store, secondLogs.Logger(), scheduler.Config{Interval: 100 * time.Millisecond})

	secondDone := make(chan struct{})
	go func() { defer close(secondDone); second.Run(secondCtx) }()

	time.Sleep(time.Second)
	if secondLogs.Contains("became scheduler leader") {
		t.Fatal("both instances led at the same time")
	}

	stopFirst()
	<-firstDone

	waitUntil(t, 10*time.Second, "the second instance to take over", func() bool {
		return secondLogs.Contains("became scheduler leader")
	})

	stopSecond()
	<-secondDone
}

func TestParseRejectsNonsense(t *testing.T) {
	t.Parallel()

	for _, expr := range []string{"", "not a cron", "* * *", "99 * * * *", "@yearlyish"} {
		if _, err := scheduler.Parse(expr); err == nil {
			t.Errorf("Parse(%q) accepted a bad expression", expr)
		}
	}
	for _, expr := range []string{"* * * * *", "* * * * * *", "@hourly", "0 3 * * *", "*/5 * * * *"} {
		if _, err := scheduler.Parse(expr); err != nil {
			t.Errorf("Parse(%q): %v", expr, err)
		}
	}
}

// A schedule that has been off for a long time must come back with one run due,
// not with every tick it missed while it was away.
func TestMissedTicksAreDroppedNotReplayed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, pool := newStore(t)

	mustUpsert(t, store, "hourly-ish", "* * * * * *", "noop")

	// Pretend the scheduler was down for an hour.
	if _, err := pool.Exec(ctx,
		`UPDATE recurring_jobs SET next_run_at = now() - interval '1 hour'`); err != nil {
		t.Fatal(err)
	}

	startSchedulers(t, store, 1, 100*time.Millisecond)
	time.Sleep(2 * time.Second)

	var jobs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	// A per-second schedule an hour behind would owe 3600 runs if it caught up.
	// It should have carried on from now instead: a couple of seconds' worth.
	if jobs > 10 {
		t.Errorf("%d jobs after an hour of downtime; missed ticks are being replayed", jobs)
	}
	if jobs == 0 {
		t.Error("no jobs at all after the schedule came back")
	}
	t.Logf("an hour of missed per-second ticks produced %d jobs, not 3600", jobs)
}

func TestDisabledSchedulesDoNotFire(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, pool := newStore(t)

	entry := mustUpsert(t, store, "paused", "* * * * * *", "noop")
	if _, err := store.SetRecurringEnabled(ctx, entry.ID, false, time.Now()); err != nil {
		t.Fatal(err)
	}

	startSchedulers(t, store, 1, 100*time.Millisecond)
	time.Sleep(2 * time.Second)

	var jobs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 {
		t.Errorf("a disabled schedule produced %d jobs", jobs)
	}

	// And re-enabling starts it again, from now.
	back, err := store.SetRecurringEnabled(ctx, entry.ID, true, time.Now().Add(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if !back.Enabled {
		t.Fatal("re-enabling did not stick")
	}

	waitUntil(t, 10*time.Second, "the re-enabled schedule to fire", func() bool {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	})
}

func waitUntil(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}
