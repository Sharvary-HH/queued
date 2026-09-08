package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/testutil"
	"github.com/Sharvary-HH/queued/internal/worker"
	"github.com/Sharvary-HH/queued/testdata/handlers"
)

type harness struct {
	store *queue.Store
	pool  *pgxpool.Pool
	set   *handlers.Set
	reg   *worker.Registry
	log   *slog.Logger
	logs  *testutil.LogCapture
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	pool := testutil.DB(t)
	set := handlers.NewSet()
	reg := worker.NewRegistry()
	set.Register(reg)
	logs := testutil.NewLogCapture()

	return &harness{
		store: queue.NewStore(pool),
		pool:  pool,
		set:   set,
		reg:   reg,
		log:   logs.Logger(),
		logs:  logs,
	}
}

// start runs a pool in the background and returns a stop function that cancels
// it and waits for Run to return, so no test leaks a pool into the next one.
func (h *harness) start(t *testing.T, cfg worker.Config) (stop func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	p := worker.New(h.store, h.reg, h.log, cfg)

	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("pool returned: %v", err)
			}
		case <-time.After(60 * time.Second):
			t.Error("pool did not shut down within 60s")
		}
	}
	t.Cleanup(stop)
	return stop
}

func (h *harness) enqueue(t *testing.T, p queue.EnqueueParams) queue.Job {
	t.Helper()
	job, _, err := h.store.Enqueue(context.Background(), p)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return job
}

func (h *harness) countByState(t *testing.T) map[queue.State]int {
	t.Helper()

	rows, err := h.pool.Query(context.Background(), `SELECT state, count(*) FROM jobs GROUP BY state`)
	if err != nil {
		t.Fatalf("count by state: %v", err)
	}
	defer rows.Close()

	out := make(map[queue.State]int)
	for rows.Next() {
		var s queue.State
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[s] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("count by state: %v", err)
	}
	return out
}

// waitFor polls until cond holds or the deadline passes. Polling rather than
// synchronising on the pool's internals, because the tests should only depend
// on what an operator could see.
func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}

func payload(t *testing.T, p handlers.Payload) []byte {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func fastConfig(concurrency int) worker.Config {
	return worker.Config{
		Queue:        "default",
		WorkerID:     "test-worker",
		Concurrency:  concurrency,
		ClaimBatch:   10,
		PollInterval: 50 * time.Millisecond,
		DrainTimeout: 10 * time.Second,
		// Retries land almost immediately so tests do not sit through a real
		// backoff ladder. The ladder itself is covered in backoff_test.go.
		Backoff: worker.Backoff{Base: time.Millisecond, Max: 20 * time.Millisecond},
	}
}

func TestPoolRunsJobsToCompletion(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	const total = 200
	batch := make([]queue.EnqueueParams, total)
	for i := range batch {
		batch[i] = queue.EnqueueParams{Kind: handlers.KindSucceed}
	}
	if _, err := h.store.EnqueueMany(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	h.start(t, fastConfig(8))

	waitFor(t, 30*time.Second, "all jobs to succeed", func() bool {
		return h.countByState(t)[queue.StateSucceeded] == total
	})

	if runs := h.set.Runs(handlers.KindSucceed); runs != total {
		t.Errorf("handler ran %d times, want exactly %d", runs, total)
	}
	if counts := h.countByState(t); counts[queue.StateClaimed] != 0 {
		t.Errorf("%d jobs left in claimed", counts[queue.StateClaimed])
	}
}

// A panicking handler must fail its own job and nothing else. The worker has to
// still be there afterwards to run the next one.
func TestPanicIsolation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	one := 1
	h.enqueue(t, queue.EnqueueParams{
		Kind:        handlers.KindPanic,
		Payload:     payload(t, handlers.Payload{ID: "the-panicking-one"}),
		MaxAttempts: &one,
	})

	// Concurrency 1 so the same goroutine that took the panic has to be the one
	// that runs the next job. With more slots a surviving sibling could hide a
	// dead goroutine.
	h.start(t, fastConfig(1))

	waitFor(t, 20*time.Second, "the panicking job to die", func() bool {
		return h.countByState(t)[queue.StateDead] == 1
	})

	// The worker is still alive: give it something else and watch it run.
	h.enqueue(t, queue.EnqueueParams{
		Kind:    handlers.KindSucceed,
		Payload: payload(t, handlers.Payload{ID: "the-one-after"}),
	})
	waitFor(t, 20*time.Second, "the next job to run after the panic", func() bool {
		return h.countByState(t)[queue.StateSucceeded] == 1
	})

	// The stack must reach last_error, or whoever has to fix it has nothing.
	var lastError string
	err := h.pool.QueryRow(context.Background(),
		`SELECT last_error FROM jobs WHERE state = 'dead'`).Scan(&lastError)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lastError, "handler panicked") {
		t.Errorf("last_error does not mention the panic: %q", truncate(lastError, 200))
	}
	if !strings.Contains(lastError, "worker.safely") {
		t.Errorf("last_error has no stack trace: %q", truncate(lastError, 200))
	}
}

func TestFlakyJobSucceedsOnRetry(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.set.FlakyFailures = 2

	h.enqueue(t, queue.EnqueueParams{
		Kind:    handlers.KindFlaky,
		Payload: payload(t, handlers.Payload{ID: "flaky-1"}),
	})
	h.start(t, fastConfig(2))

	waitFor(t, 20*time.Second, "the flaky job to eventually succeed", func() bool {
		return h.countByState(t)[queue.StateSucceeded] == 1
	})

	if runs := h.set.Runs(handlers.KindFlaky); runs != 3 {
		t.Errorf("handler ran %d times, want 3 (two failures then a success)", runs)
	}
}

func TestAlwaysFailingJobLandsInTheDLQ(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	maxAttempts := 3
	job := h.enqueue(t, queue.EnqueueParams{
		Kind:        handlers.KindFail,
		Payload:     payload(t, handlers.Payload{ID: "doomed"}),
		MaxAttempts: &maxAttempts,
	})
	h.start(t, fastConfig(2))

	waitFor(t, 20*time.Second, "the job to reach the DLQ", func() bool {
		return h.countByState(t)[queue.StateDead] == 1
	})

	// Exactly max_attempts executions, not one more. Give it a moment to prove
	// it is not still going.
	time.Sleep(500 * time.Millisecond)
	if runs := h.set.Runs(handlers.KindFail); runs != maxAttempts {
		t.Errorf("handler ran %d times, want exactly %d", runs, maxAttempts)
	}

	attempts, err := h.store.AttemptsByJobID(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != maxAttempts {
		t.Errorf("recorded %d attempts, want %d", len(attempts), maxAttempts)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	h.enqueue(t, queue.EnqueueParams{
		Kind:    handlers.KindInvalid,
		Payload: payload(t, handlers.Payload{ID: "malformed"}),
	})
	h.start(t, fastConfig(2))

	waitFor(t, 20*time.Second, "the job to be dead-lettered", func() bool {
		return h.countByState(t)[queue.StateDead] == 1
	})

	time.Sleep(500 * time.Millisecond)
	if runs := h.set.Runs(handlers.KindInvalid); runs != 1 {
		t.Errorf("handler ran %d times, want 1 — a permanent error must not burn the budget", runs)
	}
}

// An unknown kind is retried, not dead-lettered, so a rolling deploy does not
// destroy jobs the new code enqueued before the old workers went away.
func TestUnknownKindIsRetriedNotDeadLettered(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	two := 2
	job := h.enqueue(t, queue.EnqueueParams{Kind: "not-registered-anywhere", MaxAttempts: &two})
	h.start(t, fastConfig(1))

	waitFor(t, 20*time.Second, "the unknown kind to exhaust its retries", func() bool {
		return h.countByState(t)[queue.StateDead] == 1
	})

	// It got its full budget rather than dying on the first look.
	attempts, err := h.store.AttemptsByJobID(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != two {
		t.Errorf("unknown kind was tried %d times, want %d (it should retry, not fail permanently)", len(attempts), two)
	}
}

// Property 3. Jobs that take longer than the signal-to-deadline gap must still
// finish, and nothing may be left sitting in 'claimed'.
func TestGracefulShutdownDrainsInFlightJobs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	const inFlight = 4
	for i := range inFlight {
		h.enqueue(t, queue.EnqueueParams{
			Kind: handlers.KindSlow,
			Payload: payload(t, handlers.Payload{
				ID:    fmt.Sprintf("slow-%d", i),
				Sleep: "3s",
			}),
			VisibilityTimeout: 60 * time.Second,
		})
	}

	cfg := fastConfig(inFlight)
	cfg.DrainTimeout = 20 * time.Second // comfortably longer than the 3s handlers
	stop := h.start(t, cfg)

	// Let them all get going, then pull the rug.
	waitFor(t, 10*time.Second, "all slow jobs to start", func() bool {
		return h.set.Runs(handlers.KindSlow) == inFlight
	})
	time.Sleep(300 * time.Millisecond)

	shutdownStart := time.Now()
	stop() // equivalent to SIGTERM: this is the context signal.NotifyContext cancels
	elapsed := time.Since(shutdownStart)

	counts := h.countByState(t)
	if counts[queue.StateSucceeded] != inFlight {
		t.Errorf("%d jobs succeeded, want %d — in-flight work was dropped", counts[queue.StateSucceeded], inFlight)
	}
	if counts[queue.StateClaimed] != 0 {
		t.Errorf("%d jobs left in claimed after shutdown", counts[queue.StateClaimed])
	}
	// It waited for the work rather than exiting instantly.
	if elapsed < time.Second {
		t.Errorf("shutdown took only %s; it cannot have waited for 3s handlers", elapsed)
	}

	if !h.logs.Contains("shutdown: claimer stopped") {
		t.Error("shutdown sequence was not logged")
	}
	if !h.logs.Contains("shutdown: all in-flight jobs finished") {
		t.Error("clean drain was not logged")
	}
}

// A handler that outlasts the drain deadline gets cancelled, and its job goes
// straight back to pending rather than waiting out a visibility timeout.
func TestShutdownReleasesJobsStuckPastTheDeadline(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	h.enqueue(t, queue.EnqueueParams{
		Kind:    handlers.KindSlow,
		Payload: payload(t, handlers.Payload{ID: "very-slow", Sleep: "60s"}),
		// Long enough that the reaper could not be what rescues this job.
		VisibilityTimeout: 10 * time.Minute,
	})

	cfg := fastConfig(1)
	cfg.DrainTimeout = 500 * time.Millisecond
	stop := h.start(t, cfg)

	waitFor(t, 10*time.Second, "the slow job to start", func() bool {
		return h.set.Runs(handlers.KindSlow) == 1
	})

	stop()

	counts := h.countByState(t)
	if counts[queue.StatePending] != 1 {
		t.Fatalf("states after shutdown: %v; want the stuck job back in pending", counts)
	}
	if !h.logs.Contains("drain deadline reached") {
		t.Error("the forced release was not logged")
	}

	// And it is genuinely claimable again, with no wait.
	claimed, err := h.store.Claim(context.Background(), "default", "next-worker", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatal("the released job was not immediately claimable")
	}
}

// Nothing may be lost between the claim query and an executor picking the job
// up: a shutdown in that window has to hand the buffered jobs back.
func TestShutdownReleasesClaimedButUnstartedJobs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	const total = 20
	batch := make([]queue.EnqueueParams, total)
	for i := range batch {
		batch[i] = queue.EnqueueParams{
			Kind:    handlers.KindSlow,
			Payload: payload(t, handlers.Payload{ID: fmt.Sprintf("s-%d", i), Sleep: "2s"}),
		}
	}
	if _, err := h.store.EnqueueMany(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	// One executor, batches of ten: the claimer fills the buffer while the
	// single slot is busy, so there are always jobs claimed and waiting.
	cfg := fastConfig(1)
	cfg.ClaimBatch = 10
	cfg.DrainTimeout = 10 * time.Second
	stop := h.start(t, cfg)

	waitFor(t, 10*time.Second, "jobs to be claimed", func() bool {
		return h.countByState(t)[queue.StateClaimed] > 1
	})

	stop()

	counts := h.countByState(t)
	if counts[queue.StateClaimed] != 0 {
		t.Errorf("%d jobs stranded in claimed after shutdown", counts[queue.StateClaimed])
	}
	if got := counts[queue.StatePending] + counts[queue.StateSucceeded]; got != total {
		t.Errorf("only %d of %d jobs accounted for: %v", got, total, counts)
	}
}

// NOTIFY should get a job picked up well inside one poll interval.
func TestNotifyWakesTheClaimerBeforeThePollInterval(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	cfg := fastConfig(1)
	cfg.PollInterval = 30 * time.Second // so polling cannot be what finds the job
	h.start(t, cfg)

	// Wait for LISTEN to be established, otherwise the notification races the
	// subscription and the test would be measuring the poll timer.
	waitFor(t, 10*time.Second, "the listener to subscribe", func() bool {
		return h.logs.Contains("listening for enqueue notifications")
	})

	start := time.Now()
	h.enqueue(t, queue.EnqueueParams{Kind: handlers.KindSucceed})

	waitFor(t, 10*time.Second, "the notified job to run", func() bool {
		return h.countByState(t)[queue.StateSucceeded] == 1
	})

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("job took %s to be picked up with a 30s poll interval; NOTIFY is not waking the claimer", elapsed)
	}
}

func TestPoolRefusesToStartWithNoHandlers(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	p := worker.New(h.store, worker.NewRegistry(), h.log, fastConfig(1))
	if err := p.Run(context.Background()); err == nil {
		t.Error("a pool with no handlers started anyway; it would claim jobs and fail every one")
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
