package worker_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/worker"
	"github.com/Sharvary-HH/queued/testdata/handlers"
)

// This file is the README's five correctness properties, one test each, run
// against a real Postgres. If any of them fails, a claim the project makes on
// its front page is false.
//
// The tests elsewhere in this package are finer-grained and check pieces of
// mechanism. These check the promises.

// Property 1: no double execution under concurrency.
//
// A thousand jobs, twenty independent worker pools with their own worker IDs,
// their own claimers and their own connections — as close to twenty separate
// worker processes as one test binary can get.
//
// Three things are asserted, because "it worked" can be true by accident:
//
//   - every job ran, so nothing was quietly dropped;
//   - the handler was entered exactly 1000 times, counted in Go, independent of
//     anything the database says;
//   - job_attempts contains no duplicate (job_id, attempt) pair, so no two
//     workers ever believed they were on the same try of the same job.
//
// The third is the interesting one. Two workers claiming the same row would get
// *different* attempt numbers, since the counter moves at claim time — so this
// check is not what catches double claiming. It catches the other thing: two
// executions recorded against one attempt, which is what a retry bug or a
// mishandled release would produce.
func TestProperty1_NoDoubleExecutionUnderConcurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newHarness(t)

	const (
		total = 1000
		pools = 20
	)

	batch := make([]queue.EnqueueParams, total)
	for i := range batch {
		batch[i] = queue.EnqueueParams{Kind: handlers.KindSucceed}
	}
	if _, err := h.store.EnqueueMany(ctx, batch); err != nil {
		t.Fatal(err)
	}

	// Twenty pools sharing the handler set, so the execution count is global.
	var wg sync.WaitGroup
	poolCtx, cancelPools := context.WithCancel(ctx)
	defer cancelPools()

	for i := range pools {
		cfg := fastConfig(2)
		cfg.WorkerID = fmt.Sprintf("worker-%02d", i)
		p := worker.New(h.store, h.reg, h.log, cfg)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Run(poolCtx); err != nil {
				t.Errorf("%s: %v", cfg.WorkerID, err)
			}
		}()
	}

	waitFor(t, 90*time.Second, "all 1000 jobs to succeed", func() bool {
		return h.countByState(t)[queue.StateSucceeded] == total
	})
	cancelPools()
	wg.Wait()

	if runs := h.set.Runs(handlers.KindSucceed); runs != total {
		t.Errorf("handler was entered %d times, want exactly %d", runs, total)
	}

	var duplicates int
	err := h.pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT job_id, attempt FROM job_attempts
			GROUP BY job_id, attempt HAVING count(*) > 1
		) d`).Scan(&duplicates)
	if err != nil {
		t.Fatal(err)
	}
	if duplicates != 0 {
		t.Errorf("%d (job_id, attempt) pairs were executed more than once", duplicates)
	}

	// Each job should have needed exactly one attempt. More would mean a job
	// was reclaimed or retried, which with a handler that cannot fail would
	// itself be a bug.
	var attemptRows int
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM job_attempts`).Scan(&attemptRows); err != nil {
		t.Fatal(err)
	}
	if attemptRows != total {
		t.Errorf("%d attempt rows for %d jobs; something ran twice", attemptRows, total)
	}
}

// Property 2: no lost jobs on worker death.
//
// The dead worker is simulated by claiming directly and never reporting, which
// is precisely what a SIGKILLed process looks like from the database's side.
// There is no difference for the system to detect, which is the point: recovery
// cannot depend on noticing that a worker died.
func TestProperty2_NoLostJobsOnWorkerDeath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newHarness(t)

	job := h.enqueue(t, queue.EnqueueParams{
		Kind:              handlers.KindSucceed,
		VisibilityTimeout: time.Second,
	})

	claimed, err := h.store.Claim(ctx, "default", "worker-that-dies", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d, want 1", len(claimed))
	}

	// Before the timeout lapses the job is nobody else's business.
	other, err := h.store.Claim(ctx, "default", "worker-b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatal("a claimed job was handed to a second worker before its timeout expired")
	}

	h.startReaper(t, worker.ReaperConfig{Interval: 200 * time.Millisecond})
	h.start(t, fastConfig(2))

	waitFor(t, 30*time.Second, "the abandoned job to run on another worker", func() bool {
		return h.countByState(t)[queue.StateSucceeded] == 1
	})

	final, err := h.store.JobByID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != queue.StateSucceeded {
		t.Fatalf("job ended in %q, want succeeded", final.State)
	}

	// Both tries are on the record: the one that vanished and the one that
	// worked. Losing the first would erase the evidence that anything happened.
	attempts, err := h.store.AttemptsByJobID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 {
		t.Fatalf("%d attempt rows, want 2", len(attempts))
	}
	if attempts[0].WorkerID == attempts[1].WorkerID {
		t.Error("both attempts are attributed to the same worker")
	}
}

// Property 4: bounded retries.
//
// A job that can never succeed must stop, and must stop at exactly the number
// it was given — not one attempt more.
func TestProperty4_BoundedRetries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newHarness(t)

	const maxAttempts = 4
	m := maxAttempts
	job := h.enqueue(t, queue.EnqueueParams{
		Kind:        handlers.KindFail,
		Payload:     payload(t, handlers.Payload{ID: "never-works"}),
		MaxAttempts: &m,
	})

	h.start(t, fastConfig(2))

	waitFor(t, 40*time.Second, "the job to reach the dead-letter queue", func() bool {
		return h.countByState(t)[queue.StateDead] == 1
	})

	// Sit still for a while: if anything were going to try again, this is when.
	time.Sleep(2 * time.Second)

	if runs := h.set.Runs(handlers.KindFail); runs != maxAttempts {
		t.Errorf("handler ran %d times, want exactly %d", runs, maxAttempts)
	}

	attempts, err := h.store.AttemptsByJobID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != maxAttempts {
		t.Errorf("%d attempt rows, want exactly %d", len(attempts), maxAttempts)
	}

	final, err := h.store.JobByID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != queue.StateDead {
		t.Errorf("state = %q, want dead", final.State)
	}
	if final.Attempt != maxAttempts {
		t.Errorf("attempt = %d, want %d", final.Attempt, maxAttempts)
	}
	if final.LastError == nil || !strings.Contains(*final.LastError, "always fails") {
		t.Error("the reason it died was not recorded")
	}
}

// Property 5: at-least-once, not exactly-once.
//
// This test exists to prove the *negative*. A handler whose job outlives its
// visibility timeout gets run again while the first execution is still in
// flight, and that is not a bug to be fixed — it is the contract, and the
// reason handlers must be idempotent.
//
// The setup is the honest one: a handler that ignores its context entirely,
// which is what a handler doing blocking I/O in a C library, or simply written
// carelessly, actually looks like.
func TestProperty5_AtLeastOnceIsObservable(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	var (
		mu         sync.Mutex
		executions int
		release    = make(chan struct{})
	)
	// Deliberately does not select on ctx.Done().
	h.reg.MustRegister("ignores-cancellation", func(context.Context, []byte) error {
		mu.Lock()
		executions++
		mu.Unlock()
		<-release
		return nil
	})

	job := h.enqueue(t, queue.EnqueueParams{
		Kind:              "ignores-cancellation",
		VisibilityTimeout: time.Second,
	})

	h.startReaper(t, worker.ReaperConfig{Interval: 200 * time.Millisecond})
	cfg := fastConfig(4)
	cfg.DrainTimeout = time.Second
	h.start(t, cfg)

	waitFor(t, 30*time.Second, "the same job to be executing twice at once", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return executions >= 2
	})

	mu.Lock()
	got := executions
	mu.Unlock()
	t.Logf("job %d was executing %d times concurrently — this is the contract, "+
		"which is why handlers must be idempotent", job.ID, got)

	close(release)
}

// Property 7 of the phase list: an idempotency key deduplicates at enqueue.
func TestPropertyIdempotencyKeyCreatesOneJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newHarness(t)

	key := "invoice-2024-11-0042"
	for range 5 {
		if _, _, err := h.store.Enqueue(ctx, queue.EnqueueParams{
			Kind:           handlers.KindSucceed,
			IdempotencyKey: &key,
		}); err != nil {
			t.Fatal(err)
		}
	}

	var rows int
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("%d rows after five enqueues with one key, want 1", rows)
	}

	h.start(t, fastConfig(2))
	waitFor(t, 20*time.Second, "the single job to run", func() bool {
		return h.countByState(t)[queue.StateSucceeded] == 1
	})

	if runs := h.set.Runs(handlers.KindSucceed); runs != 1 {
		t.Errorf("handler ran %d times, want 1", runs)
	}
}
