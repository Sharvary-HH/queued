package queue_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/testutil"
)

func newStore(t *testing.T) (*queue.Store, *pgxpool.Pool) {
	t.Helper()
	pool := testutil.DB(t)
	return queue.NewStore(pool), pool
}

func seed(t *testing.T, s *queue.Store, n int, kind string) {
	t.Helper()
	batch := make([]queue.EnqueueParams, n)
	for i := range batch {
		batch[i] = queue.EnqueueParams{Kind: kind}
	}
	if _, err := s.EnqueueMany(context.Background(), batch); err != nil {
		t.Fatalf("seed %d jobs: %v", n, err)
	}
}

// TestClaimHandsOutDisjointSets is property 1 at the SQL level: whatever the
// interleaving, no two claims ever return the same job id.
//
// This is the version without a worker pool in the way, so a failure points at
// the query rather than at the pool. The end-to-end version lives in phase 5.
func TestClaimHandsOutDisjointSets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	const (
		jobs    = 1000
		workers = 20
		batch   = 10
	)
	seed(t, store, jobs, "noop")

	var (
		mu    sync.Mutex
		seen  = make(map[int64]string, jobs)
		dupes []string
		wg    sync.WaitGroup
	)

	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerID := fmt.Sprintf("worker-%d", w)
			for {
				claimed, err := store.Claim(ctx, "default", workerID, batch)
				if err != nil {
					t.Errorf("%s: claim: %v", workerID, err)
					return
				}
				if len(claimed) == 0 {
					return // drained
				}
				mu.Lock()
				for _, j := range claimed {
					if prev, ok := seen[j.ID]; ok {
						dupes = append(dupes, fmt.Sprintf("job %d claimed by %s and %s", j.ID, prev, workerID))
					}
					seen[j.ID] = workerID
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(dupes) > 0 {
		t.Fatalf("jobs claimed more than once: %v", dupes)
	}
	if len(seen) != jobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), jobs)
	}
}

// A job claimed once must not be claimable again until it is released, even
// though the second claim runs after the first transaction has committed.
func TestClaimSkipsAlreadyClaimed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)
	seed(t, store, 1, "noop")

	first, err := store.Claim(ctx, "default", "worker-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(first))
	}

	second, err := store.Claim(ctx, "default", "worker-b", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("claimed %d jobs from an empty queue, want 0", len(second))
	}
}

func TestClaimIncrementsAttemptAndRecordsHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)
	seed(t, store, 1, "noop")

	claimed, err := store.Claim(ctx, "default", "worker-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	job := claimed[0]

	if job.Attempt != 1 {
		t.Errorf("attempt = %d after first claim, want 1", job.Attempt)
	}
	if job.State != queue.StateClaimed {
		t.Errorf("state = %q, want claimed", job.State)
	}
	if job.ClaimExpiresAt == nil {
		t.Fatal("claim_expires_at is null on a claimed job")
	}
	// The default visibility timeout is 60s; allow slack for clock and round trip.
	if d := time.Until(*job.ClaimExpiresAt); d < 55*time.Second || d > 65*time.Second {
		t.Errorf("claim expires in %s, want about 60s", d)
	}

	// The attempt row must exist already, so a worker that dies right now still
	// leaves evidence of who took the job.
	attempts, err := store.AttemptsByJobID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 {
		t.Fatalf("got %d attempt rows, want 1", len(attempts))
	}
	if attempts[0].WorkerID != "worker-a" {
		t.Errorf("attempt worker = %q, want worker-a", attempts[0].WorkerID)
	}
	if attempts[0].FinishedAt != nil {
		t.Error("attempt is already finished immediately after the claim")
	}
}

func TestClaimRespectsPriorityThenRunAt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	low, high := 200, 10
	mustEnqueue(t, store, queue.EnqueueParams{Kind: "noop", Priority: &low})
	mustEnqueue(t, store, queue.EnqueueParams{Kind: "noop", Priority: &high})

	claimed, err := store.Claim(ctx, "default", "worker-a", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d, want 2", len(claimed))
	}
	if claimed[0].Priority != high {
		t.Errorf("first claimed job has priority %d, want %d (lower runs first)", claimed[0].Priority, high)
	}
}

func TestClaimIgnoresFutureAndOtherQueues(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	mustEnqueue(t, store, queue.EnqueueParams{Kind: "noop", RunAt: time.Now().Add(time.Hour)})
	mustEnqueue(t, store, queue.EnqueueParams{Kind: "noop", Queue: "other"})

	claimed, err := store.Claim(ctx, "default", "worker-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed %d jobs, want 0 (one is delayed, one is on another queue)", len(claimed))
	}
}

func TestCompleteClosesTheAttempt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)
	seed(t, store, 1, "noop")

	claimed := mustClaimOne(t, store, "worker-a")
	if err := store.Complete(ctx, claimed.ID, "worker-a"); err != nil {
		t.Fatal(err)
	}

	job, err := store.JobByID(ctx, claimed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != queue.StateSucceeded {
		t.Errorf("state = %q, want succeeded", job.State)
	}

	attempts, err := store.AttemptsByJobID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if attempts[0].FinishedAt == nil {
		t.Error("attempt was never closed")
	}
}

// A worker whose job was reaped out from under it must not be able to report on
// it afterwards. Otherwise a slow worker could mark a job succeeded that a
// second worker is still running, or overwrite the second worker's result.
func TestCompleteAndFailRejectStaleClaims(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)
	seed(t, store, 1, "noop")

	id := mustClaimOne(t, store, "worker-a").ID

	if err := store.Complete(ctx, id, "worker-b"); err == nil {
		t.Error("worker-b completed a job held by worker-a")
	}
	if _, err := store.Fail(ctx, queue.FailRequest{
		JobID: id, WorkerID: "worker-b", Err: "boom", RetryAt: time.Now(),
	}); err == nil {
		t.Error("worker-b failed a job held by worker-a")
	}
}

func TestFailSchedulesRetryThenGivesUp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	maxAttempts := 3
	job := mustEnqueue(t, store, queue.EnqueueParams{Kind: "always-fails", MaxAttempts: &maxAttempts})

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		claimed, err := store.Claim(ctx, "default", "worker-a", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) != 1 {
			t.Fatalf("attempt %d: claimed %d jobs, want 1", attempt, len(claimed))
		}
		if claimed[0].Attempt != attempt {
			t.Fatalf("attempt counter = %d, want %d", claimed[0].Attempt, attempt)
		}

		// RetryAt in the past so the next claim can pick it straight back up.
		state, err := store.Fail(ctx, queue.FailRequest{
			JobID:    job.ID,
			WorkerID: "worker-a",
			Err:      fmt.Sprintf("failure %d", attempt),
			RetryAt:  time.Now().Add(-time.Second),
		})
		if err != nil {
			t.Fatal(err)
		}

		want := queue.StatePending
		if attempt == maxAttempts {
			want = queue.StateDead
		}
		if state != want {
			t.Fatalf("after attempt %d of %d: state = %q, want %q", attempt, maxAttempts, state, want)
		}
	}

	// Bounded: the DLQ job must not be claimable again.
	claimed, err := store.Claim(ctx, "default", "worker-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("a dead job was claimed again (attempt %d)", claimed[0].Attempt)
	}

	attempts, err := store.AttemptsByJobID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != maxAttempts {
		t.Fatalf("recorded %d attempts, want exactly %d", len(attempts), maxAttempts)
	}
	for _, a := range attempts {
		if a.Error == nil {
			t.Errorf("attempt %d has no error recorded", a.Attempt)
		}
	}
}

func TestFailPermanentSkipsTheRetryBudget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)
	seed(t, store, 1, "bad-payload")

	claimed := mustClaimOne(t, store, "worker-a")
	state, err := store.Fail(ctx, queue.FailRequest{
		JobID:     claimed.ID,
		WorkerID:  "worker-a",
		Err:       "payload is not valid",
		RetryAt:   time.Now(),
		Permanent: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if state != queue.StateDead {
		t.Fatalf("state = %q after a permanent failure on attempt 1 of 5, want dead", state)
	}
}

// Property 2 at the SQL level: a claim nobody reports on is returned to the
// queue once it expires, and a second worker can pick it up.
func TestReapReturnsExpiredClaims(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	// One second so the test can actually wait it out rather than fake the clock.
	job := mustEnqueue(t, store, queue.EnqueueParams{Kind: "noop", VisibilityTimeout: time.Second})

	claimed, err := store.Claim(ctx, "default", "dying-worker", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d, want 1", len(claimed))
	}

	// Nothing is reapable yet.
	reclaimed, err := store.Reap(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 0 {
		t.Fatalf("reaped %d jobs before the timeout expired", len(reclaimed))
	}

	time.Sleep(1100 * time.Millisecond)

	reclaimed, err = store.Reap(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("reaped %d jobs, want 1", len(reclaimed))
	}
	if reclaimed[0].WorkerID != "dying-worker" {
		t.Errorf("reclaimed from %q, want dying-worker", reclaimed[0].WorkerID)
	}
	if reclaimed[0].State != queue.StatePending {
		t.Errorf("reclaimed into %q, want pending", reclaimed[0].State)
	}

	// A different worker must now be able to run it.
	again, err := store.Claim(ctx, "default", "healthy-worker", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].ID != job.ID {
		t.Fatalf("job %d was not picked back up after the reap", job.ID)
	}
	if again[0].Attempt != 2 {
		t.Errorf("attempt = %d on the reclaimed job, want 2 (the dead worker burned one)", again[0].Attempt)
	}
}

// A job that keeps killing its workers must still stop. This is what makes the
// claim-time attempt increment worth the cost: nobody ever reports a failure
// here, and the job still reaches the DLQ.
func TestReapSendsExhaustedJobsToTheDLQ(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	maxAttempts := 2
	job := mustEnqueue(t, store, queue.EnqueueParams{
		Kind:              "poison",
		MaxAttempts:       &maxAttempts,
		VisibilityTimeout: time.Second,
	})

	for range maxAttempts {
		if _, err := store.Claim(ctx, "default", "dying-worker", 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1100 * time.Millisecond)
		if _, err := store.Reap(ctx, 100); err != nil {
			t.Fatal(err)
		}
	}

	final, err := store.JobByID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != queue.StateDead {
		t.Fatalf("state = %q after %d silent worker deaths, want dead", final.State, maxAttempts)
	}
}

func TestReleaseReturnsJobsImmediately(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)
	seed(t, store, 3, "noop")

	claimed, err := store.Claim(ctx, "default", "worker-a", 3)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, len(claimed))
	for i, j := range claimed {
		ids[i] = j.ID
	}

	n, err := store.Release(ctx, ids, "worker-a", "shutting down")
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("released %d jobs, want 3", n)
	}

	// Available again with no wait for the visibility timeout.
	again, err := store.Claim(ctx, "default", "worker-b", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 3 {
		t.Fatalf("claimed %d released jobs, want 3", len(again))
	}
}

func TestEnqueueIdempotencyKeyDedupes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	key := "order-4711-receipt"
	first, inserted, err := store.Enqueue(ctx, queue.EnqueueParams{Kind: "email", IdempotencyKey: &key})
	if err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("first enqueue reported the job already existed")
	}

	second, inserted, err := store.Enqueue(ctx, queue.EnqueueParams{Kind: "email", IdempotencyKey: &key})
	if err != nil {
		t.Fatal(err)
	}
	if inserted {
		t.Fatal("second enqueue with the same key created another job")
	}
	if second.ID != first.ID {
		t.Fatalf("second enqueue returned job %d, want the existing %d", second.ID, first.ID)
	}

	claimed, err := store.Claim(ctx, "default", "worker-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("%d jobs in the queue, want exactly 1", len(claimed))
	}
}

// Same key from several goroutines at once: one row, and every caller gets the
// same id back rather than an error.
func TestEnqueueIdempotencyKeyUnderRace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	key := "concurrent-key"
	const callers = 16

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		ids      = map[int64]int{}
		inserts  int
		firstErr error
	)

	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, inserted, err := store.Enqueue(ctx, queue.EnqueueParams{Kind: "email", IdempotencyKey: &key})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			ids[job.ID]++
			if inserted {
				inserts++
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		t.Fatalf("concurrent enqueue: %v", firstErr)
	}
	if len(ids) != 1 {
		t.Fatalf("callers saw %d distinct job ids, want 1", len(ids))
	}
	if inserts != 1 {
		t.Fatalf("%d callers reported inserting, want exactly 1", inserts)
	}
}

func TestEnqueueRejectsBadInput(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	cases := map[string]queue.EnqueueParams{
		"no kind":           {},
		"invalid json":      {Kind: "noop", Payload: []byte(`{"a":`)},
		"empty dedupe key":  {Kind: "noop", IdempotencyKey: ptr("")},
		"zero max attempts": {Kind: "noop", MaxAttempts: ptr(0)},
	}
	for name, params := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := store.Enqueue(ctx, params); err == nil {
				t.Error("expected an error, got none")
			}
		})
	}
}

// mustClaimOne fails the test loudly when nothing is claimable, instead of
// panicking on claimed[0] and leaving the reason to be guessed from a stack.
func mustClaimOne(t *testing.T, s *queue.Store, workerID string) queue.Job {
	t.Helper()
	claimed, err := s.Claim(context.Background(), "default", workerID, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(claimed))
	}
	return claimed[0]
}

func mustEnqueue(t *testing.T, s *queue.Store, p queue.EnqueueParams) queue.Job {
	t.Helper()
	job, _, err := s.Enqueue(context.Background(), p)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return job
}

func ptr[T any](v T) *T { return &v }

// The throughput query renders the dashboard's chart, and the handler logs a
// warning and draws an empty chart if it fails — so a broken query looks
// exactly like an idle queue and returns 200. It shipped broken once for
// precisely that reason (pgx inferred the interval parameter as text and could
// not encode an int into it), so it gets a test that runs it for real.
func TestThroughputReportsFinishedAttempts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	seed(t, store, 3, "noop")
	claimed, err := store.Claim(ctx, "default", "worker-a", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d, want 3", len(claimed))
	}

	if err := store.Complete(ctx, claimed[0].ID, "worker-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Fail(ctx, queue.FailRequest{
		JobID: claimed[1].ID, WorkerID: "worker-a", Err: "nope", RetryAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	// claimed[2] is left running, so it has no finished_at and must not appear.

	points, err := store.Throughput(ctx, 60)
	if err != nil {
		t.Fatalf("throughput: %v", err)
	}
	if len(points) == 0 {
		t.Fatal("no throughput points for two attempts that just finished")
	}

	var succeeded, failed int64
	for _, p := range points {
		succeeded += p.Succeeded
		failed += p.Failed
	}
	if succeeded != 1 {
		t.Errorf("succeeded = %d, want 1", succeeded)
	}
	if failed != 1 {
		t.Errorf("failed = %d, want 1", failed)
	}
}

func TestStatsCountsByQueueAndState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	seed(t, store, 4, "noop")
	mustEnqueue(t, store, queue.EnqueueParams{Kind: "noop", Queue: "other"})

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]int64{}
	for _, s := range stats {
		got[s.Queue+"/"+string(s.State)] += s.Count
	}
	if got["default/pending"] != 4 {
		t.Errorf("default/pending = %d, want 4", got["default/pending"])
	}
	if got["other/pending"] != 1 {
		t.Errorf("other/pending = %d, want 1", got["other/pending"])
	}
}

func TestCancelOnlyAppliesToPendingJobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)
	seed(t, store, 2, "noop")

	all, _, err := store.ListJobs(ctx, queue.JobFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("listed %d jobs, want 2", len(all))
	}

	cancelled, err := store.Cancel(ctx, all[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.State != queue.StateCancelled {
		t.Errorf("state = %q, want cancelled", cancelled.State)
	}

	// A cancelled job is not claimable.
	claimed, err := store.Claim(ctx, "default", "worker-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d jobs, want 1 (the other was cancelled)", len(claimed))
	}

	// And a claimed job cannot be cancelled out from under its worker.
	if _, err := store.Cancel(ctx, claimed[0].ID); err == nil {
		t.Error("cancelled a job that a worker is already running")
	}
}

func TestListJobsFiltersAndPages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, _ := newStore(t)

	seed(t, store, 5, "alpha")
	seed(t, store, 3, "beta")

	byKind, _, err := store.ListJobs(ctx, queue.JobFilter{Kind: "beta"})
	if err != nil {
		t.Fatal(err)
	}
	if len(byKind) != 3 {
		t.Errorf("kind filter returned %d jobs, want 3", len(byKind))
	}

	page1, next, err := store.ListJobs(ctx, queue.JobFilter{Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 5 || next == 0 {
		t.Fatalf("first page: %d jobs, cursor %d", len(page1), next)
	}

	page2, _, err := store.ListJobs(ctx, queue.JobFilter{Limit: 5, Before: next})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 3 {
		t.Errorf("second page returned %d jobs, want 3", len(page2))
	}
	// Keyset paging must not repeat rows across pages.
	seen := map[int64]bool{}
	for _, j := range append(page1, page2...) {
		if seen[j.ID] {
			t.Errorf("job %d appeared on both pages", j.ID)
		}
		seen[j.ID] = true
	}

	if _, _, err := store.ListJobs(ctx, queue.JobFilter{State: "not-a-state"}); err == nil {
		t.Error("an unknown state was accepted")
	}
}
