package worker_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/worker"
	"github.com/Sharvary-HH/queued/testdata/handlers"
)

// startReaper runs a reaper in the background and stops it when the test ends.
func (h *harness) startReaper(t *testing.T, cfg worker.ReaperConfig) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.NewReaper(h.store, h.log, cfg).Run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("reaper did not stop")
		}
	})
}

// Property 2, end to end. A worker claims a job and is never heard from again;
// the reaper must put it back and a healthy worker must run it.
//
// The dead worker is simulated by claiming directly and not reporting, which is
// exactly what a SIGKILLed process looks like from the database's side — there
// is no difference to detect.
func TestReaperRecoversJobsFromDeadWorkers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newHarness(t)

	job := h.enqueue(t, queue.EnqueueParams{
		Kind:              handlers.KindSucceed,
		VisibilityTimeout: time.Second,
	})

	claimed, err := h.store.Claim(ctx, "default", "worker-that-will-die", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(claimed))
	}
	// ...and now that worker is gone. Nothing is reported.

	h.startReaper(t, worker.ReaperConfig{Interval: 200 * time.Millisecond})
	h.start(t, fastConfig(2))

	waitFor(t, 30*time.Second, "the abandoned job to be reclaimed and run", func() bool {
		return h.countByState(t)[queue.StateSucceeded] == 1
	})

	if runs := h.set.Runs(handlers.KindSucceed); runs != 1 {
		t.Errorf("handler ran %d times, want 1", runs)
	}

	// The dead worker's attempt is on the record, closed with the reason.
	attempts, err := h.store.AttemptsByJobID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 {
		t.Fatalf("recorded %d attempts, want 2 (the dead one and the real one)", len(attempts))
	}
	first := attempts[0]
	if first.WorkerID != "worker-that-will-die" {
		t.Errorf("first attempt belongs to %q", first.WorkerID)
	}
	if first.FinishedAt == nil {
		t.Error("the dead worker's attempt was never closed")
	}
	if first.Error == nil {
		t.Error("the dead worker's attempt has no error recorded")
	}

	if !h.logs.Contains("job reclaimed from an expired claim") {
		t.Error("the reclaim was not logged; it is the health signal that a worker died")
	}
	if !h.logs.Contains("worker-that-will-die") {
		t.Error("the reclaim log does not name the worker that was lost")
	}
}

// A reclaim must not hand a job back forever. The attempt was charged at claim
// time precisely so that a job killing its workers still terminates.
func TestReaperStopsReclaimingOnceAttemptsRunOut(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newHarness(t)

	maxAttempts := 3
	job := h.enqueue(t, queue.EnqueueParams{
		Kind:              handlers.KindSucceed,
		MaxAttempts:       &maxAttempts,
		VisibilityTimeout: time.Second,
	})

	h.startReaper(t, worker.ReaperConfig{Interval: 200 * time.Millisecond})

	// A succession of workers that each claim and then vanish. No pool runs, so
	// nothing ever reports a result.
	for i := range maxAttempts {
		waitFor(t, 20*time.Second, "the job to become claimable again", func() bool {
			claimed, err := h.store.Claim(ctx, "default", "doomed-worker", 1)
			if err != nil {
				t.Fatal(err)
			}
			return len(claimed) == 1
		})
		t.Logf("worker %d claimed and died", i+1)
	}

	waitFor(t, 20*time.Second, "the job to be dead-lettered", func() bool {
		final, err := h.store.JobByID(ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		return final.State == queue.StateDead
	})

	// And it stays dead: no further reclaim resurrects it.
	time.Sleep(time.Second)
	final, err := h.store.JobByID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != queue.StateDead {
		t.Fatalf("job left the DLQ on its own: state is %q", final.State)
	}
	if final.Attempt != maxAttempts {
		t.Errorf("attempt = %d, want exactly %d", final.Attempt, maxAttempts)
	}
}

// A handler that outlives its visibility timeout is cancelled by the pool, not
// rescued by the reaper — the job's context carries that timeout as a deadline,
// so the pool polices it first and the reaper never sees the job.
//
// That ordering is the point. If only the reaper enforced the timeout, a hung
// handler would keep its slot forever while a second worker ran the same job
// alongside it. Cancelling locally means the slot comes back and there is only
// ever one execution in flight.
func TestSlowHandlerIsCancelledAndFreesItsSlot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newHarness(t)

	slow := h.enqueue(t, queue.EnqueueParams{
		Kind:    handlers.KindSlow,
		Payload: payload(t, handlers.Payload{ID: "too-slow", Sleep: "5m"}),
		// Far shorter than the handler wants.
		VisibilityTimeout: 2 * time.Second,
	})

	// No reaper here on purpose. Both mechanisms would free this job, and they
	// fire at almost the same moment, so running a reaper alongside would make
	// the recorded reason a coin flip. This test is about the pool policing its
	// own deadline; the reaper has its own tests.

	// One slot, so the next job can only run if the hung one gave it back.
	cfg := fastConfig(1)
	cfg.DrainTimeout = time.Second
	h.start(t, cfg)

	waitFor(t, 20*time.Second, "the slow job to start", func() bool {
		return h.set.Runs(handlers.KindSlow) >= 1
	})

	h.enqueue(t, queue.EnqueueParams{
		Kind:    handlers.KindSucceed,
		Payload: payload(t, handlers.Payload{ID: "the-one-behind-it"}),
	})

	// Nothing like the five minutes the stuck handler asked for.
	waitFor(t, 30*time.Second, "the queued job to get the freed slot", func() bool {
		return h.set.Runs(handlers.KindSucceed) == 1
	})

	// The cancellation is on the record, so an operator can see why it failed.
	current, err := h.store.JobByID(ctx, slow.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.LastError == nil {
		t.Fatal("the cancelled job recorded no error")
	}
	if !strings.Contains(*current.LastError, "context deadline exceeded") {
		t.Errorf("last_error = %q, want it to mention the deadline", *current.LastError)
	}
}

func TestRequeueResetsTheAttemptBudget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newHarness(t)

	maxAttempts := 2
	job := h.enqueue(t, queue.EnqueueParams{
		Kind:        handlers.KindFail,
		Payload:     payload(t, handlers.Payload{ID: "will-be-requeued"}),
		MaxAttempts: &maxAttempts,
	})

	stop := h.start(t, fastConfig(1))
	waitFor(t, 20*time.Second, "the job to reach the DLQ", func() bool {
		return h.countByState(t)[queue.StateDead] == 1
	})
	stop()

	requeued, err := h.store.Requeue(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if requeued.State != queue.StatePending {
		t.Errorf("state after requeue = %q, want pending", requeued.State)
	}
	// Zero, not left where it was: a requeue means somebody fixed the cause,
	// and a job handed back with a spent budget would bounce straight into the
	// DLQ again on its first attempt.
	if requeued.Attempt != 0 {
		t.Errorf("attempt after requeue = %d, want 0", requeued.Attempt)
	}
	if requeued.LastError != nil {
		t.Errorf("last_error survived the requeue: %q", *requeued.LastError)
	}

	// The history is kept. A job that failed twice and was then requeued should
	// still show both failures to whoever asks why.
	attempts, err := h.store.AttemptsByJobID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != maxAttempts {
		t.Errorf("requeue destroyed history: %d attempt rows, want %d", len(attempts), maxAttempts)
	}

	// And it really does get a full second life.
	if _, err := h.store.Claim(ctx, "default", "worker-b", 1); err != nil {
		t.Fatal(err)
	}
}

func TestRequeueRejectsJobsThatAreNotDead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newHarness(t)

	job := h.enqueue(t, queue.EnqueueParams{Kind: handlers.KindSucceed})

	if _, err := h.store.Requeue(ctx, job.ID); err == nil {
		t.Error("requeued a pending job")
	}
	if _, err := h.store.Requeue(ctx, 999999); err == nil {
		t.Error("requeued a job that does not exist")
	}
}
