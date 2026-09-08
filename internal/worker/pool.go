package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Sharvary-HH/queued/internal/queue"
)

// Config tunes one worker process.
type Config struct {
	Queue    string
	WorkerID string

	// Concurrency is how many jobs run at once.
	Concurrency int

	// ClaimBatch is how many jobs one claim query fetches. Batching trades a
	// little fairness between workers for a lot fewer round trips: ten jobs in
	// one query is one network hop, one transaction and one index scan instead
	// of ten of each. Phase 8 measures it.
	ClaimBatch int

	// PollInterval is the fallback when NOTIFY is not delivering. It is not the
	// normal path to a job being picked up — that is the notification — it is
	// the guarantee that a missed notification costs one interval rather than
	// forever.
	PollInterval time.Duration

	// DrainTimeout is how long in-flight jobs get to finish after a shutdown
	// signal before they are cancelled and released.
	DrainTimeout time.Duration

	Backoff Backoff
}

func (c *Config) setDefaults() {
	if c.Queue == "" {
		c.Queue = "default"
	}
	if c.Concurrency < 1 {
		c.Concurrency = 1
	}
	if c.ClaimBatch < 1 {
		c.ClaimBatch = 1
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 100 * time.Millisecond
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = 30 * time.Second
	}
	if c.Backoff.Base <= 0 || c.Backoff.Max <= 0 {
		c.Backoff = DefaultBackoff
	}
}

// Pool is one worker process: a single claimer feeding N executor goroutines.
//
// One claimer rather than N is deliberate. Every goroutine running its own
// claim query would multiply the load on the hot path by the concurrency, and
// the claim query is the one part of the system that all workers everywhere
// contend on. A single claimer batching ten at a time keeps that contention
// flat as concurrency goes up.
type Pool struct {
	store *queue.Store
	reg   *Registry
	log   *slog.Logger
	cfg   Config

	// inflight is what makes shutdown honest. Jobs are added when an executor
	// picks them up and removed when a result is reported, so at the drain
	// deadline this is exactly the set that needs releasing.
	mu       sync.Mutex
	inflight map[int64]struct{}
}

func New(store *queue.Store, reg *Registry, log *slog.Logger, cfg Config) *Pool {
	cfg.setDefaults()
	return &Pool{
		store:    store,
		reg:      reg,
		log:      log.With("worker_id", cfg.WorkerID, "queue", cfg.Queue),
		cfg:      cfg,
		inflight: make(map[int64]struct{}),
	}
}

// Run blocks until ctx is cancelled and the pool has drained.
//
// The shutdown sequence, which is a named selling point, is:
//
//  1. ctx is cancelled (SIGTERM). The claimer stops immediately — no new work
//     is taken from the moment the signal arrives.
//  2. Jobs already claimed but not yet started are released straight back to
//     pending. They have not run, so there is nothing to wait for.
//  3. In-flight jobs keep running on a context that deliberately does not
//     inherit the cancellation, and get DrainTimeout to finish.
//  4. Anything still running at the deadline is cancelled, and its job is
//     released explicitly rather than left for the reaper. That turns a
//     visibility-timeout-long stall into an immediate handover.
func (p *Pool) Run(ctx context.Context) error {
	if len(p.reg.Kinds()) == 0 {
		return errors.New("worker: no handlers registered")
	}
	p.log.Info("worker pool starting",
		"concurrency", p.cfg.Concurrency,
		"claim_batch", p.cfg.ClaimBatch,
		"poll_interval", p.cfg.PollInterval.String(),
		"drain_timeout", p.cfg.DrainTimeout.String(),
		"kinds", p.reg.Kinds(),
	)

	// Executors run on a context that survives the shutdown signal, so a
	// handler in the middle of something is not cancelled the instant SIGTERM
	// lands. drain() is what eventually cancels it.
	execCtx, cancelExec := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelExec()

	// Buffered by one batch so the claimer can fetch the next batch while the
	// executors are still working through this one.
	jobs := make(chan queue.Job, p.cfg.ClaimBatch)

	listener := newListener(p.store.Pool(), p.log)
	var listenerDone sync.WaitGroup
	listenerDone.Add(1)
	go func() {
		defer listenerDone.Done()
		listener.run(ctx)
	}()

	var executors errgroup.Group
	for i := range p.cfg.Concurrency {
		executors.Go(func() error {
			p.execute(ctx, execCtx, jobs, i)
			return nil
		})
	}

	p.claim(ctx, listener, jobs)

	// The claimer has returned, which means ctx is done. Nothing else will be
	// put on the channel.
	close(jobs)
	p.log.Info("shutdown: claimer stopped, draining in-flight jobs")

	p.releaseQueued(jobs)
	p.drain(&executors, cancelExec)

	listenerDone.Wait()
	p.log.Info("shutdown complete")
	return nil
}

// claim is the single claimer goroutine. It runs on the caller's goroutine
// because Run has nothing else to do until it finishes.
func (p *Pool) claim(ctx context.Context, l *listener, out chan<- queue.Job) {
	timer := time.NewTimer(p.cfg.PollInterval)
	defer timer.Stop()

	for {
		if ctx.Err() != nil {
			return
		}

		// The claim query deliberately does not inherit the shutdown
		// cancellation, and this is not a detail.
		//
		// Cancelling a claim mid-flight is the one way this pool can strand
		// jobs. pgx gives up on the query and returns an error, but the UPDATE
		// may already have committed on the server — the cancellation and the
		// commit race, and the client cannot tell which won. The rows are then
		// marked claimed by a worker that never learned it had them, so nothing
		// releases them and they sit until the reaper's visibility timeout:
		// exactly the stall property 3 exists to prevent, arriving through the
		// shutdown path itself.
		//
		// Letting a short indexed query finish costs milliseconds and makes the
		// outcome always knowable. Whatever comes back is then either
		// dispatched or released.
		claimCtx, cancelClaim := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		batch, err := p.store.Claim(claimCtx, p.cfg.Queue, p.cfg.WorkerID, p.cfg.ClaimBatch)
		cancelClaim()

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// A failed claim is not fatal: the database may be restarting or
			// failing over, and the right response is to wait and try again
			// rather than take the process down and lose the in-flight jobs.
			p.log.Error("claim failed", "error", err)
			if !sleep(ctx, p.cfg.PollInterval) {
				return
			}
			continue
		}

		// Shutdown arrived while the claim was in flight. These jobs are ours
		// and nothing is going to run them, so give them back now rather than
		// letting them expire.
		if ctx.Err() != nil {
			p.releaseSlice(batch, "worker shut down while claiming")
			return
		}

		for i, job := range batch {
			select {
			case out <- job:
			case <-ctx.Done():
				// Shut down midway through dispatching. Everything from here on
				// is claimed and will never reach an executor, so hand it
				// straight back. batch[:i] is already on the channel and is
				// releaseQueued's problem.
				p.releaseSlice(batch[i:], "worker shut down before dispatch")
				return
			}
		}

		// A full batch almost certainly means there is more waiting, so go
		// straight back for it instead of sleeping on a busy queue.
		if len(batch) == p.cfg.ClaimBatch {
			continue
		}

		// Empty or short batch: wait for a notification, the poll timer, or
		// shutdown. Never a bare loop — an idle worker must cost nothing.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(p.cfg.PollInterval)

		select {
		case <-ctx.Done():
			return
		case <-l.C():
		case <-timer.C:
		}
	}
}

// execute is one executor goroutine.
//
// It selects on ctx as well as the channel so that a shutdown stops it from
// picking up more work even while the channel still has buffered jobs in it;
// releaseQueued deals with those.
func (p *Pool) execute(ctx context.Context, execCtx context.Context, in <-chan queue.Job, slot int) {
	log := p.log.With("slot", slot)

	for {
		// select picks at random when several cases are ready, so a plain
		// two-case select would let an executor keep pulling buffered jobs
		// after the shutdown signal for as long as the buffer lasted. Checking
		// cancellation first makes "stop taking new work" mean what it says.
		select {
		case <-ctx.Done():
			return
		default:
		}

		select {
		case <-ctx.Done():
			return
		case job, ok := <-in:
			if !ok {
				return
			}
			p.markInflight(job.ID)
			p.runJob(execCtx, log, job)
			p.clearInflight(job.ID)
		}
	}
}

func (p *Pool) runJob(ctx context.Context, log *slog.Logger, job queue.Job) {
	log = log.With("job_id", job.ID, "kind", job.Kind, "attempt", job.Attempt)

	handler, err := p.reg.Lookup(job.Kind)
	if err != nil {
		log.Warn("no handler for kind", "error", err)
		p.report(ctx, log, job, err)
		return
	}

	// The job's deadline is its visibility timeout. Past that point the reaper
	// is entitled to give the job to somebody else, so continuing to run it is
	// at best wasted work and at worst a second concurrent execution.
	jobCtx, cancel := context.WithTimeout(ctx, job.VisibilityTimeout)
	defer cancel()

	start := time.Now()
	err = safely(handler)(jobCtx, job.Payload)
	elapsed := time.Since(start)

	// Reporting gets a context of its own. If the handler was cancelled by the
	// drain deadline, the executor context is already dead, and a job whose
	// outcome cannot be written down is a job that sits in 'claimed' until the
	// reaper notices — for a job that actually finished, that means running it
	// a second time for nothing.
	reportCtx, cancelReport := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancelReport()

	if err != nil {
		log.Warn("job failed", "error", err, "duration_ms", elapsed.Milliseconds())
		p.report(reportCtx, log, job, err)
		return
	}

	if cerr := p.store.Complete(reportCtx, job.ID, p.cfg.WorkerID); cerr != nil {
		p.logReportFailure(log, cerr, "complete")
		return
	}
	log.Info("job succeeded", "duration_ms", elapsed.Milliseconds())
}

// report turns a handler error into either a scheduled retry or a trip to the
// dead-letter queue.
func (p *Pool) report(ctx context.Context, log *slog.Logger, job queue.Job, cause error) {
	permanent := IsPermanent(cause)

	state, err := p.store.Fail(ctx, queue.FailRequest{
		JobID:     job.ID,
		WorkerID:  p.cfg.WorkerID,
		Err:       cause.Error(),
		RetryAt:   p.cfg.Backoff.RetryAt(time.Now(), job.Attempt),
		Permanent: permanent,
	})
	if err != nil {
		p.logReportFailure(log, err, "fail")
		return
	}

	if state == queue.StateDead {
		log.Error("job moved to dead-letter queue",
			"reason", deadReason(permanent), "attempts_used", job.Attempt, "error", cause)
		return
	}
	log.Info("job scheduled for retry", "next_attempt", job.Attempt+1, "of", job.MaxAttempts)
}

func deadReason(permanent bool) string {
	if permanent {
		return "permanent error"
	}
	return "retries exhausted"
}

// logReportFailure separates "we could not talk to the database" from "the job
// is no longer ours". The second is not an error: the reaper decided this
// worker was gone and gave the job to somebody else, and the correct behaviour
// is to drop our result on the floor, because the new owner is authoritative.
func (p *Pool) logReportFailure(log *slog.Logger, err error, op string) {
	if errors.Is(err, queue.ErrStaleClaim) {
		log.Warn("result discarded, job is no longer held by this worker", "op", op)
		return
	}
	log.Error("could not report job result", "op", op, "error", err)
}

// safely wraps a handler so a panic fails its job instead of taking the process
// down with it.
//
// A panicking handler is a bug in one job's code, not in the queue, and one bad
// job must not be able to stop a worker from running the other thousand. The
// stack goes into last_error, because a panic with no stack is nearly useless
// to whoever has to fix it.
//
// The recovered value is not marked permanent: panics are usually nil-map or
// index-out-of-range bugs that a code fix will resolve, and there is no reason
// to think the *next* payload will hit the same path. It burns its retries and
// lands in the DLQ like any other repeated failure.
func safely(fn HandlerFunc) HandlerFunc {
	return func(ctx context.Context, payload []byte) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("handler panicked: %v\n\n%s", r, debug.Stack())
			}
		}()
		return fn(ctx, payload)
	}
}

// releaseQueued hands back jobs that were claimed and buffered but never
// started. They have not run at all, so there is nothing to wait for and no
// reason to make them sit out a visibility timeout.
func (p *Pool) releaseQueued(jobs <-chan queue.Job) {
	var stranded []queue.Job
	for job := range jobs {
		stranded = append(stranded, job)
	}
	if len(stranded) == 0 {
		return
	}
	p.log.Info("shutdown: releasing claimed-but-unstarted jobs", "count", len(stranded))
	p.releaseSlice(stranded, "worker shut down before the job started")
}

// drain waits for the executors, then forcibly releases whatever is left.
func (p *Pool) drain(executors *errgroup.Group, cancelExec context.CancelFunc) {
	finished := make(chan struct{})
	go func() {
		_ = executors.Wait()
		close(finished)
	}()

	deadline := time.NewTimer(p.cfg.DrainTimeout)
	defer deadline.Stop()

	select {
	case <-finished:
		p.log.Info("shutdown: all in-flight jobs finished")
		return
	case <-deadline.C:
	}

	// Past the deadline. Cancel the handlers' context so anything watching it
	// stops, and release their jobs immediately: the whole point of doing this
	// explicitly rather than leaving it to the reaper is that another worker
	// can pick the job up now instead of one visibility timeout from now.
	stuck := p.inflightIDs()
	p.log.Warn("shutdown: drain deadline reached, cancelling and releasing",
		"timeout", p.cfg.DrainTimeout.String(), "stuck_jobs", len(stuck))
	cancelExec()

	if len(stuck) > 0 {
		p.releaseIDs(stuck, "worker shut down while the job was still running")
	}

	// Give the cancelled handlers a moment to unwind so their goroutines are
	// not still running as the process exits. They cannot affect their jobs any
	// more: the release above cleared claimed_by, so any result they try to
	// report is rejected as a stale claim.
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		p.log.Warn("shutdown: handlers did not return after cancellation", "stuck_jobs", len(stuck))
	}
}

func (p *Pool) releaseSlice(jobs []queue.Job, reason string) {
	ids := make([]int64, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
	}
	p.releaseIDs(ids, reason)
}

func (p *Pool) releaseIDs(ids []int64, reason string) {
	// Its own context: the one that got us here is already cancelled, and this
	// is the last thing standing between a clean shutdown and a pile of jobs
	// waiting out their visibility timeouts.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	n, err := p.store.Release(ctx, ids, p.cfg.WorkerID, reason)
	if err != nil {
		p.log.Error("could not release jobs; the reaper will pick them up after their visibility timeout",
			"error", err, "count", len(ids))
		return
	}
	p.log.Info("shutdown: released jobs back to pending", "count", n, "reason", reason)
}

func (p *Pool) markInflight(id int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inflight[id] = struct{}{}
}

func (p *Pool) clearInflight(id int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.inflight, id)
}

func (p *Pool) inflightIDs() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]int64, 0, len(p.inflight))
	for id := range p.inflight {
		out = append(out, id)
	}
	return out
}

// sleep waits for d, reporting false if ctx was cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
