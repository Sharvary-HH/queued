package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Sharvary-HH/queued/internal/queue"
)

// ReaperConfig tunes the reclaim loop.
type ReaperConfig struct {
	// Interval between sweeps. This is the granularity of recovery, not the
	// recovery time: a job comes back Interval after its visibility timeout
	// expires, not Interval after the worker died.
	Interval time.Duration

	// Batch caps how many jobs one sweep reclaims. A cap matters because the
	// pathological case — a deploy that killed every worker mid-job — wants to
	// return thousands of jobs at once, and doing that in one statement holds
	// locks on all of them while the surviving workers are trying to claim.
	Batch int
}

func (c *ReaperConfig) setDefaults() {
	if c.Interval <= 0 {
		c.Interval = 5 * time.Second
	}
	if c.Batch < 1 {
		c.Batch = 100
	}
}

// Reaper returns jobs whose claim outlived its visibility timeout.
//
// This is the thing that makes worker death survivable (property 2). Nothing
// else in the system notices that a worker is gone: there is no heartbeat, no
// registry of live workers, no liveness protocol. A claim simply carries an
// expiry, and if nobody reports a result before it passes, the job goes back.
// That is a deliberately small amount of machinery for the guarantee it buys,
// and it degrades in the right direction — a worker that is merely slow gets
// its job taken away and run again, which is the at-least-once contract, rather
// than the job being lost.
//
// Every instance runs its own reaper. There is no election and none is needed:
// the sweep uses FOR UPDATE SKIP LOCKED, so concurrent reapers divide the
// expired rows between them instead of fighting over them. A reaper is not a
// singleton service, it is a janitor, and having several is strictly safer than
// having one that might be on the instance that just died.
type Reaper struct {
	store *queue.Store
	log   *slog.Logger
	cfg   ReaperConfig
}

func NewReaper(store *queue.Store, log *slog.Logger, cfg ReaperConfig) *Reaper {
	cfg.setDefaults()
	return &Reaper{store: store, log: log.With("component_role", "reaper"), cfg: cfg}
}

// Run sweeps until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) {
	r.log.Info("reaper started",
		"interval", r.cfg.Interval.String(), "batch", r.cfg.Batch)

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.Info("reaper stopped")
			return
		case <-ticker.C:
			r.sweep(ctx)
		}
	}
}

// sweep reclaims one batch. It keeps going while batches come back full, so a
// large backlog is cleared over a few queries in one tick rather than one batch
// per tick — after a fleet-wide restart, waiting Interval per hundred jobs
// would take absurdly long.
func (r *Reaper) sweep(ctx context.Context) {
	for {
		reclaimed, err := r.store.Reap(ctx, r.cfg.Batch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Not fatal. The next tick tries again, and until then the jobs sit
			// in 'claimed' where they are safe, just idle.
			r.log.Error("reap failed", "error", err)
			return
		}
		if len(reclaimed) == 0 {
			return
		}

		for _, job := range reclaimed {
			// Every reclaim is logged individually and at warn, because a
			// reclaim is never normal. It means a worker died holding this job,
			// or a handler ran past its visibility timeout. Either way somebody
			// should know which job and which worker, and a count alone does
			// not answer that at 3am.
			r.log.Warn("job reclaimed from an expired claim",
				"job_id", job.JobID,
				"attempt", job.Attempt,
				"lost_worker_id", job.WorkerID,
				"returned_to", job.State,
			)
		}

		if len(reclaimed) < r.cfg.Batch {
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}
