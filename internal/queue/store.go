package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sharvary-HH/queued/internal/metrics"
)

// ErrStaleClaim means the job was not in the state the caller thought it was:
// somebody else owns it now, or it is no longer claimed at all. In practice it
// means the reaper decided this worker was dead and handed the job on while the
// worker was still running it. The worker must drop the result on the floor —
// the new owner is authoritative.
var ErrStaleClaim = errors.New("queue: claim is no longer held by this worker")

// Store is the only thing in the project that talks to Postgres.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// jobColumns is written once so the SELECT list and scanJob cannot drift apart.
const jobColumns = `id, queue, kind, payload, state, priority, run_at, attempt,
	max_attempts, claimed_at, claimed_by, claim_expires_at,
	visibility_timeout_seconds, last_error, idempotency_key, created_at, updated_at`

// claimSQL is the heart of the project.
//
// Reading it inside out:
//
//	candidate — picks the rows this worker is going to take. The FOR UPDATE is
//	  what makes the choice exclusive, and it can only be attached to a SELECT,
//	  which is the entire reason this is a subquery rather than a bare UPDATE
//	  with an ORDER BY and a LIMIT.
//
//	SKIP LOCKED — without it, a worker whose scan reaches a row another worker
//	  has locked blocks until that worker's transaction ends. Every worker then
//	  queues up behind whichever one got to the head of the queue first, and N
//	  workers deliver exactly the throughput of one. With it, a locked row is
//	  stepped over and the scan keeps going, so each worker walks away with a
//	  disjoint set and no worker ever waits on another. This is the whole
//	  reason Postgres is a viable queue.
//
//	attempt = attempt + 1 — the counter moves at claim time, not at failure
//	  time. A worker that is SIGKILLed, loses its network, or hangs never
//	  reports anything at all, so a counter that only advanced on a reported
//	  failure would never advance for exactly the jobs most likely to be
//	  killing workers. Charging the attempt up front means a poison job that
//	  takes down every worker that touches it still walks to the DLQ instead of
//	  being reclaimed forever. The cost is that a job can burn an attempt
//	  without its handler having failed — a worker restarted mid-job pays for
//	  the interruption. That is the correct trade: bounded retries (property 4)
//	  matter more than squeezing the last attempt out of an unlucky job.
//
//	claim_expires_at — materialised here so the reaper's scan is an index range
//	  rather than a per-row interval computation. See migration 0002.
//
//	history — the attempt row is written in the same statement as the claim, so
//	  a job that is claimed and never heard from again still leaves a record of
//	  who took it and when. If this were left to the worker after the claim
//	  returned, a worker that died in between would leave no trace at all.
//
// The final SELECT re-orders because the UPDATE's RETURNING order is not
// defined, and the pool hands jobs out in the order it gets them.
const claimSQL = `
WITH candidate AS (
    SELECT id FROM jobs
    WHERE state = 'pending'
      AND queue = $1
      AND run_at <= now()
    ORDER BY priority ASC, run_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $3
), claimed AS (
    UPDATE jobs SET
        state            = 'claimed',
        claimed_at       = now(),
        claimed_by       = $2,
        claim_expires_at = now() + make_interval(secs => visibility_timeout_seconds),
        attempt          = attempt + 1
    WHERE id IN (SELECT id FROM candidate)
    RETURNING ` + jobColumns + `
), history AS (
    INSERT INTO job_attempts (job_id, attempt, worker_id, started_at)
    SELECT id, attempt, $2, now() FROM claimed
)
SELECT ` + jobColumns + ` FROM claimed ORDER BY priority ASC, run_at ASC`

// Claim atomically hands at most n runnable jobs to workerID.
//
// Batching matters: one query for ten jobs is one round trip, one transaction
// and one index scan instead of ten of each. Phase 8 measures how much that is
// worth.
func (s *Store) Claim(ctx context.Context, queue, workerID string, n int) ([]Job, error) {
	if n < 1 {
		return nil, fmt.Errorf("queue: claim batch must be >= 1, got %d", n)
	}

	start := time.Now()
	rows, err := s.pool.Query(ctx, claimSQL, queue, workerID, n)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer rows.Close()

	jobs, err := collectJobs(rows)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	// Timed around the scan as well as the query, because the round trip is
	// what a worker actually waits for.
	metrics.ClaimLatency.Observe(time.Since(start).Seconds())
	metrics.ClaimedJobs.Add(float64(len(jobs)))
	return jobs, nil
}

const completeSQL = `
WITH done AS (
    UPDATE jobs SET
        state            = 'succeeded',
        last_error       = NULL,
        claim_expires_at = NULL
    WHERE id = $1 AND state = 'claimed' AND claimed_by = $2
    RETURNING id, attempt
)
UPDATE job_attempts a SET finished_at = now()
FROM done
WHERE a.job_id = done.id AND a.attempt = done.attempt AND a.finished_at IS NULL`

// Complete marks a job succeeded and closes its attempt row.
//
// The claimed_by check is not decoration. If the reaper has already decided
// this worker was dead and given the job to somebody else, this update matches
// nothing and the caller gets ErrStaleClaim instead of stomping on the new
// owner's work.
func (s *Store) Complete(ctx context.Context, jobID int64, workerID string) error {
	tag, err := s.pool.Exec(ctx, completeSQL, jobID, workerID)
	if err != nil {
		return fmt.Errorf("complete job %d: %w", jobID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("complete job %d: %w", jobID, ErrStaleClaim)
	}
	return nil
}

// FailRequest is what a worker reports when a handler returns an error.
//
// RetryAt is computed by the caller rather than in SQL because the backoff
// policy belongs to the worker (internal/worker/backoff.go), and because tests
// need to be able to hand it an exact time.
type FailRequest struct {
	JobID    int64
	WorkerID string
	Err      string

	// RetryAt is when the job becomes runnable again. Ignored if the job is
	// going to the dead-letter queue.
	RetryAt time.Time

	// Permanent short-circuits the retry budget. A handler that says the
	// payload is malformed knows the next four attempts will fail the same way,
	// so the job goes straight to 'dead'.
	Permanent bool
}

// failSQL decides between retry and DLQ in one statement.
//
// attempt >= max_attempts is evaluated against the value already incremented at
// claim time, so a job with max_attempts = 5 runs exactly five times.
const failSQL = `
WITH failed AS (
    UPDATE jobs SET
        state = CASE
            WHEN $3::bool OR attempt >= max_attempts THEN 'dead'::job_state
            ELSE 'pending'::job_state
        END,
        run_at           = CASE WHEN $3::bool OR attempt >= max_attempts THEN run_at ELSE $4 END,
        last_error       = $5,
        claim_expires_at = NULL
    WHERE id = $1 AND state = 'claimed' AND claimed_by = $2
    RETURNING id, attempt, state
), closed AS (
    UPDATE job_attempts a SET finished_at = now(), error = $5
    FROM failed
    WHERE a.job_id = failed.id AND a.attempt = failed.attempt AND a.finished_at IS NULL
)
SELECT state, attempt FROM failed`

// Fail records a failed attempt and either schedules the retry or moves the job
// to the dead-letter queue. It reports the state the job ended up in so the
// worker can log the difference between "will try again" and "gave up".
func (s *Store) Fail(ctx context.Context, req FailRequest) (State, error) {
	var state State
	var attempt int

	err := s.pool.QueryRow(ctx, failSQL,
		req.JobID, req.WorkerID, req.Permanent, req.RetryAt, req.Err,
	).Scan(&state, &attempt)

	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("fail job %d: %w", req.JobID, ErrStaleClaim)
	}
	if err != nil {
		return "", fmt.Errorf("fail job %d: %w", req.JobID, err)
	}
	return state, nil
}

// Reclaimed describes one job the reaper took back.
type Reclaimed struct {
	JobID    int64
	Attempt  int
	WorkerID string
	State    State
}

// reapSQL returns expired claims to the queue.
//
// SKIP LOCKED again, for a different reason than the claim query: several
// queued instances run their own reaper, and they should divide the work rather
// than pile up on the same rows.
//
// The CASE is what keeps property 4 true through worker death. A job whose
// worker keeps dying has been burning attempts the whole time (they are charged
// at claim), so once the budget is gone the reaper sends it to the DLQ instead
// of putting it back for another worker to die on.
const reapSQL = `
WITH expired AS (
    SELECT id, attempt, claimed_by FROM jobs
    WHERE state = 'claimed' AND claim_expires_at < now()
    ORDER BY claim_expires_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $1
), reaped AS (
    UPDATE jobs SET
        state = CASE
            WHEN attempt >= max_attempts THEN 'dead'::job_state
            ELSE 'pending'::job_state
        END,
        run_at           = now(),
        claimed_at       = NULL,
        claimed_by       = NULL,
        claim_expires_at = NULL,
        last_error       = 'visibility timeout expired; worker never reported'
    WHERE id IN (SELECT id FROM expired)
    RETURNING id, attempt, state
), closed AS (
    UPDATE job_attempts a SET
        finished_at = now(),
        error       = 'visibility timeout expired; worker never reported'
    FROM expired e
    WHERE a.job_id = e.id AND a.attempt = e.attempt AND a.finished_at IS NULL
)
SELECT r.id, r.attempt, r.state, e.claimed_by
FROM reaped r JOIN expired e ON e.id = r.id`

// Reap returns jobs whose claim has expired to the queue. It reports every job
// it took back, because a reclaim always means something went wrong — a worker
// died, or a handler ran past its visibility timeout — and the caller logs each
// one individually.
func (s *Store) Reap(ctx context.Context, limit int) ([]Reclaimed, error) {
	if limit < 1 {
		return nil, fmt.Errorf("queue: reap limit must be >= 1, got %d", limit)
	}

	rows, err := s.pool.Query(ctx, reapSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("reap: %w", err)
	}
	defer rows.Close()

	var out []Reclaimed
	for rows.Next() {
		var r Reclaimed
		if err := rows.Scan(&r.JobID, &r.Attempt, &r.State, &r.WorkerID); err != nil {
			return nil, fmt.Errorf("reap: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

const releaseSQL = `
WITH released AS (
    UPDATE jobs SET
        state            = 'pending',
        run_at           = now(),
        claimed_at       = NULL,
        claimed_by       = NULL,
        claim_expires_at = NULL,
        last_error       = $3
    WHERE id = ANY($1) AND state = 'claimed' AND claimed_by = $2
    RETURNING id, attempt
)
UPDATE job_attempts a SET finished_at = now(), error = $3
FROM released
WHERE a.job_id = released.id AND a.attempt = released.attempt AND a.finished_at IS NULL`

// Release hands jobs back to the queue immediately instead of waiting for the
// reaper. Shutdown uses it: a worker that is going away knows its in-flight
// jobs are not going to finish, and saying so turns a visibility-timeout-long
// stall into an instant handover (property 3).
//
// The attempt already spent is not refunded — see the note on claimSQL.
func (s *Store) Release(ctx context.Context, jobIDs []int64, workerID, reason string) (int64, error) {
	if len(jobIDs) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, releaseSQL, jobIDs, workerID, reason)
	if err != nil {
		return 0, fmt.Errorf("release: %w", err)
	}
	return tag.RowsAffected(), nil
}

// requeueSQL resets a dead job so it can run again.
//
// attempt goes back to 0 rather than being left where it was, because a requeue
// is an operator saying "I fixed the thing that was breaking this" — the
// previous failures are history, not a budget already spent. Leaving the
// counter would give the job one attempt and send it straight back to the DLQ,
// which makes the button useless exactly when it matters.
//
// The history in job_attempts is deliberately not touched. Those rows are the
// record of what actually happened, and a job that failed five times and was
// then requeued should still show all five when somebody asks why.
const requeueSQL = `
UPDATE jobs SET
    state            = 'pending',
    attempt          = 0,
    run_at           = now(),
    claimed_at       = NULL,
    claimed_by       = NULL,
    claim_expires_at = NULL,
    last_error       = NULL
WHERE id = $1 AND state = 'dead'
RETURNING ` + jobColumns

// ErrNotDead is returned when a requeue targets a job that is not in the DLQ.
var ErrNotDead = errors.New("queue: job is not in the dead-letter queue")

// Requeue moves a dead job back to pending with a fresh retry budget.
func (s *Store) Requeue(ctx context.Context, jobID int64) (Job, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, requeueSQL, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		// Either it does not exist or it is not dead. Tell the two apart, so
		// the API can answer 404 or 409 rather than guessing.
		if _, lookupErr := s.JobByID(ctx, jobID); lookupErr != nil {
			return Job{}, lookupErr
		}
		return Job{}, fmt.Errorf("requeue job %d: %w", jobID, ErrNotDead)
	}
	if err != nil {
		return Job{}, fmt.Errorf("requeue job %d: %w", jobID, err)
	}
	return job, nil
}

func collectJobs(rows pgx.Rows) ([]Job, error) {
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var visibilitySeconds int

	err := row.Scan(
		&j.ID, &j.Queue, &j.Kind, &j.Payload, &j.State, &j.Priority, &j.RunAt,
		&j.Attempt, &j.MaxAttempts, &j.ClaimedAt, &j.ClaimedBy, &j.ClaimExpiresAt,
		&visibilitySeconds, &j.LastError, &j.IdempotencyKey, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return Job{}, err
	}
	j.VisibilityTimeout = time.Duration(visibilitySeconds) * time.Second
	return j, nil
}

// Connect opens a pool and waits for the database to answer. Compose starts
// Postgres and the app at the same time, so a bit of patience here saves a
// restart loop.
//
// maxConns of 0 keeps pgx's default, which is max(4, numCPU) — and that default
// is a trap for this program specifically. A worker needs one connection per
// executor reporting a result, plus one for the claimer, plus one for the
// reaper, plus one the NOTIFY listener holds for its entire life and never
// gives back. At the default CONCURRENCY of 8 that is eleven consumers sharing
// eight connections, and on a four-core box it would be eleven sharing four.
// Nothing breaks — pgx just queues — so the symptom is not an error but
// everything mysteriously going slower under load, which is the worst kind of
// bug to be handed. Callers size it deliberately.
func Connect(ctx context.Context, dsn string, maxConns int) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		// An explicit max_conns in the DSN is the operator being specific, so
		// leave it alone.
		if !strings.Contains(dsn, "pool_max_conns") {
			cfg.MaxConns = int32(maxConns)
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		err = pool.Ping(ctx)
		if err == nil {
			return pool, nil
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			pool.Close()
			return nil, fmt.Errorf("ping: %w", err)
		}
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}
