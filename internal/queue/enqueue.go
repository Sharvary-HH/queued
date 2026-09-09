package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Sharvary-HH/queued/internal/metrics"
)

// EnqueueParams describes a job to submit. Only Kind is required; the zero
// value of everything else means "use the column default".
type EnqueueParams struct {
	Queue   string
	Kind    string
	Payload []byte

	// Priority: lower runs first. Zero means the default (100) rather than
	// "most urgent possible", so an uninitialised struct does not jump the
	// queue ahead of everything else.
	Priority *int

	// RunAt zero means now. A future time is a delayed job.
	RunAt time.Time

	MaxAttempts       *int
	VisibilityTimeout time.Duration

	// IdempotencyKey, when set, makes the enqueue a no-op if a job with the
	// same key already exists.
	IdempotencyKey *string
}

const defaultQueue = "default"

func (p EnqueueParams) validate() error {
	if p.Kind == "" {
		return errors.New("queue: kind is required")
	}
	if len(p.Payload) > 0 && !json.Valid(p.Payload) {
		return errors.New("queue: payload is not valid JSON")
	}
	if p.Priority != nil && *p.Priority < 0 {
		return fmt.Errorf("queue: priority must be >= 0, got %d", *p.Priority)
	}
	if p.MaxAttempts != nil && *p.MaxAttempts < 1 {
		return fmt.Errorf("queue: max_attempts must be >= 1, got %d", *p.MaxAttempts)
	}
	if p.VisibilityTimeout < 0 {
		return errors.New("queue: visibility timeout must not be negative")
	}
	if p.IdempotencyKey != nil && *p.IdempotencyKey == "" {
		// An empty string is a real key as far as the unique index is
		// concerned, so the second such enqueue would silently dedupe against
		// an unrelated job. Almost certainly a caller bug.
		return errors.New("queue: idempotency key must not be empty")
	}
	return nil
}

// COALESCE lets the caller pass NULL for "use the column default" without the
// Go side having to build a different statement for each combination.
const insertSQL = `
INSERT INTO jobs (queue, kind, payload, priority, run_at, max_attempts,
                  visibility_timeout_seconds, idempotency_key)
VALUES (
    COALESCE($1, 'default'),
    $2,
    COALESCE($3, '{}'::jsonb),
    COALESCE($4, 100),
    COALESCE($5, now()),
    COALESCE($6, 5),
    COALESCE($7, 60),
    $8
)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING ` + jobColumns

const byKeySQL = `SELECT ` + jobColumns + ` FROM jobs WHERE idempotency_key = $1`

// Enqueue inserts a job. The bool reports whether a row was actually created:
// false means an idempotency key matched an existing job, which is a 200 rather
// than a 201 as far as the API is concerned.
//
// The dedupe is ON CONFLICT DO NOTHING followed by a lookup rather than
// "SELECT then INSERT if missing", which has a race between the two statements
// wide enough to drive a truck through. If a concurrent transaction is holding
// the conflicting key uncommitted, the insert waits on the unique index; once
// that transaction commits, DO NOTHING returns no rows and the follow-up SELECT
// gets a fresh snapshot that can see it.
func (s *Store) Enqueue(ctx context.Context, p EnqueueParams) (Job, bool, error) {
	if err := p.validate(); err != nil {
		return Job{}, false, err
	}

	var visibility *int
	if p.VisibilityTimeout > 0 {
		secs := int(p.VisibilityTimeout.Seconds())
		if secs < 1 {
			secs = 1
		}
		visibility = &secs
	}

	job, err := scanJob(s.pool.QueryRow(ctx, insertSQL,
		nullable(p.Queue), p.Kind, nullableBytes(p.Payload), p.Priority,
		nullableTime(p.RunAt), p.MaxAttempts, visibility, p.IdempotencyKey,
	))
	if err == nil {
		metrics.JobsEnqueued.WithLabelValues(job.Queue, job.Kind).Inc()
		return job, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, fmt.Errorf("enqueue: %w", err)
	}

	// No rows and no error means ON CONFLICT swallowed the insert, which can
	// only happen when a key was supplied.
	if p.IdempotencyKey == nil {
		return Job{}, false, errors.New("enqueue: insert returned no row")
	}

	existing, err := scanJob(s.pool.QueryRow(ctx, byKeySQL, *p.IdempotencyKey))
	if err != nil {
		return Job{}, false, fmt.Errorf("enqueue: look up existing key: %w", err)
	}
	return existing, false, nil
}

// EnqueueMany bulk-loads jobs over the COPY protocol, which is worth roughly an
// order of magnitude over one INSERT per job. It is what the load generator and
// the benchmarks use to build a backlog.
//
// COPY cannot express ON CONFLICT, so idempotency keys are not honoured here —
// passing one is an error rather than a silent no-op.
func (s *Store) EnqueueMany(ctx context.Context, params []EnqueueParams) (int64, error) {
	if len(params) == 0 {
		return 0, nil
	}

	// COPY cannot call now(), so an unspecified run_at has to be filled in with
	// a concrete timestamp — and it must be the *server's* clock, not this
	// process's. Claim compares run_at against the database's now(), so a
	// client running even slightly ahead would write jobs whose run_at is in
	// the database's future and which are therefore invisible to every worker
	// until the skew elapses. That failure is silent and looks like a stuck
	// queue, which is a miserable thing to debug.
	//
	// One extra round trip per bulk load, which is nothing next to the COPY
	// itself, and it makes EnqueueMany agree with Enqueue's COALESCE(..., now()).
	var serverNow time.Time
	if err := s.pool.QueryRow(ctx, `SELECT now()`).Scan(&serverNow); err != nil {
		return 0, fmt.Errorf("enqueue many: read server clock: %w", err)
	}

	rows := make([][]any, 0, len(params))
	for i, p := range params {
		if err := p.validate(); err != nil {
			return 0, fmt.Errorf("enqueue many: job %d: %w", i, err)
		}
		if p.IdempotencyKey != nil {
			return 0, fmt.Errorf("enqueue many: job %d: idempotency keys need Enqueue", i)
		}

		queue := p.Queue
		if queue == "" {
			queue = defaultQueue
		}
		payload := p.Payload
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		runAt := p.RunAt
		if runAt.IsZero() {
			runAt = serverNow
		}
		priority := 100
		if p.Priority != nil {
			priority = *p.Priority
		}
		maxAttempts := 5
		if p.MaxAttempts != nil {
			maxAttempts = *p.MaxAttempts
		}
		visibility := 60
		if p.VisibilityTimeout > 0 {
			visibility = max(1, int(p.VisibilityTimeout.Seconds()))
		}

		rows = append(rows, []any{
			queue, p.Kind, payload, priority, runAt, maxAttempts, visibility,
		})
	}

	n, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"jobs"},
		[]string{"queue", "kind", "payload", "priority", "run_at", "max_attempts", "visibility_timeout_seconds"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return 0, fmt.Errorf("enqueue many: %w", err)
	}
	for _, p := range params {
		q := p.Queue
		if q == "" {
			q = defaultQueue
		}
		metrics.JobsEnqueued.WithLabelValues(q, p.Kind).Inc()
	}
	return n, nil
}

const byIDSQL = `SELECT ` + jobColumns + ` FROM jobs WHERE id = $1`

// ErrJobNotFound is returned by lookups for an id that does not exist.
var ErrJobNotFound = errors.New("queue: job not found")

func (s *Store) JobByID(ctx context.Context, id int64) (Job, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, byIDSQL, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("job %d: %w", id, ErrJobNotFound)
	}
	if err != nil {
		return Job{}, fmt.Errorf("job %d: %w", id, err)
	}
	return job, nil
}

const attemptsSQL = `
SELECT id, job_id, attempt, worker_id, started_at, finished_at, error
FROM job_attempts WHERE job_id = $1 ORDER BY attempt ASC, id ASC`

func (s *Store) AttemptsByJobID(ctx context.Context, jobID int64) ([]Attempt, error) {
	rows, err := s.pool.Query(ctx, attemptsSQL, jobID)
	if err != nil {
		return nil, fmt.Errorf("attempts for job %d: %w", jobID, err)
	}
	defer rows.Close()

	var out []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.ID, &a.JobID, &a.Attempt, &a.WorkerID,
			&a.StartedAt, &a.FinishedAt, &a.Error); err != nil {
			return nil, fmt.Errorf("attempts for job %d: %w", jobID, err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullableBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
