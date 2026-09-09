package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const recurringColumns = `id, name, cron, queue, kind, payload, enabled,
	last_run_at, next_run_at, created_at, updated_at`

// RecurringParams describes a cron entry. NextRunAt is supplied by the caller
// because parsing the expression lives in internal/scheduler, and the store
// deliberately knows nothing about cron syntax.
type RecurringParams struct {
	Name      string
	Cron      string
	Queue     string
	Kind      string
	Payload   []byte
	Enabled   bool
	NextRunAt time.Time
}

const upsertRecurringSQL = `
INSERT INTO recurring_jobs (name, cron, queue, kind, payload, enabled, next_run_at)
VALUES ($1, $2, COALESCE($3, 'default'), $4, COALESCE($5, '{}'::jsonb), $6, $7)
ON CONFLICT (name) DO UPDATE SET
    cron        = EXCLUDED.cron,
    queue       = EXCLUDED.queue,
    kind        = EXCLUDED.kind,
    payload     = EXCLUDED.payload,
    enabled     = EXCLUDED.enabled,
    next_run_at = EXCLUDED.next_run_at
RETURNING ` + recurringColumns

// UpsertRecurring creates or replaces a cron entry, keyed by name.
//
// Upsert rather than insert because these are declarative: a deployment
// describes the schedules it wants and applying that twice should be the same
// as applying it once.
func (s *Store) UpsertRecurring(ctx context.Context, p RecurringParams) (RecurringJob, error) {
	if p.Name == "" {
		return RecurringJob{}, errors.New("queue: recurring job needs a name")
	}
	if p.Kind == "" {
		return RecurringJob{}, errors.New("queue: recurring job needs a kind")
	}
	if p.NextRunAt.IsZero() {
		return RecurringJob{}, errors.New("queue: recurring job needs next_run_at")
	}

	job, err := scanRecurring(s.pool.QueryRow(ctx, upsertRecurringSQL,
		p.Name, p.Cron, nullable(p.Queue), p.Kind, nullableBytes(p.Payload),
		p.Enabled, p.NextRunAt))
	if err != nil {
		return RecurringJob{}, fmt.Errorf("upsert recurring %q: %w", p.Name, err)
	}
	return job, nil
}

const dueRecurringSQL = `
SELECT ` + recurringColumns + `
FROM recurring_jobs
WHERE enabled AND next_run_at <= now()
ORDER BY next_run_at ASC
LIMIT $1`

// DueRecurring lists the entries whose next run has come around.
//
// No row locking here, because the lock is not what makes this safe — the
// compare-and-swap in AdvanceAndEnqueue is. Two schedulers reading the same due
// row is fine; only one of them can advance it.
func (s *Store) DueRecurring(ctx context.Context, limit int) ([]RecurringJob, error) {
	rows, err := s.pool.Query(ctx, dueRecurringSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("due recurring: %w", err)
	}
	defer rows.Close()

	var out []RecurringJob
	for rows.Next() {
		job, err := scanRecurring(rows)
		if err != nil {
			return nil, fmt.Errorf("due recurring: %w", err)
		}
		out = append(out, job)
	}
	return out, rows.Err()
}

// advanceAndEnqueueSQL is the whole of the duplicate protection, and it is
// worth reading carefully because it, not the leader election, is what makes
// the guarantee.
//
// The UPDATE is a compare-and-swap: it only matches if next_run_at is still the
// value the caller read. Whoever gets there first moves the tick forward and
// every other scheduler's UPDATE matches nothing. The INSERT selects from that
// UPDATE's output, so a scheduler that lost the swap inserts no rows — not
// because it checked and decided not to, but because there is nothing to select
// from.
//
// Both happen in one statement, so there is no window in which a scheduler has
// advanced the tick and then died before enqueueing the job it stood for.
//
// The idempotency key is the third layer, and costs nothing: it is derived from
// the entry name and the scheduled instant, so even two schedulers that somehow
// both won the swap would be writing the same key and the unique index would
// collapse them into one row.
const advanceAndEnqueueSQL = `
WITH advanced AS (
    UPDATE recurring_jobs SET
        last_run_at = now(),
        next_run_at = $3
    WHERE id = $1 AND next_run_at = $2 AND enabled
    RETURNING queue, kind, payload
)
INSERT INTO jobs (queue, kind, payload, idempotency_key)
SELECT queue, kind, payload, $4 FROM advanced
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id`

// AdvanceAndEnqueue moves a recurring entry to its next tick and enqueues the
// job for the tick it just left, atomically.
//
// Reports whether this caller was the one that did it. False means another
// scheduler got there first, which is a normal outcome and not an error.
func (s *Store) AdvanceAndEnqueue(
	ctx context.Context, id int64, expectedNextRun, newNextRun time.Time, idempotencyKey string,
) (int64, bool, error) {
	var jobID int64
	err := s.pool.QueryRow(ctx, advanceAndEnqueueSQL,
		id, expectedNextRun, newNextRun, idempotencyKey).Scan(&jobID)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("advance recurring %d: %w", id, err)
	}
	return jobID, true, nil
}

const listRecurringSQL = `SELECT ` + recurringColumns + ` FROM recurring_jobs ORDER BY name ASC`

func (s *Store) ListRecurring(ctx context.Context) ([]RecurringJob, error) {
	rows, err := s.pool.Query(ctx, listRecurringSQL)
	if err != nil {
		return nil, fmt.Errorf("list recurring: %w", err)
	}
	defer rows.Close()

	var out []RecurringJob
	for rows.Next() {
		job, err := scanRecurring(rows)
		if err != nil {
			return nil, fmt.Errorf("list recurring: %w", err)
		}
		out = append(out, job)
	}
	return out, rows.Err()
}

const setRecurringEnabledSQL = `
UPDATE recurring_jobs SET
    enabled     = $2,
    -- Re-enabling schedules the next run from now rather than from wherever the
    -- clock stopped, so a schedule that was off for a week does not come back
    -- believing it owes a week of ticks.
    next_run_at = CASE WHEN $2 AND NOT enabled THEN $3 ELSE next_run_at END
WHERE id = $1
RETURNING ` + recurringColumns

func (s *Store) SetRecurringEnabled(ctx context.Context, id int64, enabled bool, nextRunAt time.Time) (RecurringJob, error) {
	job, err := scanRecurring(s.pool.QueryRow(ctx, setRecurringEnabledSQL, id, enabled, nextRunAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return RecurringJob{}, fmt.Errorf("recurring job %d: %w", id, ErrJobNotFound)
	}
	if err != nil {
		return RecurringJob{}, fmt.Errorf("set recurring %d enabled: %w", id, err)
	}
	return job, nil
}

func scanRecurring(row pgx.Row) (RecurringJob, error) {
	var r RecurringJob
	err := row.Scan(&r.ID, &r.Name, &r.Cron, &r.Queue, &r.Kind, &r.Payload,
		&r.Enabled, &r.LastRunAt, &r.NextRunAt, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return RecurringJob{}, err
	}
	return r, nil
}
