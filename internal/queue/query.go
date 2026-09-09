package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// JobFilter narrows a job listing. Every field is optional.
type JobFilter struct {
	State State
	Queue string
	Kind  string

	// Before is a keyset cursor: return jobs with a smaller id. Keyset rather
	// than OFFSET because OFFSET makes the database walk and discard every row
	// it skips, so page 500 costs five hundred times page 1 — and on a table
	// that is being inserted into while you page, OFFSET also silently skips
	// and repeats rows as the offsets shift underneath you.
	Before int64

	Limit int
}

const maxPageSize = 200

func (f *JobFilter) normalise() {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Limit > maxPageSize {
		f.Limit = maxPageSize
	}
}

// ListJobs returns a page of jobs, newest first, and the cursor for the next
// page (0 when there is no next page).
func (s *Store) ListJobs(ctx context.Context, f JobFilter) ([]Job, int64, error) {
	f.normalise()

	if f.State != "" && !f.State.Valid() {
		return nil, 0, fmt.Errorf("queue: unknown state %q", f.State)
	}

	// Built by hand rather than with a query builder, and the args are always
	// placeholders — no filter value is ever concatenated into the SQL.
	var (
		where []string
		args  []any
	)
	add := func(clause string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(clause, len(args)))
	}
	if f.State != "" {
		add("state = $%d", f.State)
	}
	if f.Queue != "" {
		add("queue = $%d", f.Queue)
	}
	if f.Kind != "" {
		add("kind = $%d", f.Kind)
	}
	if f.Before > 0 {
		add("id < $%d", f.Before)
	}

	sql := `SELECT ` + jobColumns + ` FROM jobs`
	if len(where) > 0 {
		sql += " WHERE " + strings.Join(where, " AND ")
	}
	// One more than asked for, so we can tell whether another page exists
	// without a second count query.
	args = append(args, f.Limit+1)
	sql += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs, err := collectJobs(rows)
	if err != nil {
		return nil, 0, fmt.Errorf("list jobs: %w", err)
	}

	var next int64
	if len(jobs) > f.Limit {
		jobs = jobs[:f.Limit]
		next = jobs[len(jobs)-1].ID
	}
	return jobs, next, nil
}

// QueueStat is one (queue, state) count.
type QueueStat struct {
	Queue string
	State State
	Count int64
}

const statsSQL = `
SELECT queue, state, count(*)
FROM jobs
GROUP BY queue, state
ORDER BY queue, state`

// Stats counts jobs by queue and state.
//
// This is a full aggregate over the table and it is the most expensive query in
// the project by a wide margin — it is the one thing here that does not scale
// with the size of the backlog but with the size of the history. Callers cache
// it (the dashboard and the metrics exporter share one refresh) rather than
// running it per request, and `docs` note it as the first thing to move to a
// summary table if the jobs table is ever allowed to grow without bound.
func (s *Store) Stats(ctx context.Context) ([]QueueStat, error) {
	rows, err := s.pool.Query(ctx, statsSQL)
	if err != nil {
		return nil, fmt.Errorf("stats: %w", err)
	}
	defer rows.Close()

	var out []QueueStat
	for rows.Next() {
		var st QueueStat
		if err := rows.Scan(&st.Queue, &st.State, &st.Count); err != nil {
			return nil, fmt.Errorf("stats: %w", err)
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// ThroughputPoint is the number of jobs that reached a terminal state in one
// bucket of time.
type ThroughputPoint struct {
	Minute    string
	Succeeded int64
	Failed    int64
}

// make_interval(mins => $1) rather than the ($1 || ' minutes')::interval trick.
// The string-concatenation form makes pgx infer $1 as text, because that is what
// || wants, and it then cannot encode a Go int into it:
//
//	unable to encode 60 into text format for text (OID 25)
//
// make_interval takes an integer, so the parameter has an honest type. The
// failure was invisible from the outside — the handler logs a warning and
// renders an empty chart, so the page returned 200 and simply said "no attempts
// finished in the last hour" while thousands were finishing every minute.
const throughputSQL = `
SELECT to_char(date_trunc('minute', finished_at), 'HH24:MI') AS minute,
       count(*) FILTER (WHERE error IS NULL)     AS succeeded,
       count(*) FILTER (WHERE error IS NOT NULL) AS failed
FROM job_attempts
WHERE finished_at > now() - make_interval(mins => $1)
GROUP BY 1
ORDER BY 1`

// Throughput reads the attempt history rather than the jobs table, because that
// is where the timestamps of individual executions live. A job's own row only
// remembers its latest state.
func (s *Store) Throughput(ctx context.Context, minutes int) ([]ThroughputPoint, error) {
	if minutes < 1 {
		minutes = 60
	}
	rows, err := s.pool.Query(ctx, throughputSQL, minutes)
	if err != nil {
		return nil, fmt.Errorf("throughput: %w", err)
	}
	defer rows.Close()

	var out []ThroughputPoint
	for rows.Next() {
		var p ThroughputPoint
		if err := rows.Scan(&p.Minute, &p.Succeeded, &p.Failed); err != nil {
			return nil, fmt.Errorf("throughput: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ErrNotCancellable is returned when a cancel targets a job that is not pending.
var ErrNotCancellable = errors.New("queue: only a pending job can be cancelled")

const cancelSQL = `
UPDATE jobs SET
    state      = 'cancelled',
    last_error = 'cancelled by operator'
WHERE id = $1 AND state = 'pending'
RETURNING ` + jobColumns

// Cancel stops a job that has not started.
//
// Only 'pending' is cancellable, deliberately. Cancelling a claimed job would
// mean telling a worker to stop something already running, and there is no
// channel to tell it on — the worker would finish and report, and the two
// writers would disagree about what happened. A job that must be stoppable
// mid-flight needs its handler to watch for that, which is application
// business, not the queue's.
func (s *Store) Cancel(ctx context.Context, jobID int64) (Job, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, cancelSQL, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		current, lookupErr := s.JobByID(ctx, jobID)
		if lookupErr != nil {
			return Job{}, lookupErr
		}
		return Job{}, fmt.Errorf("cancel job %d in state %q: %w", jobID, current.State, ErrNotCancellable)
	}
	if err != nil {
		return Job{}, fmt.Errorf("cancel job %d: %w", jobID, err)
	}
	return job, nil
}

const distinctKindsSQL = `SELECT DISTINCT kind FROM jobs ORDER BY kind`

// Kinds lists the job kinds present in the table, for the dashboard's filter
// dropdown.
func (s *Store) Kinds(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, distinctKindsSQL)
	if err != nil {
		return nil, fmt.Errorf("kinds: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}
