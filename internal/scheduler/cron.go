// Package scheduler turns cron expressions into enqueued jobs.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/robfig/cron/v3"

	"github.com/Sharvary-HH/queued/internal/queue"
)

// LockID is the advisory lock the scheduler leader holds. It must not collide
// with the migration runner's (8675309) or with anything an application using
// this database picks for its own advisory locks — Postgres has one namespace
// for all of them, and a collision looks like a scheduler that mysteriously
// never becomes leader.
const LockID int64 = 5150

// parser accepts standard five-field cron, an optional leading seconds field,
// and the @-descriptors.
//
// Seconds are supported mostly so that tests do not have to take a minute each,
// but they are genuinely useful and cost nothing: the parser handles both
// widths and the rest of the system never sees the difference.
var parser = cron.NewParser(
	cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// Parse validates a cron expression. Exported so the API can reject a bad
// expression at the point somebody types it, rather than having the scheduler
// discover it later and log about it forever.
func Parse(expr string) (cron.Schedule, error) {
	schedule, err := parser.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("scheduler: parse %q: %w", expr, err)
	}
	return schedule, nil
}

// NextRun is Parse plus one step, which is what callers creating an entry want.
func NextRun(expr string, from time.Time) (time.Time, error) {
	schedule, err := Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	return schedule.Next(from), nil
}

type Config struct {
	// Interval between ticks while leading, and between attempts to become
	// leader while not. It bounds how late a job can be, so it wants to be well
	// under the finest schedule granularity in use.
	Interval time.Duration

	// Batch caps how many due entries one tick processes.
	Batch int
}

func (c *Config) setDefaults() {
	if c.Interval <= 0 {
		c.Interval = time.Second
	}
	if c.Batch < 1 {
		c.Batch = 100
	}
}

// Scheduler enqueues recurring jobs when they come due.
//
// # Leader election
//
// Every queued instance runs one of these, and only one may act at a time or
// each tick produces one job per instance. Election is a Postgres advisory
// lock: whoever wins pg_try_advisory_lock leads, everybody else retries on the
// interval.
//
// Why an advisory lock is sufficient here:
//
//   - The database is already a hard dependency. Adding etcd or Consul to elect
//     a leader for a job that only matters when the database is up would be
//     adding a second thing that can fail to protect against the first one
//     failing.
//   - Session-scoped locks are released automatically when the connection ends,
//     for any reason — clean exit, crash, kill -9, network partition, the
//     server restarting. There is no lease to expire and no TTL to tune, and no
//     way to leave a lock held by a process that no longer exists.
//   - It is one function call against a connection we already have.
//
// # Its failure mode, stated plainly
//
// **The lock is released on connection loss, and release is not coordinated
// with the leader noticing.** If the network drops between this process and
// Postgres, the server tears the session down and frees the lock immediately,
// while this process may not find out until its next query. In that window a
// second instance can acquire the lock and start scheduling while the first
// still believes it leads. The same happens if the leader is paused long enough
// for TCP to give up — a stop-the-world GC pause, a hypervisor freeze, a
// suspended laptop.
//
// This is not a flaw in advisory locks; it is the standard result that a
// distributed lock without fencing cannot guarantee mutual exclusion of *work*,
// only of lock ownership. Anything that needs the guarantee must not rely on
// leadership alone.
//
// # So leadership is an optimisation, not the guarantee
//
// Correctness comes from the data, in three layers, and would hold with the
// election removed entirely:
//
//  1. Advancing an entry is a compare-and-swap on next_run_at, so only one
//     scheduler can move a given tick forward however many are running.
//  2. The enqueue selects from that update's output in the same statement, so a
//     scheduler that lost the swap inserts nothing.
//  3. The enqueued job carries an idempotency key derived from the entry and the
//     scheduled instant, so the unique index is a final backstop.
//
// The election exists to stop N instances all running the same queries every
// second, not to make the answer correct. The test for this starts three
// schedulers with no leader at all and still gets exactly one job per tick.
type Scheduler struct {
	store *queue.Store
	log   *slog.Logger
	cfg   Config
}

func New(store *queue.Store, log *slog.Logger, cfg Config) *Scheduler {
	cfg.setDefaults()
	return &Scheduler{store: store, log: log.With("component_role", "scheduler"), cfg: cfg}
}

// Run campaigns for leadership and schedules while it holds it, until ctx ends.
func (s *Scheduler) Run(ctx context.Context) {
	s.log.Info("scheduler started", "interval", s.cfg.Interval.String())
	defer s.log.Info("scheduler stopped")

	for ctx.Err() == nil {
		conn, err := s.campaign(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("could not campaign for leadership", "error", err)
			if !sleep(ctx, s.cfg.Interval) {
				return
			}
			continue
		}
		if conn == nil { // somebody else leads
			if !sleep(ctx, s.cfg.Interval) {
				return
			}
			continue
		}

		s.log.Info("became scheduler leader")
		s.lead(ctx, conn)
		s.log.Info("no longer scheduler leader")

		// Closing the connection releases the advisory lock, so a leader that
		// is shutting down hands over immediately instead of making the next
		// instance wait for a timeout.
		_ = conn.Close(context.WithoutCancel(ctx))
	}
}

// campaign returns a connection holding the lock, or nil if another instance
// has it. The lock is session-scoped and lives on this connection, so the
// connection is the leadership.
func (s *Scheduler) campaign(ctx context.Context) (*pgx.Conn, error) {
	// Its own connection, not one borrowed from the pool: the lock must be held
	// for as long as leadership lasts, and a pooled connection handed back to
	// somebody else would take the lock with it.
	conn, err := pgx.ConnectConfig(ctx, s.store.Pool().Config().ConnConfig)
	if err != nil {
		return nil, err
	}

	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, LockID).Scan(&acquired); err != nil {
		_ = conn.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	if !acquired {
		_ = conn.Close(context.WithoutCancel(ctx))
		return nil, nil
	}
	return conn, nil
}

// lead ticks until the context ends or the leadership connection dies.
func (s *Scheduler) lead(ctx context.Context, conn *pgx.Conn) {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// If this connection is gone the lock is gone with it, and somebody
		// else may already be leading. Step down and re-campaign rather than
		// carry on believing otherwise. This narrows the window described
		// above; it does not close it, which is why the CAS exists.
		if err := conn.Ping(ctx); err != nil {
			if ctx.Err() == nil {
				s.log.Warn("leadership connection lost, standing down", "error", err)
			}
			return
		}

		if err := s.tick(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("scheduler tick failed", "error", err)
		}
	}
}

// tick enqueues every entry that has come due.
func (s *Scheduler) tick(ctx context.Context) error {
	due, err := s.store.DueRecurring(ctx, s.cfg.Batch)
	if err != nil {
		return err
	}

	for _, entry := range due {
		if err := s.fire(ctx, entry); err != nil {
			// One broken entry must not stop the others. A typo in one cron
			// expression should not silently stop every other schedule in the
			// system, which is exactly what returning here would do.
			s.log.Error("could not schedule recurring job",
				"recurring_job", entry.Name, "cron", entry.Cron, "error", err)
		}
	}
	return nil
}

func (s *Scheduler) fire(ctx context.Context, entry queue.RecurringJob) error {
	// The instant this run stands for. Using the stored next_run_at rather than
	// now() means the idempotency key is the same for every scheduler that
	// looks at this entry, which is what makes the key a real backstop instead
	// of a per-process value that never collides.
	scheduledFor := entry.NextRunAt

	// Advance from now, not from scheduledFor. A scheduler that was down for an
	// hour with a per-minute schedule would otherwise owe sixty ticks and
	// deliver them all at once, which is almost never what anybody wants from a
	// cron: the point of "every minute" is freshness, and sixty stale runs are
	// worse than one fresh one. Missed ticks are dropped, deliberately.
	next, err := NextRun(entry.Cron, time.Now().UTC())
	if err != nil {
		return err
	}

	key := fmt.Sprintf("cron:%s:%d", entry.Name, scheduledFor.UTC().Unix())

	jobID, fired, err := s.store.AdvanceAndEnqueue(ctx, entry.ID, scheduledFor, next, key)
	if err != nil {
		return err
	}
	if !fired {
		// Another scheduler won the swap. Normal, and not worth a log line at
		// anything above debug: with three instances this happens twice per
		// tick forever.
		s.log.Debug("recurring job was already scheduled by another instance",
			"recurring_job", entry.Name, "scheduled_for", scheduledFor)
		return nil
	}

	s.log.Info("recurring job enqueued",
		"recurring_job", entry.Name,
		"job_id", jobID,
		"kind", entry.Kind,
		"scheduled_for", scheduledFor.UTC().Format(time.RFC3339),
		"next_run_at", next.UTC().Format(time.RFC3339),
	)
	return nil
}

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
