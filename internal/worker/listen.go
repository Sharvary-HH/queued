package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NotifyChannel is the Postgres channel the triggers in migration 0003 signal.
const NotifyChannel = "jobs_pending"

// listener turns Postgres NOTIFY into wakeups for the claimer.
//
// The tradeoff versus plain polling, since the project's position is that you
// need both:
//
//	Polling is the one that is actually reliable. It has no state, survives
//	  every kind of disconnect, and cannot miss work — worst case a job waits
//	  one interval. It just cannot be made fast without making it expensive:
//	  latency and idle query load are the same dial turned in opposite
//	  directions.
//
//	NOTIFY is the one that is actually fast. An idle worker blocks on its
//	  connection and is woken the instant a transaction commits, costing nothing
//	  in between. But it is fire-and-forget with no durability whatsoever:
//	  notifications sent while this worker's connection was down are simply
//	  gone. Nothing replays them. A network blip, a failover, a pooler
//	  recycling the connection — each one is a window in which jobs are
//	  enqueued and nobody is told.
//
// So neither is sufficient alone, and they fail in opposite directions, which
// is exactly what makes the pair work: NOTIFY collapses the common-case latency
// from one poll interval to microseconds, and the poll timer underneath is the
// backstop that guarantees a missed notification costs one interval of delay
// rather than a job that never runs. The queue is correct because of the
// polling and fast because of the NOTIFY.
//
// It also needs a connection of its own for the whole life of the process — a
// connection inside LISTEN is blocked and cannot serve queries — which is one
// more reason not to make it load-bearing.
type listener struct {
	pool    *pgxpool.Pool
	log     *slog.Logger
	wake    chan struct{}
	backoff Backoff
}

func newListener(pool *pgxpool.Pool, log *slog.Logger) *listener {
	return &listener{
		pool: pool,
		log:  log,
		// Capacity one, and sends are dropped when it is full. A wakeup is a
		// hint that carries no information beyond "look again", so ten thousand
		// of them mean exactly what one means, and a full buffer means the
		// claimer is already about to look.
		wake:    make(chan struct{}, 1),
		backoff: Backoff{Base: 250 * time.Millisecond, Max: 30 * time.Second},
	}
}

// C is the channel the claimer selects on.
func (l *listener) C() <-chan struct{} { return l.wake }

// run holds a connection in LISTEN until ctx is done, reconnecting with backoff
// when the connection drops.
//
// Every error path here is survivable by design: if this goroutine were to give
// up entirely, the pool would keep working on the poll timer alone, just with
// worse latency. That is the whole point of the fallback.
func (l *listener) run(ctx context.Context) {
	var attempt int
	for ctx.Err() == nil {
		err := l.listen(ctx)
		if ctx.Err() != nil {
			return
		}

		attempt++
		delay := l.backoff.Next(attempt)
		// Not an error log: the poll fallback means the queue is still draining
		// while this is broken.
		l.log.Warn("notify listener disconnected, falling back to polling",
			"error", err, "retry_in", delay.String(), "attempt", attempt)

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// listen opens its own connection rather than borrowing one from the query
// pool, and that is not a stylistic preference.
//
// A LISTEN connection is held for the entire life of the process — it sits
// blocked in WaitForNotification and is never given back. Taking it from the
// query pool therefore permanently removes one connection from the pool, which
// is survivable for a single worker and is not survivable in general: several
// pools sharing one pgxpool will each take one and hold it, and once the number
// of pools reaches MaxConns the pool is entirely consumed by listeners, no
// claim query can ever acquire a connection, and the whole thing deadlocks with
// no error anywhere. (Found exactly that way, by a test that ran twenty pools
// against one pgxpool of fourteen.)
//
// Its own connection also means the query pool can be sized purely for queries,
// and there is no risk of a connection that has been in LISTEN going back into
// general rotation still carrying that state.
func (l *listener) listen(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, l.pool.Config().ConnConfig)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return err
	}
	l.log.Info("listening for enqueue notifications", "channel", NotifyChannel)

	for {
		// Blocks until a notification arrives or ctx is done. Both a real
		// connection failure and shutdown come back as an error here; run()
		// tells them apart by looking at ctx.
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		l.signal()
	}
}

func (l *listener) signal() {
	select {
	case l.wake <- struct{}{}:
	default: // already pending; one wakeup is as good as two
	}
}
