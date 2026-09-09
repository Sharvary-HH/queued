//go:build bench

package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/worker"
	"github.com/Sharvary-HH/queued/testdata/handlers"
)

// This file settles the LISTEN/NOTIFY versus polling question with numbers
// instead of the argument the README was making from first principles.
//
// The claim being tested is that they are not really alternatives, because they
// do not compete on the same axis:
//
//   - On a *saturated* queue, NOTIFY should be worth close to nothing. The
//     claimer reclaims a full batch and loops straight back round without ever
//     reaching the select, so the wakeup channel is never read.
//   - On an *idle* queue, NOTIFY should be worth almost everything, because the
//     alternative is waiting up to a full poll interval for work that is
//     already sitting there.
//
// If the first is wrong, the polling fallback is costing throughput and wants
// looking at. If the second is wrong, NOTIFY is not earning the dedicated
// connection it holds and could be deleted.

// timingHandler records when each job actually started executing, so latency
// can be measured end to end rather than inferred from the database.
type timingHandler struct {
	mu    sync.Mutex
	seen  map[string]time.Time
	ready chan struct{}
}

func newTimingHandler() *timingHandler {
	return &timingHandler{seen: make(map[string]time.Time), ready: make(chan struct{}, 1024)}
}

func (h *timingHandler) handle(_ context.Context, payload []byte) error {
	// Keyed on the decoded id, not on the raw bytes. Postgres stores payloads as
	// jsonb, which reformats them — `{"id":"lat-0"}` comes back as
	// `{"id": "lat-0"}` — so matching on the literal bytes never hits.
	var p struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	h.mu.Lock()
	h.seen[p.ID] = time.Now()
	h.mu.Unlock()

	select {
	case h.ready <- struct{}{}:
	default:
	}
	return nil
}

func (h *timingHandler) startedAt(key string) (time.Time, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	t, ok := h.seen[key]
	return t, ok
}

func poolFor(t *testing.T, store *queue.Store, notify bool, poll time.Duration, concurrency int) worker.Config {
	t.Helper()
	return worker.Config{
		Queue:         "default",
		WorkerID:      fmt.Sprintf("bench-notify-%v", notify),
		Concurrency:   concurrency,
		ClaimBatch:    10,
		PollInterval:  poll,
		DrainTimeout:  5 * time.Second,
		DisableNotify: !notify,
	}
}

// TestNotifyVsPollingThroughput drains a full backlog both ways.
//
// The expectation is a dead heat. A claimer that is never idle never reaches
// the point where a notification would tell it anything it does not already
// know.
func TestNotifyVsPollingThroughput(t *testing.T) {
	for _, notify := range []bool{false, true} {
		name := "polling-100ms"
		if notify {
			name = "notify"
		}

		t.Run(name, func(t *testing.T) {
			pool := connect(t, 24)
			store := queue.NewStore(pool)
			reset(t, pool)

			const jobs = 20000
			seed(t, store, jobs)

			h := newTimingHandler()
			reg := worker.NewRegistry()
			reg.MustRegister(kindSucceed, h.handle)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			p := worker.New(store, reg, discard(), poolFor(t, store, notify, 100*time.Millisecond, 8))
			done := make(chan struct{})
			go func() { defer close(done); _ = p.Run(ctx) }()

			start := time.Now()
			waitForCount(t, pool, jobs, 5*time.Minute)
			elapsed := time.Since(start)

			cancel()
			<-done

			t.Logf("%-14s %8.0f jobs/sec (%d jobs in %s)",
				name, float64(jobs)/elapsed.Seconds(), jobs, elapsed.Round(time.Millisecond))
		})
	}
}

// TestNotifyVsPollingLatency is the measurement that actually matters: how long
// a job sits in the table before anybody looks at it, on a queue that is
// otherwise empty.
//
// One job at a time, waiting for each to be picked up before enqueueing the
// next, so every measurement starts from a genuinely idle claimer.
func TestNotifyVsPollingLatency(t *testing.T) {
	for _, notify := range []bool{false, true} {
		name := "polling-100ms"
		if notify {
			name = "notify"
		}

		t.Run(name, func(t *testing.T) {
			pool := connect(t, 12)
			store := queue.NewStore(pool)
			reset(t, pool)

			h := newTimingHandler()
			reg := worker.NewRegistry()
			reg.MustRegister(kindSucceed, h.handle)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			p := worker.New(store, reg, discard(), poolFor(t, store, notify, 100*time.Millisecond, 4))
			done := make(chan struct{})
			go func() { defer close(done); _ = p.Run(ctx) }()

			// Let the pool settle and, when enabled, get its LISTEN established.
			// Measuring before the subscription exists would measure the poll
			// timer and call it NOTIFY.
			time.Sleep(2 * time.Second)

			const samples = 40
			latencies := make([]time.Duration, 0, samples)

			for i := range samples {
				key := fmt.Sprintf("lat-%d", i)

				enqueuedAt := time.Now()
				if _, _, err := store.Enqueue(ctx, queue.EnqueueParams{
					Kind:    kindSucceed,
					Payload: []byte(fmt.Sprintf(`{"id":%q}`, key)),
				}); err != nil {
					t.Fatal(err)
				}

				// Wait for this specific job to start.
				deadline := time.Now().Add(10 * time.Second)
				var startedAt time.Time
				for time.Now().Before(deadline) {
					if ts, ok := h.startedAt(key); ok {
						startedAt = ts
						break
					}
					time.Sleep(time.Millisecond)
				}
				if startedAt.IsZero() {
					t.Fatalf("job %d was never picked up", i)
				}

				latencies = append(latencies, startedAt.Sub(enqueuedAt))
				// Let the claimer go fully idle again before the next one.
				time.Sleep(150 * time.Millisecond)
			}

			cancel()
			<-done

			p50, p95, p99 := percentiles(latencies)
			t.Logf("%-14s enqueue-to-start  p50=%-10s p95=%-10s p99=%-10s (%d samples)",
				name, p50, p95, p99, len(latencies))
		})
	}
}

// waitForCount blocks until the table holds `want` succeeded jobs.
//
// Polled from the database rather than counted in the handler, because the
// handler returning is not the same as the job being recorded — and the
// completion write is part of what is being timed.
func waitForCount(t *testing.T, pool *pgxpool.Pool, want int, within time.Duration) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var n int
		err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM jobs WHERE state = 'succeeded'`).Scan(&n)
		if err != nil {
			t.Fatalf("count succeeded: %v", err)
		}
		if n >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d jobs to succeed", want)
}

const kindSucceed = handlers.KindSucceed
