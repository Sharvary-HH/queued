//go:build bench

// Package bench measures the things the README makes claims about.
//
// Behind a build tag because these take minutes and load a database hard; they
// have no business running during `go test ./...`. `make bench` sets the tag.
//
// They run against whatever TEST_DATABASE_URL points at — normally the compose
// Postgres, which has fsync on and default settings. That matters: the
// correctness tests use a throwaway container with fsync=off, which roughly
// doubles the write throughput and would make every number here a lie.
package bench

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sharvary-HH/queued/internal/migrate"
	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/migrations"
	"github.com/Sharvary-HH/queued/testdata/handlers"
)

func dsn(tb testing.TB) string {
	tb.Helper()
	v := os.Getenv("TEST_DATABASE_URL")
	if v == "" {
		tb.Skip("set TEST_DATABASE_URL (make up first) to run the benchmarks")
	}
	return v
}

func connect(tb testing.TB, maxConns int32) *pgxpool.Pool {
	tb.Helper()

	cfg, err := pgxpool.ParseConfig(dsn(tb))
	if err != nil {
		tb.Fatal(err)
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		tb.Fatal(err)
	}
	if err := migrate.Run(context.Background(), pool, migrations.FS, discard()); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(pool.Close)
	return pool
}

func reset(tb testing.TB, pool *pgxpool.Pool) {
	tb.Helper()
	if _, err := pool.Exec(context.Background(), `TRUNCATE jobs RESTART IDENTITY CASCADE`); err != nil {
		tb.Fatal(err)
	}
}

func seed(tb testing.TB, store *queue.Store, n int) {
	tb.Helper()

	const chunk = 20000
	for done := 0; done < n; done += chunk {
		size := min(chunk, n-done)
		batch := make([]queue.EnqueueParams, size)
		for i := range batch {
			batch[i] = queue.EnqueueParams{Kind: handlers.KindSucceed}
		}
		if _, err := store.EnqueueMany(context.Background(), batch); err != nil {
			tb.Fatal(err)
		}
	}
	// The planner needs current statistics or it will pick a plan for an empty
	// table and every number below measures the wrong thing.
	if _, err := store.Pool().Exec(context.Background(), `ANALYZE jobs`); err != nil {
		tb.Fatal(err)
	}
}

// ---- throughput ---------------------------------------------------------

// TestThroughput is the headline number: jobs per second end to end, with a
// handler that does nothing, at rising worker counts.
//
// A no-op handler on purpose. Any real handler measures the handler, and the
// question here is what the *queue* costs.
func TestThroughput(t *testing.T) {
	for _, workers := range []int{1, 4, 16, 64} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			// Each simulated worker gets its own pool of connections, as
			// separate processes would.
			pool := connect(t, int32(workers)+8)
			store := queue.NewStore(pool)
			reset(t, pool)

			const jobs = 20000
			seed(t, store, jobs)

			var (
				processed atomic.Int64
				latencies = make([][]time.Duration, workers)
				wg        sync.WaitGroup
			)

			start := time.Now()
			for w := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					id := fmt.Sprintf("bench-%d", w)
					var mine []time.Duration

					for {
						claimStart := time.Now()
						batch, err := store.Claim(context.Background(), "default", id, 10)
						mine = append(mine, time.Since(claimStart))
						if err != nil {
							t.Errorf("claim: %v", err)
							return
						}
						if len(batch) == 0 {
							latencies[w] = mine
							return
						}
						for _, job := range batch {
							// The no-op handler, then the completion write —
							// which is the other round trip a real job pays.
							if err := store.Complete(context.Background(), job.ID, id); err != nil {
								t.Errorf("complete: %v", err)
								return
							}
							processed.Add(1)
						}
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)

			var all []time.Duration
			for _, l := range latencies {
				all = append(all, l...)
			}

			rate := float64(processed.Load()) / elapsed.Seconds()
			p50, p95, p99 := percentiles(all)
			t.Logf("workers=%-3d %8.0f jobs/sec  claim p50=%-8s p95=%-8s p99=%-8s (%d jobs in %s)",
				workers, rate, p50, p95, p99, processed.Load(), elapsed.Round(time.Millisecond))
		})
	}
}

// TestBatchedVsSingleClaim quantifies the claim batching decision.
func TestBatchedVsSingleClaim(t *testing.T) {
	for _, size := range []int{1, 10} {
		t.Run(fmt.Sprintf("batch=%d", size), func(t *testing.T) {
			pool := connect(t, 16)
			store := queue.NewStore(pool)
			reset(t, pool)

			const jobs = 10000
			seed(t, store, jobs)

			const workers = 8
			var (
				processed atomic.Int64
				wg        sync.WaitGroup
			)

			start := time.Now()
			for w := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					id := fmt.Sprintf("bench-%d", w)
					for {
						batch, err := store.Claim(context.Background(), "default", id, size)
						if err != nil {
							t.Errorf("claim: %v", err)
							return
						}
						if len(batch) == 0 {
							return
						}
						for _, job := range batch {
							if err := store.Complete(context.Background(), job.ID, id); err != nil {
								t.Errorf("complete: %v", err)
								return
							}
							processed.Add(1)
						}
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)

			t.Logf("batch=%-3d %8.0f jobs/sec (%d jobs in %s)",
				size, float64(processed.Load())/elapsed.Seconds(), processed.Load(), elapsed.Round(time.Millisecond))
		})
	}
}

// TestClaimAtDepth answers whether the claim query still holds up with a
// million rows in the table — the question the partial index exists to answer.
func TestClaimAtDepth(t *testing.T) {
	pool := connect(t, 8)
	store := queue.NewStore(pool)
	ctx := context.Background()

	for _, depth := range []int{1000, 1000000} {
		reset(t, pool)
		seed(t, store, depth)

		var size, idxSize string
		if err := pool.QueryRow(ctx, `
			SELECT pg_size_pretty(pg_total_relation_size('jobs')),
			       pg_size_pretty(pg_relation_size('jobs_claim_idx'))`).Scan(&size, &idxSize); err != nil {
			t.Fatal(err)
		}

		// Warm, then measure a run of claims.
		var samples []time.Duration
		for i := range 200 {
			start := time.Now()
			batch, err := store.Claim(ctx, "default", "bench", 10)
			d := time.Since(start)
			if err != nil {
				t.Fatal(err)
			}
			if len(batch) == 0 {
				break
			}
			if i >= 20 { // discard the warm-up
				samples = append(samples, d)
			}
		}

		p50, p95, p99 := percentiles(samples)
		t.Logf("depth=%-8d table=%-8s claim_idx=%-8s  p50=%-8s p95=%-8s p99=%s",
			depth, size, idxSize, p50, p95, p99)

		// EXPLAIN returns the plan one line per row. Scanning a single row
		// gets only "Limit ..." and hides everything the plan is read for.
		rows, err := pool.Query(ctx, `
			EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
			SELECT id FROM jobs WHERE state = 'pending' AND queue = 'default' AND run_at <= now()
			ORDER BY priority, run_at FOR UPDATE SKIP LOCKED LIMIT 10`)
		if err != nil {
			t.Logf("explain: %v", err)
			continue
		}
		var plan []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, line)
		}
		rows.Close()
		t.Logf("depth=%d plan:\n%s", depth, strings.Join(plan, "\n"))
	}
}

func percentiles(d []time.Duration) (p50, p95, p99 time.Duration) {
	if len(d) == 0 {
		return 0, 0, 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	at := func(q float64) time.Duration {
		i := int(float64(len(d)-1) * q)
		return d[i].Round(time.Microsecond)
	}
	return at(0.50), at(0.95), at(0.99)
}
