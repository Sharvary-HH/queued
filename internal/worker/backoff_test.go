package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/worker"
	"github.com/Sharvary-HH/queued/testdata/handlers"
)

// The ceiling doubles per attempt, and every draw stays inside it.
func TestBackoffCeilingGrowsExponentially(t *testing.T) {
	t.Parallel()
	b := worker.Backoff{Base: time.Second, Max: time.Hour}

	for attempt := 1; attempt <= 8; attempt++ {
		ceiling := time.Duration(1<<(attempt-1)) * time.Second
		for range 200 {
			d := b.Next(attempt)
			if d <= 0 {
				t.Fatalf("attempt %d: delay %s is not positive", attempt, d)
			}
			if d > ceiling {
				t.Fatalf("attempt %d: delay %s exceeds the ceiling %s", attempt, d, ceiling)
			}
		}
	}
}

// Full jitter is the point of the exercise: if every job in a mass failure got
// the same delay, they would all come back at the same instant and knock the
// recovering dependency over again.
func TestBackoffAppliesJitter(t *testing.T) {
	t.Parallel()
	b := worker.Backoff{Base: time.Second, Max: time.Hour}

	const draws = 500
	seen := make(map[time.Duration]int, draws)
	var lo, hi time.Duration = time.Hour, 0

	for range draws {
		d := b.Next(6) // ceiling 32s, plenty of room to spread
		seen[d]++
		if d < lo {
			lo = d
		}
		if d > hi {
			hi = d
		}
	}

	if len(seen) < draws/2 {
		t.Fatalf("only %d distinct delays out of %d draws; the jitter is not doing anything", len(seen), draws)
	}
	// Uniform over [0, 32s) should comfortably reach both ends.
	ceiling := 32 * time.Second
	if lo > ceiling/4 {
		t.Errorf("smallest of %d draws was %s; the low end is not being sampled", draws, lo)
	}
	if hi < ceiling*3/4 {
		t.Errorf("largest of %d draws was %s; the high end is not being sampled", draws, hi)
	}
}

func TestBackoffRespectsMax(t *testing.T) {
	t.Parallel()
	b := worker.Backoff{Base: time.Second, Max: 5 * time.Second}

	for attempt := 1; attempt <= 40; attempt++ {
		for range 50 {
			if d := b.Next(attempt); d > 5*time.Second {
				t.Fatalf("attempt %d: delay %s exceeds max 5s", attempt, d)
			}
		}
	}
}

// A large attempt number must not shift the base into oblivion and produce a
// zero or negative delay — a "retry" scheduled in the past would spin.
func TestBackoffSurvivesAbsurdAttemptNumbers(t *testing.T) {
	t.Parallel()
	b := worker.Backoff{Base: time.Second, Max: time.Minute}

	for _, attempt := range []int{-5, 0, 62, 63, 64, 1 << 20} {
		d := b.Next(attempt)
		if d <= 0 || d > time.Minute {
			t.Errorf("attempt %d gave %s, want something in (0, 1m]", attempt, d)
		}
	}
}

func TestBackoffZeroValueUsesDefaults(t *testing.T) {
	t.Parallel()
	var b worker.Backoff

	d := b.Next(1)
	if d <= 0 || d > worker.DefaultBackoff.Max {
		t.Errorf("zero-value backoff gave %s", d)
	}
}

// The unit tests above check the function. This one checks that the delay
// actually reaches the database and that run_at moves the way it should, since
// a correct backoff wired up wrongly buys nothing.
func TestBackoffReachesRunAt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newHarness(t)

	maxAttempts := 6
	job := h.enqueue(t, queue.EnqueueParams{Kind: handlers.KindFail, MaxAttempts: &maxAttempts})
	b := worker.Backoff{Base: time.Second, Max: time.Hour}

	var delays []time.Duration
	for attempt := 1; attempt < maxAttempts; attempt++ {
		claimed, err := h.store.Claim(ctx, "default", "w", 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) != 1 {
			t.Fatalf("attempt %d: nothing claimable", attempt)
		}

		now := time.Now()
		if _, err := h.store.Fail(ctx, queue.FailRequest{
			JobID:    job.ID,
			WorkerID: "w",
			Err:      "nope",
			RetryAt:  b.RetryAt(now, attempt),
		}); err != nil {
			t.Fatal(err)
		}

		current, err := h.store.JobByID(ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		delay := current.RunAt.Sub(now)
		delays = append(delays, delay)

		ceiling := time.Duration(1<<(attempt-1)) * time.Second
		if delay <= 0 || delay > ceiling+time.Second { // slack for the round trip
			t.Errorf("attempt %d: run_at is %s away, want (0, %s]", attempt, delay, ceiling)
		}

		// Put it back so the next attempt can claim it.
		if _, err := h.pool.Exec(ctx, `UPDATE jobs SET run_at = now() WHERE id = $1`, job.ID); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("observed delays: %v", delays)
}

// Mass failure is the case backoff jitter exists for. Fail a hundred jobs at
// the same attempt number and their retries must not all land at the same
// instant.
func TestBackoffSpreadsMassFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newHarness(t)

	const total = 100
	batch := make([]queue.EnqueueParams, total)
	for i := range batch {
		batch[i] = queue.EnqueueParams{Kind: handlers.KindFail}
	}
	if _, err := h.store.EnqueueMany(ctx, batch); err != nil {
		t.Fatal(err)
	}

	claimed, err := h.store.Claim(ctx, "default", "w", total)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != total {
		t.Fatalf("claimed %d, want %d", len(claimed), total)
	}

	b := worker.Backoff{Base: 10 * time.Second, Max: time.Hour}
	now := time.Now()
	for _, job := range claimed {
		if _, err := h.store.Fail(ctx, queue.FailRequest{
			JobID: job.ID, WorkerID: "w", Err: "upstream down", RetryAt: b.RetryAt(now, job.Attempt),
		}); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := h.pool.Query(ctx, `SELECT DISTINCT run_at FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var distinct int
	for rows.Next() {
		distinct++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	// Uniform over a 10s window at microsecond resolution: collisions are
	// vanishingly unlikely, so anything close to 100 is right and a handful
	// would mean the jitter is not there.
	if distinct < total/2 {
		t.Fatalf("%d jobs failed together produced only %d distinct run_at values; "+
			"they would all retry at once", total, distinct)
	}
	t.Logf("%d jobs produced %d distinct retry times", total, distinct)
}

func TestRetryAtIsInTheFuture(t *testing.T) {
	t.Parallel()
	now := time.Now()
	at := worker.DefaultBackoff.RetryAt(now, 3)
	if !at.After(now) {
		t.Errorf("retry scheduled at %s, which is not after %s", at, now)
	}
}
