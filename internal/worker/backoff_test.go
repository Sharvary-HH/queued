package worker_test

import (
	"testing"
	"time"

	"github.com/Sharvary-HH/queued/internal/worker"
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

func TestRetryAtIsInTheFuture(t *testing.T) {
	t.Parallel()
	now := time.Now()
	at := worker.DefaultBackoff.RetryAt(now, 3)
	if !at.After(now) {
		t.Errorf("retry scheduled at %s, which is not after %s", at, now)
	}
}
