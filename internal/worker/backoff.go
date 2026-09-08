package worker

import (
	"math/rand/v2"
	"time"
)

// Backoff computes how long a failed job waits before it becomes runnable
// again.
type Backoff struct {
	// Base is the first interval, doubled on each subsequent attempt.
	Base time.Duration
	// Max caps the interval. Without it, attempt 20 would schedule a retry
	// somewhere in the next century.
	Max time.Duration
}

// DefaultBackoff: a second, doubling, capped at five minutes. With the default
// budget of five attempts the whole sequence fits inside a couple of minutes,
// which is short enough that a transient upstream outage is ridden out and long
// enough that a hard-down upstream is not hammered.
var DefaultBackoff = Backoff{Base: time.Second, Max: 5 * time.Minute}

// Next returns the delay before the given attempt is retried.
//
// The shape is full jitter: the exponential curve sets the *ceiling*, and the
// actual delay is drawn uniformly from zero up to it.
//
// The jitter is not a nicety. The failure mode this queue is most likely to
// meet is a shared dependency going down, which fails every in-flight job at
// almost the same instant. Pure exponential backoff schedules every one of
// those retries for the same moment, so the recovering dependency is hit by the
// entire fleet simultaneously, falls over again, and the herd re-forms — now
// synchronised even more tightly than before. Spreading each retry uniformly
// across its window turns that spike into a flat arrival rate.
//
// Full jitter rather than the milder "half the interval plus jitter" because
// spread matters more here than any individual job's latency: nothing in this
// system cares whether one job waits 0.2s or 1.9s, and the thundering-herd
// protection is strictly better the wider the draw.
func (b Backoff) Next(attempt int) time.Duration {
	base := b.Base
	if base <= 0 {
		base = DefaultBackoff.Base
	}
	maximum := b.Max
	if maximum <= 0 {
		maximum = DefaultBackoff.Max
	}

	ceiling := base << shift(attempt)
	// Shifting past the width of the type wraps to something negative or tiny,
	// and a "retry" scheduled in the past defeats the whole mechanism.
	if ceiling <= 0 || ceiling > maximum {
		ceiling = maximum
	}

	// rand/v2's top-level functions are safe for concurrent use, so the pool's
	// goroutines can all call this without a mutex between them.
	return time.Duration(rand.Int64N(int64(ceiling)) + 1)
}

// RetryAt is Next expressed as a wall-clock time, which is what the store wants.
func (b Backoff) RetryAt(now time.Time, attempt int) time.Time {
	return now.Add(b.Next(attempt))
}

// shift converts an attempt number to an exponent, clamped so the shift itself
// cannot overflow. attempt 1 is the first failure, so it gets base * 2^0.
func shift(attempt int) int {
	const maxShift = 32
	switch {
	case attempt < 1:
		return 0
	case attempt > maxShift:
		return maxShift
	default:
		return attempt - 1
	}
}
