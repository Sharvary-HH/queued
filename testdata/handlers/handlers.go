// Package handlers is the set of job handlers the tests and the demo stack run.
//
// They are real handlers, not fixtures: the pool cannot tell them apart from
// anything an application would register. Each one exists because some property
// needs a job that behaves that way — a job that always fails is how you find
// out whether the DLQ is bounded, and a job that panics is how you find out
// whether one bad handler can take a worker down.
//
// It lives under testdata/ because that is where the project layout puts it.
// Worth knowing: the go tool skips testdata/ when expanding ./..., so this
// package is not built by `go build ./...` or checked by `go vet ./...`. It is
// compiled by the tests that import it, which is what keeps it honest.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Sharvary-HH/queued/internal/worker"
)

// The kinds, matching what gets written into jobs.kind.
const (
	KindSucceed = "succeed"
	KindFail    = "always-fails"
	KindFlaky   = "flaky"
	KindSlow    = "sleeps-past-timeout"
	KindPanic   = "panics"
	KindInvalid = "invalid-payload"
)

// Payload is what every handler here understands. All fields are optional.
type Payload struct {
	// ID groups calls that belong to the same logical job, which is how the
	// flaky handler knows it has seen this one before.
	ID string `json:"id,omitempty"`
	// Sleep is how long the slow handler pretends to work for.
	Sleep string `json:"sleep,omitempty"`
}

// Set holds the handlers and counts what they did, so a test can ask "how many
// times did this actually run" without instrumenting the pool.
type Set struct {
	// FlakyFailures is how many times the flaky handler fails a given ID before
	// it starts succeeding. Set before registering.
	FlakyFailures int
	// SlowDefault is how long the slow handler sleeps when the payload does not
	// say. Deliberately long enough to outlast a short visibility timeout.
	SlowDefault time.Duration

	mu    sync.Mutex
	runs  map[string]int
	seen  map[string]int
	order []string
}

func NewSet() *Set {
	return &Set{
		FlakyFailures: 2,
		SlowDefault:   5 * time.Second,
		runs:          make(map[string]int),
		seen:          make(map[string]int),
	}
}

// Register wires every handler into a registry.
func (s *Set) Register(reg *worker.Registry) {
	reg.MustRegister(KindSucceed, s.succeed)
	reg.MustRegister(KindFail, s.alwaysFails)
	reg.MustRegister(KindFlaky, s.flaky)
	reg.MustRegister(KindSlow, s.slow)
	reg.MustRegister(KindPanic, s.panics)
	reg.MustRegister(KindInvalid, s.invalidPayload)
}

// Runs reports how many times a kind's handler was entered. Note that this
// counts *executions*, not successes, which is the number the at-least-once
// tests care about.
func (s *Set) Runs(kind string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runs[kind]
}

// Order is the payload IDs in the order they were executed. Used to check that
// a worker kept going after a panic.
func (s *Set) Order() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.order...)
}

func (s *Set) record(kind string, p Payload) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[kind]++
	if p.ID != "" {
		s.order = append(s.order, p.ID)
	}
}

// succeed is the boring case, and the one the throughput numbers are measured
// with.
func (s *Set) succeed(_ context.Context, payload []byte) error {
	p, err := parse(payload)
	if err != nil {
		return err
	}
	s.record(KindSucceed, p)
	return nil
}

// alwaysFails is retryable every time, so it walks the whole retry ladder and
// ends in the DLQ. This is the job that proves retries are bounded.
func (s *Set) alwaysFails(_ context.Context, payload []byte) error {
	p, err := parse(payload)
	if err != nil {
		return err
	}
	s.record(KindFail, p)
	return errors.New("this handler always fails")
}

// flaky fails the first FlakyFailures times it sees an ID and succeeds after
// that — a stand-in for an upstream that is briefly unavailable. It is what
// makes retries worth having at all.
func (s *Set) flaky(_ context.Context, payload []byte) error {
	p, err := parse(payload)
	if err != nil {
		return err
	}
	s.record(KindFlaky, p)

	s.mu.Lock()
	s.seen[p.ID]++
	attempts := s.seen[p.ID]
	threshold := s.FlakyFailures
	s.mu.Unlock()

	if attempts <= threshold {
		return fmt.Errorf("upstream unavailable (failure %d of %d)", attempts, threshold)
	}
	return nil
}

// slow runs past a short visibility timeout. It watches its context, which is
// what a well-behaved long-running handler is supposed to do: the deadline it
// carries is the point past which the reaper may hand the job to somebody else,
// so continuing to work after it is at best pointless.
func (s *Set) slow(ctx context.Context, payload []byte) error {
	p, err := parse(payload)
	if err != nil {
		return err
	}
	s.record(KindSlow, p)

	d := s.SlowDefault
	if p.Sleep != "" {
		parsed, err := time.ParseDuration(p.Sleep)
		if err != nil {
			return worker.Permanent(fmt.Errorf("bad sleep %q: %w", p.Sleep, err))
		}
		d = parsed
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cancelled after running too long: %w", ctx.Err())
	}
}

// panics is the handler that must not be able to take the worker down with it.
func (s *Set) panics(_ context.Context, payload []byte) error {
	p, err := parse(payload)
	if err != nil {
		return err
	}
	s.record(KindPanic, p)

	var nothing map[string]string
	nothing["boom"] = "this is a nil map write" //nolint:staticcheck // deliberate
	return nil
}

// invalidPayload rejects its input permanently. Running it four more times
// would produce four more identical rejections, so it says so and goes straight
// to the DLQ.
func (s *Set) invalidPayload(_ context.Context, payload []byte) error {
	p, err := parse(payload)
	if err != nil {
		return err
	}
	s.record(KindInvalid, p)
	return worker.Permanent(errors.New("payload failed validation and will never pass"))
}

func parse(payload []byte) (Payload, error) {
	var p Payload
	if len(payload) == 0 {
		return p, nil
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		// A payload the handler cannot read is not going to become readable.
		return p, worker.Permanent(fmt.Errorf("unmarshal payload: %w", err))
	}
	return p, nil
}
