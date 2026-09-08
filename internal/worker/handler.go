package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// HandlerFunc runs one job.
//
// The payload is the raw jsonb from the row; unmarshalling it is the handler's
// business, since the queue has no opinion about what is in there.
//
// The context carries the job's visibility timeout as a deadline. A handler
// that ignores it will be cancelled at the deadline and its job handed to
// somebody else, so long-running work must actually watch it.
//
// **Handlers must be idempotent.** This queue is at-least-once. See the README
// for why exactly-once is not on offer.
type HandlerFunc func(ctx context.Context, payload []byte) error

// PermanentError tells the pool not to bother retrying.
//
// The distinction the queue cares about is not "did it fail" but "would running
// it again plausibly produce a different result":
//
//	retryable — a timeout, a 503 from an upstream, a deadlock, a full disk.
//	  Nothing about the job is wrong; the world was temporarily unhelpful.
//	  These get the full retry budget with backoff.
//
//	permanent — the payload references a user that does not exist, or fails
//	  validation, or asks for a currency the system does not support. The next
//	  four attempts will fail identically. Burning the budget on them just
//	  delays the operator finding out by however long the backoff adds up to,
//	  and pollutes the metrics with failures that were never going to succeed.
//
// Handlers signal the second case by wrapping their error in Permanent().
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string {
	if e.Err == nil {
		return "permanent failure"
	}
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent marks err as not worth retrying. Returns nil for a nil error so
// `return worker.Permanent(doThing())` behaves.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// IsPermanent reports whether err is anywhere in the chain a PermanentError.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// ErrNoHandler is returned when a job names a kind this worker does not know.
//
// Deliberately *not* permanent. During a rolling deploy the old workers do not
// yet have the handler for a kind the new code enqueues; treating that as
// permanent would dead-letter every such job in the window between the first
// new enqueue and the last old worker going away. Retrying instead means the
// job waits, backs off, and eventually lands on a worker that knows what to do
// with it. If the kind really was a typo, the retry budget runs out and it
// reaches the DLQ a few minutes later, which is a much cheaper mistake than the
// other direction.
var ErrNoHandler = errors.New("worker: no handler registered for kind")

// Registry maps a job's kind to the function that runs it.
//
// Registration usually happens once at startup, but the lock is a real RWMutex
// rather than a "we promise not to" comment because Lookup is called from every
// pool goroutine on every job.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]HandlerFunc)}
}

// Register adds a handler. Registering the same kind twice is an error rather
// than a silent overwrite — it almost always means two packages both thought
// they owned a name, and the one that loses is decided by init order.
func (r *Registry) Register(kind string, fn HandlerFunc) error {
	if kind == "" {
		return errors.New("worker: handler kind must not be empty")
	}
	if fn == nil {
		return fmt.Errorf("worker: handler for %q is nil", kind)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[kind]; exists {
		return fmt.Errorf("worker: handler for %q is already registered", kind)
	}
	r.handlers[kind] = fn
	return nil
}

// MustRegister is Register for package-level wiring, where a duplicate name is
// a programming error and there is nobody to hand an error to.
func (r *Registry) MustRegister(kind string, fn HandlerFunc) {
	if err := r.Register(kind, fn); err != nil {
		panic(err)
	}
}

func (r *Registry) Lookup(kind string) (HandlerFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fn, ok := r.handlers[kind]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoHandler, kind)
	}
	return fn, nil
}

// Kinds lists what this worker can run, sorted. Logged at startup so an
// operator staring at a queue full of unclaimed work can see whether this
// worker was ever going to be the one to pick it up.
func (r *Registry) Kinds() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
