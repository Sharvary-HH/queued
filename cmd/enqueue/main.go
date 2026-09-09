package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Sharvary-HH/queued/internal/config"
	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/scheduler"
)

// enqueue is the manual-testing CLI: it drops jobs straight into the table
// without going through the HTTP API.
func main() {
	opts := options{}
	flag.StringVar(&opts.kind, "kind", "", "handler kind to run (required)")
	flag.StringVar(&opts.payload, "payload", "", "JSON payload")
	flag.StringVar(&opts.queue, "queue", "", "queue name")
	flag.StringVar(&opts.key, "idempotency-key", "", "dedupe key; a second enqueue with the same key is a no-op")
	flag.IntVar(&opts.priority, "priority", -1, "priority, lower runs first")
	flag.IntVar(&opts.maxAttempts, "max-attempts", -1, "attempts before the job goes to the DLQ")
	flag.IntVar(&opts.count, "n", 1, "how many to enqueue")
	flag.DurationVar(&opts.delay, "delay", 0, "delay before the job becomes runnable")
	flag.DurationVar(&opts.visibility, "visibility-timeout", 0, "how long a claim is honoured")
	flag.StringVar(&opts.cron, "cron", "", "register a recurring schedule instead of a single job")
	flag.StringVar(&opts.name, "name", "", "name of the recurring schedule (required with -cron)")
	flag.IntVar(&opts.rate, "rate", 0, "with -for: jobs per second to sustain")
	flag.DurationVar(&opts.duration, "for", 0, "keep enqueuing for this long (load generator)")
	flag.Parse()

	if err := run(opts); err != nil {
		fmt.Fprintln(os.Stderr, "enqueue:", err)
		os.Exit(1)
	}
}

type options struct {
	kind        string
	payload     string
	queue       string
	key         string
	priority    int
	maxAttempts int
	count       int
	delay       time.Duration
	visibility  time.Duration
	cron        string
	name        string
	rate        int
	duration    time.Duration
}

// generate is the load generator: a steady trickle rather than one enormous
// dump, so the dashboard shows a queue being worked rather than a spike that is
// gone before anyone looks.
func generate(ctx context.Context, store *queue.Store, params queue.EnqueueParams, opts options) error {
	rate := opts.rate
	if rate < 1 {
		rate = 50
	}

	// One batch per tick, ten ticks a second. Enqueueing one row at a time at
	// any real rate spends all its time on round trips.
	const ticksPerSecond = 10
	perTick := max(1, rate/ticksPerSecond)

	batch := make([]queue.EnqueueParams, perTick)
	for i := range batch {
		batch[i] = params
	}

	ticker := time.NewTicker(time.Second / ticksPerSecond)
	defer ticker.Stop()
	deadline := time.After(opts.duration)

	var total int64
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			fmt.Printf("enqueued %d jobs over %s (%.0f/sec)\n",
				total, time.Since(start).Round(time.Second), float64(total)/time.Since(start).Seconds())
			return nil
		case <-ticker.C:
			n, err := store.EnqueueMany(ctx, batch)
			if err != nil {
				return err
			}
			total += n
			if total%(int64(rate)*10) < int64(perTick) {
				fmt.Printf("%d jobs enqueued (%.0f/sec)\n", total, float64(total)/time.Since(start).Seconds())
			}
		}
	}
}

func run(opts options) error {
	if opts.kind == "" {
		flag.Usage()
		return errors.New("-kind is required")
	}
	if opts.cron != "" && opts.name == "" {
		return errors.New("-cron needs -name to identify the schedule")
	}
	if opts.count < 1 {
		return fmt.Errorf("-n must be >= 1, got %d", opts.count)
	}
	if opts.key != "" && opts.count > 1 {
		return errors.New("-idempotency-key with -n > 1 would enqueue exactly one job")
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// The load generator can be asked to run for a long time; everything else
	// is a single statement and wants a short leash.
	timeout := 5 * time.Minute
	if opts.duration > 0 {
		timeout = opts.duration + time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	pool, err := queue.Connect(ctx, cfg.DatabaseURL, 2)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := queue.NewStore(pool)

	// -cron registers a schedule; the scheduler in queued takes it from there.
	if opts.cron != "" {
		next, err := scheduler.NextRun(opts.cron, time.Now().UTC())
		if err != nil {
			return err
		}
		entry, err := store.UpsertRecurring(ctx, queue.RecurringParams{
			Name:      opts.name,
			Cron:      opts.cron,
			Queue:     opts.queue,
			Kind:      opts.kind,
			Payload:   []byte(opts.payload),
			Enabled:   true,
			NextRunAt: next,
		})
		if err != nil {
			return err
		}
		fmt.Printf("scheduled %q: %s runs %s, next at %s\n",
			entry.Name, entry.Kind, entry.Cron, entry.NextRunAt.Format(time.RFC3339))
		return nil
	}

	params := queue.EnqueueParams{
		Queue:             opts.queue,
		Kind:              opts.kind,
		Payload:           []byte(opts.payload),
		VisibilityTimeout: opts.visibility,
	}
	if opts.delay > 0 {
		params.RunAt = time.Now().Add(opts.delay)
	}
	if opts.priority >= 0 {
		params.Priority = &opts.priority
	}
	if opts.maxAttempts >= 1 {
		params.MaxAttempts = &opts.maxAttempts
	}
	if opts.key != "" {
		params.IdempotencyKey = &opts.key
	}

	if opts.duration > 0 {
		return generate(ctx, store, params, opts)
	}

	// One job goes through Enqueue so the idempotency key is honoured; a batch
	// goes over COPY, which is the point of using this to build a backlog.
	if opts.count == 1 {
		job, inserted, err := store.Enqueue(ctx, params)
		if err != nil {
			return err
		}
		if !inserted {
			fmt.Printf("job %d already existed for key %q (state %s)\n", job.ID, opts.key, job.State)
			return nil
		}
		fmt.Printf("enqueued job %d kind=%s queue=%s run_at=%s\n",
			job.ID, job.Kind, job.Queue, job.RunAt.Format(time.RFC3339))
		return nil
	}

	batch := make([]queue.EnqueueParams, opts.count)
	for i := range batch {
		batch[i] = params
	}

	start := time.Now()
	n, err := store.EnqueueMany(ctx, batch)
	if err != nil {
		return err
	}
	fmt.Printf("enqueued %d jobs kind=%s in %s\n", n, opts.kind, time.Since(start).Round(time.Millisecond))
	return nil
}
