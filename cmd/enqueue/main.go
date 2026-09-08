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
}

func run(opts options) error {
	if opts.kind == "" {
		flag.Usage()
		return errors.New("-kind is required")
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, err := queue.Connect(ctx, cfg.DatabaseURL, 2)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := queue.NewStore(pool)

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
