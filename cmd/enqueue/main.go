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

// enqueue is the manual-testing CLI: it drops a job straight into the table
// without going through the HTTP API.
func main() {
	var (
		kind    = flag.String("kind", "", "handler kind to run (required)")
		payload = flag.String("payload", "{}", "JSON payload")
		q       = flag.String("queue", "default", "queue name")
		prio    = flag.Int("priority", 100, "priority, lower runs first")
		delay   = flag.Duration("delay", 0, "delay before the job becomes runnable")
		max     = flag.Int("max-attempts", 5, "attempts before the job goes to the DLQ")
		key     = flag.String("idempotency-key", "", "optional dedupe key")
		count   = flag.Int("n", 1, "how many copies to enqueue")
	)
	flag.Parse()

	if err := run(*kind, *payload, *q, *key, *prio, *max, *count, *delay); err != nil {
		fmt.Fprintln(os.Stderr, "enqueue:", err)
		os.Exit(1)
	}
}

func run(kind, payload, q, key string, prio, max, count int, delay time.Duration) error {
	if kind == "" {
		flag.Usage()
		return errors.New("-kind is required")
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := queue.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	// queue.Enqueue arrives in phase 2 together with the schema; there is
	// nothing to insert into yet, so fail loudly rather than pretend.
	return errors.New("enqueue is implemented in phase 2 (needs the jobs table)")
}
