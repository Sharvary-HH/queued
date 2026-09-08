package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sharvary-HH/queued/internal/config"
	"github.com/Sharvary-HH/queued/internal/logging"
	"github.com/Sharvary-HH/queued/internal/queue"
)

func main() {
	if err := run(); err != nil {
		logging.New("info", "worker").Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel, "worker").With("worker_id", cfg.WorkerID)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pool, err := queue.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	log.Info("worker up",
		"queue", cfg.Queue,
		"concurrency", cfg.Concurrency,
		"claim_batch", cfg.ClaimBatch,
		"poll_interval", cfg.PollInterval.String(),
		"drain_timeout", cfg.DrainTimeout.String(),
	)

	// The pool itself is phase 3. Until then the process exists so that
	// compose, the healthcheck and the shutdown path can be exercised.
	<-ctx.Done()
	log.Info("shutdown signal received")
	return nil
}
