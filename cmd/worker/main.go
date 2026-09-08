package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sharvary-HH/queued/internal/config"
	"github.com/Sharvary-HH/queued/internal/logging"
	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/worker"
	"github.com/Sharvary-HH/queued/testdata/handlers"
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
	log := logging.New(cfg.LogLevel, "worker")

	// signal.NotifyContext is the whole of the signal handling: SIGTERM or
	// SIGINT cancels ctx, and every shutdown decision downstream hangs off
	// that one cancellation rather than off a signal channel being read in
	// several places.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pool, err := queue.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	reg := worker.NewRegistry()
	// The demo handlers. A real deployment registers its own here; this is the
	// only place in the project that knows what the jobs actually do.
	handlers.NewSet().Register(reg)

	w := worker.New(queue.NewStore(pool), reg, log, worker.Config{
		Queue:        cfg.Queue,
		WorkerID:     cfg.WorkerID,
		Concurrency:  cfg.Concurrency,
		ClaimBatch:   cfg.ClaimBatch,
		PollInterval: cfg.PollInterval,
		DrainTimeout: cfg.DrainTimeout,
	})
	return w.Run(ctx)
}
