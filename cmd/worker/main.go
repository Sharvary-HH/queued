package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
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

	// One connection per executor reporting a result, plus the claimer, the
	// reaper, and the one the NOTIFY listener holds for its whole life. Sizing
	// this off Concurrency rather than taking pgx's numCPU default is what stops
	// a high-concurrency worker from quietly queueing on connection acquisition.
	maxConns := cfg.DBMaxConns
	if maxConns == 0 {
		maxConns = cfg.Concurrency + 4
	}

	pool, err := queue.Connect(ctx, cfg.DatabaseURL, maxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	reg := worker.NewRegistry()
	// The demo handlers. A real deployment registers its own here; this is the
	// only place in the project that knows what the jobs actually do.
	handlers.NewSet().Register(reg)

	store := queue.NewStore(pool)

	// Every worker runs a reaper. They do not coordinate and do not need to:
	// the sweep uses SKIP LOCKED, so concurrent reapers split the expired rows
	// rather than fight over them. Running one everywhere means the recovery
	// mechanism is not itself a single point of failure.
	var reaperDone sync.WaitGroup
	reaperDone.Add(1)
	go func() {
		defer reaperDone.Done()
		worker.NewReaper(store, log, worker.ReaperConfig{Interval: cfg.ReapInterval}).Run(ctx)
	}()

	w := worker.New(store, reg, log, worker.Config{
		Queue:        cfg.Queue,
		WorkerID:     cfg.WorkerID,
		Concurrency:  cfg.Concurrency,
		ClaimBatch:   cfg.ClaimBatch,
		PollInterval: cfg.PollInterval,
		DrainTimeout: cfg.DrainTimeout,
	})
	err = w.Run(ctx)
	reaperDone.Wait()
	return err
}
