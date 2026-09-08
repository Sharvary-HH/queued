package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Sharvary-HH/queued/internal/api"
	"github.com/Sharvary-HH/queued/internal/config"
	"github.com/Sharvary-HH/queued/internal/logging"
	"github.com/Sharvary-HH/queued/internal/migrate"
	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/worker"
	"github.com/Sharvary-HH/queued/migrations"
)

// migrateOnly makes `queued -migrate` apply the schema and exit, which is what
// `make migrate` calls. Without the flag the server migrates on boot anyway.
var migrateOnly = flag.Bool("migrate", false, "apply migrations and exit")

func main() {
	flag.Parse()
	if err := run(); err != nil {
		// The logger may not exist yet if config failed, so use the default one.
		logging.New("info", "queued").Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel, "queued")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// The server serves HTTP and runs a reaper; it has no executors, so a
	// modest pool is plenty.
	maxConns := cfg.DBMaxConns
	if maxConns == 0 {
		maxConns = 10
	}

	pool, err := queue.Connect(ctx, cfg.DatabaseURL, maxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	log.Info("connected to postgres")

	// Migrating on every boot is safe because the runner takes an advisory lock
	// and skips what is already applied, and it means there is no ordering
	// requirement between this service and the workers in compose.
	if err := migrate.Run(ctx, pool, migrations.FS, log); err != nil {
		return err
	}
	if *migrateOnly {
		return nil
	}

	store := queue.NewStore(pool)

	// The server reaps too. If reclaiming only happened in worker processes,
	// then the one failure that most needs recovering from — every worker dying
	// at once — would be the one case with nobody left to do it.
	var reaperDone sync.WaitGroup
	reaperDone.Add(1)
	go func() {
		defer reaperDone.Done()
		worker.NewReaper(store, log, worker.ReaperConfig{Interval: cfg.ReapInterval}).Run(ctx)
	}()
	defer reaperDone.Wait()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewServer(store, log).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		log.Info("shutdown signal received, draining http")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.DrainTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	log.Info("shutdown complete")
	return nil
}
