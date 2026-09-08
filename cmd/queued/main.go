package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sharvary-HH/queued/internal/api"
	"github.com/Sharvary-HH/queued/internal/config"
	"github.com/Sharvary-HH/queued/internal/logging"
	"github.com/Sharvary-HH/queued/internal/migrate"
	"github.com/Sharvary-HH/queued/internal/queue"
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

	pool, err := queue.Connect(ctx, cfg.DatabaseURL)
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

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewServer(queue.NewStore(pool), log).Routes(),
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
