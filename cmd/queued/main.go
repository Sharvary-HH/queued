package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sharvary-HH/queued/internal/api"
	"github.com/Sharvary-HH/queued/internal/config"
	"github.com/Sharvary-HH/queued/internal/logging"
	"github.com/Sharvary-HH/queued/internal/queue"
)

func main() {
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
