package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds everything the binaries need. All of it comes from the
// environment so the same image works in compose and in prod.
type Config struct {
	DatabaseURL string

	// Server
	HTTPAddr string

	// Worker
	WorkerID      string
	Queue         string
	Concurrency   int
	ClaimBatch    int
	PollInterval  time.Duration
	DrainTimeout  time.Duration
	ReapInterval  time.Duration
	VisibilitySec int

	LogLevel string
}

func Load() (Config, error) {
	c := Config{
		DatabaseURL:   env("DATABASE_URL", "postgres://queued:queued@localhost:5432/queued?sslmode=disable"),
		HTTPAddr:      env("HTTP_ADDR", ":8080"),
		Queue:         env("QUEUE", "default"),
		LogLevel:      env("LOG_LEVEL", "info"),
		WorkerID:      env("WORKER_ID", ""),
		Concurrency:   8,
		ClaimBatch:    10,
		PollInterval:  100 * time.Millisecond,
		DrainTimeout:  30 * time.Second,
		ReapInterval:  5 * time.Second,
		VisibilitySec: 60,
	}

	var err error
	if c.Concurrency, err = envInt("CONCURRENCY", c.Concurrency); err != nil {
		return c, err
	}
	if c.ClaimBatch, err = envInt("CLAIM_BATCH", c.ClaimBatch); err != nil {
		return c, err
	}
	if c.VisibilitySec, err = envInt("VISIBILITY_TIMEOUT_SECONDS", c.VisibilitySec); err != nil {
		return c, err
	}
	if c.PollInterval, err = envDur("POLL_INTERVAL", c.PollInterval); err != nil {
		return c, err
	}
	if c.DrainTimeout, err = envDur("DRAIN_TIMEOUT", c.DrainTimeout); err != nil {
		return c, err
	}
	if c.ReapInterval, err = envDur("REAP_INTERVAL", c.ReapInterval); err != nil {
		return c, err
	}

	if c.WorkerID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "worker"
		}
		c.WorkerID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if c.Concurrency < 1 {
		return c, fmt.Errorf("CONCURRENCY must be >= 1, got %d", c.Concurrency)
	}
	if c.ClaimBatch < 1 {
		return c, fmt.Errorf("CLAIM_BATCH must be >= 1, got %d", c.ClaimBatch)
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envDur(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
