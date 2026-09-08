// Package testutil brings up a real Postgres for tests.
//
// Nothing here is mocked on purpose. The properties this project claims are
// properties of Postgres row locking — SKIP LOCKED handing disjoint sets to
// concurrent transactions is the entire mechanism — and a fake implementing the
// interface would only ever prove that the fake agrees with itself.
package testutil

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Sharvary-HH/queued/internal/migrate"
	"github.com/Sharvary-HH/queued/migrations"
)

var (
	once     sync.Once
	adminDSN string
	adminErr error
)

// admin returns a DSN with rights to create databases. One container is started
// per test binary and reused; Ryuk removes it when the process exits.
//
// Setting TEST_DATABASE_URL skips the container entirely and points at an
// existing server, which is what the compose stack is for during a tight
// edit-test loop.
func admin(t *testing.T) string {
	t.Helper()
	once.Do(func() {
		if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
			adminDSN = dsn
			return
		}
		adminDSN, adminErr = startContainer()
	})
	if adminErr != nil {
		t.Fatalf("start postgres: %v", adminErr)
	}
	return adminDSN
}

func startContainer() (string, error) {
	// Deliberately not tied to a test's context: the container outlives the
	// test that happened to start it.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("queued"),
		tcpostgres.WithPassword("queued"),
		// fsync off costs durability, which a container that is deleted in a
		// few minutes did not have anyway, and buys a large speedup on the
		// write-heavy tests.
		testcontainers.WithCmd("postgres", "-c", "fsync=off", "-c", "max_connections=400"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(2*time.Minute),
		),
	)
	if err != nil {
		return "", err
	}
	return c.ConnectionString(ctx, "sslmode=disable")
}

// DB gives the test its own freshly migrated database.
//
// Per-test databases rather than per-test transactions, because the code under
// test opens its own connections and runs its own transactions; wrapping
// everything in one outer transaction would serialise exactly the concurrency
// being measured.
func DB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	base := admin(t)
	name := fmt.Sprintf("queued_test_%d_%d", time.Now().UnixNano(), rand.IntN(100000))

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect to admin database: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE DATABASE `+pgx.Identifier{name}.Sanitize()); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("create test database: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close admin connection: %v", err)
	}

	dsn, err := withDatabase(base, name)
	if err != nil {
		t.Fatalf("build test dsn: %v", err)
	}

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test dsn: %v", err)
	}
	// Sized like a real worker: an executor per slot, plus claimer, reaper, and
	// the connection the NOTIFY listener never gives back. pgx's default of
	// max(4, numCPU) is smaller than that at the concurrency these tests use,
	// and the resulting queueing shows up as tests that are mysteriously slow
	// rather than as any kind of error.
	poolCfg.MaxConns = 14

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	if err := migrate.Run(ctx, pool, migrations.FS, discardLogger()); err != nil {
		pool.Close()
		t.Fatalf("migrate test database: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		dropDatabase(base, name)
	})
	return pool
}

func dropDatabase(base, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close(ctx) }()
	// WITH (FORCE) kicks off connections a test leaked rather than hanging.
	_, _ = conn.Exec(ctx, `DROP DATABASE IF EXISTS `+pgx.Identifier{name}.Sanitize()+` WITH (FORCE)`)
}

func withDatabase(dsn, name string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	u.Path = "/" + strings.TrimPrefix(name, "/")
	return u.String(), nil
}
