// Package migrate is a small forward-only migration runner.
//
// golang-migrate would also have done the job, but it is a dependency plus a
// CLI to install, and the whole thing it does for us fits in one file: sort the
// files, skip the ones already recorded, run the rest in a transaction. Doing
// it here also means every binary can migrate itself on boot, which is what
// makes `docker compose up` work with no manual psql step.
//
// Forward-only, deliberately: down migrations are mostly a lie in production
// (you cannot un-drop a column's data), and for local work `make down` throws
// the volume away, which is faster and more honest than a rollback path nobody
// tests.
package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// lockID is an arbitrary constant. Any two processes migrating the same
// database must pick the same number, and nothing else in the project may use
// it. The scheduler's advisory locks live in a different range (see
// internal/scheduler).
const lockID int64 = 8675309

type migration struct {
	version  int64
	name     string
	body     string
	checksum string
}

// Run applies every migration in fsys that has not been applied yet.
//
// It takes a session-level advisory lock first, so starting the API and three
// workers at the same instant does not have four processes racing to create the
// same table: the first one in migrates, the others block and then find there
// is nothing to do.
func Run(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, log *slog.Logger) error {
	migrations, err := load(fsys)
	if err != nil {
		return err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		// Best effort. If this fails the connection is broken anyway, and
		// Postgres drops session locks when the session ends.
		if _, err := conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, lockID); err != nil {
			log.Warn("releasing migration lock failed", "error", err)
		}
	}()

	if _, err := conn.Exec(ctx, createTable); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := loadApplied(ctx, conn.Conn())
	if err != nil {
		return err
	}

	var ran int
	for _, m := range migrations {
		if sum, ok := applied[m.version]; ok {
			// The file changed after it was applied. Every database in the
			// fleet is now on a different schema from what the repo describes,
			// so refuse rather than paper over it.
			if sum != m.checksum {
				return fmt.Errorf("migration %d (%s) was modified after it was applied: recorded %s, on disk %s",
					m.version, m.name, sum[:12], m.checksum[:12])
			}
			continue
		}

		if err := apply(ctx, conn.Conn(), m); err != nil {
			return err
		}
		log.Info("migration applied", "version", m.version, "name", m.name)
		ran++
	}

	if ran == 0 {
		log.Info("schema up to date", "version", migrations[len(migrations)-1].version)
	}
	return nil
}

const createTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    bigint PRIMARY KEY,
    name       text        NOT NULL,
    checksum   text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)`

func apply(ctx context.Context, conn *pgx.Conn, m migration) error {
	// One transaction per migration. Postgres has transactional DDL, so a
	// migration that fails halfway leaves nothing behind.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %d: %w", m.version, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, m.body); err != nil {
		return fmt.Errorf("migration %d (%s): %w", m.version, m.name, err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
		m.version, m.name, m.checksum)
	if err != nil {
		return fmt.Errorf("record %d: %w", m.version, err)
	}
	return tx.Commit(ctx)
}

func loadApplied(ctx context.Context, conn *pgx.Conn) (map[int64]string, error) {
	rows, err := conn.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int64]string)
	for rows.Next() {
		var v int64
		var sum string
		if err := rows.Scan(&v, &sum); err != nil {
			return nil, err
		}
		applied[v] = sum
	}
	return applied, rows.Err()
}

// load reads and sorts the migration files. Names look like 0001_init.sql; the
// number before the first underscore is the version.
func load(fsys fs.FS) ([]migration, error) {
	entries, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no migrations found")
	}

	out := make([]migration, 0, len(entries))
	seen := make(map[int64]string, len(entries))

	for _, name := range entries {
		base := path.Base(name)
		numeric, rest, found := strings.Cut(strings.TrimSuffix(base, ".sql"), "_")
		if !found {
			return nil, fmt.Errorf("migration %q: expected <version>_<name>.sql", base)
		}
		version, err := strconv.ParseInt(numeric, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("migration %q: bad version: %w", base, err)
		}
		if prev, dup := seen[version]; dup {
			return nil, fmt.Errorf("migrations %q and %q share version %d", prev, base, version)
		}
		seen[version] = base

		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", base, err)
		}
		sum := sha256.Sum256(body)

		out = append(out, migration{
			version:  version,
			name:     rest,
			body:     string(body),
			checksum: hex.EncodeToString(sum[:]),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}
