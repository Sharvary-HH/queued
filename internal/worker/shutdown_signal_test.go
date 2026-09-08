package worker_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/testutil"
	"github.com/Sharvary-HH/queued/testdata/handlers"
)

// Property 3: no lost jobs on graceful shutdown.
//
// Everything else in this package cancels a context and calls that a shutdown.
// That tests the pool but not the thing the README actually promises, which is
// that sending SIGTERM to a worker is safe. Between the two sits
// signal.NotifyContext, the process's exit path, and whether the binary is
// wired up the way the pool expects — none of which a cancelled context
// exercises.
//
// So this one builds cmd/worker, runs it as a real process against the test
// database, and sends it a real signal.
func TestProperty3_RealSIGTERMDrainsInFlightJobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	pool, dsn := testutil.DBWithDSN(t)
	store := queue.NewStore(pool)

	// Five-second jobs, and a visibility timeout long enough that the reaper
	// could not be what rescues them. If they survive, it is because the worker
	// drained them.
	const inFlight = 4
	for i := range inFlight {
		if _, _, err := store.Enqueue(ctx, queue.EnqueueParams{
			Kind: handlers.KindSlow,
			Payload: payload(t, handlers.Payload{
				ID:    "slow-" + string(rune('a'+i)),
				Sleep: "5s",
			}),
			VisibilityTimeout: 5 * time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
	}

	bin := buildWorker(t)

	var out lockedBuffer
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+dsn,
		"QUEUE=default",
		"CONCURRENCY=4",
		"CLAIM_BATCH=10",
		"POLL_INTERVAL=50ms",
		"DRAIN_TIMEOUT=30s",
		"LOG_LEVEL=info",
	)
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}

	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	t.Cleanup(func() {
		// Belt and braces: if the test failed before signalling, do not leave a
		// worker running against a database that is about to be dropped.
		_ = cmd.Process.Kill()
	})

	// Wait until the work is genuinely under way.
	waitFor(t, 30*time.Second, "the worker to claim and start jobs", func() bool {
		return countState(t, pool, queue.StateClaimed) == inFlight
	})
	// The spec's timing: signal one second in, four seconds before they finish.
	time.Sleep(time.Second)

	signalledAt := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}

	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("worker exited with an error: %v\n%s", err, out.String())
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("worker did not exit within 60s of SIGTERM\n%s", out.String())
	}
	drain := time.Since(signalledAt)

	// It waited for the work instead of abandoning it...
	if drain < 3*time.Second {
		t.Errorf("worker exited %s after SIGTERM; it cannot have waited for 5s jobs", drain)
	}
	// ...and did not simply sit out the whole deadline either.
	if drain > 20*time.Second {
		t.Errorf("drain took %s, far longer than the work needed", drain)
	}

	if got := countState(t, pool, queue.StateSucceeded); got != inFlight {
		t.Errorf("%d jobs succeeded, want %d — in-flight work was lost", got, inFlight)
	}
	if got := countState(t, pool, queue.StateClaimed); got != 0 {
		t.Errorf("%d jobs stranded in claimed after a graceful shutdown", got)
	}

	// The sequence is a documented feature, so check it was actually logged.
	logs := out.String()
	for _, want := range []string{
		"shutdown: claimer stopped",
		"shutdown: all in-flight jobs finished",
		"shutdown complete",
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("shutdown log is missing %q\n%s", want, logs)
		}
	}
	t.Logf("drained %d in-flight jobs in %s after SIGTERM", inFlight, drain.Round(time.Millisecond))
}

// buildWorker compiles cmd/worker into a temp directory. Building rather than
// `go run` so that the signal reaches the worker itself: `go run` sits between
// the test and the process and complicates the signal's path.
func buildWorker(t *testing.T) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), "worker")
	// Tests run in internal/worker; the module root is two levels up.
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/worker")
	cmd.Dir = filepath.Join("..", "..")

	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build cmd/worker: %v\n%s", err, out)
	}
	return bin
}

func countState(t *testing.T, pool *pgxpool.Pool, state queue.State) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM jobs WHERE state = $1`, state).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", state, err)
	}
	return n
}

// lockedBuffer collects the child process's output. exec writes from its own
// goroutine while the test reads, so this needs a mutex.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
