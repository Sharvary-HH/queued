package testutil

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
)

// LogCapture collects log output so a test can assert on it.
//
// The shutdown sequence is a user-visible feature — the README shows the log
// lines as evidence that draining works — so the tests check that the lines are
// actually emitted rather than only that the database ended up tidy.
type LogCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func NewLogCapture() *LogCapture {
	return &LogCapture{}
}

// Logger returns a JSON logger writing into the capture, at debug level so
// nothing is filtered out before a test can look at it.
func (c *LogCapture) Logger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(c, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// Write satisfies io.Writer. slog handlers write from whichever goroutine
// logged, so this has to be safe for concurrent use.
func (c *LogCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *LogCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

func (c *LogCapture) Contains(substr string) bool {
	return strings.Contains(c.String(), substr)
}
