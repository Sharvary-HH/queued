package testutil

import (
	"io"
	"log/slog"
)

// discardLogger keeps migration chatter out of the test output. Tests that care
// what was logged build their own handler.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
