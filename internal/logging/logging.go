package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns the JSON-to-stdout logger every binary uses. Keeping it in one
// place means the dashboard, the workers and the scheduler all produce lines
// that can be grepped the same way.
func New(level, component string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(level)})
	return slog.New(h).With("component", component)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
