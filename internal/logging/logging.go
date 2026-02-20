package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Runtime contains the initialized logger and the backing log file metadata.
type Runtime struct {
	Logger      *slog.Logger
	LogFilePath string
	file        *os.File
}

func (r Runtime) Close() error {
	if r.file == nil {
		return nil
	}
	return r.file.Close()
}

func New(level, logDir string) (Runtime, error) {
	return newWithOutputs(level, logDir, time.Now(), os.Stdout, os.Stderr)
}

func newWithOutputs(
	level string,
	logDir string,
	startup time.Time,
	stdout io.Writer,
	stderr io.Writer,
) (Runtime, error) {
	lvl := parseLevel(level)
	if lvl == nil {
		return Runtime{}, fmt.Errorf("unsupported log level %q", level)
	}

	dir := strings.TrimSpace(logDir)
	if dir == "" {
		return Runtime{}, fmt.Errorf("log directory is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Runtime{}, fmt.Errorf("create log directory %q: %w", dir, err)
	}

	logFilePath := filepath.Join(dir, startupLogFileName(startup))
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return Runtime{}, fmt.Errorf("open log file %q: %w", logFilePath, err)
	}

	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}

	options := &slog.HandlerOptions{Level: lvl}
	stdoutHandler := newLevelRangeHandler(
		slog.NewTextHandler(stdout, options),
		slog.LevelDebug,
		slog.LevelInfo,
		true,
	)
	stderrHandler := newLevelRangeHandler(
		slog.NewTextHandler(stderr, options),
		slog.LevelWarn,
		slog.Level(0),
		false,
	)
	fileHandler := slog.NewTextHandler(logFile, options)
	handler := fanoutHandler{handlers: []slog.Handler{stdoutHandler, stderrHandler, fileHandler}}

	return Runtime{
		Logger:      slog.New(handler),
		LogFilePath: logFilePath,
		file:        logFile,
	}, nil
}

func startupLogFileName(startup time.Time) string {
	return fmt.Sprintf("hdhriptv-%s.log", startup.Format("20060102-150405"))
}

func parseLevel(level string) *slog.LevelVar {
	v := new(slog.LevelVar)
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		v.Set(slog.LevelDebug)
	case "info", "":
		v.Set(slog.LevelInfo)
	case "warn":
		v.Set(slog.LevelWarn)
	case "error":
		v.Set(slog.LevelError)
	default:
		return nil
	}
	return v
}

type fanoutHandler struct {
	handlers []slog.Handler
}

func (h fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h fanoutHandler) Handle(ctx context.Context, record slog.Record) error {
	var firstErr error
	for _, handler := range h.handlers {
		if !handler.Enabled(ctx, record.Level) {
			continue
		}
		if err := handler.Handle(ctx, record); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (h fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	derived := make([]slog.Handler, 0, len(h.handlers))
	for _, handler := range h.handlers {
		derived = append(derived, handler.WithAttrs(attrs))
	}
	return fanoutHandler{handlers: derived}
}

func (h fanoutHandler) WithGroup(name string) slog.Handler {
	derived := make([]slog.Handler, 0, len(h.handlers))
	for _, handler := range h.handlers {
		derived = append(derived, handler.WithGroup(name))
	}
	return fanoutHandler{handlers: derived}
}

type levelRangeHandler struct {
	next   slog.Handler
	min    slog.Level
	max    slog.Level
	hasMax bool
}

func newLevelRangeHandler(next slog.Handler, min, max slog.Level, hasMax bool) *levelRangeHandler {
	return &levelRangeHandler{
		next:   next,
		min:    min,
		max:    max,
		hasMax: hasMax,
	}
}

func (h *levelRangeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if level < h.min {
		return false
	}
	if h.hasMax && level > h.max {
		return false
	}
	return h.next.Enabled(ctx, level)
}

func (h *levelRangeHandler) Handle(ctx context.Context, record slog.Record) error {
	if !h.Enabled(ctx, record.Level) {
		return nil
	}
	return h.next.Handle(ctx, record)
}

func (h *levelRangeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelRangeHandler{
		next:   h.next.WithAttrs(attrs),
		min:    h.min,
		max:    h.max,
		hasMax: h.hasMax,
	}
}

func (h *levelRangeHandler) WithGroup(name string) slog.Handler {
	return &levelRangeHandler{
		next:   h.next.WithGroup(name),
		min:    h.min,
		max:    h.max,
		hasMax: h.hasMax,
	}
}
