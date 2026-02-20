package logging

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewWithOutputsCreatesTimestampedLogFileAndFansOut(t *testing.T) {
	tmpDir := t.TempDir()
	startup := time.Date(2026, time.February, 7, 13, 14, 15, 0, time.UTC)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	runtime, err := newWithOutputs("info", tmpDir, startup, &stdout, &stderr)
	if err != nil {
		t.Fatalf("newWithOutputs() error = %v", err)
	}
	t.Cleanup(func() {
		_ = runtime.Close()
	})

	expectedPath := filepath.Join(tmpDir, "hdhriptv-20260207-131415.log")
	if runtime.LogFilePath != expectedPath {
		t.Fatalf("LogFilePath = %q, want %q", runtime.LogFilePath, expectedPath)
	}

	runtime.Logger.Info("info line", "component", "test")
	runtime.Logger.Warn("warn line", "component", "test")

	if err := runtime.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	stdoutOutput := stdout.String()
	if !strings.Contains(stdoutOutput, "info line") {
		t.Fatalf("stdout missing info line: %q", stdoutOutput)
	}
	if strings.Contains(stdoutOutput, "warn line") {
		t.Fatalf("stdout unexpectedly contains warn line: %q", stdoutOutput)
	}

	stderrOutput := stderr.String()
	if !strings.Contains(stderrOutput, "warn line") {
		t.Fatalf("stderr missing warn line: %q", stderrOutput)
	}
	if strings.Contains(stderrOutput, "info line") {
		t.Fatalf("stderr unexpectedly contains info line: %q", stderrOutput)
	}

	fileBytes, err := os.ReadFile(runtime.LogFilePath)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v", runtime.LogFilePath, err)
	}
	fileOutput := string(fileBytes)
	if !strings.Contains(fileOutput, "info line") {
		t.Fatalf("log file missing info line: %q", fileOutput)
	}
	if !strings.Contains(fileOutput, "warn line") {
		t.Fatalf("log file missing warn line: %q", fileOutput)
	}
}

func TestNewWithOutputsRejectsInvalidLogLevel(t *testing.T) {
	_, err := newWithOutputs("nope", t.TempDir(), time.Now(), ioDiscardBuffer{}, ioDiscardBuffer{})
	if err == nil {
		t.Fatal("expected error for invalid log level")
	}
}

type ioDiscardBuffer struct{}

func (ioDiscardBuffer) Write(p []byte) (n int, err error) {
	return len(p), nil
}
