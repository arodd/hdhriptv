package sqlite

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/jobs"
)

func TestIsSQLiteIOErrCode(t *testing.T) {
	t.Parallel()

	if !isSQLiteIOErrCode(10) {
		t.Fatalf("isSQLiteIOErrCode(10) = false, want true")
	}
	if !isSQLiteIOErrCode(778) {
		t.Fatalf("isSQLiteIOErrCode(778) = false, want true")
	}
	if isSQLiteIOErrCode(19) {
		t.Fatalf("isSQLiteIOErrCode(19) = true, want false")
	}
}

func TestIOERRDiagGuardRunOneShotByRunID(t *testing.T) {
	t.Parallel()

	guard := newIOERRDiagGuard(1*time.Second, 8*time.Second, 8)
	now := time.Unix(1_700_000_000, 0).UTC()

	allowed, reason := guard.allowAt(42, now)
	if !allowed {
		t.Fatalf("first run-scoped allowAt() denied: %s", reason)
	}
	if reason != "run_id_first_ioerr" {
		t.Fatalf("first run-scoped reason = %q, want %q", reason, "run_id_first_ioerr")
	}

	allowed, reason = guard.allowAt(42, now.Add(100*time.Millisecond))
	if allowed {
		t.Fatalf("second run-scoped allowAt() allowed, reason=%s", reason)
	}
	if reason != "run_id_already_captured" {
		t.Fatalf("second run-scoped reason = %q, want %q", reason, "run_id_already_captured")
	}

	allowed, reason = guard.allowAt(43, now.Add(200*time.Millisecond))
	if !allowed {
		t.Fatalf("different run-scoped allowAt() denied: %s", reason)
	}
}

func TestIOERRDiagGuardFallbackBackoffRateLimit(t *testing.T) {
	t.Parallel()

	guard := newIOERRDiagGuard(1*time.Second, 4*time.Second, 8)
	base := time.Unix(1_700_000_000, 0).UTC()

	allowed, reason := guard.allowAt(0, base)
	if !allowed {
		t.Fatalf("first fallback allowAt() denied: %s", reason)
	}
	if !strings.Contains(reason, "fallback_backoff=1s") {
		t.Fatalf("first fallback reason = %q, want fallback_backoff=1s", reason)
	}

	allowed, reason = guard.allowAt(0, base.Add(500*time.Millisecond))
	if allowed {
		t.Fatalf("second fallback allowAt() unexpectedly allowed: %s", reason)
	}
	if !strings.Contains(reason, "fallback_rate_limited_until=") {
		t.Fatalf("second fallback reason = %q, want fallback_rate_limited_until", reason)
	}

	allowed, reason = guard.allowAt(0, base.Add(1*time.Second))
	if !allowed {
		t.Fatalf("third fallback allowAt() denied: %s", reason)
	}
	if !strings.Contains(reason, "fallback_backoff=2s") {
		t.Fatalf("third fallback reason = %q, want fallback_backoff=2s", reason)
	}

	allowed, reason = guard.allowAt(0, base.Add(3*time.Second))
	if !allowed {
		t.Fatalf("fourth fallback allowAt() denied: %s", reason)
	}
	if !strings.Contains(reason, "fallback_backoff=4s") {
		t.Fatalf("fourth fallback reason = %q, want fallback_backoff=4s", reason)
	}
}

func TestCollectSQLiteIOERRDiagnosticBundleIncludesPragmasAndDBFileStats(t *testing.T) {
	t.Setenv(sqliteIOERRDiagCheckpointEnvVar, "")

	dbPath := filepath.Join(t.TempDir(), "ioerr-diag.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	ctx := jobs.WithRunMetadata(context.Background(), 88, jobs.JobPlaylistSync, jobs.TriggerManual)
	bundle := collectSQLiteIOERRDiagnosticBundle(
		ctx,
		store.db,
		sqliteDiagOptions{
			Phase:  "commit",
			DBPath: dbPath,
		},
		778,
		"SQLITE_IOERR_WRITE",
	)

	if bundle.RunID != 88 {
		t.Fatalf("bundle.RunID = %d, want 88", bundle.RunID)
	}
	if bundle.JobName != jobs.JobPlaylistSync {
		t.Fatalf("bundle.JobName = %q, want %q", bundle.JobName, jobs.JobPlaylistSync)
	}
	if bundle.TriggeredBy != jobs.TriggerManual {
		t.Fatalf("bundle.TriggeredBy = %q, want %q", bundle.TriggeredBy, jobs.TriggerManual)
	}
	if bundle.PragmaBusyTimeoutMS != runtimeBusyTimeoutMS {
		t.Fatalf("bundle.PragmaBusyTimeoutMS = %d, want %d", bundle.PragmaBusyTimeoutMS, runtimeBusyTimeoutMS)
	}
	if !strings.EqualFold(bundle.PragmaSynchronous, runtimeSynchronousPragm) {
		t.Fatalf("bundle.PragmaSynchronous = %q, want %q", bundle.PragmaSynchronous, runtimeSynchronousPragm)
	}
	if !strings.EqualFold(bundle.PragmaJournalMode, runtimeJournalModePragm) {
		t.Fatalf("bundle.PragmaJournalMode = %q, want %q", bundle.PragmaJournalMode, runtimeJournalModePragm)
	}
	if !bundle.DBFileStats.Exists {
		t.Fatalf("bundle.DBFileStats.Exists = false, want true")
	}
	if bundle.DBFileStats.Path != dbPath {
		t.Fatalf("bundle.DBFileStats.Path = %q, want %q", bundle.DBFileStats.Path, dbPath)
	}
	if bundle.Checkpoint.Enabled {
		t.Fatalf("bundle.Checkpoint.Enabled = true, want false by default")
	}
	if bundle.Checkpoint.Attempted {
		t.Fatalf("bundle.Checkpoint.Attempted = true, want false by default")
	}
}

func TestMaybeEmitSQLiteIOERRDiagnosticBundleWithGuardLogsOncePerRun(t *testing.T) {
	t.Setenv(sqliteIOERRDiagCheckpointEnvVar, "")

	dbPath := filepath.Join(t.TempDir(), "ioerr-diag-log.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	var buf bytes.Buffer
	previousDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(previousDefault)

	guard := newIOERRDiagGuard(1*time.Second, 4*time.Second, 8)
	ctx := jobs.WithRunMetadata(context.Background(), 55, jobs.JobPlaylistSync, jobs.TriggerManual)
	opts := sqliteDiagOptions{
		Phase:  "commit",
		DBPath: dbPath,
	}

	first := maybeEmitSQLiteIOERRDiagnosticBundleWithGuard(ctx, store.db, opts, 778, "SQLITE_IOERR_WRITE", guard)
	if !first.Emitted {
		t.Fatalf("first maybeEmitSQLiteIOERRDiagnosticBundleWithGuard() emitted=%t reason=%q, want true", first.Emitted, first.Reason)
	}

	second := maybeEmitSQLiteIOERRDiagnosticBundleWithGuard(ctx, store.db, opts, 778, "SQLITE_IOERR_WRITE", guard)
	if second.Emitted {
		t.Fatalf("second maybeEmitSQLiteIOERRDiagnosticBundleWithGuard() emitted=%t reason=%q, want false", second.Emitted, second.Reason)
	}

	logOutput := buf.String()
	if strings.Count(logOutput, "msg="+sqliteIOERRDiagEventPrefix) != 1 {
		t.Fatalf("expected exactly one %q log event, got output:\n%s", sqliteIOERRDiagEventPrefix, logOutput)
	}
	if !strings.Contains(logOutput, "run_id=55") {
		t.Fatalf("log output missing run_id field:\n%s", logOutput)
	}
}
