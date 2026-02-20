package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/jobs"
)

func TestLoadSQLiteIOERRTraceConfigFromEnvDefaults(t *testing.T) {
	t.Setenv(sqliteIOERRTraceEnabledEnvVar, "")
	t.Setenv(sqliteIOERRTraceRingSizeEnvVar, "")
	t.Setenv(sqliteIOERRTraceDumpLimitEnvVar, "")
	t.Setenv(sqliteIOERRTraceDumpIntervalEnvVar, "")

	cfg := loadSQLiteIOERRTraceConfigFromEnv()
	if cfg.Enabled {
		t.Fatalf("cfg.Enabled = true, want false by default")
	}
	if cfg.RingSize != defaultSQLiteIOERRTraceRingSize {
		t.Fatalf("cfg.RingSize = %d, want %d", cfg.RingSize, defaultSQLiteIOERRTraceRingSize)
	}
	if cfg.DumpLimit != defaultSQLiteIOERRTraceDumpLimit {
		t.Fatalf("cfg.DumpLimit = %d, want %d", cfg.DumpLimit, defaultSQLiteIOERRTraceDumpLimit)
	}
	if cfg.DumpInterval != defaultSQLiteIOERRTraceDumpInterval {
		t.Fatalf("cfg.DumpInterval = %s, want %s", cfg.DumpInterval, defaultSQLiteIOERRTraceDumpInterval)
	}
}

func TestLoadSQLiteIOERRTraceConfigFromEnvBounds(t *testing.T) {
	t.Setenv(sqliteIOERRTraceEnabledEnvVar, "true")
	t.Setenv(sqliteIOERRTraceRingSizeEnvVar, "8")
	t.Setenv(sqliteIOERRTraceDumpLimitEnvVar, "99999")
	t.Setenv(sqliteIOERRTraceDumpIntervalEnvVar, "1m15s")

	cfg := loadSQLiteIOERRTraceConfigFromEnv()
	if !cfg.Enabled {
		t.Fatalf("cfg.Enabled = false, want true")
	}
	if cfg.RingSize != minSQLiteIOERRTraceRingSize {
		t.Fatalf("cfg.RingSize = %d, want %d (clamped min)", cfg.RingSize, minSQLiteIOERRTraceRingSize)
	}
	if cfg.DumpLimit != cfg.RingSize {
		t.Fatalf("cfg.DumpLimit = %d, want ring-size clamp %d", cfg.DumpLimit, cfg.RingSize)
	}
	if cfg.DumpInterval != 75*time.Second {
		t.Fatalf("cfg.DumpInterval = %s, want 1m15s", cfg.DumpInterval)
	}
}

func TestSQLiteIOERRTraceRingRetainsMostRecentEntries(t *testing.T) {
	ring := newSQLiteIOERRTraceRing(sqliteIOERRTraceConfig{
		Enabled:      true,
		RingSize:     3,
		DumpLimit:    3,
		DumpInterval: 0,
	})
	if ring == nil {
		t.Fatal("newSQLiteIOERRTraceRing() = nil, want non-nil")
	}

	for i := 0; i < 5; i++ {
		ring.record(
			fmt.Sprintf("op-%d", i),
			fmt.Sprintf("phase-%d", i),
			time.Now().UnixNano(),
			nil,
		)
	}

	entries, reason, allowed := ring.snapshotForDumpAt(time.Unix(1_700_000_000, 0).UTC())
	if !allowed {
		t.Fatalf("snapshotForDumpAt() denied: %s", reason)
	}
	if len(entries) != 3 {
		t.Fatalf("len(entries) = %d, want 3", len(entries))
	}
	if entries[0].Operation != "op-2" || entries[1].Operation != "op-3" || entries[2].Operation != "op-4" {
		t.Fatalf("unexpected operation order: %#v", []string{entries[0].Operation, entries[1].Operation, entries[2].Operation})
	}
}

func TestSQLiteIOERRTraceRingDumpRateLimit(t *testing.T) {
	ring := newSQLiteIOERRTraceRing(sqliteIOERRTraceConfig{
		Enabled:      true,
		RingSize:     8,
		DumpLimit:    4,
		DumpInterval: 2 * time.Second,
	})
	if ring == nil {
		t.Fatal("newSQLiteIOERRTraceRing() = nil, want non-nil")
	}

	ring.record("playlist_sync_write", "upsert_item", time.Now().UnixNano(), nil)

	base := time.Unix(1_700_000_000, 0).UTC()
	if _, reason, allowed := ring.snapshotForDumpAt(base); !allowed {
		t.Fatalf("first dump denied: %s", reason)
	}

	if _, reason, allowed := ring.snapshotForDumpAt(base.Add(1 * time.Second)); allowed {
		t.Fatalf("second dump unexpectedly allowed: %s", reason)
	} else if !strings.Contains(reason, "trace_rate_limited_until=") {
		t.Fatalf("second dump reason = %q, want trace_rate_limited_until", reason)
	}

	if _, reason, allowed := ring.snapshotForDumpAt(base.Add(2 * time.Second)); !allowed {
		t.Fatalf("third dump denied: %s", reason)
	}
}

func TestMaybeEmitSQLiteIOERRTraceDumpLogsEvent(t *testing.T) {
	ring := newSQLiteIOERRTraceRing(sqliteIOERRTraceConfig{
		Enabled:      true,
		RingSize:     8,
		DumpLimit:    4,
		DumpInterval: 30 * time.Second,
	})
	if ring == nil {
		t.Fatal("newSQLiteIOERRTraceRing() = nil, want non-nil")
	}
	ring.record("playlist_sync_write", "commit", time.Now().UnixNano(), nil)
	ring.record("playlist_sync_write", "mark_inactive", time.Now().UnixNano(), fmt.Errorf("wrapped: %w", context.Canceled))

	var logBuf bytes.Buffer
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	defer slog.SetDefault(prevDefault)

	ctx := jobs.WithRunMetadata(context.Background(), 64, jobs.JobPlaylistSync, jobs.TriggerManual)
	first := maybeEmitSQLiteIOERRTraceDump(ctx, ring, 778, "SQLITE_IOERR_WRITE")
	if !first.Emitted {
		t.Fatalf("first maybeEmitSQLiteIOERRTraceDump() emitted=%t reason=%q, want true", first.Emitted, first.Reason)
	}
	if first.EntryCount != 2 {
		t.Fatalf("first maybeEmitSQLiteIOERRTraceDump() entryCount=%d, want 2", first.EntryCount)
	}

	second := maybeEmitSQLiteIOERRTraceDump(ctx, ring, 778, "SQLITE_IOERR_WRITE")
	if second.Emitted {
		t.Fatalf("second maybeEmitSQLiteIOERRTraceDump() emitted=%t reason=%q, want false", second.Emitted, second.Reason)
	}

	output := logBuf.String()
	if strings.Count(output, "msg="+sqliteIOERRTraceEventPrefix) != 1 {
		t.Fatalf("expected one %q event, got:\n%s", sqliteIOERRTraceEventPrefix, output)
	}
	requiredFragments := []string{
		"run_id=64",
		"job_name=playlist_sync",
		"triggered_by=manual",
		"entry_count=2",
		"sqlite_code=778",
		"sqlite_code_name=SQLITE_IOERR_WRITE",
		`decision_reason="trace_dump_limit=2"`,
	}
	for _, fragment := range requiredFragments {
		if !strings.Contains(output, fragment) {
			t.Fatalf("trace dump output missing fragment %q:\n%s", fragment, output)
		}
	}
}

func TestSQLiteIOERRTraceRingAppendAllocs(t *testing.T) {
	ring := newSQLiteIOERRTraceRing(sqliteIOERRTraceConfig{
		Enabled:      true,
		RingSize:     256,
		DumpLimit:    64,
		DumpInterval: 0,
	})
	if ring == nil {
		t.Fatal("newSQLiteIOERRTraceRing() = nil, want non-nil")
	}

	allocs := testing.AllocsPerRun(1000, func() {
		start := time.Now().UnixNano()
		ring.record("playlist_sync_write", "upsert_item", start, nil)
	})
	if allocs != 0 {
		t.Fatalf("sqlite IOERR trace append allocs/run = %f, want 0", allocs)
	}
}

func BenchmarkSQLiteIOERRTraceRingRecord(b *testing.B) {
	ring := newSQLiteIOERRTraceRing(sqliteIOERRTraceConfig{
		Enabled:      true,
		RingSize:     1024,
		DumpLimit:    64,
		DumpInterval: 0,
	})
	if ring == nil {
		b.Fatal("newSQLiteIOERRTraceRing() = nil")
	}

	b.Run("ok", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now().UnixNano()
			ring.record("playlist_sync_write", "upsert_item", start, nil)
		}
	})

	sqliteErr := benchmarkSQLiteIOERRTraceError(b)
	b.Run("sqlite_err", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now().UnixNano()
			ring.record("playlist_sync_write", "upsert_item", start, sqliteErr)
		}
	})
}

func BenchmarkFormatSQLiteIOERRTraceEntries(b *testing.B) {
	entries := make([]sqliteIOERRTraceEntry, 64)
	now := time.Now().UnixNano()
	for i := range entries {
		entries[i] = sqliteIOERRTraceEntry{
			TimestampUnixNano: now + int64(i),
			DurationMicros:    int64(i + 1),
			Operation:         "playlist_sync_write",
			Phase:             "upsert_item",
			ErrorClass:        "ok",
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = formatSQLiteIOERRTraceEntries(entries)
	}
}

func benchmarkSQLiteIOERRTraceError(b *testing.B) error {
	b.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	_, err = db.ExecContext(context.Background(), `SELECT * FROM missing_table`)
	if err == nil {
		b.Fatal("ExecContext() error = nil, want sqlite error")
	}
	return err
}
