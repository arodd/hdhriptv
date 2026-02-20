package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/arodd/hdhriptv/internal/jobs"
	"github.com/arodd/hdhriptv/internal/playlist"
)

func TestSQLiteErrorDetailsUnwrapsModerncError(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	_, err = db.ExecContext(context.Background(), `SELECT * FROM missing_table`)
	if err == nil {
		t.Fatal("ExecContext() error = nil, want sqlite error")
	}

	code, codeName, ok := sqliteErrorDetails(err)
	if !ok {
		t.Fatalf("sqliteErrorDetails() ok = false for sqlite error: %v", err)
	}
	if code <= 0 {
		t.Fatalf("sqliteErrorDetails() code = %d, want positive sqlite code", code)
	}
	if strings.TrimSpace(codeName) == "" {
		t.Fatalf("sqliteErrorDetails() codeName empty for code=%d", code)
	}
}

func TestSQLiteErrorDetailsFallbackForNonSQLiteError(t *testing.T) {
	t.Parallel()

	if _, _, ok := sqliteErrorDetails(errors.New("plain error")); ok {
		t.Fatal("sqliteErrorDetails() ok = true for non-sqlite error, want false")
	}
}

func TestSQLiteDiagSuffixIncludesPhaseProgressAndRunMetadataForNonSQLiteError(t *testing.T) {
	t.Parallel()

	ctx := jobs.WithRunMetadata(context.Background(), 42, jobs.JobPlaylistSync, jobs.TriggerManual)
	suffix := sqliteDiagSuffix(ctx, nil, errors.New("plain error"), sqliteDiagOptions{
		Phase:     "upsert_item",
		ItemIndex: 3,
		ItemTotal: 8,
		ItemKey:   "item-3",
	})

	requiredFragments := []string{
		`phase="upsert_item"`,
		`item_index=3`,
		`item_total=8`,
		`item_key="item-3"`,
		`run_id=42`,
		`job_name="playlist_sync"`,
		`triggered_by="manual"`,
	}
	for _, fragment := range requiredFragments {
		if !strings.Contains(suffix, fragment) {
			t.Fatalf("sqliteDiagSuffix() missing fragment %q in %q", fragment, suffix)
		}
	}
	if strings.Contains(suffix, "sqlite_code=") {
		t.Fatalf("sqliteDiagSuffix() included sqlite fields for non-sqlite error: %q", suffix)
	}
}

func TestSQLiteDiagSuffixIncludesSQLiteCodeAndDBStats(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer db.Close()

	_, err = db.ExecContext(context.Background(), `SELECT * FROM missing_table`)
	if err == nil {
		t.Fatal("ExecContext() error = nil, want sqlite error")
	}

	suffix := sqliteDiagSuffix(context.Background(), db, err, sqliteDiagOptions{Phase: "create_run"})
	requiredFragments := []string{
		`phase="create_run"`,
		`sqlite_code=`,
		`sqlite_code_name="`,
		`db_open=`,
		`db_in_use=`,
		`db_idle=`,
		`db_wait_count=`,
		`db_wait_duration=`,
	}
	for _, fragment := range requiredFragments {
		if !strings.Contains(suffix, fragment) {
			t.Fatalf("sqliteDiagSuffix() missing fragment %q in %q", fragment, suffix)
		}
	}
}

func TestUpsertPlaylistItemsErrorIncludesPhaseAndRunMetadata(t *testing.T) {
	t.Parallel()

	store, err := Open(filepath.Join(t.TempDir(), "diag.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	ctx := jobs.WithRunMetadata(context.Background(), 73, jobs.JobPlaylistSync, jobs.TriggerManual)
	err = store.UpsertPlaylistItems(ctx, []playlist.Item{{
		ItemKey:    "item-1",
		ChannelKey: "name:test",
		Name:       "Test",
		Group:      "Group",
		StreamURL:  "http://example.com/stream.ts",
	}})
	if err == nil {
		t.Fatal("UpsertPlaylistItems() error = nil, want begin_tx failure after Close()")
	}

	text := err.Error()
	requiredFragments := []string{
		`phase="begin_tx"`,
		`run_id=73`,
		`job_name="playlist_sync"`,
		`triggered_by="manual"`,
	}
	for _, fragment := range requiredFragments {
		if !strings.Contains(text, fragment) {
			t.Fatalf("UpsertPlaylistItems() error missing fragment %q in %q", fragment, text)
		}
	}
}
