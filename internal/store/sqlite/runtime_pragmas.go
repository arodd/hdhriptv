package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	runtimeBusyTimeoutMS    = 5000
	runtimeSynchronousPragm = "NORMAL"
	runtimeJournalModePragm = "WAL"
)

// RuntimePragmas captures the active SQLite runtime tuning settings.
type RuntimePragmas struct {
	BusyTimeoutMS int
	Synchronous   string
	JournalMode   string
}

func applyRuntimePragmas(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("sqlite db is required")
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", runtimeBusyTimeoutMS)); err != nil {
		return fmt.Errorf("set PRAGMA busy_timeout: %w", err)
	}
	if err := setRuntimeJournalMode(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA synchronous = %s", runtimeSynchronousPragm)); err != nil {
		return fmt.Errorf("set PRAGMA synchronous: %w", err)
	}
	return nil
}

func setRuntimeJournalMode(ctx context.Context, db *sql.DB) error {
	var mode string
	if err := db.QueryRowContext(ctx, fmt.Sprintf("PRAGMA journal_mode = %s", runtimeJournalModePragm)).Scan(&mode); err != nil {
		return fmt.Errorf("set PRAGMA journal_mode: %w", err)
	}
	if strings.TrimSpace(mode) == "" {
		return fmt.Errorf("set PRAGMA journal_mode returned empty mode")
	}
	return nil
}

// RuntimePragmas reads active SQLite runtime settings for startup logging and tests.
func (s *Store) RuntimePragmas(ctx context.Context) (RuntimePragmas, error) {
	if s == nil || s.db == nil {
		return RuntimePragmas{}, fmt.Errorf("store is not initialized")
	}

	busyTimeout, err := queryPragmaInt(ctx, s.db, "busy_timeout")
	if err != nil {
		return RuntimePragmas{}, err
	}
	synchronousCode, err := queryPragmaInt(ctx, s.db, "synchronous")
	if err != nil {
		return RuntimePragmas{}, err
	}
	journalMode, err := queryPragmaText(ctx, s.db, "journal_mode")
	if err != nil {
		return RuntimePragmas{}, err
	}

	return RuntimePragmas{
		BusyTimeoutMS: busyTimeout,
		Synchronous:   synchronousPragmaName(synchronousCode),
		JournalMode:   strings.ToLower(strings.TrimSpace(journalMode)),
	}, nil
}

func queryPragmaInt(ctx context.Context, db *sql.DB, name string) (int, error) {
	var value int
	if err := db.QueryRowContext(ctx, "PRAGMA "+name).Scan(&value); err != nil {
		return 0, fmt.Errorf("query PRAGMA %s: %w", name, err)
	}
	return value, nil
}

func queryPragmaText(ctx context.Context, db *sql.DB, name string) (string, error) {
	var value string
	if err := db.QueryRowContext(ctx, "PRAGMA "+name).Scan(&value); err != nil {
		return "", fmt.Errorf("query PRAGMA %s: %w", name, err)
	}
	return strings.TrimSpace(value), nil
}

func synchronousPragmaName(code int) string {
	switch code {
	case 0:
		return "OFF"
	case 1:
		return "NORMAL"
	case 2:
		return "FULL"
	case 3:
		return "EXTRA"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", code)
	}
}
