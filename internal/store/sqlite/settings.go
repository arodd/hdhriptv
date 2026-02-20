package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	SettingIdentityFriendlyName         = "identity.friendly_name"
	SettingIdentityDeviceID             = "identity.device_id"
	SettingIdentityDeviceAuth           = "identity.device_auth"
	SettingPlaylistURL                  = "playlist.url"
	SettingJobsTimezone                 = "jobs.timezone"
	SettingJobsPlaylistSyncEnabled      = "jobs.playlist_sync.enabled"
	SettingJobsPlaylistSyncCron         = "jobs.playlist_sync.cron"
	SettingJobsAutoPrioritizeEnabled    = "jobs.auto_prioritize.enabled"
	SettingJobsAutoPrioritizeCron       = "jobs.auto_prioritize.cron"
	SettingJobsDVRLineupSyncEnabled     = "jobs.dvr_lineup_sync.enabled"
	SettingJobsDVRLineupSyncCron        = "jobs.dvr_lineup_sync.cron"
	SettingAnalyzerProbeTimeoutMS       = "analyzer.probe.timeout_ms"
	SettingAnalyzerAnalyzeDurationUS    = "analyzer.probe.analyzeduration_us"
	SettingAnalyzerProbeSizeBytes       = "analyzer.probe.probesize_bytes"
	SettingAnalyzerBitrateMode          = "analyzer.bitrate_mode"
	SettingAnalyzerSampleSeconds        = "analyzer.sample_seconds"
	SettingAutoPrioritizeEnabledOnly    = "analyzer.autoprioritize.enabled_only"
	SettingAutoPrioritizeTopNPerChannel = "analyzer.autoprioritize.top_n_per_channel"
)

// GetSetting returns one setting value.
func (s *Store) GetSetting(ctx context.Context, key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("setting key is required")
	}

	var value string
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, key).Scan(&value); err != nil {
		if err == sql.ErrNoRows {
			return "", sql.ErrNoRows
		}
		return "", fmt.Errorf("get setting %q: %w", key, err)
	}
	return value, nil
}

// SetSetting upserts one setting value.
func (s *Store) SetSetting(ctx context.Context, key, value string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("setting key is required")
	}

	if _, err := s.db.ExecContext(
		ctx,
		`INSERT INTO settings(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key,
		value,
	); err != nil {
		return fmt.Errorf("set setting %q: %w", key, err)
	}
	return nil
}

// SetSettings upserts multiple setting values in one transaction.
func (s *Store) SetSettings(ctx context.Context, values map[string]string) error {
	if len(values) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin settings tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO settings(key, value) VALUES(?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`)
	if err != nil {
		return fmt.Errorf("prepare settings upsert: %w", err)
	}
	defer stmt.Close()

	for key, value := range values {
		key = strings.TrimSpace(key)
		if key == "" {
			return fmt.Errorf("setting key is required")
		}
		if _, err := stmt.ExecContext(ctx, key, value); err != nil {
			return fmt.Errorf("upsert setting %q: %w", key, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit settings tx: %w", err)
	}
	return nil
}

// ListSettings returns key/value settings, optionally filtered by a key prefix.
func (s *Store) ListSettings(ctx context.Context, keyPrefix string) (map[string]string, error) {
	query := `SELECT key, value FROM settings`
	args := []any{}

	keyPrefix = strings.TrimSpace(keyPrefix)
	if keyPrefix != "" {
		query += ` WHERE key LIKE ?`
		args = append(args, keyPrefix+"%")
	}
	query += ` ORDER BY key ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list settings: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var key string
		var value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("scan setting row: %w", err)
		}
		out[key] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate settings rows: %w", err)
	}
	return out, nil
}
