package sqlite

import (
	"context"
	"database/sql"
	"testing"
)

func TestAutomationSchemaAndDefaultSettings(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	for _, table := range []string{"job_runs", "stream_metrics"} {
		var found string
		if err := store.db.QueryRowContext(
			ctx,
			`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`,
			table,
		).Scan(&found); err != nil {
			t.Fatalf("expected table %q to exist: %v", table, err)
		}
	}

	defaults := map[string]string{
		SettingPlaylistURL:                  "",
		SettingJobsTimezone:                 "America/Chicago",
		SettingJobsPlaylistSyncEnabled:      "true",
		SettingJobsPlaylistSyncCron:         "*/30 * * * *",
		SettingJobsAutoPrioritizeEnabled:    "false",
		SettingJobsAutoPrioritizeCron:       "30 3 * * *",
		SettingJobsDVRLineupSyncEnabled:     "false",
		SettingJobsDVRLineupSyncCron:        "*/30 * * * *",
		SettingAnalyzerProbeTimeoutMS:       "7000",
		SettingAnalyzerAnalyzeDurationUS:    "1500000",
		SettingAnalyzerProbeSizeBytes:       "1000000",
		SettingAnalyzerBitrateMode:          "metadata_then_sample",
		SettingAnalyzerSampleSeconds:        "3",
		SettingAutoPrioritizeEnabledOnly:    "true",
		SettingAutoPrioritizeTopNPerChannel: "0",
	}

	for key, want := range defaults {
		got, err := store.GetSetting(ctx, key)
		if err != nil {
			t.Fatalf("GetSetting(%q) error = %v", key, err)
		}
		if got != want {
			t.Fatalf("GetSetting(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestSettingsHelpers(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.SetSetting(ctx, SettingJobsTimezone, "UTC"); err != nil {
		t.Fatalf("SetSetting(timezone) error = %v", err)
	}

	timezone, err := store.GetSetting(ctx, SettingJobsTimezone)
	if err != nil {
		t.Fatalf("GetSetting(timezone) error = %v", err)
	}
	if timezone != "UTC" {
		t.Fatalf("GetSetting(timezone) = %q, want UTC", timezone)
	}

	if err := store.SetSettings(ctx, map[string]string{
		SettingJobsPlaylistSyncEnabled: "false",
		SettingJobsPlaylistSyncCron:    "0 * * * *",
	}); err != nil {
		t.Fatalf("SetSettings() error = %v", err)
	}

	jobSettings, err := store.ListSettings(ctx, "jobs.")
	if err != nil {
		t.Fatalf("ListSettings(jobs.) error = %v", err)
	}
	if got := jobSettings[SettingJobsPlaylistSyncEnabled]; got != "false" {
		t.Fatalf("jobs.playlist_sync.enabled = %q, want false", got)
	}
	if got := jobSettings[SettingJobsPlaylistSyncCron]; got != "0 * * * *" {
		t.Fatalf("jobs.playlist_sync.cron = %q, want 0 * * * *", got)
	}

	if err := store.SetSetting(ctx, "   ", "value"); err == nil {
		t.Fatal("SetSetting(blank key) expected error")
	}

	if _, err := store.GetSetting(ctx, "does.not.exist"); err != sql.ErrNoRows {
		t.Fatalf("GetSetting(does.not.exist) error = %v, want sql.ErrNoRows", err)
	}
}
