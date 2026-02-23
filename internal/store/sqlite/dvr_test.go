package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/dvr"
	"github.com/arodd/hdhriptv/internal/playlist"
)

func TestDVRInstanceDefaultsAndUpdate(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	instance, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance() error = %v", err)
	}
	if instance.ID <= 0 {
		t.Fatalf("instance.ID = %d, want > 0", instance.ID)
	}
	if instance.Provider != dvr.ProviderChannels {
		t.Fatalf("instance.Provider = %q, want %q", instance.Provider, dvr.ProviderChannels)
	}
	if len(instance.ActiveProviders) != 0 {
		t.Fatalf("instance.ActiveProviders = %v, want empty by default", instance.ActiveProviders)
	}
	if instance.BaseURL != "" {
		t.Fatalf("instance.BaseURL = %q, want empty default", instance.BaseURL)
	}
	if instance.ChannelsBaseURL != "" {
		t.Fatalf("instance.ChannelsBaseURL = %q, want empty default", instance.ChannelsBaseURL)
	}
	if instance.JellyfinBaseURL != "" {
		t.Fatalf("instance.JellyfinBaseURL = %q, want empty", instance.JellyfinBaseURL)
	}
	if instance.SyncMode != dvr.SyncModeConfiguredOnly {
		t.Fatalf("instance.SyncMode = %q, want %q", instance.SyncMode, dvr.SyncModeConfiguredOnly)
	}
	if instance.JellyfinAPIToken != "" {
		t.Fatalf("instance.JellyfinAPIToken = %q, want empty", instance.JellyfinAPIToken)
	}
	if instance.JellyfinAPITokenConfigured {
		t.Fatal("instance.JellyfinAPITokenConfigured = true, want false")
	}

	updated, err := store.UpsertDVRInstance(ctx, dvr.InstanceConfig{
		ID:                    instance.ID,
		Provider:              dvr.ProviderChannels,
		BaseURL:               "http://example.invalid:8089",
		DefaultLineupID:       "USA-TEST",
		SyncEnabled:           true,
		SyncCron:              "*/15 * * * *",
		SyncMode:              dvr.SyncModeMirrorDevice,
		PreSyncRefreshDevices: true,
	})
	if err != nil {
		t.Fatalf("UpsertDVRInstance() error = %v", err)
	}
	if updated.BaseURL != "http://example.invalid:8089" {
		t.Fatalf("updated.BaseURL = %q, want http://example.invalid:8089", updated.BaseURL)
	}
	if updated.ChannelsBaseURL != "http://example.invalid:8089" {
		t.Fatalf("updated.ChannelsBaseURL = %q, want http://example.invalid:8089", updated.ChannelsBaseURL)
	}
	if got, want := updated.ActiveProviders, []dvr.ProviderType{dvr.ProviderChannels}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("updated.ActiveProviders = %v, want %v", got, want)
	}
	if updated.DefaultLineupID != "USA-TEST" {
		t.Fatalf("updated.DefaultLineupID = %q, want USA-TEST", updated.DefaultLineupID)
	}
	if !updated.SyncEnabled {
		t.Fatal("updated.SyncEnabled = false, want true")
	}
	if updated.SyncMode != dvr.SyncModeMirrorDevice {
		t.Fatalf("updated.SyncMode = %q, want %q", updated.SyncMode, dvr.SyncModeMirrorDevice)
	}
	if !updated.PreSyncRefreshDevices {
		t.Fatal("updated.PreSyncRefreshDevices = false, want true")
	}

	reloaded, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance(reloaded) error = %v", err)
	}
	if reloaded.ID != updated.ID {
		t.Fatalf("reloaded.ID = %d, want %d", reloaded.ID, updated.ID)
	}
	if reloaded.SyncCron != "*/15 * * * *" {
		t.Fatalf("reloaded.SyncCron = %q, want */15 * * * *", reloaded.SyncCron)
	}
}

func TestGetDVRInstancePreservesConfiguredLegacyDefaultBaseURL(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	instance, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance() error = %v", err)
	}
	instance.Provider = dvr.ProviderChannels
	instance.ActiveProviders = []dvr.ProviderType{dvr.ProviderChannels}
	instance.BaseURL = legacyDefaultDVRBaseURL
	instance.ChannelsBaseURL = legacyDefaultDVRBaseURL
	instance.DefaultLineupID = ""
	instance.SyncEnabled = false
	instance.SyncCron = ""
	instance.PreSyncRefreshDevices = false
	instance.JellyfinBaseURL = ""
	instance.JellyfinAPIToken = ""
	instance.JellyfinTunerHostID = ""

	updated, err := store.UpsertDVRInstance(ctx, instance)
	if err != nil {
		t.Fatalf("UpsertDVRInstance() error = %v", err)
	}
	if got, want := updated.BaseURL, legacyDefaultDVRBaseURL; got != want {
		t.Fatalf("updated.BaseURL = %q, want %q", got, want)
	}
	if got, want := updated.ChannelsBaseURL, legacyDefaultDVRBaseURL; got != want {
		t.Fatalf("updated.ChannelsBaseURL = %q, want %q", got, want)
	}

	reloaded, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance(reloaded) error = %v", err)
	}
	if got, want := reloaded.BaseURL, legacyDefaultDVRBaseURL; got != want {
		t.Fatalf("reloaded.BaseURL = %q, want %q", got, want)
	}
	if got, want := reloaded.ChannelsBaseURL, legacyDefaultDVRBaseURL; got != want {
		t.Fatalf("reloaded.ChannelsBaseURL = %q, want %q", got, want)
	}
	if got, want := reloaded.ActiveProviders, []dvr.ProviderType{dvr.ProviderChannels}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("reloaded.ActiveProviders = %v, want %v", got, want)
	}
}

func TestGetDVRInstanceCleansLegacyPlaceholderWhenUnconfigured(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if _, err := store.db.ExecContext(
		ctx,
		`
		UPDATE dvr_instances
		SET provider = ?,
		    active_providers = '',
		    base_url = ?,
		    channels_base_url = ?,
		    default_lineup_id = '',
		    sync_enabled = 0,
		    sync_cron = '',
		    sync_mode = ?,
		    pre_sync_refresh_devices = 0,
		    jellyfin_base_url = '',
		    jellyfin_api_token = '',
		    jellyfin_tuner_host_id = ''
		WHERE singleton_key = ?
	`,
		string(dvr.ProviderChannels),
		legacyDefaultDVRBaseURL,
		legacyDefaultDVRBaseURL,
		string(dvr.SyncModeConfiguredOnly),
		dvrInstanceSingletonKey,
	); err != nil {
		t.Fatalf("seed legacy placeholder dvr row error = %v", err)
	}

	instance, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance() error = %v", err)
	}
	if got := instance.BaseURL; got != "" {
		t.Fatalf("instance.BaseURL = %q, want empty after legacy cleanup", got)
	}
	if got := instance.ChannelsBaseURL; got != "" {
		t.Fatalf("instance.ChannelsBaseURL = %q, want empty after legacy cleanup", got)
	}
	if len(instance.ActiveProviders) != 0 {
		t.Fatalf("instance.ActiveProviders = %v, want empty when no provider URL is configured", instance.ActiveProviders)
	}
}

func TestDVRInstanceJellyfinFieldsPersist(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	instance, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance() error = %v", err)
	}

	updated, err := store.UpsertDVRInstance(ctx, dvr.InstanceConfig{
		ID:                    instance.ID,
		Provider:              dvr.ProviderType("JELLYFIN"),
		BaseURL:               "http://jellyfin.lan:8096",
		ActiveProviders:       []dvr.ProviderType{dvr.ProviderChannels, dvr.ProviderJellyfin},
		ChannelsBaseURL:       "http://channels.local:8089",
		JellyfinBaseURL:       "http://jellyfin.lan:8096",
		JellyfinAPIToken:      "token-abc",
		JellyfinTunerHostID:   "host-id-1",
		SyncMode:              dvr.SyncModeConfiguredOnly,
		PreSyncRefreshDevices: false,
	})
	if err != nil {
		t.Fatalf("UpsertDVRInstance() error = %v", err)
	}
	if updated.Provider != dvr.ProviderJellyfin {
		t.Fatalf("updated.Provider = %q, want %q", updated.Provider, dvr.ProviderJellyfin)
	}
	if updated.JellyfinAPIToken != "token-abc" {
		t.Fatalf("updated.JellyfinAPIToken = %q, want token-abc", updated.JellyfinAPIToken)
	}
	if !updated.JellyfinAPITokenConfigured {
		t.Fatal("updated.JellyfinAPITokenConfigured = false, want true")
	}
	if updated.JellyfinTunerHostID != "host-id-1" {
		t.Fatalf("updated.JellyfinTunerHostID = %q, want host-id-1", updated.JellyfinTunerHostID)
	}
	if got, want := updated.ChannelsBaseURL, "http://channels.local:8089"; got != want {
		t.Fatalf("updated.ChannelsBaseURL = %q, want %q", got, want)
	}
	if got, want := updated.JellyfinBaseURL, "http://jellyfin.lan:8096"; got != want {
		t.Fatalf("updated.JellyfinBaseURL = %q, want %q", got, want)
	}
	if got, want := updated.ActiveProviders, []dvr.ProviderType{dvr.ProviderChannels, dvr.ProviderJellyfin}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("updated.ActiveProviders = %v, want %v", got, want)
	}
}

func TestDVRInstanceDefaultLineupPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "dvr-instance-persist.db")

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(initial) error = %v", err)
	}

	instance, err := store.GetDVRInstance(ctx)
	if err != nil {
		store.Close()
		t.Fatalf("GetDVRInstance(initial) error = %v", err)
	}

	instance.DefaultLineupID = "USA-MN22577-X"
	if _, err := store.UpsertDVRInstance(ctx, instance); err != nil {
		store.Close()
		t.Fatalf("UpsertDVRInstance() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close(initial) error = %v", err)
	}

	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(reopened) error = %v", err)
	}
	defer reopened.Close()

	reloaded, err := reopened.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance(reopened) error = %v", err)
	}
	if reloaded.DefaultLineupID != "USA-MN22577-X" {
		t.Fatalf("reloaded.DefaultLineupID = %q, want USA-MN22577-X", reloaded.DefaultLineupID)
	}
}

func TestDVRInstanceConcurrentInitializationSingleton(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const workers = 24
	start := make(chan struct{})
	ids := make(chan int64, workers)
	errs := make(chan error, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			instance, err := store.GetDVRInstance(ctx)
			if err != nil {
				errs <- err
				return
			}
			ids <- instance.ID
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	close(ids)

	for err := range errs {
		if err != nil {
			t.Fatalf("GetDVRInstance() concurrent error = %v", err)
		}
	}

	var (
		firstID int64
		haveID  bool
		idCount int
	)
	for id := range ids {
		idCount++
		if !haveID {
			firstID = id
			haveID = true
			continue
		}
		if id != firstID {
			t.Fatalf("GetDVRInstance() returned inconsistent IDs: first=%d saw=%d", firstID, id)
		}
	}
	if idCount != workers {
		t.Fatalf("observed %d instance IDs, want %d", idCount, workers)
	}

	rowCount, err := countDVRInstanceRows(ctx, store.db)
	if err != nil {
		t.Fatalf("count dvr_instances rows error = %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("dvr_instances row count = %d, want 1", rowCount)
	}
}

func TestGetDVRInstanceMissingSingletonReturnsErrorWithoutRecreate(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if _, err := store.db.ExecContext(ctx, `DELETE FROM dvr_instances`); err != nil {
		t.Fatalf("DELETE dvr_instances error = %v", err)
	}

	_, err = store.GetDVRInstance(ctx)
	if err == nil {
		t.Fatal("GetDVRInstance() error = nil, want non-nil when singleton row is missing")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "singleton missing") {
		t.Fatalf("GetDVRInstance() error = %q, want singleton-missing context", err.Error())
	}

	rowCount, err := countDVRInstanceRows(ctx, store.db)
	if err != nil {
		t.Fatalf("count dvr_instances rows error = %v", err)
	}
	if rowCount != 0 {
		t.Fatalf("dvr_instances row count after GetDVRInstance() = %d, want 0", rowCount)
	}
}

func TestOpenNormalizesDuplicateDVRInstancesToSingleton(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy-dvr-duplicate-instance.db")

	legacyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open(legacy) error = %v", err)
	}
	if _, err := legacyDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS settings (
		  key TEXT PRIMARY KEY,
		  value TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS playlist_items (
		  item_key      TEXT PRIMARY KEY,
		  channel_key   TEXT,
		  name          TEXT NOT NULL DEFAULT '',
		  group_name    TEXT NOT NULL DEFAULT '',
		  stream_url    TEXT NOT NULL DEFAULT '',
		  tvg_id        TEXT,
		  tvg_logo      TEXT,
		  attrs_json    TEXT NOT NULL DEFAULT '{}',
		  first_seen_at INTEGER,
		  last_seen_at  INTEGER,
		  active        INTEGER NOT NULL DEFAULT 1,
		  updated_at    INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS published_channels (
		  channel_id    INTEGER PRIMARY KEY AUTOINCREMENT,
		  channel_key   TEXT,
		  guide_number  TEXT NOT NULL DEFAULT '',
		  guide_name    TEXT NOT NULL DEFAULT '',
		  order_index   INTEGER NOT NULL DEFAULT 0,
		  enabled       INTEGER NOT NULL DEFAULT 1,
		  created_at    INTEGER NOT NULL DEFAULT 0,
		  updated_at    INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS dvr_instances (
		  id                       INTEGER PRIMARY KEY AUTOINCREMENT,
		  provider                 TEXT,
		  base_url                 TEXT,
		  default_lineup_id        TEXT,
		  sync_enabled             INTEGER,
		  sync_cron                TEXT,
		  sync_mode                TEXT,
		  pre_sync_refresh_devices INTEGER,
		  updated_at               INTEGER
		);
		CREATE TABLE IF NOT EXISTS published_channel_dvr_map (
		  channel_id INTEGER NOT NULL,
		  dvr_instance_id INTEGER NOT NULL,
		  dvr_lineup_id TEXT NOT NULL,
		  dvr_lineup_channel TEXT NOT NULL,
		  PRIMARY KEY(channel_id, dvr_instance_id)
		);
		INSERT INTO published_channels(channel_id, channel_key, guide_number, guide_name, order_index, enabled, created_at, updated_at)
		VALUES (1, 'tvg:legacy', '100', 'Legacy', 0, 1, 1, 1);
		INSERT INTO dvr_instances(id, provider, base_url, default_lineup_id, sync_enabled, sync_cron, sync_mode, pre_sync_refresh_devices, updated_at)
		VALUES
		  (1, 'channels', 'http://canonical.invalid:8089', 'USA-CANON', 1, '*/15 * * * *', 'configured_only', 1, 100),
		  (2, 'channels', 'http://duplicate.invalid:8089', 'USA-DUP', 0, '0 */6 * * *', 'mirror_device', 0, 200);
		INSERT INTO published_channel_dvr_map(channel_id, dvr_instance_id, dvr_lineup_id, dvr_lineup_channel)
		VALUES
		  (1, 1, 'USA-CANON', '100'),
		  (1, 2, 'USA-DUP', '100');
	`); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("seed legacy dvr duplicate schema error = %v", err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatalf("legacyDB.Close() error = %v", err)
	}

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(legacy db) error = %v", err)
	}
	defer store.Close()

	instance, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance() error = %v", err)
	}
	if instance.ID != 1 {
		t.Fatalf("GetDVRInstance().ID = %d, want canonical ID 1", instance.ID)
	}

	rowCount, err := countDVRInstanceRows(ctx, store.db)
	if err != nil {
		t.Fatalf("count dvr_instances rows error = %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("dvr_instances row count after Open() = %d, want 1", rowCount)
	}

	var nonCanonicalMapRows int
	if err := store.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM published_channel_dvr_map WHERE dvr_instance_id <> ?`,
		instance.ID,
	).Scan(&nonCanonicalMapRows); err != nil {
		t.Fatalf("count non-canonical dvr map rows error = %v", err)
	}
	if nonCanonicalMapRows != 0 {
		t.Fatalf("non-canonical dvr map row count = %d, want 0", nonCanonicalMapRows)
	}

	updated, err := store.UpsertDVRInstance(ctx, dvr.InstanceConfig{
		ID:                    9999,
		Provider:              dvr.ProviderChannels,
		BaseURL:               "http://updated.invalid:8089",
		DefaultLineupID:       "USA-UPDATED",
		SyncEnabled:           true,
		SyncCron:              "*/10 * * * *",
		SyncMode:              dvr.SyncModeMirrorDevice,
		PreSyncRefreshDevices: true,
	})
	if err != nil {
		t.Fatalf("UpsertDVRInstance() error = %v", err)
	}
	if updated.ID != instance.ID {
		t.Fatalf("UpsertDVRInstance() ID = %d, want canonical ID %d", updated.ID, instance.ID)
	}

	rowCount, err = countDVRInstanceRows(ctx, store.db)
	if err != nil {
		t.Fatalf("count dvr_instances rows after upsert error = %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("dvr_instances row count after UpsertDVRInstance() = %d, want 1", rowCount)
	}
}

func TestChannelDVRMappingLifecycle(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News",
			Group:      "News",
			StreamURL:  "http://example.com/news.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, "src:news:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}

	instance, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance() error = %v", err)
	}

	emptyMapping, err := store.GetChannelDVRMapping(ctx, instance.ID, channel.ChannelID)
	if err != nil {
		t.Fatalf("GetChannelDVRMapping(empty) error = %v", err)
	}
	if emptyMapping.DVRLineupID != "" || emptyMapping.DVRLineupChannel != "" {
		t.Fatalf("empty mapping = %+v, want blank lineup fields", emptyMapping)
	}

	saved, err := store.UpsertChannelDVRMapping(ctx, dvr.ChannelMapping{
		ChannelID:        channel.ChannelID,
		DVRInstanceID:    instance.ID,
		DVRLineupID:      "USA-MN22577-X",
		DVRLineupChannel: "118",
		DVRStationRef:    "97047",
		DVRCallsignHint:  "TNCKHD",
	})
	if err != nil {
		t.Fatalf("UpsertChannelDVRMapping() error = %v", err)
	}
	if saved.DVRLineupID != "USA-MN22577-X" {
		t.Fatalf("saved.DVRLineupID = %q, want USA-MN22577-X", saved.DVRLineupID)
	}
	if saved.DVRLineupChannel != "118" {
		t.Fatalf("saved.DVRLineupChannel = %q, want 118", saved.DVRLineupChannel)
	}

	list, err := store.ListChannelsForDVRSync(ctx, instance.ID, true, false)
	if err != nil {
		t.Fatalf("ListChannelsForDVRSync() error = %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("len(ListChannelsForDVRSync) = %d, want 1", len(list))
	}
	if list[0].DVRStationRef != "97047" {
		t.Fatalf("list[0].DVRStationRef = %q, want 97047", list[0].DVRStationRef)
	}

	stationOnly, err := store.UpsertChannelDVRMapping(ctx, dvr.ChannelMapping{
		ChannelID:        channel.ChannelID,
		DVRInstanceID:    instance.ID,
		DVRLineupID:      "USA-MN22577-X",
		DVRLineupChannel: "",
		DVRStationRef:    "72078",
	})
	if err != nil {
		t.Fatalf("UpsertChannelDVRMapping(station-only) error = %v", err)
	}
	if stationOnly.DVRStationRef != "72078" {
		t.Fatalf("stationOnly.DVRStationRef = %q, want 72078", stationOnly.DVRStationRef)
	}
	if stationOnly.DVRLineupChannel != "" {
		t.Fatalf("stationOnly.DVRLineupChannel = %q, want empty", stationOnly.DVRLineupChannel)
	}

	if _, err := store.UpsertChannelDVRMapping(ctx, dvr.ChannelMapping{
		ChannelID:        channel.ChannelID,
		DVRInstanceID:    instance.ID,
		DVRLineupID:      "USA-MN22577-X",
		DVRLineupChannel: "",
		DVRStationRef:    "",
	}); err == nil {
		t.Fatal("UpsertChannelDVRMapping(empty lineup_channel+station_ref) error = nil, want non-nil")
	}

	if err := store.DeleteChannelDVRMapping(ctx, instance.ID, channel.ChannelID); err != nil {
		t.Fatalf("DeleteChannelDVRMapping() error = %v", err)
	}
	afterDelete, err := store.GetChannelDVRMapping(ctx, instance.ID, channel.ChannelID)
	if err != nil {
		t.Fatalf("GetChannelDVRMapping(after delete) error = %v", err)
	}
	if afterDelete.DVRLineupID != "" || afterDelete.DVRLineupChannel != "" || afterDelete.DVRStationRef != "" {
		t.Fatalf("mapping after delete = %+v, want cleared fields", afterDelete)
	}

	_, err = store.UpsertChannelDVRMapping(ctx, dvr.ChannelMapping{
		ChannelID:        9999,
		DVRInstanceID:    instance.ID,
		DVRLineupID:      "USA-MN22577-X",
		DVRLineupChannel: "119",
	})
	if !errors.Is(err, channels.ErrChannelNotFound) {
		t.Fatalf("UpsertChannelDVRMapping(missing channel) error = %v, want ErrChannelNotFound", err)
	}
}

func TestListChannelsForDVRSyncExcludesDynamicGeneratedByDefault(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:trad:one",
			ChannelKey: "name:trad one",
			Name:       "Traditional One",
			Group:      "Linear",
			StreamURL:  "http://example.com/trad-one.ts",
		},
		{
			ItemKey:    "src:dyn:one",
			ChannelKey: "name:dyn one",
			Name:       "Dynamic One",
			Group:      "Dynamic",
			StreamURL:  "http://example.com/dyn-one.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	if _, err := store.CreateChannelFromItem(ctx, "src:trad:one", "", "", nil, channels.TraditionalGuideStart); err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	if _, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Dynamic Block",
		GroupName:   "Dynamic",
		SearchQuery: "dynamic",
	}); err != nil {
		t.Fatalf("CreateDynamicChannelQuery() error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}

	instance, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance() error = %v", err)
	}

	defaultRows, err := store.ListChannelsForDVRSync(ctx, instance.ID, true, false)
	if err != nil {
		t.Fatalf("ListChannelsForDVRSync(default) error = %v", err)
	}
	if len(defaultRows) != 1 {
		t.Fatalf("len(ListChannelsForDVRSync default) = %d, want 1", len(defaultRows))
	}
	if defaultRows[0].GuideNumber == "10000" {
		t.Fatalf("default DVR sync rows unexpectedly include dynamic guide %s", defaultRows[0].GuideNumber)
	}

	includeRows, err := store.ListChannelsForDVRSync(ctx, instance.ID, true, true)
	if err != nil {
		t.Fatalf("ListChannelsForDVRSync(include dynamic) error = %v", err)
	}
	if len(includeRows) != 2 {
		t.Fatalf("len(ListChannelsForDVRSync include dynamic) = %d, want 2", len(includeRows))
	}

	foundDynamic := false
	for _, row := range includeRows {
		if row.GuideNumber == "10000" {
			foundDynamic = true
			break
		}
	}
	if !foundDynamic {
		t.Fatal("include_dynamic=true should include generated dynamic guide 10000")
	}
}

func TestListChannelsForDVRSyncPagedDeterministicOrderingAndTotals(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	instance, err := seedDVRSyncMappingsFixture(ctx, store, 9, 3)
	if err != nil {
		t.Fatalf("seedDVRSyncMappingsFixture() error = %v", err)
	}

	cases := []struct {
		name           string
		enabledOnly    bool
		includeDynamic bool
		limit          int
		offset         int
	}{
		{
			name:           "traditional_only_first_page",
			enabledOnly:    false,
			includeDynamic: false,
			limit:          4,
			offset:         0,
		},
		{
			name:           "traditional_only_middle_page",
			enabledOnly:    false,
			includeDynamic: false,
			limit:          3,
			offset:         2,
		},
		{
			name:           "enabled_only_with_dynamic",
			enabledOnly:    true,
			includeDynamic: true,
			limit:          5,
			offset:         1,
		},
		{
			name:           "offset_past_end",
			enabledOnly:    false,
			includeDynamic: true,
			limit:          3,
			offset:         99,
		},
		{
			name:           "limit_zero_normalized",
			enabledOnly:    false,
			includeDynamic: true,
			limit:          0,
			offset:         0,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			full, err := store.ListChannelsForDVRSync(ctx, instance.ID, tc.enabledOnly, tc.includeDynamic)
			if err != nil {
				t.Fatalf("ListChannelsForDVRSync() error = %v", err)
			}

			page, total, err := store.ListChannelsForDVRSyncPaged(
				ctx,
				instance.ID,
				tc.enabledOnly,
				tc.includeDynamic,
				tc.limit,
				tc.offset,
			)
			if err != nil {
				t.Fatalf("ListChannelsForDVRSyncPaged() error = %v", err)
			}

			if got, want := total, len(full); got != want {
				t.Fatalf("paged total = %d, want %d", got, want)
			}

			normalizedLimit := tc.limit
			if normalizedLimit < 1 {
				normalizedLimit = 1
			}

			expectedLen := 0
			if tc.offset < len(full) {
				expectedLen = len(full) - tc.offset
				if expectedLen > normalizedLimit {
					expectedLen = normalizedLimit
				}
			}
			if got, want := len(page), expectedLen; got != want {
				t.Fatalf("len(paged) = %d, want %d", got, want)
			}

			for i := range page {
				want := full[tc.offset+i]
				got := page[i]
				if got.ChannelID != want.ChannelID || got.GuideNumber != want.GuideNumber || got.Enabled != want.Enabled {
					t.Fatalf(
						"paged row[%d] = {channel_id:%d guide_number:%q enabled:%t}, want {channel_id:%d guide_number:%q enabled:%t}",
						i,
						got.ChannelID,
						got.GuideNumber,
						got.Enabled,
						want.ChannelID,
						want.GuideNumber,
						want.Enabled,
					)
				}
			}
		})
	}
}

func TestListChannelsForDVRSyncPagedQueryPlanAvoidsTempBTreeAtHighOffset(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	instance, err := seedDVRSyncMappingsFixture(ctx, store, 5000, 0)
	if err != nil {
		t.Fatalf("seedDVRSyncMappingsFixture() error = %v", err)
	}

	planDetails, err := explainListChannelsForDVRSyncPagedPlan(ctx, store, instance.ID, false, false, 100, 4900)
	if err != nil {
		t.Fatalf("explainListChannelsForDVRSyncPagedPlan() error = %v", err)
	}
	if len(planDetails) == 0 {
		t.Fatal("expected query-plan rows for paged dvr mapping query")
	}
	t.Logf("dvr mapping paged query plan: %s", strings.Join(planDetails, " | "))
	if !planContains(planDetails, "idx_published_channels_order") {
		t.Fatalf("query plan missing ordered channel index: %q", strings.Join(planDetails, " | "))
	}
	if planContains(planDetails, "use temp b-tree for order by") {
		t.Fatalf("query plan unexpectedly includes temp b-tree order by: %q", strings.Join(planDetails, " | "))
	}
}

func BenchmarkListChannelsForDVRSyncPagedHotPath(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	instance, err := seedDVRSyncMappingsFixture(ctx, store, 6000, 0)
	if err != nil {
		b.Fatalf("seedDVRSyncMappingsFixture() error = %v", err)
	}

	benchCases := []struct {
		name   string
		limit  int
		offset int
	}{
		{
			name:   "offset_0_limit_200",
			limit:  200,
			offset: 0,
		},
		{
			name:   "offset_5600_limit_200",
			limit:  200,
			offset: 5600,
		},
	}

	for _, benchCase := range benchCases {
		benchCase := benchCase
		b.Run(benchCase.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, total, err := store.ListChannelsForDVRSyncPaged(
					ctx,
					instance.ID,
					false,
					false,
					benchCase.limit,
					benchCase.offset,
				)
				if err != nil {
					b.Fatalf("ListChannelsForDVRSyncPaged() error = %v", err)
				}
				if total != 6000 || len(rows) == 0 {
					b.Fatalf(
						"ListChannelsForDVRSyncPaged() total/len = %d/%d, want 6000/non-zero",
						total,
						len(rows),
					)
				}
			}
		})
	}
}

func TestOpenAddsMissingDVRColumnsForLegacyDB(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy-dvr-schema.db")

	legacyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open(legacy) error = %v", err)
	}
	if _, err := legacyDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS settings (
		  key TEXT PRIMARY KEY,
		  value TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS playlist_items (
		  item_key      TEXT PRIMARY KEY,
		  channel_key   TEXT,
		  name          TEXT NOT NULL DEFAULT '',
		  group_name    TEXT NOT NULL DEFAULT '',
		  stream_url    TEXT NOT NULL DEFAULT '',
		  tvg_id        TEXT,
		  tvg_logo      TEXT,
		  attrs_json    TEXT NOT NULL DEFAULT '{}',
		  first_seen_at INTEGER,
		  last_seen_at  INTEGER,
		  active        INTEGER NOT NULL DEFAULT 1,
		  updated_at    INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS published_channels (
		  channel_id    INTEGER PRIMARY KEY AUTOINCREMENT,
		  channel_key   TEXT,
		  guide_number  TEXT NOT NULL DEFAULT '',
		  guide_name    TEXT NOT NULL DEFAULT '',
		  order_index   INTEGER NOT NULL DEFAULT 0,
		  enabled       INTEGER NOT NULL DEFAULT 1,
		  created_at    INTEGER NOT NULL DEFAULT 0,
		  updated_at    INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE IF NOT EXISTS dvr_instances (
		  id INTEGER PRIMARY KEY AUTOINCREMENT,
		  provider TEXT
		);
		CREATE TABLE IF NOT EXISTS published_channel_dvr_map (
		  channel_id INTEGER NOT NULL,
		  dvr_instance_id INTEGER NOT NULL,
		  dvr_lineup_id TEXT NOT NULL,
		  dvr_lineup_channel TEXT NOT NULL,
		  PRIMARY KEY(channel_id, dvr_instance_id)
		);
	`); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("seed legacy dvr schema error = %v", err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatalf("legacyDB.Close() error = %v", err)
	}

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(legacy db) error = %v", err)
	}
	defer store.Close()

	dvrInstanceCols := []string{
		"singleton_key",
		"provider",
		"active_providers",
		"base_url",
		"channels_base_url",
		"jellyfin_base_url",
		"default_lineup_id",
		"sync_enabled",
		"sync_cron",
		"sync_mode",
		"pre_sync_refresh_devices",
		"jellyfin_api_token",
		"jellyfin_tuner_host_id",
		"updated_at",
	}
	for _, col := range dvrInstanceCols {
		exists, err := store.columnExists(ctx, "dvr_instances", col)
		if err != nil {
			t.Fatalf("columnExists(dvr_instances.%s) error = %v", col, err)
		}
		if !exists {
			t.Fatalf("dvr_instances missing expected column %q after Open()", col)
		}
	}

	for _, col := range []string{"dvr_station_ref", "dvr_callsign_hint"} {
		exists, err := store.columnExists(ctx, "published_channel_dvr_map", col)
		if err != nil {
			t.Fatalf("columnExists(published_channel_dvr_map.%s) error = %v", col, err)
		}
		if !exists {
			t.Fatalf("published_channel_dvr_map missing expected column %q after Open()", col)
		}
	}
}

func countDVRInstanceRows(ctx context.Context, db *sql.DB) (int, error) {
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dvr_instances`).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func seedDVRSyncMappingsFixture(
	ctx context.Context,
	store *Store,
	traditionalCount int,
	dynamicCount int,
) (dvr.InstanceConfig, error) {
	instance, err := store.GetDVRInstance(ctx)
	if err != nil {
		return dvr.InstanceConfig{}, fmt.Errorf("GetDVRInstance(): %w", err)
	}

	now := time.Now().UTC().Unix()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return dvr.InstanceConfig{}, fmt.Errorf("BeginTx(): %w", err)
	}
	defer tx.Rollback()

	channelStmt, err := tx.PrepareContext(
		ctx,
		`
		INSERT INTO published_channels (
			channel_class,
			channel_key,
			guide_number,
			guide_name,
			order_index,
			enabled,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
	)
	if err != nil {
		return dvr.InstanceConfig{}, fmt.Errorf("prepare channel insert: %w", err)
	}
	defer channelStmt.Close()

	mappingStmt, err := tx.PrepareContext(
		ctx,
		`
		INSERT INTO published_channel_dvr_map (
			channel_id,
			dvr_instance_id,
			dvr_lineup_id,
			dvr_lineup_channel,
			dvr_station_ref,
			dvr_callsign_hint
		) VALUES (?, ?, ?, ?, ?, ?)
	`,
	)
	if err != nil {
		return dvr.InstanceConfig{}, fmt.Errorf("prepare mapping insert: %w", err)
	}
	defer mappingStmt.Close()

	orderIndex := 0
	for i := 0; i < traditionalCount; i++ {
		enabled := 1
		if i%3 == 2 {
			enabled = 0
		}
		guide := channels.TraditionalGuideStart + i
		channelID, err := insertDVRFixtureChannel(
			ctx,
			channelStmt,
			string(channels.ChannelClassTraditional),
			fmt.Sprintf("tvg:dvr:trad:%05d", i),
			strconv.Itoa(guide),
			fmt.Sprintf("Traditional %05d", i),
			orderIndex,
			enabled,
			now,
		)
		if err != nil {
			return dvr.InstanceConfig{}, err
		}
		if _, err := mappingStmt.ExecContext(
			ctx,
			channelID,
			instance.ID,
			"USA-DVR-BENCH",
			strconv.Itoa(guide),
			fmt.Sprintf("station-%05d", i),
			fmt.Sprintf("CALL%05d", i),
		); err != nil {
			return dvr.InstanceConfig{}, fmt.Errorf("insert traditional mapping row %d: %w", i, err)
		}
		orderIndex++
	}

	for i := 0; i < dynamicCount; i++ {
		guide := channels.DynamicGuideStart + i
		channelID, err := insertDVRFixtureChannel(
			ctx,
			channelStmt,
			string(channels.ChannelClassDynamicGenerated),
			fmt.Sprintf("dyn:dvr:%05d", i),
			strconv.Itoa(guide),
			fmt.Sprintf("Dynamic %05d", i),
			orderIndex,
			1,
			now,
		)
		if err != nil {
			return dvr.InstanceConfig{}, err
		}
		if _, err := mappingStmt.ExecContext(
			ctx,
			channelID,
			instance.ID,
			"USA-DVR-BENCH",
			strconv.Itoa(guide),
			fmt.Sprintf("dynamic-station-%05d", i),
			fmt.Sprintf("DYN%05d", i),
		); err != nil {
			return dvr.InstanceConfig{}, fmt.Errorf("insert dynamic mapping row %d: %w", i, err)
		}
		orderIndex++
	}

	if err := tx.Commit(); err != nil {
		return dvr.InstanceConfig{}, fmt.Errorf("Commit(): %w", err)
	}
	return instance, nil
}

func insertDVRFixtureChannel(
	ctx context.Context,
	stmt *sql.Stmt,
	channelClass string,
	channelKey string,
	guideNumber string,
	guideName string,
	orderIndex int,
	enabled int,
	now int64,
) (int64, error) {
	result, err := stmt.ExecContext(
		ctx,
		channelClass,
		channelKey,
		guideNumber,
		guideName,
		orderIndex,
		enabled,
		now,
		now,
	)
	if err != nil {
		return 0, fmt.Errorf("insert published channel %q: %w", guideNumber, err)
	}
	channelID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("LastInsertId() for guide %q: %w", guideNumber, err)
	}
	return channelID, nil
}

func explainListChannelsForDVRSyncPagedPlan(
	ctx context.Context,
	store *Store,
	dvrInstanceID int64,
	enabledOnly bool,
	includeDynamic bool,
	limit int,
	offset int,
) ([]string, error) {
	filterSQL, filterArgs := dvrSyncChannelFilterSQL(enabledOnly, includeDynamic)

	args := make([]any, 0, 1+len(filterArgs)+2)
	args = append(args, dvrInstanceID)
	args = append(args, filterArgs...)
	args = append(args, limit, offset)

	rows, err := store.db.QueryContext(
		ctx,
		`
		EXPLAIN QUERY PLAN
		SELECT
			pc.channel_id,
			pc.guide_number,
			pc.guide_name,
			pc.enabled,
			COALESCE(m.dvr_lineup_id, ''),
			COALESCE(m.dvr_lineup_channel, ''),
			COALESCE(m.dvr_station_ref, ''),
			COALESCE(m.dvr_callsign_hint, '')
		FROM published_channels pc
		LEFT JOIN published_channel_dvr_map m
		  ON m.channel_id = pc.channel_id
		 AND m.dvr_instance_id = ?
		`+filterSQL+`
		ORDER BY pc.order_index ASC
		LIMIT ? OFFSET ?
	`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	details := make([]string, 0, 4)
	for rows.Next() {
		var (
			id       int
			parentID int
			unused   int
			detail   string
		)
		if err := rows.Scan(&id, &parentID, &unused, &detail); err != nil {
			return nil, err
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return details, nil
}
