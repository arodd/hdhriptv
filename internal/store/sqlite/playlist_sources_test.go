package sqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/playlist"
)

func TestOpenEnsuresPrimaryPlaylistSourceAndSchema(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	exists, err := store.columnExists(ctx, "playlist_items", "playlist_source_id")
	if err != nil {
		t.Fatalf("columnExists(playlist_items.playlist_source_id) error = %v", err)
	}
	if !exists {
		t.Fatal("expected playlist_items.playlist_source_id to exist after Open()")
	}

	for _, indexName := range []string{
		"idx_playlist_sources_order",
		"idx_playlist_source_active_name_item",
		"idx_playlist_source_active_key_name_item",
		"idx_playlist_source_active_group_name",
		"idx_playlist_active_source_group_name_name_item",
	} {
		ok, err := indexExists(ctx, store.db, indexName)
		if err != nil {
			t.Fatalf("indexExists(%s) error = %v", indexName, err)
		}
		if !ok {
			t.Fatalf("expected %s to exist after Open()", indexName)
		}
	}

	sources, err := store.ListPlaylistSources(ctx)
	if err != nil {
		t.Fatalf("ListPlaylistSources() error = %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("len(ListPlaylistSources) = %d, want 1", len(sources))
	}

	primary := sources[0]
	if primary.SourceID != primaryPlaylistSourceID {
		t.Fatalf("primary source_id = %d, want %d", primary.SourceID, primaryPlaylistSourceID)
	}
	if primary.SourceKey != primaryPlaylistSourceKey {
		t.Fatalf("primary source_key = %q, want %q", primary.SourceKey, primaryPlaylistSourceKey)
	}
	if primary.Name == "" {
		t.Fatal("primary source name is empty, want non-empty")
	}
	if primary.TunerCount < 1 {
		t.Fatalf("primary tuner_count = %d, want >= 1", primary.TunerCount)
	}
	if !primary.Enabled {
		t.Fatal("primary source enabled = false, want true")
	}
	if primary.OrderIndex != 0 {
		t.Fatalf("primary order_index = %d, want 0", primary.OrderIndex)
	}
}

func TestOpenLegacyBackfillsPlaylistSourceIDAndSeedsPrimaryFromSetting(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy-playlist-sources.db")

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
		INSERT INTO settings(key, value) VALUES ('playlist.url', 'http://example.com/legacy.m3u');
		INSERT INTO playlist_items(item_key, channel_key, name, group_name, stream_url, tvg_id, tvg_logo, attrs_json, first_seen_at, last_seen_at, active, updated_at)
		VALUES ('src:legacy:001', 'name:legacy', 'Legacy', 'Legacy', 'http://example.com/legacy.ts', '', '', '{}', 1, 1, 1, 1);
	`); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("seed legacy schema error = %v", err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatalf("legacyDB.Close() error = %v", err)
	}

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(legacy db) error = %v", err)
	}
	defer store.Close()

	exists, err := store.columnExists(ctx, "playlist_items", "playlist_source_id")
	if err != nil {
		t.Fatalf("columnExists(playlist_items.playlist_source_id) error = %v", err)
	}
	if !exists {
		t.Fatal("expected playlist_items.playlist_source_id to exist after Open()")
	}

	var sourceID int64
	if err := store.db.QueryRowContext(ctx, `SELECT playlist_source_id FROM playlist_items WHERE item_key = ?`, "src:legacy:001").Scan(&sourceID); err != nil {
		t.Fatalf("query playlist_source_id for legacy item error = %v", err)
	}
	if sourceID != primaryPlaylistSourceID {
		t.Fatalf("legacy playlist item source_id = %d, want %d", sourceID, primaryPlaylistSourceID)
	}

	primary, err := store.GetPlaylistSource(ctx, primaryPlaylistSourceID)
	if err != nil {
		t.Fatalf("GetPlaylistSource(primary) error = %v", err)
	}
	if primary.PlaylistURL != "http://example.com/legacy.m3u" {
		t.Fatalf("primary playlist_url = %q, want legacy setting URL", primary.PlaylistURL)
	}
}

func TestUpsertPlaylistItemsForSourceMarksInactiveBySourceOnly(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Backup",
		PlaylistURL: "http://example.com/backup.m3u",
		TunerCount:  3,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}

	if err := store.UpsertPlaylistItemsForSource(ctx, primaryPlaylistSourceID, []playlist.Item{{
		ItemKey:    "src:primary:item:001",
		ChannelKey: "name:primary-one",
		Name:       "Primary One",
		Group:      "News",
		StreamURL:  "http://example.com/primary/one.ts",
	}}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(primary initial) error = %v", err)
	}
	if err := store.UpsertPlaylistItemsForSource(ctx, secondary.SourceID, []playlist.Item{{
		ItemKey:    "src:secondary:item:001",
		ChannelKey: "name:secondary-one",
		Name:       "Secondary One",
		Group:      "Sports",
		StreamURL:  "http://example.com/secondary/one.ts",
	}}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(secondary initial) error = %v", err)
	}

	if err := store.UpsertPlaylistItemsForSource(ctx, primaryPlaylistSourceID, []playlist.Item{{
		ItemKey:    "src:primary:item:001",
		ChannelKey: "name:primary-one",
		Name:       "Primary One HD",
		Group:      "News",
		StreamURL:  "http://example.com/primary/one-hd.ts",
	}}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(primary refresh) error = %v", err)
	}

	var (
		secondaryActive int
		secondaryTag    int64
	)
	if err := store.db.QueryRowContext(
		ctx,
		`SELECT active, playlist_source_id FROM playlist_items WHERE item_key = ?`,
		"src:secondary:item:001",
	).Scan(&secondaryActive, &secondaryTag); err != nil {
		t.Fatalf("query secondary row after primary refresh error = %v", err)
	}
	if secondaryActive != 1 {
		t.Fatalf("secondary active after primary refresh = %d, want 1", secondaryActive)
	}
	if secondaryTag != secondary.SourceID {
		t.Fatalf("secondary playlist_source_id = %d, want %d", secondaryTag, secondary.SourceID)
	}

	if err := store.UpsertPlaylistItemsForSource(ctx, secondary.SourceID, nil); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(secondary empty refresh) error = %v", err)
	}

	var (
		primaryActive int
		primaryTag    int64
	)
	if err := store.db.QueryRowContext(
		ctx,
		`SELECT active, playlist_source_id FROM playlist_items WHERE item_key = ?`,
		"src:primary:item:001",
	).Scan(&primaryActive, &primaryTag); err != nil {
		t.Fatalf("query primary row error = %v", err)
	}
	if primaryActive != 1 {
		t.Fatalf("primary active after secondary empty refresh = %d, want 1", primaryActive)
	}
	if primaryTag != primaryPlaylistSourceID {
		t.Fatalf("primary playlist_source_id = %d, want %d", primaryTag, primaryPlaylistSourceID)
	}

	if err := store.db.QueryRowContext(
		ctx,
		`SELECT active FROM playlist_items WHERE item_key = ?`,
		"src:secondary:item:001",
	).Scan(&secondaryActive); err != nil {
		t.Fatalf("query secondary row after secondary empty refresh error = %v", err)
	}
	if secondaryActive != 0 {
		t.Fatalf("secondary active after secondary empty refresh = %d, want 0", secondaryActive)
	}
}

func TestUpsertPlaylistItemsForSourceUnknownSource(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	err = store.UpsertPlaylistItemsForSource(ctx, 9999, []playlist.Item{{
		ItemKey:    "src:unknown:item:001",
		ChannelKey: "name:unknown",
		Name:       "Unknown",
		Group:      "Unknown",
		StreamURL:  "http://example.com/unknown.ts",
	}})
	if !errors.Is(err, playlist.ErrPlaylistSourceNotFound) {
		t.Fatalf("UpsertPlaylistItemsForSource(unknown source) error = %v, want ErrPlaylistSourceNotFound", err)
	}
}

func TestPlaylistSourceCRUDUpdateAndDelete(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	sourceTwo, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Backup A",
		PlaylistURL: "http://example.com/backup-a.m3u",
		TunerCount:  4,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(source two) error = %v", err)
	}
	sourceThree, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Backup B",
		PlaylistURL: "http://example.com/backup-b.m3u",
		TunerCount:  5,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(source three) error = %v", err)
	}

	if _, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        strings.ToLower(sourceTwo.Name),
		PlaylistURL: "http://example.com/another.m3u",
		TunerCount:  2,
	}); err == nil {
		t.Fatal("CreatePlaylistSource(case-variant duplicate name) error = nil, want duplicate-name failure")
	}
	if _, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Unique Name",
		PlaylistURL: "HTTP://EXAMPLE.COM/backup-a.m3u",
		TunerCount:  2,
	}); err == nil {
		t.Fatal("CreatePlaylistSource(canonical duplicate URL) error = nil, want duplicate-url failure")
	}

	sources, err := store.ListPlaylistSources(ctx)
	if err != nil {
		t.Fatalf("ListPlaylistSources() error = %v", err)
	}
	if len(sources) != 3 {
		t.Fatalf("initial source count = %d, want 3", len(sources))
	}

	updatedName := "Backup A Updated"
	updatedTunerCount := 6
	updatedEnabled := false
	updated, err := store.UpdatePlaylistSource(ctx, sourceTwo.SourceID, playlist.PlaylistSourceUpdate{
		Name:       &updatedName,
		TunerCount: &updatedTunerCount,
		Enabled:    &updatedEnabled,
	})
	if err != nil {
		t.Fatalf("UpdatePlaylistSource() error = %v", err)
	}
	if updated.Name != updatedName || updated.TunerCount != updatedTunerCount || updated.Enabled != updatedEnabled {
		t.Fatalf("updated source = %+v, want name=%q tuners=%d enabled=%t", updated, updatedName, updatedTunerCount, updatedEnabled)
	}

	caseVariantUpdatedName := strings.ToLower(updatedName)
	if _, err := store.UpdatePlaylistSource(ctx, sourceThree.SourceID, playlist.PlaylistSourceUpdate{
		Name: &caseVariantUpdatedName,
	}); err == nil {
		t.Fatal("UpdatePlaylistSource(case-variant duplicate name) error = nil, want duplicate-name failure")
	}
	canonicalVariantURL := "HTTP://EXAMPLE.COM/backup-a.m3u"
	if _, err := store.UpdatePlaylistSource(ctx, sourceThree.SourceID, playlist.PlaylistSourceUpdate{
		PlaylistURL: &canonicalVariantURL,
	}); err == nil {
		t.Fatal("UpdatePlaylistSource(canonical duplicate URL) error = nil, want duplicate-url failure")
	}

	if err := store.DeletePlaylistSource(ctx, primaryPlaylistSourceID); !errors.Is(err, playlist.ErrPrimaryPlaylistSourceDelete) {
		t.Fatalf("DeletePlaylistSource(primary) error = %v, want ErrPrimaryPlaylistSourceDelete", err)
	}

	if err := store.UpsertPlaylistItemsForSource(ctx, sourceTwo.SourceID, []playlist.Item{{
		ItemKey:    "src:source-two:item:001",
		ChannelKey: "name:source-two",
		Name:       "Source Two",
		Group:      "Movies",
		StreamURL:  "http://example.com/source-two.ts",
	}}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(source two) error = %v", err)
	}

	if err := store.DeletePlaylistSource(ctx, sourceTwo.SourceID); err != nil {
		t.Fatalf("DeletePlaylistSource(source two) error = %v", err)
	}
	if _, err := store.GetPlaylistSource(ctx, sourceTwo.SourceID); !errors.Is(err, playlist.ErrPlaylistSourceNotFound) {
		t.Fatalf("GetPlaylistSource(deleted source) error = %v, want ErrPlaylistSourceNotFound", err)
	}

	var remainingCatalogRows int
	if err := store.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM playlist_items WHERE item_key = ?`,
		"src:source-two:item:001",
	).Scan(&remainingCatalogRows); err != nil {
		t.Fatalf("query deleted-source playlist item rows error = %v", err)
	}
	if remainingCatalogRows != 0 {
		t.Fatalf("remaining deleted-source catalog rows = %d, want 0", remainingCatalogRows)
	}

	sources, err = store.ListPlaylistSources(ctx)
	if err != nil {
		t.Fatalf("ListPlaylistSources() after delete error = %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("source count after delete = %d, want 2", len(sources))
	}
	if sources[0].SourceID != primaryPlaylistSourceID || sources[0].OrderIndex != 0 {
		t.Fatalf("primary source after delete = %+v, want source_id=%d order_index=0", sources[0], primaryPlaylistSourceID)
	}
	if sources[1].SourceID != sourceThree.SourceID || sources[1].OrderIndex != 1 {
		t.Fatalf("remaining source after delete = %+v, want source_id=%d order_index=1", sources[1], sourceThree.SourceID)
	}
}

func TestPlaylistSourceMutationsRequireAtLeastOneEnabledSource(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Enabled Secondary",
		PlaylistURL: "http://example.com/enabled-secondary.m3u",
		TunerCount:  3,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}

	disable := false
	if _, err := store.UpdatePlaylistSource(ctx, primaryPlaylistSourceID, playlist.PlaylistSourceUpdate{
		Enabled: &disable,
	}); err != nil {
		t.Fatalf("UpdatePlaylistSource(primary disable) error = %v", err)
	}

	if err := store.DeletePlaylistSource(ctx, secondary.SourceID); !errors.Is(err, playlist.ErrNoEnabledPlaylistSources) {
		t.Fatalf("DeletePlaylistSource(last enabled) error = %v, want ErrNoEnabledPlaylistSources", err)
	}
	if _, err := store.GetPlaylistSource(ctx, secondary.SourceID); err != nil {
		t.Fatalf("GetPlaylistSource(secondary) after failed delete error = %v", err)
	}

	if _, err := store.UpdatePlaylistSource(ctx, secondary.SourceID, playlist.PlaylistSourceUpdate{
		Enabled: &disable,
	}); !errors.Is(err, playlist.ErrNoEnabledPlaylistSources) {
		t.Fatalf("UpdatePlaylistSource(disable last enabled) error = %v, want ErrNoEnabledPlaylistSources", err)
	}

	refreshedSecondary, err := store.GetPlaylistSource(ctx, secondary.SourceID)
	if err != nil {
		t.Fatalf("GetPlaylistSource(secondary after failed disable) error = %v", err)
	}
	if !refreshedSecondary.Enabled {
		t.Fatal("secondary source enabled flag changed after failed disable; want rollback")
	}
}

func TestDeletePlaylistSourceRemovesOwnedCatalogAndPrunesDynamicSourceReferences(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Delete Secondary",
		PlaylistURL: "http://example.com/delete-secondary.m3u",
		TunerCount:  3,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}

	if err := store.UpsertPlaylistItemsForSource(ctx, primaryPlaylistSourceID, []playlist.Item{
		{
			ItemKey:    "src:delete:primary:one",
			ChannelKey: "tvg:delete:channel",
			Name:       "Delete Primary One",
			Group:      "Delete Group",
			StreamURL:  "http://example.com/delete-primary-one.ts",
		},
		{
			ItemKey:    "src:delete:primary:two",
			ChannelKey: "tvg:delete:channel",
			Name:       "Delete Primary Two",
			Group:      "Delete Group",
			StreamURL:  "http://example.com/delete-primary-two.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(primary) error = %v", err)
	}
	if err := store.UpsertPlaylistItemsForSource(ctx, secondary.SourceID, []playlist.Item{
		{
			ItemKey:    "src:delete:secondary:one",
			ChannelKey: "tvg:delete:channel",
			Name:       "Delete Secondary One",
			Group:      "Delete Group",
			StreamURL:  "http://example.com/delete-secondary-one.ts",
		},
		{
			ItemKey:    "src:delete:secondary:dynamic",
			ChannelKey: "name:delete-secondary-dynamic",
			Name:       "Secondary Dynamic Candidate",
			Group:      "Delete Group",
			StreamURL:  "http://example.com/delete-secondary-dynamic.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(secondary) error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, "src:delete:primary:one", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	secondaryScopedChannel, err := store.CreateChannelFromItem(ctx, "src:delete:primary:two", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(secondary scoped channel) error = %v", err)
	}
	if _, err := store.AddSource(ctx, channel.ChannelID, "src:delete:secondary:one", false); err != nil {
		t.Fatalf("AddSource(secondary) error = %v", err)
	}
	if _, err := store.AddSource(ctx, channel.ChannelID, "src:delete:primary:two", false); err != nil {
		t.Fatalf("AddSource(primary two) error = %v", err)
	}

	rule := &channels.DynamicSourceRule{
		Enabled:     true,
		GroupNames:  []string{"Delete Group"},
		SourceIDs:   []int64{primaryPlaylistSourceID, secondary.SourceID},
		SearchQuery: "delete",
	}
	if _, err := store.UpdateChannel(ctx, channel.ChannelID, nil, nil, rule); err != nil {
		t.Fatalf("UpdateChannel(dynamic rule) error = %v", err)
	}
	secondaryOnlyRule := &channels.DynamicSourceRule{
		Enabled:     true,
		GroupNames:  []string{"Delete Group"},
		SourceIDs:   []int64{secondary.SourceID},
		SearchQuery: "secondary",
	}
	if _, err := store.UpdateChannel(ctx, secondaryScopedChannel.ChannelID, nil, nil, secondaryOnlyRule); err != nil {
		t.Fatalf("UpdateChannel(secondary scoped dynamic rule) error = %v", err)
	}

	enabled := true
	query, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Enabled:     &enabled,
		Name:        "Delete Secondary Dynamic Query",
		GroupNames:  []string{"Delete Group"},
		SourceIDs:   []int64{secondary.SourceID},
		SearchQuery: "secondary dynamic",
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery() error = %v", err)
	}

	beforeSync, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(before delete) error = %v", err)
	}
	if beforeSync.ChannelsAdded < 1 {
		t.Fatalf("SyncDynamicChannelBlocks(before delete) added = %d, want at least 1", beforeSync.ChannelsAdded)
	}

	beforeGenerated, beforeTotal, err := store.ListDynamicGeneratedChannelsPaged(ctx, query.QueryID, 100, 0)
	if err != nil {
		t.Fatalf("ListDynamicGeneratedChannelsPaged(before delete) error = %v", err)
	}
	if beforeTotal == 0 || len(beforeGenerated) == 0 {
		t.Fatalf("generated channel count before delete = total:%d len:%d, want >0", beforeTotal, len(beforeGenerated))
	}
	generatedChannelID := beforeGenerated[0].ChannelID

	dvrInstance, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance() error = %v", err)
	}
	if _, err := store.db.ExecContext(
		ctx,
		`INSERT INTO published_channel_dvr_map(channel_id, dvr_instance_id, dvr_lineup_id, dvr_lineup_channel)
		 VALUES (?, ?, ?, ?)`,
		generatedChannelID,
		dvrInstance.ID,
		"delete-test-lineup",
		"100.1",
	); err != nil {
		t.Fatalf("insert generated-channel dvr mapping error = %v", err)
	}

	if err := store.DeletePlaylistSource(ctx, secondary.SourceID); err != nil {
		t.Fatalf("DeletePlaylistSource(secondary) error = %v", err)
	}

	if _, err := store.GetPlaylistSource(ctx, secondary.SourceID); !errors.Is(err, playlist.ErrPlaylistSourceNotFound) {
		t.Fatalf("GetPlaylistSource(secondary after delete) error = %v, want ErrPlaylistSourceNotFound", err)
	}

	var secondaryCatalogRows int
	if err := store.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM playlist_items WHERE playlist_source_id = ?`,
		secondary.SourceID,
	).Scan(&secondaryCatalogRows); err != nil {
		t.Fatalf("count secondary catalog rows after delete error = %v", err)
	}
	if secondaryCatalogRows != 0 {
		t.Fatalf("secondary catalog row count after delete = %d, want 0", secondaryCatalogRows)
	}

	sources, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources(channel after delete) error = %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("len(channel sources after delete) = %d, want 2", len(sources))
	}
	for i, source := range sources {
		if source.PriorityIndex != i {
			t.Fatalf("source[%d] priority_index = %d, want %d", i, source.PriorityIndex, i)
		}
		if source.ItemKey == "src:delete:secondary:one" {
			t.Fatalf("deleted-source item %q still attached to channel", source.ItemKey)
		}
	}

	updatedChannel, err := store.GetChannelByGuideNumber(ctx, channel.GuideNumber)
	if err != nil {
		t.Fatalf("GetChannelByGuideNumber(channel after delete) error = %v", err)
	}
	if got, want := updatedChannel.DynamicRule.SourceIDs, []int64{primaryPlaylistSourceID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("channel dynamic_rule.source_ids after source delete = %v, want %v", got, want)
	}
	if !updatedChannel.DynamicRule.Enabled {
		t.Fatal("channel dynamic_rule.enabled after source delete = false, want true when scoped sources remain")
	}

	updatedSecondaryScopedChannel, err := store.GetChannelByGuideNumber(ctx, secondaryScopedChannel.GuideNumber)
	if err != nil {
		t.Fatalf("GetChannelByGuideNumber(secondary scoped channel after delete) error = %v", err)
	}
	if len(updatedSecondaryScopedChannel.DynamicRule.SourceIDs) != 0 {
		t.Fatalf("secondary-scoped channel dynamic_rule.source_ids after source delete = %v, want empty", updatedSecondaryScopedChannel.DynamicRule.SourceIDs)
	}
	if updatedSecondaryScopedChannel.DynamicRule.Enabled {
		t.Fatal("secondary-scoped channel dynamic_rule.enabled after source delete = true, want false to avoid implicit all-source widening")
	}

	updatedQuery, err := store.GetDynamicChannelQuery(ctx, query.QueryID)
	if err != nil {
		t.Fatalf("GetDynamicChannelQuery(after delete) error = %v", err)
	}
	if len(updatedQuery.SourceIDs) != 0 {
		t.Fatalf("dynamic query source_ids after source delete = %v, want empty", updatedQuery.SourceIDs)
	}
	if updatedQuery.Enabled {
		t.Fatal("dynamic query enabled after source delete = true, want false to avoid implicit all-source widening")
	}

	afterGenerated, afterTotal, err := store.ListDynamicGeneratedChannelsPaged(ctx, query.QueryID, 100, 0)
	if err != nil {
		t.Fatalf("ListDynamicGeneratedChannelsPaged(after delete) error = %v", err)
	}
	if afterTotal != 0 || len(afterGenerated) != 0 {
		t.Fatalf("generated channels after source delete = total:%d len:%d, want 0", afterTotal, len(afterGenerated))
	}

	var staleMappingCount int
	if err := store.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM published_channel_dvr_map WHERE channel_id = ?`,
		generatedChannelID,
	).Scan(&staleMappingCount); err != nil {
		t.Fatalf("count dvr mappings for deleted generated channel error = %v", err)
	}
	if staleMappingCount != 0 {
		t.Fatalf("dvr mapping count for deleted generated channel = %d, want 0", staleMappingCount)
	}
}

func TestBulkUpdatePlaylistSourcesRejectsAllDisabled(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Bulk Secondary",
		PlaylistURL: "http://example.com/bulk-secondary.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}

	err = store.BulkUpdatePlaylistSources(ctx, []playlist.PlaylistSourceBulkUpdate{
		{
			SourceID:    primaryPlaylistSourceID,
			Name:        "Primary",
			PlaylistURL: "",
			TunerCount:  2,
			Enabled:     false,
		},
		{
			SourceID:    secondary.SourceID,
			Name:        secondary.Name,
			PlaylistURL: secondary.PlaylistURL,
			TunerCount:  secondary.TunerCount,
			Enabled:     false,
		},
	})
	if !errors.Is(err, playlist.ErrNoEnabledPlaylistSources) {
		t.Fatalf("BulkUpdatePlaylistSources(all disabled) error = %v, want ErrNoEnabledPlaylistSources", err)
	}
}

func TestBulkUpdatePlaylistSourcesRejectsOrderChanges(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Bulk Reorder Secondary",
		PlaylistURL: "http://example.com/bulk-reorder-secondary.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}

	err = store.BulkUpdatePlaylistSources(ctx, []playlist.PlaylistSourceBulkUpdate{
		{
			SourceID:    secondary.SourceID,
			Name:        "Bulk Reorder Secondary",
			PlaylistURL: "http://example.com/bulk-reorder-secondary-updated.m3u",
			TunerCount:  4,
			Enabled:     true,
		},
		{
			SourceID:    primaryPlaylistSourceID,
			Name:        "Primary",
			PlaylistURL: "http://example.com/primary-updated.m3u",
			TunerCount:  3,
			Enabled:     true,
		},
	})
	if !errors.Is(err, playlist.ErrPlaylistSourceOrderDrift) {
		t.Fatalf("BulkUpdatePlaylistSources(reorder attempt) error = %v, want ErrPlaylistSourceOrderDrift", err)
	}
}

func TestBulkUpdatePlaylistSourcesRollsBackOnFailure(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	sourceTwo, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Bulk Backup A",
		PlaylistURL: "http://example.com/bulk-backup-a.m3u",
		TunerCount:  3,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(source two) error = %v", err)
	}
	sourceThree, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Bulk Backup B",
		PlaylistURL: "http://example.com/bulk-backup-b.m3u",
		TunerCount:  4,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(source three) error = %v", err)
	}

	beforePrimary, err := store.GetPlaylistSource(ctx, primaryPlaylistSourceID)
	if err != nil {
		t.Fatalf("GetPlaylistSource(primary before) error = %v", err)
	}
	beforeTwo, err := store.GetPlaylistSource(ctx, sourceTwo.SourceID)
	if err != nil {
		t.Fatalf("GetPlaylistSource(source two before) error = %v", err)
	}
	beforeThree, err := store.GetPlaylistSource(ctx, sourceThree.SourceID)
	if err != nil {
		t.Fatalf("GetPlaylistSource(source three before) error = %v", err)
	}

	err = store.BulkUpdatePlaylistSources(ctx, []playlist.PlaylistSourceBulkUpdate{
		{
			SourceID:    primaryPlaylistSourceID,
			Name:        "Primary Updated",
			PlaylistURL: "http://example.com/primary-updated.m3u",
			TunerCount:  9,
			Enabled:     true,
		},
		{
			SourceID:    sourceTwo.SourceID,
			Name:        beforeThree.Name,
			PlaylistURL: beforeThree.PlaylistURL,
			TunerCount:  6,
			Enabled:     true,
		},
		{
			SourceID:    sourceThree.SourceID,
			Name:        beforeTwo.Name,
			PlaylistURL: beforeTwo.PlaylistURL,
			TunerCount:  7,
			Enabled:     true,
		},
	})
	if err == nil {
		t.Fatal("BulkUpdatePlaylistSources(swapped unique fields) error = nil, want conflict failure")
	}

	afterPrimary, err := store.GetPlaylistSource(ctx, primaryPlaylistSourceID)
	if err != nil {
		t.Fatalf("GetPlaylistSource(primary after) error = %v", err)
	}
	afterTwo, err := store.GetPlaylistSource(ctx, sourceTwo.SourceID)
	if err != nil {
		t.Fatalf("GetPlaylistSource(source two after) error = %v", err)
	}
	afterThree, err := store.GetPlaylistSource(ctx, sourceThree.SourceID)
	if err != nil {
		t.Fatalf("GetPlaylistSource(source three after) error = %v", err)
	}

	if afterPrimary.Name != beforePrimary.Name || afterPrimary.PlaylistURL != beforePrimary.PlaylistURL || afterPrimary.TunerCount != beforePrimary.TunerCount {
		t.Fatalf("primary changed after failed bulk update = %+v, want %+v", afterPrimary, beforePrimary)
	}
	if afterTwo.Name != beforeTwo.Name || afterTwo.PlaylistURL != beforeTwo.PlaylistURL || afterTwo.TunerCount != beforeTwo.TunerCount {
		t.Fatalf("source two changed after failed bulk update = %+v, want %+v", afterTwo, beforeTwo)
	}
	if afterThree.Name != beforeThree.Name || afterThree.PlaylistURL != beforeThree.PlaylistURL || afterThree.TunerCount != beforeThree.TunerCount {
		t.Fatalf("source three changed after failed bulk update = %+v, want %+v", afterThree, beforeThree)
	}

	legacyURL, err := store.GetSetting(ctx, SettingPlaylistURL)
	if err != nil {
		t.Fatalf("GetSetting(playlist.url) after failed bulk update error = %v", err)
	}
	if legacyURL != beforePrimary.PlaylistURL {
		t.Fatalf("playlist.url after failed bulk update = %q, want %q", legacyURL, beforePrimary.PlaylistURL)
	}
}

func TestUpdatePrimaryPlaylistSourceMirrorsLegacyPlaylistURLSetting(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	updatedURL := "http://example.com/primary-updated.m3u"
	if _, err := store.UpdatePlaylistSource(ctx, primaryPlaylistSourceID, playlist.PlaylistSourceUpdate{
		PlaylistURL: &updatedURL,
	}); err != nil {
		t.Fatalf("UpdatePlaylistSource(primary URL) error = %v", err)
	}

	mirroredURL, err := store.GetSetting(ctx, SettingPlaylistURL)
	if err != nil {
		t.Fatalf("GetSetting(playlist.url) error = %v", err)
	}
	if mirroredURL != updatedURL {
		t.Fatalf("playlist.url setting after primary update = %q, want %q", mirroredURL, updatedURL)
	}

	emptyURL := ""
	if _, err := store.UpdatePlaylistSource(ctx, primaryPlaylistSourceID, playlist.PlaylistSourceUpdate{
		PlaylistURL: &emptyURL,
	}); err != nil {
		t.Fatalf("UpdatePlaylistSource(primary empty URL) error = %v", err)
	}

	mirroredURL, err = store.GetSetting(ctx, SettingPlaylistURL)
	if err != nil {
		t.Fatalf("GetSetting(playlist.url) after empty update error = %v", err)
	}
	if mirroredURL != "" {
		t.Fatalf("playlist.url setting after primary empty update = %q, want empty", mirroredURL)
	}

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Mirror Secondary",
		PlaylistURL: "http://example.com/mirror-secondary.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}

	if _, err := store.UpdatePlaylistSource(ctx, secondary.SourceID, playlist.PlaylistSourceUpdate{
		PlaylistURL: &emptyURL,
	}); err == nil {
		t.Fatal("UpdatePlaylistSource(non-primary empty URL) error = nil, want validation failure")
	}
}

func TestListActiveGroupNamesBySourceIDs(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Groups Secondary",
		PlaylistURL: "http://example.com/groups-secondary.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}

	if err := store.UpsertPlaylistItemsForSource(ctx, primaryPlaylistSourceID, []playlist.Item{{
		ItemKey:    "src:groups:primary:001",
		ChannelKey: "name:groups-primary-news",
		Name:       "Primary News",
		Group:      "News",
		StreamURL:  "http://example.com/groups/primary-news.ts",
	}, {
		ItemKey:    "src:groups:primary:002",
		ChannelKey: "name:groups-primary-sports",
		Name:       "Primary Sports",
		Group:      "Sports",
		StreamURL:  "http://example.com/groups/primary-sports.ts",
	}}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(primary groups) error = %v", err)
	}

	if err := store.UpsertPlaylistItemsForSource(ctx, secondary.SourceID, []playlist.Item{{
		ItemKey:    "src:groups:secondary:001",
		ChannelKey: "name:groups-secondary-kids",
		Name:       "Secondary Kids",
		Group:      "Kids",
		StreamURL:  "http://example.com/groups/secondary-kids.ts",
	}}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(secondary groups) error = %v", err)
	}

	allGroups, err := store.ListActiveGroupNamesBySourceIDs(ctx, nil)
	if err != nil {
		t.Fatalf("ListActiveGroupNamesBySourceIDs(nil) error = %v", err)
	}
	if got, want := allGroups, []string{"Kids", "News", "Sports"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListActiveGroupNamesBySourceIDs(nil) = %v, want %v", got, want)
	}

	secondaryOnly, err := store.ListActiveGroupNamesBySourceIDs(ctx, []int64{secondary.SourceID})
	if err != nil {
		t.Fatalf("ListActiveGroupNamesBySourceIDs(secondary) error = %v", err)
	}
	if got, want := secondaryOnly, []string{"Kids"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListActiveGroupNamesBySourceIDs(secondary) = %v, want %v", got, want)
	}

	combined, err := store.ListActiveGroupNamesBySourceIDs(ctx, []int64{primaryPlaylistSourceID, secondary.SourceID})
	if err != nil {
		t.Fatalf("ListActiveGroupNamesBySourceIDs(combined) error = %v", err)
	}
	if got, want := combined, []string{"Kids", "News", "Sports"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ListActiveGroupNamesBySourceIDs(combined) = %v, want %v", got, want)
	}

	_, err = store.ListActiveGroupNamesBySourceIDs(ctx, []int64{0})
	if err == nil {
		t.Fatal("ListActiveGroupNamesBySourceIDs(invalid source id) error = nil, want validation error")
	}
}

func TestGetPlaylistSourceMissing(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	_, err = store.GetPlaylistSource(ctx, 9999)
	if !errors.Is(err, playlist.ErrPlaylistSourceNotFound) {
		t.Fatalf("GetPlaylistSource(missing) error = %v, want ErrPlaylistSourceNotFound", err)
	}
}

func TestPlaylistSourcePrimaryBootstrapNameCollisionFallback(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "primary-bootstrap-name-collision.db")

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
		CREATE TABLE IF NOT EXISTS playlist_sources (
		  source_id    INTEGER PRIMARY KEY AUTOINCREMENT,
		  source_key   TEXT NOT NULL UNIQUE,
		  name         TEXT NOT NULL UNIQUE,
		  playlist_url TEXT NOT NULL DEFAULT '' UNIQUE,
		  tuner_count  INTEGER NOT NULL,
		  enabled      INTEGER NOT NULL DEFAULT 1,
		  order_index  INTEGER NOT NULL,
		  created_at   INTEGER NOT NULL,
		  updated_at   INTEGER NOT NULL
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_playlist_sources_order ON playlist_sources(order_index);
		INSERT INTO playlist_sources(source_id, source_key, name, playlist_url, tuner_count, enabled, order_index, created_at, updated_at)
		VALUES (2, 'secondary', 'Primary', 'http://example.com/existing.m3u', 3, 1, 0, 1, 1);
	`); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("seed collision schema error = %v", err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatalf("legacyDB.Close() error = %v", err)
	}

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(collision db) error = %v", err)
	}
	defer store.Close()

	sources, err := store.ListPlaylistSources(ctx)
	if err != nil {
		t.Fatalf("ListPlaylistSources() error = %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("len(ListPlaylistSources) = %d, want 2", len(sources))
	}

	nameSet := map[string]struct{}{}
	for _, source := range sources {
		if _, exists := nameSet[source.Name]; exists {
			t.Fatalf("duplicate playlist source name after bootstrap: %q", source.Name)
		}
		nameSet[source.Name] = struct{}{}
	}
	if _, ok := nameSet["Primary"]; !ok {
		t.Fatalf("expected at least one source named %q; names=%v", "Primary", keysFromNameSet(nameSet))
	}
}

func keysFromNameSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	return out
}

func TestPlaylistSourceCreateRejectsInvalidInputs(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	tests := []struct {
		name   string
		create playlist.PlaylistSourceCreate
	}{
		{
			name: "missing name",
			create: playlist.PlaylistSourceCreate{
				PlaylistURL: "http://example.com/a.m3u",
				TunerCount:  1,
			},
		},
		{
			name: "missing url",
			create: playlist.PlaylistSourceCreate{
				Name:       "Source",
				TunerCount: 1,
			},
		},
		{
			name: "invalid tuner count",
			create: playlist.PlaylistSourceCreate{
				Name:        "Source",
				PlaylistURL: "http://example.com/a.m3u",
				TunerCount:  0,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.CreatePlaylistSource(ctx, tc.create); err == nil {
				t.Fatalf("CreatePlaylistSource(%s) error = nil, want validation error", tc.name)
			}
		})
	}
}

func TestPlaylistSourceCreateGeneratesEightByteHexSourceKey(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	created, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Entropy Source",
		PlaylistURL: "http://example.com/entropy-source.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource() error = %v", err)
	}

	if created.SourceKey == "" {
		t.Fatal("created source_key is empty")
	}
	if created.SourceKey == primaryPlaylistSourceKey {
		t.Fatalf("created source_key = %q, want generated non-primary key", created.SourceKey)
	}
	if got, want := len(created.SourceKey), playlistSourceKeyBytes*2; got != want {
		t.Fatalf("len(source_key) = %d, want %d", got, want)
	}
	if _, err := hex.DecodeString(created.SourceKey); err != nil {
		t.Fatalf("source_key %q is not valid hex: %v", created.SourceKey, err)
	}
}

func TestPlaylistSourceCreateRetriesSourceKeyCollision(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const uniqueKey = "0011223344556677"
	candidates := []string{
		primaryPlaylistSourceKey, // collides with seeded primary source
		primaryPlaylistSourceKey, // collides again
		uniqueKey,
	}
	callCount := 0
	created, err := store.createPlaylistSourceWithKeyGenerator(
		ctx,
		playlist.PlaylistSourceCreate{
			Name:        "Collision Retry Source",
			PlaylistURL: "http://example.com/collision-retry.m3u",
			TunerCount:  3,
		},
		func() (string, error) {
			if callCount >= len(candidates) {
				return uniqueKey, nil
			}
			key := candidates[callCount]
			callCount++
			return key, nil
		},
	)
	if err != nil {
		t.Fatalf("createPlaylistSourceWithKeyGenerator(retry) error = %v", err)
	}
	if created.SourceKey != uniqueKey {
		t.Fatalf("created source_key = %q, want %q", created.SourceKey, uniqueKey)
	}
	if got, want := callCount, 3; got != want {
		t.Fatalf("source-key generator call count = %d, want %d", got, want)
	}
}

func TestPlaylistSourceCreateFailsAfterSourceKeyRetryBudgetExhausted(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	callCount := 0
	_, err = store.createPlaylistSourceWithKeyGenerator(
		ctx,
		playlist.PlaylistSourceCreate{
			Name:        "Collision Exhausted Source",
			PlaylistURL: "http://example.com/collision-exhausted.m3u",
			TunerCount:  3,
		},
		func() (string, error) {
			callCount++
			return primaryPlaylistSourceKey, nil
		},
	)
	if err == nil {
		t.Fatal("createPlaylistSourceWithKeyGenerator(exhausted retries) error = nil, want failure")
	}
	if !strings.Contains(err.Error(), "failed to generate unique source_key") {
		t.Fatalf("createPlaylistSourceWithKeyGenerator(exhausted retries) error = %v, want source_key exhaustion message", err)
	}
	if got, want := callCount, maxPlaylistSourceKeyAttempts; got != want {
		t.Fatalf("source-key generator call count = %d, want %d", got, want)
	}
}

func TestPlaylistSourceDeleteMissing(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	err = store.DeletePlaylistSource(ctx, 9999)
	if !errors.Is(err, playlist.ErrPlaylistSourceNotFound) {
		t.Fatalf("DeletePlaylistSource(missing) error = %v, want ErrPlaylistSourceNotFound", err)
	}
}
