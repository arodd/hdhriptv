package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/playlist"
)

func TestDeleteChannelRemovesChildRowsAndPrunesOrphans(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:delete-cleanup:primary",
			ChannelKey: "name:delete-cleanup-primary",
			Name:       "Delete Cleanup Primary",
			Group:      "Delete Cleanup",
			StreamURL:  "http://example.com/delete-cleanup-primary.ts",
		},
		{
			ItemKey:    "src:delete-cleanup:orphan",
			ChannelKey: "name:delete-cleanup-orphan",
			Name:       "Delete Cleanup Orphan",
			Group:      "Delete Cleanup",
			StreamURL:  "http://example.com/delete-cleanup-orphan.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, "src:delete-cleanup:primary", "", "", nil, channels.TraditionalGuideStart)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}

	dvrInstanceID := insertDeleteCleanupDVRInstanceForTest(t, ctx, store)
	if _, err := store.db.ExecContext(
		ctx,
		`INSERT INTO published_channel_dvr_map (
			channel_id,
			dvr_instance_id,
			dvr_lineup_id,
			dvr_lineup_channel
		)
		VALUES (?, ?, ?, ?)`,
		channel.ChannelID,
		dvrInstanceID,
		"USA-TEST",
		channel.GuideNumber,
	); err != nil {
		t.Fatalf("insert published_channel_dvr_map for channel error = %v", err)
	}

	if _, err := store.db.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("set PRAGMA foreign_keys=OFF error = %v", err)
	}
	orphanChannelID := channel.ChannelID + 1000
	now := time.Now().UTC().Unix()
	if _, err := store.db.ExecContext(
		ctx,
		`INSERT INTO channel_sources (
			channel_id,
			item_key,
			priority_index,
			enabled,
			association_type,
			created_at,
			updated_at
		)
		VALUES (?, ?, 0, 1, 'manual', ?, ?)`,
		orphanChannelID,
		"src:delete-cleanup:orphan",
		now,
		now,
	); err != nil {
		t.Fatalf("insert orphan channel_sources row error = %v", err)
	}
	if _, err := store.db.ExecContext(
		ctx,
		`INSERT INTO published_channel_dvr_map (
			channel_id,
			dvr_instance_id,
			dvr_lineup_id,
			dvr_lineup_channel
		)
		VALUES (?, ?, ?, ?)`,
		orphanChannelID,
		dvrInstanceID,
		"USA-TEST",
		"9999",
	); err != nil {
		t.Fatalf("insert orphan published_channel_dvr_map row error = %v", err)
	}

	if err := store.DeleteChannel(ctx, channel.ChannelID, channels.TraditionalGuideStart); err != nil {
		t.Fatalf("DeleteChannel() error = %v", err)
	}

	if got := countDeleteCleanupRowsForTest(t, ctx, store, `SELECT COUNT(*) FROM channel_sources WHERE channel_id = ?`, channel.ChannelID); got != 0 {
		t.Fatalf("channel_sources rows for deleted channel = %d, want 0", got)
	}
	if got := countDeleteCleanupRowsForTest(t, ctx, store, `SELECT COUNT(*) FROM published_channel_dvr_map WHERE channel_id = ?`, channel.ChannelID); got != 0 {
		t.Fatalf("published_channel_dvr_map rows for deleted channel = %d, want 0", got)
	}
	if got := countDeleteCleanupRowsForTest(t, ctx, store, `SELECT COUNT(*) FROM channel_sources WHERE channel_id = ?`, orphanChannelID); got != 0 {
		t.Fatalf("orphan channel_sources rows after delete = %d, want 0", got)
	}
	if got := countDeleteCleanupRowsForTest(t, ctx, store, `SELECT COUNT(*) FROM published_channel_dvr_map WHERE channel_id = ?`, orphanChannelID); got != 0 {
		t.Fatalf("orphan published_channel_dvr_map rows after delete = %d, want 0", got)
	}
}

func TestDeleteDynamicChannelQueryRemovesChildRowsAndPrunesOrphans(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:dynamic-delete-cleanup:one",
			ChannelKey: "name:dynamic-delete-cleanup-one",
			Name:       "Dynamic Delete Cleanup One",
			Group:      "News",
			StreamURL:  "http://example.com/dynamic-delete-cleanup-one.ts",
		},
		{
			ItemKey:    "src:dynamic-delete-cleanup:two",
			ChannelKey: "name:dynamic-delete-cleanup-two",
			Name:       "Dynamic Delete Cleanup Two",
			Group:      "News",
			StreamURL:  "http://example.com/dynamic-delete-cleanup-two.ts",
		},
		{
			ItemKey:    "src:dynamic-delete-cleanup:orphan",
			ChannelKey: "name:dynamic-delete-cleanup-orphan",
			Name:       "Dynamic Delete Cleanup Orphan",
			Group:      "News",
			StreamURL:  "http://example.com/dynamic-delete-cleanup-orphan.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	created, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Delete Cleanup Dynamic Query",
		GroupName:   "News",
		SearchQuery: "dynamic delete cleanup",
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery() error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}

	generatedRows, err := loadGeneratedChannelsForTest(ctx, store, created.QueryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest() error = %v", err)
	}
	if len(generatedRows) == 0 {
		t.Fatal("generatedRows is empty, want generated dynamic channels")
	}

	dvrInstanceID := insertDeleteCleanupDVRInstanceForTest(t, ctx, store)
	if _, err := store.db.ExecContext(
		ctx,
		`INSERT INTO published_channel_dvr_map (
			channel_id,
			dvr_instance_id,
			dvr_lineup_id,
			dvr_lineup_channel
		)
		VALUES (?, ?, ?, ?)`,
		generatedRows[0].ChannelID,
		dvrInstanceID,
		"USA-TEST",
		generatedRows[0].GuideNumber,
	); err != nil {
		t.Fatalf("insert published_channel_dvr_map for generated channel error = %v", err)
	}

	if _, err := store.db.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("set PRAGMA foreign_keys=OFF error = %v", err)
	}
	orphanChannelID := generatedRows[len(generatedRows)-1].ChannelID + 1000
	now := time.Now().UTC().Unix()
	if _, err := store.db.ExecContext(
		ctx,
		`INSERT INTO channel_sources (
			channel_id,
			item_key,
			priority_index,
			enabled,
			association_type,
			created_at,
			updated_at
		)
		VALUES (?, ?, 0, 1, 'manual', ?, ?)`,
		orphanChannelID,
		"src:dynamic-delete-cleanup:orphan",
		now,
		now,
	); err != nil {
		t.Fatalf("insert orphan channel_sources row error = %v", err)
	}
	if _, err := store.db.ExecContext(
		ctx,
		`INSERT INTO published_channel_dvr_map (
			channel_id,
			dvr_instance_id,
			dvr_lineup_id,
			dvr_lineup_channel
		)
		VALUES (?, ?, ?, ?)`,
		orphanChannelID,
		dvrInstanceID,
		"USA-TEST",
		"9998",
	); err != nil {
		t.Fatalf("insert orphan published_channel_dvr_map row error = %v", err)
	}

	if err := store.DeleteDynamicChannelQuery(ctx, created.QueryID); err != nil {
		t.Fatalf("DeleteDynamicChannelQuery() error = %v", err)
	}

	if _, _, err := store.ListDynamicGeneratedChannelsPaged(ctx, created.QueryID, 0, 0); !errors.Is(err, channels.ErrDynamicQueryNotFound) {
		t.Fatalf("ListDynamicGeneratedChannelsPaged(deleted) error = %v, want ErrDynamicQueryNotFound", err)
	}

	for _, row := range generatedRows {
		if got := countDeleteCleanupRowsForTest(t, ctx, store, `SELECT COUNT(*) FROM channel_sources WHERE channel_id = ?`, row.ChannelID); got != 0 {
			t.Fatalf("channel_sources rows for deleted generated channel %d = %d, want 0", row.ChannelID, got)
		}
		if got := countDeleteCleanupRowsForTest(t, ctx, store, `SELECT COUNT(*) FROM published_channel_dvr_map WHERE channel_id = ?`, row.ChannelID); got != 0 {
			t.Fatalf("published_channel_dvr_map rows for deleted generated channel %d = %d, want 0", row.ChannelID, got)
		}
	}
	if got := countDeleteCleanupRowsForTest(t, ctx, store, `SELECT COUNT(*) FROM channel_sources WHERE channel_id = ?`, orphanChannelID); got != 0 {
		t.Fatalf("orphan channel_sources rows after dynamic query delete = %d, want 0", got)
	}
	if got := countDeleteCleanupRowsForTest(t, ctx, store, `SELECT COUNT(*) FROM published_channel_dvr_map WHERE channel_id = ?`, orphanChannelID); got != 0 {
		t.Fatalf("orphan published_channel_dvr_map rows after dynamic query delete = %d, want 0", got)
	}
}

func insertDeleteCleanupDVRInstanceForTest(t testing.TB, ctx context.Context, store *Store) int64 {
	t.Helper()

	var existingID int64
	if err := store.db.QueryRowContext(
		ctx,
		`SELECT id FROM dvr_instances ORDER BY id ASC LIMIT 1`,
	).Scan(&existingID); err == nil {
		return existingID
	} else if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("lookup existing dvr_instances row error = %v", err)
	}

	now := time.Now().UTC().Unix()
	result, err := store.db.ExecContext(
		ctx,
		`INSERT INTO dvr_instances (
			provider,
			updated_at
		)
		VALUES (?, ?)`,
		"channels",
		now,
	)
	if err != nil {
		t.Fatalf("insert dvr_instances row error = %v", err)
	}
	instanceID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId(dvr_instances) error = %v", err)
	}
	return instanceID
}

func countDeleteCleanupRowsForTest(t testing.TB, ctx context.Context, store *Store, query string, args ...any) int {
	t.Helper()

	var count int
	if err := store.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		t.Fatalf("count query %q error = %v", query, err)
	}
	return count
}
