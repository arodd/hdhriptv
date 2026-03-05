package sqlite

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/playlist"
)

func TestSyncDynamicChannelBlocksMaterializesAndRetainsExistingOrder(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "News Block", "News", "news", 0, now)

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:one",
			ChannelKey: "name:news one",
			Name:       "News One",
			Group:      "News",
			StreamURL:  "http://example.com/news-one.ts",
		},
		{
			ItemKey:    "src:news:two",
			ChannelKey: "name:news two",
			Name:       "News Two",
			Group:      "News",
			StreamURL:  "http://example.com/news-two.ts",
		},
		{
			ItemKey:    "src:news:three",
			ChannelKey: "name:news three",
			Name:       "News Three",
			Group:      "News",
			StreamURL:  "http://example.com/news-three.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}

	firstSync, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(first) error = %v", err)
	}
	if firstSync.QueriesProcessed != 1 || firstSync.QueriesEnabled != 1 {
		t.Fatalf("first sync query stats = %+v, want processed=1 enabled=1", firstSync)
	}
	if firstSync.ChannelsAdded != 3 || firstSync.ChannelsRemoved != 0 || firstSync.TruncatedCount != 0 {
		t.Fatalf("first sync channel stats = %+v, want added=3 removed=0 truncated=0", firstSync)
	}

	dynamicRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(first) error = %v", err)
	}
	if len(dynamicRows) != 3 {
		t.Fatalf("len(dynamicRows first) = %d, want 3", len(dynamicRows))
	}
	assertGuideRangeForTest(t, dynamicRows, 10000)
	for _, row := range dynamicRows {
		sources, err := store.ListSources(ctx, row.ChannelID, false)
		if err != nil {
			t.Fatalf("ListSources(dynamic channel=%d) error = %v", row.ChannelID, err)
		}
		if len(sources) != 1 {
			t.Fatalf("len(ListSources dynamic channel=%d) = %d, want 1", row.ChannelID, len(sources))
		}
		if sources[0].AssociationType != "dynamic_channel_item" {
			t.Fatalf("dynamic channel=%d association_type = %q, want dynamic_channel_item", row.ChannelID, sources[0].AssociationType)
		}
		if sources[0].PriorityIndex != 0 {
			t.Fatalf("dynamic channel=%d priority_index = %d, want 0", row.ChannelID, sources[0].PriorityIndex)
		}
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:zero",
			ChannelKey: "name:news zero",
			Name:       "News Zero",
			Group:      "News",
			StreamURL:  "http://example.com/news-zero.ts",
		},
		{
			ItemKey:    "src:news:one",
			ChannelKey: "name:news one",
			Name:       "News One",
			Group:      "News",
			StreamURL:  "http://example.com/news-one.ts",
		},
		{
			ItemKey:    "src:news:two",
			ChannelKey: "name:news two",
			Name:       "News Two",
			Group:      "News",
			StreamURL:  "http://example.com/news-two.ts",
		},
		{
			ItemKey:    "src:news:three",
			ChannelKey: "name:news three",
			Name:       "News Three",
			Group:      "News",
			StreamURL:  "http://example.com/news-three.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(second) error = %v", err)
	}

	secondSync, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(second) error = %v", err)
	}
	if secondSync.ChannelsAdded != 1 || secondSync.ChannelsRetained != 3 {
		t.Fatalf("second sync stats = %+v, want added=1 retained=3", secondSync)
	}

	secondRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(second) error = %v", err)
	}
	if len(secondRows) != 4 {
		t.Fatalf("len(dynamicRows second) = %d, want 4", len(secondRows))
	}
	if secondRows[0].ItemKey != "src:news:one" || secondRows[1].ItemKey != "src:news:three" || secondRows[2].ItemKey != "src:news:two" || secondRows[3].ItemKey != "src:news:zero" {
		t.Fatalf("second generated item ordering = [%s %s %s %s], want existing retained order + appended new item", secondRows[0].ItemKey, secondRows[1].ItemKey, secondRows[2].ItemKey, secondRows[3].ItemKey)
	}
	assertGuideRangeForTest(t, secondRows, 10000)
}

func TestSyncDynamicChannelBlocksNoOpSyncAvoidsRowRewrites(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "News Block", "News", "news", 0, now)

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:noop:one",
			ChannelKey: "name:news noop one",
			Name:       "News Noop One",
			Group:      "News",
			StreamURL:  "http://example.com/news-noop-one.ts",
		},
		{
			ItemKey:    "src:news:noop:two",
			ChannelKey: "name:news noop two",
			Name:       "News Noop Two",
			Group:      "News",
			StreamURL:  "http://example.com/news-noop-two.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	firstSync, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(first) error = %v", err)
	}
	if firstSync.ChannelsAdded != 2 || firstSync.ChannelsUpdated != 0 || firstSync.ChannelsRetained != 0 || firstSync.ChannelsRemoved != 0 || firstSync.TruncatedCount != 0 {
		t.Fatalf("first sync stats = %+v, want added=2 updated=0 retained=0 removed=0 truncated=0", firstSync)
	}

	if err := installDynamicSyncUpdateAuditTriggersForTest(ctx, store); err != nil {
		t.Fatalf("installDynamicSyncUpdateAuditTriggersForTest() error = %v", err)
	}

	secondSync, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(second) error = %v", err)
	}
	if secondSync.ChannelsAdded != 0 || secondSync.ChannelsUpdated != 0 || secondSync.ChannelsRetained != 2 || secondSync.ChannelsRemoved != 0 || secondSync.TruncatedCount != 0 {
		t.Fatalf("second sync stats = %+v, want added=0 updated=0 retained=2 removed=0 truncated=0", secondSync)
	}

	queryRows, err := store.ListDynamicChannelQueries(ctx)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueries() error = %v", err)
	}
	if len(queryRows) != 1 {
		t.Fatalf("len(queryRows) = %d, want 1", len(queryRows))
	}
	if queryRows[0].QueryID != queryID {
		t.Fatalf("queryRows[0].QueryID = %d, want %d", queryRows[0].QueryID, queryID)
	}
	if queryRows[0].LastCount != 2 || queryRows[0].TruncatedBy != 0 {
		t.Fatalf("cached counters after no-op sync = (%d,%d), want (2,0)", queryRows[0].LastCount, queryRows[0].TruncatedBy)
	}

	channelSourceUpdates := loadDynamicSyncUpdateAuditCountForTest(t, ctx, store, "channel_sources")
	dynamicQueryUpdates := loadDynamicSyncUpdateAuditCountForTest(t, ctx, store, "dynamic_channel_queries")
	t.Logf("noop_second_sync_updates: channel_sources=%d dynamic_channel_queries=%d", channelSourceUpdates, dynamicQueryUpdates)
	if channelSourceUpdates != 0 {
		t.Fatalf("no-op second sync channel_sources updates = %d, want 0", channelSourceUpdates)
	}
	if dynamicQueryUpdates != 0 {
		t.Fatalf("no-op second sync dynamic_channel_queries updates = %d, want 0", dynamicQueryUpdates)
	}
}

func TestSyncDynamicChannelBlocksMiddleRemovalRetainsSurvivorGuideNumbers(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "News Block", "News", "news", 0, now)

	initialItems := []playlist.Item{
		{
			ItemKey:    "src:news:alpha",
			ChannelKey: "name:news alpha",
			Name:       "News Alpha",
			Group:      "News",
			StreamURL:  "http://example.com/news-alpha.ts",
		},
		{
			ItemKey:    "src:news:bravo",
			ChannelKey: "name:news bravo",
			Name:       "News Bravo",
			Group:      "News",
			StreamURL:  "http://example.com/news-bravo.ts",
		},
		{
			ItemKey:    "src:news:charlie",
			ChannelKey: "name:news charlie",
			Name:       "News Charlie",
			Group:      "News",
			StreamURL:  "http://example.com/news-charlie.ts",
		},
	}
	if err := store.UpsertPlaylistItems(ctx, initialItems); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(initial) error = %v", err)
	}

	firstRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(initial) error = %v", err)
	}
	if len(firstRows) != 3 {
		t.Fatalf("len(firstRows) = %d, want 3", len(firstRows))
	}
	firstGuides := generatedGuideByItemForTest(firstRows)
	if firstGuides["src:news:alpha"] != "10000" || firstGuides["src:news:bravo"] != "10001" || firstGuides["src:news:charlie"] != "10002" {
		t.Fatalf("unexpected initial guides = %#v", firstGuides)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{initialItems[0], initialItems[2]}); err != nil {
		t.Fatalf("UpsertPlaylistItems(after removal) error = %v", err)
	}
	secondSync, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(after removal) error = %v", err)
	}
	if secondSync.ChannelsRemoved != 1 {
		t.Fatalf("secondSync.ChannelsRemoved = %d, want 1", secondSync.ChannelsRemoved)
	}

	secondRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(after removal) error = %v", err)
	}
	if len(secondRows) != 2 {
		t.Fatalf("len(secondRows) = %d, want 2", len(secondRows))
	}
	secondGuides := generatedGuideByItemForTest(secondRows)
	if secondGuides["src:news:alpha"] != firstGuides["src:news:alpha"] {
		t.Fatalf("alpha guide changed from %q to %q", firstGuides["src:news:alpha"], secondGuides["src:news:alpha"])
	}
	if secondGuides["src:news:charlie"] != firstGuides["src:news:charlie"] {
		t.Fatalf("charlie guide changed from %q to %q", firstGuides["src:news:charlie"], secondGuides["src:news:charlie"])
	}
	if _, exists := secondGuides["src:news:bravo"]; exists {
		t.Fatalf("removed item still present with guide %q", secondGuides["src:news:bravo"])
	}
}

func TestSyncDynamicChannelBlocksCursorWrapAllocatesIntoNextFreeSlots(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "Wrap Block", "News", "news", 0, now)

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:wrap:base",
			ChannelKey: "name:wrap base",
			Name:       "News Alpha Base",
			Group:      "News",
			StreamURL:  "http://example.com/wrap-base.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(initial) error = %v", err)
	}

	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE dynamic_channel_queries
		 SET next_slot_cursor = 998
		 WHERE query_id = ?`,
		queryID,
	); err != nil {
		t.Fatalf("set next_slot_cursor test seed error = %v", err)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:wrap:base",
			ChannelKey: "name:wrap base",
			Name:       "News Alpha Base",
			Group:      "News",
			StreamURL:  "http://example.com/wrap-base.ts",
		},
		{
			ItemKey:    "src:wrap:bravo",
			ChannelKey: "name:wrap bravo",
			Name:       "News Bravo",
			Group:      "News",
			StreamURL:  "http://example.com/wrap-bravo.ts",
		},
		{
			ItemKey:    "src:wrap:charlie",
			ChannelKey: "name:wrap charlie",
			Name:       "News Charlie",
			Group:      "News",
			StreamURL:  "http://example.com/wrap-charlie.ts",
		},
		{
			ItemKey:    "src:wrap:delta",
			ChannelKey: "name:wrap delta",
			Name:       "News Delta",
			Group:      "News",
			StreamURL:  "http://example.com/wrap-delta.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(second) error = %v", err)
	}
	secondSync, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(second) error = %v", err)
	}
	if secondSync.ChannelsAdded != 3 {
		t.Fatalf("secondSync.ChannelsAdded = %d, want 3", secondSync.ChannelsAdded)
	}

	rows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(second) error = %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len(rows) = %d, want 4", len(rows))
	}
	guides := generatedGuideByItemForTest(rows)
	if guides["src:wrap:base"] != "10000" {
		t.Fatalf("base guide = %q, want 10000", guides["src:wrap:base"])
	}
	if guides["src:wrap:bravo"] != "10998" {
		t.Fatalf("bravo guide = %q, want 10998", guides["src:wrap:bravo"])
	}
	if guides["src:wrap:charlie"] != "10999" {
		t.Fatalf("charlie guide = %q, want 10999", guides["src:wrap:charlie"])
	}
	if guides["src:wrap:delta"] != "10001" {
		t.Fatalf("delta guide = %q, want 10001", guides["src:wrap:delta"])
	}

	queryRows, err := store.ListDynamicChannelQueries(ctx)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueries() error = %v", err)
	}
	if len(queryRows) != 1 {
		t.Fatalf("len(queryRows) = %d, want 1", len(queryRows))
	}
	if queryRows[0].NextSlotCursor != 2 {
		t.Fatalf("NextSlotCursor after wrap allocations = %d, want 2", queryRows[0].NextSlotCursor)
	}
}

func TestSyncDynamicChannelBlocksNameChurnRetainsGuideBySourceIdentity(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "Identity Block", "News", "alpha", 0, now)

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:identity:alpha:v1",
			ChannelKey: "name:identity alpha",
			Name:       "Alpha Feed",
			Group:      "News",
			StreamURL:  "http://example.com/live/alpha.ts?token=v1",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(initial) error = %v", err)
	}

	firstRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(initial) error = %v", err)
	}
	if len(firstRows) != 1 {
		t.Fatalf("len(firstRows) = %d, want 1", len(firstRows))
	}
	initial := firstRows[0]
	if initial.SourceIdentity == "" {
		t.Fatal("initial.SourceIdentity = empty, want non-empty identity")
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:identity:alpha:v2",
			ChannelKey: "name:identity alpha",
			Name:       "Alpha Feed Renamed",
			Group:      "News",
			StreamURL:  "http://example.com/live/alpha.ts?token=v2",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(name churn) error = %v", err)
	}

	secondSync, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(name churn) error = %v", err)
	}
	if secondSync.ChannelsAdded != 0 || secondSync.ChannelsRemoved != 0 {
		t.Fatalf("secondSync add/remove = %d/%d, want 0/0", secondSync.ChannelsAdded, secondSync.ChannelsRemoved)
	}
	if secondSync.ChannelsUpdated != 1 {
		t.Fatalf("secondSync.ChannelsUpdated = %d, want 1", secondSync.ChannelsUpdated)
	}

	secondRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(name churn) error = %v", err)
	}
	if len(secondRows) != 1 {
		t.Fatalf("len(secondRows) = %d, want 1", len(secondRows))
	}
	updated := secondRows[0]
	if updated.ChannelID != initial.ChannelID {
		t.Fatalf("channel_id changed from %d to %d", initial.ChannelID, updated.ChannelID)
	}
	if updated.GuideNumber != initial.GuideNumber {
		t.Fatalf("guide changed from %q to %q", initial.GuideNumber, updated.GuideNumber)
	}
	if updated.ItemKey != "src:identity:alpha:v2" {
		t.Fatalf("updated.ItemKey = %q, want src:identity:alpha:v2", updated.ItemKey)
	}
	if updated.SourceIdentity != initial.SourceIdentity {
		t.Fatalf("identity changed from %q to %q, want stable identity", initial.SourceIdentity, updated.SourceIdentity)
	}

	sources, err := store.ListSources(ctx, updated.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources(updated channel) error = %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("len(sources) = %d, want 1", len(sources))
	}
	if sources[0].ItemKey != "src:identity:alpha:v2" {
		t.Fatalf("sources[0].ItemKey = %q, want src:identity:alpha:v2", sources[0].ItemKey)
	}
}

func TestRefreshDynamicGeneratedGuideNamesResyncsFromCatalog(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "Refresh Names Block", "News", "alpha", 0, now)

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:refresh:alpha",
			ChannelKey: "name:refresh alpha",
			Name:       "Alpha Original",
			Group:      "News",
			StreamURL:  "http://example.com/refresh-alpha.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(initial) error = %v", err)
	}

	rows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(initial) error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}
	channelID := rows[0].ChannelID

	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE published_channels
		 SET guide_name = ?, updated_at = ?
		 WHERE channel_id = ?`,
		"Stale Name",
		time.Now().UTC().Unix(),
		channelID,
	); err != nil {
		t.Fatalf("seed stale guide_name error = %v", err)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:refresh:alpha",
			ChannelKey: "name:refresh alpha",
			Name:       "Alpha Renamed",
			Group:      "News",
			StreamURL:  "http://example.com/refresh-alpha.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(renamed) error = %v", err)
	}

	updatedCount, err := store.RefreshDynamicGeneratedGuideNames(ctx)
	if err != nil {
		t.Fatalf("RefreshDynamicGeneratedGuideNames() error = %v", err)
	}
	if updatedCount != 1 {
		t.Fatalf("updatedCount = %d, want 1", updatedCount)
	}

	rows, err = loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(after refresh) error = %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows after refresh) = %d, want 1", len(rows))
	}
	if rows[0].GuideName != "Alpha Renamed" {
		t.Fatalf("rows[0].GuideName = %q, want Alpha Renamed", rows[0].GuideName)
	}

	secondCount, err := store.RefreshDynamicGeneratedGuideNames(ctx)
	if err != nil {
		t.Fatalf("RefreshDynamicGeneratedGuideNames(second) error = %v", err)
	}
	if secondCount != 0 {
		t.Fatalf("secondCount = %d, want 0", secondCount)
	}
}

func TestRefreshDynamicGeneratedGuideNamesEdgeCases(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	alphaQueryID := insertDynamicQueryForTest(t, store, true, "Refresh Edge Alpha", "News", "alpha", 0, now)
	betaQueryID := insertDynamicQueryForTest(t, store, true, "Refresh Edge Beta", "Sports", "beta", 1, now)

	alphaItemKey := "src:refresh-edge:alpha"
	betaItemKey := "src:refresh-edge:beta"
	alphaChannelKey := "name:refresh edge alpha"
	betaChannelKey := "name:refresh edge beta"

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    alphaItemKey,
			ChannelKey: alphaChannelKey,
			Name:       "Alpha Original",
			Group:      "News",
			StreamURL:  "http://example.com/refresh-edge-alpha.ts",
		},
		{
			ItemKey:    betaItemKey,
			ChannelKey: betaChannelKey,
			Name:       "Beta Original",
			Group:      "Sports",
			StreamURL:  "http://example.com/refresh-edge-beta.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(initial) error = %v", err)
	}

	alphaRows, err := loadGeneratedChannelsForTest(ctx, store, alphaQueryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(alpha initial) error = %v", err)
	}
	if len(alphaRows) != 1 {
		t.Fatalf("len(alphaRows initial) = %d, want 1", len(alphaRows))
	}
	betaRows, err := loadGeneratedChannelsForTest(ctx, store, betaQueryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(beta initial) error = %v", err)
	}
	if len(betaRows) != 1 {
		t.Fatalf("len(betaRows initial) = %d, want 1", len(betaRows))
	}

	alphaChannelID := alphaRows[0].ChannelID
	betaChannelID := betaRows[0].ChannelID
	staleAt := time.Now().UTC().Unix()
	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE published_channels
		 SET guide_name = ?, updated_at = ?
		 WHERE channel_id = ?`,
		"Stale Alpha",
		staleAt,
		alphaChannelID,
	); err != nil {
		t.Fatalf("seed alpha stale guide_name error = %v", err)
	}
	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE published_channels
		 SET guide_name = ?, updated_at = ?
		 WHERE channel_id = ?`,
		"Stale Beta",
		staleAt,
		betaChannelID,
	); err != nil {
		t.Fatalf("seed beta stale guide_name error = %v", err)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    alphaItemKey,
			ChannelKey: alphaChannelKey,
			Name:       "   ",
			Group:      "News",
			StreamURL:  "http://example.com/refresh-edge-alpha.ts",
		},
		{
			ItemKey:    betaItemKey,
			ChannelKey: betaChannelKey,
			Name:       "Beta Original",
			Group:      "Sports",
			StreamURL:  "http://example.com/refresh-edge-beta.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(edge names) error = %v", err)
	}
	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE playlist_items
		 SET active = 0
		 WHERE item_key = ?`,
		betaItemKey,
	); err != nil {
		t.Fatalf("mark beta item inactive error = %v", err)
	}

	firstCount, err := store.RefreshDynamicGeneratedGuideNames(ctx)
	if err != nil {
		t.Fatalf("RefreshDynamicGeneratedGuideNames(first) error = %v", err)
	}
	if firstCount != 1 {
		t.Fatalf("firstCount = %d, want 1 (alpha fallback only)", firstCount)
	}

	alphaRows, err = loadGeneratedChannelsForTest(ctx, store, alphaQueryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(alpha after first refresh) error = %v", err)
	}
	if alphaRows[0].GuideName != alphaItemKey {
		t.Fatalf("alpha guide_name = %q, want item_key fallback %q", alphaRows[0].GuideName, alphaItemKey)
	}
	betaRows, err = loadGeneratedChannelsForTest(ctx, store, betaQueryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(beta after first refresh) error = %v", err)
	}
	if betaRows[0].GuideName != "Stale Beta" {
		t.Fatalf("beta guide_name = %q, want unchanged stale value while inactive", betaRows[0].GuideName)
	}

	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE playlist_items
		 SET active = 1, name = ?, updated_at = ?
		 WHERE item_key = ?`,
		"Beta Renamed",
		time.Now().UTC().Unix(),
		betaItemKey,
	); err != nil {
		t.Fatalf("reactivate beta item error = %v", err)
	}

	secondCount, err := store.RefreshDynamicGeneratedGuideNames(ctx)
	if err != nil {
		t.Fatalf("RefreshDynamicGeneratedGuideNames(second) error = %v", err)
	}
	if secondCount != 1 {
		t.Fatalf("secondCount = %d, want 1 (beta query isolated update)", secondCount)
	}

	alphaRows, err = loadGeneratedChannelsForTest(ctx, store, alphaQueryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(alpha after second refresh) error = %v", err)
	}
	if alphaRows[0].GuideName != alphaItemKey {
		t.Fatalf("alpha guide_name after beta update = %q, want %q", alphaRows[0].GuideName, alphaItemKey)
	}
	betaRows, err = loadGeneratedChannelsForTest(ctx, store, betaQueryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(beta after second refresh) error = %v", err)
	}
	if betaRows[0].GuideName != "Beta Renamed" {
		t.Fatalf("beta guide_name after reactivation = %q, want Beta Renamed", betaRows[0].GuideName)
	}
}

func TestSyncDynamicChannelBlocksDuplicateURLIdentityFallsBackToItemKey(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "Duplicate URL Block", "News", "dup", 0, now)
	sharedURL := "http://example.com/live/dup.ts?token=one"

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:dup:url:a",
			ChannelKey: "name:dup a",
			Name:       "Dup Feed A",
			Group:      "News",
			StreamURL:  sharedURL,
		},
		{
			ItemKey:    "src:dup:url:b",
			ChannelKey: "name:dup b",
			Name:       "Dup Feed B",
			Group:      "News",
			StreamURL:  sharedURL,
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(initial) error = %v", err)
	}

	firstRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(initial) error = %v", err)
	}
	if len(firstRows) != 2 {
		t.Fatalf("len(firstRows) = %d, want 2", len(firstRows))
	}
	firstGuides := generatedGuideByItemForTest(firstRows)
	guideB := firstGuides["src:dup:url:b"]
	if guideB == "" {
		t.Fatal("missing guide for src:dup:url:b in initial sync")
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:dup:url:a:v2",
			ChannelKey: "name:dup a",
			Name:       "Dup Feed A Renamed",
			Group:      "News",
			StreamURL:  "http://example.com/live/dup.ts?token=two",
		},
		{
			ItemKey:    "src:dup:url:b",
			ChannelKey: "name:dup b",
			Name:       "Dup Feed B",
			Group:      "News",
			StreamURL:  sharedURL,
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(duplicate url churn) error = %v", err)
	}

	secondSync, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(duplicate url churn) error = %v", err)
	}
	if secondSync.ChannelsAdded != 1 || secondSync.ChannelsRemoved != 1 {
		t.Fatalf("secondSync add/remove = %d/%d, want 1/1", secondSync.ChannelsAdded, secondSync.ChannelsRemoved)
	}

	secondRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(duplicate url churn) error = %v", err)
	}
	if len(secondRows) != 2 {
		t.Fatalf("len(secondRows) = %d, want 2", len(secondRows))
	}
	secondGuides := generatedGuideByItemForTest(secondRows)
	if _, exists := secondGuides["src:dup:url:a"]; exists {
		t.Fatalf("stale item src:dup:url:a should have been removed, guides=%#v", secondGuides)
	}
	if secondGuides["src:dup:url:b"] != guideB {
		t.Fatalf("guide for src:dup:url:b changed from %q to %q", guideB, secondGuides["src:dup:url:b"])
	}
	if _, exists := secondGuides["src:dup:url:a:v2"]; !exists {
		t.Fatalf("expected replacement item src:dup:url:a:v2, got guides=%#v", secondGuides)
	}
}

func TestDynamicSourceIdentityUniqueIndexRejectsDuplicateQueryIdentity(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "Identity Index Block", "News", "", 0, now)

	if _, err := store.db.ExecContext(
		ctx,
		`INSERT INTO published_channels (
			channel_class,
			channel_key,
			guide_number,
			guide_name,
			order_index,
			enabled,
			dynamic_query_id,
			dynamic_item_key,
			dynamic_source_identity,
			created_at,
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
		channels.ChannelClassDynamicGenerated,
		"name:identity one",
		"10000",
		"Identity One",
		10000,
		queryID,
		"src:index:one",
		"tvg:identity-index",
		now,
		now,
	); err != nil {
		t.Fatalf("insert first generated row error = %v", err)
	}

	_, err = store.db.ExecContext(
		ctx,
		`INSERT INTO published_channels (
			channel_class,
			channel_key,
			guide_number,
			guide_name,
			order_index,
			enabled,
			dynamic_query_id,
			dynamic_item_key,
			dynamic_source_identity,
			created_at,
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
		channels.ChannelClassDynamicGenerated,
		"name:identity two",
		"10001",
		"Identity Two",
		10001,
		queryID,
		"src:index:two",
		"tvg:identity-index",
		now,
		now,
	)
	if err == nil {
		t.Fatal("insert duplicate identity row error = nil, want unique-constraint failure")
	}
	lowerErr := strings.ToLower(err.Error())
	if !strings.Contains(lowerErr, "unique") || !strings.Contains(lowerErr, "dynamic_source_identity") {
		t.Fatalf("duplicate identity insert error = %v, want unique constraint on dynamic_source_identity", err)
	}
}

func TestSyncDynamicChannelBlocksAllowsEmptySearchQuery(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "News Block", "News", "", 0, now)

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:alpha",
			ChannelKey: "name:news alpha",
			Name:       "News Alpha",
			Group:      "News",
			StreamURL:  "http://example.com/news-alpha.ts",
		},
		{
			ItemKey:    "src:news:beta",
			ChannelKey: "name:news beta",
			Name:       "News Beta",
			Group:      "News",
			StreamURL:  "http://example.com/news-beta.ts",
		},
		{
			ItemKey:    "src:sports:gamma",
			ChannelKey: "name:sports gamma",
			Name:       "Sports Gamma",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-gamma.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	syncResult, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}
	if syncResult.QueriesProcessed != 1 || syncResult.QueriesEnabled != 1 {
		t.Fatalf("sync query stats = %+v, want processed=1 enabled=1", syncResult)
	}
	if syncResult.ChannelsAdded != 2 || syncResult.ChannelsRemoved != 0 || syncResult.TruncatedCount != 0 {
		t.Fatalf("sync channel stats = %+v, want added=2 removed=0 truncated=0", syncResult)
	}

	dynamicRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest() error = %v", err)
	}
	if len(dynamicRows) != 2 {
		t.Fatalf("len(dynamicRows) = %d, want 2", len(dynamicRows))
	}
	if dynamicRows[0].ItemKey != "src:news:alpha" || dynamicRows[1].ItemKey != "src:news:beta" {
		t.Fatalf("generated item keys = [%s %s], want [src:news:alpha src:news:beta]", dynamicRows[0].ItemKey, dynamicRows[1].ItemKey)
	}
	assertGuideRangeForTest(t, dynamicRows, 10000)
}

func TestSyncDynamicChannelBlocksSupportsMultiGroupFilters(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	query, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Mixed Prime",
		GroupNames:  []string{"Sports", "News", "sports"},
		SearchQuery: "prime",
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery() error = %v", err)
	}
	if query.GroupName != "News" {
		t.Fatalf("query.GroupName = %q, want News alias", query.GroupName)
	}
	if len(query.GroupNames) != 2 || query.GroupNames[0] != "News" || query.GroupNames[1] != "Sports" {
		t.Fatalf("query.GroupNames = %#v, want [News Sports]", query.GroupNames)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:prime:news",
			ChannelKey: "name:prime news",
			Name:       "Prime News Alpha",
			Group:      "News",
			StreamURL:  "http://example.com/prime-news-alpha.ts",
		},
		{
			ItemKey:    "src:prime:sports",
			ChannelKey: "name:prime sports",
			Name:       "Prime Sports Beta",
			Group:      "Sports",
			StreamURL:  "http://example.com/prime-sports-beta.ts",
		},
		{
			ItemKey:    "src:prime:movies",
			ChannelKey: "name:prime movies",
			Name:       "Prime Movie Gamma",
			Group:      "Movies",
			StreamURL:  "http://example.com/prime-movie-gamma.ts",
		},
		{
			ItemKey:    "src:secondary:news",
			ChannelKey: "name:secondary news",
			Name:       "Secondary News Delta",
			Group:      "News",
			StreamURL:  "http://example.com/secondary-news-delta.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	syncResult, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}
	if syncResult.ChannelsAdded != 2 {
		t.Fatalf("syncResult.ChannelsAdded = %d, want 2", syncResult.ChannelsAdded)
	}

	dynamicRows, err := loadGeneratedChannelsForTest(ctx, store, query.QueryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest() error = %v", err)
	}
	if len(dynamicRows) != 2 {
		t.Fatalf("len(dynamicRows) = %d, want 2", len(dynamicRows))
	}
	if dynamicRows[0].ItemKey != "src:prime:news" || dynamicRows[1].ItemKey != "src:prime:sports" {
		t.Fatalf("generated item keys = [%s %s], want [src:prime:news src:prime:sports]", dynamicRows[0].ItemKey, dynamicRows[1].ItemKey)
	}

	queries, err := store.ListDynamicChannelQueries(ctx)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueries() error = %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("len(queries) = %d, want 1", len(queries))
	}
	if len(queries[0].GroupNames) != 2 || queries[0].GroupNames[0] != "News" || queries[0].GroupNames[1] != "Sports" {
		t.Fatalf("queries[0].GroupNames = %#v, want [News Sports]", queries[0].GroupNames)
	}
}

func TestSyncDynamicChannelBlocksSupportsSourceIDFilters(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Secondary Dynamic Source",
		PlaylistURL: "http://example.com/secondary-dynamic.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}

	filtered, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Prime Secondary",
		GroupName:   "News",
		SourceIDs:   []int64{secondary.SourceID, secondary.SourceID, -1, 0},
		SearchQuery: "prime shared",
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery(filtered) error = %v", err)
	}
	if len(filtered.SourceIDs) != 1 || filtered.SourceIDs[0] != secondary.SourceID {
		t.Fatalf("filtered.SourceIDs = %#v, want [%d]", filtered.SourceIDs, secondary.SourceID)
	}

	allSources, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Prime All Sources",
		GroupName:   "News",
		SearchQuery: "prime shared",
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery(all sources) error = %v", err)
	}
	if len(allSources.SourceIDs) != 0 {
		t.Fatalf("allSources.SourceIDs = %#v, want empty", allSources.SourceIDs)
	}

	if err := store.UpsertPlaylistItemsForSource(ctx, primaryPlaylistSourceID, []playlist.Item{
		{
			ItemKey:    "src:source-filter:primary:prime",
			ChannelKey: "name:source-filter-primary-prime",
			Name:       "Prime Shared Alpha",
			Group:      "News",
			StreamURL:  "http://example.com/source-filter-primary-prime.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(primary) error = %v", err)
	}
	if err := store.UpsertPlaylistItemsForSource(ctx, secondary.SourceID, []playlist.Item{
		{
			ItemKey:    "src:source-filter:secondary:prime",
			ChannelKey: "name:source-filter-secondary-prime",
			Name:       "Prime Shared Zeta",
			Group:      "News",
			StreamURL:  "http://example.com/source-filter-secondary-prime.ts",
		},
		{
			ItemKey:    "src:source-filter:secondary:nonmatch",
			ChannelKey: "name:source-filter-secondary-nonmatch",
			Name:       "Secondary Other",
			Group:      "News",
			StreamURL:  "http://example.com/source-filter-secondary-other.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(secondary) error = %v", err)
	}

	syncResult, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}
	if syncResult.QueriesProcessed != 2 || syncResult.QueriesEnabled != 2 {
		t.Fatalf("syncResult query stats = %+v, want processed=2 enabled=2", syncResult)
	}

	filteredRows, err := loadGeneratedChannelsForTest(ctx, store, filtered.QueryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(filtered) error = %v", err)
	}
	if len(filteredRows) != 1 {
		t.Fatalf("len(filteredRows) = %d, want 1", len(filteredRows))
	}
	if filteredRows[0].ItemKey != "src:source-filter:secondary:prime" {
		t.Fatalf("filtered row item_key = %q, want secondary prime item", filteredRows[0].ItemKey)
	}

	allRows, err := loadGeneratedChannelsForTest(ctx, store, allSources.QueryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(all sources) error = %v", err)
	}
	if len(allRows) != 2 {
		t.Fatalf("len(allRows) = %d, want 2", len(allRows))
	}
	if allRows[0].ItemKey != "src:source-filter:primary:prime" || allRows[1].ItemKey != "src:source-filter:secondary:prime" {
		t.Fatalf("allRows item keys = [%s %s], want [src:source-filter:primary:prime src:source-filter:secondary:prime]", allRows[0].ItemKey, allRows[1].ItemKey)
	}
}

func TestSyncDynamicChannelBlocksSupportsOrSearchQuery(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "News OR Block", "News", "fox | nbc -spanish", 0, now)

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:or:fox",
			ChannelKey: "name:or fox",
			Name:       "FOX 9 KMSP",
			Group:      "News",
			StreamURL:  "http://example.com/or-fox.ts",
		},
		{
			ItemKey:    "src:or:nbc",
			ChannelKey: "name:or nbc",
			Name:       "NBC Chicago",
			Group:      "News",
			StreamURL:  "http://example.com/or-nbc.ts",
		},
		{
			ItemKey:    "src:or:nbc-spanish",
			ChannelKey: "name:or nbc spanish",
			Name:       "NBC Chicago Spanish",
			Group:      "News",
			StreamURL:  "http://example.com/or-nbc-spanish.ts",
		},
		{
			ItemKey:    "src:or:other",
			ChannelKey: "name:or other",
			Name:       "Other News",
			Group:      "News",
			StreamURL:  "http://example.com/or-other.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	syncResult, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}
	if syncResult.ChannelsAdded != 2 {
		t.Fatalf("syncResult.ChannelsAdded = %d, want 2", syncResult.ChannelsAdded)
	}
	if syncResult.ChannelsRemoved != 0 {
		t.Fatalf("syncResult.ChannelsRemoved = %d, want 0", syncResult.ChannelsRemoved)
	}

	dynamicRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest() error = %v", err)
	}
	if len(dynamicRows) != 2 {
		t.Fatalf("len(dynamicRows) = %d, want 2", len(dynamicRows))
	}
	if dynamicRows[0].ItemKey != "src:or:fox" || dynamicRows[1].ItemKey != "src:or:nbc" {
		t.Fatalf("generated item keys = [%s %s], want [src:or:fox src:or:nbc]", dynamicRows[0].ItemKey, dynamicRows[1].ItemKey)
	}
}

func TestSyncDynamicChannelBlocksSupportsRegexSearchQuery(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	created, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Regex Block",
		GroupName:   "News",
		SearchQuery: `fox\s+\d+|nbc chicago$`,
		SearchRegex: true,
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery(regex) error = %v", err)
	}
	if !created.SearchRegex {
		t.Fatal("created.SearchRegex = false, want true")
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:regex:block:fox9",
			ChannelKey: "name:regex block fox9",
			Name:       "FOX 9 KMSP",
			Group:      "News",
			StreamURL:  "http://example.com/regex-block-fox9.ts",
		},
		{
			ItemKey:    "src:regex:block:fox32",
			ChannelKey: "name:regex block fox32",
			Name:       "FOX 32 WFLD",
			Group:      "News",
			StreamURL:  "http://example.com/regex-block-fox32.ts",
		},
		{
			ItemKey:    "src:regex:block:nbc",
			ChannelKey: "name:regex block nbc",
			Name:       "NBC Chicago",
			Group:      "News",
			StreamURL:  "http://example.com/regex-block-nbc.ts",
		},
		{
			ItemKey:    "src:regex:block:nbc-spanish",
			ChannelKey: "name:regex block nbc spanish",
			Name:       "NBC Chicago Spanish",
			Group:      "News",
			StreamURL:  "http://example.com/regex-block-nbc-spanish.ts",
		},
		{
			ItemKey:    "src:regex:block:foxsports",
			ChannelKey: "name:regex block foxsports",
			Name:       "FOX Sports",
			Group:      "News",
			StreamURL:  "http://example.com/regex-block-foxsports.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	syncResult, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(regex) error = %v", err)
	}
	if syncResult.ChannelsAdded != 3 {
		t.Fatalf("syncResult.ChannelsAdded = %d, want 3", syncResult.ChannelsAdded)
	}

	dynamicRows, err := loadGeneratedChannelsForTest(ctx, store, created.QueryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest(regex) error = %v", err)
	}
	if len(dynamicRows) != 3 {
		t.Fatalf("len(dynamicRows) = %d, want 3", len(dynamicRows))
	}
	if dynamicRows[0].ItemKey != "src:regex:block:fox32" ||
		dynamicRows[1].ItemKey != "src:regex:block:fox9" ||
		dynamicRows[2].ItemKey != "src:regex:block:nbc" {
		t.Fatalf(
			"generated item keys = [%s %s %s], want [src:regex:block:fox32 src:regex:block:fox9 src:regex:block:nbc]",
			dynamicRows[0].ItemKey,
			dynamicRows[1].ItemKey,
			dynamicRows[2].ItemKey,
		)
	}
}

func TestSyncDynamicChannelBlocksEnforcesBlockCapAndTruncationCount(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "Movies Block", "Movies", "movie", 1, now)

	const totalMatches = 1005
	items := make([]playlist.Item, 0, totalMatches)
	for i := 0; i < totalMatches; i++ {
		items = append(items, playlist.Item{
			ItemKey:    "src:movie:" + strconv.Itoa(i),
			ChannelKey: "name:movie " + strconv.Itoa(i),
			Name:       "Movie " + strconv.Itoa(i),
			Group:      "Movies",
			StreamURL:  "http://example.com/movie-" + strconv.Itoa(i) + ".ts",
		})
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	syncResult, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}
	if syncResult.TruncatedCount != 5 {
		t.Fatalf("syncResult.TruncatedCount = %d, want 5", syncResult.TruncatedCount)
	}
	if syncResult.ChannelsAdded != channels.DynamicGuideBlockMaxLen {
		t.Fatalf("syncResult.ChannelsAdded = %d, want %d", syncResult.ChannelsAdded, channels.DynamicGuideBlockMaxLen)
	}

	dynamicRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest() error = %v", err)
	}
	if len(dynamicRows) != channels.DynamicGuideBlockMaxLen {
		t.Fatalf("len(dynamicRows) = %d, want %d", len(dynamicRows), channels.DynamicGuideBlockMaxLen)
	}
	assertGuideRangeForTest(t, dynamicRows, 11000)
	if dynamicRows[len(dynamicRows)-1].GuideNumber != "11999" {
		t.Fatalf("last guide number = %q, want 11999", dynamicRows[len(dynamicRows)-1].GuideNumber)
	}
}

func TestSyncDynamicChannelBlocksBroadFilterEnforcesBlockCapAndTruncationCount(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	queryID := insertDynamicQueryForTest(t, store, true, "All Channels Block", "", "", 0, now)

	const totalMatches = channels.DynamicGuideBlockMaxLen + 240
	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(totalMatches)); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	syncResult, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}
	if syncResult.TruncatedCount != totalMatches-channels.DynamicGuideBlockMaxLen {
		t.Fatalf("syncResult.TruncatedCount = %d, want %d", syncResult.TruncatedCount, totalMatches-channels.DynamicGuideBlockMaxLen)
	}
	if syncResult.ChannelsAdded != channels.DynamicGuideBlockMaxLen {
		t.Fatalf("syncResult.ChannelsAdded = %d, want %d", syncResult.ChannelsAdded, channels.DynamicGuideBlockMaxLen)
	}

	queryRows, err := store.ListDynamicChannelQueries(ctx)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueries() error = %v", err)
	}
	if len(queryRows) != 1 {
		t.Fatalf("len(queryRows) = %d, want 1", len(queryRows))
	}
	if queryRows[0].QueryID != queryID {
		t.Fatalf("queryRows[0].QueryID = %d, want %d", queryRows[0].QueryID, queryID)
	}
	if queryRows[0].LastCount != channels.DynamicGuideBlockMaxLen {
		t.Fatalf("queryRows[0].LastCount = %d, want %d", queryRows[0].LastCount, channels.DynamicGuideBlockMaxLen)
	}
	if queryRows[0].TruncatedBy != totalMatches-channels.DynamicGuideBlockMaxLen {
		t.Fatalf("queryRows[0].TruncatedBy = %d, want %d", queryRows[0].TruncatedBy, totalMatches-channels.DynamicGuideBlockMaxLen)
	}
}

func TestDynamicQueryMatchPageQueryPlanAvoidsTempBTreeBroadAndGrouped(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(12000)); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	broadPlan, err := explainDynamicQueryMatchPagePlan(ctx, store.db, nil, "", channels.DynamicGuideBlockMaxLen+1, 0)
	if err != nil {
		t.Fatalf("explainDynamicQueryMatchPagePlan(broad) error = %v", err)
	}
	if len(broadPlan) == 0 {
		t.Fatal("expected broad query-plan rows")
	}
	t.Logf("dynamic match broad query plan: %s", strings.Join(broadPlan, " | "))
	if !planContains(broadPlan, "idx_playlist_active_name_item") {
		t.Fatalf("broad query plan missing ordered active/name index: %q", strings.Join(broadPlan, " | "))
	}
	if planContains(broadPlan, "use temp b-tree for order by") {
		t.Fatalf("broad query plan unexpectedly includes temp order-by sort: %q", strings.Join(broadPlan, " | "))
	}

	groupedPlan, err := explainDynamicQueryMatchPagePlan(ctx, store.db, []string{"Group 005"}, "channel", channels.DynamicGuideBlockMaxLen+1, 0)
	if err != nil {
		t.Fatalf("explainDynamicQueryMatchPagePlan(grouped) error = %v", err)
	}
	if len(groupedPlan) == 0 {
		t.Fatal("expected grouped query-plan rows")
	}
	t.Logf("dynamic match grouped query plan: %s", strings.Join(groupedPlan, " | "))
	if !planContains(groupedPlan, "idx_playlist_active_group_name_name_item") {
		t.Fatalf("grouped query plan missing ordered active/group/name index: %q", strings.Join(groupedPlan, " | "))
	}
	if planContains(groupedPlan, "use temp b-tree for order by") {
		t.Fatalf("grouped query plan unexpectedly includes temp order-by sort: %q", strings.Join(groupedPlan, " | "))
	}

	orGroupedPlan, err := explainDynamicQueryMatchPagePlan(
		ctx,
		store.db,
		[]string{"Group 005"},
		"channel 00005 | channel 00045 | channel 00085 | channel 00125 | channel 00165 | channel 00205",
		channels.DynamicGuideBlockMaxLen+1,
		0,
	)
	if err != nil {
		t.Fatalf("explainDynamicQueryMatchPagePlan(or grouped) error = %v", err)
	}
	if len(orGroupedPlan) == 0 {
		t.Fatal("expected OR grouped query-plan rows")
	}
	t.Logf("dynamic match OR grouped query plan: %s", strings.Join(orGroupedPlan, " | "))
	if !planContains(orGroupedPlan, "idx_playlist_active_group_name_name_item") {
		t.Fatalf("OR grouped query plan missing ordered active/group/name index: %q", strings.Join(orGroupedPlan, " | "))
	}
	if planContains(orGroupedPlan, "use temp b-tree for order by") {
		t.Fatalf("OR grouped query plan unexpectedly includes temp order-by sort: %q", strings.Join(orGroupedPlan, " | "))
	}
}

func BenchmarkSyncDynamicChannelBlocksBroadMatchHighCardinality(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	insertDynamicQueryForTest(b, store, true, "All Channels Block", "", "", 0, now)

	const totalMatches = 30000
	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(totalMatches)); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	initialResult, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		b.Fatalf("SyncDynamicChannelBlocks(initial) error = %v", err)
	}
	if initialResult.ChannelsAdded != channels.DynamicGuideBlockMaxLen {
		b.Fatalf("initial ChannelsAdded = %d, want %d", initialResult.ChannelsAdded, channels.DynamicGuideBlockMaxLen)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := store.SyncDynamicChannelBlocks(ctx)
		if err != nil {
			b.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
		}
		if result.QueriesProcessed != 1 || result.QueriesEnabled != 1 {
			b.Fatalf("sync query stats = %+v, want processed=1 enabled=1", result)
		}
		if result.TruncatedCount != totalMatches-channels.DynamicGuideBlockMaxLen {
			b.Fatalf("TruncatedCount = %d, want %d", result.TruncatedCount, totalMatches-channels.DynamicGuideBlockMaxLen)
		}
	}
}

func BenchmarkSyncDynamicChannelBlocksOrSearch(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Unix()
	insertDynamicQueryForTest(
		b,
		store,
		true,
		"OR Channels Block",
		"Group 005",
		"channel 00005 | channel 00045 | channel 00085 | channel 00125 | channel 00165 | channel 00205",
		0,
		now,
	)

	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(30000)); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	initialResult, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		b.Fatalf("SyncDynamicChannelBlocks(initial) error = %v", err)
	}
	if initialResult.ChannelsAdded == 0 {
		b.Fatalf("initial ChannelsAdded = %d, want > 0", initialResult.ChannelsAdded)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := store.SyncDynamicChannelBlocks(ctx)
		if err != nil {
			b.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
		}
		if result.QueriesProcessed != 1 || result.QueriesEnabled != 1 {
			b.Fatalf("sync query stats = %+v, want processed=1 enabled=1", result)
		}
	}
}

func BenchmarkSyncDynamicChannelBlocksRegexSearch(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if _, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Enabled:     ptrBool(true),
		Name:        "Regex Channels Block",
		GroupName:   "Group 005",
		SearchQuery: `channel\s+0000[5-9] | channel\s+001[2-3][0-9] -000125`,
		SearchRegex: true,
	}); err != nil {
		b.Fatalf("CreateDynamicChannelQuery(regex) error = %v", err)
	}

	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(30000)); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	initialResult, err := store.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		b.Fatalf("SyncDynamicChannelBlocks(initial regex) error = %v", err)
	}
	if initialResult.ChannelsAdded == 0 {
		b.Fatalf("initial ChannelsAdded = %d, want > 0", initialResult.ChannelsAdded)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := store.SyncDynamicChannelBlocks(ctx)
		if err != nil {
			b.Fatalf("SyncDynamicChannelBlocks(regex) error = %v", err)
		}
		if result.QueriesProcessed != 1 || result.QueriesEnabled != 1 {
			b.Fatalf("sync query stats = %+v, want processed=1 enabled=1", result)
		}
	}
}

func ptrBool(v bool) *bool {
	return &v
}

func TestListChannelsPagedExcludesGeneratedDynamicRows(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:trad:one",
			ChannelKey: "tvg:tradone",
			Name:       "Traditional One",
			Group:      "Linear",
			StreamURL:  "http://example.com/trad-one.ts",
		},
		{
			ItemKey:    "src:news:block",
			ChannelKey: "name:block news",
			Name:       "Block News",
			Group:      "News",
			StreamURL:  "http://example.com/block-news.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	traditional, err := store.CreateChannelFromItem(ctx, "src:trad:one", "", "", nil, channels.TraditionalGuideStart)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	if traditional.ChannelClass != channels.ChannelClassTraditional {
		t.Fatalf("traditional.ChannelClass = %q, want %q", traditional.ChannelClass, channels.ChannelClassTraditional)
	}

	now := time.Now().UTC().Unix()
	insertDynamicQueryForTest(t, store, true, "News Block", "News", "news", 0, now)
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}

	paged, total, err := store.ListChannelsPaged(ctx, false, 10, 0)
	if err != nil {
		t.Fatalf("ListChannelsPaged() error = %v", err)
	}
	if total != 1 || len(paged) != 1 {
		t.Fatalf("ListChannelsPaged returned total=%d len=%d, want total=1 len=1", total, len(paged))
	}
	if paged[0].ChannelID != traditional.ChannelID {
		t.Fatalf("paged channel_id = %d, want traditional channel_id %d", paged[0].ChannelID, traditional.ChannelID)
	}

	lineup, err := store.ListLineupChannels(ctx)
	if err != nil {
		t.Fatalf("ListLineupChannels() error = %v", err)
	}
	if len(lineup) != 2 {
		t.Fatalf("len(ListLineupChannels) = %d, want 2 (traditional + generated)", len(lineup))
	}
	if lineup[0].ChannelClass != channels.ChannelClassTraditional {
		t.Fatalf("lineup[0].ChannelClass = %q, want %q", lineup[0].ChannelClass, channels.ChannelClassTraditional)
	}
	if lineup[1].ChannelClass != channels.ChannelClassDynamicGenerated {
		t.Fatalf("lineup[1].ChannelClass = %q, want %q", lineup[1].ChannelClass, channels.ChannelClassDynamicGenerated)
	}
}

func insertDynamicQueryForTest(t testing.TB, store *Store, enabled bool, name, groupName, searchQuery string, orderIndex int, now int64) int64 {
	t.Helper()

	result, err := store.db.ExecContext(
		context.Background(),
		`INSERT INTO dynamic_channel_queries (
			enabled,
			name,
			group_name,
			search_query,
			order_index,
			created_at,
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		boolToInt(enabled),
		name,
		groupName,
		searchQuery,
		orderIndex,
		now,
		now,
	)
	if err != nil {
		t.Fatalf("insert dynamic channel query error = %v", err)
	}
	queryID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId(dynamic channel query) error = %v", err)
	}
	return queryID
}

func loadGeneratedChannelsForTest(ctx context.Context, store *Store, queryID int64) ([]generatedChannelRow, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := listGeneratedChannelsForQueryTx(ctx, tx, queryID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return rows, nil
}

func generatedGuideByItemForTest(rows []generatedChannelRow) map[string]string {
	out := make(map[string]string, len(rows))
	for _, row := range rows {
		out[row.ItemKey] = row.GuideNumber
	}
	return out
}

func assertGuideRangeForTest(t *testing.T, rows []generatedChannelRow, start int) {
	t.Helper()
	for idx, row := range rows {
		want := strconv.Itoa(start + idx)
		if row.GuideNumber != want {
			t.Fatalf("row[%d].GuideNumber = %q, want %q", idx, row.GuideNumber, want)
		}
	}
}

func installDynamicSyncUpdateAuditTriggersForTest(ctx context.Context, store *Store) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS dynamic_sync_update_audit (
			table_name   TEXT PRIMARY KEY,
			update_count INTEGER NOT NULL DEFAULT 0
		)`,
		`DELETE FROM dynamic_sync_update_audit`,
		`INSERT INTO dynamic_sync_update_audit (table_name, update_count)
		VALUES ('channel_sources', 0), ('dynamic_channel_queries', 0)`,
		`DROP TRIGGER IF EXISTS dynamic_sync_audit_channel_sources_update`,
		`DROP TRIGGER IF EXISTS dynamic_sync_audit_dynamic_queries_update`,
		`CREATE TRIGGER dynamic_sync_audit_channel_sources_update
		AFTER UPDATE ON channel_sources
		BEGIN
			UPDATE dynamic_sync_update_audit
			   SET update_count = update_count + 1
			 WHERE table_name = 'channel_sources';
		END`,
		`CREATE TRIGGER dynamic_sync_audit_dynamic_queries_update
		AFTER UPDATE ON dynamic_channel_queries
		BEGIN
			UPDATE dynamic_sync_update_audit
			   SET update_count = update_count + 1
			 WHERE table_name = 'dynamic_channel_queries';
		END`,
	}
	for _, statement := range statements {
		if _, err := store.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func loadDynamicSyncUpdateAuditCountForTest(t *testing.T, ctx context.Context, store *Store, tableName string) int {
	t.Helper()

	var count int
	if err := store.db.QueryRowContext(
		ctx,
		`SELECT update_count
		FROM dynamic_sync_update_audit
		WHERE table_name = ?`,
		tableName,
	).Scan(&count); err != nil {
		t.Fatalf("QueryRowContext(dynamic_sync_update_audit table=%s) error = %v", tableName, err)
	}
	return count
}

func explainDynamicQueryMatchPagePlan(ctx context.Context, db *sql.DB, groupNames []string, searchQuery string, limit, offset int) ([]string, error) {
	spec, err := buildCatalogFilterQuerySpec(groupNames, searchQuery, false)
	if err != nil {
		return nil, err
	}
	query := `EXPLAIN QUERY PLAN
		SELECT item_key, COALESCE(channel_key, ''), name
		FROM playlist_items INDEXED BY ` + spec.OrderIndex + `
		WHERE ` + spec.WhereClause + `
		ORDER BY name ASC, item_key ASC
		LIMIT ? OFFSET ?`

	args := make([]any, 0, len(spec.Args)+2)
	args = append(args, spec.Args...)
	args = append(args, limit, offset)

	rows, err := db.QueryContext(ctx, query, args...)
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
