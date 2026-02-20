package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/playlist"
)

func TestDynamicChannelQueryCRUDAndListGeneratedChannels(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	created, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "News Block",
		GroupName:   "News",
		SearchQuery: "news",
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery() error = %v", err)
	}
	if created.QueryID <= 0 {
		t.Fatalf("created.QueryID = %d, want > 0", created.QueryID)
	}
	if !created.Enabled {
		t.Fatal("created.Enabled = false, want true default")
	}
	if created.OrderIndex != 0 {
		t.Fatalf("created.OrderIndex = %d, want 0", created.OrderIndex)
	}
	if len(created.GroupNames) != 1 || created.GroupNames[0] != "News" {
		t.Fatalf("created.GroupNames = %#v, want [News]", created.GroupNames)
	}
	if created.SearchRegex {
		t.Fatal("created.SearchRegex = true, want false default")
	}
	if created.NextSlotCursor != 0 {
		t.Fatalf("created.NextSlotCursor = %d, want 0", created.NextSlotCursor)
	}

	multiGroups := []string{"Sports", "News", "sports"}
	updatedGroups, err := store.UpdateDynamicChannelQuery(ctx, created.QueryID, channels.DynamicChannelQueryUpdate{
		GroupNames: &multiGroups,
	})
	if err != nil {
		t.Fatalf("UpdateDynamicChannelQuery(group_names) error = %v", err)
	}
	if updatedGroups.GroupName != "News" {
		t.Fatalf("updatedGroups.GroupName = %q, want News", updatedGroups.GroupName)
	}
	if len(updatedGroups.GroupNames) != 2 || updatedGroups.GroupNames[0] != "News" || updatedGroups.GroupNames[1] != "Sports" {
		t.Fatalf("updatedGroups.GroupNames = %#v, want [News Sports]", updatedGroups.GroupNames)
	}
	if updatedGroups.SearchRegex {
		t.Fatal("updatedGroups.SearchRegex = true, want false when omitted")
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:1",
			ChannelKey: "name:news 1",
			Name:       "News 1",
			Group:      "News",
			StreamURL:  "http://example.com/news-1.ts",
		},
		{
			ItemKey:    "src:news:2",
			ChannelKey: "name:news 2",
			Name:       "News 2",
			Group:      "News",
			StreamURL:  "http://example.com/news-2.ts",
		},
		{
			ItemKey:    "src:news:3",
			ChannelKey: "name:news 3",
			Name:       "News 3",
			Group:      "News",
			StreamURL:  "http://example.com/news-3.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}

	queries, err := store.ListDynamicChannelQueries(ctx)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueries() error = %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("len(queries) = %d, want 1", len(queries))
	}
	if queries[0].LastCount != 3 {
		t.Fatalf("queries[0].LastCount = %d, want 3", queries[0].LastCount)
	}
	if queries[0].NextSlotCursor != 3 {
		t.Fatalf("queries[0].NextSlotCursor = %d, want 3", queries[0].NextSlotCursor)
	}

	paged, total, err := store.ListDynamicGeneratedChannelsPaged(ctx, created.QueryID, 2, 1)
	if err != nil {
		t.Fatalf("ListDynamicGeneratedChannelsPaged() error = %v", err)
	}
	if total != 3 {
		t.Fatalf("total generated channels = %d, want 3", total)
	}
	if len(paged) != 2 {
		t.Fatalf("len(paged generated channels) = %d, want 2", len(paged))
	}
	if paged[0].GuideNumber != "10001" || paged[1].GuideNumber != "10002" {
		t.Fatalf("paged guide numbers = [%q,%q], want [10001,10002]", paged[0].GuideNumber, paged[1].GuideNumber)
	}

	noMatch := "no-such-news"
	if _, err := store.UpdateDynamicChannelQuery(ctx, created.QueryID, channels.DynamicChannelQueryUpdate{SearchQuery: &noMatch}); err != nil {
		t.Fatalf("UpdateDynamicChannelQuery() error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(second) error = %v", err)
	}
	paged, total, err = store.ListDynamicGeneratedChannelsPaged(ctx, created.QueryID, 0, 0)
	if err != nil {
		t.Fatalf("ListDynamicGeneratedChannelsPaged(second) error = %v", err)
	}
	if total != 0 || len(paged) != 0 {
		t.Fatalf("generated channels after no-match update = total=%d len=%d, want total=0 len=0", total, len(paged))
	}

	blankSearch := "   "
	if _, err := store.UpdateDynamicChannelQuery(ctx, created.QueryID, channels.DynamicChannelQueryUpdate{SearchQuery: &blankSearch}); err != nil {
		t.Fatalf("UpdateDynamicChannelQuery(blank search) error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(blank search) error = %v", err)
	}
	paged, total, err = store.ListDynamicGeneratedChannelsPaged(ctx, created.QueryID, 0, 0)
	if err != nil {
		t.Fatalf("ListDynamicGeneratedChannelsPaged(blank search) error = %v", err)
	}
	if total != 3 || len(paged) != 3 {
		t.Fatalf("generated channels after blank-search update = total=%d len=%d, want total=3 len=3", total, len(paged))
	}

	if err := store.DeleteDynamicChannelQuery(ctx, created.QueryID); err != nil {
		t.Fatalf("DeleteDynamicChannelQuery() error = %v", err)
	}
	if _, _, err := store.ListDynamicGeneratedChannelsPaged(ctx, created.QueryID, 0, 0); !errors.Is(err, channels.ErrDynamicQueryNotFound) {
		t.Fatalf("ListDynamicGeneratedChannelsPaged(deleted) error = %v, want ErrDynamicQueryNotFound", err)
	}
}

func TestDynamicChannelQuerySearchRegexPersistenceAndOmittedUpdate(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	created, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Regex Block",
		GroupName:   "News",
		SearchQuery: "news",
		SearchRegex: true,
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery() error = %v", err)
	}
	if !created.SearchRegex {
		t.Fatal("created.SearchRegex = false, want true")
	}

	nextQuery := "news updated"
	updated, err := store.UpdateDynamicChannelQuery(ctx, created.QueryID, channels.DynamicChannelQueryUpdate{
		SearchQuery: &nextQuery,
	})
	if err != nil {
		t.Fatalf("UpdateDynamicChannelQuery(search_query) error = %v", err)
	}
	if updated.SearchQuery != "news updated" {
		t.Fatalf("updated.SearchQuery = %q, want %q", updated.SearchQuery, "news updated")
	}
	if !updated.SearchRegex {
		t.Fatal("updated.SearchRegex = false, want true when field omitted")
	}

	disableRegex := false
	updated, err = store.UpdateDynamicChannelQuery(ctx, created.QueryID, channels.DynamicChannelQueryUpdate{
		SearchRegex: &disableRegex,
	})
	if err != nil {
		t.Fatalf("UpdateDynamicChannelQuery(search_regex) error = %v", err)
	}
	if updated.SearchRegex {
		t.Fatal("updated.SearchRegex = true, want false")
	}
}

func TestDynamicChannelQuerySearchRegexValidationRejectsInvalidPatterns(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if _, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Invalid Regex",
		GroupName:   "News",
		SearchQuery: "([",
		SearchRegex: true,
	}); err == nil || !strings.Contains(strings.ToLower(err.Error()), "invalid regex pattern") {
		t.Fatalf("CreateDynamicChannelQuery(invalid regex) error = %v, want invalid regex pattern validation error", err)
	}

	created, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Compatibility",
		GroupName:   "News",
		SearchQuery: "([",
		SearchRegex: false,
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery(compatibility seed) error = %v", err)
	}

	enableRegex := true
	if _, err := store.UpdateDynamicChannelQuery(ctx, created.QueryID, channels.DynamicChannelQueryUpdate{
		SearchRegex: &enableRegex,
	}); err == nil || !strings.Contains(strings.ToLower(err.Error()), "invalid regex pattern") {
		t.Fatalf("UpdateDynamicChannelQuery(enable invalid regex) error = %v, want invalid regex pattern validation error", err)
	}
}

func TestListDynamicChannelQueriesPagedDeterministicOrderingAndTotals(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	createdIDs := make([]int64, 0, 5)
	for i := 0; i < 5; i++ {
		created, createErr := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
			Name:        fmt.Sprintf("Paged Query %d", i),
			GroupName:   "Paged",
			SearchQuery: fmt.Sprintf("query-%d", i),
		})
		if createErr != nil {
			t.Fatalf("CreateDynamicChannelQuery(%d) error = %v", i, createErr)
		}
		createdIDs = append(createdIDs, created.QueryID)
	}

	page, total, err := store.ListDynamicChannelQueriesPaged(ctx, 2, 1)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueriesPaged(limit=2, offset=1) error = %v", err)
	}
	if total != len(createdIDs) {
		t.Fatalf("paged total = %d, want %d", total, len(createdIDs))
	}
	if len(page) != 2 {
		t.Fatalf("len(page) = %d, want 2", len(page))
	}
	if page[0].QueryID != createdIDs[1] || page[1].QueryID != createdIDs[2] {
		t.Fatalf(
			"paged query ids = [%d,%d], want [%d,%d]",
			page[0].QueryID,
			page[1].QueryID,
			createdIDs[1],
			createdIDs[2],
		)
	}

	allRows, allTotal, err := store.ListDynamicChannelQueriesPaged(ctx, 0, 0)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueriesPaged(limit=0, offset=0) error = %v", err)
	}
	if allTotal != len(createdIDs) {
		t.Fatalf("allTotal = %d, want %d", allTotal, len(createdIDs))
	}
	if len(allRows) != len(createdIDs) {
		t.Fatalf("len(allRows) = %d, want %d", len(allRows), len(createdIDs))
	}

	if _, _, err := store.ListDynamicChannelQueriesPaged(ctx, -1, 0); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("ListDynamicChannelQueriesPaged(limit=-1) error = %v, want limit validation error", err)
	}
	if _, _, err := store.ListDynamicChannelQueriesPaged(ctx, 1, -1); err == nil || !strings.Contains(err.Error(), "offset") {
		t.Fatalf("ListDynamicChannelQueriesPaged(offset=-1) error = %v, want offset validation error", err)
	}

	row, err := store.GetDynamicChannelQuery(ctx, createdIDs[3])
	if err != nil {
		t.Fatalf("GetDynamicChannelQuery() error = %v", err)
	}
	if row.QueryID != createdIDs[3] {
		t.Fatalf("GetDynamicChannelQuery().QueryID = %d, want %d", row.QueryID, createdIDs[3])
	}
	if _, err := store.GetDynamicChannelQuery(ctx, 999999); !errors.Is(err, channels.ErrDynamicQueryNotFound) {
		t.Fatalf("GetDynamicChannelQuery(missing) error = %v, want ErrDynamicQueryNotFound", err)
	}
}

func TestReorderDynamicGeneratedChannelsPersistsGuideOrder(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	query, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Movies Block",
		GroupName:   "Movies",
		SearchQuery: "movie",
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery() error = %v", err)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:movie:a",
			ChannelKey: "name:movie a",
			Name:       "Movie A",
			Group:      "Movies",
			StreamURL:  "http://example.com/movie-a.ts",
		},
		{
			ItemKey:    "src:movie:b",
			ChannelKey: "name:movie b",
			Name:       "Movie B",
			Group:      "Movies",
			StreamURL:  "http://example.com/movie-b.ts",
		},
		{
			ItemKey:    "src:movie:c",
			ChannelKey: "name:movie c",
			Name:       "Movie C",
			Group:      "Movies",
			StreamURL:  "http://example.com/movie-c.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}

	initial, total, err := store.ListDynamicGeneratedChannelsPaged(ctx, query.QueryID, 0, 0)
	if err != nil {
		t.Fatalf("ListDynamicGeneratedChannelsPaged(initial) error = %v", err)
	}
	if total != 3 || len(initial) != 3 {
		t.Fatalf("initial generated channels total=%d len=%d, want 3", total, len(initial))
	}
	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE dynamic_channel_queries
		 SET next_slot_cursor = 900
		 WHERE query_id = ?`,
		query.QueryID,
	); err != nil {
		t.Fatalf("seed next_slot_cursor before reorder error = %v", err)
	}

	reorderedIDs := []int64{initial[1].ChannelID, initial[2].ChannelID, initial[0].ChannelID}
	if err := store.ReorderDynamicGeneratedChannels(ctx, query.QueryID, reorderedIDs); err != nil {
		t.Fatalf("ReorderDynamicGeneratedChannels() error = %v", err)
	}
	updatedQuery, err := store.GetDynamicChannelQuery(ctx, query.QueryID)
	if err != nil {
		t.Fatalf("GetDynamicChannelQuery(after reorder) error = %v", err)
	}
	if updatedQuery.NextSlotCursor != 3 {
		t.Fatalf("updatedQuery.NextSlotCursor = %d, want 3", updatedQuery.NextSlotCursor)
	}

	afterReorder, total, err := store.ListDynamicGeneratedChannelsPaged(ctx, query.QueryID, 0, 0)
	if err != nil {
		t.Fatalf("ListDynamicGeneratedChannelsPaged(after reorder) error = %v", err)
	}
	if total != 3 || len(afterReorder) != 3 {
		t.Fatalf("after reorder generated channels total=%d len=%d, want 3", total, len(afterReorder))
	}
	if afterReorder[0].ChannelID != reorderedIDs[0] || afterReorder[1].ChannelID != reorderedIDs[1] || afterReorder[2].ChannelID != reorderedIDs[2] {
		t.Fatalf("reordered channel IDs = [%d,%d,%d], want [%d,%d,%d]", afterReorder[0].ChannelID, afterReorder[1].ChannelID, afterReorder[2].ChannelID, reorderedIDs[0], reorderedIDs[1], reorderedIDs[2])
	}
	for idx, row := range afterReorder {
		wantGuide := strconv.Itoa(channels.DynamicGuideStart + idx)
		if row.GuideNumber != wantGuide {
			t.Fatalf("afterReorder[%d].GuideNumber = %q, want %q", idx, row.GuideNumber, wantGuide)
		}
	}

	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(after reorder) error = %v", err)
	}
	afterSync, total, err := store.ListDynamicGeneratedChannelsPaged(ctx, query.QueryID, 0, 0)
	if err != nil {
		t.Fatalf("ListDynamicGeneratedChannelsPaged(after sync) error = %v", err)
	}
	if total != 3 || len(afterSync) != 3 {
		t.Fatalf("after sync generated channels total=%d len=%d, want 3", total, len(afterSync))
	}
	if afterSync[0].ChannelID != reorderedIDs[0] || afterSync[1].ChannelID != reorderedIDs[1] || afterSync[2].ChannelID != reorderedIDs[2] {
		t.Fatalf("after sync reordered channel IDs = [%d,%d,%d], want [%d,%d,%d]", afterSync[0].ChannelID, afterSync[1].ChannelID, afterSync[2].ChannelID, reorderedIDs[0], reorderedIDs[1], reorderedIDs[2])
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:movie:a",
			ChannelKey: "name:movie a",
			Name:       "Movie A",
			Group:      "Movies",
			StreamURL:  "http://example.com/movie-a.ts",
		},
		{
			ItemKey:    "src:movie:b",
			ChannelKey: "name:movie b",
			Name:       "Movie B",
			Group:      "Movies",
			StreamURL:  "http://example.com/movie-b.ts",
		},
		{
			ItemKey:    "src:movie:c",
			ChannelKey: "name:movie c",
			Name:       "Movie C",
			Group:      "Movies",
			StreamURL:  "http://example.com/movie-c.ts",
		},
		{
			ItemKey:    "src:movie:d",
			ChannelKey: "name:movie d",
			Name:       "Movie D",
			Group:      "Movies",
			StreamURL:  "http://example.com/movie-d.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(with new source) error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(after adding source) error = %v", err)
	}

	afterAdd, total, err := store.ListDynamicGeneratedChannelsPaged(ctx, query.QueryID, 0, 0)
	if err != nil {
		t.Fatalf("ListDynamicGeneratedChannelsPaged(after add) error = %v", err)
	}
	if total != 4 || len(afterAdd) != 4 {
		t.Fatalf("after add generated channels total=%d len=%d, want 4", total, len(afterAdd))
	}
	if afterAdd[3].GuideNumber != "10003" {
		t.Fatalf("after add new guide number = %q, want 10003", afterAdd[3].GuideNumber)
	}
	updatedQuery, err = store.GetDynamicChannelQuery(ctx, query.QueryID)
	if err != nil {
		t.Fatalf("GetDynamicChannelQuery(after add) error = %v", err)
	}
	if updatedQuery.NextSlotCursor != 4 {
		t.Fatalf("updatedQuery.NextSlotCursor after add = %d, want 4", updatedQuery.NextSlotCursor)
	}
}

func TestDeleteChannelRejectsDynamicGeneratedChannel(t *testing.T) {
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
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}
	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}

	generatedRows, err := loadGeneratedChannelsForTest(ctx, store, queryID)
	if err != nil {
		t.Fatalf("loadGeneratedChannelsForTest() error = %v", err)
	}
	if len(generatedRows) != 1 {
		t.Fatalf("len(generatedRows) = %d, want 1", len(generatedRows))
	}

	err = store.DeleteChannel(ctx, generatedRows[0].ChannelID, channels.TraditionalGuideStart)
	if err == nil {
		t.Fatal("DeleteChannel(dynamic generated) error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "only traditional channels can be deleted") {
		t.Fatalf("DeleteChannel(dynamic generated) error = %v, want traditional-channel guard", err)
	}
}

func TestDynamicChannelQueryCountersReflectCachedSyncState(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const totalMatches = channels.DynamicGuideBlockMaxLen + 5
	items := make([]playlist.Item, 0, totalMatches)
	for i := 0; i < totalMatches; i++ {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:movie:cached:%04d", i),
			ChannelKey: fmt.Sprintf("name:movie cached %04d", i),
			Name:       fmt.Sprintf("Movie Cached %04d", i),
			Group:      "Movies",
			StreamURL:  fmt.Sprintf("http://example.com/movie-cached-%04d.ts", i),
		})
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	created, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Movies Cached Counters",
		GroupName:   "Movies",
		SearchQuery: "movie cached",
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery() error = %v", err)
	}
	if created.LastCount != 0 || created.TruncatedBy != 0 {
		t.Fatalf("create response cached counters = (%d,%d), want (0,0) before sync", created.LastCount, created.TruncatedBy)
	}

	queries, err := store.ListDynamicChannelQueries(ctx)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueries(before sync) error = %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("len(queries before sync) = %d, want 1", len(queries))
	}
	if queries[0].LastCount != 0 || queries[0].TruncatedBy != 0 {
		t.Fatalf("pre-sync counters = (%d,%d), want (0,0)", queries[0].LastCount, queries[0].TruncatedBy)
	}

	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(enabled) error = %v", err)
	}

	queries, err = store.ListDynamicChannelQueries(ctx)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueries(after enabled sync) error = %v", err)
	}
	if queries[0].LastCount != channels.DynamicGuideBlockMaxLen {
		t.Fatalf("post-sync last_count = %d, want %d", queries[0].LastCount, channels.DynamicGuideBlockMaxLen)
	}
	if queries[0].TruncatedBy != 5 {
		t.Fatalf("post-sync truncated_by = %d, want 5", queries[0].TruncatedBy)
	}

	disabled := false
	updated, err := store.UpdateDynamicChannelQuery(ctx, created.QueryID, channels.DynamicChannelQueryUpdate{
		Enabled: &disabled,
	})
	if err != nil {
		t.Fatalf("UpdateDynamicChannelQuery(disable) error = %v", err)
	}
	if updated.LastCount != channels.DynamicGuideBlockMaxLen || updated.TruncatedBy != 5 {
		t.Fatalf("update response cached counters = (%d,%d), want previous synced values (%d,5) before disable sync",
			updated.LastCount,
			updated.TruncatedBy,
			channels.DynamicGuideBlockMaxLen,
		)
	}

	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(disabled) error = %v", err)
	}

	queries, err = store.ListDynamicChannelQueries(ctx)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueries(after disable sync) error = %v", err)
	}
	if queries[0].LastCount != 0 || queries[0].TruncatedBy != 0 {
		t.Fatalf("disabled post-sync counters = (%d,%d), want (0,0)", queries[0].LastCount, queries[0].TruncatedBy)
	}

	generated, total, err := store.ListDynamicGeneratedChannelsPaged(ctx, created.QueryID, 0, 0)
	if err != nil {
		t.Fatalf("ListDynamicGeneratedChannelsPaged(after disable sync) error = %v", err)
	}
	if total != 0 || len(generated) != 0 {
		t.Fatalf("generated channels after disable sync total=%d len=%d, want 0/0", total, len(generated))
	}
}

func TestDynamicChannelQueryCountersRefreshAcrossUpdateAndSync(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:fresh:one",
			ChannelKey: "name:news fresh one",
			Name:       "News Fresh One",
			Group:      "News",
			StreamURL:  "http://example.com/news-fresh-one.ts",
		},
		{
			ItemKey:    "src:news:fresh:two",
			ChannelKey: "name:news fresh two",
			Name:       "News Fresh Two",
			Group:      "News",
			StreamURL:  "http://example.com/news-fresh-two.ts",
		},
		{
			ItemKey:    "src:news:fresh:three",
			ChannelKey: "name:news fresh three",
			Name:       "News Fresh Three",
			Group:      "News",
			StreamURL:  "http://example.com/news-fresh-three.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}

	query, err := store.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Name:        "Freshness Block",
		GroupName:   "News",
		SearchQuery: "news fresh",
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery() error = %v", err)
	}

	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(initial) error = %v", err)
	}

	queries, err := store.ListDynamicChannelQueries(ctx)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueries(initial) error = %v", err)
	}
	if len(queries) != 1 || queries[0].LastCount != 3 || queries[0].TruncatedBy != 0 {
		t.Fatalf("initial synced counters = %+v, want len=1 last_count=3 truncated_by=0", queries)
	}

	narrowSearch := "news fresh two"
	updated, err := store.UpdateDynamicChannelQuery(ctx, query.QueryID, channels.DynamicChannelQueryUpdate{
		SearchQuery: &narrowSearch,
	})
	if err != nil {
		t.Fatalf("UpdateDynamicChannelQuery(narrow search) error = %v", err)
	}
	if updated.LastCount != 3 || updated.TruncatedBy != 0 {
		t.Fatalf("update response counters = (%d,%d), want stale cached values (3,0) until sync", updated.LastCount, updated.TruncatedBy)
	}

	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(after narrow search) error = %v", err)
	}

	queries, err = store.ListDynamicChannelQueries(ctx)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueries(after narrow search) error = %v", err)
	}
	if queries[0].LastCount != 1 || queries[0].TruncatedBy != 0 {
		t.Fatalf("post-update sync counters = (%d,%d), want (1,0)", queries[0].LastCount, queries[0].TruncatedBy)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:fresh:one",
			ChannelKey: "name:news fresh one",
			Name:       "News Fresh One",
			Group:      "News",
			StreamURL:  "http://example.com/news-fresh-one.ts",
		},
		{
			ItemKey:    "src:news:fresh:two",
			ChannelKey: "name:news fresh two",
			Name:       "News Fresh Two",
			Group:      "News",
			StreamURL:  "http://example.com/news-fresh-two.ts",
		},
		{
			ItemKey:    "src:news:fresh:three",
			ChannelKey: "name:news fresh three",
			Name:       "News Fresh Three",
			Group:      "News",
			StreamURL:  "http://example.com/news-fresh-three.ts",
		},
		{
			ItemKey:    "src:news:fresh:two:clone",
			ChannelKey: "name:news fresh two clone",
			Name:       "News Fresh Two Clone",
			Group:      "News",
			StreamURL:  "http://example.com/news-fresh-two-clone.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(clone) error = %v", err)
	}

	if _, err := store.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks(after clone) error = %v", err)
	}

	queries, err = store.ListDynamicChannelQueries(ctx)
	if err != nil {
		t.Fatalf("ListDynamicChannelQueries(after clone) error = %v", err)
	}
	if queries[0].LastCount != 2 || queries[0].TruncatedBy != 0 {
		t.Fatalf("post-clone sync counters = (%d,%d), want (2,0)", queries[0].LastCount, queries[0].TruncatedBy)
	}
}

func BenchmarkListDynamicChannelQueriesCachedCounters(b *testing.B) {
	queryCounts := []int{1, 10, 50, 200}
	itemCounts := []int{1000, 10000}
	patterns := []struct {
		name        string
		groupName   string
		searchQuery string
	}{
		{name: "any", groupName: "", searchQuery: ""},
		{name: "group_only", groupName: "News", searchQuery: ""},
		{name: "multi_term", groupName: "", searchQuery: "alpha beta"},
	}

	for _, queryCount := range queryCounts {
		queryCount := queryCount
		for _, itemCount := range itemCounts {
			itemCount := itemCount
			for _, pattern := range patterns {
				pattern := pattern
				name := fmt.Sprintf("queries_%d/items_%d/%s", queryCount, itemCount, pattern.name)
				b.Run(name, func(b *testing.B) {
					ctx := context.Background()

					store, err := Open(":memory:")
					if err != nil {
						b.Fatalf("Open() error = %v", err)
					}
					defer store.Close()

					items := make([]playlist.Item, 0, itemCount)
					groups := []string{"News", "Sports", "Movies", "Kids"}
					for i := 0; i < itemCount; i++ {
						group := groups[i%len(groups)]
						namePrefix := fmt.Sprintf("%s Benchmark %05d", group, i)
						if i%3 == 0 {
							namePrefix = fmt.Sprintf("%s Alpha Beta %05d", group, i)
						} else if i%3 == 1 {
							namePrefix = fmt.Sprintf("%s Alpha %05d", group, i)
						}
						items = append(items, playlist.Item{
							ItemKey:    fmt.Sprintf("src:dyn-bench:%d", i),
							ChannelKey: fmt.Sprintf("name:%s bench %05d", strings.ToLower(group), i),
							Name:       namePrefix,
							Group:      group,
							StreamURL:  fmt.Sprintf("http://example.com/%s/%05d.ts", strings.ToLower(group), i),
						})
					}
					if err := store.UpsertPlaylistItems(ctx, items); err != nil {
						b.Fatalf("UpsertPlaylistItems() error = %v", err)
					}

					now := time.Now().UTC().Unix()
					for i := 0; i < queryCount; i++ {
						if _, err := store.db.ExecContext(
							ctx,
							`INSERT INTO dynamic_channel_queries (
								enabled,
								name,
								group_name,
								search_query,
								order_index,
								last_count,
								truncated_by,
								created_at,
								updated_at
							)
							VALUES (1, ?, ?, ?, ?, 0, 0, ?, ?)`,
							fmt.Sprintf("Benchmark Query %02d", i),
							pattern.groupName,
							pattern.searchQuery,
							i,
							now,
							now,
						); err != nil {
							b.Fatalf("insert dynamic query row %d: %v", i, err)
						}
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						queries, err := store.ListDynamicChannelQueries(ctx)
						if err != nil {
							b.Fatalf("ListDynamicChannelQueries() error = %v", err)
						}
						if len(queries) != queryCount {
							b.Fatalf("len(queries) = %d, want %d", len(queries), queryCount)
						}
					}
				})
			}
		}
	}
}
