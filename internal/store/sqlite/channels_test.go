package sqlite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/playlist"
)

func TestChannelsLifecycle(t *testing.T) {
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
			StreamURL:  "http://example.com/news-primary.ts",
			TVGID:      "news",
			Attrs:      map[string]string{"tvg-name": "News Primary TVG"},
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "News Backup",
			Group:      "News",
			StreamURL:  "http://example.com/news-backup.ts",
			TVGID:      "news",
			Attrs:      map[string]string{"tvg-name": "News Backup TVG"},
		},
		{
			ItemKey:    "src:sports:primary",
			ChannelKey: "tvg:sports",
			Name:       "Sports",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-primary.ts",
			TVGID:      "sports",
			Attrs:      map[string]string{"tvg-name": "Sports Primary TVG"},
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, "src:news:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	if channel.GuideNumber != "100" {
		t.Fatalf("channel guide number = %q, want 100", channel.GuideNumber)
	}
	if channel.ChannelKey != "tvg:news" {
		t.Fatalf("channel key = %q, want tvg:news", channel.ChannelKey)
	}

	chByGuide, err := store.GetChannelByGuideNumber(ctx, "100")
	if err != nil {
		t.Fatalf("GetChannelByGuideNumber() error = %v", err)
	}
	if chByGuide.ChannelID != channel.ChannelID {
		t.Fatalf("channel id by guide = %d, want %d", chByGuide.ChannelID, channel.ChannelID)
	}

	source, err := store.AddSource(ctx, channel.ChannelID, "src:news:backup", false)
	if err != nil {
		t.Fatalf("AddSource(matching) error = %v", err)
	}
	if source.PriorityIndex != 1 {
		t.Fatalf("source priority = %d, want 1", source.PriorityIndex)
	}
	if source.AssociationType != "channel_key" {
		t.Fatalf("source association_type = %q, want channel_key", source.AssociationType)
	}

	_, err = store.AddSource(ctx, channel.ChannelID, "src:sports:primary", false)
	if !errors.Is(err, channels.ErrAssociationMismatch) {
		t.Fatalf("AddSource(non-matching, no override) error = %v, want ErrAssociationMismatch", err)
	}

	manualSource, err := store.AddSource(ctx, channel.ChannelID, "src:sports:primary", true)
	if err != nil {
		t.Fatalf("AddSource(non-matching, override) error = %v", err)
	}
	if manualSource.AssociationType != "manual" {
		t.Fatalf("manual source association_type = %q, want manual", manualSource.AssociationType)
	}

	sources, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 3 {
		t.Fatalf("len(ListSources) = %d, want 3", len(sources))
	}
	gotTVGNames := map[string]string{}
	for _, src := range sources {
		gotTVGNames[src.ItemKey] = src.TVGName
	}
	if gotTVGNames["src:news:primary"] != "News Primary TVG" {
		t.Fatalf("primary source tvg_name = %q, want %q", gotTVGNames["src:news:primary"], "News Primary TVG")
	}
	if gotTVGNames["src:news:backup"] != "News Backup TVG" {
		t.Fatalf("backup source tvg_name = %q, want %q", gotTVGNames["src:news:backup"], "News Backup TVG")
	}
	if gotTVGNames["src:sports:primary"] != "Sports Primary TVG" {
		t.Fatalf("manual source tvg_name = %q, want %q", gotTVGNames["src:sports:primary"], "Sports Primary TVG")
	}

	if err := store.ReorderSources(ctx, channel.ChannelID, []int64{manualSource.SourceID, source.SourceID, sources[0].SourceID}); err != nil {
		t.Fatalf("ReorderSources() error = %v", err)
	}
	resorted, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() after reorder error = %v", err)
	}
	if resorted[0].SourceID != manualSource.SourceID {
		t.Fatalf("first reordered source id = %d, want %d", resorted[0].SourceID, manualSource.SourceID)
	}

	if err := store.DeleteSource(ctx, channel.ChannelID, source.SourceID); err != nil {
		t.Fatalf("DeleteSource() error = %v", err)
	}
	remaining, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() after delete error = %v", err)
	}
	if len(remaining) != 2 {
		t.Fatalf("len(ListSources) after delete = %d, want 2", len(remaining))
	}
	if remaining[0].PriorityIndex != 0 || remaining[1].PriorityIndex != 1 {
		t.Fatalf("source priorities after delete = [%d,%d], want [0,1]", remaining[0].PriorityIndex, remaining[1].PriorityIndex)
	}
}

func TestListSourcesUsesPersistedTVGNameWhenAttrsJSONMalformed(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const (
		itemKey             = "src:news:primary"
		persistedTVGName    = "Persisted News TVG Name"
		malformedAttrsValue = "{not-json"
	)

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    itemKey,
			ChannelKey: "tvg:news",
			Name:       "News",
			Group:      "News",
			StreamURL:  "http://example.com/news.ts",
			Attrs: map[string]string{
				"tvg-name": persistedTVGName,
			},
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, itemKey, "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}

	if _, err := store.db.ExecContext(
		ctx,
		`UPDATE playlist_items SET attrs_json = ? WHERE item_key = ?`,
		malformedAttrsValue,
		itemKey,
	); err != nil {
		t.Fatalf("inject malformed attrs_json error = %v", err)
	}

	sources, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("len(ListSources) = %d, want 1", len(sources))
	}
	if got := sources[0].TVGName; got != persistedTVGName {
		t.Fatalf("TVGName = %q, want %q", got, persistedTVGName)
	}
}

func TestListChannelsPaged(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	items := []playlist.Item{
		{ItemKey: "src:page:1", ChannelKey: "name:page-1", Name: "Page 1", Group: "Paged", StreamURL: "http://example.com/page-1.ts"},
		{ItemKey: "src:page:2", ChannelKey: "name:page-2", Name: "Page 2", Group: "Paged", StreamURL: "http://example.com/page-2.ts"},
		{ItemKey: "src:page:3", ChannelKey: "name:page-3", Name: "Page 3", Group: "Paged", StreamURL: "http://example.com/page-3.ts"},
		{ItemKey: "src:page:4", ChannelKey: "name:page-4", Name: "Page 4", Group: "Paged", StreamURL: "http://example.com/page-4.ts"},
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	created := make([]channels.Channel, 0, len(items))
	for _, item := range items {
		channel, err := store.CreateChannelFromItem(ctx, item.ItemKey, "", "", nil, 100)
		if err != nil {
			t.Fatalf("CreateChannelFromItem(%q) error = %v", item.ItemKey, err)
		}
		created = append(created, channel)
	}

	enabledFalse := false
	if _, err := store.UpdateChannel(ctx, created[2].ChannelID, nil, &enabledFalse, nil); err != nil {
		t.Fatalf("UpdateChannel(disable) error = %v", err)
	}

	page, total, err := store.ListChannelsPaged(ctx, false, 2, 1)
	if err != nil {
		t.Fatalf("ListChannelsPaged(page) error = %v", err)
	}
	if total != 4 {
		t.Fatalf("ListChannelsPaged(page) total = %d, want 4", total)
	}
	if len(page) != 2 {
		t.Fatalf("len(ListChannelsPaged(page)) = %d, want 2", len(page))
	}
	if page[0].ChannelID != created[1].ChannelID || page[1].ChannelID != created[2].ChannelID {
		t.Fatalf("paged channel order = [%d,%d], want [%d,%d]", page[0].ChannelID, page[1].ChannelID, created[1].ChannelID, created[2].ChannelID)
	}

	tail, tailTotal, err := store.ListChannelsPaged(ctx, false, 0, 2)
	if err != nil {
		t.Fatalf("ListChannelsPaged(tail) error = %v", err)
	}
	if tailTotal != 4 {
		t.Fatalf("ListChannelsPaged(tail) total = %d, want 4", tailTotal)
	}
	if len(tail) != 2 {
		t.Fatalf("len(ListChannelsPaged(tail)) = %d, want 2", len(tail))
	}
	if tail[0].ChannelID != created[2].ChannelID || tail[1].ChannelID != created[3].ChannelID {
		t.Fatalf("tail channel order = [%d,%d], want [%d,%d]", tail[0].ChannelID, tail[1].ChannelID, created[2].ChannelID, created[3].ChannelID)
	}

	enabledPage, enabledTotal, err := store.ListChannelsPaged(ctx, true, 10, 0)
	if err != nil {
		t.Fatalf("ListChannelsPaged(enabled) error = %v", err)
	}
	if enabledTotal != 3 {
		t.Fatalf("ListChannelsPaged(enabled) total = %d, want 3", enabledTotal)
	}
	if len(enabledPage) != 3 {
		t.Fatalf("len(ListChannelsPaged(enabled)) = %d, want 3", len(enabledPage))
	}
	for _, channel := range enabledPage {
		if !channel.Enabled {
			t.Fatalf("enabled page includes disabled channel_id=%d", channel.ChannelID)
		}
	}
}

func TestListChannelsPagedIncludesSourceSummaryCounts(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:summary:primary",
			ChannelKey: "tvg:summary",
			Name:       "Summary Primary",
			Group:      "Summary",
			StreamURL:  "http://example.com/summary-primary.ts",
		},
		{
			ItemKey:    "src:summary:backup",
			ChannelKey: "tvg:summary",
			Name:       "Summary Backup",
			Group:      "Summary",
			StreamURL:  "http://example.com/summary-backup.ts",
		},
		{
			ItemKey:    "src:summary:manual",
			ChannelKey: "tvg:other",
			Name:       "Summary Manual",
			Group:      "Summary",
			StreamURL:  "http://example.com/summary-manual.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelWithSources, err := store.CreateChannelFromItem(
		ctx,
		"src:summary:primary",
		"",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "Summary",
			SearchQuery: "summary",
		},
		100,
	)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(dynamic seeded) error = %v", err)
	}
	if _, err := store.AddSource(ctx, channelWithSources.ChannelID, "src:summary:backup", false); err != nil {
		t.Fatalf("AddSource(backup) error = %v", err)
	}
	if _, err := store.AddSource(ctx, channelWithSources.ChannelID, "src:summary:manual", true); err != nil {
		t.Fatalf("AddSource(manual) error = %v", err)
	}
	sources, err := store.ListSources(ctx, channelWithSources.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources(channelWithSources) error = %v", err)
	}
	var manualSourceID int64
	for _, src := range sources {
		if src.ItemKey == "src:summary:manual" {
			manualSourceID = src.SourceID
			break
		}
	}
	if manualSourceID == 0 {
		t.Fatal("manual source id not found")
	}
	disabled := false
	if _, err := store.UpdateSource(ctx, channelWithSources.ChannelID, manualSourceID, &disabled); err != nil {
		t.Fatalf("UpdateSource(disable manual) error = %v", err)
	}

	channelWithoutSources, err := store.CreateChannelFromItem(
		ctx,
		"",
		"Summary Empty",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "Summary",
			SearchQuery: "no-match",
		},
		100,
	)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(dynamic no seed) error = %v", err)
	}

	page, total, err := store.ListChannelsPaged(ctx, false, 10, 0)
	if err != nil {
		t.Fatalf("ListChannelsPaged() error = %v", err)
	}
	if total != 2 {
		t.Fatalf("ListChannelsPaged() total = %d, want 2", total)
	}
	if len(page) != 2 {
		t.Fatalf("len(ListChannelsPaged()) = %d, want 2", len(page))
	}

	byID := make(map[int64]channels.Channel, len(page))
	for _, row := range page {
		byID[row.ChannelID] = row
	}

	withSources, ok := byID[channelWithSources.ChannelID]
	if !ok {
		t.Fatalf("ListChannelsPaged missing channel_id=%d", channelWithSources.ChannelID)
	}
	if withSources.SourceTotal != 3 {
		t.Fatalf("withSources.SourceTotal = %d, want 3", withSources.SourceTotal)
	}
	if withSources.SourceEnabled != 2 {
		t.Fatalf("withSources.SourceEnabled = %d, want 2", withSources.SourceEnabled)
	}
	if withSources.SourceDynamic != 1 {
		t.Fatalf("withSources.SourceDynamic = %d, want 1", withSources.SourceDynamic)
	}
	if withSources.SourceManual != 2 {
		t.Fatalf("withSources.SourceManual = %d, want 2", withSources.SourceManual)
	}

	withoutSources, ok := byID[channelWithoutSources.ChannelID]
	if !ok {
		t.Fatalf("ListChannelsPaged missing channel_id=%d", channelWithoutSources.ChannelID)
	}
	if withoutSources.SourceTotal != 0 || withoutSources.SourceEnabled != 0 || withoutSources.SourceDynamic != 0 || withoutSources.SourceManual != 0 {
		t.Fatalf(
			"withoutSources summary = total:%d enabled:%d dynamic:%d manual:%d, want all zero",
			withoutSources.SourceTotal,
			withoutSources.SourceEnabled,
			withoutSources.SourceDynamic,
			withoutSources.SourceManual,
		)
	}

	allRows, err := store.ListChannels(ctx, false)
	if err != nil {
		t.Fatalf("ListChannels() error = %v", err)
	}
	if len(allRows) != 2 {
		t.Fatalf("len(ListChannels()) = %d, want 2", len(allRows))
	}
	allByID := make(map[int64]channels.Channel, len(allRows))
	for _, row := range allRows {
		allByID[row.ChannelID] = row
	}
	if got := allByID[channelWithSources.ChannelID].SourceDynamic; got != 1 {
		t.Fatalf("ListChannels() SourceDynamic = %d, want 1", got)
	}
	if got := allByID[channelWithSources.ChannelID].SourceManual; got != 2 {
		t.Fatalf("ListChannels() SourceManual = %d, want 2", got)
	}
}

func TestListSourcesPaged(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	items := []playlist.Item{
		{
			ItemKey:    "src:paged:one",
			ChannelKey: "tvg:paged",
			Name:       "Paged One",
			Group:      "Paged",
			StreamURL:  "http://example.com/paged-one.ts",
		},
		{
			ItemKey:    "src:paged:two",
			ChannelKey: "tvg:paged",
			Name:       "Paged Two",
			Group:      "Paged",
			StreamURL:  "http://example.com/paged-two.ts",
		},
		{
			ItemKey:    "src:paged:three",
			ChannelKey: "tvg:paged",
			Name:       "Paged Three",
			Group:      "Paged",
			StreamURL:  "http://example.com/paged-three.ts",
		},
		{
			ItemKey:    "src:paged:four",
			ChannelKey: "tvg:paged",
			Name:       "Paged Four",
			Group:      "Paged",
			StreamURL:  "http://example.com/paged-four.ts",
		},
		{
			ItemKey:    "src:paged:five",
			ChannelKey: "tvg:paged",
			Name:       "Paged Five",
			Group:      "Paged",
			StreamURL:  "http://example.com/paged-five.ts",
		},
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, items[0].ItemKey, "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	for _, item := range items[1:] {
		if _, err := store.AddSource(ctx, channel.ChannelID, item.ItemKey, false); err != nil {
			t.Fatalf("AddSource(%q) error = %v", item.ItemKey, err)
		}
	}

	allSources, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources(all) error = %v", err)
	}
	if len(allSources) != len(items) {
		t.Fatalf("len(ListSources(all)) = %d, want %d", len(allSources), len(items))
	}

	enabledFalse := false
	if _, err := store.UpdateSource(ctx, channel.ChannelID, allSources[2].SourceID, &enabledFalse); err != nil {
		t.Fatalf("UpdateSource(disable) error = %v", err)
	}

	page, total, err := store.ListSourcesPaged(ctx, channel.ChannelID, false, 2, 1)
	if err != nil {
		t.Fatalf("ListSourcesPaged(page) error = %v", err)
	}
	if total != len(items) {
		t.Fatalf("ListSourcesPaged(page) total = %d, want %d", total, len(items))
	}
	if len(page) != 2 {
		t.Fatalf("len(ListSourcesPaged(page)) = %d, want 2", len(page))
	}
	if page[0].SourceID != allSources[1].SourceID || page[1].SourceID != allSources[2].SourceID {
		t.Fatalf(
			"paged source order = [%d,%d], want [%d,%d]",
			page[0].SourceID,
			page[1].SourceID,
			allSources[1].SourceID,
			allSources[2].SourceID,
		)
	}

	enabledPage, enabledTotal, err := store.ListSourcesPaged(ctx, channel.ChannelID, true, 10, 0)
	if err != nil {
		t.Fatalf("ListSourcesPaged(enabled) error = %v", err)
	}
	if enabledTotal != len(items)-1 {
		t.Fatalf("ListSourcesPaged(enabled) total = %d, want %d", enabledTotal, len(items)-1)
	}
	if len(enabledPage) != len(items)-1 {
		t.Fatalf("len(ListSourcesPaged(enabled)) = %d, want %d", len(enabledPage), len(items)-1)
	}
	for _, source := range enabledPage {
		if !source.Enabled {
			t.Fatalf("enabled page includes disabled source_id=%d", source.SourceID)
		}
	}

	for _, source := range allSources {
		if _, err := store.UpdateSource(ctx, channel.ChannelID, source.SourceID, &enabledFalse); err != nil {
			t.Fatalf("UpdateSource(disable all source_id=%d) error = %v", source.SourceID, err)
		}
	}
	emptyEnabledPage, emptyEnabledTotal, err := store.ListSourcesPaged(ctx, channel.ChannelID, true, 10, 0)
	if err != nil {
		t.Fatalf("ListSourcesPaged(enabled empty) error = %v", err)
	}
	if emptyEnabledTotal != 0 {
		t.Fatalf("ListSourcesPaged(enabled empty) total = %d, want 0", emptyEnabledTotal)
	}
	if len(emptyEnabledPage) != 0 {
		t.Fatalf("len(ListSourcesPaged(enabled empty)) = %d, want 0", len(emptyEnabledPage))
	}

	tail, tailTotal, err := store.ListSourcesPaged(ctx, channel.ChannelID, false, 0, len(items)-1)
	if err != nil {
		t.Fatalf("ListSourcesPaged(tail) error = %v", err)
	}
	if tailTotal != len(items) {
		t.Fatalf("ListSourcesPaged(tail) total = %d, want %d", tailTotal, len(items))
	}
	if len(tail) != 1 {
		t.Fatalf("len(ListSourcesPaged(tail)) = %d, want 1", len(tail))
	}
	if tail[0].SourceID != allSources[len(allSources)-1].SourceID {
		t.Fatalf("tail source id = %d, want %d", tail[0].SourceID, allSources[len(allSources)-1].SourceID)
	}

	if _, _, err := store.ListSourcesPaged(ctx, 999999, false, 10, 0); !errors.Is(err, channels.ErrChannelNotFound) {
		t.Fatalf("ListSourcesPaged(missing channel) error = %v, want ErrChannelNotFound", err)
	}
}

func TestGetSource(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	items := []playlist.Item{
		{
			ItemKey:    "src:getsource:primary",
			ChannelKey: "tvg:getsource",
			Name:       "GetSource Primary",
			Group:      "News",
			StreamURL:  "http://example.com/getsource-primary.ts",
			Attrs: map[string]string{
				"tvg-name": "GetSource TVG Primary",
			},
		},
		{
			ItemKey:    "src:getsource:backup",
			ChannelKey: "tvg:getsource",
			Name:       "GetSource Backup",
			Group:      "News",
			StreamURL:  "http://example.com/getsource-backup.ts",
			Attrs: map[string]string{
				"tvg-name": "GetSource TVG Backup",
			},
		},
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, items[0].ItemKey, "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	added, err := store.AddSource(ctx, channel.ChannelID, items[1].ItemKey, false)
	if err != nil {
		t.Fatalf("AddSource() error = %v", err)
	}

	sources, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("len(ListSources()) = %d, want 2", len(sources))
	}

	gotPrimary, err := store.GetSource(ctx, channel.ChannelID, sources[0].SourceID, true)
	if err != nil {
		t.Fatalf("GetSource(primary) error = %v", err)
	}
	if gotPrimary.SourceID != sources[0].SourceID {
		t.Fatalf("GetSource(primary).SourceID = %d, want %d", gotPrimary.SourceID, sources[0].SourceID)
	}
	if gotPrimary.TVGName == "" {
		t.Fatal("GetSource(primary).TVGName is empty, want persisted tvg-name")
	}

	enabledFalse := false
	disabled, err := store.UpdateSource(ctx, channel.ChannelID, added.SourceID, &enabledFalse)
	if err != nil {
		t.Fatalf("UpdateSource(disable) error = %v", err)
	}
	if disabled.Enabled {
		t.Fatal("UpdateSource(disable).Enabled = true, want false")
	}

	if _, err := store.GetSource(ctx, channel.ChannelID, added.SourceID, true); !errors.Is(err, channels.ErrSourceNotFound) {
		t.Fatalf("GetSource(enabledOnly=true disabled source) error = %v, want ErrSourceNotFound", err)
	}

	gotDisabled, err := store.GetSource(ctx, channel.ChannelID, added.SourceID, false)
	if err != nil {
		t.Fatalf("GetSource(enabledOnly=false disabled source) error = %v", err)
	}
	if gotDisabled.Enabled {
		t.Fatal("GetSource(enabledOnly=false disabled source).Enabled = true, want false")
	}

	if _, err := store.GetSource(ctx, 999999, added.SourceID, false); !errors.Is(err, channels.ErrChannelNotFound) {
		t.Fatalf("GetSource(missing channel) error = %v, want ErrChannelNotFound", err)
	}
	if _, err := store.GetSource(ctx, channel.ChannelID, 999999, false); !errors.Is(err, channels.ErrSourceNotFound) {
		t.Fatalf("GetSource(missing source) error = %v, want ErrSourceNotFound", err)
	}
}

func BenchmarkListSourcesHotPath(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const sourceCount = 250
	items := make([]playlist.Item, 0, sourceCount)
	for i := 0; i < sourceCount; i++ {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:bench:%03d", i),
			ChannelKey: "tvg:bench",
			Name:       fmt.Sprintf("Benchmark Source %03d", i),
			Group:      "Benchmark",
			StreamURL:  fmt.Sprintf("http://example.com/bench-%03d.ts", i),
			Attrs: map[string]string{
				"tvg-name": fmt.Sprintf("Benchmark TVG %03d", i),
			},
		})
	}

	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, items[0].ItemKey, "", "", nil, 100)
	if err != nil {
		b.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	for i := 1; i < sourceCount; i++ {
		if _, err := store.AddSource(ctx, channel.ChannelID, items[i].ItemKey, false); err != nil {
			b.Fatalf("AddSource(%d) error = %v", i, err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sources, err := store.ListSources(ctx, channel.ChannelID, true)
		if err != nil {
			b.Fatalf("ListSources() error = %v", err)
		}
		if len(sources) != sourceCount {
			b.Fatalf("len(ListSources) = %d, want %d", len(sources), sourceCount)
		}
	}
}

func TestListSourcesByChannelIDs(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	items := []playlist.Item{
		{
			ItemKey:    "src:bulk:news:primary",
			ChannelKey: "tvg:bulknews",
			Name:       "Bulk News Primary",
			Group:      "Bulk",
			StreamURL:  "http://example.com/bulk-news-primary.ts",
			Attrs: map[string]string{
				"tvg-name": "Bulk News Primary TVG",
			},
		},
		{
			ItemKey:    "src:bulk:news:backup",
			ChannelKey: "tvg:bulknews",
			Name:       "Bulk News Backup",
			Group:      "Bulk",
			StreamURL:  "http://example.com/bulk-news-backup.ts",
			Attrs: map[string]string{
				"tvg-name": "Bulk News Backup TVG",
			},
		},
		{
			ItemKey:    "src:bulk:sports:primary",
			ChannelKey: "tvg:bulksports",
			Name:       "Bulk Sports Primary",
			Group:      "Bulk",
			StreamURL:  "http://example.com/bulk-sports-primary.ts",
		},
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	newsChannel, err := store.CreateChannelFromItem(ctx, "src:bulk:news:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(news) error = %v", err)
	}
	if _, err := store.AddSource(ctx, newsChannel.ChannelID, "src:bulk:news:backup", false); err != nil {
		t.Fatalf("AddSource(news backup) error = %v", err)
	}

	sportsChannel, err := store.CreateChannelFromItem(ctx, "src:bulk:sports:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(sports) error = %v", err)
	}
	sportsSources, err := store.ListSources(ctx, sportsChannel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources(sports setup) error = %v", err)
	}
	if len(sportsSources) != 1 {
		t.Fatalf("len(sports sources) = %d, want 1", len(sportsSources))
	}
	if err := store.DeleteSource(ctx, sportsChannel.ChannelID, sportsSources[0].SourceID); err != nil {
		t.Fatalf("DeleteSource(sports seed) error = %v", err)
	}

	newsSources, err := store.ListSources(ctx, newsChannel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources(news setup) error = %v", err)
	}
	if len(newsSources) != 2 {
		t.Fatalf("len(news setup sources) = %d, want 2", len(newsSources))
	}

	enabledFalse := false
	if _, err := store.UpdateSource(ctx, newsChannel.ChannelID, newsSources[1].SourceID, &enabledFalse); err != nil {
		t.Fatalf("UpdateSource(disable backup) error = %v", err)
	}

	bulkAll, err := store.ListSourcesByChannelIDs(ctx, []int64{
		newsChannel.ChannelID,
		sportsChannel.ChannelID,
		newsChannel.ChannelID,
	}, false)
	if err != nil {
		t.Fatalf("ListSourcesByChannelIDs(all) error = %v", err)
	}
	if len(bulkAll) != 2 {
		t.Fatalf("len(bulkAll) = %d, want 2 distinct channels", len(bulkAll))
	}
	if got := len(bulkAll[newsChannel.ChannelID]); got != 2 {
		t.Fatalf("len(bulkAll[news]) = %d, want 2", got)
	}
	if got := len(bulkAll[sportsChannel.ChannelID]); got != 0 {
		t.Fatalf("len(bulkAll[sports]) = %d, want 0 (channel exists with no sources)", got)
	}
	if bulkAll[newsChannel.ChannelID][0].SourceID != newsSources[0].SourceID ||
		bulkAll[newsChannel.ChannelID][1].SourceID != newsSources[1].SourceID {
		t.Fatalf(
			"bulk news order = [%d %d], want [%d %d]",
			bulkAll[newsChannel.ChannelID][0].SourceID,
			bulkAll[newsChannel.ChannelID][1].SourceID,
			newsSources[0].SourceID,
			newsSources[1].SourceID,
		)
	}
	if bulkAll[newsChannel.ChannelID][0].TVGName != "Bulk News Primary TVG" {
		t.Fatalf("bulk primary tvg_name = %q, want %q", bulkAll[newsChannel.ChannelID][0].TVGName, "Bulk News Primary TVG")
	}
	if bulkAll[newsChannel.ChannelID][1].TVGName != "Bulk News Backup TVG" {
		t.Fatalf("bulk backup tvg_name = %q, want %q", bulkAll[newsChannel.ChannelID][1].TVGName, "Bulk News Backup TVG")
	}

	bulkEnabled, err := store.ListSourcesByChannelIDs(ctx, []int64{newsChannel.ChannelID}, true)
	if err != nil {
		t.Fatalf("ListSourcesByChannelIDs(enabledOnly) error = %v", err)
	}
	if got := len(bulkEnabled[newsChannel.ChannelID]); got != 1 {
		t.Fatalf("len(bulkEnabled[news]) = %d, want 1 enabled source", got)
	}
	if !bulkEnabled[newsChannel.ChannelID][0].Enabled {
		t.Fatal("bulk enabled-only source returned disabled row")
	}

	_, err = store.ListSourcesByChannelIDs(ctx, []int64{newsChannel.ChannelID, 999999}, false)
	if !errors.Is(err, channels.ErrChannelNotFound) {
		t.Fatalf("ListSourcesByChannelIDs(missing channel) error = %v, want ErrChannelNotFound", err)
	}
}

func BenchmarkListSourcesByChannelIDsHotPath(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const (
		channelCount      = 200
		sourcesPerChannel = 4
	)
	items := make([]playlist.Item, 0, channelCount*sourcesPerChannel)
	for channelIdx := 0; channelIdx < channelCount; channelIdx++ {
		channelKey := fmt.Sprintf("tvg:bulkbench:%03d", channelIdx)
		for sourceIdx := 0; sourceIdx < sourcesPerChannel; sourceIdx++ {
			itemKey := fmt.Sprintf("src:bulkbench:%03d:%02d", channelIdx, sourceIdx)
			items = append(items, playlist.Item{
				ItemKey:    itemKey,
				ChannelKey: channelKey,
				Name:       fmt.Sprintf("Bulk Benchmark Channel %03d", channelIdx),
				Group:      "Benchmark",
				StreamURL:  fmt.Sprintf("http://example.com/bulkbench-%03d-%02d.ts", channelIdx, sourceIdx),
			})
		}
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelIDs := make([]int64, 0, channelCount)
	for channelIdx := 0; channelIdx < channelCount; channelIdx++ {
		firstItemKey := fmt.Sprintf("src:bulkbench:%03d:00", channelIdx)
		channel, err := store.CreateChannelFromItem(ctx, firstItemKey, "", "", nil, 100)
		if err != nil {
			b.Fatalf("CreateChannelFromItem(%d) error = %v", channelIdx, err)
		}
		channelIDs = append(channelIDs, channel.ChannelID)
		for sourceIdx := 1; sourceIdx < sourcesPerChannel; sourceIdx++ {
			itemKey := fmt.Sprintf("src:bulkbench:%03d:%02d", channelIdx, sourceIdx)
			if _, err := store.AddSource(ctx, channel.ChannelID, itemKey, false); err != nil {
				b.Fatalf("AddSource(%d,%d) error = %v", channelIdx, sourceIdx, err)
			}
		}
	}

	expectedTotal := channelCount * sourcesPerChannel

	b.Run("per_channel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			totalSources := 0
			for _, channelID := range channelIDs {
				sources, err := store.ListSources(ctx, channelID, true)
				if err != nil {
					b.Fatalf("ListSources() error = %v", err)
				}
				totalSources += len(sources)
			}
			if totalSources != expectedTotal {
				b.Fatalf("totalSources = %d, want %d", totalSources, expectedTotal)
			}
		}
	})

	b.Run("bulk", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			byChannel, err := store.ListSourcesByChannelIDs(ctx, channelIDs, true)
			if err != nil {
				b.Fatalf("ListSourcesByChannelIDs() error = %v", err)
			}
			if len(byChannel) != channelCount {
				b.Fatalf("len(byChannel) = %d, want %d", len(byChannel), channelCount)
			}
			totalSources := 0
			for _, channelID := range channelIDs {
				totalSources += len(byChannel[channelID])
			}
			if totalSources != expectedTotal {
				b.Fatalf("totalSources = %d, want %d", totalSources, expectedTotal)
			}
		}
	})
}

func BenchmarkGetSourceHotPath(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const sourceCount = 250
	items := make([]playlist.Item, 0, sourceCount)
	for i := 0; i < sourceCount; i++ {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:getsource-bench:%03d", i),
			ChannelKey: "tvg:getsource-bench",
			Name:       fmt.Sprintf("GetSource Benchmark %03d", i),
			Group:      "Benchmark",
			StreamURL:  fmt.Sprintf("http://example.com/getsource-bench-%03d.ts", i),
			Attrs: map[string]string{
				"tvg-name": fmt.Sprintf("GetSource TVG %03d", i),
			},
		})
	}

	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, items[0].ItemKey, "", "", nil, 100)
	if err != nil {
		b.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	for i := 1; i < sourceCount; i++ {
		if _, err := store.AddSource(ctx, channel.ChannelID, items[i].ItemKey, false); err != nil {
			b.Fatalf("AddSource(%d) error = %v", i, err)
		}
	}

	sources, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		b.Fatalf("ListSources() setup error = %v", err)
	}
	targetSourceID := sources[len(sources)-1].SourceID

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		source, err := store.GetSource(ctx, channel.ChannelID, targetSourceID, true)
		if err != nil {
			b.Fatalf("GetSource() error = %v", err)
		}
		if source.SourceID != targetSourceID {
			b.Fatalf("GetSource().SourceID = %d, want %d", source.SourceID, targetSourceID)
		}
	}
}

func BenchmarkListChannelsPagedHotPath(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const channelCount = 1500
	items := make([]playlist.Item, 0, channelCount)
	for i := 0; i < channelCount; i++ {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:channels-bench:%04d", i),
			ChannelKey: fmt.Sprintf("name:channels-bench:%04d", i),
			Name:       fmt.Sprintf("Channel Benchmark %04d", i),
			Group:      "Benchmark",
			StreamURL:  fmt.Sprintf("http://example.com/channels-bench-%04d.ts", i),
		})
	}

	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}
	for i := 0; i < channelCount; i++ {
		if _, err := store.CreateChannelFromItem(ctx, items[i].ItemKey, "", "", nil, 100); err != nil {
			b.Fatalf("CreateChannelFromItem(%d) error = %v", i, err)
		}
	}

	const pageLimit = 100
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		page, total, err := store.ListChannelsPaged(ctx, false, pageLimit, 0)
		if err != nil {
			b.Fatalf("ListChannelsPaged() error = %v", err)
		}
		if total != channelCount {
			b.Fatalf("ListChannelsPaged() total = %d, want %d", total, channelCount)
		}
		if len(page) != pageLimit {
			b.Fatalf("len(ListChannelsPaged()) = %d, want %d", len(page), pageLimit)
		}
	}
}

func BenchmarkListSourcesPagedHotPath(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const sourceCount = 250
	items := make([]playlist.Item, 0, sourceCount)
	for i := 0; i < sourceCount; i++ {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:paged-bench:%03d", i),
			ChannelKey: "tvg:paged-bench",
			Name:       fmt.Sprintf("Paged Benchmark Source %03d", i),
			Group:      "Benchmark",
			StreamURL:  fmt.Sprintf("http://example.com/paged-bench-%03d.ts", i),
		})
	}

	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, items[0].ItemKey, "", "", nil, 100)
	if err != nil {
		b.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	for i := 1; i < sourceCount; i++ {
		if _, err := store.AddSource(ctx, channel.ChannelID, items[i].ItemKey, false); err != nil {
			b.Fatalf("AddSource(%d) error = %v", i, err)
		}
	}

	const pageLimit = 50
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		page, total, err := store.ListSourcesPaged(ctx, channel.ChannelID, true, pageLimit, 0)
		if err != nil {
			b.Fatalf("ListSourcesPaged() error = %v", err)
		}
		if total != sourceCount {
			b.Fatalf("ListSourcesPaged() total = %d, want %d", total, sourceCount)
		}
		if len(page) != pageLimit {
			b.Fatalf("len(ListSourcesPaged()) = %d, want %d", len(page), pageLimit)
		}
	}
}

func TestReorderChannels(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{ItemKey: "src:one:1", ChannelKey: "name:one", Name: "One", Group: "A", StreamURL: "http://example.com/one.ts"},
		{ItemKey: "src:two:2", ChannelKey: "name:two", Name: "Two", Group: "A", StreamURL: "http://example.com/two.ts"},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	first, err := store.CreateChannelFromItem(ctx, "src:one:1", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(first) error = %v", err)
	}
	second, err := store.CreateChannelFromItem(ctx, "src:two:2", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(second) error = %v", err)
	}

	if err := store.ReorderChannels(ctx, []int64{second.ChannelID, first.ChannelID}, 100); err != nil {
		t.Fatalf("ReorderChannels() error = %v", err)
	}

	channelsList, err := store.ListChannels(ctx, false)
	if err != nil {
		t.Fatalf("ListChannels() error = %v", err)
	}
	if len(channelsList) != 2 {
		t.Fatalf("len(ListChannels) = %d, want 2", len(channelsList))
	}
	if channelsList[0].ChannelID != second.ChannelID || channelsList[0].GuideNumber != "100" {
		t.Fatalf("first reordered channel = %+v, want channel %d guide 100", channelsList[0], second.ChannelID)
	}
	if channelsList[1].ChannelID != first.ChannelID || channelsList[1].GuideNumber != "101" {
		t.Fatalf("second reordered channel = %+v, want channel %d guide 101", channelsList[1], first.ChannelID)
	}
}

func TestUpdateChannelAndSource(t *testing.T) {
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

	guideName := "News Prime"
	enabled := false
	updatedChannel, err := store.UpdateChannel(ctx, channel.ChannelID, &guideName, &enabled, nil)
	if err != nil {
		t.Fatalf("UpdateChannel() error = %v", err)
	}
	if updatedChannel.GuideName != guideName {
		t.Fatalf("updated guide_name = %q, want %q", updatedChannel.GuideName, guideName)
	}
	if updatedChannel.Enabled {
		t.Fatal("updated channel enabled = true, want false")
	}

	sources, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("len(ListSources) = %d, want 1", len(sources))
	}

	updatedSource, err := store.UpdateSource(ctx, channel.ChannelID, sources[0].SourceID, &enabled)
	if err != nil {
		t.Fatalf("UpdateSource() error = %v", err)
	}
	if updatedSource.Enabled {
		t.Fatal("updated source enabled = true, want false")
	}

	if _, err := store.UpdateChannel(ctx, 999, &guideName, nil, nil); !errors.Is(err, channels.ErrChannelNotFound) {
		t.Fatalf("UpdateChannel(missing) error = %v, want ErrChannelNotFound", err)
	}
	if _, err := store.UpdateSource(ctx, channel.ChannelID, 999, &enabled); !errors.Is(err, channels.ErrSourceNotFound) {
		t.Fatalf("UpdateSource(missing) error = %v, want ErrSourceNotFound", err)
	}
}

func TestCreateAndUpdateChannelDynamicRule(t *testing.T) {
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
			Name:       "News Primary",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	created, err := store.CreateChannelFromItem(
		ctx,
		"src:news:primary",
		"",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: "fox kmsp",
		},
		100,
	)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(dynamic) error = %v", err)
	}
	if !created.DynamicRule.Enabled {
		t.Fatal("created.DynamicRule.Enabled = false, want true")
	}
	if created.DynamicRule.GroupName != "US News" {
		t.Fatalf("created.DynamicRule.GroupName = %q, want %q", created.DynamicRule.GroupName, "US News")
	}
	if len(created.DynamicRule.GroupNames) != 1 || created.DynamicRule.GroupNames[0] != "US News" {
		t.Fatalf("created.DynamicRule.GroupNames = %#v, want [US News]", created.DynamicRule.GroupNames)
	}
	if created.DynamicRule.SearchQuery != "fox kmsp" {
		t.Fatalf("created.DynamicRule.SearchQuery = %q, want %q", created.DynamicRule.SearchQuery, "fox kmsp")
	}
	if created.DynamicRule.SearchRegex {
		t.Fatal("created.DynamicRule.SearchRegex = true, want false default")
	}
	createdSources, err := store.ListSources(ctx, created.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources(created dynamic) error = %v", err)
	}
	if len(createdSources) != 1 {
		t.Fatalf("len(created dynamic sources) = %d, want 1", len(createdSources))
	}
	if got := createdSources[0].AssociationType; got != "dynamic_query" {
		t.Fatalf("created dynamic seed association_type = %q, want dynamic_query", got)
	}

	list, err := store.ListChannels(ctx, false)
	if err != nil {
		t.Fatalf("ListChannels() error = %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("len(ListChannels) = %d, want 1", len(list))
	}
	if !list[0].DynamicRule.Enabled {
		t.Fatal("listed channel dynamic enabled = false, want true")
	}
	if list[0].DynamicRule.SearchRegex {
		t.Fatal("listed channel dynamic search_regex = true, want false default")
	}

	guide, err := store.GetChannelByGuideNumber(ctx, created.GuideNumber)
	if err != nil {
		t.Fatalf("GetChannelByGuideNumber() error = %v", err)
	}
	if guide.DynamicRule.SearchQuery != "fox kmsp" {
		t.Fatalf("guide lookup dynamic search_query = %q, want fox kmsp", guide.DynamicRule.SearchQuery)
	}
	if guide.DynamicRule.SearchRegex {
		t.Fatal("guide lookup dynamic search_regex = true, want false default")
	}
	if len(guide.DynamicRule.GroupNames) != 1 || guide.DynamicRule.GroupNames[0] != "US News" {
		t.Fatalf("guide lookup dynamic group_names = %#v, want [US News]", guide.DynamicRule.GroupNames)
	}

	updated, err := store.UpdateChannel(
		ctx,
		created.ChannelID,
		nil,
		nil,
		&channels.DynamicSourceRule{
			Enabled:     false,
			GroupName:   "US Local",
			SearchQuery: "fox",
		},
	)
	if err != nil {
		t.Fatalf("UpdateChannel(dynamic rule) error = %v", err)
	}
	if updated.DynamicRule.Enabled {
		t.Fatal("updated.DynamicRule.Enabled = true, want false")
	}
	if updated.DynamicRule.GroupName != "US Local" {
		t.Fatalf("updated.DynamicRule.GroupName = %q, want US Local", updated.DynamicRule.GroupName)
	}
	if len(updated.DynamicRule.GroupNames) != 1 || updated.DynamicRule.GroupNames[0] != "US Local" {
		t.Fatalf("updated.DynamicRule.GroupNames = %#v, want [US Local]", updated.DynamicRule.GroupNames)
	}
	if updated.DynamicRule.SearchQuery != "fox" {
		t.Fatalf("updated.DynamicRule.SearchQuery = %q, want fox", updated.DynamicRule.SearchQuery)
	}
	if updated.DynamicRule.SearchRegex {
		t.Fatal("updated.DynamicRule.SearchRegex = true, want false when omitted")
	}

	updatedRegex, err := store.UpdateChannel(
		ctx,
		created.ChannelID,
		nil,
		nil,
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US Local",
			SearchQuery: "fox",
			SearchRegex: true,
		},
	)
	if err != nil {
		t.Fatalf("UpdateChannel(dynamic regex rule) error = %v", err)
	}
	if !updatedRegex.DynamicRule.SearchRegex {
		t.Fatal("updatedRegex.DynamicRule.SearchRegex = false, want true")
	}
}

func TestCreateChannelWithoutSeedItemWhenDynamicEnabled(t *testing.T) {
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
			Name:       "News Primary",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "News Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	created, err := store.CreateChannelFromItem(
		ctx,
		"",
		"",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: "fox kmsp",
		},
		100,
	)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(dynamic, no seed item) error = %v", err)
	}
	if created.GuideName != "fox kmsp" {
		t.Fatalf("created guide_name = %q, want fox kmsp", created.GuideName)
	}
	if created.ChannelKey != "" {
		t.Fatalf("created channel_key = %q, want empty", created.ChannelKey)
	}
	if !created.DynamicRule.Enabled {
		t.Fatal("created.DynamicRule.Enabled = false, want true")
	}
	if len(created.DynamicRule.GroupNames) != 1 || created.DynamicRule.GroupNames[0] != "US News" {
		t.Fatalf("created.DynamicRule.GroupNames = %#v, want [US News]", created.DynamicRule.GroupNames)
	}

	sources, err := store.ListSources(ctx, created.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources(created dynamic without seed item) error = %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("len(created dynamic sources) = %d, want 0", len(sources))
	}

	if _, err := store.CreateChannelFromItem(
		ctx,
		"",
		"",
		"",
		&channels.DynamicSourceRule{
			Enabled:     false,
			GroupName:   "US News",
			SearchQuery: "fox kmsp",
		},
		100,
	); err == nil || !strings.Contains(err.Error(), "item_key is required unless dynamic_rule.enabled is true") {
		t.Fatalf("CreateChannelFromItem(without seed item + disabled dynamic) error = %v, want item_key validation", err)
	}
}

func TestChannelDynamicRuleSearchRegexValidationRejectsInvalidPatterns(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:regex:channel:seed",
			ChannelKey: "name:regex channel seed",
			Name:       "Regex Channel Seed",
			Group:      "News",
			StreamURL:  "http://example.com/regex-channel-seed.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	if _, err := store.CreateChannelFromItem(
		ctx,
		"src:regex:channel:seed",
		"",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "News",
			SearchQuery: "([",
			SearchRegex: true,
		},
		100,
	); err == nil || !strings.Contains(strings.ToLower(err.Error()), "invalid regex pattern") {
		t.Fatalf("CreateChannelFromItem(invalid regex dynamic rule) error = %v, want invalid regex pattern validation error", err)
	}

	created, err := store.CreateChannelFromItem(
		ctx,
		"src:regex:channel:seed",
		"",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "News",
			SearchQuery: "([",
			SearchRegex: false,
		},
		100,
	)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(compatibility seed) error = %v", err)
	}

	if _, err := store.UpdateChannel(
		ctx,
		created.ChannelID,
		nil,
		nil,
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "News",
			SearchQuery: "([",
			SearchRegex: true,
		},
	); err == nil || !strings.Contains(strings.ToLower(err.Error()), "invalid regex pattern") {
		t.Fatalf("UpdateChannel(enable invalid regex dynamic rule) error = %v, want invalid regex pattern validation error", err)
	}
}

func TestSyncDynamicSourcesAddRemoveRetain(t *testing.T) {
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
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "News Backup",
			Group:      "News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
		{
			ItemKey:    "src:news:alt",
			ChannelKey: "tvg:news",
			Name:       "News Alt",
			Group:      "News",
			StreamURL:  "http://example.com/news-alt.ts",
		},
		{
			ItemKey:    "src:sports:manual",
			ChannelKey: "tvg:sports",
			Name:       "Sports Manual",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-manual.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, "src:news:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}

	if _, err := store.AddSource(ctx, channel.ChannelID, "src:sports:manual", true); err != nil {
		t.Fatalf("AddSource(manual) error = %v", err)
	}

	firstSync, err := store.SyncDynamicSources(ctx, channel.ChannelID, []string{"src:news:backup"})
	if err != nil {
		t.Fatalf("SyncDynamicSources(first) error = %v", err)
	}
	if firstSync.Added != 1 || firstSync.Removed != 0 || firstSync.Retained != 0 {
		t.Fatalf("firstSync = %+v, want added=1 removed=0 retained=0", firstSync)
	}

	secondSync, err := store.SyncDynamicSources(ctx, channel.ChannelID, []string{"src:sports:manual", "src:news:alt"})
	if err != nil {
		t.Fatalf("SyncDynamicSources(second) error = %v", err)
	}
	if secondSync.Added != 1 || secondSync.Removed != 1 || secondSync.Retained != 1 {
		t.Fatalf("secondSync = %+v, want added=1 removed=1 retained=1", secondSync)
	}

	sources, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 3 {
		t.Fatalf("len(ListSources) = %d, want 3", len(sources))
	}

	sourceByItem := make(map[string]channels.Source, len(sources))
	for idx, src := range sources {
		sourceByItem[src.ItemKey] = src
		if src.PriorityIndex != idx {
			t.Fatalf("source %q priority_index = %d, want %d", src.ItemKey, src.PriorityIndex, idx)
		}
	}

	if _, ok := sourceByItem["src:news:backup"]; ok {
		t.Fatal("expected src:news:backup dynamic source to be removed")
	}
	if got := sourceByItem["src:news:primary"].AssociationType; got != "channel_key" {
		t.Fatalf("src:news:primary association_type = %q, want channel_key", got)
	}
	if got := sourceByItem["src:sports:manual"].AssociationType; got != "manual" {
		t.Fatalf("src:sports:manual association_type = %q, want manual", got)
	}
	if got := sourceByItem["src:news:alt"].AssociationType; got != "dynamic_query" {
		t.Fatalf("src:news:alt association_type = %q, want dynamic_query", got)
	}
}

func TestSyncDynamicSourcesPromotesMatchedChannelKeySourceToDynamicQuery(t *testing.T) {
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
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "News Backup",
			Group:      "News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, "src:news:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}

	sourcesBefore, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources(before) error = %v", err)
	}
	if len(sourcesBefore) != 1 {
		t.Fatalf("len(sourcesBefore) = %d, want 1", len(sourcesBefore))
	}
	if got := sourcesBefore[0].AssociationType; got != "channel_key" {
		t.Fatalf("before association_type = %q, want channel_key", got)
	}

	syncResult, err := store.SyncDynamicSources(ctx, channel.ChannelID, []string{"src:news:primary", "src:news:backup"})
	if err != nil {
		t.Fatalf("SyncDynamicSources() error = %v", err)
	}
	if syncResult.Added != 1 || syncResult.Removed != 0 || syncResult.Retained != 1 {
		t.Fatalf("syncResult = %+v, want added=1 removed=0 retained=1", syncResult)
	}

	sourcesAfter, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources(after) error = %v", err)
	}
	byItem := make(map[string]channels.Source, len(sourcesAfter))
	for _, src := range sourcesAfter {
		byItem[src.ItemKey] = src
	}
	if got := byItem["src:news:primary"].AssociationType; got != "dynamic_query" {
		t.Fatalf("src:news:primary association_type = %q, want dynamic_query", got)
	}
	if got := byItem["src:news:backup"].AssociationType; got != "dynamic_query" {
		t.Fatalf("src:news:backup association_type = %q, want dynamic_query", got)
	}
}

func TestSyncDynamicSourcesDoesNotMutateMatchedKeysInput(t *testing.T) {
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
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "News Backup",
			Group:      "News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, "src:news:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}

	matched := []string{" src:news:primary ", "src:news:backup", "src:news:backup"}
	before := append([]string(nil), matched...)
	if _, err := store.SyncDynamicSources(ctx, channel.ChannelID, matched); err != nil {
		t.Fatalf("SyncDynamicSources() error = %v", err)
	}
	if !reflect.DeepEqual(matched, before) {
		t.Fatalf("SyncDynamicSources() mutated matchedItemKeys input: got %#v want %#v", matched, before)
	}
}

func TestSyncDynamicSourcesByCatalogFilterPagedAddRemoveRetain(t *testing.T) {
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
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "News Backup",
			Group:      "News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
		{
			ItemKey:    "src:news:alt",
			ChannelKey: "tvg:news",
			Name:       "News Alt",
			Group:      "News",
			StreamURL:  "http://example.com/news-alt.ts",
		},
		{
			ItemKey:    "src:sports:manual",
			ChannelKey: "tvg:sports",
			Name:       "Sports Manual",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-manual.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, "src:news:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	if _, err := store.AddSource(ctx, channel.ChannelID, "src:sports:manual", true); err != nil {
		t.Fatalf("AddSource(manual) error = %v", err)
	}

	firstSync, firstMatched, err := store.SyncDynamicSourcesByCatalogFilter(
		ctx,
		channel.ChannelID,
		[]string{"News"},
		"backup | alt",
		false,
		1,
		0,
	)
	if err != nil {
		t.Fatalf("SyncDynamicSourcesByCatalogFilter(first) error = %v", err)
	}
	if firstMatched != 2 {
		t.Fatalf("first matched count = %d, want 2", firstMatched)
	}
	if firstSync.Added != 2 || firstSync.Removed != 0 || firstSync.Retained != 0 {
		t.Fatalf("firstSync = %+v, want added=2 removed=0 retained=0", firstSync)
	}

	secondSync, secondMatched, err := store.SyncDynamicSourcesByCatalogFilter(
		ctx,
		channel.ChannelID,
		[]string{"News"},
		"alt",
		false,
		1,
		0,
	)
	if err != nil {
		t.Fatalf("SyncDynamicSourcesByCatalogFilter(second) error = %v", err)
	}
	if secondMatched != 1 {
		t.Fatalf("second matched count = %d, want 1", secondMatched)
	}
	if secondSync.Added != 0 || secondSync.Removed != 1 || secondSync.Retained != 1 {
		t.Fatalf("secondSync = %+v, want added=0 removed=1 retained=1", secondSync)
	}

	sources, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	sourceByItem := make(map[string]channels.Source, len(sources))
	for idx, src := range sources {
		sourceByItem[src.ItemKey] = src
		if src.PriorityIndex != idx {
			t.Fatalf("source %q priority_index = %d, want %d", src.ItemKey, src.PriorityIndex, idx)
		}
	}

	if got := sourceByItem["src:news:primary"].AssociationType; got != "channel_key" {
		t.Fatalf("src:news:primary association_type = %q, want channel_key", got)
	}
	if got := sourceByItem["src:sports:manual"].AssociationType; got != "manual" {
		t.Fatalf("src:sports:manual association_type = %q, want manual", got)
	}
	if got := sourceByItem["src:news:alt"].AssociationType; got != "dynamic_query" {
		t.Fatalf("src:news:alt association_type = %q, want dynamic_query", got)
	}
	if _, ok := sourceByItem["src:news:backup"]; ok {
		t.Fatal("expected src:news:backup dynamic source to be removed")
	}
}

func TestSyncDynamicSourcesByCatalogFilterRejectsMatchesOverLimit(t *testing.T) {
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
			Name:       "FOX 9 KMSP Primary",
			Group:      "News",
			StreamURL:  "http://example.com/primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX KMSP Backup",
			Group:      "News",
			StreamURL:  "http://example.com/backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channel, err := store.CreateChannelFromItem(ctx, "src:news:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}

	_, _, err = store.SyncDynamicSourcesByCatalogFilter(
		ctx,
		channel.ChannelID,
		[]string{"News"},
		"fox kmsp",
		false,
		1,
		1,
	)
	if err == nil {
		t.Fatal("SyncDynamicSourcesByCatalogFilter() error = nil, want match-limit failure")
	}
	if !strings.Contains(err.Error(), "limit is 1") {
		t.Fatalf("SyncDynamicSourcesByCatalogFilter() error = %v, want match-limit message", err)
	}

	sources, err := store.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("len(sources) = %d, want 1", len(sources))
	}
	if sources[0].ItemKey != "src:news:primary" {
		t.Fatalf("sources[0].ItemKey = %q, want src:news:primary", sources[0].ItemKey)
	}
}

func TestListDuplicateSuggestions(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:a",
			ChannelKey: "tvg:news",
			Name:       "News A",
			Group:      "News",
			StreamURL:  "http://example.com/news-a.ts",
		},
		{
			ItemKey:    "src:news:b",
			ChannelKey: "tvg:news",
			Name:       "News B",
			Group:      "News",
			StreamURL:  "http://example.com/news-b.ts",
		},
		{
			ItemKey:    "src:sports:a",
			ChannelKey: "tvg:sports",
			Name:       "Sports A",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-a.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	groups, total, err := store.ListDuplicateSuggestions(ctx, 2, "", 50, 0)
	if err != nil {
		t.Fatalf("ListDuplicateSuggestions() error = %v", err)
	}
	if total != 1 {
		t.Fatalf("duplicate total = %d, want 1", total)
	}
	if len(groups) != 1 {
		t.Fatalf("len(ListDuplicateSuggestions) = %d, want 1", len(groups))
	}
	if groups[0].ChannelKey != "tvg:news" {
		t.Fatalf("group channel_key = %q, want tvg:news", groups[0].ChannelKey)
	}
	if groups[0].Count != 2 {
		t.Fatalf("group count = %d, want 2", groups[0].Count)
	}
	if len(groups[0].Items) != 2 {
		t.Fatalf("len(group items) = %d, want 2", len(groups[0].Items))
	}
	if !strings.HasPrefix(groups[0].Items[0].ItemKey, "src:news:") {
		t.Fatalf("first suggestion item_key = %q, want src:news:*", groups[0].Items[0].ItemKey)
	}
}

func TestListDuplicateSuggestionsCaseInsensitiveSearch(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:cnn:lower",
			ChannelKey: "tvg:cnn.us",
			Name:       "CNN Lower",
			Group:      "News",
			StreamURL:  "http://example.com/cnn-lower.ts",
			TVGID:      "cnn.us",
		},
		{
			ItemKey:    "src:cnn:upper",
			ChannelKey: "tvg:CNN.us",
			Name:       "CNN Upper",
			Group:      "News",
			StreamURL:  "http://example.com/cnn-upper.ts",
			TVGID:      "CNN.us",
		},
		{
			ItemKey:    "src:bbc:one",
			ChannelKey: "tvg:bbc.uk",
			Name:       "BBC One",
			Group:      "News",
			StreamURL:  "http://example.com/bbc-one.ts",
			TVGID:      "bbc.uk",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	groups, total, err := store.ListDuplicateSuggestions(ctx, 2, "CNN.US", 50, 0)
	if err != nil {
		t.Fatalf("ListDuplicateSuggestions(filter) error = %v", err)
	}
	if total != 1 {
		t.Fatalf("filtered total = %d, want 1", total)
	}
	if len(groups) != 1 {
		t.Fatalf("len(filtered groups) = %d, want 1", len(groups))
	}
	if groups[0].ChannelKey != "tvg:cnn.us" {
		t.Fatalf("group channel_key = %q, want tvg:cnn.us", groups[0].ChannelKey)
	}
	if groups[0].Count != 2 {
		t.Fatalf("group count = %d, want 2", groups[0].Count)
	}

	channelKeyMatched, channelKeyMatchedTotal, err := store.ListDuplicateSuggestions(ctx, 2, "tvg:cnn.us", 50, 0)
	if err != nil {
		t.Fatalf("ListDuplicateSuggestions(channel key filter) error = %v", err)
	}
	if channelKeyMatchedTotal != 1 {
		t.Fatalf("channel key matched total = %d, want 1", channelKeyMatchedTotal)
	}
	if len(channelKeyMatched) != 1 {
		t.Fatalf("len(channel key matched groups) = %d, want 1", len(channelKeyMatched))
	}

	misses, missesTotal, err := store.ListDuplicateSuggestions(ctx, 2, "fox.us", 50, 0)
	if err != nil {
		t.Fatalf("ListDuplicateSuggestions(miss filter) error = %v", err)
	}
	if missesTotal != 0 {
		t.Fatalf("misses total = %d, want 0", missesTotal)
	}
	if len(misses) != 0 {
		t.Fatalf("len(miss groups) = %d, want 0", len(misses))
	}
}

func TestListDuplicateSuggestionsPaged(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{ItemKey: "src:dup:alpha:1", ChannelKey: "tvg:alpha", Name: "Alpha One", Group: "News", StreamURL: "http://example.com/alpha-1.ts"},
		{ItemKey: "src:dup:alpha:2", ChannelKey: "tvg:alpha", Name: "Alpha Two", Group: "News", StreamURL: "http://example.com/alpha-2.ts"},
		{ItemKey: "src:dup:bravo:1", ChannelKey: "tvg:bravo", Name: "Bravo One", Group: "News", StreamURL: "http://example.com/bravo-1.ts"},
		{ItemKey: "src:dup:bravo:2", ChannelKey: "tvg:bravo", Name: "Bravo Two", Group: "News", StreamURL: "http://example.com/bravo-2.ts"},
		{ItemKey: "src:dup:charlie:1", ChannelKey: "tvg:charlie", Name: "Charlie One", Group: "News", StreamURL: "http://example.com/charlie-1.ts"},
		{ItemKey: "src:dup:charlie:2", ChannelKey: "tvg:charlie", Name: "Charlie Two", Group: "News", StreamURL: "http://example.com/charlie-2.ts"},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	groups, total, err := store.ListDuplicateSuggestions(ctx, 2, "", 2, 1)
	if err != nil {
		t.Fatalf("ListDuplicateSuggestions(paged) error = %v", err)
	}
	if total != 3 {
		t.Fatalf("paged total = %d, want 3", total)
	}
	if len(groups) != 2 {
		t.Fatalf("len(paged groups) = %d, want 2", len(groups))
	}
	if groups[0].ChannelKey != "tvg:bravo" || groups[1].ChannelKey != "tvg:charlie" {
		t.Fatalf("paged channel keys = [%q, %q], want [tvg:bravo, tvg:charlie]", groups[0].ChannelKey, groups[1].ChannelKey)
	}
	for _, group := range groups {
		if group.Count != 2 {
			t.Fatalf("group %q count = %d, want 2", group.ChannelKey, group.Count)
		}
		if len(group.Items) != 2 {
			t.Fatalf("group %q len(items) = %d, want 2", group.ChannelKey, len(group.Items))
		}
	}
}

func TestListDuplicateSuggestionsQueryPlanAvoidsTempBTreeForGrouping(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, generateDuplicateSuggestionBenchmarkItems(4000, 3)); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	planDetails, err := explainDuplicateSuggestionsGroupPlan(ctx, store, 2, "", 100, 0)
	if err != nil {
		t.Fatalf("explainDuplicateSuggestionsGroupPlan() error = %v", err)
	}
	if len(planDetails) == 0 {
		t.Fatal("expected query-plan rows for duplicate suggestions group query")
	}
	t.Logf("duplicate suggestions group query plan: %s", strings.Join(planDetails, " | "))
	if !planContains(planDetails, "idx_playlist_active_key_name_item") {
		t.Fatalf("plan details missing duplicate-group index usage: %q", strings.Join(planDetails, " | "))
	}
	if planContains(planDetails, "use temp b-tree for group by") {
		t.Fatalf("plan details unexpectedly include group-by temp b-tree: %q", strings.Join(planDetails, " | "))
	}
}

func TestListDuplicateSuggestionsTreatsWildcardSearchAsLiteral(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{ItemKey: "src:dup:percent:1", ChannelKey: "tvg:key%percent", Name: "Percent One", Group: "News", StreamURL: "http://example.com/percent-1.ts", TVGID: "dup-percent"},
		{ItemKey: "src:dup:percent:2", ChannelKey: "tvg:key%percent", Name: "Percent Two", Group: "News", StreamURL: "http://example.com/percent-2.ts", TVGID: "dup-percent"},
		{ItemKey: "src:dup:underscore:1", ChannelKey: "tvg:key-under", Name: "Underscore One", Group: "News", StreamURL: "http://example.com/underscore-1.ts", TVGID: "dup_under"},
		{ItemKey: "src:dup:underscore:2", ChannelKey: "tvg:key-under", Name: "Underscore Two", Group: "News", StreamURL: "http://example.com/underscore-2.ts", TVGID: "dup_under"},
		{ItemKey: "src:dup:plain:1", ChannelKey: "tvg:keyplain", Name: "Plain One", Group: "News", StreamURL: "http://example.com/plain-1.ts", TVGID: "dupplain"},
		{ItemKey: "src:dup:plain:2", ChannelKey: "tvg:keyplain", Name: "Plain Two", Group: "News", StreamURL: "http://example.com/plain-2.ts", TVGID: "dupplain"},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	percentGroups, percentTotal, err := store.ListDuplicateSuggestions(ctx, 2, "%", 10, 0)
	if err != nil {
		t.Fatalf("ListDuplicateSuggestions(search=%%) error = %v", err)
	}
	if percentTotal != 1 || len(percentGroups) != 1 {
		t.Fatalf("ListDuplicateSuggestions(search=%%) total/len = %d/%d, want 1/1", percentTotal, len(percentGroups))
	}
	if percentGroups[0].ChannelKey != "tvg:key%percent" {
		t.Fatalf("ListDuplicateSuggestions(search=%%) channel key = %q, want tvg:key%%percent", percentGroups[0].ChannelKey)
	}

	underscoreGroups, underscoreTotal, err := store.ListDuplicateSuggestions(ctx, 2, "_", 10, 0)
	if err != nil {
		t.Fatalf("ListDuplicateSuggestions(search=_) error = %v", err)
	}
	if underscoreTotal != 1 || len(underscoreGroups) != 1 {
		t.Fatalf("ListDuplicateSuggestions(search=_) total/len = %d/%d, want 1/1", underscoreTotal, len(underscoreGroups))
	}
	if underscoreGroups[0].ChannelKey != "tvg:key-under" {
		t.Fatalf("ListDuplicateSuggestions(search=_) channel key = %q, want tvg:key-under", underscoreGroups[0].ChannelKey)
	}
}

func BenchmarkListDuplicateSuggestionsPagedHotPath(b *testing.B) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, generateDuplicateSuggestionBenchmarkItems(5000, 3)); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		groups, total, err := store.ListDuplicateSuggestions(ctx, 2, "", 100, 0)
		if err != nil {
			b.Fatalf("ListDuplicateSuggestions() error = %v", err)
		}
		if total <= 0 || len(groups) == 0 {
			b.Fatalf("duplicate suggestion benchmark returned empty results: total=%d groups=%d", total, len(groups))
		}
	}
}

func explainDuplicateSuggestionsGroupPlan(ctx context.Context, store *Store, minItems int, searchQuery string, limit, offset int) ([]string, error) {
	searchFilterSQL, searchFilterArgs := duplicateSuggestionsSearchFilter(strings.TrimSpace(searchQuery))
	args := append([]any{}, searchFilterArgs...)
	args = append(args, minItems, limit, offset)

	rows, err := store.db.QueryContext(
		ctx,
		`
			EXPLAIN QUERY PLAN
			SELECT channel_key, COUNT(*) AS item_count
			FROM playlist_items
			WHERE active = 1
			  AND channel_key <> ''
		`+searchFilterSQL+`
			GROUP BY channel_key
			HAVING COUNT(*) >= ?
			ORDER BY channel_key ASC
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

func generateDuplicateSuggestionBenchmarkItems(groupCount, itemsPerGroup int) []playlist.Item {
	if groupCount < 1 {
		groupCount = 1
	}
	if itemsPerGroup < 2 {
		itemsPerGroup = 2
	}

	items := make([]playlist.Item, 0, groupCount*itemsPerGroup)
	for groupIdx := 0; groupIdx < groupCount; groupIdx++ {
		channelKey := fmt.Sprintf("tvg:dup:%05d", groupIdx)
		tvgID := fmt.Sprintf("dup-%05d", groupIdx)
		for itemIdx := 0; itemIdx < itemsPerGroup; itemIdx++ {
			items = append(items, playlist.Item{
				ItemKey:    fmt.Sprintf("src:dup:%05d:%02d", groupIdx, itemIdx),
				ChannelKey: channelKey,
				Name:       fmt.Sprintf("Duplicate Channel %05d Variant %02d", groupIdx, itemIdx),
				Group:      fmt.Sprintf("Group %02d", groupIdx%25),
				StreamURL:  fmt.Sprintf("http://example.com/dup/%05d/%02d.ts", groupIdx, itemIdx),
				TVGID:      tvgID,
			})
		}
	}
	return items
}

func TestMarkSourceFailureAndSuccess(t *testing.T) {
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

	sources, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("len(ListSources) = %d, want 1", len(sources))
	}

	sourceID := sources[0].SourceID
	firstFailAt := time.Unix(1_700_000_000, 0).UTC()
	if err := store.MarkSourceFailure(ctx, sourceID, "connect failed", firstFailAt); err != nil {
		t.Fatalf("MarkSourceFailure(first) error = %v", err)
	}

	failed, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() after fail error = %v", err)
	}
	if failed[0].FailCount != 1 {
		t.Fatalf("FailCount after first fail = %d, want 1", failed[0].FailCount)
	}
	if failed[0].CooldownUntil != firstFailAt.Unix()+10 {
		t.Fatalf("CooldownUntil after first fail = %d, want %d", failed[0].CooldownUntil, firstFailAt.Unix()+10)
	}
	if failed[0].LastFailReason != "connect failed" {
		t.Fatalf("LastFailReason = %q, want connect failed", failed[0].LastFailReason)
	}

	secondFailAt := firstFailAt.Add(5 * time.Minute)
	if err := store.MarkSourceFailure(ctx, sourceID, "timeout", secondFailAt); err != nil {
		t.Fatalf("MarkSourceFailure(second) error = %v", err)
	}

	failed, err = store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() after second fail error = %v", err)
	}
	if failed[0].FailCount != 2 {
		t.Fatalf("FailCount after second fail = %d, want 2", failed[0].FailCount)
	}
	if failed[0].CooldownUntil != secondFailAt.Unix()+30 {
		t.Fatalf("CooldownUntil after second fail = %d, want %d", failed[0].CooldownUntil, secondFailAt.Unix()+30)
	}

	successAt := secondFailAt.Add(10 * time.Minute)
	if err := store.MarkSourceSuccess(ctx, sourceID, successAt); err != nil {
		t.Fatalf("MarkSourceSuccess() error = %v", err)
	}

	recovered, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() after success error = %v", err)
	}
	if recovered[0].FailCount != 0 {
		t.Fatalf("FailCount after success = %d, want 0", recovered[0].FailCount)
	}
	if recovered[0].CooldownUntil != 0 {
		t.Fatalf("CooldownUntil after success = %d, want 0", recovered[0].CooldownUntil)
	}
	if recovered[0].SuccessCount != 1 {
		t.Fatalf("SuccessCount after success = %d, want 1", recovered[0].SuccessCount)
	}
	if recovered[0].LastOKAt != successAt.Unix() {
		t.Fatalf("LastOKAt after success = %d, want %d", recovered[0].LastOKAt, successAt.Unix())
	}
	if recovered[0].LastFailAt != 0 {
		t.Fatalf("LastFailAt after success = %d, want 0", recovered[0].LastFailAt)
	}
	if recovered[0].LastFailReason != "" {
		t.Fatalf("LastFailReason after success = %q, want empty", recovered[0].LastFailReason)
	}

	thirdFailAt := successAt.Add(2 * time.Minute)
	if err := store.MarkSourceFailure(ctx, sourceID, "transient reset", thirdFailAt); err != nil {
		t.Fatalf("MarkSourceFailure(third) error = %v", err)
	}

	postRecoveryFail, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() after post-recovery fail error = %v", err)
	}
	if postRecoveryFail[0].FailCount != 1 {
		t.Fatalf("FailCount after post-recovery fail = %d, want 1", postRecoveryFail[0].FailCount)
	}
	if postRecoveryFail[0].CooldownUntil != thirdFailAt.Unix()+10 {
		t.Fatalf("CooldownUntil after post-recovery fail = %d, want %d", postRecoveryFail[0].CooldownUntil, thirdFailAt.Unix()+10)
	}
	if postRecoveryFail[0].LastFailReason != "transient reset" {
		t.Fatalf("LastFailReason after post-recovery fail = %q, want transient reset", postRecoveryFail[0].LastFailReason)
	}
}

func TestMarkSourceFailureReturnsErrSourceNotFoundForMissingSource(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	err = store.MarkSourceFailure(ctx, 99999, "connect failed", time.Now().UTC())
	if !errors.Is(err, channels.ErrSourceNotFound) {
		t.Fatalf("MarkSourceFailure(missing) error = %v, want ErrSourceNotFound", err)
	}
}

func TestMarkSourceSuccessStaleDoesNotClearNewerFailure(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{ItemKey: "src:stale:primary", ChannelKey: "tvg:stale", Name: "Stale", Group: "Test", StreamURL: "http://example.com/stale.ts"},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}
	channel, err := store.CreateChannelFromItem(ctx, "src:stale:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	sources, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	sourceID := sources[0].SourceID

	// Persist a newer failure at t=200.
	failAt := time.Unix(200, 0).UTC()
	if err := store.MarkSourceFailure(ctx, sourceID, "connection reset", failAt); err != nil {
		t.Fatalf("MarkSourceFailure() error = %v", err)
	}

	// Persist a stale success at t=100 (before the failure).
	staleSuccessAt := time.Unix(100, 0).UTC()
	if err := store.MarkSourceSuccess(ctx, sourceID, staleSuccessAt); err != nil {
		t.Fatalf("MarkSourceSuccess(stale) error = %v, want nil (no-op)", err)
	}

	// Verify failure state was preserved.
	after, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	src := after[0]
	if src.FailCount != 1 {
		t.Fatalf("FailCount = %d, want 1 (stale success should not clear)", src.FailCount)
	}
	if src.LastFailAt != failAt.Unix() {
		t.Fatalf("LastFailAt = %d, want %d", src.LastFailAt, failAt.Unix())
	}
	if src.CooldownUntil == 0 {
		t.Fatal("CooldownUntil = 0, want non-zero (stale success should not clear cooldown)")
	}
	if src.LastFailReason != "connection reset" {
		t.Fatalf("LastFailReason = %q, want %q", src.LastFailReason, "connection reset")
	}
}

func TestMarkSourceFailureStaleDoesNotOverwriteNewerSuccess(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{ItemKey: "src:stale-fail:primary", ChannelKey: "tvg:stalefail", Name: "StaleFail", Group: "Test", StreamURL: "http://example.com/stalefail.ts"},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}
	channel, err := store.CreateChannelFromItem(ctx, "src:stale-fail:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	sources, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	sourceID := sources[0].SourceID

	// Persist a newer success at t=200.
	successAt := time.Unix(200, 0).UTC()
	if err := store.MarkSourceSuccess(ctx, sourceID, successAt); err != nil {
		t.Fatalf("MarkSourceSuccess() error = %v", err)
	}

	// Persist a stale failure at t=100 (before the success).
	staleFailAt := time.Unix(100, 0).UTC()
	if err := store.MarkSourceFailure(ctx, sourceID, "stale_failure", staleFailAt); err != nil {
		t.Fatalf("MarkSourceFailure(stale) error = %v, want nil (no-op)", err)
	}

	// Verify success state was preserved and failure was not applied.
	after, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	src := after[0]
	if src.FailCount != 0 {
		t.Fatalf("FailCount = %d, want 0 (stale failure should not increment)", src.FailCount)
	}
	if src.CooldownUntil != 0 {
		t.Fatalf("CooldownUntil = %d, want 0 (stale failure should not set cooldown)", src.CooldownUntil)
	}
	if src.LastOKAt != successAt.Unix() {
		t.Fatalf("LastOKAt = %d, want %d", src.LastOKAt, successAt.Unix())
	}
	if src.SuccessCount != 1 {
		t.Fatalf("SuccessCount = %d, want 1", src.SuccessCount)
	}
}

func TestUpdateSourceProfilePersistsProbeFields(t *testing.T) {
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

	sources, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("len(ListSources) = %d, want 1", len(sources))
	}

	sourceID := sources[0].SourceID
	failAt := time.Unix(1_700_300_000, 0).UTC()
	if err := store.MarkSourceFailure(ctx, sourceID, "startup timeout", failAt); err != nil {
		t.Fatalf("MarkSourceFailure() error = %v", err)
	}

	probeAt := time.Unix(1_700_300_123, 0).UTC()
	if err := store.UpdateSourceProfile(ctx, sourceID, channels.SourceProfileUpdate{
		LastProbeAt: probeAt,
		Width:       1920,
		Height:      1080,
		FPS:         59.94,
		VideoCodec:  "h264",
		AudioCodec:  "aac",
		BitrateBPS:  5_200_000,
	}); err != nil {
		t.Fatalf("UpdateSourceProfile() error = %v", err)
	}

	updated, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() after profile update error = %v", err)
	}
	if got, want := updated[0].LastProbeAt, probeAt.Unix(); got != want {
		t.Fatalf("LastProbeAt = %d, want %d", got, want)
	}
	if got, want := updated[0].ProfileWidth, 1920; got != want {
		t.Fatalf("ProfileWidth = %d, want %d", got, want)
	}
	if got, want := updated[0].ProfileHeight, 1080; got != want {
		t.Fatalf("ProfileHeight = %d, want %d", got, want)
	}
	if got, want := updated[0].ProfileFPS, 59.94; got != want {
		t.Fatalf("ProfileFPS = %f, want %f", got, want)
	}
	if got, want := updated[0].ProfileVideoCodec, "h264"; got != want {
		t.Fatalf("ProfileVideoCodec = %q, want %q", got, want)
	}
	if got, want := updated[0].ProfileAudioCodec, "aac"; got != want {
		t.Fatalf("ProfileAudioCodec = %q, want %q", got, want)
	}
	if got, want := updated[0].ProfileBitrateBPS, int64(5_200_000); got != want {
		t.Fatalf("ProfileBitrateBPS = %d, want %d", got, want)
	}
	if got, want := updated[0].FailCount, 1; got != want {
		t.Fatalf("FailCount = %d, want %d (profile updates must not touch health counters)", got, want)
	}
	if got, want := updated[0].LastFailReason, "startup timeout"; got != want {
		t.Fatalf("LastFailReason = %q, want %q (profile updates must not touch health reason)", got, want)
	}

	if err := store.UpdateSourceProfile(ctx, sourceID, channels.SourceProfileUpdate{}); err != nil {
		t.Fatalf("UpdateSourceProfile(clear) error = %v", err)
	}
	cleared, err := store.ListSources(ctx, channel.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() after clear profile update error = %v", err)
	}
	if cleared[0].LastProbeAt != 0 ||
		cleared[0].ProfileWidth != 0 ||
		cleared[0].ProfileHeight != 0 ||
		cleared[0].ProfileFPS != 0 ||
		cleared[0].ProfileVideoCodec != "" ||
		cleared[0].ProfileAudioCodec != "" ||
		cleared[0].ProfileBitrateBPS != 0 {
		t.Fatalf("profile fields after clear = %+v, want zero values", cleared[0])
	}

	if err := store.UpdateSourceProfile(ctx, 99999, channels.SourceProfileUpdate{Width: 1}); !errors.Is(err, channels.ErrSourceNotFound) {
		t.Fatalf("UpdateSourceProfile(missing) error = %v, want ErrSourceNotFound", err)
	}
}

// TestUpdateSourceProfileRejectsStaleProbeOverwrite verifies that the
// monotonic WHERE guard on last_probe_at silently rejects out-of-order
// profile writes. An older LastProbeAt payload must not overwrite a newer
// persisted profile snapshot.
func TestUpdateSourceProfileRejectsStaleProbeOverwrite(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{{
		ItemKey:    "src:stale:primary",
		ChannelKey: "tvg:stale",
		Name:       "Stale Test",
		Group:      "Test",
		StreamURL:  "http://example.com/stale.ts",
	}}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}
	ch, err := store.CreateChannelFromItem(ctx, "src:stale:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	sources, err := store.ListSources(ctx, ch.ChannelID, true)
	if err != nil || len(sources) != 1 {
		t.Fatalf("ListSources() error = %v, len = %d", err, len(sources))
	}
	sourceID := sources[0].SourceID

	// Persist newer profile first (T2).
	newerTime := time.Unix(1_700_500_200, 0).UTC()
	if err := store.UpdateSourceProfile(ctx, sourceID, channels.SourceProfileUpdate{
		LastProbeAt: newerTime,
		Width:       1920,
		Height:      1080,
		FPS:         30.0,
		VideoCodec:  "h264",
		AudioCodec:  "aac",
		BitrateBPS:  5_000_000,
	}); err != nil {
		t.Fatalf("UpdateSourceProfile(newer T2) error = %v", err)
	}

	// Attempt stale overwrite (T1 < T2).
	olderTime := time.Unix(1_700_500_100, 0).UTC()
	err = store.UpdateSourceProfile(ctx, sourceID, channels.SourceProfileUpdate{
		LastProbeAt: olderTime,
		Width:       1280,
		Height:      720,
		FPS:         25.0,
		VideoCodec:  "hevc",
		AudioCodec:  "mp3",
		BitrateBPS:  2_000_000,
	})
	// Monotonic guard silently rejects stale writes (no error returned).
	if err != nil {
		t.Fatalf("UpdateSourceProfile(older T1) error = %v", err)
	}

	updated, err := store.ListSources(ctx, ch.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() after stale write error = %v", err)
	}

	// Newer T2 values survive the stale T1 overwrite attempt.
	if got := updated[0].LastProbeAt; got != newerTime.Unix() {
		t.Fatalf("LastProbeAt = %d, want %d (newer profile must survive stale overwrite)", got, newerTime.Unix())
	}
	if got := updated[0].ProfileWidth; got != 1920 {
		t.Fatalf("ProfileWidth = %d, want 1920 (newer profile must survive stale overwrite)", got)
	}
	if got := updated[0].ProfileVideoCodec; got != "h264" {
		t.Fatalf("ProfileVideoCodec = %q, want h264 (newer profile must survive stale overwrite)", got)
	}
}

// TestUpdateSourceProfileSameTimestampLastWriterWins documents same-timestamp
// update behavior: the last writer wins when LastProbeAt is equal.
func TestUpdateSourceProfileSameTimestampLastWriterWins(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{{
		ItemKey:    "src:samets:primary",
		ChannelKey: "tvg:samets",
		Name:       "Same TS",
		Group:      "Test",
		StreamURL:  "http://example.com/samets.ts",
	}}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}
	ch, err := store.CreateChannelFromItem(ctx, "src:samets:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	sources, err := store.ListSources(ctx, ch.ChannelID, true)
	if err != nil || len(sources) != 1 {
		t.Fatalf("ListSources() error = %v, len = %d", err, len(sources))
	}
	sourceID := sources[0].SourceID

	probeAt := time.Unix(1_700_600_000, 0).UTC()

	if err := store.UpdateSourceProfile(ctx, sourceID, channels.SourceProfileUpdate{
		LastProbeAt: probeAt,
		Width:       1920,
		Height:      1080,
		VideoCodec:  "h264",
		AudioCodec:  "aac",
	}); err != nil {
		t.Fatalf("UpdateSourceProfile(first) error = %v", err)
	}

	// Same-timestamp update with different payload should succeed and overwrite.
	if err := store.UpdateSourceProfile(ctx, sourceID, channels.SourceProfileUpdate{
		LastProbeAt: probeAt,
		Width:       1280,
		Height:      720,
		VideoCodec:  "hevc",
		AudioCodec:  "mp3",
	}); err != nil {
		t.Fatalf("UpdateSourceProfile(same-ts) error = %v", err)
	}

	updated, err := store.ListSources(ctx, ch.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if got, want := updated[0].ProfileWidth, 1280; got != want {
		t.Fatalf("ProfileWidth = %d, want %d (same-ts last-writer-wins)", got, want)
	}
	if got, want := updated[0].ProfileVideoCodec, "hevc"; got != want {
		t.Fatalf("ProfileVideoCodec = %q, want %q (same-ts last-writer-wins)", got, want)
	}
}

// TestUpdateSourceProfileMissingSourceWithNonZeroProbeReturnsNotFound verifies
// that UpdateSourceProfile returns ErrSourceNotFound for a missing source even
// when LastProbeAt is non-zero (the monotonic guard path). This ensures the
// stale-write no-op does not mask genuine source deletion/drift.
func TestUpdateSourceProfileMissingSourceWithNonZeroProbeReturnsNotFound(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	// Non-existent source ID with non-zero LastProbeAt.
	err = store.UpdateSourceProfile(ctx, 99999, channels.SourceProfileUpdate{
		LastProbeAt: time.Unix(1_700_500_200, 0).UTC(),
		Width:       1920,
		Height:      1080,
		VideoCodec:  "h264",
		AudioCodec:  "aac",
	})
	if !errors.Is(err, channels.ErrSourceNotFound) {
		t.Fatalf("UpdateSourceProfile(missing source, non-zero probe) error = %v, want ErrSourceNotFound", err)
	}
}

// TestUpdateSourceProfileExistenceCheckPropagatesDBError verifies that a
// non-ErrNoRows failure during the source existence check (e.g. context
// cancellation) is propagated as a wrapped error, not silently swallowed
// as ErrSourceNotFound.
func TestUpdateSourceProfileExistenceCheckPropagatesDBError(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{{
		ItemKey:    "src:dbfail:primary",
		ChannelKey: "tvg:dbfail",
		Name:       "DB Fail Test",
		Group:      "Test",
		StreamURL:  "http://example.com/dbfail.ts",
	}}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}
	ch, err := store.CreateChannelFromItem(ctx, "src:dbfail:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem() error = %v", err)
	}
	sources, err := store.ListSources(ctx, ch.ChannelID, true)
	if err != nil || len(sources) != 1 {
		t.Fatalf("ListSources() error = %v, len = %d", err, len(sources))
	}
	sourceID := sources[0].SourceID

	// Persist newer profile so the stale write below triggers rows == 0.
	newerTime := time.Unix(1_700_600_200, 0).UTC()
	if err := store.UpdateSourceProfile(ctx, sourceID, channels.SourceProfileUpdate{
		LastProbeAt: newerTime,
		Width:       1920,
		Height:      1080,
		VideoCodec:  "h264",
		AudioCodec:  "aac",
	}); err != nil {
		t.Fatalf("UpdateSourceProfile(newer) error = %v", err)
	}

	// Install test hook: cancel the context after the UPDATE returns rows == 0
	// but before the existence-check SELECT, so the SELECT fails with
	// context.Canceled instead of sql.ErrNoRows.
	staleCtx, cancel := context.WithCancel(ctx)
	testBeforeProfileExistenceCheck = func() {
		cancel()
	}
	t.Cleanup(func() { testBeforeProfileExistenceCheck = nil })

	// Attempt stale write (T1 < T2): UPDATE succeeds with rows == 0,
	// hook cancels context, existence check fails with context.Canceled.
	olderTime := time.Unix(1_700_600_100, 0).UTC()
	err = store.UpdateSourceProfile(staleCtx, sourceID, channels.SourceProfileUpdate{
		LastProbeAt: olderTime,
		Width:       1280,
		Height:      720,
		VideoCodec:  "hevc",
		AudioCodec:  "mp3",
	})

	if err == nil {
		t.Fatal("expected error from existence check, got nil")
	}
	if errors.Is(err, channels.ErrSourceNotFound) {
		t.Fatal("context error in existence check was misclassified as ErrSourceNotFound")
	}
	if !strings.Contains(err.Error(), "source existence check") {
		t.Fatalf("expected wrapped existence check error, got: %v", err)
	}
}

func TestClearSourceHealth(t *testing.T) {
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
		{
			ItemKey:    "src:sports:primary",
			ChannelKey: "tvg:sports",
			Name:       "Sports",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelOne, err := store.CreateChannelFromItem(ctx, "src:news:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(news) error = %v", err)
	}
	channelTwo, err := store.CreateChannelFromItem(ctx, "src:sports:primary", "", "", nil, 100)
	if err != nil {
		t.Fatalf("CreateChannelFromItem(sports) error = %v", err)
	}

	sourcesOne, err := store.ListSources(ctx, channelOne.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources(channel one) error = %v", err)
	}
	sourcesTwo, err := store.ListSources(ctx, channelTwo.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources(channel two) error = %v", err)
	}
	sourceOneID := sourcesOne[0].SourceID
	sourceTwoID := sourcesTwo[0].SourceID

	failAt := time.Unix(1_700_100_000, 0).UTC()
	if err := store.MarkSourceFailure(ctx, sourceOneID, "upstream timeout", failAt); err != nil {
		t.Fatalf("MarkSourceFailure(channel one) error = %v", err)
	}
	okAt := failAt.Add(5 * time.Minute)
	if err := store.MarkSourceSuccess(ctx, sourceTwoID, okAt); err != nil {
		t.Fatalf("MarkSourceSuccess(channel two) error = %v", err)
	}

	cleared, err := store.ClearSourceHealth(ctx, channelOne.ChannelID)
	if err != nil {
		t.Fatalf("ClearSourceHealth(channel one) error = %v", err)
	}
	if cleared != 1 {
		t.Fatalf("ClearSourceHealth(channel one) rows = %d, want 1", cleared)
	}

	afterOne, err := store.ListSources(ctx, channelOne.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources(channel one after clear) error = %v", err)
	}
	if afterOne[0].SuccessCount != 0 || afterOne[0].FailCount != 0 {
		t.Fatalf("channel one counts after clear = ok:%d fail:%d, want ok:0 fail:0", afterOne[0].SuccessCount, afterOne[0].FailCount)
	}
	if afterOne[0].LastOKAt != 0 || afterOne[0].LastFailAt != 0 {
		t.Fatalf("channel one timestamps after clear = ok:%d fail:%d, want ok:0 fail:0", afterOne[0].LastOKAt, afterOne[0].LastFailAt)
	}
	if afterOne[0].CooldownUntil != 0 {
		t.Fatalf("channel one cooldown after clear = %d, want 0", afterOne[0].CooldownUntil)
	}
	if afterOne[0].LastFailReason != "" {
		t.Fatalf("channel one last fail reason after clear = %q, want empty", afterOne[0].LastFailReason)
	}

	afterTwo, err := store.ListSources(ctx, channelTwo.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources(channel two after channel clear) error = %v", err)
	}
	if afterTwo[0].SuccessCount != 1 {
		t.Fatalf("channel two SuccessCount after channel-one clear = %d, want 1", afterTwo[0].SuccessCount)
	}
	if afterTwo[0].LastOKAt != okAt.Unix() {
		t.Fatalf("channel two LastOKAt after channel-one clear = %d, want %d", afterTwo[0].LastOKAt, okAt.Unix())
	}

	clearedAll, err := store.ClearAllSourceHealth(ctx)
	if err != nil {
		t.Fatalf("ClearAllSourceHealth() error = %v", err)
	}
	if clearedAll < 1 {
		t.Fatalf("ClearAllSourceHealth() rows = %d, want at least 1", clearedAll)
	}

	verifyOne, err := store.ListSources(ctx, channelOne.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources(channel one after clear all) error = %v", err)
	}
	verifyTwo, err := store.ListSources(ctx, channelTwo.ChannelID, true)
	if err != nil {
		t.Fatalf("ListSources(channel two after clear all) error = %v", err)
	}
	for _, src := range []channels.Source{verifyOne[0], verifyTwo[0]} {
		if src.SuccessCount != 0 || src.FailCount != 0 {
			t.Fatalf("source %d counts after clear all = ok:%d fail:%d, want ok:0 fail:0", src.SourceID, src.SuccessCount, src.FailCount)
		}
		if src.LastOKAt != 0 || src.LastFailAt != 0 || src.CooldownUntil != 0 {
			t.Fatalf("source %d timestamps after clear all = ok:%d fail:%d cooldown:%d, want 0", src.SourceID, src.LastOKAt, src.LastFailAt, src.CooldownUntil)
		}
		if src.LastFailReason != "" {
			t.Fatalf("source %d last fail reason after clear all = %q, want empty", src.SourceID, src.LastFailReason)
		}
	}

	_, err = store.ClearSourceHealth(ctx, 999_999)
	if !errors.Is(err, channels.ErrChannelNotFound) {
		t.Fatalf("ClearSourceHealth(missing channel) error = %v, want ErrChannelNotFound", err)
	}
}
