package reconcile_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/reconcile"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
)

func TestReconcileAddsMissingSourcesByChannelKey(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
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
			ItemKey:    "src:sports:primary",
			ChannelKey: "tvg:sports",
			Name:       "Sports Primary",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelSvc := channels.NewService(store)
	created, err := channelSvc.Create(ctx, "src:news:primary", "News", "", nil)
	if err != nil {
		t.Fatalf("Create(news channel) error = %v", err)
	}

	reconciler, err := reconcile.New(store, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	progressCalls := 0
	result, err := reconciler.Reconcile(ctx, func(cur, max int) error {
		progressCalls++
		if max != 1 {
			t.Fatalf("progress max = %d, want 1", max)
		}
		if cur != 1 {
			t.Fatalf("progress cur = %d, want 1", cur)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if progressCalls != 1 {
		t.Fatalf("progress calls = %d, want 1", progressCalls)
	}
	if result.SourcesAdded != 1 {
		t.Fatalf("SourcesAdded = %d, want 1", result.SourcesAdded)
	}

	sources, err := channelSvc.ListSources(ctx, created.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("len(sources) = %d, want 2", len(sources))
	}

	// Second run should be idempotent.
	second, err := reconciler.Reconcile(ctx, nil)
	if err != nil {
		t.Fatalf("Reconcile(second) error = %v", err)
	}
	if second.SourcesAdded != 0 {
		t.Fatalf("second SourcesAdded = %d, want 0", second.SourcesAdded)
	}
}

func TestReconcileDynamicChannelSyncsCatalogFilterMatches(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP Primary",
			Group:      "US News",
			StreamURL:  "http://example.com/primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX KMSP Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/backup.ts",
		},
		{
			ItemKey:    "src:sports:primary",
			ChannelKey: "tvg:sports",
			Name:       "Sports Primary",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}

	channelSvc := channels.NewService(store)
	created, err := channelSvc.Create(
		ctx,
		"src:news:primary",
		"News Dynamic",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: "fox kmsp",
		},
	)
	if err != nil {
		t.Fatalf("Create(dynamic channel) error = %v", err)
	}

	reconciler, err := reconcile.New(store, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first, err := reconciler.Reconcile(ctx, nil)
	if err != nil {
		t.Fatalf("Reconcile(first) error = %v", err)
	}
	if first.DynamicChannelsProcessed != 1 {
		t.Fatalf("first.DynamicChannelsProcessed = %d, want 1", first.DynamicChannelsProcessed)
	}
	if first.DynamicSourcesAdded != 1 {
		t.Fatalf("first.DynamicSourcesAdded = %d, want 1", first.DynamicSourcesAdded)
	}
	if first.DynamicSourcesRemoved != 0 {
		t.Fatalf("first.DynamicSourcesRemoved = %d, want 0", first.DynamicSourcesRemoved)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:newfeed",
			ChannelKey: "tvg:news",
			Name:       "FOX KMSP New Feed",
			Group:      "US News",
			StreamURL:  "http://example.com/new-feed.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(refresh) error = %v", err)
	}

	second, err := reconciler.Reconcile(ctx, nil)
	if err != nil {
		t.Fatalf("Reconcile(second) error = %v", err)
	}
	if second.DynamicChannelsProcessed != 1 {
		t.Fatalf("second.DynamicChannelsProcessed = %d, want 1", second.DynamicChannelsProcessed)
	}
	if second.DynamicSourcesAdded != 1 {
		t.Fatalf("second.DynamicSourcesAdded = %d, want 1", second.DynamicSourcesAdded)
	}
	if second.DynamicSourcesRemoved != 2 {
		t.Fatalf("second.DynamicSourcesRemoved = %d, want 2", second.DynamicSourcesRemoved)
	}

	sources, err := channelSvc.ListSources(ctx, created.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}

	foundByItem := make(map[string]channels.Source, len(sources))
	for _, src := range sources {
		foundByItem[src.ItemKey] = src
	}

	if _, ok := foundByItem["src:news:backup"]; ok {
		t.Fatal("expected old dynamic source src:news:backup to be removed")
	}
	if got := foundByItem["src:news:newfeed"].AssociationType; got != "dynamic_query" {
		t.Fatalf("src:news:newfeed association_type = %q, want dynamic_query", got)
	}
	if _, ok := foundByItem["src:news:primary"]; ok {
		t.Fatal("expected old dynamic source src:news:primary to be removed after refresh")
	}
}

func TestReconcileDynamicChannelLogsSyncDecision(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP Primary",
			Group:      "US News",
			StreamURL:  "http://example.com/primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX KMSP Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelSvc := channels.NewService(store)
	if _, err := channelSvc.Create(
		ctx,
		"src:news:primary",
		"News Dynamic",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: "fox kmsp",
		},
	); err != nil {
		t.Fatalf("Create(dynamic channel) error = %v", err)
	}

	reconciler, err := reconcile.New(store, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var logs bytes.Buffer
	reconciler.SetLogger(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	if _, err := reconciler.Reconcile(ctx, nil); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	logText := logs.String()
	for _, want := range []string{
		"dynamic reconcile synced",
		"channel_id=1",
		"groups=\"[US News]\"",
		"query=\"fox kmsp\"",
		"matched_items=2",
		"added_sources=1",
		"removed_sources=0",
		"retained_sources=1",
	} {
		if !strings.Contains(logText, want) {
			t.Fatalf("logs missing %q: %s", want, logText)
		}
	}
}

func TestReconcileDynamicChannelSupportsSearchWithoutGroupFilter(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:sports:fox",
			ChannelKey: "tvg:sports",
			Name:       "FOX Sports Midwest",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-fox.ts",
		},
		{
			ItemKey:    "src:movies:other",
			ChannelKey: "tvg:movies",
			Name:       "Movie Time",
			Group:      "Movies",
			StreamURL:  "http://example.com/movies.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelSvc := channels.NewService(store)
	created, err := channelSvc.Create(
		ctx,
		"src:news:primary",
		"FOX Dynamic",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "",
			SearchQuery: "fox",
		},
	)
	if err != nil {
		t.Fatalf("Create(dynamic channel) error = %v", err)
	}

	reconciler, err := reconcile.New(store, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := reconciler.Reconcile(ctx, nil)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.DynamicChannelsProcessed != 1 {
		t.Fatalf("result.DynamicChannelsProcessed = %d, want 1", result.DynamicChannelsProcessed)
	}
	if result.DynamicSourcesAdded != 1 {
		t.Fatalf("result.DynamicSourcesAdded = %d, want 1", result.DynamicSourcesAdded)
	}
	if result.DynamicSourcesRemoved != 0 {
		t.Fatalf("result.DynamicSourcesRemoved = %d, want 0", result.DynamicSourcesRemoved)
	}

	sources, err := channelSvc.ListSources(ctx, created.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	foundByItem := make(map[string]channels.Source, len(sources))
	for _, src := range sources {
		foundByItem[src.ItemKey] = src
	}
	if got := foundByItem["src:sports:fox"].AssociationType; got != "dynamic_query" {
		t.Fatalf("src:sports:fox association_type = %q, want dynamic_query", got)
	}
	if _, ok := foundByItem["src:movies:other"]; ok {
		t.Fatal("did not expect non-search match src:movies:other to be attached")
	}
}

func TestReconcileDynamicChannelSupportsExclusionSearchTokens(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:english",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-english.ts",
		},
		{
			ItemKey:    "src:news:spanish",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP Spanish",
			Group:      "US News",
			StreamURL:  "http://example.com/news-spanish.ts",
		},
		{
			ItemKey:    "src:news:720p",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP 720p",
			Group:      "US News",
			StreamURL:  "http://example.com/news-720p.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelSvc := channels.NewService(store)
	created, err := channelSvc.Create(
		ctx,
		"src:news:english",
		"FOX Dynamic",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: "fox kmsp -spanish !720p",
		},
	)
	if err != nil {
		t.Fatalf("Create(dynamic channel) error = %v", err)
	}

	reconciler, err := reconcile.New(store, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := reconciler.Reconcile(ctx, nil)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.DynamicChannelsProcessed != 1 {
		t.Fatalf("result.DynamicChannelsProcessed = %d, want 1", result.DynamicChannelsProcessed)
	}

	sources, err := channelSvc.ListSources(ctx, created.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("len(sources) = %d, want 1", len(sources))
	}
	if sources[0].ItemKey != "src:news:english" {
		t.Fatalf("sources[0].ItemKey = %q, want src:news:english", sources[0].ItemKey)
	}
	if sources[0].AssociationType != "dynamic_query" {
		t.Fatalf("sources[0].AssociationType = %q, want dynamic_query", sources[0].AssociationType)
	}
}

func TestReconcileDynamicChannelSupportsOrSearchTokens(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:fox",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-fox.ts",
		},
		{
			ItemKey:    "src:news:nbc",
			ChannelKey: "tvg:news",
			Name:       "NBC Chicago",
			Group:      "US News",
			StreamURL:  "http://example.com/news-nbc.ts",
		},
		{
			ItemKey:    "src:news:nbc-spanish",
			ChannelKey: "tvg:news",
			Name:       "NBC Chicago Spanish",
			Group:      "US News",
			StreamURL:  "http://example.com/news-nbc-spanish.ts",
		},
		{
			ItemKey:    "src:news:other",
			ChannelKey: "tvg:news",
			Name:       "News Other",
			Group:      "US News",
			StreamURL:  "http://example.com/news-other.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelSvc := channels.NewService(store)
	created, err := channelSvc.Create(
		ctx,
		"src:news:fox",
		"OR Dynamic",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: "fox | nbc -spanish",
		},
	)
	if err != nil {
		t.Fatalf("Create(dynamic channel) error = %v", err)
	}

	reconciler, err := reconcile.New(store, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := reconciler.Reconcile(ctx, nil)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.DynamicChannelsProcessed != 1 {
		t.Fatalf("result.DynamicChannelsProcessed = %d, want 1", result.DynamicChannelsProcessed)
	}

	sources, err := channelSvc.ListSources(ctx, created.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("len(sources) = %d, want 2", len(sources))
	}

	foundByItem := make(map[string]channels.Source, len(sources))
	for _, source := range sources {
		foundByItem[source.ItemKey] = source
	}
	if _, ok := foundByItem["src:news:fox"]; !ok {
		t.Fatalf("expected src:news:fox in dynamic sources, got %#v", foundByItem)
	}
	if _, ok := foundByItem["src:news:nbc"]; !ok {
		t.Fatalf("expected src:news:nbc in dynamic sources, got %#v", foundByItem)
	}
	if _, ok := foundByItem["src:news:nbc-spanish"]; ok {
		t.Fatalf("did not expect excluded src:news:nbc-spanish in dynamic sources: %#v", foundByItem)
	}
}

func TestReconcileDynamicChannelSupportsRegexSearchTokens(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:regex:fox9",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-regex-fox9.ts",
		},
		{
			ItemKey:    "src:news:regex:fox32",
			ChannelKey: "tvg:news",
			Name:       "FOX 32 WFLD",
			Group:      "US News",
			StreamURL:  "http://example.com/news-regex-fox32.ts",
		},
		{
			ItemKey:    "src:news:regex:nbc",
			ChannelKey: "tvg:news",
			Name:       "NBC Chicago",
			Group:      "US News",
			StreamURL:  "http://example.com/news-regex-nbc.ts",
		},
		{
			ItemKey:    "src:news:regex:nbc-spanish",
			ChannelKey: "tvg:news",
			Name:       "NBC Chicago Spanish",
			Group:      "US News",
			StreamURL:  "http://example.com/news-regex-nbc-spanish.ts",
		},
		{
			ItemKey:    "src:news:regex:foxsports",
			ChannelKey: "tvg:news",
			Name:       "FOX Sports",
			Group:      "US News",
			StreamURL:  "http://example.com/news-regex-foxsports.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelSvc := channels.NewService(store)
	created, err := channelSvc.Create(
		ctx,
		"src:news:regex:fox9",
		"Regex Dynamic",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: `fox\s+\d+|nbc chicago$`,
			SearchRegex: true,
		},
	)
	if err != nil {
		t.Fatalf("Create(dynamic regex channel) error = %v", err)
	}

	reconciler, err := reconcile.New(store, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := reconciler.Reconcile(ctx, nil)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.DynamicChannelsProcessed != 1 {
		t.Fatalf("result.DynamicChannelsProcessed = %d, want 1", result.DynamicChannelsProcessed)
	}

	sources, err := channelSvc.ListSources(ctx, created.ChannelID, false)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	if len(sources) != 3 {
		t.Fatalf("len(sources) = %d, want 3", len(sources))
	}

	foundByItem := make(map[string]channels.Source, len(sources))
	for _, source := range sources {
		foundByItem[source.ItemKey] = source
	}
	if _, ok := foundByItem["src:news:regex:fox9"]; !ok {
		t.Fatalf("expected src:news:regex:fox9 in dynamic sources, got %#v", foundByItem)
	}
	if _, ok := foundByItem["src:news:regex:fox32"]; !ok {
		t.Fatalf("expected src:news:regex:fox32 in dynamic sources, got %#v", foundByItem)
	}
	if _, ok := foundByItem["src:news:regex:nbc"]; !ok {
		t.Fatalf("expected src:news:regex:nbc in dynamic sources, got %#v", foundByItem)
	}
	if _, ok := foundByItem["src:news:regex:nbc-spanish"]; ok {
		t.Fatalf("did not expect excluded src:news:regex:nbc-spanish in dynamic sources: %#v", foundByItem)
	}
	if _, ok := foundByItem["src:news:regex:foxsports"]; ok {
		t.Fatalf("did not expect regex-mismatched src:news:regex:foxsports in dynamic sources: %#v", foundByItem)
	}
}

func TestReconcileDynamicChannelReusesCatalogFilterLookupForDuplicateRules(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news:1",
			Name:       "FOX 9 KMSP Primary",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news:2",
			Name:       "FOX KMSP Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
		{
			ItemKey:    "src:sports:primary",
			ChannelKey: "tvg:sports:1",
			Name:       "FOX Sports Midwest",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelSvc := channels.NewService(store)
	for idx, itemKey := range []string{"src:news:primary", "src:news:backup"} {
		if _, err := channelSvc.Create(
			ctx,
			itemKey,
			"US News Dynamic "+string(rune('A'+idx)),
			"",
			&channels.DynamicSourceRule{
				Enabled:     true,
				GroupName:   "US News",
				SearchQuery: "fox kmsp",
			},
		); err != nil {
			t.Fatalf("Create(duplicate-rule dynamic channel=%d) error = %v", idx, err)
		}
	}
	if _, err := channelSvc.Create(
		ctx,
		"src:sports:primary",
		"Sports Dynamic",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "Sports",
			SearchQuery: "fox",
		},
	); err != nil {
		t.Fatalf("Create(sports dynamic channel) error = %v", err)
	}

	countingCatalog := newCountingCatalogStore(store)
	reconciler, err := reconcile.New(countingCatalog, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := reconciler.Reconcile(ctx, nil)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.DynamicChannelsProcessed != 3 {
		t.Fatalf("DynamicChannelsProcessed = %d, want 3", result.DynamicChannelsProcessed)
	}

	if got := countingCatalog.lookupCountFor([]string{"US News"}, "fox kmsp"); got != 1 {
		t.Fatalf("catalog lookup count for duplicate rule = %d, want 1", got)
	}
	if got := countingCatalog.lookupCountFor([]string{"Sports"}, "fox"); got != 1 {
		t.Fatalf("catalog lookup count for sports rule = %d, want 1", got)
	}
	if got := countingCatalog.totalCatalogFilterCalls(); got != 2 {
		t.Fatalf("total catalog filter lookups = %d, want 2", got)
	}
}

func TestReconcileDynamicChannelUsesCatalogFilterPagedSyncWhenSupported(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP Primary",
			Group:      "US News",
			StreamURL:  "http://example.com/primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX KMSP Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	baseChannelSvc := channels.NewService(store)
	if _, err := baseChannelSvc.Create(
		ctx,
		"src:news:primary",
		"News Dynamic",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: "fox kmsp",
		},
	); err != nil {
		t.Fatalf("Create(dynamic channel) error = %v", err)
	}

	observedChannelSvc := &observedDynamicSyncChannelsService{Service: baseChannelSvc}
	reconciler, err := reconcile.New(store, observedChannelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	reconciler.SetDynamicRulePagedMode(true)

	result, err := reconciler.Reconcile(ctx, nil)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.DynamicChannelsProcessed != 1 {
		t.Fatalf("DynamicChannelsProcessed = %d, want 1", result.DynamicChannelsProcessed)
	}
	if observedChannelSvc.pagedCalls != 1 {
		t.Fatalf("paged sync calls = %d, want 1", observedChannelSvc.pagedCalls)
	}
	if observedChannelSvc.legacyCalls != 0 {
		t.Fatalf("legacy sync calls = %d, want 0", observedChannelSvc.legacyCalls)
	}
}

func TestReconcileDynamicChannelFallsBackToLegacySyncWithoutCatalogIterator(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP Primary",
			Group:      "US News",
			StreamURL:  "http://example.com/primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX KMSP Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	baseChannelSvc := channels.NewService(store)
	if _, err := baseChannelSvc.Create(
		ctx,
		"src:news:primary",
		"News Dynamic",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: "fox kmsp",
		},
	); err != nil {
		t.Fatalf("Create(dynamic channel) error = %v", err)
	}

	countingCatalog := newCountingCatalogStore(store)
	observedChannelSvc := &observedDynamicSyncChannelsService{Service: baseChannelSvc}
	reconciler, err := reconcile.New(countingCatalog, observedChannelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	reconciler.SetDynamicRulePagedMode(true)

	result, err := reconciler.Reconcile(ctx, nil)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.DynamicChannelsProcessed != 1 {
		t.Fatalf("DynamicChannelsProcessed = %d, want 1", result.DynamicChannelsProcessed)
	}
	if observedChannelSvc.pagedCalls != 0 {
		t.Fatalf("paged sync calls = %d, want 0", observedChannelSvc.pagedCalls)
	}
	if observedChannelSvc.legacyCalls != 1 {
		t.Fatalf("legacy sync calls = %d, want 1", observedChannelSvc.legacyCalls)
	}
	if got := countingCatalog.totalCatalogFilterCalls(); got != 1 {
		t.Fatalf("catalog filter lookups = %d, want 1", got)
	}
}

func TestReconcileDynamicChannelFailsWhenMatchLimitExceededLegacy(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP Primary",
			Group:      "US News",
			StreamURL:  "http://example.com/primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX KMSP Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelSvc := channels.NewService(store)
	created, err := channelSvc.Create(
		ctx,
		"src:news:primary",
		"News Dynamic",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: "fox kmsp",
		},
	)
	if err != nil {
		t.Fatalf("Create(dynamic channel) error = %v", err)
	}

	reconciler, err := reconcile.New(store, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	reconciler.SetDynamicRuleMatchLimit(1)

	_, err = reconciler.Reconcile(ctx, nil)
	if err == nil {
		t.Fatal("Reconcile() error = nil, want match-limit failure")
	}
	if !strings.Contains(err.Error(), "limit is 1") {
		t.Fatalf("Reconcile() error = %v, want match-limit message", err)
	}

	sources, err := channelSvc.ListSources(ctx, created.ChannelID, false)
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

func TestReconcileDynamicChannelFailsWhenMatchLimitExceededPaged(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP Primary",
			Group:      "US News",
			StreamURL:  "http://example.com/primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX KMSP Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelSvc := channels.NewService(store)
	created, err := channelSvc.Create(
		ctx,
		"src:news:primary",
		"News Dynamic",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: "fox kmsp",
		},
	)
	if err != nil {
		t.Fatalf("Create(dynamic channel) error = %v", err)
	}

	reconciler, err := reconcile.New(store, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	reconciler.SetDynamicRulePagedMode(true)
	reconciler.SetDynamicRuleMatchLimit(1)

	_, err = reconciler.Reconcile(ctx, nil)
	if err == nil {
		t.Fatal("Reconcile() error = nil, want match-limit failure")
	}
	if !strings.Contains(err.Error(), "limit is 1") {
		t.Fatalf("Reconcile() error = %v, want match-limit message", err)
	}

	sources, err := channelSvc.ListSources(ctx, created.ChannelID, false)
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

type countingCatalogStore struct {
	base   *sqlite.Store
	counts map[string]int
	total  int
}

func newCountingCatalogStore(base *sqlite.Store) *countingCatalogStore {
	return &countingCatalogStore{
		base:   base,
		counts: make(map[string]int),
	}
}

func (c *countingCatalogStore) ListActiveItemKeysByChannelKey(ctx context.Context, channelKey string) ([]string, error) {
	return c.base.ListActiveItemKeysByChannelKey(ctx, channelKey)
}

func (c *countingCatalogStore) ListActiveItemKeysByCatalogFilter(ctx context.Context, groupNames []string, searchQuery string, searchRegex bool) ([]string, error) {
	key := countingCatalogFilterKey(groupNames, searchQuery, searchRegex)
	c.counts[key]++
	c.total++
	return c.base.ListActiveItemKeysByCatalogFilter(ctx, groupNames, searchQuery, searchRegex)
}

func (c *countingCatalogStore) lookupCountFor(groupNames []string, searchQuery string) int {
	return c.counts[countingCatalogFilterKey(groupNames, searchQuery, false)]
}

func (c *countingCatalogStore) totalCatalogFilterCalls() int {
	return c.total
}

func countingCatalogFilterKey(groupNames []string, searchQuery string, searchRegex bool) string {
	return strings.Join(channels.NormalizeGroupNames("", groupNames), ",") + "|" + strings.TrimSpace(searchQuery) + "|" + strconv.FormatBool(searchRegex)
}

type observedDynamicSyncChannelsService struct {
	*channels.Service
	legacyCalls int
	pagedCalls  int
}

func (s *observedDynamicSyncChannelsService) SyncDynamicSources(ctx context.Context, channelID int64, matchedItemKeys []string) (channels.DynamicSourceSyncResult, error) {
	s.legacyCalls++
	return s.Service.SyncDynamicSources(ctx, channelID, matchedItemKeys)
}

func (s *observedDynamicSyncChannelsService) SyncDynamicSourcesByCatalogFilter(ctx context.Context, channelID int64, groupNames []string, searchQuery string, searchRegex bool, pageSize int, maxMatches int) (channels.DynamicSourceSyncResult, int, error) {
	s.pagedCalls++
	return s.Service.SyncDynamicSourcesByCatalogFilter(ctx, channelID, groupNames, searchQuery, searchRegex, pageSize, maxMatches)
}

func BenchmarkReconcileDuplicateDynamicRules(b *testing.B) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	b.Cleanup(func() {
		_ = store.Close()
	})

	const itemCount = 12000
	items := make([]playlist.Item, 0, itemCount)
	for i := 0; i < itemCount; i++ {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:bench:news:%05d", i),
			ChannelKey: fmt.Sprintf("name:bench-news-%05d", i),
			Name:       fmt.Sprintf("FOX KMSP Bench %05d", i),
			Group:      "US News",
			StreamURL:  fmt.Sprintf("http://example.com/bench/news/%05d.ts", i),
		})
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelSvc := channels.NewService(store)
	for idx := 0; idx < 3; idx++ {
		if _, err := channelSvc.Create(
			ctx,
			items[idx].ItemKey,
			fmt.Sprintf("Dynamic Bench %d", idx+1),
			"",
			&channels.DynamicSourceRule{
				Enabled:     true,
				GroupName:   "US News",
				SearchQuery: "fox kmsp",
			},
		); err != nil {
			b.Fatalf("Create(dynamic benchmark channel=%d) error = %v", idx, err)
		}
	}

	reconciler, err := reconcile.New(store, channelSvc)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	reconciler.SetLogger(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	reconciler.SetDynamicRuleMatchLimit(0)

	// Warm to materialize first-run sources before measuring steady-state reconcile cost.
	if _, err := reconciler.Reconcile(ctx, nil); err != nil {
		b.Fatalf("Reconcile(warm) error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := reconciler.Reconcile(ctx, nil); err != nil {
			b.Fatalf("Reconcile() error = %v", err)
		}
	}
}

func BenchmarkReconcileSingleDynamicRulePaged(b *testing.B) {
	benchmarkReconcileSingleDynamicRule(b, false)
}

func BenchmarkReconcileSingleDynamicRuleLegacySlice(b *testing.B) {
	benchmarkReconcileSingleDynamicRule(b, true)
}

func benchmarkReconcileSingleDynamicRule(b *testing.B, forceLegacy bool) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	b.Cleanup(func() {
		_ = store.Close()
	})

	const itemCount = 12000
	items := make([]playlist.Item, 0, itemCount)
	for i := 0; i < itemCount; i++ {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:bench:single:%05d", i),
			ChannelKey: fmt.Sprintf("name:bench-single-%05d", i),
			Name:       fmt.Sprintf("FOX KMSP Single %05d", i),
			Group:      "US News",
			StreamURL:  fmt.Sprintf("http://example.com/bench/single/%05d.ts", i),
		})
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelSvc := channels.NewService(store)
	if _, err := channelSvc.Create(
		ctx,
		items[0].ItemKey,
		"Single Dynamic Bench",
		"",
		&channels.DynamicSourceRule{
			Enabled:     true,
			GroupName:   "US News",
			SearchQuery: "fox kmsp",
		},
	); err != nil {
		b.Fatalf("Create(dynamic benchmark channel) error = %v", err)
	}

	var catalog reconcile.CatalogStore = store
	if forceLegacy {
		catalog = newCountingCatalogStore(store)
	}

	reconciler, err := reconcile.New(catalog, channelSvc)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	reconciler.SetLogger(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	reconciler.SetDynamicRuleMatchLimit(0)
	if !forceLegacy {
		reconciler.SetDynamicRulePagedMode(true)
	}

	// Warm to materialize first-run sources before measuring steady-state reconcile cost.
	if _, err := reconciler.Reconcile(ctx, nil); err != nil {
		b.Fatalf("Reconcile(warm) error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := reconciler.Reconcile(ctx, nil); err != nil {
			b.Fatalf("Reconcile() error = %v", err)
		}
	}
}
