package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/playlist"
)

func TestStoreCatalogLifecycle(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	initial := []playlist.Item{
		{
			ItemKey:    "src:news.1:aaaa1111bbbb",
			ChannelKey: "tvg:news.1",
			Name:       "News One",
			Group:      "News",
			StreamURL:  "http://example.com/news1.ts",
			TVGID:      "news.1",
			Attrs:      map[string]string{"group-title": "News"},
		},
		{
			ItemKey:    "src:sports.1:cccc2222dddd",
			ChannelKey: "tvg:sports.1",
			Name:       "Sports One",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports1.ts",
			TVGID:      "sports.1",
			Attrs:      map[string]string{"group-title": "Sports"},
		},
	}

	if err := store.UpsertPlaylistItems(ctx, initial); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}

	groups, err := store.GetGroups(ctx)
	if err != nil {
		t.Fatalf("GetGroups() error = %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("len(groups) = %d, want 2", len(groups))
	}

	items, total, err := store.ListItems(ctx, playlist.Query{Group: "News", Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("ListItems(news) error = %v", err)
	}
	if total != 1 {
		t.Fatalf("total = %d, want 1", total)
	}
	if len(items) != 1 || items[0].Name != "News One" {
		t.Fatalf("unexpected news items: %+v", items)
	}

	refreshed := []playlist.Item{
		{
			ItemKey:    "src:news.1:aaaa1111bbbb",
			ChannelKey: "tvg:news.1",
			Name:       "News One HD",
			Group:      "News",
			StreamURL:  "http://example.com/news1-hd.ts",
			TVGID:      "news.1",
			Attrs:      map[string]string{"group-title": "News", "quality": "hd"},
		},
	}
	if err := store.UpsertPlaylistItems(ctx, refreshed); err != nil {
		t.Fatalf("UpsertPlaylistItems(refreshed) error = %v", err)
	}

	allItems, total, err := store.ListItems(ctx, playlist.Query{Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("ListItems(all) error = %v", err)
	}
	if total != 1 || len(allItems) != 1 {
		t.Fatalf("expected only active item in ListItems, total=%d len=%d", total, len(allItems))
	}
	if allItems[0].Name != "News One HD" {
		t.Fatalf("updated item name = %q, want News One HD", allItems[0].Name)
	}

	var totalRows int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM playlist_items`).Scan(&totalRows); err != nil {
		t.Fatalf("count raw playlist rows: %v", err)
	}
	if totalRows != 2 {
		t.Fatalf("total playlist_items rows = %d, want 2 (stale rows retained)", totalRows)
	}

	var sportsActive int
	if err := store.db.QueryRowContext(ctx, `SELECT active FROM playlist_items WHERE item_key = ?`, "src:sports.1:cccc2222dddd").Scan(&sportsActive); err != nil {
		t.Fatalf("query stale item active flag: %v", err)
	}
	if sportsActive != 0 {
		t.Fatalf("stale item active = %d, want 0", sportsActive)
	}
}

func TestUpsertPlaylistItemsSkipsAlreadyInactiveStaleRows(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const (
		itemCount = 1000
		seenCount = 100
	)
	allItems := generateCatalogBenchmarkItems(itemCount)
	seenItems := append([]playlist.Item(nil), allItems[:seenCount]...)

	if err := store.UpsertPlaylistItems(ctx, allItems); err != nil {
		t.Fatalf("UpsertPlaylistItems(allItems) error = %v", err)
	}
	if err := store.UpsertPlaylistItems(ctx, seenItems); err != nil {
		t.Fatalf("UpsertPlaylistItems(seenItems first refresh) error = %v", err)
	}

	if err := installInactiveRewriteCounter(ctx, store.db); err != nil {
		t.Fatalf("installInactiveRewriteCounter() error = %v", err)
	}

	if err := store.UpsertPlaylistItems(ctx, seenItems); err != nil {
		t.Fatalf("UpsertPlaylistItems(seenItems second refresh) error = %v", err)
	}

	rewriteCount, err := inactiveRewriteCount(ctx, store.db)
	if err != nil {
		t.Fatalf("inactiveRewriteCount() error = %v", err)
	}
	if rewriteCount != 0 {
		t.Fatalf("inactive stale-row rewrite count = %d, want 0", rewriteCount)
	}

	var activeCount int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM playlist_items WHERE active = 1`).Scan(&activeCount); err != nil {
		t.Fatalf("count active rows: %v", err)
	}
	if activeCount != seenCount {
		t.Fatalf("active rows = %d, want %d", activeCount, seenCount)
	}

	var inactiveCount int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM playlist_items WHERE active = 0`).Scan(&inactiveCount); err != nil {
		t.Fatalf("count inactive rows: %v", err)
	}
	if inactiveCount != itemCount-seenCount {
		t.Fatalf("inactive rows = %d, want %d", inactiveCount, itemCount-seenCount)
	}
}

func TestUpsertPlaylistItemsStreamRollbackOnStreamError(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	seed := []playlist.Item{
		{
			ItemKey:    "src:rollback:1",
			ChannelKey: "name:rollback-one",
			Name:       "Rollback One",
			Group:      "Rollback",
			StreamURL:  "http://example.com/rollback/one.ts",
		},
		{
			ItemKey:    "src:rollback:2",
			ChannelKey: "name:rollback-two",
			Name:       "Rollback Two",
			Group:      "Rollback",
			StreamURL:  "http://example.com/rollback/two.ts",
		},
	}
	if err := store.UpsertPlaylistItems(ctx, seed); err != nil {
		t.Fatalf("UpsertPlaylistItems(seed) error = %v", err)
	}

	streamErr := errors.New("stream interrupted")
	_, err = store.UpsertPlaylistItemsStream(ctx, func(yield func(playlist.Item) error) error {
		if err := yield(playlist.Item{
			ItemKey:    "src:rollback:1",
			ChannelKey: "name:rollback-one",
			Name:       "Rollback One Updated",
			Group:      "Rollback",
			StreamURL:  "http://example.com/rollback/one-hd.ts",
		}); err != nil {
			return err
		}
		return streamErr
	})
	if !errors.Is(err, streamErr) {
		t.Fatalf("UpsertPlaylistItemsStream() error = %v, want %v", err, streamErr)
	}

	var (
		name1   string
		url1    string
		active1 int
		active2 int
	)
	if err := store.db.QueryRowContext(
		ctx,
		`SELECT name, stream_url, active FROM playlist_items WHERE item_key = ?`,
		"src:rollback:1",
	).Scan(&name1, &url1, &active1); err != nil {
		t.Fatalf("query first item after rollback: %v", err)
	}
	if err := store.db.QueryRowContext(
		ctx,
		`SELECT active FROM playlist_items WHERE item_key = ?`,
		"src:rollback:2",
	).Scan(&active2); err != nil {
		t.Fatalf("query second item after rollback: %v", err)
	}
	if name1 != "Rollback One" {
		t.Fatalf("name after rollback = %q, want %q", name1, "Rollback One")
	}
	if url1 != "http://example.com/rollback/one.ts" {
		t.Fatalf("stream_url after rollback = %q, want original value", url1)
	}
	if active1 != 1 || active2 != 1 {
		t.Fatalf("active flags after rollback = [%d,%d], want [1,1]", active1, active2)
	}
}

func TestUpsertPlaylistItemsStreamLifecycleParity(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const (
		itemCount = 600
		seenCount = 120
	)
	allItems := generateCatalogBenchmarkItems(itemCount)
	seenItems := append([]playlist.Item(nil), allItems[:seenCount]...)

	if _, err := store.UpsertPlaylistItemsStream(ctx, streamFromItems(allItems)); err != nil {
		t.Fatalf("UpsertPlaylistItemsStream(allItems) error = %v", err)
	}
	if _, err := store.UpsertPlaylistItemsStream(ctx, streamFromItems(seenItems)); err != nil {
		t.Fatalf("UpsertPlaylistItemsStream(seenItems first refresh) error = %v", err)
	}

	var activeCount int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM playlist_items WHERE active = 1`).Scan(&activeCount); err != nil {
		t.Fatalf("count active rows: %v", err)
	}
	if activeCount != seenCount {
		t.Fatalf("active rows = %d, want %d", activeCount, seenCount)
	}

	var inactiveCount int
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM playlist_items WHERE active = 0`).Scan(&inactiveCount); err != nil {
		t.Fatalf("count inactive rows: %v", err)
	}
	if inactiveCount != itemCount-seenCount {
		t.Fatalf("inactive rows = %d, want %d", inactiveCount, itemCount-seenCount)
	}
}

func streamFromItems(items []playlist.Item) playlist.ItemStream {
	return func(yield func(playlist.Item) error) error {
		for _, item := range items {
			if err := yield(item); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestListGroupsPagedDeterministicOrderingAndTotals(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:groups:archive:001",
			ChannelKey: "name:archive",
			Name:       "Archive 1",
			Group:      "Archive",
			StreamURL:  "http://example.com/groups/archive-1.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:groups:movies:001",
			ChannelKey: "name:movies",
			Name:       "Movies 1",
			Group:      "Movies",
			StreamURL:  "http://example.com/groups/movies-1.ts",
		},
		{
			ItemKey:    "src:groups:news:001",
			ChannelKey: "name:news",
			Name:       "News 1",
			Group:      "News",
			StreamURL:  "http://example.com/groups/news-1.ts",
		},
		{
			ItemKey:    "src:groups:news:002",
			ChannelKey: "name:news",
			Name:       "News 2",
			Group:      "News",
			StreamURL:  "http://example.com/groups/news-2.ts",
		},
		{
			ItemKey:    "src:groups:sports:001",
			ChannelKey: "name:sports",
			Name:       "Sports 1",
			Group:      "Sports",
			StreamURL:  "http://example.com/groups/sports-1.ts",
		},
		{
			ItemKey:    "src:groups:sports:002",
			ChannelKey: "name:sports",
			Name:       "Sports 2",
			Group:      "Sports",
			StreamURL:  "http://example.com/groups/sports-2.ts",
		},
		{
			ItemKey:    "src:groups:sports:003",
			ChannelKey: "name:sports",
			Name:       "Sports 3",
			Group:      "Sports",
			StreamURL:  "http://example.com/groups/sports-3.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(refreshed) error = %v", err)
	}

	page, total, err := store.ListGroupsPaged(ctx, 2, 1, true)
	if err != nil {
		t.Fatalf("ListGroupsPaged(limit=2, offset=1, includeCounts=true) error = %v", err)
	}
	if total != 3 {
		t.Fatalf("paged total = %d, want 3", total)
	}
	if len(page) != 2 {
		t.Fatalf("len(page) = %d, want 2", len(page))
	}
	if page[0].Name != "News" || page[1].Name != "Sports" {
		t.Fatalf("paged names = [%q,%q], want [News,Sports]", page[0].Name, page[1].Name)
	}
	if page[0].Count != 2 || page[1].Count != 3 {
		t.Fatalf("paged counts = [%d,%d], want [2,3]", page[0].Count, page[1].Count)
	}

	allRows, allTotal, err := store.ListGroupsPaged(ctx, 0, 0, true)
	if err != nil {
		t.Fatalf("ListGroupsPaged(limit=0, offset=0, includeCounts=true) error = %v", err)
	}
	if allTotal != 3 {
		t.Fatalf("allTotal = %d, want 3", allTotal)
	}
	if len(allRows) != 3 {
		t.Fatalf("len(allRows) = %d, want 3", len(allRows))
	}
	if allRows[0].Name != "Movies" || allRows[1].Name != "News" || allRows[2].Name != "Sports" {
		t.Fatalf("allRows names = [%q,%q,%q], want [Movies,News,Sports]", allRows[0].Name, allRows[1].Name, allRows[2].Name)
	}

	namesOnlyRows, namesOnlyTotal, err := store.ListGroupsPaged(ctx, 2, 0, false)
	if err != nil {
		t.Fatalf("ListGroupsPaged(limit=2, offset=0, includeCounts=false) error = %v", err)
	}
	if namesOnlyTotal != 3 {
		t.Fatalf("namesOnlyTotal = %d, want 3", namesOnlyTotal)
	}
	if len(namesOnlyRows) != 2 {
		t.Fatalf("len(namesOnlyRows) = %d, want 2", len(namesOnlyRows))
	}
	if namesOnlyRows[0].Name != "Movies" || namesOnlyRows[1].Name != "News" {
		t.Fatalf("namesOnly names = [%q,%q], want [Movies,News]", namesOnlyRows[0].Name, namesOnlyRows[1].Name)
	}
	if namesOnlyRows[0].Count != 0 || namesOnlyRows[1].Count != 0 {
		t.Fatalf("namesOnly counts = [%d,%d], want [0,0]", namesOnlyRows[0].Count, namesOnlyRows[1].Count)
	}

	if _, _, err := store.ListGroupsPaged(ctx, -1, 0, true); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("ListGroupsPaged(limit=-1) error = %v, want limit validation error", err)
	}
	if _, _, err := store.ListGroupsPaged(ctx, 1, -1, false); err == nil || !strings.Contains(err.Error(), "offset") {
		t.Fatalf("ListGroupsPaged(offset=-1) error = %v, want offset validation error", err)
	}
}

func TestListItemsSearchMatchesSplitTerms(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:fox9:0001",
			ChannelKey: "name:fox 9 kmsp",
			Name:       "FOX 9 KMSP",
			Group:      "Local",
			StreamURL:  "http://example.com/fox9.ts",
		},
		{
			ItemKey:    "src:fox32:0002",
			ChannelKey: "name:fox 32 wfld",
			Name:       "FOX 32 WFLD",
			Group:      "Local",
			StreamURL:  "http://example.com/fox32.ts",
		},
		{
			ItemKey:    "src:kmspwx:0003",
			ChannelKey: "name:kmsp weather",
			Name:       "KMSP Weather",
			Group:      "Local",
			StreamURL:  "http://example.com/kmsp-weather.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	items, total, err := store.ListItems(ctx, playlist.Query{Search: "fox kmsp", Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("ListItems(fox kmsp) error = %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("ListItems(fox kmsp) total/len = %d/%d, want 1/1", total, len(items))
	}
	if items[0].ItemKey != "src:fox9:0001" {
		t.Fatalf("ListItems(fox kmsp) matched %q, want src:fox9:0001", items[0].ItemKey)
	}

	items, total, err = store.ListItems(ctx, playlist.Query{Search: "KMSP, FOX", Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("ListItems(kmsp fox) error = %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("ListItems(kmsp fox) total/len = %d/%d, want 1/1", total, len(items))
	}
	if items[0].ItemKey != "src:fox9:0001" {
		t.Fatalf("ListItems(kmsp fox) matched %q, want src:fox9:0001", items[0].ItemKey)
	}

	items, total, err = store.ListItems(ctx, playlist.Query{Search: "fox", Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("ListItems(fox) error = %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("ListItems(fox) total/len = %d/%d, want 2/2", total, len(items))
	}
}

func TestCatalogSearchTreatsLiteralWildcardInputsAsLiterals(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:wild:percent",
			ChannelKey: "name:wild percent",
			Name:       "Wild Percent % Channel",
			Group:      "Wild",
			StreamURL:  "http://example.com/wild-percent.ts",
		},
		{
			ItemKey:    "src:wild:underscore",
			ChannelKey: "name:wild underscore",
			Name:       "Wild Underscore _ Channel",
			Group:      "Wild",
			StreamURL:  "http://example.com/wild-underscore.ts",
		},
		{
			ItemKey:    "src:wild:plain",
			ChannelKey: "name:wild plain",
			Name:       "Wild Plain Channel",
			Group:      "Wild",
			StreamURL:  "http://example.com/wild-plain.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	items, total, err := store.ListItems(ctx, playlist.Query{Search: "%", Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("ListItems(search=%%) error = %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ItemKey != "src:wild:percent" {
		t.Fatalf("ListItems(search=%%) = total %d keys %#v, want only src:wild:percent", total, itemKeys(items))
	}

	catalogItems, catalogTotal, err := store.ListCatalogItems(ctx, playlist.Query{Search: "%", Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("ListCatalogItems(search=%%) error = %v", err)
	}
	if catalogTotal != 1 || len(catalogItems) != 1 || catalogItems[0].ItemKey != "src:wild:percent" {
		t.Fatalf("ListCatalogItems(search=%%) = total %d keys %#v, want only src:wild:percent", catalogTotal, itemKeys(catalogItems))
	}

	items, total, err = store.ListItems(ctx, playlist.Query{Search: "_", Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("ListItems(search=_) error = %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ItemKey != "src:wild:underscore" {
		t.Fatalf("ListItems(search=_) = total %d keys %#v, want only src:wild:underscore", total, itemKeys(items))
	}

	catalogItems, catalogTotal, err = store.ListCatalogItems(ctx, playlist.Query{Search: "_", Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("ListCatalogItems(search=_) error = %v", err)
	}
	if catalogTotal != 1 || len(catalogItems) != 1 || catalogItems[0].ItemKey != "src:wild:underscore" {
		t.Fatalf("ListCatalogItems(search=_) = total %d keys %#v, want only src:wild:underscore", catalogTotal, itemKeys(catalogItems))
	}

	items, total, err = store.ListItems(ctx, playlist.Query{Search: "-%", Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("ListItems(search=-%%) error = %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("ListItems(search=-%%) total/len = %d/%d, want 2/2", total, len(items))
	}
	if items[0].ItemKey != "src:wild:plain" || items[1].ItemKey != "src:wild:underscore" {
		t.Fatalf("ListItems(search=-%%) item keys = %#v, want [src:wild:plain src:wild:underscore]", itemKeys(items))
	}

	catalogItems, catalogTotal, err = store.ListCatalogItems(ctx, playlist.Query{Search: "-%", Limit: 20, Offset: 0})
	if err != nil {
		t.Fatalf("ListCatalogItems(search=-%%) error = %v", err)
	}
	if catalogTotal != 2 || len(catalogItems) != 2 {
		t.Fatalf("ListCatalogItems(search=-%%) total/len = %d/%d, want 2/2", catalogTotal, len(catalogItems))
	}
	if catalogItems[0].ItemKey != "src:wild:plain" || catalogItems[1].ItemKey != "src:wild:underscore" {
		t.Fatalf("ListCatalogItems(search=-%%) item keys = %#v, want [src:wild:plain src:wild:underscore]", itemKeys(catalogItems))
	}
}

func TestCatalogSearchBuildersBoundLongTokenExpansion(t *testing.T) {
	search := strings.Join([]string{
		buildLongCatalogSearchQuery(maxCatalogSearchTerms + 40),
		strings.Repeat("x", maxCatalogSearchTermRunes+48),
		buildLongCatalogSearchQuery(maxCatalogSearchTerms + 40),
	}, " ")
	expectedTerms := catalogSearchLikeTerms(search)
	if len(expectedTerms) != maxCatalogSearchTerms {
		t.Fatalf("len(catalogSearchLikeTerms(long)) = %d, want %d", len(expectedTerms), maxCatalogSearchTerms)
	}

	catalogWhere, catalogArgs, err := buildWhere(playlist.Query{
		Group:  "US News",
		Search: search,
	})
	if err != nil {
		t.Fatalf("buildWhere(long search) error = %v", err)
	}
	if got := strings.Count(catalogWhere, catalogSearchLikeClause); got != len(expectedTerms) {
		t.Fatalf("buildWhere LIKE predicate count = %d, want %d", got, len(expectedTerms))
	}
	if got := strings.Count(catalogWhere, catalogSearchNotLikeClause); got != 0 {
		t.Fatalf("buildWhere NOT LIKE predicate count = %d, want 0", got)
	}
	if len(catalogArgs) != 1+len(expectedTerms) {
		t.Fatalf("len(buildWhere args) = %d, want %d", len(catalogArgs), 1+len(expectedTerms))
	}
	if groupArg, ok := catalogArgs[0].(string); !ok || groupArg != "US News" {
		t.Fatalf("buildWhere group arg = %#v, want %q", catalogArgs[0], "US News")
	}
	for i, term := range expectedTerms {
		wantPattern := sqlLikeContainsPattern(term)
		gotPattern, ok := catalogArgs[i+1].(string)
		if !ok {
			t.Fatalf("buildWhere arg[%d] type = %T, want string", i+1, catalogArgs[i+1])
		}
		if gotPattern != wantPattern {
			t.Fatalf("buildWhere arg[%d] = %q, want %q", i+1, gotPattern, wantPattern)
		}
	}

	spec, err := buildCatalogFilterQuerySpec([]string{"US News"}, search, false)
	if err != nil {
		t.Fatalf("buildCatalogFilterQuerySpec(long search) error = %v", err)
	}
	if got := strings.Count(spec.WhereClause, catalogSearchLikeClause); got != len(expectedTerms) {
		t.Fatalf("buildCatalogFilterQuerySpec LIKE predicate count = %d, want %d", got, len(expectedTerms))
	}
	if got := strings.Count(spec.WhereClause, catalogSearchNotLikeClause); got != 0 {
		t.Fatalf("buildCatalogFilterQuerySpec NOT LIKE predicate count = %d, want 0", got)
	}
	if len(spec.Args) != 1+len(expectedTerms) {
		t.Fatalf("len(buildCatalogFilterQuerySpec args) = %d, want %d", len(spec.Args), 1+len(expectedTerms))
	}
	if groupArg, ok := spec.Args[0].(string); !ok || groupArg != "US News" {
		t.Fatalf("buildCatalogFilterQuerySpec group arg = %#v, want %q", spec.Args[0], "US News")
	}
	for i, term := range expectedTerms {
		wantPattern := sqlLikeContainsPattern(term)
		gotPattern, ok := spec.Args[i+1].(string)
		if !ok {
			t.Fatalf("buildCatalogFilterQuerySpec arg[%d] type = %T, want string", i+1, spec.Args[i+1])
		}
		if gotPattern != wantPattern {
			t.Fatalf("buildCatalogFilterQuerySpec arg[%d] = %q, want %q", i+1, gotPattern, wantPattern)
		}
	}
}

func TestCatalogSearchBuildersKeepNormalSizedTermSemantics(t *testing.T) {
	search := "FOX kmsp"

	catalogWhere, catalogArgs, err := buildWhere(playlist.Query{Search: search})
	if err != nil {
		t.Fatalf("buildWhere(normal search) error = %v", err)
	}
	spec, err := buildCatalogFilterQuerySpec(nil, search, false)
	if err != nil {
		t.Fatalf("buildCatalogFilterQuerySpec(normal search) error = %v", err)
	}

	if got := strings.Count(catalogWhere, catalogSearchLikeClause); got != 2 {
		t.Fatalf("buildWhere LIKE predicate count = %d, want 2", got)
	}
	if got := strings.Count(catalogWhere, catalogSearchNotLikeClause); got != 0 {
		t.Fatalf("buildWhere NOT LIKE predicate count = %d, want 0", got)
	}
	if got := strings.Count(spec.WhereClause, catalogSearchLikeClause); got != 2 {
		t.Fatalf("buildCatalogFilterQuerySpec LIKE predicate count = %d, want 2", got)
	}
	if got := strings.Count(spec.WhereClause, catalogSearchNotLikeClause); got != 0 {
		t.Fatalf("buildCatalogFilterQuerySpec NOT LIKE predicate count = %d, want 0", got)
	}
	if len(catalogArgs) != 2 {
		t.Fatalf("len(buildWhere args) = %d, want 2", len(catalogArgs))
	}
	if len(spec.Args) != 2 {
		t.Fatalf("len(buildCatalogFilterQuerySpec args) = %d, want 2", len(spec.Args))
	}

	wantPatterns := []string{
		sqlLikeContainsPattern("fox"),
		sqlLikeContainsPattern("kmsp"),
	}
	for i, wantPattern := range wantPatterns {
		gotPattern, ok := catalogArgs[i].(string)
		if !ok {
			t.Fatalf("buildWhere arg[%d] type = %T, want string", i, catalogArgs[i])
		}
		if gotPattern != wantPattern {
			t.Fatalf("buildWhere arg[%d] = %q, want %q", i, gotPattern, wantPattern)
		}
	}
	for i, wantPattern := range wantPatterns {
		gotPattern, ok := spec.Args[i].(string)
		if !ok {
			t.Fatalf("buildCatalogFilterQuerySpec arg[%d] type = %T, want string", i, spec.Args[i])
		}
		if gotPattern != wantPattern {
			t.Fatalf("buildCatalogFilterQuerySpec arg[%d] = %q, want %q", i, gotPattern, wantPattern)
		}
	}
}

func TestCatalogSearchParserSupportsIncludeExcludeTokens(t *testing.T) {
	spec := parseCatalogSearchSpec("  FOX   -spanish   !720p   fox !720p  -SPANISH ")
	if len(spec.Includes) != 1 {
		t.Fatalf("len(spec.Includes) = %d, want 1", len(spec.Includes))
	}
	if spec.Includes[0] != "fox" {
		t.Fatalf("spec.Includes[0] = %q, want fox", spec.Includes[0])
	}
	if len(spec.Excludes) != 2 {
		t.Fatalf("len(spec.Excludes) = %d, want 2", len(spec.Excludes))
	}
	if spec.Excludes[0] != "spanish" || spec.Excludes[1] != "720p" {
		t.Fatalf("spec.Excludes = %#v, want [spanish 720p]", spec.Excludes)
	}
}

func TestCatalogSearchParserSupportsOrDisjuncts(t *testing.T) {
	expr := parseCatalogSearchExpr(" FOX kmsp | nbc -spanish OR !720p ")
	if len(expr.Disjuncts) != 3 {
		t.Fatalf("len(expr.Disjuncts) = %d, want 3", len(expr.Disjuncts))
	}

	first := expr.Disjuncts[0]
	if len(first.Includes) != 2 || first.Includes[0] != "fox" || first.Includes[1] != "kmsp" {
		t.Fatalf("first includes = %#v, want [fox kmsp]", first.Includes)
	}
	if len(first.Excludes) != 0 {
		t.Fatalf("first excludes = %#v, want empty", first.Excludes)
	}

	second := expr.Disjuncts[1]
	if len(second.Includes) != 1 || second.Includes[0] != "nbc" {
		t.Fatalf("second includes = %#v, want [nbc]", second.Includes)
	}
	if len(second.Excludes) != 1 || second.Excludes[0] != "spanish" {
		t.Fatalf("second excludes = %#v, want [spanish]", second.Excludes)
	}

	third := expr.Disjuncts[2]
	if len(third.Includes) != 0 {
		t.Fatalf("third includes = %#v, want empty", third.Includes)
	}
	if len(third.Excludes) != 1 || third.Excludes[0] != "720p" {
		t.Fatalf("third excludes = %#v, want [720p]", third.Excludes)
	}
}

func TestCatalogSearchParserSupportsPerDisjunctPunctuationFallback(t *testing.T) {
	expr := parseCatalogSearchExpr("% | -_")
	if len(expr.Disjuncts) != 2 {
		t.Fatalf("len(expr.Disjuncts) = %d, want 2", len(expr.Disjuncts))
	}
	if len(expr.Disjuncts[0].Includes) != 1 || expr.Disjuncts[0].Includes[0] != "%" {
		t.Fatalf("first includes = %#v, want [%%]", expr.Disjuncts[0].Includes)
	}
	if len(expr.Disjuncts[1].Excludes) != 1 || expr.Disjuncts[1].Excludes[0] != "_" {
		t.Fatalf("second excludes = %#v, want [_]", expr.Disjuncts[1].Excludes)
	}
}

func TestCatalogSearchParserBoundsDisjunctAndTermGrowth(t *testing.T) {
	expr := parseCatalogSearchExpr(strings.Join([]string{
		"alpha beta gamma",
		"delta epsilon zeta",
		"eta theta iota",
		"kappa lambda mu",
		"nu xi omicron",
	}, " | "))

	// maxCatalogSearchTerms is a global cap across all OR disjuncts.
	if len(expr.Disjuncts) != 4 {
		t.Fatalf("len(expr.Disjuncts) = %d, want 4 after global term cap", len(expr.Disjuncts))
	}
	for i, disjunct := range expr.Disjuncts {
		if len(disjunct.Includes) != 3 {
			t.Fatalf("len(disjunct %d includes) = %d, want 3", i, len(disjunct.Includes))
		}
	}

	expr = parseCatalogSearchExpr("a|b|c|d|e|f|g|h")
	if len(expr.Disjuncts) != maxCatalogSearchDisjuncts {
		t.Fatalf("len(expr.Disjuncts) = %d, want disjunct cap %d", len(expr.Disjuncts), maxCatalogSearchDisjuncts)
	}
}

func TestCatalogSearchParserSupportsExclusionWildcardFallback(t *testing.T) {
	spec := parseCatalogSearchSpec("!%")
	if len(spec.Includes) != 0 {
		t.Fatalf("len(spec.Includes) = %d, want 0", len(spec.Includes))
	}
	if len(spec.Excludes) != 1 {
		t.Fatalf("len(spec.Excludes) = %d, want 1", len(spec.Excludes))
	}
	if spec.Excludes[0] != "%" {
		t.Fatalf("spec.Excludes[0] = %q, want %%", spec.Excludes[0])
	}
}

func TestCatalogSearchBuildersSupportIncludeExcludeSemantics(t *testing.T) {
	search := "FOX -spanish !720p"

	catalogWhere, catalogArgs, err := buildWhere(playlist.Query{Search: search})
	if err != nil {
		t.Fatalf("buildWhere(include/exclude search) error = %v", err)
	}
	spec, err := buildCatalogFilterQuerySpec(nil, search, false)
	if err != nil {
		t.Fatalf("buildCatalogFilterQuerySpec(include/exclude search) error = %v", err)
	}

	if got := strings.Count(catalogWhere, catalogSearchLikeClause); got != 1 {
		t.Fatalf("buildWhere LIKE predicate count = %d, want 1", got)
	}
	if got := strings.Count(catalogWhere, catalogSearchNotLikeClause); got != 2 {
		t.Fatalf("buildWhere NOT LIKE predicate count = %d, want 2", got)
	}
	if got := strings.Count(spec.WhereClause, catalogSearchLikeClause); got != 1 {
		t.Fatalf("buildCatalogFilterQuerySpec LIKE predicate count = %d, want 1", got)
	}
	if got := strings.Count(spec.WhereClause, catalogSearchNotLikeClause); got != 2 {
		t.Fatalf("buildCatalogFilterQuerySpec NOT LIKE predicate count = %d, want 2", got)
	}

	wantPatterns := []string{
		sqlLikeContainsPattern("fox"),
		sqlLikeContainsPattern("spanish"),
		sqlLikeContainsPattern("720p"),
	}
	if len(catalogArgs) != len(wantPatterns) {
		t.Fatalf("len(buildWhere args) = %d, want %d", len(catalogArgs), len(wantPatterns))
	}
	if len(spec.Args) != len(wantPatterns) {
		t.Fatalf("len(buildCatalogFilterQuerySpec args) = %d, want %d", len(spec.Args), len(wantPatterns))
	}
	for i, wantPattern := range wantPatterns {
		gotPattern, ok := catalogArgs[i].(string)
		if !ok {
			t.Fatalf("buildWhere arg[%d] type = %T, want string", i, catalogArgs[i])
		}
		if gotPattern != wantPattern {
			t.Fatalf("buildWhere arg[%d] = %q, want %q", i, gotPattern, wantPattern)
		}
	}
	for i, wantPattern := range wantPatterns {
		gotPattern, ok := spec.Args[i].(string)
		if !ok {
			t.Fatalf("buildCatalogFilterQuerySpec arg[%d] type = %T, want string", i, spec.Args[i])
		}
		if gotPattern != wantPattern {
			t.Fatalf("buildCatalogFilterQuerySpec arg[%d] = %q, want %q", i, gotPattern, wantPattern)
		}
	}
}

func TestBuildCatalogFilterQuerySpecSupportsMultiGroupInclude(t *testing.T) {
	spec, err := buildCatalogFilterQuerySpec([]string{" Sports ", "US News", "sports"}, "fox", false)
	if err != nil {
		t.Fatalf("buildCatalogFilterQuerySpec(multigroup) error = %v", err)
	}

	if !strings.Contains(spec.WhereClause, "group_name IN (?,?)") {
		t.Fatalf("WhereClause = %q, want group_name IN (?,?) predicate", spec.WhereClause)
	}
	if spec.OrderIndex != playlistActiveNameOrderIndex {
		t.Fatalf("OrderIndex = %q, want %q for multi-group filter ordering", spec.OrderIndex, playlistActiveNameOrderIndex)
	}
	if len(spec.Args) != 3 {
		t.Fatalf("len(spec.Args) = %d, want 3 (2 groups + 1 search term)", len(spec.Args))
	}
	if first, ok := spec.Args[0].(string); !ok || first != "Sports" {
		t.Fatalf("spec.Args[0] = %#v, want %q", spec.Args[0], "Sports")
	}
	if second, ok := spec.Args[1].(string); !ok || second != "US News" {
		t.Fatalf("spec.Args[1] = %#v, want %q", spec.Args[1], "US News")
	}
	if like, ok := spec.Args[2].(string); !ok || like != sqlLikeContainsPattern("fox") {
		t.Fatalf("spec.Args[2] = %#v, want %q", spec.Args[2], sqlLikeContainsPattern("fox"))
	}
}

func TestCatalogSearchBuildersSupportOrDisjunctSemantics(t *testing.T) {
	search := "fox | nbc -spanish"

	catalogWhere, catalogArgs, err := buildWhere(playlist.Query{Search: search})
	if err != nil {
		t.Fatalf("buildWhere(or search) error = %v", err)
	}
	spec, err := buildCatalogFilterQuerySpec(nil, search, false)
	if err != nil {
		t.Fatalf("buildCatalogFilterQuerySpec(or search) error = %v", err)
	}

	if got := strings.Count(catalogWhere, catalogSearchLikeClause); got != 2 {
		t.Fatalf("buildWhere LIKE predicate count = %d, want 2", got)
	}
	if got := strings.Count(catalogWhere, catalogSearchNotLikeClause); got != 1 {
		t.Fatalf("buildWhere NOT LIKE predicate count = %d, want 1", got)
	}
	if !strings.Contains(catalogWhere, " OR ") {
		t.Fatalf("buildWhere = %q, want OR disjunct clause", catalogWhere)
	}
	if !strings.Contains(catalogWhere, "(("+catalogSearchLikeClause+") OR ("+catalogSearchLikeClause+" AND "+catalogSearchNotLikeClause+"))") {
		t.Fatalf("buildWhere = %q, want parenthesized OR-of-AND clause", catalogWhere)
	}

	if got := strings.Count(spec.WhereClause, catalogSearchLikeClause); got != 2 {
		t.Fatalf("buildCatalogFilterQuerySpec LIKE predicate count = %d, want 2", got)
	}
	if got := strings.Count(spec.WhereClause, catalogSearchNotLikeClause); got != 1 {
		t.Fatalf("buildCatalogFilterQuerySpec NOT LIKE predicate count = %d, want 1", got)
	}
	if !strings.Contains(spec.WhereClause, " OR ") {
		t.Fatalf("buildCatalogFilterQuerySpec WhereClause = %q, want OR disjunct clause", spec.WhereClause)
	}

	wantPatterns := []string{
		sqlLikeContainsPattern("fox"),
		sqlLikeContainsPattern("nbc"),
		sqlLikeContainsPattern("spanish"),
	}
	if len(catalogArgs) != len(wantPatterns) {
		t.Fatalf("len(buildWhere args) = %d, want %d", len(catalogArgs), len(wantPatterns))
	}
	if len(spec.Args) != len(wantPatterns) {
		t.Fatalf("len(buildCatalogFilterQuerySpec args) = %d, want %d", len(spec.Args), len(wantPatterns))
	}
	for i, wantPattern := range wantPatterns {
		gotPattern, ok := catalogArgs[i].(string)
		if !ok {
			t.Fatalf("buildWhere arg[%d] type = %T, want string", i, catalogArgs[i])
		}
		if gotPattern != wantPattern {
			t.Fatalf("buildWhere arg[%d] = %q, want %q", i, gotPattern, wantPattern)
		}
	}
	for i, wantPattern := range wantPatterns {
		gotPattern, ok := spec.Args[i].(string)
		if !ok {
			t.Fatalf("buildCatalogFilterQuerySpec arg[%d] type = %T, want string", i, spec.Args[i])
		}
		if gotPattern != wantPattern {
			t.Fatalf("buildCatalogFilterQuerySpec arg[%d] = %q, want %q", i, gotPattern, wantPattern)
		}
	}
}

func TestCatalogSearchRegexModeMatchesWholeNamePatterns(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:regex:fox",
			ChannelKey: "name:fox 9 kmsp",
			Name:       "FOX 9 KMSP",
			Group:      "News",
			StreamURL:  "http://example.com/regex-fox.ts",
		},
		{
			ItemKey:    "src:regex:nbc",
			ChannelKey: "name:nbc chicago",
			Name:       "NBC Chicago",
			Group:      "News",
			StreamURL:  "http://example.com/regex-nbc.ts",
		},
		{
			ItemKey:    "src:regex:nbc-spanish",
			ChannelKey: "name:nbc chicago spanish",
			Name:       "NBC Chicago Spanish",
			Group:      "News",
			StreamURL:  "http://example.com/regex-nbc-spanish.ts",
		},
		{
			ItemKey:    "src:regex:other",
			ChannelKey: "name:other news",
			Name:       "Other News",
			Group:      "News",
			StreamURL:  "http://example.com/regex-other.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	regexItems, regexTotal, err := store.ListCatalogItems(ctx, playlist.Query{
		Group:       "News",
		Search:      `fox\s+9\s+kmsp|nbc chicago$`,
		SearchRegex: true,
		Limit:       20,
		Offset:      0,
	})
	if err != nil {
		t.Fatalf("ListCatalogItems(regex mode) error = %v", err)
	}
	if regexTotal != 2 {
		t.Fatalf("regex total = %d, want 2", regexTotal)
	}
	wantKeys := []string{"src:regex:fox", "src:regex:nbc"}
	if got := itemKeys(regexItems); strings.Join(got, ",") != strings.Join(wantKeys, ",") {
		t.Fatalf("regex mode keys = %#v, want %#v", got, wantKeys)
	}

	regexFilterKeys, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{"News"}, `fox\s+9\s+kmsp|nbc chicago$`, true)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(regex mode) error = %v", err)
	}
	if strings.Join(regexFilterKeys, ",") != strings.Join(wantKeys, ",") {
		t.Fatalf("regex filter keys = %#v, want %#v", regexFilterKeys, wantKeys)
	}
}

func TestCatalogSearchRegexModeSupportsMixedOrAndExclusion(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:regex:mixed:fox9",
			ChannelKey: "name:fox 9 kmsp",
			Name:       "FOX 9 KMSP",
			Group:      "News",
			StreamURL:  "http://example.com/mixed-fox9.ts",
		},
		{
			ItemKey:    "src:regex:mixed:fox32",
			ChannelKey: "name:fox 32 wfld",
			Name:       "FOX 32 WFLD",
			Group:      "News",
			StreamURL:  "http://example.com/mixed-fox32.ts",
		},
		{
			ItemKey:    "src:regex:mixed:nbc",
			ChannelKey: "name:nbc chicago",
			Name:       "NBC Chicago",
			Group:      "News",
			StreamURL:  "http://example.com/mixed-nbc.ts",
		},
		{
			ItemKey:    "src:regex:mixed:nbc-spanish",
			ChannelKey: "name:nbc chicago spanish",
			Name:       "NBC Chicago Spanish",
			Group:      "News",
			StreamURL:  "http://example.com/mixed-nbc-spanish.ts",
		},
		{
			ItemKey:    "src:regex:mixed:foxsports",
			ChannelKey: "name:fox sports",
			Name:       "FOX Sports",
			Group:      "News",
			StreamURL:  "http://example.com/mixed-foxsports.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	search := `fox\s+\d+|nbc chicago$`
	items, total, err := store.ListCatalogItems(ctx, playlist.Query{
		Group:       "News",
		Search:      search,
		SearchRegex: true,
		Limit:       20,
		Offset:      0,
	})
	if err != nil {
		t.Fatalf("ListCatalogItems(regex mixed) error = %v", err)
	}
	if total != 3 || len(items) != 3 {
		t.Fatalf("ListCatalogItems(regex mixed) total/len = %d/%d, want 3/3", total, len(items))
	}
	wantKeys := []string{"src:regex:mixed:fox32", "src:regex:mixed:fox9", "src:regex:mixed:nbc"}
	if got := itemKeys(items); strings.Join(got, ",") != strings.Join(wantKeys, ",") {
		t.Fatalf("ListCatalogItems(regex mixed) keys = %#v, want %#v", got, wantKeys)
	}

	filterKeys, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{"News"}, search, true)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(regex mixed) error = %v", err)
	}
	if strings.Join(filterKeys, ",") != strings.Join(wantKeys, ",") {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(regex mixed) keys = %#v, want %#v", filterKeys, wantKeys)
	}
}

func TestCatalogSearchRegexModeValidationRejectsInvalidAndOverlongPatterns(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:regex:validation:one",
			ChannelKey: "name:validation one",
			Name:       "Validation One",
			Group:      "News",
			StreamURL:  "http://example.com/validation-one.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	_, _, err = store.ListCatalogItems(ctx, playlist.Query{
		Group:       "News",
		Search:      "([",
		SearchRegex: true,
		Limit:       20,
		Offset:      0,
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "invalid regex pattern") {
		t.Fatalf("ListCatalogItems(invalid regex) error = %v, want invalid regex pattern validation error", err)
	}

	_, err = store.ListActiveItemKeysByCatalogFilter(ctx, []string{"News"}, "([", true)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "invalid regex pattern") {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(invalid regex) error = %v, want invalid regex pattern validation error", err)
	}

	overlong := strings.Repeat("a", maxCatalogRegexPatternRunes+1)
	_, _, err = store.ListCatalogItems(ctx, playlist.Query{
		Group:       "News",
		Search:      overlong,
		SearchRegex: true,
		Limit:       20,
		Offset:      0,
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "pattern length exceeds") {
		t.Fatalf("ListCatalogItems(overlong regex) error = %v, want pattern length validation error", err)
	}
}

func TestListItemsIgnoresMalformedAttrsJSON(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const itemKey = "src:news:primary"
	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    itemKey,
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
			Attrs: map[string]string{
				"group-title": "News",
			},
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	if _, err := store.db.ExecContext(ctx, `UPDATE playlist_items SET attrs_json = ? WHERE item_key = ?`, "{not-json", itemKey); err != nil {
		t.Fatalf("inject malformed attrs_json error = %v", err)
	}

	items, total, err := store.ListItems(ctx, playlist.Query{Limit: 10, Offset: 0})
	if err != nil {
		t.Fatalf("ListItems() error = %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("ListItems() total/len = %d/%d, want 1/1", total, len(items))
	}
	if items[0].ItemKey != itemKey {
		t.Fatalf("ListItems() item key = %q, want %q", items[0].ItemKey, itemKey)
	}
}

func TestListCatalogItemsProjectionParity(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:fox9:0001",
			ChannelKey: "name:fox 9 kmsp",
			Name:       "FOX 9 KMSP",
			Group:      "Catalog",
			StreamURL:  "http://example.com/fox9.ts",
			TVGID:      "fox9",
			TVGLogo:    "http://img.example.com/fox9.png",
		},
		{
			ItemKey:    "src:legacy:0004",
			ChannelKey: "name:legacy feed",
			Name:       "Legacy Feed",
			Group:      "Catalog",
			StreamURL:  "http://example.com/legacy.ts",
			TVGID:      "legacy",
			TVGLogo:    "http://img.example.com/legacy.png",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:fox9:0001",
			ChannelKey: "name:fox 9 kmsp",
			Name:       "FOX 9 KMSP",
			Group:      "Catalog",
			StreamURL:  "http://example.com/fox9.ts",
			TVGID:      "fox9",
			TVGLogo:    "http://img.example.com/fox9.png",
		},
		{
			ItemKey:    "src:fox32:0002",
			ChannelKey: "name:fox 32 wfld",
			Name:       "FOX 32 WFLD",
			Group:      "Catalog",
			StreamURL:  "http://example.com/fox32.ts",
			TVGID:      "fox32",
			TVGLogo:    "http://img.example.com/fox32.png",
		},
		{
			ItemKey:    "src:sports:0003",
			ChannelKey: "name:sports now",
			Name:       "Sports Now",
			Group:      "Catalog",
			StreamURL:  "http://example.com/sports.ts",
			TVGID:      "sports",
			TVGLogo:    "http://img.example.com/sports.png",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(refreshed) error = %v", err)
	}

	cases := []playlist.Query{
		{Limit: 10, Offset: 0},
		{Limit: 2, Offset: 1},
		{Group: "Catalog", Limit: 10, Offset: 0},
		{Search: "fox kmsp", Limit: 10, Offset: 0},
	}
	for _, q := range cases {
		full, fullTotal, err := store.ListItems(ctx, q)
		if err != nil {
			t.Fatalf("ListItems(%+v) error = %v", q, err)
		}
		projected, projectedTotal, err := store.ListCatalogItems(ctx, q)
		if err != nil {
			t.Fatalf("ListCatalogItems(%+v) error = %v", q, err)
		}
		if fullTotal != projectedTotal {
			t.Fatalf("query %+v totals mismatch full/projected = %d/%d", q, fullTotal, projectedTotal)
		}
		if len(full) != len(projected) {
			t.Fatalf("query %+v len mismatch full/projected = %d/%d", q, len(full), len(projected))
		}
		for i := range full {
			if full[i].ItemKey != projected[i].ItemKey {
				t.Fatalf("query %+v item[%d] item_key mismatch full/projected = %q/%q", q, i, full[i].ItemKey, projected[i].ItemKey)
			}
			if full[i].ChannelKey != projected[i].ChannelKey {
				t.Fatalf("query %+v item[%d] channel_key mismatch full/projected = %q/%q", q, i, full[i].ChannelKey, projected[i].ChannelKey)
			}
			if full[i].Name != projected[i].Name {
				t.Fatalf("query %+v item[%d] name mismatch full/projected = %q/%q", q, i, full[i].Name, projected[i].Name)
			}
			if full[i].Group != projected[i].Group {
				t.Fatalf("query %+v item[%d] group mismatch full/projected = %q/%q", q, i, full[i].Group, projected[i].Group)
			}
			if full[i].StreamURL != projected[i].StreamURL {
				t.Fatalf("query %+v item[%d] stream_url mismatch full/projected = %q/%q", q, i, full[i].StreamURL, projected[i].StreamURL)
			}
			if full[i].TVGLogo != projected[i].TVGLogo {
				t.Fatalf("query %+v item[%d] logo mismatch full/projected = %q/%q", q, i, full[i].TVGLogo, projected[i].TVGLogo)
			}
			if full[i].Active != projected[i].Active {
				t.Fatalf("query %+v item[%d] active mismatch full/projected = %v/%v", q, i, full[i].Active, projected[i].Active)
			}
		}
	}
}

func TestListCatalogItemsSupportsMultiGroupFilters(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:multigroup:news:1",
			ChannelKey: "name:multigroup news",
			Name:       "MultiGroup News",
			Group:      "News",
			StreamURL:  "http://example.com/multigroup-news.ts",
		},
		{
			ItemKey:    "src:multigroup:sports:1",
			ChannelKey: "name:multigroup sports",
			Name:       "MultiGroup Sports",
			Group:      "Sports",
			StreamURL:  "http://example.com/multigroup-sports.ts",
		},
		{
			ItemKey:    "src:multigroup:movies:1",
			ChannelKey: "name:multigroup movies",
			Name:       "MultiGroup Movies",
			Group:      "Movies",
			StreamURL:  "http://example.com/multigroup-movies.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	filtered, total, err := store.ListCatalogItems(ctx, playlist.Query{
		GroupNames: []string{"Sports", "News", "sports"},
		Limit:      10,
		Offset:     0,
	})
	if err != nil {
		t.Fatalf("ListCatalogItems(multigroup) error = %v", err)
	}
	if total != 2 || len(filtered) != 2 {
		t.Fatalf("ListCatalogItems(multigroup) total/len = %d/%d, want 2/2", total, len(filtered))
	}
	gotKeys := []string{filtered[0].ItemKey, filtered[1].ItemKey}
	wantKeys := []string{"src:multigroup:news:1", "src:multigroup:sports:1"}
	if gotKeys[0] != wantKeys[0] || gotKeys[1] != wantKeys[1] {
		t.Fatalf("ListCatalogItems(multigroup) keys = %#v, want %#v", gotKeys, wantKeys)
	}

	csvCompatible, totalCSV, err := store.ListCatalogItems(ctx, playlist.Query{
		Group:      "Movies",
		GroupNames: []string{"News", "Movies"},
		Limit:      10,
		Offset:     0,
	})
	if err != nil {
		t.Fatalf("ListCatalogItems(multigroup legacy alias) error = %v", err)
	}
	if totalCSV != 2 || len(csvCompatible) != 2 {
		t.Fatalf("ListCatalogItems(multigroup legacy alias) total/len = %d/%d, want 2/2", totalCSV, len(csvCompatible))
	}
}

func TestListCatalogItemsSupportsSourceIDFilters(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Secondary Catalog Source",
		PlaylistURL: "http://example.com/secondary-catalog.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}
	primary, err := store.GetPlaylistSource(ctx, primaryPlaylistSourceID)
	if err != nil {
		t.Fatalf("GetPlaylistSource(primary) error = %v", err)
	}

	if err := store.UpsertPlaylistItemsForSource(ctx, primaryPlaylistSourceID, []playlist.Item{
		{
			ItemKey:    "src:sourcefilter:primary:news",
			ChannelKey: "name:sourcefilter-primary-news",
			Name:       "Primary News",
			Group:      "News",
			StreamURL:  "http://example.com/sourcefilter-primary-news.ts",
		},
		{
			ItemKey:    "src:sourcefilter:primary:sports",
			ChannelKey: "name:sourcefilter-primary-sports",
			Name:       "Primary Sports",
			Group:      "Sports",
			StreamURL:  "http://example.com/sourcefilter-primary-sports.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(primary) error = %v", err)
	}
	if err := store.UpsertPlaylistItemsForSource(ctx, secondary.SourceID, []playlist.Item{
		{
			ItemKey:    "src:sourcefilter:secondary:movies",
			ChannelKey: "name:sourcefilter-secondary-movies",
			Name:       "Secondary Movies",
			Group:      "Movies",
			StreamURL:  "http://example.com/sourcefilter-secondary-movies.ts",
		},
		{
			ItemKey:    "src:sourcefilter:secondary:news",
			ChannelKey: "name:sourcefilter-secondary-news",
			Name:       "Secondary News",
			Group:      "News",
			StreamURL:  "http://example.com/sourcefilter-secondary-news.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(secondary) error = %v", err)
	}

	secondaryOnly, secondaryTotal, err := store.ListCatalogItems(ctx, playlist.Query{
		SourceIDs: []int64{secondary.SourceID},
		Limit:     10,
		Offset:    0,
	})
	if err != nil {
		t.Fatalf("ListCatalogItems(secondary source filter) error = %v", err)
	}
	if secondaryTotal != 2 || len(secondaryOnly) != 2 {
		t.Fatalf("ListCatalogItems(secondary source filter) total/len = %d/%d, want 2/2", secondaryTotal, len(secondaryOnly))
	}
	if got := itemKeys(secondaryOnly); strings.Join(got, ",") != "src:sourcefilter:secondary:movies,src:sourcefilter:secondary:news" {
		t.Fatalf("ListCatalogItems(secondary source filter) keys = %#v, want [src:sourcefilter:secondary:movies src:sourcefilter:secondary:news]", got)
	}
	for _, item := range secondaryOnly {
		if item.PlaylistSourceID != secondary.SourceID {
			t.Fatalf("secondary-only item %q playlist_source_id = %d, want %d", item.ItemKey, item.PlaylistSourceID, secondary.SourceID)
		}
		if item.PlaylistSourceName != secondary.Name {
			t.Fatalf("secondary-only item %q playlist_source_name = %q, want %q", item.ItemKey, item.PlaylistSourceName, secondary.Name)
		}
	}

	combinedNews, combinedNewsTotal, err := store.ListCatalogItems(ctx, playlist.Query{
		SourceIDs:  []int64{secondary.SourceID, primaryPlaylistSourceID, secondary.SourceID},
		GroupNames: []string{"News"},
		Limit:      10,
		Offset:     0,
	})
	if err != nil {
		t.Fatalf("ListCatalogItems(combined source+group filter) error = %v", err)
	}
	if combinedNewsTotal != 2 || len(combinedNews) != 2 {
		t.Fatalf("ListCatalogItems(combined source+group filter) total/len = %d/%d, want 2/2", combinedNewsTotal, len(combinedNews))
	}
	if got := itemKeys(combinedNews); strings.Join(got, ",") != "src:sourcefilter:primary:news,src:sourcefilter:secondary:news" {
		t.Fatalf("ListCatalogItems(combined source+group filter) keys = %#v, want [src:sourcefilter:primary:news src:sourcefilter:secondary:news]", got)
	}
	for _, item := range combinedNews {
		switch item.ItemKey {
		case "src:sourcefilter:primary:news":
			if item.PlaylistSourceID != primary.SourceID || item.PlaylistSourceName != primary.Name {
				t.Fatalf("combined primary item metadata = (%d,%q), want (%d,%q)", item.PlaylistSourceID, item.PlaylistSourceName, primary.SourceID, primary.Name)
			}
		case "src:sourcefilter:secondary:news":
			if item.PlaylistSourceID != secondary.SourceID || item.PlaylistSourceName != secondary.Name {
				t.Fatalf("combined secondary item metadata = (%d,%q), want (%d,%q)", item.PlaylistSourceID, item.PlaylistSourceName, secondary.SourceID, secondary.Name)
			}
		default:
			t.Fatalf("unexpected combined item key %q", item.ItemKey)
		}
	}

	allItems, allTotal, err := store.ListCatalogItems(ctx, playlist.Query{
		SourceIDs: []int64{},
		Limit:     10,
		Offset:    0,
	})
	if err != nil {
		t.Fatalf("ListCatalogItems(empty source filter) error = %v", err)
	}
	if allTotal != 4 || len(allItems) != 4 {
		t.Fatalf("ListCatalogItems(empty source filter) total/len = %d/%d, want 4/4", allTotal, len(allItems))
	}

	if _, _, err := store.ListCatalogItems(ctx, playlist.Query{
		SourceIDs: []int64{0},
		Limit:     10,
		Offset:    0,
	}); err == nil {
		t.Fatal("ListCatalogItems(invalid source_ids) error = nil, want validation error")
	}
}

func TestListCatalogItemsStableOrderingForNameTies(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:tiebreak:03",
			ChannelKey: "tvg:tiebreak:03",
			Name:       "Tie Break Feed",
			Group:      "Tie Group",
			StreamURL:  "http://example.com/tie-03.ts",
		},
		{
			ItemKey:    "src:tiebreak:01",
			ChannelKey: "tvg:tiebreak:01",
			Name:       "Tie Break Feed",
			Group:      "Tie Group",
			StreamURL:  "http://example.com/tie-01.ts",
		},
		{
			ItemKey:    "src:tiebreak:02",
			ChannelKey: "tvg:tiebreak:02",
			Name:       "Tie Break Feed",
			Group:      "Tie Group",
			StreamURL:  "http://example.com/tie-02.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	pageOne, total, err := store.ListCatalogItems(ctx, playlist.Query{Limit: 2, Offset: 0})
	if err != nil {
		t.Fatalf("ListCatalogItems(page one) error = %v", err)
	}
	if total != 3 || len(pageOne) != 2 {
		t.Fatalf("ListCatalogItems(page one) total/len = %d/%d, want 3/2", total, len(pageOne))
	}
	if pageOne[0].ItemKey != "src:tiebreak:01" || pageOne[1].ItemKey != "src:tiebreak:02" {
		t.Fatalf("page one item keys = %#v, want [src:tiebreak:01 src:tiebreak:02]", []string{pageOne[0].ItemKey, pageOne[1].ItemKey})
	}

	pageTwo, total, err := store.ListCatalogItems(ctx, playlist.Query{Limit: 2, Offset: 2})
	if err != nil {
		t.Fatalf("ListCatalogItems(page two) error = %v", err)
	}
	if total != 3 || len(pageTwo) != 1 {
		t.Fatalf("ListCatalogItems(page two) total/len = %d/%d, want 3/1", total, len(pageTwo))
	}
	if pageTwo[0].ItemKey != "src:tiebreak:03" {
		t.Fatalf("page two item key = %q, want src:tiebreak:03", pageTwo[0].ItemKey)
	}
}

func TestListCatalogItemsHighOffsetLargeCatalog(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const itemCount = 50000
	const pageLimit = 100
	items := generateCatalogBenchmarkItems(itemCount)
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	startFirst := time.Now()
	gotFirst, totalFirst, err := store.ListCatalogItems(ctx, playlist.Query{Limit: pageLimit, Offset: 0})
	firstDur := time.Since(startFirst)
	if err != nil {
		t.Fatalf("ListCatalogItems(offset=0) error = %v", err)
	}
	if totalFirst != itemCount || len(gotFirst) != pageLimit {
		t.Fatalf("ListCatalogItems(offset=0) total/len = %d/%d, want %d/%d", totalFirst, len(gotFirst), itemCount, pageLimit)
	}

	startHigh := time.Now()
	gotHigh, totalHigh, err := store.ListCatalogItems(ctx, playlist.Query{Limit: pageLimit, Offset: itemCount - pageLimit})
	highDur := time.Since(startHigh)
	if err != nil {
		t.Fatalf("ListCatalogItems(high offset) error = %v", err)
	}
	if totalHigh != itemCount || len(gotHigh) != pageLimit {
		t.Fatalf("ListCatalogItems(high offset) total/len = %d/%d, want %d/%d", totalHigh, len(gotHigh), itemCount, pageLimit)
	}
	t.Logf("high-offset query timings offset=0:%s offset=%d:%s", firstDur, itemCount-pageLimit, highDur)
}

func TestListCatalogItemsQueryPlanAvoidsTempBTreeAtHighOffset(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(5000)); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	planDetails, err := explainCatalogListQueryPlan(ctx, store.db, playlist.Query{Limit: 100, Offset: 4900})
	if err != nil {
		t.Fatalf("explainCatalogListQueryPlan() error = %v", err)
	}
	if len(planDetails) == 0 {
		t.Fatal("expected query-plan rows for catalog list query")
	}
	t.Logf("catalog list query plan: %s", strings.Join(planDetails, " | "))
	if !planContains(planDetails, "idx_playlist_active_group_name_name_item") {
		t.Fatalf("plan details missing ordered pagination index: %q", strings.Join(planDetails, " | "))
	}
	if planContains(planDetails, "use temp b-tree for order by") {
		t.Fatalf("plan details unexpectedly include temp b-tree sort: %q", strings.Join(planDetails, " | "))
	}
}

func TestListCatalogItemsSourceFilteredQueryPlanUsesSourceSelectiveOrderedIndex(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Plan Secondary",
		PlaylistURL: "http://example.com/plan-secondary.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}

	primaryItems := generateCatalogBenchmarkItems(12000)
	if err := store.UpsertPlaylistItemsForSource(ctx, primaryPlaylistSourceID, primaryItems); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(primary) error = %v", err)
	}

	secondaryItems := make([]playlist.Item, 0, 120)
	for i := 0; i < 120; i++ {
		secondaryItems = append(secondaryItems, playlist.Item{
			ItemKey:    fmt.Sprintf("src:source-plan:secondary:%04d", i),
			ChannelKey: fmt.Sprintf("name:source-plan-secondary-%04d", i),
			Name:       fmt.Sprintf("Secondary Plan Channel %04d", i),
			Group:      fmt.Sprintf("Plan Group %02d", i%8),
			StreamURL:  fmt.Sprintf("http://example.com/plan/secondary/%04d.ts", i),
		})
	}
	if err := store.UpsertPlaylistItemsForSource(ctx, secondary.SourceID, secondaryItems); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(secondary) error = %v", err)
	}

	planDetails, err := explainCatalogListQueryPlan(ctx, store.db, playlist.Query{
		SourceIDs: []int64{secondary.SourceID},
		Limit:     100,
		Offset:    0,
	})
	if err != nil {
		t.Fatalf("explainCatalogListQueryPlan(source filtered) error = %v", err)
	}
	if len(planDetails) == 0 {
		t.Fatal("expected source-filtered query-plan rows for catalog list query")
	}
	t.Logf("catalog source-filtered query plan: %s", strings.Join(planDetails, " | "))
	if !planContains(planDetails, "idx_playlist_active_source_group_name_name_item") {
		t.Fatalf("source-filtered plan missing source-selective ordered index: %q", strings.Join(planDetails, " | "))
	}
	if planContains(planDetails, "use temp b-tree for order by") {
		t.Fatalf("source-filtered plan unexpectedly includes temp b-tree sort: %q", strings.Join(planDetails, " | "))
	}
}

func BenchmarkListItemsHotPath(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const itemCount = 500
	items := make([]playlist.Item, 0, itemCount)
	for i := 0; i < itemCount; i++ {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:bench:item:%04d", i),
			ChannelKey: fmt.Sprintf("tvg:bench:%04d", i),
			Name:       fmt.Sprintf("Benchmark Item %04d", i),
			Group:      "Benchmark",
			StreamURL:  fmt.Sprintf("http://example.com/bench-%04d.ts", i),
			Attrs: map[string]string{
				"group-title": "Benchmark",
				"quality":     "hd",
				"provider":    "bench",
			},
		})
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		got, total, err := store.ListCatalogItems(ctx, playlist.Query{
			Limit:  itemCount,
			Offset: 0,
		})
		if err != nil {
			b.Fatalf("ListCatalogItems() error = %v", err)
		}
		if total != itemCount || len(got) != itemCount {
			b.Fatalf("ListCatalogItems() total/len = %d/%d, want %d/%d", total, len(got), itemCount, itemCount)
		}
	}
}

func BenchmarkListGroupsPagedHotPath(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const groupCount = 20000
	items := make([]playlist.Item, 0, groupCount)
	for i := 0; i < groupCount; i++ {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:bench:group:%05d", i),
			ChannelKey: fmt.Sprintf("name:bench-group-%05d", i),
			Name:       fmt.Sprintf("Bench Group Channel %05d", i),
			Group:      fmt.Sprintf("Bench Group %05d", i),
			StreamURL:  fmt.Sprintf("http://example.com/bench/groups/%05d.ts", i),
		})
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	benchCases := []struct {
		name          string
		limit         int
		offset        int
		includeCounts bool
	}{
		{name: "counts_offset_0_limit_200", limit: 200, offset: 0, includeCounts: true},
		{name: "names_only_offset_0_limit_200", limit: 200, offset: 0, includeCounts: false},
		{name: "counts_offset_19800_limit_200", limit: 200, offset: 19800, includeCounts: true},
		{name: "names_only_offset_19800_limit_200", limit: 200, offset: 19800, includeCounts: false},
	}

	for _, tc := range benchCases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				rows, total, listErr := store.ListGroupsPaged(ctx, tc.limit, tc.offset, tc.includeCounts)
				if listErr != nil {
					b.Fatalf("ListGroupsPaged() error = %v", listErr)
				}
				if total != groupCount {
					b.Fatalf("ListGroupsPaged() total = %d, want %d", total, groupCount)
				}
				if len(rows) != tc.limit {
					b.Fatalf("len(ListGroupsPaged()) = %d, want %d", len(rows), tc.limit)
				}
			}
		})
	}
}

func BenchmarkListCatalogItemsHighOffset(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const itemCount = 50000
	items := generateCatalogBenchmarkItems(itemCount)
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	benchCases := []struct {
		name string
		q    playlist.Query
	}{
		{
			name: "offset_0_limit_100",
			q: playlist.Query{
				Limit:  100,
				Offset: 0,
			},
		},
		{
			name: "offset_49000_limit_100",
			q: playlist.Query{
				Limit:  100,
				Offset: 49000,
			},
		},
		{
			name: "search_single_term_offset_49000",
			q: playlist.Query{
				Search: "channel",
				Limit:  100,
				Offset: 49000,
			},
		},
		{
			name: "search_multi_term_offset_49000",
			q: playlist.Query{
				Search: "channel 123",
				Limit:  100,
				Offset: 49000,
			},
		},
	}

	for _, tc := range benchCases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _, err := store.ListCatalogItems(ctx, tc.q)
				if err != nil {
					b.Fatalf("ListCatalogItems(%+v) error = %v", tc.q, err)
				}
			}
		})
	}
}

func BenchmarkCatalogSearchLongTokenInput(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const itemCount = 20000
	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(itemCount)); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	search := buildLongCatalogSearchQuery(maxCatalogSearchTerms * 8)

	b.Run("list_catalog_items", func(b *testing.B) {
		q := playlist.Query{
			Search: search,
			Limit:  100,
			Offset: 0,
		}

		if _, _, err := store.ListCatalogItems(ctx, q); err != nil {
			b.Fatalf("ListCatalogItems(warm) error = %v", err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, _, err := store.ListCatalogItems(ctx, q); err != nil {
				b.Fatalf("ListCatalogItems() error = %v", err)
			}
		}
	})

	b.Run("catalog_filter_keys", func(b *testing.B) {
		groupName := "Group 005"

		if _, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{groupName}, search, false); err != nil {
			b.Fatalf("ListActiveItemKeysByCatalogFilter(warm) error = %v", err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{groupName}, search, false); err != nil {
				b.Fatalf("ListActiveItemKeysByCatalogFilter() error = %v", err)
			}
		}
	})
}

func BenchmarkUpsertPlaylistItemsStableSeenSubset(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const (
		itemCount = 50000
		seenCount = 100
	)
	allItems := generateCatalogBenchmarkItems(itemCount)
	seenItems := append([]playlist.Item(nil), allItems[:seenCount]...)

	if err := store.UpsertPlaylistItems(ctx, allItems); err != nil {
		b.Fatalf("UpsertPlaylistItems(allItems) error = %v", err)
	}
	if err := store.UpsertPlaylistItems(ctx, seenItems); err != nil {
		b.Fatalf("UpsertPlaylistItems(seenItems warmup) error = %v", err)
	}
	if err := installInactiveRewriteCounter(ctx, store.db); err != nil {
		b.Fatalf("installInactiveRewriteCounter() error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.UpsertPlaylistItems(ctx, seenItems); err != nil {
			b.Fatalf("UpsertPlaylistItems(seenItems) error = %v", err)
		}
	}
	b.StopTimer()

	rewriteCount, err := inactiveRewriteCount(ctx, store.db)
	if err != nil {
		b.Fatalf("inactiveRewriteCount() error = %v", err)
	}
	if rewriteCount != 0 {
		b.Fatalf("inactive stale-row rewrite count = %d, want 0", rewriteCount)
	}
}

func BenchmarkListItemsFullHotPath(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const itemCount = 500
	items := make([]playlist.Item, 0, itemCount)
	for i := 0; i < itemCount; i++ {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:bench:item:%04d", i),
			ChannelKey: fmt.Sprintf("tvg:bench:%04d", i),
			Name:       fmt.Sprintf("Benchmark Item %04d", i),
			Group:      "Benchmark",
			StreamURL:  fmt.Sprintf("http://example.com/bench-%04d.ts", i),
			Attrs: map[string]string{
				"group-title": "Benchmark",
				"quality":     "hd",
				"provider":    "bench",
			},
		})
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		got, total, err := store.ListItems(ctx, playlist.Query{
			Limit:  itemCount,
			Offset: 0,
		})
		if err != nil {
			b.Fatalf("ListItems() error = %v", err)
		}
		if total != itemCount || len(got) != itemCount {
			b.Fatalf("ListItems() total/len = %d/%d, want %d/%d", total, len(got), itemCount, itemCount)
		}
	}
}

func TestOpenAddsMissingChannelSourceProfileColumnsForLegacyDB(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy-profile-schema.db")

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
		CREATE TABLE IF NOT EXISTS channel_sources (
		  source_id         INTEGER PRIMARY KEY AUTOINCREMENT,
		  channel_id        INTEGER NOT NULL,
		  item_key          TEXT NOT NULL,
		  priority_index    INTEGER NOT NULL,
		  enabled           INTEGER NOT NULL DEFAULT 1,
		  association_type  TEXT NOT NULL DEFAULT 'manual',
		  last_ok_at        INTEGER,
		  last_fail_at      INTEGER,
		  last_fail_reason  TEXT,
		  success_count     INTEGER NOT NULL DEFAULT 0,
		  fail_count        INTEGER NOT NULL DEFAULT 0,
		  cooldown_until    INTEGER NOT NULL DEFAULT 0,
		  created_at        INTEGER NOT NULL,
		  updated_at        INTEGER NOT NULL
		);
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

	requiredCols := []string{
		"last_probe_at",
		"profile_width",
		"profile_height",
		"profile_fps",
		"profile_video_codec",
		"profile_audio_codec",
		"profile_bitrate_bps",
	}
	for _, col := range requiredCols {
		exists, err := store.columnExists(ctx, "channel_sources", col)
		if err != nil {
			t.Fatalf("columnExists(channel_sources.%s) error = %v", col, err)
		}
		if !exists {
			t.Fatalf("channel_sources missing expected column %q after Open()", col)
		}
	}

	requiredPublishedCols := []string{
		"channel_class",
		"dynamic_query_id",
		"dynamic_item_key",
		"dynamic_source_identity",
		"dynamic_sources_enabled",
		"dynamic_group_name",
		"dynamic_group_names_json",
		"dynamic_search_query",
	}
	for _, col := range requiredPublishedCols {
		exists, err := store.columnExists(ctx, "published_channels", col)
		if err != nil {
			t.Fatalf("columnExists(published_channels.%s) error = %v", col, err)
		}
		if !exists {
			t.Fatalf("published_channels missing expected column %q after Open()", col)
		}
	}

	requiredDynamicQueryCols := []string{
		"group_name",
		"group_names_json",
		"search_query",
		"search_regex",
		"next_slot_cursor",
	}
	for _, col := range requiredDynamicQueryCols {
		exists, err := store.columnExists(ctx, "dynamic_channel_queries", col)
		if err != nil {
			t.Fatalf("columnExists(dynamic_channel_queries.%s) error = %v", col, err)
		}
		if !exists {
			t.Fatalf("dynamic_channel_queries missing expected column %q after Open()", col)
		}
	}
}

func TestOpenAppliesRuntimePragmasOnFileBackedDB(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "runtime-pragmas.db")

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(file db) error = %v", err)
	}
	defer store.Close()

	pragmas, err := store.RuntimePragmas(ctx)
	if err != nil {
		t.Fatalf("RuntimePragmas() error = %v", err)
	}

	if pragmas.BusyTimeoutMS != runtimeBusyTimeoutMS {
		t.Fatalf("busy_timeout = %d, want %d", pragmas.BusyTimeoutMS, runtimeBusyTimeoutMS)
	}
	if !strings.EqualFold(pragmas.Synchronous, runtimeSynchronousPragm) {
		t.Fatalf("synchronous = %q, want %q", pragmas.Synchronous, runtimeSynchronousPragm)
	}
	if !strings.EqualFold(pragmas.JournalMode, runtimeJournalModePragm) {
		t.Fatalf("journal_mode = %q, want %q", pragmas.JournalMode, runtimeJournalModePragm)
	}
}

func TestOpenEnsuresCompositeReconcileLookupIndex(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "composite-index.db")

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(file db) error = %v", err)
	}
	defer store.Close()

	exists, err := indexExists(ctx, store.db, "idx_playlist_active_key_name_item")
	if err != nil {
		t.Fatalf("indexExists(idx_playlist_active_key_name_item) error = %v", err)
	}
	if !exists {
		t.Fatal("expected idx_playlist_active_key_name_item to exist after Open()")
	}
}

func TestOpenEnsuresCatalogOrderedPaginationIndex(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "catalog-ordered-pagination-index.db")

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(file db) error = %v", err)
	}
	defer store.Close()

	exists, err := indexExists(ctx, store.db, "idx_playlist_active_group_name_name_item")
	if err != nil {
		t.Fatalf("indexExists(idx_playlist_active_group_name_name_item) error = %v", err)
	}
	if !exists {
		t.Fatal("expected idx_playlist_active_group_name_name_item to exist after Open()")
	}
}

func TestOpenEnsuresCatalogSourceFilteredOrderedPaginationIndex(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "catalog-source-filtered-ordered-pagination-index.db")

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(file db) error = %v", err)
	}
	defer store.Close()

	exists, err := indexExists(ctx, store.db, "idx_playlist_active_source_group_name_name_item")
	if err != nil {
		t.Fatalf("indexExists(idx_playlist_active_source_group_name_name_item) error = %v", err)
	}
	if !exists {
		t.Fatal("expected idx_playlist_active_source_group_name_name_item to exist after Open()")
	}
}

func TestOpenEnsuresCatalogBroadOrderedPaginationIndex(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "catalog-broad-ordered-pagination-index.db")

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(file db) error = %v", err)
	}
	defer store.Close()

	exists, err := indexExists(ctx, store.db, "idx_playlist_active_name_item")
	if err != nil {
		t.Fatalf("indexExists(idx_playlist_active_name_item) error = %v", err)
	}
	if !exists {
		t.Fatal("expected idx_playlist_active_name_item to exist after Open()")
	}
}

func TestOpenAddsMissingStreamMetricCodecColumnsForLegacyDB(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "legacy-stream-metrics.db")

	legacyDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open(legacy) error = %v", err)
	}
	if _, err := legacyDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS stream_metrics (
		  item_key     TEXT PRIMARY KEY,
		  analyzed_at  INTEGER NOT NULL,
		  width        INTEGER,
		  height       INTEGER,
		  fps          REAL,
		  bitrate_bps  INTEGER,
		  variant_bps  INTEGER,
		  score_hint   REAL,
		  error        TEXT
		);
		INSERT INTO stream_metrics (
		  item_key,
		  analyzed_at,
		  width,
		  height,
		  fps,
		  bitrate_bps,
		  variant_bps,
		  score_hint,
		  error
		) VALUES ('src:legacy:1', 1700300000, 1920, 1080, 30, 4000000, 0, 2.5, '');
	`); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("seed legacy metrics schema error = %v", err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatalf("legacyDB.Close() error = %v", err)
	}

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open(legacy db) error = %v", err)
	}
	defer store.Close()

	for _, col := range []string{"video_codec", "audio_codec"} {
		exists, err := store.columnExists(ctx, "stream_metrics", col)
		if err != nil {
			t.Fatalf("columnExists(stream_metrics.%s) error = %v", col, err)
		}
		if !exists {
			t.Fatalf("stream_metrics missing expected column %q after Open()", col)
		}
	}

	metric, err := store.GetStreamMetric(ctx, "src:legacy:1")
	if err != nil {
		t.Fatalf("GetStreamMetric() error = %v", err)
	}
	if metric.VideoCodec != "" {
		t.Fatalf("VideoCodec = %q, want empty default", metric.VideoCodec)
	}
	if metric.AudioCodec != "" {
		t.Fatalf("AudioCodec = %q, want empty default", metric.AudioCodec)
	}
}

func TestListActiveItemKeysByChannelKey(t *testing.T) {
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
			ChannelKey: "TVG:NEWS",
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
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}

	newsKeys, err := store.ListActiveItemKeysByChannelKey(ctx, "tvg:news")
	if err != nil {
		t.Fatalf("ListActiveItemKeysByChannelKey(news) error = %v", err)
	}
	if len(newsKeys) != 2 {
		t.Fatalf("len(newsKeys) = %d, want 2", len(newsKeys))
	}
	if newsKeys[0] != "src:news:backup" || newsKeys[1] != "src:news:primary" {
		t.Fatalf("newsKeys = %#v, want name-sorted keys", newsKeys)
	}

	newsKeysUpper, err := store.ListActiveItemKeysByChannelKey(ctx, "TVG:NEWS")
	if err != nil {
		t.Fatalf("ListActiveItemKeysByChannelKey(news upper) error = %v", err)
	}
	if len(newsKeysUpper) != 2 {
		t.Fatalf("len(newsKeysUpper) = %d, want 2", len(newsKeysUpper))
	}

	sportsKeys, err := store.ListActiveItemKeysByChannelKey(ctx, "tvg:sports")
	if err != nil {
		t.Fatalf("ListActiveItemKeysByChannelKey(sports) error = %v", err)
	}
	if len(sportsKeys) != 1 || sportsKeys[0] != "src:sports:primary" {
		t.Fatalf("sportsKeys = %#v, want [src:sports:primary]", sportsKeys)
	}
}

func TestListActiveItemKeysByCatalogFilter(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:fox9:0001",
			ChannelKey: "name:fox 9 kmsp",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/fox9.ts",
		},
		{
			ItemKey:    "src:fox9:0002",
			ChannelKey: "name:fox 9 kmsp",
			Name:       "FOX KMSP Prime",
			Group:      "US News",
			StreamURL:  "http://example.com/fox9-prime.ts",
		},
		{
			ItemKey:    "src:fox32:0003",
			ChannelKey: "name:fox 32 wfld",
			Name:       "FOX 32 WFLD",
			Group:      "US News",
			StreamURL:  "http://example.com/fox32.ts",
		},
		{
			ItemKey:    "src:fox9:spanish",
			ChannelKey: "name:fox spanish",
			Name:       "FOX Noticias Spanish",
			Group:      "US News",
			StreamURL:  "http://example.com/fox-spanish.ts",
		},
		{
			ItemKey:    "src:fox9:720p",
			ChannelKey: "name:fox 9 720p",
			Name:       "FOX 9 HD 720p",
			Group:      "US News",
			StreamURL:  "http://example.com/fox-720p.ts",
		},
		{
			ItemKey:    "src:sports:0004",
			ChannelKey: "name:sports now",
			Name:       "Sports Now",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}

	keys, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{"US News"}, "fox kmsp", false)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(US News, fox kmsp) error = %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("len(keys) = %d, want 2", len(keys))
	}
	if keys[0] != "src:fox9:0001" || keys[1] != "src:fox9:0002" {
		t.Fatalf("keys = %#v, want FOX/KMSP matches ordered by name", keys)
	}

	keys, err = store.ListActiveItemKeysByCatalogFilter(ctx, []string{"Sports"}, "fox kmsp", false)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(Sports, fox kmsp) error = %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("len(sports fox/kmsp keys) = %d, want 0", len(keys))
	}

	keys, err = store.ListActiveItemKeysByCatalogFilter(ctx, nil, "fox", false)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(all groups, fox) error = %v", err)
	}
	if len(keys) != 5 {
		t.Fatalf("len(all-group fox keys) = %d, want 5", len(keys))
	}

	keys, err = store.ListActiveItemKeysByCatalogFilter(ctx, []string{"US News"}, "fox -spanish !720p", false)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(US News, fox -spanish !720p) error = %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("len(keys with exclusions) = %d, want 3", len(keys))
	}
	if keys[0] != "src:fox32:0003" || keys[1] != "src:fox9:0001" || keys[2] != "src:fox9:0002" {
		t.Fatalf("keys with exclusions = %#v, want [src:fox32:0003 src:fox9:0001 src:fox9:0002]", keys)
	}

	keys, err = store.ListActiveItemKeysByCatalogFilter(ctx, []string{"US News"}, "-spanish", false)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(US News, -spanish) error = %v", err)
	}
	if len(keys) != 4 {
		t.Fatalf("len(keys exclusion only) = %d, want 4", len(keys))
	}
	for _, key := range keys {
		if key == "src:fox9:spanish" {
			t.Fatalf("keys exclusion-only unexpectedly contains src:fox9:spanish: %#v", keys)
		}
	}

	keys, err = store.ListActiveItemKeysByCatalogFilter(ctx, []string{"  Sports ", "US News", "sports"}, "", false)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter([Sports, US News], any, false) error = %v", err)
	}
	if len(keys) != 6 {
		t.Fatalf("len(multi-group keys) = %d, want 6", len(keys))
	}
	if keys[0] != "src:fox32:0003" ||
		keys[1] != "src:fox9:720p" ||
		keys[2] != "src:fox9:0001" ||
		keys[3] != "src:fox9:0002" ||
		keys[4] != "src:fox9:spanish" ||
		keys[5] != "src:sports:0004" {
		t.Fatalf("multi-group keys = %#v, want deterministic merged ordering", keys)
	}
}

func TestCatalogListAndCatalogFilterStayInSyncForOrQueries(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:fox9:0001",
			ChannelKey: "name:fox 9 kmsp",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/fox9.ts",
		},
		{
			ItemKey:    "src:fox9:spanish",
			ChannelKey: "name:fox spanish",
			Name:       "FOX Noticias Spanish",
			Group:      "US News",
			StreamURL:  "http://example.com/fox-spanish.ts",
		},
		{
			ItemKey:    "src:nbcchicago:0002",
			ChannelKey: "name:nbc chicago",
			Name:       "NBC Chicago",
			Group:      "US News",
			StreamURL:  "http://example.com/nbc-chicago.ts",
		},
		{
			ItemKey:    "src:nbclatino:0003",
			ChannelKey: "name:nbc latino",
			Name:       "NBC Latino Spanish",
			Group:      "US News",
			StreamURL:  "http://example.com/nbc-latino.ts",
		},
		{
			ItemKey:    "src:sports:0004",
			ChannelKey: "name:sports now",
			Name:       "Sports Now",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}

	const search = "fox | nbc -spanish"
	groups := []string{"US News"}

	catalogItems, catalogTotal, err := store.ListCatalogItems(ctx, playlist.Query{
		GroupNames: groups,
		Search:     search,
		Limit:      50,
		Offset:     0,
	})
	if err != nil {
		t.Fatalf("ListCatalogItems(groups=%v, search=%q) error = %v", groups, search, err)
	}

	filterKeys, err := store.ListActiveItemKeysByCatalogFilter(ctx, groups, search, false)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(groups=%v, search=%q) error = %v", groups, search, err)
	}

	if catalogTotal != len(filterKeys) {
		t.Fatalf("catalog total = %d, want filter key count %d", catalogTotal, len(filterKeys))
	}
	if len(catalogItems) != len(filterKeys) {
		t.Fatalf("len(catalog items) = %d, want filter key count %d", len(catalogItems), len(filterKeys))
	}

	catalogKeys := itemKeys(catalogItems)
	for i := range catalogKeys {
		if catalogKeys[i] != filterKeys[i] {
			t.Fatalf("catalog/filter key mismatch at index %d: catalog=%q filter=%q (catalog=%#v filter=%#v)", i, catalogKeys[i], filterKeys[i], catalogKeys, filterKeys)
		}
	}
}

func TestListActiveItemKeysByCatalogFilterTreatsWildcardSearchAsLiteral(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:catalog-wild:percent",
			ChannelKey: "name:catalog wild percent",
			Name:       "Catalog Wild Percent % Channel",
			Group:      "Wild",
			StreamURL:  "http://example.com/catalog-wild-percent.ts",
		},
		{
			ItemKey:    "src:catalog-wild:underscore",
			ChannelKey: "name:catalog wild underscore",
			Name:       "Catalog Wild Underscore _ Channel",
			Group:      "Wild",
			StreamURL:  "http://example.com/catalog-wild-underscore.ts",
		},
		{
			ItemKey:    "src:catalog-wild:plain",
			ChannelKey: "name:catalog wild plain",
			Name:       "Catalog Wild Plain Channel",
			Group:      "Wild",
			StreamURL:  "http://example.com/catalog-wild-plain.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	keys, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{"Wild"}, "%", false)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(Wild, %%) error = %v", err)
	}
	if len(keys) != 1 || keys[0] != "src:catalog-wild:percent" {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(Wild, %%) = %#v, want [src:catalog-wild:percent]", keys)
	}

	keys, err = store.ListActiveItemKeysByCatalogFilter(ctx, []string{"Wild"}, "_", false)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(Wild, _) error = %v", err)
	}
	if len(keys) != 1 || keys[0] != "src:catalog-wild:underscore" {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(Wild, _) = %#v, want [src:catalog-wild:underscore]", keys)
	}

	keys, err = store.ListActiveItemKeysByCatalogFilter(ctx, []string{"Wild"}, "-%", false)
	if err != nil {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(Wild, -%%) error = %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(Wild, -%%) len = %d, want 2", len(keys))
	}
	if keys[0] != "src:catalog-wild:plain" || keys[1] != "src:catalog-wild:underscore" {
		t.Fatalf("ListActiveItemKeysByCatalogFilter(Wild, -%%) = %#v, want [src:catalog-wild:plain src:catalog-wild:underscore]", keys)
	}
}

func TestListActiveItemKeysByCatalogFilterQueryPlanAvoidsTempBTreeBroadAndGrouped(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(8000)); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	broadPlan, err := explainActiveItemKeysByCatalogFilterPlan(ctx, store.db, nil, "")
	if err != nil {
		t.Fatalf("explainActiveItemKeysByCatalogFilterPlan(broad) error = %v", err)
	}
	if len(broadPlan) == 0 {
		t.Fatal("expected broad query-plan rows")
	}
	t.Logf("catalog filter broad query plan: %s", strings.Join(broadPlan, " | "))
	if !planContains(broadPlan, "idx_playlist_active_name_item") {
		t.Fatalf("broad query plan missing ordered active/name index: %q", strings.Join(broadPlan, " | "))
	}
	if planContains(broadPlan, "use temp b-tree for order by") {
		t.Fatalf("broad query plan unexpectedly includes temp order-by sort: %q", strings.Join(broadPlan, " | "))
	}

	groupedPlan, err := explainActiveItemKeysByCatalogFilterPlan(ctx, store.db, []string{"Group 005"}, "channel")
	if err != nil {
		t.Fatalf("explainActiveItemKeysByCatalogFilterPlan(grouped) error = %v", err)
	}
	if len(groupedPlan) == 0 {
		t.Fatal("expected grouped query-plan rows")
	}
	t.Logf("catalog filter grouped query plan: %s", strings.Join(groupedPlan, " | "))
	if !planContains(groupedPlan, "idx_playlist_active_group_name_name_item") {
		t.Fatalf("grouped query plan missing ordered active/group/name index: %q", strings.Join(groupedPlan, " | "))
	}
	if planContains(groupedPlan, "use temp b-tree for order by") {
		t.Fatalf("grouped query plan unexpectedly includes temp order-by sort: %q", strings.Join(groupedPlan, " | "))
	}

	orGroupedPlan, err := explainActiveItemKeysByCatalogFilterPlan(
		ctx,
		store.db,
		[]string{"Group 005"},
		"channel 00005 | channel 00045 | channel 00085 | channel 00125 | channel 00165 | channel 00205",
	)
	if err != nil {
		t.Fatalf("explainActiveItemKeysByCatalogFilterPlan(or grouped) error = %v", err)
	}
	if len(orGroupedPlan) == 0 {
		t.Fatal("expected OR grouped query-plan rows")
	}
	t.Logf("catalog filter OR grouped query plan: %s", strings.Join(orGroupedPlan, " | "))
	if !planContains(orGroupedPlan, "idx_playlist_active_group_name_name_item") {
		t.Fatalf("OR grouped query plan missing ordered active/group/name index: %q", strings.Join(orGroupedPlan, " | "))
	}
	if planContains(orGroupedPlan, "use temp b-tree for order by") {
		t.Fatalf("OR grouped query plan unexpectedly includes temp order-by sort: %q", strings.Join(orGroupedPlan, " | "))
	}
}

func BenchmarkListActiveItemKeysByCatalogFilterBroad(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const itemCount = 20000
	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(itemCount)); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		keys, err := store.ListActiveItemKeysByCatalogFilter(ctx, nil, "", false)
		if err != nil {
			b.Fatalf("ListActiveItemKeysByCatalogFilter(broad) error = %v", err)
		}
		if len(keys) != itemCount {
			b.Fatalf("len(keys) = %d, want %d", len(keys), itemCount)
		}
	}
}

func BenchmarkListActiveItemKeysByCatalogFilterGrouped(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const itemCount = 20000
	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(itemCount)); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	const (
		groupName = "Group 005"
		search    = "channel"
	)

	warmKeys, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{groupName}, search, false)
	if err != nil {
		b.Fatalf("ListActiveItemKeysByCatalogFilter(warm) error = %v", err)
	}
	if len(warmKeys) == 0 {
		b.Fatalf("warm grouped lookup returned %d keys, want > 0", len(warmKeys))
	}
	expectedCount := len(warmKeys)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		keys, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{groupName}, search, false)
		if err != nil {
			b.Fatalf("ListActiveItemKeysByCatalogFilter(grouped) error = %v", err)
		}
		if len(keys) != expectedCount {
			b.Fatalf("len(keys) = %d, want %d", len(keys), expectedCount)
		}
	}
}

func BenchmarkListActiveItemKeysByCatalogFilterGroupedOr(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const itemCount = 20000
	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(itemCount)); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	const (
		groupName = "Group 005"
		search    = "channel 00005 | channel 00045 | channel 00085 | channel 00125 | channel 00165 | channel 00205"
	)

	warmKeys, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{groupName}, search, false)
	if err != nil {
		b.Fatalf("ListActiveItemKeysByCatalogFilter(warm) error = %v", err)
	}
	if len(warmKeys) == 0 {
		b.Fatalf("warm grouped OR lookup returned %d keys, want > 0", len(warmKeys))
	}
	expectedCount := len(warmKeys)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		keys, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{groupName}, search, false)
		if err != nil {
			b.Fatalf("ListActiveItemKeysByCatalogFilter(grouped OR) error = %v", err)
		}
		if len(keys) != expectedCount {
			b.Fatalf("len(keys) = %d, want %d", len(keys), expectedCount)
		}
	}
}

func BenchmarkListActiveItemKeysByCatalogFilterGroupedRegex(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const itemCount = 20000
	if err := store.UpsertPlaylistItems(ctx, generateCatalogBenchmarkItems(itemCount)); err != nil {
		b.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	const (
		groupName = "Group 005"
		search    = `channel\s+0000[5-9]|channel\s+0012[0-9]|channel\s+0013[0-9]`
	)

	warmKeys, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{groupName}, search, true)
	if err != nil {
		b.Fatalf("ListActiveItemKeysByCatalogFilter(regex warm) error = %v", err)
	}
	if len(warmKeys) == 0 {
		b.Fatalf("warm grouped regex lookup returned %d keys, want > 0", len(warmKeys))
	}
	expectedCount := len(warmKeys)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		keys, err := store.ListActiveItemKeysByCatalogFilter(ctx, []string{groupName}, search, true)
		if err != nil {
			b.Fatalf("ListActiveItemKeysByCatalogFilter(grouped regex) error = %v", err)
		}
		if len(keys) != expectedCount {
			b.Fatalf("len(keys) = %d, want %d", len(keys), expectedCount)
		}
	}
}

func installInactiveRewriteCounter(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		DROP TRIGGER IF EXISTS playlist_items_inactive_rewrite_counter;
		DROP TABLE IF EXISTS playlist_items_inactive_rewrite_counter;
		CREATE TABLE playlist_items_inactive_rewrite_counter (
		  rewrite_count INTEGER NOT NULL DEFAULT 0
		);
		INSERT INTO playlist_items_inactive_rewrite_counter(rewrite_count) VALUES (0);
		CREATE TRIGGER playlist_items_inactive_rewrite_counter
		AFTER UPDATE OF active ON playlist_items
		FOR EACH ROW
		WHEN OLD.active = 0 AND NEW.active = 0
		BEGIN
		  UPDATE playlist_items_inactive_rewrite_counter
		  SET rewrite_count = rewrite_count + 1;
		END;
	`)
	if err != nil {
		return fmt.Errorf("install inactive rewrite counter: %w", err)
	}
	return nil
}

func inactiveRewriteCount(ctx context.Context, db *sql.DB) (int64, error) {
	var count int64
	if err := db.QueryRowContext(ctx, `SELECT rewrite_count FROM playlist_items_inactive_rewrite_counter`).Scan(&count); err != nil {
		return 0, fmt.Errorf("query inactive rewrite counter: %w", err)
	}
	return count, nil
}

func itemKeys(items []playlist.Item) []string {
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.ItemKey)
	}
	return keys
}

func indexExists(ctx context.Context, db *sql.DB, indexName string) (bool, error) {
	var found string
	err := db.QueryRowContext(
		ctx,
		`SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?`,
		indexName,
	).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return found == indexName, nil
}

func explainCatalogListQueryPlan(ctx context.Context, db *sql.DB, q playlist.Query) ([]string, error) {
	q = normalizeCatalogQuery(q)
	where, args, err := buildWhere(q)
	if err != nil {
		return nil, err
	}
	planSQL := `
		EXPLAIN QUERY PLAN
		SELECT item_key, channel_key, name, group_name, tvg_logo, active
		FROM playlist_items
	` + where + `
		ORDER BY group_name ASC, name ASC, item_key ASC
		LIMIT ? OFFSET ?
	`

	planArgs := buildListArgs(args, q)
	rows, err := db.QueryContext(ctx, planSQL, planArgs...)
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

func explainActiveItemKeysByCatalogFilterPlan(ctx context.Context, db *sql.DB, groupNames []string, searchQuery string) ([]string, error) {
	spec, err := buildCatalogFilterQuerySpec(groupNames, searchQuery, false)
	if err != nil {
		return nil, err
	}
	query := `EXPLAIN QUERY PLAN
		SELECT item_key
		FROM playlist_items INDEXED BY ` + spec.OrderIndex + `
		WHERE ` + spec.WhereClause + `
		ORDER BY name ASC, item_key ASC`

	rows, err := db.QueryContext(ctx, query, spec.Args...)
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

func planContains(details []string, fragment string) bool {
	fragment = strings.ToLower(strings.TrimSpace(fragment))
	for _, detail := range details {
		if strings.Contains(strings.ToLower(detail), fragment) {
			return true
		}
	}
	return false
}

func generateCatalogBenchmarkItems(itemCount int) []playlist.Item {
	items := make([]playlist.Item, 0, itemCount)
	for i := 0; i < itemCount; i++ {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:catalog:bench:%05d", i),
			ChannelKey: fmt.Sprintf("tvg:catalog:bench:%05d", i),
			Name:       fmt.Sprintf("Channel %05d", i),
			Group:      fmt.Sprintf("Group %03d", i%40),
			StreamURL:  fmt.Sprintf("http://example.com/catalog-bench-%05d.ts", i),
		})
	}
	return items
}

func buildLongCatalogSearchQuery(uniqueTerms int) string {
	if uniqueTerms <= 0 {
		return ""
	}
	parts := make([]string, 0, uniqueTerms+1)
	parts = append(parts, "channel")
	for i := 0; i < uniqueTerms; i++ {
		parts = append(parts, fmt.Sprintf("longtoken%03d", i))
	}
	return strings.Join(parts, " ")
}
