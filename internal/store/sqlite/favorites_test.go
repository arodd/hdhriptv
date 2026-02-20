package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/arodd/hdhriptv/internal/favorites"
	"github.com/arodd/hdhriptv/internal/playlist"
)

func TestFavoritesLifecycle(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:1111aaaa",
			ChannelKey: "tvg:news",
			Name:       "News",
			Group:      "News",
			StreamURL:  "http://example.com/news.ts",
			TVGLogo:    "http://example.com/news.png",
			Attrs:      map[string]string{"group-title": "News"},
		},
		{
			ItemKey:    "src:sports:2222bbbb",
			ChannelKey: "tvg:sports",
			Name:       "Sports",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports.ts",
			Attrs:      map[string]string{"group-title": "Sports"},
		},
		{
			ItemKey:    "src:movie:3333cccc",
			ChannelKey: "tvg:movie",
			Name:       "Movies",
			Group:      "Movies",
			StreamURL:  "http://example.com/movies.ts",
			Attrs:      map[string]string{"group-title": "Movies"},
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	first, err := store.AddFavorite(ctx, "src:news:1111aaaa", 100)
	if err != nil {
		t.Fatalf("AddFavorite(news) error = %v", err)
	}
	if first.OrderIndex != 0 || first.GuideNumber != "100" {
		t.Fatalf("first favorite numbering = (%d, %q), want (0, 100)", first.OrderIndex, first.GuideNumber)
	}

	second, err := store.AddFavorite(ctx, "src:sports:2222bbbb", 100)
	if err != nil {
		t.Fatalf("AddFavorite(sports) error = %v", err)
	}
	if second.OrderIndex != 1 || second.GuideNumber != "101" {
		t.Fatalf("second favorite numbering = (%d, %q), want (1, 101)", second.OrderIndex, second.GuideNumber)
	}

	third, err := store.AddFavorite(ctx, "src:movie:3333cccc", 100)
	if err != nil {
		t.Fatalf("AddFavorite(movie) error = %v", err)
	}

	dupe, err := store.AddFavorite(ctx, "src:news:1111aaaa", 100)
	if err != nil {
		t.Fatalf("AddFavorite(news duplicate) error = %v", err)
	}
	if dupe.FavID != first.FavID {
		t.Fatalf("duplicate favorite id = %d, want %d", dupe.FavID, first.FavID)
	}

	list, err := store.ListFavorites(ctx, false)
	if err != nil {
		t.Fatalf("ListFavorites() error = %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("len(ListFavorites) = %d, want 3", len(list))
	}

	if err := store.ReorderFavorites(ctx, []int64{third.FavID, first.FavID, second.FavID}, 100); err != nil {
		t.Fatalf("ReorderFavorites() error = %v", err)
	}

	list, err = store.ListFavorites(ctx, false)
	if err != nil {
		t.Fatalf("ListFavorites() after reorder error = %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("len(ListFavorites) after reorder = %d, want 3", len(list))
	}
	if list[0].FavID != third.FavID || list[0].GuideNumber != "100" {
		t.Fatalf("first reordered favorite = %+v, want fav_id=%d guide=100", list[0], third.FavID)
	}
	if list[1].FavID != first.FavID || list[1].GuideNumber != "101" {
		t.Fatalf("second reordered favorite = %+v, want fav_id=%d guide=101", list[1], first.FavID)
	}

	if err := store.RemoveFavorite(ctx, first.FavID, 100); err != nil {
		t.Fatalf("RemoveFavorite() error = %v", err)
	}

	list, err = store.ListFavorites(ctx, false)
	if err != nil {
		t.Fatalf("ListFavorites() after remove error = %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("len(ListFavorites) after remove = %d, want 2", len(list))
	}
	if list[0].GuideNumber != "100" || list[1].GuideNumber != "101" {
		t.Fatalf("guide numbers not renumbered after remove: %+v", list)
	}
}

func TestFavoritesErrors(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if _, err := store.AddFavorite(ctx, "src:missing:0000", 100); !errors.Is(err, favorites.ErrItemNotFound) {
		t.Fatalf("AddFavorite(missing) error = %v, want ErrItemNotFound", err)
	}

	if err := store.RemoveFavorite(ctx, 999, 100); !errors.Is(err, favorites.ErrNotFound) {
		t.Fatalf("RemoveFavorite(missing) error = %v, want ErrNotFound", err)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:one:aaaa",
			ChannelKey: "name:one",
			Name:       "One",
			Group:      "G",
			StreamURL:  "http://example.com/one.ts",
		},
		{
			ItemKey:    "src:two:bbbb",
			ChannelKey: "name:two",
			Name:       "Two",
			Group:      "G",
			StreamURL:  "http://example.com/two.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	one, err := store.AddFavorite(ctx, "src:one:aaaa", 100)
	if err != nil {
		t.Fatalf("AddFavorite(one) error = %v", err)
	}
	_, err = store.AddFavorite(ctx, "src:two:bbbb", 100)
	if err != nil {
		t.Fatalf("AddFavorite(two) error = %v", err)
	}

	if err := store.ReorderFavorites(ctx, []int64{one.FavID}, 100); err == nil {
		t.Fatal("ReorderFavorites() expected count mismatch error")
	}

	if err := store.ReorderFavorites(ctx, []int64{one.FavID, one.FavID}, 100); err == nil {
		t.Fatal("ReorderFavorites() expected duplicate id error")
	}
}
