package playlist

import (
	"context"
	"errors"
	"testing"
)

func TestRefresherRefreshUsesStreamingPathWhenAvailable(t *testing.T) {
	t.Parallel()

	items := []Item{
		{ItemKey: "src:1", Name: "One", Group: "News", StreamURL: "http://example.com/1.ts"},
		{ItemKey: "src:2", Name: "Two", Group: "News", StreamURL: "http://example.com/2.ts"},
		{ItemKey: "src:3", Name: "Three", Group: "News", StreamURL: "http://example.com/3.ts"},
	}
	fetcher := &fakeStreamingFetcher{items: items}
	store := &fakeStreamingStore{}

	refresher := NewRefresher(fetcher, store)
	count, err := refresher.Refresh(context.Background(), "http://example.com/playlist.m3u")
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if count != len(items) {
		t.Fatalf("Refresh() count = %d, want %d", count, len(items))
	}
	if fetcher.streamCalls != 1 {
		t.Fatalf("stream FetchAndParseEach calls = %d, want 1", fetcher.streamCalls)
	}
	if fetcher.legacyCalls != 0 {
		t.Fatalf("legacy FetchAndParse calls = %d, want 0", fetcher.legacyCalls)
	}
	if store.streamCalls != 1 {
		t.Fatalf("stream UpsertPlaylistItemsStream calls = %d, want 1", store.streamCalls)
	}
	if store.legacyCalls != 0 {
		t.Fatalf("legacy UpsertPlaylistItems calls = %d, want 0", store.legacyCalls)
	}
	if len(store.seen) != len(items) {
		t.Fatalf("streamed item count = %d, want %d", len(store.seen), len(items))
	}
}

func TestRefresherRefreshFallsBackToLegacyPath(t *testing.T) {
	t.Parallel()

	items := []Item{
		{ItemKey: "src:1", Name: "One", Group: "News", StreamURL: "http://example.com/1.ts"},
		{ItemKey: "src:2", Name: "Two", Group: "News", StreamURL: "http://example.com/2.ts"},
	}
	fetcher := &fakeFetcher{items: items}
	store := &fakeStore{}

	refresher := NewRefresher(fetcher, store)
	count, err := refresher.Refresh(context.Background(), "http://example.com/playlist.m3u")
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if count != len(items) {
		t.Fatalf("Refresh() count = %d, want %d", count, len(items))
	}
	if fetcher.calls != 1 {
		t.Fatalf("FetchAndParse calls = %d, want 1", fetcher.calls)
	}
	if store.calls != 1 {
		t.Fatalf("UpsertPlaylistItems calls = %d, want 1", store.calls)
	}
}

func TestRefresherRefreshStreamingError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("stream write failed")
	refresher := NewRefresher(
		&fakeStreamingFetcher{items: []Item{{ItemKey: "src:1", StreamURL: "http://example.com/1.ts"}}},
		&fakeStreamingStore{streamErr: wantErr},
	)
	_, err := refresher.Refresh(context.Background(), "http://example.com/playlist.m3u")
	if !errors.Is(err, wantErr) {
		t.Fatalf("Refresh() error = %v, want %v", err, wantErr)
	}
}

type fakeFetcher struct {
	items []Item
	err   error
	calls int
}

func (f *fakeFetcher) FetchAndParse(context.Context, string) ([]Item, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return append([]Item(nil), f.items...), nil
}

type fakeStore struct {
	err   error
	calls int
	seen  []Item
}

func (s *fakeStore) UpsertPlaylistItems(_ context.Context, items []Item) error {
	s.calls++
	if s.err != nil {
		return s.err
	}
	s.seen = append(s.seen[:0], items...)
	return nil
}

type fakeStreamingFetcher struct {
	items       []Item
	streamErr   error
	legacyErr   error
	streamCalls int
	legacyCalls int
}

func (f *fakeStreamingFetcher) FetchAndParse(ctx context.Context, playlistURL string) ([]Item, error) {
	f.legacyCalls++
	if f.legacyErr != nil {
		return nil, f.legacyErr
	}
	out := make([]Item, 0, len(f.items))
	_, err := f.FetchAndParseEach(ctx, playlistURL, func(item Item) error {
		out = append(out, item)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (f *fakeStreamingFetcher) FetchAndParseEach(_ context.Context, _ string, onItem func(Item) error) (int, error) {
	f.streamCalls++
	if f.streamErr != nil {
		return 0, f.streamErr
	}
	for i, item := range f.items {
		if err := onItem(item); err != nil {
			return i, err
		}
	}
	return len(f.items), nil
}

type fakeStreamingStore struct {
	seen        []Item
	streamErr   error
	legacyErr   error
	streamCalls int
	legacyCalls int
}

func (s *fakeStreamingStore) UpsertPlaylistItems(_ context.Context, _ []Item) error {
	s.legacyCalls++
	return s.legacyErr
}

func (s *fakeStreamingStore) UpsertPlaylistItemsStream(_ context.Context, stream ItemStream) (int, error) {
	s.streamCalls++
	if s.streamErr != nil {
		return 0, s.streamErr
	}
	s.seen = s.seen[:0]
	count := 0
	if err := stream(func(item Item) error {
		s.seen = append(s.seen, item)
		count++
		return nil
	}); err != nil {
		return count, err
	}
	return count, nil
}
