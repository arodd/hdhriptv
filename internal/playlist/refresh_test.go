package playlist

import (
	"context"
	"errors"
	"strings"
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

func TestRefresherRefreshForSourceUsesStreamingNamespacedKeys(t *testing.T) {
	t.Parallel()

	items := []Item{
		{ItemKey: "src:alpha", Name: "Alpha", Group: "News", StreamURL: "http://example.com/a.ts"},
		{ItemKey: "src:beta", Name: "Beta", Group: "News", StreamURL: "http://example.com/b.ts"},
	}
	source := PlaylistSource{
		SourceID:    2,
		SourceKey:   "a1b2c3d4",
		Name:        "Backup",
		PlaylistURL: "http://example.com/playlist.m3u",
	}
	fetcher := &fakeStreamingFetcher{items: items}
	store := &fakeStreamingStore{}

	refresher := NewRefresher(fetcher, store)
	count, err := refresher.RefreshForSource(context.Background(), source)
	if err != nil {
		t.Fatalf("RefreshForSource() error = %v", err)
	}
	if count != len(items) {
		t.Fatalf("RefreshForSource() count = %d, want %d", count, len(items))
	}
	if store.streamSourceCalls != 1 {
		t.Fatalf("UpsertPlaylistItemsStreamForSource calls = %d, want 1", store.streamSourceCalls)
	}
	if store.streamSourceID != source.SourceID {
		t.Fatalf("stream source_id = %d, want %d", store.streamSourceID, source.SourceID)
	}
	if got, want := store.seen[0].ItemKey, "ps:a1b2c3d4:src:alpha"; got != want {
		t.Fatalf("first namespaced item_key = %q, want %q", got, want)
	}
	if got, want := store.seen[1].ItemKey, "ps:a1b2c3d4:src:beta"; got != want {
		t.Fatalf("second namespaced item_key = %q, want %q", got, want)
	}
}

func TestRefresherRefreshForSourcePrimaryPreservesLegacyItemKeys(t *testing.T) {
	t.Parallel()

	items := []Item{
		{ItemKey: "src:legacy", Name: "Legacy", Group: "News", StreamURL: "http://example.com/legacy.ts"},
	}
	source := PlaylistSource{
		SourceID:    1,
		SourceKey:   "primary",
		Name:        "Primary",
		PlaylistURL: "http://example.com/playlist.m3u",
	}
	fetcher := &fakeFetcher{items: items}
	store := &fakeStore{}

	refresher := NewRefresher(fetcher, store)
	count, err := refresher.RefreshForSource(context.Background(), source)
	if err != nil {
		t.Fatalf("RefreshForSource() error = %v", err)
	}
	if count != len(items) {
		t.Fatalf("RefreshForSource() count = %d, want %d", count, len(items))
	}
	if len(store.seen) != 1 {
		t.Fatalf("stored item count = %d, want 1", len(store.seen))
	}
	if got, want := store.seen[0].ItemKey, "src:legacy"; got != want {
		t.Fatalf("stored primary item_key = %q, want %q", got, want)
	}
}

func TestRefresherRefreshForSourceRejectsMissingSourceKey(t *testing.T) {
	t.Parallel()

	refresher := NewRefresher(
		&fakeFetcher{items: []Item{{ItemKey: "src:legacy", StreamURL: "http://example.com/legacy.ts"}}},
		&fakeStore{},
	)
	_, err := refresher.RefreshForSource(context.Background(), PlaylistSource{
		SourceID:    2,
		SourceKey:   "",
		Name:        "Backup",
		PlaylistURL: "http://example.com/playlist.m3u",
	})
	if err == nil || !strings.Contains(err.Error(), "key is required") {
		t.Fatalf("RefreshForSource() error = %v, want missing source key validation", err)
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

	sourceCalls int
	sourceID    int64
}

func (s *fakeStore) UpsertPlaylistItems(_ context.Context, items []Item) error {
	s.calls++
	if s.err != nil {
		return s.err
	}
	s.seen = append(s.seen[:0], items...)
	return nil
}

func (s *fakeStore) UpsertPlaylistItemsForSource(_ context.Context, sourceID int64, items []Item) error {
	s.sourceCalls++
	s.sourceID = sourceID
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

	streamSourceCalls int
	streamSourceID    int64
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

func (s *fakeStreamingStore) UpsertPlaylistItemsStreamForSource(_ context.Context, sourceID int64, stream ItemStream) (int, error) {
	s.streamSourceCalls++
	s.streamSourceID = sourceID
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
