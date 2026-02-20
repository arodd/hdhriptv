package playlist

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Fetcher fetches and parses playlists from an external source.
type Fetcher interface {
	FetchAndParse(ctx context.Context, playlistURL string) ([]Item, error)
}

// StreamingFetcher fetches and parses playlists incrementally.
type StreamingFetcher interface {
	FetchAndParseEach(ctx context.Context, playlistURL string, onItem func(Item) error) (int, error)
}

// CatalogStore persists playlist catalog items.
type CatalogStore interface {
	UpsertPlaylistItems(ctx context.Context, items []Item) error
}

// StreamingCatalogStore persists playlist items from a streaming source.
type StreamingCatalogStore interface {
	UpsertPlaylistItemsStream(ctx context.Context, stream ItemStream) (int, error)
}

// Refresher coordinates fetch + persist operations with a single refresh lock.
type Refresher struct {
	fetcher Fetcher
	store   CatalogStore
	logger  *slog.Logger

	mu sync.Mutex
}

func NewRefresher(fetcher Fetcher, store CatalogStore, logger ...*slog.Logger) *Refresher {
	activeLogger := slog.Default()
	for _, candidate := range logger {
		if candidate != nil {
			activeLogger = candidate
			break
		}
	}
	return &Refresher{fetcher: fetcher, store: store, logger: activeLogger}
}

func (r *Refresher) Refresh(ctx context.Context, playlistURL string) (int, error) {
	if r.fetcher == nil || r.store == nil {
		return 0, fmt.Errorf("refresher is not fully configured")
	}

	startedAt := time.Now().UTC()
	if r.logger != nil {
		r.logger.Info(
			"playlist refresh started",
			"playlist_url_configured", strings.TrimSpace(playlistURL) != "",
			"started_at", startedAt,
		)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if streamingFetcher, ok := r.fetcher.(StreamingFetcher); ok {
		if streamingStore, ok := r.store.(StreamingCatalogStore); ok {
			itemCount, err := streamingStore.UpsertPlaylistItemsStream(ctx, func(yield func(Item) error) error {
				_, streamErr := streamingFetcher.FetchAndParseEach(ctx, playlistURL, yield)
				return streamErr
			})
			if err != nil {
				r.logRefreshFinished(startedAt, itemCount, err)
				return 0, err
			}
			r.logRefreshFinished(startedAt, itemCount, nil)
			return itemCount, nil
		}
	}

	items, err := r.fetcher.FetchAndParse(ctx, playlistURL)
	if err != nil {
		r.logRefreshFinished(startedAt, 0, err)
		return 0, err
	}
	if err := r.store.UpsertPlaylistItems(ctx, items); err != nil {
		r.logRefreshFinished(startedAt, len(items), err)
		return 0, err
	}
	r.logRefreshFinished(startedAt, len(items), nil)
	return len(items), nil
}

func (r *Refresher) logRefreshFinished(startedAt time.Time, itemCount int, err error) {
	if r == nil || r.logger == nil {
		return
	}

	duration := time.Since(startedAt)
	if duration < 0 {
		duration = 0
	}
	if err != nil {
		r.logger.Warn(
			"playlist refresh finished",
			"result", "error",
			"item_count", itemCount,
			"duration", duration.String(),
			"error", err,
		)
		return
	}
	r.logger.Info(
		"playlist refresh finished",
		"result", "success",
		"item_count", itemCount,
		"duration", duration.String(),
	)
}
