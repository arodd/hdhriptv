package playlist

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

const (
	primaryPlaylistSourceIDForRefresh  int64 = 1
	primaryPlaylistSourceKeyForRefresh       = "primary"
	playlistSourceItemKeyPrefix              = "ps:"
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

// SourceCatalogStore persists playlist items for one playlist source.
type SourceCatalogStore interface {
	UpsertPlaylistItemsForSource(ctx context.Context, sourceID int64, items []Item) error
}

// StreamingCatalogStore persists playlist items from a streaming source.
type StreamingCatalogStore interface {
	UpsertPlaylistItemsStream(ctx context.Context, stream ItemStream) (int, error)
}

// StreamingSourceCatalogStore persists playlist items incrementally for one playlist source.
type StreamingSourceCatalogStore interface {
	UpsertPlaylistItemsStreamForSource(ctx context.Context, sourceID int64, stream ItemStream) (int, error)
}

// Refresher coordinates fetch + persist operations.
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

// RefreshForSource refreshes one playlist source into source-scoped catalog storage.
func (r *Refresher) RefreshForSource(ctx context.Context, source PlaylistSource) (int, error) {
	if r.fetcher == nil || r.store == nil {
		return 0, fmt.Errorf("refresher is not fully configured")
	}

	if source.SourceID <= 0 {
		return 0, fmt.Errorf("playlist source id must be greater than zero")
	}
	playlistURL := strings.TrimSpace(source.PlaylistURL)
	if playlistURL == "" {
		return 0, fmt.Errorf("playlist source %d URL is not configured", source.SourceID)
	}

	startedAt := time.Now().UTC()
	if r.logger != nil {
		r.logger.Info(
			"playlist source refresh started",
			"playlist_source_id", source.SourceID,
			"playlist_source_name", strings.TrimSpace(source.Name),
			"playlist_source_key", strings.TrimSpace(source.SourceKey),
			"playlist_url_configured", playlistURL != "",
			"started_at", startedAt,
		)
	}

	if streamingFetcher, ok := r.fetcher.(StreamingFetcher); ok {
		if streamingStore, ok := r.store.(StreamingSourceCatalogStore); ok {
			itemCount, err := streamingStore.UpsertPlaylistItemsStreamForSource(ctx, source.SourceID, func(yield func(Item) error) error {
				_, streamErr := streamingFetcher.FetchAndParseEach(ctx, playlistURL, func(item Item) error {
					namespaced, namespaceErr := namespacePlaylistItemForSource(item, source)
					if namespaceErr != nil {
						return namespaceErr
					}
					return yield(namespaced)
				})
				return streamErr
			})
			r.logSourceRefreshFinished(startedAt, source, itemCount, err)
			if err != nil {
				return 0, err
			}
			return itemCount, nil
		}
		if source.SourceID != primaryPlaylistSourceIDForRefresh {
			return 0, fmt.Errorf("source-scoped streaming catalog store is not configured")
		}
	}

	items, err := r.fetcher.FetchAndParse(ctx, playlistURL)
	if err != nil {
		r.logSourceRefreshFinished(startedAt, source, 0, err)
		return 0, err
	}
	for i := range items {
		namespaced, namespaceErr := namespacePlaylistItemForSource(items[i], source)
		if namespaceErr != nil {
			r.logSourceRefreshFinished(startedAt, source, len(items), namespaceErr)
			return 0, namespaceErr
		}
		items[i] = namespaced
	}

	if sourceStore, ok := r.store.(SourceCatalogStore); ok {
		if err := sourceStore.UpsertPlaylistItemsForSource(ctx, source.SourceID, items); err != nil {
			r.logSourceRefreshFinished(startedAt, source, len(items), err)
			return 0, err
		}
		r.logSourceRefreshFinished(startedAt, source, len(items), nil)
		return len(items), nil
	}

	if source.SourceID != primaryPlaylistSourceIDForRefresh {
		storeErr := fmt.Errorf("source-scoped catalog store is not configured")
		r.logSourceRefreshFinished(startedAt, source, len(items), storeErr)
		return 0, storeErr
	}
	if err := r.store.UpsertPlaylistItems(ctx, items); err != nil {
		r.logSourceRefreshFinished(startedAt, source, len(items), err)
		return 0, err
	}
	r.logSourceRefreshFinished(startedAt, source, len(items), nil)
	return len(items), nil
}

func namespacePlaylistItemForSource(item Item, source PlaylistSource) (Item, error) {
	namespacedKey, err := namespacePlaylistItemKeyForSource(item.ItemKey, source)
	if err != nil {
		return Item{}, err
	}
	item.ItemKey = namespacedKey
	return item, nil
}

func namespacePlaylistItemKeyForSource(itemKey string, source PlaylistSource) (string, error) {
	itemKey = strings.TrimSpace(itemKey)
	if itemKey == "" {
		return "", fmt.Errorf("playlist item key is required")
	}

	sourceKey := strings.TrimSpace(source.SourceKey)
	if source.SourceID == primaryPlaylistSourceIDForRefresh || strings.EqualFold(sourceKey, primaryPlaylistSourceKeyForRefresh) {
		return itemKey, nil
	}
	if source.SourceID <= 0 {
		return "", fmt.Errorf("playlist source id must be greater than zero")
	}
	if sourceKey == "" {
		return "", fmt.Errorf("playlist source %d key is required for namespacing", source.SourceID)
	}

	prefix := playlistSourceItemKeyPrefix + sourceKey + ":"
	if strings.HasPrefix(itemKey, prefix) {
		return itemKey, nil
	}
	return prefix + itemKey, nil
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

func (r *Refresher) logSourceRefreshFinished(startedAt time.Time, source PlaylistSource, itemCount int, err error) {
	if r == nil || r.logger == nil {
		return
	}

	duration := time.Since(startedAt)
	if duration < 0 {
		duration = 0
	}
	fields := []any{
		"playlist_source_id", source.SourceID,
		"playlist_source_name", strings.TrimSpace(source.Name),
		"playlist_source_key", strings.TrimSpace(source.SourceKey),
		"item_count", itemCount,
		"duration", duration.String(),
	}
	if err != nil {
		fields = append(fields, "result", "error", "error", err)
		r.logger.Warn("playlist source refresh finished", fields...)
		return
	}
	fields = append(fields, "result", "success")
	r.logger.Info("playlist source refresh finished", fields...)
}
