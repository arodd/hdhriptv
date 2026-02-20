package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/reconcile"
)

const settingPlaylistURL = "playlist.url"

const (
	playlistSyncProgressPersistEvery    = 5
	playlistSyncProgressPersistInterval = 1 * time.Second
)

// PlaylistSettingsStore reads automation settings required by playlist sync jobs.
type PlaylistSettingsStore interface {
	GetSetting(ctx context.Context, key string) (string, error)
}

// PlaylistRefresher performs playlist fetch+parse+catalog upsert.
type PlaylistRefresher interface {
	Refresh(ctx context.Context, playlistURL string) (int, error)
}

// PlaylistReconciler updates channel source mappings after catalog refresh.
type PlaylistReconciler interface {
	CountChannels(ctx context.Context) (int, error)
	Reconcile(ctx context.Context, onProgress func(cur, max int) error) (reconcile.Result, error)
}

// DVRLineupReloader refreshes downstream DVR lineup state after playlist sync.
type DVRLineupReloader interface {
	ReloadLineup(ctx context.Context) error
}

// DVRLineupReloaderWithStatus optionally allows provider-aware non-fatal skip
// semantics for post-playlist-sync lineup reload paths.
type DVRLineupReloaderWithStatus interface {
	ReloadLineupForPlaylistSync(ctx context.Context) (reloaded bool, skipped bool, skipReason string, err error)
}

// PlaylistSyncJob runs playlist refresh then reconcile.
type PlaylistSyncJob struct {
	settings   PlaylistSettingsStore
	refresher  PlaylistRefresher
	reconciler PlaylistReconciler
	reloader   DVRLineupReloader
}

func NewPlaylistSyncJob(
	settings PlaylistSettingsStore,
	refresher PlaylistRefresher,
	reconciler PlaylistReconciler,
) (*PlaylistSyncJob, error) {
	if settings == nil {
		return nil, fmt.Errorf("playlist settings store is required")
	}
	if refresher == nil {
		return nil, fmt.Errorf("playlist refresher is required")
	}
	if reconciler == nil {
		return nil, fmt.Errorf("playlist reconciler is required")
	}
	return &PlaylistSyncJob{
		settings:   settings,
		refresher:  refresher,
		reconciler: reconciler,
	}, nil
}

// SetPostSyncLineupReloader configures an optional DVR lineup reload hook
// executed after successful refresh+reconcile completion.
func (j *PlaylistSyncJob) SetPostSyncLineupReloader(reloader DVRLineupReloader) {
	if j == nil {
		return
	}
	j.reloader = reloader
}

// Run executes refresh + reconcile and updates job progress by channel.
func (j *PlaylistSyncJob) Run(ctx context.Context, run *RunContext) error {
	if run == nil {
		return fmt.Errorf("run context is required")
	}

	playlistURL, err := j.settings.GetSetting(ctx, settingPlaylistURL)
	if err == sql.ErrNoRows {
		return fmt.Errorf("playlist URL is not configured")
	}
	if err != nil {
		return fmt.Errorf("read playlist URL setting: %w", err)
	}
	playlistURL = strings.TrimSpace(playlistURL)
	if playlistURL == "" {
		return fmt.Errorf("playlist URL is not configured")
	}

	refreshedCount, err := j.refresher.Refresh(ctx, playlistURL)
	if err != nil {
		return fmt.Errorf("refresh playlist: %w", err)
	}

	channelCount, err := j.reconciler.CountChannels(ctx)
	if err != nil {
		return fmt.Errorf("count channels for reconcile: %w", err)
	}
	if err := run.SetProgress(ctx, 0, channelCount); err != nil {
		return err
	}

	progressThrottle := newProgressPersistThrottle(
		playlistSyncProgressPersistEvery,
		playlistSyncProgressPersistInterval,
	)
	progressThrottle.markPersist(time.Now(), 0, channelCount)

	reconcileResult, err := j.reconciler.Reconcile(ctx, func(cur, max int) error {
		if err := run.setProgressInMemory(cur, max); err != nil {
			return err
		}

		now := time.Now()
		if !progressThrottle.shouldPersist(now, cur, max) {
			return nil
		}
		if err := run.persistProgress(ctx); err != nil {
			return err
		}
		progressThrottle.markPersist(now, cur, max)
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconcile channels: %w", err)
	}

	cur, max, _ := run.Snapshot()
	if progressThrottle.needsPersist(cur, max) {
		if err := run.persistProgress(ctx); err != nil {
			return err
		}
		progressThrottle.markPersist(time.Now(), cur, max)
	}

	reloadedLineup := false
	reloadStatus := "disabled"
	reloadSkipReason := "none"
	if j.reloader != nil {
		if statusReloader, ok := j.reloader.(DVRLineupReloaderWithStatus); ok {
			reloaded, skipped, skipReason, err := statusReloader.ReloadLineupForPlaylistSync(ctx)
			if err != nil {
				return fmt.Errorf("reload dvr lineup after playlist sync: %w", err)
			}
			reloadedLineup = reloaded
			switch {
			case reloaded && skipped:
				reloadStatus = "partial"
				if reason := strings.TrimSpace(skipReason); reason != "" {
					reloadSkipReason = reason
				}
			case skipped:
				reloadStatus = "skipped"
				if reason := strings.TrimSpace(skipReason); reason != "" {
					reloadSkipReason = reason
				}
			case reloaded:
				reloadStatus = "reloaded"
			default:
				reloadStatus = "unknown"
			}
		} else {
			if err := j.reloader.ReloadLineup(ctx); err != nil {
				return fmt.Errorf("reload dvr lineup after playlist sync: %w", err)
			}
			reloadedLineup = true
			reloadStatus = "reloaded"
		}
	}

	summary := fmt.Sprintf(
		"playlist refreshed items=%d; channels processed=%d/%d; added_sources=%d; existing_sources=%d; dynamic_blocks=%d enabled=%d added=%d updated=%d retained=%d removed=%d truncated=%d; dynamic_channels=%d; dynamic_added=%d; dynamic_removed=%d; dynamic_name_updates=%d; dvr_lineup_reloaded=%t; dvr_lineup_reload_status=%s; dvr_lineup_reload_skip_reason=%s",
		refreshedCount,
		reconcileResult.ChannelsProcessed,
		reconcileResult.ChannelsTotal,
		reconcileResult.SourcesAdded,
		reconcileResult.SourcesAlreadySeen,
		reconcileResult.DynamicBlocksProcessed,
		reconcileResult.DynamicBlocksEnabled,
		reconcileResult.DynamicChannelsAdded,
		reconcileResult.DynamicChannelsUpdated,
		reconcileResult.DynamicChannelsRetained,
		reconcileResult.DynamicChannelsRemoved,
		reconcileResult.DynamicChannelsTruncated,
		reconcileResult.DynamicChannelsProcessed,
		reconcileResult.DynamicSourcesAdded,
		reconcileResult.DynamicSourcesRemoved,
		reconcileResult.DynamicGuideNamesUpdated,
		reloadedLineup,
		reloadStatus,
		reloadSkipReason,
	)
	if err := run.SetSummary(ctx, summary); err != nil {
		return err
	}

	return nil
}

type progressPersistThrottle struct {
	persistEvery    int
	persistInterval time.Duration
	lastPersistAt   time.Time
	lastPersistCur  int
	lastPersistMax  int
}

func newProgressPersistThrottle(persistEvery int, persistInterval time.Duration) *progressPersistThrottle {
	return &progressPersistThrottle{
		persistEvery:    persistEvery,
		persistInterval: persistInterval,
	}
}

func (t *progressPersistThrottle) shouldPersist(now time.Time, cur, max int) bool {
	if t == nil {
		return true
	}
	if cur <= 0 {
		return false
	}
	if max > 0 && cur >= max {
		return true
	}
	if t.persistEvery > 0 && (cur-t.lastPersistCur) >= t.persistEvery {
		return true
	}
	if t.persistInterval > 0 && !t.lastPersistAt.IsZero() && now.Sub(t.lastPersistAt) >= t.persistInterval {
		return true
	}
	return false
}

func (t *progressPersistThrottle) needsPersist(cur, max int) bool {
	if t == nil {
		return true
	}
	return t.lastPersistCur != cur || t.lastPersistMax != max
}

func (t *progressPersistThrottle) markPersist(now time.Time, cur, max int) {
	if t == nil {
		return
	}
	t.lastPersistAt = now
	t.lastPersistCur = cur
	t.lastPersistMax = max
}
