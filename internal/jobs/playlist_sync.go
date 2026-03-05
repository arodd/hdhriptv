package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arodd/hdhriptv/internal/dvr"
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/reconcile"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const settingPlaylistURL = "playlist.url"

const (
	playlistSyncProgressPersistEvery     = 5
	playlistSyncProgressPersistInterval  = 1 * time.Second
	defaultPlaylistSyncSourceConcurrency = 1
	maxPlaylistSyncSourceConcurrency     = 16
	playlistSyncSummarySchemaVersion     = 2
	playlistSyncFailurePhaseRefresh      = "refresh"
	playlistSyncFailurePhaseCount        = "count_channels"
	playlistSyncFailurePhaseReconcile    = "reconcile"
)

var (
	playlistSyncSourceDurationMetric = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "playlist_sync_source_duration_seconds",
		Help:    "Duration of playlist sync refreshes per playlist source.",
		Buckets: []float64{0.25, 0.5, 1, 2, 4, 8, 16, 30, 60, 120, 300},
	}, []string{"playlist_source"})
	playlistSyncSourceErrorsMetric = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "playlist_sync_source_errors_total",
		Help: "Total number of playlist sync refresh errors per playlist source.",
	}, []string{"playlist_source"})
	playlistSyncSourceItemsMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "playlist_sync_source_items",
		Help: "Latest successful playlist item count refreshed per playlist source.",
	}, []string{"playlist_source"})
)

// PlaylistSettingsStore reads automation settings required by playlist sync jobs.
type PlaylistSettingsStore interface {
	GetSetting(ctx context.Context, key string) (string, error)
	ListPlaylistSources(ctx context.Context) ([]playlist.PlaylistSource, error)
}

// PlaylistRefresher performs playlist fetch+parse+catalog upsert.
type PlaylistRefresher interface {
	RefreshForSource(ctx context.Context, source playlist.PlaylistSource) (int, error)
}

// PlaylistReconciler updates channel source mappings after catalog refresh.
type PlaylistReconciler interface {
	CountChannels(ctx context.Context) (int, error)
	Reconcile(ctx context.Context, onProgress func(cur, max int) error) (reconcile.Result, error)
}

// DVRLineupReloader refreshes downstream DVR lineup state after playlist sync
// and returns provider-aware non-fatal skip metadata.
type DVRLineupReloader interface {
	ReloadLineupForPlaylistSyncOutcome(ctx context.Context) (dvr.ReloadOutcome, error)
}

// PlaylistSyncJob runs playlist refresh then reconcile.
type PlaylistSyncJob struct {
	settings   PlaylistSettingsStore
	refresher  PlaylistRefresher
	reconciler PlaylistReconciler
	reloader   DVRLineupReloader

	sourceRefreshConcurrency int
}

type playlistSyncSourceOutcome struct {
	Source    playlist.PlaylistSource
	ItemCount int
	Err       error
}

type playlistSyncRunSummary struct {
	SchemaVersion     int                            `json:"schema_version"`
	RequestedSourceID string                         `json:"requested_source_id"`
	Sources           playlistSyncRunSummarySources  `json:"sources"`
	Refresh           playlistSyncRunSummaryRefresh  `json:"refresh"`
	Reconcile         *playlistSyncRunSummaryMetrics `json:"reconcile,omitempty"`
	DVRLineup         playlistSyncRunSummaryDVR      `json:"dvr_lineup"`
	Failure           *playlistSyncRunSummaryFailure `json:"failure,omitempty"`
}

type playlistSyncRunSummarySources struct {
	Attempted int                                `json:"attempted"`
	Succeeded int                                `json:"succeeded"`
	Failed    int                                `json:"failed"`
	Outcomes  []playlistSyncRunSummarySourceItem `json:"outcomes"`
}

type playlistSyncRunSummarySourceItem struct {
	SourceID   int64  `json:"source_id"`
	SourceKey  string `json:"source_key,omitempty"`
	SourceName string `json:"source_name,omitempty"`
	Status     string `json:"status"`
	Items      int    `json:"items,omitempty"`
	Error      string `json:"error,omitempty"`
}

type playlistSyncRunSummaryRefresh struct {
	Items int `json:"items"`
}

type playlistSyncRunSummaryMetrics struct {
	ChannelsProcessed     int `json:"channels_processed"`
	ChannelsTotal         int `json:"channels_total"`
	SourcesAdded          int `json:"sources_added"`
	SourcesAlreadySeen    int `json:"sources_existing"`
	DynamicBlocks         int `json:"dynamic_blocks"`
	DynamicBlocksEnabled  int `json:"dynamic_blocks_enabled"`
	DynamicAdded          int `json:"dynamic_added"`
	DynamicUpdated        int `json:"dynamic_updated"`
	DynamicRetained       int `json:"dynamic_retained"`
	DynamicRemoved        int `json:"dynamic_removed"`
	DynamicTruncated      int `json:"dynamic_truncated"`
	DynamicChannels       int `json:"dynamic_channels"`
	DynamicSourcesAdded   int `json:"dynamic_sources_added"`
	DynamicSourcesRemoved int `json:"dynamic_sources_removed"`
	DynamicNameUpdates    int `json:"dynamic_name_updates"`
}

type playlistSyncRunSummaryDVR struct {
	Reloaded   bool   `json:"reloaded"`
	Status     string `json:"status"`
	SkipReason string `json:"skip_reason"`
}

type playlistSyncRunSummaryFailure struct {
	Phase string `json:"phase"`
	Error string `json:"error"`
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
		settings:                 settings,
		refresher:                refresher,
		reconciler:               reconciler,
		sourceRefreshConcurrency: defaultPlaylistSyncSourceConcurrency,
	}, nil
}

// SetSourceRefreshConcurrency controls bounded source refresh parallelism for
// all-source runs. Values <= 1 keep sequential behavior. The value is clamped
// to [1, 16].
func (j *PlaylistSyncJob) SetSourceRefreshConcurrency(concurrency int) {
	if j == nil {
		return
	}
	j.sourceRefreshConcurrency = normalizeSourceRefreshConcurrency(concurrency)
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

	requestedSourceID, requestedSingleSource := PlaylistSyncSourceIDFromContext(ctx)
	sources, err := j.refreshSourcesForRun(ctx, requestedSourceID, requestedSingleSource)
	if err != nil {
		return err
	}

	refreshOutcomes, refreshedCount := j.refreshSources(ctx, sources)
	summary := buildPlaylistSyncRunSummary(requestedSourceID, requestedSingleSource, refreshOutcomes, refreshedCount)
	if summary.Sources.Succeeded == 0 {
		summary.DVRLineup.Status = dvr.ReloadStatusSkipped
		summary.DVRLineup.SkipReason = "no_sources_succeeded"
		summary.Failure = &playlistSyncRunSummaryFailure{
			Phase: playlistSyncFailurePhaseRefresh,
			Error: fmt.Sprintf(
				"all source refreshes failed (%s)",
				summarizePlaylistSyncFailedSourcesForError(refreshOutcomes),
			),
		}
		if err := setPlaylistSyncRunSummary(ctx, run, summary); err != nil {
			return err
		}
		return fmt.Errorf("refresh playlist: %s", summary.Failure.Error)
	}

	channelCount, err := j.reconciler.CountChannels(ctx)
	if err != nil {
		summary.Failure = &playlistSyncRunSummaryFailure{
			Phase: playlistSyncFailurePhaseCount,
			Error: strings.TrimSpace(err.Error()),
		}
		if setErr := setPlaylistSyncRunSummary(ctx, run, summary); setErr != nil {
			return setErr
		}
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
		cur, max, _ := run.Snapshot()
		summary.Reconcile = &playlistSyncRunSummaryMetrics{
			ChannelsProcessed: cur,
			ChannelsTotal:     max,
		}
		summary.Failure = &playlistSyncRunSummaryFailure{
			Phase: playlistSyncFailurePhaseReconcile,
			Error: strings.TrimSpace(err.Error()),
		}
		if setErr := setPlaylistSyncRunSummary(ctx, run, summary); setErr != nil {
			return setErr
		}
		return fmt.Errorf("reconcile channels: %w", err)
	}

	cur, max, _ := run.Snapshot()
	if progressThrottle.needsPersist(cur, max) {
		if err := run.persistProgress(ctx); err != nil {
			return err
		}
		progressThrottle.markPersist(time.Now(), cur, max)
	}

	summary.Reconcile = &playlistSyncRunSummaryMetrics{
		ChannelsProcessed:     reconcileResult.ChannelsProcessed,
		ChannelsTotal:         reconcileResult.ChannelsTotal,
		SourcesAdded:          reconcileResult.SourcesAdded,
		SourcesAlreadySeen:    reconcileResult.SourcesAlreadySeen,
		DynamicBlocks:         reconcileResult.DynamicBlocksProcessed,
		DynamicBlocksEnabled:  reconcileResult.DynamicBlocksEnabled,
		DynamicAdded:          reconcileResult.DynamicChannelsAdded,
		DynamicUpdated:        reconcileResult.DynamicChannelsUpdated,
		DynamicRetained:       reconcileResult.DynamicChannelsRetained,
		DynamicRemoved:        reconcileResult.DynamicChannelsRemoved,
		DynamicTruncated:      reconcileResult.DynamicChannelsTruncated,
		DynamicChannels:       reconcileResult.DynamicChannelsProcessed,
		DynamicSourcesAdded:   reconcileResult.DynamicSourcesAdded,
		DynamicSourcesRemoved: reconcileResult.DynamicSourcesRemoved,
		DynamicNameUpdates:    reconcileResult.DynamicGuideNamesUpdated,
	}

	if j.reloader != nil {
		outcome, err := j.reloader.ReloadLineupForPlaylistSyncOutcome(ctx)
		if err != nil {
			summary.DVRLineup.Status = dvr.ReloadStatusUnknown
			summary.DVRLineup.SkipReason = "reload_error"
			slog.Warn(
				"playlist sync dvr lineup reload failed (non-fatal)",
				"error", err,
			)
		} else {
			summary.DVRLineup.Reloaded, summary.DVRLineup.Status, summary.DVRLineup.SkipReason = normalizeReloadOutcomeForSummary(outcome)
		}
	}

	if err := setPlaylistSyncRunSummary(ctx, run, summary); err != nil {
		return err
	}

	return nil
}

func (j *PlaylistSyncJob) refreshSourcesForRun(ctx context.Context, requestedSourceID int64, requestedSingleSource bool) ([]playlist.PlaylistSource, error) {
	allSources, err := j.settings.ListPlaylistSources(ctx)
	if err != nil {
		return nil, fmt.Errorf("list playlist sources: %w", err)
	}
	if len(allSources) == 0 {
		return nil, fmt.Errorf("no playlist sources are configured")
	}

	legacyPrimaryURL, err := readPrimaryPlaylistURLSetting(ctx, j.settings)
	if err != nil {
		return nil, err
	}

	out := make([]playlist.PlaylistSource, 0, len(allSources))
	if requestedSingleSource {
		for _, source := range allSources {
			if source.SourceID != requestedSourceID {
				continue
			}
			source = applyLegacyPrimaryURL(source, legacyPrimaryURL)
			if !source.Enabled {
				return nil, fmt.Errorf("playlist source %d is disabled", requestedSourceID)
			}
			out = append(out, source)
			return out, nil
		}
		return nil, fmt.Errorf("%w: source_id %d", playlist.ErrPlaylistSourceNotFound, requestedSourceID)
	}

	for _, source := range allSources {
		source = applyLegacyPrimaryURL(source, legacyPrimaryURL)
		if !source.Enabled {
			continue
		}
		out = append(out, source)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no enabled playlist sources are configured")
	}
	return out, nil
}

func (j *PlaylistSyncJob) refreshSources(ctx context.Context, sources []playlist.PlaylistSource) ([]playlistSyncSourceOutcome, int) {
	if len(sources) == 0 {
		return nil, 0
	}

	workerCount := j.sourceRefreshWorkerCount(len(sources))
	if workerCount <= 1 || len(sources) == 1 {
		outcomes := make([]playlistSyncSourceOutcome, 0, len(sources))
		refreshedCount := 0
		for _, source := range sources {
			outcome := j.refreshSource(ctx, source)
			outcomes = append(outcomes, outcome)
			if outcome.Err == nil {
				refreshedCount += outcome.ItemCount
			}
		}
		return outcomes, refreshedCount
	}

	slog.Info(
		"playlist sync source refresh worker pool enabled",
		"source_count", len(sources),
		"worker_count", workerCount,
	)

	type indexedOutcome struct {
		index   int
		outcome playlistSyncSourceOutcome
	}

	workCh := make(chan int)
	resultCh := make(chan indexedOutcome, len(sources))

	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range workCh {
				resultCh <- indexedOutcome{
					index:   index,
					outcome: j.refreshSource(ctx, sources[index]),
				}
			}
		}()
	}

	go func() {
		for index := range sources {
			workCh <- index
		}
		close(workCh)
	}()

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	outcomes := make([]playlistSyncSourceOutcome, len(sources))
	refreshedCount := 0
	for result := range resultCh {
		outcomes[result.index] = result.outcome
		if result.outcome.Err == nil {
			refreshedCount += result.outcome.ItemCount
		}
	}
	return outcomes, refreshedCount
}

func (j *PlaylistSyncJob) refreshSource(ctx context.Context, source playlist.PlaylistSource) playlistSyncSourceOutcome {
	sourceLabel := playlistSyncSourceMetricLabel(source)
	startedAt := time.Now()
	slog.Info(
		"playlist sync source refresh started",
		"playlist_source_id", source.SourceID,
		"playlist_source_name", strings.TrimSpace(source.Name),
		"playlist_source_key", strings.TrimSpace(source.SourceKey),
		"playlist_url_configured", strings.TrimSpace(source.PlaylistURL) != "",
	)
	itemCount, refreshErr := j.refresher.RefreshForSource(ctx, source)
	playlistSyncSourceDurationMetric.WithLabelValues(sourceLabel).Observe(time.Since(startedAt).Seconds())
	if refreshErr != nil {
		playlistSyncSourceErrorsMetric.WithLabelValues(sourceLabel).Inc()
		playlistSyncSourceItemsMetric.WithLabelValues(sourceLabel).Set(0)
		slog.Warn(
			"playlist sync source refresh failed",
			"playlist_source_id", source.SourceID,
			"playlist_source_name", strings.TrimSpace(source.Name),
			"playlist_source_key", strings.TrimSpace(source.SourceKey),
			"error", refreshErr,
		)
		return playlistSyncSourceOutcome{
			Source:    source,
			ItemCount: 0,
			Err:       refreshErr,
		}
	}

	playlistSyncSourceItemsMetric.WithLabelValues(sourceLabel).Set(float64(itemCount))
	slog.Info(
		"playlist sync source refresh succeeded",
		"playlist_source_id", source.SourceID,
		"playlist_source_name", strings.TrimSpace(source.Name),
		"playlist_source_key", strings.TrimSpace(source.SourceKey),
		"item_count", itemCount,
	)
	return playlistSyncSourceOutcome{
		Source:    source,
		ItemCount: itemCount,
		Err:       nil,
	}
}

func (j *PlaylistSyncJob) sourceRefreshWorkerCount(sourceCount int) int {
	if sourceCount <= 1 {
		return 1
	}
	concurrency := defaultPlaylistSyncSourceConcurrency
	if j != nil {
		concurrency = normalizeSourceRefreshConcurrency(j.sourceRefreshConcurrency)
	}
	if concurrency > sourceCount {
		return sourceCount
	}
	return concurrency
}

func normalizeSourceRefreshConcurrency(concurrency int) int {
	if concurrency < 1 {
		return 1
	}
	if concurrency > maxPlaylistSyncSourceConcurrency {
		return maxPlaylistSyncSourceConcurrency
	}
	return concurrency
}

func readPrimaryPlaylistURLSetting(ctx context.Context, settings PlaylistSettingsStore) (string, error) {
	playlistURL, err := settings.GetSetting(ctx, settingPlaylistURL)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read playlist URL setting: %w", err)
	}
	return strings.TrimSpace(playlistURL), nil
}

func applyLegacyPrimaryURL(source playlist.PlaylistSource, legacyPrimaryURL string) playlist.PlaylistSource {
	source.Name = strings.TrimSpace(source.Name)
	source.SourceKey = strings.TrimSpace(source.SourceKey)
	source.PlaylistURL = strings.TrimSpace(source.PlaylistURL)
	if source.SourceID == 1 && strings.TrimSpace(legacyPrimaryURL) != "" {
		source.PlaylistURL = strings.TrimSpace(legacyPrimaryURL)
	}
	return source
}

func playlistSyncSourceMetricLabel(source playlist.PlaylistSource) string {
	if sourceKey := strings.TrimSpace(source.SourceKey); sourceKey != "" {
		return sourceKey
	}
	if source.SourceID > 0 {
		return strconv.FormatInt(source.SourceID, 10)
	}
	if sourceName := sanitizePlaylistSourceSummaryValue(source.Name); sourceName != "" {
		return sourceName
	}
	return "unknown"
}

func buildPlaylistSyncRunSummary(
	requestedSourceID int64,
	requestedSingleSource bool,
	outcomes []playlistSyncSourceOutcome,
	refreshedItems int,
) playlistSyncRunSummary {
	summary := playlistSyncRunSummary{
		SchemaVersion:     playlistSyncSummarySchemaVersion,
		RequestedSourceID: formatRequestedSourceIDSummary(requestedSourceID, requestedSingleSource),
		Sources: playlistSyncRunSummarySources{
			Attempted: len(outcomes),
			Outcomes:  make([]playlistSyncRunSummarySourceItem, 0, len(outcomes)),
		},
		Refresh: playlistSyncRunSummaryRefresh{
			Items: refreshedItems,
		},
		DVRLineup: playlistSyncRunSummaryDVR{
			Reloaded:   false,
			Status:     dvr.ReloadStatusDisabled,
			SkipReason: "none",
		},
	}

	for _, outcome := range outcomes {
		item := playlistSyncRunSummarySourceItem{
			SourceID:   outcome.Source.SourceID,
			SourceKey:  strings.TrimSpace(outcome.Source.SourceKey),
			SourceName: strings.TrimSpace(outcome.Source.Name),
		}
		if outcome.Err != nil {
			item.Status = "error"
			item.Error = strings.TrimSpace(outcome.Err.Error())
			summary.Sources.Failed++
		} else {
			item.Status = "success"
			item.Items = outcome.ItemCount
			summary.Sources.Succeeded++
		}
		summary.Sources.Outcomes = append(summary.Sources.Outcomes, item)
	}
	return summary
}

func setPlaylistSyncRunSummary(ctx context.Context, run *RunContext, summary playlistSyncRunSummary) error {
	encoded, err := json.Marshal(summary)
	if err != nil {
		return fmt.Errorf("marshal playlist sync summary: %w", err)
	}
	if err := run.SetSummary(ctx, string(encoded)); err != nil {
		return err
	}
	return nil
}

func summarizePlaylistSyncFailedSourcesForError(outcomes []playlistSyncSourceOutcome) string {
	failedParts := make([]string, 0, len(outcomes))
	for _, outcome := range outcomes {
		if outcome.Err == nil {
			continue
		}
		sourceName := sanitizePlaylistSourceSummaryValue(outcome.Source.Name)
		if sourceName == "" {
			sourceName = "unnamed"
		}
		errText := sanitizePlaylistSourceSummaryValue(outcome.Err.Error())
		failedParts = append(failedParts, fmt.Sprintf("id=%d,name=%s,error=%s", outcome.Source.SourceID, sourceName, errText))
	}
	if len(failedParts) == 0 {
		return "none"
	}
	return strings.Join(failedParts, "|")
}

func sanitizePlaylistSourceSummaryValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	replacer := strings.NewReplacer(
		"\n", " ",
		"\r", " ",
		"\t", " ",
		";", ",",
		"|", "/",
	)
	value = replacer.Replace(value)
	return strings.Join(strings.Fields(value), "_")
}

func formatRequestedSourceIDSummary(sourceID int64, requested bool) string {
	if !requested {
		return "all"
	}
	return strconv.FormatInt(sourceID, 10)
}

func normalizeReloadOutcomeForSummary(outcome dvr.ReloadOutcome) (reloaded bool, status string, skipReason string) {
	reloaded = outcome.Reloaded
	status = normalizeKnownReloadStatus(outcome.Status)
	if status == "" {
		switch {
		case outcome.Reloaded && (outcome.Skipped || outcome.Failed):
			status = dvr.ReloadStatusPartial
		case outcome.Failed:
			status = dvr.ReloadStatusFailed
		case outcome.Skipped:
			status = dvr.ReloadStatusSkipped
		case outcome.Reloaded:
			status = dvr.ReloadStatusReloaded
		default:
			status = dvr.ReloadStatusUnknown
		}
	}

	skipReason = "none"
	reasons := make([]string, 0, len(outcome.SkipReasons)+len(outcome.FailureReasons))
	for _, reason := range outcome.SkipReasons {
		trimmed := sanitizeReloadReasonSummaryValue(reason)
		if trimmed == "" {
			continue
		}
		reasons = append(reasons, trimmed)
	}
	for _, reason := range outcome.FailureReasons {
		trimmed := sanitizeReloadReasonSummaryValue(reason)
		if trimmed == "" {
			continue
		}
		reasons = append(reasons, trimmed)
	}
	if len(reasons) == 0 {
		return reloaded, status, skipReason
	}
	skipReason = strings.Join(reasons, ",")
	return reloaded, status, skipReason
}

func normalizeKnownReloadStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case dvr.ReloadStatusDisabled:
		return dvr.ReloadStatusDisabled
	case dvr.ReloadStatusFailed:
		return dvr.ReloadStatusFailed
	case dvr.ReloadStatusReloaded:
		return dvr.ReloadStatusReloaded
	case dvr.ReloadStatusPartial:
		return dvr.ReloadStatusPartial
	case dvr.ReloadStatusSkipped:
		return dvr.ReloadStatusSkipped
	case dvr.ReloadStatusUnknown:
		return dvr.ReloadStatusUnknown
	default:
		return ""
	}
}

func sanitizeReloadReasonSummaryValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	replacer := strings.NewReplacer(
		"\n", " ",
		"\r", " ",
		"\t", " ",
		";", " ",
		",", " ",
		"|", "/",
	)
	value = replacer.Replace(value)
	return strings.Join(strings.Fields(value), "_")
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
