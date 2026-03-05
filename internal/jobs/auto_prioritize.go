package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arodd/hdhriptv/internal/analyzer"
	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/stream"
)

const (
	settingAutoPrioritizeEnabledOnly    = "analyzer.autoprioritize.enabled_only"
	settingAutoPrioritizeTopNPerChannel = "analyzer.autoprioritize.top_n_per_channel"
	defaultHTTP429Backoff               = 60 * time.Second
	defaultProbeSlotAcquireBackoff      = 250 * time.Millisecond
	maxProbeSlotAcquireBackoff          = 2 * time.Second
	maxProbeSlotAcquireAttempts         = 3
	autoPrioritizeProgressPersistEvery  = 5
	autoPrioritizeProgressPersistWindow = 1 * time.Second

	healthPenaltyForcedDemotionFailCount = 2
	healthPenaltyRecentFailWindow        = 5 * time.Minute
	healthPenaltyWarmFailWindow          = 30 * time.Minute
	healthPenaltyCoolFailWindow          = 2 * time.Hour

	autoPrioritizeSkipReasonSourceLoadChannelNotFound = "source_load_channel_not_found"
	autoPrioritizeSkipReasonSourceLoadSourceNotFound  = "source_load_source_not_found"
	autoPrioritizeSkipReasonSourceLoadSourceSetDrift  = "source_load_source_set_drift"
	autoPrioritizeSkipReasonReorderChannelNotFound    = "reorder_channel_not_found"
	autoPrioritizeSkipReasonReorderSourceNotFound     = "reorder_source_not_found"
	autoPrioritizeSkipReasonReorderSourceSetDrift     = "reorder_source_set_drift"
)

var ErrProbePreempted = stream.ErrProbePreempted

var reServerReturnedStatus = regexp.MustCompile(`(?i)\bserver returned\s+([1-5][0-9]{2})\b`)

// TunerUsageProvider reports active stream usage from the shared tuner pool.
type TunerUsageProvider interface {
	InUseCount() int
	AcquireProbe(ctx context.Context, label string, cancel context.CancelCauseFunc) (*stream.Lease, error)
	AcquireProbeForSource(ctx context.Context, sourceID int64, label string, cancel context.CancelCauseFunc) (*stream.Lease, error)
}

// SourcePoolTunerUsageProvider exposes optional per-source pool accounting.
type SourcePoolTunerUsageProvider interface {
	CapacityForSource(sourceID int64) int
	InUseCountForSource(sourceID int64) int
}

// AutoPrioritizeSettingsStore reads analyzer-related automation settings.
type AutoPrioritizeSettingsStore interface {
	GetSetting(ctx context.Context, key string) (string, error)
}

// AutoPrioritizeChannelStore provides published channel/source operations.
type AutoPrioritizeChannelStore interface {
	ListEnabled(ctx context.Context) ([]channels.Channel, error)
	ListSourcesByChannelIDs(ctx context.Context, channelIDs []int64, enabledOnly bool) (map[int64][]channels.Source, error)
	ListSources(ctx context.Context, channelID int64, enabledOnly bool) ([]channels.Source, error)
	ReorderSources(ctx context.Context, channelID int64, sourceIDs []int64) error
	UpdateSourceProfile(ctx context.Context, sourceID int64, profile channels.SourceProfileUpdate) error
}

// AutoPrioritizeMetricsStore provides stream metrics cache operations.
type AutoPrioritizeMetricsStore interface {
	GetStreamMetric(ctx context.Context, itemKey string) (StreamMetric, error)
	UpsertStreamMetric(ctx context.Context, metric StreamMetric) error
}

// StreamAnalyzer probes one stream URL for quality metrics.
type StreamAnalyzer interface {
	Analyze(ctx context.Context, streamURL string) (analyzer.Metrics, error)
}

// AutoPrioritizeOptions customizes cache freshness behavior.
type AutoPrioritizeOptions struct {
	SuccessFreshness time.Duration
	ErrorRetry       time.Duration
	DefaultWorkers   int
	WorkerMode       string
	FixedWorkers     int
	TunerCount       int
	TunerUsage       TunerUsageProvider
	ProbeTuneDelay   time.Duration
	HTTP429Backoff   time.Duration
}

// AutoPrioritizeJob analyzes source quality and rewrites per-channel source order.
type AutoPrioritizeJob struct {
	settings AutoPrioritizeSettingsStore
	channels AutoPrioritizeChannelStore
	metrics  AutoPrioritizeMetricsStore
	analyzer StreamAnalyzer
	opts     AutoPrioritizeOptions
}

func NewAutoPrioritizeJob(
	settings AutoPrioritizeSettingsStore,
	channelsStore AutoPrioritizeChannelStore,
	metricsStore AutoPrioritizeMetricsStore,
	streamAnalyzer StreamAnalyzer,
	opts AutoPrioritizeOptions,
) (*AutoPrioritizeJob, error) {
	if settings == nil {
		return nil, fmt.Errorf("auto-prioritize settings store is required")
	}
	if channelsStore == nil {
		return nil, fmt.Errorf("auto-prioritize channel store is required")
	}
	if metricsStore == nil {
		return nil, fmt.Errorf("auto-prioritize metrics store is required")
	}
	if streamAnalyzer == nil {
		return nil, fmt.Errorf("auto-prioritize analyzer is required")
	}

	if opts.SuccessFreshness <= 0 {
		opts.SuccessFreshness = DefaultMetricsFreshness
	}
	if opts.ErrorRetry <= 0 {
		opts.ErrorRetry = DefaultErrorRetry
	}
	if opts.DefaultWorkers <= 0 {
		opts.DefaultWorkers = 4
	}
	opts.WorkerMode = strings.ToLower(strings.TrimSpace(opts.WorkerMode))
	if opts.WorkerMode == "" {
		opts.WorkerMode = "auto"
	}
	switch opts.WorkerMode {
	case "auto":
	case "fixed":
		if opts.FixedWorkers <= 0 {
			return nil, fmt.Errorf("auto-prioritize fixed workers must be at least 1")
		}
	default:
		return nil, fmt.Errorf("auto-prioritize worker mode %q is invalid", opts.WorkerMode)
	}
	if opts.ProbeTuneDelay < 0 {
		opts.ProbeTuneDelay = 0
	}
	if opts.HTTP429Backoff <= 0 {
		opts.HTTP429Backoff = defaultHTTP429Backoff
	}

	return &AutoPrioritizeJob{
		settings: settings,
		channels: channelsStore,
		metrics:  metricsStore,
		analyzer: streamAnalyzer,
		opts:     opts,
	}, nil
}

// Run executes source analysis and channel source reordering.
func (j *AutoPrioritizeJob) Run(ctx context.Context, run *RunContext) error {
	if run == nil {
		return fmt.Errorf("run context is required")
	}

	scope := j.resolveAnalysisScope(ctx)

	publishedChannels, err := j.channels.ListEnabled(ctx)
	if err != nil {
		return fmt.Errorf("list enabled channels: %w", err)
	}
	if err := run.SetProgress(ctx, 0, len(publishedChannels)); err != nil {
		return err
	}
	progressThrottle := newProgressPersistThrottle(
		autoPrioritizeProgressPersistEvery,
		autoPrioritizeProgressPersistWindow,
	)
	progressThrottle.markPersist(time.Now(), 0, len(publishedChannels))

	now := time.Now().UTC()
	channelSources := make(map[int64][]channels.Source, len(publishedChannels))
	skipTelemetry := newAutoPrioritizeSkipTelemetry()
	channelIDs := make([]int64, 0, len(publishedChannels))
	for _, channel := range publishedChannels {
		channelIDs = append(channelIDs, channel.ChannelID)
	}
	sourcesByChannel, err := j.channels.ListSourcesByChannelIDs(ctx, channelIDs, false)
	if err != nil {
		if _, drift := classifyAutoPrioritizeSourceLoadSkipReason(err); !drift {
			return fmt.Errorf("list channel sources: %w", err)
		}
		sourcesByChannel, err = j.listChannelSourcesBestEffort(ctx, publishedChannels, skipTelemetry)
		if err != nil {
			return err
		}
	}

	sourceIDsByItem := make(map[string][]int64)
	sourceIDSeenByItem := make(map[string]map[int64]struct{})
	metricsByItem := make(map[string]StreamMetric)
	tasks := make([]analysisTask, 0)
	queuedItemKeys := make(map[string]struct{})
	cacheHits := 0
	limitedChannels := 0

	for _, channel := range publishedChannels {
		sources, ok := sourcesByChannel[channel.ChannelID]
		if !ok {
			// Best-effort fallback should populate all requested channel IDs.
			// If one is absent, treat it as mutation drift and continue.
			skipTelemetry.mark(channel.ChannelID, autoPrioritizeSkipReasonSourceLoadChannelNotFound)
			continue
		}
		channelSources[channel.ChannelID] = sources
		for _, source := range sources {
			itemKey := strings.TrimSpace(source.ItemKey)
			if itemKey == "" || source.SourceID <= 0 {
				continue
			}
			seenByItem, ok := sourceIDSeenByItem[itemKey]
			if !ok {
				seenByItem = make(map[int64]struct{})
				sourceIDSeenByItem[itemKey] = seenByItem
			}
			if _, exists := seenByItem[source.SourceID]; exists {
				continue
			}
			seenByItem[source.SourceID] = struct{}{}
			sourceIDsByItem[itemKey] = append(sourceIDsByItem[itemKey], source.SourceID)
		}

		analysisSources := selectAnalysisSources(sources, scope.EnabledOnly, scope.TopNPerChannel)
		if scope.TopNPerChannel > 0 && len(analysisSources) == scope.TopNPerChannel {
			eligible := 0
			for _, source := range sources {
				if scope.EnabledOnly && !source.Enabled {
					continue
				}
				eligible++
			}
			if eligible > scope.TopNPerChannel {
				limitedChannels++
			}
		}

		for _, source := range analysisSources {
			itemKey := strings.TrimSpace(source.ItemKey)
			if itemKey == "" {
				continue
			}
			if _, exists := metricsByItem[itemKey]; exists {
				continue
			}
			if _, exists := queuedItemKeys[itemKey]; exists {
				continue
			}

			if strings.TrimSpace(source.StreamURL) == "" {
				metric := StreamMetric{
					ItemKey:    itemKey,
					AnalyzedAt: now.Unix(),
					Error:      "stream URL is empty",
				}
				metricsByItem[itemKey] = metric
				continue
			}

			cached, cacheErr := j.metrics.GetStreamMetric(ctx, itemKey)
			switch {
			case cacheErr == nil:
				if !cached.NeedsRefresh(now, j.opts.SuccessFreshness, j.opts.ErrorRetry) {
					metricsByItem[itemKey] = cached
					cacheHits++
					continue
				}
				tasks = append(tasks, analysisTask{
					ItemKey:          itemKey,
					StreamURL:        source.StreamURL,
					PlaylistSourceID: source.PlaylistSourceID,
				})
				queuedItemKeys[itemKey] = struct{}{}
			case cacheErr == sql.ErrNoRows:
				tasks = append(tasks, analysisTask{
					ItemKey:          itemKey,
					StreamURL:        source.StreamURL,
					PlaylistSourceID: source.PlaylistSourceID,
				})
				queuedItemKeys[itemKey] = struct{}{}
			default:
				return fmt.Errorf("load cached metric for %q: %w", itemKey, cacheErr)
			}
		}
	}

	workers := j.resolveWorkers()
	sourceProbeAvailability := j.resolveSourceProbeAvailability(tasks)
	if len(tasks) > 0 && workers <= 0 {
		// Even when initial slot snapshots are fully occupied, probe acquisition
		// should still run bounded retries so transient contention can recover.
		workers = 1
	}
	analysisResult, err := j.analyzePending(ctx, tasks, workers, sourceProbeAvailability, now)
	if err != nil {
		return err
	}
	for itemKey, metric := range analysisResult.ByItem {
		metricsByItem[itemKey] = metric
		if err := j.metrics.UpsertStreamMetric(ctx, metric); err != nil {
			return fmt.Errorf("cache analyzed metric for %q: %w", itemKey, err)
		}
	}
	if err := j.persistFreshProfiles(ctx, analysisResult.ByItem, sourceIDsByItem); err != nil {
		return err
	}

	reorderedChannels := 0
	for i, channel := range publishedChannels {
		sources := channelSources[channel.ChannelID]
		orderedIDs, changed := orderSourcesByScore(sources, metricsByItem)
		if changed {
			if err := j.channels.ReorderSources(ctx, channel.ChannelID, orderedIDs); err != nil {
				if reason, drift := classifyAutoPrioritizeReorderSkipReason(err); drift {
					skipTelemetry.mark(channel.ChannelID, reason)
				} else {
					return fmt.Errorf("reorder sources for channel %d: %w", channel.ChannelID, err)
				}
			} else {
				reorderedChannels++
			}
		}
		cur := i + 1
		max := len(publishedChannels)
		if err := run.setProgressInMemory(cur, max); err != nil {
			return err
		}
		persistNow := time.Now()
		if !progressThrottle.shouldPersist(persistNow, cur, max) {
			continue
		}
		if err := run.persistProgress(ctx); err != nil {
			return err
		}
		progressThrottle.markPersist(persistNow, cur, max)
	}

	cur, max, _ := run.Snapshot()
	if progressThrottle.needsPersist(cur, max) {
		if err := run.persistProgress(ctx); err != nil {
			return err
		}
		progressThrottle.markPersist(time.Now(), cur, max)
	}

	summary := fmt.Sprintf(
		"channels=%d analyzed=%d cache_hits=%d reordered=%d skipped_channels=%d analysis_errors=%d analysis_error_buckets=%s skip_reason_buckets=%s enabled_only=%t top_n_per_channel=%d limited_channels=%d",
		len(publishedChannels),
		analysisResult.Count,
		cacheHits,
		reorderedChannels,
		skipTelemetry.skippedChannels(),
		analysisResult.Errors,
		formatAnalysisErrorBucketsSummary(analysisResult.ErrorBuckets),
		formatSkipReasonBucketsSummary(skipTelemetry.reasonBuckets),
		scope.EnabledOnly,
		scope.TopNPerChannel,
		limitedChannels,
	)
	if err := run.SetSummary(ctx, summary); err != nil {
		return err
	}

	return nil
}

func (j *AutoPrioritizeJob) listChannelSourcesBestEffort(
	ctx context.Context,
	publishedChannels []channels.Channel,
	skipTelemetry *autoPrioritizeSkipTelemetry,
) (map[int64][]channels.Source, error) {
	out := make(map[int64][]channels.Source, len(publishedChannels))
	for _, channel := range publishedChannels {
		sources, err := j.channels.ListSources(ctx, channel.ChannelID, false)
		if err != nil {
			if reason, drift := classifyAutoPrioritizeSourceLoadSkipReason(err); drift {
				skipTelemetry.mark(channel.ChannelID, reason)
				out[channel.ChannelID] = nil
				continue
			}
			return nil, fmt.Errorf("list channel sources for channel %d: %w", channel.ChannelID, err)
		}
		out[channel.ChannelID] = sources
	}
	return out, nil
}

type autoPrioritizeSkipTelemetry struct {
	skippedByChannel map[int64]struct{}
	reasonBuckets    map[string]int
}

func newAutoPrioritizeSkipTelemetry() *autoPrioritizeSkipTelemetry {
	return &autoPrioritizeSkipTelemetry{
		skippedByChannel: make(map[int64]struct{}),
		reasonBuckets:    make(map[string]int),
	}
}

func (t *autoPrioritizeSkipTelemetry) mark(channelID int64, reason string) {
	if t == nil {
		return
	}
	if channelID > 0 {
		t.skippedByChannel[channelID] = struct{}{}
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return
	}
	t.reasonBuckets[reason]++
}

func (t *autoPrioritizeSkipTelemetry) skippedChannels() int {
	if t == nil {
		return 0
	}
	return len(t.skippedByChannel)
}

func classifyAutoPrioritizeSourceLoadSkipReason(err error) (string, bool) {
	reason, ok := classifyAutoPrioritizeMutationDrift(err)
	if !ok {
		return "", false
	}
	switch reason {
	case "channel_not_found":
		return autoPrioritizeSkipReasonSourceLoadChannelNotFound, true
	case "source_not_found":
		return autoPrioritizeSkipReasonSourceLoadSourceNotFound, true
	default:
		return autoPrioritizeSkipReasonSourceLoadSourceSetDrift, true
	}
}

func classifyAutoPrioritizeReorderSkipReason(err error) (string, bool) {
	reason, ok := classifyAutoPrioritizeMutationDrift(err)
	if !ok {
		return "", false
	}
	switch reason {
	case "channel_not_found":
		return autoPrioritizeSkipReasonReorderChannelNotFound, true
	case "source_not_found":
		return autoPrioritizeSkipReasonReorderSourceNotFound, true
	default:
		return autoPrioritizeSkipReasonReorderSourceSetDrift, true
	}
}

func classifyAutoPrioritizeMutationDrift(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	switch {
	case errors.Is(err, channels.ErrChannelNotFound):
		return "channel_not_found", true
	case errors.Is(err, channels.ErrSourceNotFound):
		return "source_not_found", true
	case errors.Is(err, channels.ErrSourceOrderDrift):
		return "source_set_drift", true
	}

	text := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(text, "source_ids count mismatch"):
		return "source_set_drift", true
	case strings.Contains(text, "source_ids missing id"):
		return "source_set_drift", true
	default:
		return "", false
	}
}

func (j *AutoPrioritizeJob) resolveWorkers() int {
	if j.opts.WorkerMode == "fixed" {
		workers := j.opts.FixedWorkers
		if workers <= 0 {
			workers = 1
		}
		if workers > 64 {
			workers = 64
		}

		// When tuner capacity is configured, fixed workers are still capped by
		// currently available tuner slots to avoid immediate probe lease failures.
		if j.opts.TunerCount > 0 {
			inUse := 0
			if j.opts.TunerUsage != nil {
				inUse = j.opts.TunerUsage.InUseCount()
			}
			if inUse < 0 {
				inUse = 0
			}
			available := j.opts.TunerCount - inUse
			if available < 0 {
				available = 0
			}
			if workers > available {
				workers = available
			}
		}
		return workers
	}

	if j.opts.TunerCount > 0 {
		inUse := 0
		if j.opts.TunerUsage != nil {
			inUse = j.opts.TunerUsage.InUseCount()
		}
		if inUse < 0 {
			inUse = 0
		}
		available := j.opts.TunerCount - inUse
		if available < 0 {
			available = 0
		}
		return available
	}

	workers := j.opts.DefaultWorkers
	if workers <= 0 {
		workers = 4
	}
	if workers > 64 {
		return 64
	}
	return workers
}

func (j *AutoPrioritizeJob) resolveSourceProbeAvailability(tasks []analysisTask) map[int64]int {
	if j == nil || j.opts.TunerUsage == nil || len(tasks) == 0 {
		return nil
	}
	perSource, ok := j.opts.TunerUsage.(SourcePoolTunerUsageProvider)
	if !ok {
		return nil
	}

	availability := make(map[int64]int)
	for _, task := range tasks {
		sourceID := task.PlaylistSourceID
		if sourceID <= 0 {
			continue
		}
		if _, seen := availability[sourceID]; seen {
			continue
		}
		capacity := perSource.CapacityForSource(sourceID)
		if capacity < 0 {
			// Negative capacity indicates the provider does not expose source-
			// specific accounting for this source (legacy/shared-pool path).
			continue
		}
		inUse := perSource.InUseCountForSource(sourceID)
		if inUse < 0 {
			inUse = 0
		}
		available := capacity - inUse
		if available < 0 {
			available = 0
		}
		availability[sourceID] = available
	}
	return availability
}

type analysisScope struct {
	EnabledOnly    bool
	TopNPerChannel int
}

func (j *AutoPrioritizeJob) resolveAnalysisScope(ctx context.Context) analysisScope {
	scope := analysisScope{
		EnabledOnly:    true,
		TopNPerChannel: 0,
	}

	enabledOnlyRaw, err := j.settings.GetSetting(ctx, settingAutoPrioritizeEnabledOnly)
	if err == nil {
		switch strings.ToLower(strings.TrimSpace(enabledOnlyRaw)) {
		case "1", "true", "yes", "on":
			scope.EnabledOnly = true
		case "0", "false", "no", "off":
			scope.EnabledOnly = false
		}
	}

	topNRaw, err := j.settings.GetSetting(ctx, settingAutoPrioritizeTopNPerChannel)
	if err == nil {
		if parsed, parseErr := strconv.Atoi(strings.TrimSpace(topNRaw)); parseErr == nil {
			if parsed < 0 {
				parsed = 0
			}
			if parsed > 100 {
				parsed = 100
			}
			scope.TopNPerChannel = parsed
		}
	}

	return scope
}

func selectAnalysisSources(sources []channels.Source, enabledOnly bool, topNPerChannel int) []channels.Source {
	selected := make([]channels.Source, 0, len(sources))
	for _, source := range sources {
		if enabledOnly && !source.Enabled {
			continue
		}
		selected = append(selected, source)
	}

	if topNPerChannel > 0 && len(selected) > topNPerChannel {
		selected = selected[:topNPerChannel]
	}
	return selected
}

type analysisTask struct {
	ItemKey          string
	StreamURL        string
	PlaylistSourceID int64
}

type analysisAggregate struct {
	ByItem       map[string]StreamMetric
	Count        int
	Errors       int
	ErrorBuckets map[string]int
}

func (j *AutoPrioritizeJob) analyzePending(
	ctx context.Context,
	tasks []analysisTask,
	workers int,
	sourceProbeAvailability map[int64]int,
	now time.Time,
) (analysisAggregate, error) {
	result := analysisAggregate{
		ByItem:       make(map[string]StreamMetric),
		ErrorBuckets: make(map[string]int),
	}
	if len(tasks) == 0 {
		return result, nil
	}
	if workers < 1 {
		workers = 1
	}

	taskCh := make(chan analysisTask)
	resultCh := make(chan StreamMetric, len(tasks))
	workCtx, cancelWork := context.WithCancelCause(ctx)
	defer cancelWork(nil)

	var (
		fatalMu  sync.Mutex
		fatalErr error
	)
	setFatal := func(err error) {
		if err == nil {
			return
		}
		fatalMu.Lock()
		if fatalErr == nil {
			fatalErr = err
			cancelWork(err)
		}
		fatalMu.Unlock()
	}

	sourceSemaphores := make(map[int64]chan struct{}, len(sourceProbeAvailability))
	for sourceID, available := range sourceProbeAvailability {
		if available <= 0 {
			continue
		}
		sem := make(chan struct{}, available)
		for i := 0; i < available; i++ {
			sem <- struct{}{}
		}
		sourceSemaphores[sourceID] = sem
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				if workCtx.Err() != nil {
					return
				}

				metric := StreamMetric{
					ItemKey:    task.ItemKey,
					AnalyzedAt: now.Unix(),
				}
				taskSourceID := task.PlaylistSourceID

				permit := sourceSemaphores[taskSourceID]
				acquiredSourcePermit := false
				if permit != nil {
					select {
					case <-workCtx.Done():
						return
					case <-permit:
						acquiredSourcePermit = true
					}
				}
				retriedAfterHTTP429 := false
				probeContentionAttempts := 0

				for {
					probeCtx := workCtx
					var (
						probeCancel context.CancelCauseFunc
						probeLease  *stream.Lease
					)
					acquiredProbeLease := false
					if j.opts.TunerUsage != nil {
						probeCtx, probeCancel = context.WithCancelCause(workCtx)
						var acquireErr error
						probeLease, acquireErr = j.opts.TunerUsage.AcquireProbeForSource(
							probeCtx,
							taskSourceID,
							task.ItemKey,
							probeCancel,
						)
						if acquireErr != nil {
							if probeCancel != nil {
								probeCancel(nil)
							}
							if workCtx.Err() != nil {
								if acquiredSourcePermit {
									permit <- struct{}{}
								}
								return
							}
							if errors.Is(acquireErr, stream.ErrNoTunersAvailable) {
								probeContentionAttempts++
								if probeContentionAttempts >= maxProbeSlotAcquireAttempts {
									metric.Error = formatProbeSlotUnavailableError(taskSourceID, probeContentionAttempts, acquireErr)
									break
								}
								if waitErr := waitForProbeTuneDelay(workCtx, probeSlotAcquireBackoff(probeContentionAttempts)); waitErr != nil {
									if acquiredSourcePermit {
										permit <- struct{}{}
									}
									return
								}
								continue
							}
							setFatal(fmt.Errorf("acquire probe slot for %q: %w", task.ItemKey, acquireErr))
							break
						}
						acquiredProbeLease = true
						probeContentionAttempts = 0
					}

					probe, err := j.analyzer.Analyze(probeCtx, task.StreamURL)
					if probeLease != nil {
						probeLease.Release()
					}
					if probeCancel != nil {
						probeCancel(nil)
					}
					if err != nil {
						if errors.Is(context.Cause(probeCtx), ErrProbePreempted) {
							setFatal(fmt.Errorf("auto-prioritize interrupted: %w", ErrProbePreempted))
							if acquiredSourcePermit {
								permit <- struct{}{}
							}
							return
						}
						if workCtx.Err() != nil {
							if acquiredSourcePermit {
								permit <- struct{}{}
							}
							return
						}
						if classifyAnalysisError(err.Error()) == "http_429" && !retriedAfterHTTP429 {
							retriedAfterHTTP429 = true
							if waitErr := waitForProbeTuneDelay(workCtx, j.opts.HTTP429Backoff); waitErr != nil {
								if acquiredSourcePermit {
									permit <- struct{}{}
								}
								return
							}
							continue
						}
						metric.Error = err.Error()
					} else {
						metric.Width = probe.Width
						metric.Height = probe.Height
						metric.FPS = probe.FPS
						metric.VideoCodec = strings.TrimSpace(probe.VideoCodec)
						metric.AudioCodec = strings.TrimSpace(probe.AudioCodec)
						metric.BitrateBPS = probe.BitrateBPS
						metric.VariantBPS = probe.VariantBPS
					}
					if acquiredProbeLease {
						if err := waitForProbeTuneDelay(workCtx, j.opts.ProbeTuneDelay); err != nil {
							if acquiredSourcePermit {
								permit <- struct{}{}
							}
							return
						}
					}
					break
				}
				if workCtx.Err() != nil {
					if acquiredSourcePermit {
						permit <- struct{}{}
					}
					return
				}
				if acquiredSourcePermit {
					permit <- struct{}{}
				}
				resultCh <- metric
			}
		}()
	}

	go func() {
		for _, task := range tasks {
			select {
			case <-workCtx.Done():
				close(taskCh)
				return
			case taskCh <- task:
			}
		}
		close(taskCh)
	}()

	wg.Wait()
	close(resultCh)

	for metric := range resultCh {
		if metric.ItemKey == "" {
			continue
		}
		result.ByItem[metric.ItemKey] = metric
		result.Count++
		if strings.TrimSpace(metric.Error) != "" {
			result.Errors++
			bucket := classifyAnalysisError(metric.Error)
			if bucket == "" {
				bucket = "other"
			}
			result.ErrorBuckets[bucket]++
		}
	}

	fatalMu.Lock()
	err := fatalErr
	fatalMu.Unlock()
	if err != nil {
		return analysisAggregate{}, err
	}

	if err := ctx.Err(); err != nil {
		return analysisAggregate{}, err
	}
	return result, nil
}

func (j *AutoPrioritizeJob) persistFreshProfiles(
	ctx context.Context,
	analyzedByItem map[string]StreamMetric,
	sourceIDsByItem map[string][]int64,
) error {
	for itemKey, metric := range analyzedByItem {
		itemKey = strings.TrimSpace(itemKey)
		if itemKey == "" {
			continue
		}
		if strings.TrimSpace(metric.Error) != "" {
			continue
		}

		sourceIDs := sourceIDsByItem[itemKey]
		if len(sourceIDs) == 0 {
			continue
		}

		probeAt := time.Time{}
		if metric.AnalyzedAt > 0 {
			probeAt = time.Unix(metric.AnalyzedAt, 0).UTC()
		}
		profile := channels.SourceProfileUpdate{
			LastProbeAt: probeAt,
			Width:       metric.Width,
			Height:      metric.Height,
			FPS:         metric.FPS,
			VideoCodec:  strings.TrimSpace(metric.VideoCodec),
			AudioCodec:  strings.TrimSpace(metric.AudioCodec),
			BitrateBPS:  metric.BitrateBPS,
		}

		for _, sourceID := range sourceIDs {
			if sourceID <= 0 {
				continue
			}
			if err := j.channels.UpdateSourceProfile(ctx, sourceID, profile); err != nil {
				return fmt.Errorf("persist source profile for item %q source %d: %w", itemKey, sourceID, err)
			}
		}
	}
	return nil
}

func waitForProbeTuneDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func classifyAnalysisError(errText string) string {
	text := strings.ToLower(strings.TrimSpace(errText))
	if text == "" {
		return ""
	}

	if match := reServerReturnedStatus.FindStringSubmatch(text); len(match) == 2 {
		return "http_" + match[1]
	}

	switch {
	case strings.Contains(text, "decode ffprobe json"):
		return "decode_ffprobe_json"
	case strings.Contains(text, "ffprobe returned no video streams"):
		return "ffprobe_no_video_streams"
	case strings.Contains(text, "stream url is empty"):
		return "stream_url_empty"
	case strings.Contains(text, "probe slot unavailable"):
		return "probe_slot_unavailable"
	case strings.Contains(text, "deadline exceeded"),
		strings.Contains(text, "timed out"),
		strings.Contains(text, "timeout"):
		return "timeout"
	case strings.Contains(text, "no such host"):
		return "dns_no_such_host"
	case strings.Contains(text, "connection refused"):
		return "connection_refused"
	case strings.Contains(text, "ffmpeg sample failed"):
		return "ffmpeg_sample_failed"
	case strings.Contains(text, "ffprobe failed"):
		return "ffprobe_failed"
	default:
		return "other"
	}
}

func probeSlotAcquireBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	backoff := defaultProbeSlotAcquireBackoff << (attempt - 1)
	if backoff > maxProbeSlotAcquireBackoff {
		return maxProbeSlotAcquireBackoff
	}
	return backoff
}

func formatProbeSlotUnavailableError(sourceID int64, attempts int, cause error) string {
	return fmt.Sprintf(
		"probe slot unavailable for playlist source %d after %d attempts: %v",
		sourceID,
		attempts,
		cause,
	)
}

func formatAnalysisErrorBucketsSummary(buckets map[string]int) string {
	return formatCountBucketsSummary(buckets)
}

func formatSkipReasonBucketsSummary(buckets map[string]int) string {
	return formatCountBucketsSummary(buckets)
}

func formatCountBucketsSummary(buckets map[string]int) string {
	if len(buckets) == 0 {
		return "none"
	}

	type bucketCount struct {
		name  string
		count int
	}

	ordered := make([]bucketCount, 0, len(buckets))
	for name, count := range buckets {
		if strings.TrimSpace(name) == "" || count <= 0 {
			continue
		}
		ordered = append(ordered, bucketCount{name: strings.TrimSpace(name), count: count})
	}
	if len(ordered) == 0 {
		return "none"
	}

	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].count != ordered[j].count {
			return ordered[i].count > ordered[j].count
		}
		return ordered[i].name < ordered[j].name
	})

	parts := make([]string, 0, len(ordered))
	for _, entry := range ordered {
		parts = append(parts, fmt.Sprintf("%s:%d", entry.name, entry.count))
	}
	return strings.Join(parts, ",")
}

type scoredSource struct {
	Source channels.Source
	Score  float64
}

func orderSourcesByScore(sources []channels.Source, metricsByItem map[string]StreamMetric) ([]int64, bool) {
	if len(sources) == 0 {
		return nil, false
	}
	nowUnix := time.Now().UTC().Unix()

	original := make([]int64, 0, len(sources))
	enabled := make([]scoredSource, 0, len(sources))
	disabled := make([]int64, 0)

	maxRes := float64(0)
	maxFPS := float64(0)
	maxBitrate := float64(0)

	for _, source := range sources {
		original = append(original, source.SourceID)
		if !source.Enabled {
			disabled = append(disabled, source.SourceID)
			continue
		}

		metric, ok := metricsByItem[strings.TrimSpace(source.ItemKey)]
		if !ok {
			enabled = append(enabled, scoredSource{Source: source})
			continue
		}

		res := float64(metric.Width * metric.Height)
		fps := metric.FPS
		bitrate := float64(metric.BitrateBPS)
		if metric.BitrateBPS <= 0 && metric.VariantBPS > 0 {
			bitrate = float64(metric.VariantBPS)
		}

		if metric.IsScorable() {
			if res > maxRes {
				maxRes = res
			}
			if fps > maxFPS {
				maxFPS = fps
			}
			if bitrate > maxBitrate {
				maxBitrate = bitrate
			}
		}

		enabled = append(enabled, scoredSource{Source: source})
	}

	for i := range enabled {
		metric := metricsByItem[strings.TrimSpace(enabled[i].Source.ItemKey)]
		if !metric.IsScorable() {
			enabled[i].Score = 0
			continue
		}

		resNorm := 0.0
		fpsNorm := 0.0
		brNorm := 0.0

		if maxRes > 0 {
			resNorm = float64(metric.Width*metric.Height) / maxRes
		}
		if maxFPS > 0 {
			fpsNorm = metric.FPS / maxFPS
		}
		bitrate := float64(metric.BitrateBPS)
		if metric.BitrateBPS <= 0 && metric.VariantBPS > 0 {
			bitrate = float64(metric.VariantBPS)
		}
		if maxBitrate > 0 {
			brNorm = bitrate / maxBitrate
		}

		qualityScore := resNorm + fpsNorm + brNorm
		healthPenalty := sourceFailureHealthPenalty(enabled[i].Source, nowUnix)
		enabled[i].Score = qualityScore - healthPenalty
	}

	sort.SliceStable(enabled, func(i, k int) bool {
		iDemoted := sourceHasPersistentFailureDemotion(enabled[i].Source)
		kDemoted := sourceHasPersistentFailureDemotion(enabled[k].Source)
		if iDemoted != kDemoted {
			return !iDemoted
		}
		if !almostEqual(enabled[i].Score, enabled[k].Score) {
			return enabled[i].Score > enabled[k].Score
		}
		if enabled[i].Source.LastOKAt != enabled[k].Source.LastOKAt {
			return enabled[i].Source.LastOKAt > enabled[k].Source.LastOKAt
		}
		return enabled[i].Source.SourceID < enabled[k].Source.SourceID
	})

	ordered := make([]int64, 0, len(sources))
	for _, src := range enabled {
		ordered = append(ordered, src.Source.SourceID)
	}
	ordered = append(ordered, disabled...)

	changed := len(ordered) == len(original)
	if changed {
		for i := range ordered {
			if ordered[i] != original[i] {
				changed = true
				break
			}
			if i == len(ordered)-1 {
				changed = false
			}
		}
	}
	return ordered, changed
}

func sourceHasPersistentFailureDemotion(source channels.Source) bool {
	return source.FailCount >= healthPenaltyForcedDemotionFailCount
}

func sourceFailureHealthPenalty(source channels.Source, nowUnix int64) float64 {
	failCount := source.FailCount
	if failCount < 0 {
		failCount = 0
	}

	// If there is no active failure streak and the source has recovered after
	// its last failure, do not apply a ranking penalty.
	if failCount == 0 && source.LastFailAt <= source.LastOKAt {
		return 0
	}

	penalty := 0.0
	switch {
	case failCount <= 0:
		// no-op
	case failCount == 1:
		penalty += 0.40
	case failCount == 2:
		penalty += 0.85
	case failCount == 3:
		penalty += 1.30
	default:
		penalty += 1.90
	}

	if source.CooldownUntil > nowUnix {
		penalty += 0.90
	}

	if source.LastFailAt > 0 {
		ageSeconds := nowUnix - source.LastFailAt
		switch {
		case ageSeconds <= int64(healthPenaltyRecentFailWindow.Seconds()):
			penalty += 0.75
		case ageSeconds <= int64(healthPenaltyWarmFailWindow.Seconds()):
			penalty += 0.50
		case ageSeconds <= int64(healthPenaltyCoolFailWindow.Seconds()):
			penalty += 0.25
		default:
			penalty += 0.10
		}
	}

	// If the most recent health signal is a failure, keep an additional guardrail
	// so quality probes do not immediately re-promote this source.
	if source.LastFailAt > source.LastOKAt {
		penalty += 0.35
	}

	if penalty > 2.95 {
		return 2.95
	}
	return penalty
}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}
