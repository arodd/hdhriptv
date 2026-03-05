package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type dynamicChannelSyncRequest struct {
	channel   channels.Channel
	shouldRun bool
	version   uint64
}

type dynamicChannelSyncState struct {
	running       bool
	pending       dynamicChannelSyncRequest
	hasPending    bool
	latestVersion uint64
	runCancel     context.CancelFunc
}

type dynamicBlockSyncState struct {
	running       bool
	hasPending    bool
	latestVersion uint64
	runCancel     context.CancelFunc
}

type dvrLineupReloadState struct {
	running        bool
	pending        bool
	timerSeq       uint64
	timerDueAt     time.Time
	timerCancel    chan struct{}
	firstQueuedAt  time.Time
	lastQueuedAt   time.Time
	coalescedCount int
	reasonCounts   map[string]int
}

type dvrLineupReloadBatch struct {
	reasonSummary   string
	coalescedCount  int
	firstQueuedAt   time.Time
	lastQueuedAt    time.Time
	queuedDuration  time.Duration
	queueWindowSpan time.Duration
}

var dynamicChannelImmediateSyncFailuresMetric = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "admin_dynamic_channel_immediate_sync_failures_total",
	Help: "Total number of admin-triggered immediate dynamic channel sync failures grouped by failure stage and reason.",
}, []string{"stage", "reason"})

func (h *AdminHandler) scheduleDynamicChannelSync(r *http.Request, channel channels.Channel) {
	if h == nil || r == nil {
		return
	}
	shouldRun := shouldRunDynamicChannelSync(channel)
	queuedWhileRunning, canceledRunning := h.enqueueDynamicChannelSync(channel, shouldRun)
	if !shouldRun {
		if canceledRunning {
			h.logAdminMutation(
				r,
				"admin dynamic channel immediate sync canceled",
				"channel_id", channel.ChannelID,
				"guide_number", channel.GuideNumber,
				"source_ids", channel.DynamicRule.SourceIDs,
				"dynamic_enabled", channel.DynamicRule.Enabled,
				"group", channel.DynamicRule.GroupName,
				"query", channel.DynamicRule.SearchQuery,
				"query_regex", channel.DynamicRule.SearchRegex,
				"queued_while_running", queuedWhileRunning,
				"canceled_running", canceledRunning,
			)
		}
		return
	}

	h.logAdminMutation(
		r,
		"admin dynamic channel immediate sync queued",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"source_ids", channel.DynamicRule.SourceIDs,
		"group", channel.DynamicRule.GroupName,
		"query", channel.DynamicRule.SearchQuery,
		"query_regex", channel.DynamicRule.SearchRegex,
		"queued_while_running", queuedWhileRunning,
		"canceled_running", canceledRunning,
	)
}

func shouldRunDynamicChannelSync(channel channels.Channel) bool {
	if !channel.DynamicRule.Enabled {
		return false
	}
	return strings.TrimSpace(channel.DynamicRule.SearchQuery) != ""
}

func (h *AdminHandler) enqueueDynamicChannelSync(channel channels.Channel, shouldRun bool) (bool, bool) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	select {
	case <-h.closeCh:
		return false, false
	default:
	}

	if h.dynamicSyncStates == nil {
		h.dynamicSyncStates = make(map[int64]*dynamicChannelSyncState)
	}

	state, ok := h.dynamicSyncStates[channel.ChannelID]
	if !ok {
		if !shouldRun {
			return false, false
		}
		state = &dynamicChannelSyncState{}
		h.dynamicSyncStates[channel.ChannelID] = state
	}
	state.latestVersion++
	request := dynamicChannelSyncRequest{
		channel:   channel,
		shouldRun: shouldRun,
		version:   state.latestVersion,
	}

	if state.running {
		state.pending = request
		state.hasPending = true
		canceledRunning := false
		// Superseded enabled updates should preempt stale in-flight lookup work.
		if state.runCancel != nil {
			state.runCancel()
			canceledRunning = true
		}
		return true, canceledRunning
	}

	if !shouldRun {
		delete(h.dynamicSyncStates, channel.ChannelID)
		return false, false
	}

	state.running = true
	h.workerWg.Add(1)
	go func() {
		defer h.workerWg.Done()
		h.runDynamicChannelSyncLoop(request)
	}()
	return false, false
}

func (h *AdminHandler) runDynamicChannelSyncLoop(request dynamicChannelSyncRequest) {
	current := request
	for {
		h.runDynamicChannelSyncOnce(current)

		next, ok := h.consumePendingDynamicChannelSync(current.channel.ChannelID)
		if !ok {
			return
		}
		current = next
	}
}

func (h *AdminHandler) runDynamicChannelSyncOnce(request dynamicChannelSyncRequest) {
	if !request.shouldRun {
		return
	}

	channel := request.channel
	sourceIDs := channels.NormalizeSourceIDs(channel.DynamicRule.SourceIDs)
	logger := h.loggerOrDefault()
	startedAt := time.Now()
	timeout := h.dynamicSyncTimeout
	if timeout <= 0 {
		timeout = defaultDynamicSyncTimeout
	}

	logger.Info(
		"admin dynamic channel immediate sync started",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"source_ids", sourceIDs,
		"group", channel.DynamicRule.GroupName,
		"query", channel.DynamicRule.SearchQuery,
		"query_regex", channel.DynamicRule.SearchRegex,
		"timeout", timeout.String(),
		"sync_version", request.version,
	)

	ctx, cancelWorker := h.workerContext()
	ctx, cancelRun := context.WithCancel(ctx)
	h.setDynamicChannelSyncRunCancel(channel.ChannelID, cancelRun)
	defer func() {
		cancelRun()
		cancelWorker()
		h.clearDynamicChannelSyncRunCancel(channel.ChannelID)
	}()

	stale, latestVersion := h.isDynamicChannelSyncRunStale(channel.ChannelID, request.version)
	if stale {
		logger.Info(
			"admin dynamic channel immediate sync skipped stale run",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"source_ids", sourceIDs,
			"group", channel.DynamicRule.GroupName,
			"query", channel.DynamicRule.SearchQuery,
			"matched_items", 0,
			"sync_version", request.version,
			"latest_version", latestVersion,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"stale_stage", "before_lookup",
		)
		return
	}

	ctx, cancelTimeout := context.WithTimeout(ctx, timeout)
	defer cancelTimeout()

	var (
		itemKeys []string
		err      error
	)
	if len(sourceIDs) > 0 {
		sourceScopedCatalog, ok := h.catalog.(sourceScopedCatalogStore)
		if !ok {
			dynamicChannelImmediateSyncFailuresMetric.WithLabelValues("list_active_item_keys", "source_scoped_catalog_unsupported").Inc()
			logger.Warn(
				"admin dynamic channel immediate sync failed",
				"stage", "list_active_item_keys",
				"channel_id", channel.ChannelID,
				"guide_number", channel.GuideNumber,
				"source_ids", sourceIDs,
				"group", channel.DynamicRule.GroupName,
				"query", channel.DynamicRule.SearchQuery,
				"query_regex", channel.DynamicRule.SearchRegex,
				"matched_items", 0,
				"duration_ms", time.Since(startedAt).Milliseconds(),
				"sync_version", request.version,
				"error", "catalog store does not support source-scoped filtering",
			)
			return
		}
		itemKeys, err = sourceScopedCatalog.ListActiveItemKeysByCatalogFilterBySourceIDs(
			ctx,
			sourceIDs,
			channel.DynamicRule.GroupNames,
			channel.DynamicRule.SearchQuery,
			channel.DynamicRule.SearchRegex,
		)
	} else {
		itemKeys, err = h.catalog.ListActiveItemKeysByCatalogFilter(
			ctx,
			channel.DynamicRule.GroupNames,
			channel.DynamicRule.SearchQuery,
			channel.DynamicRule.SearchRegex,
		)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			cancelReason, latestVersion := h.dynamicChannelSyncCancelReason(channel.ChannelID, request.version)
			logger.Info(
				"admin dynamic channel immediate sync canceled",
				"channel_id", channel.ChannelID,
				"guide_number", channel.GuideNumber,
				"source_ids", sourceIDs,
				"group", channel.DynamicRule.GroupName,
				"query", channel.DynamicRule.SearchQuery,
				"query_regex", channel.DynamicRule.SearchRegex,
				"matched_items", 0,
				"duration_ms", time.Since(startedAt).Milliseconds(),
				"sync_version", request.version,
				"latest_version", latestVersion,
				"cancel_reason", cancelReason,
			)
			return
		}
		dynamicChannelImmediateSyncFailuresMetric.WithLabelValues("list_active_item_keys", "catalog_lookup_failed").Inc()
		logger.Warn(
			"admin dynamic channel immediate sync failed",
			"stage", "list_active_item_keys",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"source_ids", sourceIDs,
			"group", channel.DynamicRule.GroupName,
			"query", channel.DynamicRule.SearchQuery,
			"query_regex", channel.DynamicRule.SearchRegex,
			"matched_items", 0,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"sync_version", request.version,
			"error", err,
		)
		return
	}

	stale, latestVersion = h.isDynamicChannelSyncRunStale(channel.ChannelID, request.version)
	if stale {
		logger.Info(
			"admin dynamic channel immediate sync skipped stale run",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"source_ids", sourceIDs,
			"group", channel.DynamicRule.GroupName,
			"query", channel.DynamicRule.SearchQuery,
			"query_regex", channel.DynamicRule.SearchRegex,
			"matched_items", len(itemKeys),
			"sync_version", request.version,
			"latest_version", latestVersion,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"stale_stage", "after_lookup",
		)
		return
	}

	result, err := h.channels.SyncDynamicSources(ctx, channel.ChannelID, itemKeys)
	if err != nil {
		dynamicChannelImmediateSyncFailuresMetric.WithLabelValues("sync_dynamic_sources", "sync_failed").Inc()
		logger.Warn(
			"admin dynamic channel immediate sync failed",
			"stage", "sync_dynamic_sources",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"source_ids", sourceIDs,
			"group", channel.DynamicRule.GroupName,
			"query", channel.DynamicRule.SearchQuery,
			"query_regex", channel.DynamicRule.SearchRegex,
			"matched_items", len(itemKeys),
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"sync_version", request.version,
			"error", err,
		)
		return
	}

	logger.Info(
		"admin dynamic channel immediate sync completed",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"source_ids", sourceIDs,
		"group", channel.DynamicRule.GroupName,
		"query", channel.DynamicRule.SearchQuery,
		"query_regex", channel.DynamicRule.SearchRegex,
		"matched_items", len(itemKeys),
		"added_sources", result.Added,
		"removed_sources", result.Removed,
		"retained_sources", result.Retained,
		"duration_ms", time.Since(startedAt).Milliseconds(),
		"sync_version", request.version,
	)
}

func (h *AdminHandler) consumePendingDynamicChannelSync(channelID int64) (dynamicChannelSyncRequest, bool) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return dynamicChannelSyncRequest{}, false
	}
	state.runCancel = nil

	if state.hasPending {
		next := state.pending
		state.pending = dynamicChannelSyncRequest{}
		state.hasPending = false
		if !next.shouldRun {
			delete(h.dynamicSyncStates, channelID)
			return dynamicChannelSyncRequest{}, false
		}
		return next, true
	}

	delete(h.dynamicSyncStates, channelID)
	return dynamicChannelSyncRequest{}, false
}

func (h *AdminHandler) setDynamicChannelSyncRunCancel(channelID int64, cancel context.CancelFunc) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return
	}
	state.runCancel = cancel
}

func (h *AdminHandler) clearDynamicChannelSyncRunCancel(channelID int64) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return
	}
	state.runCancel = nil
}

func (h *AdminHandler) isDynamicChannelSyncRunStale(channelID int64, version uint64) (bool, uint64) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return true, 0
	}
	return state.latestVersion != version, state.latestVersion
}

func (h *AdminHandler) dynamicChannelSyncCancelReason(channelID int64, version uint64) (string, uint64) {
	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return "state_removed", 0
	}
	if state.latestVersion != version {
		if state.hasPending && !state.pending.shouldRun {
			return "disabled_or_deleted", state.latestVersion
		}
		return "superseded", state.latestVersion
	}
	return "canceled", state.latestVersion
}

func (h *AdminHandler) cancelDynamicChannelSyncForDelete(channelID int64) (bool, bool, bool) {
	if h == nil {
		return false, false, false
	}

	h.dynamicSyncMu.Lock()
	defer h.dynamicSyncMu.Unlock()

	state, ok := h.dynamicSyncStates[channelID]
	if !ok {
		return false, false, false
	}

	state.latestVersion++
	state.pending = dynamicChannelSyncRequest{
		channel:   channels.Channel{ChannelID: channelID},
		shouldRun: false,
		version:   state.latestVersion,
	}
	state.hasPending = true

	canceledRunning := false
	if state.runCancel != nil {
		state.runCancel()
		state.runCancel = nil
		canceledRunning = true
	}

	wasRunning := state.running
	if !wasRunning {
		delete(h.dynamicSyncStates, channelID)
	}

	return true, wasRunning, canceledRunning
}

func (h *AdminHandler) scheduleDynamicBlockSync(r *http.Request, reason string) (bool, bool) {
	if h == nil || r == nil {
		return false, false
	}
	queuedWhileRunning, canceledRunning := h.enqueueDynamicBlockSync()
	h.logAdminMutation(
		r,
		"admin dynamic block sync queued",
		"reason", strings.TrimSpace(reason),
		"queued_while_running", queuedWhileRunning,
		"canceled_running", canceledRunning,
	)
	return queuedWhileRunning, canceledRunning
}

func (h *AdminHandler) enqueueDynamicBlockSync() (bool, bool) {
	h.dynamicBlockSyncMu.Lock()
	defer h.dynamicBlockSyncMu.Unlock()

	select {
	case <-h.closeCh:
		return false, false
	default:
	}

	h.dynamicBlockSyncState.latestVersion++
	currentVersion := h.dynamicBlockSyncState.latestVersion
	if h.dynamicBlockSyncState.running {
		h.dynamicBlockSyncState.hasPending = true
		canceledRunning := false
		if h.dynamicBlockSyncState.runCancel != nil {
			h.dynamicBlockSyncState.runCancel()
			canceledRunning = true
		}
		return true, canceledRunning
	}

	h.dynamicBlockSyncState.running = true
	h.workerWg.Add(1)
	go func() {
		defer h.workerWg.Done()
		h.runDynamicBlockSyncLoop(currentVersion)
	}()
	return false, false
}

func (h *AdminHandler) runDynamicBlockSyncLoop(version uint64) {
	currentVersion := version
	for {
		h.runDynamicBlockSyncOnce(currentVersion)
		nextVersion, ok := h.consumePendingDynamicBlockSync()
		if !ok {
			return
		}
		currentVersion = nextVersion
	}
}

func (h *AdminHandler) runDynamicBlockSyncOnce(version uint64) {
	logger := h.loggerOrDefault()
	timeout := h.dynamicBlockSyncTimeout
	if timeout <= 0 {
		timeout = defaultDynamicSyncTimeout
	}

	startedAt := time.Now()
	logger.Info(
		"admin dynamic block immediate sync started",
		"sync_version", version,
		"timeout", timeout.String(),
	)

	ctx, cancelWorker := h.workerContext()
	ctx, cancelRun := context.WithCancel(ctx)
	h.setDynamicBlockSyncRunCancel(cancelRun)
	defer func() {
		cancelRun()
		cancelWorker()
		h.clearDynamicBlockSyncRunCancel()
	}()

	ctx, cancelTimeout := context.WithTimeout(ctx, timeout)
	defer cancelTimeout()

	result, err := h.channels.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			logger.Info(
				"admin dynamic block immediate sync canceled",
				"sync_version", version,
				"duration_ms", time.Since(startedAt).Milliseconds(),
				"error", err,
			)
			return
		}
		logger.Warn(
			"admin dynamic block immediate sync failed",
			"sync_version", version,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"error", err,
		)
		return
	}

	logger.Info(
		"admin dynamic block immediate sync completed",
		"sync_version", version,
		"duration_ms", time.Since(startedAt).Milliseconds(),
		"queries_processed", result.QueriesProcessed,
		"queries_enabled", result.QueriesEnabled,
		"channels_added", result.ChannelsAdded,
		"channels_updated", result.ChannelsUpdated,
		"channels_retained", result.ChannelsRetained,
		"channels_removed", result.ChannelsRemoved,
		"truncated", result.TruncatedCount,
	)

	changed := result.ChannelsAdded+result.ChannelsUpdated+result.ChannelsRemoved > 0
	if changed {
		h.enqueueDVRLineupReload(
			"dynamic_block_sync",
			"sync_version", version,
			"channels_added", result.ChannelsAdded,
			"channels_updated", result.ChannelsUpdated,
			"channels_removed", result.ChannelsRemoved,
		)
	}
}

func (h *AdminHandler) enqueueDVRLineupReload(reason string, attrs ...any) {
	if h == nil || h.dvr == nil {
		return
	}

	now := time.Now()
	normalizedReason := normalizeDVRLineupReloadReason(reason)

	var (
		prevDueAt          time.Time
		dueAt              time.Time
		coalescedCount     int
		queuedWhileRunning bool
		scheduled          bool
	)

	h.dvrLineupReloadMu.Lock()
	select {
	case <-h.closeCh:
		h.dvrLineupReloadMu.Unlock()
		return
	default:
	}

	h.recordDVRLineupReloadEnqueueLocked(normalizedReason, now)
	state := &h.dvrLineupReloadState
	coalescedCount = state.coalescedCount
	if state.running {
		state.pending = true
		queuedWhileRunning = true
	} else {
		prevDueAt, dueAt = h.rescheduleDVRLineupReloadLocked(now)
		scheduled = true
	}
	h.dvrLineupReloadMu.Unlock()

	logFields := []any{
		"reason", normalizedReason,
		"coalesced_count", coalescedCount,
		"debounce", h.dvrLineupReloadDebounceDuration().String(),
		"max_wait", h.dvrLineupReloadMaxWaitDuration().String(),
		"queued_while_running", queuedWhileRunning,
	}
	if scheduled {
		dueIn := time.Until(dueAt).Milliseconds()
		if dueIn < 0 {
			dueIn = 0
		}
		logFields = append(logFields,
			"due_in_ms", dueIn,
			"due_at", dueAt.UTC().Format(time.RFC3339Nano),
		)
		if !prevDueAt.IsZero() {
			logFields = append(logFields, "previous_due_at", prevDueAt.UTC().Format(time.RFC3339Nano))
		}
	}
	logFields = append(logFields, attrs...)
	h.loggerOrDefault().Info("admin dvr lineup reload queued", logFields...)
}

func (h *AdminHandler) recordDVRLineupReloadEnqueueLocked(reason string, now time.Time) {
	state := &h.dvrLineupReloadState
	if state.firstQueuedAt.IsZero() {
		state.firstQueuedAt = now
	}
	state.lastQueuedAt = now
	state.coalescedCount++
	if state.reasonCounts == nil {
		state.reasonCounts = make(map[string]int)
	}
	state.reasonCounts[reason]++
}

func (h *AdminHandler) rescheduleDVRLineupReloadLocked(now time.Time) (time.Time, time.Time) {
	state := &h.dvrLineupReloadState
	if state.firstQueuedAt.IsZero() {
		state.firstQueuedAt = now
	}

	debounce := h.dvrLineupReloadDebounceDuration()
	maxWait := h.dvrLineupReloadMaxWaitDuration()
	dueAt := now.Add(debounce)
	maxDueAt := state.firstQueuedAt.Add(maxWait)
	if dueAt.After(maxDueAt) {
		dueAt = maxDueAt
	}

	prevDueAt := state.timerDueAt
	if state.timerCancel != nil {
		close(state.timerCancel)
	}
	state.timerSeq++
	seq := state.timerSeq
	state.timerDueAt = dueAt
	cancelCh := make(chan struct{})
	state.timerCancel = cancelCh

	delay := dueAt.Sub(now)
	if delay < 0 {
		delay = 0
	}

	h.workerWg.Add(1)
	go h.waitForDVRLineupReloadTimer(seq, delay, cancelCh)

	return prevDueAt, dueAt
}

func (h *AdminHandler) waitForDVRLineupReloadTimer(seq uint64, delay time.Duration, cancelCh <-chan struct{}) {
	defer h.workerWg.Done()

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		h.fireQueuedDVRLineupReload(seq)
	case <-cancelCh:
	case <-h.closeCh:
	}
}

func (h *AdminHandler) fireQueuedDVRLineupReload(seq uint64) {
	batch, ok := h.beginQueuedDVRLineupReloadRun(seq)
	if !ok {
		return
	}

	h.runDVRLineupReloadOnce(batch)
	h.finalizeDVRLineupReloadRun()
}

func (h *AdminHandler) beginQueuedDVRLineupReloadRun(seq uint64) (dvrLineupReloadBatch, bool) {
	now := time.Now()

	h.dvrLineupReloadMu.Lock()
	defer h.dvrLineupReloadMu.Unlock()

	state := &h.dvrLineupReloadState
	if state.timerSeq != seq || state.timerCancel == nil {
		return dvrLineupReloadBatch{}, false
	}

	state.timerCancel = nil
	state.timerDueAt = time.Time{}
	if state.running || state.firstQueuedAt.IsZero() || state.coalescedCount == 0 {
		return dvrLineupReloadBatch{}, false
	}

	state.running = true
	batch := dvrLineupReloadBatch{
		reasonSummary:  formatDVRLineupReloadReasons(state.reasonCounts),
		coalescedCount: state.coalescedCount,
		firstQueuedAt:  state.firstQueuedAt,
		lastQueuedAt:   state.lastQueuedAt,
	}
	if !batch.firstQueuedAt.IsZero() {
		batch.queuedDuration = now.Sub(batch.firstQueuedAt)
		if batch.queuedDuration < 0 {
			batch.queuedDuration = 0
		}
	}
	if !batch.firstQueuedAt.IsZero() && !batch.lastQueuedAt.IsZero() {
		batch.queueWindowSpan = batch.lastQueuedAt.Sub(batch.firstQueuedAt)
		if batch.queueWindowSpan < 0 {
			batch.queueWindowSpan = 0
		}
	}

	state.firstQueuedAt = time.Time{}
	state.lastQueuedAt = time.Time{}
	state.coalescedCount = 0
	state.reasonCounts = nil

	return batch, true
}

func (h *AdminHandler) runDVRLineupReloadOnce(batch dvrLineupReloadBatch) {
	if h == nil || h.dvr == nil {
		return
	}

	timeout := h.dvrLineupReloadTimeoutDuration()

	logger := h.loggerOrDefault()
	logFields := []any{
		"reason_summary", batch.reasonSummary,
		"coalesced_count", batch.coalescedCount,
		"queued_duration_ms", batch.queuedDuration.Milliseconds(),
		"queue_window_ms", batch.queueWindowSpan.Milliseconds(),
		"timeout", timeout.String(),
	}

	logger.Info("admin dvr lineup reload started", logFields...)

	ctx, cancelWorker := h.workerContext()
	defer cancelWorker()
	ctx, cancelTimeout := context.WithTimeout(ctx, timeout)
	defer cancelTimeout()

	startedAt := time.Now()
	if err := h.dvr.ReloadLineup(ctx); err != nil {
		errorFields := append(append([]any{}, logFields...), "duration_ms", time.Since(startedAt).Milliseconds(), "error", err)
		if errors.Is(err, context.Canceled) {
			logger.Info("admin dvr lineup reload canceled", errorFields...)
			return
		}
		logger.Warn("admin dvr lineup reload failed", errorFields...)
		return
	}

	completedFields := append(append([]any{}, logFields...), "duration_ms", time.Since(startedAt).Milliseconds())
	logger.Info("admin dvr lineup reload completed", completedFields...)
}

func (h *AdminHandler) finalizeDVRLineupReloadRun() {
	if h == nil {
		return
	}

	now := time.Now()
	var (
		scheduled      bool
		coalescedCount int
		prevDueAt      time.Time
		dueAt          time.Time
	)

	h.dvrLineupReloadMu.Lock()
	state := &h.dvrLineupReloadState
	state.running = false

	select {
	case <-h.closeCh:
		state.pending = false
		state.firstQueuedAt = time.Time{}
		state.lastQueuedAt = time.Time{}
		state.coalescedCount = 0
		state.reasonCounts = nil
		h.dvrLineupReloadMu.Unlock()
		return
	default:
	}

	if state.pending && !state.firstQueuedAt.IsZero() {
		state.pending = false
		coalescedCount = state.coalescedCount
		prevDueAt, dueAt = h.rescheduleDVRLineupReloadLocked(now)
		scheduled = true
	} else {
		state.pending = false
	}
	h.dvrLineupReloadMu.Unlock()

	if !scheduled {
		return
	}

	dueIn := time.Until(dueAt).Milliseconds()
	if dueIn < 0 {
		dueIn = 0
	}
	logFields := []any{
		"coalesced_count", coalescedCount,
		"due_in_ms", dueIn,
		"due_at", dueAt.UTC().Format(time.RFC3339Nano),
	}
	if !prevDueAt.IsZero() {
		logFields = append(logFields, "previous_due_at", prevDueAt.UTC().Format(time.RFC3339Nano))
	}
	h.loggerOrDefault().Info("admin dvr lineup reload follow-up queued", logFields...)
}

func (h *AdminHandler) dvrLineupReloadDebounceDuration() time.Duration {
	if h == nil || h.dvrLineupReloadDebounce <= 0 {
		return defaultDVRLineupReloadDebounce
	}
	return h.dvrLineupReloadDebounce
}

func (h *AdminHandler) dvrLineupReloadTimeoutDuration() time.Duration {
	if h == nil || h.dvrLineupReloadTimeout <= 0 {
		return defaultDVRLineupReloadTimeout
	}
	return h.dvrLineupReloadTimeout
}

func (h *AdminHandler) dvrLineupReloadMaxWaitDuration() time.Duration {
	if h == nil || h.dvrLineupReloadMaxWait <= 0 {
		return defaultDVRLineupReloadMaxWait
	}
	return h.dvrLineupReloadMaxWait
}

func normalizeDVRLineupReloadReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "unspecified"
	}
	return reason
}

func formatDVRLineupReloadReasons(reasonCounts map[string]int) string {
	if len(reasonCounts) == 0 {
		return "unspecified(1)"
	}
	reasons := make([]string, 0, len(reasonCounts))
	for reason := range reasonCounts {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)

	parts := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		parts = append(parts, fmt.Sprintf("%s(%d)", reason, reasonCounts[reason]))
	}
	return strings.Join(parts, ",")
}

func (h *AdminHandler) consumePendingDynamicBlockSync() (uint64, bool) {
	h.dynamicBlockSyncMu.Lock()
	defer h.dynamicBlockSyncMu.Unlock()

	h.dynamicBlockSyncState.runCancel = nil
	if h.dynamicBlockSyncState.hasPending {
		h.dynamicBlockSyncState.hasPending = false
		return h.dynamicBlockSyncState.latestVersion, true
	}

	h.dynamicBlockSyncState.running = false
	return 0, false
}

func (h *AdminHandler) setDynamicBlockSyncRunCancel(cancel context.CancelFunc) {
	h.dynamicBlockSyncMu.Lock()
	defer h.dynamicBlockSyncMu.Unlock()
	h.dynamicBlockSyncState.runCancel = cancel
}

func (h *AdminHandler) clearDynamicBlockSyncRunCancel() {
	h.dynamicBlockSyncMu.Lock()
	defer h.dynamicBlockSyncMu.Unlock()
	h.dynamicBlockSyncState.runCancel = nil
}
