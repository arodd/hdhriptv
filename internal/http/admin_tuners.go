package httpapi

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/stream"
)

type resolveClientHostCacheEntry struct {
	host      string
	expiresAt time.Time
}

type resolveClientHostCacheExpiryHeapItem struct {
	ip        string
	expiresAt time.Time
}

type resolveClientHostCacheExpiryMinHeap []resolveClientHostCacheExpiryHeapItem

func (h resolveClientHostCacheExpiryMinHeap) Len() int {
	return len(h)
}

func (h resolveClientHostCacheExpiryMinHeap) Less(i, j int) bool {
	return h[i].expiresAt.Before(h[j].expiresAt)
}

func (h resolveClientHostCacheExpiryMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *resolveClientHostCacheExpiryMinHeap) Push(x any) {
	*h = append(*h, x.(resolveClientHostCacheExpiryHeapItem))
}

func (h *resolveClientHostCacheExpiryMinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type resolveClientHostResolutionStats struct {
	requestCount       int
	cacheHits          int
	cacheNegativeHits  int
	cacheMisses        int
	memoizedHits       int
	memoizedEmptyHits  int
	lookupCalls        int
	lookupErrors       int
	lookupEmptyResults int
}

func (s *resolveClientHostResolutionStats) add(other resolveClientHostResolutionStats) {
	if s == nil {
		return
	}
	s.requestCount += other.requestCount
	s.cacheHits += other.cacheHits
	s.cacheNegativeHits += other.cacheNegativeHits
	s.cacheMisses += other.cacheMisses
	s.memoizedHits += other.memoizedHits
	s.memoizedEmptyHits += other.memoizedEmptyHits
	s.lookupCalls += other.lookupCalls
	s.lookupErrors += other.lookupErrors
	s.lookupEmptyResults += other.lookupEmptyResults
}

func (s resolveClientHostResolutionStats) cacheHitRate() float64 {
	total := s.cacheHits + s.cacheNegativeHits + s.cacheMisses
	if total == 0 {
		return 0
	}
	return float64(s.cacheHits+s.cacheNegativeHits) / float64(total)
}

func (h *AdminHandler) handleTunerStatus(w http.ResponseWriter, r *http.Request) {
	if h.tunerStatus == nil {
		http.Error(w, "tuner status is not configured", http.StatusServiceUnavailable)
		return
	}

	resolveIP, err := parseOptionalBoolQueryParam(r, "resolve_ip", false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	snapshot := h.tunerStatus.TunerStatusSnapshot()
	if resolveIP {
		resolveCtx, cancel := context.WithTimeout(r.Context(), h.resolveClientHostsTimeoutBudget())
		var resolveStats resolveClientHostResolutionStats
		snapshot, resolveStats = h.resolveClientHostsInSnapshot(resolveCtx, snapshot)
		cancel()
		if h.logger != nil && h.logger.Enabled(r.Context(), slog.LevelDebug) {
			resolveStats.requestCount = 1
			h.logResolveClientHostSummary(resolveStats)
		}
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (h *AdminHandler) handleTriggerTunerRecovery(w http.ResponseWriter, r *http.Request) {
	if h.tunerStatus == nil {
		http.Error(w, "tuner status is not configured", http.StatusServiceUnavailable)
		return
	}
	trigger, ok := h.tunerStatus.(TunerRecoveryTrigger)
	if !ok {
		http.Error(w, "tuner recovery is not supported", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		ChannelID int64  `json:"channel_id"`
		Reason    string `json:"reason"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}
	if req.ChannelID <= 0 {
		http.Error(w, "channel_id must be greater than zero", http.StatusBadRequest)
		return
	}

	recoveryReason := strings.TrimSpace(req.Reason)
	if recoveryReason == "" {
		recoveryReason = "ui_manual_trigger"
	}
	if err := trigger.TriggerSessionRecovery(req.ChannelID, recoveryReason); err != nil {
		switch {
		case errors.Is(err, stream.ErrSessionNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case errors.Is(err, stream.ErrSessionRecoveryAlreadyPending):
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if isInputError(err) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, fmt.Sprintf("trigger tuner recovery: %v", err), http.StatusInternalServerError)
		return
	}

	h.logAdminMutation(
		r,
		"admin tuner recovery triggered",
		"channel_id", req.ChannelID,
		"recovery_reason", recoveryReason,
	)
	writeJSON(w, http.StatusOK, map[string]any{
		"channel_id":      req.ChannelID,
		"recovery_reason": recoveryReason,
		"accepted":        true,
	})
}

func (h *AdminHandler) resolveClientHostsTimeoutBudget() time.Duration {
	if h == nil || h.resolveClientHostsTimeout <= 0 {
		return defaultResolveClientHostsTotalTimeout
	}
	return h.resolveClientHostsTimeout
}

func (h *AdminHandler) resolveClientHostSummaryInterval() time.Duration {
	if h == nil || h.resolveClientHostSummaryLogInterval <= 0 {
		return defaultResolveClientHostSummaryLogInterval
	}
	return h.resolveClientHostSummaryLogInterval
}

func (h *AdminHandler) logResolveClientHostSummary(stats resolveClientHostResolutionStats) {
	if h == nil || h.logger == nil {
		return
	}
	if h.resolveClientHostSummaryTracker == nil {
		return
	}
	nowFn := h.resolveClientHostSummaryNow
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()
	interval := h.resolveClientHostSummaryInterval()
	summarySnapshot, shouldLog := h.resolveClientHostSummaryTracker.Record(now, interval, stats)
	if !shouldLog {
		return
	}
	summary := summarySnapshot.Stats

	h.logger.Debug(
		"admin tuner resolve_ip summary",
		"summary_window", summarySnapshot.WindowDuration,
		"summary_window_start", summarySnapshot.WindowStart.UTC(),
		"summary_window_end", summarySnapshot.WindowEnd.UTC(),
		"resolve_requests", summary.requestCount,
		"cache_hits", summary.cacheHits,
		"cache_negative_hits", summary.cacheNegativeHits,
		"cache_misses", summary.cacheMisses,
		"cache_hit_rate", summary.cacheHitRate(),
		"memoized_hits", summary.memoizedHits,
		"memoized_empty_hits", summary.memoizedEmptyHits,
		"lookup_calls", summary.lookupCalls,
		"lookup_errors", summary.lookupErrors,
		"lookup_empty_results", summary.lookupEmptyResults,
	)
}

func (h *AdminHandler) resolveClientHostsInSnapshot(
	ctx context.Context,
	snapshot stream.TunerStatusSnapshot,
) (stream.TunerStatusSnapshot, resolveClientHostResolutionStats) {
	if h == nil {
		return snapshot, resolveClientHostResolutionStats{}
	}
	return resolveClientHostsInSnapshotWithCache(
		ctx,
		snapshot,
		h.lookupAddr,
		h.loadCachedResolvedClientHost,
		h.storeCachedResolvedClientHost,
	)
}

func (h *AdminHandler) loadCachedResolvedClientHost(ip string) (string, bool) {
	if h == nil || ip == "" || h.resolveClientHostCacheTTL <= 0 {
		return "", false
	}

	now := h.resolveClientHostCacheNow()

	h.resolveClientHostCacheMu.Lock()
	defer h.resolveClientHostCacheMu.Unlock()
	h.sweepExpiredResolvedClientHostEntriesLocked(now, false)

	entry, ok := h.resolveClientHostCache[ip]
	if !ok {
		return "", false
	}
	if !entry.expiresAt.After(now) {
		delete(h.resolveClientHostCache, ip)
		return "", false
	}
	return entry.host, true
}

func (h *AdminHandler) storeCachedResolvedClientHost(ip, host string) {
	if h == nil || ip == "" {
		return
	}

	ttl := h.resolveClientHostCacheTTL
	if host == "" {
		ttl = h.resolveClientHostCacheNegativeTTL
	}
	if ttl <= 0 {
		return
	}
	now := h.resolveClientHostCacheNow()
	expiresAt := now.Add(ttl)

	h.resolveClientHostCacheMu.Lock()
	defer h.resolveClientHostCacheMu.Unlock()
	if h.resolveClientHostCache == nil {
		h.resolveClientHostCache = make(map[string]resolveClientHostCacheEntry)
	}
	h.sweepExpiredResolvedClientHostEntriesLocked(now, false)
	h.evictResolvedClientHostCacheToCapacityLocked(ip, now)
	h.resolveClientHostCache[ip] = resolveClientHostCacheEntry{host: host, expiresAt: expiresAt}
	heap.Push(
		&h.resolveClientHostCacheExpiryHeap,
		resolveClientHostCacheExpiryHeapItem{ip: ip, expiresAt: expiresAt},
	)
	h.compactResolvedClientHostCacheExpiryHeapLocked()
}

func (h *AdminHandler) sweepExpiredResolvedClientHostEntriesLocked(now time.Time, force bool) {
	if h == nil || h.resolveClientHostCache == nil {
		return
	}
	if !force {
		interval := h.resolveClientHostCacheSweepInterval
		if interval > 0 &&
			!h.resolveClientHostCacheLastSweep.IsZero() &&
			now.Sub(h.resolveClientHostCacheLastSweep) < interval {
			return
		}
	}
	for cachedIP, entry := range h.resolveClientHostCache {
		if !entry.expiresAt.After(now) {
			delete(h.resolveClientHostCache, cachedIP)
		}
	}
	h.resolveClientHostCacheLastSweep = now
}

func (h *AdminHandler) evictResolvedClientHostCacheToCapacityLocked(ip string, now time.Time) {
	if h == nil || h.resolveClientHostCache == nil {
		return
	}
	maxEntries := h.resolveClientHostCacheMaxEntries
	if maxEntries <= 0 {
		return
	}
	if _, exists := h.resolveClientHostCache[ip]; exists {
		return
	}
	if len(h.resolveClientHostCache) < maxEntries {
		return
	}
	h.sweepExpiredResolvedClientHostEntriesLocked(now, true)
	if len(h.resolveClientHostCache) < maxEntries {
		return
	}

	for h.resolveClientHostCacheExpiryHeap.Len() > 0 {
		oldest := heap.Pop(&h.resolveClientHostCacheExpiryHeap).(resolveClientHostCacheExpiryHeapItem)
		entry, ok := h.resolveClientHostCache[oldest.ip]
		if !ok || !entry.expiresAt.Equal(oldest.expiresAt) {
			continue
		}
		delete(h.resolveClientHostCache, oldest.ip)
		h.compactResolvedClientHostCacheExpiryHeapLocked()
		return
	}

	// Fallback for empty/stale heap state; this keeps eviction resilient even
	// if the heap was intentionally compacted or reset.
	var oldestIP string
	var oldestExpiry time.Time
	for cachedIP, entry := range h.resolveClientHostCache {
		if oldestIP == "" || entry.expiresAt.Before(oldestExpiry) {
			oldestIP = cachedIP
			oldestExpiry = entry.expiresAt
		}
	}
	if oldestIP != "" {
		delete(h.resolveClientHostCache, oldestIP)
	}
	h.compactResolvedClientHostCacheExpiryHeapLocked()
}

func (h *AdminHandler) compactResolvedClientHostCacheExpiryHeapLocked() {
	if h == nil {
		return
	}
	cacheLen := len(h.resolveClientHostCache)
	if cacheLen == 0 {
		h.resolveClientHostCacheExpiryHeap = nil
		return
	}
	// Avoid unbounded stale-heap growth when hot IPs are refreshed repeatedly.
	const staleHeapMultiplier = 4
	if len(h.resolveClientHostCacheExpiryHeap) <= cacheLen*staleHeapMultiplier {
		return
	}
	rebuilt := make(resolveClientHostCacheExpiryMinHeap, 0, cacheLen)
	for cachedIP, entry := range h.resolveClientHostCache {
		rebuilt = append(rebuilt, resolveClientHostCacheExpiryHeapItem{
			ip:        cachedIP,
			expiresAt: entry.expiresAt,
		})
	}
	heap.Init(&rebuilt)
	h.resolveClientHostCacheExpiryHeap = rebuilt
}

func resolveClientHostsInSnapshotWithCache(
	ctx context.Context,
	snapshot stream.TunerStatusSnapshot,
	lookupAddr func(ctx context.Context, addr string) ([]string, error),
	loadCachedHost func(ip string) (string, bool),
	storeCachedHost func(ip, host string),
) (stream.TunerStatusSnapshot, resolveClientHostResolutionStats) {
	stats := resolveClientHostResolutionStats{}
	if lookupAddr == nil {
		return snapshot, stats
	}

	if len(snapshot.ClientStreams) == 0 && len(snapshot.SessionHistory) == 0 {
		return snapshot, stats
	}

	// Clone slices so we do not mutate shared backing arrays from providers.
	if len(snapshot.ClientStreams) > 0 {
		snapshot.ClientStreams = append([]stream.ClientStreamStatus(nil), snapshot.ClientStreams...)
	}
	if len(snapshot.SessionHistory) > 0 {
		snapshot.SessionHistory = append([]stream.SharedSessionHistory(nil), snapshot.SessionHistory...)
		for i := range snapshot.SessionHistory {
			if len(snapshot.SessionHistory[i].Subscribers) == 0 {
				continue
			}
			snapshot.SessionHistory[i].Subscribers = append(
				[]stream.SharedSessionSubscriberHistory(nil),
				snapshot.SessionHistory[i].Subscribers...,
			)
		}
	}

	resolvedByIP := make(map[string]string, len(snapshot.ClientStreams))
	for i := range snapshot.ClientStreams {
		if ctx != nil && ctx.Err() != nil {
			return snapshot, stats
		}
		if host := resolveClientHost(
			ctx,
			snapshot.ClientStreams[i].ClientAddr,
			resolvedByIP,
			lookupAddr,
			loadCachedHost,
			storeCachedHost,
			&stats,
		); host != "" {
			snapshot.ClientStreams[i].ClientHost = host
		}
	}
	for i := range snapshot.SessionHistory {
		if ctx != nil && ctx.Err() != nil {
			return snapshot, stats
		}
		for j := range snapshot.SessionHistory[i].Subscribers {
			if ctx != nil && ctx.Err() != nil {
				return snapshot, stats
			}
			if host := resolveClientHost(
				ctx,
				snapshot.SessionHistory[i].Subscribers[j].ClientAddr,
				resolvedByIP,
				lookupAddr,
				loadCachedHost,
				storeCachedHost,
				&stats,
			); host != "" {
				snapshot.SessionHistory[i].Subscribers[j].ClientHost = host
			}
		}
	}

	return snapshot, stats
}

func resolveClientHost(
	ctx context.Context,
	clientAddr string,
	resolvedByIP map[string]string,
	lookupAddr func(ctx context.Context, addr string) ([]string, error),
	loadCachedHost func(ip string) (string, bool),
	storeCachedHost func(ip, host string),
	stats *resolveClientHostResolutionStats,
) string {
	if ctx == nil {
		ctx = context.Background()
	}

	ip, ok := parseClientAddressIP(clientAddr)
	if !ok {
		return ""
	}
	if resolved, seen := resolvedByIP[ip]; seen {
		if stats != nil {
			stats.memoizedHits++
			if resolved == "" {
				stats.memoizedEmptyHits++
			}
		}
		return resolved
	}
	if loadCachedHost != nil {
		if cachedHost, ok := loadCachedHost(ip); ok {
			resolvedByIP[ip] = cachedHost
			if stats != nil {
				if cachedHost == "" {
					stats.cacheNegativeHits++
				} else {
					stats.cacheHits++
				}
			}
			return cachedHost
		}
		if stats != nil {
			stats.cacheMisses++
		}
	}
	if stats != nil {
		stats.lookupCalls++
	}

	lookupCtx, cancel := context.WithTimeout(ctx, defaultResolveClientHostLookupTimeout)
	defer cancel()

	names, err := lookupAddr(lookupCtx, ip)
	if err != nil {
		if stats != nil {
			stats.lookupErrors++
		}
		resolvedByIP[ip] = ""
		if storeCachedHost != nil {
			storeCachedHost(ip, "")
		}
		return ""
	}
	host := firstResolvedHostname(names)
	if stats != nil && host == "" {
		stats.lookupEmptyResults++
	}
	resolvedByIP[ip] = host
	if storeCachedHost != nil {
		storeCachedHost(ip, host)
	}
	return host
}

func parseClientAddressIP(clientAddr string) (string, bool) {
	addr := strings.TrimSpace(clientAddr)
	if addr == "" {
		return "", false
	}

	host := addr
	if splitHost, _, err := net.SplitHostPort(addr); err == nil {
		host = splitHost
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if zoneSep := strings.Index(host, "%"); zoneSep >= 0 {
		host = host[:zoneSep]
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return "", false
	}
	return ip.String(), true
}

func firstResolvedHostname(names []string) string {
	for _, name := range names {
		trimmed := strings.TrimSuffix(strings.TrimSpace(name), ".")
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
