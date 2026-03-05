package stream

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	defaultPlaylistSourceID   int64 = 1
	defaultPlaylistSourceName       = "Primary"
)

var (
	virtualTunerUtilizationMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stream_virtual_tuner_utilization_ratio",
		Help: "Current in-use virtual tuner ratio per playlist source pool.",
	}, []string{"playlist_source"})
)

// VirtualTunerSource describes one configured source-level tuner pool.
type VirtualTunerSource struct {
	SourceID   int64
	Name       string
	TunerCount int
	Enabled    bool
	OrderIndex int
}

type virtualPoolEntry struct {
	sourceID   int64
	sourceName string
	orderIndex int
	baseID     int
	pool       *Pool
}

// VirtualTunerManager routes tuner leases to per-source pools while
// presenting a single aggregate tuner surface to existing callers.
type VirtualTunerManager struct {
	mu              sync.RWMutex
	entries         []*virtualPoolEntry
	bySourceID      map[int64]*virtualPoolEntry
	defaultSourceID int64
	nextBaseID      int
	preemptSettle   time.Duration
	closed          atomic.Bool
}

func NewVirtualTunerManager(sources []VirtualTunerSource) *VirtualTunerManager {
	manager := &VirtualTunerManager{
		entries:    make([]*virtualPoolEntry, 0),
		bySourceID: make(map[int64]*virtualPoolEntry),
	}
	manager.Reconfigure(sources)
	manager.updateVirtualTunerUtilizationMetrics()

	return manager
}

// Reconfigure updates source-level virtual pools at runtime so subsequent tuner
// acquisitions reflect the latest playlist source configuration.
func (m *VirtualTunerManager) Reconfigure(sources []VirtualTunerSource) {
	if m == nil || m.closed.Load() {
		return
	}

	normalized := normalizeVirtualTunerSources(sources)

	m.mu.Lock()
	existingBySourceID := m.bySourceID
	if existingBySourceID == nil {
		existingBySourceID = make(map[int64]*virtualPoolEntry)
	}
	activeBySourceID := make(map[int64]*virtualPoolEntry, len(normalized))
	nextEntries := append(make([]*virtualPoolEntry, 0, len(m.entries)+len(normalized)), m.entries...)

	for _, source := range normalized {
		if existing, ok := existingBySourceID[source.SourceID]; ok &&
			existing != nil &&
			existing.pool != nil &&
			existing.pool.Capacity() == source.TunerCount {
			existing.sourceName = source.Name
			existing.orderIndex = source.OrderIndex
			activeBySourceID[source.SourceID] = existing
			continue
		}

		entry := &virtualPoolEntry{
			sourceID:   source.SourceID,
			sourceName: source.Name,
			orderIndex: source.OrderIndex,
			baseID:     m.nextBaseID,
			pool:       NewPool(source.TunerCount),
		}
		entry.pool.SetPreemptSettleDelay(m.preemptSettle)
		m.nextBaseID += entry.pool.Capacity()
		nextEntries = append(nextEntries, entry)
		activeBySourceID[source.SourceID] = entry
	}

	kept := nextEntries[:0]
	for _, entry := range nextEntries {
		if entry == nil || entry.pool == nil {
			continue
		}
		if activeEntry, ok := activeBySourceID[entry.sourceID]; ok && activeEntry == entry {
			kept = append(kept, entry)
			continue
		}
		if entry.pool.InUseCount() > 0 {
			kept = append(kept, entry)
			continue
		}
		virtualTunerUtilizationMetric.WithLabelValues(
			playlistSourceMetricLabel(entry.sourceID, entry.sourceName),
		).Set(0)
	}

	m.entries = kept
	m.bySourceID = activeBySourceID
	m.defaultSourceID = 0
	if _, ok := activeBySourceID[defaultPlaylistSourceID]; ok {
		m.defaultSourceID = defaultPlaylistSourceID
	} else if len(normalized) > 0 {
		m.defaultSourceID = normalized[0].SourceID
	}
	m.mu.Unlock()
}

func normalizeVirtualTunerSources(sources []VirtualTunerSource) []VirtualTunerSource {
	normalized := make([]VirtualTunerSource, 0, len(sources))
	for _, source := range sources {
		if source.SourceID <= 0 {
			continue
		}
		if !source.Enabled || source.TunerCount <= 0 {
			continue
		}
		name := strings.TrimSpace(source.Name)
		if name == "" {
			if source.SourceID == defaultPlaylistSourceID {
				name = defaultPlaylistSourceName
			} else {
				name = "Source " + sourceIDString(source.SourceID)
			}
		}
		normalized = append(normalized, VirtualTunerSource{
			SourceID:   source.SourceID,
			Name:       name,
			TunerCount: source.TunerCount,
			Enabled:    true,
			OrderIndex: source.OrderIndex,
		})
	}
	sort.SliceStable(normalized, func(i, j int) bool {
		if normalized[i].OrderIndex == normalized[j].OrderIndex {
			return normalized[i].SourceID < normalized[j].SourceID
		}
		return normalized[i].OrderIndex < normalized[j].OrderIndex
	})

	seen := make(map[int64]struct{}, len(normalized))
	deduped := make([]VirtualTunerSource, 0, len(normalized))
	for _, source := range normalized {
		if _, ok := seen[source.SourceID]; ok {
			continue
		}
		seen[source.SourceID] = struct{}{}
		deduped = append(deduped, source)
	}
	return deduped
}

func (m *VirtualTunerManager) SetPreemptSettleDelay(delay time.Duration) {
	if m == nil {
		return
	}
	if delay < 0 {
		delay = 0
	}
	m.mu.Lock()
	m.preemptSettle = delay
	for _, entry := range m.entries {
		if entry == nil || entry.pool == nil {
			continue
		}
		entry.pool.SetPreemptSettleDelay(delay)
	}
	m.mu.Unlock()
}

func (m *VirtualTunerManager) Close() {
	if m == nil {
		return
	}
	m.closed.Store(true)
}

func (m *VirtualTunerManager) Acquire(ctx context.Context, guideNumber, clientAddr string) (*Lease, error) {
	return m.AcquireClient(ctx, guideNumber, clientAddr)
}

func (m *VirtualTunerManager) AcquireClient(ctx context.Context, guideNumber, clientAddr string) (*Lease, error) {
	return m.AcquireClientForSource(ctx, 0, guideNumber, clientAddr)
}

func (m *VirtualTunerManager) AcquireClientForSource(
	ctx context.Context,
	sourceID int64,
	guideNumber,
	clientAddr string,
) (*Lease, error) {
	if m == nil || m.closed.Load() {
		return nil, ErrNoTunersAvailable
	}

	entry, totalInUseForSource, resolvedSourceID := m.activeEntryForAcquire(sourceID)
	if entry == nil || entry.pool == nil {
		return nil, ErrNoTunersAvailable
	}
	sourceCapacity := entry.pool.Capacity()
	if totalInUseForSource >= sourceCapacity {
		// When the source is exactly at capacity, still allow client acquire
		// attempts if the active pool has preemptible leases. The underlying
		// pool acquire path will reclaim probe/idle capacity and preserve the
		// configured cap via replacement, not growth.
		if totalInUseForSource > sourceCapacity || !entry.pool.hasPreemptibleLeaseForSource(entry.sourceID) {
			return nil, ErrNoTunersAvailable
		}
	}

	lease, err := entry.pool.AcquireClient(ctx, guideNumber, clientAddr)
	if err != nil {
		return nil, err
	}
	return m.finalizeAcquiredLease(resolvedSourceID, entry, lease)
}

func (m *VirtualTunerManager) AcquireProbe(
	ctx context.Context,
	label string,
	cancel context.CancelCauseFunc,
) (*Lease, error) {
	return m.AcquireProbeForSource(ctx, 0, label, cancel)
}

func (m *VirtualTunerManager) AcquireProbeForSource(
	ctx context.Context,
	sourceID int64,
	label string,
	cancel context.CancelCauseFunc,
) (*Lease, error) {
	if m == nil || m.closed.Load() {
		return nil, ErrNoTunersAvailable
	}

	entry, totalInUseForSource, resolvedSourceID := m.activeEntryForAcquire(sourceID)
	if entry == nil || entry.pool == nil {
		return nil, ErrNoTunersAvailable
	}
	if totalInUseForSource >= entry.pool.Capacity() {
		return nil, ErrNoTunersAvailable
	}

	lease, err := entry.pool.AcquireProbe(ctx, label, cancel)
	if err != nil {
		return nil, err
	}
	return m.finalizeAcquiredLease(resolvedSourceID, entry, lease)
}

func (m *VirtualTunerManager) Snapshot() []Session {
	if m == nil {
		return nil
	}

	m.mu.RLock()
	entries := append([]*virtualPoolEntry(nil), m.entries...)
	m.mu.RUnlock()

	totalInUse := 0
	for _, entry := range entries {
		if entry == nil || entry.pool == nil {
			continue
		}
		totalInUse += entry.pool.InUseCount()
	}
	if totalInUse <= 0 {
		return nil
	}

	out := make([]Session, 0, totalInUse)
	for _, entry := range entries {
		if entry == nil || entry.pool == nil {
			continue
		}
		inner := entry.pool.Snapshot()
		for i := range inner {
			inner[i].VirtualTunerSlot = inner[i].ID
			inner[i].ID = entry.baseID + inner[i].ID
			inner[i].PlaylistSourceID = entry.sourceID
			inner[i].PlaylistSourceName = entry.sourceName
			out = append(out, inner[i])
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

func (m *VirtualTunerManager) InUseCount() int {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	entries := append([]*virtualPoolEntry(nil), m.entries...)
	m.mu.RUnlock()

	total := 0
	for _, entry := range entries {
		if entry == nil || entry.pool == nil {
			continue
		}
		total += entry.pool.InUseCount()
	}
	return total
}

func (m *VirtualTunerManager) InUseCountForSource(sourceID int64) int {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	if sourceID <= 0 {
		sourceID = m.defaultSourceID
	}
	entries := append([]*virtualPoolEntry(nil), m.entries...)
	m.mu.RUnlock()
	if sourceID <= 0 {
		return 0
	}

	total := 0
	for _, entry := range entries {
		if entry == nil || entry.pool == nil || entry.sourceID != sourceID {
			continue
		}
		total += entry.pool.InUseCount()
	}
	return total
}

func (m *VirtualTunerManager) Capacity() int {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	entries := make([]*virtualPoolEntry, 0, len(m.bySourceID))
	for _, entry := range m.bySourceID {
		entries = append(entries, entry)
	}
	m.mu.RUnlock()

	total := 0
	for _, entry := range entries {
		if entry == nil || entry.pool == nil {
			continue
		}
		total += entry.pool.Capacity()
	}
	return total
}

func (m *VirtualTunerManager) CapacityForSource(sourceID int64) int {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	if sourceID <= 0 {
		sourceID = m.defaultSourceID
	}
	entry := m.bySourceID[sourceID]
	m.mu.RUnlock()
	if entry == nil || entry.pool == nil {
		return 0
	}
	return entry.pool.Capacity()
}

func (m *VirtualTunerManager) hasPreemptibleLeaseForSource(sourceID int64) bool {
	if m == nil {
		return false
	}

	m.mu.RLock()
	if sourceID <= 0 {
		sourceID = m.defaultSourceID
	}
	entries := append([]*virtualPoolEntry(nil), m.entries...)
	m.mu.RUnlock()
	if sourceID <= 0 {
		return false
	}
	for _, entry := range entries {
		if entry == nil || entry.pool == nil || entry.sourceID != sourceID {
			continue
		}
		if entry.pool.hasPreemptibleLeaseForSource(entry.sourceID) {
			return true
		}
	}
	return false
}

func (m *VirtualTunerManager) VirtualTunerSnapshot() []VirtualTunerPoolSnapshot {
	if m == nil {
		return nil
	}

	m.mu.RLock()
	entries := append([]*virtualPoolEntry(nil), m.entries...)
	activeEntries := make([]*virtualPoolEntry, 0, len(m.bySourceID))
	for _, entry := range m.bySourceID {
		activeEntries = append(activeEntries, entry)
	}
	m.mu.RUnlock()

	sort.Slice(activeEntries, func(i, j int) bool {
		if activeEntries[i].orderIndex == activeEntries[j].orderIndex {
			return activeEntries[i].sourceID < activeEntries[j].sourceID
		}
		return activeEntries[i].orderIndex < activeEntries[j].orderIndex
	})

	type retainedSourceMeta struct {
		name       string
		orderIndex int
	}

	sourceInUse := make(map[int64]int, len(entries))
	retainedMeta := make(map[int64]retainedSourceMeta, len(entries))
	for _, entry := range entries {
		if entry == nil || entry.pool == nil {
			continue
		}
		sourceID := entry.sourceID
		if sourceID <= 0 {
			sourceID = defaultPlaylistSourceID
		}
		sourceInUse[sourceID] += entry.pool.InUseCount()
		meta, ok := retainedMeta[sourceID]
		if !ok {
			retainedMeta[sourceID] = retainedSourceMeta{
				name:       entry.sourceName,
				orderIndex: entry.orderIndex,
			}
			continue
		}
		if entry.orderIndex < meta.orderIndex {
			meta.orderIndex = entry.orderIndex
		}
		if strings.TrimSpace(meta.name) == "" && strings.TrimSpace(entry.sourceName) != "" {
			meta.name = entry.sourceName
		}
		retainedMeta[sourceID] = meta
	}

	summary := make([]VirtualTunerPoolSnapshot, 0, len(activeEntries)+len(retainedMeta))
	seenSources := make(map[int64]struct{}, len(activeEntries))
	for _, entry := range activeEntries {
		if entry == nil || entry.pool == nil {
			continue
		}
		sourceID := entry.sourceID
		if sourceID <= 0 {
			sourceID = defaultPlaylistSourceID
		}
		sourceName := strings.TrimSpace(entry.sourceName)
		if sourceName == "" {
			if sourceID == defaultPlaylistSourceID {
				sourceName = defaultPlaylistSourceName
			} else {
				sourceName = "Source " + sourceIDString(sourceID)
			}
		}
		tunerCount := entry.pool.Capacity()
		inUseCount := sourceInUse[sourceID]
		idleCount := tunerCount - inUseCount
		if idleCount < 0 {
			idleCount = 0
		}
		summary = append(summary, VirtualTunerPoolSnapshot{
			PlaylistSourceID:    sourceID,
			PlaylistSourceName:  sourceName,
			PlaylistSourceOrder: entry.orderIndex,
			TunerCount:          tunerCount,
			InUseCount:          inUseCount,
			IdleCount:           idleCount,
		})
		seenSources[sourceID] = struct{}{}
	}

	nextOrder := len(summary)
	for sourceID, inUseCount := range sourceInUse {
		if _, seen := seenSources[sourceID]; seen {
			continue
		}
		if inUseCount <= 0 {
			continue
		}
		meta := retainedMeta[sourceID]
		sourceName := strings.TrimSpace(meta.name)
		if sourceName == "" {
			if sourceID == defaultPlaylistSourceID {
				sourceName = defaultPlaylistSourceName
			} else {
				sourceName = "Source " + sourceIDString(sourceID)
			}
		}
		orderIndex := meta.orderIndex
		if orderIndex < 0 {
			orderIndex = nextOrder
		}
		summary = append(summary, VirtualTunerPoolSnapshot{
			PlaylistSourceID:    sourceID,
			PlaylistSourceName:  sourceName,
			PlaylistSourceOrder: orderIndex,
			TunerCount:          inUseCount,
			InUseCount:          inUseCount,
			IdleCount:           0,
		})
		nextOrder++
	}

	sort.Slice(summary, func(i, j int) bool {
		if summary[i].PlaylistSourceOrder == summary[j].PlaylistSourceOrder {
			return summary[i].PlaylistSourceID < summary[j].PlaylistSourceID
		}
		return summary[i].PlaylistSourceOrder < summary[j].PlaylistSourceOrder
	})
	return summary
}

func (m *VirtualTunerManager) clearClientPreemptible(id int, leaseToken uint64) {
	entry, localID, ok := m.entryForGlobalTunerID(id)
	if !ok || entry.pool == nil {
		return
	}
	entry.pool.clearClientPreemptible(localID, leaseToken)
}

func (m *VirtualTunerManager) markClientPreemptible(id int, leaseToken uint64, preemptFn func() bool) {
	entry, localID, ok := m.entryForGlobalTunerID(id)
	if !ok || entry.pool == nil {
		return
	}
	entry.pool.markClientPreemptible(localID, leaseToken, preemptFn)
}

func (m *VirtualTunerManager) failoverSettleDelayWhenFull() time.Duration {
	if m == nil {
		return 0
	}

	m.mu.RLock()
	entries := make([]*virtualPoolEntry, 0, len(m.bySourceID))
	for _, entry := range m.bySourceID {
		entries = append(entries, entry)
	}
	m.mu.RUnlock()

	wait := time.Duration(0)
	for _, entry := range entries {
		if entry == nil || entry.pool == nil {
			continue
		}
		if poolWait := entry.pool.failoverSettleDelayWhenFull(); poolWait > wait {
			wait = poolWait
		}
	}
	return wait
}

func (m *VirtualTunerManager) failoverSettleDelayWhenFullForSource(sourceID int64) time.Duration {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	if sourceID <= 0 {
		sourceID = m.defaultSourceID
	}
	entries := append([]*virtualPoolEntry(nil), m.entries...)
	m.mu.RUnlock()
	if sourceID <= 0 {
		return 0
	}
	wait := time.Duration(0)
	for _, entry := range entries {
		if entry == nil || entry.pool == nil || entry.sourceID != sourceID {
			continue
		}
		if poolWait := entry.pool.failoverSettleDelayWhenFull(); poolWait > wait {
			wait = poolWait
		}
	}
	return wait
}

func (m *VirtualTunerManager) activeEntryForAcquire(sourceID int64) (*virtualPoolEntry, int, int64) {
	if m == nil {
		return nil, 0, 0
	}

	m.mu.RLock()
	if sourceID <= 0 {
		sourceID = m.defaultSourceID
	}
	entry := m.bySourceID[sourceID]
	entries := append([]*virtualPoolEntry(nil), m.entries...)
	m.mu.RUnlock()
	if entry == nil || sourceID <= 0 {
		return nil, 0, 0
	}

	totalInUseForSource := 0
	for _, candidate := range entries {
		if candidate == nil || candidate.pool == nil || candidate.sourceID != sourceID {
			continue
		}
		totalInUseForSource += candidate.pool.InUseCount()
	}
	return entry, totalInUseForSource, sourceID
}

func (m *VirtualTunerManager) hasAcquirableClientSlotForSource(sourceID int64) bool {
	if m == nil {
		return false
	}
	entry, totalInUseForSource, _ := m.activeEntryForAcquire(sourceID)
	if entry == nil || entry.pool == nil {
		return false
	}
	sourceCapacity := entry.pool.Capacity()
	if totalInUseForSource < sourceCapacity {
		return true
	}
	if totalInUseForSource > sourceCapacity {
		return false
	}
	return entry.pool.hasPreemptibleLeaseForSource(entry.sourceID)
}

func (m *VirtualTunerManager) finalizeAcquiredLease(
	sourceID int64,
	entry *virtualPoolEntry,
	lease *Lease,
) (*Lease, error) {
	if entry == nil || entry.pool == nil || lease == nil {
		if lease != nil {
			lease.Release()
		}
		return nil, ErrNoTunersAvailable
	}

	if !m.isActiveEntryForSource(sourceID, entry) {
		lease.Release()
		m.updateVirtualTunerUtilizationForEntry(entry)
		m.updateVirtualTunerUtilizationBySource(sourceID, entry.sourceName)
		return nil, ErrNoTunersAvailable
	}

	wrapped := m.wrapLease(entry, lease)
	m.updateVirtualTunerUtilizationForEntry(entry)
	return wrapped, nil
}

func (m *VirtualTunerManager) isActiveEntryForSource(sourceID int64, entry *virtualPoolEntry) bool {
	if m == nil || sourceID <= 0 || entry == nil {
		return false
	}
	m.mu.RLock()
	active := m.bySourceID[sourceID]
	m.mu.RUnlock()
	return active == entry
}

func (m *VirtualTunerManager) entryForSourceID(sourceID int64) *virtualPoolEntry {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.bySourceID) == 0 {
		return nil
	}
	if sourceID <= 0 {
		sourceID = m.defaultSourceID
	}
	if sourceID > 0 {
		if entry, ok := m.bySourceID[sourceID]; ok {
			return entry
		}
	}
	return nil
}

func (m *VirtualTunerManager) entryForGlobalTunerID(id int) (*virtualPoolEntry, int, bool) {
	if m == nil || id < 0 {
		return nil, 0, false
	}
	m.mu.RLock()
	entries := append([]*virtualPoolEntry(nil), m.entries...)
	m.mu.RUnlock()
	for _, entry := range entries {
		if entry == nil || entry.pool == nil {
			continue
		}
		base := entry.baseID
		limit := base + entry.pool.Capacity()
		if id < base || id >= limit {
			continue
		}
		return entry, id - base, true
	}
	return nil, 0, false
}

func (m *VirtualTunerManager) wrapLease(entry *virtualPoolEntry, lease *Lease) *Lease {
	if entry == nil || lease == nil {
		return lease
	}
	release := lease.Release
	playlistSourceID := entry.sourceID
	playlistSourceName := entry.sourceName
	return &Lease{
		ID:                 entry.baseID + lease.ID,
		PlaylistSourceID:   playlistSourceID,
		PlaylistSourceName: playlistSourceName,
		VirtualTunerSlot:   lease.ID,
		token:              lease.token,
		releaseFn: func() {
			release()
			m.updateVirtualTunerUtilizationForEntry(entry)
			m.updateVirtualTunerUtilizationBySource(playlistSourceID, playlistSourceName)
		},
	}
}

func (m *VirtualTunerManager) updateVirtualTunerUtilizationMetrics() {
	if m == nil {
		return
	}

	m.mu.RLock()
	entries := append([]*virtualPoolEntry(nil), m.entries...)
	m.mu.RUnlock()

	for _, entry := range entries {
		m.updateVirtualTunerUtilizationForEntry(entry)
	}
}

func (m *VirtualTunerManager) updateVirtualTunerUtilizationBySource(sourceID int64, sourceName string) {
	if m == nil {
		return
	}
	entry := m.entryForSourceID(sourceID)
	if entry == nil || entry.pool == nil {
		// Keep the label present and reset when the pool is unavailable.
		virtualTunerUtilizationMetric.WithLabelValues(playlistSourceMetricLabel(sourceID, sourceName)).Set(0)
		return
	}
	m.updateVirtualTunerUtilizationForEntry(entry)
}

func (m *VirtualTunerManager) updateVirtualTunerUtilizationForEntry(entry *virtualPoolEntry) {
	if m == nil || entry == nil || entry.pool == nil {
		return
	}
	capacity := entry.pool.Capacity()
	if capacity <= 0 {
		virtualTunerUtilizationMetric.WithLabelValues(
			playlistSourceMetricLabel(entry.sourceID, entry.sourceName),
		).Set(0)
		return
	}
	inUse := entry.pool.InUseCount()
	utilization := float64(inUse) / float64(capacity)
	if utilization < 0 {
		utilization = 0
	}
	if utilization > 1 {
		utilization = 1
	}
	virtualTunerUtilizationMetric.WithLabelValues(
		playlistSourceMetricLabel(entry.sourceID, entry.sourceName),
	).Set(utilization)
}

func sourceIDString(sourceID int64) string {
	if sourceID < 0 {
		sourceID = -sourceID
	}
	if sourceID == 0 {
		return "0"
	}
	buf := make([]byte, 0, 20)
	for sourceID > 0 {
		buf = append([]byte{byte('0' + sourceID%10)}, buf...)
		sourceID /= 10
	}
	return string(buf)
}
