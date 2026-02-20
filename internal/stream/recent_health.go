package stream

import (
	"strings"
	"sync"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

const recentSourceHealthMaxPendingEventsPerSource = 8

type recentSourceHealth struct {
	mu              sync.Mutex
	nextEventID     uint64
	pendingBySource map[int64][]recentSourceHealthEvent
}

type recentSourceHealthEvent struct {
	eventID    uint64
	sourceID   int64
	success    bool
	reason     string
	observedAt time.Time
}

func newRecentSourceHealth() *recentSourceHealth {
	return &recentSourceHealth{
		pendingBySource: make(map[int64][]recentSourceHealthEvent),
	}
}

func (h *recentSourceHealth) stageFailure(sourceID int64, reason string, observedAt time.Time) uint64 {
	return h.stageEvent(recentSourceHealthEvent{
		sourceID:   sourceID,
		success:    false,
		reason:     strings.TrimSpace(reason),
		observedAt: observedAt,
	})
}

func (h *recentSourceHealth) stageSuccess(sourceID int64, observedAt time.Time) uint64 {
	return h.stageEvent(recentSourceHealthEvent{
		sourceID:   sourceID,
		success:    true,
		observedAt: observedAt,
	})
}

func (h *recentSourceHealth) stageEvent(event recentSourceHealthEvent) uint64 {
	if h == nil || event.sourceID <= 0 {
		return 0
	}

	event.observedAt = event.observedAt.UTC()
	if event.observedAt.IsZero() {
		event.observedAt = time.Now().UTC()
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.nextEventID++
	if h.nextEventID == 0 {
		h.nextEventID++
	}
	event.eventID = h.nextEventID

	pending := h.pendingBySource[event.sourceID]
	if len(pending) >= recentSourceHealthMaxPendingEventsPerSource {
		pending = append(pending[1:], event)
	} else {
		pending = append(pending, event)
	}
	h.pendingBySource[event.sourceID] = pending
	return event.eventID
}

func (h *recentSourceHealth) markPersisted(sourceID int64, eventID uint64) {
	h.removeEvent(sourceID, eventID)
}

func (h *recentSourceHealth) dropEvent(sourceID int64, eventID uint64) {
	h.removeEvent(sourceID, eventID)
}

func (h *recentSourceHealth) removeEvent(sourceID int64, eventID uint64) {
	if h == nil || sourceID <= 0 || eventID == 0 {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	pending := h.pendingBySource[sourceID]
	if len(pending) == 0 {
		return
	}
	for i := range pending {
		if pending[i].eventID != eventID {
			continue
		}

		updated := append(pending[:i], pending[i+1:]...)
		if len(updated) == 0 {
			delete(h.pendingBySource, sourceID)
		} else {
			h.pendingBySource[sourceID] = updated
		}
		return
	}
}

func (h *recentSourceHealth) apply(sources []channels.Source) []channels.Source {
	return h.applyWithClearAfter(sources, time.Time{}, nil)
}

func (h *recentSourceHealth) applyWithClearAfter(
	sources []channels.Source,
	clearAfter time.Time,
	clearAfterByChannel map[int64]time.Time,
) []channels.Source {
	if h == nil || len(sources) == 0 {
		return sources
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pendingBySource) == 0 {
		return sources
	}

	var updated []channels.Source
	for i := range sources {
		pending := h.pendingBySource[sources[i].SourceID]
		if len(pending) == 0 {
			continue
		}
		sourceClearAfter := clearAfter
		if len(clearAfterByChannel) > 0 && sources[i].ChannelID > 0 {
			if byChannel, ok := clearAfterByChannel[sources[i].ChannelID]; ok && byChannel.After(sourceClearAfter) {
				sourceClearAfter = byChannel
			}
		}
		if !sourceClearAfter.IsZero() {
			filtered := make([]recentSourceHealthEvent, 0, len(pending))
			for _, event := range pending {
				if event.observedAt.After(sourceClearAfter) {
					filtered = append(filtered, event)
				}
			}
			pending = filtered
			if len(pending) == 0 {
				continue
			}
		}

		if updated == nil {
			updated = append([]channels.Source(nil), sources...)
		}
		source := updated[i]
		for _, event := range pending {
			source = applyRecentSourceHealthEvent(source, event)
		}
		updated[i] = source
	}

	if updated == nil {
		return sources
	}
	return updated
}

func (h *recentSourceHealth) clearAll() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pendingBySource = make(map[int64][]recentSourceHealthEvent)
}

func (h *recentSourceHealth) clearBySourceIDs(sourceIDs []int64) {
	if h == nil || len(sourceIDs) == 0 {
		return
	}

	toClear := make(map[int64]struct{}, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		if sourceID <= 0 {
			continue
		}
		toClear[sourceID] = struct{}{}
	}
	if len(toClear) == 0 {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	for sourceID := range toClear {
		delete(h.pendingBySource, sourceID)
	}
}

func applyRecentSourceHealthEvent(
	source channels.Source,
	event recentSourceHealthEvent,
) channels.Source {
	observedUnix := event.observedAt.UTC().Unix()
	if observedUnix <= 0 {
		return source
	}

	if event.success {
		source.SuccessCount++
		if observedUnix > source.LastOKAt {
			source.LastOKAt = observedUnix
		}
		source.FailCount = 0
		source.LastFailAt = 0
		source.LastFailReason = ""
		source.CooldownUntil = 0
		return source
	}

	source.FailCount++
	if observedUnix > source.LastFailAt {
		source.LastFailAt = observedUnix
	}

	reason := strings.TrimSpace(event.reason)
	if reason == "" {
		reason = "startup failed"
	}
	source.LastFailReason = reason

	cooldownUntil := observedUnix + int64(sourceBackoffDurationForRecentHealth(source.FailCount).Seconds())
	if cooldownUntil > source.CooldownUntil {
		source.CooldownUntil = cooldownUntil
	}
	return source
}

func sourceBackoffDurationForRecentHealth(failCount int) time.Duration {
	switch {
	case failCount <= 1:
		return 10 * time.Second
	case failCount == 2:
		return 30 * time.Second
	case failCount == 3:
		return 2 * time.Minute
	case failCount == 4:
		return 10 * time.Minute
	default:
		return 1 * time.Hour
	}
}

func (m *SessionManager) stageRecentSourceFailure(sourceID int64, reason string, observedAt time.Time) uint64 {
	if m == nil || m.recentHealth == nil {
		return 0
	}
	return m.recentHealth.stageFailure(sourceID, reason, observedAt)
}

func (m *SessionManager) stageRecentSourceSuccess(sourceID int64, observedAt time.Time) uint64 {
	if m == nil || m.recentHealth == nil {
		return 0
	}
	return m.recentHealth.stageSuccess(sourceID, observedAt)
}

func (m *SessionManager) markRecentSourceHealthPersisted(sourceID int64, eventID uint64) {
	if m == nil || m.recentHealth == nil {
		return
	}
	m.recentHealth.markPersisted(sourceID, eventID)
}

func (m *SessionManager) dropRecentSourceHealthEvent(sourceID int64, eventID uint64) {
	if m == nil || m.recentHealth == nil {
		return
	}
	m.recentHealth.dropEvent(sourceID, eventID)
}

func (m *SessionManager) applyRecentSourceHealth(sources []channels.Source) []channels.Source {
	if m == nil || m.recentHealth == nil {
		return sources
	}
	clearAfter, clearAfterByChannel := m.sourceHealthClearSnapshot()
	return m.recentHealth.applyWithClearAfter(sources, clearAfter, clearAfterByChannel)
}

func (m *SessionManager) sourceHealthClearSnapshot() (time.Time, map[int64]time.Time) {
	if m == nil {
		return time.Time{}, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	byChannel := make(map[int64]time.Time, len(m.sourceHealthClearAfterByChan))
	for channelID, cutoff := range m.sourceHealthClearAfterByChan {
		if !cutoff.IsZero() {
			byChannel[channelID] = cutoff
		}
	}

	return m.sourceHealthClearAfter, byChannel
}
