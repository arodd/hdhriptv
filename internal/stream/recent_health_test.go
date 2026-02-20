package stream

import (
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

func TestApplyRecentSourceHealthEventSuccessResetsFailureState(t *testing.T) {
	observedAt := time.Unix(1_700_000_600, 0).UTC()
	source := channels.Source{
		SourceID:       42,
		SuccessCount:   3,
		LastOKAt:       time.Unix(1_700_000_100, 0).UTC().Unix(),
		FailCount:      4,
		LastFailAt:     time.Unix(1_700_000_500, 0).UTC().Unix(),
		LastFailReason: "startup timeout",
		CooldownUntil:  observedAt.Add(10 * time.Minute).Unix(),
	}

	updated := applyRecentSourceHealthEvent(source, recentSourceHealthEvent{
		sourceID:   source.SourceID,
		success:    true,
		observedAt: observedAt,
	})

	if got, want := updated.SuccessCount, 4; got != want {
		t.Fatalf("SuccessCount = %d, want %d", got, want)
	}
	if got, want := updated.LastOKAt, observedAt.Unix(); got != want {
		t.Fatalf("LastOKAt = %d, want %d", got, want)
	}
	if got := updated.FailCount; got != 0 {
		t.Fatalf("FailCount = %d, want 0", got)
	}
	if got := updated.LastFailAt; got != 0 {
		t.Fatalf("LastFailAt = %d, want 0", got)
	}
	if got := updated.LastFailReason; got != "" {
		t.Fatalf("LastFailReason = %q, want empty", got)
	}
	if got := updated.CooldownUntil; got != 0 {
		t.Fatalf("CooldownUntil = %d, want 0", got)
	}
}

func TestApplyRecentSourceHealthEventPostRecoveryFailureStartsAtFirstCooldownTier(t *testing.T) {
	successAt := time.Unix(1_700_001_000, 0).UTC()
	failAt := successAt.Add(2 * time.Minute)
	source := channels.Source{
		SourceID:       43,
		SuccessCount:   1,
		FailCount:      3,
		LastFailAt:     successAt.Add(-1 * time.Minute).Unix(),
		LastFailReason: "old failure",
		CooldownUntil:  successAt.Add(5 * time.Minute).Unix(),
	}

	recovered := applyRecentSourceHealthEvent(source, recentSourceHealthEvent{
		sourceID:   source.SourceID,
		success:    true,
		observedAt: successAt,
	})
	postRecoveryFail := applyRecentSourceHealthEvent(recovered, recentSourceHealthEvent{
		sourceID:   source.SourceID,
		success:    false,
		reason:     "transient reset",
		observedAt: failAt,
	})

	if got, want := postRecoveryFail.FailCount, 1; got != want {
		t.Fatalf("FailCount after post-recovery fail = %d, want %d", got, want)
	}
	if got, want := postRecoveryFail.CooldownUntil, failAt.Unix()+10; got != want {
		t.Fatalf("CooldownUntil after post-recovery fail = %d, want %d", got, want)
	}
	if got, want := postRecoveryFail.LastFailAt, failAt.Unix(); got != want {
		t.Fatalf("LastFailAt after post-recovery fail = %d, want %d", got, want)
	}
	if got, want := postRecoveryFail.LastFailReason, "transient reset"; got != want {
		t.Fatalf("LastFailReason after post-recovery fail = %q, want %q", got, want)
	}
}

func TestRecentSourceHealthStagePersistDropConvergence(t *testing.T) {
	h := newRecentSourceHealth()
	sourceID := int64(100)

	// Stage multiple events.
	eid1 := h.stageFailure(sourceID, "fail1", time.Unix(1_700_000_100, 0).UTC())
	eid2 := h.stageSuccess(sourceID, time.Unix(1_700_000_200, 0).UTC())
	eid3 := h.stageFailure(sourceID, "fail3", time.Unix(1_700_000_300, 0).UTC())

	// Verify all three are pending.
	h.mu.Lock()
	count := len(h.pendingBySource[sourceID])
	h.mu.Unlock()
	if count != 3 {
		t.Fatalf("pending after staging = %d, want 3", count)
	}

	// Verify overlay applies all events cumulatively.
	applied := h.apply([]channels.Source{{SourceID: sourceID}})
	if applied[0].FailCount != 1 {
		t.Fatalf("FailCount after apply = %d, want 1 (fail->success->fail)", applied[0].FailCount)
	}

	// Persist first event, drop second, persist third.
	h.markPersisted(sourceID, eid1)
	h.dropEvent(sourceID, eid2)
	h.markPersisted(sourceID, eid3)

	// Verify all pending events are cleared.
	h.mu.Lock()
	remaining := len(h.pendingBySource[sourceID])
	h.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("pending after persist/drop = %d, want 0", remaining)
	}

	// Overlay should be a no-op now.
	clean := h.apply([]channels.Source{{SourceID: sourceID}})
	if clean[0].FailCount != 0 {
		t.Fatalf("FailCount after clearing = %d, want 0 (base value with no overlay)", clean[0].FailCount)
	}

	_ = eid1
	_ = eid2
	_ = eid3
}

func TestRecentSourceHealthMaxPendingEviction(t *testing.T) {
	h := newRecentSourceHealth()
	sourceID := int64(101)

	// Stage maxPending+1 events to trigger eviction.
	var eventIDs []uint64
	for i := 0; i < recentSourceHealthMaxPendingEventsPerSource+1; i++ {
		eid := h.stageFailure(sourceID, "fail", time.Unix(int64(1_700_000_000+i), 0).UTC())
		eventIDs = append(eventIDs, eid)
	}

	// The first event should have been evicted.
	h.mu.Lock()
	pending := h.pendingBySource[sourceID]
	h.mu.Unlock()
	if len(pending) != recentSourceHealthMaxPendingEventsPerSource {
		t.Fatalf("pending = %d, want %d (max cap)", len(pending), recentSourceHealthMaxPendingEventsPerSource)
	}
	// First event should not be in the pending list.
	for _, p := range pending {
		if p.eventID == eventIDs[0] {
			t.Fatal("first event should have been evicted but is still pending")
		}
	}
}

func TestRecentSourceHealthApplyWithClearAfterFiltersOlderEvents(t *testing.T) {
	h := newRecentSourceHealth()
	sourceID := int64(200)

	firstAt := time.Unix(1_700_010_000, 0).UTC()
	secondAt := firstAt.Add(20 * time.Second)
	h.stageFailure(sourceID, "first", firstAt)
	h.stageFailure(sourceID, "second", secondAt)

	sources := []channels.Source{{
		SourceID:  sourceID,
		ChannelID: 1,
	}}
	cutoff := firstAt.Add(10 * time.Second)
	applied := h.applyWithClearAfter(sources, cutoff, nil)
	got := applied[0]

	if got.FailCount != 1 {
		t.Fatalf("FailCount = %d, want 1 after clearAfter filtering", got.FailCount)
	}
	if got.LastFailReason != "second" {
		t.Fatalf("LastFailReason = %q, want %q", got.LastFailReason, "second")
	}
	if got.LastFailAt != secondAt.Unix() {
		t.Fatalf("LastFailAt = %d, want %d", got.LastFailAt, secondAt.Unix())
	}
}

func TestRecentSourceHealthApplyWithClearAfterPerChannelOverrideTakesLaterCutoff(t *testing.T) {
	h := newRecentSourceHealth()
	channelFiltered := int64(11)
	channelPreserved := int64(12)
	filteredSourceID := int64(210)
	preservedSourceID := int64(220)

	observedAt := time.Unix(1_700_020_000, 0).UTC()
	h.stageFailure(filteredSourceID, "filtered", observedAt)
	h.stageFailure(preservedSourceID, "preserved", observedAt)

	sources := []channels.Source{
		{
			SourceID:  filteredSourceID,
			ChannelID: channelFiltered,
		},
		{
			SourceID:  preservedSourceID,
			ChannelID: channelPreserved,
		},
	}

	globalCutoff := observedAt.Add(-1 * time.Second)
	perChannelCutoff := map[int64]time.Time{
		channelFiltered: observedAt.Add(1 * time.Second),
	}
	applied := h.applyWithClearAfter(sources, globalCutoff, perChannelCutoff)

	if got := applied[0].FailCount; got != 0 {
		t.Fatalf("filtered channel FailCount = %d, want 0", got)
	}
	if got := applied[1].FailCount; got != 1 {
		t.Fatalf("preserved channel FailCount = %d, want 1", got)
	}
	if got := applied[1].LastFailReason; got != "preserved" {
		t.Fatalf("preserved channel LastFailReason = %q, want %q", got, "preserved")
	}
}

func TestRecentSourceHealthClearBySourceIDsRemovesOnlySelectedEntries(t *testing.T) {
	h := newRecentSourceHealth()
	clearSourceID := int64(300)
	keepSourceID := int64(301)
	observedAt := time.Unix(1_700_030_000, 0).UTC()

	h.stageFailure(clearSourceID, "clear-me", observedAt)
	h.stageFailure(keepSourceID, "keep-me", observedAt)
	h.clearBySourceIDs([]int64{clearSourceID, -1, 0})

	applied := h.apply([]channels.Source{
		{SourceID: clearSourceID, ChannelID: 1},
		{SourceID: keepSourceID, ChannelID: 1},
	})
	if got := applied[0].FailCount; got != 0 {
		t.Fatalf("cleared source FailCount = %d, want 0", got)
	}
	if got := applied[1].FailCount; got != 1 {
		t.Fatalf("kept source FailCount = %d, want 1", got)
	}
	if got := applied[1].LastFailReason; got != "keep-me" {
		t.Fatalf("kept source LastFailReason = %q, want %q", got, "keep-me")
	}
}

func TestRecentSourceHealthClearAllDropsPendingEntries(t *testing.T) {
	h := newRecentSourceHealth()
	sourceID := int64(400)
	observedAt := time.Unix(1_700_040_000, 0).UTC()

	h.stageFailure(sourceID, "first", observedAt)
	h.stageSuccess(sourceID, observedAt.Add(5*time.Second))
	h.clearAll()

	applied := h.apply([]channels.Source{{SourceID: sourceID, ChannelID: 1}})
	if got := applied[0].FailCount; got != 0 {
		t.Fatalf("FailCount = %d, want 0 after clearAll", got)
	}
	if got := applied[0].SuccessCount; got != 0 {
		t.Fatalf("SuccessCount = %d, want 0 after clearAll", got)
	}
}
