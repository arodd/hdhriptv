package stream

import (
	"context"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

func TestEnsureLeaseForSourceReusesLeaseWhenSelectionUsesSharedPool(t *testing.T) {
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), &fakeChannelsProvider{})

	channel := channels.Channel{
		ChannelID:   1,
		GuideNumber: "101",
		GuideName:   "News",
		Enabled:     true,
	}
	initialLease, err := manager.tuners.AcquireClientForSource(
		context.Background(),
		1,
		channel.GuideNumber,
		"shared:"+channel.GuideNumber,
	)
	if err != nil {
		t.Fatalf("AcquireClientForSource(initial lease) error = %v", err)
	}

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channel,
		lease:   initialLease,
		ctx:     context.Background(),
		cancel:  func() {},
		readyCh: make(chan struct{}),
	}
	t.Cleanup(func() {
		if session.lease != nil {
			session.lease.Release()
		}
	})

	originalLease := session.lease
	originalToken := session.lease.token
	originalTunerID := session.lease.ID

	err = session.ensureLeaseForSource(context.Background(), channels.Source{
		SourceID:           11,
		ChannelID:          channel.ChannelID,
		ItemKey:            "src:news:backup",
		StreamURL:          "http://backup.example/stream.ts",
		PlaylistSourceID:   2,
		PlaylistSourceName: "Backup",
		PriorityIndex:      1,
		Enabled:            true,
	})
	if err != nil {
		t.Fatalf("ensureLeaseForSource(shared pool) error = %v", err)
	}
	if session.lease != originalLease {
		t.Fatal("ensureLeaseForSource reused source pool should preserve existing lease pointer")
	}
	if got, want := session.lease.token, originalToken; got != want {
		t.Fatalf("session lease token = %d, want %d (no release/reacquire churn)", got, want)
	}
	if got, want := session.lease.ID, originalTunerID; got != want {
		t.Fatalf("session lease tuner_id = %d, want %d", got, want)
	}
	if got, want := session.lease.PlaylistSourceID, int64(2); got != want {
		t.Fatalf("session lease playlist_source_id = %d, want %d", got, want)
	}
	if got, want := session.lease.PlaylistSourceName, "Backup"; got != want {
		t.Fatalf("session lease playlist_source_name = %q, want %q", got, want)
	}
	if got, want := manager.tuners.InUseCount(), 1; got != want {
		t.Fatalf("manager.tuners.InUseCount() = %d, want %d", got, want)
	}
}

func TestSourceHasAvailableTunerSlotReturnsFalseWhenSourceIsOverCapacity(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 2, Enabled: true, OrderIndex: 0},
	})

	probeCtxA, probeCancelA := context.WithCancelCause(context.Background())
	leaseA, err := manager.AcquireProbeForSource(probeCtxA, 1, "probe:a", probeCancelA)
	if err != nil {
		t.Fatalf("AcquireProbeForSource(probe:a) error = %v", err)
	}
	t.Cleanup(func() {
		probeCancelA(nil)
		leaseA.Release()
	})

	probeCtxB, probeCancelB := context.WithCancelCause(context.Background())
	leaseB, err := manager.AcquireProbeForSource(probeCtxB, 1, "probe:b", probeCancelB)
	if err != nil {
		t.Fatalf("AcquireProbeForSource(probe:b) error = %v", err)
	}
	t.Cleanup(func() {
		probeCancelB(nil)
		leaseB.Release()
	})

	manager.Reconfigure([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
	})

	if got := sourceHasAvailableTunerSlot(manager, 1, 0, false); got {
		t.Fatal("sourceHasAvailableTunerSlot() = true, want false when retained in-use exceeds active source capacity")
	}
}

func TestSourceHasAvailableTunerSlotReturnsFalseWhenOnlyRetainedEntryIsPreemptibleAtCapacity(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 2, Enabled: true, OrderIndex: 0},
	})

	probeCtx, probeCancel := context.WithCancelCause(context.Background())
	probeLease, err := manager.AcquireProbeForSource(probeCtx, 1, "probe:retained", probeCancel)
	if err != nil {
		t.Fatalf("AcquireProbeForSource(probe:retained) error = %v", err)
	}
	t.Cleanup(func() {
		probeCancel(nil)
		probeLease.Release()
	})

	manager.Reconfigure([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
	})

	if got := manager.hasPreemptibleLeaseForSource(1); !got {
		t.Fatal("hasPreemptibleLeaseForSource(1) = false, want true from retained transitional probe lease")
	}
	if got := sourceHasAvailableTunerSlot(manager, 1, 0, false); got {
		t.Fatal("sourceHasAvailableTunerSlot() = true, want false when only retained entry is preemptible at active capacity")
	}
}
