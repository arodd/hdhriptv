package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestVirtualTunerManagerAcquireClientForSourceIsolatedPools(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
		{SourceID: 2, Name: "Backup", TunerCount: 1, Enabled: true, OrderIndex: 1},
	})

	leaseA, err := manager.AcquireClientForSource(context.Background(), 1, "101", "client-a")
	if err != nil {
		t.Fatalf("AcquireClientForSource(source=1) error = %v", err)
	}
	defer leaseA.Release()

	leaseB, err := manager.AcquireClientForSource(context.Background(), 2, "102", "client-b")
	if err != nil {
		t.Fatalf("AcquireClientForSource(source=2) error = %v", err)
	}
	defer leaseB.Release()

	if _, err := manager.AcquireClientForSource(context.Background(), 1, "103", "client-c"); !errors.Is(err, ErrNoTunersAvailable) {
		t.Fatalf("AcquireClientForSource(source=1 full) error = %v, want ErrNoTunersAvailable", err)
	}
}

func TestVirtualTunerManagerAcquireProbeUsesDefaultEnabledSource(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: false, OrderIndex: 0},
		{SourceID: 2, Name: "Backup", TunerCount: 1, Enabled: true, OrderIndex: 1},
	})

	probeCtx, probeCancel := context.WithCancelCause(context.Background())
	defer probeCancel(nil)
	lease, err := manager.AcquireProbe(probeCtx, "probe:test", probeCancel)
	if err != nil {
		t.Fatalf("AcquireProbe() error = %v", err)
	}
	defer lease.Release()

	if got := lease.PlaylistSourceID; got != 2 {
		t.Fatalf("probe lease playlist_source_id = %d, want 2", got)
	}
	if got := lease.PlaylistSourceName; got != "Backup" {
		t.Fatalf("probe lease playlist_source_name = %q, want Backup", got)
	}
}

func TestVirtualTunerManagerAcquireProbeForSourceUsesRequestedPool(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
		{SourceID: 2, Name: "Backup", TunerCount: 1, Enabled: true, OrderIndex: 1},
	})

	primaryClientLease, err := manager.AcquireClientForSource(context.Background(), 1, "100", "client-primary")
	if err != nil {
		t.Fatalf("AcquireClientForSource(source=1) error = %v", err)
	}
	defer primaryClientLease.Release()

	probeCtx, probeCancel := context.WithCancelCause(context.Background())
	defer probeCancel(nil)
	backupProbeLease, err := manager.AcquireProbeForSource(probeCtx, 2, "probe:backup", probeCancel)
	if err != nil {
		t.Fatalf("AcquireProbeForSource(source=2) error = %v", err)
	}
	defer backupProbeLease.Release()

	if got := backupProbeLease.PlaylistSourceID; got != 2 {
		t.Fatalf("backup probe lease playlist_source_id = %d, want 2", got)
	}
	if got := backupProbeLease.PlaylistSourceName; got != "Backup" {
		t.Fatalf("backup probe lease playlist_source_name = %q, want Backup", got)
	}

	if _, err := manager.AcquireProbeForSource(probeCtx, 1, "probe:primary", probeCancel); !errors.Is(err, ErrNoTunersAvailable) {
		t.Fatalf("AcquireProbeForSource(source=1 full) error = %v, want ErrNoTunersAvailable", err)
	}
}

func TestVirtualTunerManagerSnapshotIncludesSourceMetadata(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
		{SourceID: 2, Name: "Backup", TunerCount: 2, Enabled: true, OrderIndex: 1},
	})

	leaseA, err := manager.AcquireClientForSource(context.Background(), 2, "101", "client-a")
	if err != nil {
		t.Fatalf("AcquireClientForSource(source=2, leaseA) error = %v", err)
	}
	defer leaseA.Release()
	leaseB, err := manager.AcquireClientForSource(context.Background(), 2, "102", "client-b")
	if err != nil {
		t.Fatalf("AcquireClientForSource(source=2, leaseB) error = %v", err)
	}
	defer leaseB.Release()

	snapshot := manager.Snapshot()
	if got := len(snapshot); got != 2 {
		t.Fatalf("len(Snapshot()) = %d, want 2", got)
	}
	for i := range snapshot {
		if got := snapshot[i].PlaylistSourceID; got != 2 {
			t.Fatalf("snapshot[%d].playlist_source_id = %d, want 2", i, got)
		}
		if got := snapshot[i].PlaylistSourceName; got != "Backup" {
			t.Fatalf("snapshot[%d].playlist_source_name = %q, want Backup", i, got)
		}
	}
	if snapshot[0].VirtualTunerSlot == snapshot[1].VirtualTunerSlot {
		t.Fatalf("virtual tuner slots must differ, got %d and %d", snapshot[0].VirtualTunerSlot, snapshot[1].VirtualTunerSlot)
	}
}

func TestVirtualTunerManagerUpdatesTunerUtilizationMetric(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 2, Enabled: true, OrderIndex: 0},
	})
	label := playlistSourceMetricLabel(1, "Primary")
	if got := testutil.ToFloat64(virtualTunerUtilizationMetric.WithLabelValues(label)); got != 0 {
		t.Fatalf("initial utilization gauge = %.2f, want 0", got)
	}

	lease, err := manager.AcquireClientForSource(context.Background(), 1, "101", "client-a")
	if err != nil {
		t.Fatalf("AcquireClientForSource() error = %v", err)
	}
	if got := testutil.ToFloat64(virtualTunerUtilizationMetric.WithLabelValues(label)); got != 0.5 {
		t.Fatalf("utilization gauge after acquire = %.2f, want 0.5", got)
	}

	lease.Release()
	if got := testutil.ToFloat64(virtualTunerUtilizationMetric.WithLabelValues(label)); got != 0 {
		t.Fatalf("utilization gauge after release = %.2f, want 0", got)
	}
}

func TestVirtualTunerManagerReconfigureAppliesSourceChangesWithoutRestart(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
	})

	legacyLease, err := manager.AcquireClientForSource(context.Background(), 1, "100", "client-legacy")
	if err != nil {
		t.Fatalf("AcquireClientForSource(initial source 1) error = %v", err)
	}
	defer legacyLease.Release()

	manager.Reconfigure([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 2, Enabled: true, OrderIndex: 0},
		{SourceID: 2, Name: "Backup", TunerCount: 1, Enabled: true, OrderIndex: 1},
	})

	if got, want := manager.Capacity(), 3; got != want {
		t.Fatalf("Capacity() after reconfigure = %d, want %d", got, want)
	}

	backupLease, err := manager.AcquireClientForSource(context.Background(), 2, "200", "client-backup")
	if err != nil {
		t.Fatalf("AcquireClientForSource(source=2) after reconfigure error = %v", err)
	}
	defer backupLease.Release()

	primaryLease, err := manager.AcquireClientForSource(context.Background(), 1, "101", "client-primary")
	if err != nil {
		t.Fatalf("AcquireClientForSource(source=1) after reconfigure error = %v", err)
	}
	defer primaryLease.Release()

	if _, err := manager.AcquireClientForSource(context.Background(), 1, "102", "client-primary-overflow"); !errors.Is(err, ErrNoTunersAvailable) {
		t.Fatalf("AcquireClientForSource(source=1 overflow) error = %v, want ErrNoTunersAvailable", err)
	}

	if got, want := manager.InUseCountForSource(1), 2; got != want {
		t.Fatalf("InUseCountForSource(1) = %d, want %d", got, want)
	}
}

func TestVirtualTunerManagerFinalizeAcquireRejectsStaleEntryAfterReconfigure(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
	})

	staleEntry, _, resolvedSourceID := manager.activeEntryForAcquire(1)
	if staleEntry == nil || staleEntry.pool == nil {
		t.Fatal("activeEntryForAcquire(source=1) returned nil entry")
	}

	manager.Reconfigure([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 2, Enabled: true, OrderIndex: 0},
	})

	staleLease, err := staleEntry.pool.AcquireClient(context.Background(), "101", "client-stale")
	if err != nil {
		t.Fatalf("staleEntry.pool.AcquireClient() error = %v", err)
	}

	wrapped, err := manager.finalizeAcquiredLease(resolvedSourceID, staleEntry, staleLease)
	if !errors.Is(err, ErrNoTunersAvailable) {
		t.Fatalf("finalizeAcquiredLease(stale entry) error = %v, want ErrNoTunersAvailable", err)
	}
	if wrapped != nil {
		t.Fatalf("finalizeAcquiredLease(stale entry) lease = %#v, want nil", wrapped)
	}

	if got, want := staleEntry.pool.InUseCount(), 0; got != want {
		t.Fatalf("staleEntry.pool.InUseCount() = %d, want %d after stale release", got, want)
	}
	if got, want := manager.InUseCountForSource(1), 0; got != want {
		t.Fatalf("manager.InUseCountForSource(1) = %d, want %d", got, want)
	}
	if got, want := manager.CapacityForSource(1), 2; got != want {
		t.Fatalf("manager.CapacityForSource(1) = %d, want %d", got, want)
	}
}

func TestVirtualTunerManagerReconfigureDisablesSourceForNewLeases(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
		{SourceID: 2, Name: "Backup", TunerCount: 1, Enabled: true, OrderIndex: 1},
	})

	backupLease, err := manager.AcquireClientForSource(context.Background(), 2, "200", "client-backup")
	if err != nil {
		t.Fatalf("AcquireClientForSource(source=2) initial error = %v", err)
	}
	defer backupLease.Release()

	manager.Reconfigure([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
		{SourceID: 2, Name: "Backup", TunerCount: 1, Enabled: false, OrderIndex: 1},
	})

	if _, err := manager.AcquireClientForSource(context.Background(), 2, "201", "client-backup-disabled"); !errors.Is(err, ErrNoTunersAvailable) {
		t.Fatalf("AcquireClientForSource(disabled source=2) error = %v, want ErrNoTunersAvailable", err)
	}

	snapshot := manager.Snapshot()
	if got, want := len(snapshot), 1; got != want {
		t.Fatalf("len(Snapshot()) after disable with legacy lease = %d, want %d", got, want)
	}
	if got, want := snapshot[0].PlaylistSourceID, int64(2); got != want {
		t.Fatalf("snapshot[0].playlist_source_id = %d, want %d", got, want)
	}
}

func TestVirtualTunerManagerHasPreemptibleLeaseIncludesRetainedEntries(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
		{SourceID: 2, Name: "Backup", TunerCount: 1, Enabled: true, OrderIndex: 1},
	})

	probeCtx, probeCancel := context.WithCancelCause(context.Background())
	probeLease, err := manager.AcquireProbeForSource(probeCtx, 2, "probe:backup", probeCancel)
	if err != nil {
		t.Fatalf("AcquireProbeForSource(source=2) error = %v", err)
	}
	defer func() {
		probeCancel(nil)
		probeLease.Release()
	}()

	manager.Reconfigure([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
		{SourceID: 2, Name: "Backup", TunerCount: 1, Enabled: false, OrderIndex: 1},
	})

	if got := manager.hasPreemptibleLeaseForSource(2); !got {
		t.Fatal("hasPreemptibleLeaseForSource(2) = false, want true for retained transitional probe lease")
	}
}

func TestVirtualTunerManagerVirtualSnapshotIncludesRetainedDisabledSource(t *testing.T) {
	manager := NewVirtualTunerManager([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
		{SourceID: 2, Name: "Backup", TunerCount: 1, Enabled: true, OrderIndex: 1},
	})

	backupLease, err := manager.AcquireClientForSource(context.Background(), 2, "200", "client-backup")
	if err != nil {
		t.Fatalf("AcquireClientForSource(source=2) initial error = %v", err)
	}
	defer backupLease.Release()

	manager.Reconfigure([]VirtualTunerSource{
		{SourceID: 1, Name: "Primary", TunerCount: 1, Enabled: true, OrderIndex: 0},
		{SourceID: 2, Name: "Backup", TunerCount: 1, Enabled: false, OrderIndex: 1},
	})

	virtualSnapshot := manager.VirtualTunerSnapshot()
	if got, want := len(virtualSnapshot), 2; got != want {
		t.Fatalf("len(VirtualTunerSnapshot()) = %d, want %d", got, want)
	}

	var (
		primaryRow VirtualTunerPoolSnapshot
		backupRow  VirtualTunerPoolSnapshot
	)
	for _, row := range virtualSnapshot {
		switch row.PlaylistSourceID {
		case 1:
			primaryRow = row
		case 2:
			backupRow = row
		}
	}

	if got, want := primaryRow.TunerCount, 1; got != want {
		t.Fatalf("primary virtual snapshot tuner_count = %d, want %d", got, want)
	}
	if got, want := primaryRow.InUseCount, 0; got != want {
		t.Fatalf("primary virtual snapshot in_use_count = %d, want %d", got, want)
	}

	if got, want := backupRow.PlaylistSourceName, "Backup"; got != want {
		t.Fatalf("backup virtual snapshot playlist_source_name = %q, want %q", got, want)
	}
	if got, want := backupRow.TunerCount, 1; got != want {
		t.Fatalf("backup virtual snapshot tuner_count = %d, want %d", got, want)
	}
	if got, want := backupRow.InUseCount, 1; got != want {
		t.Fatalf("backup virtual snapshot in_use_count = %d, want %d", got, want)
	}
	if got, want := backupRow.IdleCount, 0; got != want {
		t.Fatalf("backup virtual snapshot idle_count = %d, want %d", got, want)
	}
}
