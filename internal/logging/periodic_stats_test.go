package logging

import (
	"testing"
	"time"
)

type periodicTestStats struct {
	value int
}

func mergePeriodicTestStats(dst *periodicTestStats, sample periodicTestStats) {
	if dst == nil {
		return
	}
	dst.value += sample.value
}

func TestPeriodicStatsWindowRecordDelayedFirstFlush(t *testing.T) {
	window := NewPeriodicStatsWindow(false, mergePeriodicTestStats)
	base := time.Unix(1_700_100_000, 0).UTC()

	if _, ok := window.Record(base, 10*time.Second, periodicTestStats{value: 1}); ok {
		t.Fatal("first record should not flush when emitFirst=false")
	}
	if _, ok := window.Record(base.Add(5*time.Second), 10*time.Second, periodicTestStats{value: 2}); ok {
		t.Fatal("record inside interval should not flush")
	}
	snapshot, ok := window.Record(base.Add(11*time.Second), 10*time.Second, periodicTestStats{value: 3})
	if !ok {
		t.Fatal("expected flush once interval elapsed")
	}
	if got, want := snapshot.Stats.value, 6; got != want {
		t.Fatalf("snapshot sum = %d, want %d", got, want)
	}
	if !snapshot.WindowStart.Equal(base) {
		t.Fatalf("snapshot start = %s, want %s", snapshot.WindowStart, base)
	}
	if !snapshot.WindowEnd.Equal(base.Add(11 * time.Second)) {
		t.Fatalf("snapshot end = %s, want %s", snapshot.WindowEnd, base.Add(11*time.Second))
	}
	if got, want := snapshot.WindowDuration, 11*time.Second; got != want {
		t.Fatalf("snapshot duration = %s, want %s", got, want)
	}
}

func TestPeriodicStatsWindowRecordImmediateFirstFlush(t *testing.T) {
	window := NewPeriodicStatsWindow(true, func(dst *periodicTestStats, sample periodicTestStats) {
		if dst == nil {
			return
		}
		*dst = sample
	})
	base := time.Unix(1_700_100_100, 0).UTC()

	first, ok := window.Record(base, 10*time.Second, periodicTestStats{value: 1})
	if !ok {
		t.Fatal("first record should flush when emitFirst=true")
	}
	if got, want := first.Stats.value, 1; got != want {
		t.Fatalf("first snapshot value = %d, want %d", got, want)
	}
	if !first.WindowStart.Equal(base) {
		t.Fatalf("first snapshot start = %s, want %s", first.WindowStart, base)
	}

	if _, ok := window.Record(base.Add(5*time.Second), 10*time.Second, periodicTestStats{value: 2}); ok {
		t.Fatal("record inside interval should not flush")
	}
	second, ok := window.Record(base.Add(11*time.Second), 10*time.Second, periodicTestStats{value: 3})
	if !ok {
		t.Fatal("expected flush after interval for pending samples")
	}
	if got, want := second.Stats.value, 3; got != want {
		t.Fatalf("second snapshot value = %d, want %d", got, want)
	}
	if !second.WindowStart.Equal(base.Add(5 * time.Second)) {
		t.Fatalf("second snapshot start = %s, want %s", second.WindowStart, base.Add(5*time.Second))
	}
}

func TestPeriodicStatsWindowResetClearsCadence(t *testing.T) {
	window := NewPeriodicStatsWindow(false, mergePeriodicTestStats)
	base := time.Unix(1_700_100_200, 0).UTC()

	if _, ok := window.Record(base, 10*time.Second, periodicTestStats{value: 1}); ok {
		t.Fatal("first record should not flush")
	}
	window.Reset()

	if _, ok := window.Record(base.Add(time.Second), 10*time.Second, periodicTestStats{value: 2}); ok {
		t.Fatal("record after reset should restart cadence without immediate flush")
	}
	snapshot, ok := window.Record(base.Add(12*time.Second), 10*time.Second, periodicTestStats{value: 3})
	if !ok {
		t.Fatal("expected flush after reset cadence interval elapsed")
	}
	if got, want := snapshot.Stats.value, 5; got != want {
		t.Fatalf("snapshot value = %d, want %d", got, want)
	}
	if !snapshot.WindowStart.Equal(base.Add(time.Second)) {
		t.Fatalf("snapshot start = %s, want %s", snapshot.WindowStart, base.Add(time.Second))
	}
}
