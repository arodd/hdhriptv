package jobs

import (
	"testing"
	"time"
)

func TestStreamMetricNeedsRefresh(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()

	if !(StreamMetric{}).NeedsRefresh(now, 24*time.Hour, 30*time.Minute) {
		t.Fatal("zero metric should require refresh")
	}

	freshSuccess := StreamMetric{
		AnalyzedAt: now.Add(-2 * time.Hour).Unix(),
		Width:      1920,
		Height:     1080,
		FPS:        30,
	}
	if freshSuccess.NeedsRefresh(now, 24*time.Hour, 30*time.Minute) {
		t.Fatal("successful metric within freshness window should not require refresh")
	}

	staleSuccess := StreamMetric{
		AnalyzedAt: now.Add(-25 * time.Hour).Unix(),
		Width:      1920,
		Height:     1080,
		FPS:        30,
	}
	if !staleSuccess.NeedsRefresh(now, 24*time.Hour, 30*time.Minute) {
		t.Fatal("successful metric older than freshness window should require refresh")
	}

	recentError := StreamMetric{
		AnalyzedAt: now.Add(-10 * time.Minute).Unix(),
		Error:      "timeout",
	}
	if recentError.NeedsRefresh(now, 24*time.Hour, 30*time.Minute) {
		t.Fatal("errored metric within retry window should not require refresh")
	}

	staleError := StreamMetric{
		AnalyzedAt: now.Add(-45 * time.Minute).Unix(),
		Error:      "timeout",
	}
	if !staleError.NeedsRefresh(now, 24*time.Hour, 30*time.Minute) {
		t.Fatal("errored metric older than retry window should require refresh")
	}
}

func TestStreamMetricIsScorable(t *testing.T) {
	scorable := StreamMetric{Width: 1920, Height: 1080, FPS: 30}
	if !scorable.IsScorable() {
		t.Fatal("expected metric to be scorable")
	}

	invalid := []StreamMetric{
		{Width: 0, Height: 1080, FPS: 30},
		{Width: 1920, Height: 0, FPS: 30},
		{Width: 1920, Height: 1080, FPS: 0},
		{Width: 1920, Height: 1080, FPS: 30, Error: "probe failed"},
	}
	for i, metric := range invalid {
		if metric.IsScorable() {
			t.Fatalf("case %d: expected metric to be non-scorable", i)
		}
	}
}
