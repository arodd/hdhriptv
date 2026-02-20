package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/jobs"
)

func TestStreamMetricsUpsertAndGet(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	_, err = store.GetStreamMetric(ctx, "missing")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetStreamMetric(missing) error = %v, want sql.ErrNoRows", err)
	}

	inserted := jobs.StreamMetric{
		ItemKey:    "src:news:primary",
		AnalyzedAt: time.Unix(1_700_000_000, 0).Unix(),
		Width:      1920,
		Height:     1080,
		FPS:        59.94,
		VideoCodec: "h264",
		AudioCodec: "aac",
		BitrateBPS: 5_000_000,
		VariantBPS: 5_100_000,
		ScoreHint:  2.75,
	}
	if err := store.UpsertStreamMetric(ctx, inserted); err != nil {
		t.Fatalf("UpsertStreamMetric(inserted) error = %v", err)
	}

	got, err := store.GetStreamMetric(ctx, inserted.ItemKey)
	if err != nil {
		t.Fatalf("GetStreamMetric(inserted) error = %v", err)
	}
	if got.ItemKey != inserted.ItemKey {
		t.Fatalf("ItemKey = %q, want %q", got.ItemKey, inserted.ItemKey)
	}
	if got.Width != 1920 || got.Height != 1080 {
		t.Fatalf("resolution = %dx%d, want 1920x1080", got.Width, got.Height)
	}
	if got.BitrateBPS != inserted.BitrateBPS {
		t.Fatalf("BitrateBPS = %d, want %d", got.BitrateBPS, inserted.BitrateBPS)
	}
	if got.VideoCodec != inserted.VideoCodec {
		t.Fatalf("VideoCodec = %q, want %q", got.VideoCodec, inserted.VideoCodec)
	}
	if got.AudioCodec != inserted.AudioCodec {
		t.Fatalf("AudioCodec = %q, want %q", got.AudioCodec, inserted.AudioCodec)
	}
	if got.Error != "" {
		t.Fatalf("Error = %q, want empty", got.Error)
	}

	updated := jobs.StreamMetric{
		ItemKey:    inserted.ItemKey,
		AnalyzedAt: inserted.AnalyzedAt + 600,
		Width:      1280,
		Height:     720,
		FPS:        30,
		VideoCodec: "mpeg2video",
		AudioCodec: "mp2",
		BitrateBPS: 2_000_000,
		VariantBPS: 2_200_000,
		ScoreHint:  1.50,
		Error:      "timeout",
	}
	if err := store.UpsertStreamMetric(ctx, updated); err != nil {
		t.Fatalf("UpsertStreamMetric(updated) error = %v", err)
	}

	got, err = store.GetStreamMetric(ctx, inserted.ItemKey)
	if err != nil {
		t.Fatalf("GetStreamMetric(updated) error = %v", err)
	}
	if got.Width != 1280 || got.Height != 720 {
		t.Fatalf("updated resolution = %dx%d, want 1280x720", got.Width, got.Height)
	}
	if got.Error != "timeout" {
		t.Fatalf("updated Error = %q, want timeout", got.Error)
	}
	if got.VideoCodec != "mpeg2video" {
		t.Fatalf("updated VideoCodec = %q, want mpeg2video", got.VideoCodec)
	}
	if got.AudioCodec != "mp2" {
		t.Fatalf("updated AudioCodec = %q, want mp2", got.AudioCodec)
	}
	if got.AnalyzedAt != updated.AnalyzedAt {
		t.Fatalf("updated AnalyzedAt = %d, want %d", got.AnalyzedAt, updated.AnalyzedAt)
	}
}

func TestUpsertStreamMetricDefaultsAnalyzedAt(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	before := time.Now().UTC().Unix()
	if err := store.UpsertStreamMetric(ctx, jobs.StreamMetric{ItemKey: "src:auto:1"}); err != nil {
		t.Fatalf("UpsertStreamMetric() error = %v", err)
	}
	got, err := store.GetStreamMetric(ctx, "src:auto:1")
	if err != nil {
		t.Fatalf("GetStreamMetric() error = %v", err)
	}
	if got.AnalyzedAt < before {
		t.Fatalf("AnalyzedAt = %d, want >= %d", got.AnalyzedAt, before)
	}
}

func TestDeleteAllStreamMetrics(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	for _, metric := range []jobs.StreamMetric{
		{ItemKey: "src:auto:1", AnalyzedAt: 1},
		{ItemKey: "src:auto:2", AnalyzedAt: 2},
	} {
		if err := store.UpsertStreamMetric(ctx, metric); err != nil {
			t.Fatalf("UpsertStreamMetric(%q) error = %v", metric.ItemKey, err)
		}
	}

	deleted, err := store.DeleteAllStreamMetrics(ctx)
	if err != nil {
		t.Fatalf("DeleteAllStreamMetrics() error = %v", err)
	}
	if deleted != 2 {
		t.Fatalf("DeleteAllStreamMetrics() deleted = %d, want 2", deleted)
	}

	_, err = store.GetStreamMetric(ctx, "src:auto:1")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetStreamMetric(src:auto:1) error = %v, want sql.ErrNoRows", err)
	}

	_, err = store.GetStreamMetric(ctx, "src:auto:2")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetStreamMetric(src:auto:2) error = %v, want sql.ErrNoRows", err)
	}
}
