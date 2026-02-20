package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/jobs"
)

// GetStreamMetric returns one cached stream analysis row by item key.
func (s *Store) GetStreamMetric(ctx context.Context, itemKey string) (jobs.StreamMetric, error) {
	itemKey = strings.TrimSpace(itemKey)
	if itemKey == "" {
		return jobs.StreamMetric{}, fmt.Errorf("item_key is required")
	}

	var metric jobs.StreamMetric
	err := s.db.QueryRowContext(
		ctx,
		`SELECT
			item_key,
			analyzed_at,
			COALESCE(width, 0),
			COALESCE(height, 0),
			COALESCE(fps, 0),
			COALESCE(video_codec, ''),
			COALESCE(audio_codec, ''),
			COALESCE(bitrate_bps, 0),
			COALESCE(variant_bps, 0),
			COALESCE(score_hint, 0),
			COALESCE(error, '')
		 FROM stream_metrics
		 WHERE item_key = ?`,
		itemKey,
	).Scan(
		&metric.ItemKey,
		&metric.AnalyzedAt,
		&metric.Width,
		&metric.Height,
		&metric.FPS,
		&metric.VideoCodec,
		&metric.AudioCodec,
		&metric.BitrateBPS,
		&metric.VariantBPS,
		&metric.ScoreHint,
		&metric.Error,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return jobs.StreamMetric{}, sql.ErrNoRows
		}
		return jobs.StreamMetric{}, fmt.Errorf("query stream metric for %q: %w", itemKey, err)
	}
	return metric, nil
}

// UpsertStreamMetric creates or updates one cached stream analysis row.
func (s *Store) UpsertStreamMetric(ctx context.Context, metric jobs.StreamMetric) error {
	metric.ItemKey = strings.TrimSpace(metric.ItemKey)
	if metric.ItemKey == "" {
		return fmt.Errorf("item_key is required")
	}
	if metric.AnalyzedAt <= 0 {
		metric.AnalyzedAt = time.Now().UTC().Unix()
	}
	metric.VideoCodec = strings.TrimSpace(metric.VideoCodec)
	metric.AudioCodec = strings.TrimSpace(metric.AudioCodec)
	metric.Error = strings.TrimSpace(metric.Error)

	if _, err := s.db.ExecContext(
		ctx,
		`INSERT INTO stream_metrics (
			item_key,
			analyzed_at,
			width,
			height,
			fps,
			video_codec,
			audio_codec,
			bitrate_bps,
			variant_bps,
			score_hint,
			error
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(item_key) DO UPDATE SET
			analyzed_at = excluded.analyzed_at,
			width = excluded.width,
			height = excluded.height,
			fps = excluded.fps,
			video_codec = excluded.video_codec,
			audio_codec = excluded.audio_codec,
			bitrate_bps = excluded.bitrate_bps,
			variant_bps = excluded.variant_bps,
			score_hint = excluded.score_hint,
			error = excluded.error`,
		metric.ItemKey,
		metric.AnalyzedAt,
		metric.Width,
		metric.Height,
		metric.FPS,
		metric.VideoCodec,
		metric.AudioCodec,
		metric.BitrateBPS,
		metric.VariantBPS,
		metric.ScoreHint,
		metric.Error,
	); err != nil {
		return fmt.Errorf("upsert stream metric for %q: %w", metric.ItemKey, err)
	}

	return nil
}

// DeleteAllStreamMetrics removes all cached stream analysis rows.
func (s *Store) DeleteAllStreamMetrics(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM stream_metrics`)
	if err != nil {
		return 0, fmt.Errorf("delete stream metrics: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return rows, nil
}
