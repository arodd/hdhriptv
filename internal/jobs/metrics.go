package jobs

import (
	"strings"
	"time"
)

const (
	DefaultMetricsFreshness = 24 * time.Hour
	DefaultErrorRetry       = 30 * time.Minute
)

// StreamMetric stores cached probe metrics for one catalog source item.
type StreamMetric struct {
	ItemKey    string  `json:"item_key"`
	AnalyzedAt int64   `json:"analyzed_at"`
	Width      int     `json:"width,omitempty"`
	Height     int     `json:"height,omitempty"`
	FPS        float64 `json:"fps,omitempty"`
	VideoCodec string  `json:"video_codec,omitempty"`
	AudioCodec string  `json:"audio_codec,omitempty"`
	BitrateBPS int64   `json:"bitrate_bps,omitempty"`
	VariantBPS int64   `json:"variant_bps,omitempty"`
	ScoreHint  float64 `json:"score_hint,omitempty"`
	Error      string  `json:"error,omitempty"`
}

// NeedsRefresh returns whether this cache entry should be re-analyzed.
func (m StreamMetric) NeedsRefresh(now time.Time, successFreshness, errorRetry time.Duration) bool {
	if m.AnalyzedAt <= 0 {
		return true
	}
	if successFreshness <= 0 {
		successFreshness = DefaultMetricsFreshness
	}
	if errorRetry <= 0 {
		errorRetry = DefaultErrorRetry
	}

	last := time.Unix(m.AnalyzedAt, 0).UTC()
	if strings.TrimSpace(m.Error) != "" {
		return now.UTC().After(last.Add(errorRetry))
	}
	return now.UTC().After(last.Add(successFreshness))
}

func (m StreamMetric) IsScorable() bool {
	return strings.TrimSpace(m.Error) == "" && m.Width > 0 && m.Height > 0 && m.FPS > 0
}
