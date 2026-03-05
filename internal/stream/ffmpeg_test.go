package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestStartupFailureReasonDetectsUpstreamStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "ffmpeg HTTP 404 wrapped in startup error",
			err: fmt.Errorf("open ffmpeg stream: %w", fmt.Errorf("%w (ffmpeg: exit status 1: [http @ 0x3] HTTP error 404 Not Found)", &sourceStartupError{
				reason: "source closed before startup bytes were received",
				err:    io.EOF,
			})),
			want: "upstream returned status 404",
		},
		{
			name: "ffmpeg server returned status",
			err: fmt.Errorf("open ffmpeg stream: %w", fmt.Errorf("%w (ffmpeg: exit status 1: [http @ 0x9] Server returned 503 Service Unavailable)", &sourceStartupError{
				reason: "source closed before startup bytes were received",
				err:    io.EOF,
			})),
			want: "upstream returned status 503",
		},
		{
			name: "direct mode reason with body preview is preserved",
			err: &sourceStartupError{
				reason: "upstream returned status 404: missing token",
				err:    io.EOF,
			},
			want: "upstream returned status 404: missing token",
		},
		{
			name: "nil error",
			err:  nil,
			want: "startup failed",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := startupFailureReason(tc.err); got != tc.want {
				t.Fatalf("startupFailureReason() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsLikelyUpstreamOverlimitStatus(t *testing.T) {
	tests := []struct {
		name string
		code int
		want bool
	}{
		{name: "not found is not overlimit", code: http.StatusNotFound, want: false},
		{name: "too many requests is overlimit", code: http.StatusTooManyRequests, want: true},
		{name: "service unavailable is not overlimit", code: http.StatusServiceUnavailable, want: false},
		{name: "zero status is not overlimit", code: 0, want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := isLikelyUpstreamOverlimitStatus(tc.code); got != tc.want {
				t.Fatalf("isLikelyUpstreamOverlimitStatus(%d) = %t, want %t", tc.code, got, tc.want)
			}
		})
	}
}

func TestNormalizeFFmpegReconnectSettings(t *testing.T) {
	tests := []struct {
		name       string
		mode       string
		enabled    bool
		delayMax   time.Duration
		maxRetries int
		httpErrors string
		wantEnable bool
		wantDelay  time.Duration
		wantMax    int
		wantErrors string
	}{
		{
			name:       "ffmpeg defaults",
			mode:       "ffmpeg-copy",
			enabled:    false,
			delayMax:   0,
			maxRetries: 0,
			httpErrors: "",
			wantEnable: false,
			wantDelay:  3 * time.Second,
			wantMax:    1,
			wantErrors: "",
		},
		{
			name:       "explicit ffmpeg disabled",
			mode:       "ffmpeg-copy",
			enabled:    false,
			delayMax:   3 * time.Second,
			maxRetries: 2,
			httpErrors: "404,429,5xx",
			wantEnable: false,
			wantDelay:  3 * time.Second,
			wantMax:    2,
			wantErrors: "404,429,5xx",
		},
		{
			name:       "direct mode forces reconnect off",
			mode:       "direct",
			enabled:    true,
			delayMax:   3 * time.Second,
			maxRetries: 4,
			httpErrors: "404,429,5xx",
			wantEnable: false,
			wantDelay:  0,
			wantMax:    -1,
			wantErrors: "",
		},
		{
			name:       "trim and clamp",
			mode:       "ffmpeg-transcode",
			enabled:    true,
			delayMax:   -1 * time.Second,
			maxRetries: -3,
			httpErrors: "  429,503  ",
			wantEnable: true,
			wantDelay:  0,
			wantMax:    -1,
			wantErrors: "429,503",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotEnabled, gotDelay, gotMaxRetries, gotErrors := normalizeFFmpegReconnectSettings(
				tc.mode,
				tc.enabled,
				tc.delayMax,
				tc.maxRetries,
				tc.httpErrors,
			)
			if gotEnabled != tc.wantEnable {
				t.Fatalf("enabled = %t, want %t", gotEnabled, tc.wantEnable)
			}
			if gotDelay != tc.wantDelay {
				t.Fatalf("delay = %s, want %s", gotDelay, tc.wantDelay)
			}
			if gotMaxRetries != tc.wantMax {
				t.Fatalf("maxRetries = %d, want %d", gotMaxRetries, tc.wantMax)
			}
			if gotErrors != tc.wantErrors {
				t.Fatalf("httpErrors = %q, want %q", gotErrors, tc.wantErrors)
			}
		})
	}
}

func TestResolveFFmpegCopyRegenerateTimestamps(t *testing.T) {
	falseValue := false
	trueValue := true
	tests := []struct {
		name       string
		mode       string
		configured *bool
		want       bool
	}{
		{
			name:       "copy mode defaults to enabled",
			mode:       "ffmpeg-copy",
			configured: nil,
			want:       true,
		},
		{
			name:       "copy mode explicit false",
			mode:       "ffmpeg-copy",
			configured: &falseValue,
			want:       false,
		},
		{
			name:       "copy mode explicit true",
			mode:       "ffmpeg-copy",
			configured: &trueValue,
			want:       true,
		},
		{
			name:       "transcode mode defaults off",
			mode:       "ffmpeg-transcode",
			configured: nil,
			want:       false,
		},
		{
			name:       "direct mode defaults off",
			mode:       "direct",
			configured: nil,
			want:       false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveFFmpegCopyRegenerateTimestamps(tc.mode, tc.configured); got != tc.want {
				t.Fatalf("resolveFFmpegCopyRegenerateTimestamps(%q, %v) = %t, want %t", tc.mode, tc.configured, got, tc.want)
			}
		})
	}
}

func TestNormalizeFFmpegSourceLogLevel(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		fallback string
		want     string
	}{
		{
			name:     "explicit warning",
			input:    "warning",
			fallback: defaultFFmpegInputLogLevel,
			want:     "warning",
		},
		{
			name:     "warn alias",
			input:    "warn",
			fallback: defaultFFmpegInputLogLevel,
			want:     "warning",
		},
		{
			name:     "empty uses fallback",
			input:    "",
			fallback: "debug",
			want:     "debug",
		},
		{
			name:     "invalid uses default fallback",
			input:    "trace",
			fallback: "invalid",
			want:     defaultFFmpegInputLogLevel,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeFFmpegSourceLogLevel(tc.input, tc.fallback); got != tc.want {
				t.Fatalf("normalizeFFmpegSourceLogLevel(%q, %q) = %q, want %q", tc.input, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestFFmpegReconnectDelayMaxSeconds(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want int
	}{
		{name: "zero", in: 0, want: 0},
		{name: "negative", in: -1 * time.Second, want: 0},
		{name: "whole second", in: 2 * time.Second, want: 2},
		{name: "round up fractional", in: 1500 * time.Millisecond, want: 2},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := ffmpegReconnectDelayMaxSeconds(tc.in); got != tc.want {
				t.Fatalf("ffmpegReconnectDelayMaxSeconds(%s) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseCloseWithTimeoutWorkerBudget(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{
			name: "empty uses default budget",
			raw:  "",
			want: boundedCloseWorkerBudget,
		},
		{
			name: "trimmed valid value",
			raw:  " 24 ",
			want: 24,
		},
		{
			name: "minimum value",
			raw:  "1",
			want: 1,
		},
		{
			name:    "zero rejected",
			raw:     "0",
			wantErr: true,
		},
		{
			name:    "too large rejected",
			raw:     fmt.Sprintf("%d", closeWithTimeoutWorkerBudgetMax+1),
			wantErr: true,
		},
		{
			name:    "non integer rejected",
			raw:     "abc",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCloseWithTimeoutWorkerBudget(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseCloseWithTimeoutWorkerBudget(%q) error = nil, want non-nil", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCloseWithTimeoutWorkerBudget(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("parseCloseWithTimeoutWorkerBudget(%q) = %d, want %d", tc.raw, got, tc.want)
			}
		})
	}
}

func TestNextPreviewRetryBackoff(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{
			name: "non-positive uses initial backoff",
			in:   0,
			want: previewRetryInitialBackoff,
		},
		{
			name: "doubles within max",
			in:   previewRetryInitialBackoff,
			want: 2 * previewRetryInitialBackoff,
		},
		{
			name: "caps at max",
			in:   previewRetryMaxBackoff,
			want: previewRetryMaxBackoff,
		},
		{
			name: "near max caps",
			in:   previewRetryMaxBackoff - 1*time.Millisecond,
			want: previewRetryMaxBackoff,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := nextPreviewRetryBackoff(tc.in); got != tc.want {
				t.Fatalf("nextPreviewRetryBackoff(%s) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

func TestHandlerFailsOverToSecondSourceOnFFmpeg404(t *testing.T) {
	tmp := t.TempDir()
	videoAudioPath := filepath.Join(tmp, "video_audio.ts")
	if err := os.WriteFile(videoAudioPath, startupTestProbeWithPMTStreams(0x1B, 0x0F), 0o644); err != nil {
		t.Fatalf("WriteFile(videoAudioPath) error = %v", err)
	}
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-404-failover.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

video_audio=%q
input=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == "-i" ]]; then
    input="$arg"
    break
  fi
  prev="$arg"
done

case "$input" in
  *"/src404")
    echo "[http @ 0x4f3] HTTP error 404 Not Found" >&2
    exit 1
    ;;
  *"/srcok")
    cat "$video_audio"
    for _ in {1..200}; do
      printf "good-stream"
      sleep 0.01
    done
    exit 0
    ;;
  *)
    echo "unexpected input URL: $input" >&2
    exit 1
    ;;
esac
`, videoAudioPath))

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:bad-ffmpeg-404",
					StreamURL:     "http://example.test/src404",
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:good-ffmpeg",
					StreamURL:     "http://example.test/srcok",
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	handler := NewHandler(
		Config{
			Mode:                       "ffmpeg-copy",
			FFmpegPath:                 ffmpegPath,
			StartupTimeout:             500 * time.Millisecond,
			FailoverTotalTimeout:       2 * time.Second,
			MinProbeBytes:              testVideoAudioStartupMinProbeBytes(),
			BufferChunkBytes:           1,
			BufferPublishFlushInterval: 10 * time.Millisecond,
			SessionIdleTimeout:         20 * time.Millisecond,
			HTTPClient:                 &http.Client{Timeout: 250 * time.Millisecond},
		},
		NewPool(1),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer reqCancel()

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil).WithContext(reqCtx)
	rec := newDeadlineResponseRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); !strings.Contains(got, "good-stream") {
		t.Fatalf("body = %q, want data containing good-stream", got)
	}
	waitFor(t, time.Second, func() bool {
		return len(provider.sourceFailures()) >= 1 && len(provider.sourceSuccesses()) >= 1
	})
	failures := provider.sourceFailures()
	if failures[0].sourceID != 10 {
		t.Fatalf("failures[0].source_id = %d, want 10", failures[0].sourceID)
	}
	if !strings.Contains(failures[0].reason, "upstream returned status 404") {
		t.Fatalf("failure reason = %q, want upstream status details", failures[0].reason)
	}
	successes := provider.sourceSuccesses()
	if successes[0] != 11 {
		t.Fatalf("successes[0] = %d, want 11", successes[0])
	}
}

func TestSessionManagerReturnsStartupTimeoutWhenFFmpegProducesNoBytes(t *testing.T) {
	tmp := t.TempDir()
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-timeout.sh", `#!/usr/bin/env bash
set -euo pipefail
while true; do
  :
done
`)

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"118": {ChannelID: 20, GuideNumber: "118", GuideName: "Timeout Test", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			20: {
				{
					SourceID:      106,
					ChannelID:     20,
					ItemKey:       "src:timeout",
					StreamURL:     "http://example.test/slow",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "ffmpeg-copy",
		FFmpegPath:                 ffmpegPath,
		StartupTimeout:             120 * time.Millisecond,
		FailoverTotalTimeout:       400 * time.Millisecond,
		MinProbeBytes:              testVideoAudioStartupMinProbeBytes(),
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         20 * time.Millisecond,
	}, NewPool(1), provider)

	subscribeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := manager.Subscribe(subscribeCtx, provider.channelsByGuide["118"])
	if err == nil {
		t.Fatal("Subscribe() error = nil, want startup timeout failure")
	}
	if !strings.Contains(err.Error(), "open ffmpeg stream: startup timeout waiting for source bytes") {
		t.Fatalf("Subscribe() error = %q, want startup-timeout ffmpeg error", err)
	}

	waitFor(t, time.Second, func() bool {
		return len(provider.sourceFailures()) == 1
	})
	failures := provider.sourceFailures()
	if failures[0].sourceID != 106 {
		t.Fatalf("failures[0].source_id = %d, want 106", failures[0].sourceID)
	}
	if !strings.Contains(failures[0].reason, "startup timeout waiting for source bytes") {
		t.Fatalf("failure reason = %q, want startup-timeout reason", failures[0].reason)
	}
}

func TestHandlerFailsOverWhenFirstFFmpegSourceTimesOutOnStartup(t *testing.T) {
	tmp := t.TempDir()
	videoAudioPath := filepath.Join(tmp, "video_audio.ts")
	if err := os.WriteFile(videoAudioPath, startupTestProbeWithPMTStreams(0x1B, 0x0F), 0o644); err != nil {
		t.Fatalf("WriteFile(videoAudioPath) error = %v", err)
	}
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-timeout-failover.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

video_audio=%q
input=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == "-i" ]]; then
    input="$arg"
    break
  fi
  prev="$arg"
done

case "$input" in
  *"/srcslow")
    while true; do
      :
    done
    ;;
  *"/srcok")
    cat "$video_audio"
    for _ in {1..120}; do
      printf "good-stream"
      sleep 0.01
    done
    exit 0
    ;;
  *)
    echo "unexpected input URL: $input" >&2
    exit 1
    ;;
esac
`, videoAudioPath))

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"118": {ChannelID: 20, GuideNumber: "118", GuideName: "Timeout Failover", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			20: {
				{
					SourceID:      106,
					ChannelID:     20,
					ItemKey:       "src:slow",
					StreamURL:     "http://example.test/srcslow",
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      107,
					ChannelID:     20,
					ItemKey:       "src:ok",
					StreamURL:     "http://example.test/srcok",
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	handler := NewHandler(
		Config{
			Mode:                       "ffmpeg-copy",
			FFmpegPath:                 ffmpegPath,
			StartupTimeout:             120 * time.Millisecond,
			FailoverTotalTimeout:       2 * time.Second,
			MinProbeBytes:              testVideoAudioStartupMinProbeBytes(),
			BufferChunkBytes:           1,
			BufferPublishFlushInterval: 10 * time.Millisecond,
			SessionIdleTimeout:         20 * time.Millisecond,
		},
		NewPool(1),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer reqCancel()

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v118", nil).WithContext(reqCtx)
	rec := newDeadlineResponseRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); !strings.Contains(got, "good-stream") {
		t.Fatalf("body = %q, want data containing good-stream", got)
	}
	waitFor(t, time.Second, func() bool {
		return len(provider.sourceFailures()) >= 1 && len(provider.sourceSuccesses()) >= 1
	})
	failures := provider.sourceFailures()
	if failures[0].sourceID != 106 {
		t.Fatalf("failures[0].source_id = %d, want 106", failures[0].sourceID)
	}
	if !strings.Contains(failures[0].reason, "startup timeout waiting for source bytes") {
		t.Fatalf("failure reason = %q, want startup-timeout reason", failures[0].reason)
	}
	successes := provider.sourceSuccesses()
	if successes[0] != 107 {
		t.Fatalf("successes = %#v, want startup success for source 107", successes)
	}
}

func TestHandlerFailsOverWhenFFmpegSourceClosesBeforeStartupBytes(t *testing.T) {
	tmp := t.TempDir()
	videoAudioPath := filepath.Join(tmp, "video_audio.ts")
	if err := os.WriteFile(videoAudioPath, startupTestProbeWithPMTStreams(0x1B, 0x0F), 0o644); err != nil {
		t.Fatalf("WriteFile(videoAudioPath) error = %v", err)
	}
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-source-closed-failover.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

video_audio=%q
input=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == "-i" ]]; then
    input="$arg"
    break
  fi
  prev="$arg"
done

case "$input" in
  *"/srcclosed")
    echo "Error opening input file $input." >&2
    exit 8
    ;;
  *"/srcok")
    cat "$video_audio"
    for _ in {1..120}; do
      printf "good-stream"
      sleep 0.01
    done
    exit 0
    ;;
  *)
    echo "unexpected input URL: $input" >&2
    exit 1
    ;;
esac
`, videoAudioPath))

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"109": {ChannelID: 9, GuideNumber: "109", GuideName: "US Fox News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			9: {
				{
					SourceID:      49,
					ChannelID:     9,
					ItemKey:       "src:source-closed",
					StreamURL:     "http://example.test/srcclosed",
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      50,
					ChannelID:     9,
					ItemKey:       "src:good",
					StreamURL:     "http://example.test/srcok",
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	handler := NewHandler(
		Config{
			Mode:                       "ffmpeg-copy",
			FFmpegPath:                 ffmpegPath,
			StartupTimeout:             250 * time.Millisecond,
			FailoverTotalTimeout:       2 * time.Second,
			MinProbeBytes:              testVideoAudioStartupMinProbeBytes(),
			BufferChunkBytes:           1,
			BufferPublishFlushInterval: 10 * time.Millisecond,
			SessionIdleTimeout:         20 * time.Millisecond,
		},
		NewPool(1),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer reqCancel()

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v109", nil).WithContext(reqCtx)
	rec := newDeadlineResponseRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); !strings.Contains(got, "good-stream") {
		t.Fatalf("body = %q, want data containing good-stream", got)
	}
	waitFor(t, time.Second, func() bool {
		return len(provider.sourceFailures()) >= 1 && len(provider.sourceSuccesses()) >= 1
	})
	failures := provider.sourceFailures()
	if failures[0].sourceID != 49 {
		t.Fatalf("failures[0].source_id = %d, want 49", failures[0].sourceID)
	}
	if !strings.Contains(failures[0].reason, "source closed before startup bytes were received") {
		t.Fatalf("failure reason = %q, want source-closed startup reason", failures[0].reason)
	}
	successes := provider.sourceSuccesses()
	if successes[0] != 50 {
		t.Fatalf("successes = %#v, want startup success for source 50", successes)
	}
}

func TestSessionManagerReturnsDetailedFFmpeg404ErrorWhenAllSourcesFail(t *testing.T) {
	tmp := t.TempDir()
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-404-all-fail.sh", `#!/usr/bin/env bash
set -euo pipefail

input=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == "-i" ]]; then
    input="$arg"
    break
  fi
  prev="$arg"
done

echo "[in#0 @ 0x561f3e89d600] Error opening input: Server returned 404 Not Found" >&2
echo "Error opening input file $input." >&2
echo "Error opening input files: Server returned 404 Not Found" >&2
exit 8
`)

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"109": {ChannelID: 9, GuideNumber: "109", GuideName: "US Fox News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			9: {
				{
					SourceID:      49,
					ChannelID:     9,
					ItemKey:       "src:a",
					StreamURL:     "http://example.test/a",
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      50,
					ChannelID:     9,
					ItemKey:       "src:b",
					StreamURL:     "http://example.test/b",
					PriorityIndex: 1,
					Enabled:       true,
				},
				{
					SourceID:      51,
					ChannelID:     9,
					ItemKey:       "src:c",
					StreamURL:     "http://example.test/c",
					PriorityIndex: 2,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "ffmpeg-copy",
		FFmpegPath:                 ffmpegPath,
		StartupTimeout:             300 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              testVideoAudioStartupMinProbeBytes(),
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         20 * time.Millisecond,
	}, NewPool(1), provider)

	subscribeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := manager.Subscribe(subscribeCtx, provider.channelsByGuide["109"])
	if err == nil {
		t.Fatal("Subscribe() error = nil, want ffmpeg startup failure")
	}
	if !strings.Contains(err.Error(), "open ffmpeg stream: source closed before startup bytes were received") {
		t.Fatalf("Subscribe() error = %q, want source-closed ffmpeg startup failure", err)
	}
	if !strings.Contains(err.Error(), "Server returned 404 Not Found") {
		t.Fatalf("Subscribe() error = %q, want ffmpeg stderr 404 detail", err)
	}

	waitFor(t, time.Second, func() bool {
		return len(provider.sourceFailures()) == 3
	})
	failures := provider.sourceFailures()
	if len(failures) != 3 {
		t.Fatalf("failures = %#v, want one startup failure per source", failures)
	}
	for i := range failures {
		if !strings.Contains(failures[i].reason, "upstream returned status 404") {
			t.Fatalf("failure[%d].reason = %q, want normalized upstream 404 reason", i, failures[i].reason)
		}
	}
	if successes := provider.sourceSuccesses(); len(successes) != 0 {
		t.Fatalf("successes = %#v, want no source startup successes", successes)
	}

	waitFor(t, 2*time.Second, func() bool {
		stats := manager.Snapshot()
		return len(stats) == 0
	})
}

func TestStartDirectTimesOutWaitingForResponseHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(350 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("late-header"))
	}))
	defer upstream.Close()

	started := time.Now()
	_, err := startDirect(
		context.Background(),
		&http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				ResponseHeaderTimeout: 2 * time.Second,
			},
		},
		upstream.URL,
		120*time.Millisecond,
		1,
		false,
	)
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("startDirect() error = nil, want startup header timeout")
	}

	var streamErr *streamFailure
	if !errors.As(err, &streamErr) {
		t.Fatalf("startDirect() error type = %T, want *streamFailure", err)
	}
	if !strings.Contains(err.Error(), "startup timeout waiting for upstream response headers") {
		t.Fatalf("startDirect() error = %q, want startup header-timeout reason", err)
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("startDirect() elapsed = %s, want timeout near startup window", elapsed)
	}
}

func TestStartDirectTimeoutClosesLateResponseBody(t *testing.T) {
	transport := newDelayedStartupResponseTransport(350 * time.Millisecond)
	client := &http.Client{Transport: transport}

	started := time.Now()
	_, err := startDirect(
		context.Background(),
		client,
		"http://example.test/late-timeout",
		80*time.Millisecond,
		1,
		false,
	)
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("startDirect() error = nil, want startup header timeout")
	}
	if !strings.Contains(err.Error(), "startup timeout waiting for upstream response headers") {
		t.Fatalf("startDirect() error = %q, want startup header-timeout reason", err)
	}
	if elapsed > 250*time.Millisecond {
		t.Fatalf("startDirect() elapsed = %s, want timeout return without late-response wait", elapsed)
	}
	if !transport.waitForBodyClose(2 * time.Second) {
		t.Fatal("late startup response body was not closed after timeout abort")
	}
}

func TestStartDirectReturnsParentContextCancellationDuringHeaderWait(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(350 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("late-header"))
	}))
	defer upstream.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()

	_, err := startDirect(ctx, &http.Client{}, upstream.URL, 200*time.Millisecond, 1, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("startDirect() error = %v, want context canceled", err)
	}
}

func TestStartDirectParentCancelClosesLateResponseBody(t *testing.T) {
	transport := newDelayedStartupResponseTransport(350 * time.Millisecond)
	client := &http.Client{Transport: transport}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()

	started := time.Now()
	_, err := startDirect(ctx, client, "http://example.test/late-cancel", 600*time.Millisecond, 1, false)
	elapsed := time.Since(started)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("startDirect() error = %v, want context canceled", err)
	}
	if elapsed > 250*time.Millisecond {
		t.Fatalf("startDirect() elapsed = %s, want parent cancel to return quickly", elapsed)
	}
	if !transport.waitForBodyClose(2 * time.Second) {
		t.Fatal("late startup response body was not closed after parent cancel abort")
	}
}

func TestHandlerFailsOverWhenFirstDirectSourceStallsBeforeResponseHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/srcslow":
			time.Sleep(350 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("slow-stream"))
		case "/srcok":
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			for i := 0; i < 120; i++ {
				if _, err := w.Write(testVideoAudioStartupFixtureText("good-stream")); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
				time.Sleep(5 * time.Millisecond)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"121": {ChannelID: 21, GuideNumber: "121", GuideName: "Header Timeout", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			21: {
				{
					SourceID:      201,
					ChannelID:     21,
					ItemKey:       "src:slow-header",
					StreamURL:     upstream.URL + "/srcslow",
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      202,
					ChannelID:     21,
					ItemKey:       "src:good-header",
					StreamURL:     upstream.URL + "/srcok",
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	handler := NewHandler(
		Config{
			Mode:                       "direct",
			StartupTimeout:             120 * time.Millisecond,
			FailoverTotalTimeout:       2 * time.Second,
			MinProbeBytes:              testVideoAudioStartupMinProbeBytes(),
			BufferChunkBytes:           1,
			BufferPublishFlushInterval: 10 * time.Millisecond,
			SessionIdleTimeout:         20 * time.Millisecond,
		},
		NewPool(1),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 450*time.Millisecond)
	defer reqCancel()

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v121", nil).WithContext(reqCtx)
	rec := newDeadlineResponseRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); !strings.Contains(got, "good-stream") {
		t.Fatalf("body = %q, want data containing good-stream", got)
	}

	waitFor(t, time.Second, func() bool {
		return len(provider.sourceFailures()) >= 1 && len(provider.sourceSuccesses()) >= 1
	})
	failures := provider.sourceFailures()
	if failures[0].sourceID != 201 {
		t.Fatalf("failures[0].source_id = %d, want 201", failures[0].sourceID)
	}
	if !strings.Contains(failures[0].reason, "startup timeout waiting for upstream response headers") {
		t.Fatalf("failure reason = %q, want startup header-timeout reason", failures[0].reason)
	}
	successes := provider.sourceSuccesses()
	if successes[0] != 202 {
		t.Fatalf("successes = %#v, want startup success for source 202", successes)
	}
}

func TestHandlerClientContextCancelReturnsHTTP200(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 400; i++ {
			if _, err := w.Write(testVideoAudioStartupFixtureText("x")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"109": {ChannelID: 9, GuideNumber: "109", GuideName: "US Fox News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			9: {
				{
					SourceID:      51,
					ChannelID:     9,
					ItemKey:       "src:fox",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	handler := NewHandler(
		Config{
			Mode:                       "direct",
			StartupTimeout:             2 * time.Second,
			FailoverTotalTimeout:       5 * time.Second,
			MinProbeBytes:              testVideoAudioStartupMinProbeBytes(),
			BufferChunkBytes:           1,
			BufferPublishFlushInterval: 10 * time.Millisecond,
			SessionIdleTimeout:         20 * time.Millisecond,
		},
		NewPool(1),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer reqCancel()

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v109", nil).WithContext(reqCtx)
	rec := newDeadlineResponseRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	waitFor(t, time.Second, func() bool {
		successes := provider.sourceSuccesses()
		return len(successes) >= 1
	})
	successes := provider.sourceSuccesses()
	if successes[0] != 51 {
		t.Fatalf("successes = %#v, want startup success for source 51", successes)
	}
}

func TestStartFFmpegReturnsSourceClosedErrorWith404Details(t *testing.T) {
	tmp := t.TempDir()
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-source-closed-404.sh", `#!/usr/bin/env bash
set -euo pipefail
echo "[in#0 @ 0x561f3e89d600] Error opening input: Server returned 404 Not Found" >&2
echo "Error opening input file https://example.test/live/stream.m3u8." >&2
echo "Error opening input files: Server returned 404 Not Found" >&2
exit 8
`)

	_, err := startFFmpeg(
		context.Background(),
		ffmpegPath,
		"https://example.test/live/stream.m3u8",
		"ffmpeg-copy",
		300*time.Millisecond,
		1,
		1,
		1,
		32*1024,
		200*time.Millisecond,
		true,
		2*time.Second,
		-1,
		"404,429,5xx",
		false,
	)
	if err == nil {
		t.Fatal("startFFmpeg() error = nil, want startup failure")
	}

	var streamErr *streamFailure
	if !errors.As(err, &streamErr) {
		t.Fatalf("startFFmpeg() error type = %T, want *streamFailure", err)
	}
	if !strings.Contains(err.Error(), "open ffmpeg stream: source closed before startup bytes were received") {
		t.Fatalf("startFFmpeg() error = %q, want source-closed startup error", err)
	}
	if !strings.Contains(err.Error(), "Server returned 404 Not Found") {
		t.Fatalf("startFFmpeg() error = %q, want ffmpeg 404 stderr detail", err)
	}
	if got := startupFailureReason(err); got != "upstream returned status 404" {
		t.Fatalf("startupFailureReason() = %q, want %q", got, "upstream returned status 404")
	}
}

func TestStartFFmpegFallsBackWhenReadrateInitialBurstUnsupported(t *testing.T) {
	tmp := t.TempDir()
	argsLogPath := filepath.Join(tmp, "args.log")
	videoAudioPath := filepath.Join(tmp, "video_audio.ts")
	if err := os.WriteFile(videoAudioPath, startupTestProbeWithPMTStreams(0x1B, 0x0F), 0o644); err != nil {
		t.Fatalf("WriteFile(videoAudioPath) error = %v", err)
	}
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-readrate-initial-burst-unsupported.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
args_log=%q
video_audio=%q
printf '%%s\n' "$*" >> "$args_log"
for arg in "$@"; do
  if [[ "$arg" == "-readrate_initial_burst" ]]; then
    echo "Unrecognized option 'readrate_initial_burst'." >&2
    echo "Error splitting the argument list: Option not found" >&2
    exit 1
  fi
done
cat "$video_audio"
sleep 1
`, argsLogPath, videoAudioPath))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	session, err := startFFmpeg(
		ctx,
		ffmpegPath,
		"https://example.test/live/stream.m3u8",
		"ffmpeg-copy",
		500*time.Millisecond,
		testVideoAudioStartupMinProbeBytes(),
		1,
		1,
		32*1024,
		250*time.Millisecond,
		false,
		0,
		-1,
		"",
		false,
	)
	if err != nil {
		t.Fatalf("startFFmpeg() error = %v", err)
	}
	defer session.close()

	if !session.startupNoInitialBurstFallback {
		t.Fatal("startupNoInitialBurstFallback = false, want true")
	}

	argsRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("ReadFile(argsLogPath) error = %v", readErr)
	}
	lines := strings.Split(strings.TrimSpace(string(argsRaw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("len(argsLogLines) = %d, want 2; lines=%q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "-readrate_initial_burst 1") {
		t.Fatalf("first attempt args = %q, want -readrate_initial_burst 1", lines[0])
	}
	if strings.Contains(lines[1], "-readrate_initial_burst") {
		t.Fatalf("fallback attempt args = %q, want readrate_initial_burst removed", lines[1])
	}
}

func TestStartFFmpegFallsBackWhenReadrateCatchupUnsupported(t *testing.T) {
	tmp := t.TempDir()
	argsLogPath := filepath.Join(tmp, "args.log")
	videoAudioPath := filepath.Join(tmp, "video_audio.ts")
	if err := os.WriteFile(videoAudioPath, startupTestProbeWithPMTStreams(0x1B, 0x0F), 0o644); err != nil {
		t.Fatalf("WriteFile(videoAudioPath) error = %v", err)
	}
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-readrate-catchup-unsupported.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
args_log=%q
video_audio=%q
printf '%%s\n' "$*" >> "$args_log"
for arg in "$@"; do
  if [[ "$arg" == "-readrate_catchup" ]]; then
    echo "Unrecognized option 'readrate_catchup'." >&2
    echo "Error splitting the argument list: Option not found" >&2
    exit 1
  fi
done
cat "$video_audio"
sleep 1
`, argsLogPath, videoAudioPath))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	session, err := startFFmpeg(
		ctx,
		ffmpegPath,
		"https://example.test/live/stream.m3u8",
		"ffmpeg-copy",
		500*time.Millisecond,
		testVideoAudioStartupMinProbeBytes(),
		1,
		1,
		32*1024,
		250*time.Millisecond,
		false,
		0,
		-1,
		"",
		false,
	)
	if err != nil {
		t.Fatalf("startFFmpeg() error = %v", err)
	}
	defer session.close()

	if !session.startupNoReadrateCatchupFallback {
		t.Fatal("startupNoReadrateCatchupFallback = false, want true")
	}

	argsRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("ReadFile(argsLogPath) error = %v", readErr)
	}
	lines := strings.Split(strings.TrimSpace(string(argsRaw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("len(argsLogLines) = %d, want 2; lines=%q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "-readrate_catchup 1") {
		t.Fatalf("first attempt args = %q, want -readrate_catchup 1", lines[0])
	}
	if strings.Contains(lines[1], "-readrate_catchup") {
		t.Fatalf("fallback attempt args = %q, want readrate_catchup removed", lines[1])
	}
}

func TestStartSourceSessionFallsBackWhenInputBufferSizeUnsupported(t *testing.T) {
	tmp := t.TempDir()
	argsLogPath := filepath.Join(tmp, "args.log")
	videoAudioPath := filepath.Join(tmp, "video_audio.ts")
	if err := os.WriteFile(videoAudioPath, startupTestProbeWithPMTStreams(0x1B, 0x0F), 0o644); err != nil {
		t.Fatalf("WriteFile(videoAudioPath) error = %v", err)
	}
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-input-buffer-size-unsupported.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
args_log=%q
video_audio=%q
printf '%%s\n' "$*" >> "$args_log"
for arg in "$@"; do
  if [[ "$arg" == "-buffer_size" ]]; then
    echo "Option buffer_size not found." >&2
    echo "Error opening input files: Option not found" >&2
    exit 1
  fi
done
cat "$video_audio"
sleep 1
`, argsLogPath, videoAudioPath))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	session, err := startSourceSessionWithContextsConfigured(
		ctx,
		ctx,
		"ffmpeg-copy",
		nil,
		ffmpegPath,
		"https://example.test/live/stream.m3u8",
		500*time.Millisecond,
		testVideoAudioStartupMinProbeBytes(),
		1,
		1,
		1,
		32*1024,
		250*time.Millisecond,
		false,
		0,
		-1,
		"",
		8*1024*1024,
		0,
		false,
		true,
		0,
		defaultFFmpegInputLogLevel,
		ffmpegSourceStderrLoggingOptions{},
		false,
	)
	if err != nil {
		t.Fatalf("startSourceSessionWithContextsConfigured() error = %v", err)
	}
	defer session.close()

	if !session.startupNoInputBufferSizeFallback {
		t.Fatal("startupNoInputBufferSizeFallback = false, want true")
	}

	argsRaw, readErr := os.ReadFile(argsLogPath)
	if readErr != nil {
		t.Fatalf("ReadFile(argsLogPath) error = %v", readErr)
	}
	lines := strings.Split(strings.TrimSpace(string(argsRaw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("len(argsLogLines) = %d, want 2; lines=%q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "-buffer_size 8388608") {
		t.Fatalf("first attempt args = %q, want -buffer_size 8388608", lines[0])
	}
	if strings.Contains(lines[1], "-buffer_size") {
		t.Fatalf("fallback attempt args = %q, want buffer_size removed", lines[1])
	}
}

func TestBoundedTailBufferRetainsRecentStderr(t *testing.T) {
	buf := newBoundedTailBuffer(8)
	if _, err := buf.Write([]byte("12345")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := buf.Write([]byte("6789")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	got := string(buf.Bytes())
	if !strings.HasPrefix(got, "[stderr truncated to last 8 bytes] ") {
		t.Fatalf("Bytes() prefix = %q, want truncation marker", got)
	}
	if !strings.HasSuffix(got, "23456789") {
		t.Fatalf("Bytes() suffix = %q, want retained tail bytes", got)
	}
}

func TestSanitizeFFmpegStderrLineForLog(t *testing.T) {
	raw := "open failed url=https://alice:secret@example.test/live.m3u8?token=signed&sig=abc authorization: Bearer x.y.z token=opaque"
	sanitized := sanitizeFFmpegStderrLineForLog(raw)
	if strings.Contains(sanitized, "alice:secret@") || strings.Contains(sanitized, "signed") || strings.Contains(sanitized, "x.y.z") || strings.Contains(sanitized, "opaque") {
		t.Fatalf("sanitizeFFmpegStderrLineForLog leaked sensitive data: %q", sanitized)
	}
	if !strings.Contains(sanitized, "https://example.test/live.m3u8") {
		t.Fatalf("sanitizeFFmpegStderrLineForLog missing sanitized URL: %q", sanitized)
	}
	if !strings.Contains(sanitized, "token=<redacted>") {
		t.Fatalf("sanitizeFFmpegStderrLineForLog missing token redaction: %q", sanitized)
	}
}

func TestFFmpegStderrLineLoggerSanitizesAndEmitsContext(t *testing.T) {
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	writer := newFFmpegStderrLineLogger(
		"ffmpeg-copy",
		ffmpegSourceStderrLoggingOptions{
			enabled:      true,
			logger:       logger,
			logLevel:     slog.LevelInfo,
			maxLineBytes: 512,
			context: ffmpegSourceStderrLogContext{
				channelID:   91,
				guideNumber: "191",
				sourceID:    8,
				sourceURL:   "https://alice:secret@example.test/live.m3u8?token=signed",
				tunerID:     2,
			},
		},
	)
	if writer == nil {
		t.Fatal("newFFmpegStderrLineLogger() = nil, want configured logger")
	}

	if _, err := writer.Write([]byte("first line\nhttp error opening https://alice:secret@example.test/live.m3u8?token=signed\n")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	writer.Flush()

	text := logs.String()
	if got := strings.Count(text, "ffmpeg source stderr line"); got != 2 {
		t.Fatalf("log line count = %d, want 2; logs=%q", got, text)
	}
	if !strings.Contains(text, "ffmpeg_stderr_seq=1") || !strings.Contains(text, "ffmpeg_stderr_seq=2") {
		t.Fatalf("expected monotonic ffmpeg_stderr_seq values in logs: %q", text)
	}
	if !strings.Contains(text, "channel_id=91") || !strings.Contains(text, "guide_number=191") || !strings.Contains(text, "source_id=8") || !strings.Contains(text, "tuner_id=2") {
		t.Fatalf("stderr pass-through context fields missing from logs: %q", text)
	}
	if !strings.Contains(text, "source_url=https://example.test/live.m3u8") {
		t.Fatalf("source_url not sanitized in context fields: %q", text)
	}
	if strings.Contains(text, "alice:secret@") || strings.Contains(text, "token=signed") {
		t.Fatalf("stderr pass-through logs leaked sensitive source URL fields: %q", text)
	}
}

func TestFFmpegStderrLineLoggerTruncatesAndFlushesPartialLine(t *testing.T) {
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	writer := newFFmpegStderrLineLogger(
		"ffmpeg-copy",
		ffmpegSourceStderrLoggingOptions{
			enabled:      true,
			logger:       logger,
			logLevel:     slog.LevelInfo,
			maxLineBytes: 8,
			context:      ffmpegSourceStderrLogContext{},
		},
	)
	if writer == nil {
		t.Fatal("newFFmpegStderrLineLogger() = nil, want configured logger")
	}

	if _, err := writer.Write([]byte("abcdef")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if _, err := writer.Write([]byte("ghi\nxyz012345")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	writer.Flush()

	text := logs.String()
	if got := strings.Count(text, "ffmpeg source stderr line"); got != 2 {
		t.Fatalf("log line count = %d, want 2; logs=%q", got, text)
	}
	if !strings.Contains(text, "ffmpeg_stderr_line=abcdefgh") {
		t.Fatalf("expected first truncated line in logs: %q", text)
	}
	if !strings.Contains(text, "ffmpeg_stderr_line=xyz01234") {
		t.Fatalf("expected flushed truncated line in logs: %q", text)
	}
	if got := strings.Count(text, "ffmpeg_stderr_line_truncated=true"); got != 2 {
		t.Fatalf("truncated marker count = %d, want 2; logs=%q", got, text)
	}
}

func TestFFmpegAnalyzeDurationMicroseconds(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want int64
	}{
		{name: "disabled", in: 0, want: 0},
		{name: "negative", in: -1 * time.Second, want: 0},
		{name: "exact", in: 200 * time.Millisecond, want: 200000},
		{name: "round_up", in: 1500 * time.Nanosecond, want: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ffmpegAnalyzeDurationMicroseconds(tc.in); got != tc.want {
				t.Fatalf("ffmpegAnalyzeDurationMicroseconds(%s) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestAppendFFmpegRWTimeoutInputArg(t *testing.T) {
	noTimeout := appendFFmpegRWTimeoutInputArg([]string{"-hide_banner"}, 0)
	if len(noTimeout) != 1 {
		t.Fatalf("appendFFmpegRWTimeoutInputArg(0) len = %d, want 1", len(noTimeout))
	}
	if noTimeout[0] != "-hide_banner" {
		t.Fatalf("appendFFmpegRWTimeoutInputArg(0)[0] = %q, want -hide_banner", noTimeout[0])
	}

	args := appendFFmpegRWTimeoutInputArg([]string{"-hide_banner"}, 1500*time.Nanosecond)
	if len(args) != 3 {
		t.Fatalf("appendFFmpegRWTimeoutInputArg(1500ns) len = %d, want 3", len(args))
	}
	if args[1] != ffmpegRWTimeoutOption || args[2] != "2" {
		t.Fatalf("appendFFmpegRWTimeoutInputArg(1500ns) = %#v, want %q %q", args, ffmpegRWTimeoutOption, "2")
	}
}

func TestFormatFFmpegReadRateCatchup(t *testing.T) {
	tests := []struct {
		name             string
		readRate         float64
		readRateCatchup  float64
		wantCatchupValue string
	}{
		{
			name:             "fallback_to_base_when_missing",
			readRate:         1.25,
			readRateCatchup:  0,
			wantCatchupValue: "1.25",
		},
		{
			name:             "fallback_to_default_when_readrate_invalid",
			readRate:         0,
			readRateCatchup:  0,
			wantCatchupValue: "1",
		},
		{
			name:             "preserve_explicit_catchup_below_base",
			readRate:         1.5,
			readRateCatchup:  1.25,
			wantCatchupValue: "1.25",
		},
		{
			name:             "preserve_explicit_catchup_above_base",
			readRate:         1.0,
			readRateCatchup:  1.75,
			wantCatchupValue: "1.75",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := formatFFmpegReadRateCatchup(tc.readRate, tc.readRateCatchup)
			if got != tc.wantCatchupValue {
				t.Fatalf("formatFFmpegReadRateCatchup(%v, %v) = %q, want %q", tc.readRate, tc.readRateCatchup, got, tc.wantCatchupValue)
			}
		})
	}
}

func TestStartupProbeStreamInventoryFromPMT(t *testing.T) {
	probe := startupTestProbeWithPMTStreams(0x1B, 0x0F)
	inventory := startupProbeStreamInventory(probe).normalized()

	if inventory.detectionMethod != "pmt" {
		t.Fatalf("detectionMethod = %q, want pmt", inventory.detectionMethod)
	}
	if inventory.videoStreamCount != 1 {
		t.Fatalf("videoStreamCount = %d, want 1", inventory.videoStreamCount)
	}
	if inventory.audioStreamCount != 1 {
		t.Fatalf("audioStreamCount = %d, want 1", inventory.audioStreamCount)
	}
	if inventory.componentState() != "video_audio" {
		t.Fatalf("componentState = %q, want video_audio", inventory.componentState())
	}
	if len(inventory.videoCodecs) != 1 || inventory.videoCodecs[0] != "h264" {
		t.Fatalf("videoCodecs = %#v, want [h264]", inventory.videoCodecs)
	}
	if len(inventory.audioCodecs) != 1 || inventory.audioCodecs[0] != "aac" {
		t.Fatalf("audioCodecs = %#v, want [aac]", inventory.audioCodecs)
	}
}

func TestStartupInventoryRequiresVideoAudioStrict(t *testing.T) {
	tests := []struct {
		name      string
		inventory startupStreamInventory
		wantErr   bool
		wantState string
	}{
		{
			name: "video_audio accepted",
			inventory: startupStreamInventory{
				detectionMethod:  "pmt",
				videoStreamCount: 1,
				audioStreamCount: 1,
			},
			wantErr: false,
		},
		{
			name: "video_only rejected",
			inventory: startupStreamInventory{
				detectionMethod:  "pmt",
				videoStreamCount: 1,
				audioStreamCount: 0,
			},
			wantErr:   true,
			wantState: "video_only",
		},
		{
			name: "audio_only rejected",
			inventory: startupStreamInventory{
				detectionMethod:  "pmt",
				videoStreamCount: 0,
				audioStreamCount: 1,
			},
			wantErr:   true,
			wantState: "audio_only",
		},
		{
			name: "undetected rejected",
			inventory: startupStreamInventory{
				detectionMethod:  "pmt",
				videoStreamCount: 0,
				audioStreamCount: 0,
			},
			wantErr:   true,
			wantState: "undetected",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := startupInventoryRequiresVideoAudio(tc.inventory)
			if tc.wantErr {
				if err == nil {
					t.Fatal("startupInventoryRequiresVideoAudio() error = nil, want non-nil")
				}
				if !strings.Contains(err.Error(), "missing required audio+video components") {
					t.Fatalf("startupInventoryRequiresVideoAudio() error = %q, want strict component requirement detail", err.Error())
				}
				if tc.wantState != "" && !strings.Contains(err.Error(), "state="+tc.wantState) {
					t.Fatalf("startupInventoryRequiresVideoAudio() error = %q, want state=%s detail", err.Error(), tc.wantState)
				}
				return
			}
			if err != nil {
				t.Fatalf("startupInventoryRequiresVideoAudio() unexpected error: %v", err)
			}
		})
	}
}

func TestStartFFmpegRetriesWithRelaxedStartupDetectionWhenInventoryIncomplete(t *testing.T) {
	tmp := t.TempDir()
	statePath := filepath.Join(tmp, "attempt.txt")
	argsLogPath := filepath.Join(tmp, "args.log")
	videoOnlyPath := filepath.Join(tmp, "video_only.ts")
	videoAudioPath := filepath.Join(tmp, "video_audio.ts")

	videoOnlyProbe := startupTestProbeWithPMTStreams(0x1B)
	videoAudioProbe := startupTestProbeWithPMTStreams(0x1B, 0x0F)

	if err := os.WriteFile(videoOnlyPath, videoOnlyProbe, 0o644); err != nil {
		t.Fatalf("WriteFile(videoOnlyPath) error = %v", err)
	}
	if err := os.WriteFile(videoAudioPath, videoAudioProbe, 0o644); err != nil {
		t.Fatalf("WriteFile(videoAudioPath) error = %v", err)
	}

	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-startup-retry.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

state=%q
args_log=%q
video_only=%q
video_audio=%q

attempt=0
if [[ -f "$state" ]]; then
  attempt=$(cat "$state")
fi
attempt=$((attempt+1))
echo "$attempt" > "$state"
printf 'attempt=%%s args=%%s\n' "$attempt" "$*" >> "$args_log"
if [[ "$attempt" -eq 1 ]]; then
  cat "$video_only"
else
  cat "$video_audio"
fi

sleep 1
`, statePath, argsLogPath, videoOnlyPath, videoAudioPath))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	session, err := startFFmpeg(
		ctx,
		ffmpegPath,
		"https://example.test/live/stream.m3u8",
		"ffmpeg-copy",
		2*time.Second,
		376,
		1,
		1,
		16*1024,
		100*time.Millisecond,
		false,
		0,
		-1,
		"",
		false,
	)
	if err != nil {
		t.Fatalf("startFFmpeg() error = %v", err)
	}
	defer session.close()

	if !session.startupRetryRelaxedProbe {
		t.Fatal("startupRetryRelaxedProbe = false, want true")
	}
	if session.startupRetryRelaxedProbeToken != "startup_detection_incomplete" {
		t.Fatalf(
			"startupRetryRelaxedProbeToken = %q, want %q",
			session.startupRetryRelaxedProbeToken,
			"startup_detection_incomplete",
		)
	}
	if session.startupInventory.componentState() != "video_audio" {
		t.Fatalf("startupInventory.componentState = %q, want video_audio", session.startupInventory.componentState())
	}

	argsRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("ReadFile(argsLogPath) error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(argsRaw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("len(argsLogLines) = %d, want 2; lines=%q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "-probesize 128000") {
		t.Fatalf("first attempt args = %q, want -probesize 128000", lines[0])
	}
	if !strings.Contains(lines[0], "-analyzeduration 1000000") {
		t.Fatalf("first attempt args = %q, want -analyzeduration 1000000", lines[0])
	}
	if !strings.Contains(lines[1], "-probesize 2000000") {
		t.Fatalf("retry attempt args = %q, want -probesize 2000000", lines[1])
	}
	if !strings.Contains(lines[1], "-analyzeduration 3000000") {
		t.Fatalf("retry attempt args = %q, want -analyzeduration 3000000", lines[1])
	}
}

func TestStartFFmpegRetriesWithRelaxedStartupDetectionWhenInventoryUndetected(t *testing.T) {
	tmp := t.TempDir()
	statePath := filepath.Join(tmp, "attempt.txt")
	argsLogPath := filepath.Join(tmp, "args.log")
	undetectedPath := filepath.Join(tmp, "undetected.ts")
	videoAudioPath := filepath.Join(tmp, "video_audio.ts")

	undetectedProbe := startupTestProbeWithPMTStreams()
	videoAudioProbe := startupTestProbeWithPMTStreams(0x1B, 0x0F)

	if err := os.WriteFile(undetectedPath, undetectedProbe, 0o644); err != nil {
		t.Fatalf("WriteFile(undetectedPath) error = %v", err)
	}
	if err := os.WriteFile(videoAudioPath, videoAudioProbe, 0o644); err != nil {
		t.Fatalf("WriteFile(videoAudioPath) error = %v", err)
	}

	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-startup-retry-undetected.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

state=%q
args_log=%q
undetected=%q
video_audio=%q

attempt=0
if [[ -f "$state" ]]; then
  attempt=$(cat "$state")
fi
attempt=$((attempt+1))
echo "$attempt" > "$state"
printf 'attempt=%%s args=%%s\n' "$attempt" "$*" >> "$args_log"
if [[ "$attempt" -eq 1 ]]; then
  cat "$undetected"
else
  cat "$video_audio"
fi

sleep 1
`, statePath, argsLogPath, undetectedPath, videoAudioPath))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	session, err := startFFmpeg(
		ctx,
		ffmpegPath,
		"https://example.test/live/stream.m3u8",
		"ffmpeg-copy",
		2*time.Second,
		len(videoAudioProbe),
		1,
		1,
		16*1024,
		100*time.Millisecond,
		false,
		0,
		-1,
		"",
		false,
	)
	if err != nil {
		t.Fatalf("startFFmpeg() error = %v", err)
	}
	defer session.close()

	if !session.startupRetryRelaxedProbe {
		t.Fatal("startupRetryRelaxedProbe = false, want true")
	}
	if session.startupRetryRelaxedProbeToken != startupRetryReasonIncomplete {
		t.Fatalf("startupRetryRelaxedProbeToken = %q, want %q", session.startupRetryRelaxedProbeToken, startupRetryReasonIncomplete)
	}
	if got := session.startupInventory.componentState(); got != "video_audio" {
		t.Fatalf("startupInventory.componentState = %q, want video_audio", got)
	}

	argsRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("ReadFile(argsLogPath) error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(argsRaw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("len(argsLogLines) = %d, want 2; lines=%q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "-probesize 128000") {
		t.Fatalf("first attempt args = %q, want -probesize 128000", lines[0])
	}
	if !strings.Contains(lines[0], "-analyzeduration 1000000") {
		t.Fatalf("first attempt args = %q, want -analyzeduration 1000000", lines[0])
	}
	if !strings.Contains(lines[1], "-probesize 2000000") {
		t.Fatalf("retry attempt args = %q, want -probesize 2000000", lines[1])
	}
	if !strings.Contains(lines[1], "-analyzeduration 3000000") {
		t.Fatalf("retry attempt args = %q, want -analyzeduration 3000000", lines[1])
	}
}

func TestStartFFmpegFailsWhenRelaxedRetryRemainsUndetected(t *testing.T) {
	tmp := t.TempDir()
	statePath := filepath.Join(tmp, "attempt.txt")
	argsLogPath := filepath.Join(tmp, "args.log")
	undetectedPath := filepath.Join(tmp, "undetected.ts")

	undetectedProbe := startupTestProbeWithPMTStreams()
	if err := os.WriteFile(undetectedPath, undetectedProbe, 0o644); err != nil {
		t.Fatalf("WriteFile(undetectedPath) error = %v", err)
	}

	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-startup-retry-undetected-fail.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

state=%q
args_log=%q
undetected=%q

attempt=0
if [[ -f "$state" ]]; then
  attempt=$(cat "$state")
fi
attempt=$((attempt+1))
echo "$attempt" > "$state"
printf 'attempt=%%s args=%%s\n' "$attempt" "$*" >> "$args_log"
cat "$undetected"
sleep 1
`, statePath, argsLogPath, undetectedPath))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := startFFmpeg(
		ctx,
		ffmpegPath,
		"https://example.test/live/stream.m3u8",
		"ffmpeg-copy",
		2*time.Second,
		len(undetectedProbe),
		1,
		1,
		16*1024,
		100*time.Millisecond,
		false,
		0,
		-1,
		"",
		false,
	)
	if err == nil {
		t.Fatal("startFFmpeg() error = nil, want startup failure for undetected inventory")
	}
	if !strings.Contains(err.Error(), "missing required audio+video components") {
		t.Fatalf("startFFmpeg() error = %q, want strict component requirement detail", err.Error())
	}
	if !strings.Contains(err.Error(), "state=undetected") {
		t.Fatalf("startFFmpeg() error = %q, want undetected state detail", err.Error())
	}

	argsRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("ReadFile(argsLogPath) error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(argsRaw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("len(argsLogLines) = %d, want 2; lines=%q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "-probesize 128000") {
		t.Fatalf("first attempt args = %q, want -probesize 128000", lines[0])
	}
	if !strings.Contains(lines[0], "-analyzeduration 1000000") {
		t.Fatalf("first attempt args = %q, want -analyzeduration 1000000", lines[0])
	}
	if !strings.Contains(lines[1], "-probesize 2000000") {
		t.Fatalf("retry attempt args = %q, want -probesize 2000000", lines[1])
	}
	if !strings.Contains(lines[1], "-analyzeduration 3000000") {
		t.Fatalf("retry attempt args = %q, want -analyzeduration 3000000", lines[1])
	}
}

func TestStartFFmpegFailsWhenRelaxedRetryFailsAfterIncompleteInventory(t *testing.T) {
	tmp := t.TempDir()
	statePath := filepath.Join(tmp, "attempt.txt")
	argsLogPath := filepath.Join(tmp, "args.log")
	videoOnlyPath := filepath.Join(tmp, "video_only.ts")

	videoOnlyProbe := startupTestProbeWithPMTStreams(0x1B)
	if err := os.WriteFile(videoOnlyPath, videoOnlyProbe, 0o644); err != nil {
		t.Fatalf("WriteFile(videoOnlyPath) error = %v", err)
	}

	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-startup-retry-fallback.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

state=%q
args_log=%q
video_only=%q

attempt=0
if [[ -f "$state" ]]; then
  attempt=$(cat "$state")
fi
attempt=$((attempt+1))
echo "$attempt" > "$state"
printf 'attempt=%%s args=%%s\n' "$attempt" "$*" >> "$args_log"

if [[ "$attempt" -eq 1 ]]; then
  cat "$video_only"
  sleep 2
else
  echo "[http @ 0x5f2] HTTP error 503 Service Unavailable" >&2
  exit 1
fi
`, statePath, argsLogPath, videoOnlyPath))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := startFFmpeg(
		ctx,
		ffmpegPath,
		"https://example.test/live/stream.m3u8",
		"ffmpeg-copy",
		2*time.Second,
		len(videoOnlyProbe),
		1,
		1,
		16*1024,
		100*time.Millisecond,
		false,
		0,
		-1,
		"",
		false,
	)
	if err == nil {
		t.Fatal("startFFmpeg() error = nil, want startup failure after relaxed retry failure")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("startFFmpeg() error = %q, want retry failure detail", err.Error())
	}

	argsRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("ReadFile(argsLogPath) error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(argsRaw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("len(argsLogLines) = %d, want 2; lines=%q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "-probesize 128000") {
		t.Fatalf("first attempt args = %q, want -probesize 128000", lines[0])
	}
	if !strings.Contains(lines[0], "-analyzeduration 1000000") {
		t.Fatalf("first attempt args = %q, want -analyzeduration 1000000", lines[0])
	}
	if !strings.Contains(lines[1], "-probesize 2000000") {
		t.Fatalf("retry attempt args = %q, want -probesize 2000000", lines[1])
	}
	if !strings.Contains(lines[1], "-analyzeduration 3000000") {
		t.Fatalf("retry attempt args = %q, want -analyzeduration 3000000", lines[1])
	}
}

func TestStartFFmpegSkipsRelaxedRetryWhenPreCutoverInventoryHasVideoAndAudio(t *testing.T) {
	tmp := t.TempDir()
	argsLogPath := filepath.Join(tmp, "args.log")
	probePath := filepath.Join(tmp, "probe.ts")

	preIDRPES := startupTestPESPackets(
		0x0101,
		0x00,
		0xE0,
		[]byte{
			0x00, 0x00, 0x01, 0x41, 0x9a, 0x01, // non-IDR slice
		},
	)
	randomAccessPES := startupTestPESPackets(
		0x0101,
		0x01,
		0xE0,
		[]byte{
			0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f, // SPS
			0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x06, 0xe2, // PPS
			0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, // IDR
		},
	)
	// PMT metadata includes both video+audio, but cutover trimming removes it from the
	// startup stream prefix. Startup inventory should still use the pre-cutover bytes.
	probe := append(startupTestProbeWithPMTStreams(0x1B, 0x0F), preIDRPES...)
	probe = append(probe, randomAccessPES...)
	if err := os.WriteFile(probePath, probe, 0o644); err != nil {
		t.Fatalf("WriteFile(probePath) error = %v", err)
	}

	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-startup-pretrim-inventory.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

args_log=%q
probe=%q

printf 'args=%%s\n' "$*" >> "$args_log"
cat "$probe"
sleep 1
`, argsLogPath, probePath))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	session, err := startFFmpeg(
		ctx,
		ffmpegPath,
		"https://example.test/live/stream.m3u8",
		"ffmpeg-copy",
		2*time.Second,
		1,
		1,
		1,
		16*1024,
		100*time.Millisecond,
		false,
		0,
		-1,
		"",
		true,
	)
	if err != nil {
		t.Fatalf("startFFmpeg() error = %v", err)
	}
	defer session.close()

	if !session.startupProbeRA {
		t.Fatal("startupProbeRA = false, want true")
	}
	if session.startupProbeCodec != "h264" {
		t.Fatalf("startupProbeCodec = %q, want h264", session.startupProbeCodec)
	}
	if got := session.startupInventory.componentState(); got != "video_audio" {
		t.Fatalf("startupInventory.componentState = %q, want video_audio", got)
	}
	if session.startupRetryRelaxedProbe {
		t.Fatal("startupRetryRelaxedProbe = true, want false")
	}

	argsRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("ReadFile(argsLogPath) error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(argsRaw)), "\n")
	if len(lines) != 1 {
		t.Fatalf("len(argsLogLines) = %d, want 1; lines=%q", len(lines), lines)
	}
}

func TestStartFFmpegSkipsRelaxedRetryWhenStartupTimeoutBudgetIsNearlyExhausted(t *testing.T) {
	tmp := t.TempDir()
	statePath := filepath.Join(tmp, "attempt.txt")
	argsLogPath := filepath.Join(tmp, "args.log")
	videoOnlyPath := filepath.Join(tmp, "video_only.ts")

	videoOnlyProbe := startupTestProbeWithPMTStreams(0x1B)
	if err := os.WriteFile(videoOnlyPath, videoOnlyProbe, 0o644); err != nil {
		t.Fatalf("WriteFile(videoOnlyPath) error = %v", err)
	}

	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-startup-retry-time-budget.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

state=%q
args_log=%q
video_only=%q

attempt=0
if [[ -f "$state" ]]; then
  attempt=$(cat "$state")
fi
attempt=$((attempt+1))
echo "$attempt" > "$state"
printf 'attempt=%%s args=%%s\n' "$attempt" "$*" >> "$args_log"

# Consume most of startup timeout budget so relaxed retry should be skipped.
sleep 0.30
cat "$video_only"
sleep 1
`, statePath, argsLogPath, videoOnlyPath))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := startFFmpeg(
		ctx,
		ffmpegPath,
		"https://example.test/live/stream.m3u8",
		"ffmpeg-copy",
		500*time.Millisecond,
		len(videoOnlyProbe),
		1,
		1,
		16*1024,
		100*time.Millisecond,
		false,
		0,
		-1,
		"",
		false,
	)
	if err == nil {
		t.Fatal("startFFmpeg() error = nil, want startup failure for incomplete inventory")
	}
	if !strings.Contains(err.Error(), "missing required audio+video components") {
		t.Fatalf("startFFmpeg() error = %q, want strict component requirement detail", err.Error())
	}

	argsRaw, err := os.ReadFile(argsLogPath)
	if err != nil {
		t.Fatalf("ReadFile(argsLogPath) error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(argsRaw)), "\n")
	if len(lines) != 1 {
		t.Fatalf("len(argsLogLines) = %d, want 1; lines=%q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "-probesize 128000") {
		t.Fatalf("first attempt args = %q, want normalized -probesize 128000", lines[0])
	}
	if !strings.Contains(lines[0], "-analyzeduration 1000000") {
		t.Fatalf("first attempt args = %q, want normalized -analyzeduration 1000000", lines[0])
	}
	if strings.Contains(lines[0], "2000000") || strings.Contains(lines[0], "3000000") {
		t.Fatalf("first attempt args unexpectedly include relaxed retry probe/analyze settings: %q", lines[0])
	}
}

func TestStartFFmpegStderrCaptureIsBounded(t *testing.T) {
	const tailToken = "tail-diagnostic-token-expected"
	tmp := t.TempDir()
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-stderr-heavy.sh", `#!/usr/bin/env bash
set -euo pipefail
for i in $(seq 1 4096); do
  printf 'noise-line-%05d this is intentionally verbose stderr output for bounded capture testing\n' "$i" >&2
done
echo "`+tailToken+`" >&2
exit 1
`)

	_, err := startFFmpeg(
		context.Background(),
		ffmpegPath,
		"https://example.test/live/stream.m3u8",
		"ffmpeg-copy",
		300*time.Millisecond,
		1,
		1,
		1,
		32*1024,
		200*time.Millisecond,
		true,
		2*time.Second,
		-1,
		"404,429,5xx",
		false,
	)
	if err == nil {
		t.Fatal("startFFmpeg() error = nil, want startup failure")
	}

	msg := err.Error()
	if !strings.Contains(msg, tailToken) {
		t.Fatalf("startFFmpeg() error = %q, want stderr tail token", msg)
	}
	if strings.Contains(msg, "noise-line-00001") {
		t.Fatalf("startFFmpeg() error unexpectedly retained oldest stderr output")
	}
	if !strings.Contains(msg, "[stderr truncated to last ") {
		t.Fatalf("startFFmpeg() error = %q, want truncation marker", msg)
	}
	if got, max := len(msg), ffmpegStreamStderrCaptureMax+2*1024; got > max {
		t.Fatalf("startFFmpeg() error length = %d, want <= %d", got, max)
	}
}

func TestStartupProbeHasRandomAccessH264(t *testing.T) {
	videoPES := startupTestPESPackets(
		0x0101,
		0x00,
		0xE0,
		[]byte{
			0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f, // SPS
			0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x06, 0xe2, // PPS
			0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, // IDR
		},
	)

	ready, codec := startupProbeHasRandomAccess(videoPES)
	if !ready {
		t.Fatal("startupProbeHasRandomAccess() = false, want true")
	}
	if codec != "h264" {
		t.Fatalf("startupProbeHasRandomAccess() codec = %q, want h264", codec)
	}
}

func TestStartupProbeHasRandomAccessHEVC(t *testing.T) {
	videoPES := startupTestPESPackets(
		0x0101,
		0x00,
		0xE0,
		[]byte{
			0x00, 0x00, 0x01, 0x40, 0x01, 0x0c, // VPS (type 32)
			0x00, 0x00, 0x01, 0x42, 0x01, 0x01, // SPS (type 33)
			0x00, 0x00, 0x01, 0x44, 0x01, 0xc0, // PPS (type 34)
			0x00, 0x00, 0x01, 0x26, 0x01, 0xa0, // IDR_W_RADL (type 19)
		},
	)

	ready, codec := startupProbeHasRandomAccess(videoPES)
	if !ready {
		t.Fatal("startupProbeHasRandomAccess() = false, want true")
	}
	if codec != "hevc" {
		t.Fatalf("startupProbeHasRandomAccess() codec = %q, want hevc", codec)
	}
}

func TestStartupProbeRandomAccessInfoUsesPESStartCutover(t *testing.T) {
	filler := bytes.Repeat([]byte{0xFF}, 175)
	videoPES := startupTestPESPackets(
		0x0101,
		0x00,
		0xE0,
		append(filler,
			0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f, // SPS
			0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x06, 0xe2, // PPS
			0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, // IDR
		),
	)

	marker, ok := startupProbeRandomAccessInfo(videoPES)
	if !ok {
		t.Fatal("startupProbeRandomAccessInfo() = false, want true")
	}
	if marker.codec != "h264" {
		t.Fatalf("startupProbeRandomAccessInfo() codec = %q, want h264", marker.codec)
	}
	if marker.cutoverOffset != 0 {
		t.Fatalf(
			"startupProbeRandomAccessInfo() cutover = %d, want 0 (PES start packet offset)",
			marker.cutoverOffset,
		)
	}
}

func TestStartupProbeHasRandomAccessRequiresParameterSets(t *testing.T) {
	videoPES := startupTestPESPackets(
		0x0101,
		0x00,
		0xE0,
		[]byte{
			0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, // H.264 IDR without SPS/PPS
		},
	)

	ready, codec := startupProbeHasRandomAccess(videoPES)
	if ready {
		t.Fatalf("startupProbeHasRandomAccess() ready = true, want false (codec=%q)", codec)
	}
}

func TestReadStartupProbeWithRandomAccessTrimsToTSCutover(t *testing.T) {
	preIDRPES := startupTestPESPackets(
		0x0101,
		0x00,
		0xE0,
		[]byte{
			0x00, 0x00, 0x01, 0x41, 0x9a, 0x01, // non-IDR slice
		},
	)
	randomAccessPES := startupTestPESPackets(
		0x0101,
		0x01,
		0xE0,
		[]byte{
			0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f, // SPS
			0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x06, 0xe2, // PPS
			0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, // IDR
		},
	)
	probe := append(append([]byte{}, preIDRPES...), randomAccessPES...)

	trimmed, ready, codec, inventoryProbe, telemetry, err := readStartupProbeWithRandomAccess(
		context.Background(),
		bytes.NewReader(probe),
		1,
		time.Second,
		true,
		nil,
	)
	if err != nil {
		t.Fatalf("readStartupProbeWithRandomAccess() error = %v", err)
	}
	if !ready {
		t.Fatal("readStartupProbeWithRandomAccess() randomAccessReady = false, want true")
	}
	if codec != "h264" {
		t.Fatalf("readStartupProbeWithRandomAccess() codec = %q, want h264", codec)
	}
	if len(trimmed) >= len(probe) {
		t.Fatalf("len(trimmed) = %d, want < %d after cutover trim", len(trimmed), len(probe))
	}
	if !bytes.Equal(trimmed, randomAccessPES) {
		t.Fatalf("trimmed startup probe does not match expected random-access packet sequence")
	}
	if !bytes.Equal(inventoryProbe, probe) {
		t.Fatalf("inventory probe does not preserve pre-cutover bytes")
	}
	if got, want := telemetry.rawBytes, len(probe); got != want {
		t.Fatalf("telemetry.raw_bytes = %d, want %d", got, want)
	}
	if got, want := telemetry.trimmedBytes, len(randomAccessPES); got != want {
		t.Fatalf("telemetry.trimmed_bytes = %d, want %d", got, want)
	}
	if got, want := telemetry.cutoverOffset, len(preIDRPES); got != want {
		t.Fatalf("telemetry.cutover_offset = %d, want %d", got, want)
	}
	if got, want := telemetry.droppedBytes, len(preIDRPES); got != want {
		t.Fatalf("telemetry.dropped_bytes = %d, want %d", got, want)
	}
}

func TestReadStartupProbeWithRandomAccessTelemetryZeroCutover(t *testing.T) {
	randomAccessPES := startupTestPESPackets(
		0x0101,
		0x01,
		0xE0,
		[]byte{
			0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f, // SPS
			0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x06, 0xe2, // PPS
			0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, // IDR
		},
	)

	trimmed, ready, codec, inventoryProbe, telemetry, err := readStartupProbeWithRandomAccess(
		context.Background(),
		bytes.NewReader(randomAccessPES),
		1,
		time.Second,
		true,
		nil,
	)
	if err != nil {
		t.Fatalf("readStartupProbeWithRandomAccess() error = %v", err)
	}
	if !ready {
		t.Fatal("readStartupProbeWithRandomAccess() randomAccessReady = false, want true")
	}
	if codec != "h264" {
		t.Fatalf("readStartupProbeWithRandomAccess() codec = %q, want h264", codec)
	}
	if !bytes.Equal(trimmed, randomAccessPES) {
		t.Fatalf("trimmed startup probe does not match expected random-access payload")
	}
	if !bytes.Equal(inventoryProbe, randomAccessPES) {
		t.Fatalf("inventory probe does not preserve original bytes")
	}
	if got, want := telemetry.rawBytes, len(randomAccessPES); got != want {
		t.Fatalf("telemetry.raw_bytes = %d, want %d", got, want)
	}
	if got, want := telemetry.trimmedBytes, len(randomAccessPES); got != want {
		t.Fatalf("telemetry.trimmed_bytes = %d, want %d", got, want)
	}
	if got := telemetry.cutoverOffset; got != 0 {
		t.Fatalf("telemetry.cutover_offset = %d, want 0", got)
	}
	if got := telemetry.droppedBytes; got != 0 {
		t.Fatalf("telemetry.dropped_bytes = %d, want 0", got)
	}
}

func TestReadStartupProbeWithRandomAccessTelemetryNearProbeLimit(t *testing.T) {
	preIDRPES := startupTestPESPackets(
		0x0101,
		0x00,
		0xE0,
		[]byte{
			0x00, 0x00, 0x01, 0x41, 0x9a, 0x01, // non-IDR slice
		},
	)
	randomAccessPES := startupTestPESPackets(
		0x0101,
		0x01,
		0xE0,
		[]byte{
			0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f, // SPS
			0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x06, 0xe2, // PPS
			0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, // IDR
		},
	)

	targetRawBytes := (startupRandomAccessProbeMaxBytes * 3) / 4
	if targetRawBytes <= len(randomAccessPES) {
		targetRawBytes = len(randomAccessPES) + len(preIDRPES)
	}
	preCutover := make([]byte, 0, targetRawBytes-len(randomAccessPES))
	for len(preCutover)+len(preIDRPES)+len(randomAccessPES) < targetRawBytes {
		preCutover = append(preCutover, preIDRPES...)
	}
	probe := append(append([]byte{}, preCutover...), randomAccessPES...)

	trimmed, ready, codec, _, telemetry, err := readStartupProbeWithRandomAccess(
		context.Background(),
		bytes.NewReader(probe),
		1,
		30*time.Second,
		true,
		nil,
	)
	if err != nil {
		t.Fatalf("readStartupProbeWithRandomAccess() error = %v", err)
	}
	if !ready {
		t.Fatal("readStartupProbeWithRandomAccess() randomAccessReady = false, want true")
	}
	if codec != "h264" {
		t.Fatalf("readStartupProbeWithRandomAccess() codec = %q, want h264", codec)
	}
	if !bytes.Equal(trimmed, randomAccessPES) {
		t.Fatalf("trimmed startup probe does not match expected random-access packet sequence")
	}
	if got, want := telemetry.rawBytes, len(probe); got != want {
		t.Fatalf("telemetry.raw_bytes = %d, want %d", got, want)
	}
	if got, want := telemetry.trimmedBytes, len(randomAccessPES); got != want {
		t.Fatalf("telemetry.trimmed_bytes = %d, want %d", got, want)
	}
	if got, want := telemetry.cutoverOffset, len(preCutover); got != want {
		t.Fatalf("telemetry.cutover_offset = %d, want %d", got, want)
	}
	if got, want := telemetry.droppedBytes, len(preCutover); got != want {
		t.Fatalf("telemetry.dropped_bytes = %d, want %d", got, want)
	}
	if got := telemetry.cutoverPercent(); got < 75 {
		t.Fatalf("telemetry.cutover_percent = %d, want >= 75", got)
	}
}

func TestStartupProbeHasRandomAccessIgnoresNonVideoPES(t *testing.T) {
	audioPESWithAnnexBMarkers := startupTestPESPackets(
		0x0201,
		0x00,
		0xC0,
		[]byte{
			0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f, // fake SPS in audio PES
			0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x06, 0xe2, // fake PPS in audio PES
			0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, // fake IDR in audio PES
		},
	)
	videoPESWithoutIDR := startupTestPESPackets(
		0x0101,
		0x00,
		0xE0,
		[]byte{
			0x00, 0x00, 0x01, 0x41, 0x9a, 0x01, // non-IDR slice only
		},
	)
	probe := append(audioPESWithAnnexBMarkers, videoPESWithoutIDR...)

	ready, codec := startupProbeHasRandomAccess(probe)
	if ready {
		t.Fatalf("startupProbeHasRandomAccess() ready = true, want false (codec=%q)", codec)
	}
}

func TestStartupProbeHasRandomAccessRejectsRawAnnexB(t *testing.T) {
	raw := []byte{
		0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1f, // SPS
		0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x06, 0xe2, // PPS
		0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, // IDR
	}

	ready, codec := startupProbeHasRandomAccess(raw)
	if ready {
		t.Fatalf("startupProbeHasRandomAccess() ready = true for raw Annex-B bytes (codec=%q), want false", codec)
	}
}

type startupTestPMTStream struct {
	streamType byte
	pid        uint16
}

func startupTestProbeWithPMTStreams(streamTypes ...byte) []byte {
	const (
		programNumber = uint16(1)
		pmtPID        = uint16(0x1000)
		pcrPID        = uint16(0x0101)
	)

	streams := make([]startupTestPMTStream, 0, len(streamTypes))
	nextPID := uint16(0x0101)
	for _, streamType := range streamTypes {
		streams = append(streams, startupTestPMTStream{
			streamType: streamType,
			pid:        nextPID,
		})
		nextPID++
	}

	patPayload := append([]byte{0x00}, mpegTSPATSectionWithVersion(programNumber, pmtPID, 0)...)
	pmtPayload := append([]byte{0x00}, startupTestPMTSection(programNumber, pcrPID, streams)...)

	probe := make([]byte, 0, mpegTSPacketSize*2)
	probe = append(probe, mpegTSPayloadPacket(0x0000, 0x00, patPayload, true)...)
	probe = append(probe, mpegTSPayloadPacket(pmtPID, 0x00, pmtPayload, true)...)
	return probe
}

func startupTestPMTSection(programNumber uint16, pcrPID uint16, streams []startupTestPMTStream) []byte {
	sectionLength := 9 + (5 * len(streams)) + 4
	section := []byte{
		0x02, // table_id (PMT)
		0xB0 | byte((sectionLength>>8)&0x0F),
		byte(sectionLength),
		byte((programNumber >> 8) & 0xFF),
		byte(programNumber & 0xFF),
		0xC1, // version 0 + current_next=1
		0x00, // section_number
		0x00, // last_section_number
		0xE0 | byte((pcrPID>>8)&0x1F),
		byte(pcrPID & 0xFF),
		0xF0, 0x00, // program_info_length = 0
	}
	for _, stream := range streams {
		section = append(section,
			stream.streamType,
			0xE0|byte((stream.pid>>8)&0x1F),
			byte(stream.pid&0xFF),
			0xF0, 0x00, // ES_info_length = 0
		)
	}
	crc := mpegTSCRC32(section)
	return append(section, byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc))
}

func startupTestPESPackets(pid uint16, continuity byte, streamID byte, elementary []byte) []byte {
	pes := make([]byte, 0, 9+len(elementary))
	pes = append(pes,
		0x00, 0x00, 0x01, streamID,
		0x00, 0x00, // PES packet length unknown
		0x80, // marker bits
		0x00, // flags
		0x00, // header data length
	)
	pes = append(pes, elementary...)

	const tsPayloadMax = mpegTSPacketSize - 4
	out := make([]byte, 0)
	payloadUnitStart := true
	for len(pes) > 0 {
		chunkSize := tsPayloadMax
		if chunkSize > len(pes) {
			chunkSize = len(pes)
		}
		chunk := pes[:chunkSize]
		pes = pes[chunkSize:]
		packet := mpegTSPayloadPacket(pid, continuity&0x0F, chunk, payloadUnitStart)
		out = append(out, packet...)
		continuity = (continuity + 1) & 0x0F
		payloadUnitStart = false
	}
	return out
}

type delayedStartupResponseTransport struct {
	delay       time.Duration
	bodyClosedC chan struct{}
	closeOnce   sync.Once
}

func newDelayedStartupResponseTransport(delay time.Duration) *delayedStartupResponseTransport {
	return &delayedStartupResponseTransport{
		delay:       delay,
		bodyClosedC: make(chan struct{}),
	}
}

func (t *delayedStartupResponseTransport) RoundTrip(_ *http.Request) (*http.Response, error) {
	time.Sleep(t.delay)
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body: &callbackReadCloser{
			Reader: strings.NewReader("late-startup-body"),
			onClose: func() {
				t.closeOnce.Do(func() {
					close(t.bodyClosedC)
				})
			},
		},
	}, nil
}

func (t *delayedStartupResponseTransport) waitForBodyClose(timeout time.Duration) bool {
	select {
	case <-t.bodyClosedC:
		return true
	case <-time.After(timeout):
		return false
	}
}

type callbackReadCloser struct {
	io.Reader
	onClose func()
}

func (r *callbackReadCloser) Close() error {
	if r.onClose != nil {
		r.onClose()
	}
	return nil
}

func writeExecutable(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
	return path
}

func waitForStableGoroutines() int {
	runtime.GC()
	runtime.Gosched()
	time.Sleep(80 * time.Millisecond)
	return runtime.NumGoroutine()
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type zeroThenDataReadCloser struct {
	mu    sync.Mutex
	calls int
}

func (r *zeroThenDataReadCloser) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.calls {
	case 0:
		r.calls++
		return 0, nil
	case 1:
		r.calls++
		return copy(p, []byte("ok")), nil
	default:
		return 0, io.EOF
	}
}

func (*zeroThenDataReadCloser) Close() error { return nil }

type zeroThenPartialDataReadCloser struct {
	mu    sync.Mutex
	calls int
}

func (r *zeroThenPartialDataReadCloser) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.calls {
	case 0:
		r.calls++
		return 0, nil
	case 1:
		r.calls++
		return copy(p, []byte("o")), nil
	case 2:
		r.calls++
		return copy(p, []byte("k")), io.EOF
	default:
		return 0, io.EOF
	}
}

func (*zeroThenPartialDataReadCloser) Close() error { return nil }

type alwaysZeroNilReadCloser struct{}

func (*alwaysZeroNilReadCloser) Read([]byte) (int, error) { return 0, nil }
func (*alwaysZeroNilReadCloser) Close() error             { return nil }

type closeCountingBlockingReadCloser struct {
	mu         sync.Mutex
	releaseCh  chan struct{}
	readDoneCh chan struct{}
	readDone   sync.Once
	closeCalls int
	released   bool
}

func newCloseCountingBlockingReadCloser() *closeCountingBlockingReadCloser {
	return &closeCountingBlockingReadCloser{
		releaseCh:  make(chan struct{}),
		readDoneCh: make(chan struct{}),
	}
}

func (r *closeCountingBlockingReadCloser) Read([]byte) (int, error) {
	<-r.releaseCh
	r.readDone.Do(func() { close(r.readDoneCh) })
	return 0, io.EOF
}

func (r *closeCountingBlockingReadCloser) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closeCalls++
	if !r.released {
		close(r.releaseCh)
		r.released = true
	}
	return nil
}

func (r *closeCountingBlockingReadCloser) closeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeCalls
}

func (r *closeCountingBlockingReadCloser) waitForReadDone(timeout time.Duration) bool {
	select {
	case <-r.readDoneCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

func TestReadBodyPreviewBoundedNilReaderAndNonPositiveMaxBytes(t *testing.T) {
	if got := readBodyPreviewBounded(context.Background(), nil, 128); got != nil {
		t.Fatalf("readBodyPreviewBounded(nil) = %q, want nil", string(got))
	}

	body := io.NopCloser(strings.NewReader("ignored"))
	if got := readBodyPreviewBounded(context.Background(), body, 0); got != nil {
		t.Fatalf("readBodyPreviewBounded(maxBytes=0) = %q, want nil", string(got))
	}
	if got := readBodyPreviewBounded(context.Background(), body, -1); got != nil {
		t.Fatalf("readBodyPreviewBounded(maxBytes=-1) = %q, want nil", string(got))
	}
}

// Issue: outstanding-todo commit review 281cec4 M1.
func TestReadBodyPreviewBoundedRetriesAfterZeroNilRead(t *testing.T) {
	reader := &zeroThenDataReadCloser{}

	got := readBodyPreviewBounded(context.Background(), reader, 2)
	if string(got) != "ok" {
		t.Fatalf("readBodyPreviewBounded() = %q, want %q", string(got), "ok")
	}
}

func TestReadBodyPreviewBoundedContinuesAfterZeroProgressPartialReads(t *testing.T) {
	reader := &zeroThenPartialDataReadCloser{}

	got := readBodyPreviewBounded(context.Background(), reader, 4)
	if string(got) != "ok" {
		t.Fatalf("readBodyPreviewBounded() = %q, want %q", string(got), "ok")
	}
}

func TestReadBodyPreviewBoundedAlwaysZeroNilReadRespectsContextCancel(t *testing.T) {
	reader := &alwaysZeroNilReadCloser{}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	started := time.Now()
	got := readBodyPreviewBounded(ctx, reader, 16)
	elapsed := time.Since(started)

	if len(got) != 0 {
		t.Fatalf("readBodyPreviewBounded() length = %d, want 0", len(got))
	}
	if elapsed < 15*time.Millisecond {
		t.Fatalf("readBodyPreviewBounded() elapsed = %s, want >= 15ms", elapsed)
	}
	if elapsed > 350*time.Millisecond {
		t.Fatalf("readBodyPreviewBounded() elapsed = %s, want < 350ms", elapsed)
	}
}

// Issue: outstanding-todo commit review 281cec4 M4.
func TestReadBodyPreviewBoundedHugeMaxBytesDoesNotRequireHugeInput(t *testing.T) {
	reader := io.NopCloser(strings.NewReader("abc"))
	maxBytes := int64(^uint(0) >> 1)

	got := readBodyPreviewBounded(context.Background(), reader, maxBytes)
	if string(got) != "abc" {
		t.Fatalf("readBodyPreviewBounded() = %q, want %q", string(got), "abc")
	}
}

func TestReadBodyPreviewBoundedTruncatesToSmallMaxBytes(t *testing.T) {
	reader := io.NopCloser(strings.NewReader("hello"))

	got := readBodyPreviewBounded(context.Background(), reader, 2)
	if string(got) != "he" {
		t.Fatalf("readBodyPreviewBounded() = %q, want %q", string(got), "he")
	}
}

// Issue: outstanding-todo commit review 281cec4 M6.
func TestReadBodyPreviewBoundedCancelDoesNotCloseReader(t *testing.T) {
	reader := newCloseCountingBlockingReadCloser()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	started := time.Now()
	got := readBodyPreviewBounded(ctx, reader, 64)
	elapsed := time.Since(started)

	if len(got) != 0 {
		t.Fatalf("readBodyPreviewBounded() length = %d, want 0", len(got))
	}
	if elapsed < 15*time.Millisecond {
		t.Fatalf("readBodyPreviewBounded() elapsed = %s, want >= 15ms", elapsed)
	}
	if elapsed > 350*time.Millisecond {
		t.Fatalf("readBodyPreviewBounded() elapsed = %s, want < 350ms", elapsed)
	}
	if reader.closeCount() != 0 {
		t.Fatalf("reader close count = %d, want 0 (caller-owned close)", reader.closeCount())
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("reader.Close() error = %v", err)
	}
	if !reader.waitForReadDone(250 * time.Millisecond) {
		t.Fatal("reader read goroutine did not exit after caller-owned close")
	}
}

// TestCloseWithTimeoutRepeatedBlockingCloseAttemptsAreBounded verifies that
// helper-level closeWithTimeout does not create unbounded blocked close workers
// when the same closer keeps Close() blocked repeatedly.
//
// Issue: TODO-stream-close-with-timeout-detached-goroutine-accumulation.md
func TestCloseWithTimeoutRepeatedBlockingCloseAttemptsAreBounded(t *testing.T) {
	body := newStartupAbortBlockingBody()
	defer body.release()

	before := waitForStableGoroutines()

	const attempts = boundedCloseWorkerBudget + 8
	for i := 0; i < attempts; i++ {
		go closeWithTimeout(body, boundedCloseTimeout)
	}

	after := waitForStableGoroutines()
	if after > before+boundedCloseWorkerBudget {
		t.Fatalf(
			"detached close worker growth exceeded budget after repeated attempts: before=%d after=%d budget=%d",
			before,
			after,
			boundedCloseWorkerBudget,
		)
	}
}

func TestCloseWithTimeoutTracksSuppressedAndLateCompletionEvents(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	bodyA := newStartupAbortBlockingBody()
	defer bodyA.release()

	bodyB := newStartupAbortBlockingBody()
	defer bodyB.release()

	closeWithTimeout(bodyA, 10*time.Millisecond)
	closeWithTimeout(bodyA, 10*time.Millisecond) // suppressed for same closer
	closeWithTimeout(bodyB, 10*time.Millisecond)

	// Both close attempts should have timed out by now.
	time.Sleep(40 * time.Millisecond)
	stats := closeWithTimeoutStatsSnapshot()
	if stats.Started != 2 {
		t.Fatalf("stats.Started = %d, want 2", stats.Started)
	}
	if stats.Suppressed != 1 {
		t.Fatalf("stats.Suppressed = %d, want 1", stats.Suppressed)
	}
	if stats.Timeouts != 2 {
		t.Fatalf("stats.Timeouts = %d, want 2", stats.Timeouts)
	}
	if stats.LateCompletions != 0 {
		t.Fatalf("stats.LateCompletions = %d, want 0 before release", stats.LateCompletions)
	}

	bodyA.release()
	bodyB.release()
	time.Sleep(80 * time.Millisecond)

	stats = closeWithTimeoutStatsSnapshot()
	if stats.LateCompletions != 2 {
		t.Fatalf("stats.LateCompletions = %d, want 2 after release", stats.LateCompletions)
	}
}

func TestCloseWithTimeoutLateReleaseFreesWorkerSlotAndKeepsDedup(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	const closeTimeout = 10 * time.Millisecond
	const lateReleaseTimeout = 30 * time.Millisecond

	blocked := newStartupAbortBlockingBody()
	t.Cleanup(blocked.release)
	if got := closeWithTimeoutStartWorker(blocked); got != closeWithTimeoutStartSuccess {
		t.Fatalf("closeWithTimeoutStartWorker(blocked) = %v, want %v", got, closeWithTimeoutStartSuccess)
	}

	go closeWithTimeoutRunWorkerWithLateCompletionTimeout(blocked, closeTimeout, lateReleaseTimeout)

	select {
	case <-blocked.closeStartedCh:
	case <-time.After(time.Second):
		t.Fatal("blocked close did not start within 1s")
	}

	waitFor(t, time.Second, func() bool {
		stats := closeWithTimeoutStatsSnapshot()
		return stats.Started == 1 && stats.Timeouts == 1
	})

	waitFor(t, time.Second, func() bool {
		return len(closeWithTimeoutWorkers) == 0
	})

	blockedKey := closeWithTimeoutCloserKey(blocked)
	closeWithTimeoutInFlightMu.Lock()
	_, stillInFlight := closeWithTimeoutInFlightByCloser[blockedKey]
	closeWithTimeoutInFlightMu.Unlock()
	if !stillInFlight {
		t.Fatal("blocked closer should remain deduped in-flight until Close returns")
	}

	closeWithTimeout(blocked, closeTimeout)
	waitFor(t, time.Second, func() bool {
		stats := closeWithTimeoutStatsSnapshot()
		return stats.Started == 1 && stats.Suppressed >= 1
	})

	other := newStartupAbortBlockingBody()
	t.Cleanup(other.release)
	closeWithTimeout(other, 200*time.Millisecond)
	select {
	case <-other.closeStartedCh:
	case <-time.After(time.Second):
		t.Fatal("other closer did not start after late-release freed worker slot")
	}
	other.release()
	waitFor(t, time.Second, func() bool {
		return closeWithTimeoutStatsSnapshot().Started == 2
	})

	blocked.release()
	waitFor(t, time.Second, func() bool {
		closeWithTimeoutInFlightMu.Lock()
		_, exists := closeWithTimeoutInFlightByCloser[blockedKey]
		closeWithTimeoutInFlightMu.Unlock()
		return !exists && closeWithTimeoutStatsSnapshot().LateCompletions >= 1
	})
}

func TestCloseWithTimeoutLateReleaseAbandonClearsInFlightForStuckClose(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	const closeTimeout = 10 * time.Millisecond
	const lateReleaseTimeout = 20 * time.Millisecond
	const lateAbandonTimeout = 30 * time.Millisecond

	blocked := newStartupAbortBlockingBody()
	t.Cleanup(blocked.release)
	if got := closeWithTimeoutStartWorker(blocked); got != closeWithTimeoutStartSuccess {
		t.Fatalf("closeWithTimeoutStartWorker(blocked) = %v, want %v", got, closeWithTimeoutStartSuccess)
	}

	go closeWithTimeoutRunWorkerWithLateCompletionTimeouts(
		blocked,
		closeTimeout,
		lateReleaseTimeout,
		lateAbandonTimeout,
	)

	select {
	case <-blocked.closeStartedCh:
	case <-time.After(time.Second):
		t.Fatal("blocked close did not start within 1s")
	}

	waitFor(t, time.Second, func() bool {
		stats := closeWithTimeoutStatsSnapshot()
		return stats.Started == 1 && stats.Timeouts == 1
	})

	waitFor(t, time.Second, func() bool {
		return len(closeWithTimeoutWorkers) == 0
	})

	waitFor(t, time.Second, func() bool {
		stats := closeWithTimeoutStatsSnapshot()
		return stats.LateAbandoned == 1
	})

	blockedKey := closeWithTimeoutCloserKey(blocked)
	waitFor(t, time.Second, func() bool {
		closeWithTimeoutInFlightMu.Lock()
		_, inFlight := closeWithTimeoutInFlightByCloser[blockedKey]
		closeWithTimeoutInFlightMu.Unlock()
		return !inFlight
	})

	stats := closeWithTimeoutStatsSnapshot()
	if stats.LateCompletions != 0 {
		t.Fatalf("stats.LateCompletions = %d, want 0 before blocked close is released", stats.LateCompletions)
	}
	if stats.ReleaseUnderflow != 0 {
		t.Fatalf("stats.ReleaseUnderflow = %d, want 0", stats.ReleaseUnderflow)
	}
}

func TestCloseWithTimeoutAwaitLateCompletionAfterReleaseNoAbandonWaitsForDone(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	blocked := newStartupAbortBlockingBody()
	t.Cleanup(blocked.release)

	blockedKey := closeWithTimeoutCloserKey(blocked)
	closeWithTimeoutInFlightMu.Lock()
	closeWithTimeoutInFlightByCloser[blockedKey] = struct{}{}
	closeWithTimeoutInFlightMu.Unlock()

	done := make(chan struct{})
	awaitFinished := make(chan struct{})
	go func() {
		closeWithTimeoutAwaitLateCompletionAfterRelease(blocked, done, 0)
		close(awaitFinished)
	}()

	select {
	case <-awaitFinished:
		t.Fatal("closeWithTimeoutAwaitLateCompletionAfterRelease returned before done closed")
	case <-time.After(50 * time.Millisecond):
		// expected: lateAbandonTimeout<=0 path blocks until done.
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.LateCompletions != 0 {
		t.Fatalf("stats.LateCompletions = %d, want 0 before done closed", stats.LateCompletions)
	}
	if stats.LateAbandoned != 0 {
		t.Fatalf("stats.LateAbandoned = %d, want 0 before done closed", stats.LateAbandoned)
	}
	if stats.ReleaseUnderflow != 0 {
		t.Fatalf("stats.ReleaseUnderflow = %d, want 0", stats.ReleaseUnderflow)
	}

	closeWithTimeoutInFlightMu.Lock()
	_, inFlight := closeWithTimeoutInFlightByCloser[blockedKey]
	closeWithTimeoutInFlightMu.Unlock()
	if !inFlight {
		t.Fatal("blocked closer unexpectedly cleared from in-flight before done closed")
	}

	close(done)
	waitFor(t, time.Second, func() bool {
		select {
		case <-awaitFinished:
			return true
		default:
			return false
		}
	})

	stats = closeWithTimeoutStatsSnapshot()
	if stats.LateCompletions != 1 {
		t.Fatalf("stats.LateCompletions = %d, want 1 after done closed", stats.LateCompletions)
	}
	if stats.LateAbandoned != 0 {
		t.Fatalf("stats.LateAbandoned = %d, want 0", stats.LateAbandoned)
	}
	if stats.ReleaseUnderflow != 0 {
		t.Fatalf("stats.ReleaseUnderflow = %d, want 0", stats.ReleaseUnderflow)
	}

	closeWithTimeoutInFlightMu.Lock()
	_, inFlight = closeWithTimeoutInFlightByCloser[blockedKey]
	closeWithTimeoutInFlightMu.Unlock()
	if inFlight {
		t.Fatal("blocked closer should be cleared from in-flight after done closed")
	}
}

func TestCloseWithTimeoutAwaitLateCompletionAfterReleaseCompletesBeforeAbandonTimeout(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	const lateAbandonTimeout = 200 * time.Millisecond

	blocked := newStartupAbortBlockingBody()
	t.Cleanup(blocked.release)

	blockedKey := closeWithTimeoutCloserKey(blocked)
	closeWithTimeoutInFlightMu.Lock()
	closeWithTimeoutInFlightByCloser[blockedKey] = struct{}{}
	closeWithTimeoutInFlightMu.Unlock()

	done := make(chan struct{})
	awaitFinished := make(chan struct{})
	go func() {
		closeWithTimeoutAwaitLateCompletionAfterRelease(blocked, done, lateAbandonTimeout)
		close(awaitFinished)
	}()

	select {
	case <-awaitFinished:
		t.Fatal("closeWithTimeoutAwaitLateCompletionAfterRelease returned before done closed")
	case <-time.After(50 * time.Millisecond):
		// expected: waiter should still be waiting for done.
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.LateCompletions != 0 {
		t.Fatalf("stats.LateCompletions = %d, want 0 before done closed", stats.LateCompletions)
	}
	if stats.LateAbandoned != 0 {
		t.Fatalf("stats.LateAbandoned = %d, want 0 before done closed", stats.LateAbandoned)
	}

	close(done)
	waitFor(t, time.Second, func() bool {
		select {
		case <-awaitFinished:
			return true
		default:
			return false
		}
	})

	stats = closeWithTimeoutStatsSnapshot()
	if stats.LateCompletions != 1 {
		t.Fatalf("stats.LateCompletions = %d, want 1 after done closed", stats.LateCompletions)
	}
	if stats.LateAbandoned != 0 {
		t.Fatalf("stats.LateAbandoned = %d, want 0 when done closes before abandon timeout", stats.LateAbandoned)
	}
	if stats.ReleaseUnderflow != 0 {
		t.Fatalf("stats.ReleaseUnderflow = %d, want 0", stats.ReleaseUnderflow)
	}

	closeWithTimeoutInFlightMu.Lock()
	_, inFlight := closeWithTimeoutInFlightByCloser[blockedKey]
	closeWithTimeoutInFlightMu.Unlock()
	if inFlight {
		t.Fatal("blocked closer should be cleared from in-flight after done closed")
	}
}

func TestCloseWithTimeoutRecordFunctionsIncrementPrometheusCounters(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	steps := []struct {
		name      string
		metric    func() float64
		record    func()
		wantDelta float64
	}{
		{
			name: "started",
			metric: func() float64 {
				return testutil.ToFloat64(closeWithTimeoutStartedMetric.WithLabelValues(playlistSourceMetricUnknown))
			},
			record:    closeWithTimeoutRecordStarted,
			wantDelta: 1,
		},
		{
			name: "retried",
			metric: func() float64 {
				return testutil.ToFloat64(closeWithTimeoutRetriedMetric.WithLabelValues(playlistSourceMetricUnknown))
			},
			record:    closeWithTimeoutRecordRetried,
			wantDelta: 1,
		},
		{
			name: "suppressed_total",
			metric: func() float64 {
				return testutil.ToFloat64(closeWithTimeoutSuppressedMetric.WithLabelValues(playlistSourceMetricUnknown))
			},
			record:    closeWithTimeoutRecordSuppressed,
			wantDelta: 1,
		},
		{
			name: "suppressed_duplicate",
			metric: func() float64 {
				return testutil.ToFloat64(closeWithTimeoutSuppressedDuplicateMetric.WithLabelValues(playlistSourceMetricUnknown))
			},
			record:    closeWithTimeoutRecordSuppressedDuplicate,
			wantDelta: 1,
		},
		{
			name: "suppressed_budget",
			metric: func() float64 {
				return testutil.ToFloat64(closeWithTimeoutSuppressedBudgetMetric.WithLabelValues(playlistSourceMetricUnknown))
			},
			record:    closeWithTimeoutRecordSuppressedBudget,
			wantDelta: 1,
		},
		{
			name: "dropped",
			metric: func() float64 {
				return testutil.ToFloat64(closeWithTimeoutDroppedMetric.WithLabelValues(playlistSourceMetricUnknown))
			},
			record:    closeWithTimeoutRecordDropped,
			wantDelta: 1,
		},
		{
			name: "timed_out",
			metric: func() float64 {
				return testutil.ToFloat64(closeWithTimeoutTimedOutMetric.WithLabelValues(playlistSourceMetricUnknown))
			},
			record:    closeWithTimeoutRecordTimedOut,
			wantDelta: 1,
		},
		{
			name: "late_completion",
			metric: func() float64 {
				return testutil.ToFloat64(closeWithTimeoutLateCompletionsMetric.WithLabelValues(playlistSourceMetricUnknown))
			},
			record:    closeWithTimeoutRecordLateCompletion,
			wantDelta: 1,
		},
		{
			name: "late_abandoned",
			metric: func() float64 {
				return testutil.ToFloat64(closeWithTimeoutLateAbandonedMetric.WithLabelValues(playlistSourceMetricUnknown))
			},
			record:    closeWithTimeoutRecordLateAbandoned,
			wantDelta: 1,
		},
		{
			name: "release_underflow",
			metric: func() float64 {
				return testutil.ToFloat64(closeWithTimeoutReleaseUnderflowMetric.WithLabelValues(playlistSourceMetricUnknown))
			},
			record:    closeWithTimeoutRecordReleaseUnderflow,
			wantDelta: 1,
		},
	}

	for _, step := range steps {
		step := step
		t.Run(step.name, func(t *testing.T) {
			before := step.metric()
			step.record()
			after := step.metric()
			if got := after - before; got != step.wantDelta {
				t.Fatalf("%s metric delta = %.0f, want %.0f", step.name, got, step.wantDelta)
			}
		})
	}
}

func TestCloseWithTimeoutRecordSuppression(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	steps := []struct {
		name             string
		result           closeWithTimeoutStartResult
		countSuppression bool
		wantSuppressed   uint64
		wantDuplicate    uint64
		wantBudget       uint64
	}{
		{
			name:             "count false no-op",
			result:           closeWithTimeoutStartSuppressedDuplicate,
			countSuppression: false,
			wantSuppressed:   0,
			wantDuplicate:    0,
			wantBudget:       0,
		},
		{
			name:             "duplicate suppression increments duplicate and total",
			result:           closeWithTimeoutStartSuppressedDuplicate,
			countSuppression: true,
			wantSuppressed:   1,
			wantDuplicate:    1,
			wantBudget:       0,
		},
		{
			name:             "budget suppression increments budget and total",
			result:           closeWithTimeoutStartSuppressedBudget,
			countSuppression: true,
			wantSuppressed:   2,
			wantDuplicate:    1,
			wantBudget:       1,
		},
		{
			name:             "non-suppressed result is no-op",
			result:           closeWithTimeoutStartSuccess,
			countSuppression: true,
			wantSuppressed:   2,
			wantDuplicate:    1,
			wantBudget:       1,
		},
	}

	for _, step := range steps {
		step := step
		t.Run(step.name, func(t *testing.T) {
			closeWithTimeoutRecordSuppression(step.result, step.countSuppression)
			stats := closeWithTimeoutStatsSnapshot()
			if stats.Suppressed != step.wantSuppressed {
				t.Fatalf("stats.Suppressed = %d, want %d", stats.Suppressed, step.wantSuppressed)
			}
			if stats.SuppressedDuplicate != step.wantDuplicate {
				t.Fatalf("stats.SuppressedDuplicate = %d, want %d", stats.SuppressedDuplicate, step.wantDuplicate)
			}
			if stats.SuppressedBudget != step.wantBudget {
				t.Fatalf("stats.SuppressedBudget = %d, want %d", stats.SuppressedBudget, step.wantBudget)
			}
		})
	}
}

func TestCloseWithTimeoutSuppressionReason(t *testing.T) {
	tests := []struct {
		name      string
		duplicate uint64
		budget    uint64
		want      string
	}{
		{
			name:      "budget",
			duplicate: 0,
			budget:    1,
			want:      "budget",
		},
		{
			name:      "duplicate",
			duplicate: 1,
			budget:    0,
			want:      "duplicate",
		},
		{
			name:      "mixed",
			duplicate: 2,
			budget:    3,
			want:      "mixed",
		},
		{
			name:      "unknown",
			duplicate: 0,
			budget:    0,
			want:      "unknown",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := closeWithTimeoutSuppressionReason(tc.duplicate, tc.budget); got != tc.want {
				t.Fatalf("closeWithTimeoutSuppressionReason(%d, %d) = %q, want %q", tc.duplicate, tc.budget, got, tc.want)
			}
		})
	}
}

func TestCloseWithTimeoutFinishWorkerLogsReleaseUnderflow(t *testing.T) {
	// Do not run t.Parallel(): mutates global slog default.
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	logs := newTestLogBuffer()
	oldDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() {
		slog.SetDefault(oldDefault)
	})

	blocked := newStartupAbortBlockingBody()
	t.Cleanup(blocked.release)
	blockedKey := closeWithTimeoutCloserKey(blocked)
	closeWithTimeoutInFlightMu.Lock()
	closeWithTimeoutInFlightByCloser[blockedKey] = struct{}{}
	closeWithTimeoutInFlightMu.Unlock()

	closeWithTimeoutFinishWorker(blocked)

	logText := logs.String()
	if !strings.Contains(logText, "closeWithTimeout worker slot release underflow") {
		t.Fatalf("logs = %q, want underflow warning message", logText)
	}
	if !strings.Contains(logText, "close_release_underflow=1") {
		t.Fatalf("logs = %q, want close_release_underflow field", logText)
	}
	if !strings.Contains(logText, "close_timeouts=0") {
		t.Fatalf("logs = %q, want close_timeouts field", logText)
	}
	if !strings.Contains(logText, "close_late_completions=0") {
		t.Fatalf("logs = %q, want close_late_completions field", logText)
	}
	if !strings.Contains(logText, "close_late_abandoned=0") {
		t.Fatalf("logs = %q, want close_late_abandoned field", logText)
	}

	closeWithTimeoutInFlightMu.Lock()
	_, inFlight := closeWithTimeoutInFlightByCloser[blockedKey]
	closeWithTimeoutInFlightMu.Unlock()
	if inFlight {
		t.Fatal("blocked closer should be cleared from in-flight after closeWithTimeoutFinishWorker")
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.ReleaseUnderflow != 1 {
		t.Fatalf("stats.ReleaseUnderflow = %d, want 1", stats.ReleaseUnderflow)
	}
}

func TestCloseWithTimeoutFinishWorkerLogsUnderflowWithNonZeroTimeoutAndLateCompletion(t *testing.T) {
	// Do not run t.Parallel(): mutates global slog default.
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	logs := newTestLogBuffer()
	oldDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() {
		slog.SetDefault(oldDefault)
	})

	closeWithTimeoutRecordTimedOut()
	closeWithTimeoutRecordLateCompletion()

	blocked := newStartupAbortBlockingBody()
	t.Cleanup(blocked.release)
	blockedKey := closeWithTimeoutCloserKey(blocked)
	closeWithTimeoutInFlightMu.Lock()
	closeWithTimeoutInFlightByCloser[blockedKey] = struct{}{}
	closeWithTimeoutInFlightMu.Unlock()

	closeWithTimeoutFinishWorker(blocked)

	logText := logs.String()
	if !strings.Contains(logText, "closeWithTimeout worker slot release underflow") {
		t.Fatalf("logs = %q, want underflow warning message", logText)
	}
	if !strings.Contains(logText, "close_timeouts=1") {
		t.Fatalf("logs = %q, want non-zero close_timeouts field", logText)
	}
	if !strings.Contains(logText, "close_late_completions=1") {
		t.Fatalf("logs = %q, want non-zero close_late_completions field", logText)
	}
	if !strings.Contains(logText, "close_late_abandoned=0") {
		t.Fatalf("logs = %q, want close_late_abandoned field", logText)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Timeouts != 1 {
		t.Fatalf("stats.Timeouts = %d, want 1", stats.Timeouts)
	}
	if stats.LateCompletions != 1 {
		t.Fatalf("stats.LateCompletions = %d, want 1", stats.LateCompletions)
	}
	if stats.ReleaseUnderflow != 1 {
		t.Fatalf("stats.ReleaseUnderflow = %d, want 1", stats.ReleaseUnderflow)
	}
}

func TestCloseWithTimeoutFinishWorkerReleasesSlotWithoutUnderflowWarning(t *testing.T) {
	// Do not run t.Parallel(): mutates global slog default.
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	logs := newTestLogBuffer()
	oldDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() {
		slog.SetDefault(oldDefault)
	})

	blocked := newStartupAbortBlockingBody()
	t.Cleanup(blocked.release)
	blockedKey := closeWithTimeoutCloserKey(blocked)

	select {
	case closeWithTimeoutWorkers <- struct{}{}:
	default:
		t.Fatal("failed to seed closeWithTimeoutWorkers with one token")
	}

	closeWithTimeoutInFlightMu.Lock()
	closeWithTimeoutInFlightByCloser[blockedKey] = struct{}{}
	closeWithTimeoutInFlightMu.Unlock()

	closeWithTimeoutFinishWorker(blocked)

	if strings.Contains(logs.String(), "closeWithTimeout worker slot release underflow") {
		t.Fatalf("logs = %q, want no underflow warning on successful worker release", logs.String())
	}

	if len(closeWithTimeoutWorkers) != 0 {
		t.Fatalf("len(closeWithTimeoutWorkers) = %d, want 0 after successful release", len(closeWithTimeoutWorkers))
	}

	closeWithTimeoutInFlightMu.Lock()
	_, inFlight := closeWithTimeoutInFlightByCloser[blockedKey]
	closeWithTimeoutInFlightMu.Unlock()
	if inFlight {
		t.Fatal("blocked closer should be cleared from in-flight after closeWithTimeoutFinishWorker")
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.ReleaseUnderflow != 0 {
		t.Fatalf("stats.ReleaseUnderflow = %d, want 0", stats.ReleaseUnderflow)
	}
}

func TestCloseWithTimeoutReleaseWorkerSlotTracksUnderflow(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	if released := closeWithTimeoutReleaseWorkerSlot(); released {
		t.Fatal("closeWithTimeoutReleaseWorkerSlot() = true, want false on empty worker channel")
	}
	stats := closeWithTimeoutStatsSnapshot()
	if stats.ReleaseUnderflow != 1 {
		t.Fatalf("stats.ReleaseUnderflow = %d, want 1", stats.ReleaseUnderflow)
	}

	select {
	case closeWithTimeoutWorkers <- struct{}{}:
	default:
		t.Fatal("failed to seed closeWithTimeoutWorkers with one token")
	}
	if released := closeWithTimeoutReleaseWorkerSlot(); !released {
		t.Fatal("closeWithTimeoutReleaseWorkerSlot() = false, want true when worker token is present")
	}
	stats = closeWithTimeoutStatsSnapshot()
	if stats.ReleaseUnderflow != 1 {
		t.Fatalf("stats.ReleaseUnderflow = %d, want 1 after successful release", stats.ReleaseUnderflow)
	}
}

func TestResetCloseWithTimeoutStatsForTestClearsInFlightAndWorkerState(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	inFlight := newStartupAbortBlockingBody()
	queued := newStartupAbortBlockingBody()
	t.Cleanup(inFlight.release)
	t.Cleanup(queued.release)

	if got := closeWithTimeoutStartWorker(inFlight); got != closeWithTimeoutStartSuccess {
		t.Fatalf("closeWithTimeoutStartWorker() = %v, want %v", got, closeWithTimeoutStartSuccess)
	}
	closeWithTimeoutQueueRetry(queued, boundedCloseTimeout)

	if len(closeWithTimeoutWorkers) == 0 {
		t.Fatal("closeWithTimeoutWorkers should contain started worker before reset")
	}

	closeWithTimeoutInFlightMu.Lock()
	inFlightCount := len(closeWithTimeoutInFlightByCloser)
	closeWithTimeoutInFlightMu.Unlock()
	if inFlightCount == 0 {
		t.Fatal("closeWithTimeoutInFlightByCloser should contain started closer before reset")
	}

	if stats := closeWithTimeoutStatsSnapshot(); stats.Queued == 0 {
		t.Fatal("closeWithTimeout retry queue should contain queued closer before reset")
	}

	resetCloseWithTimeoutStatsForTest()

	if len(closeWithTimeoutWorkers) != 0 {
		t.Fatalf("len(closeWithTimeoutWorkers) = %d, want 0 after reset", len(closeWithTimeoutWorkers))
	}

	closeWithTimeoutInFlightMu.Lock()
	inFlightCount = len(closeWithTimeoutInFlightByCloser)
	closeWithTimeoutInFlightMu.Unlock()
	if inFlightCount != 0 {
		t.Fatalf("len(closeWithTimeoutInFlightByCloser) = %d, want 0 after reset", inFlightCount)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Queued != 0 {
		t.Fatalf("stats.Queued = %d, want 0 after reset", stats.Queued)
	}
	if stats.Started != 0 ||
		stats.Retried != 0 ||
		stats.Suppressed != 0 ||
		stats.SuppressedDuplicate != 0 ||
		stats.SuppressedBudget != 0 ||
		stats.Dropped != 0 ||
		stats.Timeouts != 0 ||
		stats.LateCompletions != 0 ||
		stats.LateAbandoned != 0 ||
		stats.ReleaseUnderflow != 0 {
		t.Fatalf("stats after reset = %+v, want all counters zero", stats)
	}
}

func TestCloseWithTimeoutSuppressionLogsBudgetPressure(t *testing.T) {
	// Do not run t.Parallel(): mutates global slog default.
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	logs := newTestLogBuffer()
	oldDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() {
		slog.SetDefault(oldDefault)
	})

	budget := cap(closeWithTimeoutWorkers)
	blockers := make([]*startupAbortBlockingBody, 0, budget)
	queued := newStartupAbortBlockingBody()
	t.Cleanup(func() {
		queued.release()
		for _, blocker := range blockers {
			closeWithTimeoutFinishWorker(blocker)
			blocker.release()
		}
	})

	for i := 0; i < budget; i++ {
		blocker := newStartupAbortBlockingBody()
		blockers = append(blockers, blocker)
		if got := closeWithTimeoutStartWorker(blocker); got != closeWithTimeoutStartSuccess {
			t.Fatalf("closeWithTimeoutStartWorker blocker %d = %v, want %v", i, got, closeWithTimeoutStartSuccess)
		}
	}

	closeWithTimeout(queued, 750*time.Millisecond)

	waitFor(t, time.Second, func() bool {
		text := logs.String()
		return strings.Contains(text, "closeWithTimeout suppression observed") &&
			strings.Contains(text, "reason=budget")
	})

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Suppressed != 1 {
		t.Fatalf("stats.Suppressed = %d, want 1", stats.Suppressed)
	}
	if stats.Queued != 1 {
		t.Fatalf("stats.Queued = %d, want 1", stats.Queued)
	}
}

func TestCloseWithTimeoutSuppressionLogsEvery64thEvent(t *testing.T) {
	// Do not run t.Parallel(): mutates global slog default.
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	logs := newTestLogBuffer()
	oldDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() {
		slog.SetDefault(oldDefault)
	})

	budget := cap(closeWithTimeoutWorkers)
	blockers := make([]*startupAbortBlockingBody, 0, budget)
	queued := make([]*startupAbortBlockingBody, 0, closeWithTimeoutSuppressionLogEvery)
	t.Cleanup(func() {
		for _, body := range queued {
			body.release()
		}
		for _, blocker := range blockers {
			closeWithTimeoutFinishWorker(blocker)
			blocker.release()
		}
	})

	for i := 0; i < budget; i++ {
		blocker := newStartupAbortBlockingBody()
		blockers = append(blockers, blocker)
		if got := closeWithTimeoutStartWorker(blocker); got != closeWithTimeoutStartSuccess {
			t.Fatalf("closeWithTimeoutStartWorker blocker %d = %v, want %v", i, got, closeWithTimeoutStartSuccess)
		}
	}

	for i := 0; i < closeWithTimeoutSuppressionLogEvery; i++ {
		body := newStartupAbortBlockingBody()
		queued = append(queued, body)
		closeWithTimeout(body, 750*time.Millisecond)
	}

	text := logs.String()
	if got := strings.Count(text, "closeWithTimeout suppression observed"); got != 2 {
		t.Fatalf("suppression warning count = %d, want 2 (first + 64th)", got)
	}
	if got := strings.Count(text, "reason=budget"); got != 2 {
		t.Fatalf("suppression reason=budget count = %d, want 2", got)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Suppressed != closeWithTimeoutSuppressionLogEvery {
		t.Fatalf("stats.Suppressed = %d, want %d", stats.Suppressed, closeWithTimeoutSuppressionLogEvery)
	}
	if stats.SuppressedBudget != closeWithTimeoutSuppressionLogEvery {
		t.Fatalf("stats.SuppressedBudget = %d, want %d", stats.SuppressedBudget, closeWithTimeoutSuppressionLogEvery)
	}
	if stats.SuppressedDuplicate != 0 {
		t.Fatalf("stats.SuppressedDuplicate = %d, want 0", stats.SuppressedDuplicate)
	}
}

func TestCloseWithTimeoutConcurrentDuplicateCallsCloseOnce(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	beforeStats := closeWithTimeoutStatsSnapshot()

	const attempts = boundedCloseWorkerBudget + 8
	// Keep this timeout intentionally large so duplicate calls return via
	// in-flight suppression and not local timeout expiration.
	const closeTimeout = 5 * time.Second
	body := newStartupAbortBlockingBody()
	defer body.release()

	startCh := make(chan struct{})
	doneCh := make(chan struct{}, attempts)
	var ready sync.WaitGroup
	ready.Add(attempts)
	var wg sync.WaitGroup
	wg.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer wg.Done()
			ready.Done()
			<-startCh
			closeWithTimeout(body, closeTimeout)
			doneCh <- struct{}{}
		}()
	}

	ready.Wait()
	close(startCh)

	select {
	case <-body.closeStartedCh:
	case <-time.After(time.Second):
		t.Fatal("closeWithTimeout did not start a close call within 1s")
	}

	// Keep the underlying Close call blocked until all duplicate attempts have
	// returned from closeWithTimeout.
	waitFor(t, 2*time.Second, func() bool {
		return len(doneCh) >= attempts-1
	})
	if got := body.closeCallCount(); got != 1 {
		t.Fatalf("body close calls while blocked = %d, want 1", got)
	}
	if !closeWithTimeoutIsInFlight(body) {
		t.Fatal("body closer should remain in-flight while Close is blocked")
	}

	body.release()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent closeWithTimeout calls did not finish within 2s")
	}
	if got := len(doneCh); got != attempts {
		t.Fatalf("completed closeWithTimeout calls = %d, want %d", got, attempts)
	}
	if got := body.closeCallCount(); got != 1 {
		t.Fatalf("body close calls after release = %d, want 1", got)
	}
	waitFor(t, time.Second, func() bool {
		return !closeWithTimeoutIsInFlight(body)
	})

	afterStats := closeWithTimeoutStatsSnapshot()
	if startedDelta := afterStats.Started - beforeStats.Started; startedDelta < 1 {
		t.Fatalf("closeWithTimeout started delta = %d, want >= 1", startedDelta)
	}
	if suppressedDelta := afterStats.Suppressed - beforeStats.Suppressed; suppressedDelta < attempts-1 {
		t.Fatalf("closeWithTimeout suppressed delta = %d, want >= %d", suppressedDelta, attempts-1)
	}
}

func TestCloseWithTimeoutBudgetSuppressionQueuesDeferredCloses(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	const attempts = boundedCloseWorkerBudget + 8
	const expectedQueued = attempts - boundedCloseWorkerBudget
	const closeTimeout = 2 * time.Second
	bodies := make([]*startupAbortBlockingBody, attempts)
	for i := 0; i < attempts; i++ {
		bodies[i] = newStartupAbortBlockingBody()
	}
	t.Cleanup(func() {
		for _, body := range bodies {
			body.release()
		}
	})

	for i := 0; i < attempts; i++ {
		go closeWithTimeout(bodies[i], closeTimeout)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats := closeWithTimeoutStatsSnapshot()
		if stats.Started == boundedCloseWorkerBudget &&
			stats.Suppressed == expectedQueued &&
			stats.Queued == expectedQueued {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Started != boundedCloseWorkerBudget {
		t.Fatalf("stats.Started = %d, want %d before release", stats.Started, boundedCloseWorkerBudget)
	}
	if stats.Suppressed != expectedQueued {
		t.Fatalf("stats.Suppressed = %d, want %d", stats.Suppressed, expectedQueued)
	}
	if stats.Queued != expectedQueued {
		t.Fatalf("stats.Queued = %d, want %d before release", stats.Queued, expectedQueued)
	}
	if stats.Dropped != 0 {
		t.Fatalf("stats.Dropped = %d, want 0", stats.Dropped)
	}

	for _, body := range bodies {
		body.release()
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		closedCount := 0
		for _, body := range bodies {
			select {
			case <-body.closeStartedCh:
				closedCount++
			default:
			}
		}
		stats = closeWithTimeoutStatsSnapshot()
		if closedCount == attempts && stats.Queued == 0 && stats.Started == attempts {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	closedCount := 0
	for _, body := range bodies {
		select {
		case <-body.closeStartedCh:
			closedCount++
		default:
		}
	}
	if closedCount != attempts {
		t.Fatalf("close started count = %d, want %d", closedCount, attempts)
	}

	stats = closeWithTimeoutStatsSnapshot()
	if stats.Queued != 0 {
		t.Fatalf("stats.Queued = %d, want 0 after release", stats.Queued)
	}
	if stats.Dropped != 0 {
		t.Fatalf("stats.Dropped = %d, want 0", stats.Dropped)
	}
	if stats.Retried != expectedQueued {
		t.Fatalf("stats.Retried = %d, want %d", stats.Retried, expectedQueued)
	}
	if stats.Started != attempts {
		t.Fatalf("stats.Started = %d, want %d", stats.Started, attempts)
	}
}

func TestCloseWithTimeoutRetryLoopTickerDrainsQueuedRetryWithoutNotify(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)
	closeWithTimeoutEnsureRetryLoop()

	queued := newStartupAbortBlockingBody()
	queuedKey := closeWithTimeoutCloserKey(queued)
	t.Cleanup(queued.release)

	// Exercise ticker fallback deterministically by queueing a deferred retry
	// without signaling the notify channel.
	closeWithTimeoutQueueRetryWithSignal(queued, 750*time.Millisecond, false)

	waitFor(t, 2*time.Second, func() bool {
		select {
		case <-queued.closeStartedCh:
			return true
		default:
			return false
		}
	})

	queued.release()
	waitFor(t, time.Second, func() bool {
		closeWithTimeoutInFlightMu.Lock()
		_, inFlight := closeWithTimeoutInFlightByCloser[queuedKey]
		closeWithTimeoutInFlightMu.Unlock()
		return !inFlight && len(closeWithTimeoutWorkers) == 0
	})

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Started != 1 {
		t.Fatalf("stats.Started = %d, want 1 after ticker retry drain", stats.Started)
	}
	if stats.Retried != 1 {
		t.Fatalf("stats.Retried = %d, want 1 after ticker retry drain", stats.Retried)
	}
	if stats.Queued != 0 {
		t.Fatalf("stats.Queued = %d, want 0 after ticker retry drain", stats.Queued)
	}
}

func TestCloseWithTimeoutRetryQueueOverflowTracksDroppedAndBoundedDepth(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	queued := make([]*startupAbortBlockingBody, 0, closeWithTimeoutRetryQueueCap+16)
	t.Cleanup(func() {
		for _, body := range queued {
			body.release()
		}
	})

	const overflow = 11
	attempts := closeWithTimeoutRetryQueueCap + overflow
	for i := 0; i < attempts; i++ {
		body := newStartupAbortBlockingBody()
		queued = append(queued, body)
		closeWithTimeoutQueueRetryWithSignal(body, 750*time.Millisecond, false)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Queued != uint64(closeWithTimeoutRetryQueueCap) {
		t.Fatalf("stats.Queued = %d, want %d", stats.Queued, closeWithTimeoutRetryQueueCap)
	}
	if stats.Dropped != uint64(overflow) {
		t.Fatalf("stats.Dropped = %d, want %d", stats.Dropped, overflow)
	}
}

func TestCloseWithTimeoutRetryQueueDeduplicatesRepeatedCloser(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	body := newStartupAbortBlockingBody()
	t.Cleanup(body.release)

	const attempts = 9
	for i := 0; i < attempts; i++ {
		closeWithTimeoutQueueRetryWithSignal(body, 500*time.Millisecond, false)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Queued != 1 {
		t.Fatalf("stats.Queued = %d, want 1 (deduplicated queued closer)", stats.Queued)
	}
	if stats.Dropped != 0 {
		t.Fatalf("stats.Dropped = %d, want 0", stats.Dropped)
	}
}

func TestCloseWithTimeoutRetryDrainWorkersAreBoundedAndFullyCleanedUp(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	const attempts = (2 * boundedCloseWorkerBudget) + 8
	const runtimeGoroutineJitter = 6
	const drainResidualAllowance = 6
	const retryTimeout = 20 * time.Millisecond
	expectedQueued := attempts - boundedCloseWorkerBudget

	bodies := make([]*startupAbortBlockingBody, attempts)
	for i := 0; i < attempts; i++ {
		bodies[i] = newStartupAbortBlockingBody()
	}
	t.Cleanup(func() {
		for _, body := range bodies {
			body.release()
		}
	})

	goroutinesBefore := waitForStableGoroutines()
	for i := 0; i < attempts; i++ {
		go closeWithTimeout(bodies[i], retryTimeout)
	}

	waitFor(t, 2*time.Second, func() bool {
		stats := closeWithTimeoutStatsSnapshot()
		return int(stats.Started) == boundedCloseWorkerBudget &&
			int(stats.Suppressed) == expectedQueued &&
			int(stats.Queued) == expectedQueued
	})

	goroutinesAfterAttempts := waitForStableGoroutines()
	maxExpectedGrowth := (2 * boundedCloseWorkerBudget) + runtimeGoroutineJitter
	if goroutinesAfterAttempts > goroutinesBefore+maxExpectedGrowth {
		t.Fatalf(
			"retry-drain close workers exceeded bounded growth before release: before=%d after=%d maxGrowth=%d",
			goroutinesBefore,
			goroutinesAfterAttempts,
			maxExpectedGrowth,
		)
	}

	for _, body := range bodies {
		body.release()
	}

	waitFor(t, 3*time.Second, func() bool {
		stats := closeWithTimeoutStatsSnapshot()
		return int(stats.Started) == attempts &&
			int(stats.Retried) == expectedQueued &&
			stats.Queued == 0 &&
			stats.Dropped == 0 &&
			int(stats.Timeouts) == boundedCloseWorkerBudget &&
			int(stats.LateCompletions) == boundedCloseWorkerBudget
	})

	drainCeiling := goroutinesBefore + drainResidualAllowance
	waitFor(t, 3*time.Second, func() bool {
		return waitForStableGoroutines() <= drainCeiling
	})
	goroutinesAfterRelease := waitForStableGoroutines()
	if goroutinesAfterRelease > drainCeiling {
		t.Fatalf(
			"retry-drain close workers did not fully clean up: before=%d afterRelease=%d ceiling=%d",
			goroutinesBefore,
			goroutinesAfterRelease,
			drainCeiling,
		)
	}
}

func TestCloseWithTimeoutRetryDrainCountsStartedOncePerRetriedCloser(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	const closeTimeout = 500 * time.Millisecond

	blocking := make([]*startupAbortBlockingBody, boundedCloseWorkerBudget)
	for i := 0; i < len(blocking); i++ {
		blocking[i] = newStartupAbortBlockingBody()
	}
	retried := newStartupAbortBlockingBody()
	t.Cleanup(func() {
		for _, body := range blocking {
			body.release()
		}
		retried.release()
	})

	for _, body := range blocking {
		go closeWithTimeout(body, closeTimeout)
	}

	waitFor(t, 2*time.Second, func() bool {
		stats := closeWithTimeoutStatsSnapshot()
		return int(stats.Started) == boundedCloseWorkerBudget &&
			stats.Suppressed == 0 &&
			stats.Queued == 0 &&
			stats.Retried == 0
	})

	closeWithTimeout(retried, closeTimeout)
	waitFor(t, 2*time.Second, func() bool {
		stats := closeWithTimeoutStatsSnapshot()
		return int(stats.Started) == boundedCloseWorkerBudget &&
			stats.Suppressed == 1 &&
			stats.Queued == 1 &&
			stats.Retried == 0
	})

	// Releasing one worker should allow exactly one deferred retry start.
	blocking[0].release()
	waitFor(t, 2*time.Second, func() bool {
		select {
		case <-retried.closeStartedCh:
		default:
			return false
		}
		stats := closeWithTimeoutStatsSnapshot()
		return int(stats.Started) == boundedCloseWorkerBudget+1 &&
			stats.Suppressed == 1 &&
			stats.Queued == 0 &&
			stats.Retried == 1
	})
}

// ---------------------------------------------------------------------------
// Regression tests: blocking-close / startup-abort lifecycle convergence bugs
// See: TODO-stream-blocking-close-startup-abort-lifecycle-convergence.md
// ---------------------------------------------------------------------------

// stallingReader blocks on Read until its done channel is closed, then returns io.EOF.
type stallingReader struct {
	enteredCh chan struct{}
	doneCh    chan struct{}
	enterOnce sync.Once
}

func newStallingReader() *stallingReader {
	return &stallingReader{
		enteredCh: make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

func (r *stallingReader) Read([]byte) (int, error) {
	r.enterOnce.Do(func() { close(r.enteredCh) })
	<-r.doneCh
	return 0, io.EOF
}

func (r *stallingReader) release() {
	select {
	case <-r.doneCh:
	default:
		close(r.doneCh)
	}
}

// startupAbortBlockingBody is an http response body where both Read and Close block
// until a shared release channel is closed. Used to simulate misbehaving
// upstream transports in startup abort path tests.
type startupAbortBlockingBody struct {
	closeStartedCh chan struct{}
	releaseCh      chan struct{}
	closeOnce      sync.Once
	mu             sync.Mutex
	closeCalls     int
}

func newStartupAbortBlockingBody() *startupAbortBlockingBody {
	return &startupAbortBlockingBody{
		closeStartedCh: make(chan struct{}),
		releaseCh:      make(chan struct{}),
	}
}

func (b *startupAbortBlockingBody) Read([]byte) (int, error) {
	<-b.releaseCh
	return 0, io.EOF
}

func (b *startupAbortBlockingBody) Close() error {
	b.mu.Lock()
	b.closeCalls++
	b.mu.Unlock()
	b.closeOnce.Do(func() { close(b.closeStartedCh) })
	<-b.releaseCh
	return nil
}

func (b *startupAbortBlockingBody) closeCallCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closeCalls
}

func (b *startupAbortBlockingBody) release() {
	select {
	case <-b.releaseCh:
	default:
		close(b.releaseCh)
	}
}

// startupAbortBlockingBodyTransport returns a 200 OK response with a startupAbortBlockingBody.
type startupAbortBlockingBodyTransport struct {
	body *startupAbortBlockingBody
}

func (t *startupAbortBlockingBodyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       t.body,
	}, nil
}

type delayedBlockingStartupResponseTransport struct {
	delay    time.Duration
	mu       sync.Mutex
	bodies   []*startupAbortBlockingBody
	returned int
}

func (t *delayedBlockingStartupResponseTransport) RoundTrip(*http.Request) (*http.Response, error) {
	body := newStartupAbortBlockingBody()

	t.mu.Lock()
	t.bodies = append(t.bodies, body)
	t.mu.Unlock()

	time.Sleep(t.delay)
	t.mu.Lock()
	t.returned++
	t.mu.Unlock()

	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       body,
	}, nil
}

func (t *delayedBlockingStartupResponseTransport) releaseAll() {
	t.mu.Lock()
	bodies := append([]*startupAbortBlockingBody(nil), t.bodies...)
	t.mu.Unlock()

	for _, body := range bodies {
		body.release()
	}
}

func (t *delayedBlockingStartupResponseTransport) closeCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	count := 0
	for _, body := range t.bodies {
		select {
		case <-body.closeStartedCh:
			count++
		default:
		}
	}
	return count
}

func (t *delayedBlockingStartupResponseTransport) returnedCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.returned
}

type non2xxPreviewBodyTransport struct {
	mu     sync.Mutex
	status int
	bodies []*startupAbortBlockingBody
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func (t *non2xxPreviewBodyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	body := newStartupAbortBlockingBody()

	t.mu.Lock()
	t.bodies = append(t.bodies, body)
	t.mu.Unlock()

	return &http.Response{
		StatusCode: t.status,
		Status:     fmt.Sprintf("%d %s", t.status, http.StatusText(t.status)),
		Header:     make(http.Header),
		Body:       body,
	}, nil
}

func (t *non2xxPreviewBodyTransport) releaseAll() {
	t.mu.Lock()
	bodies := append([]*startupAbortBlockingBody(nil), t.bodies...)
	t.mu.Unlock()

	for _, body := range bodies {
		body.release()
	}
}

func (t *non2xxPreviewBodyTransport) closeCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	count := 0
	for _, body := range t.bodies {
		select {
		case <-body.closeStartedCh:
			count++
		default:
		}
	}
	return count
}

type non2xxDrainingBody struct {
	mu             sync.Mutex
	payload        []byte
	offset         int
	closeCalls     int
	drainedOnClose bool
}

func newNon2xxDrainingBody(payload string) *non2xxDrainingBody {
	return &non2xxDrainingBody{
		payload: []byte(payload),
	}
}

func (b *non2xxDrainingBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.offset >= len(b.payload) {
		return 0, io.EOF
	}
	n := copy(p, b.payload[b.offset:])
	b.offset += n
	if b.offset >= len(b.payload) {
		return n, io.EOF
	}
	return n, nil
}

func (b *non2xxDrainingBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closeCalls++
	if b.offset < len(b.payload) {
		b.drainedOnClose = true
		b.offset = len(b.payload)
	}
	return nil
}

func (b *non2xxDrainingBody) closeSnapshot() (closeCalls int, drainedOnClose bool, offset int, total int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closeCalls, b.drainedOnClose, b.offset, len(b.payload)
}

// TestStartupProbeTimeoutCleansUpBlockedReaderGoroutine verifies that
// readStartupProbeWithRandomAccess does not leak its probe-read goroutine
// after a startup timeout when the reader remains blocked.
//
// Issue: TODO-stream-startup-probe-timeout-abort-drain-goroutine-leak.md
// Bug: abortProbeRead waits only 250ms then returns, leaving the goroutine alive.
func TestStartupProbeTimeoutCleansUpBlockedReaderGoroutine(t *testing.T) {
	reader := newStallingReader()
	defer reader.release()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Stabilize goroutine count before measurement.
	runtime.GC()
	runtime.Gosched()
	time.Sleep(50 * time.Millisecond)
	goroutinesBefore := runtime.NumGoroutine()

	_, _, _, _, _, err := readStartupProbeWithRandomAccess(
		ctx,
		reader,
		1,                           // minProbeBytes
		40*time.Millisecond,         // startupTimeout
		false,                       // requireRandomAccess
		func() { reader.release() }, // abort releases reader (simulates HTTP connection close unblocking Read)
	)
	if err == nil {
		t.Fatal("readStartupProbeWithRandomAccess() error = nil, want startup timeout")
	}
	if !strings.Contains(err.Error(), "startup timeout") {
		t.Fatalf("error = %q, want startup timeout", err)
	}

	// Cancel context — after fix, the goroutine should observe cancellation and exit.
	cancel()

	// Give goroutine time to observe cancellation.
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	runtime.Gosched()

	goroutinesAfter := runtime.NumGoroutine()

	// Release the reader for cleanup before asserting (prevents permanent leak).
	reader.release()
	time.Sleep(100 * time.Millisecond)

	if goroutinesAfter > goroutinesBefore {
		t.Fatalf(
			"probe-read goroutine leaked after timeout + context cancel: before=%d after=%d",
			goroutinesBefore, goroutinesAfter,
		)
	}
}

// TestStartupProbeTimeoutReturnsPromptlyWhenAbortDoesNotUnblockRead verifies
// that readStartupProbeWithRandomAccess returns within bounded time even when
// the abort callback does NOT unblock the stalled Read (e.g., misbehaving
// transport where close/cancel doesn't interrupt pending I/O). The context-
// aware select inside the probe goroutine should allow it to exit promptly
// via probeCtx.Done() without waiting for the blocked Read to return.
//
// Issue: TODO-stream-startup-probe-timeout-abort-drain-goroutine-leak.md
func TestStartupProbeTimeoutReturnsPromptlyWhenAbortDoesNotUnblockRead(t *testing.T) {
	reader := newStallingReader()
	defer reader.release()

	ctx := context.Background()

	start := time.Now()
	_, _, _, _, _, err := readStartupProbeWithRandomAccess(
		ctx,
		reader,
		1,                   // minProbeBytes
		40*time.Millisecond, // startupTimeout
		false,               // requireRandomAccess
		func() {},           // no-op abort: does NOT unblock the reader
	)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("readStartupProbeWithRandomAccess() error = nil, want startup timeout")
	}
	if !strings.Contains(err.Error(), "startup timeout") {
		t.Fatalf("error = %q, want startup timeout", err)
	}

	// Function should return within bounded time (probe timeout + drain wait).
	// Before the fix, this would block until Read eventually returned.
	if elapsed > 5*time.Second {
		t.Fatalf(
			"readStartupProbeWithRandomAccess blocked for %v with no-op abort; expected bounded return near probe timeout",
			elapsed,
		)
	}
}

// TestFFmpegWrapperChildPipeWaitBoundsSessionClose verifies that
// session.close() returns within bounded time even when ffmpeg is a wrapper
// script whose child processes inherit and hold stdout/stderr pipe FDs.
//
// Issue: TODO-stream-ffmpeg-wrapper-child-pipe-wait-lifecycle-stall.md
// Bug: killCommand only kills direct process; children keep pipe FDs, blocking cmd.Wait().
func TestFFmpegWrapperChildPipeWaitBoundsSessionClose(t *testing.T) {
	tmp := t.TempDir()
	sentinelPath := filepath.Join(tmp, "stop-child")
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-wrapper-child-stall.sh", fmt.Sprintf(`#!/usr/bin/env bash
# Output startup probe byte, then spawn a child that holds stdout pipe FD open.
printf 'x'
(while [ ! -f %q ]; do sleep 0.1; done) &
exit 0
`, sentinelPath))

	t.Cleanup(func() {
		_ = os.WriteFile(sentinelPath, []byte("stop"), 0o644)
		time.Sleep(300 * time.Millisecond)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	session, err := startFFmpegOnce(
		ctx,
		ffmpegPath,
		"http://example.test/wrapper-child-stall",
		"ffmpeg-copy",
		2*time.Second,
		1,  // minProbeBytes
		1,  // readRate
		-1, // initialBurst (disabled)
		128000,
		time.Second,
		false, 0, -1, "",
		false,
	)
	if err != nil {
		t.Fatalf("startFFmpegOnce() error = %v", err)
	}

	closeDone := make(chan struct{})
	go func() {
		session.close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// close completed within bounds — fixed behavior.
	case <-time.After(1 * time.Second):
		// BUG: close is blocked because child process holds pipe FDs.
		_ = os.WriteFile(sentinelPath, []byte("stop"), 0o644)
		cancel()
		select {
		case <-closeDone:
		case <-time.After(5 * time.Second):
		}
		t.Fatal("session.close() blocked >1s — wrapper child pipe FDs keep cmd.Wait() pinned")
	}
}

// TestStartDirectNon2xxBodyPreviewBoundedByStartupTimeout verifies that
// the non-2xx response body preview read in startDirect does not block
// beyond the configured startup timeout when the upstream stalls the body.
//
// Issue: TODO-stream-direct-startup-non2xx-body-preview-blocking-lifecycle-stall.md
// Bug: io.ReadAll on non-2xx body uses parent context, not startup timeout.
func TestStartDirectNon2xxBodyPreviewBoundedByStartupTimeout(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Send 503 headers immediately, then stall body indefinitely.
		w.WriteHeader(http.StatusServiceUnavailable)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(10 * time.Second)
	}))
	defer upstream.Close()

	parentCtx, parentCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer parentCancel()

	started := time.Now()
	_, err := startDirect(
		parentCtx,
		&http.Client{},
		upstream.URL,
		80*time.Millisecond, // startupTimeout
		1,
		false,
	)
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("startDirect() error = nil, want non-2xx startup failure")
	}

	// After fix, function should return near startup timeout (80ms),
	// not be pinned until parent context timeout (2s).
	if elapsed > 500*time.Millisecond {
		t.Fatalf(
			"startDirect() elapsed = %s, want < 500ms (bounded by startup timeout, not parent context %s)",
			elapsed, 2*time.Second,
		)
	}
}

// TestStartDirectNon2xxBodyCloseDrainsRemainingBytes verifies that the non-2xx
// startup path invokes response-body close and allows close to drain remaining
// unread bytes before release.
//
// Issue: TODO-commit-review-04c0048-harden-startup-churn-regressions.md (M1)
func TestStartDirectNon2xxBodyCloseDrainsRemainingBytes(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	body := newNon2xxDrainingBody(strings.Repeat("x", 512))
	client := &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Status:     "503 Service Unavailable",
				Header:     make(http.Header),
				Body:       body,
			}, nil
		}),
	}

	_, err := startDirect(
		context.Background(),
		client,
		"http://example.test/non2xx-drain",
		150*time.Millisecond,
		1,
		false,
	)
	if err == nil {
		t.Fatal("startDirect() error = nil, want non-2xx startup failure")
	}
	if !strings.Contains(err.Error(), "upstream returned status 503") {
		t.Fatalf("startDirect() error = %q, want upstream returned status 503", err)
	}

	waitFor(t, time.Second, func() bool {
		closeCalls, _, _, _ := body.closeSnapshot()
		return closeCalls > 0 && len(closeWithTimeoutWorkers) == 0
	})

	closeCalls, drainedOnClose, offset, total := body.closeSnapshot()
	if closeCalls != 1 {
		t.Fatalf("non-2xx body close calls = %d, want 1", closeCalls)
	}
	if !drainedOnClose {
		t.Fatal("non-2xx body close did not drain unread preview remainder")
	}
	if offset != total {
		t.Fatalf("non-2xx body read offset = %d, want %d after close drain", offset, total)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Started != 1 {
		t.Fatalf("closeWithTimeout started=%d, want 1", stats.Started)
	}
	if stats.Timeouts != 0 {
		t.Fatalf("closeWithTimeout timeouts=%d, want 0 for non-blocking non-2xx body closes", stats.Timeouts)
	}
}

// TestStartDirectNon2xxBodyCloseCancelsAttemptContext verifies that the
// non-2xx startup path eventually cancels the request child context after the
// bounded body close completes.
//
// Issue: TODO-commit-review-675b897-non2xx-close-path-churn.md (H1/M1)
func TestStartDirectNon2xxBodyCloseCancelsAttemptContext(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	reqCtxCh := make(chan context.Context, 1)
	body := newNon2xxDrainingBody(strings.Repeat("x", 64))
	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			select {
			case reqCtxCh <- req.Context():
			default:
			}
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Status:     "503 Service Unavailable",
				Header:     make(http.Header),
				Body:       body,
			}, nil
		}),
	}

	_, err := startDirect(
		context.Background(),
		client,
		"http://example.test/non2xx-attempt-cancel",
		150*time.Millisecond,
		1,
		false,
	)
	if err == nil {
		t.Fatal("startDirect() error = nil, want non-2xx startup failure")
	}
	if !strings.Contains(err.Error(), "upstream returned status 503") {
		t.Fatalf("startDirect() error = %q, want upstream returned status 503", err)
	}

	var reqCtx context.Context
	select {
	case reqCtx = <-reqCtxCh:
	case <-time.After(time.Second):
		t.Fatal("non-2xx transport did not receive request context")
	}

	waitFor(t, time.Second, func() bool {
		select {
		case <-reqCtx.Done():
			return true
		default:
			return false
		}
	})
	if !errors.Is(reqCtx.Err(), context.Canceled) {
		t.Fatalf("request context err = %v, want %v", reqCtx.Err(), context.Canceled)
	}

	waitFor(t, time.Second, func() bool {
		closeCalls, _, _, _ := body.closeSnapshot()
		return closeCalls == 1
	})
}

// TestStartDirectNon2xxBodyCloseCancelsAttemptContextAfterCloseTimeout verifies that
// the non-2xx startup path still cancels the request child context when bounded
// close times out and the response-body Close remains blocked.
//
// Issue: TODO-commit-review-675b897-non2xx-close-path-churn.md (M1)
func TestStartDirectNon2xxBodyCloseCancelsAttemptContextAfterCloseTimeout(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	reqCtxCh := make(chan context.Context, 1)
	body := newStartupAbortBlockingBody()
	t.Cleanup(body.release)

	client := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			select {
			case reqCtxCh <- req.Context():
			default:
			}
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Status:     "503 Service Unavailable",
				Header:     make(http.Header),
				Body:       body,
			}, nil
		}),
	}

	started := time.Now()
	_, err := startDirect(
		context.Background(),
		client,
		"http://example.test/non2xx-attempt-cancel-slow-close",
		120*time.Millisecond,
		1,
		false,
	)
	if err == nil {
		t.Fatal("startDirect() error = nil, want non-2xx startup failure")
	}
	if !strings.Contains(err.Error(), "upstream returned status 503") {
		t.Fatalf("startDirect() error = %q, want upstream returned status 503", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("startDirect() elapsed = %s, want < 500ms under non-2xx startup timeout", elapsed)
	}

	var reqCtx context.Context
	select {
	case reqCtx = <-reqCtxCh:
	case <-time.After(time.Second):
		t.Fatal("non-2xx transport did not receive request context")
	}

	select {
	case <-body.closeStartedCh:
	case <-time.After(time.Second):
		t.Fatal("non-2xx body close did not start")
	}

	waitFor(t, boundedCloseTimeout+time.Second, func() bool {
		select {
		case <-reqCtx.Done():
			return errors.Is(reqCtx.Err(), context.Canceled)
		default:
			return false
		}
	})

	waitFor(t, time.Second, func() bool {
		return closeWithTimeoutStatsSnapshot().Timeouts == 1
	})

	body.release()
	waitFor(t, time.Second, func() bool {
		return len(closeWithTimeoutWorkers) == 0
	})
}

// TestStartDirectNon2xxBlockedPreviewBodyDoesNotCreateUnboundedGoroutines verifies that
// repeated non-2xx startup churn with stalled preview-body cleanup does not create
// unbounded detached preview-read or close goroutines.
//
// Issue: TODO-stream-direct-startup-non2xx-preview-read-detached-goroutine-accumulation.md
func TestStartDirectNon2xxBlockedPreviewBodyDoesNotCreateUnboundedGoroutines(t *testing.T) {
	transport := &non2xxPreviewBodyTransport{
		status: http.StatusServiceUnavailable,
	}
	t.Cleanup(transport.releaseAll)
	client := &http.Client{
		Transport: transport,
	}
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	// Keep goroutine pressure low and deterministic before measuring churn.
	goroutinesBefore := waitForStableGoroutines()
	closeWorkerBudget := cap(closeWithTimeoutWorkers)

	const attempts = 24
	const startupTimeout = 35 * time.Millisecond
	const maxElapsed = 200 * time.Millisecond
	for i := 0; i < attempts; i++ {
		started := time.Now()
		_, err := startDirect(
			context.Background(),
			client,
			"http://example.test/non2xx-blocked-preview",
			startupTimeout,
			1,
			false,
		)
		elapsed := time.Since(started)

		if err == nil {
			t.Fatal("startDirect() error = nil, want non-2xx startup failure")
		}
		if !strings.Contains(err.Error(), "upstream returned status 503") {
			t.Fatalf("startDirect() error = %q, want upstream returned status 503", err)
		}
		if elapsed > maxElapsed {
			t.Fatalf("startDirect() elapsed = %s, want < %s under startup timeout pressure", elapsed, maxElapsed)
		}
	}

	goroutinesAfterAttempts := waitForStableGoroutines()
	// Under repeated blocked body churn, growth should stay bounded by:
	// one probe-preview read worker per attempt, plus two goroutines per
	// in-flight bounded close worker (blocked Close + late-completion waiter).
	maxExpectedGrowth := attempts + (2 * closeWorkerBudget) + 8 // +8 for runtime jitter.
	if goroutinesAfterAttempts > goroutinesBefore+maxExpectedGrowth {
		t.Fatalf(
			"detached goroutine growth under repeated non-2xx startup churn exceeded bounded ceiling: before=%d after=%d maxGrowth=%d",
			goroutinesBefore,
			goroutinesAfterAttempts,
			maxExpectedGrowth,
		)
	}

	closeCount := transport.closeCount()
	minExpectedClose := minInt(attempts, closeWorkerBudget)
	if closeCount < minExpectedClose {
		t.Fatalf(
			"non-2xx cleanup did not start bounded close workers: closeCount=%d want>=%d",
			closeCount,
			minExpectedClose,
		)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if int(stats.Started) > closeWorkerBudget {
		t.Fatalf("closeWithTimeout started=%d exceeds budget=%d", stats.Started, closeWorkerBudget)
	}
	if int(stats.Started) < minExpectedClose {
		t.Fatalf("closeWithTimeout started=%d want >= %d", stats.Started, minExpectedClose)
	}

	// Allow cleanup paths and release-waiting reads to resolve.
	transport.releaseAll()
	time.Sleep(200 * time.Millisecond)
	goroutinesAfterRelease := waitForStableGoroutines()
	if goroutinesAfterRelease > goroutinesBefore+8 {
		t.Fatalf(
			"preview/close worker cleanup goroutines remained after release: before=%d after=%d",
			goroutinesBefore,
			goroutinesAfterRelease,
		)
	}
}

// TestStartDirectTimeoutAbortNotBlockedByResponseClose verifies that
// the startup timeout abort path in startDirect does not block indefinitely
// on a synchronous resp.Body.Close() call when the upstream misbehaves.
//
// Issue: TODO-stream-direct-startup-timeout-response-close-blocking-lifecycle-stall.md
// Bug: abort callback calls resp.Body.Close() synchronously which can block.
func TestStartDirectTimeoutAbortNotBlockedByResponseClose(t *testing.T) {
	body := newStartupAbortBlockingBody()
	defer body.release()

	transport := &startupAbortBlockingBodyTransport{body: body}
	client := &http.Client{Transport: transport}

	doneCh := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := startDirect(
			context.Background(),
			client,
			"http://example.test/blocking-close-abort",
			80*time.Millisecond, // startupTimeout
			1,
			false,
		)
		doneCh <- err
	}()

	// Wait for Close to be entered, confirming the abort path was reached.
	select {
	case <-body.closeStartedCh:
		// Good — abort called Close.
	case <-time.After(2 * time.Second):
		body.release()
		t.Fatal("resp.Body.Close() was never called in abort path")
	}

	// After fix, function should return despite blocking Close.
	select {
	case err := <-doneCh:
		elapsed := time.Since(started)
		if err == nil {
			t.Fatal("startDirect() error = nil, want startup timeout")
		}
		if elapsed > 500*time.Millisecond {
			t.Fatalf("startDirect() elapsed = %s, want < 500ms", elapsed)
		}
	case <-time.After(1 * time.Second):
		body.release() // unblock for cleanup
		<-doneCh
		t.Fatal("startDirect() blocked >1s on resp.Body.Close() in startup timeout abort path")
	}
}

// TestStartDirectLateResponseTimeoutDoesNotCreateUnboundedGoroutines verifies that
// repeated late-response startup-abort paths with blocking response close do not
// leave unbounded blocked cleanup goroutines behind.
//
// Issue: TODO-stream-direct-startup-late-response-close-blocking-goroutine-accumulation.md
// Bug: late result transport goroutines close response bodies synchronously and can pin indefinitely.
func TestStartDirectLateResponseTimeoutDoesNotCreateUnboundedGoroutines(t *testing.T) {
	transport := &delayedBlockingStartupResponseTransport{
		delay: 350 * time.Millisecond,
	}
	t.Cleanup(transport.releaseAll)
	client := &http.Client{
		Transport: transport,
	}
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	// Keep baseline goroutine measurement low and deterministic.
	goroutinesBefore := waitForStableGoroutines()
	closeWorkerBudget := cap(closeWithTimeoutWorkers)

	const attempts = 24
	const startupTimeout = 70 * time.Millisecond
	const maxElapsed = 500 * time.Millisecond
	const upstreamURL = "http://example.test/late-response-timeout"
	for i := 0; i < attempts; i++ {
		started := time.Now()
		_, err := startDirect(
			context.Background(),
			client,
			upstreamURL,
			startupTimeout,
			1,
			false,
		)
		elapsed := time.Since(started)

		if err == nil {
			t.Fatalf("startDirect() error = nil on attempt=%d, want startup timeout", i)
		}
		if !strings.Contains(err.Error(), "startup timeout waiting for upstream response headers") {
			t.Fatalf("startDirect() error on attempt=%d = %q, want startup timeout reason", i, err)
		}
		if elapsed > maxElapsed {
			t.Fatalf("startDirect() elapsed on attempt=%d = %s, want < %s", i, elapsed, maxElapsed)
		}
	}

	goroutinesAfterAttempts := waitForStableGoroutines()
	// At assertion time there may still be delayed transport request goroutines
	// in flight in addition to two goroutines per in-flight bounded close worker
	// (blocked Close + late-completion waiter).
	maxInFlightLateResponses := int((transport.delay / startupTimeout)) + 1
	maxExpectedGrowth := maxInFlightLateResponses + (2 * closeWorkerBudget) + 8 // +8 for runtime jitter.
	if goroutinesAfterAttempts > goroutinesBefore+maxExpectedGrowth {
		t.Fatalf(
			"late-response cleanup worker accumulation exceeded bounded ceiling: before=%d after=%d maxGrowth=%d",
			goroutinesBefore,
			goroutinesAfterAttempts,
			maxExpectedGrowth,
		)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if transport.returnedCount() >= attempts {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	expectedCloseCount := minInt(attempts, closeWorkerBudget)
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if transport.closeCount() >= expectedCloseCount {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if closeCount := transport.closeCount(); closeCount < expectedCloseCount {
		t.Fatalf(
			"late-response cleanup did not start bounded close workers: closeCount=%d expected>=%d (budget=%d attempts=%d)",
			closeCount,
			expectedCloseCount,
			closeWorkerBudget,
			attempts,
		)
	}

	// Assert against this test transport's closers directly instead of the global
	// closeWithTimeout counter, which can include late-completion activity from
	// other tests still draining in the package process.
	if closeCount := transport.closeCount(); closeCount > closeWorkerBudget {
		t.Fatalf("late-response cleanup started %d closers, exceeds budget=%d", closeCount, closeWorkerBudget)
	}

	time.Sleep(200 * time.Millisecond)
	transport.releaseAll()
	time.Sleep(200 * time.Millisecond)
	goroutinesAfterRelease := waitForStableGoroutines()
	if goroutinesAfterRelease > goroutinesBefore+8 {
		t.Fatalf(
			"late-response cleanup workers remained after release: before=%d after=%d",
			goroutinesBefore,
			goroutinesAfterRelease,
		)
	}
}

// TestStartDirectProbePhaseBlockedBodyDoesNotCreateUnboundedGoroutines verifies that
// repeated startup-probe timeout churn after 200 OK headers keeps detached
// goroutine growth bounded while preserving timeout-bounded startup returns.
//
// Issue: commit review 281cec4 M2
func TestStartDirectProbePhaseBlockedBodyDoesNotCreateUnboundedGoroutines(t *testing.T) {
	transport := &non2xxPreviewBodyTransport{
		status: http.StatusOK,
	}
	t.Cleanup(transport.releaseAll)
	client := &http.Client{
		Transport: transport,
	}
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)
	resetStartupProbeReadWorkerStatsForTest()
	t.Cleanup(resetStartupProbeReadWorkerStatsForTest)

	goroutinesBefore := waitForStableGoroutines()
	closeWorkerBudget := cap(closeWithTimeoutWorkers)
	startupProbeWorkerStatsBefore := startupProbeReadWorkerStatsSnapshot()
	startupProbeReadWorkerBudget := startupProbeWorkerStatsBefore.Budget

	const attempts = 24
	const startupTimeout = 40 * time.Millisecond
	const maxElapsed = 250 * time.Millisecond
	for i := 0; i < attempts; i++ {
		started := time.Now()
		_, err := startDirect(
			context.Background(),
			client,
			"http://example.test/probe-phase-blocked-body",
			startupTimeout,
			1,
			false,
		)
		elapsed := time.Since(started)
		if err == nil {
			t.Fatalf("startDirect() error = nil on attempt=%d, want startup timeout", i)
		}
		if !strings.Contains(err.Error(), "startup timeout waiting for source bytes") {
			t.Fatalf("startDirect() error on attempt=%d = %q, want startup timeout waiting for source bytes", i, err)
		}
		if elapsed > maxElapsed {
			t.Fatalf("startDirect() elapsed on attempt=%d = %s, want < %s", i, elapsed, maxElapsed)
		}
	}

	goroutinesAfterAttempts := waitForStableGoroutines()
	// Bound expected growth as one blocked probe-read goroutine per detached
	// read-worker budget slot plus two goroutines per in-flight bounded close
	// worker (blocked Close + late-completion waiter).
	maxExpectedGrowth := startupProbeReadWorkerBudget + (2 * closeWorkerBudget) + 8 // +8 for runtime jitter.
	if goroutinesAfterAttempts > goroutinesBefore+maxExpectedGrowth {
		t.Fatalf(
			"probe-phase churn exceeded bounded goroutine ceiling: before=%d after=%d maxGrowth=%d",
			goroutinesBefore,
			goroutinesAfterAttempts,
			maxExpectedGrowth,
		)
	}
	startupProbeWorkerStatsAfterAttempts := startupProbeReadWorkerStatsSnapshot()
	inFlightDelta := startupProbeWorkerStatsAfterAttempts.InFlight - startupProbeWorkerStatsBefore.InFlight
	if inFlightDelta > startupProbeReadWorkerBudget {
		t.Fatalf(
			"startup probe detached read workers exceeded budget: before=%d after=%d budget=%d",
			startupProbeWorkerStatsBefore.InFlight,
			startupProbeWorkerStatsAfterAttempts.InFlight,
			startupProbeReadWorkerBudget,
		)
	}
	if startupProbeWorkerStatsAfterAttempts.Waits <= startupProbeWorkerStatsBefore.Waits {
		t.Fatalf(
			"startup probe detached read-worker waits did not increase under saturation: before=%d after=%d",
			startupProbeWorkerStatsBefore.Waits,
			startupProbeWorkerStatsAfterAttempts.Waits,
		)
	}
	if startupProbeWorkerStatsAfterAttempts.AcquireTimeouts <= startupProbeWorkerStatsBefore.AcquireTimeouts {
		t.Fatalf(
			"startup probe detached read-worker acquire timeouts did not increase under saturation: before=%d after=%d",
			startupProbeWorkerStatsBefore.AcquireTimeouts,
			startupProbeWorkerStatsAfterAttempts.AcquireTimeouts,
		)
	}

	expectedCloseCount := minInt(attempts, closeWorkerBudget)
	if closeCount := transport.closeCount(); closeCount < expectedCloseCount {
		t.Fatalf(
			"probe-phase churn did not start bounded close workers: closeCount=%d expected>=%d",
			closeCount,
			expectedCloseCount,
		)
	}
	stats := closeWithTimeoutStatsSnapshot()
	if int(stats.Started) > closeWorkerBudget {
		t.Fatalf("closeWithTimeout started=%d exceeds budget=%d", stats.Started, closeWorkerBudget)
	}
	if int(stats.Started) < expectedCloseCount {
		t.Fatalf("closeWithTimeout started=%d want >= %d", stats.Started, expectedCloseCount)
	}
	if stats.Timeouts > stats.Started {
		t.Fatalf("closeWithTimeout timeouts=%d exceeds started=%d", stats.Timeouts, stats.Started)
	}

	transport.releaseAll()
	time.Sleep(200 * time.Millisecond)
	goroutinesAfterRelease := waitForStableGoroutines()
	if goroutinesAfterRelease > goroutinesBefore+8 {
		t.Fatalf(
			"probe-phase cleanup goroutines remained after release: before=%d after=%d",
			goroutinesBefore,
			goroutinesAfterRelease,
		)
	}
	startupProbeWorkerStatsAfterRelease := startupProbeReadWorkerStatsSnapshot()
	if startupProbeWorkerStatsAfterRelease.InFlight > startupProbeWorkerStatsBefore.InFlight {
		t.Fatalf(
			"startup probe detached read workers remained after release: before=%d after=%d",
			startupProbeWorkerStatsBefore.InFlight,
			startupProbeWorkerStatsAfterRelease.InFlight,
		)
	}
}
