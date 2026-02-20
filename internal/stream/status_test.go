package stream

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

type probeCloseTelemetryTestProvider struct{}

func (p *probeCloseTelemetryTestProvider) GetByGuideNumber(_ context.Context, _ string) (channels.Channel, error) {
	return channels.Channel{}, channels.ErrChannelNotFound
}

func (p *probeCloseTelemetryTestProvider) ListEnabled(_ context.Context) ([]channels.Channel, error) {
	return nil, nil
}

func (p *probeCloseTelemetryTestProvider) ListSources(_ context.Context, _ int64, _ bool) ([]channels.Source, error) {
	return nil, nil
}

func (p *probeCloseTelemetryTestProvider) MarkSourceFailure(_ context.Context, _ int64, _ string, _ time.Time) error {
	return nil
}

func (p *probeCloseTelemetryTestProvider) MarkSourceSuccess(_ context.Context, _ int64, _ time.Time) error {
	return nil
}

func (p *probeCloseTelemetryTestProvider) UpdateSourceProfile(_ context.Context, _ int64, _ channels.SourceProfileUpdate) error {
	return nil
}

func TestDeriveTunerState(t *testing.T) {
	tests := []struct {
		name             string
		kind             string
		subscriberCount  int
		hasSharedSession bool
		want             string
	}{
		{
			name:             "probe lease",
			kind:             sessionKindProbe,
			subscriberCount:  0,
			hasSharedSession: false,
			want:             tunerStateProbe,
		},
		{
			name:             "client with subscribers",
			kind:             sessionKindClient,
			subscriberCount:  2,
			hasSharedSession: true,
			want:             tunerStateActiveSubscribers,
		},
		{
			name:             "client idle grace",
			kind:             sessionKindClient,
			subscriberCount:  0,
			hasSharedSession: true,
			want:             tunerStateIdleGraceNoSubscribers,
		},
		{
			name:             "client allocating",
			kind:             sessionKindClient,
			subscriberCount:  0,
			hasSharedSession: false,
			want:             tunerStateAllocatingSharedSession,
		},
		{
			name:             "unknown kind defaults to unknown",
			kind:             "mystery",
			subscriberCount:  0,
			hasSharedSession: false,
			want:             tunerStateUnknown,
		},
		{
			name:             "unknown kind with subscribers",
			kind:             "mystery",
			subscriberCount:  1,
			hasSharedSession: false,
			want:             tunerStateActiveSubscribers,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := deriveTunerState(tc.kind, tc.subscriberCount, tc.hasSharedSession)
			if got != tc.want {
				t.Fatalf("deriveTunerState(%q, %d, %t) = %q, want %q", tc.kind, tc.subscriberCount, tc.hasSharedSession, got, tc.want)
			}
		})
	}
}

func TestBuildChurnSummaryEmpty(t *testing.T) {
	summary := buildChurnSummary(nil)

	if got, want := summary.ReselectAlertThreshold, sameSourceReselectAlertThreshold; got != want {
		t.Fatalf("summary.reselect_alert_threshold = %d, want %d", got, want)
	}
	if got := summary.SessionCount; got != 0 {
		t.Fatalf("summary.session_count = %d, want 0", got)
	}
	if got := summary.TotalRecoveryCycles; got != 0 {
		t.Fatalf("summary.total_recovery_cycles = %d, want 0", got)
	}
}

func TestBuildChurnSummaryAggregates(t *testing.T) {
	summary := buildChurnSummary([]SharedSessionStats{
		{
			ChannelID:               7,
			GuideNumber:             "107",
			RecoveryCycle:           5,
			RecoveryReason:          "stall",
			SourceSelectCount:       11,
			SameSourceReselectCount: 4,
		},
		{
			ChannelID:               8,
			GuideNumber:             "108",
			RecoveryCycle:           1,
			RecoveryReason:          "source_eof",
			SourceSelectCount:       2,
			SameSourceReselectCount: 1,
		},
		{
			ChannelID:               9,
			GuideNumber:             "109",
			RecoveryCycle:           0,
			RecoveryReason:          "",
			SourceSelectCount:       1,
			SameSourceReselectCount: 0,
		},
	})

	if got, want := summary.SessionCount, 3; got != want {
		t.Fatalf("summary.session_count = %d, want %d", got, want)
	}
	if got, want := summary.RecoveringSessionCount, 2; got != want {
		t.Fatalf("summary.recovering_session_count = %d, want %d", got, want)
	}
	if got, want := summary.SessionsWithReselectCount, 2; got != want {
		t.Fatalf("summary.sessions_with_reselect_count = %d, want %d", got, want)
	}
	if got, want := summary.SessionsOverReselectThreshold, 1; got != want {
		t.Fatalf("summary.sessions_over_reselect_threshold = %d, want %d", got, want)
	}
	if got, want := summary.TotalRecoveryCycles, int64(6); got != want {
		t.Fatalf("summary.total_recovery_cycles = %d, want %d", got, want)
	}
	if got, want := summary.TotalSourceSelectCount, int64(14); got != want {
		t.Fatalf("summary.total_source_select_count = %d, want %d", got, want)
	}
	if got, want := summary.TotalSameSourceReselectCount, int64(5); got != want {
		t.Fatalf("summary.total_same_source_reselect_count = %d, want %d", got, want)
	}
	if got, want := summary.MaxSameSourceReselectCount, int64(4); got != want {
		t.Fatalf("summary.max_same_source_reselect_count = %d, want %d", got, want)
	}
	if got, want := summary.MaxReselectChannelID, int64(7); got != want {
		t.Fatalf("summary.max_reselect_channel_id = %d, want %d", got, want)
	}
	if got, want := summary.MaxReselectGuideNumber, "107"; got != want {
		t.Fatalf("summary.max_reselect_guide_number = %q, want %q", got, want)
	}
}

func TestTunerStatusSnapshotIncludesDrainWaitTelemetry(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	manager := &SessionManager{}
	startedAt := time.Now().Add(-40 * time.Millisecond)
	if err := manager.recordDrainWaitResult(startedAt, nil); err != nil {
		t.Fatalf("recordDrainWaitResult(success) error = %v, want nil", err)
	}
	if err := manager.recordDrainWaitResult(startedAt, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("recordDrainWaitResult(error) error = %v, want context canceled", err)
	}

	handler := &Handler{
		tuners:   NewPool(1),
		sessions: &SessionManager{},
	}
	snapshot := handler.TunerStatusSnapshot()
	if got, want := snapshot.DrainWait.OK, uint64(1); got != want {
		t.Fatalf("snapshot.drain_wait.ok = %d, want %d", got, want)
	}
	if got, want := snapshot.DrainWait.Error, uint64(1); got != want {
		t.Fatalf("snapshot.drain_wait.error = %d, want %d", got, want)
	}
	if got := snapshot.DrainWait.WaitDurationUS; got == 0 {
		t.Fatalf("snapshot.drain_wait.wait_duration_us = %d, want > 0", got)
	}
	if got := snapshot.DrainWait.WaitDurationMS; got == 0 {
		t.Fatalf("snapshot.drain_wait.wait_duration_ms = %d, want > 0", got)
	}
}

func TestTunerStatusSnapshotIncludesProbeCloseTelemetry(t *testing.T) {
	baseline, _ := captureProbeCloseStatsForTest(t)

	atomic.AddUint64(&probeCloseInlineCount, 3)
	atomic.AddUint64(&probeCloseQueueFullCount, 2)

	handler := &Handler{
		tuners:   NewPool(1),
		sessions: &SessionManager{},
	}
	snapshot := handler.TunerStatusSnapshot()
	if got, want := saturatingCounterDelta(snapshot.ProbeClose.InlineCount, baseline.InlineCount), uint64(3); got != want {
		t.Fatalf("snapshot.probe_close.inline_count = %d, want %d", got, want)
	}
	if got, want := saturatingCounterDelta(snapshot.ProbeClose.QueueFullCount, baseline.QueueFullCount), uint64(2); got != want {
		t.Fatalf("snapshot.probe_close.queue_full_count = %d, want %d", got, want)
	}
}

func TestTunerStatusSnapshotIncludesProbeCloseTelemetryFromEnqueuePath(t *testing.T) {
	baseline, deltaStats := captureProbeCloseStatsForTest(t)

	provider := &probeCloseTelemetryTestProvider{}
	handler := NewHandler(Config{Mode: "direct"}, NewPool(1), provider)
	prober := NewBackgroundProber(ProberConfig{
		Mode:                 "direct",
		MinProbeBytes:        1,
		ProbeTimeout:         1 * time.Second,
		ProbeCloseQueueDepth: 1,
	}, provider)

	blockCh := make(chan struct{})
	releaseBlockedClose := func() {
		select {
		case <-blockCh:
		default:
			close(blockCh)
		}
	}

	t.Cleanup(func() {
		releaseBlockedClose()
		prober.Close()
		_ = handler.CloseWithContext(context.Background())
	})

	sessionCloseStarted := make(chan struct{})
	prober.enqueueProbeSessionClose(10, &streamSession{
		closeFn: func() error {
			close(sessionCloseStarted)
			<-blockCh
			return nil
		},
	})
	select {
	case <-sessionCloseStarted:
	case <-time.After(time.Second):
		t.Fatal("background prober close worker did not start blocked session close")
	}

	// Fill the only queue slot so the next enqueue is forced through inline
	// fallback and increments both inline and queue_full counters.
	prober.enqueueProbeSessionClose(11, &streamSession{closeFn: func() error { return nil }})

	overflowClosed := make(chan struct{})
	prober.enqueueProbeSessionClose(12, &streamSession{
		closeFn: func() error {
			close(overflowClosed)
			return nil
		},
	})
	select {
	case <-overflowClosed:
	case <-time.After(time.Second):
		t.Fatal("overflow enqueue did not close session inline")
	}

	releaseBlockedClose()
	prober.Close()

	afterCloseInline := make(chan struct{})
	prober.enqueueProbeSessionClose(13, &streamSession{
		closeFn: func() error {
			close(afterCloseInline)
			return nil
		},
	})
	select {
	case <-afterCloseInline:
	case <-time.After(time.Second):
		t.Fatal("enqueue after Close() did not execute inline fallback close")
	}

	delta := deltaStats()
	if got, want := delta.InlineCount, uint64(2); got != want {
		t.Fatalf("probe close inline delta = %d, want %d", got, want)
	}
	if got, want := delta.QueueFullCount, uint64(1); got != want {
		t.Fatalf("probe close queue_full delta = %d, want %d", got, want)
	}

	snapshot := handler.TunerStatusSnapshot()
	if got, want := saturatingCounterDelta(snapshot.ProbeClose.InlineCount, baseline.InlineCount), delta.InlineCount; got != want {
		t.Fatalf("snapshot.probe_close.inline_count delta = %d, want %d", got, want)
	}
	if got, want := saturatingCounterDelta(snapshot.ProbeClose.QueueFullCount, baseline.QueueFullCount), delta.QueueFullCount; got != want {
		t.Fatalf("snapshot.probe_close.queue_full_count delta = %d, want %d", got, want)
	}
}

func TestDrainWaitTelemetryJSONIncludesMicrosecondsWhenZero(t *testing.T) {
	payload := DrainWaitTelemetry{
		OK:             2,
		Error:          1,
		WaitDurationUS: 0,
		WaitDurationMS: 0,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(payload) error = %v, want nil", err)
	}
	jsonText := string(body)
	if !strings.Contains(jsonText, `"wait_duration_us":0`) {
		t.Fatalf("drain telemetry JSON missing zero wait_duration_us field: %s", jsonText)
	}
	if !strings.Contains(jsonText, `"wait_duration_ms":0`) {
		t.Fatalf("drain telemetry JSON missing wait_duration_ms field: %s", jsonText)
	}
}

func TestTunerStatusSnapshotJSONIncludesDrainWaitWhenZero(t *testing.T) {
	payload := TunerStatusSnapshot{
		DrainWait: DrainWaitTelemetry{},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(payload) error = %v, want nil", err)
	}
	jsonText := string(body)
	if !strings.Contains(jsonText, `"drain_wait":`) {
		t.Fatalf("tuner snapshot JSON missing drain_wait field: %s", jsonText)
	}
	if !strings.Contains(jsonText, `"wait_duration_us":0`) {
		t.Fatalf("tuner snapshot JSON missing zero drain_wait.wait_duration_us field: %s", jsonText)
	}
	if !strings.Contains(jsonText, `"wait_duration_ms":0`) {
		t.Fatalf("tuner snapshot JSON missing zero drain_wait.wait_duration_ms field: %s", jsonText)
	}
}

func TestApplySharedSessionStatusCopiesStreamDetails(t *testing.T) {
	dst := &TunerStatus{}
	startedAt := time.Unix(1_770_000_000, 0).UTC()
	stoppedAt := startedAt.Add(1500 * time.Millisecond)
	session := SharedSessionStats{
		SourceStartupProbeRawBytes:           2048,
		SourceStartupProbeTrimmedBytes:       512,
		SourceStartupProbeCutoverOffset:      1536,
		SourceStartupProbeDroppedBytes:       1536,
		SourceStartupProbeBytes:              512,
		Resolution:                           "1920x1080",
		FrameRate:                            59.94,
		VideoCodec:                           "h264",
		AudioCodec:                           "aac",
		CurrentBitrateBPS:                    4_500_000,
		ProfileBitrateBPS:                    4_200_000,
		SlowSkipEventsTotal:                  9,
		SlowSkipLagChunksTotal:               72,
		SlowSkipLagBytesTotal:                524288,
		SlowSkipMaxLagChunks:                 16,
		SubscriberWriteDeadlineTimeoutsTotal: 3,
		SubscriberWriteShortWritesTotal:      5,
		SubscriberWriteBlockedDurationUS:     24500,
		SubscriberWriteBlockedDurationMS:     24,
		RecoveryKeepaliveStartedAt:           startedAt,
		RecoveryKeepaliveStoppedAt:           stoppedAt,
		RecoveryKeepaliveDuration:            "1.5s",
		RecoveryKeepaliveBytes:               123456,
		RecoveryKeepaliveChunks:              789,
		RecoveryKeepaliveRateBytesPerSecond:  54321.5,
		RecoveryKeepaliveExpectedRate:        50000,
		RecoveryKeepaliveRealtimeMultiplier:  1.08,
		RecoveryKeepaliveGuardrailCount:      2,
		RecoveryKeepaliveGuardrailReason:     "rate_bytes_per_second=54321 exceeds_limit=50000",
		SourceHealthPersistCoalescedTotal:    7,
		SourceHealthPersistDroppedTotal:      2,
		SourceHealthPersistCoalescedBySource: map[int64]int64{10: 5, 11: 2},
		SourceHealthPersistDroppedBySource:   map[int64]int64{10: 1, 12: 1},
	}

	applySharedSessionStatus(dst, session)

	if got, want := dst.SourceStartupProbeRawBytes, 2048; got != want {
		t.Fatalf("dst.source_startup_probe_raw_bytes = %d, want %d", got, want)
	}
	if got, want := dst.SourceStartupProbeTrimmedBytes, 512; got != want {
		t.Fatalf("dst.source_startup_probe_trimmed_bytes = %d, want %d", got, want)
	}
	if got, want := dst.SourceStartupProbeCutoverOffset, 1536; got != want {
		t.Fatalf("dst.source_startup_probe_cutover_offset = %d, want %d", got, want)
	}
	if got, want := dst.SourceStartupProbeDroppedBytes, 1536; got != want {
		t.Fatalf("dst.source_startup_probe_dropped_bytes = %d, want %d", got, want)
	}
	if got, want := dst.SourceStartupProbeBytes, 512; got != want {
		t.Fatalf("dst.source_startup_probe_bytes = %d, want %d", got, want)
	}
	if got, want := dst.Resolution, "1920x1080"; got != want {
		t.Fatalf("dst.resolution = %q, want %q", got, want)
	}
	if got, want := dst.FrameRate, 59.94; got != want {
		t.Fatalf("dst.frame_rate = %f, want %f", got, want)
	}
	if got, want := dst.VideoCodec, "h264"; got != want {
		t.Fatalf("dst.video_codec = %q, want %q", got, want)
	}
	if got, want := dst.AudioCodec, "aac"; got != want {
		t.Fatalf("dst.audio_codec = %q, want %q", got, want)
	}
	if got, want := dst.CurrentBitrateBPS, int64(4_500_000); got != want {
		t.Fatalf("dst.current_bitrate_bps = %d, want %d", got, want)
	}
	if got, want := dst.ProfileBitrateBPS, int64(4_200_000); got != want {
		t.Fatalf("dst.profile_bitrate_bps = %d, want %d", got, want)
	}
	if got, want := dst.SlowSkipEventsTotal, uint64(9); got != want {
		t.Fatalf("dst.slow_skip_events_total = %d, want %d", got, want)
	}
	if got, want := dst.SlowSkipLagChunksTotal, uint64(72); got != want {
		t.Fatalf("dst.slow_skip_lag_chunks_total = %d, want %d", got, want)
	}
	if got, want := dst.SlowSkipLagBytesTotal, uint64(524288); got != want {
		t.Fatalf("dst.slow_skip_lag_bytes_total = %d, want %d", got, want)
	}
	if got, want := dst.SlowSkipMaxLagChunks, uint64(16); got != want {
		t.Fatalf("dst.slow_skip_max_lag_chunks = %d, want %d", got, want)
	}
	if got, want := dst.SubscriberWriteDeadlineTimeoutsTotal, uint64(3); got != want {
		t.Fatalf("dst.subscriber_write_deadline_timeouts_total = %d, want %d", got, want)
	}
	if got, want := dst.SubscriberWriteShortWritesTotal, uint64(5); got != want {
		t.Fatalf("dst.subscriber_write_short_writes_total = %d, want %d", got, want)
	}
	if got, want := dst.SubscriberWriteBlockedDurationUS, uint64(24500); got != want {
		t.Fatalf("dst.subscriber_write_blocked_duration_us = %d, want %d", got, want)
	}
	if got, want := dst.SubscriberWriteBlockedDurationMS, uint64(24); got != want {
		t.Fatalf("dst.subscriber_write_blocked_duration_ms = %d, want %d", got, want)
	}
	if got, want := dst.RecoveryKeepaliveStartedAt, startedAt; got != want {
		t.Fatalf("dst.recovery_keepalive_started_at = %v, want %v", got, want)
	}
	if got, want := dst.RecoveryKeepaliveStoppedAt, stoppedAt; got != want {
		t.Fatalf("dst.recovery_keepalive_stopped_at = %v, want %v", got, want)
	}
	if got, want := dst.RecoveryKeepaliveDuration, "1.5s"; got != want {
		t.Fatalf("dst.recovery_keepalive_duration = %q, want %q", got, want)
	}
	if got, want := dst.RecoveryKeepaliveBytes, int64(123456); got != want {
		t.Fatalf("dst.recovery_keepalive_bytes = %d, want %d", got, want)
	}
	if got, want := dst.RecoveryKeepaliveChunks, int64(789); got != want {
		t.Fatalf("dst.recovery_keepalive_chunks = %d, want %d", got, want)
	}
	if got, want := dst.RecoveryKeepaliveRateBytesPerSecond, 54321.5; got != want {
		t.Fatalf("dst.recovery_keepalive_rate_bytes_per_second = %f, want %f", got, want)
	}
	if got, want := dst.RecoveryKeepaliveExpectedRate, 50000.0; got != want {
		t.Fatalf("dst.recovery_keepalive_expected_rate = %f, want %f", got, want)
	}
	if got, want := dst.RecoveryKeepaliveRealtimeMultiplier, 1.08; got != want {
		t.Fatalf("dst.recovery_keepalive_realtime_multiplier = %f, want %f", got, want)
	}
	if got, want := dst.RecoveryKeepaliveGuardrailCount, int64(2); got != want {
		t.Fatalf("dst.recovery_keepalive_guardrail_count = %d, want %d", got, want)
	}
	if got, want := dst.RecoveryKeepaliveGuardrailReason, "rate_bytes_per_second=54321 exceeds_limit=50000"; got != want {
		t.Fatalf("dst.recovery_keepalive_guardrail_reason = %q, want %q", got, want)
	}
	if got, want := dst.SourceHealthPersistCoalescedTotal, int64(7); got != want {
		t.Fatalf("dst.source_health_persist_coalesced_total = %d, want %d", got, want)
	}
	if got, want := dst.SourceHealthPersistDroppedTotal, int64(2); got != want {
		t.Fatalf("dst.source_health_persist_dropped_total = %d, want %d", got, want)
	}
	if got, want := dst.SourceHealthPersistCoalescedBySource[10], int64(5); got != want {
		t.Fatalf("dst.source_health_persist_coalesced_by_source[10] = %d, want %d", got, want)
	}
	if got, want := dst.SourceHealthPersistDroppedBySource[12], int64(1); got != want {
		t.Fatalf("dst.source_health_persist_dropped_by_source[12] = %d, want %d", got, want)
	}

	session.SourceHealthPersistCoalescedBySource[10] = 999
	session.SourceHealthPersistDroppedBySource[12] = 999
	if got, want := dst.SourceHealthPersistCoalescedBySource[10], int64(5); got != want {
		t.Fatalf("dst.source_health_persist_coalesced_by_source should be copied; got %d, want %d", got, want)
	}
	if got, want := dst.SourceHealthPersistDroppedBySource[12], int64(1); got != want {
		t.Fatalf("dst.source_health_persist_dropped_by_source should be copied; got %d, want %d", got, want)
	}
}

func TestTunerStatusSnapshotSanitizesSourceStreamURL(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		want  string
		block []string
	}{
		{
			name:  "credential and token URL",
			raw:   "https://user:pass@example.com/live/stream.m3u8?token=abc123&expires=456#frag",
			want:  "https://example.com/live/stream.m3u8",
			block: []string{"user:pass@", "token=abc123", "#frag"},
		},
		{
			name:  "fallback malformed query escapes",
			raw:   "http://user:secret@example.org/live/index.m3u8?sig=%ZZ",
			want:  "http://example.org/live/index.m3u8",
			block: []string{"user:secret@", "sig=%ZZ"},
		},
		{
			name:  "already clean URL preserved",
			raw:   "http://example.net/channels/news.ts",
			want:  "http://example.net/channels/news.ts",
			block: []string{"?"},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			startedAt := time.Unix(1_770_000_000, 0).UTC()
			session := &sharedRuntimeSession{
				channel: channels.Channel{
					ChannelID:   42,
					GuideNumber: "142",
					GuideName:   "Test Channel",
				},
				lease:           &Lease{ID: 0},
				sourceID:        7,
				sourceItemKey:   "src:test:primary",
				sourceStreamURL: tc.raw,
				subscribers: map[uint64]SubscriberStats{
					1: {
						SubscriberID: 1,
						ClientAddr:   "192.168.1.100:50000",
						StartedAt:    startedAt,
					},
				},
			}

			handler := &Handler{
				tuners: NewPool(2),
				sessions: &SessionManager{
					sessions: map[int64]*sharedRuntimeSession{
						42: session,
					},
				},
			}

			snapshot := handler.TunerStatusSnapshot()
			if got := len(snapshot.Tuners); got != 1 {
				t.Fatalf("len(snapshot.tuners) = %d, want 1", got)
			}
			if got := len(snapshot.ClientStreams); got != 1 {
				t.Fatalf("len(snapshot.client_streams) = %d, want 1", got)
			}

			tunerURL := snapshot.Tuners[0].SourceStreamURL
			if tunerURL != tc.want {
				t.Fatalf("snapshot.tuners[0].source_stream_url = %q, want %q", tunerURL, tc.want)
			}
			clientURL := snapshot.ClientStreams[0].SourceStreamURL
			if clientURL != tc.want {
				t.Fatalf("snapshot.client_streams[0].source_stream_url = %q, want %q", clientURL, tc.want)
			}
			for _, blocked := range tc.block {
				if blocked == "" {
					continue
				}
				if strings.Contains(tunerURL, blocked) {
					t.Fatalf("snapshot.tuners[0].source_stream_url leaked %q in %q", blocked, tunerURL)
				}
				if strings.Contains(clientURL, blocked) {
					t.Fatalf("snapshot.client_streams[0].source_stream_url leaked %q in %q", blocked, clientURL)
				}
			}
		})
	}
}

func TestTunerStatusSnapshotIncludesSessionHistory(t *testing.T) {
	now := time.Unix(1_770_000_000, 0).UTC()
	activeSourceURL := "https://user:pass@example.com/live/active.ts?token=abc123"
	closedSourceURL := "http://token-user:secret@example.net/live/closed.ts?sig=xyz"

	active := &sharedRuntimeSession{
		channel: channels.Channel{
			ChannelID:   42,
			GuideNumber: "142",
			GuideName:   "Active Session",
		},
		lease:           &Lease{ID: 1},
		closed:          false,
		sourceID:        10,
		sourceItemKey:   "src:active",
		sourceStreamURL: activeSourceURL,
		startedAt:       now.Add(-2 * time.Minute),
		subscribers: map[uint64]SubscriberStats{
			1: {
				SubscriberID: 1,
				ClientAddr:   "192.168.1.110:50000",
				StartedAt:    now.Add(-90 * time.Second),
			},
		},
		historySessionID:                 200,
		historyPeakSubscribers:           1,
		historyTotalSubscribers:          1,
		historyCompletedSubscribers:      0,
		slowSkipEventsTotal:              4,
		slowSkipLagChunksTotal:           19,
		slowSkipLagBytesTotal:            12288,
		slowSkipMaxLagChunks:             7,
		subscriberWriteDeadlineTimeouts:  2,
		subscriberWriteShortWrites:       3,
		subscriberWriteBlockedDurationUS: 9600,
		historyCurrentSourceIdx:          0,
		historySources: []SharedSessionSourceHistory{
			{
				SourceID:   10,
				ItemKey:    "src:active",
				StreamURL:  activeSourceURL,
				SelectedAt: now.Add(-110 * time.Second),
			},
		},
		historySubscribers: []SharedSessionSubscriberHistory{
			{
				SubscriberID: 1,
				ClientAddr:   "192.168.1.110:50000",
				ConnectedAt:  now.Add(-90 * time.Second),
			},
		},
		historySourcesTruncated:     4,
		historySubscribersTruncated: 2,
	}

	manager := &SessionManager{
		cfg: sessionManagerConfig{
			sessionHistoryLimit:           2,
			sessionSourceHistoryLimit:     5,
			sessionSubscriberHistoryLimit: 7,
		},
		sessions: map[int64]*sharedRuntimeSession{
			42: active,
		},
		sessionHistory: []SharedSessionHistory{
			{
				SessionID:                            100,
				ChannelID:                            11,
				GuideNumber:                          "111",
				GuideName:                            "Closed Session",
				OpenedAt:                             now.Add(-10 * time.Minute),
				ClosedAt:                             now.Add(-8 * time.Minute),
				Active:                               false,
				SlowSkipEventsTotal:                  6,
				SlowSkipLagChunksTotal:               31,
				SlowSkipLagBytesTotal:                20480,
				SlowSkipMaxLagChunks:                 9,
				SubscriberWriteDeadlineTimeoutsTotal: 4,
				SubscriberWriteShortWritesTotal:      8,
				SubscriberWriteBlockedDurationUS:     18000,
				SubscriberWriteBlockedDurationMS:     18,
				Sources: []SharedSessionSourceHistory{
					{
						SourceID:   8,
						ItemKey:    "src:closed",
						StreamURL:  closedSourceURL,
						SelectedAt: now.Add(-9 * time.Minute),
					},
				},
				SourceHistoryLimit:         9,
				SourceHistoryTruncated:     1,
				SubscriberHistoryLimit:     12,
				SubscriberHistoryTruncated: 3,
			},
		},
		sessionHistoryTruncatedCount: 3,
	}
	active.manager = manager

	handler := &Handler{
		tuners:   NewPool(2),
		sessions: manager,
	}

	snapshot := handler.TunerStatusSnapshot()
	if got, want := snapshot.SessionHistoryLimit, 2; got != want {
		t.Fatalf("snapshot.session_history_limit = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistoryTruncatedCount, int64(3); got != want {
		t.Fatalf("snapshot.session_history_truncated_count = %d, want %d", got, want)
	}
	if got, want := len(snapshot.SessionHistory), 2; got != want {
		t.Fatalf("len(snapshot.session_history) = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SessionID, uint64(200); got != want {
		t.Fatalf("snapshot.session_history[0].session_id = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SessionID, uint64(100); got != want {
		t.Fatalf("snapshot.session_history[1].session_id = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SourceHistoryLimit, 5; got != want {
		t.Fatalf("snapshot.session_history[0].source_history_limit = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SourceHistoryTruncated, int64(4); got != want {
		t.Fatalf("snapshot.session_history[0].source_history_truncated_count = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SubscriberHistoryLimit, 7; got != want {
		t.Fatalf("snapshot.session_history[0].subscriber_history_limit = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SubscriberHistoryTruncated, int64(2); got != want {
		t.Fatalf("snapshot.session_history[0].subscriber_history_truncated_count = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SourceHistoryLimit, 9; got != want {
		t.Fatalf("snapshot.session_history[1].source_history_limit = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SourceHistoryTruncated, int64(1); got != want {
		t.Fatalf("snapshot.session_history[1].source_history_truncated_count = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SubscriberHistoryLimit, 12; got != want {
		t.Fatalf("snapshot.session_history[1].subscriber_history_limit = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SubscriberHistoryTruncated, int64(3); got != want {
		t.Fatalf("snapshot.session_history[1].subscriber_history_truncated_count = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SlowSkipEventsTotal, uint64(4); got != want {
		t.Fatalf("snapshot.session_history[0].slow_skip_events_total = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SlowSkipLagChunksTotal, uint64(19); got != want {
		t.Fatalf("snapshot.session_history[0].slow_skip_lag_chunks_total = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SlowSkipLagBytesTotal, uint64(12288); got != want {
		t.Fatalf("snapshot.session_history[0].slow_skip_lag_bytes_total = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SlowSkipMaxLagChunks, uint64(7); got != want {
		t.Fatalf("snapshot.session_history[0].slow_skip_max_lag_chunks = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SubscriberWriteDeadlineTimeoutsTotal, uint64(2); got != want {
		t.Fatalf("snapshot.session_history[0].subscriber_write_deadline_timeouts_total = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SubscriberWriteShortWritesTotal, uint64(3); got != want {
		t.Fatalf("snapshot.session_history[0].subscriber_write_short_writes_total = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SubscriberWriteBlockedDurationUS, uint64(9600); got != want {
		t.Fatalf("snapshot.session_history[0].subscriber_write_blocked_duration_us = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[0].SubscriberWriteBlockedDurationMS, uint64(9); got != want {
		t.Fatalf("snapshot.session_history[0].subscriber_write_blocked_duration_ms = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SlowSkipEventsTotal, uint64(6); got != want {
		t.Fatalf("snapshot.session_history[1].slow_skip_events_total = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SlowSkipLagChunksTotal, uint64(31); got != want {
		t.Fatalf("snapshot.session_history[1].slow_skip_lag_chunks_total = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SlowSkipLagBytesTotal, uint64(20480); got != want {
		t.Fatalf("snapshot.session_history[1].slow_skip_lag_bytes_total = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SlowSkipMaxLagChunks, uint64(9); got != want {
		t.Fatalf("snapshot.session_history[1].slow_skip_max_lag_chunks = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SubscriberWriteDeadlineTimeoutsTotal, uint64(4); got != want {
		t.Fatalf("snapshot.session_history[1].subscriber_write_deadline_timeouts_total = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SubscriberWriteShortWritesTotal, uint64(8); got != want {
		t.Fatalf("snapshot.session_history[1].subscriber_write_short_writes_total = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SubscriberWriteBlockedDurationUS, uint64(18000); got != want {
		t.Fatalf("snapshot.session_history[1].subscriber_write_blocked_duration_us = %d, want %d", got, want)
	}
	if got, want := snapshot.SessionHistory[1].SubscriberWriteBlockedDurationMS, uint64(18); got != want {
		t.Fatalf("snapshot.session_history[1].subscriber_write_blocked_duration_ms = %d, want %d", got, want)
	}

	for i, session := range snapshot.SessionHistory {
		for j, source := range session.Sources {
			if strings.Contains(source.StreamURL, "user:pass@") || strings.Contains(source.StreamURL, "secret@") {
				t.Fatalf("snapshot.session_history[%d].sources[%d].stream_url leaked credentials: %q", i, j, source.StreamURL)
			}
			if strings.Contains(source.StreamURL, "token=") || strings.Contains(source.StreamURL, "sig=") {
				t.Fatalf("snapshot.session_history[%d].sources[%d].stream_url leaked query token: %q", i, j, source.StreamURL)
			}
		}
	}
}
