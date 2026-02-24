package stream

import (
	"sort"
	"strings"
	"time"
)

const (
	tunerStateActiveSubscribers       = "active_subscribers"
	tunerStateIdleGraceNoSubscribers  = "idle_grace_no_subscribers"
	tunerStateProbe                   = "probe"
	tunerStateAllocatingSharedSession = "allocating_session"
	tunerStateUnknown                 = "unknown"
)

// TunerStatusSnapshot reports current tuner/session runtime state for admin diagnostics.
type TunerStatusSnapshot struct {
	GeneratedAt                  time.Time              `json:"generated_at"`
	TunerCount                   int                    `json:"tuner_count"`
	InUseCount                   int                    `json:"in_use_count"`
	IdleCount                    int                    `json:"idle_count"`
	Churn                        ChurnSummary           `json:"churn"`
	DrainWait                    DrainWaitTelemetry     `json:"drain_wait"`
	ProbeClose                   ProbeCloseTelemetry    `json:"probe_close"`
	Tuners                       []TunerStatus          `json:"tuners"`
	ClientStreams                []ClientStreamStatus   `json:"client_streams"`
	SessionHistory               []SharedSessionHistory `json:"session_history,omitempty"`
	SessionHistoryLimit          int                    `json:"session_history_limit,omitempty"`
	SessionHistoryTruncatedCount int64                  `json:"session_history_truncated_count,omitempty"`
}

// ChurnSummary captures high-level recovery/reselection churn counters across active shared sessions.
type ChurnSummary struct {
	ReselectAlertThreshold        int64  `json:"reselect_alert_threshold"`
	SessionCount                  int    `json:"session_count"`
	RecoveringSessionCount        int    `json:"recovering_session_count"`
	SessionsWithReselectCount     int    `json:"sessions_with_reselect_count"`
	SessionsOverReselectThreshold int    `json:"sessions_over_reselect_threshold"`
	TotalRecoveryCycles           int64  `json:"total_recovery_cycles"`
	TotalSourceSelectCount        int64  `json:"total_source_select_count"`
	TotalSameSourceReselectCount  int64  `json:"total_same_source_reselect_count"`
	MaxSameSourceReselectCount    int64  `json:"max_same_source_reselect_count"`
	MaxReselectChannelID          int64  `json:"max_reselect_channel_id,omitempty"`
	MaxReselectGuideNumber        string `json:"max_reselect_guide_number,omitempty"`
}

// DrainWaitTelemetry captures process-lifetime WaitForDrain result counters.
type DrainWaitTelemetry struct {
	OK             uint64 `json:"ok"`
	Error          uint64 `json:"error"`
	WaitDurationUS uint64 `json:"wait_duration_us"`
	WaitDurationMS uint64 `json:"wait_duration_ms"`
}

// ProbeCloseTelemetry captures process-lifetime background-prober close fallback counters.
type ProbeCloseTelemetry struct {
	InlineCount    uint64 `json:"inline_count"`
	QueueFullCount uint64 `json:"queue_full_count"`
}

// TunerStatus describes one active tuner lease and linked shared-session state.
type TunerStatus struct {
	TunerID                              int               `json:"tuner_id"`
	Kind                                 string            `json:"kind"`
	State                                string            `json:"state"`
	GuideNumber                          string            `json:"guide_number,omitempty"`
	LeaseClientAddr                      string            `json:"lease_client_addr,omitempty"`
	LeaseStartedAt                       time.Time         `json:"lease_started_at"`
	ChannelID                            int64             `json:"channel_id,omitempty"`
	GuideName                            string            `json:"guide_name,omitempty"`
	SourceID                             int64             `json:"source_id,omitempty"`
	SourceItemKey                        string            `json:"source_item_key,omitempty"`
	SourceStreamURL                      string            `json:"source_stream_url,omitempty"`
	SourceStartupProbeRawBytes           int               `json:"source_startup_probe_raw_bytes,omitempty"`
	SourceStartupProbeTrimmedBytes       int               `json:"source_startup_probe_trimmed_bytes,omitempty"`
	SourceStartupProbeCutoverOffset      int               `json:"source_startup_probe_cutover_offset,omitempty"`
	SourceStartupProbeDroppedBytes       int               `json:"source_startup_probe_dropped_bytes,omitempty"`
	SourceStartupProbeBytes              int               `json:"source_startup_probe_bytes,omitempty"`
	SourceStartupRandomAccessReady       bool              `json:"source_startup_random_access_ready,omitempty"`
	SourceStartupRandomAccessCodec       string            `json:"source_startup_random_access_codec,omitempty"`
	SourceStartupInventoryMethod         string            `json:"source_startup_inventory_method,omitempty"`
	SourceStartupVideoStreams            int               `json:"source_startup_video_streams,omitempty"`
	SourceStartupAudioStreams            int               `json:"source_startup_audio_streams,omitempty"`
	SourceStartupVideoCodecs             string            `json:"source_startup_video_codecs,omitempty"`
	SourceStartupAudioCodecs             string            `json:"source_startup_audio_codecs,omitempty"`
	SourceStartupComponentState          string            `json:"source_startup_component_state,omitempty"`
	SourceStartupRetryRelaxedProbe       bool              `json:"source_startup_retry_relaxed_probe,omitempty"`
	SourceStartupRetryRelaxedProbeReason string            `json:"source_startup_retry_relaxed_probe_reason,omitempty"`
	Resolution                           string            `json:"resolution,omitempty"`
	FrameRate                            float64           `json:"frame_rate,omitempty"`
	VideoCodec                           string            `json:"video_codec,omitempty"`
	AudioCodec                           string            `json:"audio_codec,omitempty"`
	CurrentBitrateBPS                    int64             `json:"current_bitrate_bps,omitempty"`
	ProfileBitrateBPS                    int64             `json:"profile_bitrate_bps,omitempty"`
	Producer                             string            `json:"producer,omitempty"`
	SessionStartedAt                     time.Time         `json:"session_started_at"`
	LastByteAt                           time.Time         `json:"last_byte_at"`
	LastPushAt                           time.Time         `json:"last_push_at"`
	BytesRead                            int64             `json:"bytes_read"`
	BytesPushed                          int64             `json:"bytes_pushed"`
	ChunksPushed                         int64             `json:"chunks_pushed"`
	SlowSkipEventsTotal                  uint64            `json:"slow_skip_events_total"`
	SlowSkipLagChunksTotal               uint64            `json:"slow_skip_lag_chunks_total"`
	SlowSkipLagBytesTotal                uint64            `json:"slow_skip_lag_bytes_total"`
	SlowSkipMaxLagChunks                 uint64            `json:"slow_skip_max_lag_chunks"`
	SubscriberWriteDeadlineTimeoutsTotal uint64            `json:"subscriber_write_deadline_timeouts_total"`
	SubscriberWriteShortWritesTotal      uint64            `json:"subscriber_write_short_writes_total"`
	SubscriberWriteBlockedDurationUS     uint64            `json:"subscriber_write_blocked_duration_us"`
	SubscriberWriteBlockedDurationMS     uint64            `json:"subscriber_write_blocked_duration_ms"`
	StallCount                           int64             `json:"stall_count"`
	RecoveryCycle                        int64             `json:"recovery_cycle"`
	RecoveryReason                       string            `json:"recovery_reason,omitempty"`
	RecoveryTransitionMode               string            `json:"recovery_transition_mode,omitempty"`
	RecoveryTransitionEffectiveMode      string            `json:"recovery_transition_effective_mode,omitempty"`
	RecoveryTransitionSignalsApplied     string            `json:"recovery_transition_signals_applied,omitempty"`
	RecoveryTransitionSignalSkips        string            `json:"recovery_transition_signal_skips,omitempty"`
	RecoveryTransitionFallbackCount      int64             `json:"recovery_transition_fallback_count,omitempty"`
	RecoveryTransitionFallbackReason     string            `json:"recovery_transition_fallback_reason,omitempty"`
	RecoveryTransitionStitchApplied      bool              `json:"recovery_transition_stitch_applied,omitempty"`
	RecoveryKeepaliveMode                string            `json:"recovery_keepalive_mode,omitempty"`
	RecoveryKeepaliveFallbackCount       int64             `json:"recovery_keepalive_fallback_count,omitempty"`
	RecoveryKeepaliveFallbackReason      string            `json:"recovery_keepalive_fallback_reason,omitempty"`
	RecoveryKeepaliveStartedAt           time.Time         `json:"recovery_keepalive_started_at,omitempty"`
	RecoveryKeepaliveStoppedAt           time.Time         `json:"recovery_keepalive_stopped_at,omitempty"`
	RecoveryKeepaliveDuration            string            `json:"recovery_keepalive_duration,omitempty"`
	RecoveryKeepaliveBytes               int64             `json:"recovery_keepalive_bytes,omitempty"`
	RecoveryKeepaliveChunks              int64             `json:"recovery_keepalive_chunks,omitempty"`
	RecoveryKeepaliveRateBytesPerSecond  float64           `json:"recovery_keepalive_rate_bytes_per_second,omitempty"`
	RecoveryKeepaliveExpectedRate        float64           `json:"recovery_keepalive_expected_rate_bytes_per_second,omitempty"`
	RecoveryKeepaliveRealtimeMultiplier  float64           `json:"recovery_keepalive_realtime_multiplier,omitempty"`
	RecoveryKeepaliveGuardrailCount      int64             `json:"recovery_keepalive_guardrail_count,omitempty"`
	RecoveryKeepaliveGuardrailReason     string            `json:"recovery_keepalive_guardrail_reason,omitempty"`
	SourceSelectCount                    int64             `json:"source_select_count"`
	SameSourceReselectCount              int64             `json:"same_source_reselect_count"`
	LastSourceSelectedAt                 time.Time         `json:"last_source_selected_at"`
	LastSourceSelectReason               string            `json:"last_source_select_reason,omitempty"`
	SinceLastSourceSelect                string            `json:"since_last_source_select,omitempty"`
	LastError                            string            `json:"last_error,omitempty"`
	SourceHealthPersistCoalescedTotal    int64             `json:"source_health_persist_coalesced_total,omitempty"`
	SourceHealthPersistDroppedTotal      int64             `json:"source_health_persist_dropped_total,omitempty"`
	SourceHealthPersistCoalescedBySource map[int64]int64   `json:"source_health_persist_coalesced_by_source,omitempty"`
	SourceHealthPersistDroppedBySource   map[int64]int64   `json:"source_health_persist_dropped_by_source,omitempty"`
	Subscribers                          []SubscriberStats `json:"subscribers"`
}

// ClientStreamStatus maps one connected client subscriber to its backing tuner session.
type ClientStreamStatus struct {
	TunerID           int       `json:"tuner_id"`
	Kind              string    `json:"kind"`
	ChannelID         int64     `json:"channel_id,omitempty"`
	GuideNumber       string    `json:"guide_number,omitempty"`
	GuideName         string    `json:"guide_name,omitempty"`
	SourceID          int64     `json:"source_id,omitempty"`
	SourceItemKey     string    `json:"source_item_key,omitempty"`
	SourceStreamURL   string    `json:"source_stream_url,omitempty"`
	ClientHost        string    `json:"client_host,omitempty"`
	Resolution        string    `json:"resolution,omitempty"`
	FrameRate         float64   `json:"frame_rate,omitempty"`
	VideoCodec        string    `json:"video_codec,omitempty"`
	AudioCodec        string    `json:"audio_codec,omitempty"`
	CurrentBitrateBPS int64     `json:"current_bitrate_bps,omitempty"`
	ProfileBitrateBPS int64     `json:"profile_bitrate_bps,omitempty"`
	Producer          string    `json:"producer,omitempty"`
	SubscriberID      uint64    `json:"subscriber_id"`
	ClientAddr        string    `json:"client_addr,omitempty"`
	ConnectedAt       time.Time `json:"connected_at"`
}

// TunerStatusSnapshot returns a structured view of active tuner leases and subscribers.
func (h *Handler) TunerStatusSnapshot() TunerStatusSnapshot {
	now := time.Now().UTC()
	if h == nil || h.tuners == nil || h.sessions == nil {
		return TunerStatusSnapshot{GeneratedAt: now}
	}

	leaseSnapshot := h.tuners.Snapshot()
	sessionSnapshot := h.sessions.Snapshot()
	sessionHistory, sessionHistoryLimit, sessionHistoryTruncatedCount := h.sessions.HistorySnapshot()
	for i := range sessionHistory {
		for j := range sessionHistory[i].Sources {
			sessionHistory[i].Sources[j].StreamURL = sanitizeStreamURLForStatus(sessionHistory[i].Sources[j].StreamURL)
		}
	}
	churn := buildChurnSummary(sessionSnapshot)
	drainStats := streamDrainStatsSnapshot()
	probeCloseStats := probeCloseStatsSnapshot()

	sessionByTuner := make(map[int]SharedSessionStats, len(sessionSnapshot))
	for _, session := range sessionSnapshot {
		if session.TunerID < 0 {
			continue
		}
		sessionByTuner[session.TunerID] = session
	}

	tuners := make([]TunerStatus, 0, len(leaseSnapshot))
	seenTuners := make(map[int]struct{}, len(leaseSnapshot))
	for _, lease := range leaseSnapshot {
		row := TunerStatus{
			TunerID:         lease.ID,
			Kind:            lease.Kind,
			GuideNumber:     lease.GuideNumber,
			LeaseClientAddr: lease.ClientAddr,
			LeaseStartedAt:  lease.StartedAt,
		}
		hasSharedSession := false
		if session, ok := sessionByTuner[lease.ID]; ok {
			applySharedSessionStatus(&row, session)
			seenTuners[lease.ID] = struct{}{}
			hasSharedSession = true
		}
		row.State = deriveTunerState(row.Kind, len(row.Subscribers), hasSharedSession)
		tuners = append(tuners, row)
	}

	// Include any shared session snapshots that raced with tuner lease snapshots.
	for _, session := range sessionSnapshot {
		if session.TunerID < 0 {
			continue
		}
		if _, exists := seenTuners[session.TunerID]; exists {
			continue
		}

		row := TunerStatus{
			TunerID:     session.TunerID,
			Kind:        sessionKindClient,
			GuideNumber: session.GuideNumber,
		}
		applySharedSessionStatus(&row, session)
		row.State = deriveTunerState(row.Kind, len(row.Subscribers), true)
		tuners = append(tuners, row)
	}

	sort.Slice(tuners, func(i, j int) bool {
		if tuners[i].TunerID == tuners[j].TunerID {
			return tuners[i].Kind < tuners[j].Kind
		}
		return tuners[i].TunerID < tuners[j].TunerID
	})

	clientStreams := make([]ClientStreamStatus, 0)
	for _, tuner := range tuners {
		for _, subscriber := range tuner.Subscribers {
			clientStreams = append(clientStreams, ClientStreamStatus{
				TunerID:           tuner.TunerID,
				Kind:              tuner.Kind,
				ChannelID:         tuner.ChannelID,
				GuideNumber:       tuner.GuideNumber,
				GuideName:         tuner.GuideName,
				SourceID:          tuner.SourceID,
				SourceItemKey:     tuner.SourceItemKey,
				SourceStreamURL:   sanitizeStreamURLForStatus(tuner.SourceStreamURL),
				Resolution:        tuner.Resolution,
				FrameRate:         tuner.FrameRate,
				VideoCodec:        tuner.VideoCodec,
				AudioCodec:        tuner.AudioCodec,
				CurrentBitrateBPS: tuner.CurrentBitrateBPS,
				ProfileBitrateBPS: tuner.ProfileBitrateBPS,
				Producer:          tuner.Producer,
				SubscriberID:      subscriber.SubscriberID,
				ClientAddr:        subscriber.ClientAddr,
				ConnectedAt:       subscriber.StartedAt,
			})
		}
	}
	sort.Slice(clientStreams, func(i, j int) bool {
		if clientStreams[i].TunerID == clientStreams[j].TunerID {
			return clientStreams[i].ConnectedAt.Before(clientStreams[j].ConnectedAt)
		}
		return clientStreams[i].TunerID < clientStreams[j].TunerID
	})

	tunerCount := h.tuners.Capacity()
	inUse := len(leaseSnapshot)
	idle := tunerCount - inUse
	if idle < 0 {
		idle = 0
	}

	return TunerStatusSnapshot{
		GeneratedAt: now,
		TunerCount:  tunerCount,
		InUseCount:  inUse,
		IdleCount:   idle,
		Churn:       churn,
		DrainWait: DrainWaitTelemetry{
			OK:             drainStats.OK,
			Error:          drainStats.Error,
			WaitDurationUS: drainStats.WaitDurationUS,
			WaitDurationMS: drainStats.WaitDurationMS,
		},
		ProbeClose: ProbeCloseTelemetry{
			InlineCount:    probeCloseStats.InlineCount,
			QueueFullCount: probeCloseStats.QueueFullCount,
		},
		Tuners:                       tuners,
		ClientStreams:                clientStreams,
		SessionHistory:               sessionHistory,
		SessionHistoryLimit:          sessionHistoryLimit,
		SessionHistoryTruncatedCount: sessionHistoryTruncatedCount,
	}
}

func applySharedSessionStatus(dst *TunerStatus, session SharedSessionStats) {
	if dst == nil {
		return
	}

	dst.ChannelID = session.ChannelID
	if session.GuideNumber != "" {
		dst.GuideNumber = session.GuideNumber
	}
	dst.GuideName = session.GuideName
	dst.SourceID = session.SourceID
	dst.SourceItemKey = session.SourceItemKey
	dst.SourceStreamURL = sanitizeStreamURLForStatus(session.SourceStreamURL)
	dst.SourceStartupProbeRawBytes = session.SourceStartupProbeRawBytes
	dst.SourceStartupProbeTrimmedBytes = session.SourceStartupProbeTrimmedBytes
	dst.SourceStartupProbeCutoverOffset = session.SourceStartupProbeCutoverOffset
	dst.SourceStartupProbeDroppedBytes = session.SourceStartupProbeDroppedBytes
	dst.SourceStartupProbeBytes = session.SourceStartupProbeBytes
	dst.SourceStartupRandomAccessReady = session.SourceStartupRandomAccessReady
	dst.SourceStartupRandomAccessCodec = session.SourceStartupRandomAccessCodec
	dst.SourceStartupInventoryMethod = session.SourceStartupInventoryMethod
	dst.SourceStartupVideoStreams = session.SourceStartupVideoStreams
	dst.SourceStartupAudioStreams = session.SourceStartupAudioStreams
	dst.SourceStartupVideoCodecs = session.SourceStartupVideoCodecs
	dst.SourceStartupAudioCodecs = session.SourceStartupAudioCodecs
	dst.SourceStartupComponentState = session.SourceStartupComponentState
	dst.SourceStartupRetryRelaxedProbe = session.SourceStartupRetryRelaxedProbe
	dst.SourceStartupRetryRelaxedProbeReason = session.SourceStartupRetryRelaxedProbeReason
	dst.Resolution = session.Resolution
	dst.FrameRate = session.FrameRate
	dst.VideoCodec = session.VideoCodec
	dst.AudioCodec = session.AudioCodec
	dst.CurrentBitrateBPS = session.CurrentBitrateBPS
	dst.ProfileBitrateBPS = session.ProfileBitrateBPS
	dst.Producer = session.Producer
	dst.SessionStartedAt = session.StartedAt
	dst.LastByteAt = session.LastByteAt
	dst.LastPushAt = session.LastPushAt
	dst.BytesRead = session.BytesRead
	dst.BytesPushed = session.BytesPushed
	dst.ChunksPushed = session.ChunksPushed
	dst.SlowSkipEventsTotal = session.SlowSkipEventsTotal
	dst.SlowSkipLagChunksTotal = session.SlowSkipLagChunksTotal
	dst.SlowSkipLagBytesTotal = session.SlowSkipLagBytesTotal
	dst.SlowSkipMaxLagChunks = session.SlowSkipMaxLagChunks
	dst.SubscriberWriteDeadlineTimeoutsTotal = session.SubscriberWriteDeadlineTimeoutsTotal
	dst.SubscriberWriteShortWritesTotal = session.SubscriberWriteShortWritesTotal
	dst.SubscriberWriteBlockedDurationUS = session.SubscriberWriteBlockedDurationUS
	dst.SubscriberWriteBlockedDurationMS = session.SubscriberWriteBlockedDurationMS
	dst.StallCount = session.StallCount
	dst.RecoveryCycle = session.RecoveryCycle
	dst.RecoveryReason = session.RecoveryReason
	dst.RecoveryTransitionMode = session.RecoveryTransitionMode
	dst.RecoveryTransitionEffectiveMode = session.RecoveryTransitionEffectiveMode
	dst.RecoveryTransitionSignalsApplied = session.RecoveryTransitionSignalsApplied
	dst.RecoveryTransitionSignalSkips = session.RecoveryTransitionSignalSkips
	dst.RecoveryTransitionFallbackCount = session.RecoveryTransitionFallbackCount
	dst.RecoveryTransitionFallbackReason = session.RecoveryTransitionFallbackReason
	dst.RecoveryTransitionStitchApplied = session.RecoveryTransitionStitchApplied
	dst.RecoveryKeepaliveMode = session.RecoveryKeepaliveMode
	dst.RecoveryKeepaliveFallbackCount = session.RecoveryKeepaliveFallbackCount
	dst.RecoveryKeepaliveFallbackReason = session.RecoveryKeepaliveFallbackReason
	dst.RecoveryKeepaliveStartedAt = session.RecoveryKeepaliveStartedAt
	dst.RecoveryKeepaliveStoppedAt = session.RecoveryKeepaliveStoppedAt
	dst.RecoveryKeepaliveDuration = session.RecoveryKeepaliveDuration
	dst.RecoveryKeepaliveBytes = session.RecoveryKeepaliveBytes
	dst.RecoveryKeepaliveChunks = session.RecoveryKeepaliveChunks
	dst.RecoveryKeepaliveRateBytesPerSecond = session.RecoveryKeepaliveRateBytesPerSecond
	dst.RecoveryKeepaliveExpectedRate = session.RecoveryKeepaliveExpectedRate
	dst.RecoveryKeepaliveRealtimeMultiplier = session.RecoveryKeepaliveRealtimeMultiplier
	dst.RecoveryKeepaliveGuardrailCount = session.RecoveryKeepaliveGuardrailCount
	dst.RecoveryKeepaliveGuardrailReason = session.RecoveryKeepaliveGuardrailReason
	dst.SourceSelectCount = session.SourceSelectCount
	dst.SameSourceReselectCount = session.SameSourceReselectCount
	dst.LastSourceSelectedAt = session.LastSourceSelectedAt
	dst.LastSourceSelectReason = session.LastSourceSelectReason
	dst.SinceLastSourceSelect = session.SinceLastSourceSelect
	dst.LastError = session.LastError
	dst.SourceHealthPersistCoalescedTotal = session.SourceHealthPersistCoalescedTotal
	dst.SourceHealthPersistDroppedTotal = session.SourceHealthPersistDroppedTotal
	dst.SourceHealthPersistCoalescedBySource = nil
	dst.SourceHealthPersistDroppedBySource = nil
	if len(session.SourceHealthPersistCoalescedBySource) > 0 {
		dst.SourceHealthPersistCoalescedBySource = make(map[int64]int64, len(session.SourceHealthPersistCoalescedBySource))
		for sourceID, count := range session.SourceHealthPersistCoalescedBySource {
			dst.SourceHealthPersistCoalescedBySource[sourceID] = count
		}
	}
	if len(session.SourceHealthPersistDroppedBySource) > 0 {
		dst.SourceHealthPersistDroppedBySource = make(map[int64]int64, len(session.SourceHealthPersistDroppedBySource))
		for sourceID, count := range session.SourceHealthPersistDroppedBySource {
			dst.SourceHealthPersistDroppedBySource[sourceID] = count
		}
	}
	dst.Subscribers = append([]SubscriberStats(nil), session.SubscriberInfo...)
}

func deriveTunerState(kind string, subscriberCount int, hasSharedSession bool) string {
	normalizedKind := strings.ToLower(strings.TrimSpace(kind))
	switch normalizedKind {
	case sessionKindProbe:
		return tunerStateProbe
	case sessionKindClient:
		if subscriberCount > 0 {
			return tunerStateActiveSubscribers
		}
		if hasSharedSession {
			return tunerStateIdleGraceNoSubscribers
		}
		return tunerStateAllocatingSharedSession
	default:
		if subscriberCount > 0 {
			return tunerStateActiveSubscribers
		}
		return tunerStateUnknown
	}
}

func buildChurnSummary(sessions []SharedSessionStats) ChurnSummary {
	summary := ChurnSummary{
		ReselectAlertThreshold: sameSourceReselectAlertThreshold,
		SessionCount:           len(sessions),
	}
	if len(sessions) == 0 {
		return summary
	}

	for _, session := range sessions {
		if session.RecoveryCycle > 0 || strings.TrimSpace(session.RecoveryReason) != "" {
			summary.RecoveringSessionCount++
		}
		if session.SameSourceReselectCount > 0 {
			summary.SessionsWithReselectCount++
		}
		if sameSourceReselectAlertThreshold > 0 &&
			session.SameSourceReselectCount >= sameSourceReselectAlertThreshold {
			summary.SessionsOverReselectThreshold++
		}

		summary.TotalRecoveryCycles += session.RecoveryCycle
		summary.TotalSourceSelectCount += session.SourceSelectCount
		summary.TotalSameSourceReselectCount += session.SameSourceReselectCount

		if session.SameSourceReselectCount > summary.MaxSameSourceReselectCount {
			summary.MaxSameSourceReselectCount = session.SameSourceReselectCount
			summary.MaxReselectChannelID = session.ChannelID
			summary.MaxReselectGuideNumber = strings.TrimSpace(session.GuideNumber)
		}
	}

	return summary
}
