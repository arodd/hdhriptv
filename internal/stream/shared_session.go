package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	stallPolicyRestartSame    = "restart_same"
	stallPolicyFailoverSource = "failover_source"
	stallPolicyCloseSession   = "close_session"

	slowClientPolicyDisconnect = "disconnect"
	slowClientPolicySkip       = "skip"

	recoveryFillerModeNull = "null"
	recoveryFillerModePSI  = "psi"

	recoveryTransitionStrategyDefault = "default"

	defaultSharedStallDetect               = 4 * time.Second
	defaultSharedStallHardDeadline         = 32 * time.Second
	defaultSharedStallPolicy               = stallPolicyFailoverSource
	defaultSharedStallMaxFailoversPerTry   = 3
	defaultSharedCycleFailureMinHealth     = 20 * time.Second
	defaultSharedSubscriberJoinLagBytes    = 2 * 1024 * 1024
	defaultSharedSlowClientPolicy          = slowClientPolicyDisconnect
	defaultSharedSubscriberMaxBlockedWrite = 6 * time.Second
	defaultSharedSessionIdleTimeout        = 5 * time.Second
	defaultSharedSessionHistoryLimit       = 256
	defaultSharedSourceHistoryLimit        = 256
	defaultSharedSubscriberHistoryLimit    = 256
	minSharedSourceHistoryLimit            = 16
	maxSharedSourceHistoryLimit            = 4096
	minSharedSubscriberHistoryLimit        = 16
	maxSharedSubscriberHistoryLimit        = 4096
	defaultSharedRecoveryFillerInterval    = 200 * time.Millisecond
	defaultSharedRecoveryFillerMode        = recoveryFillerModeSlateAV
	defaultSharedRecoveryTransitionMode    = recoveryTransitionStrategyDefault
	defaultSourceHealthPersistQueueSize    = 128
	defaultSourceHealthPersistTimeout      = 2 * time.Second
	defaultSourceHealthDrainTimeout        = 250 * time.Millisecond
	sameSourceReselectAlertThreshold       = int64(3)
	startupProbeHighCutoverWarnPercent     = 75
	startupProbeHighCutoverWarnWindow      = 30 * time.Second
	sharedRingLagChunkCushion              = 16
	sharedRingMinChunkCapacity             = 32
	sharedRingMaxChunkCapacity             = 32768

	recoveryAlternateMinAttemptWindow    = 3 * time.Second
	recoveryAlternateMaxAttemptWindow    = 8 * time.Second
	recoveryAlternateFallbackReserve     = 500 * time.Millisecond
	recoveryAlternateMinStartupAttempts  = 3
	recoveryAlternatePenaltyReasonPrefix = "alternate_startup_failed_"
	recoveryTransientPenaltyBase         = 1 * time.Second
	recoveryTransientPenaltyMax          = 12 * time.Second
	recoveryTransientPenaltyResetWindow  = 45 * time.Second
	recoveryTransientPenaltyStateTTL     = 2 * time.Minute
	recoveryCycleBudgetMultiplier        = 8
	recoveryCycleBudgetMin               = 12
	recoveryCycleBudgetMax               = 48
	recoveryCycleBudgetResetMinWindow    = 4 * time.Second
	recoveryCycleBudgetResetMaxWindow    = 20 * time.Second
	recoveryCycleBudgetPaceMinWindow     = 50 * time.Millisecond
	recoveryCycleBudgetPaceMaxWindow     = 1 * time.Second
	recoveryTriggerLogCoalesceWindow     = 1 * time.Second
	slateAVCloseWarnCoalesceWindow       = 1 * time.Second
	slowSkipLogCoalesceWindow            = 1 * time.Second
	sourceReadPauseMinDuration           = 1 * time.Second
	sourceReadPauseReasonRecovered       = "recovered"
	sourceReadPauseReasonPumpExit        = "pump_exit"
	sourceReadPauseReasonContextCancel   = "ctx_cancel"
	recoveryTriggerOwnershipRetryLimit   = 8
	recoveryKeepaliveExpectedBitrateMin  = int64(1_500_000)
	recoveryKeepaliveGuardrailMultiplier = 2.5
	recoveryKeepaliveGuardrailWarmup     = 750 * time.Millisecond
	recoveryKeepaliveGuardrailSustain    = 1500 * time.Millisecond
	recoverySubscriberGuardPollInterval  = 50 * time.Millisecond
	providerOverlimitScopeDefault        = "global"
	providerOverlimitScopeSourcePrefix   = "source:"
	providerOverlimitScopeStateLimit     = 256
	recoveryFallbackReasonNoAlternates   = "no_alternate_sources"
	recoveryModeRestartSameFallback      = "restart_same_fallback"
	profileProbeRecoveryDelayMin         = 100 * time.Millisecond
	profileProbeRecoveryDelayMax         = 5 * time.Second
	profileProbeRestartCooldownMin       = 100 * time.Millisecond
	profileProbeRestartCooldownMax       = 2 * time.Second

	subscriberRemovalReasonHTTPContextCanceled = "http_context_canceled"
	subscriberRemovalReasonSlowClientLagged    = "slow_client_lagged"
	subscriberRemovalReasonStreamWriteError    = "stream_write_error"
	subscriberRemovalReasonSessionClosed       = "session_closed"
)

var (
	ErrSessionMaxSubscribers         = errors.New("shared session max subscribers reached")
	ErrSessionNoSources              = errors.New("channel has no enabled sources")
	ErrSessionNotFound               = errors.New("shared session not found")
	ErrSessionRecoveryAlreadyPending = errors.New(
		"shared session manual recovery is already pending",
	)
	ErrSlowClientLagged    = errors.New("subscriber fell behind stream tail")
	errSharedSessionClosed = errors.New("shared session is closed")
)

var (
	streamDrainOKCount        uint64
	streamDrainErrorCount     uint64
	streamDrainWaitDurationUS uint64

	streamSlowSkipEventsCount    uint64
	streamSlowSkipLagChunksTotal uint64
	streamSlowSkipLagBytesTotal  uint64

	streamSubscriberWriteDeadlineTimeoutsCount uint64
	streamSubscriberWriteShortWritesCount      uint64
	streamSubscriberWriteBlockedDurationUS     uint64

	streamSourceReadPauseEventsCount   uint64
	streamSourceReadPauseDurationUS    uint64
	streamSourceReadPauseMaxDurationUS uint64
	streamSourceReadPauseInProgress    uint64

	streamSlowSkipEventsMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_slow_skip_events_total",
		Help: "Total number of shared-session subscriber lag skip events.",
	})
	streamSlowSkipLagChunksMetric = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "stream_slow_skip_lag_chunks",
		Help:    "Lag depth in chunks for shared-session subscriber lag skip events.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096},
	})
	streamSlowSkipLagBytesMetric = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "stream_slow_skip_lag_bytes",
		Help:    "Estimated lag depth in bytes for shared-session subscriber lag skip events.",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 16),
	})
	streamSubscriberWriteDeadlineTimeoutsMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_subscriber_write_deadline_timeouts_total",
		Help: "Total number of shared-session subscriber writes that timed out on write deadline.",
	})
	streamSubscriberWriteShortWritesMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_subscriber_write_short_writes_total",
		Help: "Total number of shared-session subscriber writes that returned short writes.",
	})
	streamSubscriberWriteBlockedSecondsMetric = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "stream_subscriber_write_blocked_seconds",
		Help:    "Duration spent blocked in shared-session subscriber write calls.",
		Buckets: []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8},
	})
	streamSourceReadPauseEventsMetric = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stream_source_read_pause_events_total",
		Help: "Total number of shared-session source read pauses that exceeded the minimum duration threshold.",
	}, []string{"reason"})
	streamSourceReadPauseSecondsMetric = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "stream_source_read_pause_seconds",
		Help:    "Duration of shared-session source read pauses that exceeded the minimum duration threshold.",
		Buckets: []float64{0.25, 0.5, 1, 2, 4, 8, 16},
	}, []string{"reason"})
)

var errPumpStallDetected = errors.New("stream pump stall detected")
var errSourceEnded = errors.New("source stream ended")
var errManualRecoveryRequested = errors.New("manual recovery requested")
var errRecoveryKeepaliveOverproduction = errors.New("recovery keepalive output exceeded realtime envelope")
var errRecoveryAbortedNoSubscribers = errors.New("recovery aborted without subscribers")

type streamDrainStats struct {
	OK             uint64
	Error          uint64
	WaitDurationUS uint64
	WaitDurationMS uint64
}

type streamSlowSkipStats struct {
	Events         uint64
	LagChunksTotal uint64
	LagBytesTotal  uint64
}

type streamSubscriberWriteStats struct {
	DeadlineTimeouts  uint64
	ShortWrites       uint64
	BlockedDurationUS uint64
	BlockedDurationMS uint64
}

type streamSourceReadPauseStats struct {
	Events        uint64
	InProgress    uint64
	DurationUS    uint64
	DurationMS    uint64
	MaxDurationUS uint64
	MaxDurationMS uint64
}

func streamDrainStatsSnapshot() streamDrainStats {
	waitDurationUS := atomic.LoadUint64(&streamDrainWaitDurationUS)
	return streamDrainStats{
		OK:             atomic.LoadUint64(&streamDrainOKCount),
		Error:          atomic.LoadUint64(&streamDrainErrorCount),
		WaitDurationUS: waitDurationUS,
		WaitDurationMS: waitDurationUS / 1000,
	}
}

func resetStreamDrainStatsForTest() {
	atomic.StoreUint64(&streamDrainOKCount, 0)
	atomic.StoreUint64(&streamDrainErrorCount, 0)
	atomic.StoreUint64(&streamDrainWaitDurationUS, 0)
}

func streamSlowSkipStatsSnapshot() streamSlowSkipStats {
	return streamSlowSkipStats{
		Events:         atomic.LoadUint64(&streamSlowSkipEventsCount),
		LagChunksTotal: atomic.LoadUint64(&streamSlowSkipLagChunksTotal),
		LagBytesTotal:  atomic.LoadUint64(&streamSlowSkipLagBytesTotal),
	}
}

func resetStreamSlowSkipStatsForTest() {
	atomic.StoreUint64(&streamSlowSkipEventsCount, 0)
	atomic.StoreUint64(&streamSlowSkipLagChunksTotal, 0)
	atomic.StoreUint64(&streamSlowSkipLagBytesTotal, 0)
}

func streamSubscriberWriteStatsSnapshot() streamSubscriberWriteStats {
	blockedDurationUS := atomic.LoadUint64(&streamSubscriberWriteBlockedDurationUS)
	return streamSubscriberWriteStats{
		DeadlineTimeouts:  atomic.LoadUint64(&streamSubscriberWriteDeadlineTimeoutsCount),
		ShortWrites:       atomic.LoadUint64(&streamSubscriberWriteShortWritesCount),
		BlockedDurationUS: blockedDurationUS,
		BlockedDurationMS: blockedDurationUS / 1000,
	}
}

func resetStreamSubscriberWriteStatsForTest() {
	atomic.StoreUint64(&streamSubscriberWriteDeadlineTimeoutsCount, 0)
	atomic.StoreUint64(&streamSubscriberWriteShortWritesCount, 0)
	atomic.StoreUint64(&streamSubscriberWriteBlockedDurationUS, 0)
}

func streamSourceReadPauseStatsSnapshot() streamSourceReadPauseStats {
	durationUS := atomic.LoadUint64(&streamSourceReadPauseDurationUS)
	maxDurationUS := atomic.LoadUint64(&streamSourceReadPauseMaxDurationUS)
	return streamSourceReadPauseStats{
		Events:        atomic.LoadUint64(&streamSourceReadPauseEventsCount),
		InProgress:    atomic.LoadUint64(&streamSourceReadPauseInProgress),
		DurationUS:    durationUS,
		DurationMS:    durationUS / 1000,
		MaxDurationUS: maxDurationUS,
		MaxDurationMS: maxDurationUS / 1000,
	}
}

func resetStreamSourceReadPauseStatsForTest() {
	atomic.StoreUint64(&streamSourceReadPauseEventsCount, 0)
	atomic.StoreUint64(&streamSourceReadPauseDurationUS, 0)
	atomic.StoreUint64(&streamSourceReadPauseMaxDurationUS, 0)
	atomic.StoreUint64(&streamSourceReadPauseInProgress, 0)
}

func normalizeStreamSourceReadPauseReason(reason string) string {
	switch reason {
	case sourceReadPauseReasonRecovered,
		sourceReadPauseReasonPumpExit,
		sourceReadPauseReasonContextCancel:
		return reason
	default:
		return sourceReadPauseReasonRecovered
	}
}

func incrementStreamSourceReadPauseInProgress() {
	atomic.AddUint64(&streamSourceReadPauseInProgress, 1)
}

func decrementStreamSourceReadPauseInProgress() {
	for {
		current := atomic.LoadUint64(&streamSourceReadPauseInProgress)
		if current == 0 {
			return
		}
		if atomic.CompareAndSwapUint64(&streamSourceReadPauseInProgress, current, current-1) {
			return
		}
	}
}

func recordStreamSlowSkipTelemetry(lagChunks, lagBytes uint64) {
	atomic.AddUint64(&streamSlowSkipEventsCount, 1)
	atomic.AddUint64(&streamSlowSkipLagChunksTotal, lagChunks)
	atomic.AddUint64(&streamSlowSkipLagBytesTotal, lagBytes)
	streamSlowSkipEventsMetric.Inc()
	streamSlowSkipLagChunksMetric.Observe(float64(lagChunks))
	streamSlowSkipLagBytesMetric.Observe(float64(lagBytes))
}

func recordStreamSubscriberWriteTelemetry(sample subscriberWritePressureSample) {
	if sample.DeadlineTimeout {
		atomic.AddUint64(&streamSubscriberWriteDeadlineTimeoutsCount, 1)
		streamSubscriberWriteDeadlineTimeoutsMetric.Inc()
	}
	if sample.ShortWrite {
		atomic.AddUint64(&streamSubscriberWriteShortWritesCount, 1)
		streamSubscriberWriteShortWritesMetric.Inc()
	}
	if sample.BlockedDuration > 0 {
		atomic.AddUint64(&streamSubscriberWriteBlockedDurationUS, sample.BlockedDuration)
		streamSubscriberWriteBlockedSecondsMetric.Observe(float64(sample.BlockedDuration) / float64(time.Second/time.Microsecond))
	}
}

func recordStreamSourceReadPauseTelemetry(reason string, duration time.Duration) {
	if duration <= 0 {
		return
	}
	reason = normalizeStreamSourceReadPauseReason(reason)

	durationUS := uint64(duration / time.Microsecond)
	if durationUS == 0 {
		durationUS = 1
	}
	atomic.AddUint64(&streamSourceReadPauseEventsCount, 1)
	atomic.AddUint64(&streamSourceReadPauseDurationUS, durationUS)

	for {
		current := atomic.LoadUint64(&streamSourceReadPauseMaxDurationUS)
		if durationUS <= current {
			break
		}
		if atomic.CompareAndSwapUint64(&streamSourceReadPauseMaxDurationUS, current, durationUS) {
			break
		}
	}

	streamSourceReadPauseEventsMetric.WithLabelValues(reason).Inc()
	streamSourceReadPauseSecondsMetric.WithLabelValues(reason).Observe(float64(duration) / float64(time.Second))
}

type manualRecoveryError struct {
	reason string
}

func (e *manualRecoveryError) Error() string {
	if e == nil {
		return errManualRecoveryRequested.Error()
	}
	reason := normalizeManualRecoveryReason(e.reason)
	if reason == "" {
		return errManualRecoveryRequested.Error()
	}
	return errManualRecoveryRequested.Error() + ": " + reason
}

func (e *manualRecoveryError) Is(target error) bool {
	return target == errManualRecoveryRequested
}

func normalizeManualRecoveryReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "manual_trigger"
	}
	return strings.ToLower(strings.Join(strings.Fields(reason), "_"))
}

func manualRecoveryReason(err error) (string, bool) {
	var recoveryErr *manualRecoveryError
	if !errors.As(err, &recoveryErr) {
		return "", false
	}
	return normalizeManualRecoveryReason(recoveryErr.reason), true
}

type slowClientLagDetails struct {
	RequestedSeq   uint64
	OldestSeq      uint64
	LagChunks      uint64
	RingNextSeq    uint64
	BufferedChunks int
	BufferedBytes  int
}

type slowClientLagError struct {
	details slowClientLagDetails
}

func (e *slowClientLagError) Error() string {
	if e == nil {
		return ErrSlowClientLagged.Error()
	}
	details := e.details
	return fmt.Sprintf(
		"%s (requested_seq=%d oldest_seq=%d lag_chunks=%d ring_next_seq=%d buffered_chunks=%d buffered_bytes=%d)",
		ErrSlowClientLagged,
		details.RequestedSeq,
		details.OldestSeq,
		details.LagChunks,
		details.RingNextSeq,
		details.BufferedChunks,
		details.BufferedBytes,
	)
}

func (e *slowClientLagError) Unwrap() error {
	return ErrSlowClientLagged
}

func slowClientLagDetailsFromReadResult(read ReadResult) slowClientLagDetails {
	lagChunks := uint64(0)
	if read.OldestSeq > read.RequestedSeq {
		lagChunks = read.OldestSeq - read.RequestedSeq
	}
	return slowClientLagDetails{
		RequestedSeq:   read.RequestedSeq,
		OldestSeq:      read.OldestSeq,
		LagChunks:      lagChunks,
		RingNextSeq:    read.RingNextSeq,
		BufferedChunks: read.BufferedChunks,
		BufferedBytes:  read.BufferedBytes,
	}
}

func estimateSlowClientLagBytes(read ReadResult, lagChunks uint64) uint64 {
	if lagChunks == 0 || read.BufferedBytes <= 0 || read.BufferedChunks <= 0 {
		return 0
	}
	bufferedBytes := uint64(read.BufferedBytes)
	bufferedChunks := uint64(read.BufferedChunks)
	maxUint64 := ^uint64(0)
	if lagChunks > maxUint64/bufferedBytes {
		return maxUint64
	}
	estimated := (bufferedBytes * lagChunks) / bufferedChunks
	if estimated == 0 {
		return 1
	}
	return estimated
}

func newSlowClientLagError(read ReadResult) error {
	return &slowClientLagError{
		details: slowClientLagDetailsFromReadResult(read),
	}
}

func slowClientLagDetailsFromError(err error) (slowClientLagDetails, bool) {
	var lagErr *slowClientLagError
	if !errors.As(err, &lagErr) || lagErr == nil {
		return slowClientLagDetails{}, false
	}
	return lagErr.details, true
}

// SessionManagerConfig controls shared per-channel stream sessions.
type SessionManagerConfig struct {
	Mode                            string
	FFmpegPath                      string
	HTTPClient                      *http.Client
	Logger                          *slog.Logger
	StartupTimeout                  time.Duration
	StartupRandomAccessRecoveryOnly bool
	MinProbeBytes                   int
	MaxFailovers                    int
	FailoverTotalTimeout            time.Duration
	UpstreamOverlimitCooldown       time.Duration
	FFmpegReconnectEnabled          bool
	FFmpegReconnectDelayMax         time.Duration
	FFmpegReconnectMaxRetries       int
	FFmpegReconnectHTTPErrors       string
	FFmpegStartupProbeSize          int
	FFmpegStartupAnalyzeDelay       time.Duration
	FFmpegInputBufferSize           int
	FFmpegDiscardCorrupt            bool
	FFmpegCopyRegenerateTimestamps  *bool
	ProducerReadRate                float64
	ProducerReadRateCatchup         float64
	ProducerInitialBurst            int

	BufferChunkBytes           int
	BufferPublishFlushInterval time.Duration
	BufferTSAlign188           bool

	StallDetect               time.Duration
	StallHardDeadline         time.Duration
	StallPolicy               string
	StallMaxFailoversPerStall int
	CycleFailureMinHealth     time.Duration

	RecoveryFillerEnabled     bool
	RecoveryFillerMode        string
	RecoveryFillerInterval    time.Duration
	RecoveryFillerText        string
	RecoveryFillerEnableAudio bool

	SubscriberJoinLagBytes        int
	SubscriberSlowClientPolicy    string
	SubscriberMaxBlockedWrite     time.Duration
	SessionIdleTimeout            time.Duration
	SessionMaxSubscribers         int
	SessionHistoryLimit           int
	SessionSourceHistoryLimit     int
	SessionSubscriberHistoryLimit int
	SessionDrainTimeout           time.Duration
	SourceHealthDrainTimeout      time.Duration
}

type sessionManagerConfig struct {
	mode                            string
	ffmpegPath                      string
	httpClient                      *http.Client
	logger                          *slog.Logger
	startupWait                     time.Duration
	startupRandomAccessRecoveryOnly bool
	minProbe                        int
	maxFailover                     int
	failoverTTL                     time.Duration
	upstreamOverlimitCooldown       time.Duration
	ffmpegReconnectEnabled          bool
	ffmpegReconnectDelayMax         time.Duration
	ffmpegReconnectMaxRetries       int
	ffmpegReconnectHTTPErrors       string
	ffmpegStartupProbeSize          int
	ffmpegStartupAnalyzeDelay       time.Duration
	ffmpegInputBufferSize           int
	ffmpegDiscardCorrupt            bool
	ffmpegCopyRegenerateTimestamps  bool
	producerReadRate                float64
	producerReadRateCatchup         float64
	producerInitialBurst            int

	bufferChunkBytes  int
	bufferFlushPeriod time.Duration
	bufferTSAlign188  bool

	stallDetect             time.Duration
	stallHardDeadline       time.Duration
	stallPolicy             string
	stallMaxFailoversPerTry int
	cycleFailureMinHealth   time.Duration

	recoveryFillerEnabled     bool
	recoveryFillerMode        string
	recoveryFillerInterval    time.Duration
	recoveryTransitionMode    string
	recoveryFillerText        string
	recoveryFillerEnableAudio bool
	recoverySlateAVFactory    slateAVProducerFactory

	subscriberJoinLagBytes        int
	subscriberSlowPolicy          string
	subscriberMaxBlockedWrite     time.Duration
	sessionIdleTimeout            time.Duration
	sessionMaxSubscribers         int
	sessionHistoryLimit           int
	sessionSourceHistoryLimit     int
	sessionSubscriberHistoryLimit int
	sessionDrainTimeout           time.Duration
	sourceHealthDrainTimeout      time.Duration
}

// SharedSessionStats reports per-channel shared-session state for diagnostics.
type SharedSessionStats struct {
	TunerID                              int
	ChannelID                            int64
	GuideNumber                          string
	GuideName                            string
	SourceID                             int64
	SourceItemKey                        string
	SourceStreamURL                      string
	SourceStartupProbeRawBytes           int
	SourceStartupProbeTrimmedBytes       int
	SourceStartupProbeCutoverOffset      int
	SourceStartupProbeDroppedBytes       int
	SourceStartupProbeBytes              int
	SourceStartupRandomAccessReady       bool
	SourceStartupRandomAccessCodec       string
	SourceStartupInventoryMethod         string
	SourceStartupVideoStreams            int
	SourceStartupAudioStreams            int
	SourceStartupVideoCodecs             string
	SourceStartupAudioCodecs             string
	SourceStartupComponentState          string
	SourceStartupRetryRelaxedProbe       bool
	SourceStartupRetryRelaxedProbeReason string
	Resolution                           string
	FrameRate                            float64
	VideoCodec                           string
	AudioCodec                           string
	CurrentBitrateBPS                    int64
	ProfileBitrateBPS                    int64
	Producer                             string
	StartedAt                            time.Time
	LastByteAt                           time.Time
	LastPushAt                           time.Time
	BytesRead                            int64
	BytesPushed                          int64
	ChunksPushed                         int64
	Subscribers                          int
	SubscriberInfo                       []SubscriberStats
	SlowSkipEventsTotal                  uint64
	SlowSkipLagChunksTotal               uint64
	SlowSkipLagBytesTotal                uint64
	SlowSkipMaxLagChunks                 uint64
	SubscriberWriteDeadlineTimeoutsTotal uint64
	SubscriberWriteShortWritesTotal      uint64
	SubscriberWriteBlockedDurationUS     uint64
	SubscriberWriteBlockedDurationMS     uint64
	StallCount                           int64
	RecoveryCycle                        int64
	RecoveryReason                       string
	RecoveryTransitionMode               string
	RecoveryTransitionEffectiveMode      string
	RecoveryTransitionSignalsApplied     string
	RecoveryTransitionSignalSkips        string
	RecoveryTransitionFallbackCount      int64
	RecoveryTransitionFallbackReason     string
	RecoveryTransitionStitchApplied      bool
	RecoveryKeepaliveMode                string
	RecoveryKeepaliveFallbackCount       int64
	RecoveryKeepaliveFallbackReason      string
	RecoveryKeepaliveStartedAt           time.Time
	RecoveryKeepaliveStoppedAt           time.Time
	RecoveryKeepaliveDuration            string
	RecoveryKeepaliveBytes               int64
	RecoveryKeepaliveChunks              int64
	RecoveryKeepaliveRateBytesPerSecond  float64
	RecoveryKeepaliveExpectedRate        float64
	RecoveryKeepaliveRealtimeMultiplier  float64
	RecoveryKeepaliveGuardrailCount      int64
	RecoveryKeepaliveGuardrailReason     string
	SourceSelectCount                    int64
	SameSourceReselectCount              int64
	LastSourceSelectedAt                 time.Time
	LastSourceSelectReason               string
	SinceLastSourceSelect                string
	LastError                            string
	SourceHealthPersistCoalescedTotal    int64
	SourceHealthPersistDroppedTotal      int64
	SourceHealthPersistCoalescedBySource map[int64]int64
	SourceHealthPersistDroppedBySource   map[int64]int64
}

// SubscriberStats reports one connected subscriber on a shared session.
type SubscriberStats struct {
	SubscriberID uint64
	ClientAddr   string
	StartedAt    time.Time
}

// SharedSessionHistory reports bounded historical shared-session lifecycle
// windows for both active and recently closed sessions.
type SharedSessionHistory struct {
	SessionID                            uint64                           `json:"session_id"`
	ChannelID                            int64                            `json:"channel_id,omitempty"`
	GuideNumber                          string                           `json:"guide_number,omitempty"`
	GuideName                            string                           `json:"guide_name,omitempty"`
	TunerID                              int                              `json:"tuner_id"`
	OpenedAt                             time.Time                        `json:"opened_at"`
	ClosedAt                             time.Time                        `json:"closed_at,omitempty"`
	Active                               bool                             `json:"active"`
	TerminalStatus                       string                           `json:"terminal_status,omitempty"`
	TerminalError                        string                           `json:"terminal_error,omitempty"`
	PeakSubscribers                      int                              `json:"peak_subscribers"`
	TotalSubscribers                     int64                            `json:"total_subscribers"`
	CompletedSubscribers                 int64                            `json:"completed_subscribers"`
	SlowSkipEventsTotal                  uint64                           `json:"slow_skip_events_total"`
	SlowSkipLagChunksTotal               uint64                           `json:"slow_skip_lag_chunks_total"`
	SlowSkipLagBytesTotal                uint64                           `json:"slow_skip_lag_bytes_total"`
	SlowSkipMaxLagChunks                 uint64                           `json:"slow_skip_max_lag_chunks"`
	SubscriberWriteDeadlineTimeoutsTotal uint64                           `json:"subscriber_write_deadline_timeouts_total"`
	SubscriberWriteShortWritesTotal      uint64                           `json:"subscriber_write_short_writes_total"`
	SubscriberWriteBlockedDurationUS     uint64                           `json:"subscriber_write_blocked_duration_us"`
	SubscriberWriteBlockedDurationMS     uint64                           `json:"subscriber_write_blocked_duration_ms"`
	RecoveryCycleCount                   int64                            `json:"recovery_cycle_count"`
	LastRecoveryReason                   string                           `json:"last_recovery_reason,omitempty"`
	SourceSelectCount                    int64                            `json:"source_select_count"`
	SameSourceReselectCount              int64                            `json:"same_source_reselect_count"`
	LastSourceSelectedAt                 time.Time                        `json:"last_source_selected_at,omitempty"`
	LastSourceSelectReason               string                           `json:"last_source_select_reason,omitempty"`
	LastError                            string                           `json:"last_error,omitempty"`
	Sources                              []SharedSessionSourceHistory     `json:"sources,omitempty"`
	Subscribers                          []SharedSessionSubscriberHistory `json:"subscribers,omitempty"`
	SourceHistoryLimit                   int                              `json:"source_history_limit,omitempty"`
	SourceHistoryTruncated               int64                            `json:"source_history_truncated_count,omitempty"`
	SubscriberHistoryLimit               int                              `json:"subscriber_history_limit,omitempty"`
	SubscriberHistoryTruncated           int64                            `json:"subscriber_history_truncated_count,omitempty"`
}

// SharedSessionSourceHistory reports one source-selection window for a shared session.
type SharedSessionSourceHistory struct {
	SourceID                       int64     `json:"source_id,omitempty"`
	ItemKey                        string    `json:"item_key,omitempty"`
	StreamURL                      string    `json:"stream_url,omitempty"`
	SelectedAt                     time.Time `json:"selected_at"`
	DeselectedAt                   time.Time `json:"deselected_at,omitempty"`
	SelectionReason                string    `json:"selection_reason,omitempty"`
	StartupProbeRawBytes           int       `json:"startup_probe_raw_bytes,omitempty"`
	StartupProbeTrimmedBytes       int       `json:"startup_probe_trimmed_bytes,omitempty"`
	StartupProbeCutoverOffset      int       `json:"startup_probe_cutover_offset,omitempty"`
	StartupProbeDroppedBytes       int       `json:"startup_probe_dropped_bytes,omitempty"`
	StartupRandomAccessReady       bool      `json:"startup_random_access_ready,omitempty"`
	StartupRandomAccessCodec       string    `json:"startup_random_access_codec,omitempty"`
	StartupInventoryMethod         string    `json:"startup_inventory_method,omitempty"`
	StartupVideoStreams            int       `json:"startup_video_streams,omitempty"`
	StartupAudioStreams            int       `json:"startup_audio_streams,omitempty"`
	StartupVideoCodecs             string    `json:"startup_video_codecs,omitempty"`
	StartupAudioCodecs             string    `json:"startup_audio_codecs,omitempty"`
	StartupComponentState          string    `json:"startup_component_state,omitempty"`
	StartupRetryRelaxedProbe       bool      `json:"startup_retry_relaxed_probe,omitempty"`
	StartupRetryRelaxedProbeReason string    `json:"startup_retry_relaxed_probe_reason,omitempty"`
	Resolution                     string    `json:"resolution,omitempty"`
	FrameRate                      float64   `json:"frame_rate,omitempty"`
	VideoCodec                     string    `json:"video_codec,omitempty"`
	AudioCodec                     string    `json:"audio_codec,omitempty"`
	ProfileBitrateBPS              int64     `json:"profile_bitrate_bps,omitempty"`
	Producer                       string    `json:"producer,omitempty"`
}

// SharedSessionSubscriberHistory reports one subscriber lifecycle window.
type SharedSessionSubscriberHistory struct {
	SubscriberID uint64    `json:"subscriber_id"`
	ClientAddr   string    `json:"client_addr,omitempty"`
	ClientHost   string    `json:"client_host,omitempty"`
	ConnectedAt  time.Time `json:"connected_at"`
	ClosedAt     time.Time `json:"closed_at,omitempty"`
	CloseReason  string    `json:"close_reason,omitempty"`
}

// SessionManager coordinates shared channel stream sessions.
type SessionManager struct {
	channels     ChannelsProvider
	tuners       *Pool
	cfg          sessionManagerConfig
	logger       *slog.Logger
	recentHealth *recentSourceHealth

	mu            sync.Mutex
	sessions      map[int64]*sharedRuntimeSession
	creating      map[int64]*sessionCreateWait
	closed        bool
	wg            sync.WaitGroup // tracks session.run + session.runSourceHealthPersist goroutines
	closeWaitOnce sync.Once
	closeWaitDone chan struct{}
	drainCh       chan struct{}
	// waitForDrainBlockedHook is a test-only callback invoked when WaitForDrain
	// observes an open drain signal and is about to block.
	waitForDrainBlockedHook func()

	sourceHealthClearAfter       time.Time
	sourceHealthClearAfterByChan map[int64]time.Time

	sessionHistory               []SharedSessionHistory
	sessionHistoryTruncatedCount int64
	nextHistorySessionID         uint64

	overlimitMu             sync.Mutex
	providerCooldownByScope map[string]providerOverlimitScopeState
}

type sessionCreateWait struct {
	done    chan struct{}
	session *sharedRuntimeSession
	err     error
	cancel  context.CancelFunc // cancels in-flight AcquireClient during shutdown
}

type providerOverlimitScopeState struct {
	until       time.Time
	lastTouched time.Time
	armCount    uint32
}

type singleSourceLookupProvider interface {
	GetSource(ctx context.Context, channelID, sourceID int64, enabledOnly bool) (channels.Source, error)
}

type sourceHealthPersistRequest struct {
	sourceID    int64
	eventID     uint64
	success     bool
	reason      string
	observedAt  time.Time
	channelID   int64
	guideNumber string
}

type shortLivedRecoveryPenalty struct {
	count          int
	cooldownUntil  time.Time
	lastObservedAt time.Time
	lastReason     string
}

type startupAdmissionFunc func(channelID int64) error

type sharedRuntimeSession struct {
	manager   *SessionManager
	channel   channels.Channel
	ring      *ChunkRing
	publisher ChunkPublisher
	pump      *Pump
	lease     *Lease

	ctx    context.Context
	cancel context.CancelFunc

	readyOnce sync.Once
	readyCh   chan struct{}
	readyErr  error

	mu                               sync.Mutex
	subscribers                      map[uint64]SubscriberStats
	nextSubscriberID                 uint64
	idleTimer                        *time.Timer
	idleToken                        uint64
	idleCancelValidatedHook          func()
	startupAttemptIdleCancelHook     func()
	closed                           bool
	lastErr                          error
	sourceID                         int64
	sourceItemKey                    string
	sourceStreamURL                  string
	sourceStartupProbeRawBytes       int
	sourceStartupProbeTrimmedBytes   int
	sourceStartupProbeCutoverOffset  int
	sourceStartupProbeDroppedBytes   int
	sourceStartupProbeBytes          int
	sourceStartupRandomAccessReady   bool
	sourceStartupRandomAccessCodec   string
	sourceStartupInventoryMethod     string
	sourceStartupVideoStreams        int
	sourceStartupAudioStreams        int
	sourceStartupVideoCodecs         string
	sourceStartupAudioCodecs         string
	sourceStartupComponentState      string
	sourceStartupRetryRelaxedProbe   bool
	sourceStartupRetryReason         string
	sourceSelectBytesBase            int64
	sourceProfile                    streamProfile
	producer                         string
	sourceSelects                    int64
	lastSourceSelectedAt             time.Time
	lastSourceSelectReason           string
	sameSourceReselectCount          int64
	startupProbeWarnedAt             time.Time
	startupProbeWarnSuppressed       int64
	profileProbeGeneration           uint64
	profileProbeCancel               context.CancelFunc
	profileProbeLastStartedAt        time.Time
	recoveryCycle                    int64
	recoveryReason                   string
	recoveryTransitionMode           string
	recoveryTransitionEffective      string
	recoveryTransitionSignalsApplied string
	recoveryTransitionSignalSkips    string
	recoveryTransitionFallbacks      int64
	recoveryTransitionLastFallback   string
	recoveryTransitionStitchApplied  bool
	recoveryKeepaliveMode            string
	recoveryKeepaliveFallbacks       int64
	recoveryKeepaliveLastFallback    string
	recoveryKeepaliveStartedAt       time.Time
	recoveryKeepaliveStoppedAt       time.Time
	recoveryKeepaliveBytes           int64
	recoveryKeepaliveChunks          int64
	recoveryKeepaliveRateBytesPerSec float64
	recoveryKeepaliveExpectedRate    float64
	recoveryKeepaliveProfileRate     float64
	recoveryKeepaliveMultiplier      float64
	recoveryKeepaliveOverrateSince   time.Time
	recoveryKeepaliveGuardrailCount  int64
	recoveryKeepaliveGuardrailReason string
	recoveryPATContinuity            byte
	recoveryPMTContinuity            byte
	recoveryPCRContinuity            byte
	recoveryPATVersion               byte
	recoveryPMTVersion               byte
	recoveryObservedPCRBase          uint64
	recoveryObservedPCRAt            time.Time
	recoveryBoundaryPCRBase          uint64
	recoveryBoundaryPCRSet           bool
	recoveryBurstCount               int
	recoveryBurstStartedAt           time.Time
	recoveryTriggerLastLogAt         time.Time
	recoveryTriggerLastSourceID      int64
	recoveryTriggerLastReason        string
	recoveryTriggerLogsSuppressed    int64
	slateAVCloseWarnLastLogAt        time.Time
	slateAVCloseWarnLogsSuppressed   int64
	firstByteLogged                  bool
	slowSkipEventsTotal              uint64
	slowSkipLagChunksTotal           uint64
	slowSkipLagBytesTotal            uint64
	slowSkipMaxLagChunks             uint64
	subscriberWriteDeadlineTimeouts  uint64
	subscriberWriteShortWrites       uint64
	subscriberWriteBlockedDurationUS uint64
	stallCount                       int64
	shortLivedRecoveryBySource       map[int64]shortLivedRecoveryPenalty
	manualRecoveryCh                 chan string
	manualRecoveryPending            bool
	manualRecoveryInProgress         bool
	startedAt                        time.Time
	sourceHealthPersistCh            chan sourceHealthPersistRequest
	sourceHealthPersistDone          chan struct{}
	profileProbePersistWg            sync.WaitGroup
	sourceHealthPersistMu            sync.Mutex
	sourceHealthCoalesced            map[int64]sourceHealthPersistRequest
	sourceHealthQueue                []int64
	sourceHealthCoalescedTotal       int64
	sourceHealthDroppedTotal         int64
	sourceHealthCoalescedBySource    map[int64]int64
	sourceHealthDroppedBySource      map[int64]int64

	historySessionID            uint64
	historyPeakSubscribers      int
	historyTotalSubscribers     int64
	historyCompletedSubscribers int64
	historySources              []SharedSessionSourceHistory
	historySourcesTruncated     int64
	historyCurrentSourceIdx     int
	historySubscribers          []SharedSessionSubscriberHistory
	historySubscribersTruncated int64
	historySubscriberIndex      map[uint64]int
	closedAt                    time.Time
}

type streamSessionReadCloser struct {
	session *streamSession
	once    sync.Once
}

type sessionChunkPublisher struct {
	session *sharedRuntimeSession
}

type recoveryKeepaliveChunkPublisher struct {
	session *sharedRuntimeSession
}

type recoveryKeepaliveSnapshot struct {
	StartedAt          time.Time
	StoppedAt          time.Time
	Duration           time.Duration
	Bytes              int64
	Chunks             int64
	RateBytesPerSecond float64
	ExpectedRate       float64
	RealtimeMultiplier float64
	GuardrailCount     int64
	GuardrailReason    string
}

// SessionSubscription represents one active client subscription to a shared channel session.
type SessionSubscription struct {
	session *sharedRuntimeSession

	nextSeq      uint64
	subscriberID uint64
	clientAddr   string

	slowPolicy      string
	maxBlockedWrite time.Duration
	lastSlowSkipLog time.Time
	slowSkipSkipped uint64

	once sync.Once
}

type subscriberClientAddrContextKey struct{}

// NewSessionManager builds a new shared session manager.
func NewSessionManager(cfg SessionManagerConfig, tuners *Pool, channelsProvider ChannelsProvider) *SessionManager {
	if tuners == nil || channelsProvider == nil {
		return nil
	}

	normalized := normalizeSessionManagerConfig(cfg)
	return &SessionManager{
		channels:                     channelsProvider,
		tuners:                       tuners,
		cfg:                          normalized,
		logger:                       normalized.logger,
		recentHealth:                 newRecentSourceHealth(),
		sessions:                     make(map[int64]*sharedRuntimeSession),
		creating:                     make(map[int64]*sessionCreateWait),
		drainCh:                      closedSignalChan(),
		sessionHistory:               make([]SharedSessionHistory, 0, normalized.sessionHistoryLimit),
		sourceHealthClearAfterByChan: make(map[int64]time.Time),
		providerCooldownByScope:      make(map[string]providerOverlimitScopeState),
	}
}

func closedSignalChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (m *SessionManager) syncDrainSignalLocked() {
	active := len(m.sessions) + len(m.creating)
	if active == 0 {
		if m.drainCh == nil {
			m.drainCh = make(chan struct{})
		}
		select {
		case <-m.drainCh:
		default:
			close(m.drainCh)
		}
		return
	}

	if m.drainCh == nil {
		m.drainCh = make(chan struct{})
		return
	}
	select {
	case <-m.drainCh:
		m.drainCh = make(chan struct{})
	default:
	}
}

// Close cancels all active sessions and blocks until their goroutines
// (including source-health persistence drain) complete. This must be called
// before store.Close() to guarantee no session-scoped DB work races with
// database teardown.
func (m *SessionManager) Close() {
	_ = m.CloseWithContext(context.Background())
}

// CloseWithContext cancels all active sessions and waits until their goroutines
// complete or the provided context expires.
func (m *SessionManager) CloseWithContext(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.Lock()
	m.closed = true
	cancels := make([]context.CancelFunc, 0, len(m.sessions)+len(m.creating))
	for _, session := range m.sessions {
		if session.cancel != nil {
			cancels = append(cancels, session.cancel)
		}
	}
	for _, wait := range m.creating {
		if wait.cancel != nil {
			cancels = append(cancels, wait.cancel)
		}
	}
	if m.closeWaitDone == nil {
		m.closeWaitDone = make(chan struct{})
	}
	done := m.closeWaitDone
	m.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}

	// Reuse a single wait goroutine across all CloseWithContext calls.
	// If a caller times out early, subsequent callers still observe the same
	// eventual completion signal when m.wg reaches zero.
	m.closeWaitOnce.Do(func() {
		go func() {
			m.wg.Wait()
			close(done)
		}()
	})

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *SessionManager) recordDrainWaitResult(waitStartedAt time.Time, err error) error {
	waitDurationUS := time.Since(waitStartedAt).Microseconds()
	if waitDurationUS < 0 {
		waitDurationUS = 0
	}
	atomic.AddUint64(&streamDrainWaitDurationUS, uint64(waitDurationUS))
	managerID := fmt.Sprintf("%p", m)

	if err != nil {
		atomic.AddUint64(&streamDrainErrorCount, 1)
		if m != nil && m.logger != nil {
			m.logger.Warn(
				"stream drain wait failed",
				"manager_id", managerID,
				"drain_result", "error",
				"drain_wait_duration_us", waitDurationUS,
				"error", err,
			)
		}
		return err
	}

	atomic.AddUint64(&streamDrainOKCount, 1)
	if m != nil && m.logger != nil {
		m.logger.Info(
			"stream drain wait completed",
			"manager_id", managerID,
			"drain_result", "ok",
			"drain_wait_duration_us", waitDurationUS,
		)
	}
	return nil
}

// WaitForDrain blocks until the manager has no active or pending channel sessions.
func (m *SessionManager) WaitForDrain(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	waitStartedAt := time.Now()

	for {
		m.mu.Lock()
		if len(m.sessions) == 0 && len(m.creating) == 0 {
			m.mu.Unlock()
			return m.recordDrainWaitResult(waitStartedAt, nil)
		}
		drainCh := m.drainCh
		waitForDrainBlockedHook := m.waitForDrainBlockedHook
		if drainCh == nil {
			m.syncDrainSignalLocked()
			drainCh = m.drainCh
		}
		m.mu.Unlock()

		if waitForDrainBlockedHook != nil {
			// Best-effort test synchronization: only signal the blocked hook when
			// the drain signal appears open at observation time.
			select {
			case <-drainCh:
			default:
				waitForDrainBlockedHook()
			}
		}

		select {
		case <-drainCh:
		case <-ctx.Done():
			return m.recordDrainWaitResult(waitStartedAt, ctx.Err())
		}
	}
}

func withSubscriberClientAddr(ctx context.Context, clientAddr string) context.Context {
	clientAddr = strings.TrimSpace(clientAddr)
	if ctx == nil || clientAddr == "" {
		return ctx
	}
	return context.WithValue(ctx, subscriberClientAddrContextKey{}, clientAddr)
}

func subscriberClientAddrFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value := ctx.Value(subscriberClientAddrContextKey{})
	clientAddr, _ := value.(string)
	return strings.TrimSpace(clientAddr)
}

// Snapshot returns all active shared sessions with per-session diagnostics.
func (m *SessionManager) Snapshot() []SharedSessionStats {
	if m == nil {
		return nil
	}

	m.mu.Lock()
	sessions := make([]*sharedRuntimeSession, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.mu.Unlock()

	stats := make([]SharedSessionStats, 0, len(sessions))
	for _, session := range sessions {
		stats = append(stats, session.stats())
	}
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].TunerID == stats[j].TunerID {
			if stats[i].GuideNumber == stats[j].GuideNumber {
				return stats[i].ChannelID < stats[j].ChannelID
			}
			return stats[i].GuideNumber < stats[j].GuideNumber
		}
		return stats[i].TunerID < stats[j].TunerID
	})
	return stats
}

// HistorySnapshot returns bounded shared-session history for active and
// recently closed sessions. Results are newest-first.
func (m *SessionManager) HistorySnapshot() ([]SharedSessionHistory, int, int64) {
	if m == nil {
		return nil, 0, 0
	}

	m.mu.Lock()
	limit := m.cfg.sessionHistoryLimit
	if limit <= 0 {
		limit = defaultSharedSessionHistoryLimit
	}
	truncated := m.sessionHistoryTruncatedCount
	closed := append([]SharedSessionHistory(nil), m.sessionHistory...)
	sessions := make([]*sharedRuntimeSession, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.mu.Unlock()

	history := make([]SharedSessionHistory, 0, len(closed)+len(sessions))
	seen := make(map[uint64]struct{}, len(closed))
	for _, session := range sessions {
		if session == nil {
			continue
		}
		entry := session.historySnapshot()
		if entry.SessionID == 0 {
			history = append(history, entry)
			continue
		}
		if _, exists := seen[entry.SessionID]; exists {
			continue
		}
		seen[entry.SessionID] = struct{}{}
		history = append(history, entry)
	}
	for _, entry := range closed {
		if entry.SessionID == 0 {
			history = append(history, entry)
			continue
		}
		if _, exists := seen[entry.SessionID]; exists {
			continue
		}
		seen[entry.SessionID] = struct{}{}
		history = append(history, entry)
	}
	sort.Slice(history, func(i, j int) bool {
		return sharedSessionHistorySortLess(history[i], history[j])
	})
	if len(history) > limit {
		truncated += int64(len(history) - limit)
		history = history[:limit]
	}
	return history, limit, truncated
}

func sharedSessionHistorySortLess(a, b SharedSessionHistory) bool {
	if !a.OpenedAt.Equal(b.OpenedAt) {
		return a.OpenedAt.After(b.OpenedAt)
	}
	if !a.ClosedAt.Equal(b.ClosedAt) {
		return a.ClosedAt.After(b.ClosedAt)
	}
	return a.SessionID > b.SessionID
}

func (m *SessionManager) nextSessionHistoryID() uint64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextHistorySessionID++
	if m.nextHistorySessionID == 0 {
		m.nextHistorySessionID++
	}
	return m.nextHistorySessionID
}

func (m *SessionManager) recordClosedSessionHistory(entry SharedSessionHistory) {
	if m == nil {
		return
	}
	if entry.SessionID == 0 || entry.OpenedAt.IsZero() {
		return
	}
	entry.Active = false
	limit := m.cfg.sessionHistoryLimit
	if limit <= 0 {
		limit = defaultSharedSessionHistoryLimit
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.sessionHistory = append([]SharedSessionHistory{entry}, m.sessionHistory...)
	if overflow := len(m.sessionHistory) - limit; overflow > 0 {
		m.sessionHistory = m.sessionHistory[:limit]
		m.sessionHistoryTruncatedCount += int64(overflow)
	}
}

// HasActiveOrPendingSession reports whether a channel currently has an admissible
// shared session, or a shared session in creation.
func (m *SessionManager) HasActiveOrPendingSession(channelID int64) bool {
	if m == nil || channelID <= 0 {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.creating[channelID]; ok {
		return true
	}
	session := m.sessions[channelID]
	if session == nil || !session.isAdmissible() {
		return false
	}
	return true
}

// TriggerRecovery requests an in-session recovery cycle for an active channel
// session without waiting for a real upstream stall.
func (m *SessionManager) TriggerRecovery(channelID int64, reason string) error {
	if m == nil {
		return errors.New("shared session manager is not configured")
	}
	if channelID <= 0 {
		return errors.New("channel_id must be greater than zero")
	}

	normalizedReason := normalizeManualRecoveryReason(reason)
	attemptedOwners := make(map[*sharedRuntimeSession]struct{}, 2)
	var lastErr error

	for attempts := 0; attempts < recoveryTriggerOwnershipRetryLimit; attempts++ {
		owner, creating := m.triggerRecoveryOwner(channelID)
		if owner == nil {
			if creating {
				return ErrSessionRecoveryAlreadyPending
			}
			return ErrSessionNotFound
		}

		if _, seen := attemptedOwners[owner]; seen {
			break
		}
		attemptedOwners[owner] = struct{}{}

		// Check admissibility after m.mu is released to avoid holding
		// both m.mu and s.mu simultaneously in this path. If the owner
		// is canceled/closed, skip to re-check for a replacement.
		if !owner.isAdmissible() {
			lastErr = ErrSessionNotFound
			continue
		}

		err := owner.requestManualRecovery(normalizedReason)

		currentOwner, createPending := m.triggerRecoveryOwner(channelID)
		if currentOwner != owner {
			// Best effort rollback when a stale owner accepted the trigger but
			// ownership moved before we classified the outcome.
			if err == nil {
				owner.discardManualRecoveryRequest(normalizedReason)
			}
			lastErr = err
			if currentOwner == nil && createPending {
				return ErrSessionRecoveryAlreadyPending
			}
			continue
		}

		if err == nil {
			return nil
		}
		if errors.Is(err, ErrSessionRecoveryAlreadyPending) {
			return ErrSessionRecoveryAlreadyPending
		}
		if errors.Is(err, ErrSessionNotFound) {
			lastErr = err
			if createPending {
				return ErrSessionRecoveryAlreadyPending
			}
			continue
		}
		return err
	}

	if errors.Is(lastErr, ErrSessionRecoveryAlreadyPending) {
		return ErrSessionRecoveryAlreadyPending
	}
	if errors.Is(lastErr, ErrSessionNotFound) || lastErr == nil {
		session, creating := m.triggerRecoveryOwner(channelID)
		if creating {
			return ErrSessionRecoveryAlreadyPending
		}
		if session == nil || !session.isAdmissible() {
			return ErrSessionNotFound
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return ErrSessionNotFound
}

func (m *SessionManager) ClearSourceHealth(channelID int64) error {
	if m == nil || m.channels == nil {
		return errors.New("shared session manager is not configured")
	}
	if channelID <= 0 {
		return errors.New("channel_id must be greater than zero")
	}

	cutoff := time.Now().UTC()
	sessions := make([]*sharedRuntimeSession, 0, 1)

	m.mu.Lock()
	m.sourceHealthClearAfterByChan[channelID] = cutoff
	for _, session := range m.sessions {
		if session == nil || session.channel.ChannelID != channelID {
			continue
		}
		sessions = append(sessions, session)
	}
	m.mu.Unlock()

	// Clear in-memory overlay state for this channel's known sources so overlay
	// reapplication converges immediately after clear operations.
	if m.recentHealth != nil {
		sources, err := m.channels.ListSources(context.Background(), channelID, false)
		if err == nil {
			sourceIDs := make([]int64, 0, len(sources))
			for _, source := range sources {
				if source.SourceID > 0 {
					sourceIDs = append(sourceIDs, source.SourceID)
				}
			}
			m.recentHealth.clearBySourceIDs(sourceIDs)
		}
	}

	for _, session := range sessions {
		session.clearSourceHealthPersistState()
	}
	return nil
}

func (m *SessionManager) ClearAllSourceHealth() error {
	if m == nil || m.channels == nil {
		return errors.New("shared session manager is not configured")
	}

	cutoff := time.Now().UTC()

	m.mu.Lock()
	m.sourceHealthClearAfter = cutoff
	m.sourceHealthClearAfterByChan = make(map[int64]time.Time)
	sessions := make([]*sharedRuntimeSession, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.mu.Unlock()

	if m.recentHealth != nil {
		m.recentHealth.clearAll()
	}

	for _, session := range sessions {
		session.clearSourceHealthPersistState()
	}
	return nil
}

func (m *SessionManager) triggerRecoveryOwner(channelID int64) (*sharedRuntimeSession, bool) {
	if m == nil {
		return nil, false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	session := m.sessions[channelID]
	_, creating := m.creating[channelID]
	return session, creating
}

// Subscribe returns a client subscription for the given channel.
func (m *SessionManager) Subscribe(ctx context.Context, channel channels.Channel) (*SessionSubscription, error) {
	subscription, _, err := m.subscribe(ctx, channel, true, nil)
	return subscription, err
}

// SubscribeImmediate returns a subscription without waiting for startup readiness.
// This is useful for clients that expect a prompt HTTP response while startup proceeds.
func (m *SessionManager) SubscribeImmediate(ctx context.Context, channel channels.Channel) (*SessionSubscription, error) {
	subscription, _, err := m.subscribe(ctx, channel, false, nil)
	return subscription, err
}

func (m *SessionManager) subscribe(
	ctx context.Context,
	channel channels.Channel,
	waitReady bool,
	admitStartup startupAdmissionFunc,
) (*SessionSubscription, bool, error) {
	if m == nil || m.channels == nil || m.tuners == nil {
		return nil, false, errors.New("shared session manager is not configured")
	}
	if channel.ChannelID <= 0 {
		return nil, false, errors.New("channel_id must be greater than zero")
	}
	if strings.TrimSpace(channel.GuideNumber) == "" {
		return nil, false, errors.New("guide_number is required")
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}

		session, created, err := m.getOrCreateSession(ctx, channel, admitStartup)
		if err != nil {
			return nil, false, err
		}
		if err := ctx.Err(); err != nil {
			if created {
				// Pass subscriberID=0 because no subscriber was actually added yet
				// (context canceled between getOrCreateSession and addSubscriber).
				// The subscriberID!=0 guard inside removeSubscriber skips the delete,
				// but the zero-subscriber idle-cancel logic still fires, cleaning up
				// this newly created but now-orphaned session.
				session.removeSubscriber(0, subscriberRemovalReasonHTTPContextCanceled)
			}
			return nil, created, err
		}

		clientAddr := subscriberClientAddrFromContext(ctx)
		nextSeq, subscriberID, err := session.addSubscriber(clientAddr)
		if err != nil {
			if errors.Is(err, errSharedSessionClosed) {
				m.removeSession(channel.ChannelID, session)
				select {
				case <-ctx.Done():
					return nil, false, ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
				continue
			}
			return nil, false, err
		}
		if err := ctx.Err(); err != nil {
			session.removeSubscriber(subscriberID, subscriberRemovalReasonHTTPContextCanceled)
			return nil, created, err
		}

		subscription := &SessionSubscription{
			session:         session,
			nextSeq:         nextSeq,
			subscriberID:    subscriberID,
			clientAddr:      clientAddr,
			slowPolicy:      m.cfg.subscriberSlowPolicy,
			maxBlockedWrite: m.cfg.subscriberMaxBlockedWrite,
		}
		if waitReady {
			if err := session.waitReady(ctx); err != nil {
				subscription.Close()
				return nil, created, err
			}
		} else if err := ctx.Err(); err != nil {
			subscription.Close()
			return nil, created, err
		}

		return subscription, created, nil
	}
}

// Stream writes shared stream chunks to the provided response writer until completion/cancel.
func (s *SessionSubscription) Stream(ctx context.Context, w http.ResponseWriter) error {
	if s == nil || s.session == nil || w == nil {
		return errors.New("subscription writer is not configured")
	}

	writeFailed := false
	var streamErr error
	defer func() {
		s.closeWithReason(classifySubscriberRemovalReason(streamErr, ctx, writeFailed))
	}()

	flusher, _ := w.(http.Flusher)
	controller := http.NewResponseController(w)

	for {
		read := s.session.ring.ReadFrom(s.nextSeq)
		if read.Behind {
			if s.slowPolicy == slowClientPolicySkip {
				s.recordSlowSkipEvent(read)
				s.nextSeq = read.NextSeq
				continue
			}
			streamErr = newSlowClientLagError(read)
			return streamErr
		}

		if len(read.Chunks) == 0 {
			if read.Closed {
				if isGracefulSessionCloseErr(read.Err) {
					streamErr = nil
					return nil
				}
				streamErr = read.Err
				return streamErr
			}

			if err := s.session.ring.WaitForChange(ctx, read.WaitSeq); err != nil {
				streamErr = err
				return streamErr
			}
			continue
		}

		for _, chunk := range read.Chunks {
			writePressure, err := writeChunk(w, controller, chunk.Data, s.maxBlockedWrite)
			if s.session != nil {
				s.session.recordSubscriberWritePressure(writePressure)
			}
			if err != nil {
				read.Release()
				writeFailed = true
				streamErr = err
				return streamErr
			}
			if flusher != nil {
				flusher.Flush()
			}
			s.nextSeq = chunk.Seq + 1
		}
		read.Release()
	}
}

type slowSkipSessionSnapshot struct {
	EventsTotal    uint64
	LagChunksTotal uint64
	LagBytesTotal  uint64
	MaxLagChunks   uint64
}

type subscriberWritePressureSample struct {
	DeadlineTimeout bool
	ShortWrite      bool
	BlockedDuration uint64
}

func (s *SessionSubscription) recordSlowSkipEvent(read ReadResult) {
	if s == nil {
		return
	}
	details := slowClientLagDetailsFromReadResult(read)
	lagBytes := estimateSlowClientLagBytes(read, details.LagChunks)
	recordStreamSlowSkipTelemetry(details.LagChunks, lagBytes)

	sessionSnapshot := slowSkipSessionSnapshot{}
	if s.session != nil {
		sessionSnapshot = s.session.recordSlowSkipEvent(details.LagChunks, lagBytes)
	}
	s.logSlowSkipEvent(details, lagBytes, sessionSnapshot)
}

func (s *SessionSubscription) logSlowSkipEvent(
	details slowClientLagDetails,
	lagBytes uint64,
	sessionSnapshot slowSkipSessionSnapshot,
) {
	if s == nil || s.session == nil || s.session.manager == nil || s.session.manager.logger == nil {
		return
	}

	now := time.Now().UTC()
	if !s.lastSlowSkipLog.IsZero() && now.Sub(s.lastSlowSkipLog) < slowSkipLogCoalesceWindow {
		s.slowSkipSkipped++
		return
	}

	coalesced := s.slowSkipSkipped
	s.slowSkipSkipped = 0
	s.lastSlowSkipLog = now

	fields := []any{
		"channel_id", s.session.channel.ChannelID,
		"guide_number", strings.TrimSpace(s.session.channel.GuideNumber),
		"subscriber_id", s.subscriberID,
		"client_addr", strings.TrimSpace(s.clientAddr),
		"requested_seq", details.RequestedSeq,
		"oldest_seq", details.OldestSeq,
		"lag_chunks", details.LagChunks,
		"buffered_chunks", details.BufferedChunks,
		"buffered_bytes", details.BufferedBytes,
		"estimated_lag_bytes", lagBytes,
		"slow_skip_events_total", sessionSnapshot.EventsTotal,
		"slow_skip_lag_chunks_total", sessionSnapshot.LagChunksTotal,
		"slow_skip_lag_bytes_total", sessionSnapshot.LagBytesTotal,
		"slow_skip_max_lag_chunks", sessionSnapshot.MaxLagChunks,
	}
	if coalesced > 0 {
		fields = append(fields, "slow_skip_logs_coalesced", coalesced)
	}

	s.session.manager.logger.Warn("shared session subscriber lag skip", fields...)
}

func isGracefulSessionCloseErr(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, errSourceEnded)
}

func normalizeSubscriberRemovalReason(reason string) string {
	switch strings.TrimSpace(reason) {
	case subscriberRemovalReasonHTTPContextCanceled:
		return subscriberRemovalReasonHTTPContextCanceled
	case subscriberRemovalReasonSlowClientLagged:
		return subscriberRemovalReasonSlowClientLagged
	case subscriberRemovalReasonStreamWriteError:
		return subscriberRemovalReasonStreamWriteError
	default:
		return subscriberRemovalReasonSessionClosed
	}
}

func classifySubscriberRemovalReason(err error, requestCtx context.Context, writeFailed bool) string {
	if errors.Is(err, ErrSlowClientLagged) {
		return subscriberRemovalReasonSlowClientLagged
	}
	if writeFailed {
		return subscriberRemovalReasonStreamWriteError
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if requestCtx != nil && requestCtx.Err() != nil {
			return subscriberRemovalReasonHTTPContextCanceled
		}
		return subscriberRemovalReasonSessionClosed
	}
	return subscriberRemovalReasonSessionClosed
}

// Close releases one subscriber slot from the shared session.
func (s *SessionSubscription) Close() {
	s.closeWithReason(subscriberRemovalReasonSessionClosed)
}

func (s *SessionSubscription) closeWithReason(reason string) {
	if s == nil || s.session == nil {
		return
	}
	reason = normalizeSubscriberRemovalReason(reason)
	s.once.Do(func() {
		s.session.removeSubscriber(s.subscriberID, reason)
	})
}

// Stats returns a snapshot of the current shared session state.
func (s *SessionSubscription) Stats() SharedSessionStats {
	if s == nil || s.session == nil {
		return SharedSessionStats{}
	}
	return s.session.stats()
}

var errSessionManagerClosed = errors.New("session manager is closed")

func (m *SessionManager) getOrCreateSession(
	ctx context.Context,
	channel channels.Channel,
	admitStartup startupAdmissionFunc,
) (*sharedRuntimeSession, bool, error) {
	channelID := channel.ChannelID
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}

		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, false, errSessionManagerClosed
		}
		if existing, ok := m.sessions[channelID]; ok {
			if !existing.isAdmissible() {
				delete(m.sessions, channelID)
				m.syncDrainSignalLocked()
				m.mu.Unlock()
				continue
			}
			m.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			m.logSessionState("shared session reused", existing, channel, false)
			return existing, false, nil
		}
		if wait, ok := m.creating[channelID]; ok {
			done := wait.done
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-done:
			}
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			if wait.session != nil {
				if !wait.session.isAdmissible() {
					m.mu.Lock()
					if existing, ok := m.sessions[channelID]; ok && existing == wait.session {
						delete(m.sessions, channelID)
						m.syncDrainSignalLocked()
					}
					m.mu.Unlock()
					continue
				}
				if err := ctx.Err(); err != nil {
					return nil, false, err
				}
				m.logSessionState("shared session reused", wait.session, channel, true)
				return wait.session, false, nil
			}
			if wait.err != nil {
				if errors.Is(wait.err, context.Canceled) || errors.Is(wait.err, context.DeadlineExceeded) {
					if err := ctx.Err(); err != nil {
						return nil, false, err
					}
					continue
				}
				return nil, false, wait.err
			}
			continue
		}

		if admitStartup != nil {
			if err := admitStartup(channelID); err != nil {
				m.mu.Unlock()
				return nil, false, err
			}
		}

		createCtx, createCancel := context.WithCancel(ctx)
		wait := &sessionCreateWait{done: make(chan struct{}), cancel: createCancel}
		m.creating[channelID] = wait
		m.syncDrainSignalLocked()
		m.wg.Add(1) // track creation lifecycle from reservation; happens-before Close's wg.Wait
		m.mu.Unlock()

		lease, err := m.tuners.AcquireClient(createCtx, channel.GuideNumber, "shared:"+channel.GuideNumber)
		if err != nil {
			createCancel()
			m.mu.Lock()
			wait.err = err
			delete(m.creating, channelID)
			m.syncDrainSignalLocked()
			close(wait.done)
			m.mu.Unlock()
			m.wg.Done() // balance Add from reservation
			return nil, false, err
		}

		session := m.newSession(channel, lease)

		m.mu.Lock()
		if m.closed {
			// Close() ran while we were acquiring the tuner or creating
			// the session. Tear down without registering or tracking.
			wait.err = errSessionManagerClosed
			delete(m.creating, channelID)
			m.syncDrainSignalLocked()
			close(wait.done)
			m.mu.Unlock()
			createCancel()
			session.cancel()
			if session.sourceHealthPersistDone != nil {
				<-session.sourceHealthPersistDone
			}
			lease.Release()
			m.wg.Done() // balance Add from reservation
			return nil, false, errSessionManagerClosed
		}
		m.sessions[channelID] = session
		wait.session = session
		delete(m.creating, channelID)
		m.syncDrainSignalLocked()
		close(wait.done)
		m.mu.Unlock()
		createCancel() // AcquireClient done; release create context

		m.logSessionState("shared session created", session, channel, false)
		go func() {
			defer m.wg.Done() // balance Add from reservation
			session.run()
		}()
		return session, true, nil
	}
}

func (s *sharedRuntimeSession) isClosed() bool {
	if s == nil {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// isAdmissible reports whether a session can be reused for new subscribers.
// A session that has already been canceled is treated as non-admissible even
// before finish() flips closed=true.
func (s *sharedRuntimeSession) isAdmissible() bool {
	if s == nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	return s.ctx == nil || s.ctx.Err() == nil
}

func (m *SessionManager) logSessionState(
	event string,
	session *sharedRuntimeSession,
	channel channels.Channel,
	waitedForCreate bool,
) {
	if m == nil || m.logger == nil || session == nil {
		return
	}

	tunerID, sourceID, sessionStartedAt, subscribers := session.logState()
	fields := []any{
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"guide_name", channel.GuideName,
		"tuner_id", tunerID,
		"source_id", sourceID,
		"session_started_at", sessionStartedAt,
		"subscribers", subscribers,
	}
	if waitedForCreate {
		fields = append(fields, "waited_for_create", true)
	}
	m.logger.Info(event, fields...)
}

func (m *SessionManager) newSession(channel channels.Channel, lease *Lease) *sharedRuntimeSession {
	ctx, cancel := context.WithCancel(context.Background())
	sessionStartedAt := time.Now().UTC()
	historySessionID := m.nextSessionHistoryID()

	ringMaxChunks := ringCapacityForLag(
		m.cfg.bufferChunkBytes,
		m.cfg.subscriberJoinLagBytes,
	)
	ringMaxBytes := ringByteBudgetForLag(
		m.cfg.bufferChunkBytes,
		m.cfg.subscriberJoinLagBytes,
	)
	ring := NewChunkRingWithLimitsAndStartupHint(
		ringMaxChunks,
		ringMaxBytes,
		m.cfg.bufferChunkBytes,
	)

	session := &sharedRuntimeSession{
		manager:                     m,
		channel:                     channel,
		ring:                        ring,
		lease:                       lease,
		ctx:                         ctx,
		cancel:                      cancel,
		readyCh:                     make(chan struct{}),
		subscribers:                 make(map[uint64]SubscriberStats),
		startedAt:                   sessionStartedAt,
		recoveryTransitionMode:      m.cfg.recoveryTransitionMode,
		recoveryTransitionEffective: m.cfg.recoveryTransitionMode,
		manualRecoveryCh:            make(chan string, 1),
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			defaultSourceHealthPersistQueueSize,
		),
		sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
		sourceHealthQueue:             make([]int64, 0, defaultSourceHealthPersistQueueSize),
		sourceHealthCoalescedBySource: make(map[int64]int64),
		sourceHealthDroppedBySource:   make(map[int64]int64),
		shortLivedRecoveryBySource:    make(map[int64]shortLivedRecoveryPenalty),
		historySessionID:              historySessionID,
		historyCurrentSourceIdx:       -1,
		historySubscriberIndex:        make(map[uint64]int),
	}
	session.publisher = &sessionChunkPublisher{session: session}
	session.pump = NewPump(
		PumpConfig{
			ChunkBytes:           m.cfg.bufferChunkBytes,
			PublishFlushInterval: m.cfg.bufferFlushPeriod,
			TSAlign188:           m.cfg.bufferTSAlign188,
		},
		session.publisher,
	)
	session.sourceHealthPersistDone = make(chan struct{})
	go session.runSourceHealthPersist()
	return session
}

func (s *sharedRuntimeSession) runSourceHealthPersist() {
	defer func() {
		if s.sourceHealthPersistDone != nil {
			close(s.sourceHealthPersistDone)
		}
	}()
	if s == nil || s.sourceHealthPersistCh == nil || s.manager == nil || s.manager.channels == nil {
		return
	}

	for {
		select {
		case <-s.ctx.Done():
			started := time.Now().UTC()
			drained, dropped, timedOut := s.drainSourceHealthPersistQueue()
			if s.manager != nil && s.manager.logger != nil && (drained > 0 || dropped > 0 || timedOut) {
				fields := []any{
					"channel_id", s.channel.ChannelID,
					"guide_number", s.channel.GuideNumber,
					"drained", drained,
					"dropped", dropped,
					"timed_out", timedOut,
					"duration", time.Since(started).String(),
				}
				if timedOut {
					s.manager.logger.Warn("source health drain timed out during shared session shutdown", fields...)
				} else {
					s.manager.logger.Info("source health drain completed during shared session shutdown", fields...)
				}
			}
			return
		case req := <-s.sourceHealthPersistCh:
			s.persistSourceHealth(req)
			if s.ctx != nil && s.ctx.Err() != nil {
				started := time.Now().UTC()
				drained, dropped, timedOut := s.drainSourceHealthPersistQueue()
				if s.manager != nil && s.manager.logger != nil && (drained > 0 || dropped > 0 || timedOut) {
					fields := []any{
						"channel_id", s.channel.ChannelID,
						"guide_number", s.channel.GuideNumber,
						"drained", drained,
						"dropped", dropped,
						"timed_out", timedOut,
						"duration", time.Since(started).String(),
					}
					if timedOut {
						s.manager.logger.Warn("source health drain timed out during shared session shutdown", fields...)
					} else {
						s.manager.logger.Info("source health drain completed during shared session shutdown", fields...)
					}
				}
				return
			}
			s.persistCoalescedSourceHealth(time.Time{})
		}
	}
}

func (s *sharedRuntimeSession) clearSourceHealthPersistState() {
	if s == nil {
		return
	}

	s.sourceHealthPersistMu.Lock()
	defer s.sourceHealthPersistMu.Unlock()

	for sourceID, req := range s.sourceHealthCoalesced {
		if s.manager != nil {
			s.manager.dropRecentSourceHealthEvent(sourceID, req.eventID)
		}
	}

	s.sourceHealthQueue = nil
	s.sourceHealthCoalesced = nil
	s.sourceHealthCoalescedBySource = nil
	s.sourceHealthDroppedBySource = nil
}

func (s *sharedRuntimeSession) drainSourceHealthPersistQueue() (int, int, bool) {
	if s == nil {
		return 0, 0, false
	}

	timeout := time.Duration(0)
	if s.manager != nil {
		timeout = s.manager.cfg.sourceHealthDrainTimeout
	}

	return s.drainSourceHealthPersistQueueWithTimeout(timeout)
}

func (s *sharedRuntimeSession) drainSourceHealthPersistQueueWithTimeout(timeout time.Duration) (int, int, bool) {
	if s == nil {
		return 0, 0, false
	}

	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().UTC().Add(timeout)
	}

	drained := 0
	for {
		if !deadline.IsZero() && time.Now().UTC().After(deadline) {
			dropped := s.dropPendingSourceHealthPersist()
			return drained, dropped, true
		}

		drainedOne := false
		select {
		case req := <-s.sourceHealthPersistCh:
			s.persistSourceHealthWithDeadline(req, deadline)
			drained++
			drainedOne = true
		default:
		}
		if req, ok := s.dequeueCoalescedSourceHealthPersist(); ok {
			s.persistSourceHealthWithDeadline(req, deadline)
			drained++
			drainedOne = true
		}
		if !drainedOne {
			return drained, 0, false
		}
	}
}

// drainSourceHealthQueue preserves compatibility for tests that still refer to
// the pre-refactor helper name.
func (s *sharedRuntimeSession) drainSourceHealthQueue() {
	s.drainSourceHealthPersistQueue()
}

func (s *sharedRuntimeSession) dropPendingSourceHealthPersist() int {
	if s == nil {
		return 0
	}
	dropped := s.dropQueuedSourceHealthPersist()
	dropped += s.dropCoalescedSourceHealthPersist()
	return dropped
}

func (s *sharedRuntimeSession) dropQueuedSourceHealthPersist() int {
	if s == nil || s.sourceHealthPersistCh == nil {
		return 0
	}

	dropped := 0
	for {
		select {
		case req := <-s.sourceHealthPersistCh:
			if s.manager != nil {
				s.manager.dropRecentSourceHealthEvent(req.sourceID, req.eventID)
			}
			dropped++
		default:
			return dropped
		}
	}
}

func (s *sharedRuntimeSession) dropCoalescedSourceHealthPersist() int {
	if s == nil {
		return 0
	}

	s.sourceHealthPersistMu.Lock()
	defer s.sourceHealthPersistMu.Unlock()

	dropped := 0
	for sourceID, req := range s.sourceHealthCoalesced {
		if s.manager != nil {
			s.manager.dropRecentSourceHealthEvent(sourceID, req.eventID)
		}
		dropped++
	}

	s.sourceHealthQueue = nil
	s.sourceHealthCoalesced = nil
	s.sourceHealthCoalescedBySource = nil
	s.sourceHealthDroppedBySource = nil

	return dropped
}

func (s *sharedRuntimeSession) persistCoalescedSourceHealth(deadline time.Time) {
	if s == nil {
		return
	}
	for {
		if !deadline.IsZero() && time.Now().UTC().After(deadline) {
			return
		}
		req, ok := s.dequeueCoalescedSourceHealthPersist()
		if !ok {
			return
		}
		s.persistSourceHealthWithDeadline(req, deadline)
	}
}

func (s *sharedRuntimeSession) dequeueCoalescedSourceHealthPersist() (sourceHealthPersistRequest, bool) {
	if s == nil {
		return sourceHealthPersistRequest{}, false
	}
	s.sourceHealthPersistMu.Lock()
	defer s.sourceHealthPersistMu.Unlock()

	if len(s.sourceHealthQueue) == 0 || len(s.sourceHealthCoalesced) == 0 {
		return sourceHealthPersistRequest{}, false
	}
	for len(s.sourceHealthQueue) > 0 {
		sourceID := s.sourceHealthQueue[0]
		s.sourceHealthQueue = s.sourceHealthQueue[1:]
		req, ok := s.sourceHealthCoalesced[sourceID]
		if !ok {
			continue
		}
		delete(s.sourceHealthCoalesced, sourceID)
		return req, true
	}
	return sourceHealthPersistRequest{}, false
}

const sourceHealthPersistMaxAttempts = 3

func (s *sharedRuntimeSession) persistSourceHealth(req sourceHealthPersistRequest) {
	s.persistSourceHealthWithDeadline(req, time.Time{})
}

func (s *sharedRuntimeSession) persistSourceHealthWithDeadline(req sourceHealthPersistRequest, deadline time.Time) {
	if s == nil || s.manager == nil || s.manager.channels == nil || req.sourceID <= 0 {
		return
	}

	observedAt := req.observedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}

	reason := strings.TrimSpace(req.reason)
	if !req.success && reason == "" {
		reason = "startup failed"
	}
	clearAfter, clearAfterByChannel := s.manager.sourceHealthClearSnapshot()
	cutoff := clearAfter
	if req.channelID <= 0 {
		req.channelID = s.channel.ChannelID
	}
	if req.channelID > 0 && len(clearAfterByChannel) > 0 {
		if byChannel, ok := clearAfterByChannel[req.channelID]; ok && byChannel.After(cutoff) {
			cutoff = byChannel
		}
	}
	if !cutoff.IsZero() && !observedAt.After(cutoff) {
		s.manager.dropRecentSourceHealthEvent(req.sourceID, req.eventID)
		return
	}

	for attempt := 1; attempt <= sourceHealthPersistMaxAttempts; attempt++ {
		persistTimeout := defaultSourceHealthPersistTimeout
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				s.manager.dropRecentSourceHealthEvent(req.sourceID, req.eventID)
				return
			}
			if remaining < persistTimeout {
				persistTimeout = remaining
			}
		}

		persistCtx, cancel := context.WithTimeout(context.Background(), persistTimeout)
		var persistErr error
		if req.success {
			persistErr = s.manager.channels.MarkSourceSuccess(persistCtx, req.sourceID, observedAt)
		} else {
			persistErr = s.manager.channels.MarkSourceFailure(persistCtx, req.sourceID, reason, observedAt)
		}
		cancel()

		if persistErr == nil {
			s.manager.markRecentSourceHealthPersisted(req.sourceID, req.eventID)
			return
		}

		if errors.Is(persistErr, channels.ErrSourceNotFound) {
			s.manager.dropRecentSourceHealthEvent(req.sourceID, req.eventID)
			return
		}

		if req.success {
			s.manager.logger.Warn(
				"failed to update source success state",
				"channel_id", req.channelID,
				"guide_number", req.guideNumber,
				"source_id", req.sourceID,
				"attempt", attempt,
				"error", persistErr,
			)
		} else {
			s.manager.logger.Warn(
				"failed to update source failure state",
				"channel_id", req.channelID,
				"guide_number", req.guideNumber,
				"source_id", req.sourceID,
				"reason", reason,
				"attempt", attempt,
				"error", persistErr,
			)
		}

		if attempt == sourceHealthPersistMaxAttempts {
			s.manager.dropRecentSourceHealthEvent(req.sourceID, req.eventID)
			return
		}

		if s.ctx.Err() != nil {
			s.manager.dropRecentSourceHealthEvent(req.sourceID, req.eventID)
			return
		}

		backoff := time.Duration(attempt) * 50 * time.Millisecond
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				s.manager.dropRecentSourceHealthEvent(req.sourceID, req.eventID)
				return
			}
			if remaining < backoff {
				backoff = remaining
			}
		}
		select {
		case <-time.After(backoff):
		case <-s.ctx.Done():
			s.manager.dropRecentSourceHealthEvent(req.sourceID, req.eventID)
			return
		}
	}
}

func (s *sharedRuntimeSession) enqueueSourceHealthPersist(req sourceHealthPersistRequest) {
	if s == nil || req.sourceID <= 0 {
		return
	}

	req.channelID = s.channel.ChannelID
	req.guideNumber = s.channel.GuideNumber
	req.observedAt = req.observedAt.UTC()
	if req.observedAt.IsZero() {
		req.observedAt = time.Now().UTC()
	}

	if s.sourceHealthPersistCh == nil {
		s.persistSourceHealth(req)
		return
	}

	// Safety net: if the persistence worker has already exited,
	// drop the staged event to prevent orphaned overlays.
	if s.sourceHealthPersistDone != nil {
		select {
		case <-s.sourceHealthPersistDone:
			if s.manager != nil {
				s.manager.dropRecentSourceHealthEvent(req.sourceID, req.eventID)
			}
			return
		default:
		}
	}

	select {
	case s.sourceHealthPersistCh <- req:
	default:
		s.enqueueCoalescedSourceHealthPersist(req)
	}
}

func (s *sharedRuntimeSession) enqueueCoalescedSourceHealthPersist(req sourceHealthPersistRequest) {
	if s == nil || req.sourceID <= 0 {
		return
	}

	var (
		coalescedReq       sourceHealthPersistRequest
		droppedReq         sourceHealthPersistRequest
		droppedEvent       bool
		coalescedTotal     int64
		coalescedForSource int64
		droppedTotal       int64
		droppedForSource   int64
	)

	s.sourceHealthPersistMu.Lock()
	if s.sourceHealthCoalesced == nil {
		s.sourceHealthCoalesced = make(map[int64]sourceHealthPersistRequest)
	}
	if s.sourceHealthCoalescedBySource == nil {
		s.sourceHealthCoalescedBySource = make(map[int64]int64)
	}
	if s.sourceHealthDroppedBySource == nil {
		s.sourceHealthDroppedBySource = make(map[int64]int64)
	}
	if existing, ok := s.sourceHealthCoalesced[req.sourceID]; ok {
		coalescedReq = existing
		s.sourceHealthCoalesced[req.sourceID] = req
		s.sourceHealthCoalescedTotal++
		s.sourceHealthCoalescedBySource[req.sourceID]++
		coalescedTotal = s.sourceHealthCoalescedTotal
		coalescedForSource = s.sourceHealthCoalescedBySource[req.sourceID]
		s.sourceHealthPersistMu.Unlock()

		if s.manager != nil {
			s.manager.dropRecentSourceHealthEvent(coalescedReq.sourceID, coalescedReq.eventID)
		}
		if s.manager != nil && s.manager.logger != nil {
			s.manager.logger.Warn(
				"coalesced source health persistence event",
				"channel_id", req.channelID,
				"guide_number", req.guideNumber,
				"source_id", req.sourceID,
				"success", req.success,
				"reason", strings.TrimSpace(req.reason),
				"coalesced_total", coalescedTotal,
				"coalesced_for_source", coalescedForSource,
			)
		}
		return
	}

	if len(s.sourceHealthCoalesced) >= defaultSourceHealthPersistQueueSize {
		droppedSourceID := int64(0)
		if len(s.sourceHealthQueue) > 0 {
			droppedSourceID = s.sourceHealthQueue[0]
			s.sourceHealthQueue = s.sourceHealthQueue[1:]
		}
		if droppedSourceID == 0 {
			for sourceID := range s.sourceHealthCoalesced {
				droppedSourceID = sourceID
				break
			}
		}
		if droppedSourceID > 0 {
			if existing, ok := s.sourceHealthCoalesced[droppedSourceID]; ok {
				droppedReq = existing
				droppedEvent = true
				delete(s.sourceHealthCoalesced, droppedSourceID)
				s.sourceHealthDroppedTotal++
				s.sourceHealthDroppedBySource[droppedReq.sourceID]++
				droppedTotal = s.sourceHealthDroppedTotal
				droppedForSource = s.sourceHealthDroppedBySource[droppedReq.sourceID]
			}
		}
	}

	s.sourceHealthCoalesced[req.sourceID] = req
	s.sourceHealthQueue = append(s.sourceHealthQueue, req.sourceID)
	s.sourceHealthPersistMu.Unlock()

	if droppedEvent && s.manager != nil {
		s.manager.dropRecentSourceHealthEvent(droppedReq.sourceID, droppedReq.eventID)
	}
	if droppedEvent && s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Warn(
			"dropped coalesced source health persistence event",
			"channel_id", droppedReq.channelID,
			"guide_number", droppedReq.guideNumber,
			"source_id", droppedReq.sourceID,
			"success", droppedReq.success,
			"reason", strings.TrimSpace(droppedReq.reason),
			"dropped_total", droppedTotal,
			"dropped_for_source", droppedForSource,
		)
	}
}

func (s *sharedRuntimeSession) waitReady(ctx context.Context) error {
	if s == nil {
		return errors.New("shared session is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.readyCh:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.readyErr
}

func (s *sharedRuntimeSession) run() {
	var terminalErr error
	defer func() {
		s.finish(terminalErr)
	}()

	reader, source, err := s.startInitialSource(s.ctx)
	if err != nil {
		terminalErr = err
		return
	}
	s.markReady(nil)
	if s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Info(
			"shared session ready",
			"channel_id", s.channel.ChannelID,
			"guide_number", s.channel.GuideNumber,
			"guide_name", s.channel.GuideName,
			"tuner_id", s.tunerID(),
			"source_id", source.SourceID,
			"session_started_at", s.startedAt,
		)
	}

	currentSource := source
	for {
		s.setManualRecoveryInProgress(false)
		cycleErr, stallDetected := s.runCycle(reader)
		if cycleErr == nil {
			return
		}
		if s.subscriberCount() == 0 {
			if s.manager != nil && s.manager.logger != nil {
				s.manager.logger.Info(
					"shared session skipped recovery while idle without subscribers",
					"channel_id", s.channel.ChannelID,
					"guide_number", s.channel.GuideNumber,
					"source_id", currentSource.SourceID,
					"reason", startupFailureReason(cycleErr),
					"stall_detected", stallDetected,
				)
			}
			terminalErr = nil
			return
		}

		reason := cycleErr
		if errors.Is(reason, io.EOF) {
			reason = errSourceEnded
		}
		_, syntheticManualRecovery := manualRecoveryReason(reason)

		cycleEndedAt := time.Now().UTC()
		sourceBytesPublished := s.sourceBytesPublishedSinceSelection()
		sourceStartupProbe := s.currentSourceStartupProbeTelemetry()
		sourceStartupProbeBytes := sourceStartupProbe.trimmedBytes
		trackCycleFailure, sourceUptime, sourceSelectReason := s.shouldTrackCycleFailure(cycleEndedAt)
		shortLivedCount := 0
		transientCooldown := time.Duration(0)
		transientCooldownUntil := time.Time{}
		unstableClassification := ""
		if !trackCycleFailure && !syntheticManualRecovery {
			unstableClassification = classifyShortLivedRecoveryFailure(reason, stallDetected)
			shortLivedCount, transientCooldownUntil, transientCooldown = s.recordShortLivedRecoveryFailure(
				currentSource.SourceID,
				unstableClassification,
				cycleEndedAt,
			)
		}
		// Always record the source failure that directly ended the active cycle.
		// Recovery-attempt startup failures are handled separately and may be
		// treated as transient for health scoring. Manual trigger recovery is a
		// synthetic signal and should not mark source failure state.
		if !syntheticManualRecovery {
			s.recordSourceFailure(s.ctx, currentSource.SourceID, currentSource.StreamURL, reason, trackCycleFailure)
		}
		if !trackCycleFailure && !syntheticManualRecovery && s.manager != nil && s.manager.logger != nil {
			fields := []any{
				"channel_id", s.channel.ChannelID,
				"guide_number", s.channel.GuideNumber,
				"source_id", currentSource.SourceID,
				"reason", startupFailureReason(reason),
				"source_uptime", sourceUptime.String(),
				"source_bytes_published", sourceBytesPublished,
				"source_startup_probe_raw_bytes", sourceStartupProbe.rawBytes,
				"source_startup_probe_trimmed_bytes", sourceStartupProbe.trimmedBytes,
				"source_startup_probe_cutover_offset", sourceStartupProbe.cutoverOffset,
				"source_startup_probe_dropped_bytes", sourceStartupProbe.droppedBytes,
				"source_startup_probe_bytes", sourceStartupProbeBytes,
				"source_select_reason", sourceSelectReason,
				"cycle_failure_min_health", s.manager.cfg.cycleFailureMinHealth.String(),
				"short_lived_recovery_count", shortLivedCount,
				"unstable_classification", unstableClassification,
			}
			if transientCooldown > 0 {
				fields = append(
					fields,
					"transient_cooldown", transientCooldown.String(),
					"transient_cooldown_until", transientCooldownUntil,
				)
			}
			s.manager.logger.Info("shared session cycle failure not persisted before healthy threshold", fields...)
		}

		if s.manager.cfg.stallPolicy == stallPolicyCloseSession {
			terminalErr = reason
			return
		}

		s.setManualRecoveryInProgress(true)
		recoveryReason := classifyRecoveryReason(reason, stallDetected)
		recoveryCycle, sinceLastSelect, sameSourceReselectCount, lastSelectedAt := s.beginRecoveryCycle(recoveryReason)

		recoverCtx := s.ctx
		cancel := func() {}
		if s.manager.cfg.stallHardDeadline > 0 {
			recoverCtx, cancel = context.WithTimeout(s.ctx, s.manager.cfg.stallHardDeadline)
		}

		recoveryPacingTarget, recoveryPacingWait, pacingErr := s.waitForRecoveryCyclePacing(
			recoverCtx,
			recoveryCycle,
			recoveryReason,
			sameSourceReselectCount,
			sinceLastSelect,
		)
		if pacingErr != nil {
			cancel()
			if errors.Is(pacingErr, errRecoveryAbortedNoSubscribers) {
				terminalErr = nil
				return
			}
			terminalErr = pacingErr
			return
		}
		if recoveryPacingWait > 0 {
			cycleEndedAt = time.Now().UTC()
			if !lastSelectedAt.IsZero() {
				sinceLastSelect = cycleEndedAt.Sub(lastSelectedAt)
				if sinceLastSelect < 0 {
					sinceLastSelect = 0
				}
			}
		}

		recoveryBurstCount, recoveryBurstBudgetCount, recoveryBurstLimit, recoveryBurstResetWindow, recoveryBurstPaceWindow, recoveryBurstStartedAt, recoveryBudgetExceeded := s.trackRecoveryBurst(
			cycleEndedAt,
			sinceLastSelect,
		)
		shouldLogRecovery, suppressedRecoveryLogs := s.shouldEmitRecoveryTriggerLog(
			cycleEndedAt,
			currentSource.SourceID,
			recoveryReason,
		)
		if shouldLogRecovery && s.manager != nil && s.manager.logger != nil {
			fields := []any{
				"channel_id", s.channel.ChannelID,
				"guide_number", s.channel.GuideNumber,
				"tuner_id", s.tunerID(),
				"source_id", currentSource.SourceID,
				"recovery_cycle", recoveryCycle,
				"recovery_reason", recoveryReason,
				"stall_detected", stallDetected,
				"same_source_reselect_count", sameSourceReselectCount,
				"since_last_source_select", sinceLastSelect.String(),
				"recovery_burst_count", recoveryBurstCount,
				"recovery_burst_budget_count", recoveryBurstBudgetCount,
				"recovery_burst_limit", recoveryBurstLimit,
				"recovery_burst_reset_window", recoveryBurstResetWindow.String(),
				"recovery_burst_pace_window", recoveryBurstPaceWindow.String(),
				"source_bytes_published", sourceBytesPublished,
				"source_startup_probe_raw_bytes", sourceStartupProbe.rawBytes,
				"source_startup_probe_trimmed_bytes", sourceStartupProbe.trimmedBytes,
				"source_startup_probe_cutover_offset", sourceStartupProbe.cutoverOffset,
				"source_startup_probe_dropped_bytes", sourceStartupProbe.droppedBytes,
				"source_startup_probe_bytes", sourceStartupProbeBytes,
			}
			if recoveryPacingTarget > 0 {
				fields = append(fields, "recovery_same_source_pacing_target", recoveryPacingTarget.String())
			}
			if recoveryPacingWait > 0 {
				fields = append(fields, "recovery_same_source_pacing_wait", recoveryPacingWait.String())
			}
			if !lastSelectedAt.IsZero() {
				fields = append(fields, "last_source_selected_at", lastSelectedAt)
			}
			if !recoveryBurstStartedAt.IsZero() {
				fields = append(fields, "recovery_burst_started_at", recoveryBurstStartedAt)
			}
			if suppressedRecoveryLogs > 0 {
				fields = append(fields, "recovery_trigger_logs_coalesced", suppressedRecoveryLogs)
			}
			s.manager.logger.Warn("shared session recovery triggered", fields...)
		}
		if recoveryBudgetExceeded {
			suppressedRecoveryLogs := suppressedRecoveryLogs + s.takeSuppressedRecoveryTriggerLogs()
			terminalErr = fmt.Errorf(
				"recovery cycle budget exhausted (cycles=%d, budget_cycles=%d, limit=%d, reset_window=%s, pace_window=%s)",
				recoveryBurstCount,
				recoveryBurstBudgetCount,
				recoveryBurstLimit,
				recoveryBurstResetWindow.String(),
				recoveryBurstPaceWindow.String(),
			)
			if s.manager != nil && s.manager.logger != nil {
				fields := []any{
					"channel_id", s.channel.ChannelID,
					"guide_number", s.channel.GuideNumber,
					"source_id", currentSource.SourceID,
					"recovery_cycle", recoveryCycle,
					"recovery_reason", recoveryReason,
					"recovery_burst_count", recoveryBurstCount,
					"recovery_burst_budget_count", recoveryBurstBudgetCount,
					"recovery_burst_limit", recoveryBurstLimit,
					"recovery_burst_reset_window", recoveryBurstResetWindow.String(),
					"recovery_burst_pace_window", recoveryBurstPaceWindow.String(),
					"source_bytes_published", sourceBytesPublished,
					"source_startup_probe_raw_bytes", sourceStartupProbe.rawBytes,
					"source_startup_probe_trimmed_bytes", sourceStartupProbe.trimmedBytes,
					"source_startup_probe_cutover_offset", sourceStartupProbe.cutoverOffset,
					"source_startup_probe_dropped_bytes", sourceStartupProbe.droppedBytes,
					"source_startup_probe_bytes", sourceStartupProbeBytes,
				}
				if recoveryPacingTarget > 0 {
					fields = append(fields, "recovery_same_source_pacing_target", recoveryPacingTarget.String())
				}
				if recoveryPacingWait > 0 {
					fields = append(fields, "recovery_same_source_pacing_wait", recoveryPacingWait.String())
				}
				if suppressedRecoveryLogs > 0 {
					fields = append(fields, "recovery_trigger_logs_coalesced", suppressedRecoveryLogs)
				}
				s.manager.logger.Warn("shared session recovery cycle budget exhausted", fields...)
			}
			cancel()
			return
		}

		stopHeartbeat := s.startRecoveryHeartbeat(recoverCtx)
		nextReader, nextSource, recoverErr := s.startRecoverySource(
			recoverCtx,
			currentSource,
			stallDetected,
			recoveryCycle,
			recoveryReason,
		)
		stopHeartbeat(recoverErr == nil)
		cancel()
		if recoverErr != nil {
			if errors.Is(recoverErr, errRecoveryAbortedNoSubscribers) {
				terminalErr = nil
				return
			}
			terminalErr = recoverErr
			return
		}

		reader = nextReader
		currentSource = nextSource
	}
}

func (s *sharedRuntimeSession) runCycle(reader io.ReadCloser) (error, bool) {
	runCtx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.pump.Run(runCtx, reader)
	}()

	checkInterval := 250 * time.Millisecond
	if s.manager.cfg.stallDetect > 0 && s.manager.cfg.stallDetect < checkInterval {
		checkInterval = s.manager.cfg.stallDetect / 2
		if checkInterval < 50*time.Millisecond {
			checkInterval = 50 * time.Millisecond
		}
	}
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	stallDetected := false
	cycleBaselineChunks := s.pump.Stats().ChunksPublished
	cycleStartTime := time.Now()
	manualReason := ""
	deferredManualReason := ""
	manualRecoveryCh := s.manualRecoveryCh
	sourceReadPauseStartedAt := time.Time{}
	startSourceReadPause := func(startedAt time.Time) {
		if startedAt.IsZero() || !sourceReadPauseStartedAt.IsZero() {
			return
		}
		sourceReadPauseStartedAt = startedAt
		incrementStreamSourceReadPauseInProgress()
	}
	clearSourceReadPauseWithoutTelemetry := func() {
		if sourceReadPauseStartedAt.IsZero() {
			return
		}
		sourceReadPauseStartedAt = time.Time{}
		decrementStreamSourceReadPauseInProgress()
	}
	finalizeSourceReadPause := func(now time.Time, reason string) {
		if sourceReadPauseStartedAt.IsZero() {
			return
		}
		pauseDuration := now.Sub(sourceReadPauseStartedAt)
		if pauseDuration < 0 {
			pauseDuration = 0
		}
		recordStreamSourceReadPauseTelemetry(reason, pauseDuration)
		sourceReadPauseStartedAt = time.Time{}
		decrementStreamSourceReadPauseInProgress()
	}
	for {
		if !stallDetected && deferredManualReason != "" && s.subscriberCount() > 0 {
			stallDetected = true
			manualReason = normalizeManualRecoveryReason(deferredManualReason)
			deferredManualReason = ""
			manualRecoveryCh = nil
			s.setManualRecoveryPending(false)
			s.setManualRecoveryInProgress(true)
			s.incrementStallCount()
			cancel()
		}

		select {
		case <-s.ctx.Done():
			finalizeSourceReadPause(time.Now(), sourceReadPauseReasonContextCancel)
			cancel()
			<-errCh
			return nil, false
		case requestedReason := <-manualRecoveryCh:
			normalizedReason := normalizeManualRecoveryReason(requestedReason)
			if s.subscriberCount() == 0 {
				deferredManualReason = normalizedReason
				s.setManualRecoveryPending(true)
				continue
			}
			if stallDetected {
				continue
			}
			stallDetected = true
			manualReason = normalizedReason
			manualRecoveryCh = nil
			s.setManualRecoveryPending(false)
			s.setManualRecoveryInProgress(true)
			s.incrementStallCount()
			cancel()
		case err := <-errCh:
			finalizeSourceReadPause(time.Now(), sourceReadPauseReasonPumpExit)
			return runCyclePumpResult(err, s.ctx.Err(), stallDetected, manualReason)
		case <-ticker.C:
			stats := s.pump.Stats()
			now := time.Now()
			lastByteReadAt := stats.LastByteReadAt
			if !lastByteReadAt.IsZero() {
				s.maybeLogFirstSourceBytes(lastByteReadAt)
			}
			if s.subscriberCount() == 0 {
				clearSourceReadPauseWithoutTelemetry()
				continue
			}
			if !lastByteReadAt.IsZero() {
				readGap := now.Sub(lastByteReadAt)
				if readGap < 0 {
					readGap = 0
				}
				if readGap >= sourceReadPauseMinDuration {
					startSourceReadPause(lastByteReadAt)
				} else {
					finalizeSourceReadPause(now, sourceReadPauseReasonRecovered)
				}
			}
			if s.manager.cfg.stallDetect <= 0 {
				continue
			}
			lastPublishAt := stats.LastPublishAt
			if lastPublishAt.IsZero() {
				continue
			}
			// Use cycle-aware stall baseline: if the pump has not yet
			// published in this cycle, compare against cycleStartTime
			// instead of the (potentially stale) prior-cycle lastPublishAt.
			// This prevents false stalls from inherited timestamps while
			// still detecting true stalls when a new source never publishes.
			stallBaseline := lastPublishAt
			if stats.ChunksPublished <= cycleBaselineChunks {
				stallBaseline = cycleStartTime
			}
			if time.Since(stallBaseline) >= s.manager.cfg.stallDetect && !stallDetected {
				stallDetected = true
				s.setManualRecoveryInProgress(true)
				s.incrementStallCount()
				cancel()
			}
		}
	}
}

func runCyclePumpResult(
	err error,
	sessionErr error,
	stallDetected bool,
	manualReason string,
) (error, bool) {
	if stallDetected && strings.TrimSpace(manualReason) != "" {
		return &manualRecoveryError{reason: manualReason}, true
	}
	if err == nil {
		return io.EOF, stallDetected
	}
	if errors.Is(err, context.Canceled) {
		if sessionErr != nil {
			return nil, false
		}
		if stallDetected {
			return errPumpStallDetected, true
		}
	}
	return err, stallDetected
}

func (s *sharedRuntimeSession) startInitialSource(ctx context.Context) (io.ReadCloser, channels.Source, error) {
	candidates, err := s.loadSourceCandidates(ctx)
	if err != nil {
		return nil, channels.Source{}, err
	}

	limited := limitSourcesByFailovers(candidates, s.manager.cfg.maxFailover)
	return s.startSourceWithCandidates(ctx, limited, s.manager.cfg.failoverTTL, "initial_startup", 0, "")
}

func (s *sharedRuntimeSession) startRecoverySource(
	ctx context.Context,
	currentSource channels.Source,
	stallDetected bool,
	recoveryCycle int64,
	recoveryReason string,
) (io.ReadCloser, channels.Source, error) {
	if err := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "load_candidates"); err != nil {
		return nil, channels.Source{}, err
	}

	candidates, err := s.loadSourceCandidates(ctx)
	if err != nil {
		return nil, channels.Source{}, err
	}

	failoverTTL := s.manager.cfg.failoverTTL
	if stallDetected && s.manager.cfg.stallHardDeadline > 0 && failoverTTL > s.manager.cfg.stallHardDeadline {
		failoverTTL = s.manager.cfg.stallHardDeadline
	}
	if failoverTTL <= 0 && s.manager.cfg.stallHardDeadline > 0 {
		failoverTTL = s.manager.cfg.stallHardDeadline
	}

	selectionReason := fmt.Sprintf("recovery_cycle_%d:%s", recoveryCycle, strings.TrimSpace(recoveryReason))
	now := time.Now().UTC()
	currentCandidate, currentCandidateEnabled := sourceByID(candidates, currentSource.SourceID)
	if s.manager.cfg.stallPolicy == stallPolicyFailoverSource {
		candidates = s.applyShortLivedRecoveryPenalties(candidates, now)
		ordered := orderFailoverRecoveryCandidates(candidates, currentSource.SourceID, now)
		limited := limitSourcesByFailovers(ordered, s.manager.cfg.maxFailover)
		if candidate, ok := sourceByID(limited, currentSource.SourceID); ok {
			currentCandidate = candidate
		}
		alternateCount, startupEligibleAlternateCount := failoverAlternateCandidateCounts(
			limited,
			currentSource.SourceID,
			now,
		)
		if startupEligibleAlternateCount == 0 && currentCandidateEnabled {
			if persisted, found := s.loadPersistedSourceCandidate(ctx, currentCandidate.SourceID); found {
				currentCandidate.CooldownUntil = persisted.CooldownUntil
			}
			s.logFailoverRestartSameFallback(
				recoveryCycle,
				recoveryReason,
				currentCandidate,
				alternateCount,
				startupEligibleAlternateCount,
			)
			return s.startCurrentSourceWithBackoff(
				ctx,
				currentCandidate,
				failoverTTL,
				recoveryCycle,
				recoveryReason,
				1,
			)
		}

		recoverDeadline := recoveryDeadline(ctx, failoverTTL)
		if alternate, ok := firstAlternateCandidate(limited, currentSource.SourceID, now); ok {
			if settleErr := s.waitForRecoveryFailoverSettle(ctx, recoverDeadline, recoveryCycle, recoveryReason); settleErr != nil {
				return nil, channels.Source{}, settleErr
			}
			if err := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "post_failover_settle"); err != nil {
				return nil, channels.Source{}, err
			}

			alternateBudget := recoveryAlternateAttemptWindow(time.Until(recoverDeadline), s.manager.cfg.startupWait)
			if alternateBudget > 0 {
				alternateCtx := ctx
				cancel := func() {}
				if deadline := time.Now().UTC().Add(alternateBudget); deadline.Before(recoverDeadline) {
					alternateCtx, cancel = context.WithDeadline(ctx, deadline)
				}
				reader, selected, alternateErr := s.startCurrentSourceWithBackoff(
					alternateCtx,
					alternate,
					alternateBudget,
					recoveryCycle,
					recoveryReason,
					recoveryAlternateMinStartupAttempts,
				)
				cancel()
				if alternateErr == nil {
					return reader, selected, nil
				}

				s.manager.logger.Warn(
					"shared session failover_source alternate retry window exhausted; falling back to full candidate pass",
					"channel_id", s.channel.ChannelID,
					"guide_number", s.channel.GuideNumber,
					"source_id", alternate.SourceID,
					"recovery_cycle", recoveryCycle,
					"recovery_reason", recoveryReason,
					"alternate_budget", alternateBudget.String(),
					"error", alternateErr,
				)
			}
		}

		remaining := time.Until(recoverDeadline)
		if remaining <= 0 {
			if ctx.Err() != nil {
				return nil, channels.Source{}, ctx.Err()
			}
			return nil, channels.Source{}, context.DeadlineExceeded
		}
		if err := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "pre_candidate_pass"); err != nil {
			return nil, channels.Source{}, err
		}
		limited = s.applyShortLivedRecoveryPenalties(limited, time.Now().UTC())
		return s.startSourceWithCandidates(
			ctx,
			limited,
			remaining,
			selectionReason,
			recoveryCycle,
			recoveryReason,
		)
	}

	// restart_same mode preserves source continuity during recovery and retries
	// only the current source with bounded backoff.
	if !currentCandidateEnabled {
		// The prior in-memory source is no longer enabled/eligible. Fall back to
		// enabled-candidate startup so stale disabled sources are not retried.
		limited := limitSourcesByFailovers(candidates, s.manager.cfg.maxFailover)
		return s.startSourceWithCandidates(
			ctx,
			limited,
			failoverTTL,
			selectionReason,
			recoveryCycle,
			recoveryReason,
		)
	}
	// Keep restart_same recovery responsive by ignoring pending in-memory
	// cooldown overlays while still honoring persisted cooldown from the store.
	if persisted, found := s.loadPersistedSourceCandidate(ctx, currentCandidate.SourceID); found {
		currentCandidate.CooldownUntil = persisted.CooldownUntil
	}
	if strings.TrimSpace(currentCandidate.StreamURL) == "" {
		return nil, channels.Source{}, ErrSessionNoSources
	}

	return s.startCurrentSourceWithBackoff(
		ctx,
		currentCandidate,
		failoverTTL,
		recoveryCycle,
		recoveryReason,
		1,
	)
}

func (s *sharedRuntimeSession) logFailoverRestartSameFallback(
	recoveryCycle int64,
	recoveryReason string,
	currentSource channels.Source,
	alternateCount int,
	startupEligibleAlternateCount int,
) {
	if s == nil || s.manager == nil || s.manager.logger == nil {
		return
	}

	s.manager.logger.Info(
		"shared session failover_source recovery fallback to same-source retry",
		"channel_id", s.channel.ChannelID,
		"guide_number", s.channel.GuideNumber,
		"source_id", currentSource.SourceID,
		"recovery_cycle", recoveryCycle,
		"recovery_reason", strings.TrimSpace(recoveryReason),
		"stall_policy", stallPolicyFailoverSource,
		"effective_recovery_mode", recoveryModeRestartSameFallback,
		"fallback_reason", recoveryFallbackReasonNoAlternates,
		"alternate_candidates", alternateCount,
		"alternate_startup_eligible", startupEligibleAlternateCount,
	)
}

func (s *sharedRuntimeSession) loadSourceCandidates(ctx context.Context) ([]channels.Source, error) {
	sources, err := s.manager.channels.ListSources(ctx, s.channel.ChannelID, true)
	if err != nil {
		if errors.Is(err, channels.ErrChannelNotFound) {
			return nil, ErrSessionNoSources
		}
		return nil, err
	}
	if len(sources) == 0 {
		return nil, ErrSessionNoSources
	}
	sources = s.manager.applyRecentSourceHealth(sources)

	return orderSourcesByAvailability(sources, time.Now().UTC()), nil
}

func (s *sharedRuntimeSession) loadPersistedSourceCandidate(ctx context.Context, sourceID int64) (channels.Source, bool) {
	if s == nil || s.manager == nil || s.manager.channels == nil || sourceID <= 0 {
		return channels.Source{}, false
	}

	if lookupProvider, ok := s.manager.channels.(singleSourceLookupProvider); ok {
		source, err := lookupProvider.GetSource(ctx, s.channel.ChannelID, sourceID, true)
		if err == nil {
			return source, true
		}
		return channels.Source{}, false
	}

	sources, err := s.manager.channels.ListSources(ctx, s.channel.ChannelID, true)
	if err != nil {
		return channels.Source{}, false
	}
	return sourceByID(sources, sourceID)
}

func (s *sharedRuntimeSession) startSourceWithCandidates(
	ctx context.Context,
	candidates []channels.Source,
	totalTimeout time.Duration,
	selectionReason string,
	recoveryCycle int64,
	recoveryReason string,
) (io.ReadCloser, channels.Source, error) {
	if len(candidates) == 0 {
		return nil, channels.Source{}, ErrSessionNoSources
	}

	selectionReason = strings.TrimSpace(selectionReason)
	if selectionReason == "" {
		selectionReason = "initial_startup"
	}

	if totalTimeout <= 0 {
		totalTimeout = s.manager.cfg.startupWait
	}

	startedAt := time.Now().UTC()
	deadline := startedAt.Add(totalTimeout)
	trackStartupFailures := recoveryCycle <= 0
	requireRandomAccess := s.shouldRequireStartupRandomAccess(recoveryCycle)

	var lastErr error
	for {
		if ctx.Err() != nil {
			return nil, channels.Source{}, ctx.Err()
		}
		if err := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "candidate_pass_loop"); err != nil {
			return nil, channels.Source{}, err
		}
		if time.Until(deadline) <= 0 {
			break
		}

		passCandidates := candidates
		if recoveryCycle > 0 && s.manager.cfg.stallPolicy == stallPolicyFailoverSource {
			passCandidates = s.applyShortLivedRecoveryPenalties(candidates, time.Now().UTC())
		}

		now := time.Now().UTC()
		forcedCoolingSourceID, forceCoolingAttempt := selectCoolingFallbackSource(passCandidates, now)
		nextCooldownAt := time.Time{}
		for i, source := range passCandidates {
			if ctx.Err() != nil {
				return nil, channels.Source{}, ctx.Err()
			}
			if err := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "candidate_iteration"); err != nil {
				return nil, channels.Source{}, err
			}

			if cooldownAt, cooling := sourceCooldownDeadline(source); cooling {
				if !forceCoolingAttempt || source.SourceID != forcedCoolingSourceID {
					if nextCooldownAt.IsZero() || cooldownAt.Before(nextCooldownAt) {
						nextCooldownAt = cooldownAt
					}
					continue
				}
				forceCoolingAttempt = false
			}

			streamURL := strings.TrimSpace(source.StreamURL)
			if streamURL == "" {
				if err := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "candidate_empty_stream_url"); err != nil {
					return nil, channels.Source{}, err
				}
				lastErr = &sourceStartupError{
					reason: "source stream URL is empty",
					err:    errors.New("source stream URL is empty"),
				}
				s.recordSourceFailure(ctx, source.SourceID, source.StreamURL, lastErr, trackStartupFailures)
				s.recordRecoveryAlternateStartupFailure(source.SourceID, lastErr, recoveryCycle)
				continue
			}

			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			if err := s.waitForProviderOverlimitCooldown(
				ctx,
				deadline,
				source.SourceID,
				source.StreamURL,
				recoveryCycle,
				recoveryReason,
			); err != nil {
				return nil, channels.Source{}, err
			}
			remaining = time.Until(deadline)
			if remaining <= 0 {
				break
			}

			startupTimeout := s.manager.cfg.startupWait
			if startupTimeout <= 0 || startupTimeout > remaining {
				startupTimeout = remaining
			}
			if startupTimeout <= 0 {
				break
			}

			startupCtx, stopStartupCtx, startupCanceledByIdleGuard := s.startupAttemptContext(
				ctx,
				recoveryCycle,
				recoveryReason,
				"candidate_startup_attempt",
			)
			startupPTSOffset := time.Duration(0)
			if recoveryCycle > 0 {
				startupPTSOffset = s.currentRecoveryTimelinePTSOffset()
			}
			reader, producer, startupProbe, startupRandomAccessReady, startupRandomAccessCodec, startupInventory, startupRetryRelaxedProbe, startupRetryReason, err := s.startSourceReader(
				startupCtx,
				source.SourceID,
				streamURL,
				startupTimeout,
				requireRandomAccess,
				startupPTSOffset,
			)
			stopStartupCtx()
			if err != nil {
				lastErr = err
				// Only treat context cancellation/deadline as terminal when the
				// parent session context itself is canceled/deadline-exceeded.
				// Startup probes and HTTP clients can surface DeadlineExceeded as
				// source failures, which should still be eligible for failover.
				if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					return nil, channels.Source{}, err
				}
				if guardErr := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "candidate_startup_failure"); guardErr != nil {
					return nil, channels.Source{}, guardErr
				}

				s.recordSourceFailure(ctx, source.SourceID, source.StreamURL, err, trackStartupFailures)
				if !(startupCanceledByIdleGuard() && isRecoveryStartupLifecycleCancellation(err)) {
					s.recordRecoveryAlternateStartupFailure(source.SourceID, err, recoveryCycle)
				}
				fields := []any{
					"channel_id", s.channel.ChannelID,
					"guide_number", s.channel.GuideNumber,
					"source_id", source.SourceID,
					"source_item_key", source.ItemKey,
					"reason", startupFailureReason(err),
					"idle_startup_canceled", startupCanceledByIdleGuard(),
				}
				if recoveryCycle > 0 {
					fields = append(fields, "recovery_cycle", recoveryCycle, "recovery_reason", recoveryReason)
				}
				s.manager.logger.Warn("shared session source startup failed", fields...)
				if i < len(passCandidates)-1 {
					if settleErr := s.waitForFailoverRetrySettle(ctx, deadline, recoveryCycle, recoveryReason); settleErr != nil {
						return nil, channels.Source{}, settleErr
					}
				}
				continue
			}
			if guardErr := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "candidate_startup_success"); guardErr != nil {
				closeWithTimeout(reader, s.manager.cfg.sessionDrainTimeout)
				return nil, channels.Source{}, guardErr
			}

			startupInventory = enhanceInventoryWithPersistedProfile(startupInventory, source)
			s.setSourceStateWithStartupProbe(
				source,
				producer,
				selectionReason,
				startupProbe,
				startupRandomAccessReady,
				startupRandomAccessCodec,
				startupInventory,
				startupRetryRelaxedProbe,
				startupRetryReason,
			)
			s.recordSourceSuccess(ctx, source.SourceID)
			s.manager.clearProviderOverlimitCooldownForSource(source.SourceID, source.StreamURL)
			return reader, source, nil
		}

		if !nextCooldownAt.IsZero() {
			var err error
			if recoveryCycle > 0 {
				err = s.waitUntilRecoveryWithContext(
					ctx,
					deadline,
					nextCooldownAt,
					recoveryCycle,
					recoveryReason,
					"candidate_cooldown_wait",
				)
			} else {
				err = waitUntilWithContext(ctx, deadline, nextCooldownAt)
			}
			if err != nil {
				return nil, channels.Source{}, err
			}
			if time.Until(deadline) > 0 {
				lastErr = errors.New("all source candidates currently in cooldown")
				continue
			}
		}

		if !shouldRetryOverlimitStartupPass(lastErr, deadline, s.manager.cfg.upstreamOverlimitCooldown) {
			break
		}
	}

	if lastErr == nil {
		lastErr = errors.New("all source attempts exhausted")
	}
	return nil, channels.Source{}, lastErr
}

func (s *sharedRuntimeSession) shouldRequireStartupRandomAccess(recoveryCycle int64) bool {
	if recoveryCycle > 0 {
		return true
	}
	if s == nil || s.manager == nil {
		return false
	}
	if s.manager.cfg.startupRandomAccessRecoveryOnly {
		return false
	}
	if !s.manager.cfg.recoveryFillerEnabled {
		return false
	}
	if strings.TrimSpace(s.manager.cfg.recoveryFillerMode) != recoveryFillerModeSlateAV {
		return false
	}
	switch strings.TrimSpace(s.manager.cfg.mode) {
	case "ffmpeg-copy", "ffmpeg-transcode":
		return true
	default:
		return false
	}
}

func selectCoolingFallbackSource(candidates []channels.Source, now time.Time) (int64, bool) {
	if len(candidates) == 0 {
		return 0, false
	}

	nowUnix := now.UTC().Unix()
	cooling := make([]channels.Source, 0, len(candidates))
	for i := range candidates {
		source := candidates[i]
		if source.CooldownUntil <= nowUnix {
			return 0, false
		}
		cooling = append(cooling, source)
	}

	sort.SliceStable(cooling, func(i, j int) bool {
		a := cooling[i]
		b := cooling[j]
		if a.FailCount != b.FailCount {
			return a.FailCount < b.FailCount
		}
		if a.PriorityIndex != b.PriorityIndex {
			return a.PriorityIndex < b.PriorityIndex
		}
		if a.LastFailAt != b.LastFailAt {
			return a.LastFailAt < b.LastFailAt
		}
		return a.SourceID < b.SourceID
	})

	return cooling[0].SourceID, true
}

func shouldRetryOverlimitStartupPass(lastErr error, deadline time.Time, overlimitCooldown time.Duration) bool {
	if lastErr == nil || overlimitCooldown <= 0 {
		return false
	}
	if time.Until(deadline) <= 0 {
		return false
	}

	reason := startupFailureReason(lastErr)
	code := upstreamStatusCode(reason)
	return isLikelyUpstreamOverlimitStatus(code)
}

func sourceByID(sources []channels.Source, sourceID int64) (channels.Source, bool) {
	if sourceID <= 0 {
		return channels.Source{}, false
	}
	for _, source := range sources {
		if source.SourceID == sourceID {
			return source, true
		}
	}
	return channels.Source{}, false
}

func recoveryDeadline(ctx context.Context, totalTimeout time.Duration) time.Time {
	deadline := time.Now().UTC().Add(totalTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		return ctxDeadline
	}
	return deadline
}

func firstAlternateCandidate(candidates []channels.Source, currentSourceID int64, now time.Time) (channels.Source, bool) {
	nowUnix := now.UTC().Unix()
	for _, source := range candidates {
		if source.SourceID == currentSourceID {
			continue
		}
		if strings.TrimSpace(source.StreamURL) == "" {
			continue
		}
		if source.CooldownUntil > nowUnix {
			continue
		}
		return source, true
	}
	return channels.Source{}, false
}

func failoverAlternateCandidateCounts(candidates []channels.Source, currentSourceID int64, now time.Time) (int, int) {
	if len(candidates) == 0 {
		return 0, 0
	}

	nowUnix := now.UTC().Unix()
	alternateCount := 0
	startupEligibleAlternateCount := 0
	for _, source := range candidates {
		if source.SourceID == currentSourceID {
			continue
		}
		if strings.TrimSpace(source.StreamURL) == "" {
			continue
		}
		alternateCount++
		if source.CooldownUntil <= nowUnix {
			startupEligibleAlternateCount++
		}
	}
	return alternateCount, startupEligibleAlternateCount
}

func recoveryAlternateAttemptWindow(remaining, startupWait time.Duration) time.Duration {
	if remaining <= 0 {
		return 0
	}
	if remaining <= recoveryAlternateFallbackReserve {
		return remaining
	}

	window := startupWait * 2
	if window < recoveryAlternateMinAttemptWindow {
		window = recoveryAlternateMinAttemptWindow
	}
	if window > recoveryAlternateMaxAttemptWindow {
		window = recoveryAlternateMaxAttemptWindow
	}

	maxWindow := remaining - recoveryAlternateFallbackReserve
	if maxWindow <= 0 {
		return remaining
	}
	if window > maxWindow {
		window = maxWindow
	}
	if window <= 0 {
		return remaining
	}
	return window
}

func recoveryAlternateSettleDelay(startupWait, stallDetect time.Duration) time.Duration {
	if startupWait <= 0 {
		startupWait = 800 * time.Millisecond
	}
	wait := startupWait / 2
	if wait < 400*time.Millisecond {
		wait = 400 * time.Millisecond
	}
	if wait > 2*time.Second {
		wait = 2 * time.Second
	}
	if stallDetect > 0 {
		maxWait := stallDetect / 2
		if maxWait < 50*time.Millisecond {
			maxWait = 50 * time.Millisecond
		}
		if wait > maxWait {
			wait = maxWait
		}
	}
	return wait
}

func recoveryRetryBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return 250 * time.Millisecond
	}

	backoff := 250 * time.Millisecond
	for i := 1; i < attempt; i++ {
		backoff *= 2
		if backoff >= 2*time.Second {
			return 2 * time.Second
		}
	}
	return backoff
}

func recoveryStartupAttemptLimit(stallMaxFailoversPerStall int) int {
	if stallMaxFailoversPerStall < 0 {
		stallMaxFailoversPerStall = 0
	}
	limit := stallMaxFailoversPerStall + 1
	if limit < 1 {
		limit = 1
	}
	return limit
}

func classifyRecoveryReason(err error, stallDetected bool) string {
	if reason, ok := manualRecoveryReason(err); ok {
		return reason
	}
	if stallDetected || errors.Is(err, errPumpStallDetected) {
		return "stall"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, errSourceEnded) {
		return "source_eof"
	}

	reason := startupFailureReason(err)
	if code := upstreamStatusCode(reason); code > 0 {
		return "upstream_" + strconv.Itoa(code)
	}
	if strings.TrimSpace(reason) == "" {
		return "source_error"
	}
	return "source_error"
}

func (s *sharedRuntimeSession) beginRecoveryCycle(
	recoveryReason string,
) (int64, time.Duration, int64, time.Time) {
	if s == nil {
		return 0, 0, 0, time.Time{}
	}

	recoveryReason = strings.TrimSpace(recoveryReason)
	if recoveryReason == "" {
		recoveryReason = "source_error"
	}

	now := time.Now().UTC()
	s.mu.Lock()
	s.recoveryCycle++
	s.recoveryReason = recoveryReason
	cycle := s.recoveryCycle
	lastSelectedAt := s.lastSourceSelectedAt
	sameSourceReselectCount := s.sameSourceReselectCount
	s.mu.Unlock()

	if lastSelectedAt.IsZero() {
		return cycle, 0, sameSourceReselectCount, time.Time{}
	}

	sinceLastSelect := now.Sub(lastSelectedAt)
	if sinceLastSelect < 0 {
		sinceLastSelect = 0
	}
	return cycle, sinceLastSelect, sameSourceReselectCount, lastSelectedAt
}

func recoverySameSourcePacingDelay(recoveryReason string, sameSourceReselectCount int64) time.Duration {
	if strings.TrimSpace(recoveryReason) != "source_eof" || sameSourceReselectCount <= 0 {
		return 0
	}

	attempt := int(sameSourceReselectCount)
	if attempt < 1 {
		attempt = 1
	}
	return recoveryRetryBackoff(attempt)
}

func (s *sharedRuntimeSession) waitForRecoveryCyclePacing(
	ctx context.Context,
	recoveryCycle int64,
	recoveryReason string,
	sameSourceReselectCount int64,
	sinceLastSelect time.Duration,
) (time.Duration, time.Duration, error) {
	if s == nil || s.manager == nil {
		return 0, 0, nil
	}
	if err := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "recovery_cycle_pacing"); err != nil {
		return 0, 0, err
	}

	target := recoverySameSourcePacingDelay(recoveryReason, sameSourceReselectCount)
	if target <= 0 {
		return 0, 0, nil
	}

	if sinceLastSelect < 0 {
		sinceLastSelect = 0
	}
	if sinceLastSelect >= target {
		return target, 0, nil
	}

	wait := target - sinceLastSelect
	if err := s.waitRecoveryWithContext(
		ctx,
		wait,
		recoveryCycle,
		recoveryReason,
		"recovery_cycle_pacing",
	); err != nil {
		return target, 0, err
	}
	return target, wait, nil
}

func (s *sharedRuntimeSession) shouldEmitRecoveryTriggerLog(
	now time.Time,
	sourceID int64,
	recoveryReason string,
) (bool, int64) {
	if s == nil {
		return true, 0
	}

	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	recoveryReason = strings.TrimSpace(recoveryReason)
	if recoveryReason == "" {
		recoveryReason = "source_error"
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sameEvent := !s.recoveryTriggerLastLogAt.IsZero() &&
		now.Sub(s.recoveryTriggerLastLogAt) < recoveryTriggerLogCoalesceWindow &&
		s.recoveryTriggerLastSourceID == sourceID &&
		strings.TrimSpace(s.recoveryTriggerLastReason) == recoveryReason
	if sameEvent {
		s.recoveryTriggerLogsSuppressed++
		return false, 0
	}

	suppressed := s.recoveryTriggerLogsSuppressed
	s.recoveryTriggerLogsSuppressed = 0
	s.recoveryTriggerLastLogAt = now
	s.recoveryTriggerLastSourceID = sourceID
	s.recoveryTriggerLastReason = recoveryReason
	return true, suppressed
}

func (s *sharedRuntimeSession) takeSuppressedRecoveryTriggerLogs() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	suppressed := s.recoveryTriggerLogsSuppressed
	s.recoveryTriggerLogsSuppressed = 0
	return suppressed
}

func (s *sharedRuntimeSession) shouldEmitSlateAVCloseWarn(now time.Time) (bool, int64) {
	if s == nil {
		return true, 0
	}

	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.slateAVCloseWarnLastLogAt.IsZero() &&
		now.Sub(s.slateAVCloseWarnLastLogAt) < slateAVCloseWarnCoalesceWindow {
		s.slateAVCloseWarnLogsSuppressed++
		return false, 0
	}

	suppressed := s.slateAVCloseWarnLogsSuppressed
	s.slateAVCloseWarnLogsSuppressed = 0
	s.slateAVCloseWarnLastLogAt = now
	return true, suppressed
}

func (s *sharedRuntimeSession) sourceBytesPublishedSinceSelection() int64 {
	if s == nil || s.pump == nil {
		return 0
	}

	pumpStats := s.pump.Stats()
	s.mu.Lock()
	base := s.sourceSelectBytesBase
	s.mu.Unlock()

	if pumpStats.BytesPublished <= base {
		return 0
	}
	return pumpStats.BytesPublished - base
}

func (s *sharedRuntimeSession) currentSourceStartupProbeBytes() int {
	telemetry := s.currentSourceStartupProbeTelemetry()
	if telemetry.trimmedBytes <= 0 {
		return 0
	}
	return telemetry.trimmedBytes
}

func (s *sharedRuntimeSession) currentSourceStartupProbeTelemetry() startupProbeTelemetry {
	if s == nil {
		return startupProbeTelemetry{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	telemetry := startupProbeTelemetryFromLengths(
		s.sourceStartupProbeRawBytes,
		s.sourceStartupProbeTrimmedBytes,
		s.sourceStartupProbeCutoverOffset,
	)
	if telemetry.trimmedBytes == 0 && s.sourceStartupProbeBytes > 0 {
		telemetry = startupProbeTelemetryFromLengths(
			s.sourceStartupProbeBytes,
			s.sourceStartupProbeBytes,
			0,
		)
	}
	return telemetry
}

func (s *sharedRuntimeSession) shortLivedRecoveryCount(sourceID int64) int {
	if s == nil || sourceID <= 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	penalty, ok := s.shortLivedRecoveryBySource[sourceID]
	if !ok {
		return 0
	}
	return penalty.count
}

func (s *sharedRuntimeSession) shouldRecordRecoveryAlternateStartupFailure(sourceID int64, recoveryCycle int64) bool {
	if s == nil || s.manager == nil || sourceID <= 0 || recoveryCycle <= 0 {
		return false
	}
	if s.manager.cfg.stallPolicy != stallPolicyFailoverSource {
		return false
	}
	currentSourceID := s.currentSourceID()
	if currentSourceID > 0 && sourceID == currentSourceID {
		return false
	}
	return true
}

func (s *sharedRuntimeSession) recordRecoveryAlternateStartupFailure(sourceID int64, err error, recoveryCycle int64) {
	if !s.shouldRecordRecoveryAlternateStartupFailure(sourceID, recoveryCycle) {
		return
	}

	classification := strings.TrimSpace(classifyRecoveryReason(err, false))
	if classification == "" {
		classification = "source_error"
	}
	s.recordShortLivedRecoveryFailure(
		sourceID,
		recoveryAlternatePenaltyReasonPrefix+classification,
		time.Now().UTC(),
	)
}

func (s *sharedRuntimeSession) clearShortLivedRecoveryPenalty(sourceID int64) {
	if s == nil || sourceID <= 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.shortLivedRecoveryBySource) == 0 {
		return
	}
	delete(s.shortLivedRecoveryBySource, sourceID)
}

func (s *sharedRuntimeSession) applyShortLivedRecoveryPenalties(
	candidates []channels.Source,
	now time.Time,
) []channels.Source {
	if len(candidates) == 0 {
		return nil
	}

	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}

	out := append([]channels.Source(nil), candidates...)
	if s == nil {
		return out
	}

	maxInt := int(^uint(0) >> 1)

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.shortLivedRecoveryBySource) == 0 {
		return out
	}

	for sourceID, penalty := range s.shortLivedRecoveryBySource {
		if penalty.lastObservedAt.IsZero() {
			delete(s.shortLivedRecoveryBySource, sourceID)
			continue
		}
		if now.Sub(penalty.lastObservedAt) > recoveryTransientPenaltyStateTTL {
			delete(s.shortLivedRecoveryBySource, sourceID)
		}
	}

	for i := range out {
		penalty, ok := s.shortLivedRecoveryBySource[out[i].SourceID]
		if !ok {
			continue
		}
		if penalty.cooldownUntil.After(now) {
			cooldownUnix := penalty.cooldownUntil.UTC().Unix()
			if cooldownUnix > out[i].CooldownUntil {
				out[i].CooldownUntil = cooldownUnix
			}
		}
		if penalty.count > 0 {
			boosted := out[i].FailCount
			if boosted > maxInt-penalty.count {
				boosted = maxInt
			} else {
				boosted += penalty.count
			}
			out[i].FailCount = boosted
		}
	}

	return out
}

func (s *sharedRuntimeSession) recordShortLivedRecoveryFailure(
	sourceID int64,
	reason string,
	observedAt time.Time,
) (int, time.Time, time.Duration) {
	if s == nil || sourceID <= 0 {
		return 0, time.Time{}, 0
	}

	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	} else {
		observedAt = observedAt.UTC()
	}
	reason = strings.TrimSpace(reason)

	s.mu.Lock()
	if s.shortLivedRecoveryBySource == nil {
		s.shortLivedRecoveryBySource = make(map[int64]shortLivedRecoveryPenalty)
	}
	penalty := s.shortLivedRecoveryBySource[sourceID]
	if !penalty.lastObservedAt.IsZero() && observedAt.Sub(penalty.lastObservedAt) > recoveryTransientPenaltyResetWindow {
		penalty.count = 0
	}
	penalty.count++
	penalty.lastObservedAt = observedAt
	penalty.lastReason = reason
	duration := shortLivedRecoveryPenaltyDuration(penalty.count)
	penalty.cooldownUntil = observedAt.Add(duration)
	s.shortLivedRecoveryBySource[sourceID] = penalty
	count := penalty.count
	cooldownUntil := penalty.cooldownUntil
	s.mu.Unlock()

	return count, cooldownUntil, duration
}

func shortLivedRecoveryPenaltyDuration(shortLivedCount int) time.Duration {
	if shortLivedCount <= 1 {
		return recoveryTransientPenaltyBase
	}

	penalty := recoveryTransientPenaltyBase
	for i := 1; i < shortLivedCount; i++ {
		penalty *= 2
		if penalty >= recoveryTransientPenaltyMax {
			return recoveryTransientPenaltyMax
		}
	}
	if penalty > recoveryTransientPenaltyMax {
		return recoveryTransientPenaltyMax
	}
	return penalty
}

func classifyShortLivedRecoveryFailure(err error, stallDetected bool) string {
	reason := strings.TrimSpace(classifyRecoveryReason(err, stallDetected))
	if reason == "" {
		reason = "source_error"
	}
	return "startup_passed_unstable_" + reason
}

func recoveryCycleBudgetLimit(stallMaxFailoversPerStall int) int {
	attemptLimit := recoveryStartupAttemptLimit(stallMaxFailoversPerStall)
	limit := attemptLimit * recoveryCycleBudgetMultiplier
	if limit < recoveryCycleBudgetMin {
		limit = recoveryCycleBudgetMin
	}
	if limit > recoveryCycleBudgetMax {
		limit = recoveryCycleBudgetMax
	}
	return limit
}

func recoveryCycleBudgetResetWindow(stallDetect, startupWait time.Duration) time.Duration {
	window := stallDetect*2 + startupWait
	if window < recoveryCycleBudgetResetMinWindow {
		window = recoveryCycleBudgetResetMinWindow
	}
	if window > recoveryCycleBudgetResetMaxWindow {
		window = recoveryCycleBudgetResetMaxWindow
	}
	return window
}

func recoveryCycleBudgetPaceWindow(stallDetect, startupWait time.Duration) time.Duration {
	window := recoveryRetryBackoff(1)
	if stallDetect > 0 && stallDetect < window {
		window = stallDetect
	}
	if startupWait > 0 && startupWait < window {
		window = startupWait
	}
	if window < recoveryCycleBudgetPaceMinWindow {
		window = recoveryCycleBudgetPaceMinWindow
	}
	if window > recoveryCycleBudgetPaceMaxWindow {
		window = recoveryCycleBudgetPaceMaxWindow
	}
	return window
}

func (s *sharedRuntimeSession) trackRecoveryBurst(
	now time.Time,
	sinceLastSelect time.Duration,
) (int, int, int, time.Duration, time.Duration, time.Time, bool) {
	if s == nil || s.manager == nil {
		return 0, 0, 0, 0, 0, time.Time{}, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if sinceLastSelect < 0 {
		sinceLastSelect = 0
	}

	limit := recoveryCycleBudgetLimit(s.manager.cfg.stallMaxFailoversPerTry)
	resetWindow := recoveryCycleBudgetResetWindow(s.manager.cfg.stallDetect, s.manager.cfg.startupWait)
	paceWindow := recoveryCycleBudgetPaceWindow(s.manager.cfg.stallDetect, s.manager.cfg.startupWait)

	s.mu.Lock()
	if s.recoveryBurstCount <= 0 || sinceLastSelect >= resetWindow {
		s.recoveryBurstCount = 1
		s.recoveryBurstStartedAt = now
	} else {
		s.recoveryBurstCount++
	}
	count := s.recoveryBurstCount
	startedAt := s.recoveryBurstStartedAt
	s.mu.Unlock()

	budgetCount := count
	if !startedAt.IsZero() && paceWindow > 0 {
		elapsed := now.Sub(startedAt)
		if elapsed < 0 {
			elapsed = 0
		}
		pacedCount := int(elapsed/paceWindow) + 1
		if pacedCount < budgetCount {
			budgetCount = pacedCount
		}
	}

	exhausted := limit > 0 && budgetCount > limit
	return count, budgetCount, limit, resetWindow, paceWindow, startedAt, exhausted
}

func (s *sharedRuntimeSession) shouldTrackCycleFailure(now time.Time) (bool, time.Duration, string) {
	if s == nil || s.manager == nil {
		return true, 0, ""
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	minHealth := s.manager.cfg.cycleFailureMinHealth
	if minHealth <= 0 {
		return true, 0, ""
	}

	s.mu.Lock()
	selectedAt := s.lastSourceSelectedAt
	selectionReason := strings.TrimSpace(s.lastSourceSelectReason)
	s.mu.Unlock()

	if selectedAt.IsZero() {
		return true, 0, selectionReason
	}
	healthyFor := now.Sub(selectedAt)
	if healthyFor < 0 {
		healthyFor = 0
	}

	// Only suppress cycle-close failure persistence for sources that were chosen
	// during recovery and never stayed healthy long enough to be trusted.
	if strings.HasPrefix(selectionReason, "recovery_cycle_") && healthyFor < minHealth {
		return false, healthyFor, selectionReason
	}
	return true, healthyFor, selectionReason
}

func isRecoverySelectionReason(selectionReason string) bool {
	return strings.HasPrefix(strings.TrimSpace(selectionReason), "recovery_cycle_")
}

func recoveryProfileProbeDelay(stallDetect, cycleFailureMinHealth time.Duration) time.Duration {
	if cycleFailureMinHealth <= 0 {
		return 0
	}

	delay := cycleFailureMinHealth
	if stallDetect > 0 && stallDetect < delay {
		delay = stallDetect
	}
	if delay < profileProbeRecoveryDelayMin {
		delay = profileProbeRecoveryDelayMin
	}
	if delay > profileProbeRecoveryDelayMax {
		delay = profileProbeRecoveryDelayMax
	}
	return delay
}

func profileProbeRestartCooldown(stallDetect time.Duration) time.Duration {
	if stallDetect <= 0 {
		return 0
	}

	cooldown := stallDetect / 2
	if cooldown < profileProbeRestartCooldownMin {
		cooldown = profileProbeRestartCooldownMin
	}
	if cooldown > profileProbeRestartCooldownMax {
		cooldown = profileProbeRestartCooldownMax
	}
	return cooldown
}

func (s *sharedRuntimeSession) profileProbeScheduleDelayLocked(selectionReason string, now time.Time) time.Duration {
	if s == nil || s.manager == nil {
		return 0
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	delay := time.Duration(0)
	if isRecoverySelectionReason(selectionReason) {
		delay = recoveryProfileProbeDelay(s.manager.cfg.stallDetect, s.manager.cfg.cycleFailureMinHealth)
	}

	cooldown := profileProbeRestartCooldown(s.manager.cfg.stallDetect)
	if cooldown <= 0 || s.profileProbeLastStartedAt.IsZero() {
		return delay
	}
	remaining := cooldown - now.Sub(s.profileProbeLastStartedAt)
	if remaining <= 0 {
		return delay
	}
	if remaining > delay {
		return remaining
	}
	return delay
}

func (s *sharedRuntimeSession) tunerID() int {
	if s == nil {
		return -1
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease == nil {
		return -1
	}
	return s.lease.ID
}

func waitWithContext(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func waitUntilWithContext(ctx context.Context, deadline time.Time, until time.Time) error {
	if until.IsZero() {
		return nil
	}

	wait := time.Until(until)
	if wait <= 0 {
		return nil
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil
	}
	if wait > remaining {
		wait = remaining
	}
	return waitWithContext(ctx, wait)
}

func (s *sharedRuntimeSession) abortRecoveryIfIdle(recoveryCycle int64, recoveryReason, stage string) error {
	if recoveryCycle <= 0 || s == nil {
		return nil
	}
	if s.subscriberCount() > 0 {
		return nil
	}

	if s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Info(
			"shared session aborted recovery while idle without subscribers",
			"channel_id", s.channel.ChannelID,
			"guide_number", s.channel.GuideNumber,
			"source_id", s.currentSourceID(),
			"recovery_cycle", recoveryCycle,
			"recovery_reason", strings.TrimSpace(recoveryReason),
			"stage", strings.TrimSpace(stage),
		)
	}
	return errRecoveryAbortedNoSubscribers
}

func isRecoveryStartupLifecycleCancellation(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (s *sharedRuntimeSession) waitRecoveryWithContext(
	ctx context.Context,
	wait time.Duration,
	recoveryCycle int64,
	recoveryReason string,
	stage string,
) error {
	if wait <= 0 {
		return s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, stage)
	}
	if err := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, stage); err != nil {
		return err
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	poll := time.NewTicker(recoverySubscriberGuardPollInterval)
	defer poll.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, stage)
		case <-poll.C:
			if err := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, stage); err != nil {
				return err
			}
		}
	}
}

func (s *sharedRuntimeSession) startupAttemptContext(
	ctx context.Context,
	recoveryCycle int64,
	recoveryReason string,
	stage string,
) (context.Context, context.CancelFunc, func() bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if recoveryCycle <= 0 || s == nil {
		return ctx, func() {}, func() bool { return false }
	}

	attemptCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var canceledByIdleGuard atomic.Bool
	go func() {
		defer close(done)

		poll := time.NewTicker(recoverySubscriberGuardPollInterval)
		defer poll.Stop()

		for {
			select {
			case <-attemptCtx.Done():
				return
			case <-poll.C:
				if s.subscriberCount() > 0 {
					continue
				}
				s.mu.Lock()
				hook := s.startupAttemptIdleCancelHook
				s.mu.Unlock()
				if hook != nil {
					hook()
				}
				canceledByIdleGuard.Store(true)
				if s.manager != nil && s.manager.logger != nil {
					s.manager.logger.Info(
						"shared session canceled startup attempt while idle without subscribers",
						"channel_id", s.channel.ChannelID,
						"guide_number", s.channel.GuideNumber,
						"source_id", s.currentSourceID(),
						"recovery_cycle", recoveryCycle,
						"recovery_reason", strings.TrimSpace(recoveryReason),
						"stage", strings.TrimSpace(stage),
					)
				}
				cancel()
				return
			}
		}
	}()

	return attemptCtx, func() {
			cancel()
			<-done
		}, func() bool {
			return canceledByIdleGuard.Load()
		}
}

func (s *sharedRuntimeSession) waitUntilRecoveryWithContext(
	ctx context.Context,
	deadline time.Time,
	until time.Time,
	recoveryCycle int64,
	recoveryReason string,
	stage string,
) error {
	if until.IsZero() {
		return s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, stage)
	}

	wait := time.Until(until)
	if wait <= 0 {
		return s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, stage)
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, stage)
	}
	if wait > remaining {
		wait = remaining
	}

	return s.waitRecoveryWithContext(ctx, wait, recoveryCycle, recoveryReason, stage)
}

func sourceCooldownDeadline(source channels.Source) (time.Time, bool) {
	if source.CooldownUntil <= 0 {
		return time.Time{}, false
	}

	until := time.Unix(source.CooldownUntil, 0).UTC()
	if !until.After(time.Now().UTC()) {
		return time.Time{}, false
	}
	return until, true
}

func (s *sharedRuntimeSession) startCurrentSourceWithBackoff(
	ctx context.Context,
	source channels.Source,
	totalTimeout time.Duration,
	recoveryCycle int64,
	recoveryReason string,
	minAttempts int,
) (io.ReadCloser, channels.Source, error) {
	if strings.TrimSpace(source.StreamURL) == "" {
		return nil, channels.Source{}, ErrSessionNoSources
	}
	if totalTimeout <= 0 {
		totalTimeout = s.manager.cfg.startupWait
	}

	deadline := time.Now().UTC().Add(totalTimeout)
	attempt := 0
	maxAttempts := recoveryStartupAttemptLimit(s.manager.cfg.stallMaxFailoversPerTry)
	if minAttempts > maxAttempts {
		maxAttempts = minAttempts
	}
	limitedByCap := false
	trackStartupFailures := recoveryCycle <= 0
	var lastErr error

	for {
		if ctx.Err() != nil {
			return nil, channels.Source{}, ctx.Err()
		}
		if err := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "restart_same_attempt_loop"); err != nil {
			return nil, channels.Source{}, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		if maxAttempts > 0 && attempt >= maxAttempts {
			limitedByCap = true
			break
		}

		if cooldownAt, cooling := sourceCooldownDeadline(source); cooling {
			var err error
			if recoveryCycle > 0 {
				err = s.waitUntilRecoveryWithContext(
					ctx,
					deadline,
					cooldownAt,
					recoveryCycle,
					recoveryReason,
					"restart_same_cooldown_wait",
				)
			} else {
				err = waitUntilWithContext(ctx, deadline, cooldownAt)
			}
			if err != nil {
				return nil, channels.Source{}, err
			}
			if time.Until(deadline) <= 0 {
				break
			}
		}

		attempt++
		if attempt > 1 {
			remaining = time.Until(deadline)
			if remaining <= 0 {
				break
			}
			backoff := recoveryRetryBackoff(attempt - 1)
			if backoff > remaining {
				backoff = remaining
			}
			var err error
			if recoveryCycle > 0 {
				err = s.waitRecoveryWithContext(
					ctx,
					backoff,
					recoveryCycle,
					recoveryReason,
					"restart_same_backoff_wait",
				)
			} else {
				err = waitWithContext(ctx, backoff)
			}
			if err != nil {
				return nil, channels.Source{}, err
			}
		}

		if err := s.waitForProviderOverlimitCooldown(
			ctx,
			deadline,
			source.SourceID,
			source.StreamURL,
			recoveryCycle,
			recoveryReason,
		); err != nil {
			return nil, channels.Source{}, err
		}
		remaining = time.Until(deadline)
		if remaining <= 0 {
			break
		}

		startupTimeout := s.manager.cfg.startupWait
		if startupTimeout <= 0 || startupTimeout > remaining {
			startupTimeout = remaining
		}
		if startupTimeout <= 0 {
			break
		}

		startupCtx, stopStartupCtx, startupCanceledByIdleGuard := s.startupAttemptContext(
			ctx,
			recoveryCycle,
			recoveryReason,
			"restart_same_startup_attempt",
		)
		startupPTSOffset := s.currentRecoveryTimelinePTSOffset()
		reader, producer, startupProbe, startupRandomAccessReady, startupRandomAccessCodec, startupInventory, startupRetryRelaxedProbe, startupRetryReason, err := s.startSourceReader(
			startupCtx,
			source.SourceID,
			strings.TrimSpace(source.StreamURL),
			startupTimeout,
			true,
			startupPTSOffset,
		)
		stopStartupCtx()
		if err != nil {
			lastErr = err
			if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return nil, channels.Source{}, err
			}
			if guardErr := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "restart_same_startup_failure"); guardErr != nil {
				return nil, channels.Source{}, guardErr
			}

			s.recordSourceFailure(ctx, source.SourceID, source.StreamURL, err, trackStartupFailures)
			if !(startupCanceledByIdleGuard() && isRecoveryStartupLifecycleCancellation(err)) {
				s.recordRecoveryAlternateStartupFailure(source.SourceID, err, recoveryCycle)
			}
			s.manager.logger.Warn(
				"shared session source startup failed",
				"channel_id", s.channel.ChannelID,
				"guide_number", s.channel.GuideNumber,
				"source_id", source.SourceID,
				"source_item_key", source.ItemKey,
				"reason", startupFailureReason(err),
				"idle_startup_canceled", startupCanceledByIdleGuard(),
				"recovery_cycle", recoveryCycle,
				"recovery_reason", recoveryReason,
				"recovery_attempt", attempt,
				"recovery_max_attempts", maxAttempts,
			)
			continue
		}
		if guardErr := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "restart_same_startup_success"); guardErr != nil {
			closeWithTimeout(reader, s.manager.cfg.sessionDrainTimeout)
			return nil, channels.Source{}, guardErr
		}

		startupInventory = enhanceInventoryWithPersistedProfile(startupInventory, source)
		s.setSourceStateWithStartupProbe(
			source,
			producer,
			fmt.Sprintf("recovery_cycle_%d:%s", recoveryCycle, strings.TrimSpace(recoveryReason)),
			startupProbe,
			startupRandomAccessReady,
			startupRandomAccessCodec,
			startupInventory,
			startupRetryRelaxedProbe,
			startupRetryReason,
		)
		s.recordSourceSuccess(ctx, source.SourceID)
		s.manager.clearProviderOverlimitCooldownForSource(source.SourceID, source.StreamURL)
		return reader, source, nil
	}

	if lastErr == nil {
		lastErr = errors.New("recovery source attempts exhausted")
	} else if limitedByCap {
		lastErr = fmt.Errorf(
			"recovery source attempts exhausted by stall max failovers (attempts=%d, stall_max_failovers_per_stall=%d): %w",
			attempt,
			s.manager.cfg.stallMaxFailoversPerTry,
			lastErr,
		)
	}
	return nil, channels.Source{}, lastErr
}

func (s *sharedRuntimeSession) waitForProviderOverlimitCooldown(
	ctx context.Context,
	deadline time.Time,
	sourceID int64,
	streamURL string,
	recoveryCycle int64,
	recoveryReason string,
) error {
	if s == nil || s.manager == nil {
		return nil
	}
	if recoveryCycle <= 0 {
		return s.manager.waitForProviderOverlimitCooldownForSource(ctx, deadline, sourceID, streamURL)
	}

	scope := providerOverlimitScopeKey(sourceID, streamURL)
	for {
		if err := s.abortRecoveryIfIdle(recoveryCycle, recoveryReason, "provider_overlimit_cooldown"); err != nil {
			return err
		}

		wait := s.manager.providerOverlimitRemainingForScope(scope)
		if wait <= 0 {
			return nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		if wait > remaining {
			wait = remaining
		}
		if err := s.waitRecoveryWithContext(
			ctx,
			wait,
			recoveryCycle,
			recoveryReason,
			"provider_overlimit_cooldown",
		); err != nil {
			return err
		}
	}
}

func (s *sharedRuntimeSession) waitForRecoveryFailoverSettle(
	ctx context.Context,
	deadline time.Time,
	recoveryCycle int64,
	recoveryReason string,
) error {
	if s == nil || s.manager == nil {
		return nil
	}

	wait := recoveryAlternateSettleDelay(s.manager.cfg.startupWait, s.manager.cfg.stallDetect)
	if s.manager.tuners != nil {
		if poolWait := s.manager.tuners.failoverSettleDelayWhenFull(); poolWait > wait {
			wait = poolWait
		}
	}
	if wait <= 0 {
		return nil
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil
	}
	if wait > remaining {
		wait = remaining
	}
	return s.waitRecoveryWithContext(
		ctx,
		wait,
		recoveryCycle,
		recoveryReason,
		"recovery_failover_settle",
	)
}

func (s *sharedRuntimeSession) waitForFailoverRetrySettle(
	ctx context.Context,
	deadline time.Time,
	recoveryCycle int64,
	recoveryReason string,
) error {
	if s == nil || s.manager == nil || s.manager.tuners == nil {
		return nil
	}

	wait := s.manager.tuners.failoverSettleDelayWhenFull()
	if wait <= 0 {
		return nil
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil
	}
	if wait > remaining {
		wait = remaining
	}
	if recoveryCycle > 0 {
		return s.waitRecoveryWithContext(
			ctx,
			wait,
			recoveryCycle,
			recoveryReason,
			"recovery_retry_settle",
		)
	}
	return waitWithContext(ctx, wait)
}

func providerOverlimitScopeKey(sourceID int64, streamURL string) string {
	if host := providerOverlimitScopeHost(streamURL); host != "" {
		return host
	}
	if sourceID > 0 {
		return providerOverlimitScopeSourcePrefix + strconv.FormatInt(sourceID, 10)
	}
	return providerOverlimitScopeDefault
}

func providerOverlimitScopeHost(streamURL string) string {
	streamURL = strings.TrimSpace(streamURL)
	if streamURL == "" {
		return ""
	}

	candidates := []string{streamURL}
	if !strings.Contains(streamURL, "://") {
		candidates = append(candidates, "http://"+streamURL)
	}

	for _, candidate := range candidates {
		parsed, err := url.Parse(candidate)
		if err != nil {
			continue
		}
		host := strings.TrimSpace(strings.ToLower(parsed.Hostname()))
		if host != "" {
			if port := strings.TrimSpace(parsed.Port()); port != "" {
				return host + ":" + port
			}
			return host
		}
	}
	return ""
}

func (m *SessionManager) waitForProviderOverlimitCooldown(ctx context.Context, deadline time.Time) error {
	return m.waitForProviderOverlimitCooldownForScope(ctx, deadline, providerOverlimitScopeDefault)
}

func (m *SessionManager) waitForProviderOverlimitCooldownForSource(
	ctx context.Context,
	deadline time.Time,
	sourceID int64,
	streamURL string,
) error {
	return m.waitForProviderOverlimitCooldownForScope(ctx, deadline, providerOverlimitScopeKey(sourceID, streamURL))
}

func (m *SessionManager) waitForProviderOverlimitCooldownForScope(
	ctx context.Context,
	deadline time.Time,
	scope string,
) error {
	if m == nil || m.cfg.upstreamOverlimitCooldown <= 0 {
		return nil
	}

	for {
		wait := m.providerOverlimitRemainingForScope(scope)
		if wait <= 0 {
			return nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		if wait > remaining {
			wait = remaining
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *SessionManager) providerOverlimitRemaining() time.Duration {
	return m.providerOverlimitRemainingForScope(providerOverlimitScopeDefault)
}

func (m *SessionManager) providerOverlimitRemainingForSource(sourceID int64, streamURL string) time.Duration {
	return m.providerOverlimitRemainingForScope(providerOverlimitScopeKey(sourceID, streamURL))
}

func (m *SessionManager) providerOverlimitRemainingForScope(scope string) time.Duration {
	if m == nil {
		return 0
	}
	scope = normalizeProviderOverlimitScope(scope)
	now := time.Now().UTC()

	m.overlimitMu.Lock()
	defer m.overlimitMu.Unlock()
	m.pruneProviderOverlimitCooldownLocked(now)

	state, ok := m.providerCooldownByScope[scope]
	if !ok {
		return 0
	}
	remaining := state.until.Sub(now)
	if remaining <= 0 {
		delete(m.providerCooldownByScope, scope)
		return 0
	}
	state.lastTouched = now
	m.providerCooldownByScope[scope] = state
	return remaining
}

func (m *SessionManager) armProviderOverlimitCooldown(statusCode int, reason string) {
	m.armProviderOverlimitCooldownForScope(statusCode, reason, providerOverlimitScopeDefault)
}

func (m *SessionManager) armProviderOverlimitCooldownForSource(
	statusCode int,
	reason string,
	sourceID int64,
	streamURL string,
) {
	m.armProviderOverlimitCooldownForScope(statusCode, reason, providerOverlimitScopeKey(sourceID, streamURL))
}

func (m *SessionManager) armProviderOverlimitCooldownForScope(statusCode int, reason, scope string) {
	if m == nil || m.cfg.upstreamOverlimitCooldown <= 0 || !isLikelyUpstreamOverlimitStatus(statusCode) {
		return
	}
	scope = normalizeProviderOverlimitScope(scope)

	now := time.Now().UTC()
	until := now.Add(m.cfg.upstreamOverlimitCooldown)

	armed := false
	m.overlimitMu.Lock()
	if m.providerCooldownByScope == nil {
		m.providerCooldownByScope = make(map[string]providerOverlimitScopeState)
	}
	m.pruneProviderOverlimitCooldownLocked(now)
	state := m.providerCooldownByScope[scope]
	if until.After(state.until) {
		state.until = until
		armed = true
	}
	state.lastTouched = now
	if state.armCount < ^uint32(0) {
		state.armCount++
	}
	m.providerCooldownByScope[scope] = state
	m.pruneProviderOverlimitCooldownLocked(now)
	m.overlimitMu.Unlock()

	if armed && m.logger != nil {
		m.logger.Warn(
			"shared session provider overlimit cooldown armed",
			"scope", scope,
			"status_code", statusCode,
			"cooldown", m.cfg.upstreamOverlimitCooldown.String(),
			"until", until,
			"reason", strings.TrimSpace(reason),
		)
	}
}

func (m *SessionManager) clearProviderOverlimitCooldown() {
	m.clearProviderOverlimitCooldownForScope(providerOverlimitScopeDefault)
}

func (m *SessionManager) clearProviderOverlimitCooldownForSource(sourceID int64, streamURL string) {
	m.clearProviderOverlimitCooldownForScope(providerOverlimitScopeKey(sourceID, streamURL))
}

func (m *SessionManager) clearProviderOverlimitCooldownForScope(scope string) {
	if m == nil || m.cfg.upstreamOverlimitCooldown <= 0 {
		return
	}
	scope = normalizeProviderOverlimitScope(scope)
	now := time.Now().UTC()

	m.overlimitMu.Lock()
	m.pruneProviderOverlimitCooldownLocked(now)
	delete(m.providerCooldownByScope, scope)
	m.overlimitMu.Unlock()
}

func normalizeProviderOverlimitScope(scope string) string {
	scope = strings.TrimSpace(strings.ToLower(scope))
	if scope == "" {
		return providerOverlimitScopeDefault
	}
	return scope
}

func (m *SessionManager) pruneProviderOverlimitCooldownLocked(now time.Time) {
	if m == nil || len(m.providerCooldownByScope) == 0 {
		return
	}

	for scope, state := range m.providerCooldownByScope {
		if state.until.IsZero() || !state.until.After(now) {
			delete(m.providerCooldownByScope, scope)
		}
	}
	if len(m.providerCooldownByScope) <= providerOverlimitScopeStateLimit {
		return
	}

	type cooldownScope struct {
		scope       string
		until       time.Time
		lastTouched time.Time
		armCount    uint32
	}
	scopes := make([]cooldownScope, 0, len(m.providerCooldownByScope))
	for scope, state := range m.providerCooldownByScope {
		scopes = append(scopes, cooldownScope{
			scope:       scope,
			until:       state.until,
			lastTouched: state.lastTouched,
			armCount:    state.armCount,
		})
	}
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].armCount != scopes[j].armCount {
			return scopes[i].armCount < scopes[j].armCount
		}
		if !scopes[i].lastTouched.Equal(scopes[j].lastTouched) {
			return scopes[i].lastTouched.Before(scopes[j].lastTouched)
		}
		if scopes[i].until.Equal(scopes[j].until) {
			return scopes[i].scope < scopes[j].scope
		}
		return scopes[i].until.Before(scopes[j].until)
	})

	for len(scopes) > providerOverlimitScopeStateLimit {
		evicted := scopes[0]
		delete(m.providerCooldownByScope, evicted.scope)
		scopes = scopes[1:]
	}
}

func (s *sharedRuntimeSession) startSourceReader(
	ctx context.Context,
	sourceID int64,
	streamURL string,
	startupTimeout time.Duration,
	requireRandomAccess bool,
	startupPTSOffset time.Duration,
) (io.ReadCloser, string, startupProbeTelemetry, bool, string, startupStreamInventory, bool, string, error) {
	streamCtx := ctx
	if s != nil && s.ctx != nil {
		// Successful selected stream lifetime is tied to session context while
		// startup handshake/probe remains bound to the per-attempt startup context.
		streamCtx = s.ctx
	}

	session, err := startSourceSessionWithContextsConfigured(
		ctx,
		streamCtx,
		s.manager.cfg.mode,
		s.manager.cfg.httpClient,
		s.manager.cfg.ffmpegPath,
		streamURL,
		startupTimeout,
		s.manager.cfg.minProbe,
		s.manager.cfg.producerReadRate,
		s.manager.cfg.producerReadRateCatchup,
		s.manager.cfg.producerInitialBurst,
		s.manager.cfg.ffmpegStartupProbeSize,
		s.manager.cfg.ffmpegStartupAnalyzeDelay,
		s.manager.cfg.ffmpegReconnectEnabled,
		s.manager.cfg.ffmpegReconnectDelayMax,
		s.manager.cfg.ffmpegReconnectMaxRetries,
		s.manager.cfg.ffmpegReconnectHTTPErrors,
		s.manager.cfg.ffmpegInputBufferSize,
		s.manager.cfg.ffmpegDiscardCorrupt,
		s.manager.cfg.ffmpegCopyRegenerateTimestamps,
		startupPTSOffset,
		requireRandomAccess,
	)
	if err != nil {
		return nil, "", startupProbeTelemetry{}, false, "", startupStreamInventory{}, false, "", err
	}
	if session.startupNoInitialBurstFallback && s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Warn(
			"ffmpeg startup option unsupported; continuing without producer initial burst",
			"source_id", sourceID,
			"source_url", sanitizeStreamURLForLog(streamURL),
			"ffmpeg_path", strings.TrimSpace(s.manager.cfg.ffmpegPath),
			"unsupported_option", ffmpegReadrateInitialBurstOption,
		)
	}
	if session.startupNoReadrateCatchupFallback && s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Warn(
			"ffmpeg startup option unsupported; continuing without producer readrate catchup",
			"source_id", sourceID,
			"source_url", sanitizeStreamURLForLog(streamURL),
			"ffmpeg_path", strings.TrimSpace(s.manager.cfg.ffmpegPath),
			"unsupported_option", ffmpegReadrateCatchupOption,
		)
	}
	producer := fmt.Sprintf(
		"%s:source=%s",
		s.manager.cfg.mode,
		strconv.FormatInt(sourceID, 10),
	)
	if session.startupRetryRelaxedProbe {
		producer += ":startup_retry_relaxed_probe"
	}

	return &streamSessionReadCloser{session: session}, producer,
		session.startupProbeTelemetry.normalized(),
		session.startupProbeRA,
		session.startupProbeCodec,
		session.startupInventory,
		session.startupRetryRelaxedProbe,
		strings.TrimSpace(session.startupRetryRelaxedProbeToken),
		nil
}

func (s *sharedRuntimeSession) addSubscriber(clientAddr string) (uint64, uint64, error) {
	if s == nil {
		return 0, 0, errors.New("shared session is not configured")
	}

	tunerID := -1
	tunerLeaseToken := uint64(0)
	if s.lease != nil {
		tunerID = s.lease.ID
		tunerLeaseToken = s.lease.token
	}

	s.mu.Lock()
	if s.closed || (s.ctx != nil && s.ctx.Err() != nil) {
		s.mu.Unlock()
		if s.lastErr != nil {
			return 0, 0, s.lastErr
		}
		return 0, 0, errSharedSessionClosed
	}
	if s.subscribers == nil {
		s.subscribers = make(map[uint64]SubscriberStats)
	}
	previousSubscribers := len(s.subscribers)
	if s.manager.cfg.sessionMaxSubscribers > 0 && len(s.subscribers) >= s.manager.cfg.sessionMaxSubscribers {
		s.mu.Unlock()
		return 0, 0, ErrSessionMaxSubscribers
	}

	s.nextSubscriberID++
	if s.nextSubscriberID == 0 {
		s.nextSubscriberID++
	}
	subscriberID := s.nextSubscriberID
	subscriber := SubscriberStats{
		SubscriberID: subscriberID,
		ClientAddr:   strings.TrimSpace(clientAddr),
		StartedAt:    time.Now().UTC(),
	}
	s.subscribers[subscriberID] = subscriber
	subscriberStartedAt := subscriber.StartedAt
	if s.historySubscriberIndex == nil {
		s.historySubscriberIndex = make(map[uint64]int)
	}
	historyIdx := len(s.historySubscribers)
	s.historySubscribers = append(s.historySubscribers, SharedSessionSubscriberHistory{
		SubscriberID: subscriber.SubscriberID,
		ClientAddr:   subscriber.ClientAddr,
		ConnectedAt:  subscriber.StartedAt,
	})
	s.historySubscriberIndex[subscriberID] = historyIdx
	s.historyTotalSubscribers++
	s.truncateSubscriberHistoryLocked()
	if current := len(s.subscribers); current > s.historyPeakSubscribers {
		s.historyPeakSubscribers = current
	}
	sourceID := s.sourceID
	sourceItemKey := s.sourceItemKey
	producer := s.producer
	sessionStartedAt := s.startedAt
	currentSubscribers := len(s.subscribers)
	s.idleToken++
	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}
	startSeq := s.ring.StartSeqByLagBytes(s.manager.cfg.subscriberJoinLagBytes)
	s.mu.Unlock()

	if tunerID >= 0 && tunerLeaseToken != 0 && s.manager != nil && s.manager.tuners != nil {
		s.manager.tuners.clearClientPreemptible(tunerID, tunerLeaseToken)
	}
	if s.manager != nil && s.manager.logger != nil {
		sinceSessionStart := time.Since(sessionStartedAt)
		if sinceSessionStart < 0 {
			sinceSessionStart = 0
		}
		s.manager.logger.Info(
			"shared session subscriber connected",
			"channel_id", s.channel.ChannelID,
			"guide_number", s.channel.GuideNumber,
			"guide_name", s.channel.GuideName,
			"tuner_id", tunerID,
			"source_id", sourceID,
			"source_item_key", sourceItemKey,
			"producer", producer,
			"subscriber_id", subscriberID,
			"client_addr", strings.TrimSpace(clientAddr),
			"session_started_at", sessionStartedAt,
			"subscriber_started_at", subscriberStartedAt,
			"duration", sinceSessionStart.String(),
			"subscribers_before", previousSubscribers,
			"subscribers_after", currentSubscribers,
		)
	}
	if previousSubscribers == 0 && s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Info(
			"shared session entered active_subscribers",
			"channel_id", s.channel.ChannelID,
			"guide_number", s.channel.GuideNumber,
			"guide_name", s.channel.GuideName,
			"tuner_id", tunerID,
			"subscriber_id", subscriberID,
			"subscriber_started_at", subscriberStartedAt,
			"subscribers", currentSubscribers,
			"session_started_at", sessionStartedAt,
			"source_id", sourceID,
			"source_item_key", sourceItemKey,
			"producer", producer,
		)
	}

	return startSeq, subscriberID, nil
}

func (s *sharedRuntimeSession) removeSubscriber(subscriberID uint64, reason string) {
	if s == nil {
		return
	}

	reason = normalizeSubscriberRemovalReason(reason)
	removedAt := time.Now().UTC()

	var (
		subscriber          SubscriberStats
		found               bool
		previousSubscribers int
		currentSubscribers  int
		tunerID             = -1
		tunerLeaseToken     uint64
		sourceID            int64
		sessionStartedAt    time.Time
		token               uint64
		shouldMarkPreempt   bool
		idleCancelReason    string
		closed              bool
	)

	s.mu.Lock()
	previousSubscribers = len(s.subscribers)
	if subscriberID != 0 {
		subscriber, found = s.subscribers[subscriberID]
		delete(s.subscribers, subscriberID)
		if historyIdx, ok := s.historySubscriberIndex[subscriberID]; ok {
			if historyIdx >= 0 && historyIdx < len(s.historySubscribers) {
				if s.historySubscribers[historyIdx].ClosedAt.IsZero() {
					s.historySubscribers[historyIdx].ClosedAt = removedAt
					s.historySubscribers[historyIdx].CloseReason = reason
					s.historyCompletedSubscribers++
				}
			}
			delete(s.historySubscriberIndex, subscriberID)
		}
		s.truncateSubscriberHistoryLocked()
	}
	currentSubscribers = len(s.subscribers)
	if s.lease != nil {
		tunerID = s.lease.ID
		tunerLeaseToken = s.lease.token
	}
	sourceID = s.sourceID
	sessionStartedAt = s.startedAt
	closed = s.closed
	if currentSubscribers == 0 && !closed {
		ready := false
		if s.readyCh != nil {
			select {
			case <-s.readyCh:
				ready = true
			default:
			}
		}
		if !ready {
			s.idleToken++
			token = s.idleToken
			idleCancelReason = "no_subscribers_before_ready"
		} else {
			s.idleToken++
			token = s.idleToken
			timeout := time.Duration(0)
			if s.manager != nil {
				timeout = s.manager.cfg.sessionIdleTimeout
			}
			if timeout <= 0 {
				idleCancelReason = "idle_timeout_disabled"
			} else {
				if s.idleTimer != nil {
					s.idleTimer.Stop()
				}
				s.idleTimer = time.AfterFunc(timeout, func() {
					s.onIdleTimer(token)
				})
				shouldMarkPreempt = true
			}
		}
	}
	s.mu.Unlock()

	s.logSubscriberDisconnected(
		found,
		subscriber,
		reason,
		removedAt,
		tunerID,
		sourceID,
		sessionStartedAt,
		previousSubscribers,
		currentSubscribers,
	)

	if currentSubscribers != 0 || closed {
		return
	}

	if tunerID >= 0 && tunerLeaseToken != 0 && s.manager != nil && s.manager.tuners != nil {
		s.manager.tuners.clearClientPreemptible(tunerID, tunerLeaseToken)
	}
	if idleCancelReason != "" {
		s.cancelIdleAsync(token, idleCancelReason)
		return
	}
	if shouldMarkPreempt && tunerID >= 0 && tunerLeaseToken != 0 && s.manager != nil && s.manager.tuners != nil {
		s.manager.tuners.markClientPreemptible(tunerID, tunerLeaseToken, func() bool {
			return s.tryPreemptIdle(token)
		})
	}
}

func (s *sharedRuntimeSession) recordSlowSkipEvent(lagChunks, lagBytes uint64) slowSkipSessionSnapshot {
	if s == nil {
		return slowSkipSessionSnapshot{}
	}
	s.mu.Lock()
	s.slowSkipEventsTotal++
	s.slowSkipLagChunksTotal += lagChunks
	s.slowSkipLagBytesTotal += lagBytes
	if lagChunks > s.slowSkipMaxLagChunks {
		s.slowSkipMaxLagChunks = lagChunks
	}
	snapshot := slowSkipSessionSnapshot{
		EventsTotal:    s.slowSkipEventsTotal,
		LagChunksTotal: s.slowSkipLagChunksTotal,
		LagBytesTotal:  s.slowSkipLagBytesTotal,
		MaxLagChunks:   s.slowSkipMaxLagChunks,
	}
	s.mu.Unlock()
	return snapshot
}

func (s *sharedRuntimeSession) recordSubscriberWritePressure(sample subscriberWritePressureSample) {
	if s == nil {
		return
	}
	recordStreamSubscriberWriteTelemetry(sample)
	if sample.DeadlineTimeout {
		atomic.AddUint64(&s.subscriberWriteDeadlineTimeouts, 1)
	}
	if sample.ShortWrite {
		atomic.AddUint64(&s.subscriberWriteShortWrites, 1)
	}
	if sample.BlockedDuration > 0 {
		atomic.AddUint64(&s.subscriberWriteBlockedDurationUS, sample.BlockedDuration)
	}
}

func (s *sharedRuntimeSession) subscriberWritePressureSnapshot() (
	deadlineTimeouts uint64,
	shortWrites uint64,
	blockedDurationUS uint64,
	blockedDurationMS uint64,
) {
	if s == nil {
		return 0, 0, 0, 0
	}
	blockedDurationUS = atomic.LoadUint64(&s.subscriberWriteBlockedDurationUS)
	return atomic.LoadUint64(&s.subscriberWriteDeadlineTimeouts),
		atomic.LoadUint64(&s.subscriberWriteShortWrites),
		blockedDurationUS,
		blockedDurationUS / 1000
}

func (s *sharedRuntimeSession) sourceHistoryLimitLocked() int {
	if s == nil || s.manager == nil {
		return defaultSharedSourceHistoryLimit
	}
	limit := s.manager.cfg.sessionSourceHistoryLimit
	if limit <= 0 {
		limit = defaultSharedSourceHistoryLimit
	}
	return limit
}

func (s *sharedRuntimeSession) truncateSourceHistoryLocked() {
	if s == nil {
		return
	}
	limit := s.sourceHistoryLimitLocked()
	if limit <= 0 || len(s.historySources) <= limit {
		return
	}

	overflow := len(s.historySources) - limit
	drop := make([]bool, len(s.historySources))

	for i := 0; i < len(s.historySources) && overflow > 0; i++ {
		if i == s.historyCurrentSourceIdx {
			continue
		}
		if !s.historySources[i].DeselectedAt.IsZero() {
			drop[i] = true
			overflow--
		}
	}
	for i := 0; i < len(s.historySources) && overflow > 0; i++ {
		if i == s.historyCurrentSourceIdx || drop[i] {
			continue
		}
		drop[i] = true
		overflow--
	}

	removed := 0
	newCurrentIdx := -1
	compacted := s.historySources[:0]
	for i, entry := range s.historySources {
		if drop[i] {
			removed++
			continue
		}
		if i == s.historyCurrentSourceIdx {
			newCurrentIdx = len(compacted)
		}
		compacted = append(compacted, entry)
	}

	s.historySources = compacted
	s.historyCurrentSourceIdx = newCurrentIdx
	if removed > 0 {
		s.historySourcesTruncated += int64(removed)
	}
}

func (s *sharedRuntimeSession) subscriberHistoryLimitLocked() int {
	if s == nil || s.manager == nil {
		return defaultSharedSubscriberHistoryLimit
	}
	limit := s.manager.cfg.sessionSubscriberHistoryLimit
	if limit <= 0 {
		limit = defaultSharedSubscriberHistoryLimit
	}
	return limit
}

func (s *sharedRuntimeSession) isActiveSubscriberHistoryEntryLocked(index int) bool {
	if s == nil || index < 0 || index >= len(s.historySubscribers) {
		return false
	}
	entry := s.historySubscribers[index]
	if entry.ClosedAt.IsZero() {
		if _, ok := s.subscribers[entry.SubscriberID]; ok {
			if mapped, exists := s.historySubscriberIndex[entry.SubscriberID]; exists && mapped == index {
				return true
			}
		}
	}
	return false
}

func (s *sharedRuntimeSession) truncateSubscriberHistoryLocked() {
	if s == nil {
		return
	}
	limit := s.subscriberHistoryLimitLocked()
	if limit <= 0 {
		return
	}

	activeSubscribers := 0
	for subscriberID := range s.subscribers {
		historyIdx, ok := s.historySubscriberIndex[subscriberID]
		if !ok || historyIdx < 0 || historyIdx >= len(s.historySubscribers) {
			continue
		}
		if s.historySubscribers[historyIdx].ClosedAt.IsZero() {
			activeSubscribers++
		}
	}
	effectiveLimit := limit
	if activeSubscribers > effectiveLimit {
		effectiveLimit = activeSubscribers
	}
	if len(s.historySubscribers) <= effectiveLimit {
		return
	}

	overflow := len(s.historySubscribers) - effectiveLimit
	drop := make([]bool, len(s.historySubscribers))

	for i := 0; i < len(s.historySubscribers) && overflow > 0; i++ {
		if s.isActiveSubscriberHistoryEntryLocked(i) {
			continue
		}
		drop[i] = true
		overflow--
	}
	for i := 0; i < len(s.historySubscribers) && overflow > 0; i++ {
		if drop[i] {
			continue
		}
		drop[i] = true
		overflow--
	}

	removed := 0
	compacted := s.historySubscribers[:0]
	newIndex := make(map[uint64]int, len(s.subscribers))
	for i, entry := range s.historySubscribers {
		if drop[i] {
			removed++
			continue
		}
		nextIdx := len(compacted)
		compacted = append(compacted, entry)
		if entry.ClosedAt.IsZero() {
			if _, ok := s.subscribers[entry.SubscriberID]; ok {
				newIndex[entry.SubscriberID] = nextIdx
			}
		}
	}

	s.historySubscribers = compacted
	s.historySubscriberIndex = newIndex
	if removed > 0 {
		s.historySubscribersTruncated += int64(removed)
	}
}

func (s *sharedRuntimeSession) logSubscriberDisconnected(
	found bool,
	subscriber SubscriberStats,
	reason string,
	removedAt time.Time,
	tunerID int,
	sourceID int64,
	sessionStartedAt time.Time,
	subscribersBefore int,
	subscribersAfter int,
) {
	if !found || s == nil || s.manager == nil || s.manager.logger == nil {
		return
	}
	duration := removedAt.Sub(subscriber.StartedAt)
	if duration < 0 {
		duration = 0
	}
	s.manager.logger.Info(
		"shared session subscriber disconnected",
		"channel_id", s.channel.ChannelID,
		"guide_number", s.channel.GuideNumber,
		"guide_name", s.channel.GuideName,
		"tuner_id", tunerID,
		"source_id", sourceID,
		"subscriber_id", subscriber.SubscriberID,
		"client_addr", subscriber.ClientAddr,
		"reason", normalizeSubscriberRemovalReason(reason),
		"session_started_at", sessionStartedAt,
		"subscriber_started_at", subscriber.StartedAt,
		"removed_at", removedAt,
		"duration", duration.String(),
		"subscribers_before", subscribersBefore,
		"subscribers_after", subscribersAfter,
	)
}

func (s *sharedRuntimeSession) logIdleCancellation(reason string, tunerID int, token uint64) {
	if s == nil || s.manager == nil || s.manager.logger == nil {
		return
	}
	s.manager.logger.Info(
		"shared session canceled while idle",
		"channel_id", s.channel.ChannelID,
		"guide_number", s.channel.GuideNumber,
		"guide_name", s.channel.GuideName,
		"tuner_id", tunerID,
		"session_started_at", s.startedAt,
		"idle_token", token,
		"reason", strings.TrimSpace(reason),
	)
}

func (s *sharedRuntimeSession) cancelIdleAsync(token uint64, reason string) {
	if s == nil {
		return
	}
	go s.cancelIdleIfCurrent(token, reason, false)
}

func (s *sharedRuntimeSession) cancelIdleIfCurrent(token uint64, reason string, stopIdleTimer bool) bool {
	if s == nil {
		return false
	}

	s.mu.Lock()
	if s.closed || len(s.subscribers) > 0 || s.idleToken != token {
		s.mu.Unlock()
		return false
	}
	hook := s.idleCancelValidatedHook
	s.mu.Unlock()

	if hook != nil {
		hook()
	}

	tunerID := -1
	tunerLeaseToken := uint64(0)
	s.mu.Lock()
	if s.closed || len(s.subscribers) > 0 || s.idleToken != token {
		s.mu.Unlock()
		return false
	}
	if s.lease != nil {
		tunerID = s.lease.ID
		tunerLeaseToken = s.lease.token
	}
	if stopIdleTimer && s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}
	cancelNow := s.cancel
	if cancelNow != nil {
		cancelNow()
	}
	s.mu.Unlock()

	if tunerID >= 0 && tunerLeaseToken != 0 && s.manager != nil && s.manager.tuners != nil {
		s.manager.tuners.clearClientPreemptible(tunerID, tunerLeaseToken)
	}
	s.logIdleCancellation(reason, tunerID, token)
	return true
}

func (s *sharedRuntimeSession) cancelNow() {
	if s == nil || s.cancel == nil {
		return
	}
	s.cancel()
}

func (s *sharedRuntimeSession) onIdleTimer(token uint64) {
	s.cancelIdleIfCurrent(token, "idle_timeout", false)
}

func (s *sharedRuntimeSession) tryPreemptIdle(token uint64) bool {
	return s.cancelIdleIfCurrent(token, "idle_preempted", true)
}

func (s *sharedRuntimeSession) logState() (int, int64, time.Time, int) {
	if s == nil {
		return -1, 0, time.Time{}, 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tunerID := -1
	if s.lease != nil {
		tunerID = s.lease.ID
	}
	return tunerID, s.sourceID, s.startedAt, len(s.subscribers)
}

// enhanceInventoryWithPersistedProfile augments an "undetected" startup
// inventory with persisted source profile codec hints. This only applies
// when the live startup probe did not detect any component streams, to avoid
// overriding real probe data with potentially stale persisted metadata.
func enhanceInventoryWithPersistedProfile(inventory startupStreamInventory, source channels.Source) startupStreamInventory {
	if inventory.componentState() != "undetected" {
		return inventory
	}
	inferred := inferSourceComponentState(source)
	if inferred == "undetected" {
		return inventory
	}

	if strings.TrimSpace(source.ProfileVideoCodec) != "" {
		inventory.videoStreamCount = 1
		inventory.videoCodecs = []string{strings.TrimSpace(strings.ToLower(source.ProfileVideoCodec))}
	}
	if strings.TrimSpace(source.ProfileAudioCodec) != "" {
		inventory.audioStreamCount = 1
		inventory.audioCodecs = []string{strings.TrimSpace(strings.ToLower(source.ProfileAudioCodec))}
	}
	if inventory.detectionMethod == "" || inventory.detectionMethod == "none" {
		inventory.detectionMethod = "persisted_profile"
	}
	return inventory
}

func (s *sharedRuntimeSession) setSourceState(source channels.Source, producer, selectionReason string) {
	s.setSourceStateWithStartupProbe(source, producer, selectionReason, startupProbeTelemetry{}, false, "", startupStreamInventory{}, false, "")
}

func (s *sharedRuntimeSession) setSourceStateWithStartupProbe(
	source channels.Source,
	producer string,
	selectionReason string,
	startupProbe startupProbeTelemetry,
	startupRandomAccessReady bool,
	startupRandomAccessCodec string,
	startupInventory startupStreamInventory,
	startupRetryRelaxedProbe bool,
	startupRetryReason string,
) {
	selectedAt := time.Now().UTC()
	sourceStreamURL := strings.TrimSpace(source.StreamURL)
	pumpStats := PumpStats{}
	if s.pump != nil {
		pumpStats = s.pump.Stats()
	}
	startupProbe = startupProbe.normalized()
	startupInventory = startupInventory.normalized()
	startupRetryReason = strings.TrimSpace(startupRetryReason)
	startupVideoCodecs := strings.Join(startupInventory.videoCodecs, ",")
	startupAudioCodecs := strings.Join(startupInventory.audioCodecs, ",")
	startupComponentState := startupInventory.componentState()
	startupProbeCutoverPercent := startupProbe.cutoverPercent()

	var (
		profileProbeCtx        context.Context
		profileProbeCancel     context.CancelFunc
		profileProbeGeneration uint64
		profileProbeDelay      time.Duration
		warnHighCutover        bool
		highCutoverCoalesced   int64
		runProfileProbe        bool
	)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}

	previousSourceID := s.sourceID
	previousSourceURL := strings.TrimSpace(s.sourceStreamURL)
	previousSelectedAt := s.lastSourceSelectedAt
	if s.profileProbeCancel != nil {
		s.profileProbeCancel()
		s.profileProbeCancel = nil
	}
	s.sourceID = source.SourceID
	s.sourceItemKey = strings.TrimSpace(source.ItemKey)
	s.sourceStreamURL = sourceStreamURL
	s.sourceStartupProbeRawBytes = startupProbe.rawBytes
	s.sourceStartupProbeTrimmedBytes = startupProbe.trimmedBytes
	s.sourceStartupProbeCutoverOffset = startupProbe.cutoverOffset
	s.sourceStartupProbeDroppedBytes = startupProbe.droppedBytes
	s.sourceStartupProbeBytes = startupProbe.trimmedBytes
	s.sourceStartupRandomAccessReady = startupRandomAccessReady
	s.sourceStartupRandomAccessCodec = strings.TrimSpace(startupRandomAccessCodec)
	s.sourceStartupInventoryMethod = startupInventory.detectionMethod
	s.sourceStartupVideoStreams = startupInventory.videoStreamCount
	s.sourceStartupAudioStreams = startupInventory.audioStreamCount
	s.sourceStartupVideoCodecs = startupVideoCodecs
	s.sourceStartupAudioCodecs = startupAudioCodecs
	s.sourceStartupComponentState = startupComponentState
	s.sourceStartupRetryRelaxedProbe = startupRetryRelaxedProbe
	s.sourceStartupRetryReason = startupRetryReason
	s.sourceSelectBytesBase = pumpStats.BytesPublished
	if s.historyCurrentSourceIdx >= 0 && s.historyCurrentSourceIdx < len(s.historySources) {
		if s.historySources[s.historyCurrentSourceIdx].DeselectedAt.IsZero() {
			s.historySources[s.historyCurrentSourceIdx].DeselectedAt = selectedAt
		}
	}
	s.historySources = append(s.historySources, SharedSessionSourceHistory{
		SourceID:                       source.SourceID,
		ItemKey:                        strings.TrimSpace(source.ItemKey),
		StreamURL:                      sourceStreamURL,
		SelectedAt:                     selectedAt,
		SelectionReason:                strings.TrimSpace(selectionReason),
		StartupProbeRawBytes:           startupProbe.rawBytes,
		StartupProbeTrimmedBytes:       startupProbe.trimmedBytes,
		StartupProbeCutoverOffset:      startupProbe.cutoverOffset,
		StartupProbeDroppedBytes:       startupProbe.droppedBytes,
		StartupRandomAccessReady:       startupRandomAccessReady,
		StartupRandomAccessCodec:       strings.TrimSpace(startupRandomAccessCodec),
		StartupInventoryMethod:         startupInventory.detectionMethod,
		StartupVideoStreams:            startupInventory.videoStreamCount,
		StartupAudioStreams:            startupInventory.audioStreamCount,
		StartupVideoCodecs:             startupVideoCodecs,
		StartupAudioCodecs:             startupAudioCodecs,
		StartupComponentState:          startupComponentState,
		StartupRetryRelaxedProbe:       startupRetryRelaxedProbe,
		StartupRetryRelaxedProbeReason: startupRetryReason,
		Producer:                       strings.TrimSpace(producer),
	})
	s.historyCurrentSourceIdx = len(s.historySources) - 1
	s.truncateSourceHistoryLocked()
	sourceChanged := previousSourceID == 0 || previousSourceID != source.SourceID
	sourceURLChanged := previousSourceURL != sourceStreamURL
	if sourceChanged || sourceURLChanged {
		s.sourceProfile = streamProfile{}
	}
	s.producer = producer
	s.sourceSelects++
	if previousSourceID > 0 && previousSourceID == source.SourceID {
		s.sameSourceReselectCount++
	} else {
		s.sameSourceReselectCount = 0
	}
	s.lastSourceSelectedAt = selectedAt
	s.lastSourceSelectReason = strings.TrimSpace(selectionReason)
	if startupProbe.rawBytes > 0 &&
		startupProbe.droppedBytes > 0 &&
		startupProbeCutoverPercent >= startupProbeHighCutoverWarnPercent {
		if s.startupProbeWarnedAt.IsZero() ||
			selectedAt.Sub(s.startupProbeWarnedAt) >= startupProbeHighCutoverWarnWindow {
			warnHighCutover = true
			highCutoverCoalesced = s.startupProbeWarnSuppressed
			s.startupProbeWarnedAt = selectedAt
			s.startupProbeWarnSuppressed = 0
		} else {
			s.startupProbeWarnSuppressed++
		}
	}
	s.profileProbeGeneration++
	profileProbeGeneration = s.profileProbeGeneration
	s.updateCurrentHistorySourceProfileLocked()
	profileMissing := sourceProfileIsEmpty(s.sourceProfile)
	shouldProbeProfile := sourceStreamURL != "" &&
		s.ctx != nil &&
		s.manager != nil &&
		s.manager.cfg.mode != "direct" &&
		(sourceChanged || sourceURLChanged || profileMissing)
	if shouldProbeProfile {
		profileProbeDelay = s.profileProbeScheduleDelayLocked(strings.TrimSpace(selectionReason), selectedAt)
		profileProbeCtx, profileProbeCancel = context.WithCancel(s.ctx)
		s.profileProbeCancel = profileProbeCancel
		s.profileProbePersistWg.Add(1)
		runProfileProbe = true
	}
	sourceSelects := s.sourceSelects
	sameSourceReselectCount := s.sameSourceReselectCount
	alertCrossed := sameSourceReselectAlertThreshold > 0 &&
		sameSourceReselectCount == sameSourceReselectAlertThreshold
	recoveryCycle := s.recoveryCycle
	recoveryReason := s.recoveryReason
	sessionStartedAt := s.startedAt
	tunerID := -1
	if s.lease != nil {
		tunerID = s.lease.ID
	}
	s.mu.Unlock()

	if runProfileProbe && profileProbeCtx != nil {
		go func() {
			defer s.profileProbePersistWg.Done()
			s.runStreamProfileProbe(
				profileProbeCtx,
				profileProbeCancel,
				profileProbeGeneration,
				source.SourceID,
				sourceStreamURL,
				profileProbeDelay,
			)
		}()
	}

	if s.manager != nil && s.manager.logger != nil {
		since := time.Since(sessionStartedAt)
		if since < 0 {
			since = 0
		}
		sinceLastSelect := time.Duration(0)
		if !previousSelectedAt.IsZero() {
			sinceLastSelect = selectedAt.Sub(previousSelectedAt)
			if sinceLastSelect < 0 {
				sinceLastSelect = 0
			}
		}
		s.manager.logger.Info(
			"shared session selected source",
			"channel_id", s.channel.ChannelID,
			"guide_number", s.channel.GuideNumber,
			"guide_name", s.channel.GuideName,
			"tuner_id", tunerID,
			"source_id", source.SourceID,
			"source_item_key", strings.TrimSpace(source.ItemKey),
			"source_url", sanitizeStreamURLForLog(sourceStreamURL),
			"source_startup_probe_raw_bytes", startupProbe.rawBytes,
			"source_startup_probe_trimmed_bytes", startupProbe.trimmedBytes,
			"source_startup_probe_cutover_offset", startupProbe.cutoverOffset,
			"source_startup_probe_dropped_bytes", startupProbe.droppedBytes,
			"source_startup_probe_bytes", startupProbe.trimmedBytes,
			"source_startup_random_access_ready", startupRandomAccessReady,
			"source_startup_random_access_codec", strings.TrimSpace(startupRandomAccessCodec),
			"source_startup_inventory_method", startupInventory.detectionMethod,
			"source_startup_video_streams", startupInventory.videoStreamCount,
			"source_startup_audio_streams", startupInventory.audioStreamCount,
			"source_startup_video_codecs", startupVideoCodecs,
			"source_startup_audio_codecs", startupAudioCodecs,
			"source_startup_component_state", startupComponentState,
			"source_startup_retry_relaxed_probe", startupRetryRelaxedProbe,
			"source_startup_retry_reason", startupRetryReason,
			"producer", strings.TrimSpace(producer),
			"session_started_at", sessionStartedAt,
			"selected_at", selectedAt,
			"since_session_start", since.String(),
			"selection_reason", strings.TrimSpace(selectionReason),
			"source_select_count", sourceSelects,
			"since_last_source_select", sinceLastSelect.String(),
			"same_source_reselect_count", sameSourceReselectCount,
			"recovery_cycle", recoveryCycle,
			"recovery_reason", strings.TrimSpace(recoveryReason),
		)
		if alertCrossed {
			s.manager.logger.Warn(
				"shared session same-source reselection threshold crossed",
				"channel_id", s.channel.ChannelID,
				"guide_number", s.channel.GuideNumber,
				"guide_name", s.channel.GuideName,
				"tuner_id", tunerID,
				"source_id", source.SourceID,
				"source_item_key", strings.TrimSpace(source.ItemKey),
				"threshold", sameSourceReselectAlertThreshold,
				"same_source_reselect_count", sameSourceReselectCount,
				"source_select_count", sourceSelects,
				"since_last_source_select", sinceLastSelect.String(),
				"selection_reason", strings.TrimSpace(selectionReason),
				"recovery_cycle", recoveryCycle,
				"recovery_reason", strings.TrimSpace(recoveryReason),
			)
		}
		if warnHighCutover {
			warnFields := []any{
				"channel_id", s.channel.ChannelID,
				"guide_number", s.channel.GuideNumber,
				"guide_name", s.channel.GuideName,
				"tuner_id", tunerID,
				"source_id", source.SourceID,
				"source_item_key", strings.TrimSpace(source.ItemKey),
				"source_startup_probe_raw_bytes", startupProbe.rawBytes,
				"source_startup_probe_trimmed_bytes", startupProbe.trimmedBytes,
				"source_startup_probe_cutover_offset", startupProbe.cutoverOffset,
				"source_startup_probe_dropped_bytes", startupProbe.droppedBytes,
				"source_startup_probe_cutover_percent", startupProbeCutoverPercent,
				"source_startup_probe_cutover_warn_threshold_percent", startupProbeHighCutoverWarnPercent,
			}
			if highCutoverCoalesced > 0 {
				warnFields = append(warnFields, "source_startup_probe_cutover_warn_logs_coalesced", highCutoverCoalesced)
			}
			s.manager.logger.Warn("shared session startup probe cutover warning", warnFields...)
		}
	}
}

func (s *sharedRuntimeSession) runStreamProfileProbe(
	probeCtx context.Context,
	probeCancel context.CancelFunc,
	generation uint64,
	sourceID int64,
	streamURL string,
	delay time.Duration,
) {
	if probeCancel != nil {
		defer probeCancel()
	}
	if s == nil {
		return
	}

	defer func() {
		s.mu.Lock()
		if s.profileProbeGeneration == generation {
			s.profileProbeCancel = nil
		}
		s.mu.Unlock()
	}()

	if delay > 0 {
		if err := waitWithContext(probeCtx, delay); err != nil {
			return
		}
	}

	startedAt := time.Now().UTC()
	s.mu.Lock()
	if s.profileProbeGeneration != generation {
		s.mu.Unlock()
		return
	}
	if s.sourceID != sourceID {
		s.mu.Unlock()
		return
	}
	if strings.TrimSpace(s.sourceStreamURL) != strings.TrimSpace(streamURL) {
		s.mu.Unlock()
		return
	}
	s.profileProbeLastStartedAt = startedAt
	s.mu.Unlock()

	profile, err := streamProfileProbeRunner(probeCtx, "", streamURL, 0)
	if err != nil {
		if probeCtx.Err() == nil && s.manager != nil && s.manager.logger != nil {
			s.manager.logger.Debug(
				"shared session stream profile probe failed",
				"channel_id", s.channel.ChannelID,
				"guide_number", s.channel.GuideNumber,
				"source_id", sourceID,
				"source_url", sanitizeStreamURLForLog(streamURL),
				"error", err,
			)
		}
		return
	}

	s.mu.Lock()
	if s.profileProbeGeneration != generation {
		s.mu.Unlock()
		return
	}
	if s.sourceID != sourceID {
		s.mu.Unlock()
		return
	}
	if strings.TrimSpace(s.sourceStreamURL) != strings.TrimSpace(streamURL) {
		s.mu.Unlock()
		return
	}
	s.sourceProfile = profile
	s.updateCurrentHistorySourceProfileLocked()
	s.mu.Unlock()

	if s.manager == nil || s.manager.channels == nil {
		return
	}

	profileUpdate := channels.SourceProfileUpdate{
		LastProbeAt: time.Now().UTC(),
		Width:       profile.Width,
		Height:      profile.Height,
		FPS:         profile.FrameRate,
		VideoCodec:  profile.VideoCodec,
		AudioCodec:  profile.AudioCodec,
		BitrateBPS:  profile.BitrateBPS,
	}
	s.persistSourceProfileWithRetry(probeCtx, sourceID, streamURL, profileUpdate)
}

// persistSourceProfileWithRetry attempts to persist a source profile update
// with bounded retry for transient failures. Retry is cancellation-aware and
// stops immediately if the probe context is canceled (session teardown, new
// source selection, etc.).
func (s *sharedRuntimeSession) persistSourceProfileWithRetry(
	probeCtx context.Context,
	sourceID int64,
	streamURL string,
	profileUpdate channels.SourceProfileUpdate,
) {
	const maxAttempts = 3
	const retryDelay = 250 * time.Millisecond
	if probeCtx == nil {
		probeCtx = context.Background()
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		persistCtx, cancel := context.WithTimeout(probeCtx, 2*time.Second)
		err := s.manager.channels.UpdateSourceProfile(persistCtx, sourceID, profileUpdate)
		cancel()
		if err == nil {
			return
		}

		if s.manager.logger != nil {
			s.manager.logger.Debug(
				"shared session failed to persist stream profile",
				"channel_id", s.channel.ChannelID,
				"guide_number", s.channel.GuideNumber,
				"source_id", sourceID,
				"source_url", sanitizeStreamURLForLog(streamURL),
				"attempt", attempt,
				"max_attempts", maxAttempts,
				"error", err,
			)
		}

		if attempt >= maxAttempts {
			return
		}

		// Wait before retry, but honor probe context cancellation
		// (session teardown, new source selection, etc.).
		if err := waitWithContext(probeCtx, retryDelay); err != nil {
			return
		}
	}
}

func (s *sharedRuntimeSession) maybeLogFirstSourceBytes(observedAt time.Time) {
	if s == nil || observedAt.IsZero() {
		return
	}

	s.mu.Lock()
	if s.firstByteLogged {
		s.mu.Unlock()
		return
	}
	s.firstByteLogged = true
	tunerID := -1
	if s.lease != nil {
		tunerID = s.lease.ID
	}
	sessionStartedAt := s.startedAt
	sourceID := s.sourceID
	sourceItemKey := s.sourceItemKey
	producer := s.producer
	subscriberCount := len(s.subscribers)
	s.mu.Unlock()

	if s.manager == nil || s.manager.logger == nil {
		return
	}

	observedAt = observedAt.UTC()
	since := observedAt.Sub(sessionStartedAt)
	if since < 0 {
		since = 0
	}
	s.manager.logger.Info(
		"shared session observed first source bytes",
		"channel_id", s.channel.ChannelID,
		"guide_number", s.channel.GuideNumber,
		"guide_name", s.channel.GuideName,
		"tuner_id", tunerID,
		"source_id", sourceID,
		"source_item_key", sourceItemKey,
		"producer", producer,
		"session_started_at", sessionStartedAt,
		"first_byte_at", observedAt,
		"since_session_start", since.String(),
		"subscribers", subscriberCount,
	)
}

func (s *sharedRuntimeSession) incrementStallCount() {
	s.mu.Lock()
	s.stallCount++
	s.mu.Unlock()
}

func (s *sharedRuntimeSession) subscriberCount() int {
	if s == nil {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subscribers)
}

func (p *sessionChunkPublisher) PublishChunk(data []byte, publishedAt time.Time) error {
	if p == nil || p.session == nil || p.session.ring == nil {
		return ErrRingClosed
	}
	if len(data) == 0 {
		return nil
	}
	p.session.observePublishedChunk(data, publishedAt)
	return p.session.ring.PublishChunk(data, publishedAt)
}

func (p *sessionChunkPublisher) CopiesPublishedChunkData() bool {
	return true
}

func (p *recoveryKeepaliveChunkPublisher) PublishChunk(data []byte, publishedAt time.Time) error {
	if p == nil || p.session == nil {
		return ErrRingClosed
	}
	return p.session.publishRecoveryKeepaliveChunk(data, publishedAt)
}

func (p *recoveryKeepaliveChunkPublisher) CopiesPublishedChunkData() bool {
	return true
}

func (s *sharedRuntimeSession) chunkPublisher() ChunkPublisher {
	if s == nil {
		return nil
	}
	if s.publisher != nil {
		return s.publisher
	}
	return s.ring
}

func (s *sharedRuntimeSession) publishChunk(data []byte, publishedAt time.Time) error {
	if s == nil || len(data) == 0 {
		return nil
	}
	publisher := s.chunkPublisher()
	if publisher == nil {
		return nil
	}
	return publisher.PublishChunk(data, publishedAt)
}

func (s *sharedRuntimeSession) publishRecoveryKeepaliveChunk(data []byte, publishedAt time.Time) error {
	if s == nil || len(data) == 0 {
		return nil
	}
	if err := s.publishChunk(data, publishedAt); err != nil {
		return err
	}
	return s.observeRecoveryKeepaliveChunk(len(data), publishedAt)
}

func (s *sharedRuntimeSession) beginRecoveryKeepalive(startedAt time.Time) (float64, float64) {
	if s == nil {
		return 0, 0
	}
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	} else {
		startedAt = startedAt.UTC()
	}

	s.mu.Lock()
	s.recoveryKeepaliveStartedAt = startedAt
	s.recoveryKeepaliveStoppedAt = time.Time{}
	s.recoveryKeepaliveBytes = 0
	s.recoveryKeepaliveChunks = 0
	s.recoveryKeepaliveRateBytesPerSec = 0
	s.recoveryKeepaliveMultiplier = 0
	s.recoveryKeepaliveOverrateSince = time.Time{}
	s.recoveryKeepaliveGuardrailCount = 0
	s.recoveryKeepaliveGuardrailReason = ""
	expectedRate, profileRate := s.recoveryKeepaliveExpectedRatesLocked()
	s.recoveryKeepaliveExpectedRate = expectedRate
	s.recoveryKeepaliveProfileRate = profileRate
	s.mu.Unlock()
	return expectedRate, profileRate
}

func (s *sharedRuntimeSession) completeRecoveryKeepalive(stoppedAt time.Time) recoveryKeepaliveSnapshot {
	if s == nil {
		return recoveryKeepaliveSnapshot{}
	}
	if stoppedAt.IsZero() {
		stoppedAt = time.Now().UTC()
	} else {
		stoppedAt = stoppedAt.UTC()
	}

	s.mu.Lock()
	if s.recoveryKeepaliveStartedAt.IsZero() {
		s.recoveryKeepaliveStoppedAt = stoppedAt
		snapshot := recoveryKeepaliveSnapshot{
			StoppedAt:       stoppedAt,
			GuardrailCount:  s.recoveryKeepaliveGuardrailCount,
			GuardrailReason: s.recoveryKeepaliveGuardrailReason,
		}
		s.mu.Unlock()
		return snapshot
	}
	if stoppedAt.Before(s.recoveryKeepaliveStartedAt) {
		stoppedAt = s.recoveryKeepaliveStartedAt
	}
	s.recoveryKeepaliveStoppedAt = stoppedAt
	s.updateRecoveryKeepaliveMetricsLocked(stoppedAt)
	snapshot := s.currentRecoveryKeepaliveSnapshotLocked(stoppedAt)
	s.mu.Unlock()
	return snapshot
}

func (s *sharedRuntimeSession) observeRecoveryKeepaliveChunk(chunkBytes int, publishedAt time.Time) error {
	if s == nil || chunkBytes <= 0 {
		return nil
	}
	if publishedAt.IsZero() {
		publishedAt = time.Now().UTC()
	} else {
		publishedAt = publishedAt.UTC()
	}

	triggered := false
	guardrailReason := ""
	guardrailCount := int64(0)
	channelID := int64(0)
	guideNumber := ""
	sourceID := int64(0)
	mode := ""

	s.mu.Lock()
	if s.recoveryKeepaliveStartedAt.IsZero() {
		s.recoveryKeepaliveStartedAt = publishedAt
		s.recoveryKeepaliveStoppedAt = time.Time{}
		expectedRate, profileRate := s.recoveryKeepaliveExpectedRatesLocked()
		s.recoveryKeepaliveExpectedRate = expectedRate
		s.recoveryKeepaliveProfileRate = profileRate
	}

	s.recoveryKeepaliveBytes += int64(chunkBytes)
	s.recoveryKeepaliveChunks++
	s.updateRecoveryKeepaliveMetricsLocked(publishedAt)

	elapsed := publishedAt.Sub(s.recoveryKeepaliveStartedAt)
	if elapsed < 0 {
		elapsed = 0
	}
	if elapsed >= recoveryKeepaliveGuardrailWarmup && s.recoveryKeepaliveExpectedRate > 0 {
		limit := s.recoveryKeepaliveExpectedRate * recoveryKeepaliveGuardrailMultiplier
		if s.recoveryKeepaliveRateBytesPerSec > limit {
			if s.recoveryKeepaliveOverrateSince.IsZero() {
				s.recoveryKeepaliveOverrateSince = publishedAt
			} else if publishedAt.Sub(s.recoveryKeepaliveOverrateSince) >= recoveryKeepaliveGuardrailSustain {
				triggered = true
				guardrailReason = fmt.Sprintf(
					"rate_bytes_per_second=%.0f exceeds_limit=%.0f expected_bytes_per_second=%.0f",
					s.recoveryKeepaliveRateBytesPerSec,
					limit,
					s.recoveryKeepaliveExpectedRate,
				)
				if s.recoveryKeepaliveProfileRate > 0 {
					guardrailReason = fmt.Sprintf(
						"%s realtime_multiplier=%.2f",
						guardrailReason,
						s.recoveryKeepaliveMultiplier,
					)
				}
				s.recoveryKeepaliveGuardrailCount++
				s.recoveryKeepaliveGuardrailReason = guardrailReason
				guardrailCount = s.recoveryKeepaliveGuardrailCount
				channelID = s.channel.ChannelID
				guideNumber = s.channel.GuideNumber
				sourceID = s.sourceID
				mode = s.recoveryKeepaliveMode
			}
		} else {
			s.recoveryKeepaliveOverrateSince = time.Time{}
		}
	}
	s.mu.Unlock()

	if !triggered {
		return nil
	}
	if s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Warn(
			"shared session recovery keepalive overproduction guardrail triggered",
			"channel_id", channelID,
			"guide_number", guideNumber,
			"source_id", sourceID,
			"mode", strings.TrimSpace(mode),
			"guardrail_count", guardrailCount,
			"reason", guardrailReason,
		)
	}
	return fmt.Errorf("%w: %s", errRecoveryKeepaliveOverproduction, guardrailReason)
}

func (s *sharedRuntimeSession) recoveryKeepaliveExpectedRatesLocked() (float64, float64) {
	if s == nil {
		return 0, 0
	}
	profileExpectedRate := float64(0)
	if s.sourceProfile.BitrateBPS > 0 {
		profileExpectedRate = float64(s.sourceProfile.BitrateBPS) / 8.0
	}

	expectedBitrate := s.sourceProfile.BitrateBPS
	if expectedBitrate <= 0 {
		expectedBitrate = recoveryKeepaliveExpectedBitrateMin
	}
	expectedRate := float64(expectedBitrate) / 8.0
	return expectedRate, profileExpectedRate
}

func (s *sharedRuntimeSession) updateRecoveryKeepaliveMetricsLocked(now time.Time) {
	if s == nil || s.recoveryKeepaliveStartedAt.IsZero() {
		return
	}
	if now.Before(s.recoveryKeepaliveStartedAt) {
		now = s.recoveryKeepaliveStartedAt
	}
	duration := now.Sub(s.recoveryKeepaliveStartedAt)
	if duration <= 0 || s.recoveryKeepaliveBytes <= 0 {
		s.recoveryKeepaliveRateBytesPerSec = 0
		s.recoveryKeepaliveMultiplier = 0
		return
	}
	s.recoveryKeepaliveRateBytesPerSec = float64(s.recoveryKeepaliveBytes) / duration.Seconds()
	if s.recoveryKeepaliveProfileRate > 0 {
		s.recoveryKeepaliveMultiplier = s.recoveryKeepaliveRateBytesPerSec / s.recoveryKeepaliveProfileRate
	} else {
		s.recoveryKeepaliveMultiplier = 0
	}
}

func (s *sharedRuntimeSession) currentRecoveryKeepaliveSnapshotLocked(now time.Time) recoveryKeepaliveSnapshot {
	if s == nil {
		return recoveryKeepaliveSnapshot{}
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	startedAt := s.recoveryKeepaliveStartedAt
	stoppedAt := s.recoveryKeepaliveStoppedAt
	end := stoppedAt
	if end.IsZero() {
		end = now
	}
	if !startedAt.IsZero() && end.Before(startedAt) {
		end = startedAt
	}

	snapshot := recoveryKeepaliveSnapshot{
		StartedAt:          startedAt,
		StoppedAt:          stoppedAt,
		Bytes:              s.recoveryKeepaliveBytes,
		Chunks:             s.recoveryKeepaliveChunks,
		RateBytesPerSecond: s.recoveryKeepaliveRateBytesPerSec,
		ExpectedRate:       s.recoveryKeepaliveExpectedRate,
		RealtimeMultiplier: s.recoveryKeepaliveMultiplier,
		GuardrailCount:     s.recoveryKeepaliveGuardrailCount,
		GuardrailReason:    s.recoveryKeepaliveGuardrailReason,
	}
	if !startedAt.IsZero() {
		snapshot.Duration = end.Sub(startedAt)
	}
	return snapshot
}

func (s *sharedRuntimeSession) observePublishedChunk(data []byte, publishedAt time.Time) {
	if s == nil || len(data) < mpegTSPacketSize {
		return
	}
	pcrBase, ok := mpegTSLastPCRBase(data)
	if !ok {
		return
	}
	if publishedAt.IsZero() {
		publishedAt = time.Now().UTC()
	} else {
		publishedAt = publishedAt.UTC()
	}

	s.mu.Lock()
	s.recoveryObservedPCRBase = pcrBase
	s.recoveryObservedPCRAt = publishedAt
	s.mu.Unlock()
}

func (s *sharedRuntimeSession) startRecoveryHeartbeat(ctx context.Context) func(...bool) {
	if s == nil || s.manager == nil || s.ring == nil {
		return func(...bool) {}
	}
	if !s.manager.cfg.recoveryFillerEnabled {
		return func(...bool) {}
	}
	if s.subscriberCount() == 0 {
		return func(...bool) {}
	}

	interval := s.manager.cfg.recoveryFillerInterval
	if interval <= 0 {
		return func(...bool) {}
	}

	keepaliveMode := strings.ToLower(strings.TrimSpace(s.manager.cfg.recoveryFillerMode))
	switch keepaliveMode {
	case "", recoveryFillerModeNull:
		keepaliveMode = recoveryFillerModeNull
	case recoveryFillerModePSI, recoveryFillerModeSlateAV:
	default:
		keepaliveMode = recoveryFillerModeNull
	}
	transitionStrategy := defaultSharedRecoveryTransitionMode

	keepaliveCtx, cancelKeepalive := context.WithCancel(ctx)
	doneCh := make(chan struct{})
	s.setRecoveryTransitionState(
		transitionStrategy,
		transitionStrategy,
		false,
	)
	s.setRecoveryKeepaliveMode(keepaliveMode)
	startedAt := time.Now().UTC()
	expectedRate, profileRate := s.beginRecoveryKeepalive(startedAt)
	_ = s.publishRecoveryTransitionBoundary("live_to_keepalive")

	if s.manager != nil && s.manager.logger != nil {
		profile := s.currentRecoveryFillerProfile()
		s.manager.logger.Info(
			"shared session recovery keepalive started",
			"channel_id", s.channel.ChannelID,
			"guide_number", s.channel.GuideNumber,
			"source_id", s.currentSourceID(),
			"mode", keepaliveMode,
			"recovery_transition_strategy", transitionStrategy,
			"profile_resolution", formatResolution(profile.Width, profile.Height),
			"profile_fps", profile.FrameRate,
			"profile_audio_channels", profile.AudioChannels,
			"profile_audio_sample_rate", profile.AudioSampleRate,
			"keepalive_expected_rate_bytes_per_second", expectedRate,
			"keepalive_profile_expected_rate_bytes_per_second", profileRate,
			"keepalive_guardrail_multiplier", recoveryKeepaliveGuardrailMultiplier,
		)
	}

	go func() {
		defer close(doneCh)
		defer s.setRecoveryKeepaliveMode("")

		switch keepaliveMode {
		case recoveryFillerModeSlateAV:
			err := s.runRecoverySlateAVKeepalive(keepaliveCtx)
			if isRecoveryKeepaliveStopError(err) {
				return
			}
			if s.recordRecoveryKeepaliveFallback(recoveryFillerModeSlateAV, recoveryFillerModePSI, err) {
				err = s.runRecoveryPacketKeepalive(keepaliveCtx, recoveryFillerModePSI, interval)
				if isRecoveryKeepaliveStopError(err) {
					return
				}
				if s.recordRecoveryKeepaliveFallback(recoveryFillerModePSI, recoveryFillerModeNull, err) {
					_ = s.runRecoveryPacketKeepalive(keepaliveCtx, recoveryFillerModeNull, interval)
				}
			}
		case recoveryFillerModePSI:
			err := s.runRecoveryPacketKeepalive(keepaliveCtx, recoveryFillerModePSI, interval)
			if isRecoveryKeepaliveStopError(err) {
				return
			}
			if s.recordRecoveryKeepaliveFallback(recoveryFillerModePSI, recoveryFillerModeNull, err) {
				_ = s.runRecoveryPacketKeepalive(keepaliveCtx, recoveryFillerModeNull, interval)
			}
		default:
			_ = s.runRecoveryPacketKeepalive(keepaliveCtx, recoveryFillerModeNull, interval)
		}
	}()

	return func(emitLiveTransition ...bool) {
		publishLiveTransition := true
		if len(emitLiveTransition) > 0 {
			publishLiveTransition = emitLiveTransition[0]
		}
		cancelKeepalive()
		<-doneCh
		snapshot := s.completeRecoveryKeepalive(time.Now().UTC())
		if s.manager != nil && s.manager.logger != nil {
			s.manager.logger.Info(
				"shared session recovery keepalive stopped",
				"channel_id", s.channel.ChannelID,
				"guide_number", s.channel.GuideNumber,
				"source_id", s.currentSourceID(),
				"mode", keepaliveMode,
				"keepalive_started_at", snapshot.StartedAt,
				"keepalive_stopped_at", snapshot.StoppedAt,
				"keepalive_duration", snapshot.Duration.String(),
				"keepalive_bytes", snapshot.Bytes,
				"keepalive_chunks", snapshot.Chunks,
				"keepalive_rate_bytes_per_second", snapshot.RateBytesPerSecond,
				"keepalive_expected_rate_bytes_per_second", snapshot.ExpectedRate,
				"keepalive_realtime_multiplier", snapshot.RealtimeMultiplier,
				"keepalive_guardrail_count", snapshot.GuardrailCount,
				"keepalive_guardrail_reason", snapshot.GuardrailReason,
				"emit_keepalive_to_live_transition", publishLiveTransition,
			)
		}
		if publishLiveTransition {
			_ = s.publishRecoveryTransitionBoundary("keepalive_to_live")
		}
	}
}

func (s *sharedRuntimeSession) currentSourceID() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sourceID
}

func (s *sharedRuntimeSession) setManualRecoveryPending(pending bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.manualRecoveryPending = pending
	s.mu.Unlock()
}

func (s *sharedRuntimeSession) setManualRecoveryInProgress(inProgress bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.manualRecoveryInProgress = inProgress
	s.mu.Unlock()
}

func (s *sharedRuntimeSession) discardManualRecoveryRequest(reason string) bool {
	if s == nil {
		return false
	}

	s.mu.Lock()
	triggerCh := s.manualRecoveryCh
	s.mu.Unlock()
	if triggerCh == nil {
		return false
	}

	expectedReason := normalizeManualRecoveryReason(reason)
	select {
	case queuedReason := <-triggerCh:
		if queuedReason == expectedReason {
			s.setManualRecoveryPending(false)
			return true
		}
		// Preserve an unrelated pending trigger if we consumed one.
		select {
		case triggerCh <- queuedReason:
		default:
		}
		return false
	default:
		return false
	}
}

func (s *sharedRuntimeSession) requestManualRecovery(reason string) error {
	if s == nil {
		return ErrSessionNotFound
	}

	normalizedReason := normalizeManualRecoveryReason(reason)
	s.mu.Lock()
	if s.closed || (s.ctx != nil && s.ctx.Err() != nil) || len(s.subscribers) == 0 {
		s.mu.Unlock()
		return ErrSessionNotFound
	}

	ready := false
	if s.readyCh != nil {
		select {
		case <-s.readyCh:
			ready = true
		default:
		}
	}
	if !ready || s.readyErr != nil {
		s.mu.Unlock()
		return ErrSessionNotFound
	}
	if s.manualRecoveryPending || s.manualRecoveryInProgress {
		s.mu.Unlock()
		return ErrSessionRecoveryAlreadyPending
	}

	triggerCh := s.manualRecoveryCh
	channelID := s.channel.ChannelID
	guideNumber := s.channel.GuideNumber
	sourceID := s.sourceID
	if triggerCh == nil {
		s.mu.Unlock()
		return errors.New("shared session recovery channel is not initialized")
	}

	select {
	case triggerCh <- normalizedReason:
		s.manualRecoveryPending = true
		s.mu.Unlock()
		if s.manager != nil && s.manager.logger != nil {
			s.manager.logger.Info(
				"shared session manual recovery requested",
				"channel_id", channelID,
				"guide_number", guideNumber,
				"source_id", sourceID,
				"recovery_reason", normalizedReason,
			)
		}
		return nil
	default:
		s.mu.Unlock()
		return ErrSessionRecoveryAlreadyPending
	}
}

func (s *sharedRuntimeSession) currentRecoveryFillerProfile() streamProfile {
	if s == nil {
		return streamProfile{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sourceProfile
}

func (s *sharedRuntimeSession) currentRecoveryTimelinePTSOffset() time.Duration {
	if s == nil {
		return 0
	}
	now := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.recoveryObservedPCRAt.IsZero() {
		age := now.Sub(s.recoveryObservedPCRAt)
		if age < 0 {
			age = 0
		}
		if age <= (2 * defaultSharedStallHardDeadline) {
			pcrBase := advancePCRBase(s.recoveryObservedPCRBase, age)
			if pcrBase > 0 {
				return clampRecoveryTimelinePTSOffset(pcrBaseToDuration(pcrBase))
			}
		}
	} else if s.recoveryObservedPCRBase > 0 {
		return clampRecoveryTimelinePTSOffset(pcrBaseToDuration(s.recoveryObservedPCRBase))
	}
	if s.recoveryBoundaryPCRSet && s.recoveryBoundaryPCRBase > 0 {
		return clampRecoveryTimelinePTSOffset(pcrBaseToDuration(s.recoveryBoundaryPCRBase))
	}
	return 0
}

func (s *sharedRuntimeSession) setRecoveryKeepaliveMode(mode string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.recoveryKeepaliveMode = strings.TrimSpace(mode)
	s.mu.Unlock()
}

func (s *sharedRuntimeSession) setRecoveryTransitionState(
	mode string,
	effectiveMode string,
	stitchApplied bool,
) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.recoveryTransitionMode = strings.TrimSpace(mode)
	s.recoveryTransitionEffective = strings.TrimSpace(effectiveMode)
	s.recoveryTransitionStitchApplied = stitchApplied
	s.mu.Unlock()
}

func (s *sharedRuntimeSession) setRecoveryTransitionStitchApplied(stitchApplied bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.recoveryTransitionStitchApplied = stitchApplied
	s.mu.Unlock()
}

func (s *sharedRuntimeSession) setRecoveryTransitionSignalState(applied []string, skipped []string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.recoveryTransitionSignalsApplied = strings.TrimSpace(strings.Join(applied, ","))
	s.recoveryTransitionSignalSkips = strings.TrimSpace(strings.Join(skipped, ","))
	s.mu.Unlock()
}

func (s *sharedRuntimeSession) nextRecoveryPSIChunk() []byte {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	pat := s.recoveryPATContinuity
	pmt := s.recoveryPMTContinuity
	patVersion := s.recoveryPATVersion
	pmtVersion := s.recoveryPMTVersion
	s.recoveryPATContinuity = (s.recoveryPATContinuity + 1) & 0x0F
	s.recoveryPMTContinuity = (s.recoveryPMTContinuity + 1) & 0x0F
	s.mu.Unlock()
	return mpegTSPSIHeartbeatChunkWithVersions(pat, pmt, patVersion, pmtVersion)
}

func (s *sharedRuntimeSession) nextRecoveryTransitionBoundaryChunk(transition string) ([]byte, []string, []string, bool, string) {
	if s == nil {
		return nil, nil, []string{"session_unavailable"}, false, "session_unavailable"
	}

	now := time.Now().UTC()
	transition = strings.TrimSpace(strings.ToLower(transition))

	s.mu.Lock()
	patContinuity := (s.recoveryPATContinuity + 8) & 0x0F
	pmtContinuity := (s.recoveryPMTContinuity + 8) & 0x0F
	pcrContinuity := (s.recoveryPCRContinuity + 8) & 0x0F
	patVersion := (s.recoveryPATVersion + 1) & 0x1F
	pmtVersion := (s.recoveryPMTVersion + 1) & 0x1F
	pcrBase := uint64(0)
	stitchApplied := false
	stitchFallback := ""
	observedAt := s.recoveryObservedPCRAt
	observedPCR := s.recoveryObservedPCRBase
	if !observedAt.IsZero() {
		age := now.Sub(observedAt)
		if age < 0 {
			age = 0
		}
		if age <= (2 * defaultSharedStallHardDeadline) {
			pcrBase = advancePCRBase(observedPCR, age)
			stitchApplied = true
		} else {
			stitchFallback = "stitch_no_recent_pcr"
		}
	} else {
		stitchFallback = "stitch_no_observed_pcr"
	}
	if !stitchApplied {
		if s.recoveryBoundaryPCRSet {
			pcrBase = advancePCRBase(s.recoveryBoundaryPCRBase, time.Second)
		}
	}
	s.recoveryBoundaryPCRBase = pcrBase
	s.recoveryBoundaryPCRSet = true

	startupRandomAccessReady := s.sourceStartupRandomAccessReady
	startupRandomAccessCodec := strings.TrimSpace(s.sourceStartupRandomAccessCodec)
	keepaliveMode := strings.TrimSpace(strings.ToLower(s.recoveryKeepaliveMode))
	s.recoveryPATContinuity = (patContinuity + 1) & 0x0F
	s.recoveryPMTContinuity = (pmtContinuity + 1) & 0x0F
	s.recoveryPCRContinuity = (pcrContinuity + 1) & 0x0F
	s.recoveryPATVersion = patVersion
	s.recoveryPMTVersion = pmtVersion
	s.mu.Unlock()

	appliedSignals := []string{"disc", "cc", "pcr", "psi_version"}
	skippedSignals := make([]string, 0, 2)
	fallbackReason := ""
	if stitchApplied {
		appliedSignals = append(appliedSignals, "stitch")
	} else if stitchFallback != "" {
		skippedSignals = append(skippedSignals, stitchFallback)
		fallbackReason = stitchFallback
	}

	switch transition {
	case "live_to_keepalive":
		if keepaliveMode == recoveryFillerModeSlateAV {
			appliedSignals = append(appliedSignals, "idr")
		} else {
			skippedSignals = append(skippedSignals, "idr_keepalive_non_av")
		}
	case "keepalive_to_live":
		if startupRandomAccessReady {
			appliedSignals = append(appliedSignals, "idr")
		} else {
			reason := "idr_not_ready"
			if startupRandomAccessCodec != "" {
				reason = "idr_not_ready:" + startupRandomAccessCodec
			}
			skippedSignals = append(skippedSignals, reason)
			if fallbackReason == "" {
				fallbackReason = reason
			}
		}
	}

	chunk := mpegTSRecoveryTransitionChunk(
		patContinuity,
		pmtContinuity,
		pcrContinuity,
		patVersion,
		pmtVersion,
		pcrBase,
	)
	if len(chunk) == 0 {
		return nil, nil, []string{"chunk_generation_failed"}, stitchApplied, "chunk_generation_failed"
	}
	return chunk, appliedSignals, skippedSignals, stitchApplied, fallbackReason
}

func (s *sharedRuntimeSession) publishRecoveryTransitionBoundary(transition string) error {
	if s == nil || s.ring == nil {
		return nil
	}
	if s.subscriberCount() == 0 {
		return nil
	}

	chunk, appliedSignals, skippedSignals, stitchApplied, fallbackReason := s.nextRecoveryTransitionBoundaryChunk(transition)
	s.setRecoveryTransitionSignalState(appliedSignals, skippedSignals)
	s.setRecoveryTransitionStitchApplied(stitchApplied)
	if fallbackReason != "" {
		s.recordRecoveryTransitionFallback(
			defaultSharedRecoveryTransitionMode,
			defaultSharedRecoveryTransitionMode,
			errors.New(strings.TrimSpace(fallbackReason)),
		)
	}
	if len(chunk) == 0 {
		return nil
	}
	if err := s.publishChunk(chunk, time.Now().UTC()); err != nil {
		return err
	}

	if s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Debug(
			"shared session recovery keepalive transition boundary published",
			"channel_id", s.channel.ChannelID,
			"guide_number", s.channel.GuideNumber,
			"source_id", s.currentSourceID(),
			"transition", strings.TrimSpace(transition),
			"recovery_transition_strategy", s.manager.cfg.recoveryTransitionMode,
			"recovery_transition_signals_applied", strings.TrimSpace(strings.Join(appliedSignals, ",")),
			"recovery_transition_signal_skips", strings.TrimSpace(strings.Join(skippedSignals, ",")),
			"recovery_transition_stitch_applied", stitchApplied,
			"recovery_transition_fallback_reason", strings.TrimSpace(fallbackReason),
		)
	}
	return nil
}

func (s *sharedRuntimeSession) runRecoveryPacketKeepalive(
	ctx context.Context,
	mode string,
	interval time.Duration,
) error {
	if s == nil || s.ring == nil {
		return nil
	}
	if interval <= 0 {
		interval = defaultSharedRecoveryFillerInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			if s.subscriberCount() == 0 {
				continue
			}

			var payload []byte
			if mode == recoveryFillerModePSI {
				payload = s.nextRecoveryPSIChunk()
			} else {
				payload = mpegTSNullPacketChunk(1)
			}
			if err := s.publishRecoveryKeepaliveChunk(payload, now.UTC()); err != nil {
				return err
			}
		}
	}
}

func (s *sharedRuntimeSession) runRecoverySlateAVKeepalive(ctx context.Context) error {
	if s == nil || s.manager == nil || s.ring == nil {
		return nil
	}
	factory := s.manager.cfg.recoverySlateAVFactory
	if factory == nil {
		return errors.New("recovery slate AV filler factory is not configured")
	}

	rawProfile := s.currentRecoveryFillerProfile()
	normalizedProfile := normalizeRecoveryFillerProfile(
		rawProfile,
		s.manager.cfg.recoveryFillerEnableAudio,
	)
	profile := streamProfile{
		Width:           normalizedProfile.Width,
		Height:          normalizedProfile.Height,
		FrameRate:       normalizedProfile.FrameRate,
		AudioSampleRate: normalizedProfile.AudioSampleRate,
		AudioChannels:   normalizedProfile.AudioChannels,
		BitrateBPS:      rawProfile.BitrateBPS,
		VideoCodec:      rawProfile.VideoCodec,
		AudioCodec:      rawProfile.AudioCodec,
	}
	if s.manager.logger != nil {
		reason := recoveryFillerResolutionNormalizationReason(
			rawProfile.Width,
			rawProfile.Height,
			normalizedProfile.Width,
			normalizedProfile.Height,
		)
		if reason != "" {
			s.manager.logger.Debug(
				"shared session slate AV recovery filler profile normalized",
				"channel_id", s.channel.ChannelID,
				"guide_number", s.channel.GuideNumber,
				"source_id", s.currentSourceID(),
				"normalization_reason", reason,
				"original_resolution", formatResolution(rawProfile.Width, rawProfile.Height),
				"original_width", rawProfile.Width,
				"original_height", rawProfile.Height,
				"normalized_resolution", formatResolution(normalizedProfile.Width, normalizedProfile.Height),
				"normalized_width", normalizedProfile.Width,
				"normalized_height", normalizedProfile.Height,
			)
		}
	}
	producerCfg := slateAVFillerConfig{
		FFmpegPath:  s.manager.cfg.ffmpegPath,
		Profile:     profile,
		Text:        s.manager.cfg.recoveryFillerText,
		EnableAudio: s.manager.cfg.recoveryFillerEnableAudio,
		PTSOffset:   s.currentRecoveryTimelinePTSOffset(),
	}

	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		if s.subscriberCount() == 0 {
			return nil
		}

		producer, err := factory(producerCfg)
		if err != nil {
			return fmt.Errorf("build slate AV recovery filler producer: %w", err)
		}

		reader, err := producer.Start(ctx)
		if err != nil {
			lastErr = fmt.Errorf("start slate AV recovery filler producer: %w", err)
		} else {
			pump := NewPump(
				PumpConfig{
					ChunkBytes:           s.manager.cfg.bufferChunkBytes,
					PublishFlushInterval: s.manager.cfg.bufferFlushPeriod,
					TSAlign188:           s.manager.cfg.bufferTSAlign188,
				},
				&recoveryKeepaliveChunkPublisher{session: s},
			)
			pumpErr := pump.Run(ctx, reader)
			closeErr := s.closeSlateAVRecoveryReaderWithTimeout(reader, s.manager.cfg.sessionDrainTimeout)
			switch {
			case pumpErr != nil:
				lastErr = pumpErr
			case closeErr != nil:
				lastErr = fmt.Errorf("slate AV recovery filler process exit: %w", closeErr)
			default:
				lastErr = io.EOF
			}
		}

		if isRecoveryKeepaliveStopError(lastErr) {
			return lastErr
		}

		if attempt < 2 {
			if s.manager != nil && s.manager.logger != nil {
				s.manager.logger.Warn(
					"shared session slate AV keepalive exited; restarting once",
					"channel_id", s.channel.ChannelID,
					"guide_number", s.channel.GuideNumber,
					"source_id", s.currentSourceID(),
					"attempt", attempt,
					"error", lastErr,
				)
			}
			continue
		}
	}

	return fmt.Errorf("slate AV recovery filler failed after restart: %w", lastErr)
}

func (s *sharedRuntimeSession) closeSlateAVRecoveryReaderWithTimeout(
	reader io.ReadCloser,
	timeout time.Duration,
) error {
	if reader == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = boundedCloseTimeout
	}
	switch closeWithTimeoutStartWorker(reader) {
	case closeWithTimeoutStartSuccess:
		// continue below
	case closeWithTimeoutStartSuppressedDuplicate:
		return fmt.Errorf("timed out waiting for slate AV recovery filler shutdown: bounded close worker suppressed (duplicate close in-flight)")
	case closeWithTimeoutStartSuppressedBudget:
		closeWithTimeoutQueueRetry(reader, timeout)
		return fmt.Errorf("timed out waiting for slate AV recovery filler shutdown: bounded close worker suppressed (queued retry)")
	default:
		return fmt.Errorf("timed out waiting for slate AV recovery filler shutdown: bounded close worker suppressed")
	}

	done := make(chan error, 1)
	go func() {
		done <- reader.Close()
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-done:
		if err != nil && s != nil && s.manager != nil && s.manager.logger != nil {
			if shouldLog, coalesced := s.shouldEmitSlateAVCloseWarn(time.Now()); shouldLog {
				stats := closeWithTimeoutStatsSnapshot()
				fields := []any{
					"close_error_type", "non_timeout",
					"channel_id", s.channel.ChannelID,
					"guide_number", s.channel.GuideNumber,
					"source_id", s.currentSourceID(),
					"error", err,
					"close_started", stats.Started,
					"close_retried", stats.Retried,
					"close_suppressed", stats.Suppressed,
					"close_dropped", stats.Dropped,
					"close_queued", stats.Queued,
					"close_timeouts", stats.Timeouts,
					"close_late_completions", stats.LateCompletions,
					"close_late_abandoned", stats.LateAbandoned,
					"close_release_underflow", stats.ReleaseUnderflow,
					"close_suppressed_duplicate", stats.SuppressedDuplicate,
					"close_suppressed_budget", stats.SuppressedBudget,
				}
				if coalesced > 0 {
					fields = append(fields, "close_non_timeout_logs_coalesced", coalesced)
				}
				s.manager.logger.Warn("shared session slate AV close error", fields...)
			}
		}
		closeWithTimeoutFinishWorker(reader)
		return err
	case <-timer.C:
		closeWithTimeoutRecordTimedOut()
		timeoutErr := fmt.Errorf("timed out waiting for slate AV recovery filler shutdown after %s", timeout)
		if s != nil && s.manager != nil && s.manager.logger != nil {
			// Timeouts intentionally bypass non-timeout warn coalescing so each
			// prolonged shutdown event remains visible to operators.
			stats := closeWithTimeoutStatsSnapshot()
			s.manager.logger.Warn(
				"shared session slate AV close error",
				"close_error_type", "timeout",
				"channel_id", s.channel.ChannelID,
				"guide_number", s.channel.GuideNumber,
				"source_id", s.currentSourceID(),
				"timeout", timeout,
				"error", timeoutErr,
				"close_started", stats.Started,
				"close_retried", stats.Retried,
				"close_suppressed", stats.Suppressed,
				"close_dropped", stats.Dropped,
				"close_queued", stats.Queued,
				"close_timeouts", stats.Timeouts,
				"close_late_completions", stats.LateCompletions,
				"close_late_abandoned", stats.LateAbandoned,
				"close_release_underflow", stats.ReleaseUnderflow,
				"close_suppressed_duplicate", stats.SuppressedDuplicate,
				"close_suppressed_budget", stats.SuppressedBudget,
			)
		}
		go func() {
			_ = <-done
			closeWithTimeoutRecordLateCompletion()
			closeWithTimeoutFinishWorker(reader)
		}()
		return timeoutErr
	}
}

func (s *sharedRuntimeSession) recordRecoveryKeepaliveFallback(fromMode, toMode string, cause error) bool {
	if s == nil {
		return false
	}
	if strings.TrimSpace(toMode) == "" {
		return false
	}
	if isRecoveryKeepaliveStopError(cause) {
		return false
	}

	reason := strings.TrimSpace(fmt.Sprintf("%s: %v", strings.TrimSpace(fromMode), cause))
	s.mu.Lock()
	s.recoveryKeepaliveFallbacks++
	s.recoveryKeepaliveLastFallback = reason
	s.recoveryKeepaliveMode = strings.TrimSpace(toMode)
	fallbackCount := s.recoveryKeepaliveFallbacks
	s.mu.Unlock()

	if s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Warn(
			"shared session recovery keepalive fallback",
			"channel_id", s.channel.ChannelID,
			"guide_number", s.channel.GuideNumber,
			"source_id", s.currentSourceID(),
			"from_mode", strings.TrimSpace(fromMode),
			"to_mode", strings.TrimSpace(toMode),
			"fallback_count", fallbackCount,
			"reason", reason,
		)
	}
	return true
}

func (s *sharedRuntimeSession) recordRecoveryTransitionFallback(fromMode, toMode string, cause error) bool {
	if s == nil {
		return false
	}
	if strings.TrimSpace(toMode) == "" || cause == nil {
		return false
	}

	reason := strings.TrimSpace(fmt.Sprintf("%s: %v", strings.TrimSpace(fromMode), cause))
	s.mu.Lock()
	if s.recoveryTransitionLastFallback == reason && s.recoveryTransitionFallbacks > 0 {
		s.recoveryTransitionEffective = strings.TrimSpace(toMode)
		s.mu.Unlock()
		return false
	}
	s.recoveryTransitionFallbacks++
	s.recoveryTransitionLastFallback = reason
	s.recoveryTransitionEffective = strings.TrimSpace(toMode)
	fallbackCount := s.recoveryTransitionFallbacks
	s.mu.Unlock()

	if s.manager != nil && s.manager.logger != nil {
		s.manager.logger.Warn(
			"shared session recovery transition fallback",
			"channel_id", s.channel.ChannelID,
			"guide_number", s.channel.GuideNumber,
			"source_id", s.currentSourceID(),
			"from_mode", strings.TrimSpace(fromMode),
			"to_mode", strings.TrimSpace(toMode),
			"fallback_count", fallbackCount,
			"reason", reason,
		)
	}
	return true
}

func isRecoveryKeepaliveStopError(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (s *sharedRuntimeSession) markReady(err error) {
	s.readyOnce.Do(func() {
		s.readyErr = err
		close(s.readyCh)
	})
}

func classifySharedSessionTerminalStatus(err error) string {
	switch {
	case err == nil:
		return "completed"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	default:
		return "error"
	}
}

func (s *sharedRuntimeSession) historySnapshot() SharedSessionHistory {
	if s == nil {
		return SharedSessionHistory{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.historySnapshotLocked()
}

func (s *sharedRuntimeSession) historySnapshotLocked() SharedSessionHistory {
	if s == nil {
		return SharedSessionHistory{}
	}
	// Keep snapshot output bounded even if a mutation path misses truncation.
	// This defensive compaction mutates truncated state under s.mu. Repeated
	// snapshots become idempotent only after history already fits configured
	// bounds (truncate helpers become no-ops in that steady state).
	s.truncateSourceHistoryLocked()
	s.truncateSubscriberHistoryLocked()

	lastErr := ""
	if s.lastErr != nil {
		lastErr = s.lastErr.Error()
	}
	tunerID := -1
	if s.lease != nil {
		tunerID = s.lease.ID
	}
	subscriberWriteDeadlineTimeouts, subscriberWriteShortWrites, subscriberWriteBlockedDurationUS, subscriberWriteBlockedDurationMS := s.subscriberWritePressureSnapshot()
	return SharedSessionHistory{
		SessionID:                            s.historySessionID,
		ChannelID:                            s.channel.ChannelID,
		GuideNumber:                          strings.TrimSpace(s.channel.GuideNumber),
		GuideName:                            strings.TrimSpace(s.channel.GuideName),
		TunerID:                              tunerID,
		OpenedAt:                             s.startedAt,
		ClosedAt:                             s.closedAt,
		Active:                               !s.closed,
		TerminalStatus:                       classifySharedSessionTerminalStatus(s.lastErr),
		TerminalError:                        lastErr,
		PeakSubscribers:                      s.historyPeakSubscribers,
		TotalSubscribers:                     s.historyTotalSubscribers,
		CompletedSubscribers:                 s.historyCompletedSubscribers,
		SlowSkipEventsTotal:                  s.slowSkipEventsTotal,
		SlowSkipLagChunksTotal:               s.slowSkipLagChunksTotal,
		SlowSkipLagBytesTotal:                s.slowSkipLagBytesTotal,
		SlowSkipMaxLagChunks:                 s.slowSkipMaxLagChunks,
		SubscriberWriteDeadlineTimeoutsTotal: subscriberWriteDeadlineTimeouts,
		SubscriberWriteShortWritesTotal:      subscriberWriteShortWrites,
		SubscriberWriteBlockedDurationUS:     subscriberWriteBlockedDurationUS,
		SubscriberWriteBlockedDurationMS:     subscriberWriteBlockedDurationMS,
		RecoveryCycleCount:                   s.recoveryCycle,
		LastRecoveryReason:                   strings.TrimSpace(s.recoveryReason),
		SourceSelectCount:                    s.sourceSelects,
		SameSourceReselectCount:              s.sameSourceReselectCount,
		LastSourceSelectedAt:                 s.lastSourceSelectedAt,
		LastSourceSelectReason:               strings.TrimSpace(s.lastSourceSelectReason),
		LastError:                            lastErr,
		Sources:                              append([]SharedSessionSourceHistory(nil), s.historySources...),
		Subscribers:                          append([]SharedSessionSubscriberHistory(nil), s.historySubscribers...),
		SourceHistoryLimit:                   s.sourceHistoryLimitLocked(),
		SourceHistoryTruncated:               s.historySourcesTruncated,
		SubscriberHistoryLimit:               s.subscriberHistoryLimitLocked(),
		SubscriberHistoryTruncated:           s.historySubscribersTruncated,
	}
}

func (s *sharedRuntimeSession) updateCurrentHistorySourceProfileLocked() {
	if s == nil {
		return
	}
	if s.historyCurrentSourceIdx < 0 || s.historyCurrentSourceIdx >= len(s.historySources) {
		return
	}
	profile := s.sourceProfile
	s.historySources[s.historyCurrentSourceIdx].Resolution = formatResolution(profile.Width, profile.Height)
	s.historySources[s.historyCurrentSourceIdx].FrameRate = profile.FrameRate
	s.historySources[s.historyCurrentSourceIdx].VideoCodec = strings.TrimSpace(profile.VideoCodec)
	s.historySources[s.historyCurrentSourceIdx].AudioCodec = strings.TrimSpace(profile.AudioCodec)
	s.historySources[s.historyCurrentSourceIdx].ProfileBitrateBPS = profile.BitrateBPS
}

func (s *sharedRuntimeSession) finish(err error) {
	if s == nil {
		return
	}

	closedAt := time.Now().UTC()
	historyEntry := SharedSessionHistory{}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.closedAt = closedAt
	s.lastErr = err
	s.manualRecoveryPending = false
	s.manualRecoveryInProgress = false
	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}
	if s.profileProbeCancel != nil {
		s.profileProbeCancel()
		s.profileProbeCancel = nil
	}
	subs := len(s.subscribers)
	sourceID := s.sourceID
	if s.historyCurrentSourceIdx >= 0 && s.historyCurrentSourceIdx < len(s.historySources) {
		if s.historySources[s.historyCurrentSourceIdx].DeselectedAt.IsZero() {
			s.historySources[s.historyCurrentSourceIdx].DeselectedAt = closedAt
		}
	}
	for i := range s.historySubscribers {
		if !s.historySubscribers[i].ClosedAt.IsZero() {
			continue
		}
		s.historySubscribers[i].ClosedAt = closedAt
		s.historySubscribers[i].CloseReason = subscriberRemovalReasonSessionClosed
		s.historyCompletedSubscribers++
	}
	for subscriberID := range s.historySubscriberIndex {
		delete(s.historySubscriberIndex, subscriberID)
	}
	historyEntry = s.historySnapshotLocked()
	s.mu.Unlock()

	// Ensure all session-scoped background workers (for example source-health
	// persistence) observe cancellation on every terminal path.
	s.cancelNow()

	// Decouple terminal signaling from post-cancel persistence drain so clients
	// unblocks and subscribers terminate promptly even if persistence is
	// stalled or blocked.
	s.markReady(err)
	s.ring.Close(err)

	// Wait for source-health persistence drain after signaling terminal state.
	// This preserves durability attempts while avoiding lifecycle coupling for
	// response timing and ring teardown.
	if s.sourceHealthPersistDone != nil {
		<-s.sourceHealthPersistDone
	}
	// Wait for profile-probe persistence workers to observe terminal
	// cancellation and converge before release.
	s.profileProbePersistWg.Wait()
	if s.lease != nil {
		tunerID := s.lease.ID
		tunerLeaseToken := s.lease.token
		if tunerLeaseToken != 0 && s.manager != nil && s.manager.tuners != nil {
			s.manager.tuners.clearClientPreemptible(tunerID, tunerLeaseToken)
		}
		s.lease.Release()
	}
	if s.manager != nil {
		s.manager.recordClosedSessionHistory(historyEntry)
	}
	s.manager.removeSession(s.channel.ChannelID, s)

	fields := []any{
		"channel_id", s.channel.ChannelID,
		"guide_number", s.channel.GuideNumber,
		"source_id", sourceID,
		"subscribers", subs,
		"duration", time.Since(s.startedAt).String(),
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fields = append(fields, "error", err)
		s.manager.logger.Warn("shared stream session stopped with error", fields...)
		return
	}
	s.manager.logger.Info("shared stream session stopped", fields...)
}

func (s *sharedRuntimeSession) stats() SharedSessionStats {
	if s == nil {
		return SharedSessionStats{}
	}
	now := time.Now().UTC()
	coalescedTotal, droppedTotal, coalescedBySource, droppedBySource := s.sourceHealthPersistBackpressureSnapshot()
	subscriberWriteDeadlineTimeouts, subscriberWriteShortWrites, subscriberWriteBlockedDurationUS, subscriberWriteBlockedDurationMS := s.subscriberWritePressureSnapshot()

	pumpStats := PumpStats{}
	if s.pump != nil {
		pumpStats = s.pump.Stats()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	lastErr := ""
	if s.lastErr != nil {
		lastErr = s.lastErr.Error()
	}

	subscriberInfo := make([]SubscriberStats, 0, len(s.subscribers))
	for _, subscriber := range s.subscribers {
		subscriberInfo = append(subscriberInfo, subscriber)
	}
	sort.Slice(subscriberInfo, func(i, j int) bool {
		if subscriberInfo[i].StartedAt.Equal(subscriberInfo[j].StartedAt) {
			return subscriberInfo[i].SubscriberID < subscriberInfo[j].SubscriberID
		}
		return subscriberInfo[i].StartedAt.Before(subscriberInfo[j].StartedAt)
	})

	tunerID := -1
	if s.lease != nil {
		tunerID = s.lease.ID
	}
	sinceLastSourceSelect := ""
	if !s.lastSourceSelectedAt.IsZero() {
		since := now.Sub(s.lastSourceSelectedAt)
		if since < 0 {
			since = 0
		}
		sinceLastSourceSelect = since.String()
	}
	resolution := formatResolution(s.sourceProfile.Width, s.sourceProfile.Height)
	frameRate := s.sourceProfile.FrameRate
	videoCodec := s.sourceProfile.VideoCodec
	audioCodec := s.sourceProfile.AudioCodec
	currentBitrateBPS := estimateCurrentBitrateBPS(
		now,
		s.lastSourceSelectedAt,
		pumpStats.BytesPublished,
		s.sourceSelectBytesBase,
	)
	profileBitrateBPS := s.sourceProfile.BitrateBPS
	keepaliveSnapshot := s.currentRecoveryKeepaliveSnapshotLocked(now)
	keepaliveDuration := ""
	if !keepaliveSnapshot.StartedAt.IsZero() {
		keepaliveDuration = keepaliveSnapshot.Duration.String()
	}

	return SharedSessionStats{
		TunerID:                              tunerID,
		ChannelID:                            s.channel.ChannelID,
		GuideNumber:                          s.channel.GuideNumber,
		GuideName:                            s.channel.GuideName,
		SourceID:                             s.sourceID,
		SourceItemKey:                        s.sourceItemKey,
		SourceStreamURL:                      s.sourceStreamURL,
		SourceStartupProbeRawBytes:           s.sourceStartupProbeRawBytes,
		SourceStartupProbeTrimmedBytes:       s.sourceStartupProbeTrimmedBytes,
		SourceStartupProbeCutoverOffset:      s.sourceStartupProbeCutoverOffset,
		SourceStartupProbeDroppedBytes:       s.sourceStartupProbeDroppedBytes,
		SourceStartupProbeBytes:              s.sourceStartupProbeBytes,
		SourceStartupRandomAccessReady:       s.sourceStartupRandomAccessReady,
		SourceStartupRandomAccessCodec:       s.sourceStartupRandomAccessCodec,
		SourceStartupInventoryMethod:         s.sourceStartupInventoryMethod,
		SourceStartupVideoStreams:            s.sourceStartupVideoStreams,
		SourceStartupAudioStreams:            s.sourceStartupAudioStreams,
		SourceStartupVideoCodecs:             s.sourceStartupVideoCodecs,
		SourceStartupAudioCodecs:             s.sourceStartupAudioCodecs,
		SourceStartupComponentState:          s.sourceStartupComponentState,
		SourceStartupRetryRelaxedProbe:       s.sourceStartupRetryRelaxedProbe,
		SourceStartupRetryRelaxedProbeReason: s.sourceStartupRetryReason,
		Resolution:                           resolution,
		FrameRate:                            frameRate,
		VideoCodec:                           videoCodec,
		AudioCodec:                           audioCodec,
		CurrentBitrateBPS:                    currentBitrateBPS,
		ProfileBitrateBPS:                    profileBitrateBPS,
		Producer:                             s.producer,
		StartedAt:                            s.startedAt,
		LastByteAt:                           pumpStats.LastByteReadAt,
		LastPushAt:                           pumpStats.LastPublishAt,
		BytesRead:                            pumpStats.BytesRead,
		BytesPushed:                          pumpStats.BytesPublished,
		ChunksPushed:                         pumpStats.ChunksPublished,
		Subscribers:                          len(s.subscribers),
		SubscriberInfo:                       subscriberInfo,
		SlowSkipEventsTotal:                  s.slowSkipEventsTotal,
		SlowSkipLagChunksTotal:               s.slowSkipLagChunksTotal,
		SlowSkipLagBytesTotal:                s.slowSkipLagBytesTotal,
		SlowSkipMaxLagChunks:                 s.slowSkipMaxLagChunks,
		SubscriberWriteDeadlineTimeoutsTotal: subscriberWriteDeadlineTimeouts,
		SubscriberWriteShortWritesTotal:      subscriberWriteShortWrites,
		SubscriberWriteBlockedDurationUS:     subscriberWriteBlockedDurationUS,
		SubscriberWriteBlockedDurationMS:     subscriberWriteBlockedDurationMS,
		StallCount:                           s.stallCount,
		RecoveryCycle:                        s.recoveryCycle,
		RecoveryReason:                       s.recoveryReason,
		RecoveryTransitionMode:               s.recoveryTransitionMode,
		RecoveryTransitionEffectiveMode:      s.recoveryTransitionEffective,
		RecoveryTransitionSignalsApplied:     s.recoveryTransitionSignalsApplied,
		RecoveryTransitionSignalSkips:        s.recoveryTransitionSignalSkips,
		RecoveryTransitionFallbackCount:      s.recoveryTransitionFallbacks,
		RecoveryTransitionFallbackReason:     s.recoveryTransitionLastFallback,
		RecoveryTransitionStitchApplied:      s.recoveryTransitionStitchApplied,
		RecoveryKeepaliveMode:                s.recoveryKeepaliveMode,
		RecoveryKeepaliveFallbackCount:       s.recoveryKeepaliveFallbacks,
		RecoveryKeepaliveFallbackReason:      s.recoveryKeepaliveLastFallback,
		RecoveryKeepaliveStartedAt:           keepaliveSnapshot.StartedAt,
		RecoveryKeepaliveStoppedAt:           keepaliveSnapshot.StoppedAt,
		RecoveryKeepaliveDuration:            keepaliveDuration,
		RecoveryKeepaliveBytes:               keepaliveSnapshot.Bytes,
		RecoveryKeepaliveChunks:              keepaliveSnapshot.Chunks,
		RecoveryKeepaliveRateBytesPerSecond:  keepaliveSnapshot.RateBytesPerSecond,
		RecoveryKeepaliveExpectedRate:        keepaliveSnapshot.ExpectedRate,
		RecoveryKeepaliveRealtimeMultiplier:  keepaliveSnapshot.RealtimeMultiplier,
		RecoveryKeepaliveGuardrailCount:      keepaliveSnapshot.GuardrailCount,
		RecoveryKeepaliveGuardrailReason:     keepaliveSnapshot.GuardrailReason,
		SourceSelectCount:                    s.sourceSelects,
		SameSourceReselectCount:              s.sameSourceReselectCount,
		LastSourceSelectedAt:                 s.lastSourceSelectedAt,
		LastSourceSelectReason:               s.lastSourceSelectReason,
		SinceLastSourceSelect:                sinceLastSourceSelect,
		LastError:                            lastErr,
		SourceHealthPersistCoalescedTotal:    coalescedTotal,
		SourceHealthPersistDroppedTotal:      droppedTotal,
		SourceHealthPersistCoalescedBySource: coalescedBySource,
		SourceHealthPersistDroppedBySource:   droppedBySource,
	}
}

func (s *sharedRuntimeSession) sourceHealthPersistBackpressureSnapshot() (
	coalescedTotal int64,
	droppedTotal int64,
	coalescedBySource map[int64]int64,
	droppedBySource map[int64]int64,
) {
	if s == nil {
		return 0, 0, nil, nil
	}
	s.sourceHealthPersistMu.Lock()
	defer s.sourceHealthPersistMu.Unlock()

	coalescedTotal = s.sourceHealthCoalescedTotal
	droppedTotal = s.sourceHealthDroppedTotal
	if len(s.sourceHealthCoalescedBySource) > 0 {
		coalescedBySource = make(map[int64]int64, len(s.sourceHealthCoalescedBySource))
		for sourceID, count := range s.sourceHealthCoalescedBySource {
			coalescedBySource[sourceID] = count
		}
	}
	if len(s.sourceHealthDroppedBySource) > 0 {
		droppedBySource = make(map[int64]int64, len(s.sourceHealthDroppedBySource))
		for sourceID, count := range s.sourceHealthDroppedBySource {
			droppedBySource[sourceID] = count
		}
	}
	return coalescedTotal, droppedTotal, coalescedBySource, droppedBySource
}

func (s *sharedRuntimeSession) recordSourceFailure(
	ctx context.Context,
	sourceID int64,
	streamURL string,
	err error,
	track bool,
) {
	if sourceID <= 0 {
		return
	}

	reason := startupFailureReason(err)
	if code := upstreamStatusCode(reason); isLikelyUpstreamOverlimitStatus(code) {
		s.manager.armProviderOverlimitCooldownForSource(code, reason, sourceID, streamURL)
	}
	if !track {
		return
	}
	// Don't stage events after terminal cancellation — the persistence
	// worker is shutting down and won't process them, which would leave
	// orphaned overlays biasing source ordering.
	if s.ctx != nil && s.ctx.Err() != nil {
		return
	}
	observedAt := time.Now().UTC()
	eventID := uint64(0)
	if s.manager != nil {
		eventID = s.manager.stageRecentSourceFailure(sourceID, reason, observedAt)
	}
	s.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    eventID,
		success:    false,
		reason:     reason,
		observedAt: observedAt,
	})
}

func (s *sharedRuntimeSession) recordSourceSuccess(ctx context.Context, sourceID int64) {
	if sourceID <= 0 {
		return
	}
	s.clearShortLivedRecoveryPenalty(sourceID)
	// Don't stage events after terminal cancellation — the persistence
	// worker is shutting down and won't process them, which would leave
	// orphaned overlays biasing source ordering.
	if s.ctx != nil && s.ctx.Err() != nil {
		return
	}
	observedAt := time.Now().UTC()
	eventID := uint64(0)
	if s.manager != nil {
		eventID = s.manager.stageRecentSourceSuccess(sourceID, observedAt)
	}
	s.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    eventID,
		success:    true,
		observedAt: observedAt,
	})
}

func (m *SessionManager) removeSession(channelID int64, session *sharedRuntimeSession) {
	if m == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.sessions[channelID]
	if !ok || existing != session {
		return
	}
	delete(m.sessions, channelID)
	m.syncDrainSignalLocked()
}

func (s *streamSessionReadCloser) Read(p []byte) (int, error) {
	if s == nil || s.session == nil || s.session.reader == nil {
		return 0, io.EOF
	}
	return s.session.reader.Read(p)
}

func (s *streamSessionReadCloser) Close() error {
	if s == nil {
		return nil
	}

	s.once.Do(func() {
		if s.session != nil {
			s.session.close()
		}
	})
	return nil
}

// writeChunk writes a single subscriber payload chunk and returns write-pressure
// telemetry for the attempt.
//
// When maxBlockedWrite is enabled and a response controller is available, the
// helper sets a per-write deadline and always clears it before returning so the
// deadline does not leak into later writes.
func writeChunk(
	w http.ResponseWriter,
	controller *http.ResponseController,
	data []byte,
	maxBlockedWrite time.Duration,
) (subscriberWritePressureSample, error) {
	sample := subscriberWritePressureSample{}
	if len(data) == 0 {
		return sample, nil
	}
	startedAt := time.Now()

	if maxBlockedWrite > 0 && controller != nil {
		if err := controller.SetWriteDeadline(time.Now().Add(maxBlockedWrite)); err != nil {
			sample.BlockedDuration = elapsedDurationUS(startedAt)
			return sample, fmt.Errorf("set response write deadline: %w", err)
		}
		defer func() {
			_ = controller.SetWriteDeadline(time.Time{})
		}()
	}

	n, err := w.Write(data)
	sample.BlockedDuration = elapsedDurationUS(startedAt)
	if err != nil {
		sample.DeadlineTimeout = isWriteDeadlineTimeout(err)
		return sample, err
	}
	if n != len(data) {
		sample.ShortWrite = true
		return sample, io.ErrShortWrite
	}
	return sample, nil
}

// elapsedDurationUS returns the elapsed time from startedAt in microseconds.
// It guards zero-value and future timestamps by returning zero.
func elapsedDurationUS(startedAt time.Time) uint64 {
	if startedAt.IsZero() {
		return 0
	}
	elapsedUS := time.Since(startedAt).Microseconds()
	if elapsedUS < 0 {
		return 0
	}
	return uint64(elapsedUS)
}

// isWriteDeadlineTimeout reports whether err represents a write deadline
// timeout using three tiers: nil guard, deadline sentinels, then net.Error.
func isWriteDeadlineTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func normalizeSessionManagerConfig(cfg SessionManagerConfig) sessionManagerConfig {
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode == "" {
		mode = "ffmpeg-copy"
	}

	ffmpegPath := strings.TrimSpace(cfg.FFmpegPath)
	if ffmpegPath == "" {
		ffmpegPath = "ffmpeg"
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				MaxIdleConns:          64,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 15 * time.Second,
				DisableCompression:    true,
			},
		}
	}

	startupWait := cfg.StartupTimeout
	if startupWait <= 0 {
		startupWait = defaultStartupTimeout
	}

	minProbe := cfg.MinProbeBytes
	if minProbe <= 0 {
		minProbe = defaultMinProbeBytes
	}

	maxFailover := cfg.MaxFailovers
	if maxFailover < 0 {
		maxFailover = 0
	}

	failoverTTL := cfg.FailoverTotalTimeout
	if failoverTTL <= 0 {
		failoverTTL = defaultFailoverTotalTimeout
	}

	producerReadRate := cfg.ProducerReadRate
	if producerReadRate <= 0 {
		producerReadRate = defaultProducerReadRate
	}
	producerReadRateCatchup := cfg.ProducerReadRateCatchup
	if producerReadRateCatchup <= 0 {
		producerReadRateCatchup = producerReadRate
	}
	if producerReadRateCatchup < producerReadRate {
		producerReadRateCatchup = producerReadRate
	}

	producerInitialBurst := cfg.ProducerInitialBurst
	if producerInitialBurst <= 0 {
		producerInitialBurst = defaultProducerInitialBurstSeconds
	}

	bufferChunkBytes := cfg.BufferChunkBytes
	if bufferChunkBytes <= 0 {
		bufferChunkBytes = defaultPumpChunkBytes
	}

	bufferFlushPeriod := cfg.BufferPublishFlushInterval
	if bufferFlushPeriod <= 0 {
		bufferFlushPeriod = defaultPumpFlushInterval
	}
	bufferTSAlign188 := cfg.BufferTSAlign188

	stallDetect := cfg.StallDetect
	if stallDetect <= 0 {
		stallDetect = defaultSharedStallDetect
	}

	stallHardDeadline := cfg.StallHardDeadline
	if stallHardDeadline <= 0 {
		stallHardDeadline = defaultSharedStallHardDeadline
	}

	stallPolicy := strings.ToLower(strings.TrimSpace(cfg.StallPolicy))
	switch stallPolicy {
	case "":
		stallPolicy = defaultSharedStallPolicy
	case stallPolicyFailoverSource, stallPolicyRestartSame, stallPolicyCloseSession:
	default:
		stallPolicy = defaultSharedStallPolicy
	}

	stallMaxFailovers := cfg.StallMaxFailoversPerStall
	if stallMaxFailovers < 0 {
		stallMaxFailovers = defaultSharedStallMaxFailoversPerTry
	}
	cycleFailureMinHealth := cfg.CycleFailureMinHealth
	if cycleFailureMinHealth < 0 {
		cycleFailureMinHealth = defaultSharedCycleFailureMinHealth
	}

	upstreamOverlimitCooldown := cfg.UpstreamOverlimitCooldown
	if upstreamOverlimitCooldown < 0 {
		upstreamOverlimitCooldown = 0
	}
	ffmpegReconnectEnabled, ffmpegReconnectDelayMax, ffmpegReconnectMaxRetries, ffmpegReconnectHTTPErrors := normalizeFFmpegReconnectSettings(
		mode,
		cfg.FFmpegReconnectEnabled,
		cfg.FFmpegReconnectDelayMax,
		cfg.FFmpegReconnectMaxRetries,
		cfg.FFmpegReconnectHTTPErrors,
	)
	ffmpegStartupProbeSize, ffmpegStartupAnalyzeDelay := normalizeFFmpegStartupDetection(
		cfg.FFmpegStartupProbeSize,
		cfg.FFmpegStartupAnalyzeDelay,
	)
	ffmpegInputBufferSize := cfg.FFmpegInputBufferSize
	if ffmpegInputBufferSize < 0 {
		ffmpegInputBufferSize = 0
	}
	ffmpegCopyRegenerateTimestamps := resolveFFmpegCopyRegenerateTimestamps(
		mode,
		cfg.FFmpegCopyRegenerateTimestamps,
	)

	recoveryFillerEnabled := cfg.RecoveryFillerEnabled
	recoveryFillerInterval := cfg.RecoveryFillerInterval
	if recoveryFillerInterval <= 0 {
		recoveryFillerInterval = defaultSharedRecoveryFillerInterval
	}
	recoveryFillerMode := strings.ToLower(strings.TrimSpace(cfg.RecoveryFillerMode))
	switch recoveryFillerMode {
	case "":
		recoveryFillerMode = defaultSharedRecoveryFillerMode
	case recoveryFillerModeNull, recoveryFillerModePSI:
	case recoveryFillerModeSlateAV, "slate-av":
		recoveryFillerMode = recoveryFillerModeSlateAV
	default:
		recoveryFillerMode = defaultSharedRecoveryFillerMode
	}
	recoveryTransitionMode := defaultSharedRecoveryTransitionMode
	recoveryFillerText := strings.TrimSpace(cfg.RecoveryFillerText)
	if recoveryFillerText == "" {
		recoveryFillerText = defaultRecoveryFillerText
	}

	joinLagBytes := cfg.SubscriberJoinLagBytes
	if joinLagBytes < 0 {
		joinLagBytes = 0
	}
	if joinLagBytes == 0 {
		joinLagBytes = defaultSharedSubscriberJoinLagBytes
	}

	slowPolicy := strings.ToLower(strings.TrimSpace(cfg.SubscriberSlowClientPolicy))
	switch slowPolicy {
	case "":
		slowPolicy = defaultSharedSlowClientPolicy
	case slowClientPolicyDisconnect, slowClientPolicySkip:
	default:
		slowPolicy = defaultSharedSlowClientPolicy
	}

	maxBlockedWrite := cfg.SubscriberMaxBlockedWrite
	if maxBlockedWrite < 0 {
		maxBlockedWrite = 0
	}
	if maxBlockedWrite == 0 {
		maxBlockedWrite = defaultSharedSubscriberMaxBlockedWrite
	}

	sessionIdleTimeout := cfg.SessionIdleTimeout
	if sessionIdleTimeout < 0 {
		sessionIdleTimeout = 0
	}
	if sessionIdleTimeout == 0 {
		sessionIdleTimeout = defaultSharedSessionIdleTimeout
	}

	sessionMaxSubscribers := cfg.SessionMaxSubscribers
	if sessionMaxSubscribers < 0 {
		sessionMaxSubscribers = 0
	}

	sessionHistoryLimit := cfg.SessionHistoryLimit
	if sessionHistoryLimit <= 0 {
		sessionHistoryLimit = defaultSharedSessionHistoryLimit
	}
	sessionSourceHistoryLimit := normalizeSharedSessionTimelineLimit(
		cfg.SessionSourceHistoryLimit,
		sessionHistoryLimit,
		defaultSharedSourceHistoryLimit,
		minSharedSourceHistoryLimit,
		maxSharedSourceHistoryLimit,
	)
	sessionSubscriberHistoryLimit := normalizeSharedSessionTimelineLimit(
		cfg.SessionSubscriberHistoryLimit,
		sessionHistoryLimit,
		defaultSharedSubscriberHistoryLimit,
		minSharedSubscriberHistoryLimit,
		maxSharedSubscriberHistoryLimit,
	)
	sessionDrainTimeout := cfg.SessionDrainTimeout
	if sessionDrainTimeout < 0 {
		sessionDrainTimeout = 0
	}
	if sessionDrainTimeout == 0 {
		sessionDrainTimeout = boundedCloseTimeout
	}

	sourceHealthDrainTimeout := cfg.SourceHealthDrainTimeout
	if sourceHealthDrainTimeout < 0 {
		sourceHealthDrainTimeout = 0
	}
	if sourceHealthDrainTimeout == 0 {
		sourceHealthDrainTimeout = defaultSourceHealthDrainTimeout
	}

	return sessionManagerConfig{
		mode:                            mode,
		ffmpegPath:                      ffmpegPath,
		httpClient:                      httpClient,
		logger:                          logger,
		startupWait:                     startupWait,
		startupRandomAccessRecoveryOnly: cfg.StartupRandomAccessRecoveryOnly,
		minProbe:                        minProbe,
		maxFailover:                     maxFailover,
		failoverTTL:                     failoverTTL,
		upstreamOverlimitCooldown:       upstreamOverlimitCooldown,
		ffmpegReconnectEnabled:          ffmpegReconnectEnabled,
		ffmpegReconnectDelayMax:         ffmpegReconnectDelayMax,
		ffmpegReconnectMaxRetries:       ffmpegReconnectMaxRetries,
		ffmpegReconnectHTTPErrors:       ffmpegReconnectHTTPErrors,
		ffmpegStartupProbeSize:          ffmpegStartupProbeSize,
		ffmpegStartupAnalyzeDelay:       ffmpegStartupAnalyzeDelay,
		ffmpegInputBufferSize:           ffmpegInputBufferSize,
		ffmpegDiscardCorrupt:            cfg.FFmpegDiscardCorrupt,
		ffmpegCopyRegenerateTimestamps:  ffmpegCopyRegenerateTimestamps,
		producerReadRate:                producerReadRate,
		producerReadRateCatchup:         producerReadRateCatchup,
		producerInitialBurst:            producerInitialBurst,
		bufferChunkBytes:                bufferChunkBytes,
		bufferFlushPeriod:               bufferFlushPeriod,
		bufferTSAlign188:                bufferTSAlign188,
		stallDetect:                     stallDetect,
		stallHardDeadline:               stallHardDeadline,
		stallPolicy:                     stallPolicy,
		stallMaxFailoversPerTry:         stallMaxFailovers,
		cycleFailureMinHealth:           cycleFailureMinHealth,
		recoveryFillerEnabled:           recoveryFillerEnabled,
		recoveryFillerMode:              recoveryFillerMode,
		recoveryFillerInterval:          recoveryFillerInterval,
		recoveryTransitionMode:          recoveryTransitionMode,
		recoveryFillerText:              recoveryFillerText,
		recoveryFillerEnableAudio:       cfg.RecoveryFillerEnableAudio,
		recoverySlateAVFactory:          defaultSlateAVProducerFactory,
		subscriberJoinLagBytes:          joinLagBytes,
		subscriberSlowPolicy:            slowPolicy,
		subscriberMaxBlockedWrite:       maxBlockedWrite,
		sessionIdleTimeout:              sessionIdleTimeout,
		sessionMaxSubscribers:           sessionMaxSubscribers,
		sessionHistoryLimit:             sessionHistoryLimit,
		sessionSourceHistoryLimit:       sessionSourceHistoryLimit,
		sessionSubscriberHistoryLimit:   sessionSubscriberHistoryLimit,
		sessionDrainTimeout:             sessionDrainTimeout,
		sourceHealthDrainTimeout:        sourceHealthDrainTimeout,
	}
}

func normalizeSharedSessionTimelineLimit(
	configured int,
	fallback int,
	defaultLimit int,
	minLimit int,
	maxLimit int,
) int {
	limit := configured
	if limit <= 0 {
		limit = fallback
	}
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit < minLimit {
		limit = minLimit
	}
	if maxLimit > 0 && limit > maxLimit {
		limit = maxLimit
	}
	return limit
}

func ringCapacityForLag(chunkBytes, joinLagBytes int) int {
	if chunkBytes <= 0 {
		chunkBytes = defaultPumpChunkBytes
	}
	if joinLagBytes <= 0 {
		return defaultChunkRingCapacity
	}

	capacity := chunksForBytes(joinLagBytes, chunkBytes) + sharedRingLagChunkCushion

	minChunkBytes := chunkBytes
	if minChunkBytes > mpegTSPacketSize {
		minChunkBytes = mpegTSPacketSize
	}
	if minChunkBytes < 1 {
		minChunkBytes = 1
	}

	byteBudget := ringByteBudgetForLag(chunkBytes, joinLagBytes)
	if chunksForBudget := chunksForBytes(byteBudget, minChunkBytes); chunksForBudget > capacity {
		capacity = chunksForBudget
	}

	if capacity < sharedRingMinChunkCapacity {
		capacity = sharedRingMinChunkCapacity
	}
	if capacity > sharedRingMaxChunkCapacity {
		capacity = sharedRingMaxChunkCapacity
	}
	return capacity
}

func ringByteBudgetForLag(chunkBytes, joinLagBytes int) int {
	if joinLagBytes <= 0 {
		return 0
	}
	if chunkBytes <= 0 {
		chunkBytes = defaultPumpChunkBytes
	}

	maxInt := int64(^uint(0) >> 1)
	lagBytes := int64(joinLagBytes)
	cushionBytes := int64(chunkBytes) * sharedRingLagChunkCushion
	budget := lagBytes + cushionBytes
	if budget < lagBytes {
		return int(maxInt)
	}
	if budget > maxInt {
		return int(maxInt)
	}
	return int(budget)
}

func chunksForBytes(totalBytes, chunkBytes int) int {
	if totalBytes <= 0 {
		return 0
	}
	if chunkBytes <= 0 {
		chunkBytes = 1
	}

	chunks := totalBytes / chunkBytes
	if totalBytes%chunkBytes != 0 {
		chunks++
	}
	return chunks
}

func orderFailoverRecoveryCandidates(candidates []channels.Source, currentSourceID int64, now time.Time) []channels.Source {
	if len(candidates) == 0 {
		return nil
	}

	ordered := append([]channels.Source(nil), candidates...)
	if currentSourceID <= 0 {
		return ordered
	}

	nowUnix := now.UTC().Unix()
	ready := make([]channels.Source, 0, len(candidates))
	cooling := make([]channels.Source, 0, len(candidates))
	var current *channels.Source
	currentCooling := false

	for i := range candidates {
		source := candidates[i]
		if source.SourceID == currentSourceID {
			copied := source
			current = &copied
			currentCooling = source.CooldownUntil > nowUnix
			continue
		}
		if source.CooldownUntil > nowUnix {
			cooling = append(cooling, source)
			continue
		}
		ready = append(ready, source)
	}

	// If current source is not present, preserve caller ordering.
	if current == nil {
		return ordered
	}

	out := make([]channels.Source, 0, len(candidates))
	if len(ready) > 0 {
		// Try one alternate first, then quickly retry the current source before
		// exploring additional or cooling candidates.
		out = append(out, ready[0])
		if !currentCooling {
			out = append(out, *current)
		}
		out = append(out, ready[1:]...)
		if currentCooling {
			cooling = append([]channels.Source{*current}, cooling...)
		}
	} else {
		// No ready alternates remain; retry current even if cooling to avoid deadlock.
		out = append(out, *current)
	}
	out = append(out, cooling...)
	return out
}

func mpegTSWithDiscontinuityIndicator(chunk []byte) []byte {
	if len(chunk) == 0 || len(chunk)%mpegTSPacketSize != 0 {
		return chunk
	}

	out := append([]byte(nil), chunk...)
	for offset := 0; offset < len(out); offset += mpegTSPacketSize {
		packet := out[offset : offset+mpegTSPacketSize]
		if packet[0] != 0x47 {
			return chunk
		}

		adaptationControl := (packet[3] >> 4) & 0x03
		switch adaptationControl {
		case 0x01: // payload only; add a one-byte adaptation field with discontinuity flag
			payload := append([]byte(nil), packet[4:]...)
			packet[3] = (packet[3] & 0xCF) | 0x30
			packet[4] = 0x01
			packet[5] = 0x80
			maxPayload := mpegTSPacketSize - 6
			if len(payload) > maxPayload {
				payload = payload[:maxPayload]
			}
			copy(packet[6:], payload)
			if end := 6 + len(payload); end < mpegTSPacketSize {
				copy(packet[end:], bytesRepeat(0xFF, mpegTSPacketSize-end))
			}
		case 0x02, 0x03: // adaptation present
			adaptationLength := int(packet[4])
			if adaptationLength < 1 || adaptationLength > (mpegTSPacketSize-5) {
				return chunk
			}
			packet[5] |= 0x80
		default:
			return chunk
		}
	}
	return out
}

const (
	mpegTSPCRClockHz  = 90000
	mpegTSPCRBaseMask = (1 << 33) - 1
	// Keep ffmpeg timeline rebasing bounded well below the 33-bit PCR wrap
	// window (~26.5h) to avoid pathological near-wrap offsets.
	maxRecoveryTimelinePTSOffset = 12 * time.Hour
)

func clampRecoveryTimelinePTSOffset(offset time.Duration) time.Duration {
	if offset <= 0 {
		return 0
	}
	if offset > maxRecoveryTimelinePTSOffset {
		return maxRecoveryTimelinePTSOffset
	}
	return offset
}

func pcrBaseToDuration(base uint64) time.Duration {
	if base == 0 {
		return 0
	}
	seconds := base / mpegTSPCRClockHz
	remainder := base % mpegTSPCRClockHz
	return time.Duration(seconds)*time.Second +
		time.Duration(remainder)*time.Second/time.Duration(mpegTSPCRClockHz)
}

func advancePCRBase(base uint64, delta time.Duration) uint64 {
	if delta < 0 {
		delta = 0
	}
	ticks := uint64(delta.Nanoseconds() * mpegTSPCRClockHz / int64(time.Second))
	return (base + ticks) & mpegTSPCRBaseMask
}

func mpegTSLastPCRBase(chunk []byte) (uint64, bool) {
	if len(chunk) < mpegTSPacketSize {
		return 0, false
	}

	lastBase := uint64(0)
	found := false
	for offset := 0; offset+mpegTSPacketSize <= len(chunk); offset += mpegTSPacketSize {
		packet := chunk[offset : offset+mpegTSPacketSize]
		if packet[0] != 0x47 {
			continue
		}
		adaptationControl := (packet[3] >> 4) & 0x03
		if adaptationControl != 0x02 && adaptationControl != 0x03 {
			continue
		}
		adaptationLength := int(packet[4])
		if adaptationLength < 7 || adaptationLength > (mpegTSPacketSize-5) {
			continue
		}
		if packet[5]&0x10 == 0 {
			continue
		}
		base := uint64(packet[6])<<25 |
			uint64(packet[7])<<17 |
			uint64(packet[8])<<9 |
			uint64(packet[9])<<1 |
			uint64((packet[10]>>7)&0x01)
		lastBase = base & mpegTSPCRBaseMask
		found = true
	}
	return lastBase, found
}

func mpegTSNullPacketChunk(packetCount int) []byte {
	if packetCount < 1 {
		packetCount = 1
	}

	packet := []byte{0x47, 0x1F, 0xFF, 0x10}
	packet = append(packet, bytesRepeat(0xFF, mpegTSPacketSize-len(packet))...)

	chunk := make([]byte, 0, packetCount*mpegTSPacketSize)
	for i := 0; i < packetCount; i++ {
		chunk = append(chunk, packet...)
	}
	return chunk
}

const (
	heartbeatPATPID        uint16 = 0x0000
	heartbeatPMTPID        uint16 = 0x1000
	heartbeatPCRPID        uint16 = 0x0102
	heartbeatVideoPID      uint16 = 0x0100
	heartbeatProgramNumber uint16 = 0x0001
)

func mpegTSPSIHeartbeatChunk(patContinuity, pmtContinuity byte) []byte {
	return mpegTSPSIHeartbeatChunkWithVersions(patContinuity, pmtContinuity, 0, 0)
}

func mpegTSPSIHeartbeatChunkWithVersions(
	patContinuity byte,
	pmtContinuity byte,
	patVersion byte,
	pmtVersion byte,
) []byte {
	patSection := mpegTSPATSectionWithVersion(heartbeatProgramNumber, heartbeatPMTPID, patVersion)
	pmtSection := mpegTSPMTSectionWithVersion(
		heartbeatProgramNumber,
		heartbeatPCRPID,
		heartbeatVideoPID,
		pmtVersion,
	)

	patPayload := make([]byte, 0, len(patSection)+1)
	patPayload = append(patPayload, 0x00) // pointer_field for payload_unit_start
	patPayload = append(patPayload, patSection...)

	pmtPayload := make([]byte, 0, len(pmtSection)+1)
	pmtPayload = append(pmtPayload, 0x00) // pointer_field for payload_unit_start
	pmtPayload = append(pmtPayload, pmtSection...)

	patPacket := mpegTSPayloadPacket(heartbeatPATPID, patContinuity, patPayload, true)
	pmtPacket := mpegTSPayloadPacket(heartbeatPMTPID, pmtContinuity, pmtPayload, true)

	chunk := make([]byte, 0, 2*mpegTSPacketSize)
	chunk = append(chunk, patPacket...)
	chunk = append(chunk, pmtPacket...)
	return chunk
}

func mpegTSRecoveryTransitionChunk(
	patContinuity byte,
	pmtContinuity byte,
	pcrContinuity byte,
	patVersion byte,
	pmtVersion byte,
	pcrBase uint64,
) []byte {
	psiChunk := mpegTSPSIHeartbeatChunkWithVersions(
		patContinuity,
		pmtContinuity,
		patVersion,
		pmtVersion,
	)
	pcrPacket := mpegTSPCRPacket(heartbeatPCRPID, pcrContinuity, pcrBase)
	if len(psiChunk) == 0 || len(pcrPacket) != mpegTSPacketSize {
		return nil
	}

	chunk := make([]byte, 0, len(psiChunk)+mpegTSPacketSize)
	chunk = append(chunk, psiChunk...)
	chunk = append(chunk, pcrPacket...)
	return mpegTSWithDiscontinuityIndicator(chunk)
}

func mpegTSPayloadPacket(pid uint16, continuity byte, payload []byte, payloadUnitStart bool) []byte {
	packet := bytesRepeat(0xFF, mpegTSPacketSize)
	packet[0] = 0x47
	packet[1] = byte((pid >> 8) & 0x1F)
	if payloadUnitStart {
		packet[1] |= 0x40
	}
	packet[2] = byte(pid & 0xFF)
	packet[3] = 0x10 | (continuity & 0x0F) // payload only

	maxPayload := mpegTSPacketSize - 4
	if len(payload) > maxPayload {
		payload = payload[:maxPayload]
	}
	copy(packet[4:], payload)
	return packet
}

func mpegTSPCRPacket(pid uint16, continuity byte, pcrBase uint64) []byte {
	packet := bytesRepeat(0xFF, mpegTSPacketSize)
	packet[0] = 0x47
	packet[1] = byte((pid >> 8) & 0x1F)
	packet[2] = byte(pid & 0xFF)
	packet[3] = 0x20 | (continuity & 0x0F) // adaptation only

	packet[4] = mpegTSPacketSize - 5 // adaptation field length
	packet[5] = 0x10                 // PCR flag set

	pcrBase &= mpegTSPCRBaseMask
	packet[6] = byte((pcrBase >> 25) & 0xFF)
	packet[7] = byte((pcrBase >> 17) & 0xFF)
	packet[8] = byte((pcrBase >> 9) & 0xFF)
	packet[9] = byte((pcrBase >> 1) & 0xFF)
	packet[10] = byte(((pcrBase & 0x1) << 7) | 0x7E)
	packet[11] = 0x00 // PCR extension
	return packet
}

func mpegTSPATSection(programNumber uint16, pmtPID uint16) []byte {
	return mpegTSPATSectionWithVersion(programNumber, pmtPID, 0)
}

func mpegTSPATSectionWithVersion(programNumber uint16, pmtPID uint16, version byte) []byte {
	version &= 0x1F
	sectionLength := 13 // one program + CRC
	section := []byte{
		0x00, // table_id (PAT)
		0xB0 | byte((sectionLength>>8)&0x0F),
		byte(sectionLength),
		0x00, 0x01, // transport_stream_id
		0xC1 | (version << 1), // version + current_next=1
		0x00,                  // section_number
		0x00,                  // last_section_number
		byte((programNumber >> 8) & 0xFF),
		byte(programNumber & 0xFF),
		0xE0 | byte((pmtPID>>8)&0x1F),
		byte(pmtPID & 0xFF),
	}
	crc := mpegTSCRC32(section)
	return append(section, byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc))
}

func mpegTSPMTSection(programNumber uint16, pcrPID uint16, videoPID uint16) []byte {
	return mpegTSPMTSectionWithVersion(programNumber, pcrPID, videoPID, 0)
}

func mpegTSPMTSectionWithVersion(programNumber uint16, pcrPID uint16, videoPID uint16, version byte) []byte {
	version &= 0x1F
	sectionLength := 18 // fixed PMT with one H.264 stream + CRC
	section := []byte{
		0x02, // table_id (PMT)
		0xB0 | byte((sectionLength>>8)&0x0F),
		byte(sectionLength),
		byte((programNumber >> 8) & 0xFF),
		byte(programNumber & 0xFF),
		0xC1 | (version << 1), // version + current_next=1
		0x00,                  // section_number
		0x00,                  // last_section_number
		0xE0 | byte((pcrPID>>8)&0x1F),
		byte(pcrPID & 0xFF),
		0xF0, 0x00, // program_info_length = 0
		0x1B, // stream_type H.264 video
		0xE0 | byte((videoPID>>8)&0x1F),
		byte(videoPID & 0xFF),
		0xF0, 0x00, // ES_info_length = 0
	}
	crc := mpegTSCRC32(section)
	return append(section, byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc))
}

func mpegTSCRC32(data []byte) uint32 {
	crc := uint32(0xFFFFFFFF)
	for _, b := range data {
		crc ^= uint32(b) << 24
		for i := 0; i < 8; i++ {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04C11DB7
				continue
			}
			crc <<= 1
		}
	}
	return crc
}

func bytesRepeat(b byte, count int) []byte {
	if count <= 0 {
		return nil
	}
	out := make([]byte, count)
	for i := range out {
		out[i] = b
	}
	return out
}
