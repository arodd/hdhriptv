package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

const (
	defaultStartupTimeout       = 6 * time.Second
	defaultMinProbeBytes        = 940
	defaultFailoverTotalTimeout = 32 * time.Second

	startupRecentFailureWindow = 20 * time.Minute
)

// ChannelsProvider supplies channel and source operations required by stream tuning.
type ChannelsProvider interface {
	GetByGuideNumber(ctx context.Context, guideNumber string) (channels.Channel, error)
	ListSources(ctx context.Context, channelID int64, enabledOnly bool) ([]channels.Source, error)
	MarkSourceFailure(ctx context.Context, sourceID int64, reason string, failedAt time.Time) error
	MarkSourceSuccess(ctx context.Context, sourceID int64, succeededAt time.Time) error
	UpdateSourceProfile(ctx context.Context, sourceID int64, profile channels.SourceProfileUpdate) error
}

// Config controls stream delivery behavior.
type Config struct {
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
	FFmpegCopyRegenerateTimestamps  *bool

	ProducerReadRate     float64
	ProducerInitialBurst int

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

	SubscriberJoinLagBytes     int
	SubscriberSlowClientPolicy string
	SubscriberMaxBlockedWrite  time.Duration

	SessionIdleTimeout    time.Duration
	SessionDrainTimeout   time.Duration
	SessionMaxSubscribers int
	SessionHistoryLimit   int
	// SessionSourceHistoryLimit and SessionSubscriberHistoryLimit are optional
	// per-session timeline overrides. When non-positive, SessionManager falls
	// back to SessionHistoryLimit/defaults and applies min/max clamp guardrails.
	SessionSourceHistoryLimit     int
	SessionSubscriberHistoryLimit int
	// SourceHealthDrainTimeout bounds post-cancel source-health queue draining
	// during session teardown. Non-positive values normalize to defaults in
	// SessionManager config.
	SourceHealthDrainTimeout time.Duration

	TuneBackoffMaxTunes int
	TuneBackoffInterval time.Duration
	TuneBackoffCooldown time.Duration
}

// Handler serves /auto/v{guideNumber} requests.
type Handler struct {
	channels    ChannelsProvider
	logger      *slog.Logger
	tuners      *Pool
	sessions    *SessionManager
	tuneBackoff *tuneBackoffGate
}

type tuneBackoffAdmissionError struct {
	decision tuneBackoffDecision
}

func (e *tuneBackoffAdmissionError) Error() string {
	return "channel tune backoff active"
}

// Close cancels all active sessions so they drain before store close.
func (h *Handler) Close() {
	_ = h.CloseWithContext(context.Background())
}

// CloseWithContext cancels all active sessions and waits for teardown within
// the provided context budget.
func (h *Handler) CloseWithContext(ctx context.Context) error {
	if h == nil || h.sessions == nil {
		return nil
	}
	return h.sessions.CloseWithContext(ctx)
}

// TriggerSessionRecovery requests an in-session recovery cycle for an active
// shared session on the provided channel.
// Recovery routing is resolved from in-memory shared-session state and does
// not require channel/source store lookups.
func (h *Handler) TriggerSessionRecovery(channelID int64, reason string) error {
	if h == nil || h.sessions == nil {
		return errors.New("stream handler is not configured")
	}
	return h.sessions.TriggerRecovery(channelID, reason)
}

// ClearSourceHealth clears source-health convergence state for a channel.
func (h *Handler) ClearSourceHealth(channelID int64) error {
	if h == nil || h.sessions == nil {
		return errors.New("stream handler is not configured")
	}
	return h.sessions.ClearSourceHealth(channelID)
}

// ClearAllSourceHealth clears all source-health convergence state.
func (h *Handler) ClearAllSourceHealth() error {
	if h == nil || h.sessions == nil {
		return errors.New("stream handler is not configured")
	}
	return h.sessions.ClearAllSourceHealth()
}

func NewHandler(cfg Config, tuners *Pool, channelsProvider ChannelsProvider) *Handler {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode == "" {
		mode = "ffmpeg-copy"
	}

	ffmpegPath := strings.TrimSpace(cfg.FFmpegPath)
	if ffmpegPath == "" {
		ffmpegPath = "ffmpeg"
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

	upstreamOverlimitCooldown := cfg.UpstreamOverlimitCooldown
	if upstreamOverlimitCooldown < 0 {
		upstreamOverlimitCooldown = 0
	}

	sessions := NewSessionManager(SessionManagerConfig{
		Mode:                            mode,
		FFmpegPath:                      ffmpegPath,
		HTTPClient:                      cfg.HTTPClient,
		Logger:                          logger,
		StartupTimeout:                  startupWait,
		StartupRandomAccessRecoveryOnly: cfg.StartupRandomAccessRecoveryOnly,
		MinProbeBytes:                   minProbe,
		MaxFailovers:                    maxFailover,
		FailoverTotalTimeout:            failoverTTL,
		UpstreamOverlimitCooldown:       upstreamOverlimitCooldown,
		FFmpegReconnectEnabled:          cfg.FFmpegReconnectEnabled,
		FFmpegReconnectDelayMax:         cfg.FFmpegReconnectDelayMax,
		FFmpegReconnectMaxRetries:       cfg.FFmpegReconnectMaxRetries,
		FFmpegReconnectHTTPErrors:       cfg.FFmpegReconnectHTTPErrors,
		FFmpegStartupProbeSize:          cfg.FFmpegStartupProbeSize,
		FFmpegStartupAnalyzeDelay:       cfg.FFmpegStartupAnalyzeDelay,
		FFmpegCopyRegenerateTimestamps:  cfg.FFmpegCopyRegenerateTimestamps,
		ProducerReadRate:                cfg.ProducerReadRate,
		ProducerInitialBurst:            cfg.ProducerInitialBurst,
		BufferChunkBytes:                cfg.BufferChunkBytes,
		BufferPublishFlushInterval:      cfg.BufferPublishFlushInterval,
		BufferTSAlign188:                cfg.BufferTSAlign188,
		StallDetect:                     cfg.StallDetect,
		StallHardDeadline:               cfg.StallHardDeadline,
		StallPolicy:                     cfg.StallPolicy,
		StallMaxFailoversPerStall:       cfg.StallMaxFailoversPerStall,
		CycleFailureMinHealth:           cfg.CycleFailureMinHealth,
		RecoveryFillerEnabled:           cfg.RecoveryFillerEnabled,
		RecoveryFillerMode:              cfg.RecoveryFillerMode,
		RecoveryFillerInterval:          cfg.RecoveryFillerInterval,
		RecoveryFillerText:              cfg.RecoveryFillerText,
		RecoveryFillerEnableAudio:       cfg.RecoveryFillerEnableAudio,
		SubscriberJoinLagBytes:          cfg.SubscriberJoinLagBytes,
		SubscriberSlowClientPolicy:      cfg.SubscriberSlowClientPolicy,
		SubscriberMaxBlockedWrite:       cfg.SubscriberMaxBlockedWrite,
		SessionIdleTimeout:              cfg.SessionIdleTimeout,
		SessionDrainTimeout:             cfg.SessionDrainTimeout,
		SessionMaxSubscribers:           cfg.SessionMaxSubscribers,
		SessionHistoryLimit:             cfg.SessionHistoryLimit,
		SessionSourceHistoryLimit:       cfg.SessionSourceHistoryLimit,
		SessionSubscriberHistoryLimit:   cfg.SessionSubscriberHistoryLimit,
		SourceHealthDrainTimeout:        cfg.SourceHealthDrainTimeout,
	}, tuners, channelsProvider)

	tuneBackoff := newTuneBackoffGate(
		cfg.TuneBackoffMaxTunes,
		cfg.TuneBackoffInterval,
		cfg.TuneBackoffCooldown,
	)

	return &Handler{
		channels:    channelsProvider,
		logger:      logger,
		tuners:      tuners,
		sessions:    sessions,
		tuneBackoff: tuneBackoff,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.channels == nil || h.sessions == nil {
		http.Error(w, "stream handler is not configured", http.StatusInternalServerError)
		return
	}

	clientAddr := strings.TrimSpace(r.RemoteAddr)
	guideNumber := normalizeGuideNumber(r)
	if guideNumber == "" {
		h.logger.Warn(
			"stream tune rejected",
			"guide_number", guideNumber,
			"client_addr", clientAddr,
			"method", r.Method,
			"path", r.URL.Path,
			"result", "rejected",
			"reason", "missing_guide_number",
		)
		http.Error(w, "guide number is required", http.StatusBadRequest)
		return
	}

	channel, err := h.channels.GetByGuideNumber(r.Context(), guideNumber)
	if err != nil {
		if errors.Is(err, channels.ErrChannelNotFound) {
			h.logger.Warn(
				"stream tune rejected",
				"guide_number", guideNumber,
				"client_addr", clientAddr,
				"method", r.Method,
				"path", r.URL.Path,
				"result", "rejected",
				"reason", "channel_not_found",
			)
			http.Error(w, "channel not found", http.StatusNotFound)
			return
		}
		h.logger.Error(
			"stream tune failed",
			"guide_number", guideNumber,
			"client_addr", clientAddr,
			"method", r.Method,
			"path", r.URL.Path,
			"result", "error",
			"reason", "channel_lookup_failed",
			"error", err,
		)
		http.Error(w, fmt.Sprintf("lookup channel: %v", err), http.StatusInternalServerError)
		return
	}
	if !channel.Enabled {
		h.logger.Warn(
			"stream tune rejected",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"guide_name", channel.GuideName,
			"client_addr", clientAddr,
			"method", r.Method,
			"path", r.URL.Path,
			"result", "rejected",
			"reason", "channel_disabled",
		)
		http.Error(w, "channel not found", http.StatusNotFound)
		return
	}

	startupAdmission := func(scopeKey int64) error {
		decision := h.tuneBackoff.allow(scopeKey, time.Now().UTC())
		if !decision.Allowed {
			return &tuneBackoffAdmissionError{decision: decision}
		}
		return nil
	}

	subscribeCtx := withSubscriberClientAddr(r.Context(), clientAddr)
	var subscription *SessionSubscription
	var startupLeader bool
	subscription, startupLeader, err = h.sessions.subscribe(
		subscribeCtx,
		channel,
		true,
		startupAdmission,
	)
	if err != nil {
		var admissionErr *tuneBackoffAdmissionError
		if errors.As(err, &admissionErr) {
			retryAfter := retryAfterHeaderValue(admissionErr.decision.RetryAfter)
			w.Header().Set("Retry-After", retryAfter)
			h.logger.Warn(
				"stream tune rejected",
				"channel_id", channel.ChannelID,
				"guide_number", channel.GuideNumber,
				"guide_name", channel.GuideName,
				"client_addr", clientAddr,
				"method", r.Method,
				"path", r.URL.Path,
				"result", "rejected",
				"reason", "channel_tune_backoff",
				"retry_after", retryAfter,
				"backoff_until", admissionErr.decision.BackoffUntil,
				"failures_in_window", admissionErr.decision.FailuresInWindow,
				"max_tunes", admissionErr.decision.Limit,
				"interval", admissionErr.decision.Interval.String(),
			)
			http.Error(w, "channel tune backoff active; retry later", http.StatusServiceUnavailable)
			return
		}
		if startupLeader && shouldTrackTuneBackoffFailure(err) {
			h.tuneBackoff.recordFailure(channel.ChannelID, time.Now().UTC())
		}
		reason := classifySubscribeFailure(err)
		h.logger.Warn(
			"stream tune rejected",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"guide_name", channel.GuideName,
			"client_addr", clientAddr,
			"method", r.Method,
			"path", r.URL.Path,
			"result", "rejected",
			"reason", reason,
			"error", err,
		)
		switch {
		case errors.Is(err, ErrNoTunersAvailable):
			http.Error(w, "all tuners are busy", http.StatusServiceUnavailable)
		case errors.Is(err, ErrSessionMaxSubscribers):
			http.Error(w, "channel session has reached max subscribers", http.StatusServiceUnavailable)
		case errors.Is(err, ErrSessionNoSources):
			http.Error(w, "channel has no enabled sources", http.StatusServiceUnavailable)
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			http.Error(w, "request canceled", http.StatusRequestTimeout)
		default:
			http.Error(w, "no sources available for channel", http.StatusServiceUnavailable)
		}
		return
	}
	if startupLeader {
		h.tuneBackoff.recordSuccess(channel.ChannelID, time.Now().UTC())
	}
	defer subscription.Close()

	subscriberID := subscription.subscriberID
	stats := subscription.Stats()
	started := time.Now().UTC()
	h.logger.Info(
		"stream subscriber started",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"guide_name", channel.GuideName,
		"source_id", stats.SourceID,
		"producer", stats.Producer,
		"tuner_id", stats.TunerID,
		"subscriber_id", subscriberID,
		"client_addr", clientAddr,
		"subscribers", stats.Subscribers,
	)

	w.Header().Set("Content-Type", "video/MP2T")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	err = subscription.Stream(r.Context(), w)
	duration := time.Since(started)

	if err == nil {
		h.logger.Info(
			"stream subscriber ended",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"source_id", stats.SourceID,
			"subscriber_id", subscriberID,
			"client_addr", clientAddr,
			"duration", duration.String(),
		)
		return
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		h.logger.Info(
			"stream subscriber canceled",
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"source_id", stats.SourceID,
			"subscriber_id", subscriberID,
			"client_addr", clientAddr,
			"duration", duration.String(),
		)
		return
	}

	if errors.Is(err, ErrSlowClientLagged) {
		fields := []any{
			"channel_id", channel.ChannelID,
			"guide_number", channel.GuideNumber,
			"source_id", stats.SourceID,
			"subscriber_id", subscriberID,
			"duration", duration.String(),
			"client_addr", clientAddr,
		}
		if lag, ok := slowClientLagDetailsFromError(err); ok {
			fields = append(
				fields,
				"requested_seq", lag.RequestedSeq,
				"oldest_seq", lag.OldestSeq,
				"lag_chunks", lag.LagChunks,
				"ring_next_seq", lag.RingNextSeq,
				"ring_buffered_chunks", lag.BufferedChunks,
				"ring_buffered_bytes", lag.BufferedBytes,
			)
		}
		h.logger.Warn("stream subscriber disconnected due to lag", fields...)
		return
	}

	h.logger.Warn(
		"stream subscriber ended with error",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"source_id", stats.SourceID,
		"subscriber_id", subscriberID,
		"client_addr", clientAddr,
		"duration", duration.String(),
		"error", err,
	)
}

func classifySubscribeFailure(err error) string {
	switch {
	case errors.Is(err, ErrNoTunersAvailable):
		return "no_tuners"
	case errors.Is(err, ErrSessionMaxSubscribers):
		return "max_subscribers"
	case errors.Is(err, ErrSessionNoSources):
		return "no_sources"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	default:
		return "other"
	}
}

func shouldTrackTuneBackoffFailure(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, ErrNoTunersAvailable):
		return false
	case errors.Is(err, ErrSessionMaxSubscribers):
		return false
	case errors.Is(err, ErrSessionNoSources):
		return false
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	default:
		return true
	}
}

func orderSourcesByAvailability(sources []channels.Source, now time.Time) []channels.Source {
	ready := make([]channels.Source, 0, len(sources))
	cooling := make([]channels.Source, 0, len(sources))
	nowUnix := now.UTC().Unix()

	for _, source := range sources {
		if source.CooldownUntil > nowUnix {
			cooling = append(cooling, source)
			continue
		}
		ready = append(ready, source)
	}

	if len(ready) == 0 {
		orderedCooling := append([]channels.Source(nil), cooling...)
		sortSourcesForStartup(orderedCooling, nowUnix)
		return orderedCooling
	}

	ordered := make([]channels.Source, 0, len(sources))
	sortSourcesForStartup(ready, nowUnix)
	sortSourcesForStartup(cooling, nowUnix)
	ordered = append(ordered, ready...)
	ordered = append(ordered, cooling...)
	return ordered
}

func sortSourcesForStartup(sources []channels.Source, nowUnix int64) {
	if len(sources) <= 1 {
		return
	}

	sort.SliceStable(sources, func(i, j int) bool {
		a := sources[i]
		b := sources[j]

		aRecentFail := hasRecentFailure(a, nowUnix)
		bRecentFail := hasRecentFailure(b, nowUnix)

		// Prefer sources that have not recently failed.
		if aRecentFail != bRecentFail {
			return !aRecentFail
		}
		// Prefer lower current failure counts.
		if a.FailCount != b.FailCount {
			return a.FailCount < b.FailCount
		}
		// If both recently failed, prefer the less-recent failure.
		if aRecentFail && bRecentFail && a.LastFailAt != b.LastFailAt {
			return a.LastFailAt < b.LastFailAt
		}
		// Prefer sources with more historical successes when health ties.
		if a.SuccessCount != b.SuccessCount {
			return a.SuccessCount > b.SuccessCount
		}
		// Preserve operator-defined source order as the final baseline.
		if a.PriorityIndex != b.PriorityIndex {
			return a.PriorityIndex < b.PriorityIndex
		}
		return a.SourceID < b.SourceID
	})
}

func hasRecentFailure(source channels.Source, nowUnix int64) bool {
	if source.LastFailAt <= 0 {
		return false
	}
	if source.LastFailAt >= nowUnix {
		return true
	}
	return nowUnix-source.LastFailAt <= int64(startupRecentFailureWindow.Seconds())
}

func limitSourcesByFailovers(sources []channels.Source, maxFailovers int) []channels.Source {
	if len(sources) == 0 {
		return nil
	}
	if maxFailovers <= 0 {
		return append([]channels.Source(nil), sources...)
	}

	maxAttempts := maxFailovers + 1
	if maxAttempts >= len(sources) {
		return append([]channels.Source(nil), sources...)
	}
	return append([]channels.Source(nil), sources[:maxAttempts]...)
}

func retryAfterHeaderValue(wait time.Duration) string {
	if wait <= 0 {
		return "1"
	}
	seconds := int(wait / time.Second)
	if wait%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	return strconv.Itoa(seconds)
}

func normalizeGuideNumber(r *http.Request) string {
	guideNumber := strings.TrimSpace(r.PathValue("guideNumber"))
	if guideNumber != "" {
		return guideNumber
	}

	segment := strings.TrimSpace(r.PathValue("guide"))
	if segment == "" {
		return ""
	}

	if len(segment) > 1 && (segment[0] == 'v' || segment[0] == 'V') {
		return strings.TrimSpace(segment[1:])
	}

	return segment
}
