package stream

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

const backgroundProberCloseQueueDepth = 8

var (
	probeCloseInlineCount    uint64
	probeCloseQueueFullCount uint64
)

type probeCloseStats struct {
	InlineCount    uint64
	QueueFullCount uint64
}

func probeCloseStatsSnapshot() probeCloseStats {
	return probeCloseStats{
		InlineCount:    atomic.LoadUint64(&probeCloseInlineCount),
		QueueFullCount: atomic.LoadUint64(&probeCloseQueueFullCount),
	}
}

// ProbeChannelsProvider supplies channel/source operations required by the background prober.
type ProbeChannelsProvider interface {
	ListEnabled(ctx context.Context) ([]channels.Channel, error)
	ListSources(ctx context.Context, channelID int64, enabledOnly bool) ([]channels.Source, error)
	MarkSourceFailure(ctx context.Context, sourceID int64, reason string, failedAt time.Time) error
	MarkSourceSuccess(ctx context.Context, sourceID int64, succeededAt time.Time) error
}

// ProbeTunerUsage exposes probe-slot acquisition for background probing.
type ProbeTunerUsage interface {
	AcquireProbe(ctx context.Context, label string, cancel context.CancelCauseFunc) (*Lease, error)
}

// ProberConfig controls optional background source probing.
type ProberConfig struct {
	Mode                           string
	FFmpegPath                     string
	HTTPClient                     *http.Client
	Logger                         *slog.Logger
	ProducerReadRate               float64
	ProducerInitialBurst           int
	FFmpegReconnectEnabled         bool
	FFmpegReconnectDelayMax        time.Duration
	FFmpegReconnectMaxRetries      int
	FFmpegReconnectHTTPErrors      string
	FFmpegStartupProbeSize         int
	FFmpegStartupAnalyzeDelay      time.Duration
	FFmpegCopyRegenerateTimestamps *bool
	MinProbeBytes                  int
	ProbeInterval                  time.Duration
	ProbeTimeout                   time.Duration
	ProbeCloseQueueDepth           int
	TunerUsage                     ProbeTunerUsage
	ProbeTuneDelay                 time.Duration
}

// BackgroundProber periodically checks source startup readiness to pre-populate health state.
type BackgroundProber struct {
	provider ProbeChannelsProvider

	logger                         *slog.Logger
	httpClient                     *http.Client
	mode                           string
	ffmpegPath                     string
	readRate                       float64
	initialBurst                   int
	ffmpegReconnectEnabled         bool
	ffmpegReconnectDelayMax        time.Duration
	ffmpegReconnectMaxRetries      int
	ffmpegReconnectHTTPErrors      string
	ffmpegStartupProbeSize         int
	ffmpegStartupAnalyzeDelay      time.Duration
	ffmpegCopyRegenerateTimestamps bool
	minProbe                       int
	interval                       time.Duration
	probeTimeout                   time.Duration
	tunerUsage                     ProbeTunerUsage
	probeDelay                     time.Duration
	probeCloseSessionMu            sync.RWMutex
	probeCloseSessionCh            chan *streamSession
	probeCloseWorkerDone           chan struct{}
	probeCloseOnce                 sync.Once
	probeCloseQueueDepth           int
}

func NewBackgroundProber(cfg ProberConfig, provider ProbeChannelsProvider) *BackgroundProber {
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
				MaxIdleConns:          32,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 15 * time.Second,
				DisableCompression:    true,
			},
		}
	}

	minProbe := cfg.MinProbeBytes
	if minProbe < 1 {
		minProbe = defaultMinProbeBytes
	}

	readRate := cfg.ProducerReadRate
	if readRate <= 0 {
		readRate = defaultProducerReadRate
	}

	initialBurst := cfg.ProducerInitialBurst
	if initialBurst <= 0 {
		initialBurst = defaultProducerInitialBurstSeconds
	}
	ffmpegReconnectEnabled, ffmpegReconnectDelayMax, ffmpegReconnectMaxRetries, ffmpegReconnectHTTPErrors := normalizeFFmpegReconnectSettings(
		mode,
		cfg.FFmpegReconnectEnabled,
		cfg.FFmpegReconnectDelayMax,
		cfg.FFmpegReconnectMaxRetries,
		cfg.FFmpegReconnectHTTPErrors,
	)
	ffmpegStartupProbeSize := cfg.FFmpegStartupProbeSize
	if ffmpegStartupProbeSize <= 0 {
		ffmpegStartupProbeSize = defaultFFmpegStartupProbeSize
	}
	ffmpegStartupAnalyzeDelay := cfg.FFmpegStartupAnalyzeDelay
	if ffmpegStartupAnalyzeDelay <= 0 {
		ffmpegStartupAnalyzeDelay = defaultFFmpegStartupAnalyzeDelay
	}
	ffmpegCopyRegenerateTimestamps := resolveFFmpegCopyRegenerateTimestamps(
		mode,
		cfg.FFmpegCopyRegenerateTimestamps,
	)

	probeTimeout := cfg.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = 3 * time.Second
	}

	probeDelay := cfg.ProbeTuneDelay
	if probeDelay < 0 {
		probeDelay = 0
	}

	probeCloseQueueDepth := cfg.ProbeCloseQueueDepth
	if probeCloseQueueDepth <= 0 {
		probeCloseQueueDepth = backgroundProberCloseQueueDepth
	}
	probeCloseSessionCh := make(chan *streamSession, probeCloseQueueDepth)

	prober := &BackgroundProber{
		provider:                       provider,
		logger:                         logger,
		httpClient:                     httpClient,
		mode:                           mode,
		ffmpegPath:                     ffmpegPath,
		readRate:                       readRate,
		initialBurst:                   initialBurst,
		ffmpegReconnectEnabled:         ffmpegReconnectEnabled,
		ffmpegReconnectDelayMax:        ffmpegReconnectDelayMax,
		ffmpegReconnectMaxRetries:      ffmpegReconnectMaxRetries,
		ffmpegReconnectHTTPErrors:      ffmpegReconnectHTTPErrors,
		ffmpegStartupProbeSize:         ffmpegStartupProbeSize,
		ffmpegStartupAnalyzeDelay:      ffmpegStartupAnalyzeDelay,
		ffmpegCopyRegenerateTimestamps: ffmpegCopyRegenerateTimestamps,
		minProbe:                       minProbe,
		interval:                       cfg.ProbeInterval,
		probeTimeout:                   probeTimeout,
		tunerUsage:                     cfg.TunerUsage,
		probeDelay:                     probeDelay,
		probeCloseSessionCh:            probeCloseSessionCh,
		probeCloseWorkerDone:           make(chan struct{}),
		probeCloseQueueDepth:           probeCloseQueueDepth,
	}

	go prober.runSessionCloseWorker(probeCloseSessionCh)

	return prober
}

func (p *BackgroundProber) Enabled() bool {
	return p != nil && p.provider != nil && p.interval > 0
}

// Run executes periodic probe ticks until ctx is canceled.
// Caller-owned lifecycle: Run does not call Close; the caller must invoke
// Close() during shutdown to drain queued session closes.
func (p *BackgroundProber) Run(ctx context.Context) {
	if !p.Enabled() {
		return
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.ProbeOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				p.logger.Warn("background source probe tick failed", "error", err)
			}
		}
	}
}

// Close stops the background session-close worker.
func (p *BackgroundProber) Close() {
	if p == nil {
		return
	}
	p.probeCloseOnce.Do(func() {
		p.probeCloseSessionMu.Lock()
		ch := p.probeCloseSessionCh
		p.probeCloseSessionCh = nil
		p.probeCloseSessionMu.Unlock()
		if ch != nil {
			close(ch)
		}
	})
	if p.probeCloseWorkerDone != nil {
		<-p.probeCloseWorkerDone
	}
}

func (p *BackgroundProber) ProbeOnce(ctx context.Context) error {
	if p == nil || p.provider == nil {
		return nil
	}

	chList, err := p.provider.ListEnabled(ctx)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	for _, channel := range chList {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		sources, err := p.provider.ListSources(ctx, channel.ChannelID, true)
		if err != nil {
			p.logger.Warn(
				"background probe skipped channel due to source list error",
				"channel_id", channel.ChannelID,
				"guide_number", channel.GuideNumber,
				"error", err,
			)
			continue
		}
		source, ok := selectProbeSource(sources, now)
		if !ok {
			continue
		}

		streamURL := strings.TrimSpace(source.StreamURL)
		if streamURL == "" {
			p.recordFailure(ctx, source.SourceID, &sourceStartupError{
				reason: "source stream URL is empty",
				err:    errors.New("source stream URL is empty"),
			})
			continue
		}

		probeCtx := ctx
		var (
			probeCancel context.CancelCauseFunc
			probeLease  *Lease
		)
		acquiredProbeLease := false
		if p.tunerUsage != nil {
			probeCtx, probeCancel = context.WithCancelCause(ctx)
			var acquireErr error
			probeLease, acquireErr = p.tunerUsage.AcquireProbe(probeCtx, strings.TrimSpace(source.ItemKey), probeCancel)
			if acquireErr != nil {
				probeCancel(nil)
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if errors.Is(acquireErr, ErrNoTunersAvailable) {
					p.logger.Debug(
						"background probe skipped source due to probe tuner availability",
						"source_id", source.SourceID,
						"item_key", source.ItemKey,
					)
					continue
				}
				p.logger.Warn(
					"background probe skipped source due to probe lease error",
					"source_id", source.SourceID,
					"item_key", source.ItemKey,
					"error", acquireErr,
				)
				continue
			}
			acquiredProbeLease = true
		}

		probeCtx, cancel := context.WithTimeout(probeCtx, p.probeTimeout)
		session, err := startSourceSessionWithContextsConfigured(
			probeCtx,
			probeCtx,
			p.mode,
			p.httpClient,
			p.ffmpegPath,
			streamURL,
			p.probeTimeout,
			p.minProbe,
			p.readRate,
			p.initialBurst,
			p.ffmpegStartupProbeSize,
			p.ffmpegStartupAnalyzeDelay,
			p.ffmpegReconnectEnabled,
			p.ffmpegReconnectDelayMax,
			p.ffmpegReconnectMaxRetries,
			p.ffmpegReconnectHTTPErrors,
			p.ffmpegCopyRegenerateTimestamps,
			0,
			false,
		)
		if err != nil {
			cancel()
			if probeLease != nil {
				probeLease.Release()
			}
			if probeCancel != nil {
				probeCancel(nil)
			}
			if errors.Is(context.Cause(probeCtx), ErrProbePreempted) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.recordFailure(ctx, source.SourceID, err)
			if acquiredProbeLease {
				if delayErr := waitForProbeTuneDelay(ctx, p.probeDelay); delayErr != nil {
					return delayErr
				}
			}
			continue
		}

		// Check whether the probe lifecycle was invalidated (timeout expired
		// during close) before we explicitly cancel, so stale success writes
		// do not clear newer failure/cooldown state.
		probeExpired := probeCtx.Err() != nil
		// Release probe resources (context, lease) BEFORE session close so a
		// blocked close cannot pin the probe tuner slot or delay lifecycle
		// convergence. Session close runs bounded in background.
		cancel()
		if probeLease != nil {
			probeLease.Release()
		}
		if probeCancel != nil {
			probeCancel(nil)
		}
		p.enqueueProbeSessionClose(source.SourceID, session)
		if !probeExpired {
			if err := p.provider.MarkSourceSuccess(ctx, source.SourceID, time.Now().UTC()); err != nil {
				p.logger.Warn(
					"background probe failed to mark source success",
					"source_id", source.SourceID,
					"error", err,
				)
			}
		}
		if acquiredProbeLease {
			if delayErr := waitForProbeTuneDelay(ctx, p.probeDelay); delayErr != nil {
				return delayErr
			}
		}
	}

	return nil
}

func (p *BackgroundProber) enqueueProbeSessionClose(sourceID int64, session *streamSession) {
	if p == nil || session == nil {
		return
	}

	p.probeCloseSessionMu.RLock()
	ch := p.probeCloseSessionCh
	if ch == nil {
		p.probeCloseSessionMu.RUnlock()
		// Keep fallback close inline so close dispatch remains bounded even when
		// shutdown has already stopped the close worker.
		atomic.AddUint64(&probeCloseInlineCount, 1)
		p.logger.Debug(
			"background prober close worker unavailable; closing probe session inline",
			"source_id", sourceID,
		)
		session.close()
		return
	}

	select {
	case ch <- session:
		p.probeCloseSessionMu.RUnlock()
	default:
		// Keep fallback close inline to avoid spawning detached goroutines when
		// the bounded close queue is saturated.
		atomic.AddUint64(&probeCloseInlineCount, 1)
		atomic.AddUint64(&probeCloseQueueFullCount, 1)
		queueDepth := len(ch)
		queueCap := cap(ch)
		p.probeCloseSessionMu.RUnlock()
		p.logger.Warn(
			"background prober close worker queue full; closing probe session inline",
			"source_id", sourceID,
			"queue_depth", queueDepth,
			"queue_cap", queueCap,
		)
		session.close()
	}
}

func (p *BackgroundProber) runSessionCloseWorker(ch <-chan *streamSession) {
	defer close(p.probeCloseWorkerDone)
	for session := range ch {
		if session != nil {
			session.close()
		}
	}
}

func (p *BackgroundProber) recordFailure(ctx context.Context, sourceID int64, err error) {
	reason := startupFailureReason(err)
	if markErr := p.provider.MarkSourceFailure(ctx, sourceID, reason, time.Now().UTC()); markErr != nil {
		p.logger.Warn(
			"background probe failed to mark source failure",
			"source_id", sourceID,
			"reason", reason,
			"error", markErr,
		)
	}
}

func selectProbeSource(sources []channels.Source, now time.Time) (channels.Source, bool) {
	if len(sources) == 0 {
		return channels.Source{}, false
	}

	ordered := orderSourcesByAvailability(sources, now)
	if len(ordered) == 0 {
		return channels.Source{}, false
	}

	// Skip probe when all sources are cooling down to avoid extending cooldown windows.
	if ordered[0].CooldownUntil > now.UTC().Unix() {
		return channels.Source{}, false
	}
	return ordered[0], true
}

func waitForProbeTuneDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
