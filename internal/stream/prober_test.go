package stream

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

type probeFailureCall struct {
	sourceID int64
	reason   string
}

type fakeProbeProvider struct {
	mu sync.Mutex

	enabledChannels []channels.Channel
	sourcesByID     map[int64][]channels.Source
	failures        []probeFailureCall
	successes       []int64
}

func newBackgroundProberForTest(t *testing.T, cfg ProberConfig, provider ProbeChannelsProvider) *BackgroundProber {
	t.Helper()

	prober := NewBackgroundProber(cfg, provider)
	t.Cleanup(func() {
		prober.Close()
	})
	return prober
}

func captureProbeCloseStatsForTest(t *testing.T) (probeCloseStats, func() probeCloseStats) {
	t.Helper()

	baseline := probeCloseStatsSnapshot()
	return baseline, func() probeCloseStats {
		current := probeCloseStatsSnapshot()
		return probeCloseStats{
			InlineCount:    saturatingCounterDelta(current.InlineCount, baseline.InlineCount),
			QueueFullCount: saturatingCounterDelta(current.QueueFullCount, baseline.QueueFullCount),
		}
	}
}

func saturatingCounterDelta(current, baseline uint64) uint64 {
	if current <= baseline {
		return 0
	}
	return current - baseline
}

func (p *fakeProbeProvider) ListEnabled(_ context.Context) ([]channels.Channel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]channels.Channel, 0, len(p.enabledChannels))
	out = append(out, p.enabledChannels...)
	return out, nil
}

func (p *fakeProbeProvider) ListSources(_ context.Context, channelID int64, enabledOnly bool) ([]channels.Source, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	list := p.sourcesByID[channelID]
	out := make([]channels.Source, 0, len(list))
	for _, source := range list {
		if enabledOnly && !source.Enabled {
			continue
		}
		out = append(out, source)
	}
	return out, nil
}

func (p *fakeProbeProvider) MarkSourceFailure(_ context.Context, sourceID int64, reason string, _ time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.failures = append(p.failures, probeFailureCall{
		sourceID: sourceID,
		reason:   reason,
	})
	return nil
}

func (p *fakeProbeProvider) MarkSourceSuccess(_ context.Context, sourceID int64, _ time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.successes = append(p.successes, sourceID)
	return nil
}

func TestBackgroundProberProbeOnceMarksSuccess(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("probe-bytes"))
	}))
	defer upstream.Close()

	provider := &fakeProbeProvider{
		enabledChannels: []channels.Channel{
			{ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:primary",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	prober := newBackgroundProberForTest(t, ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeTimeout:  2 * time.Second,
	}, provider)

	if err := prober.ProbeOnce(context.Background()); err != nil {
		t.Fatalf("ProbeOnce() error = %v", err)
	}

	if len(provider.successes) != 1 || provider.successes[0] != 10 {
		t.Fatalf("successes = %#v, want [10]", provider.successes)
	}
	if len(provider.failures) != 0 {
		t.Fatalf("failures = %#v, want none", provider.failures)
	}
}

func TestBackgroundProberSkipsAllCoolingSources(t *testing.T) {
	now := time.Now().UTC()
	provider := &fakeProbeProvider{
		enabledChannels: []channels.Channel{
			{ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:primary",
					StreamURL:     "http://example.com/stream.ts",
					PriorityIndex: 0,
					CooldownUntil: now.Add(1 * time.Minute).Unix(),
					Enabled:       true,
				},
			},
		},
	}

	prober := newBackgroundProberForTest(t, ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeTimeout:  1 * time.Second,
	}, provider)

	if err := prober.ProbeOnce(context.Background()); err != nil {
		t.Fatalf("ProbeOnce() error = %v", err)
	}
	if len(provider.successes) != 0 {
		t.Fatalf("successes = %#v, want none", provider.successes)
	}
	if len(provider.failures) != 0 {
		t.Fatalf("failures = %#v, want none", provider.failures)
	}
}

func TestBackgroundProberProbeOnceSkipsWhenNoProbeTunerSlot(t *testing.T) {
	var requestCount atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("probe-bytes"))
	}))
	defer upstream.Close()

	pool := NewPool(1)
	clientLease, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient() setup error = %v", err)
	}
	defer clientLease.Release()

	provider := &fakeProbeProvider{
		enabledChannels: []channels.Channel{
			{ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:primary",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	prober := newBackgroundProberForTest(t, ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeTimeout:  1 * time.Second,
		TunerUsage:    pool,
	}, provider)

	if err := prober.ProbeOnce(context.Background()); err != nil {
		t.Fatalf("ProbeOnce() error = %v", err)
	}
	if got := requestCount.Load(); got != 0 {
		t.Fatalf("upstream request count = %d, want 0", got)
	}
	if len(provider.successes) != 0 {
		t.Fatalf("successes = %#v, want none", provider.successes)
	}
	if len(provider.failures) != 0 {
		t.Fatalf("failures = %#v, want none", provider.failures)
	}
}

func TestBackgroundProberProbeOnceWaitsForGlobalRefillSettleGate(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("probe-bytes"))
	}))
	defer upstream.Close()

	pool := NewPool(2)
	pool.SetPreemptSettleDelay(220 * time.Millisecond)

	lease1, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient() lease1 setup error = %v", err)
	}
	defer lease1.Release()

	lease2, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient() lease2 setup error = %v", err)
	}
	lease2.Release() // full(2) -> partial(1), arms global refill settle window.

	provider := &fakeProbeProvider{
		enabledChannels: []channels.Channel{
			{ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:primary",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	prober := newBackgroundProberForTest(t, ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeTimeout:  1 * time.Second,
		TunerUsage:    pool,
	}, provider)

	started := time.Now()
	if err := prober.ProbeOnce(context.Background()); err != nil {
		t.Fatalf("ProbeOnce() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed < 170*time.Millisecond {
		t.Fatalf("ProbeOnce() completed in %s, want wait for global refill settle gate", elapsed)
	}
	if len(provider.successes) != 1 || provider.successes[0] != 10 {
		t.Fatalf("successes = %#v, want [10]", provider.successes)
	}
	if len(provider.failures) != 0 {
		t.Fatalf("failures = %#v, want none", provider.failures)
	}
}

// blockingCloseTransport wraps an http.RoundTripper and replaces the response
// body with one whose Close() blocks until the provided channel is closed.
type blockingCloseTransport struct {
	inner   http.RoundTripper
	blockCh chan struct{}
}

func (t *blockingCloseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.inner.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &blockingCloseBody{inner: resp.Body, blockCh: t.blockCh}
	return resp, nil
}

type blockingCloseBody struct {
	inner   io.ReadCloser
	blockCh chan struct{}
}

func (b *blockingCloseBody) Read(p []byte) (int, error) {
	return b.inner.Read(p)
}

func (b *blockingCloseBody) Close() error {
	<-b.blockCh
	return b.inner.Close()
}

func TestBackgroundProberSkipsStaleSuccessAfterProbeTimeout(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("probe-bytes-data"))
	}))
	defer upstream.Close()

	blockCh := make(chan struct{})
	closeProbeSession := sync.OnceFunc(func() {
		close(blockCh)
	})
	defer closeProbeSession()

	provider := &fakeProbeProvider{
		enabledChannels: []channels.Channel{
			{ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:primary",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	// Use a short probe timeout with a blocking close transport.
	// With bounded close, session.close() runs in background so the probe
	// returns promptly and the timeout does not expire during close.
	prober := newBackgroundProberForTest(t, ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeTimeout:  50 * time.Millisecond,
		HTTPClient: &http.Client{
			Transport: &blockingCloseTransport{
				inner:   http.DefaultTransport,
				blockCh: blockCh,
			},
		},
	}, provider)

	done := make(chan error, 1)
	go func() {
		done <- prober.ProbeOnce(context.Background())
	}()

	// ProbeOnce should return promptly because session close is non-blocking.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ProbeOnce() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ProbeOnce() did not complete")
	}

	// With bounded close, the probe returns before the timeout expires so
	// the startup success IS recorded (the probe itself succeeded within its
	// timeout). Previously, the blocking close caused the timeout to expire
	// which was a side effect of the bug; now close is non-blocking.
	if len(provider.successes) != 1 || provider.successes[0] != 10 {
		t.Fatalf("successes = %#v, want [10] (probe succeeded within timeout; close is now non-blocking)", provider.successes)
	}
}

// TestBackgroundProberProbeOnceBlocksOnSessionCloseAndPinsTunerLease verifies
// that ProbeOnce() returns promptly and releases the probe tuner lease even
// when session.close() blocks. The fix reorders teardown so probe cancellation
// and lease release happen BEFORE session close, and session close runs in
// background.
//
// Regression: TODO-stream-background-prober-session-close-blocking-tuner-lifecycle-stall.md
func TestBackgroundProberProbeOnceBlocksOnSessionCloseAndPinsTunerLease(t *testing.T) {
	var requestReceived atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestReceived.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("probe-bytes-data"))
	}))
	defer upstream.Close()

	blockCh := make(chan struct{})
	defer close(blockCh) // unblock close on test exit

	provider := &fakeProbeProvider{
		enabledChannels: []channels.Channel{
			{ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:blocking-close",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(2)

	prober := newBackgroundProberForTest(t, ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeTimeout:  5 * time.Second, // Long timeout so probe succeeds before it expires.
		TunerUsage:    pool,
		HTTPClient: &http.Client{
			Transport: &blockingCloseTransport{
				inner:   http.DefaultTransport,
				blockCh: blockCh,
			},
		},
	}, provider)

	probeDone := make(chan error, 1)
	go func() {
		probeDone <- prober.ProbeOnce(context.Background())
	}()

	// ProbeOnce should return promptly even though session close is blocked,
	// because lease release now happens before close and close runs in background.
	select {
	case err := <-probeDone:
		if err != nil {
			t.Fatalf("ProbeOnce() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ProbeOnce() did not complete within 2s — session close may still be blocking lease release")
	}

	// Probe tuner lease should have been released promptly despite blocked close.
	if inUse := pool.InUseCount(); inUse != 0 {
		t.Fatalf("pool.InUseCount() = %d after ProbeOnce() completed, want 0 (lease should be released before close)", inUse)
	}

	// Verify success was recorded.
	if len(provider.successes) != 1 || provider.successes[0] != 10 {
		t.Fatalf("successes = %#v, want [10]", provider.successes)
	}
}

func TestBackgroundProberProbeOnceCloseDispatchIsBoundedUnderBlockedClose(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("probe-bytes"))
	}))
	defer upstream.Close()

	blockCh := make(chan struct{})
	closeProbeSession := sync.OnceFunc(func() {
		close(blockCh)
	})
	defer closeProbeSession()

	provider := &fakeProbeProvider{
		enabledChannels: []channels.Channel{
			{ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:bounded-close",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	prober := newBackgroundProberForTest(t, ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeTimeout:  1 * time.Second,
		HTTPClient: &http.Client{
			Transport: &blockingCloseTransport{
				inner:   http.DefaultTransport,
				blockCh: blockCh,
			},
		},
	}, provider)

	waitForStableGoroutines := func() int {
		runtime.GC()
		runtime.Gosched()
		time.Sleep(50 * time.Millisecond)
		return runtime.NumGoroutine()
	}

	base := waitForStableGoroutines()
	const attempts = backgroundProberCloseQueueDepth * 3
	for i := 0; i < attempts; i++ {
		if err := prober.ProbeOnce(context.Background()); err != nil {
			t.Fatalf("ProbeOnce() error = %v at attempt %d", err, i)
		}
	}

	select {
	case <-prober.probeCloseWorkerDone:
		t.Fatal("background prober close worker exited unexpectedly while close body is still blocked")
	default:
	}

	after := waitForStableGoroutines()
	// Queue dispatch itself is single-worker, but each queued session close may
	// exercise bounded close-timeout workers and retry-drain workers.
	maxExpectedGrowth := (2 * boundedCloseWorkerBudget) + 4
	if after > base+maxExpectedGrowth {
		t.Fatalf(
			"detached close goroutine growth under blocked close exceeded bound: before=%d after=%d max_growth=%d",
			base,
			after,
			maxExpectedGrowth,
		)
	}

	// Allow blocked close worker to complete after unblocking close.
	closeProbeSession()
	closeDone := make(chan struct{})
	go func() {
		prober.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("background prober Close() did not converge after unblocking close")
	}
}

func TestBackgroundProberCloseStopsSessionCloseWorker(t *testing.T) {
	prober := NewBackgroundProber(ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeTimeout:  time.Second,
	}, &fakeProbeProvider{})

	if prober.probeCloseWorkerDone == nil {
		t.Fatal("probe close worker done channel is nil")
	}

	prober.Close()
	prober.Close() // Verify repeated close calls are no-ops.

	select {
	case <-prober.probeCloseWorkerDone:
	case <-time.After(time.Second):
		t.Fatal("background prober close worker did not exit after Close()")
	}
}

func TestBackgroundProberCloseOnDisabledProberConverges(t *testing.T) {
	tests := []struct {
		name     string
		provider ProbeChannelsProvider
		interval time.Duration
	}{
		{
			name:     "nil provider",
			provider: nil,
			interval: time.Second,
		},
		{
			name:     "zero interval",
			provider: &fakeProbeProvider{},
			interval: 0,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			prober := NewBackgroundProber(ProberConfig{
				Mode:          "direct",
				MinProbeBytes: 1,
				ProbeInterval: tc.interval,
				ProbeTimeout:  time.Second,
			}, tc.provider)
			if prober.Enabled() {
				t.Fatal("Enabled() = true, want false")
			}

			closeDone := make(chan struct{})
			go func() {
				prober.Close()
				close(closeDone)
			}()
			select {
			case <-closeDone:
			case <-time.After(time.Second):
				t.Fatal("Close() did not converge for disabled background prober")
			}
		})
	}
}

func TestBackgroundProberRunCancellationThenCloseConverges(t *testing.T) {
	prober := NewBackgroundProber(ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeInterval: time.Hour,
		ProbeTimeout:  time.Second,
	}, &fakeProbeProvider{})

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		prober.Run(runCtx)
		close(runDone)
	}()

	cancel()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after context cancellation")
	}

	closeDone := make(chan struct{})
	go func() {
		prober.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close() did not converge after Run() cancellation")
	}
}

func TestBackgroundProberCloseQueueDepthDefaultAndOverride(t *testing.T) {
	tests := []struct {
		name      string
		cfgDepth  int
		wantDepth int
	}{
		{
			name:      "default depth",
			cfgDepth:  0,
			wantDepth: backgroundProberCloseQueueDepth,
		},
		{
			name:      "negative depth falls back to default",
			cfgDepth:  -4,
			wantDepth: backgroundProberCloseQueueDepth,
		},
		{
			name:      "configured depth",
			cfgDepth:  3,
			wantDepth: 3,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			prober := NewBackgroundProber(ProberConfig{
				Mode:                 "direct",
				MinProbeBytes:        1,
				ProbeTimeout:         time.Second,
				ProbeCloseQueueDepth: tc.cfgDepth,
			}, &fakeProbeProvider{})
			t.Cleanup(func() {
				prober.Close()
			})

			if got := cap(prober.probeCloseSessionCh); got != tc.wantDepth {
				t.Fatalf("cap(probeCloseSessionCh) = %d, want %d", got, tc.wantDepth)
			}
			if got := prober.probeCloseQueueDepth; got != tc.wantDepth {
				t.Fatalf("probeCloseQueueDepth = %d, want %d", got, tc.wantDepth)
			}
		})
	}
}

func TestBackgroundProberCopyTimestampRegenerationDefaultsAndOverride(t *testing.T) {
	falseValue := false
	tests := []struct {
		name       string
		mode       string
		configured *bool
		want       bool
	}{
		{
			name:       "copy mode defaults enabled",
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
			name:       "direct mode defaults off",
			mode:       "direct",
			configured: nil,
			want:       false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			prober := NewBackgroundProber(ProberConfig{
				Mode:                           tc.mode,
				FFmpegCopyRegenerateTimestamps: tc.configured,
				MinProbeBytes:                  1,
				ProbeTimeout:                   time.Second,
			}, &fakeProbeProvider{})
			t.Cleanup(func() {
				prober.Close()
			})

			if got := prober.ffmpegCopyRegenerateTimestamps; got != tc.want {
				t.Fatalf("ffmpegCopyRegenerateTimestamps = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestBackgroundProberCloseWaitsForQueuedSessionDrain(t *testing.T) {
	prober := NewBackgroundProber(ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeTimeout:  1 * time.Second,
	}, &fakeProbeProvider{})

	blockCh := make(chan struct{})
	sessionCloseStarted := make(chan struct{})
	session := &streamSession{
		closeFn: func() error {
			close(sessionCloseStarted)
			<-blockCh
			return nil
		},
	}

	prober.enqueueProbeSessionClose(10, session)
	select {
	case <-sessionCloseStarted:
	case <-time.After(time.Second):
		t.Fatal("background prober close worker did not begin queued session close")
	}

	closeDone := make(chan struct{})
	go func() {
		prober.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		t.Fatal("Close() returned while queued session close was still blocked")
	case <-time.After(150 * time.Millisecond):
	}

	close(blockCh)
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() did not return after queued session close was released")
	}
}

func TestBackgroundProberEnqueueProbeSessionCloseQueueFullClosesInline(t *testing.T) {
	_, deltaStats := captureProbeCloseStatsForTest(t)

	prober := NewBackgroundProber(ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeTimeout:  1 * time.Second,
	}, &fakeProbeProvider{})

	blockCh := make(chan struct{})
	sessionCloseStarted := make(chan struct{})
	session := &streamSession{
		closeFn: func() error {
			close(sessionCloseStarted)
			<-blockCh
			return nil
		},
	}
	prober.enqueueProbeSessionClose(10, session)
	select {
	case <-sessionCloseStarted:
	case <-time.After(time.Second):
		t.Fatal("background prober close worker did not begin queued session close")
	}

	for i := 0; i < backgroundProberCloseQueueDepth; i++ {
		prober.enqueueProbeSessionClose(
			int64(20+i),
			&streamSession{closeFn: func() error { return nil }},
		)
	}
	if got, want := len(prober.probeCloseSessionCh), backgroundProberCloseQueueDepth; got != want {
		t.Fatalf("probe close queue depth = %d, want %d before overflow enqueue", got, want)
	}

	overflowClosed := make(chan struct{})
	prober.enqueueProbeSessionClose(
		999,
		&streamSession{
			closeFn: func() error {
				close(overflowClosed)
				return nil
			},
		},
	)
	select {
	case <-overflowClosed:
	case <-time.After(time.Second):
		t.Fatal("overflow enqueue did not close session inline when queue was full")
	}
	if got, want := len(prober.probeCloseSessionCh), backgroundProberCloseQueueDepth; got != want {
		t.Fatalf("probe close queue depth = %d, want %d after overflow inline close", got, want)
	}
	stats := deltaStats()
	if got, want := stats.InlineCount, uint64(1); got != want {
		t.Fatalf("probe close inline count = %d, want %d", got, want)
	}
	if got, want := stats.QueueFullCount, uint64(1); got != want {
		t.Fatalf("probe close queue_full count = %d, want %d", got, want)
	}

	close(blockCh)
	prober.Close()
}

func TestBackgroundProberEnqueueProbeSessionCloseAfterCloseClosesInline(t *testing.T) {
	_, deltaStats := captureProbeCloseStatsForTest(t)

	prober := NewBackgroundProber(ProberConfig{
		Mode:          "direct",
		MinProbeBytes: 1,
		ProbeTimeout:  1 * time.Second,
	}, &fakeProbeProvider{})
	prober.Close()

	blockCh := make(chan struct{})
	sessionCloseStarted := make(chan struct{})
	session := &streamSession{
		closeFn: func() error {
			close(sessionCloseStarted)
			<-blockCh
			return nil
		},
	}

	enqueueDone := make(chan struct{})
	go func() {
		prober.enqueueProbeSessionClose(10, session)
		close(enqueueDone)
	}()
	select {
	case <-sessionCloseStarted:
	case <-time.After(time.Second):
		t.Fatal("enqueueProbeSessionClose() did not run inline close after prober Close()")
	}
	select {
	case <-enqueueDone:
		t.Fatal("enqueueProbeSessionClose() returned before inline close unblocked")
	case <-time.After(150 * time.Millisecond):
	}

	close(blockCh)
	select {
	case <-enqueueDone:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueueProbeSessionClose() did not return after inline close unblocked")
	}
	stats := deltaStats()
	if got, want := stats.InlineCount, uint64(1); got != want {
		t.Fatalf("probe close inline count = %d, want %d", got, want)
	}
	if got, want := stats.QueueFullCount, uint64(0); got != want {
		t.Fatalf("probe close queue_full count = %d, want %d", got, want)
	}
}
