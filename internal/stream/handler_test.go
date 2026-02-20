package stream

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

type sourceFailureCall struct {
	sourceID int64
	reason   string
}

type sourceProfileUpdateCall struct {
	sourceID int64
	profile  channels.SourceProfileUpdate
}

type fakeChannelsProvider struct {
	mu sync.Mutex

	channelsByGuide  map[string]channels.Channel
	sourcesByID      map[int64][]channels.Source
	failures         []sourceFailureCall
	successes        []int64
	profileUpdates   []sourceProfileUpdateCall
	markFailureDelay time.Duration
	// markFailureDelays, when set, overrides markFailureDelay per call in FIFO
	// order to keep timeout-branch tests deterministic.
	markFailureDelays []time.Duration
	markSuccessDelay  time.Duration
	listSourcesCalls  int
	getSourceCalls    int

	// markFailureErrs is consumed in FIFO order: the first call to
	// MarkSourceFailure pops the first element; when empty, nil is returned.
	markFailureErrs []error
	// markSuccessErrs works the same way for MarkSourceSuccess.
	markSuccessErrs []error
	// updateProfileErrs is consumed in FIFO order for UpdateSourceProfile.
	updateProfileErrs []error
}

func (p *fakeChannelsProvider) GetByGuideNumber(_ context.Context, guideNumber string) (channels.Channel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	channel, ok := p.channelsByGuide[guideNumber]
	if !ok {
		return channels.Channel{}, channels.ErrChannelNotFound
	}
	return channel, nil
}

func (p *fakeChannelsProvider) ListSources(_ context.Context, channelID int64, enabledOnly bool) ([]channels.Source, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listSourcesCalls++

	list, ok := p.sourcesByID[channelID]
	if !ok {
		return nil, channels.ErrChannelNotFound
	}

	out := make([]channels.Source, 0, len(list))
	for _, source := range list {
		if enabledOnly && !source.Enabled {
			continue
		}
		out = append(out, source)
	}
	return out, nil
}

func (p *fakeChannelsProvider) GetSource(_ context.Context, channelID, sourceID int64, enabledOnly bool) (channels.Source, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.getSourceCalls++

	list, ok := p.sourcesByID[channelID]
	if !ok {
		return channels.Source{}, channels.ErrChannelNotFound
	}
	for _, source := range list {
		if source.SourceID != sourceID {
			continue
		}
		if enabledOnly && !source.Enabled {
			return channels.Source{}, channels.ErrSourceNotFound
		}
		return source, nil
	}
	return channels.Source{}, channels.ErrSourceNotFound
}

func (p *fakeChannelsProvider) MarkSourceFailure(_ context.Context, sourceID int64, reason string, _ time.Time) error {
	delay := p.markFailureDelay
	p.mu.Lock()
	if len(p.markFailureDelays) > 0 {
		delay = p.markFailureDelays[0]
		p.markFailureDelays = p.markFailureDelays[1:]
	}
	p.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.markFailureErrs) > 0 {
		err := p.markFailureErrs[0]
		p.markFailureErrs = p.markFailureErrs[1:]
		if err != nil {
			return err
		}
	}

	p.failures = append(p.failures, sourceFailureCall{
		sourceID: sourceID,
		reason:   reason,
	})
	return nil
}

func (p *fakeChannelsProvider) MarkSourceSuccess(_ context.Context, sourceID int64, _ time.Time) error {
	if p.markSuccessDelay > 0 {
		time.Sleep(p.markSuccessDelay)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.markSuccessErrs) > 0 {
		err := p.markSuccessErrs[0]
		p.markSuccessErrs = p.markSuccessErrs[1:]
		if err != nil {
			return err
		}
	}

	p.successes = append(p.successes, sourceID)
	return nil
}

func (p *fakeChannelsProvider) UpdateSourceProfile(_ context.Context, sourceID int64, profile channels.SourceProfileUpdate) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.updateProfileErrs) > 0 {
		err := p.updateProfileErrs[0]
		p.updateProfileErrs = p.updateProfileErrs[1:]
		if err != nil {
			return err
		}
	}

	p.profileUpdates = append(p.profileUpdates, sourceProfileUpdateCall{
		sourceID: sourceID,
		profile:  profile,
	})
	return nil
}

func (p *fakeChannelsProvider) sourceFailures() []sourceFailureCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]sourceFailureCall, len(p.failures))
	copy(out, p.failures)
	return out
}

func (p *fakeChannelsProvider) sourceSuccesses() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]int64, len(p.successes))
	copy(out, p.successes)
	return out
}

func (p *fakeChannelsProvider) listSourceCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.listSourcesCalls
}

func (p *fakeChannelsProvider) getSourceCallCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.getSourceCalls
}

type slowResponseWriter struct {
	mu         sync.Mutex
	header     http.Header
	statusCode int
	body       bytes.Buffer
	writeDelay time.Duration
	deadline   time.Time
}

func (w *slowResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *slowResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

func (w *slowResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	delay := w.writeDelay
	deadline := w.deadline
	w.mu.Unlock()

	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	if delay > 0 {
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return 0, context.DeadlineExceeded
			}
			if delay > remaining {
				time.Sleep(remaining)
				return 0, context.DeadlineExceeded
			}
		}
		time.Sleep(delay)
	}
	return w.body.Write(p)
}

func (w *slowResponseWriter) Flush() {}

func (w *slowResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.deadline = deadline
	w.mu.Unlock()
	return nil
}

func TestNewHandlerPlumbsSessionHistoryAndDrainConfig(t *testing.T) {
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{},
		sourcesByID:     map[int64][]channels.Source{},
	}

	t.Run("explicit history and drain settings", func(t *testing.T) {
		handler := NewHandler(
			Config{
				Mode:                          "direct",
				SessionHistoryLimit:           64,
				SessionSourceHistoryLimit:     24,
				SessionSubscriberHistoryLimit: 20,
				SourceHealthDrainTimeout:      375 * time.Millisecond,
			},
			NewPool(1),
			provider,
		)
		if handler == nil || handler.sessions == nil {
			t.Fatal("handler.sessions is nil")
		}
		if got, want := handler.sessions.cfg.sessionHistoryLimit, 64; got != want {
			t.Fatalf("sessionHistoryLimit = %d, want %d", got, want)
		}
		if got, want := handler.sessions.cfg.sessionSourceHistoryLimit, 24; got != want {
			t.Fatalf("sessionSourceHistoryLimit = %d, want %d", got, want)
		}
		if got, want := handler.sessions.cfg.sessionSubscriberHistoryLimit, 20; got != want {
			t.Fatalf("sessionSubscriberHistoryLimit = %d, want %d", got, want)
		}
		if got, want := handler.sessions.cfg.sourceHealthDrainTimeout, 375*time.Millisecond; got != want {
			t.Fatalf("sourceHealthDrainTimeout = %s, want %s", got, want)
		}
	})

	t.Run("per-timeline fallback uses session history limit", func(t *testing.T) {
		handler := NewHandler(
			Config{
				Mode:                "direct",
				SessionHistoryLimit: 48,
			},
			NewPool(1),
			provider,
		)
		if handler == nil || handler.sessions == nil {
			t.Fatal("handler.sessions is nil")
		}
		if got, want := handler.sessions.cfg.sessionSourceHistoryLimit, 48; got != want {
			t.Fatalf("sessionSourceHistoryLimit = %d, want %d", got, want)
		}
		if got, want := handler.sessions.cfg.sessionSubscriberHistoryLimit, 48; got != want {
			t.Fatalf("sessionSubscriberHistoryLimit = %d, want %d", got, want)
		}
	})

	t.Run("per-timeline fallback clamps to guardrails", func(t *testing.T) {
		handler := NewHandler(
			Config{
				Mode:                "direct",
				SessionHistoryLimit: 5,
			},
			NewPool(1),
			provider,
		)
		if handler == nil || handler.sessions == nil {
			t.Fatal("handler.sessions is nil")
		}
		if got, want := handler.sessions.cfg.sessionSourceHistoryLimit, minSharedSourceHistoryLimit; got != want {
			t.Fatalf("sessionSourceHistoryLimit = %d, want %d", got, want)
		}
		if got, want := handler.sessions.cfg.sessionSubscriberHistoryLimit, minSharedSubscriberHistoryLimit; got != want {
			t.Fatalf("sessionSubscriberHistoryLimit = %d, want %d", got, want)
		}

		handler = NewHandler(
			Config{
				Mode:                "direct",
				SessionHistoryLimit: 10_000,
			},
			NewPool(1),
			provider,
		)
		if handler == nil || handler.sessions == nil {
			t.Fatal("handler.sessions is nil")
		}
		if got, want := handler.sessions.cfg.sessionSourceHistoryLimit, maxSharedSourceHistoryLimit; got != want {
			t.Fatalf("sessionSourceHistoryLimit = %d, want %d", got, want)
		}
		if got, want := handler.sessions.cfg.sessionSubscriberHistoryLimit, maxSharedSubscriberHistoryLimit; got != want {
			t.Fatalf("sessionSubscriberHistoryLimit = %d, want %d", got, want)
		}
	})

	t.Run("zero drain timeout normalizes to default", func(t *testing.T) {
		handler := NewHandler(
			Config{
				Mode:                     "direct",
				SourceHealthDrainTimeout: 0,
			},
			NewPool(1),
			provider,
		)
		if handler == nil || handler.sessions == nil {
			t.Fatal("handler.sessions is nil")
		}
		if got, want := handler.sessions.cfg.sourceHealthDrainTimeout, defaultSourceHealthDrainTimeout; got != want {
			t.Fatalf("sourceHealthDrainTimeout = %s, want %s", got, want)
		}
	})
}

func TestHandlerCloseWithContextLifecycle(t *testing.T) {
	t.Run("nil guards", func(t *testing.T) {
		var nilHandler *Handler
		if err := nilHandler.CloseWithContext(context.Background()); err != nil {
			t.Fatalf("(*Handler)(nil).CloseWithContext() error = %v, want nil", err)
		}
		handler := &Handler{}
		if err := handler.CloseWithContext(context.Background()); err != nil {
			t.Fatalf("handler without sessions CloseWithContext() error = %v, want nil", err)
		}
	})

	t.Run("timeout propagates manager deadline", func(t *testing.T) {
		var canceled atomic.Bool
		manager := &SessionManager{
			sessions: map[int64]*sharedRuntimeSession{
				101: {cancel: func() { canceled.Store(true) }},
			},
			creating: make(map[int64]*sessionCreateWait),
		}
		manager.wg.Add(1)
		defer manager.wg.Done()

		handler := &Handler{sessions: manager}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		err := handler.CloseWithContext(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("CloseWithContext() error = %v, want %v", err, context.DeadlineExceeded)
		}
		if !canceled.Load() {
			t.Fatal("session cancel callback was not invoked")
		}
	})

	t.Run("close wrapper remains backwards compatible", func(t *testing.T) {
		var canceled atomic.Bool
		manager := &SessionManager{
			sessions: map[int64]*sharedRuntimeSession{
				202: {cancel: func() { canceled.Store(true) }},
			},
			creating: make(map[int64]*sessionCreateWait),
		}
		manager.wg.Add(1)
		handler := &Handler{sessions: manager}

		done := make(chan struct{})
		go func() {
			handler.Close()
			close(done)
		}()

		waitFor(t, time.Second, func() bool {
			return canceled.Load()
		})
		manager.wg.Done()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("handler.Close() did not return after manager drain")
		}
	})
}

func TestHandlerClearSourceHealthWrappers(t *testing.T) {
	t.Run("nil guards", func(t *testing.T) {
		var nilHandler *Handler
		if err := nilHandler.ClearSourceHealth(1); err == nil {
			t.Fatal("(*Handler)(nil).ClearSourceHealth() error = nil, want non-nil")
		}
		if err := nilHandler.ClearAllSourceHealth(); err == nil {
			t.Fatal("(*Handler)(nil).ClearAllSourceHealth() error = nil, want non-nil")
		}

		handler := &Handler{}
		if err := handler.ClearSourceHealth(1); err == nil {
			t.Fatal("handler without sessions ClearSourceHealth() error = nil, want non-nil")
		}
		if err := handler.ClearAllSourceHealth(); err == nil {
			t.Fatal("handler without sessions ClearAllSourceHealth() error = nil, want non-nil")
		}
	})

	t.Run("delegates to session manager", func(t *testing.T) {
		provider := &fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{
				"188": {ChannelID: 88, GuideNumber: "188", GuideName: "Clear", Enabled: true},
			},
			sourcesByID: map[int64][]channels.Source{
				88: {
					{
						SourceID:      44,
						ChannelID:     88,
						ItemKey:       "src:clear:44",
						StreamURL:     "http://example.com/clear.ts",
						PriorityIndex: 0,
						Enabled:       true,
					},
				},
			},
		}
		handler := NewHandler(
			Config{Mode: "direct"},
			NewPool(1),
			provider,
		)
		if handler == nil || handler.sessions == nil {
			t.Fatal("handler.sessions is nil")
		}

		if err := handler.ClearSourceHealth(88); err != nil {
			t.Fatalf("ClearSourceHealth() error = %v", err)
		}
		handler.sessions.mu.Lock()
		cutoff, ok := handler.sessions.sourceHealthClearAfterByChan[88]
		handler.sessions.mu.Unlock()
		if !ok || cutoff.IsZero() {
			t.Fatal("sourceHealthClearAfterByChan[88] was not set by ClearSourceHealth")
		}

		if err := handler.ClearAllSourceHealth(); err != nil {
			t.Fatalf("ClearAllSourceHealth() error = %v", err)
		}
		handler.sessions.mu.Lock()
		allCutoff := handler.sessions.sourceHealthClearAfter
		perChannel := len(handler.sessions.sourceHealthClearAfterByChan)
		handler.sessions.mu.Unlock()
		if allCutoff.IsZero() {
			t.Fatal("sourceHealthClearAfter is zero after ClearAllSourceHealth")
		}
		if perChannel != 0 {
			t.Fatalf("len(sourceHealthClearAfterByChan) = %d, want 0 after ClearAllSourceHealth", perChannel)
		}
	})
}

func TestHandlerDirectModeStreamsChannel(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 20; i++ {
			_, _ = w.Write([]byte("transport-stream-data"))
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
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

	handler := NewHandler(
		Config{
			Mode:                       "direct",
			StartupTimeout:             2 * time.Second,
			FailoverTotalTimeout:       5 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
		},
		NewPool(1),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer reqCancel()

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil).WithContext(reqCtx)
	req.RemoteAddr = "192.168.1.55:45678"
	rec := newDeadlineResponseRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "video/MP2T" {
		t.Fatalf("Content-Type = %q, want video/MP2T", got)
	}
	if got := rec.Body.String(); !strings.Contains(got, "transport-stream-data") {
		t.Fatalf("body = %q, want data containing transport-stream-data", got)
	}
	waitFor(t, time.Second, func() bool {
		successes := provider.sourceSuccesses()
		return len(successes) >= 1 && successes[0] == 10
	})
}

func TestHandlerDirectModeStreamsDynamicGuideRangeChannel(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		ticker := time.NewTicker(fastStreamTestTiming.upstreamChunkInterval)
		defer ticker.Stop()
		for i := 0; i < 200; i++ {
			if _, err := w.Write([]byte("dynamic-transport-stream")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			<-ticker.C
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"10000": {
				ChannelID:      10000,
				ChannelClass:   channels.ChannelClassDynamicGenerated,
				GuideNumber:    "10000",
				GuideName:      "Dynamic News",
				Enabled:        true,
				DynamicQueryID: 4,
				DynamicItemKey: "src:dyn:news",
			},
		},
		sourcesByID: map[int64][]channels.Source{
			10000: {
				{
					SourceID:      10,
					ChannelID:     10000,
					ItemKey:       "src:dyn:news",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	cfg := fastStreamTestTiming.handlerConfig("direct")
	cfg.BufferChunkBytes = 64
	handler := NewHandler(cfg, NewPool(1), provider)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
	defer reqCancel()
	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v10000", nil).WithContext(reqCtx)
	rec := newDeadlineResponseRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "video/MP2T" {
		t.Fatalf("Content-Type = %q, want video/MP2T", got)
	}
	if got := rec.Body.String(); !strings.Contains(got, "dynamic-transport-stream") {
		t.Fatalf("body = %q, want dynamic-transport-stream", got)
	}
}

func TestHandlerRangeRequestWaitsForStartupReadiness(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(350 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("delayed-stream-data"))
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:delayed",
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
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
			SessionIdleTimeout:         25 * time.Millisecond,
		},
		NewPool(1),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer reqCancel()

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil).WithContext(reqCtx)
	req.Header.Set("Range", "bytes=0-")
	rec := newDeadlineResponseRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestTimeout {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestTimeout)
	}
	if got := rec.Body.String(); !strings.Contains(got, "request canceled") {
		t.Fatalf("body = %q, want request canceled", got)
	}
}

func TestHandlerFallsBackToNextSource(t *testing.T) {
	badUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer badUpstream.Close()

	goodUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 20; i++ {
			_, _ = w.Write([]byte("good-stream"))
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer goodUpstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:bad",
					StreamURL:     badUpstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:good",
					StreamURL:     goodUpstream.URL,
					PriorityIndex: 1,
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
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
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
		failures := provider.sourceFailures()
		successes := provider.sourceSuccesses()
		return len(failures) == 1 && failures[0].sourceID == 10 && len(successes) == 1 && successes[0] == 11
	})
	failures := provider.sourceFailures()
	if !strings.Contains(failures[0].reason, "upstream returned status 404") {
		t.Fatalf("failure reason = %q, want upstream status details", failures[0].reason)
	}
}

func TestHandlerReturnsNotFoundForUnknownGuideNumber(t *testing.T) {
	handler := NewHandler(
		Config{Mode: "direct"},
		NewPool(1),
		&fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{},
			sourcesByID:     map[int64][]channels.Source{},
		},
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v999", nil)
	rec := newDeadlineResponseRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandlerReturnsServiceUnavailableWhenTunersBusy(t *testing.T) {
	pool := NewPool(1)
	lease, err := pool.Acquire(context.Background(), "101", "192.168.1.55:45678")
	if err != nil {
		t.Fatalf("Acquire() setup error = %v", err)
	}
	defer lease.Release()

	handler := NewHandler(
		Config{Mode: "direct"},
		pool,
		&fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{
				"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
			},
			sourcesByID: map[int64][]channels.Source{
				1: {
					{
						SourceID:      10,
						ChannelID:     1,
						ItemKey:       "src:news:primary",
						StreamURL:     "http://example.com/stream.ts",
						PriorityIndex: 0,
						Enabled:       true,
					},
				},
			},
		},
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil)
	rec := newDeadlineResponseRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHandlerTuneBackoffRejectsRepeatedStartupFailuresPerChannel(t *testing.T) {
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	badUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer badUpstream.Close()

	goodUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 80; i++ {
			if _, err := w.Write([]byte("channel-two-stream")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(4 * time.Millisecond)
		}
	}))
	defer goodUpstream.Close()

	handler := NewHandler(
		Config{
			Mode:                       "direct",
			Logger:                     logger,
			TuneBackoffMaxTunes:        1,
			TuneBackoffInterval:        1 * time.Minute,
			TuneBackoffCooldown:        300 * time.Millisecond,
			SessionIdleTimeout:         50 * time.Millisecond,
			FailoverTotalTimeout:       2 * time.Second,
			StartupTimeout:             1 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
		},
		NewPool(1),
		&fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{
				"101": {ChannelID: 1, GuideNumber: "101", GuideName: "Flaky", Enabled: true},
				"102": {ChannelID: 2, GuideNumber: "102", GuideName: "Healthy", Enabled: true},
			},
			sourcesByID: map[int64][]channels.Source{
				1: {
					{
						SourceID:      11,
						ChannelID:     1,
						ItemKey:       "src:flaky",
						StreamURL:     badUpstream.URL,
						PriorityIndex: 0,
						Enabled:       true,
					},
				},
				2: {
					{
						SourceID:      21,
						ChannelID:     2,
						ItemKey:       "src:healthy",
						StreamURL:     goodUpstream.URL,
						PriorityIndex: 0,
						Enabled:       true,
					},
				},
			},
		},
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	doReq := func() *deadlineResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil)
		req.RemoteAddr = "198.51.100.20:42000"
		rec := newDeadlineResponseRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	first := doReq()
	if first.Code != http.StatusServiceUnavailable {
		t.Fatalf("first status = %d, want %d", first.Code, http.StatusServiceUnavailable)
	}

	second := doReq()
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("second status = %d, want %d", second.Code, http.StatusServiceUnavailable)
	}
	if retryAfter := strings.TrimSpace(second.Header().Get("Retry-After")); retryAfter == "" {
		t.Fatal("second response missing Retry-After header")
	}
	if body := second.Body.String(); !strings.Contains(body, "channel tune backoff active") {
		t.Fatalf("second body = %q, want channel tune backoff message", body)
	}

	thirdCtx, thirdCancel := context.WithTimeout(context.Background(), 220*time.Millisecond)
	defer thirdCancel()
	thirdReq := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v102", nil).WithContext(thirdCtx)
	thirdReq.RemoteAddr = "198.51.100.30:42030"
	third := newDeadlineResponseRecorder()
	mux.ServeHTTP(third, thirdReq)
	if third.Code != http.StatusOK {
		t.Fatalf("third status = %d, want %d (unrelated channel should not be backoff-blocked)", third.Code, http.StatusOK)
	}

	text := logs.String()
	if !strings.Contains(text, "stream tune rejected") || !strings.Contains(text, "reason=channel_tune_backoff") {
		t.Fatalf("missing expected channel tune backoff log: %s", text)
	}
}

func TestHandlerTuneBackoffSuccessfulStartsDoNotConsumeBudget(t *testing.T) {
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 120; i++ {
			_, _ = w.Write([]byte("shared-session-stream"))
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(4 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	handler := NewHandler(
		Config{
			Mode:                       "direct",
			Logger:                     logger,
			TuneBackoffMaxTunes:        1,
			TuneBackoffInterval:        1 * time.Minute,
			TuneBackoffCooldown:        300 * time.Millisecond,
			StartupTimeout:             2 * time.Second,
			FailoverTotalTimeout:       5 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
			SessionIdleTimeout:         25 * time.Millisecond,
		},
		NewPool(1),
		&fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{
				"101": {ChannelID: 1, GuideNumber: "101", GuideName: "Primary", Enabled: true},
			},
			sourcesByID: map[int64][]channels.Source{
				1: {
					{
						SourceID:      10,
						ChannelID:     1,
						ItemKey:       "src:primary",
						StreamURL:     upstream.URL,
						PriorityIndex: 0,
						Enabled:       true,
					},
				},
			},
		},
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	run := func(remoteAddr string) *deadlineResponseRecorder {
		reqCtx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
		defer cancel()
		req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil).WithContext(reqCtx)
		req.RemoteAddr = remoteAddr
		rec := newDeadlineResponseRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	first := run("198.51.100.60:42060")
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d, want %d", first.Code, http.StatusOK)
	}
	if retryAfter := strings.TrimSpace(first.Header().Get("Retry-After")); retryAfter != "" {
		t.Fatalf("first Retry-After = %q, want empty on successful startup", retryAfter)
	}

	waitFor(t, time.Second, func() bool {
		return !handler.sessions.HasActiveOrPendingSession(1)
	})

	second := run("198.51.100.61:42061")
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d, want %d", second.Code, http.StatusOK)
	}
	if retryAfter := strings.TrimSpace(second.Header().Get("Retry-After")); retryAfter != "" {
		t.Fatalf("second Retry-After = %q, want empty (successful startup should not consume backoff budget)", retryAfter)
	}

	waitFor(t, time.Second, func() bool {
		return !handler.sessions.HasActiveOrPendingSession(1)
	})

	third := run("198.51.100.62:42062")
	if third.Code != http.StatusOK {
		t.Fatalf("third status = %d, want %d", third.Code, http.StatusOK)
	}
	if retryAfter := strings.TrimSpace(third.Header().Get("Retry-After")); retryAfter != "" {
		t.Fatalf("third Retry-After = %q, want empty across repeated successful startups", retryAfter)
	}

	if strings.Contains(logs.String(), "reason=channel_tune_backoff") {
		t.Fatalf("unexpected tune backoff rejection logged for repeated successful startups: %s", logs.String())
	}
}

func TestHandlerTuneBackoffConcurrentStartupFailureCountsOneOutcomePerCycle(t *testing.T) {
	startupRelease := make(chan struct{})
	badUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-startupRelease
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer badUpstream.Close()

	handler := NewHandler(
		Config{
			Mode:                       "direct",
			TuneBackoffMaxTunes:        2,
			TuneBackoffInterval:        1 * time.Minute,
			TuneBackoffCooldown:        2 * time.Second,
			StartupTimeout:             1 * time.Second,
			FailoverTotalTimeout:       2 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
			SessionIdleTimeout:         40 * time.Millisecond,
		},
		NewPool(1),
		&fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{
				"101": {ChannelID: 1, GuideNumber: "101", GuideName: "Flaky", Enabled: true},
			},
			sourcesByID: map[int64][]channels.Source{
				1: {
					{
						SourceID:      11,
						ChannelID:     1,
						ItemKey:       "src:flaky",
						StreamURL:     badUpstream.URL,
						PriorityIndex: 0,
						Enabled:       true,
					},
				},
			},
		},
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	type requestResult struct {
		status     int
		retryAfter string
		body       string
	}

	doRequest := func(remoteAddr string) requestResult {
		reqCtx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
		defer cancel()
		req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil).WithContext(reqCtx)
		req.RemoteAddr = remoteAddr
		rec := newDeadlineResponseRecorder()
		mux.ServeHTTP(rec, req)
		return requestResult{
			status:     rec.Code,
			retryAfter: strings.TrimSpace(rec.Header().Get("Retry-After")),
			body:       rec.Body.String(),
		}
	}

	results := make([]requestResult, 2)
	remotes := []string{"198.51.100.70:42070", "198.51.100.71:42071"}
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = doRequest(remotes[idx])
		}(i)
	}

	waitFor(t, time.Second, func() bool {
		stats := handler.sessions.Snapshot()
		return len(stats) == 1 && stats[0].Subscribers >= 2
	})
	close(startupRelease)
	wg.Wait()

	for i, result := range results {
		if result.status != http.StatusServiceUnavailable {
			t.Fatalf("first-cycle request %d status = %d, want %d", i+1, result.status, http.StatusServiceUnavailable)
		}
		if result.retryAfter != "" {
			t.Fatalf("first-cycle request %d Retry-After = %q, want empty startup-failure response", i+1, result.retryAfter)
		}
	}

	secondCycle := doRequest("198.51.100.72:42072")
	if secondCycle.status != http.StatusServiceUnavailable {
		t.Fatalf("second-cycle status = %d, want %d", secondCycle.status, http.StatusServiceUnavailable)
	}
	if secondCycle.retryAfter != "" {
		t.Fatalf("second-cycle Retry-After = %q, want empty startup-failure response", secondCycle.retryAfter)
	}

	blocked := doRequest("198.51.100.73:42073")
	if blocked.status != http.StatusServiceUnavailable {
		t.Fatalf("blocked status = %d, want %d", blocked.status, http.StatusServiceUnavailable)
	}
	if blocked.retryAfter == "" {
		t.Fatal("blocked request missing Retry-After header")
	}
	if !strings.Contains(blocked.body, "channel tune backoff active") {
		t.Fatalf("blocked body = %q, want channel tune backoff message", blocked.body)
	}
}

func TestHandlerTuneBackoffJoinersNotRejectedWhileStartupPending(t *testing.T) {
	startupRelease := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-startupRelease
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 80; i++ {
			_, _ = w.Write([]byte("pending-startup-stream"))
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(3 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	handler := NewHandler(
		Config{
			Mode:                       "direct",
			TuneBackoffMaxTunes:        1,
			TuneBackoffInterval:        1 * time.Minute,
			TuneBackoffCooldown:        2 * time.Second,
			StartupTimeout:             1 * time.Second,
			FailoverTotalTimeout:       2 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
			SessionIdleTimeout:         40 * time.Millisecond,
		},
		NewPool(1),
		&fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{
				"101": {ChannelID: 1, GuideNumber: "101", GuideName: "Pending", Enabled: true},
			},
			sourcesByID: map[int64][]channels.Source{
				1: {
					{
						SourceID:      12,
						ChannelID:     1,
						ItemKey:       "src:pending",
						StreamURL:     upstream.URL,
						PriorityIndex: 0,
						Enabled:       true,
					},
				},
			},
		},
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	type requestResult struct {
		status     int
		retryAfter string
	}

	doRequest := func(remoteAddr string) requestResult {
		reqCtx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
		defer cancel()
		req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil).WithContext(reqCtx)
		req.RemoteAddr = remoteAddr
		rec := newDeadlineResponseRecorder()
		mux.ServeHTTP(rec, req)
		return requestResult{
			status:     rec.Code,
			retryAfter: strings.TrimSpace(rec.Header().Get("Retry-After")),
		}
	}

	results := make([]requestResult, 2)
	remotes := []string{"198.51.100.80:42080", "198.51.100.81:42081"}
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = doRequest(remotes[idx])
		}(i)
	}

	waitFor(t, time.Second, func() bool {
		stats := handler.sessions.Snapshot()
		return len(stats) == 1 && stats[0].Subscribers >= 2
	})
	close(startupRelease)
	wg.Wait()

	for i, result := range results {
		if result.status != http.StatusOK {
			t.Fatalf("request %d status = %d, want %d", i+1, result.status, http.StatusOK)
		}
		if result.retryAfter != "" {
			t.Fatalf("request %d Retry-After = %q, want empty while joining pending startup", i+1, result.retryAfter)
		}
	}
}

func TestHandlerReturnsServiceUnavailableWhenAllSourcesFail(t *testing.T) {
	badUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer badUpstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:bad-a",
					StreamURL:     badUpstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:bad-b",
					StreamURL:     badUpstream.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	handler := NewHandler(
		Config{
			Mode:                 "direct",
			StartupTimeout:       1 * time.Second,
			FailoverTotalTimeout: 2 * time.Second,
			MinProbeBytes:        1,
		},
		NewPool(1),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil)
	rec := newDeadlineResponseRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	waitFor(t, time.Second, func() bool {
		return len(provider.sourceFailures()) == 2
	})
	if successes := provider.sourceSuccesses(); len(successes) != 0 {
		t.Fatalf("successes = %#v, want none", successes)
	}
}

func TestHandlerStartupDoesNotBlockOnSourceSuccessPersistence(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 12; i++ {
			_, _ = w.Write([]byte("steady-stream"))
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
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
		markSuccessDelay: 350 * time.Millisecond,
	}

	handler := NewHandler(
		Config{
			Mode:                       "direct",
			StartupTimeout:             2 * time.Second,
			FailoverTotalTimeout:       5 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
		},
		NewPool(1),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 175*time.Millisecond)
	defer reqCancel()

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil).WithContext(reqCtx)
	rec := newDeadlineResponseRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); !strings.Contains(got, "steady-stream") {
		t.Fatalf("body = %q, want data containing steady-stream", got)
	}

	waitFor(t, time.Second, func() bool {
		successes := provider.sourceSuccesses()
		return len(successes) == 1 && successes[0] == 10
	})
}

func TestHandlerStartupFailoverDoesNotBlockOnSourceFailurePersistence(t *testing.T) {
	badUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer badUpstream.Close()

	goodUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("good-stream"))
	}))
	defer goodUpstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:bad",
					StreamURL:     badUpstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:good",
					StreamURL:     goodUpstream.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
		markFailureDelay: 350 * time.Millisecond,
	}

	handler := NewHandler(
		Config{
			Mode:                       "direct",
			StartupTimeout:             2 * time.Second,
			FailoverTotalTimeout:       5 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
		},
		NewPool(1),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 175*time.Millisecond)
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
		failures := provider.sourceFailures()
		source10Failed := false
		for _, failure := range failures {
			if failure.sourceID == 10 {
				source10Failed = true
				break
			}
		}
		return source10Failed
	})
}

func TestHandlerAllSourcesFailDoesNotBlockOnSourceHealthPersistence(t *testing.T) {
	badUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer badUpstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:bad-a",
					StreamURL:     badUpstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:bad-b",
					StreamURL:     badUpstream.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
		// Persist writes are delayed intentionally to exercise finish-path coupling.
		markFailureDelay: 200 * time.Millisecond,
	}

	handler := NewHandler(
		Config{
			Mode:                       "direct",
			StartupTimeout:             2 * time.Second,
			FailoverTotalTimeout:       4 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
			SessionIdleTimeout:         50 * time.Millisecond,
		},
		NewPool(1),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	started := time.Now()
	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil)
	rec := newDeadlineResponseRecorder()
	mux.ServeHTTP(rec, req)
	elapsed := time.Since(started)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	// Without terminal-ready decoupling from persistence drain, two 200ms
	// persistence delays should extend handler completion well past 300ms.
	if elapsed > 300*time.Millisecond {
		t.Fatalf(
			"handler completion elapsed = %s, want <=300ms (decoupled from source-health persistence)",
			elapsed,
		)
	}

	waitFor(t, time.Second, func() bool {
		return len(provider.sourceFailures()) == 2
	})
}

func TestHandlerLogsTuneRejectionReasons(t *testing.T) {
	t.Run("missing guide number", func(t *testing.T) {
		logs := newTestLogBuffer()
		logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

		handler := NewHandler(
			Config{Mode: "direct", Logger: logger},
			NewPool(1),
			&fakeChannelsProvider{
				channelsByGuide: map[string]channels.Channel{},
				sourcesByID:     map[int64][]channels.Source{},
			},
		)

		req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/", nil)
		req.RemoteAddr = "198.51.100.10:40000"
		rec := newDeadlineResponseRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}

		text := logs.String()
		if !strings.Contains(text, "stream tune rejected") || !strings.Contains(text, "reason=missing_guide_number") {
			t.Fatalf("missing expected tune rejection reason in logs: %s", text)
		}
	})

	t.Run("channel not found", func(t *testing.T) {
		logs := newTestLogBuffer()
		logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

		handler := NewHandler(
			Config{Mode: "direct", Logger: logger},
			NewPool(1),
			&fakeChannelsProvider{
				channelsByGuide: map[string]channels.Channel{},
				sourcesByID:     map[int64][]channels.Source{},
			},
		)

		mux := http.NewServeMux()
		mux.Handle("GET /auto/{guide}", handler)

		req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v404", nil)
		req.RemoteAddr = "198.51.100.11:40001"
		rec := newDeadlineResponseRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}

		text := logs.String()
		if !strings.Contains(text, "stream tune rejected") || !strings.Contains(text, "reason=channel_not_found") {
			t.Fatalf("missing expected tune rejection reason in logs: %s", text)
		}
	})

	t.Run("channel disabled", func(t *testing.T) {
		logs := newTestLogBuffer()
		logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

		handler := NewHandler(
			Config{Mode: "direct", Logger: logger},
			NewPool(1),
			&fakeChannelsProvider{
				channelsByGuide: map[string]channels.Channel{
					"101": {ChannelID: 1, GuideNumber: "101", GuideName: "Disabled", Enabled: false},
				},
				sourcesByID: map[int64][]channels.Source{},
			},
		)

		mux := http.NewServeMux()
		mux.Handle("GET /auto/{guide}", handler)

		req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil)
		req.RemoteAddr = "198.51.100.12:40002"
		rec := newDeadlineResponseRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}

		text := logs.String()
		if !strings.Contains(text, "stream tune rejected") || !strings.Contains(text, "reason=channel_disabled") {
			t.Fatalf("missing expected tune rejection reason in logs: %s", text)
		}
	})

	t.Run("subscribe failure no tuners", func(t *testing.T) {
		logs := newTestLogBuffer()
		logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

		pool := NewPool(1)
		lease, err := pool.Acquire(context.Background(), "999", "198.51.100.50:49999")
		if err != nil {
			t.Fatalf("Acquire() setup error = %v", err)
		}
		defer lease.Release()

		handler := NewHandler(
			Config{Mode: "direct", Logger: logger},
			pool,
			&fakeChannelsProvider{
				channelsByGuide: map[string]channels.Channel{
					"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
				},
				sourcesByID: map[int64][]channels.Source{
					1: {
						{
							SourceID:      10,
							ChannelID:     1,
							ItemKey:       "src:news:primary",
							StreamURL:     "http://example.com/stream.ts",
							PriorityIndex: 0,
							Enabled:       true,
						},
					},
				},
			},
		)

		mux := http.NewServeMux()
		mux.Handle("GET /auto/{guide}", handler)

		req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil)
		req.RemoteAddr = "198.51.100.13:40003"
		rec := newDeadlineResponseRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
		}

		text := logs.String()
		if !strings.Contains(text, "stream tune rejected") || !strings.Contains(text, "reason=no_tuners") {
			t.Fatalf("missing expected tune rejection reason in logs: %s", text)
		}
	})

	t.Run("subscribe failure no sources", func(t *testing.T) {
		logs := newTestLogBuffer()
		logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

		handler := NewHandler(
			Config{Mode: "direct", Logger: logger},
			NewPool(1),
			&fakeChannelsProvider{
				channelsByGuide: map[string]channels.Channel{
					"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
				},
				sourcesByID: map[int64][]channels.Source{
					1: {},
				},
			},
		)

		mux := http.NewServeMux()
		mux.Handle("GET /auto/{guide}", handler)

		req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil)
		req.RemoteAddr = "198.51.100.14:40004"
		rec := newDeadlineResponseRecorder()
		mux.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
		}

		text := logs.String()
		if !strings.Contains(text, "stream tune rejected") || !strings.Contains(text, "reason=no_sources") {
			t.Fatalf("missing expected tune rejection reason in logs: %s", text)
		}
	})
}

func TestHandlerLogsLagContextForSlowSubscriber(t *testing.T) {
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 4096; i++ {
			if _, err := w.Write([]byte("x")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:lag",
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
			Logger:                     logger,
			StartupTimeout:             1500 * time.Millisecond,
			FailoverTotalTimeout:       2 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           1,
			BufferPublishFlushInterval: 1 * time.Millisecond,
			SubscriberJoinLagBytes:     1,
			SubscriberSlowClientPolicy: slowClientPolicyDisconnect,
			SessionIdleTimeout:         25 * time.Millisecond,
		},
		NewPool(1),
		provider,
	)

	reqCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil).WithContext(reqCtx)
	req.RemoteAddr = "198.51.100.20:41234"
	writer := &slowResponseWriter{writeDelay: 20 * time.Millisecond}
	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)
	mux.ServeHTTP(writer, req)

	if writer.statusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", writer.statusCode, http.StatusOK)
	}

	text := logs.String()
	if !strings.Contains(text, "stream subscriber disconnected due to lag") {
		t.Fatalf("missing lag disconnect log: %s", text)
	}
	for _, fragment := range []string{
		"requested_seq=",
		"oldest_seq=",
		"lag_chunks=",
		"ring_next_seq=",
		"ring_buffered_chunks=",
		"ring_buffered_bytes=",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("lag log missing %q: %s", fragment, text)
		}
	}
}

func TestHandlerTunerStatusSnapshotTracksSharedSubscribers(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 400; i++ {
			_, _ = w.Write([]byte("x"))
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
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

	handler := NewHandler(
		Config{
			Mode:                       "direct",
			StartupTimeout:             2 * time.Second,
			FailoverTotalTimeout:       5 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           1,
			BufferPublishFlushInterval: 20 * time.Millisecond,
			SessionIdleTimeout:         500 * time.Millisecond,
		},
		NewPool(2),
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	runClient := func(remoteAddr string) {
		reqCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil).WithContext(reqCtx)
		req.RemoteAddr = remoteAddr
		rec := newDeadlineResponseRecorder()
		mux.ServeHTTP(rec, req)
	}

	doneCh := make(chan struct{}, 2)
	go func() {
		runClient("192.168.1.10:40001")
		doneCh <- struct{}{}
	}()
	go func() {
		runClient("192.168.1.11:40002")
		doneCh <- struct{}{}
	}()

	var snapshot TunerStatusSnapshot
	waitFor(t, time.Second, func() bool {
		snapshot = handler.TunerStatusSnapshot()
		return len(snapshot.ClientStreams) >= 2
	})

	if len(snapshot.Tuners) != 1 {
		t.Fatalf("len(snapshot.tuners) = %d, want 1 shared tuner session", len(snapshot.Tuners))
	}
	if got := snapshot.Tuners[0].GuideNumber; got != "101" {
		t.Fatalf("snapshot.tuners[0].guide_number = %q, want 101", got)
	}
	if got := snapshot.Tuners[0].State; got != tunerStateActiveSubscribers {
		t.Fatalf("snapshot.tuners[0].state = %q, want %q", got, tunerStateActiveSubscribers)
	}
	if got := len(snapshot.Tuners[0].Subscribers); got != 2 {
		t.Fatalf("len(snapshot.tuners[0].subscribers) = %d, want 2", got)
	}

	addresses := map[string]bool{}
	for _, stream := range snapshot.ClientStreams {
		addresses[stream.ClientAddr] = true
	}
	if !addresses["192.168.1.10:40001"] || !addresses["192.168.1.11:40002"] {
		t.Fatalf("snapshot client addresses = %#v, want both test clients", addresses)
	}

	<-doneCh
	<-doneCh
}

func TestHandlerThirdClientJoinSharedChannelDoesNotEOFExistingClientWhenOtherTunerActive(t *testing.T) {
	newUpstream := func(payload byte) *httptest.Server {
		chunk := bytes.Repeat([]byte{payload}, 188)
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			ticker := time.NewTicker(fastStreamTestTiming.upstreamChunkInterval)
			defer ticker.Stop()
			for i := 0; i < 4000; i++ {
				if _, err := w.Write(chunk); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
				<-ticker.C
			}
		}))
	}

	upstreamA := newUpstream('A')
	defer upstreamA.Close()
	upstreamB := newUpstream('B')
	defer upstreamB.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News 101", Enabled: true},
			"102": {ChannelID: 2, GuideNumber: "102", GuideName: "News 102", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:101",
					StreamURL:     upstreamA.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
			2: {
				{
					SourceID:      20,
					ChannelID:     2,
					ItemKey:       "src:news:102",
					StreamURL:     upstreamB.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	cfg := fastStreamTestTiming.handlerConfig("direct")
	cfg.BufferChunkBytes = 188
	handler := NewHandler(cfg, NewPool(2), provider)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)
	server := httptest.NewServer(mux)
	defer server.Close()

	type streamClient struct {
		cancel    context.CancelFunc
		errCh     chan error
		bytesRead atomic.Int64
	}

	startClient := func(path string) (*streamClient, error) {
		ctx, cancel := context.WithCancel(context.Background())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path, nil)
		if err != nil {
			cancel()
			return nil, err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			cancel()
			return nil, errors.New("unexpected status " + resp.Status)
		}

		client := &streamClient{
			cancel: cancel,
			errCh:  make(chan error, 1),
		}
		go func() {
			defer resp.Body.Close()
			buf := make([]byte, 16*1024)
			for {
				n, err := resp.Body.Read(buf)
				if n > 0 {
					client.bytesRead.Add(int64(n))
				}
				if err != nil {
					client.errCh <- err
					return
				}
			}
		}()

		return client, nil
	}

	stopClient := func(client *streamClient) {
		if client == nil {
			return
		}
		client.cancel()
		select {
		case <-client.errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for stream client to stop")
		}
	}

	for attempt := 1; attempt <= fastStreamTestTiming.thirdClientAttemptsDirect; attempt++ {
		clientA, err := startClient("/auto/v101")
		if err != nil {
			t.Fatalf("attempt %d: start client A error = %v", attempt, err)
		}
		clientB1, err := startClient("/auto/v102")
		if err != nil {
			stopClient(clientA)
			t.Fatalf("attempt %d: start first client B error = %v", attempt, err)
		}

		waitFor(t, 1500*time.Millisecond, func() bool {
			return clientA.bytesRead.Load() > 0 && clientB1.bytesRead.Load() > 0
		})

		clientB2, err := startClient("/auto/v102")
		if err != nil {
			stopClient(clientA)
			stopClient(clientB1)
			t.Fatalf("attempt %d: start second client B error = %v", attempt, err)
		}
		waitFor(t, 1500*time.Millisecond, func() bool {
			return clientB2.bytesRead.Load() > 0
		})

		assertClientStillStreaming := func(phase string) {
			baseline := clientB1.bytesRead.Load()
			deadline := time.Now().Add(fastStreamTestTiming.progressWindow)
			for time.Now().Before(deadline) {
				select {
				case err := <-clientB1.errCh:
					stopClient(clientA)
					t.Fatalf("attempt %d: first shared-channel client ended early %s: %v", attempt, phase, err)
				default:
				}
				if clientB1.bytesRead.Load() > baseline {
					return
				}
				time.Sleep(fastStreamTestTiming.pollInterval)
			}
			select {
			case err := <-clientB1.errCh:
				stopClient(clientA)
				t.Fatalf("attempt %d: first shared-channel client ended early %s: %v", attempt, phase, err)
			default:
			}
			if clientB1.bytesRead.Load() <= baseline {
				stopClient(clientA)
				t.Fatalf("attempt %d: first shared-channel client made no forward progress %s", attempt, phase)
			}
		}
		assertClientStillStreaming("after second joined")

		stopClient(clientB2)

		assertClientStillStreaming("after second disconnected")

		stopClient(clientA)
		stopClient(clientB1)
	}
}

func TestOrderSourcesByAvailability(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	ordered := orderSourcesByAvailability([]channels.Source{
		{SourceID: 1, CooldownUntil: now.Add(2 * time.Minute).Unix()},
		{SourceID: 2, CooldownUntil: 0},
		{SourceID: 3, CooldownUntil: now.Add(-1 * time.Second).Unix()},
	}, now)

	if len(ordered) != 3 {
		t.Fatalf("len(ordered) = %d, want 3", len(ordered))
	}
	if ordered[0].SourceID != 2 || ordered[1].SourceID != 3 || ordered[2].SourceID != 1 {
		t.Fatalf("ordered source ids = [%d,%d,%d], want [2,3,1]", ordered[0].SourceID, ordered[1].SourceID, ordered[2].SourceID)
	}

	allCooling := orderSourcesByAvailability([]channels.Source{
		{SourceID: 10, CooldownUntil: now.Add(1 * time.Minute).Unix()},
		{SourceID: 11, CooldownUntil: now.Add(2 * time.Minute).Unix()},
	}, now)
	if allCooling[0].SourceID != 10 || allCooling[1].SourceID != 11 {
		t.Fatalf("all-cooling order = [%d,%d], want original order [10,11]", allCooling[0].SourceID, allCooling[1].SourceID)
	}
}

func TestOrderSourcesByAvailabilityDemotesRecentAndRepeatedFailures(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	ordered := orderSourcesByAvailability([]channels.Source{
		{
			SourceID:      10,
			PriorityIndex: 0,
			FailCount:     3,
			LastFailAt:    now.Add(-1 * time.Minute).Unix(),
		},
		{
			SourceID:      11,
			PriorityIndex: 1,
			SuccessCount:  4,
		},
		{
			SourceID:      12,
			PriorityIndex: 2,
			FailCount:     0,
			LastFailAt:    now.Add(-5 * time.Minute).Unix(),
		},
		{
			SourceID:      13,
			PriorityIndex: 3,
			FailCount:     0,
			LastFailAt:    now.Add(-45 * time.Minute).Unix(),
		},
	}, now)

	if len(ordered) != 4 {
		t.Fatalf("len(ordered) = %d, want 4", len(ordered))
	}
	if ordered[0].SourceID != 11 || ordered[1].SourceID != 13 || ordered[2].SourceID != 12 || ordered[3].SourceID != 10 {
		t.Fatalf(
			"ordered source ids = [%d,%d,%d,%d], want [11,13,12,10]",
			ordered[0].SourceID,
			ordered[1].SourceID,
			ordered[2].SourceID,
			ordered[3].SourceID,
		)
	}
}

func TestLimitSourcesByFailovers(t *testing.T) {
	sources := []channels.Source{
		{SourceID: 1},
		{SourceID: 2},
		{SourceID: 3},
	}

	all := limitSourcesByFailovers(sources, 0)
	if len(all) != 3 {
		t.Fatalf("len(all) = %d, want 3", len(all))
	}

	limited := limitSourcesByFailovers(sources, 1)
	if len(limited) != 2 {
		t.Fatalf("len(limited) = %d, want 2", len(limited))
	}
	if limited[0].SourceID != 1 || limited[1].SourceID != 2 {
		t.Fatalf("limited ids = [%d,%d], want [1,2]", limited[0].SourceID, limited[1].SourceID)
	}
}

func TestFFmpegArgs(t *testing.T) {
	findArg := func(args []string, key string) (int, string, bool) {
		for i := 0; i < len(args)-1; i++ {
			if args[i] == key {
				return i, args[i+1], true
			}
		}
		return -1, "", false
	}
	hasArg := func(args []string, key string) bool {
		for _, arg := range args {
			if arg == key {
				return true
			}
		}
		return false
	}

	copyArgs := ffmpegArgs(
		"ffmpeg-copy",
		"http://example.com/live.m3u8",
		1,
		1,
		32*1024,
		200*time.Millisecond,
		true,
		1500*time.Millisecond,
		3,
		"4xx,5xx",
	)
	if len(copyArgs) == 0 {
		t.Fatal("ffmpegArgs(ffmpeg-copy) returned empty args")
	}
	copyReadRateIndex, copyReadRateValue, ok := findArg(copyArgs, "-readrate")
	if !ok || copyReadRateValue != "1" {
		t.Fatalf("ffmpeg-copy args missing -readrate 1: %#v", copyArgs)
	}
	copyInputIndex, _, ok := findArg(copyArgs, "-i")
	if !ok {
		t.Fatalf("ffmpeg-copy args missing -i: %#v", copyArgs)
	}
	if copyReadRateIndex > copyInputIndex {
		t.Fatalf("ffmpeg-copy args place -readrate after -i: %#v", copyArgs)
	}
	copyProbeSizeIndex, copyProbeSizeValue, ok := findArg(copyArgs, "-probesize")
	if !ok || copyProbeSizeValue != "128000" {
		t.Fatalf("ffmpeg-copy args missing -probesize 128000 floor: %#v", copyArgs)
	}
	if copyProbeSizeIndex > copyInputIndex {
		t.Fatalf("ffmpeg-copy args place -probesize after -i: %#v", copyArgs)
	}
	copyAnalyzeIndex, copyAnalyzeValue, ok := findArg(copyArgs, "-analyzeduration")
	if !ok || copyAnalyzeValue != "1000000" {
		t.Fatalf("ffmpeg-copy args missing -analyzeduration 1000000 floor: %#v", copyArgs)
	}
	if copyAnalyzeIndex > copyInputIndex {
		t.Fatalf("ffmpeg-copy args place -analyzeduration after -i: %#v", copyArgs)
	}
	copyReconnectIndex, copyReconnectValue, ok := findArg(copyArgs, "-reconnect")
	if !ok || copyReconnectValue != "1" {
		t.Fatalf("ffmpeg-copy args missing -reconnect 1: %#v", copyArgs)
	}
	if copyReconnectIndex > copyInputIndex {
		t.Fatalf("ffmpeg-copy args place -reconnect after -i: %#v", copyArgs)
	}
	copyHTTPReconnectIndex, copyHTTPReconnectValue, ok := findArg(copyArgs, "-reconnect_on_http_error")
	if !ok || copyHTTPReconnectValue != "4xx,5xx" {
		t.Fatalf("ffmpeg-copy args missing -reconnect_on_http_error 4xx,5xx: %#v", copyArgs)
	}
	if copyHTTPReconnectIndex > copyInputIndex {
		t.Fatalf("ffmpeg-copy args place -reconnect_on_http_error after -i: %#v", copyArgs)
	}
	copyDelayIndex, copyDelayValue, ok := findArg(copyArgs, "-reconnect_delay_max")
	if !ok || copyDelayValue != "2" {
		t.Fatalf("ffmpeg-copy args missing -reconnect_delay_max 2: %#v", copyArgs)
	}
	if copyDelayIndex > copyInputIndex {
		t.Fatalf("ffmpeg-copy args place -reconnect_delay_max after -i: %#v", copyArgs)
	}
	copyMaxRetriesIndex, copyMaxRetriesValue, ok := findArg(copyArgs, "-reconnect_max_retries")
	if !ok || copyMaxRetriesValue != "3" {
		t.Fatalf("ffmpeg-copy args missing -reconnect_max_retries 3: %#v", copyArgs)
	}
	if copyMaxRetriesIndex > copyInputIndex {
		t.Fatalf("ffmpeg-copy args place -reconnect_max_retries after -i: %#v", copyArgs)
	}
	copyFFlagsIndex, copyFFlagsValue, ok := findArg(copyArgs, "-fflags")
	if !ok || copyFFlagsValue != "+genpts" {
		t.Fatalf("ffmpeg-copy args missing -fflags +genpts: %#v", copyArgs)
	}
	if copyFFlagsIndex > copyInputIndex {
		t.Fatalf("ffmpeg-copy args place -fflags after -i: %#v", copyArgs)
	}
	if got := copyArgs[len(copyArgs)-1]; got != "pipe:1" {
		t.Fatalf("ffmpeg-copy args tail = %q, want pipe:1", got)
	}

	copyNoRegenArgs := ffmpegArgsWithCopyTimestampRegeneration(
		"ffmpeg-copy",
		"http://example.com/live.m3u8",
		1,
		1,
		32*1024,
		200*time.Millisecond,
		true,
		1500*time.Millisecond,
		3,
		"4xx,5xx",
		false,
	)
	if hasArg(copyNoRegenArgs, "-fflags") {
		t.Fatalf("ffmpeg-copy args unexpectedly include -fflags when regeneration disabled: %#v", copyNoRegenArgs)
	}

	transcodeArgs := ffmpegArgs(
		"ffmpeg-transcode",
		"http://example.com/live.m3u8",
		1,
		1,
		32*1024,
		200*time.Millisecond,
		true,
		1500*time.Millisecond,
		3,
		"4xx,5xx",
	)
	if len(transcodeArgs) == 0 {
		t.Fatal("ffmpegArgs(ffmpeg-transcode) returned empty args")
	}
	_, transcodeReadRateValue, ok := findArg(transcodeArgs, "-readrate")
	if !ok || transcodeReadRateValue != "1" {
		t.Fatalf("ffmpeg-transcode args missing -readrate 1: %#v", transcodeArgs)
	}
	transcodeInputIndex, _, ok := findArg(transcodeArgs, "-i")
	if !ok {
		t.Fatalf("ffmpeg-transcode args missing -i: %#v", transcodeArgs)
	}
	transcodeReconnectIndex, transcodeReconnectValue, ok := findArg(transcodeArgs, "-reconnect")
	if !ok || transcodeReconnectValue != "1" {
		t.Fatalf("ffmpeg-transcode args missing -reconnect 1: %#v", transcodeArgs)
	}
	if transcodeReconnectIndex > transcodeInputIndex {
		t.Fatalf("ffmpeg-transcode args place -reconnect after -i: %#v", transcodeArgs)
	}
	if hasArg(transcodeArgs, "-fflags") {
		t.Fatalf("ffmpeg-transcode args unexpectedly include -fflags: %#v", transcodeArgs)
	}
	_, transcodeCodecValue, ok := findArg(transcodeArgs, "-c:v")
	if !ok || transcodeCodecValue != "libx264" {
		t.Fatalf("ffmpeg-transcode args missing -c:v libx264: %#v", transcodeArgs)
	}

	disabledReconnectArgs := ffmpegArgs(
		"ffmpeg-copy",
		"http://example.com/live.m3u8",
		1,
		1,
		32*1024,
		200*time.Millisecond,
		false,
		2*time.Second,
		3,
		"4xx,5xx",
	)
	if hasArg(disabledReconnectArgs, "-reconnect") {
		t.Fatalf("ffmpeg-copy disabled reconnect args unexpectedly include -reconnect: %#v", disabledReconnectArgs)
	}
	if hasArg(disabledReconnectArgs, "-reconnect_on_http_error") {
		t.Fatalf("ffmpeg-copy disabled reconnect args unexpectedly include -reconnect_on_http_error: %#v", disabledReconnectArgs)
	}
	if hasArg(disabledReconnectArgs, "-reconnect_delay_max") {
		t.Fatalf("ffmpeg-copy disabled reconnect args unexpectedly include -reconnect_delay_max: %#v", disabledReconnectArgs)
	}
	if hasArg(disabledReconnectArgs, "-reconnect_max_retries") {
		t.Fatalf("ffmpeg-copy disabled reconnect args unexpectedly include -reconnect_max_retries: %#v", disabledReconnectArgs)
	}
}

// TestHandlerHTTPShutdownTimeoutForceClosesActiveStreams validates the
// assumption that main.go's shutdown sequence depends on: http.Server.Shutdown()
// with a timeout does NOT terminate active streaming connections. Only
// Server.Close() force-closes them. main.go calls srv.Close() after Shutdown
// timeout to ensure active streams are terminated before deferred store.Close().
func TestHandlerHTTPShutdownTimeoutForceClosesActiveStreams(t *testing.T) {
	// Upstream that streams indefinitely.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("X"), mpegTSPacketSize)
		for {
			if _, err := w.Write(payload); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "ShutdownTest", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {{SourceID: 10, ChannelID: 1, ItemKey: "src:shutdown", StreamURL: upstream.URL, Enabled: true}},
		},
	}

	pool := NewPool(1)
	handler := NewHandler(
		Config{
			Mode:                       "direct",
			StartupTimeout:             2 * time.Second,
			FailoverTotalTimeout:       5 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
			SessionIdleTimeout:         200 * time.Millisecond,
		},
		pool,
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	// Use a real HTTP server (not httptest.NewRecorder) to test shutdown behavior.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	// Start a streaming client connection.
	streamStarted := make(chan struct{})
	streamDone := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String() + "/auto/v101")
		if err != nil {
			streamDone <- err
			return
		}
		close(streamStarted)
		defer resp.Body.Close()
		buf := make([]byte, 4096)
		for {
			if _, err := resp.Body.Read(buf); err != nil {
				streamDone <- err
				return
			}
		}
	}()

	// Wait for the stream to start.
	select {
	case <-streamStarted:
	case err := <-streamDone:
		t.Fatalf("stream failed before starting: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream to start")
	}

	// Verify tuner is held.
	if pool.InUseCount() == 0 {
		t.Fatal("tuner should be in use during active stream")
	}

	// Call Shutdown with a very short timeout. Shutdown waits for active
	// connections to complete; when it times out, it returns context.DeadlineExceeded
	// but does NOT close active connections or cancel request contexts.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer shutdownCancel()
	shutdownErr := srv.Shutdown(shutdownCtx)

	if shutdownErr == nil {
		t.Fatal("expected Shutdown() to time out with active stream, but it returned nil")
	}

	// After Shutdown() timeout, the stream must still be alive because
	// Shutdown only stops accepting new connections — it does not force-close
	// existing ones. main.go must call srv.Close() after timeout to terminate.
	select {
	case <-streamDone:
		t.Fatal("stream terminated after Shutdown() timeout alone; " +
			"expected it to remain alive until Close() is called")
	case <-time.After(200 * time.Millisecond):
		// Stream confirmed alive 200ms after Shutdown timeout.
	}

	// Close() force-terminates the listener and all active connections.
	// main.go calls this after Shutdown timeout to ensure streams end.
	srv.Close()

	select {
	case <-streamDone:
	case <-time.After(3 * time.Second):
		t.Fatal("active stream was not terminated after Server.Close()")
	}

	// Tuner should be released after stream termination + idle timeout.
	waitFor(t, 3*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

// TestHandlerSharedSessionOutlivesSubscriberDisconnect verifies the structural
// lifecycle property that shared sessions hold tuner leases during the idle
// grace period after all subscribers disconnect. main.go calls
// streamHandler.Close() during shutdown to cancel these sessions before
// store.Close() runs. This test validates the precondition: without explicit
// cancellation, sessions remain active after subscriber disconnect.
func TestHandlerSharedSessionOutlivesSubscriberDisconnect(t *testing.T) {
	// Upstream that streams until closed.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("D"), mpegTSPacketSize)
		for {
			if _, err := w.Write(payload); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "StoreCloseTest", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {{SourceID: 10, ChannelID: 1, ItemKey: "src:storeclose", StreamURL: upstream.URL, Enabled: true}},
		},
	}

	pool := NewPool(1)
	// Use a long idle timeout (1s) to create a wide window where the session
	// holds resources after subscriber disconnect — simulating the default 5s
	// idle timeout in production.
	handler := NewHandler(
		Config{
			Mode:                       "direct",
			StartupTimeout:             2 * time.Second,
			FailoverTotalTimeout:       5 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           64,
			BufferPublishFlushInterval: 20 * time.Millisecond,
			SessionIdleTimeout:         1 * time.Second,
		},
		pool,
		provider,
	)

	mux := http.NewServeMux()
	mux.Handle("GET /auto/{guide}", handler)

	// Start a stream request.
	reqCtx, reqCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer reqCancel()
	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil).WithContext(reqCtx)
	req.RemoteAddr = "192.168.1.55:45678"

	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		rec := newDeadlineResponseRecorder()
		mux.ServeHTTP(rec, req)
	}()

	// Wait for the stream to become active (tuner in use).
	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() > 0
	})

	// Disconnect the subscriber by canceling the request context.
	// This simulates HTTP server shutdown canceling request contexts.
	reqCancel()

	select {
	case <-streamDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stream handler did not return after request cancellation")
	}

	// After subscriber disconnects, the tuner should STILL be held by the
	// idle-grace session. This is the lifecycle gap: in production, store.Close()
	// runs during this window while the session (rooted at context.Background())
	// still holds resources and can make persistence calls.
	if pool.InUseCount() == 0 {
		t.Fatal("tuner was released immediately after subscriber disconnect; " +
			"expected session idle grace to hold tuner (proving lifecycle gap)")
	}

	// The session should eventually release the tuner after idle timeout.
	// In production, store.Close() would run BEFORE this timeout expires,
	// creating the race condition where persistence calls hit a closed store.
	waitFor(t, 3*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}
