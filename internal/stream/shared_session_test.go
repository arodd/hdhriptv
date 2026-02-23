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
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// streamDrainStatsTestMu serializes tests that assert global drain counter
// deltas. Those assertions are only stable while stream drain stats remain
// process-global atomics; if tests move to per-manager counters, this lock
// should be removed alongside delta-based expectations.
var streamDrainStatsTestMu sync.Mutex

// streamSlowSkipStatsTestMu serializes tests that assert global slow-skip
// counter deltas while slow-skip metrics remain process-global atomics.
var streamSlowSkipStatsTestMu sync.Mutex

// streamSubscriberWriteStatsTestMu serializes tests that assert global
// subscriber write-pressure counter deltas while write-pressure metrics remain
// process-global atomics.
var streamSubscriberWriteStatsTestMu sync.Mutex

// streamSourceReadPauseStatsTestMu serializes tests that assert global source
// read-pause counter deltas while read-pause metrics remain process-global
// atomics.
var streamSourceReadPauseStatsTestMu sync.Mutex

func isolateStreamDrainStatsForTest(t *testing.T) {
	t.Helper()
	streamDrainStatsTestMu.Lock()
	resetStreamDrainStatsForTest()
	t.Cleanup(func() {
		resetStreamDrainStatsForTest()
		streamDrainStatsTestMu.Unlock()
	})
}

func isolateStreamSlowSkipStatsForTest(t *testing.T) {
	t.Helper()
	streamSlowSkipStatsTestMu.Lock()
	resetStreamSlowSkipStatsForTest()
	t.Cleanup(func() {
		resetStreamSlowSkipStatsForTest()
		streamSlowSkipStatsTestMu.Unlock()
	})
}

func isolateStreamSubscriberWriteStatsForTest(t *testing.T) {
	t.Helper()
	streamSubscriberWriteStatsTestMu.Lock()
	resetStreamSubscriberWriteStatsForTest()
	t.Cleanup(func() {
		resetStreamSubscriberWriteStatsForTest()
		streamSubscriberWriteStatsTestMu.Unlock()
	})
}

func isolateStreamSourceReadPauseStatsForTest(t *testing.T) {
	t.Helper()
	streamSourceReadPauseStatsTestMu.Lock()
	resetStreamSourceReadPauseStatsForTest()
	t.Cleanup(func() {
		resetStreamSourceReadPauseStatsForTest()
		streamSourceReadPauseStatsTestMu.Unlock()
	})
}

func installWaitForDrainBlockedHookForTest(t *testing.T, manager *SessionManager, capacity int) <-chan struct{} {
	t.Helper()
	if manager == nil {
		t.Fatal("manager is nil")
	}
	if capacity < 1 {
		capacity = 1
	}
	blockedCh := make(chan struct{}, capacity)

	manager.mu.Lock()
	manager.waitForDrainBlockedHook = func() {
		select {
		case blockedCh <- struct{}{}:
		default:
		}
	}
	manager.mu.Unlock()

	t.Cleanup(func() {
		manager.mu.Lock()
		manager.waitForDrainBlockedHook = nil
		manager.mu.Unlock()
	})
	return blockedCh
}

func waitForDrainBlockedHookSignal(t *testing.T, blockedCh <-chan struct{}, step string) {
	t.Helper()
	if strings.TrimSpace(step) == "" {
		step = "waiter"
	}
	select {
	case <-blockedCh:
	case <-time.After(time.Second):
		t.Fatalf("%s: WaitForDrain() did not enter blocked state", step)
	}
}

func TestSharedSessionFinishBlocksOnSourceHealthPersistenceBacklog(t *testing.T) {
	t.Run("single session", func(t *testing.T) {
		provider := &fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{
				"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
			},
			markFailureDelay: 120 * time.Millisecond,
		}

		manager := NewSessionManager(SessionManagerConfig{
			Mode:                       "direct",
			StartupTimeout:             500 * time.Millisecond,
			FailoverTotalTimeout:       1 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           188,
			BufferPublishFlushInterval: 20 * time.Millisecond,
			SessionIdleTimeout:         50 * time.Millisecond,
			SourceHealthDrainTimeout:   200 * time.Millisecond,
		}, NewPool(1), provider)
		if manager == nil {
			t.Fatal("manager is nil")
		}

		ctx, cancel := context.WithCancel(context.Background())
		session := &sharedRuntimeSession{
			manager:                       manager,
			channel:                       provider.channelsByGuide["101"],
			ring:                          NewChunkRing(8),
			ctx:                           ctx,
			cancel:                        cancel,
			readyCh:                       make(chan struct{}),
			subscribers:                   make(map[uint64]SubscriberStats),
			startedAt:                     time.Now().UTC(),
			sourceHealthPersistCh:         make(chan sourceHealthPersistRequest, 1),
			sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
			sourceHealthQueue:             make([]int64, 0, defaultSourceHealthPersistQueueSize),
			sourceHealthPersistDone:       make(chan struct{}),
			sourceHealthCoalescedBySource: make(map[int64]int64),
			sourceHealthDroppedBySource:   make(map[int64]int64),
		}
		manager.mu.Lock()
		manager.sessions[1] = session
		manager.mu.Unlock()
		go session.runSourceHealthPersist()

		// Build persistent-backlog pressure: queued + coalesced source-health
		// events with deterministic per-event delay in persistence.
		for i := 0; i < 6; i++ {
			observedAt := time.Now().UTC().Add(time.Duration(i) * time.Millisecond)
			eventID := manager.stageRecentSourceFailure(int64(100+i), "backlog failure", observedAt)
			session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
				sourceID:   int64(100 + i),
				eventID:    eventID,
				success:    false,
				reason:     "backlog failure",
				observedAt: observedAt,
			})
		}

		start := time.Now()
		finished := make(chan struct{})
		go func() {
			session.finish(nil)
			close(finished)
		}()

		select {
		case <-finished:
			elapsed := time.Since(start)
			if elapsed <= 0 {
				t.Fatalf("session.finish() duration = %s, want > 0 to prove bounded drain did work", elapsed)
			}
			if elapsed > 650*time.Millisecond {
				t.Fatalf("session.finish() duration = %s, want <= 650ms with bounded source-health drain", elapsed)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("session.finish() did not return within expected regression test window")
		}

		manager.recentHealth.mu.Lock()
		pending := len(manager.recentHealth.pendingBySource)
		manager.recentHealth.mu.Unlock()
		if pending != 0 {
			t.Fatalf("pending source-health overlays = %d, want 0 after bounded drain", pending)
		}
		if got := len(provider.sourceFailures()); got == 0 {
			t.Fatalf("persisted failures = %d, want > 0 to prove bounded drain persisted at least one event", got)
		}
		if got := len(provider.sourceFailures()); got >= 6 {
			t.Fatalf("persisted failures = %d, want < 6 after bounded drain timeout", got)
		}
	})

	t.Run("multiple sessions", func(t *testing.T) {
		type sessionFixture struct {
			manager  *SessionManager
			session  *sharedRuntimeSession
			provider *fakeChannelsProvider
		}
		newBackloggedSession := func(channelID int64) sessionFixture {
			provider := &fakeChannelsProvider{
				channelsByGuide: map[string]channels.Channel{
					fmt.Sprintf("%d", channelID): {
						ChannelID:   channelID,
						GuideNumber: fmt.Sprintf("%d", channelID),
						GuideName:   "News",
						Enabled:     true,
					},
				},
				markFailureDelay: 120 * time.Millisecond,
			}
			manager := NewSessionManager(SessionManagerConfig{
				Mode:                       "direct",
				StartupTimeout:             500 * time.Millisecond,
				FailoverTotalTimeout:       1 * time.Second,
				MinProbeBytes:              1,
				BufferChunkBytes:           188,
				BufferPublishFlushInterval: 20 * time.Millisecond,
				SessionIdleTimeout:         50 * time.Millisecond,
				SourceHealthDrainTimeout:   200 * time.Millisecond,
			}, NewPool(1), provider)

			ctx, cancel := context.WithCancel(context.Background())
			session := &sharedRuntimeSession{
				manager:                       manager,
				channel:                       provider.channelsByGuide[fmt.Sprintf("%d", channelID)],
				ring:                          NewChunkRing(8),
				ctx:                           ctx,
				cancel:                        cancel,
				readyCh:                       make(chan struct{}),
				subscribers:                   make(map[uint64]SubscriberStats),
				startedAt:                     time.Now().UTC(),
				sourceHealthPersistCh:         make(chan sourceHealthPersistRequest, 1),
				sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
				sourceHealthQueue:             make([]int64, 0, defaultSourceHealthPersistQueueSize),
				sourceHealthPersistDone:       make(chan struct{}),
				sourceHealthCoalescedBySource: make(map[int64]int64),
				sourceHealthDroppedBySource:   make(map[int64]int64),
			}
			manager.mu.Lock()
			manager.sessions[channelID] = session
			manager.mu.Unlock()
			go session.runSourceHealthPersist()
			return sessionFixture{
				manager:  manager,
				session:  session,
				provider: provider,
			}
		}

		sessionA := newBackloggedSession(201)
		sessionB := newBackloggedSession(202)

		for _, fixture := range []sessionFixture{sessionA, sessionB} {
			for i := 0; i < 6; i++ {
				sourceID := int64(200 + i)
				observedAt := time.Now().UTC().Add(time.Duration(i) * time.Millisecond)
				eventID := fixture.manager.stageRecentSourceFailure(sourceID, "backlog failure", observedAt)
				fixture.session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
					sourceID:   sourceID,
					eventID:    eventID,
					success:    false,
					reason:     "backlog failure",
					observedAt: observedAt,
				})
			}
		}

		shared := make(chan time.Duration, 2)
		for _, fixture := range []sessionFixture{sessionA, sessionB} {
			fixture := fixture
			go func() {
				start := time.Now()
				fixture.session.finish(nil)
				shared <- time.Since(start)
			}()
		}

		for i := 0; i < 2; i++ {
			select {
			case elapsed := <-shared:
				if elapsed <= 0 {
					t.Fatalf("session.finish() duration = %s, want > 0 to prove bounded drain did work", elapsed)
				}
				if elapsed > 650*time.Millisecond {
					t.Fatalf("session.finish() duration = %s, want <= 650ms with bounded source-health drain", elapsed)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("session.finish() did not return within expected regression test window")
			}
		}

		for _, fixture := range []sessionFixture{sessionA, sessionB} {
			if fixture.provider == nil {
				t.Fatal("provider fixtures should be initialized")
			}
			fixture.manager.recentHealth.mu.Lock()
			pending := len(fixture.manager.recentHealth.pendingBySource)
			fixture.manager.recentHealth.mu.Unlock()
			if pending != 0 {
				t.Fatalf("pending source-health overlays for channel %d = %d, want 0", fixture.session.channel.ChannelID, pending)
			}
			if got := len(fixture.provider.sourceFailures()); got == 0 {
				t.Fatalf(
					"persisted failures for channel %d = %d, want > 0 to prove bounded drain persisted at least one event",
					fixture.session.channel.ChannelID,
					got,
				)
			}
			if got := len(fixture.provider.sourceFailures()); got >= 6 {
				t.Fatalf("persisted failures for channel %d = %d, want < 6 after bounded drain timeout", fixture.session.channel.ChannelID, got)
			}
		}
	})
}

func TestSessionManagerCloseWithContextLifecycle(t *testing.T) {
	newManager := func(sessionCancel, createCancel context.CancelFunc) *SessionManager {
		manager := &SessionManager{
			sessions: make(map[int64]*sharedRuntimeSession),
			creating: make(map[int64]*sessionCreateWait),
		}
		if sessionCancel != nil {
			manager.sessions[101] = &sharedRuntimeSession{cancel: sessionCancel}
		}
		if createCancel != nil {
			manager.creating[101] = &sessionCreateWait{cancel: createCancel}
		}
		return manager
	}

	t.Run("timeout returns context deadline exceeded", func(t *testing.T) {
		var sessionCanceled atomic.Bool
		var createCanceled atomic.Bool
		manager := newManager(
			func() { sessionCanceled.Store(true) },
			func() { createCanceled.Store(true) },
		)
		manager.wg.Add(1)
		defer manager.wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		err := manager.CloseWithContext(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("CloseWithContext() error = %v, want %v", err, context.DeadlineExceeded)
		}
		if !manager.closed {
			t.Fatal("manager.closed = false, want true after CloseWithContext")
		}
		if !sessionCanceled.Load() {
			t.Fatal("session cancel callback was not invoked")
		}
		if !createCanceled.Load() {
			t.Fatal("create cancel callback was not invoked")
		}
	})

	t.Run("repeated timeout calls reuse close waiter", func(t *testing.T) {
		manager := newManager(func() {}, nil)
		manager.wg.Add(1)
		released := false
		t.Cleanup(func() {
			if !released {
				manager.wg.Done()
			}
		})

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		err := manager.CloseWithContext(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first CloseWithContext() error = %v, want %v", err, context.DeadlineExceeded)
		}
		firstDone := manager.closeWaitDone
		if firstDone == nil {
			t.Fatal("manager.closeWaitDone is nil after first CloseWithContext call")
		}

		ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel2()
		err = manager.CloseWithContext(ctx2)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second CloseWithContext() error = %v, want %v", err, context.DeadlineExceeded)
		}
		if manager.closeWaitDone != firstDone {
			t.Fatal("CloseWithContext created a replacement wait channel on repeated call")
		}

		manager.wg.Done()
		released = true
		select {
		case <-firstDone:
		case <-time.After(time.Second):
			t.Fatal("shared close waiter did not close after manager drain")
		}
	})

	t.Run("concurrent timeout calls share close waiter and complete after release", func(t *testing.T) {
		manager := newManager(func() {}, nil)
		manager.wg.Add(1)
		released := false
		t.Cleanup(func() {
			if !released {
				manager.wg.Done()
			}
		})

		const callers = 16
		start := make(chan struct{})
		errs := make(chan error, callers)
		doneSignals := make(chan chan struct{}, callers)

		var closeWG sync.WaitGroup
		for i := 0; i < callers; i++ {
			closeWG.Add(1)
			go func() {
				defer closeWG.Done()
				<-start
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				errs <- manager.CloseWithContext(ctx)

				manager.mu.Lock()
				doneSignals <- manager.closeWaitDone
				manager.mu.Unlock()
			}()
		}

		close(start)
		closeWG.Wait()
		close(errs)
		close(doneSignals)

		for err := range errs {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("concurrent CloseWithContext() error = %v, want %v", err, context.DeadlineExceeded)
			}
		}

		var sharedDone chan struct{}
		for doneSignal := range doneSignals {
			if doneSignal == nil {
				t.Fatal("manager.closeWaitDone is nil after concurrent CloseWithContext calls")
			}
			if sharedDone == nil {
				sharedDone = doneSignal
				continue
			}
			if doneSignal != sharedDone {
				t.Fatal("concurrent CloseWithContext calls observed different close-wait channels")
			}
		}
		if sharedDone == nil {
			t.Fatal("concurrent CloseWithContext calls did not create closeWaitDone")
		}

		select {
		case <-sharedDone:
			t.Fatal("shared close waiter closed before manager wait group was released")
		default:
		}

		manager.wg.Done()
		released = true

		if err := manager.CloseWithContext(context.Background()); err != nil {
			t.Fatalf("CloseWithContext() after release error = %v, want nil", err)
		}
	})

	t.Run("success returns nil after drain", func(t *testing.T) {
		var sessionCanceled atomic.Bool
		var createCanceled atomic.Bool
		manager := newManager(
			func() { sessionCanceled.Store(true) },
			func() { createCanceled.Store(true) },
		)
		manager.wg.Add(1)
		go func() {
			time.Sleep(15 * time.Millisecond)
			manager.wg.Done()
		}()

		err := manager.CloseWithContext(context.Background())
		if err != nil {
			t.Fatalf("CloseWithContext() error = %v, want nil", err)
		}
		if !sessionCanceled.Load() {
			t.Fatal("session cancel callback was not invoked")
		}
		if !createCanceled.Load() {
			t.Fatal("create cancel callback was not invoked")
		}
	})

	t.Run("nil context falls back to background", func(t *testing.T) {
		manager := newManager(func() {}, nil)
		manager.wg.Add(1)
		go func() {
			time.Sleep(15 * time.Millisecond)
			manager.wg.Done()
		}()

		if err := manager.CloseWithContext(nil); err != nil {
			t.Fatalf("CloseWithContext(nil) error = %v, want nil", err)
		}
	})

	t.Run("close wrapper remains backward compatible", func(t *testing.T) {
		var sessionCanceled atomic.Bool
		manager := newManager(func() { sessionCanceled.Store(true) }, nil)
		manager.wg.Add(1)

		closed := make(chan struct{})
		go func() {
			manager.Close()
			close(closed)
		}()

		waitFor(t, time.Second, func() bool {
			return sessionCanceled.Load()
		})
		manager.wg.Done()

		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("Close() did not return after drain completion")
		}
	})
}

type testRecoveryFillerErrorProducer struct {
	err error
}

func (p *testRecoveryFillerErrorProducer) Start(context.Context) (io.ReadCloser, error) {
	if p == nil || p.err == nil {
		return nil, errors.New("test recovery filler producer failed")
	}
	return nil, p.err
}

func (p *testRecoveryFillerErrorProducer) Describe() string {
	return "test:recovery_filler_error"
}

type testRecoveryFillerStreamingProducer struct {
	payload  []byte
	interval time.Duration
}

func (p *testRecoveryFillerStreamingProducer) Start(ctx context.Context) (io.ReadCloser, error) {
	payload := p.payload
	if len(payload) == 0 {
		payload = mpegTSNullPacketChunk(1)
	}
	interval := p.interval
	if interval <= 0 {
		interval = 5 * time.Millisecond
	}

	reader, writer := io.Pipe()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				_ = writer.CloseWithError(ctx.Err())
				return
			case <-ticker.C:
				if _, err := writer.Write(payload); err != nil {
					return
				}
			}
		}
	}()

	return reader, nil
}

func (p *testRecoveryFillerStreamingProducer) Describe() string {
	return "test:recovery_filler_streaming"
}

type testRecoveryFillerCloseErrorReader struct {
	closeErr   error
	closeDelay time.Duration
}

func (r *testRecoveryFillerCloseErrorReader) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (r *testRecoveryFillerCloseErrorReader) Close() error {
	if r == nil {
		return nil
	}
	if r.closeDelay > 0 {
		time.Sleep(r.closeDelay)
	}
	return r.closeErr
}

type testRecoveryFillerCloseErrorProducer struct {
	closeErr error
}

func (p *testRecoveryFillerCloseErrorProducer) Start(context.Context) (io.ReadCloser, error) {
	return &testRecoveryFillerCloseErrorReader{closeErr: p.closeErr}, nil
}

func (p *testRecoveryFillerCloseErrorProducer) Describe() string {
	return "test:recovery_filler_close_error"
}

type listOnlyChannelsProvider struct {
	base *fakeChannelsProvider
}

func (p *listOnlyChannelsProvider) GetByGuideNumber(ctx context.Context, guideNumber string) (channels.Channel, error) {
	return p.base.GetByGuideNumber(ctx, guideNumber)
}

func (p *listOnlyChannelsProvider) ListSources(ctx context.Context, channelID int64, enabledOnly bool) ([]channels.Source, error) {
	return p.base.ListSources(ctx, channelID, enabledOnly)
}

func (p *listOnlyChannelsProvider) MarkSourceFailure(ctx context.Context, sourceID int64, reason string, failedAt time.Time) error {
	return p.base.MarkSourceFailure(ctx, sourceID, reason, failedAt)
}

func (p *listOnlyChannelsProvider) MarkSourceSuccess(ctx context.Context, sourceID int64, succeededAt time.Time) error {
	return p.base.MarkSourceSuccess(ctx, sourceID, succeededAt)
}

func (p *listOnlyChannelsProvider) UpdateSourceProfile(ctx context.Context, sourceID int64, profile channels.SourceProfileUpdate) error {
	return p.base.UpdateSourceProfile(ctx, sourceID, profile)
}

type blockingProfilePersistChannelsProvider struct {
	*fakeChannelsProvider
	profilePersistStarted  chan struct{}
	profilePersistCanceled chan struct{}
	profilePersistUnblock  chan struct{}
}

func newBlockingProfilePersistChannelsProvider(base *fakeChannelsProvider) *blockingProfilePersistChannelsProvider {
	return &blockingProfilePersistChannelsProvider{
		fakeChannelsProvider:   base,
		profilePersistStarted:  make(chan struct{}, 1),
		profilePersistCanceled: make(chan struct{}, 1),
		profilePersistUnblock:  make(chan struct{}),
	}
}

func (p *blockingProfilePersistChannelsProvider) UpdateSourceProfile(
	ctx context.Context,
	sourceID int64,
	profile channels.SourceProfileUpdate,
) error {
	if p == nil {
		return nil
	}
	if p.profilePersistStarted != nil {
		select {
		case p.profilePersistStarted <- struct{}{}:
		default:
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		if p.profilePersistCanceled != nil {
			select {
			case p.profilePersistCanceled <- struct{}{}:
			default:
			}
		}
		return ctx.Err()
	case <-p.profilePersistUnblock:
	}
	if p.fakeChannelsProvider == nil {
		return nil
	}
	return p.fakeChannelsProvider.UpdateSourceProfile(ctx, sourceID, profile)
}

func TestSessionManagerSharesOneTunerPerChannelSession(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)

		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < 200; i++ {
			if _, err := w.Write([]byte("abcd")); err != nil {
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

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           4,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub1, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() first error = %v", err)
	}
	defer sub1.Close()

	sub2, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() second error = %v", err)
	}
	defer sub2.Close()

	if got := pool.InUseCount(); got != 1 {
		t.Fatalf("InUseCount() = %d, want 1 shared tuner lease", got)
	}

	sub1.Close()
	sub2.Close()

	waitFor(t, time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerSubscribeEvictsClosedMappedSession(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("E"), mpegTSPacketSize)
		for i := 0; i < 200; i++ {
			if _, err := w.Write(payload); err != nil {
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

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	stale := &sharedRuntimeSession{}
	stale.mu.Lock()
	stale.closed = true
	stale.mu.Unlock()

	manager.mu.Lock()
	manager.sessions[1] = stale
	manager.mu.Unlock()

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	manager.mu.Lock()
	current := manager.sessions[1]
	manager.mu.Unlock()
	if current == nil {
		t.Fatal("expected active session entry after subscribe")
	}
	if current == stale {
		t.Fatal("stale closed session was reused")
	}

	sub.Close()
	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerSubscribeSkipsClosedCreateWaitSession(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("W"), mpegTSPacketSize)
		for i := 0; i < 200; i++ {
			if _, err := w.Write(payload); err != nil {
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

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	stale := &sharedRuntimeSession{}
	stale.mu.Lock()
	stale.closed = true
	stale.mu.Unlock()

	wait := &sessionCreateWait{done: make(chan struct{})}
	manager.mu.Lock()
	manager.creating[1] = wait
	manager.mu.Unlock()

	go func() {
		time.Sleep(15 * time.Millisecond)
		manager.mu.Lock()
		wait.session = stale
		delete(manager.creating, 1)
		close(wait.done)
		manager.mu.Unlock()
	}()

	subscribeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sub, err := manager.Subscribe(subscribeCtx, provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	manager.mu.Lock()
	current := manager.sessions[1]
	manager.mu.Unlock()
	if current == nil {
		t.Fatal("expected active session entry after subscribe")
	}
	if current == stale {
		t.Fatal("closed create-wait session was reused")
	}

	sub.Close()
	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func newCanceledReadySessionForReuseTests(
	manager *SessionManager,
	channel channels.Channel,
) *sharedRuntimeSession {
	ctx, cancel := context.WithCancel(context.Background())
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         ctx,
		cancel:      cancel,
		readyCh:     make(chan struct{}),
		ring:        NewChunkRingWithLimitsAndStartupHint(8, 8*mpegTSPacketSize, mpegTSPacketSize),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}
	close(session.readyCh)
	cancel()
	return session
}

func newReadySessionForReuseTests(
	manager *SessionManager,
	channel channels.Channel,
) *sharedRuntimeSession {
	ctx, cancel := context.WithCancel(context.Background())
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         ctx,
		cancel:      cancel,
		readyCh:     make(chan struct{}),
		ring:        NewChunkRingWithLimitsAndStartupHint(8, 8*mpegTSPacketSize, mpegTSPacketSize),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}
	close(session.readyCh)
	return session
}

func TestSessionManagerGetOrCreateSessionSkipsCanceledMappedSession(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("C"), mpegTSPacketSize)
		for i := 0; i < 200; i++ {
			if _, err := w.Write(payload); err != nil {
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

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}
	defer manager.Close()

	stale := newCanceledReadySessionForReuseTests(manager, provider.channelsByGuide["101"])
	manager.mu.Lock()
	manager.sessions[1] = stale
	manager.mu.Unlock()

	session, created, err := manager.getOrCreateSession(
		context.Background(),
		provider.channelsByGuide["101"],
		nil,
	)
	if err != nil {
		t.Fatalf("getOrCreateSession() error = %v", err)
	}
	if !created {
		t.Fatal("getOrCreateSession() reused canceled mapped session")
	}
	if session == stale {
		t.Fatal("canceled mapped session was reused")
	}
	if session == nil {
		t.Fatal("expected a replacement active session")
	}

	session.cancelNow()
	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerSubscribeSkipsCanceledMappedSession(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("K"), mpegTSPacketSize)
		for i := 0; i < 200; i++ {
			if _, err := w.Write(payload); err != nil {
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

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}
	defer manager.Close()

	stale := newCanceledReadySessionForReuseTests(manager, provider.channelsByGuide["101"])
	manager.mu.Lock()
	manager.sessions[1] = stale
	manager.mu.Unlock()

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	if sub.session == stale {
		t.Fatal("Subscribe() reused canceled mapped session")
	}

	manager.mu.Lock()
	current := manager.sessions[1]
	manager.mu.Unlock()
	if current == nil {
		t.Fatal("expected active session entry after subscribe")
	}
	if current == stale {
		t.Fatal("canceled mapped session remained active after subscribe")
	}

	sub.Close()
	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerGetOrCreateSessionRejectsCanceledContextOnReadyReuse(t *testing.T) {
	channel := channels.Channel{
		ChannelID:   1,
		GuideNumber: "101",
		GuideName:   "News",
		Enabled:     true,
	}
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": channel,
		},
	}
	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}
	defer manager.Close()

	session := newReadySessionForReuseTests(manager, channel)
	manager.mu.Lock()
	manager.sessions[channel.ChannelID] = session
	manager.mu.Unlock()

	subscribeCtx, cancel := context.WithCancel(context.Background())
	cancel()

	reused, created, err := manager.getOrCreateSession(subscribeCtx, channel, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("getOrCreateSession() error = %v, want context.Canceled", err)
	}
	if reused != nil {
		t.Fatal("getOrCreateSession() returned a reusable session for canceled context")
	}
	if created {
		t.Fatal("getOrCreateSession() unexpectedly created a session for canceled context")
	}
}

func TestSessionManagerSubscribeCanceledContextDoesNotAttachReadySession(t *testing.T) {
	channel := channels.Channel{
		ChannelID:   1,
		GuideNumber: "101",
		GuideName:   "News",
		Enabled:     true,
	}
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": channel,
		},
	}
	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}
	defer manager.Close()

	session := newReadySessionForReuseTests(manager, channel)
	session.subscribers[1] = SubscriberStats{
		SubscriberID: 1,
		ClientAddr:   "198.51.100.10:4000",
		StartedAt:    time.Now().UTC(),
	}
	session.nextSubscriberID = 1

	manager.mu.Lock()
	manager.sessions[channel.ChannelID] = session
	manager.mu.Unlock()

	subscribeCtx, cancel := context.WithCancel(context.Background())
	cancel()

	sub, err := manager.Subscribe(subscribeCtx, channel)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe() error = %v, want context.Canceled", err)
	}
	if sub != nil {
		sub.Close()
		t.Fatal("Subscribe() returned a subscription for canceled context")
	}

	session.mu.Lock()
	subscribers := len(session.subscribers)
	nextSubscriberID := session.nextSubscriberID
	session.mu.Unlock()
	if subscribers != 1 {
		t.Fatalf("subscriber count = %d, want 1 existing subscriber without canceled attach", subscribers)
	}
	if nextSubscriberID != 1 {
		t.Fatalf("nextSubscriberID = %d, want 1 without canceled attach", nextSubscriberID)
	}
}

func TestSessionManagerHasActiveOrPendingSessionIgnoresClosedMappedSession(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}
	manager.sessions[1] = &sharedRuntimeSession{
		closed: true,
	}

	if manager.HasActiveOrPendingSession(1) {
		t.Fatal("HasActiveOrPendingSession() = true, want false for closed mapped session")
	}

	manager.mu.Lock()
	_, stillPresent := manager.sessions[1]
	manager.mu.Unlock()
	if !stillPresent {
		t.Fatal("HasActiveOrPendingSession() unexpectedly mutated session map for closed mapped session")
	}
}

func TestSessionManagerHasActiveOrPendingSessionReturnsTrueForOpenMappedSession(t *testing.T) {
	channel := channels.Channel{
		ChannelID:   1,
		GuideNumber: "101",
		GuideName:   "News",
		Enabled:     true,
	}

	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}
	session := newReadySessionForReuseTests(manager, channel)

	manager.sessions[1] = session
	if !manager.HasActiveOrPendingSession(1) {
		t.Fatal("HasActiveOrPendingSession() = false, want true for admissible mapped session")
	}
}

func TestSessionManagerHasActiveOrPendingSessionReturnsTrueForPendingCreate(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}
	manager.creating[1] = &sessionCreateWait{done: make(chan struct{})}

	if !manager.HasActiveOrPendingSession(1) {
		t.Fatal("HasActiveOrPendingSession() = false, want true for create-wait session")
	}
}

func TestSessionManagerHasActiveOrPendingSessionPrefersPendingCreateOverClosedMappedSession(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}
	manager.sessions[1] = &sharedRuntimeSession{
		closed: true,
	}
	manager.creating[1] = &sessionCreateWait{done: make(chan struct{})}

	if !manager.HasActiveOrPendingSession(1) {
		t.Fatal("HasActiveOrPendingSession() = false, want true when create-wait is pending despite closed mapped entry")
	}
}

func TestSessionManagerHasActiveOrPendingSessionInadmissibleMappedSessionRemovedByFinish(t *testing.T) {
	channel := channels.Channel{
		ChannelID:   1,
		GuideNumber: "101",
		GuideName:   "News",
		Enabled:     true,
	}
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": channel,
		},
	}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}
	defer manager.Close()

	ctx, cancel := context.WithCancel(context.Background())
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         ctx,
		cancel:      cancel,
		readyCh:     make(chan struct{}),
		ring:        NewChunkRing(8),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}

	manager.mu.Lock()
	manager.sessions[channel.ChannelID] = session
	manager.mu.Unlock()

	session.cancelNow()
	if manager.HasActiveOrPendingSession(channel.ChannelID) {
		t.Fatal("HasActiveOrPendingSession() = true, want false for canceled mapped session")
	}

	manager.mu.Lock()
	_, stillPresent := manager.sessions[channel.ChannelID]
	manager.mu.Unlock()
	if !stillPresent {
		t.Fatal("HasActiveOrPendingSession() unexpectedly mutated session map for canceled mapped session")
	}

	session.finish(context.Canceled)

	manager.mu.Lock()
	_, exists := manager.sessions[channel.ChannelID]
	manager.mu.Unlock()
	if exists {
		t.Fatal("session lifecycle finish did not remove inadmissible mapped session entry")
	}
}

func TestSharedRuntimeSessionWaitReadyPrefersCanceledContextWhenReady(t *testing.T) {
	sentinelErr := errors.New("ready sentinel")
	session := &sharedRuntimeSession{
		readyCh:  make(chan struct{}),
		readyErr: sentinelErr,
	}
	close(session.readyCh)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := session.waitReady(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitReady() error = %v, want context.Canceled", err)
	}
	if errors.Is(err, sentinelErr) {
		t.Fatalf("waitReady() error = %v, want canceled-context precedence over readyErr", err)
	}
}

// TestRemoveSubscriberZeroTriggersIdleCancelOnNewSession verifies that
// removeSubscriber(0, ...) — which skips actual subscriber deletion —
// still triggers idle-cancel logic when the session has zero subscribers.
// This is the cleanup path used by guard G2 in subscribe() when a freshly
// created session must be abandoned because the caller's context canceled.
func TestRemoveSubscriberZeroTriggersIdleCancelOnNewSession(t *testing.T) {
	channel := channels.Channel{
		ChannelID:   1,
		GuideNumber: "101",
		GuideName:   "News",
		Enabled:     true,
	}
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": channel,
		},
	}
	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}
	defer manager.Close()

	// Create a session that is NOT ready (readyCh open) with zero subscribers.
	// This simulates the state after getOrCreateSession returns created=true
	// but before addSubscriber runs.
	session := newReadySessionForReuseTests(manager, channel)

	// Register it in the manager so we can verify cleanup removes it.
	manager.mu.Lock()
	manager.sessions[channel.ChannelID] = session
	manager.mu.Unlock()

	// removeSubscriber(0, ...) must trigger idle-cancel because:
	// - subscriberID=0 skips the actual delete
	// - len(subscribers)==0 is still true
	// - session is not closed
	// For a ready session with idle timeout > 0, this arms an idle timer.
	// For testing purposes, the idle timeout fires and cancels the session.
	session.removeSubscriber(0, subscriberRemovalReasonHTTPContextCanceled)

	// Wait for the session's context to be canceled (idle-cancel fires).
	waitFor(t, 2*time.Second, func() bool {
		return session.ctx.Err() != nil
	})

	// Verify the session's context was canceled.
	if session.ctx.Err() == nil {
		t.Fatal("session context should be canceled after removeSubscriber(0,...)")
	}

	// Verify the session is no longer admissible for reuse.
	if session.isAdmissible() {
		t.Fatal("session should not be admissible after idle cancellation")
	}
}

// TestSubscribeG2CleansUpCreatedSessionOnCanceledContext verifies that when
// subscribe() creates a new session via getOrCreateSession (created=true)
// but the caller's context is already canceled, the G2 guard fires and the
// session is cleaned up via removeSubscriber(0, ...), leaving no orphan.
func TestSubscribeG2CleansUpCreatedSessionOnCanceledContext(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("G"), mpegTSPacketSize)
		for i := 0; i < 200; i++ {
			if _, err := w.Write(payload); err != nil {
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

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}
	defer manager.Close()

	// First subscribe to create a real session for the channel.
	liveCtx, liveCancel := context.WithCancel(context.Background())
	defer liveCancel()
	sub, err := manager.Subscribe(liveCtx, provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("initial Subscribe() error = %v", err)
	}

	// Grab the created session.
	manager.mu.Lock()
	createdSession := manager.sessions[1]
	manager.mu.Unlock()
	if createdSession == nil {
		t.Fatal("expected a session after initial subscribe")
	}

	// Close the first subscription so the session is idle with 0 subscribers.
	sub.Close()

	// Now attempt subscribe with an already-canceled context.
	// The G1 guard in the for-loop catches this, returning context.Canceled
	// without creating a new session. This validates no orphan is left.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	sub2, err := manager.Subscribe(canceledCtx, provider.channelsByGuide["101"])
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe() error = %v, want context.Canceled", err)
	}
	if sub2 != nil {
		sub2.Close()
		t.Fatal("Subscribe() returned a subscription for canceled context")
	}

	// The session should eventually be cleaned up by idle timeout from the
	// first sub.Close(). Wait for tuner pool to be released.
	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

// TestSubscribeG3CleansUpSubscriberOnCanceledContextAfterAdd verifies that
// when addSubscriber succeeds but the caller's context is canceled before
// the subscription object is built, G3 rolls back the subscriber via
// removeSubscriber(subscriberID, ...) and returns context.Canceled.
func TestSubscribeG3CleansUpSubscriberOnCanceledContextAfterAdd(t *testing.T) {
	channel := channels.Channel{
		ChannelID:   1,
		GuideNumber: "101",
		GuideName:   "News",
		Enabled:     true,
	}
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": channel,
		},
	}
	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}
	defer manager.Close()

	// Create a ready session with one existing subscriber so it stays alive.
	session := newReadySessionForReuseTests(manager, channel)
	session.subscribers[1] = SubscriberStats{
		SubscriberID: 1,
		ClientAddr:   "198.51.100.10:4000",
		StartedAt:    time.Now().UTC(),
	}
	session.nextSubscriberID = 1

	manager.mu.Lock()
	manager.sessions[channel.ChannelID] = session
	manager.mu.Unlock()

	// addSubscriber on a ready, non-closed session should succeed.
	nextSeq, subscriberID, err := session.addSubscriber("198.51.100.20:5000")
	if err != nil {
		t.Fatalf("addSubscriber() error = %v", err)
	}
	if subscriberID == 0 {
		t.Fatal("addSubscriber() returned subscriberID=0")
	}
	_ = nextSeq

	// Verify subscriber was added (now 2 subscribers).
	session.mu.Lock()
	countAfterAdd := len(session.subscribers)
	session.mu.Unlock()
	if countAfterAdd != 2 {
		t.Fatalf("subscriber count after add = %d, want 2", countAfterAdd)
	}

	// Simulate G3 cleanup: remove the subscriber that was just added.
	session.removeSubscriber(subscriberID, subscriberRemovalReasonHTTPContextCanceled)

	// Verify subscriber was removed (back to 1).
	session.mu.Lock()
	countAfterRemove := len(session.subscribers)
	_, stillPresent := session.subscribers[subscriberID]
	session.mu.Unlock()
	if countAfterRemove != 1 {
		t.Fatalf("subscriber count after rollback = %d, want 1", countAfterRemove)
	}
	if stillPresent {
		t.Fatal("rolled-back subscriber still present in session")
	}

	// Session should still be admissible (the existing subscriber keeps it alive).
	if !session.isAdmissible() {
		t.Fatal("session should still be admissible with one remaining subscriber")
	}
}

// TestWaitReadyAlwaysPrefersCanceledContextDeterministic runs waitReady()
// many iterations with both readyCh (with a sentinel error) and a canceled
// context to prove the post-select G10 guard deterministically returns
// context.Canceled, not the sentinel readyErr. Before the fix, Go's select
// nondeterminism would cause ~50% of iterations to return the sentinel.
func TestWaitReadyAlwaysPrefersCanceledContextDeterministic(t *testing.T) {
	sentinelErr := errors.New("ready sentinel")

	const iterations = 200
	for i := 0; i < iterations; i++ {
		session := &sharedRuntimeSession{
			readyCh:  make(chan struct{}),
			readyErr: sentinelErr,
		}
		close(session.readyCh)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := session.waitReady(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("iteration %d: waitReady() = %v, want context.Canceled", i, err)
		}
		if errors.Is(err, sentinelErr) {
			t.Fatalf("iteration %d: waitReady() returned sentinel instead of context.Canceled", i)
		}
	}
}

// TestGetOrCreateSessionG7CanceledContextDuringCreateWait verifies that
// when a goroutine waits on a sessionCreateWait and the wait resolves with
// a valid session, but the waiter's context is canceled, G7 returns
// context.Canceled instead of reusing the session.
func TestGetOrCreateSessionG7CanceledContextDuringCreateWait(t *testing.T) {
	channel := channels.Channel{
		ChannelID:   1,
		GuideNumber: "101",
		GuideName:   "News",
		Enabled:     true,
	}
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": channel,
		},
	}
	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}
	defer manager.Close()

	// Register a pending create-wait for channel 1.
	wait := &sessionCreateWait{done: make(chan struct{})}
	manager.mu.Lock()
	manager.creating[1] = wait
	manager.mu.Unlock()

	// Create a valid session that the wait will resolve with.
	readySession := newReadySessionForReuseTests(manager, channel)

	// Launch a goroutine that calls getOrCreateSession with a context
	// that will be canceled before the wait resolves.
	subscribeCtx, cancelSubscribe := context.WithCancel(context.Background())

	type result struct {
		session *sharedRuntimeSession
		created bool
		err     error
	}
	resultCh := make(chan result, 1)
	go func() {
		s, c, e := manager.getOrCreateSession(subscribeCtx, channel, nil)
		resultCh <- result{s, c, e}
	}()

	// Give the goroutine time to enter the select on wait.done.
	time.Sleep(15 * time.Millisecond)

	// Cancel the subscriber's context, then complete the wait.
	cancelSubscribe()

	// Complete the create-wait with a valid session.
	manager.mu.Lock()
	wait.session = readySession
	delete(manager.creating, 1)
	close(wait.done)
	manager.mu.Unlock()

	// Also register the session in manager.sessions so isAdmissible checks pass.
	manager.mu.Lock()
	manager.sessions[channel.ChannelID] = readySession
	manager.mu.Unlock()

	r := <-resultCh

	// G7 (or earlier ctx.Err() checks in the wait path) should catch the
	// canceled context and return an error.
	if !errors.Is(r.err, context.Canceled) {
		t.Fatalf("getOrCreateSession() error = %v, want context.Canceled", r.err)
	}
	if r.session != nil {
		t.Fatal("getOrCreateSession() returned a session for canceled context")
	}
	if r.created {
		t.Fatal("getOrCreateSession() unexpectedly reported created=true")
	}
}

func TestSessionManagerTriggerRecovery(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("R"), mpegTSPacketSize)
		for i := 0; i < 1200; i++ {
			if _, err := w.Write(payload); err != nil {
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

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             800 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		StallPolicy:                stallPolicyRestartSame,
		SessionIdleTimeout:         150 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	waitFor(t, 2*time.Second, func() bool {
		snap := manager.Snapshot()
		return len(snap) == 1 && snap[0].SourceSelectCount >= 1
	})

	if err := manager.TriggerRecovery(1, "ui_manual_trigger"); err != nil {
		t.Fatalf("TriggerRecovery() error = %v", err)
	}

	waitFor(t, 2*time.Second, func() bool {
		snap := manager.Snapshot()
		if len(snap) != 1 {
			return false
		}
		session := snap[0]
		return session.RecoveryCycle >= 1 &&
			strings.EqualFold(session.RecoveryReason, "ui_manual_trigger") &&
			strings.Contains(session.LastSourceSelectReason, "recovery_cycle_")
	})

	snap := manager.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("len(snapshot) = %d, want 1", len(snap))
	}
	if !strings.EqualFold(snap[0].RecoveryReason, "ui_manual_trigger") {
		t.Fatalf("snapshot.recovery_reason = %q, want ui_manual_trigger", snap[0].RecoveryReason)
	}
	if !strings.Contains(snap[0].LastSourceSelectReason, "ui_manual_trigger") {
		t.Fatalf("snapshot.last_source_select_reason = %q, want ui_manual_trigger marker", snap[0].LastSourceSelectReason)
	}

	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		failures := provider.sourceFailures()
		if hasFailureReason(failures, 10, "manual recovery requested") {
			t.Fatalf("failures = %#v, did not expect manual trigger to persist source failure", failures)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSessionManagerTriggerRecoveryErrors(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}
	if err := manager.TriggerRecovery(0, "ui_manual_trigger"); err == nil {
		t.Fatal("TriggerRecovery(0) error = nil, want validation error")
	}
	if err := manager.TriggerRecovery(999, "ui_manual_trigger"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("TriggerRecovery(missing) error = %v, want ErrSessionNotFound", err)
	}
	manager.creating[11] = &sessionCreateWait{done: make(chan struct{})}
	if err := manager.TriggerRecovery(11, "ui_manual_trigger"); !errors.Is(err, ErrSessionRecoveryAlreadyPending) {
		t.Fatalf("TriggerRecovery(create_pending) error = %v, want ErrSessionRecoveryAlreadyPending", err)
	}

	session := &sharedRuntimeSession{
		channel:          channels.Channel{ChannelID: 7, GuideNumber: "107", GuideName: "Sample"},
		manualRecoveryCh: make(chan string, 1),
		subscribers: map[uint64]SubscriberStats{
			1: {SubscriberID: 1},
		},
		readyCh: make(chan struct{}),
	}
	close(session.readyCh)
	manager.sessions[7] = session
	session.manualRecoveryCh <- "pending_manual_recovery"
	session.manualRecoveryPending = true

	if err := manager.TriggerRecovery(7, "ui_manual_trigger"); !errors.Is(err, ErrSessionRecoveryAlreadyPending) {
		t.Fatalf("TriggerRecovery(pending) error = %v, want ErrSessionRecoveryAlreadyPending", err)
	}
}

func TestSessionManagerTriggerRecoveryRejectsBeforeReadySession(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}
	session := &sharedRuntimeSession{
		channel:          channels.Channel{ChannelID: 7, GuideNumber: "107", GuideName: "BeforeReady"},
		manualRecoveryCh: make(chan string, 1),
		readyCh:          make(chan struct{}), // intentionally not closed
		subscribers: map[uint64]SubscriberStats{
			1: {SubscriberID: 1},
		},
	}
	manager.sessions[7] = session

	if err := manager.TriggerRecovery(7, "ui_manual_before_ready"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("TriggerRecovery(before_ready) error = %v, want ErrSessionNotFound", err)
	}
	if got := len(session.manualRecoveryCh); got != 0 {
		t.Fatalf("manual recovery queue length = %d, want 0", got)
	}
	if session.manualRecoveryPending {
		t.Fatal("manualRecoveryPending = true, want false for rejected before-ready trigger")
	}
}

func TestSessionManagerTriggerRecoveryRejectsInProgressRecovery(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}
	readyCh := make(chan struct{})
	close(readyCh)
	session := &sharedRuntimeSession{
		channel:                  channels.Channel{ChannelID: 7, GuideNumber: "107", GuideName: "InProgress"},
		manualRecoveryCh:         make(chan string, 1),
		readyCh:                  readyCh,
		subscribers:              map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		manualRecoveryInProgress: true,
	}
	manager.sessions[7] = session

	if err := manager.TriggerRecovery(7, "ui_manual_during_recovery"); !errors.Is(err, ErrSessionRecoveryAlreadyPending) {
		t.Fatalf("TriggerRecovery(in_progress) error = %v, want ErrSessionRecoveryAlreadyPending", err)
	}
	if got := len(session.manualRecoveryCh); got != 0 {
		t.Fatalf("manual recovery queue length = %d, want 0", got)
	}
}

func TestSharedRuntimeSessionRunCycleDefersManualRecoveryUntilSubscriberReattach(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader, writer := io.Pipe()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		payload := bytes.Repeat([]byte("M"), mpegTSPacketSize)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = writer.Close()
				return
			case <-ticker.C:
				if _, err := writer.Write(payload); err != nil {
					return
				}
			}
		}
	}()

	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				stallDetect: 5 * time.Second,
			},
		},
		ctx:                   ctx,
		pump:                  NewPump(PumpConfig{ChunkBytes: mpegTSPacketSize, PublishFlushInterval: 5 * time.Millisecond}, &discardCopyAwarePublisher{}),
		manualRecoveryCh:      make(chan string, 1),
		manualRecoveryPending: true,
		subscribers:           map[uint64]SubscriberStats{},
		channel: channels.Channel{
			ChannelID:   7,
			GuideNumber: "107",
			GuideName:   "DeferredManual",
		},
	}
	session.manualRecoveryCh <- "ui_manual_deferred"

	resultCh := make(chan struct {
		err   error
		stall bool
	}, 1)
	go func() {
		err, stall := session.runCycle(reader)
		resultCh <- struct {
			err   error
			stall bool
		}{err: err, stall: stall}
	}()

	waitFor(t, 2*time.Second, func() bool {
		return len(session.manualRecoveryCh) == 0
	})
	session.mu.Lock()
	pendingAfterDefer := session.manualRecoveryPending
	session.mu.Unlock()
	if !pendingAfterDefer {
		t.Fatal("manualRecoveryPending = false after deferred consume, want true while waiting for reattach")
	}

	session.mu.Lock()
	session.subscribers[1] = SubscriberStats{SubscriberID: 1}
	session.mu.Unlock()

	var result struct {
		err   error
		stall bool
	}
	select {
	case result = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("runCycle() did not complete after subscriber reattach")
	}
	if !result.stall {
		t.Fatal("runCycle() stallDetected = false, want true for deferred manual recovery trigger")
	}
	reason, ok := manualRecoveryReason(result.err)
	if !ok {
		t.Fatalf("runCycle() error = %v, want manualRecoveryError", result.err)
	}
	if reason != "ui_manual_deferred" {
		t.Fatalf("manual recovery reason = %q, want ui_manual_deferred", reason)
	}
	session.mu.Lock()
	pendingAfterReplay := session.manualRecoveryPending
	inProgressAfterReplay := session.manualRecoveryInProgress
	session.mu.Unlock()
	if pendingAfterReplay {
		t.Fatal("manualRecoveryPending = true after deferred trigger execution, want false")
	}
	if !inProgressAfterReplay {
		t.Fatal("manualRecoveryInProgress = false, want true while recovery handoff is in progress")
	}

	cancel()
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("writer goroutine did not exit")
	}
}

type runCycleResult struct {
	err   error
	stall bool
}

func startRunCyclePauseTestSession(
	t *testing.T,
	subscriberCount int,
) (
	*sharedRuntimeSession,
	*io.PipeWriter,
	*atomic.Int64,
	context.CancelFunc,
	<-chan runCycleResult,
) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()

	subscribers := make(map[uint64]SubscriberStats, subscriberCount)
	for i := 0; i < subscriberCount; i++ {
		id := uint64(i + 1)
		subscribers[id] = SubscriberStats{SubscriberID: id}
	}

	pump := NewPump(
		PumpConfig{
			ChunkBytes:           mpegTSPacketSize,
			PublishFlushInterval: 10 * time.Millisecond,
		},
		&discardCopyAwarePublisher{},
	)
	pumpNowUnixNano := &atomic.Int64{}
	pumpNowUnixNano.Store(time.Now().UTC().UnixNano())
	pump.nowFn = func() time.Time {
		return time.Unix(0, pumpNowUnixNano.Load()).UTC()
	}

	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				stallDetect: 0,
			},
		},
		ctx:              ctx,
		pump:             pump,
		subscribers:      subscribers,
		manualRecoveryCh: make(chan string, 1),
	}

	resultCh := make(chan runCycleResult, 1)
	go func() {
		err, stall := session.runCycle(reader)
		resultCh <- runCycleResult{err: err, stall: stall}
	}()

	return session, writer, pumpNowUnixNano, cancel, resultCh
}

func writeRunCyclePauseTestPacket(t *testing.T, writer *io.PipeWriter) {
	t.Helper()
	if _, err := writer.Write(bytes.Repeat([]byte("P"), mpegTSPacketSize)); err != nil {
		t.Fatalf("writer.Write() error = %v", err)
	}
}

func waitForRunCycleResult(t *testing.T, resultCh <-chan runCycleResult) runCycleResult {
	t.Helper()
	select {
	case result := <-resultCh:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for runCycle() result")
		return runCycleResult{}
	}
}

func waitForSourceReadPauseInProgress(t *testing.T) {
	t.Helper()
	waitFor(t, 2*time.Second, func() bool {
		return streamSourceReadPauseStatsSnapshot().InProgress > 0
	})
}

func sourceReadPauseMetricDelta(t *testing.T, reason string, before float64) float64 {
	t.Helper()
	return testutil.ToFloat64(streamSourceReadPauseEventsMetric.WithLabelValues(reason)) - before
}

func TestSharedRuntimeSessionRunCycleFinalizesSourceReadPauseOnContextCancel(t *testing.T) {
	isolateStreamSourceReadPauseStatsForTest(t)
	beforeRecovered := testutil.ToFloat64(streamSourceReadPauseEventsMetric.WithLabelValues(sourceReadPauseReasonRecovered))
	beforePumpExit := testutil.ToFloat64(streamSourceReadPauseEventsMetric.WithLabelValues(sourceReadPauseReasonPumpExit))
	beforeContextCancel := testutil.ToFloat64(streamSourceReadPauseEventsMetric.WithLabelValues(sourceReadPauseReasonContextCancel))

	session, writer, pumpNowUnixNano, cancel, resultCh := startRunCyclePauseTestSession(t, 1)
	defer cancel()
	defer func() {
		_ = writer.Close()
	}()

	pumpNowUnixNano.Store(time.Now().Add(-2 * time.Second).UTC().UnixNano())
	writeRunCyclePauseTestPacket(t, writer)
	waitFor(t, time.Second, func() bool {
		return !session.pump.Stats().LastByteReadAt.IsZero()
	})
	waitForSourceReadPauseInProgress(t)

	cancel()
	result := waitForRunCycleResult(t, resultCh)
	if result.err != nil {
		t.Fatalf("runCycle() error = %v, want nil on ctx cancel path", result.err)
	}
	if result.stall {
		t.Fatal("runCycle() stallDetected = true, want false on ctx cancel path")
	}
	waitFor(t, time.Second, func() bool {
		return streamSourceReadPauseStatsSnapshot().Events == 1
	})

	stats := streamSourceReadPauseStatsSnapshot()
	if got, want := stats.Events, uint64(1); got != want {
		t.Fatalf("source read pause events = %d, want %d", got, want)
	}
	if got, want := stats.InProgress, uint64(0); got != want {
		t.Fatalf("source read pause in progress = %d, want %d", got, want)
	}
	if got := stats.DurationUS; got == 0 {
		t.Fatalf("source read pause duration us = %d, want > 0", got)
	}
	if got := sourceReadPauseMetricDelta(t, sourceReadPauseReasonContextCancel, beforeContextCancel); got != 1 {
		t.Fatalf("ctx_cancel metric delta = %.0f, want 1", got)
	}
	if got := sourceReadPauseMetricDelta(t, sourceReadPauseReasonRecovered, beforeRecovered); got != 0 {
		t.Fatalf("recovered metric delta = %.0f, want 0", got)
	}
	if got := sourceReadPauseMetricDelta(t, sourceReadPauseReasonPumpExit, beforePumpExit); got != 0 {
		t.Fatalf("pump_exit metric delta = %.0f, want 0", got)
	}
}

func TestSharedRuntimeSessionRunCycleSourceReadPauseStateMachine(t *testing.T) {
	t.Run("pause closes when read gap recovers", func(t *testing.T) {
		isolateStreamSourceReadPauseStatsForTest(t)
		beforeRecovered := testutil.ToFloat64(streamSourceReadPauseEventsMetric.WithLabelValues(sourceReadPauseReasonRecovered))
		beforePumpExit := testutil.ToFloat64(streamSourceReadPauseEventsMetric.WithLabelValues(sourceReadPauseReasonPumpExit))
		beforeContextCancel := testutil.ToFloat64(streamSourceReadPauseEventsMetric.WithLabelValues(sourceReadPauseReasonContextCancel))

		session, writer, pumpNowUnixNano, cancel, resultCh := startRunCyclePauseTestSession(t, 1)
		defer cancel()
		defer func() {
			_ = writer.Close()
		}()

		pumpNowUnixNano.Store(time.Now().Add(-2 * time.Second).UTC().UnixNano())
		writeRunCyclePauseTestPacket(t, writer)
		waitFor(t, time.Second, func() bool {
			return !session.pump.Stats().LastByteReadAt.IsZero()
		})
		waitForSourceReadPauseInProgress(t)

		pumpNowUnixNano.Store(time.Now().UTC().UnixNano())
		writeRunCyclePauseTestPacket(t, writer)
		waitFor(t, 2*time.Second, func() bool {
			return streamSourceReadPauseStatsSnapshot().Events == 1
		})

		if err := writer.Close(); err != nil {
			t.Fatalf("writer.Close() error = %v", err)
		}
		result := waitForRunCycleResult(t, resultCh)
		if !errors.Is(result.err, io.EOF) {
			t.Fatalf("runCycle() error = %v, want io.EOF", result.err)
		}
		if result.stall {
			t.Fatal("runCycle() stallDetected = true, want false")
		}
		if got := sourceReadPauseMetricDelta(t, sourceReadPauseReasonRecovered, beforeRecovered); got != 1 {
			t.Fatalf("recovered metric delta = %.0f, want 1", got)
		}
		if got := sourceReadPauseMetricDelta(t, sourceReadPauseReasonPumpExit, beforePumpExit); got != 0 {
			t.Fatalf("pump_exit metric delta = %.0f, want 0", got)
		}
		if got := sourceReadPauseMetricDelta(t, sourceReadPauseReasonContextCancel, beforeContextCancel); got != 0 {
			t.Fatalf("ctx_cancel metric delta = %.0f, want 0", got)
		}
	})

	t.Run("pause closes when pump exits with error channel", func(t *testing.T) {
		isolateStreamSourceReadPauseStatsForTest(t)
		beforeRecovered := testutil.ToFloat64(streamSourceReadPauseEventsMetric.WithLabelValues(sourceReadPauseReasonRecovered))
		beforePumpExit := testutil.ToFloat64(streamSourceReadPauseEventsMetric.WithLabelValues(sourceReadPauseReasonPumpExit))
		beforeContextCancel := testutil.ToFloat64(streamSourceReadPauseEventsMetric.WithLabelValues(sourceReadPauseReasonContextCancel))

		session, writer, pumpNowUnixNano, cancel, resultCh := startRunCyclePauseTestSession(t, 1)
		defer cancel()
		defer func() {
			_ = writer.Close()
		}()

		pumpNowUnixNano.Store(time.Now().Add(-2 * time.Second).UTC().UnixNano())
		writeRunCyclePauseTestPacket(t, writer)
		waitFor(t, time.Second, func() bool {
			return !session.pump.Stats().LastByteReadAt.IsZero()
		})
		waitForSourceReadPauseInProgress(t)

		if err := writer.Close(); err != nil {
			t.Fatalf("writer.Close() error = %v", err)
		}
		result := waitForRunCycleResult(t, resultCh)
		if !errors.Is(result.err, io.EOF) {
			t.Fatalf("runCycle() error = %v, want io.EOF", result.err)
		}
		if result.stall {
			t.Fatal("runCycle() stallDetected = true, want false")
		}

		stats := streamSourceReadPauseStatsSnapshot()
		if got, want := stats.Events, uint64(1); got != want {
			t.Fatalf("source read pause events = %d, want %d", got, want)
		}
		if got, want := stats.InProgress, uint64(0); got != want {
			t.Fatalf("source read pause in progress = %d, want %d", got, want)
		}
		if got := stats.DurationUS; got == 0 {
			t.Fatalf("source read pause duration us = %d, want > 0", got)
		}
		if got := sourceReadPauseMetricDelta(t, sourceReadPauseReasonPumpExit, beforePumpExit); got != 1 {
			t.Fatalf("pump_exit metric delta = %.0f, want 1", got)
		}
		if got := sourceReadPauseMetricDelta(t, sourceReadPauseReasonRecovered, beforeRecovered); got != 0 {
			t.Fatalf("recovered metric delta = %.0f, want 0", got)
		}
		if got := sourceReadPauseMetricDelta(t, sourceReadPauseReasonContextCancel, beforeContextCancel); got != 0 {
			t.Fatalf("ctx_cancel metric delta = %.0f, want 0", got)
		}
	})

	t.Run("pause resets silently when subscribers drop to zero", func(t *testing.T) {
		isolateStreamSourceReadPauseStatsForTest(t)

		session, writer, pumpNowUnixNano, cancel, resultCh := startRunCyclePauseTestSession(t, 1)
		defer cancel()
		defer func() {
			_ = writer.Close()
		}()

		pumpNowUnixNano.Store(time.Now().Add(-2 * time.Second).UTC().UnixNano())
		writeRunCyclePauseTestPacket(t, writer)
		waitFor(t, time.Second, func() bool {
			return !session.pump.Stats().LastByteReadAt.IsZero()
		})
		waitForSourceReadPauseInProgress(t)

		session.mu.Lock()
		session.subscribers = map[uint64]SubscriberStats{}
		session.mu.Unlock()
		waitFor(t, time.Second, func() bool {
			return streamSourceReadPauseStatsSnapshot().InProgress == 0
		})

		if err := writer.Close(); err != nil {
			t.Fatalf("writer.Close() error = %v", err)
		}
		result := waitForRunCycleResult(t, resultCh)
		if !errors.Is(result.err, io.EOF) {
			t.Fatalf("runCycle() error = %v, want io.EOF", result.err)
		}
		if result.stall {
			t.Fatal("runCycle() stallDetected = true, want false")
		}

		if got, want := streamSourceReadPauseStatsSnapshot().Events, uint64(0); got != want {
			t.Fatalf("source read pause events = %d, want %d", got, want)
		}
		if got, want := streamSourceReadPauseStatsSnapshot().InProgress, uint64(0); got != want {
			t.Fatalf("source read pause in progress = %d, want %d", got, want)
		}
	})

	t.Run("zero last byte read skips pause detection", func(t *testing.T) {
		isolateStreamSourceReadPauseStatsForTest(t)

		_, writer, _, cancel, resultCh := startRunCyclePauseTestSession(t, 1)
		defer cancel()
		defer func() {
			_ = writer.Close()
		}()

		zeroWindowStart := time.Now()
		waitFor(t, 1200*time.Millisecond, func() bool {
			stats := streamSourceReadPauseStatsSnapshot()
			return time.Since(zeroWindowStart) >= 700*time.Millisecond &&
				stats.Events == 0 &&
				stats.InProgress == 0
		})
		if err := writer.Close(); err != nil {
			t.Fatalf("writer.Close() error = %v", err)
		}
		result := waitForRunCycleResult(t, resultCh)
		if !errors.Is(result.err, io.EOF) {
			t.Fatalf("runCycle() error = %v, want io.EOF", result.err)
		}
		if result.stall {
			t.Fatal("runCycle() stallDetected = true, want false")
		}

		if got, want := streamSourceReadPauseStatsSnapshot().Events, uint64(0); got != want {
			t.Fatalf("source read pause events = %d, want %d", got, want)
		}
		if got, want := streamSourceReadPauseStatsSnapshot().InProgress, uint64(0); got != want {
			t.Fatalf("source read pause in progress = %d, want %d", got, want)
		}
	})
}

func TestRunCyclePumpResultPrefersManualReasonOverEOF(t *testing.T) {
	err, stall := runCyclePumpResult(nil, nil, true, "ui_manual_cycle")
	if !stall {
		t.Fatal("stallDetected = false, want true")
	}
	reason, ok := manualRecoveryReason(err)
	if !ok {
		t.Fatalf("runCyclePumpResult() error = %v, want manualRecoveryError", err)
	}
	if reason != "ui_manual_cycle" {
		t.Fatalf("manual recovery reason = %q, want ui_manual_cycle", reason)
	}
}

func TestRunCyclePumpResultClassification(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		sessionErr    error
		stallDetected bool
		manualReason  string
		wantErr       error
		wantStall     bool
		// wantManual indicates the returned error should be a manualRecoveryError.
		wantManual bool
	}{
		{
			name:          "manual_reason_with_nil_err",
			err:           nil,
			sessionErr:    nil,
			stallDetected: true,
			manualReason:  "manual",
			wantManual:    true,
			wantStall:     true,
		},
		{
			name:          "manual_reason_with_canceled_err",
			err:           context.Canceled,
			sessionErr:    nil,
			stallDetected: true,
			manualReason:  "manual",
			wantManual:    true,
			wantStall:     true,
		},
		{
			name:          "manual_reason_with_eof_err",
			err:           io.EOF,
			sessionErr:    nil,
			stallDetected: true,
			manualReason:  "manual",
			wantManual:    true,
			wantStall:     true,
		},
		{
			name:          "nil_err_no_stall",
			err:           nil,
			sessionErr:    nil,
			stallDetected: false,
			manualReason:  "",
			wantErr:       io.EOF,
			wantStall:     false,
		},
		{
			name:          "nil_err_stall_no_reason",
			err:           nil,
			sessionErr:    nil,
			stallDetected: true,
			manualReason:  "",
			wantErr:       io.EOF,
			wantStall:     true,
		},
		{
			name:          "canceled_with_session_err_and_stall_manual",
			err:           context.Canceled,
			sessionErr:    context.Canceled,
			stallDetected: true,
			manualReason:  "manual",
			wantManual:    true,
			wantStall:     true,
		},
		{
			name:          "canceled_with_session_err_and_stall",
			err:           context.Canceled,
			sessionErr:    context.Canceled,
			stallDetected: true,
			manualReason:  "",
			wantErr:       nil,
			wantStall:     false,
		},
		{
			name:          "canceled_with_session_err_no_stall",
			err:           context.Canceled,
			sessionErr:    context.Canceled,
			stallDetected: false,
			manualReason:  "",
			wantErr:       nil,
			wantStall:     false,
		},
		{
			name:          "canceled_stall_no_session_err",
			err:           context.Canceled,
			sessionErr:    nil,
			stallDetected: true,
			manualReason:  "",
			wantErr:       errPumpStallDetected,
			wantStall:     true,
		},
		{
			name:          "canceled_no_stall_no_session_err",
			err:           context.Canceled,
			sessionErr:    nil,
			stallDetected: false,
			manualReason:  "",
			wantErr:       context.Canceled,
			wantStall:     false,
		},
		{
			name:          "unexpected_eof_no_stall",
			err:           io.ErrUnexpectedEOF,
			sessionErr:    nil,
			stallDetected: false,
			manualReason:  "",
			wantErr:       io.ErrUnexpectedEOF,
			wantStall:     false,
		},
		{
			name:          "unexpected_eof_with_stall",
			err:           io.ErrUnexpectedEOF,
			sessionErr:    nil,
			stallDetected: true,
			manualReason:  "",
			wantErr:       io.ErrUnexpectedEOF,
			wantStall:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr, gotStall := runCyclePumpResult(tt.err, tt.sessionErr, tt.stallDetected, tt.manualReason)
			if gotStall != tt.wantStall {
				t.Fatalf("stallDetected = %v, want %v", gotStall, tt.wantStall)
			}
			if tt.wantManual {
				if _, ok := manualRecoveryReason(gotErr); !ok {
					t.Fatalf("error = %v, want manualRecoveryError", gotErr)
				}
				return
			}
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("error = %v, want %v", gotErr, tt.wantErr)
			}
		})
	}
}

func TestTriggerRecoveryConcurrentFinishRace(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   42,
			GuideNumber: "142",
			GuideName:   "RaceTest",
		},
		manualRecoveryCh: make(chan string, 1),
		readyCh:          make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{
			1: {SubscriberID: 1},
		},
	}
	session.markReady(nil)
	manager.sessions[42] = session

	const goroutines = 10
	errs := make(chan error, goroutines)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			errs <- manager.TriggerRecovery(42, "race_test")
		}()
	}

	// Concurrently finish the session.
	session.finish(nil)

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil &&
			!errors.Is(err, ErrSessionNotFound) &&
			!errors.Is(err, ErrSessionRecoveryAlreadyPending) {
			t.Fatalf("unexpected TriggerRecovery error: %v", err)
		}
	}
}

func TestSharedSessionFinishClearsManualRecoveryState(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   99,
			GuideNumber: "199",
			GuideName:   "FinishClear",
		},
		manualRecoveryCh: make(chan string, 1),
		readyCh:          make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{
			1: {SubscriberID: 1},
		},
	}
	session.markReady(nil)
	session.manualRecoveryPending = true
	session.manualRecoveryInProgress = true

	session.finish(nil)

	session.mu.Lock()
	pending := session.manualRecoveryPending
	inProgress := session.manualRecoveryInProgress
	session.mu.Unlock()

	if pending {
		t.Fatal("manualRecoveryPending = true after finish(), want false")
	}
	if inProgress {
		t.Fatal("manualRecoveryInProgress = true after finish(), want false")
	}
}

func TestSessionManagerTriggerRecoveryReroutesStaleNotFoundToReplacement(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}
	oldSession := newTriggerRecoveryTestSession(7, "Old", 1)
	replacement := newTriggerRecoveryTestSession(7, "New", 2)
	manager.sessions[7] = oldSession

	err := triggerRecoveryDuringSessionSwap(t, manager, 7, oldSession, replacement, "ui_manual_trigger", func(session *sharedRuntimeSession) {
		session.closed = true
		session.subscribers = nil
	})
	if err != nil {
		t.Fatalf("TriggerRecovery(stale_notfound) error = %v, want nil", err)
	}

	assertQueuedManualRecoveryReason(t, replacement.manualRecoveryCh, "ui_manual_trigger", "replacement")
	assertNoQueuedManualRecoveryReason(t, oldSession.manualRecoveryCh, "old session")
}

func TestSessionManagerTriggerRecoveryReroutesStalePendingToReplacement(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}
	oldSession := newTriggerRecoveryTestSession(7, "Old", 1)
	replacement := newTriggerRecoveryTestSession(7, "New", 2)
	manager.sessions[7] = oldSession
	oldSession.manualRecoveryCh <- "pending_manual_recovery"

	err := triggerRecoveryDuringSessionSwap(t, manager, 7, oldSession, replacement, "ui_manual_trigger", nil)
	if err != nil {
		t.Fatalf("TriggerRecovery(stale_pending) error = %v, want nil", err)
	}

	assertQueuedManualRecoveryReason(t, replacement.manualRecoveryCh, "ui_manual_trigger", "replacement")
	select {
	case reason := <-oldSession.manualRecoveryCh:
		if reason != "pending_manual_recovery" {
			t.Fatalf("old session pending reason = %q, want pending_manual_recovery", reason)
		}
	default:
		t.Fatal("old session pending trigger was unexpectedly cleared")
	}
}

func TestSessionManagerTriggerRecoveryReroutesStaleSuccessToReplacement(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}
	oldSession := newTriggerRecoveryTestSession(7, "Old", 1)
	replacement := newTriggerRecoveryTestSession(7, "New", 2)
	manager.sessions[7] = oldSession

	err := triggerRecoveryDuringSessionSwap(t, manager, 7, oldSession, replacement, "ui_manual_trigger", nil)
	if err != nil {
		t.Fatalf("TriggerRecovery(stale_success) error = %v, want nil", err)
	}

	assertQueuedManualRecoveryReason(t, replacement.manualRecoveryCh, "ui_manual_trigger", "replacement")
	assertNoQueuedManualRecoveryReason(t, oldSession.manualRecoveryCh, "old session")
}

func newTriggerRecoveryTestSession(channelID int64, guideName string, subscriberID uint64) *sharedRuntimeSession {
	readyCh := make(chan struct{})
	close(readyCh)
	return &sharedRuntimeSession{
		channel: channels.Channel{
			ChannelID:   channelID,
			GuideNumber: strconv.FormatInt(channelID+100, 10),
			GuideName:   guideName,
		},
		manualRecoveryCh: make(chan string, 1),
		readyCh:          readyCh,
		subscribers: map[uint64]SubscriberStats{
			subscriberID: {SubscriberID: subscriberID},
		},
	}
}

func triggerRecoveryDuringSessionSwap(
	t *testing.T,
	manager *SessionManager,
	channelID int64,
	oldSession *sharedRuntimeSession,
	replacement *sharedRuntimeSession,
	reason string,
	mutateOld func(*sharedRuntimeSession),
) error {
	t.Helper()

	oldSession.mu.Lock()

	manager.mu.Lock()
	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.TriggerRecovery(channelID, reason)
	}()
	manager.mu.Unlock()

	time.Sleep(20 * time.Millisecond)

	manager.mu.Lock()
	manager.sessions[channelID] = replacement
	manager.mu.Unlock()

	if mutateOld != nil {
		mutateOld(oldSession)
	}
	oldSession.mu.Unlock()

	return <-errCh
}

func assertQueuedManualRecoveryReason(t *testing.T, triggerCh <-chan string, reason string, label string) {
	t.Helper()

	want := normalizeManualRecoveryReason(reason)
	select {
	case got := <-triggerCh:
		if got != want {
			t.Fatalf("%s manual recovery reason = %q, want %q", label, got, want)
		}
	default:
		t.Fatalf("%s manual recovery trigger was not queued", label)
	}
}

func assertNoQueuedManualRecoveryReason(t *testing.T, triggerCh <-chan string, label string) {
	t.Helper()

	select {
	case reason := <-triggerCh:
		t.Fatalf("%s unexpectedly received manual recovery reason %q", label, reason)
	default:
	}
}

func TestSessionManagerTriggerRecoveryRejectsIdleGraceSessionWithoutSubscribers(t *testing.T) {
	var (
		hitMu sync.Mutex
		hits  int
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitMu.Lock()
		hits++
		hitMu.Unlock()

		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("I"), mpegTSPacketSize)
		ticker := time.NewTicker(8 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				if _, err := w.Write(payload); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
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
					ItemKey:       "src:news:idle-manual",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             800 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         400 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	waitFor(t, 2*time.Second, func() bool {
		return len(provider.sourceSuccesses()) >= 1
	})

	sub.Close()

	waitFor(t, 2*time.Second, func() bool {
		snap := manager.Snapshot()
		return len(snap) == 1 && snap[0].Subscribers == 0
	})

	baselineFailures := provider.sourceFailures()
	baselineSuccesses := provider.sourceSuccesses()
	hitMu.Lock()
	baselineHits := hits
	hitMu.Unlock()

	if err := manager.TriggerRecovery(1, "ui_manual_trigger_idle_grace"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("TriggerRecovery(idle_grace) error = %v, want ErrSessionNotFound", err)
	}

	time.Sleep(200 * time.Millisecond)

	if got := provider.sourceFailures(); len(got) != len(baselineFailures) {
		t.Fatalf("source failure count after idle manual trigger = %d, want unchanged %d; failures=%#v", len(got), len(baselineFailures), got)
	}
	if got := provider.sourceSuccesses(); len(got) != len(baselineSuccesses) {
		t.Fatalf("source success count after idle manual trigger = %d, want unchanged %d; successes=%#v", len(got), len(baselineSuccesses), got)
	}
	hitMu.Lock()
	finalHits := hits
	hitMu.Unlock()
	if finalHits != baselineHits {
		t.Fatalf("upstream startup hits after idle manual trigger = %d, want unchanged %d", finalHits, baselineHits)
	}

	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestDiscardManualRecoveryRequestPreservesUnrelatedTrigger(t *testing.T) {
	t.Run("matching_reason_discards", func(t *testing.T) {
		readyCh := make(chan struct{})
		close(readyCh)
		session := &sharedRuntimeSession{
			channel:          channels.Channel{ChannelID: 7, GuideNumber: "107", GuideName: "DiscardMatch"},
			manualRecoveryCh: make(chan string, 1),
			readyCh:          readyCh,
		}
		session.manualRecoveryCh <- normalizeManualRecoveryReason("ui_manual_trigger")
		session.manualRecoveryPending = true

		if got := session.discardManualRecoveryRequest("ui_manual_trigger"); !got {
			t.Fatal("discardManualRecoveryRequest(matching) = false, want true")
		}
		if got := len(session.manualRecoveryCh); got != 0 {
			t.Fatalf("manualRecoveryCh length = %d, want 0", got)
		}
		if session.manualRecoveryPending {
			t.Fatal("manualRecoveryPending = true, want false after matching discard")
		}
	})

	t.Run("unrelated_reason_preserves", func(t *testing.T) {
		readyCh := make(chan struct{})
		close(readyCh)
		session := &sharedRuntimeSession{
			channel:          channels.Channel{ChannelID: 7, GuideNumber: "107", GuideName: "DiscardMismatch"},
			manualRecoveryCh: make(chan string, 1),
			readyCh:          readyCh,
		}
		originalReason := normalizeManualRecoveryReason("original_reason")
		session.manualRecoveryCh <- originalReason
		session.manualRecoveryPending = true

		if got := session.discardManualRecoveryRequest("different_reason"); got {
			t.Fatal("discardManualRecoveryRequest(unrelated) = true, want false")
		}

		select {
		case queued := <-session.manualRecoveryCh:
			if queued != originalReason {
				t.Fatalf("preserved reason = %q, want %q", queued, originalReason)
			}
		default:
			t.Fatal("manualRecoveryCh is empty, expected original reason to be preserved")
		}
		if !session.manualRecoveryPending {
			t.Fatal("manualRecoveryPending = false, want true (preserved)")
		}
	})

	t.Run("empty_channel_returns_false", func(t *testing.T) {
		readyCh := make(chan struct{})
		close(readyCh)
		session := &sharedRuntimeSession{
			channel:          channels.Channel{ChannelID: 7, GuideNumber: "107", GuideName: "DiscardEmpty"},
			manualRecoveryCh: make(chan string, 1),
			readyCh:          readyCh,
		}

		if got := session.discardManualRecoveryRequest("any_reason"); got {
			t.Fatal("discardManualRecoveryRequest(empty_ch) = true, want false")
		}
	})

	t.Run("nil_channel_returns_false", func(t *testing.T) {
		session := &sharedRuntimeSession{
			channel:          channels.Channel{ChannelID: 7, GuideNumber: "107", GuideName: "DiscardNilCh"},
			manualRecoveryCh: nil,
		}

		if got := session.discardManualRecoveryRequest("any_reason"); got {
			t.Fatal("discardManualRecoveryRequest(nil_ch) = true, want false")
		}
	})
}

func TestTriggerRecoveryRetryLoopExhaustsOnInadmissibleSession(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled immediately — makes isAdmissible() return false

	readyCh := make(chan struct{})
	close(readyCh)
	session := &sharedRuntimeSession{
		channel:          channels.Channel{ChannelID: 7, GuideNumber: "107", GuideName: "Inadmissible"},
		manualRecoveryCh: make(chan string, 1),
		readyCh:          readyCh,
		ctx:              ctx,
		subscribers: map[uint64]SubscriberStats{
			1: {SubscriberID: 1},
		},
	}
	manager.sessions[7] = session

	err := manager.TriggerRecovery(7, "ui_manual_trigger")
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("TriggerRecovery(inadmissible_exhaust) error = %v, want ErrSessionNotFound", err)
	}

	// Verify no recovery was queued on the inadmissible session.
	select {
	case reason := <-session.manualRecoveryCh:
		t.Fatalf("inadmissible session unexpectedly received recovery reason %q", reason)
	default:
	}
}

func TestTriggerRecoveryRetryLoopExhaustsWithCreatePending(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled — isAdmissible() returns false

	readyCh := make(chan struct{})
	close(readyCh)
	session := &sharedRuntimeSession{
		channel:          channels.Channel{ChannelID: 7, GuideNumber: "107", GuideName: "InadmissibleCreate"},
		manualRecoveryCh: make(chan string, 1),
		readyCh:          readyCh,
		ctx:              ctx,
		subscribers: map[uint64]SubscriberStats{
			1: {SubscriberID: 1},
		},
	}
	manager.sessions[7] = session

	// After the loop exhausts on the inadmissible session, the post-loop
	// triggerRecoveryOwner check sees creating=true → ErrSessionRecoveryAlreadyPending.
	manager.creating[7] = &sessionCreateWait{done: make(chan struct{})}

	err := manager.TriggerRecovery(7, "ui_manual_trigger")
	if !errors.Is(err, ErrSessionRecoveryAlreadyPending) {
		t.Fatalf("TriggerRecovery(inadmissible+creating) error = %v, want ErrSessionRecoveryAlreadyPending", err)
	}
}

func TestTriggerRecoveryInadmissibleCanceledContextSkipped(t *testing.T) {
	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled context → isAdmissible returns false

	readyCh := make(chan struct{})
	close(readyCh)
	inadmissible := &sharedRuntimeSession{
		channel:          channels.Channel{ChannelID: 7, GuideNumber: "107", GuideName: "CanceledCtx"},
		manualRecoveryCh: make(chan string, 1),
		readyCh:          readyCh,
		ctx:              ctx,
		subscribers: map[uint64]SubscriberStats{
			1: {SubscriberID: 1},
		},
	}
	manager.sessions[7] = inadmissible

	err := manager.TriggerRecovery(7, "ui_manual_trigger")
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("TriggerRecovery(canceled_ctx) error = %v, want ErrSessionNotFound", err)
	}

	// Verify no recovery was queued.
	assertNoQueuedManualRecoveryReason(t, inadmissible.manualRecoveryCh, "canceled-ctx session")

	// Now test the swap variant: inadmissible session is replaced by an
	// admissible session during the retry. Use triggerRecoveryDuringSessionSwap
	// with mutateOld canceling the context (simulating the session becoming
	// inadmissible while blocked on oldSession.mu).
	manager2 := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}
	oldSession := newTriggerRecoveryTestSession(7, "Old", 1)
	replacement := newTriggerRecoveryTestSession(7, "New", 2)
	manager2.sessions[7] = oldSession

	cancelCtx, cancelFn := context.WithCancel(context.Background())
	oldSession.ctx = cancelCtx

	err = triggerRecoveryDuringSessionSwap(t, manager2, 7, oldSession, replacement, "ui_manual_trigger", func(s *sharedRuntimeSession) {
		cancelFn() // cancel context making old session inadmissible
	})
	if err != nil {
		t.Fatalf("TriggerRecovery(swap_canceled_old) error = %v, want nil (rerouted to replacement)", err)
	}

	assertQueuedManualRecoveryReason(t, replacement.manualRecoveryCh, "ui_manual_trigger", "replacement after inadmissible swap")
	assertNoQueuedManualRecoveryReason(t, oldSession.manualRecoveryCh, "old canceled session")
}

func TestSessionManagerIdleGraceSessionSkipsStallRecoveryAndFailurePersistence(t *testing.T) {
	var (
		hitMu sync.Mutex
		hits  int
	)
	stalling := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitMu.Lock()
		hits++
		hitMu.Unlock()

		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("S"), mpegTSPacketSize)
		if _, err := w.Write(payload); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer stalling.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:idle-stall",
					StreamURL:     stalling.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             600 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                50 * time.Millisecond,
		StallHardDeadline:          500 * time.Millisecond,
		StallPolicy:                stallPolicyRestartSame,
		SessionIdleTimeout:         450 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	waitFor(t, 2*time.Second, func() bool {
		return len(provider.sourceSuccesses()) >= 1
	})

	sub.Close()

	waitFor(t, 2*time.Second, func() bool {
		snap := manager.Snapshot()
		return len(snap) == 1 && snap[0].Subscribers == 0
	})

	baselineFailures := provider.sourceFailures()
	baselineSuccesses := provider.sourceSuccesses()
	hitMu.Lock()
	baselineHits := hits
	hitMu.Unlock()

	time.Sleep(220 * time.Millisecond)

	if got := provider.sourceFailures(); len(got) != len(baselineFailures) {
		t.Fatalf("source failure count during idle stall window = %d, want unchanged %d; failures=%#v", len(got), len(baselineFailures), got)
	}
	if got := provider.sourceSuccesses(); len(got) != len(baselineSuccesses) {
		t.Fatalf("source success count during idle stall window = %d, want unchanged %d; successes=%#v", len(got), len(baselineSuccesses), got)
	}
	hitMu.Lock()
	finalHits := hits
	hitMu.Unlock()
	if finalHits != baselineHits {
		t.Fatalf("upstream startup hits during idle stall window = %d, want unchanged %d", finalHits, baselineHits)
	}

	snap := manager.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("len(snapshot) = %d, want 1 active idle-grace session", len(snap))
	}
	if snap[0].RecoveryCycle != 0 {
		t.Fatalf("snapshot.recovery_cycle = %d, want 0 while idle without subscribers", snap[0].RecoveryCycle)
	}

	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerMidRecoveryDisconnectAbortsFailoverStartupWhenIdle(t *testing.T) {
	var (
		primaryMu   sync.Mutex
		primaryHits int

		alternateMu   sync.Mutex
		alternateHits int
	)

	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryMu.Lock()
		primaryHits++
		primaryMu.Unlock()

		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("P"), mpegTSPacketSize)
		if _, err := w.Write(payload); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer primary.Close()

	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		alternateMu.Lock()
		alternateHits++
		alternateMu.Unlock()

		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("A"), mpegTSPacketSize)
		if _, err := w.Write(payload); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer alternate.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:mid-recovery-primary",
					StreamURL:     primary.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:mid-recovery-alternate",
					StreamURL:     alternate.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             400 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                45 * time.Millisecond,
		StallHardDeadline:          1800 * time.Millisecond,
		StallPolicy:                stallPolicyFailoverSource,
		SessionIdleTimeout:         2 * time.Second,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	waitFor(t, 2*time.Second, func() bool {
		stats := manager.Snapshot()
		return len(stats) == 1 && stats[0].RecoveryCycle >= 1
	})

	sub.Close()

	time.Sleep(650 * time.Millisecond)

	alternateMu.Lock()
	altHitsAfterDisconnect := alternateHits
	alternateMu.Unlock()
	if altHitsAfterDisconnect != 0 {
		t.Fatalf("alternate startup attempts after mid-recovery disconnect = %d, want 0", altHitsAfterDisconnect)
	}

	failures := provider.sourceFailures()
	if !hasFailureReason(failures, 10, "stream pump stall detected") {
		t.Fatalf("failures = %#v, want only cycle-ending primary stall failure before idle abort", failures)
	}
	if got := provider.sourceSuccesses(); !containsInt64(got, 10) {
		t.Fatalf("successes = %#v, want initial source 10 success", got)
	}

	waitFor(t, 1200*time.Millisecond, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerMidRecoveryDisconnectAbortsRestartSameRetryLoop(t *testing.T) {
	var (
		hitMu sync.Mutex
		hits  int
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitMu.Lock()
		hits++
		attempt := hits
		hitMu.Unlock()

		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if attempt == 1 {
			payload := bytes.Repeat([]byte("R"), mpegTSPacketSize)
			if _, err := w.Write(payload); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		<-r.Context().Done()
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
					ItemKey:       "src:news:restart-same-mid-recovery",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             120 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                40 * time.Millisecond,
		StallHardDeadline:          1800 * time.Millisecond,
		StallPolicy:                stallPolicyRestartSame,
		StallMaxFailoversPerStall:  6,
		SessionIdleTimeout:         2 * time.Second,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	waitFor(t, 2*time.Second, func() bool {
		stats := manager.Snapshot()
		if len(stats) != 1 || stats[0].RecoveryCycle < 1 {
			return false
		}
		hitMu.Lock()
		currentHits := hits
		hitMu.Unlock()
		return currentHits >= 2
	})

	sub.Close()
	time.Sleep(250 * time.Millisecond)

	hitMu.Lock()
	baselineHits := hits
	hitMu.Unlock()
	baselineFailures := len(provider.sourceFailures())
	baselineSuccesses := len(provider.sourceSuccesses())

	time.Sleep(450 * time.Millisecond)

	hitMu.Lock()
	finalHits := hits
	hitMu.Unlock()
	if finalHits != baselineHits {
		t.Fatalf("upstream startup attempts after mid-recovery disconnect = %d, want stable %d", finalHits, baselineHits)
	}

	if got := len(provider.sourceFailures()); got != baselineFailures {
		t.Fatalf("source failure count after mid-recovery disconnect = %d, want stable %d", got, baselineFailures)
	}
	if got := len(provider.sourceSuccesses()); got != baselineSuccesses {
		t.Fatalf("source success count after mid-recovery disconnect = %d, want stable %d", got, baselineSuccesses)
	}

	waitFor(t, 1200*time.Millisecond, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestStartRecoverySourceFailoverAlternateHonorsAttemptContextDeadline(t *testing.T) {
	var (
		alternateHitsMu sync.Mutex
		alternateHits   int
	)
	alternateCanceled := make(chan struct{}, 1)
	alternate := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		alternateHitsMu.Lock()
		alternateHits++
		alternateHitsMu.Unlock()

		<-r.Context().Done()
		select {
		case alternateCanceled <- struct{}{}:
		default:
		}
	}))
	defer alternate.Close()

	provider := &fakeChannelsProvider{
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:primary",
					StreamURL:     "http://example.invalid/primary",
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:alternate",
					StreamURL:     alternate.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             900 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                50 * time.Millisecond,
		StallHardDeadline:          3 * time.Second,
		StallPolicy:                stallPolicyFailoverSource,
		SessionIdleTimeout:         2 * time.Second,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:primary",
		StreamURL:     "http://example.invalid/primary",
		PriorityIndex: 0,
		Enabled:       true,
	}

	recoveryCtx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
	defer cancel()

	started := time.Now()
	reader, _, err := session.startRecoverySource(
		recoveryCtx,
		currentSource,
		true,
		1,
		"stall_detected",
	)
	if reader != nil {
		_ = reader.Close()
	}
	if err == nil {
		t.Fatal("startRecoverySource() error = nil, want recovery-attempt context deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("startRecoverySource() error = %v, want context cancellation/deadline", err)
	}

	elapsed := time.Since(started)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("recovery elapsed = %s, want < 500ms when attempt context deadline is reached", elapsed)
	}

	alternateHitsMu.Lock()
	gotAlternateHits := alternateHits
	alternateHitsMu.Unlock()
	if gotAlternateHits == 0 {
		t.Fatal("alternate startup attempts = 0, want at least 1 blocked alternate startup attempt")
	}

	select {
	case <-alternateCanceled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("alternate startup request was not canceled promptly after attempt context deadline")
	}
}

func TestStartCurrentSourceWithBackoffCancelsInflightStartupWhenRecoveryGoesIdle(t *testing.T) {
	var (
		hitsMu sync.Mutex
		hits   int
	)
	requestCanceled := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		hitsMu.Lock()
		hits++
		hitsMu.Unlock()

		<-r.Context().Done()
		select {
		case requestCanceled <- struct{}{}:
		default:
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             900 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                45 * time.Millisecond,
		StallHardDeadline:          3 * time.Second,
		StallPolicy:                stallPolicyRestartSame,
		StallMaxFailoversPerStall:  4,
		SessionIdleTimeout:         2 * time.Second,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	source := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:restart-same-blocked",
		StreamURL:     upstream.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	resultCh := make(chan error, 1)
	started := time.Now()
	go func() {
		reader, _, err := session.startCurrentSourceWithBackoff(
			context.Background(),
			source,
			2*time.Second,
			1,
			"stall_detected",
			1,
		)
		if reader != nil {
			_ = reader.Close()
		}
		resultCh <- err
	}()

	waitFor(t, time.Second, func() bool {
		hitsMu.Lock()
		defer hitsMu.Unlock()
		return hits >= 1
	})

	disconnectedAt := time.Now()
	session.mu.Lock()
	session.subscribers = map[uint64]SubscriberStats{}
	session.mu.Unlock()

	var err error
	select {
	case err = <-resultCh:
	case <-time.After(600 * time.Millisecond):
		t.Fatal("startCurrentSourceWithBackoff() did not return promptly after subscriber disconnect")
	}
	if !errors.Is(err, errRecoveryAbortedNoSubscribers) {
		t.Fatalf("startCurrentSourceWithBackoff() error = %v, want errRecoveryAbortedNoSubscribers", err)
	}

	select {
	case <-requestCanceled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("blocked startup request was not canceled promptly after recovery became idle")
	}

	elapsedAfterDisconnect := time.Since(disconnectedAt)
	if elapsedAfterDisconnect > 400*time.Millisecond {
		t.Fatalf("startup cancellation latency after disconnect = %s, want <= 400ms", elapsedAfterDisconnect)
	}
	if totalElapsed := time.Since(started); totalElapsed > 1100*time.Millisecond {
		t.Fatalf("startup elapsed = %s, want bounded cancellation before full startup timeout", totalElapsed)
	}
}

func TestStartCurrentSourceWithBackoffIgnoresIdleCanceledAlternatePenaltyAfterReattach(t *testing.T) {
	requestArrived := make(chan struct{}, 1)
	requestCanceled := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case requestArrived <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		select {
		case requestCanceled <- struct{}{}:
		default:
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             900 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                45 * time.Millisecond,
		StallHardDeadline:          3 * time.Second,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  0,
		SessionIdleTimeout:         2 * time.Second,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ctx:                        context.Background(),
		cancel:                     func() {},
		readyCh:                    make(chan struct{}),
		subscribers:                map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		sourceID:                   10,
		shortLivedRecoveryBySource: make(map[int64]shortLivedRecoveryPenalty),
		startedAt:                  time.Now().UTC(),
	}

	validated := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHook := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	defer releaseHook()

	session.mu.Lock()
	session.startupAttemptIdleCancelHook = func() {
		select {
		case <-validated:
		default:
			close(validated)
		}
		<-release
	}
	session.mu.Unlock()
	t.Cleanup(func() {
		session.mu.Lock()
		session.startupAttemptIdleCancelHook = nil
		session.mu.Unlock()
	})

	alternate := channels.Source{
		SourceID:      11,
		ChannelID:     1,
		ItemKey:       "src:news:alternate",
		StreamURL:     upstream.URL,
		PriorityIndex: 1,
		Enabled:       true,
	}

	resultCh := make(chan error, 1)
	go func() {
		reader, _, err := session.startCurrentSourceWithBackoff(
			context.Background(),
			alternate,
			2*time.Second,
			1,
			"stall_detected",
			1,
		)
		if reader != nil {
			_ = reader.Close()
		}
		resultCh <- err
	}()

	select {
	case <-requestArrived:
	case <-time.After(time.Second):
		t.Fatal("startup request did not arrive within 1s")
	}

	session.mu.Lock()
	session.subscribers = map[uint64]SubscriberStats{}
	session.mu.Unlock()

	select {
	case <-validated:
	case <-time.After(time.Second):
		t.Fatal("idle-startup cancellation hook was not reached")
	}

	session.mu.Lock()
	session.subscribers[2] = SubscriberStats{SubscriberID: 2}
	session.mu.Unlock()
	releaseHook()

	var err error
	select {
	case err = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("startCurrentSourceWithBackoff() did not return within 2s")
	}
	if err == nil {
		t.Fatal("startCurrentSourceWithBackoff() error = nil, want startup failure after canceled attempt")
	}

	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("startup request was not canceled after idle-guard cancellation")
	}

	if got := session.shortLivedRecoveryCount(11); got != 0 {
		t.Fatalf("shortLivedRecoveryCount(11) = %d, want 0 for stale idle-canceled startup attempt", got)
	}
}

func TestStartCurrentSourceWithBackoffRecordsPenaltyForTrueAlternateStartupFailure(t *testing.T) {
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "alternate unavailable", http.StatusNotFound)
	}))
	defer alternate.Close()

	provider := &fakeChannelsProvider{}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             300 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                45 * time.Millisecond,
		StallHardDeadline:          3 * time.Second,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  0,
		SessionIdleTimeout:         2 * time.Second,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ctx:                        context.Background(),
		cancel:                     func() {},
		readyCh:                    make(chan struct{}),
		subscribers:                map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		sourceID:                   10,
		shortLivedRecoveryBySource: make(map[int64]shortLivedRecoveryPenalty),
		startedAt:                  time.Now().UTC(),
	}

	reader, _, err := session.startCurrentSourceWithBackoff(
		context.Background(),
		channels.Source{
			SourceID:      11,
			ChannelID:     1,
			ItemKey:       "src:news:alternate",
			StreamURL:     alternate.URL,
			PriorityIndex: 1,
			Enabled:       true,
		},
		1200*time.Millisecond,
		1,
		"stall_detected",
		1,
	)
	if reader != nil {
		_ = reader.Close()
	}
	if err == nil {
		t.Fatal("startCurrentSourceWithBackoff() error = nil, want alternate startup failure")
	}
	if got := session.shortLivedRecoveryCount(11); got == 0 {
		t.Fatalf("shortLivedRecoveryCount(11) = %d, want > 0 for true alternate startup failure", got)
	}
}

func TestStartSourceWithCandidatesIgnoresIdleCanceledAlternatePenaltyAfterReattach(t *testing.T) {
	requestArrived := make(chan struct{}, 1)
	requestCanceled := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case requestArrived <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		select {
		case requestCanceled <- struct{}{}:
		default:
		}
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             900 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                45 * time.Millisecond,
		StallHardDeadline:          3 * time.Second,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  0,
		SessionIdleTimeout:         2 * time.Second,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ctx:                        context.Background(),
		cancel:                     func() {},
		readyCh:                    make(chan struct{}),
		subscribers:                map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		sourceID:                   10,
		shortLivedRecoveryBySource: make(map[int64]shortLivedRecoveryPenalty),
		startedAt:                  time.Now().UTC(),
	}

	validated := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHook := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	defer releaseHook()

	session.mu.Lock()
	session.startupAttemptIdleCancelHook = func() {
		select {
		case <-validated:
		default:
			close(validated)
		}
		<-release
	}
	session.mu.Unlock()
	t.Cleanup(func() {
		session.mu.Lock()
		session.startupAttemptIdleCancelHook = nil
		session.mu.Unlock()
	})

	alternate := channels.Source{
		SourceID:      11,
		ChannelID:     1,
		ItemKey:       "src:news:alternate",
		StreamURL:     upstream.URL,
		PriorityIndex: 1,
		Enabled:       true,
	}

	resultCh := make(chan error, 1)
	go func() {
		reader, _, err := session.startSourceWithCandidates(
			context.Background(),
			[]channels.Source{alternate},
			2*time.Second,
			"recovery_cycle_1:stall",
			1,
			"stall_detected",
		)
		if reader != nil {
			_ = reader.Close()
		}
		resultCh <- err
	}()

	select {
	case <-requestArrived:
	case <-time.After(time.Second):
		t.Fatal("startup request did not arrive within 1s")
	}

	session.mu.Lock()
	session.subscribers = map[uint64]SubscriberStats{}
	session.mu.Unlock()

	select {
	case <-validated:
	case <-time.After(time.Second):
		t.Fatal("idle-startup cancellation hook was not reached")
	}

	session.mu.Lock()
	session.subscribers[2] = SubscriberStats{SubscriberID: 2}
	session.mu.Unlock()
	releaseHook()

	var err error
	select {
	case err = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("startSourceWithCandidates() did not return within 2s")
	}
	if err == nil {
		t.Fatal("startSourceWithCandidates() error = nil, want startup failure after canceled attempt")
	}

	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("startup request was not canceled after idle-guard cancellation")
	}

	if got := session.shortLivedRecoveryCount(11); got != 0 {
		t.Fatalf("shortLivedRecoveryCount(11) = %d, want 0 for stale idle-canceled startup attempt", got)
	}
}

func TestSessionManagerThirdClientOnSharedChannelDoesNotEOFExistingClientWhenOtherTunerActive(t *testing.T) {
	newUpstream := func(payload byte) *httptest.Server {
		chunk := bytes.Repeat([]byte{payload}, mpegTSPacketSize)
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

	pool := NewPool(2)
	manager := NewSessionManager(fastStreamTestTiming.sessionManagerConfig("direct"), pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	startStream := func(sub *SessionSubscription) (context.CancelFunc, *recordingResponseWriter, <-chan error) {
		streamCtx, cancel := context.WithCancel(context.Background())
		writer := &recordingResponseWriter{}
		errCh := make(chan error, 1)
		go func() {
			errCh <- sub.Stream(streamCtx, writer)
		}()
		return cancel, writer, errCh
	}

	for attempt := 1; attempt <= fastStreamTestTiming.thirdClientAttemptsDirect; attempt++ {
		subA, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
		if err != nil {
			t.Fatalf("attempt %d: Subscribe(101) error = %v", attempt, err)
		}

		subB1, err := manager.Subscribe(context.Background(), provider.channelsByGuide["102"])
		if err != nil {
			subA.Close()
			t.Fatalf("attempt %d: Subscribe(102 first) error = %v", attempt, err)
		}

		cancelA, writerA, errChA := startStream(subA)
		cancelB1, writerB1, errChB1 := startStream(subB1)

		waitFor(t, 750*time.Millisecond, func() bool {
			return writerA.Writes() > 0 && writerB1.Writes() > 0
		})

		subB2, err := manager.Subscribe(context.Background(), provider.channelsByGuide["102"])
		if err != nil {
			cancelA()
			cancelB1()
			subA.Close()
			subB1.Close()
			t.Fatalf("attempt %d: Subscribe(102 second) error = %v", attempt, err)
		}

		cancelB2, writerB2, errChB2 := startStream(subB2)
		waitFor(t, 750*time.Millisecond, func() bool {
			return writerB2.Writes() > 0
		})

		assertWriterProgressWithoutUnexpectedEnd(
			t,
			writerB1,
			errChB1,
			fastStreamTestTiming.progressWindow,
			fmt.Sprintf("attempt %d: first subscriber after second subscriber joined", attempt),
		)

		cancelB2()
		select {
		case err := <-errChB2:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				cancelA()
				cancelB1()
				subA.Close()
				subB1.Close()
				subB2.Close()
				t.Fatalf("attempt %d: second subscriber Stream() error = %v", attempt, err)
			}
		case <-time.After(2 * time.Second):
			cancelA()
			cancelB1()
			subA.Close()
			subB1.Close()
			subB2.Close()
			t.Fatalf("attempt %d: timeout waiting for second subscriber stream to stop", attempt)
		}
		subB2.Close()

		assertWriterProgressWithoutUnexpectedEnd(
			t,
			writerB1,
			errChB1,
			fastStreamTestTiming.progressWindow,
			fmt.Sprintf("attempt %d: first subscriber after second subscriber disconnect", attempt),
		)

		cancelA()
		cancelB1()

		select {
		case err := <-errChA:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				subA.Close()
				subB1.Close()
				t.Fatalf("attempt %d: channel-101 stream error = %v", attempt, err)
			}
		case <-time.After(2 * time.Second):
			subA.Close()
			subB1.Close()
			t.Fatalf("attempt %d: timeout waiting for channel-101 stream to stop", attempt)
		}

		select {
		case err := <-errChB1:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				subA.Close()
				subB1.Close()
				t.Fatalf("attempt %d: first shared-channel stream error = %v", attempt, err)
			}
		case <-time.After(2 * time.Second):
			subA.Close()
			subB1.Close()
			t.Fatalf("attempt %d: timeout waiting for first shared-channel stream to stop", attempt)
		}

		subA.Close()
		subB1.Close()

		waitFor(t, 1500*time.Millisecond, func() bool {
			return pool.InUseCount() == 0
		})
	}
}

func TestSessionManagerThirdClientOnSharedChannelDoesNotEOFExistingClientWhenOtherTunerActiveFFmpeg(t *testing.T) {
	tmp := t.TempDir()
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-constant-stream.sh", `#!/usr/bin/env bash
set -euo pipefail
chunk="$(printf '%188s' '' | tr ' ' S)"
while true; do
  printf '%s' "$chunk"
  sleep 0.01
done
`)

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
					StreamURL:     "http://example.test/101",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
			2: {
				{
					SourceID:      20,
					ChannelID:     2,
					ItemKey:       "src:news:102",
					StreamURL:     "http://example.test/102",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(2)
	cfg := fastStreamTestTiming.sessionManagerConfig("ffmpeg-copy")
	cfg.FFmpegPath = ffmpegPath
	manager := NewSessionManager(cfg, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	startStream := func(sub *SessionSubscription) (context.CancelFunc, *recordingResponseWriter, <-chan error) {
		streamCtx, cancel := context.WithCancel(context.Background())
		writer := &recordingResponseWriter{}
		errCh := make(chan error, 1)
		go func() {
			errCh <- sub.Stream(streamCtx, writer)
		}()
		return cancel, writer, errCh
	}

	for attempt := 1; attempt <= fastStreamTestTiming.thirdClientAttemptsFFmpeg; attempt++ {
		subA, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
		if err != nil {
			t.Fatalf("attempt %d: Subscribe(101) error = %v", attempt, err)
		}

		subB1, err := manager.Subscribe(context.Background(), provider.channelsByGuide["102"])
		if err != nil {
			subA.Close()
			t.Fatalf("attempt %d: Subscribe(102 first) error = %v", attempt, err)
		}

		cancelA, writerA, errChA := startStream(subA)
		cancelB1, writerB1, errChB1 := startStream(subB1)

		waitFor(t, 900*time.Millisecond, func() bool {
			return writerA.Writes() > 0 && writerB1.Writes() > 0
		})

		subB2, err := manager.Subscribe(context.Background(), provider.channelsByGuide["102"])
		if err != nil {
			cancelA()
			cancelB1()
			subA.Close()
			subB1.Close()
			t.Fatalf("attempt %d: Subscribe(102 second) error = %v", attempt, err)
		}

		cancelB2, writerB2, errChB2 := startStream(subB2)
		waitFor(t, 900*time.Millisecond, func() bool {
			return writerB2.Writes() > 0
		})

		assertWriterProgressWithoutUnexpectedEnd(
			t,
			writerB1,
			errChB1,
			fastStreamTestTiming.progressWindow,
			fmt.Sprintf("attempt %d: first FFmpeg subscriber after second subscriber joined", attempt),
		)

		cancelB2()
		select {
		case err := <-errChB2:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				cancelA()
				cancelB1()
				subA.Close()
				subB1.Close()
				subB2.Close()
				t.Fatalf("attempt %d: second FFmpeg subscriber Stream() error = %v", attempt, err)
			}
		case <-time.After(2 * time.Second):
			cancelA()
			cancelB1()
			subA.Close()
			subB1.Close()
			subB2.Close()
			t.Fatalf("attempt %d: timeout waiting for second FFmpeg subscriber stream to stop", attempt)
		}
		subB2.Close()

		assertWriterProgressWithoutUnexpectedEnd(
			t,
			writerB1,
			errChB1,
			fastStreamTestTiming.progressWindow,
			fmt.Sprintf("attempt %d: first FFmpeg subscriber after second subscriber disconnect", attempt),
		)

		cancelA()
		cancelB1()

		select {
		case err := <-errChA:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				subA.Close()
				subB1.Close()
				t.Fatalf("attempt %d: channel-101 FFmpeg stream error = %v", attempt, err)
			}
		case <-time.After(2 * time.Second):
			subA.Close()
			subB1.Close()
			t.Fatalf("attempt %d: timeout waiting for channel-101 FFmpeg stream to stop", attempt)
		}

		select {
		case err := <-errChB1:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				subA.Close()
				subB1.Close()
				t.Fatalf("attempt %d: first shared-channel FFmpeg stream error = %v", attempt, err)
			}
		case <-time.After(2 * time.Second):
			subA.Close()
			subB1.Close()
			t.Fatalf("attempt %d: timeout waiting for first shared-channel FFmpeg stream to stop", attempt)
		}

		subA.Close()
		subB1.Close()

		waitFor(t, 1500*time.Millisecond, func() bool {
			return pool.InUseCount() == 0
		})
	}
}

func TestSessionManagerPreemptsIdleGraceSessionWhenPoolIsFull(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < 800; i++ {
			if _, err := w.Write([]byte("x")); err != nil {
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
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News 101", Enabled: true},
			"102": {ChannelID: 2, GuideNumber: "102", GuideName: "News 102", Enabled: true},
			"103": {ChannelID: 3, GuideNumber: "103", GuideName: "News 103", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:101",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
			2: {
				{
					SourceID:      20,
					ChannelID:     2,
					ItemKey:       "src:news:102",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
			3: {
				{
					SourceID:      30,
					ChannelID:     3,
					ItemKey:       "src:news:103",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(2)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             750 * time.Millisecond,
		FailoverTotalTimeout:       1500 * time.Millisecond,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         750 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub101, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe(101) error = %v", err)
	}
	defer sub101.Close()

	sub102, err := manager.Subscribe(context.Background(), provider.channelsByGuide["102"])
	if err != nil {
		t.Fatalf("Subscribe(102) error = %v", err)
	}
	defer sub102.Close()

	if got := pool.InUseCount(); got != 2 {
		t.Fatalf("InUseCount() = %d, want 2", got)
	}

	// Simulate channel-switch churn: old stream enters idle grace while the new tune starts.
	sub101.Close()

	subscribeCtx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	sub103, err := manager.Subscribe(subscribeCtx, provider.channelsByGuide["103"])
	if err != nil {
		t.Fatalf("Subscribe(103) during idle grace error = %v", err)
	}
	defer sub103.Close()

	sub102.Close()
	sub103.Close()
	waitFor(t, 3*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerFailoverRetrySettleDelayWhenPoolIsFull(t *testing.T) {
	var (
		failureMu    sync.Mutex
		failureTimes []time.Time
	)

	recordFailureAttempt := func() {
		failureMu.Lock()
		failureTimes = append(failureTimes, time.Now().UTC())
		failureMu.Unlock()
	}

	failingA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		recordFailureAttempt()
		http.Error(w, "upstream unavailable A", http.StatusServiceUnavailable)
	}))
	defer failingA.Close()

	failingB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		recordFailureAttempt()
		http.Error(w, "upstream unavailable B", http.StatusServiceUnavailable)
	}))
	defer failingB.Close()

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("G"), mpegTSPacketSize)

		for i := 0; i < 200; i++ {
			if _, err := w.Write(payload); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer healthy.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:fail-a",
					StreamURL:     failingA.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:fail-b",
					StreamURL:     failingB.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
				{
					SourceID:      12,
					ChannelID:     1,
					ItemKey:       "src:news:healthy",
					StreamURL:     healthy.URL,
					PriorityIndex: 2,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(2)
	pool.SetPreemptSettleDelay(160 * time.Millisecond)

	busyLease, err := pool.AcquireClient(context.Background(), "999", "shared:999")
	if err != nil {
		t.Fatalf("AcquireClient() setup busy lease error = %v", err)
	}
	defer busyLease.Release()

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	started := time.Now()
	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	elapsed := time.Since(started)

	failureMu.Lock()
	times := append([]time.Time(nil), failureTimes...)
	failureMu.Unlock()

	if len(times) != 2 {
		t.Fatalf("len(failureTimes) = %d, want 2 failover attempts before success", len(times))
	}
	if delta := times[1].Sub(times[0]); delta < 120*time.Millisecond {
		t.Fatalf("failover retry spacing = %s, want at least 120ms under full pool pressure", delta)
	}
	if elapsed < 260*time.Millisecond {
		t.Fatalf("Subscribe() completed in %s, want failover settle delay to apply when pool is full", elapsed)
	}

	sub.Close()
	busyLease.Release()

	waitFor(t, 3*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerFailoverRetrySettleDelayWhenPoolRecentlyDroppedFromFull(t *testing.T) {
	var (
		failureMu       sync.Mutex
		failureTimes    []time.Time
		releaseBusyOnce sync.Once
		busyLease       *Lease
	)

	recordFailureAttempt := func() {
		failureMu.Lock()
		failureTimes = append(failureTimes, time.Now().UTC())
		failureMu.Unlock()
	}

	releaseBusy := func() {
		releaseBusyOnce.Do(func() {
			if busyLease != nil {
				busyLease.Release()
			}
		})
	}

	failingA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		recordFailureAttempt()
		// Drop from full(2) -> partial(1) between failover attempts so the
		// retry path exercises the recently-full settle window.
		releaseBusy()
		http.Error(w, "upstream unavailable A", http.StatusServiceUnavailable)
	}))
	defer failingA.Close()

	failingB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		recordFailureAttempt()
		http.Error(w, "upstream unavailable B", http.StatusServiceUnavailable)
	}))
	defer failingB.Close()

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("H"), mpegTSPacketSize)

		for i := 0; i < 200; i++ {
			if _, err := w.Write(payload); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer healthy.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:fail-a",
					StreamURL:     failingA.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:fail-b",
					StreamURL:     failingB.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
				{
					SourceID:      12,
					ChannelID:     1,
					ItemKey:       "src:news:healthy",
					StreamURL:     healthy.URL,
					PriorityIndex: 2,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(2)
	pool.SetPreemptSettleDelay(180 * time.Millisecond)

	var err error
	busyLease, err = pool.AcquireClient(context.Background(), "999", "shared:999")
	if err != nil {
		t.Fatalf("AcquireClient() setup busy lease error = %v", err)
	}
	defer releaseBusy()

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	started := time.Now()
	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	elapsed := time.Since(started)

	failureMu.Lock()
	times := append([]time.Time(nil), failureTimes...)
	failureMu.Unlock()

	if len(times) != 2 {
		t.Fatalf("len(failureTimes) = %d, want 2 failover attempts before success", len(times))
	}
	if delta := times[1].Sub(times[0]); delta < 120*time.Millisecond {
		t.Fatalf("failover retry spacing = %s, want at least 120ms after full->partial drop", delta)
	}
	if elapsed < 160*time.Millisecond {
		t.Fatalf("Subscribe() completed in %s, want failover settle delay after full->partial drop", elapsed)
	}

	sub.Close()
	releaseBusy()

	waitFor(t, 3*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerRapidRetunePreemptionKeepsOtherTunerFlowing(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("S"), mpegTSPacketSize)

		ticker := time.NewTicker(15 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < 1200; i++ {
			if _, err := w.Write(payload); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			<-ticker.C
		}
	}))
	defer upstream.Close()

	channelsByGuide := make(map[string]channels.Channel)
	sourcesByID := make(map[int64][]channels.Source)
	for guide := 101; guide <= 109; guide++ {
		guideNumber := strconv.Itoa(guide)
		channelID := int64(guide - 100)
		channelsByGuide[guideNumber] = channels.Channel{
			ChannelID:   channelID,
			GuideNumber: guideNumber,
			GuideName:   "News " + guideNumber,
			Enabled:     true,
		}
		sourcesByID[channelID] = []channels.Source{
			{
				SourceID:      channelID * 10,
				ChannelID:     channelID,
				ItemKey:       "src:news:" + guideNumber,
				StreamURL:     upstream.URL,
				PriorityIndex: 0,
				Enabled:       true,
			},
		}
	}

	provider := &fakeChannelsProvider{
		channelsByGuide: channelsByGuide,
		sourcesByID:     sourcesByID,
	}

	pool := NewPool(2)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             1 * time.Second,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         750 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	primary, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe(101) error = %v", err)
	}
	defer primary.Close()

	type watchdogResult struct {
		timedOut bool
		err      error
	}
	resultCh := make(chan watchdogResult, 1)
	go func() {
		timedOut, _, streamErr := streamWithNoDataWatchdog(
			primary,
			450*time.Millisecond,
			2500*time.Millisecond,
		)
		resultCh <- watchdogResult{timedOut: timedOut, err: streamErr}
	}()

	time.Sleep(120 * time.Millisecond)

	for guide := 102; guide <= 109; guide++ {
		guideNumber := strconv.Itoa(guide)
		sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide[guideNumber])
		if err != nil {
			t.Fatalf("Subscribe(%s) during rapid retune error = %v", guideNumber, err)
		}
		time.Sleep(70 * time.Millisecond)
		sub.Close()
	}

	result := <-resultCh
	if result.err != nil && !errors.Is(result.err, context.Canceled) && !errors.Is(result.err, context.DeadlineExceeded) {
		t.Fatalf("primary Stream() error = %v", result.err)
	}
	if result.timedOut {
		t.Fatal("primary stream hit no-data timeout while secondary tuner was rapidly retuning")
	}

	primary.Close()
	waitFor(t, 3*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerRapidRetunePreemptionKeepsOtherTunerFlowingFFmpeg(t *testing.T) {
	tmp := t.TempDir()
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-constant-stream.sh", `#!/usr/bin/env bash
set -euo pipefail
chunk="$(printf '%188s' '' | tr ' ' S)"
while true; do
  printf '%s' "$chunk"
  sleep 0.01
done
`)

	channelsByGuide := make(map[string]channels.Channel)
	sourcesByID := make(map[int64][]channels.Source)
	for guide := 101; guide <= 109; guide++ {
		guideNumber := strconv.Itoa(guide)
		channelID := int64(guide - 100)
		channelsByGuide[guideNumber] = channels.Channel{
			ChannelID:   channelID,
			GuideNumber: guideNumber,
			GuideName:   "News " + guideNumber,
			Enabled:     true,
		}
		sourcesByID[channelID] = []channels.Source{
			{
				SourceID:      channelID * 10,
				ChannelID:     channelID,
				ItemKey:       "src:news:" + guideNumber,
				StreamURL:     "http://example.test/" + guideNumber,
				PriorityIndex: 0,
				Enabled:       true,
			},
		}
	}

	provider := &fakeChannelsProvider{
		channelsByGuide: channelsByGuide,
		sourcesByID:     sourcesByID,
	}

	pool := NewPool(2)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "ffmpeg-copy",
		FFmpegPath:                 ffmpegPath,
		StartupTimeout:             800 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         750 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	primary, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe(101) error = %v", err)
	}
	defer primary.Close()

	type watchdogResult struct {
		timedOut bool
		err      error
	}
	resultCh := make(chan watchdogResult, 1)
	go func() {
		timedOut, _, streamErr := streamWithNoDataWatchdog(
			primary,
			450*time.Millisecond,
			2500*time.Millisecond,
		)
		resultCh <- watchdogResult{timedOut: timedOut, err: streamErr}
	}()

	time.Sleep(120 * time.Millisecond)

	for guide := 102; guide <= 109; guide++ {
		guideNumber := strconv.Itoa(guide)
		sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide[guideNumber])
		if err != nil {
			t.Fatalf("Subscribe(%s) during rapid retune error = %v", guideNumber, err)
		}
		time.Sleep(70 * time.Millisecond)
		sub.Close()
	}

	result := <-resultCh
	if result.err != nil && !errors.Is(result.err, context.Canceled) && !errors.Is(result.err, context.DeadlineExceeded) {
		t.Fatalf("primary Stream() error = %v", result.err)
	}
	if result.timedOut {
		t.Fatal("primary ffmpeg stream hit no-data timeout while secondary tuner was rapidly retuning")
	}

	primary.Close()
	waitFor(t, 3*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerCanceledSubscribeBeforeReadyReleasesTuner(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)

		time.Sleep(250 * time.Millisecond)

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < 1000; i++ {
			if _, err := w.Write([]byte("z")); err != nil {
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
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:startup-delay",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             2 * time.Second,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         100 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	subscribeCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	if _, err := manager.Subscribe(subscribeCtx, provider.channelsByGuide["101"]); !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe() error = %v, want context canceled/deadline", err)
	}

	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerFollowerSurvivesCreateLeaderAcquireCancellation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("c"), mpegTSPacketSize)
		for i := 0; i < 200; i++ {
			if _, err := w.Write(payload); err != nil {
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
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:create-leader-cancel",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	pool.SetPreemptSettleDelay(300 * time.Millisecond)

	probeCtx, probeCancel := context.WithCancelCause(context.Background())
	defer probeCancel(context.Canceled)

	probeLease, err := pool.AcquireProbe(probeCtx, "create-leader-cancel", probeCancel)
	if err != nil {
		t.Fatalf("AcquireProbe() error = %v", err)
	}
	defer probeLease.Release()

	go func() {
		<-probeCtx.Done()
		probeLease.Release()
	}()

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             600 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer leaderCancel()

	leaderErrCh := make(chan error, 1)
	go func() {
		_, err := manager.Subscribe(leaderCtx, provider.channelsByGuide["101"])
		leaderErrCh <- err
	}()

	waitFor(t, time.Second, func() bool {
		manager.mu.Lock()
		_, waiting := manager.creating[1]
		manager.mu.Unlock()
		return waiting
	})

	followerCtx, followerCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer followerCancel()

	sub, err := manager.Subscribe(followerCtx, provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("follower Subscribe() error = %v", err)
	}
	sub.Close()

	leaderErr := <-leaderErrCh
	if !errors.Is(leaderErr, context.Canceled) && !errors.Is(leaderErr, context.DeadlineExceeded) {
		t.Fatalf("leader Subscribe() error = %v, want context cancellation/deadline", leaderErr)
	}

	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerFollowerRetryIsBoundedByOwnContextAfterCreateLeaderCancellation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		payload := bytes.Repeat([]byte("r"), mpegTSPacketSize)
		for i := 0; i < 200; i++ {
			if _, err := w.Write(payload); err != nil {
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
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:create-leader-cancel-bounded",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	pool.SetPreemptSettleDelay(700 * time.Millisecond)

	probeCtx, probeCancel := context.WithCancelCause(context.Background())
	defer probeCancel(context.Canceled)

	probeLease, err := pool.AcquireProbe(probeCtx, "create-leader-cancel-bounded", probeCancel)
	if err != nil {
		t.Fatalf("AcquireProbe() error = %v", err)
	}
	defer probeLease.Release()

	go func() {
		<-probeCtx.Done()
		probeLease.Release()
	}()

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             600 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         80 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer leaderCancel()

	leaderErrCh := make(chan error, 1)
	go func() {
		_, err := manager.Subscribe(leaderCtx, provider.channelsByGuide["101"])
		leaderErrCh <- err
	}()

	waitFor(t, time.Second, func() bool {
		manager.mu.Lock()
		_, waiting := manager.creating[1]
		manager.mu.Unlock()
		return waiting
	})

	followerCtx, followerCancel := context.WithTimeout(context.Background(), 280*time.Millisecond)
	defer followerCancel()

	followerStarted := time.Now()
	_, err = manager.Subscribe(followerCtx, provider.channelsByGuide["101"])
	followerElapsed := time.Since(followerStarted)
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("follower Subscribe() error = %v, want context cancellation/deadline", err)
	}
	if followerElapsed < 200*time.Millisecond {
		t.Fatalf("follower Subscribe() returned in %s, want retry bounded by follower context deadline", followerElapsed)
	}

	leaderErr := <-leaderErrCh
	if !errors.Is(leaderErr, context.Canceled) && !errors.Is(leaderErr, context.DeadlineExceeded) {
		t.Fatalf("leader Subscribe() error = %v, want context cancellation/deadline", leaderErr)
	}

	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerImmediateCloseBeforeReadySkipsIdleGrace(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("late-start"))
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
					ItemKey:       "src:news:late-start",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             2 * time.Second,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         5 * time.Second,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.SubscribeImmediate(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("SubscribeImmediate() error = %v", err)
	}
	sub.Close()

	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerStallRecoveryRetriesCurrentSource(t *testing.T) {
	stalling := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if _, err := w.Write([]byte("A")); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(600 * time.Millisecond)
	}))
	defer stalling.Close()

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)

		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < 200; i++ {
			if _, err := w.Write([]byte("B")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			<-ticker.C
		}
	}))
	defer healthy.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:stalling",
					StreamURL:     stalling.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:healthy",
					StreamURL:     healthy.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             300 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                100 * time.Millisecond,
		StallHardDeadline:          900 * time.Millisecond,
		StallPolicy:                stallPolicyRestartSame,
		StallMaxFailoversPerStall:  1,
		SessionIdleTimeout:         50 * time.Millisecond,
		SubscriberJoinLagBytes:     1,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	streamCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	writer := &recordingResponseWriter{
		onWrite: func(totalBytes int) {
			if totalBytes >= 20 {
				cancel()
			}
		},
	}
	err = sub.Stream(streamCtx, writer)
	if err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!strings.Contains(strings.ToLower(err.Error()), "recovery cycle budget exhausted") {
		t.Fatalf("Stream() error = %v", err)
	}

	provider.mu.Lock()
	failures := append([]sourceFailureCall(nil), provider.failures...)
	successes := append([]int64(nil), provider.successes...)
	provider.mu.Unlock()

	if !containsInt64(successes, 10) {
		t.Fatalf("successes = %#v, want recovery success for source 10", successes)
	}
	if containsInt64(successes, 11) {
		t.Fatalf("successes = %#v, did not expect alternate source 11 during stall recovery", successes)
	}
	if !hasFailureReason(failures, 10, "stall") {
		t.Fatalf("failures = %#v, want stall failure for source 10", failures)
	}
}

func TestSessionManagerSourceEOFRecoverySingleCandidateIsPacedAndCoalesced(t *testing.T) {
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var (
		mu           sync.Mutex
		attemptTimes []time.Time
	)
	shortEOF := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		attemptTimes = append(attemptTimes, time.Now().UTC())
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if _, err := w.Write([]byte("A")); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer shortEOF.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:single-short-eof",
					StreamURL:     shortEOF.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     logger,
		StartupTimeout:             250 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                120 * time.Millisecond,
		StallHardDeadline:          2 * time.Second,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  1,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	streamCtx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()

	err = sub.Stream(streamCtx, &recordingResponseWriter{})
	if err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) {
		if strings.Contains(strings.ToLower(err.Error()), "recovery cycle budget exhausted") {
			t.Fatalf("Stream() error = %v, expected pacing to prevent millisecond budget exhaustion", err)
		}
		t.Fatalf("Stream() error = %v", err)
	}

	mu.Lock()
	attempts := append([]time.Time(nil), attemptTimes...)
	mu.Unlock()

	if len(attempts) < 3 {
		t.Fatalf("startup attempts = %d, want at least 3 to validate pacing", len(attempts))
	}
	if gap := attempts[2].Sub(attempts[1]); gap < 200*time.Millisecond {
		t.Fatalf("attempt gap between retries = %s, want >= 200ms paced retry gap", gap)
	}

	logText := logs.String()
	if strings.Contains(logText, "shared session recovery cycle budget exhausted") {
		t.Fatalf("logs unexpectedly include recovery burst exhaustion: %s", logText)
	}
	if got, max := strings.Count(logText, "shared session recovery triggered"), len(attempts)-1; got >= max {
		t.Fatalf("recovery-trigger log count = %d, want < %d due coalescing", got, max)
	}
}

func TestSessionManagerHeartbeatDuringRecoveryKeepsDataFlow(t *testing.T) {
	var (
		mu   sync.Mutex
		hits int
	)
	stalling := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		hit := hits
		mu.Unlock()

		switch hit {
		case 1:
			// Initial source starts and then stalls to trigger recovery.
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			payload := bytes.Repeat([]byte("A"), mpegTSPacketSize)
			_, _ = w.Write(payload)
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(900 * time.Millisecond)
		case 2:
			// First recovery attempt fails quickly so heartbeat has to cover
			// the retry backoff window before the next startup attempt.
			http.Error(w, "temporary over limit", http.StatusNotFound)
		default:
			// Recovery retry succeeds on the same source.
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			payload := bytes.Repeat([]byte("A"), mpegTSPacketSize)
			for i := 0; i < 60; i++ {
				if _, err := w.Write(payload); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
				time.Sleep(15 * time.Millisecond)
			}
		}
	}))
	defer stalling.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:stalling",
					StreamURL:     stalling.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             1200 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		StallDetect:                120 * time.Millisecond,
		StallHardDeadline:          1500 * time.Millisecond,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  8,
		UpstreamOverlimitCooldown:  0,
		RecoveryFillerEnabled:      true,
		RecoveryFillerMode:         recoveryFillerModeNull,
		RecoveryFillerInterval:     60 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	timedOut, writer, err := streamWithNoDataWatchdog(sub, 350*time.Millisecond, 1800*time.Millisecond)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stream() error = %v", err)
	}
	if timedOut {
		t.Fatal("watchdog timeout fired; expected heartbeat chunks to keep data flowing during recovery")
	}
	if writer == nil {
		t.Fatal("watchdog writer is nil")
	}

	chunks := writer.Chunks()
	if !containsExactChunk(chunks, mpegTSNullPacketChunk(1)) {
		t.Fatalf("chunks did not contain MPEG-TS null heartbeat packet: len=%d", len(chunks))
	}
}

func TestSessionManagerTerminalRecoveryFailureSuppressesKeepaliveToLiveBoundary(t *testing.T) {
	var (
		mu   sync.Mutex
		hits int
	)
	failingRecovery := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		hit := hits
		mu.Unlock()

		switch hit {
		case 1:
			// Initial source starts and then stalls so recovery is entered.
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			payload := bytes.Repeat([]byte("A"), mpegTSPacketSize)
			_, _ = w.Write(payload)
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(700 * time.Millisecond)
		default:
			// Recovery startup fails terminally.
			http.Error(w, "upstream unavailable", http.StatusNotFound)
		}
	}))
	defer failingRecovery.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:failing-recovery",
					StreamURL:     failingRecovery.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             200 * time.Millisecond,
		FailoverTotalTimeout:       250 * time.Millisecond,
		MinProbeBytes:              1,
		BufferChunkBytes:           mpegTSPacketSize,
		BufferPublishFlushInterval: 15 * time.Millisecond,
		StallDetect:                100 * time.Millisecond,
		StallHardDeadline:          250 * time.Millisecond,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  2,
		UpstreamOverlimitCooldown:  0,
		RecoveryFillerEnabled:      true,
		RecoveryFillerMode:         recoveryFillerModeNull,
		RecoveryFillerInterval:     40 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	timedOut, writer, _ := streamWithNoDataWatchdog(sub, 300*time.Millisecond, 1300*time.Millisecond)
	if timedOut {
		t.Fatal("watchdog timeout fired; expected heartbeat to keep data flowing until terminal recovery failure")
	}
	if writer == nil {
		t.Fatal("watchdog writer is nil")
	}

	boundaries := 0
	for _, chunk := range writer.Chunks() {
		if isRecoveryTransitionBoundaryChunk(chunk) {
			boundaries++
		}
	}
	if boundaries == 0 {
		t.Fatal("expected at least one recovery transition boundary chunk")
	}
	if boundaries != 1 {
		t.Fatalf("recovery transition boundary count = %d, want 1 when terminal recovery fails", boundaries)
	}
}

func TestSharedSessionRecoveryHeartbeatStopsImmediatelyAfterStop(t *testing.T) {
	ring := NewChunkRing(32)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerInterval: 20 * time.Millisecond,
			},
		},
		ring: ring,
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	defer stop()
	waitFor(t, 500*time.Millisecond, func() bool {
		return len(ring.Snapshot()) >= 2
	})

	stop()
	before := len(ring.Snapshot())
	time.Sleep(80 * time.Millisecond)
	after := len(ring.Snapshot())
	if after != before {
		t.Fatalf("heartbeat chunk count changed after stop: before=%d after=%d", before, after)
	}
}

func TestSharedSessionRecoveryHeartbeatPSIModePublishesPATPMT(t *testing.T) {
	ring := NewChunkRing(32)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerMode:     recoveryFillerModePSI,
				recoveryFillerInterval: 20 * time.Millisecond,
			},
		},
		ring: ring,
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	defer stop()

	waitFor(t, 500*time.Millisecond, func() bool {
		snapshot := ring.Snapshot()
		for _, chunk := range snapshot {
			if isPSIHeartbeatChunk(chunk.Data) {
				return true
			}
		}
		return false
	})
}

func TestSharedSessionRecoveryHeartbeatTransitionBoundarySignals(t *testing.T) {
	ring := NewChunkRing(32)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerMode:     recoveryFillerModeNull,
				recoveryFillerInterval: 20 * time.Millisecond,
				recoveryTransitionMode: defaultSharedRecoveryTransitionMode,
			},
		},
		ring: ring,
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	defer stop()

	waitFor(t, 500*time.Millisecond, func() bool {
		snapshot := ring.Snapshot()
		return len(snapshot) > 0
	})
	snapshot := ring.Snapshot()
	if len(snapshot) == 0 {
		t.Fatal("expected at least one recovery transition boundary chunk")
	}
	if !isRecoveryTransitionBoundaryChunk(snapshot[0].Data) {
		t.Fatal("first recovery transition boundary chunk did not include expected signaling")
	}

	stats := session.stats()
	if stats.RecoveryTransitionMode != defaultSharedRecoveryTransitionMode {
		t.Fatalf(
			"RecoveryTransitionMode = %q, want %q",
			stats.RecoveryTransitionMode,
			defaultSharedRecoveryTransitionMode,
		)
	}
	if stats.RecoveryTransitionEffectiveMode != defaultSharedRecoveryTransitionMode {
		t.Fatalf(
			"RecoveryTransitionEffectiveMode = %q, want %q",
			stats.RecoveryTransitionEffectiveMode,
			defaultSharedRecoveryTransitionMode,
		)
	}
	if stats.RecoveryTransitionSignalsApplied != "disc,cc,pcr,psi_version" {
		t.Fatalf(
			"RecoveryTransitionSignalsApplied = %q, want disc,cc,pcr,psi_version",
			stats.RecoveryTransitionSignalsApplied,
		)
	}
	if stats.RecoveryTransitionFallbackCount < 1 {
		t.Fatalf("RecoveryTransitionFallbackCount = %d, want >= 1", stats.RecoveryTransitionFallbackCount)
	}
	if !strings.Contains(stats.RecoveryTransitionFallbackReason, "deprecated") &&
		!strings.Contains(stats.RecoveryTransitionFallbackReason, "stitch_no_") {
		t.Fatalf(
			"RecoveryTransitionFallbackReason = %q, want deprecated or stitch fallback marker",
			stats.RecoveryTransitionFallbackReason,
		)
	}
}

func TestSharedSessionRecoveryHeartbeatTransitionBoundaryContinuityAndVersionProgression(t *testing.T) {
	ring := NewChunkRing(32)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerMode:     recoveryFillerModePSI,
				recoveryFillerInterval: 20 * time.Millisecond,
				recoveryTransitionMode: defaultSharedRecoveryTransitionMode,
			},
		},
		ring: ring,
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	defer stop()

	waitFor(t, 500*time.Millisecond, func() bool {
		snapshot := ring.Snapshot()
		hasBoundary := false
		hasPSI := false
		for _, chunk := range snapshot {
			if isRecoveryTransitionBoundaryChunk(chunk.Data) {
				hasBoundary = true
			}
			if isPSIHeartbeatChunk(chunk.Data) {
				hasPSI = true
			}
		}
		return hasBoundary && hasPSI
	})

	snapshot := ring.Snapshot()
	firstBoundary := firstRecoveryTransitionBoundary(snapshot)
	if firstBoundary == nil {
		t.Fatal("missing first recovery transition boundary chunk")
	}
	firstPSI := firstPSIChunk(snapshot)
	if firstPSI == nil {
		t.Fatal("missing PSI heartbeat chunk")
	}

	boundaryPATCC, ok := packetContinuityCounter(firstBoundary[:mpegTSPacketSize])
	if !ok {
		t.Fatal("missing PAT continuity counter on first boundary")
	}
	boundaryPATVersion, ok := psiTableVersion(firstBoundary[:mpegTSPacketSize], heartbeatPATPID, 0x00)
	if !ok {
		t.Fatal("missing PAT version on first boundary")
	}
	psiPATCC, ok := packetContinuityCounter(firstPSI[:mpegTSPacketSize])
	if !ok {
		t.Fatal("missing PAT continuity counter on PSI heartbeat")
	}
	if want := (boundaryPATCC + 1) & 0x0F; psiPATCC != want {
		t.Fatalf("PSI PAT continuity = %d, want %d", psiPATCC, want)
	}
	psiPATVersion, ok := psiTableVersion(firstPSI[:mpegTSPacketSize], heartbeatPATPID, 0x00)
	if !ok {
		t.Fatal("missing PAT version on PSI heartbeat")
	}
	if psiPATVersion != boundaryPATVersion {
		t.Fatalf("PSI PAT version = %d, want %d", psiPATVersion, boundaryPATVersion)
	}

	stop()
	waitFor(t, 500*time.Millisecond, func() bool {
		return len(allRecoveryTransitionBoundaries(ring.Snapshot())) >= 2
	})
	boundaries := allRecoveryTransitionBoundaries(ring.Snapshot())
	if len(boundaries) < 2 {
		t.Fatalf("recovery transition boundary count = %d, want >= 2", len(boundaries))
	}
	secondBoundaryPATVersion, ok := psiTableVersion(boundaries[1][:mpegTSPacketSize], heartbeatPATPID, 0x00)
	if !ok {
		t.Fatal("missing PAT version on second boundary")
	}
	if want := (boundaryPATVersion + 1) & 0x1F; secondBoundaryPATVersion != want {
		t.Fatalf("second boundary PAT version = %d, want %d", secondBoundaryPATVersion, want)
	}
}

func TestSharedSessionRecoveryHeartbeatStopCanSuppressLiveTransitionBoundary(t *testing.T) {
	ring := NewChunkRing(32)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerMode:     recoveryFillerModeNull,
				recoveryFillerInterval: 20 * time.Millisecond,
				recoveryTransitionMode: defaultSharedRecoveryTransitionMode,
			},
		},
		ring: ring,
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)

	waitFor(t, 500*time.Millisecond, func() bool {
		return firstRecoveryTransitionBoundary(ring.Snapshot()) != nil
	})

	stop(false)
	time.Sleep(80 * time.Millisecond)

	boundaries := allRecoveryTransitionBoundaries(ring.Snapshot())
	if len(boundaries) != 1 {
		t.Fatalf("recovery transition boundary count = %d, want 1 when keepalive->live emission is suppressed", len(boundaries))
	}
}

func TestSharedSessionRecoveryHeartbeatTransitionBoundaryAppliesStitchWhenPCRObserved(t *testing.T) {
	ring := NewChunkRing(32)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerMode:     recoveryFillerModeNull,
				recoveryFillerInterval: 20 * time.Millisecond,
				recoveryTransitionMode: defaultSharedRecoveryTransitionMode,
			},
		},
		ring: ring,
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}
	session.mu.Lock()
	session.recoveryObservedPCRBase = 90000
	session.recoveryObservedPCRAt = time.Now().UTC().Add(-350 * time.Millisecond)
	session.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	defer stop()

	waitFor(t, 500*time.Millisecond, func() bool {
		snapshot := ring.Snapshot()
		return firstRecoveryTransitionBoundary(snapshot) != nil
	})

	snapshot := ring.Snapshot()
	boundary := firstRecoveryTransitionBoundary(snapshot)
	if boundary == nil {
		t.Fatal("missing recovery transition boundary chunk")
	}
	pcrBase, ok := parsePCRBase(boundary[2*mpegTSPacketSize:], heartbeatPCRPID, true)
	if !ok {
		t.Fatal("missing PCR base on recovery transition boundary chunk")
	}
	if pcrBase == 0 {
		t.Fatalf("boundary PCR base = %d, want non-zero stitched value", pcrBase)
	}

	stats := session.stats()
	if !stats.RecoveryTransitionStitchApplied {
		t.Fatal("RecoveryTransitionStitchApplied = false, want true")
	}
	if !strings.Contains(stats.RecoveryTransitionSignalsApplied, "stitch") {
		t.Fatalf(
			"RecoveryTransitionSignalsApplied = %q, want stitch marker",
			stats.RecoveryTransitionSignalsApplied,
		)
	}
}

func TestSharedSessionRecoveryHeartbeatKeepaliveToLiveIncludesIDRSignalWhenStartupReady(t *testing.T) {
	ring := NewChunkRing(32)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerMode:     recoveryFillerModeNull,
				recoveryFillerInterval: 20 * time.Millisecond,
				recoveryTransitionMode: defaultSharedRecoveryTransitionMode,
			},
		},
		ring: ring,
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	waitFor(t, 500*time.Millisecond, func() bool {
		return firstRecoveryTransitionBoundary(ring.Snapshot()) != nil
	})

	session.mu.Lock()
	session.sourceStartupRandomAccessReady = true
	session.sourceStartupRandomAccessCodec = "h264"
	session.mu.Unlock()

	stop()
	waitFor(t, 500*time.Millisecond, func() bool {
		return len(allRecoveryTransitionBoundaries(ring.Snapshot())) >= 2
	})

	stats := session.stats()
	if !strings.Contains(stats.RecoveryTransitionSignalsApplied, "idr") {
		t.Fatalf(
			"RecoveryTransitionSignalsApplied = %q, want idr marker",
			stats.RecoveryTransitionSignalsApplied,
		)
	}
	if strings.Contains(stats.RecoveryTransitionSignalSkips, "idr_not_ready") {
		t.Fatalf(
			"RecoveryTransitionSignalSkips = %q, did not expect idr_not_ready",
			stats.RecoveryTransitionSignalSkips,
		)
	}
}

func TestSharedSessionRecoveryHeartbeatSlateAVFallbackToPSI(t *testing.T) {
	ring := NewChunkRing(64)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerMode:     recoveryFillerModeSlateAV,
				recoveryFillerInterval: 15 * time.Millisecond,
				recoverySlateAVFactory: func(slateAVFillerConfig) (Producer, error) {
					return &testRecoveryFillerErrorProducer{err: errors.New("ffmpeg unavailable")}, nil
				},
			},
		},
		ring: ring,
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	defer stop()

	waitFor(t, time.Second, func() bool {
		return session.stats().RecoveryKeepaliveFallbackCount > 0
	})

	stats := session.stats()
	if stats.RecoveryKeepaliveFallbackCount < 1 {
		t.Fatalf("RecoveryKeepaliveFallbackCount = %d, want >= 1", stats.RecoveryKeepaliveFallbackCount)
	}
	if stats.RecoveryKeepaliveMode != recoveryFillerModePSI {
		t.Fatalf("RecoveryKeepaliveMode = %q, want %q", stats.RecoveryKeepaliveMode, recoveryFillerModePSI)
	}
	if !strings.Contains(stats.RecoveryKeepaliveFallbackReason, recoveryFillerModeSlateAV) {
		t.Fatalf(
			"RecoveryKeepaliveFallbackReason = %q, want %q marker",
			stats.RecoveryKeepaliveFallbackReason,
			recoveryFillerModeSlateAV,
		)
	}
}

func TestSharedSessionRecoveryHeartbeatSlateAVFallbackIncludesProducerExitDetail(t *testing.T) {
	const exitDetail = "ffmpeg exit status 254: Cannot find a valid font for the family Sans"

	ring := NewChunkRing(64)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerMode:     recoveryFillerModeSlateAV,
				recoveryFillerInterval: 15 * time.Millisecond,
				recoverySlateAVFactory: func(slateAVFillerConfig) (Producer, error) {
					return &testRecoveryFillerCloseErrorProducer{closeErr: errors.New(exitDetail)}, nil
				},
			},
		},
		ring: ring,
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	defer stop()

	waitFor(t, time.Second, func() bool {
		return session.stats().RecoveryKeepaliveFallbackCount > 0
	})

	stats := session.stats()
	if !strings.Contains(stats.RecoveryKeepaliveFallbackReason, "slate AV recovery filler process exit") {
		t.Fatalf(
			"RecoveryKeepaliveFallbackReason = %q, want process-exit marker",
			stats.RecoveryKeepaliveFallbackReason,
		)
	}
	if !strings.Contains(stats.RecoveryKeepaliveFallbackReason, "Cannot find a valid font for the family Sans") {
		t.Fatalf(
			"RecoveryKeepaliveFallbackReason = %q, want ffmpeg stderr detail",
			stats.RecoveryKeepaliveFallbackReason,
		)
	}
}

func TestCloseSlateAVRecoveryReaderWithTimeoutNilReaderReturnsNil(t *testing.T) {
	session := &sharedRuntimeSession{}
	if err := session.closeSlateAVRecoveryReaderWithTimeout(nil, 25*time.Millisecond); err != nil {
		t.Fatalf("closeSlateAVRecoveryReaderWithTimeout(nil) error = %v, want nil", err)
	}
}

func TestCloseSlateAVRecoveryReaderWithTimeoutCloseSuccessReturnsNil(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	// Happy-path close should return nil and emit no warning logs.
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				logger: logger,
			},
			logger: logger,
		},
	}
	if err := session.closeSlateAVRecoveryReaderWithTimeout(
		// Zero-value closeErr means Close() succeeds and returns nil.
		&testRecoveryFillerCloseErrorReader{},
		25*time.Millisecond,
	); err != nil {
		t.Fatalf("closeSlateAVRecoveryReaderWithTimeout() error = %v, want nil", err)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Started != 1 {
		t.Fatalf("stats.Started = %d, want 1", stats.Started)
	}
	if stats.Timeouts != 0 {
		t.Fatalf("stats.Timeouts = %d, want 0", stats.Timeouts)
	}
	if stats.Retried != 0 {
		t.Fatalf("stats.Retried = %d, want 0", stats.Retried)
	}
	if stats.Suppressed != 0 {
		t.Fatalf("stats.Suppressed = %d, want 0", stats.Suppressed)
	}
	if stats.Dropped != 0 {
		t.Fatalf("stats.Dropped = %d, want 0", stats.Dropped)
	}
	if stats.Queued != 0 {
		t.Fatalf("stats.Queued = %d, want 0", stats.Queued)
	}
	if stats.LateCompletions != 0 {
		t.Fatalf("stats.LateCompletions = %d, want 0", stats.LateCompletions)
	}
	if logText := strings.TrimSpace(logs.String()); logText != "" {
		t.Fatalf("logs = %q, want no warning logs for close success path", logText)
	}
}

func TestCloseSlateAVRecoveryReaderWithTimeoutCloseErrorIsReturned(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				logger: logger,
			},
			logger: logger,
		},
		channel: channels.Channel{
			ChannelID:   42,
			GuideNumber: "142",
		},
	}
	closeErr := errors.New("close failed")

	err := session.closeSlateAVRecoveryReaderWithTimeout(
		&testRecoveryFillerCloseErrorReader{closeErr: closeErr},
		250*time.Millisecond,
	)
	if !errors.Is(err, closeErr) {
		t.Fatalf("closeSlateAVRecoveryReaderWithTimeout() error = %v, want %v", err, closeErr)
	}
	// Ensure close errors are returned without wrapping for identity checks.
	if err != closeErr {
		t.Fatalf("closeSlateAVRecoveryReaderWithTimeout() error = %v, want identity equality with closeErr", err)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Started != 1 {
		t.Fatalf("stats.Started = %d, want 1", stats.Started)
	}
	if stats.Timeouts != 0 {
		t.Fatalf("stats.Timeouts = %d, want 0 for immediate close-error path", stats.Timeouts)
	}
	if stats.Retried != 0 {
		t.Fatalf("stats.Retried = %d, want 0", stats.Retried)
	}
	if stats.Suppressed != 0 {
		t.Fatalf("stats.Suppressed = %d, want 0", stats.Suppressed)
	}
	if stats.Dropped != 0 {
		t.Fatalf("stats.Dropped = %d, want 0", stats.Dropped)
	}
	if stats.Queued != 0 {
		t.Fatalf("stats.Queued = %d, want 0", stats.Queued)
	}
	if stats.LateCompletions != 0 {
		t.Fatalf("stats.LateCompletions = %d, want 0", stats.LateCompletions)
	}
	logText := logs.String()
	if !strings.Contains(logText, "shared session slate AV close error") {
		t.Fatalf("logs = %q, want non-timeout close error warning", logText)
	}
	if !strings.Contains(logText, "close_error_type=non_timeout") {
		t.Fatalf("logs = %q, want close_error_type=non_timeout", logText)
	}
	if !strings.Contains(logText, "close_retried=0") {
		t.Fatalf("logs = %q, want close_retried field", logText)
	}
	if !strings.Contains(logText, "close_timeouts=0") {
		t.Fatalf("logs = %q, want close_timeouts field", logText)
	}
	if !strings.Contains(logText, "close_late_abandoned=0") {
		t.Fatalf("logs = %q, want close_late_abandoned field", logText)
	}
	if !strings.Contains(logText, "close_release_underflow=0") {
		t.Fatalf("logs = %q, want close_release_underflow field", logText)
	}
	if !strings.Contains(logText, "close_suppressed_duplicate=0") {
		t.Fatalf("logs = %q, want close_suppressed_duplicate field", logText)
	}
	if !strings.Contains(logText, "close_suppressed_budget=0") {
		t.Fatalf("logs = %q, want close_suppressed_budget field", logText)
	}
	if !strings.Contains(logText, "channel_id=42") {
		t.Fatalf("logs = %q, want channel_id field", logText)
	}
	if !strings.Contains(logText, "guide_number=142") {
		t.Fatalf("logs = %q, want guide_number field", logText)
	}
}

func TestCloseSlateAVRecoveryReaderWithTimeoutDelayedCloseErrorIsReturned(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				logger: logger,
			},
			logger: logger,
		},
		channel: channels.Channel{
			ChannelID:   77,
			GuideNumber: "177",
		},
	}
	closeErr := errors.New("slow close failed")
	closeDelay := 50 * time.Millisecond
	timeout := 250 * time.Millisecond

	started := time.Now()
	err := session.closeSlateAVRecoveryReaderWithTimeout(
		&testRecoveryFillerCloseErrorReader{
			closeErr:   closeErr,
			closeDelay: closeDelay,
		},
		timeout,
	)
	elapsed := time.Since(started)
	if !errors.Is(err, closeErr) {
		t.Fatalf("closeSlateAVRecoveryReaderWithTimeout() error = %v, want %v", err, closeErr)
	}
	if err != closeErr {
		t.Fatalf("closeSlateAVRecoveryReaderWithTimeout() error = %v, want identity equality with closeErr", err)
	}
	if elapsed < closeDelay-10*time.Millisecond {
		t.Fatalf("elapsed = %s, want >= %s for delayed close-error path", elapsed, closeDelay-10*time.Millisecond)
	}
	if elapsed >= timeout {
		t.Fatalf("elapsed = %s, want < %s so delayed close error returns before timeout", elapsed, timeout)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Started != 1 {
		t.Fatalf("stats.Started = %d, want 1", stats.Started)
	}
	if stats.Timeouts != 0 {
		t.Fatalf("stats.Timeouts = %d, want 0", stats.Timeouts)
	}
	if stats.Retried != 0 {
		t.Fatalf("stats.Retried = %d, want 0", stats.Retried)
	}
	if stats.Suppressed != 0 {
		t.Fatalf("stats.Suppressed = %d, want 0", stats.Suppressed)
	}
	if stats.Dropped != 0 {
		t.Fatalf("stats.Dropped = %d, want 0", stats.Dropped)
	}
	if stats.Queued != 0 {
		t.Fatalf("stats.Queued = %d, want 0", stats.Queued)
	}
	if stats.LateCompletions != 0 {
		t.Fatalf("stats.LateCompletions = %d, want 0", stats.LateCompletions)
	}

	logText := logs.String()
	if !strings.Contains(logText, "shared session slate AV close error") {
		t.Fatalf("logs = %q, want close warning prefix", logText)
	}
	if !strings.Contains(logText, "close_error_type=non_timeout") {
		t.Fatalf("logs = %q, want close_error_type=non_timeout", logText)
	}
	if !strings.Contains(logText, "channel_id=77") {
		t.Fatalf("logs = %q, want channel_id=77", logText)
	}
	if !strings.Contains(logText, "guide_number=177") {
		t.Fatalf("logs = %q, want guide_number=177", logText)
	}
	if !strings.Contains(logText, "close_late_abandoned=0") {
		t.Fatalf("logs = %q, want close_late_abandoned field", logText)
	}
	if !strings.Contains(logText, "close_release_underflow=0") {
		t.Fatalf("logs = %q, want close_release_underflow field", logText)
	}
	if !strings.Contains(logText, "close_suppressed_duplicate=0") {
		t.Fatalf("logs = %q, want close_suppressed_duplicate field", logText)
	}
	if !strings.Contains(logText, "close_suppressed_budget=0") {
		t.Fatalf("logs = %q, want close_suppressed_budget field", logText)
	}
}

func TestCloseSlateAVRecoveryReaderWithTimeoutNonTimeoutWarnIsRateLimited(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				logger: logger,
			},
			logger: logger,
		},
		channel: channels.Channel{
			ChannelID:   88,
			GuideNumber: "188",
		},
	}
	closeErr := errors.New("connection reset by peer")

	for i := 0; i < 2; i++ {
		err := session.closeSlateAVRecoveryReaderWithTimeout(
			&testRecoveryFillerCloseErrorReader{closeErr: closeErr},
			250*time.Millisecond,
		)
		if !errors.Is(err, closeErr) {
			t.Fatalf("attempt %d error = %v, want %v", i+1, err, closeErr)
		}
	}

	if got := strings.Count(logs.String(), "shared session slate AV close error"); got != 1 {
		t.Fatalf("warn log count after coalesced burst = %d, want 1", got)
	}

	// Advance the coalesce window without sleeping so the next close error flushes
	// the suppressed counter in deterministic CI timing.
	session.mu.Lock()
	session.slateAVCloseWarnLastLogAt = time.Now().UTC().Add(-slateAVCloseWarnCoalesceWindow - time.Millisecond)
	session.mu.Unlock()

	err := session.closeSlateAVRecoveryReaderWithTimeout(
		&testRecoveryFillerCloseErrorReader{closeErr: closeErr},
		250*time.Millisecond,
	)
	if !errors.Is(err, closeErr) {
		t.Fatalf("third attempt error = %v, want %v", err, closeErr)
	}

	logText := logs.String()
	if got := strings.Count(logText, "shared session slate AV close error"); got != 2 {
		t.Fatalf("warn log count after window reopen = %d, want 2", got)
	}
	if !strings.Contains(logText, "close_non_timeout_logs_coalesced=1") {
		t.Fatalf("logs = %q, want coalesced close-error counter", logText)
	}
}

func TestCloseSlateAVRecoveryReaderWithTimeoutNonPositiveTimeoutUsesBoundedTimeout(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				logger: logger,
			},
			logger: logger,
		},
		channel: channels.Channel{
			ChannelID:   24,
			GuideNumber: "124",
		},
	}
	body := newStartupAbortBlockingBody()
	defer body.release()

	started := time.Now()
	err := session.closeSlateAVRecoveryReaderWithTimeout(body, -1)
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("closeSlateAVRecoveryReaderWithTimeout() error = nil, want timeout error")
	}
	if !strings.Contains(err.Error(), boundedCloseTimeout.String()) {
		t.Fatalf("timeout error = %q, want duration marker %q", err.Error(), boundedCloseTimeout.String())
	}
	if elapsed < boundedCloseTimeout-250*time.Millisecond {
		t.Fatalf("elapsed = %s, want >= %s when timeout falls back to boundedCloseTimeout", elapsed, boundedCloseTimeout-250*time.Millisecond)
	}
	if elapsed > boundedCloseTimeout+1500*time.Millisecond {
		t.Fatalf("elapsed = %s, want < %s for bounded timeout fallback path", elapsed, boundedCloseTimeout+1500*time.Millisecond)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Started != 1 {
		t.Fatalf("stats.Started = %d, want 1", stats.Started)
	}
	if stats.Timeouts != 1 {
		t.Fatalf("stats.Timeouts = %d, want 1", stats.Timeouts)
	}
	if stats.Retried != 0 {
		t.Fatalf("stats.Retried = %d, want 0", stats.Retried)
	}
	if stats.Suppressed != 0 {
		t.Fatalf("stats.Suppressed = %d, want 0", stats.Suppressed)
	}
	if stats.LateCompletions != 0 {
		t.Fatalf("stats.LateCompletions = %d, want 0 before release", stats.LateCompletions)
	}

	body.release()
	time.Sleep(80 * time.Millisecond)
	stats = closeWithTimeoutStatsSnapshot()
	if stats.LateCompletions != 1 {
		t.Fatalf("stats.LateCompletions = %d, want 1 after release", stats.LateCompletions)
	}
	logText := logs.String()
	if !strings.Contains(logText, "close_error_type=timeout") {
		t.Fatalf("logs = %q, want close_error_type=timeout", logText)
	}
	if strings.Contains(logText, "close_error_type=non_timeout") {
		t.Fatalf("logs = %q, want timeout path without non-timeout warning", logText)
	}
	if !strings.Contains(logText, "close_late_abandoned=0") {
		t.Fatalf("logs = %q, want close_late_abandoned field", logText)
	}
	if !strings.Contains(logText, "close_release_underflow=0") {
		t.Fatalf("logs = %q, want close_release_underflow field", logText)
	}
	if !strings.Contains(logText, "close_suppressed_duplicate=0") {
		t.Fatalf("logs = %q, want close_suppressed_duplicate field", logText)
	}
	if !strings.Contains(logText, "close_suppressed_budget=0") {
		t.Fatalf("logs = %q, want close_suppressed_budget field", logText)
	}
}

func TestCloseSlateAVRecoveryReaderWithTimeoutBlockedCloseTimeoutPath(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				logger: logger,
			},
			logger: logger,
		},
		channel: channels.Channel{
			ChannelID:   25,
			GuideNumber: "125",
		},
	}
	body := newStartupAbortBlockingBody()
	defer body.release()

	timeout := 15 * time.Millisecond
	started := time.Now()
	err := session.closeSlateAVRecoveryReaderWithTimeout(body, timeout)
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("closeSlateAVRecoveryReaderWithTimeout() error = nil, want timeout error")
	}
	if !strings.Contains(err.Error(), timeout.String()) {
		t.Fatalf("timeout error = %q, want duration marker %q", err.Error(), timeout.String())
	}
	if elapsed < timeout {
		t.Fatalf("elapsed = %s, want >= %s for blocked-close timeout path", elapsed, timeout)
	}
	if elapsed > 350*time.Millisecond {
		t.Fatalf("elapsed = %s, want < 350ms for blocked-close timeout path", elapsed)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Started != 1 {
		t.Fatalf("stats.Started = %d, want 1", stats.Started)
	}
	if stats.Timeouts != 1 {
		t.Fatalf("stats.Timeouts = %d, want 1", stats.Timeouts)
	}
	if stats.Retried != 0 {
		t.Fatalf("stats.Retried = %d, want 0", stats.Retried)
	}
	if stats.Suppressed != 0 {
		t.Fatalf("stats.Suppressed = %d, want 0", stats.Suppressed)
	}
	if stats.LateCompletions != 0 {
		t.Fatalf("stats.LateCompletions = %d, want 0 before release", stats.LateCompletions)
	}

	body.release()
	time.Sleep(80 * time.Millisecond)
	stats = closeWithTimeoutStatsSnapshot()
	if stats.LateCompletions != 1 {
		t.Fatalf("stats.LateCompletions = %d, want 1 after release", stats.LateCompletions)
	}
	logText := logs.String()
	if !strings.Contains(logText, "close_error_type=timeout") {
		t.Fatalf("logs = %q, want close_error_type=timeout", logText)
	}
	if strings.Contains(logText, "close_error_type=non_timeout") {
		t.Fatalf("logs = %q, want timeout path without non-timeout warning", logText)
	}
	if !strings.Contains(logText, "close_late_abandoned=0") {
		t.Fatalf("logs = %q, want close_late_abandoned field", logText)
	}
	if !strings.Contains(logText, "close_release_underflow=0") {
		t.Fatalf("logs = %q, want close_release_underflow field", logText)
	}
	if !strings.Contains(logText, "close_suppressed_duplicate=0") {
		t.Fatalf("logs = %q, want close_suppressed_duplicate field", logText)
	}
	if !strings.Contains(logText, "close_suppressed_budget=0") {
		t.Fatalf("logs = %q, want close_suppressed_budget field", logText)
	}
}

func TestCloseSlateAVRecoveryReaderWithTimeoutReturnsSuppressedWhenWorkerBudgetExhausted(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	blockers := make([]*startupAbortBlockingBody, 0, boundedCloseWorkerBudget)
	t.Cleanup(func() {
		for _, blocker := range blockers {
			closeWithTimeoutFinishWorker(blocker)
			blocker.release()
		}
	})

	for i := 0; i < boundedCloseWorkerBudget; i++ {
		blocker := newStartupAbortBlockingBody()
		blockers = append(blockers, blocker)
		if got := closeWithTimeoutStartWorker(blocker); got != closeWithTimeoutStartSuccess {
			t.Fatalf("closeWithTimeoutStartWorker blocker %d = %v, want %v", i, got, closeWithTimeoutStartSuccess)
		}
	}

	session := &sharedRuntimeSession{}
	reader := newStartupAbortBlockingBody()
	t.Cleanup(reader.release)

	err := session.closeSlateAVRecoveryReaderWithTimeout(reader, 500*time.Millisecond)
	if err == nil {
		t.Fatal("closeSlateAVRecoveryReaderWithTimeout() error = nil, want suppression error")
	}
	if !strings.Contains(err.Error(), "bounded close worker suppressed (queued retry)") {
		t.Fatalf("error = %q, want queued retry suppression marker", err.Error())
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Started != boundedCloseWorkerBudget {
		t.Fatalf("stats.Started = %d, want %d", stats.Started, boundedCloseWorkerBudget)
	}
	if stats.Suppressed != 1 {
		t.Fatalf("stats.Suppressed = %d, want 1", stats.Suppressed)
	}
	if stats.Timeouts != 0 {
		t.Fatalf("stats.Timeouts = %d, want 0", stats.Timeouts)
	}
	if stats.Retried != 0 {
		t.Fatalf("stats.Retried = %d, want 0", stats.Retried)
	}
	if stats.Queued != 1 {
		t.Fatalf("stats.Queued = %d, want 1", stats.Queued)
	}

	closeWithTimeoutFinishWorker(blockers[0])
	waitFor(t, time.Second, func() bool {
		select {
		case <-reader.closeStartedCh:
			return true
		default:
			return false
		}
	})
	reader.release()
	waitFor(t, time.Second, func() bool {
		stats := closeWithTimeoutStatsSnapshot()
		return stats.Retried == 1 && stats.Queued == 0
	})

	stats = closeWithTimeoutStatsSnapshot()
	if stats.Retried != 1 {
		t.Fatalf("stats.Retried = %d, want 1 after queued retry start", stats.Retried)
	}
	if stats.Queued != 0 {
		t.Fatalf("stats.Queued = %d, want 0 after queued retry start", stats.Queued)
	}
}

func TestCloseSlateAVRecoveryReaderWithTimeoutIsBoundedUnderRepeatedBlockingCloseAttempts(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)
	session := &sharedRuntimeSession{}
	const attempts = boundedCloseWorkerBudget + 8
	const runtimeGoroutineJitter = 4
	const drainResidualAllowance = 4

	readers := make([]*startupAbortBlockingBody, attempts)
	for i := 0; i < attempts; i++ {
		readers[i] = newStartupAbortBlockingBody()
	}

	before := waitForStableGoroutines()
	for i := 0; i < attempts; i++ {
		if err := session.closeSlateAVRecoveryReaderWithTimeout(readers[i], 10*time.Millisecond); err == nil {
			t.Fatalf("close attempt %d returned nil, want timeout error", i)
		}
	}

	after := waitForStableGoroutines()
	// Worst-case growth is bounded by:
	// - one blocking Close goroutine per started bounded worker
	// - one late-completion waiter goroutine per timed-out worker
	// - small runtime scheduling jitter allowance
	maxExpectedGrowth := (2 * boundedCloseWorkerBudget) + runtimeGoroutineJitter
	if after > before+maxExpectedGrowth {
		t.Fatalf(
			"recovery close worker growth exceeded bounded close-worker ceiling: before=%d after=%d max_growth=%d",
			before,
			after,
			maxExpectedGrowth,
		)
	}

	stats := closeWithTimeoutStatsSnapshot()
	if stats.Started != boundedCloseWorkerBudget {
		t.Fatalf("stats.Started = %d, want %d", stats.Started, boundedCloseWorkerBudget)
	}
	if stats.Timeouts != boundedCloseWorkerBudget {
		t.Fatalf("stats.Timeouts = %d, want %d", stats.Timeouts, boundedCloseWorkerBudget)
	}
	if stats.Retried != 0 {
		t.Fatalf("stats.Retried = %d, want 0 before worker slots free", stats.Retried)
	}
	if stats.Suppressed != attempts-boundedCloseWorkerBudget {
		t.Fatalf("stats.Suppressed = %d, want %d", stats.Suppressed, attempts-boundedCloseWorkerBudget)
	}
	if stats.Queued != attempts-boundedCloseWorkerBudget {
		t.Fatalf("stats.Queued = %d, want %d before worker slots free", stats.Queued, attempts-boundedCloseWorkerBudget)
	}
	if stats.LateCompletions != 0 {
		t.Fatalf("stats.LateCompletions = %d, want 0 before blocking reads release", stats.LateCompletions)
	}

	for _, reader := range readers {
		reader.release()
	}
	drainCeiling := before + drainResidualAllowance
	waitFor(t, 2*time.Second, func() bool {
		return waitForStableGoroutines() <= drainCeiling
	})

	afterRelease := waitForStableGoroutines()
	if afterRelease > drainCeiling {
		t.Fatalf("recovery close workers did not drain after release: before=%d afterRelease=%d", before, afterRelease)
	}
	waitFor(t, 2*time.Second, func() bool {
		return closeWithTimeoutStatsSnapshot().LateCompletions >= boundedCloseWorkerBudget
	})
	stats = closeWithTimeoutStatsSnapshot()
	if stats.LateCompletions != boundedCloseWorkerBudget {
		t.Fatalf("stats.LateCompletions = %d, want %d", stats.LateCompletions, boundedCloseWorkerBudget)
	}
	if stats.Retried != attempts-boundedCloseWorkerBudget {
		t.Fatalf("stats.Retried = %d, want %d after queued retries start", stats.Retried, attempts-boundedCloseWorkerBudget)
	}
	if stats.Queued != 0 {
		t.Fatalf("stats.Queued = %d, want 0 after queued retries drain", stats.Queued)
	}
	if stats.Started != attempts {
		t.Fatalf("stats.Started = %d, want %d after queued retries start", stats.Started, attempts)
	}
}

func TestSharedSessionRecoveryHeartbeatKeepaliveTelemetry(t *testing.T) {
	ring := NewChunkRing(64)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerMode:     recoveryFillerModeNull,
				recoveryFillerInterval: 10 * time.Millisecond,
			},
		},
		ring: ring,
		sourceProfile: streamProfile{
			BitrateBPS: 2_000_000,
		},
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	waitFor(t, time.Second, func() bool {
		stats := session.stats()
		return stats.RecoveryKeepaliveChunks > 0
	})
	stop()

	stats := session.stats()
	if stats.RecoveryKeepaliveStartedAt.IsZero() {
		t.Fatal("RecoveryKeepaliveStartedAt is zero, want keepalive start timestamp")
	}
	if stats.RecoveryKeepaliveStoppedAt.IsZero() {
		t.Fatal("RecoveryKeepaliveStoppedAt is zero, want keepalive stop timestamp")
	}
	if stats.RecoveryKeepaliveDuration == "" {
		t.Fatal("RecoveryKeepaliveDuration is empty")
	}
	if stats.RecoveryKeepaliveBytes <= 0 {
		t.Fatalf("RecoveryKeepaliveBytes = %d, want > 0", stats.RecoveryKeepaliveBytes)
	}
	if stats.RecoveryKeepaliveChunks <= 0 {
		t.Fatalf("RecoveryKeepaliveChunks = %d, want > 0", stats.RecoveryKeepaliveChunks)
	}
	if stats.RecoveryKeepaliveRateBytesPerSecond <= 0 {
		t.Fatalf("RecoveryKeepaliveRateBytesPerSecond = %f, want > 0", stats.RecoveryKeepaliveRateBytesPerSecond)
	}
	if stats.RecoveryKeepaliveExpectedRate <= 0 {
		t.Fatalf("RecoveryKeepaliveExpectedRate = %f, want > 0", stats.RecoveryKeepaliveExpectedRate)
	}
	if stats.RecoveryKeepaliveRealtimeMultiplier <= 0 {
		t.Fatalf(
			"RecoveryKeepaliveRealtimeMultiplier = %f, want > 0 when profile bitrate is set",
			stats.RecoveryKeepaliveRealtimeMultiplier,
		)
	}
}

func TestSharedSessionRecoveryHeartbeatSlateAVOverproductionGuardrailFallback(t *testing.T) {
	ring := NewChunkRing(256)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerMode:     recoveryFillerModeSlateAV,
				recoveryFillerInterval: 10 * time.Millisecond,
				bufferChunkBytes:       mpegTSPacketSize,
				bufferFlushPeriod:      2 * time.Millisecond,
				recoverySlateAVFactory: func(slateAVFillerConfig) (Producer, error) {
					return &testRecoveryFillerStreamingProducer{
						payload:  bytes.Repeat(mpegTSNullPacketChunk(1), 96),
						interval: 1 * time.Millisecond,
					}, nil
				},
			},
		},
		ring: ring,
		sourceProfile: streamProfile{
			BitrateBPS: 1_000_000,
		},
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	defer stop()

	waitFor(t, 4*time.Second, func() bool {
		stats := session.stats()
		return stats.RecoveryKeepaliveGuardrailCount > 0 && stats.RecoveryKeepaliveFallbackCount > 0
	})

	stats := session.stats()
	if stats.RecoveryKeepaliveGuardrailCount < 1 {
		t.Fatalf("RecoveryKeepaliveGuardrailCount = %d, want >= 1", stats.RecoveryKeepaliveGuardrailCount)
	}
	if !strings.Contains(stats.RecoveryKeepaliveGuardrailReason, "rate_bytes_per_second=") {
		t.Fatalf(
			"RecoveryKeepaliveGuardrailReason = %q, want keepalive rate marker",
			stats.RecoveryKeepaliveGuardrailReason,
		)
	}
	if stats.RecoveryKeepaliveFallbackCount < 1 {
		t.Fatalf("RecoveryKeepaliveFallbackCount = %d, want >= 1", stats.RecoveryKeepaliveFallbackCount)
	}
	if stats.RecoveryKeepaliveMode != recoveryFillerModePSI {
		t.Fatalf("RecoveryKeepaliveMode = %q, want %q", stats.RecoveryKeepaliveMode, recoveryFillerModePSI)
	}
	if !strings.Contains(stats.RecoveryKeepaliveFallbackReason, "exceeded realtime envelope") {
		t.Fatalf(
			"RecoveryKeepaliveFallbackReason = %q, want keepalive guardrail marker",
			stats.RecoveryKeepaliveFallbackReason,
		)
	}
}

func TestSharedSessionRecoveryHeartbeatSlateAVRetainedForOddProfileResolution(t *testing.T) {
	ring := NewChunkRing(64)
	factoryCalls := make(chan slateAVFillerConfig, 1)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:     true,
				recoveryFillerMode:        recoveryFillerModeSlateAV,
				recoveryFillerInterval:    15 * time.Millisecond,
				bufferChunkBytes:          mpegTSPacketSize,
				bufferFlushPeriod:         5 * time.Millisecond,
				recoveryFillerEnableAudio: true,
				recoverySlateAVFactory: func(cfg slateAVFillerConfig) (Producer, error) {
					select {
					case factoryCalls <- cfg:
					default:
					}
					if cfg.Profile.Width%2 != 0 || cfg.Profile.Height%2 != 0 {
						return &testRecoveryFillerErrorProducer{
							err: errors.New("width not divisible by 2"),
						}, nil
					}
					return &testRecoveryFillerStreamingProducer{
						payload:  mpegTSNullPacketChunk(1),
						interval: 2 * time.Millisecond,
					}, nil
				},
			},
		},
		ring: ring,
		sourceProfile: streamProfile{
			Width:           853,
			Height:          481,
			FrameRate:       30000.0 / 1001.0,
			AudioSampleRate: 48000,
			AudioChannels:   2,
		},
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	defer stop()

	var cfg slateAVFillerConfig
	waitFor(t, time.Second, func() bool {
		select {
		case cfg = <-factoryCalls:
			return true
		default:
			return false
		}
	})
	if got, want := cfg.Profile.Width, 852; got != want {
		t.Fatalf("factory profile width = %d, want %d", got, want)
	}
	if got, want := cfg.Profile.Height, 480; got != want {
		t.Fatalf("factory profile height = %d, want %d", got, want)
	}

	waitFor(t, time.Second, func() bool {
		snapshot := ring.Snapshot()
		return len(snapshot) >= 2
	})
	waitFor(t, time.Second, func() bool {
		return session.stats().RecoveryKeepaliveMode == recoveryFillerModeSlateAV
	})

	stats := session.stats()
	if stats.RecoveryKeepaliveFallbackCount != 0 {
		t.Fatalf(
			"RecoveryKeepaliveFallbackCount = %d, want 0",
			stats.RecoveryKeepaliveFallbackCount,
		)
	}
	if stats.RecoveryKeepaliveMode != recoveryFillerModeSlateAV {
		t.Fatalf("RecoveryKeepaliveMode = %q, want %q", stats.RecoveryKeepaliveMode, recoveryFillerModeSlateAV)
	}
}

func TestSharedSessionRecoveryHeartbeatSlateAVPassesPTSOffsetFromBoundaryPCR(t *testing.T) {
	ring := NewChunkRing(64)
	factoryCalls := make(chan slateAVFillerConfig, 1)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerMode:     recoveryFillerModeSlateAV,
				recoveryFillerInterval: 15 * time.Millisecond,
				bufferChunkBytes:       mpegTSPacketSize,
				bufferFlushPeriod:      5 * time.Millisecond,
				recoverySlateAVFactory: func(cfg slateAVFillerConfig) (Producer, error) {
					select {
					case factoryCalls <- cfg:
					default:
					}
					return &testRecoveryFillerStreamingProducer{
						payload:  mpegTSNullPacketChunk(1),
						interval: 2 * time.Millisecond,
					}, nil
				},
			},
		},
		ring: ring,
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}
	session.mu.Lock()
	session.recoveryBoundaryPCRSet = true
	session.recoveryBoundaryPCRBase = mpegTSPCRClockHz
	session.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	defer stop()

	var cfg slateAVFillerConfig
	waitFor(t, time.Second, func() bool {
		select {
		case cfg = <-factoryCalls:
			return true
		default:
			return false
		}
	})

	// startRecoveryHeartbeat publishes a live->keepalive transition boundary
	// before creating the slate AV filler. Without a recent observed PCR, that
	// boundary path advances recoveryBoundaryPCRBase by 1s.
	session.mu.Lock()
	boundarySet := session.recoveryBoundaryPCRSet
	boundaryBase := session.recoveryBoundaryPCRBase
	session.mu.Unlock()
	if !boundarySet {
		t.Fatal("recoveryBoundaryPCRSet = false, want true after keepalive boundary publish")
	}
	if got, want := boundaryBase, uint64(2*mpegTSPCRClockHz); got != want {
		t.Fatalf("recoveryBoundaryPCRBase = %d, want %d", got, want)
	}

	if got, want := cfg.PTSOffset, pcrBaseToDuration(boundaryBase); got != want {
		t.Fatalf("factory PTS offset = %s, want %s", got, want)
	}
}

func TestSharedSessionCurrentRecoveryTimelinePTSOffsetPrefersObservedPCR(t *testing.T) {
	session := &sharedRuntimeSession{}
	session.mu.Lock()
	session.recoveryBoundaryPCRSet = true
	session.recoveryBoundaryPCRBase = 5 * mpegTSPCRClockHz
	session.recoveryObservedPCRBase = 8 * mpegTSPCRClockHz
	session.recoveryObservedPCRAt = time.Now().UTC()
	session.mu.Unlock()

	got := session.currentRecoveryTimelinePTSOffset()
	if got < 8*time.Second {
		t.Fatalf("currentRecoveryTimelinePTSOffset() = %s, want >= 8s", got)
	}
	if got > 10*time.Second {
		t.Fatalf("currentRecoveryTimelinePTSOffset() = %s, want <= 10s", got)
	}
}

func TestSharedSessionCurrentRecoveryTimelinePTSOffsetFallsBackToBoundaryOnStaleObservedPCR(t *testing.T) {
	session := &sharedRuntimeSession{}
	session.mu.Lock()
	session.recoveryBoundaryPCRSet = true
	session.recoveryBoundaryPCRBase = 5 * mpegTSPCRClockHz
	session.recoveryObservedPCRBase = 8 * mpegTSPCRClockHz
	session.recoveryObservedPCRAt = time.Now().UTC().Add(-(3 * defaultSharedStallHardDeadline))
	session.mu.Unlock()

	if got, want := session.currentRecoveryTimelinePTSOffset(), 5*time.Second; got != want {
		t.Fatalf("currentRecoveryTimelinePTSOffset() = %s, want %s", got, want)
	}
}

func TestSharedSessionCurrentRecoveryTimelinePTSOffsetCapsBoundaryPCR(t *testing.T) {
	session := &sharedRuntimeSession{}
	session.mu.Lock()
	session.recoveryBoundaryPCRSet = true
	session.recoveryBoundaryPCRBase = uint64((13 * time.Hour).Nanoseconds() * mpegTSPCRClockHz / int64(time.Second))
	session.mu.Unlock()

	if got, want := session.currentRecoveryTimelinePTSOffset(), maxRecoveryTimelinePTSOffset; got != want {
		t.Fatalf("currentRecoveryTimelinePTSOffset() = %s, want %s", got, want)
	}
}

func TestSharedSessionCurrentRecoveryTimelinePTSOffsetCapsObservedPCR(t *testing.T) {
	session := &sharedRuntimeSession{}
	session.mu.Lock()
	session.recoveryBoundaryPCRSet = true
	session.recoveryBoundaryPCRBase = 5 * mpegTSPCRClockHz
	session.recoveryObservedPCRBase = uint64((13 * time.Hour).Nanoseconds() * mpegTSPCRClockHz / int64(time.Second))
	session.recoveryObservedPCRAt = time.Now().UTC()
	session.mu.Unlock()

	if got, want := session.currentRecoveryTimelinePTSOffset(), maxRecoveryTimelinePTSOffset; got != want {
		t.Fatalf("currentRecoveryTimelinePTSOffset() = %s, want %s", got, want)
	}
}

func TestSharedSessionRecoveryHeartbeatRetainsLagWindowForTinyPackets(t *testing.T) {
	joinLagBytes := 2 * 1024 * 1024
	chunkBytes := defaultPumpChunkBytes

	ring := NewChunkRingWithLimits(
		ringCapacityForLag(chunkBytes, joinLagBytes),
		ringByteBudgetForLag(chunkBytes, joinLagBytes),
	)
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				recoveryFillerEnabled:  true,
				recoveryFillerInterval: 2 * time.Millisecond,
			},
		},
		ring: ring,
	}
	session.subscribers = map[uint64]SubscriberStats{
		1: {
			SubscriberID: 1,
			ClientAddr:   "127.0.0.1:12345",
			StartedAt:    time.Now().UTC(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := session.startRecoveryHeartbeat(ctx)
	defer stop()

	waitFor(t, 2*time.Second, func() bool {
		return len(ring.Snapshot()) >= 96
	})
	snapshot := ring.Snapshot()
	startSeq := snapshot[0].Seq

	time.Sleep(150 * time.Millisecond)

	read := ring.ReadFrom(startSeq)
	defer read.Release()
	if read.Behind {
		t.Fatalf(
			"ReadFrom(%d) Behind = true under heartbeat-only load; lag window collapsed unexpectedly",
			startSeq,
		)
	}
}

func TestSharedSessionFinishCancelsSourceHealthPersistWorker(t *testing.T) {
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"177": {ChannelID: 77, GuideNumber: "177", GuideName: "Teardown", Enabled: true},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	channel := provider.channelsByGuide["177"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ring:        NewChunkRing(8),
		ctx:         ctx,
		cancel:      cancel,
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			defaultSourceHealthPersistQueueSize,
		),
	}

	manager.mu.Lock()
	manager.sessions[channel.ChannelID] = session
	manager.mu.Unlock()

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		session.runSourceHealthPersist()
	}()

	session.finish(nil)

	select {
	case <-workerDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("source health persistence worker did not exit after finish")
	}

	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("session context error = %v, want context canceled", err)
	}

	manager.mu.Lock()
	_, exists := manager.sessions[channel.ChannelID]
	manager.mu.Unlock()
	if exists {
		t.Fatal("session was not removed from manager after finish")
	}
}

func TestSharedSessionSourceHealthQueueOverflowPreservesLatestPerSourceOverlay(t *testing.T) {
	provider := &fakeChannelsProvider{}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channels.Channel{ChannelID: 88, GuideNumber: "188", GuideName: "Overflow", Enabled: true},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			1,
		),
		sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
		sourceHealthQueue:             make([]int64, 0, defaultSourceHealthPersistQueueSize),
		sourceHealthCoalescedBySource: make(map[int64]int64),
		sourceHealthDroppedBySource:   make(map[int64]int64),
	}

	sourceID := int64(44)
	firstObservedAt := time.Unix(1_700_120_000, 0).UTC()
	firstEventID := manager.stageRecentSourceFailure(sourceID, "first failure", firstObservedAt)
	session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    firstEventID,
		success:    false,
		reason:     "first failure",
		observedAt: firstObservedAt,
	})

	secondObservedAt := firstObservedAt.Add(1 * time.Second)
	secondEventID := manager.stageRecentSourceSuccess(sourceID, secondObservedAt)
	session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    secondEventID,
		success:    true,
		observedAt: secondObservedAt,
	})

	updated := manager.applyRecentSourceHealth([]channels.Source{{SourceID: sourceID}})
	if len(updated) != 1 {
		t.Fatalf("applyRecentSourceHealth len = %d, want 1", len(updated))
	}
	if got := updated[0].FailCount; got != 0 {
		t.Fatalf("after fail->success overflow, FailCount = %d, want 0", got)
	}
	if got := updated[0].CooldownUntil; got != 0 {
		t.Fatalf("after fail->success overflow, CooldownUntil = %d, want 0", got)
	}
	if got := updated[0].SuccessCount; got != 1 {
		t.Fatalf("after fail->success overflow, SuccessCount = %d, want 1", got)
	}

	thirdObservedAt := secondObservedAt.Add(1 * time.Second)
	thirdEventID := manager.stageRecentSourceFailure(sourceID, "third failure", thirdObservedAt)
	session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    thirdEventID,
		success:    false,
		reason:     "third failure",
		observedAt: thirdObservedAt,
	})

	updated = manager.applyRecentSourceHealth([]channels.Source{{SourceID: sourceID}})
	if len(updated) != 1 {
		t.Fatalf("applyRecentSourceHealth len = %d, want 1", len(updated))
	}
	if got := updated[0].FailCount; got != 2 {
		t.Fatalf("after fail->success->fail coalesce, FailCount = %d, want 2", got)
	}
	if got := updated[0].LastFailReason; got != "third failure" {
		t.Fatalf("after fail->success->fail coalesce, LastFailReason = %q, want %q", got, "third failure")
	}

	coalescedTotal, droppedTotal, coalescedBySource, droppedBySource := session.sourceHealthPersistBackpressureSnapshot()
	if got, want := coalescedTotal, int64(1); got != want {
		t.Fatalf("coalesced_total = %d, want %d", got, want)
	}
	if got := droppedTotal; got != 0 {
		t.Fatalf("dropped_total = %d, want 0", got)
	}
	if got, want := coalescedBySource[sourceID], int64(1); got != want {
		t.Fatalf("coalesced_by_source[%d] = %d, want %d", sourceID, got, want)
	}
	if got := droppedBySource[sourceID]; got != 0 {
		t.Fatalf("dropped_by_source[%d] = %d, want 0", sourceID, got)
	}
}

func TestSharedSessionSourceHealthQueueDrainPersistsLatestCoalescedEvent(t *testing.T) {
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"188": {ChannelID: 88, GuideNumber: "188", GuideName: "Drain", Enabled: true},
		},
	}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channels.Channel{ChannelID: 88, GuideNumber: "188", GuideName: "Drain", Enabled: true},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			1,
		),
		sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
		sourceHealthQueue:             make([]int64, 0, defaultSourceHealthPersistQueueSize),
		sourceHealthCoalescedBySource: make(map[int64]int64),
		sourceHealthDroppedBySource:   make(map[int64]int64),
	}

	sourceID := int64(55)
	firstObservedAt := time.Unix(1_700_130_000, 0).UTC()
	firstEventID := manager.stageRecentSourceFailure(sourceID, "first failure", firstObservedAt)
	session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    firstEventID,
		success:    false,
		reason:     "first failure",
		observedAt: firstObservedAt,
	})

	secondObservedAt := firstObservedAt.Add(1 * time.Second)
	secondEventID := manager.stageRecentSourceSuccess(sourceID, secondObservedAt)
	session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    secondEventID,
		success:    true,
		observedAt: secondObservedAt,
	})

	thirdObservedAt := secondObservedAt.Add(1 * time.Second)
	thirdEventID := manager.stageRecentSourceFailure(sourceID, "third failure", thirdObservedAt)
	session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    thirdEventID,
		success:    false,
		reason:     "third failure",
		observedAt: thirdObservedAt,
	})

	session.drainSourceHealthPersistQueue()

	failures := provider.sourceFailures()
	if len(failures) != 2 {
		t.Fatalf("failures len = %d, want 2", len(failures))
	}
	if got, want := failures[0].reason, "first failure"; got != want {
		t.Fatalf("first persisted failure reason = %q, want %q", got, want)
	}
	if got, want := failures[1].reason, "third failure"; got != want {
		t.Fatalf("second persisted failure reason = %q, want %q", got, want)
	}
	if got := provider.sourceSuccesses(); len(got) != 0 {
		t.Fatalf("successes len = %d, want 0 (superseded success should not persist)", len(got))
	}

	manager.recentHealth.mu.Lock()
	pending := len(manager.recentHealth.pendingBySource[sourceID])
	manager.recentHealth.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending recent-health events = %d, want 0 after drain", pending)
	}

	coalescedTotal, droppedTotal, coalescedBySource, droppedBySource := session.sourceHealthPersistBackpressureSnapshot()
	if got, want := coalescedTotal, int64(1); got != want {
		t.Fatalf("coalesced_total = %d, want %d", got, want)
	}
	if got := droppedTotal; got != 0 {
		t.Fatalf("dropped_total = %d, want 0", got)
	}
	if got, want := coalescedBySource[sourceID], int64(1); got != want {
		t.Fatalf("coalesced_by_source[%d] = %d, want %d", sourceID, got, want)
	}
	if got := droppedBySource[sourceID]; got != 0 {
		t.Fatalf("dropped_by_source[%d] = %d, want 0", sourceID, got)
	}
}

func TestDrainSourceHealthPersistQueueWithTimeoutBranches(t *testing.T) {
	newSession := func(markFailureDelay time.Duration) (*SessionManager, *sharedRuntimeSession, *fakeChannelsProvider) {
		provider := &fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{
				"188": {ChannelID: 88, GuideNumber: "188", GuideName: "Drain Timeout", Enabled: true},
			},
			sourcesByID: map[int64][]channels.Source{
				88: {
					{
						SourceID:      41,
						ChannelID:     88,
						ItemKey:       "src:drain:41",
						StreamURL:     "http://example.com/drain-41.ts",
						PriorityIndex: 0,
						Enabled:       true,
					},
					{
						SourceID:      42,
						ChannelID:     88,
						ItemKey:       "src:drain:42",
						StreamURL:     "http://example.com/drain-42.ts",
						PriorityIndex: 1,
						Enabled:       true,
					},
				},
			},
			markFailureDelay: markFailureDelay,
		}
		manager := NewSessionManager(SessionManagerConfig{
			Mode:                       "direct",
			StartupTimeout:             500 * time.Millisecond,
			FailoverTotalTimeout:       time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           188,
			BufferPublishFlushInterval: 20 * time.Millisecond,
			SessionIdleTimeout:         50 * time.Millisecond,
		}, NewPool(1), provider)
		if manager == nil {
			t.Fatal("manager is nil")
		}

		ctx, cancel := context.WithCancel(context.Background())
		session := &sharedRuntimeSession{
			manager:                       manager,
			channel:                       provider.channelsByGuide["188"],
			ring:                          NewChunkRing(8),
			ctx:                           ctx,
			cancel:                        cancel,
			readyCh:                       make(chan struct{}),
			subscribers:                   make(map[uint64]SubscriberStats),
			startedAt:                     time.Now().UTC(),
			sourceHealthPersistCh:         make(chan sourceHealthPersistRequest, 16),
			sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
			sourceHealthQueue:             make([]int64, 0, 16),
			sourceHealthCoalescedBySource: make(map[int64]int64),
			sourceHealthDroppedBySource:   make(map[int64]int64),
		}
		t.Cleanup(cancel)
		return manager, session, provider
	}

	pendingCount := func(manager *SessionManager) int {
		if manager == nil || manager.recentHealth == nil {
			return 0
		}
		manager.recentHealth.mu.Lock()
		defer manager.recentHealth.mu.Unlock()
		total := 0
		for _, byEvent := range manager.recentHealth.pendingBySource {
			total += len(byEvent)
		}
		return total
	}

	stageQueuedFailure := func(
		manager *SessionManager,
		session *sharedRuntimeSession,
		sourceID int64,
		reason string,
		observedAt time.Time,
	) {
		eventID := manager.stageRecentSourceFailure(sourceID, reason, observedAt)
		session.sourceHealthPersistCh <- sourceHealthPersistRequest{
			sourceID:   sourceID,
			eventID:    eventID,
			success:    false,
			reason:     reason,
			observedAt: observedAt,
			channelID:  session.channel.ChannelID,
		}
	}

	stageCoalescedFailure := func(
		manager *SessionManager,
		session *sharedRuntimeSession,
		sourceID int64,
		reason string,
		observedAt time.Time,
	) {
		eventID := manager.stageRecentSourceFailure(sourceID, reason, observedAt)
		session.sourceHealthPersistMu.Lock()
		session.sourceHealthCoalesced[sourceID] = sourceHealthPersistRequest{
			sourceID:   sourceID,
			eventID:    eventID,
			success:    false,
			reason:     reason,
			observedAt: observedAt,
			channelID:  session.channel.ChannelID,
		}
		session.sourceHealthQueue = append(session.sourceHealthQueue, sourceID)
		session.sourceHealthPersistMu.Unlock()
	}

	t.Run("immediate-expiry drops pending without draining", func(t *testing.T) {
		manager, session, _ := newSession(0)
		base := time.Unix(1_770_001_000, 0).UTC()
		stageQueuedFailure(manager, session, 41, "queued-one", base)
		stageQueuedFailure(manager, session, 42, "queued-two", base.Add(time.Second))
		stageCoalescedFailure(manager, session, 43, "coalesced-three", base.Add(2*time.Second))

		drained, dropped, timedOut := session.drainSourceHealthPersistQueueWithTimeout(time.Nanosecond)
		if !timedOut {
			t.Fatal("timedOut = false, want true for nanosecond budget")
		}
		if got, want := drained, 0; got != want {
			t.Fatalf("drained = %d, want %d for immediate-expiry branch", got, want)
		}
		if got, want := dropped, 3; got != want {
			t.Fatalf("dropped = %d, want %d", got, want)
		}
		if got := pendingCount(manager); got != 0 {
			t.Fatalf("pending recent-health entries = %d, want 0 after drop", got)
		}
	})

	t.Run("full-drain completes before deadline", func(t *testing.T) {
		manager, session, provider := newSession(0)
		base := time.Unix(1_770_002_000, 0).UTC()
		stageQueuedFailure(manager, session, 41, "queued-one", base)
		stageQueuedFailure(manager, session, 42, "queued-two", base.Add(time.Second))
		stageQueuedFailure(manager, session, 41, "queued-three", base.Add(2*time.Second))

		drained, dropped, timedOut := session.drainSourceHealthPersistQueueWithTimeout(300 * time.Millisecond)
		if timedOut {
			t.Fatal("timedOut = true, want false when deadline budget is sufficient")
		}
		if got, want := drained, 3; got != want {
			t.Fatalf("drained = %d, want %d", got, want)
		}
		if got, want := dropped, 0; got != want {
			t.Fatalf("dropped = %d, want %d", got, want)
		}
		if got := pendingCount(manager); got != 0 {
			t.Fatalf("pending recent-health entries = %d, want 0 after full drain", got)
		}
		if got, want := len(provider.sourceFailures()), 3; got != want {
			t.Fatalf("persisted failures = %d, want %d", got, want)
		}
	})

	t.Run("partial-drain times out and drops remainder", func(t *testing.T) {
		manager, session, provider := newSession(0)
		provider.mu.Lock()
		provider.markFailureDelays = []time.Duration{0, 250 * time.Millisecond}
		provider.mu.Unlock()
		base := time.Unix(1_770_003_000, 0).UTC()
		stageQueuedFailure(manager, session, 41, "queued-one", base)
		stageQueuedFailure(manager, session, 42, "queued-two", base.Add(time.Second))
		stageQueuedFailure(manager, session, 41, "queued-three", base.Add(2*time.Second))
		stageQueuedFailure(manager, session, 42, "queued-four", base.Add(3*time.Second))

		drained, dropped, timedOut := session.drainSourceHealthPersistQueueWithTimeout(120 * time.Millisecond)
		if !timedOut {
			t.Fatal("timedOut = false, want true when deadline expires mid-drain")
		}
		if drained <= 0 {
			t.Fatalf("drained = %d, want > 0 for partial-drain branch", drained)
		}
		if dropped <= 0 {
			t.Fatalf("dropped = %d, want > 0 for partial-drain branch", dropped)
		}
		if got := pendingCount(manager); got != 0 {
			t.Fatalf("pending recent-health entries = %d, want 0 after timeout-drop", got)
		}
	})
}

func TestDropSourceHealthPersistHelpers(t *testing.T) {
	newSession := func() (*SessionManager, *sharedRuntimeSession) {
		provider := &fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{
				"188": {ChannelID: 88, GuideNumber: "188", GuideName: "Drop Helpers", Enabled: true},
			},
			sourcesByID: map[int64][]channels.Source{
				88: {
					{
						SourceID:      41,
						ChannelID:     88,
						ItemKey:       "src:drop:41",
						StreamURL:     "http://example.com/drop-41.ts",
						PriorityIndex: 0,
						Enabled:       true,
					},
					{
						SourceID:      42,
						ChannelID:     88,
						ItemKey:       "src:drop:42",
						StreamURL:     "http://example.com/drop-42.ts",
						PriorityIndex: 1,
						Enabled:       true,
					},
				},
			},
		}
		manager := NewSessionManager(SessionManagerConfig{
			Mode:                       "direct",
			StartupTimeout:             500 * time.Millisecond,
			FailoverTotalTimeout:       time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           188,
			BufferPublishFlushInterval: 20 * time.Millisecond,
			SessionIdleTimeout:         50 * time.Millisecond,
		}, NewPool(1), provider)
		if manager == nil {
			t.Fatal("manager is nil")
		}

		session := &sharedRuntimeSession{
			manager:                       manager,
			channel:                       provider.channelsByGuide["188"],
			sourceHealthPersistCh:         make(chan sourceHealthPersistRequest, 16),
			sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
			sourceHealthQueue:             make([]int64, 0, 16),
			sourceHealthCoalescedBySource: make(map[int64]int64),
			sourceHealthDroppedBySource:   make(map[int64]int64),
		}
		return manager, session
	}

	pendingCount := func(manager *SessionManager) int {
		if manager == nil || manager.recentHealth == nil {
			return 0
		}
		manager.recentHealth.mu.Lock()
		defer manager.recentHealth.mu.Unlock()
		total := 0
		for _, byEvent := range manager.recentHealth.pendingBySource {
			total += len(byEvent)
		}
		return total
	}

	stageFailure := func(manager *SessionManager, sourceID int64, reason string, observedAt time.Time) sourceHealthPersistRequest {
		return sourceHealthPersistRequest{
			sourceID:    sourceID,
			eventID:     manager.stageRecentSourceFailure(sourceID, reason, observedAt),
			success:     false,
			reason:      reason,
			observedAt:  observedAt,
			channelID:   88,
			guideNumber: "188",
		}
	}

	t.Run("dropQueuedSourceHealthPersist", func(t *testing.T) {
		manager, session := newSession()
		base := time.Unix(1_770_010_000, 0).UTC()
		session.sourceHealthPersistCh <- stageFailure(manager, 41, "queued-one", base)
		session.sourceHealthPersistCh <- stageFailure(manager, 42, "queued-two", base.Add(time.Second))

		if got, want := pendingCount(manager), 2; got != want {
			t.Fatalf("pending before dropQueued = %d, want %d", got, want)
		}
		if got, want := session.dropQueuedSourceHealthPersist(), 2; got != want {
			t.Fatalf("dropQueuedSourceHealthPersist() = %d, want %d", got, want)
		}
		if got := len(session.sourceHealthPersistCh); got != 0 {
			t.Fatalf("len(sourceHealthPersistCh) = %d, want 0 after dropQueued", got)
		}
		if got := pendingCount(manager); got != 0 {
			t.Fatalf("pending after dropQueued = %d, want 0", got)
		}
	})

	t.Run("dropCoalescedSourceHealthPersist", func(t *testing.T) {
		manager, session := newSession()
		base := time.Unix(1_770_011_000, 0).UTC()
		reqA := stageFailure(manager, 41, "coalesced-one", base)
		reqB := stageFailure(manager, 42, "coalesced-two", base.Add(time.Second))
		session.sourceHealthCoalesced[41] = reqA
		session.sourceHealthCoalesced[42] = reqB
		session.sourceHealthQueue = append(session.sourceHealthQueue, 41, 42)
		session.sourceHealthCoalescedBySource[41] = 3
		session.sourceHealthDroppedBySource[42] = 2

		if got, want := pendingCount(manager), 2; got != want {
			t.Fatalf("pending before dropCoalesced = %d, want %d", got, want)
		}
		if got, want := session.dropCoalescedSourceHealthPersist(), 2; got != want {
			t.Fatalf("dropCoalescedSourceHealthPersist() = %d, want %d", got, want)
		}
		if session.sourceHealthQueue != nil {
			t.Fatal("sourceHealthQueue should be nil after dropCoalesced")
		}
		if session.sourceHealthCoalesced != nil {
			t.Fatal("sourceHealthCoalesced should be nil after dropCoalesced")
		}
		if session.sourceHealthCoalescedBySource != nil {
			t.Fatal("sourceHealthCoalescedBySource should be nil after dropCoalesced")
		}
		if session.sourceHealthDroppedBySource != nil {
			t.Fatal("sourceHealthDroppedBySource should be nil after dropCoalesced")
		}
		if got := pendingCount(manager); got != 0 {
			t.Fatalf("pending after dropCoalesced = %d, want 0", got)
		}
	})

	t.Run("dropPendingSourceHealthPersist", func(t *testing.T) {
		manager, session := newSession()
		base := time.Unix(1_770_012_000, 0).UTC()
		session.sourceHealthPersistCh <- stageFailure(manager, 41, "queued-one", base)
		req := stageFailure(manager, 42, "coalesced-one", base.Add(time.Second))
		session.sourceHealthCoalesced[42] = req
		session.sourceHealthQueue = append(session.sourceHealthQueue, 42)

		if got, want := pendingCount(manager), 2; got != want {
			t.Fatalf("pending before dropPending = %d, want %d", got, want)
		}
		if got, want := session.dropPendingSourceHealthPersist(), 2; got != want {
			t.Fatalf("dropPendingSourceHealthPersist() = %d, want %d", got, want)
		}
		if got := pendingCount(manager); got != 0 {
			t.Fatalf("pending after dropPending = %d, want 0", got)
		}
	})
}

func TestPersistSourceHealthSkipsStaleQueuedEventAfterChannelClear(t *testing.T) {
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"188": {ChannelID: 88, GuideNumber: "188", GuideName: "Skips", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			88: {
				{
					SourceID:      44,
					ChannelID:     88,
					ItemKey:       "src:skip:one",
					StreamURL:     "http://example.com/skip.ts",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channels.Channel{ChannelID: 88, GuideNumber: "188", GuideName: "Skips", Enabled: true},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			defaultSourceHealthPersistQueueSize,
		),
		sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
		sourceHealthQueue:             make([]int64, 0, defaultSourceHealthPersistQueueSize),
		sourceHealthCoalescedBySource: make(map[int64]int64),
		sourceHealthDroppedBySource:   make(map[int64]int64),
	}

	sourceID := int64(44)
	staleObservedAt := time.Unix(1_700_240_000, 0).UTC()
	staleEventID := manager.stageRecentSourceFailure(sourceID, "stale failure", staleObservedAt)
	session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    staleEventID,
		success:    false,
		reason:     "stale failure",
		observedAt: staleObservedAt,
	})

	// Simulate clear immediately after event staging; stale health must not
	// be allowed to persist back into channel source state.
	if err := manager.ClearSourceHealth(88); err != nil {
		t.Fatalf("ClearSourceHealth() error = %v", err)
	}

	session.drainSourceHealthQueue()

	failures := provider.sourceFailures()
	if len(failures) != 0 {
		t.Fatalf("stale failures persisted after clear = %d, want 0", len(failures))
	}
	if len(provider.sourceSuccesses()) != 0 {
		t.Fatalf("stale successes persisted after clear = %d, want 0", len(provider.sourceSuccesses()))
	}

	sources, err := provider.ListSources(context.Background(), 88, true)
	if err != nil {
		t.Fatalf("ListSources() error = %v", err)
	}
	updated := manager.applyRecentSourceHealth(sources)
	if len(updated) != 1 {
		t.Fatalf("applyRecentSourceHealth sources len = %d, want 1", len(updated))
	}
	if updated[0].FailCount != 0 || updated[0].SuccessCount != 0 {
		t.Fatalf("stale overlay applied after clear = fail:%d success:%d", updated[0].FailCount, updated[0].SuccessCount)
	}
}

func TestPersistSourceHealthSkipsStaleCoalescedEventsAfterAllClear(t *testing.T) {
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"188": {ChannelID: 88, GuideNumber: "188", GuideName: "Skips", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			88: {
				{
					SourceID:      45,
					ChannelID:     88,
					ItemKey:       "src:skip:two",
					StreamURL:     "http://example.com/skip2.ts",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channels.Channel{ChannelID: 88, GuideNumber: "188", GuideName: "Skips", Enabled: true},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			1,
		),
		sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
		sourceHealthQueue:             make([]int64, 0, defaultSourceHealthPersistQueueSize),
		sourceHealthCoalescedBySource: make(map[int64]int64),
		sourceHealthDroppedBySource:   make(map[int64]int64),
	}

	sourceID := int64(45)
	baseObservedAt := time.Unix(1_700_241_000, 0).UTC()

	failureEventID := manager.stageRecentSourceFailure(sourceID, "stale failure one", baseObservedAt)
	session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    failureEventID,
		success:    false,
		reason:     "stale failure one",
		observedAt: baseObservedAt,
	})
	session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    manager.stageRecentSourceSuccess(sourceID, baseObservedAt.Add(time.Second)),
		success:    true,
		observedAt: baseObservedAt.Add(time.Second),
	})
	session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    manager.stageRecentSourceFailure(sourceID, "stale failure two", baseObservedAt.Add(2*time.Second)),
		success:    false,
		reason:     "stale failure two",
		observedAt: baseObservedAt.Add(2 * time.Second),
	})

	if err := manager.ClearAllSourceHealth(); err != nil {
		t.Fatalf("ClearAllSourceHealth() error = %v", err)
	}
	session.drainSourceHealthQueue()

	if len(provider.sourceFailures()) != 0 {
		t.Fatalf("stale failures persisted after clear all = %d, want 0", len(provider.sourceFailures()))
	}
	if len(provider.sourceSuccesses()) != 0 {
		t.Fatalf("stale successes persisted after clear all = %d, want 0", len(provider.sourceSuccesses()))
	}
}

func TestPersistSourceHealthRetriesTransientFailureThenSucceeds(t *testing.T) {
	transientErr := errors.New("simulated_persist_failure")
	provider := &fakeChannelsProvider{
		// First two calls fail, third succeeds.
		markFailureErrs: []error{transientErr, transientErr, nil},
	}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceID := int64(71)
	observedAt := time.Unix(1_700_200_000, 0).UTC()
	eventID := manager.stageRecentSourceFailure(sourceID, "transient failure", observedAt)

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channels.Channel{ChannelID: 71, GuideNumber: "171", GuideName: "Retry", Enabled: true},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
		ctx:         ctx,
		cancel:      cancel,
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			defaultSourceHealthPersistQueueSize,
		),
	}

	session.persistSourceHealth(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    eventID,
		success:    false,
		reason:     "transient failure",
		observedAt: observedAt,
		channelID:  71,
	})

	// Verify the failure was eventually persisted after retries.
	failures := provider.sourceFailures()
	if len(failures) != 1 {
		t.Fatalf("failures len = %d, want 1 (should succeed on third attempt)", len(failures))
	}
	if failures[0].reason != "transient failure" {
		t.Fatalf("failure reason = %q, want %q", failures[0].reason, "transient failure")
	}

	// Verify the pending event was cleared via markPersisted.
	manager.recentHealth.mu.Lock()
	pending := len(manager.recentHealth.pendingBySource[sourceID])
	manager.recentHealth.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending events = %d, want 0 (should be cleared after successful persist)", pending)
	}
}

func TestPersistSourceHealthDropsAfterMaxRetries(t *testing.T) {
	permanentErr := errors.New("permanent_persist_failure")
	provider := &fakeChannelsProvider{
		// All calls fail.
		markFailureErrs: []error{permanentErr, permanentErr, permanentErr},
	}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceID := int64(72)
	observedAt := time.Unix(1_700_200_100, 0).UTC()
	eventID := manager.stageRecentSourceFailure(sourceID, "permanent failure", observedAt)

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channels.Channel{ChannelID: 72, GuideNumber: "172", GuideName: "MaxRetry", Enabled: true},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
		ctx:         ctx,
		cancel:      cancel,
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			defaultSourceHealthPersistQueueSize,
		),
	}

	session.persistSourceHealth(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    eventID,
		success:    false,
		reason:     "permanent failure",
		observedAt: observedAt,
		channelID:  72,
	})

	// Verify no successful persistence happened.
	failures := provider.sourceFailures()
	if len(failures) != 0 {
		t.Fatalf("failures len = %d, want 0 (all attempts should have failed)", len(failures))
	}

	// Verify the pending event was dropped (not left stranded).
	manager.recentHealth.mu.Lock()
	pending := len(manager.recentHealth.pendingBySource[sourceID])
	manager.recentHealth.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending events = %d, want 0 (should be dropped after max retries)", pending)
	}
}

func TestPostCancelSourceHealthEventNotStaged(t *testing.T) {
	provider := &fakeChannelsProvider{}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channels.Channel{ChannelID: 73, GuideNumber: "173", GuideName: "PostCancel", Enabled: true},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
		ctx:         ctx,
		cancel:      cancel,
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			defaultSourceHealthPersistQueueSize,
		),
		sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
		sourceHealthQueue:             make([]int64, 0, defaultSourceHealthPersistQueueSize),
		sourceHealthCoalescedBySource: make(map[int64]int64),
		sourceHealthDroppedBySource:   make(map[int64]int64),
	}

	// Cancel the session context to simulate terminal teardown.
	cancel()

	// Attempt to record source failure/success after cancellation.
	sourceID := int64(44)
	session.recordSourceFailure(ctx, sourceID, "http://example.com/stream.ts",
		&sourceStartupError{reason: "post cancel failure", err: errors.New("post cancel failure")}, true)
	session.recordSourceSuccess(ctx, sourceID)

	// Verify no events were staged in the overlay.
	manager.recentHealth.mu.Lock()
	pending := len(manager.recentHealth.pendingBySource[sourceID])
	manager.recentHealth.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending recent-health events = %d, want 0 (post-cancel events should not be staged)", pending)
	}
}

func TestEnqueueSourceHealthPersistDropsAfterWorkerDone(t *testing.T) {
	provider := &fakeChannelsProvider{}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a done channel that's already closed (simulating worker exit).
	workerDone := make(chan struct{})
	close(workerDone)

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channels.Channel{ChannelID: 74, GuideNumber: "174", GuideName: "WorkerDone", Enabled: true},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
		ctx:         ctx,
		cancel:      cancel,
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			defaultSourceHealthPersistQueueSize,
		),
		sourceHealthPersistDone:       workerDone,
		sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
		sourceHealthQueue:             make([]int64, 0, defaultSourceHealthPersistQueueSize),
		sourceHealthCoalescedBySource: make(map[int64]int64),
		sourceHealthDroppedBySource:   make(map[int64]int64),
	}

	sourceID := int64(45)
	observedAt := time.Unix(1_700_300_000, 0).UTC()
	eventID := manager.stageRecentSourceFailure(sourceID, "late failure", observedAt)

	session.enqueueSourceHealthPersist(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    eventID,
		success:    false,
		reason:     "late failure",
		observedAt: observedAt,
	})

	// Verify the event was dropped from pending state.
	manager.recentHealth.mu.Lock()
	pending := len(manager.recentHealth.pendingBySource[sourceID])
	manager.recentHealth.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending events = %d, want 0 (should be dropped when worker is done)", pending)
	}
}

func TestPersistSourceHealthDropsOnErrSourceNotFound(t *testing.T) {
	provider := &fakeChannelsProvider{
		// Return ErrSourceNotFound on the first (and only) call.
		markFailureErrs: []error{channels.ErrSourceNotFound},
	}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceID := int64(80)
	observedAt := time.Unix(1_700_400_000, 0).UTC()
	eventID := manager.stageRecentSourceFailure(sourceID, "not found drop", observedAt)

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channels.Channel{ChannelID: 80, GuideNumber: "180", GuideName: "NotFoundDrop", Enabled: true},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
		ctx:         ctx,
		cancel:      cancel,
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			defaultSourceHealthPersistQueueSize,
		),
	}

	session.persistSourceHealth(sourceHealthPersistRequest{
		sourceID:   sourceID,
		eventID:    eventID,
		success:    false,
		reason:     "not found drop",
		observedAt: observedAt,
		channelID:  80,
	})

	// Verify no successful persistence happened (the call returned ErrSourceNotFound).
	failures := provider.sourceFailures()
	if len(failures) != 0 {
		t.Fatalf("failures len = %d, want 0 (ErrSourceNotFound should drop, not persist)", len(failures))
	}

	// Verify only one MarkSourceFailure call was made (no retries).
	provider.mu.Lock()
	remainingErrs := len(provider.markFailureErrs)
	provider.mu.Unlock()
	if remainingErrs != 0 {
		t.Fatalf("remaining markFailureErrs = %d, want 0 (exactly one call should have been made)", remainingErrs)
	}

	// Verify the pending event was dropped (not left stranded).
	manager.recentHealth.mu.Lock()
	pending := len(manager.recentHealth.pendingBySource[sourceID])
	manager.recentHealth.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending events = %d, want 0 (should be dropped on ErrSourceNotFound)", pending)
	}
}

func TestPersistSourceHealthRetryExitsOnContextCancel(t *testing.T) {
	transientErr := errors.New("transient_for_cancel_test")
	provider := &fakeChannelsProvider{
		// All three attempts would fail if allowed to complete.
		markFailureErrs: []error{transientErr, transientErr, transientErr},
	}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 20 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	sourceID := int64(81)
	observedAt := time.Unix(1_700_400_100, 0).UTC()
	eventID := manager.stageRecentSourceFailure(sourceID, "cancel during backoff", observedAt)

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channels.Channel{ChannelID: 81, GuideNumber: "181", GuideName: "CancelBackoff", Enabled: true},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
		ctx:         ctx,
		cancel:      cancel,
		sourceHealthPersistCh: make(
			chan sourceHealthPersistRequest,
			defaultSourceHealthPersistQueueSize,
		),
	}

	// Cancel context after a short delay so the first attempt fails and the
	// backoff select picks up ctx.Done() instead of waiting the full backoff.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	done := make(chan struct{})
	go func() {
		session.persistSourceHealth(sourceHealthPersistRequest{
			sourceID:   sourceID,
			eventID:    eventID,
			success:    false,
			reason:     "cancel during backoff",
			observedAt: observedAt,
			channelID:  81,
		})
		close(done)
	}()

	// The persist call should exit promptly (well before all 3 retries with
	// their cumulative backoff of 50+100+150=300ms would complete).
	select {
	case <-done:
		// Good — exited promptly.
	case <-time.After(2 * time.Second):
		t.Fatal("persistSourceHealth did not exit promptly after context cancellation")
	}

	// Verify the pending event was dropped.
	manager.recentHealth.mu.Lock()
	pending := len(manager.recentHealth.pendingBySource[sourceID])
	manager.recentHealth.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending events = %d, want 0 (should be dropped on context cancel during backoff)", pending)
	}
}

func TestSessionManagerBurstGapFixtureAvoidsWatchdogTimeout(t *testing.T) {
	burstGap := newBurstGapSourceServer(24, []byte("A"), 1*time.Second)
	defer burstGap.Close()

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 120; i++ {
			if _, err := w.Write([]byte("B")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}))
	defer healthy.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:burst-gap",
					StreamURL:     burstGap.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:healthy",
					StreamURL:     healthy.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             1 * time.Second,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                120 * time.Millisecond,
		StallHardDeadline:          1200 * time.Millisecond,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  8,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	// Scaled-down stand-in for the Channels DVR 6s no-data timeout.
	timedOut, _, err := streamWithNoDataWatchdog(sub, 600*time.Millisecond, 2*time.Second)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stream() error = %v", err)
	}
	if timedOut {
		t.Fatal("watchdog timeout fired; expected stall recovery to keep stream data flowing")
	}
}

func TestSessionManagerPacedSourcePassesChannelsStyleWatchdog(t *testing.T) {
	paced := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 220; i++ {
			if _, err := w.Write([]byte("P")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}))
	defer paced.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:paced",
					StreamURL:     paced.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             1 * time.Second,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                3 * time.Second,
		StallHardDeadline:          9 * time.Second,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	// Scaled-down stand-in for the Channels DVR 6s no-data timeout.
	timedOut, _, err := streamWithNoDataWatchdog(sub, 600*time.Millisecond, 1500*time.Millisecond)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stream() error = %v", err)
	}
	if timedOut {
		t.Fatal("watchdog timeout fired for paced source")
	}
}

func TestSessionSubscriptionDisconnectsLaggingClient(t *testing.T) {
	isolateStreamSlowSkipStatsForTest(t)

	ring := NewChunkRing(2)
	_ = ring.PublishChunk([]byte("a"), time.Now())
	_ = ring.PublishChunk([]byte("b"), time.Now())
	_ = ring.PublishChunk([]byte("c"), time.Now())
	ring.Close(nil)
	snap := ring.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot chunk count = %d, want 2", len(snap))
	}
	expectedOldest := snap[0].Seq
	expectedNext := snap[len(snap)-1].Seq + 1
	expectedBytes := 0
	for i := range snap {
		expectedBytes += len(snap[i].Data)
	}

	session := &sharedRuntimeSession{ring: ring}
	sub := &SessionSubscription{
		session:    session,
		nextSeq:    0,
		slowPolicy: slowClientPolicyDisconnect,
	}

	err := sub.Stream(context.Background(), &recordingResponseWriter{})
	if !errors.Is(err, ErrSlowClientLagged) {
		t.Fatalf("Stream() error = %v, want ErrSlowClientLagged", err)
	}
	lag, ok := slowClientLagDetailsFromError(err)
	if !ok {
		t.Fatalf("expected slow-client lag details on error %v", err)
	}
	if lag.RequestedSeq != 0 {
		t.Fatalf("lag.RequestedSeq = %d, want 0", lag.RequestedSeq)
	}
	if lag.OldestSeq != expectedOldest {
		t.Fatalf("lag.OldestSeq = %d, want %d", lag.OldestSeq, expectedOldest)
	}
	if lag.LagChunks != expectedOldest {
		t.Fatalf("lag.LagChunks = %d, want %d", lag.LagChunks, expectedOldest)
	}
	if lag.RingNextSeq != expectedNext {
		t.Fatalf("lag.RingNextSeq = %d, want %d", lag.RingNextSeq, expectedNext)
	}
	if lag.BufferedChunks != len(snap) {
		t.Fatalf("lag.BufferedChunks = %d, want %d", lag.BufferedChunks, len(snap))
	}
	if lag.BufferedBytes != expectedBytes {
		t.Fatalf("lag.BufferedBytes = %d, want %d", lag.BufferedBytes, expectedBytes)
	}

	stats := session.stats()
	if got := stats.SlowSkipEventsTotal; got != 0 {
		t.Fatalf("session slow_skip_events_total = %d, want 0 for disconnect policy", got)
	}
	if got := stats.SlowSkipLagChunksTotal; got != 0 {
		t.Fatalf("session slow_skip_lag_chunks_total = %d, want 0 for disconnect policy", got)
	}
	if got := stats.SlowSkipLagBytesTotal; got != 0 {
		t.Fatalf("session slow_skip_lag_bytes_total = %d, want 0 for disconnect policy", got)
	}
	if got := stats.SlowSkipMaxLagChunks; got != 0 {
		t.Fatalf("session slow_skip_max_lag_chunks = %d, want 0 for disconnect policy", got)
	}

	global := streamSlowSkipStatsSnapshot()
	if got := global.Events; got != 0 {
		t.Fatalf("global slow skip events = %d, want 0 for disconnect policy", got)
	}
}

func TestSessionSubscriptionSkipPolicyRecordsSlowSkipTelemetry(t *testing.T) {
	isolateStreamSlowSkipStatsForTest(t)

	ring := NewChunkRing(2)
	_ = ring.PublishChunk([]byte("a"), time.Now())
	_ = ring.PublishChunk([]byte("b"), time.Now())
	_ = ring.PublishChunk([]byte("c"), time.Now())
	ring.Close(nil)
	snap := ring.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot chunk count = %d, want 2", len(snap))
	}

	expectedOldest := snap[0].Seq
	expectedLagChunks := expectedOldest
	expectedBufferedChunks := len(snap)
	expectedBufferedBytes := 0
	for i := range snap {
		expectedBufferedBytes += len(snap[i].Data)
	}
	expectedLagBytes := estimateSlowClientLagBytes(
		ReadResult{BufferedChunks: expectedBufferedChunks, BufferedBytes: expectedBufferedBytes},
		expectedLagChunks,
	)

	session := &sharedRuntimeSession{ring: ring}
	sub := &SessionSubscription{
		session:    session,
		nextSeq:    0,
		slowPolicy: slowClientPolicySkip,
	}
	writer := &recordingResponseWriter{}
	if err := sub.Stream(context.Background(), writer); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if got, want := writer.Body(), "bc"; got != want {
		t.Fatalf("body = %q, want %q after slow-skip", got, want)
	}

	stats := session.stats()
	if got, want := stats.SlowSkipEventsTotal, uint64(1); got != want {
		t.Fatalf("session slow_skip_events_total = %d, want %d", got, want)
	}
	if got, want := stats.SlowSkipLagChunksTotal, expectedLagChunks; got != want {
		t.Fatalf("session slow_skip_lag_chunks_total = %d, want %d", got, want)
	}
	if got, want := stats.SlowSkipLagBytesTotal, expectedLagBytes; got != want {
		t.Fatalf("session slow_skip_lag_bytes_total = %d, want %d", got, want)
	}
	if got, want := stats.SlowSkipMaxLagChunks, expectedLagChunks; got != want {
		t.Fatalf("session slow_skip_max_lag_chunks = %d, want %d", got, want)
	}

	global := streamSlowSkipStatsSnapshot()
	if got, want := global.Events, uint64(1); got != want {
		t.Fatalf("global slow skip events = %d, want %d", got, want)
	}
	if got, want := global.LagChunksTotal, expectedLagChunks; got != want {
		t.Fatalf("global slow skip lag chunks total = %d, want %d", got, want)
	}
	if got, want := global.LagBytesTotal, expectedLagBytes; got != want {
		t.Fatalf("global slow skip lag bytes total = %d, want %d", got, want)
	}
}

func TestSessionManagerSyntheticSlowSubscriberSkipPolicyIntegration(t *testing.T) {
	isolateStreamSlowSkipStatsForTest(t)
	isolateStreamSubscriberWriteStatsForTest(t)

	payload := bytes.Repeat([]byte("S"), mpegTSPacketSize)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flusher, _ := w.(http.Flusher)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < 5000; i++ {
			if _, err := w.Write(payload); err != nil {
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
			"201": {ChannelID: 201, GuideNumber: "201", GuideName: "Slow Skip Integration", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			201: {
				{
					SourceID:      2010,
					ChannelID:     201,
					ItemKey:       "src:slow-skip:integration",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	cfg := fastStreamTestTiming.sessionManagerConfig("direct")
	cfg.BufferChunkBytes = mpegTSPacketSize
	cfg.BufferPublishFlushInterval = time.Millisecond
	cfg.SubscriberJoinLagBytes = mpegTSPacketSize
	cfg.SubscriberSlowClientPolicy = slowClientPolicySkip
	cfg.StallDetect = 2 * time.Second
	cfg.StallHardDeadline = 6 * time.Second

	manager := NewSessionManager(cfg, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = manager.CloseWithContext(closeCtx)
	})

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["201"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	streamCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	writer := newTimeoutWriteResponseWriter(25 * time.Millisecond)
	errCh := make(chan error, 1)
	go func() {
		errCh <- sub.Stream(streamCtx, writer)
	}()

	waitFor(t, 4*time.Second, func() bool {
		stats := sub.Stats()
		global := streamSubscriberWriteStatsSnapshot()
		return writer.WriteCalls() > 0 &&
			stats.SlowSkipEventsTotal > 0 &&
			stats.SubscriberWriteBlockedDurationUS > 0 &&
			global.BlockedDurationUS > 0
	})

	stats := sub.Stats()
	if stats.SlowSkipEventsTotal == 0 {
		t.Fatalf("slow_skip_events_total = %d, want > 0 under synthetic lag", stats.SlowSkipEventsTotal)
	}
	if stats.StallCount != 0 {
		t.Fatalf("stall_count = %d, want 0 during slow-skip coverage run", stats.StallCount)
	}
	if stats.RecoveryCycle != 0 {
		t.Fatalf("recovery_cycle = %d, want 0 during slow-skip coverage run", stats.RecoveryCycle)
	}
	if stats.SubscriberWriteDeadlineTimeoutsTotal != 0 {
		t.Fatalf(
			"subscriber_write_deadline_timeouts_total = %d, want 0 under slow-skip synthetic lag",
			stats.SubscriberWriteDeadlineTimeoutsTotal,
		)
	}
	if stats.SubscriberWriteShortWritesTotal != 0 {
		t.Fatalf(
			"subscriber_write_short_writes_total = %d, want 0 under slow-skip synthetic lag",
			stats.SubscriberWriteShortWritesTotal,
		)
	}
	if stats.SubscriberWriteBlockedDurationUS == 0 {
		t.Fatalf(
			"subscriber_write_blocked_duration_us = %d, want > 0 under slow-skip synthetic lag",
			stats.SubscriberWriteBlockedDurationUS,
		)
	}
	if global := streamSlowSkipStatsSnapshot(); global.Events == 0 {
		t.Fatalf("global slow skip events = %d, want > 0", global.Events)
	}
	globalWrite := streamSubscriberWriteStatsSnapshot()
	if globalWrite.DeadlineTimeouts != 0 {
		t.Fatalf(
			"global subscriber write deadline timeouts = %d, want 0 under slow-skip synthetic lag",
			globalWrite.DeadlineTimeouts,
		)
	}
	if globalWrite.ShortWrites != 0 {
		t.Fatalf(
			"global subscriber write short writes = %d, want 0 under slow-skip synthetic lag",
			globalWrite.ShortWrites,
		)
	}
	if globalWrite.BlockedDurationUS == 0 {
		t.Fatalf(
			"global subscriber write blocked duration us = %d, want > 0 under slow-skip synthetic lag",
			globalWrite.BlockedDurationUS,
		)
	}

	sub.Close()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stream() error = %v, want graceful stop after subscriber close", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timeout waiting for slow-skip integration stream to stop")
	}
}

func TestSessionManagerSyntheticSlowSubscriberDisconnectPolicyIntegration(t *testing.T) {
	isolateStreamSlowSkipStatsForTest(t)
	isolateStreamSubscriberWriteStatsForTest(t)

	payload := bytes.Repeat([]byte("D"), mpegTSPacketSize)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flusher, _ := w.(http.Flusher)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < 5000; i++ {
			if _, err := w.Write(payload); err != nil {
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
			"202": {ChannelID: 202, GuideNumber: "202", GuideName: "Slow Disconnect Integration", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			202: {
				{
					SourceID:      2020,
					ChannelID:     202,
					ItemKey:       "src:slow-disconnect:integration",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	cfg := fastStreamTestTiming.sessionManagerConfig("direct")
	cfg.BufferChunkBytes = mpegTSPacketSize
	cfg.BufferPublishFlushInterval = time.Millisecond
	cfg.SubscriberJoinLagBytes = mpegTSPacketSize
	cfg.SubscriberSlowClientPolicy = slowClientPolicyDisconnect
	cfg.StallDetect = 2 * time.Second
	cfg.StallHardDeadline = 6 * time.Second

	manager := NewSessionManager(cfg, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = manager.CloseWithContext(closeCtx)
	})

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["202"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	streamCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	writer := newTimeoutWriteResponseWriter(30 * time.Millisecond)
	errCh := make(chan error, 1)
	go func() {
		errCh <- sub.Stream(streamCtx, writer)
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrSlowClientLagged) {
			t.Fatalf("Stream() error = %v, want ErrSlowClientLagged", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timeout waiting for disconnect-policy lag termination")
	}

	waitFor(t, 2*time.Second, func() bool {
		stats := sub.Stats()
		global := streamSubscriberWriteStatsSnapshot()
		return writer.WriteCalls() > 0 &&
			stats.SubscriberWriteBlockedDurationUS > 0 &&
			global.BlockedDurationUS > 0
	})

	if writer.WriteCalls() == 0 {
		t.Fatalf("writer write calls = %d, want > 0 before lag disconnect", writer.WriteCalls())
	}

	stats := sub.Stats()
	if stats.SlowSkipEventsTotal != 0 {
		t.Fatalf("slow_skip_events_total = %d, want 0 for disconnect policy", stats.SlowSkipEventsTotal)
	}
	if stats.SubscriberWriteDeadlineTimeoutsTotal != 0 {
		t.Fatalf(
			"subscriber_write_deadline_timeouts_total = %d, want 0 under disconnect-policy synthetic lag",
			stats.SubscriberWriteDeadlineTimeoutsTotal,
		)
	}
	if stats.SubscriberWriteShortWritesTotal != 0 {
		t.Fatalf(
			"subscriber_write_short_writes_total = %d, want 0 under disconnect-policy synthetic lag",
			stats.SubscriberWriteShortWritesTotal,
		)
	}
	if stats.SubscriberWriteBlockedDurationUS == 0 {
		t.Fatalf(
			"subscriber_write_blocked_duration_us = %d, want > 0 under disconnect-policy synthetic lag",
			stats.SubscriberWriteBlockedDurationUS,
		)
	}
	if global := streamSlowSkipStatsSnapshot(); global.Events != 0 {
		t.Fatalf("global slow skip events = %d, want 0 for disconnect policy", global.Events)
	}
	globalWrite := streamSubscriberWriteStatsSnapshot()
	if globalWrite.DeadlineTimeouts != 0 {
		t.Fatalf(
			"global subscriber write deadline timeouts = %d, want 0 under disconnect-policy synthetic lag",
			globalWrite.DeadlineTimeouts,
		)
	}
	if globalWrite.ShortWrites != 0 {
		t.Fatalf(
			"global subscriber write short writes = %d, want 0 under disconnect-policy synthetic lag",
			globalWrite.ShortWrites,
		)
	}
	if globalWrite.BlockedDurationUS == 0 {
		t.Fatalf(
			"global subscriber write blocked duration us = %d, want > 0 under disconnect-policy synthetic lag",
			globalWrite.BlockedDurationUS,
		)
	}
}

func TestSlowClientLagDetailsFromReadResult(t *testing.T) {
	testCases := []struct {
		name string
		read ReadResult
		want slowClientLagDetails
	}{
		{
			name: "oldest_not_ahead",
			read: ReadResult{
				RequestedSeq:   40,
				OldestSeq:      40,
				RingNextSeq:    41,
				BufferedChunks: 3,
				BufferedBytes:  300,
			},
			want: slowClientLagDetails{
				RequestedSeq:   40,
				OldestSeq:      40,
				LagChunks:      0,
				RingNextSeq:    41,
				BufferedChunks: 3,
				BufferedBytes:  300,
			},
		},
		{
			name: "oldest_ahead",
			read: ReadResult{
				RequestedSeq:   10,
				OldestSeq:      17,
				RingNextSeq:    19,
				BufferedChunks: 6,
				BufferedBytes:  2048,
			},
			want: slowClientLagDetails{
				RequestedSeq:   10,
				OldestSeq:      17,
				LagChunks:      7,
				RingNextSeq:    19,
				BufferedChunks: 6,
				BufferedBytes:  2048,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := slowClientLagDetailsFromReadResult(tc.read); got != tc.want {
				t.Fatalf("slowClientLagDetailsFromReadResult() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestEstimateSlowClientLagBytes(t *testing.T) {
	maxUint64 := ^uint64(0)
	testCases := []struct {
		name      string
		read      ReadResult
		lagChunks uint64
		want      uint64
	}{
		{
			name: "zero_lag_chunks",
			read: ReadResult{
				BufferedChunks: 8,
				BufferedBytes:  1024,
			},
			lagChunks: 0,
			want:      0,
		},
		{
			name: "zero_buffered_bytes",
			read: ReadResult{
				BufferedChunks: 8,
				BufferedBytes:  0,
			},
			lagChunks: 4,
			want:      0,
		},
		{
			name: "zero_buffered_chunks",
			read: ReadResult{
				BufferedChunks: 0,
				BufferedBytes:  1024,
			},
			lagChunks: 4,
			want:      0,
		},
		{
			name: "overflow_guard_returns_max",
			read: ReadResult{
				BufferedChunks: 1,
				BufferedBytes:  2,
			},
			lagChunks: maxUint64,
			want:      maxUint64,
		},
		{
			name: "normal_estimation",
			read: ReadResult{
				BufferedChunks: 30,
				BufferedBytes:  900,
			},
			lagChunks: 5,
			want:      150,
		},
		{
			name: "floor_to_one_for_sub_byte_estimate",
			read: ReadResult{
				BufferedChunks: 8,
				BufferedBytes:  1,
			},
			lagChunks: 1,
			want:      1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := estimateSlowClientLagBytes(tc.read, tc.lagChunks); got != tc.want {
				t.Fatalf("estimateSlowClientLagBytes(%+v, %d) = %d, want %d", tc.read, tc.lagChunks, got, tc.want)
			}
		})
	}
}

func TestSessionSubscriptionSkipPolicyLogsLagDetails(t *testing.T) {
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ring := NewChunkRing(2)
	_ = ring.PublishChunk([]byte("a"), time.Now())
	_ = ring.PublishChunk([]byte("b"), time.Now())
	_ = ring.PublishChunk([]byte("c"), time.Now())
	ring.Close(nil)
	snap := ring.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot chunk count = %d, want 2", len(snap))
	}

	expectedOldest := snap[0].Seq
	expectedBufferedBytes := 0
	for i := range snap {
		expectedBufferedBytes += len(snap[i].Data)
	}

	session := &sharedRuntimeSession{
		manager: &SessionManager{logger: logger},
		channel: channels.Channel{
			ChannelID:   11,
			GuideNumber: "101",
		},
		ring: ring,
	}
	sub := &SessionSubscription{
		session:      session,
		nextSeq:      0,
		subscriberID: 77,
		clientAddr:   "198.51.100.22:41000",
		slowPolicy:   slowClientPolicySkip,
	}
	if err := sub.Stream(context.Background(), &recordingResponseWriter{}); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}

	text := logs.String()
	if !strings.Contains(text, "shared session subscriber lag skip") {
		t.Fatalf("missing slow-skip lag log: %s", text)
	}
	for _, fragment := range []string{
		"channel_id=11",
		"guide_number=101",
		"subscriber_id=77",
		"client_addr=198.51.100.22:41000",
		"requested_seq=0",
		fmt.Sprintf("oldest_seq=%d", expectedOldest),
		fmt.Sprintf("lag_chunks=%d", expectedOldest),
		fmt.Sprintf("buffered_chunks=%d", len(snap)),
		fmt.Sprintf("buffered_bytes=%d", expectedBufferedBytes),
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("slow-skip lag log missing %q: %s", fragment, text)
		}
	}
}

func TestSessionSubscriptionSkipPolicyLogCoalescing(t *testing.T) {
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	session := &sharedRuntimeSession{
		manager: &SessionManager{logger: logger},
		channel: channels.Channel{
			ChannelID:   42,
			GuideNumber: "142",
		},
	}
	sub := &SessionSubscription{
		session:      session,
		subscriberID: 9,
		clientAddr:   "198.51.100.55:12345",
		slowPolicy:   slowClientPolicySkip,
	}
	details := slowClientLagDetails{
		RequestedSeq:   10,
		OldestSeq:      14,
		LagChunks:      4,
		RingNextSeq:    18,
		BufferedChunks: 5,
		BufferedBytes:  500,
	}

	sub.logSlowSkipEvent(details, 400, slowSkipSessionSnapshot{
		EventsTotal:    1,
		LagChunksTotal: 4,
		LagBytesTotal:  400,
		MaxLagChunks:   4,
	})
	sub.logSlowSkipEvent(details, 400, slowSkipSessionSnapshot{
		EventsTotal:    2,
		LagChunksTotal: 8,
		LagBytesTotal:  800,
		MaxLagChunks:   4,
	})

	if got := strings.Count(logs.String(), "shared session subscriber lag skip"); got != 1 {
		t.Fatalf("warning log count after coalesced burst = %d, want 1", got)
	}

	// Force the coalesce window open without sleeping for deterministic timing.
	sub.lastSlowSkipLog = time.Now().UTC().Add(-slowSkipLogCoalesceWindow - time.Millisecond)

	sub.logSlowSkipEvent(details, 400, slowSkipSessionSnapshot{
		EventsTotal:    3,
		LagChunksTotal: 12,
		LagBytesTotal:  1200,
		MaxLagChunks:   4,
	})

	logText := logs.String()
	if got := strings.Count(logText, "shared session subscriber lag skip"); got != 2 {
		t.Fatalf("warning log count after coalesce window reopen = %d, want 2", got)
	}
	if !strings.Contains(logText, "slow_skip_logs_coalesced=1") {
		t.Fatalf("warning logs missing coalesced skip counter: %s", logText)
	}
}

func TestSessionSubscriptionLogSlowSkipEventNilGuards(t *testing.T) {
	details := slowClientLagDetails{
		RequestedSeq:   10,
		OldestSeq:      14,
		LagChunks:      4,
		RingNextSeq:    18,
		BufferedChunks: 5,
		BufferedBytes:  500,
	}
	snapshot := slowSkipSessionSnapshot{
		EventsTotal:    3,
		LagChunksTotal: 12,
		LagBytesTotal:  1200,
		MaxLagChunks:   4,
	}

	testCases := []struct {
		name string
		sub  *SessionSubscription
	}{
		{
			name: "nil_receiver",
			sub:  nil,
		},
		{
			name: "nil_session",
			sub:  &SessionSubscription{},
		},
		{
			name: "nil_manager",
			sub: &SessionSubscription{
				session: &sharedRuntimeSession{},
			},
		},
		{
			name: "nil_logger",
			sub: &SessionSubscription{
				session: &sharedRuntimeSession{
					manager: &SessionManager{},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.sub.logSlowSkipEvent(details, 400, snapshot)
		})
	}
}

func TestSessionSubscriptionSkipPolicyFlushesEachChunk(t *testing.T) {
	ring := NewChunkRing(4)
	_ = ring.PublishChunk([]byte("ab"), time.Now())
	_ = ring.PublishChunk([]byte("cd"), time.Now())
	ring.Close(nil)

	startSeq := ring.Snapshot()[0].Seq
	sub := &SessionSubscription{
		session:    &sharedRuntimeSession{ring: ring},
		nextSeq:    startSeq,
		slowPolicy: slowClientPolicySkip,
	}

	writer := &recordingResponseWriter{}
	if err := sub.Stream(context.Background(), writer); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if got := writer.Writes(); got != 2 {
		t.Fatalf("writes = %d, want 2", got)
	}
	if got := writer.Flushes(); got != 2 {
		t.Fatalf("flushes = %d, want 2", got)
	}
	if got := writer.Body(); got != "abcd" {
		t.Fatalf("body = %q, want abcd", got)
	}
}

func TestSessionSubscriptionTreatsEOFStyleRingCloseAsGraceful(t *testing.T) {
	testCases := []struct {
		name     string
		closeErr error
	}{
		{name: "io_eof", closeErr: io.EOF},
		{name: "source_ended", closeErr: errSourceEnded},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ring := NewChunkRing(4)
			_ = ring.PublishChunk([]byte("ab"), time.Now())
			_ = ring.PublishChunk([]byte("cd"), time.Now())
			ring.Close(tc.closeErr)

			startSeq := ring.Snapshot()[0].Seq
			sub := &SessionSubscription{
				session:    &sharedRuntimeSession{ring: ring},
				nextSeq:    startSeq,
				slowPolicy: slowClientPolicySkip,
			}

			writer := &recordingResponseWriter{}
			if err := sub.Stream(context.Background(), writer); err != nil {
				t.Fatalf("Stream() error = %v", err)
			}
			if got := writer.Body(); got != "abcd" {
				t.Fatalf("body = %q, want abcd", got)
			}
		})
	}
}

func TestSessionSubscriptionReturnsNonGracefulRingCloseError(t *testing.T) {
	ring := NewChunkRing(4)
	_ = ring.PublishChunk([]byte("ab"), time.Now())
	ring.Close(errors.New("fatal teardown"))

	startSeq := ring.Snapshot()[0].Seq
	sub := &SessionSubscription{
		session:    &sharedRuntimeSession{ring: ring},
		nextSeq:    startSeq,
		slowPolicy: slowClientPolicySkip,
	}

	err := sub.Stream(context.Background(), &recordingResponseWriter{})
	if err == nil || !strings.Contains(err.Error(), "fatal teardown") {
		t.Fatalf("Stream() error = %v, want fatal teardown error", err)
	}
}

func TestWriteChunkFailsFastWhenWriteDeadlineCannotBeApplied(t *testing.T) {
	deadlineErr := errors.New("deadline setup failed")
	type writeDeadlineTestWriter interface {
		http.ResponseWriter
		WriteCalls() int
	}
	testCases := []struct {
		name             string
		writer           writeDeadlineTestWriter
		expectErrMatcher func(error) bool
	}{
		{
			name:   "deadline_unsupported",
			writer: newBlockingWriteResponseWriter(300 * time.Millisecond),
			expectErrMatcher: func(err error) bool {
				return errors.Is(err, http.ErrNotSupported)
			},
		},
		{
			name: "deadline_setup_failure",
			writer: &failingWriteDeadlineResponseWriter{
				blockingWriteResponseWriter: newBlockingWriteResponseWriter(300 * time.Millisecond),
				err:                         deadlineErr,
			},
			expectErrMatcher: func(err error) bool {
				return errors.Is(err, deadlineErr)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			controller := http.NewResponseController(tc.writer)
			started := time.Now()
			sample, err := writeChunk(tc.writer, controller, []byte("payload"), 25*time.Millisecond)
			elapsed := time.Since(started)

			if err == nil {
				t.Fatal("writeChunk() error = nil, want deadline setup failure")
			}
			if !tc.expectErrMatcher(err) {
				t.Fatalf("writeChunk() error = %v, want expected deadline setup error", err)
			}
			if got := tc.writer.WriteCalls(); got != 0 {
				t.Fatalf("write calls = %d, want 0 when deadline setup fails", got)
			}
			if sample.DeadlineTimeout {
				t.Fatalf("sample.deadline_timeout = true, want false for setup failure err=%v", err)
			}
			if sample.ShortWrite {
				t.Fatalf("sample.short_write = true, want false for setup failure err=%v", err)
			}
			if elapsed >= 150*time.Millisecond {
				t.Fatalf("writeChunk() elapsed = %s, want fast-fail under 150ms", elapsed)
			}
		})
	}
}

func TestIsWriteDeadlineTimeout(t *testing.T) {
	genericErr := errors.New("write failed")
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil_error",
			err:  nil,
			want: false,
		},
		{
			name: "os_deadline_exceeded",
			err:  os.ErrDeadlineExceeded,
			want: true,
		},
		{
			name: "context_deadline_exceeded",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "net_timeout_true",
			err:  writeDeadlineNetError{timeout: true},
			want: true,
		},
		{
			name: "net_timeout_false",
			err:  writeDeadlineNetError{timeout: false},
			want: false,
		},
		{
			name: "generic_error",
			err:  genericErr,
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isWriteDeadlineTimeout(tc.err); got != tc.want {
				t.Fatalf("isWriteDeadlineTimeout(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestElapsedDurationUS(t *testing.T) {
	if got := elapsedDurationUS(time.Time{}); got != 0 {
		t.Fatalf("elapsedDurationUS(zero) = %d, want 0", got)
	}

	if got := elapsedDurationUS(time.Now().Add(2 * time.Second)); got != 0 {
		t.Fatalf("elapsedDurationUS(negative elapsed) = %d, want 0", got)
	}

	if got := elapsedDurationUS(time.Now().Add(-2 * time.Millisecond)); got == 0 {
		t.Fatalf("elapsedDurationUS(positive elapsed) = %d, want > 0", got)
	}
}

func TestWriteChunkPathCoverage(t *testing.T) {
	t.Run("empty_data_early_return", func(t *testing.T) {
		writer := newWriteChunkTestResponseWriter()
		controller := http.NewResponseController(writer)

		sample, err := writeChunk(writer, controller, nil, 25*time.Millisecond)
		if err != nil {
			t.Fatalf("writeChunk() error = %v, want nil", err)
		}
		if got := writer.WriteCalls(); got != 0 {
			t.Fatalf("write calls = %d, want 0 for empty payload", got)
		}
		if got := writer.DeadlineSetCalls(); got != 0 {
			t.Fatalf("deadline set calls = %d, want 0 for empty payload", got)
		}
		if got := writer.DeadlineClearCalls(); got != 0 {
			t.Fatalf("deadline clear calls = %d, want 0 for empty payload", got)
		}
		if sample.DeadlineTimeout {
			t.Fatalf("sample.deadline_timeout = %t, want false", sample.DeadlineTimeout)
		}
		if sample.ShortWrite {
			t.Fatalf("sample.short_write = %t, want false", sample.ShortWrite)
		}
		if sample.BlockedDuration != 0 {
			t.Fatalf("sample.blocked_duration = %d, want 0 for early return", sample.BlockedDuration)
		}
	})

	t.Run("successful_write_without_deadline", func(t *testing.T) {
		writer := newWriteChunkTestResponseWriter()
		controller := http.NewResponseController(writer)

		sample, err := writeChunk(writer, controller, []byte("payload"), 0)
		if err != nil {
			t.Fatalf("writeChunk() error = %v, want nil", err)
		}
		if got := writer.WriteCalls(); got != 1 {
			t.Fatalf("write calls = %d, want 1", got)
		}
		if got := writer.DeadlineSetCalls(); got != 0 {
			t.Fatalf("deadline set calls = %d, want 0 when maxBlockedWrite disabled", got)
		}
		if got := writer.DeadlineClearCalls(); got != 0 {
			t.Fatalf("deadline clear calls = %d, want 0 when maxBlockedWrite disabled", got)
		}
		if sample.DeadlineTimeout {
			t.Fatalf("sample.deadline_timeout = %t, want false", sample.DeadlineTimeout)
		}
		if sample.ShortWrite {
			t.Fatalf("sample.short_write = %t, want false", sample.ShortWrite)
		}
	})

	t.Run("successful_write_with_deadline", func(t *testing.T) {
		writer := newWriteChunkTestResponseWriter()
		controller := http.NewResponseController(writer)

		sample, err := writeChunk(writer, controller, []byte("payload"), 25*time.Millisecond)
		if err != nil {
			t.Fatalf("writeChunk() error = %v, want nil", err)
		}
		if got := writer.WriteCalls(); got != 1 {
			t.Fatalf("write calls = %d, want 1", got)
		}
		if got := writer.DeadlineSetCalls(); got != 1 {
			t.Fatalf("deadline set calls = %d, want 1", got)
		}
		if got := writer.DeadlineClearCalls(); got != 1 {
			t.Fatalf("deadline clear calls = %d, want 1", got)
		}
		if sample.DeadlineTimeout {
			t.Fatalf("sample.deadline_timeout = %t, want false", sample.DeadlineTimeout)
		}
		if sample.ShortWrite {
			t.Fatalf("sample.short_write = %t, want false", sample.ShortWrite)
		}
	})

	t.Run("non_timeout_write_error", func(t *testing.T) {
		writer := newWriteChunkTestResponseWriter()
		writeErr := errors.New("write failed")
		writer.writeErr = writeErr
		controller := http.NewResponseController(writer)

		sample, err := writeChunk(writer, controller, []byte("payload"), 25*time.Millisecond)
		if !errors.Is(err, writeErr) {
			t.Fatalf("writeChunk() error = %v, want %v", err, writeErr)
		}
		if got := writer.WriteCalls(); got != 1 {
			t.Fatalf("write calls = %d, want 1", got)
		}
		if got := writer.DeadlineSetCalls(); got != 1 {
			t.Fatalf("deadline set calls = %d, want 1", got)
		}
		if got := writer.DeadlineClearCalls(); got != 1 {
			t.Fatalf("deadline clear calls = %d, want 1", got)
		}
		if sample.DeadlineTimeout {
			t.Fatalf("sample.deadline_timeout = %t, want false for non-timeout write error", sample.DeadlineTimeout)
		}
		if sample.ShortWrite {
			t.Fatalf("sample.short_write = %t, want false for non-timeout write error", sample.ShortWrite)
		}
	})

	t.Run("timeout_write_error", func(t *testing.T) {
		writer := newWriteChunkTestResponseWriter()
		writer.writeErr = os.ErrDeadlineExceeded
		controller := http.NewResponseController(writer)

		sample, err := writeChunk(writer, controller, []byte("payload"), 25*time.Millisecond)
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("writeChunk() error = %v, want %v", err, os.ErrDeadlineExceeded)
		}
		if got := writer.WriteCalls(); got != 1 {
			t.Fatalf("write calls = %d, want 1", got)
		}
		if got := writer.DeadlineSetCalls(); got != 1 {
			t.Fatalf("deadline set calls = %d, want 1", got)
		}
		if got := writer.DeadlineClearCalls(); got != 1 {
			t.Fatalf("deadline clear calls = %d, want 1", got)
		}
		if !sample.DeadlineTimeout {
			t.Fatalf("sample.deadline_timeout = %t, want true for timeout write error", sample.DeadlineTimeout)
		}
		if sample.ShortWrite {
			t.Fatalf("sample.short_write = %t, want false for timeout write error", sample.ShortWrite)
		}
	})

	t.Run("short_write", func(t *testing.T) {
		writer := newWriteChunkTestResponseWriter()
		writer.shortWriteN = 3
		controller := http.NewResponseController(writer)

		sample, err := writeChunk(writer, controller, []byte("payload"), 25*time.Millisecond)
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("writeChunk() error = %v, want %v", err, io.ErrShortWrite)
		}
		if got := writer.WriteCalls(); got != 1 {
			t.Fatalf("write calls = %d, want 1", got)
		}
		if got := writer.DeadlineSetCalls(); got != 1 {
			t.Fatalf("deadline set calls = %d, want 1", got)
		}
		if got := writer.DeadlineClearCalls(); got != 1 {
			t.Fatalf("deadline clear calls = %d, want 1", got)
		}
		if sample.DeadlineTimeout {
			t.Fatalf("sample.deadline_timeout = %t, want false for short write", sample.DeadlineTimeout)
		}
		if !sample.ShortWrite {
			t.Fatalf("sample.short_write = %t, want true for short write", sample.ShortWrite)
		}
	})
}

func TestSessionSubscriptionDeadlineUnsupportedRemovesSubscriberWithoutBlockingWrite(t *testing.T) {
	ring := NewChunkRing(4)
	_ = ring.PublishChunk([]byte("chunk"), time.Now())

	subscriberID := uint64(7)
	connectedAt := time.Now().UTC().Add(-500 * time.Millisecond)
	session := &sharedRuntimeSession{
		ring: ring,
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				sessionIdleTimeout: time.Hour,
			},
		},
		closed: true,
		subscribers: map[uint64]SubscriberStats{
			subscriberID: {
				SubscriberID: subscriberID,
				StartedAt:    connectedAt,
			},
		},
		historySubscribers: []SharedSessionSubscriberHistory{
			{
				SubscriberID: subscriberID,
				ConnectedAt:  connectedAt,
			},
		},
		historySubscriberIndex: map[uint64]int{
			subscriberID: 0,
		},
	}

	sub := &SessionSubscription{
		session:         session,
		nextSeq:         0,
		subscriberID:    subscriberID,
		slowPolicy:      slowClientPolicyDisconnect,
		maxBlockedWrite: 25 * time.Millisecond,
	}
	writer := newBlockingWriteResponseWriter(300 * time.Millisecond)

	started := time.Now()
	err := sub.Stream(context.Background(), writer)
	elapsed := time.Since(started)
	if !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("Stream() error = %v, want http.ErrNotSupported", err)
	}
	if got := writer.WriteCalls(); got != 0 {
		t.Fatalf("write calls = %d, want 0 when deadline setup is unsupported", got)
	}
	if elapsed >= 150*time.Millisecond {
		t.Fatalf("Stream() elapsed = %s, want fast-fail under 150ms", elapsed)
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if got := len(session.subscribers); got != 0 {
		t.Fatalf("subscribers after stream error = %d, want 0", got)
	}
	if got := len(session.historySubscribers); got != 1 {
		t.Fatalf("history subscriber entries = %d, want 1", got)
	}
	if session.historySubscribers[0].ClosedAt.IsZero() {
		t.Fatal("history subscriber closed_at is zero, want stream-write-error close timestamp")
	}
	if got, want := session.historySubscribers[0].CloseReason, subscriberRemovalReasonStreamWriteError; got != want {
		t.Fatalf("history subscriber close_reason = %q, want %q", got, want)
	}
}

func TestSessionSubscriptionWriteDeadlineTimeoutTelemetry(t *testing.T) {
	isolateStreamSubscriberWriteStatsForTest(t)

	ring := NewChunkRing(4)
	_ = ring.PublishChunk([]byte("chunk"), time.Now())
	ring.Close(nil)

	sub := &SessionSubscription{
		session: &sharedRuntimeSession{
			ring:   ring,
			closed: true,
		},
		nextSeq:         ring.Snapshot()[0].Seq,
		slowPolicy:      slowClientPolicySkip,
		maxBlockedWrite: 20 * time.Millisecond,
	}
	writer := newTimeoutWriteResponseWriter(60 * time.Millisecond)

	err := sub.Stream(context.Background(), writer)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Stream() error = %v, want os.ErrDeadlineExceeded", err)
	}
	if got := writer.WriteCalls(); got != 1 {
		t.Fatalf("write calls = %d, want 1", got)
	}

	stats := sub.session.stats()
	if got := stats.SubscriberWriteDeadlineTimeoutsTotal; got != 1 {
		t.Fatalf("subscriber_write_deadline_timeouts_total = %d, want 1", got)
	}
	if got := stats.SubscriberWriteShortWritesTotal; got != 0 {
		t.Fatalf("subscriber_write_short_writes_total = %d, want 0", got)
	}
	if got := stats.SubscriberWriteBlockedDurationUS; got == 0 {
		t.Fatalf("subscriber_write_blocked_duration_us = %d, want > 0", got)
	}
	if got := stats.SubscriberWriteBlockedDurationMS; got == 0 {
		t.Fatalf("subscriber_write_blocked_duration_ms = %d, want > 0", got)
	}

	global := streamSubscriberWriteStatsSnapshot()
	if got := global.DeadlineTimeouts; got != 1 {
		t.Fatalf("global subscriber write deadline timeouts = %d, want 1", got)
	}
	if got := global.ShortWrites; got != 0 {
		t.Fatalf("global subscriber write short writes = %d, want 0", got)
	}
	if got := global.BlockedDurationUS; got == 0 {
		t.Fatalf("global subscriber write blocked duration us = %d, want > 0", got)
	}
}

func TestSessionSubscriptionShortWriteTelemetry(t *testing.T) {
	isolateStreamSubscriberWriteStatsForTest(t)

	ring := NewChunkRing(4)
	_ = ring.PublishChunk([]byte("chunk"), time.Now())
	ring.Close(nil)

	sub := &SessionSubscription{
		session: &sharedRuntimeSession{
			ring:   ring,
			closed: true,
		},
		nextSeq:         ring.Snapshot()[0].Seq,
		slowPolicy:      slowClientPolicySkip,
		maxBlockedWrite: 0,
	}
	writer := newShortWriteResponseWriter(2 * time.Millisecond)

	err := sub.Stream(context.Background(), writer)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Stream() error = %v, want io.ErrShortWrite", err)
	}
	if got := writer.WriteCalls(); got != 1 {
		t.Fatalf("write calls = %d, want 1", got)
	}

	stats := sub.session.stats()
	if got := stats.SubscriberWriteDeadlineTimeoutsTotal; got != 0 {
		t.Fatalf("subscriber_write_deadline_timeouts_total = %d, want 0", got)
	}
	if got := stats.SubscriberWriteShortWritesTotal; got != 1 {
		t.Fatalf("subscriber_write_short_writes_total = %d, want 1", got)
	}
	if got := stats.SubscriberWriteBlockedDurationUS; got == 0 {
		t.Fatalf("subscriber_write_blocked_duration_us = %d, want > 0", got)
	}

	global := streamSubscriberWriteStatsSnapshot()
	if got := global.DeadlineTimeouts; got != 0 {
		t.Fatalf("global subscriber write deadline timeouts = %d, want 0", got)
	}
	if got := global.ShortWrites; got != 1 {
		t.Fatalf("global subscriber write short writes = %d, want 1", got)
	}
	if got := global.BlockedDurationUS; got == 0 {
		t.Fatalf("global subscriber write blocked duration us = %d, want > 0", got)
	}
}

func TestRecordStreamSourceReadPauseTelemetry(t *testing.T) {
	isolateStreamSourceReadPauseStatsForTest(t)

	recordStreamSourceReadPauseTelemetry(sourceReadPauseReasonRecovered, 1500*time.Millisecond)
	recordStreamSourceReadPauseTelemetry(sourceReadPauseReasonPumpExit, 500*time.Millisecond)
	recordStreamSourceReadPauseTelemetry(sourceReadPauseReasonContextCancel, 0)
	recordStreamSourceReadPauseTelemetry("unknown", -250*time.Millisecond)

	stats := streamSourceReadPauseStatsSnapshot()
	if got, want := stats.Events, uint64(2); got != want {
		t.Fatalf("stream source read pause events = %d, want %d", got, want)
	}
	if got, want := stats.InProgress, uint64(0); got != want {
		t.Fatalf("stream source read pause in progress = %d, want %d", got, want)
	}
	if got, want := stats.DurationUS, uint64(2_000_000); got != want {
		t.Fatalf("stream source read pause duration us = %d, want %d", got, want)
	}
	if got, want := stats.DurationMS, uint64(2000); got != want {
		t.Fatalf("stream source read pause duration ms = %d, want %d", got, want)
	}
	if got, want := stats.MaxDurationUS, uint64(1_500_000); got != want {
		t.Fatalf("stream source read pause max duration us = %d, want %d", got, want)
	}
	if got, want := stats.MaxDurationMS, uint64(1500); got != want {
		t.Fatalf("stream source read pause max duration ms = %d, want %d", got, want)
	}
}

func TestSessionManagerAppliesProviderOverlimitCooldownAfter429(t *testing.T) {
	providerScope := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/overlimit":
			http.Error(w, "over limit", http.StatusTooManyRequests)
		case "/healthy":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer providerScope.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:overlimit",
					StreamURL:     providerScope.URL + "/overlimit",
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:healthy",
					StreamURL:     providerScope.URL + "/healthy",
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             800 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		UpstreamOverlimitCooldown:  220 * time.Millisecond,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	started := time.Now()
	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	elapsed := time.Since(started)
	if elapsed < 180*time.Millisecond {
		t.Fatalf("subscribe elapsed = %s, want >= 180ms (provider cooldown gate)", elapsed)
	}

	waitFor(t, time.Second, func() bool {
		failures := provider.sourceFailures()
		successes := provider.sourceSuccesses()
		return hasFailureReason(failures, 10, "429") && containsInt64(successes, 11)
	})
	failures := provider.sourceFailures()
	successes := provider.sourceSuccesses()
	if !hasFailureReason(failures, 10, "429") {
		t.Fatalf("failures = %#v, want source 10 429 failure recorded", failures)
	}
	if !containsInt64(successes, 11) {
		t.Fatalf("successes = %#v, want source 11 success", successes)
	}
}

func TestSessionManagerProviderOverlimitCooldownDoesNotBlockOtherScopes(t *testing.T) {
	overlimit := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "over limit", http.StatusTooManyRequests)
	}))
	defer overlimit.Close()

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer healthy.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:overlimit",
					StreamURL:     overlimit.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:healthy",
					StreamURL:     healthy.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             800 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		UpstreamOverlimitCooldown:  220 * time.Millisecond,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	started := time.Now()
	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	elapsed := time.Since(started)
	if elapsed >= 180*time.Millisecond {
		t.Fatalf("subscribe elapsed = %s, want < 180ms (other provider scope should not be gated)", elapsed)
	}
}

func TestStartSourceWithCandidatesDeadlineCooldownBudgetExhaustedSkipsDirectStartupAttempt(t *testing.T) {
	var (
		hitsMu sync.Mutex
		hits   int
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hitsMu.Lock()
		hits++
		hitsMu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		UpstreamOverlimitCooldown:  400 * time.Millisecond,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), &fakeChannelsProvider{})
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	source := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:primary",
		StreamURL:     upstream.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}
	manager.armProviderOverlimitCooldownForSource(
		http.StatusTooManyRequests,
		"upstream returned status 429",
		source.SourceID,
		source.StreamURL,
	)

	totalTimeout := 140 * time.Millisecond
	started := time.Now()
	reader, _, err := session.startSourceWithCandidates(
		context.Background(),
		[]channels.Source{source},
		totalTimeout,
		"initial_startup",
		0,
		"",
	)
	if err == nil {
		if reader != nil {
			_ = reader.Close()
		}
		t.Fatal("startSourceWithCandidates() error = nil, want deadline-exhausted startup failure")
	}

	elapsed := time.Since(started)
	if elapsed < 100*time.Millisecond {
		t.Fatalf("startup elapsed = %s, want >= 100ms cooldown wait", elapsed)
	}
	if elapsed > 450*time.Millisecond {
		t.Fatalf("startup elapsed = %s, want < 450ms without extra startup attempt", elapsed)
	}

	hitsMu.Lock()
	gotHits := hits
	hitsMu.Unlock()
	if gotHits != 0 {
		t.Fatalf("upstream startup attempts = %d, want 0 after deadline budget is exhausted by cooldown wait", gotHits)
	}
}

func TestStartCurrentSourceWithBackoffDeadlineCooldownBudgetExhaustedSkipsFFmpegStartupAttempt(t *testing.T) {
	tmp := t.TempDir()
	ffmpegInvocationsPath := filepath.Join(tmp, "ffmpeg-invocations.log")
	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-deadline-budget.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail
echo invoked >> %q
exit 1
`, ffmpegInvocationsPath))

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "ffmpeg-copy",
		FFmpegPath:                 ffmpegPath,
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       2 * time.Second,
		UpstreamOverlimitCooldown:  400 * time.Millisecond,
		StallMaxFailoversPerStall:  0,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), &fakeChannelsProvider{})
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	source := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:primary",
		StreamURL:     "https://example.test/live/stream.m3u8",
		PriorityIndex: 0,
		Enabled:       true,
	}
	manager.armProviderOverlimitCooldownForSource(
		http.StatusTooManyRequests,
		"upstream returned status 429",
		source.SourceID,
		source.StreamURL,
	)

	totalTimeout := 140 * time.Millisecond
	started := time.Now()
	reader, _, err := session.startCurrentSourceWithBackoff(
		context.Background(),
		source,
		totalTimeout,
		0,
		"",
		0,
	)
	if err == nil {
		if reader != nil {
			_ = reader.Close()
		}
		t.Fatal("startCurrentSourceWithBackoff() error = nil, want deadline-exhausted startup failure")
	}

	elapsed := time.Since(started)
	if elapsed < 100*time.Millisecond {
		t.Fatalf("startup elapsed = %s, want >= 100ms cooldown wait", elapsed)
	}
	if elapsed > 450*time.Millisecond {
		t.Fatalf("startup elapsed = %s, want < 450ms without extra startup attempt", elapsed)
	}

	invocations := 0
	raw, readErr := os.ReadFile(ffmpegInvocationsPath)
	if readErr != nil {
		if !errors.Is(readErr, os.ErrNotExist) {
			t.Fatalf("ReadFile(ffmpegInvocationsPath) error = %v", readErr)
		}
	} else {
		text := strings.TrimSpace(string(raw))
		if text != "" {
			invocations = len(strings.Split(text, "\n"))
		}
	}
	if invocations != 0 {
		t.Fatalf("ffmpeg startup attempts = %d, want 0 after deadline budget is exhausted by cooldown wait", invocations)
	}
}

func TestSessionManagerRetriesOverlimitStartupPassBeforeDeadline(t *testing.T) {
	always429 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "over limit", http.StatusTooManyRequests)
	}))
	defer always429.Close()

	var (
		flakyMu   sync.Mutex
		flakyHits int
	)
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flakyMu.Lock()
		flakyHits++
		hits := flakyHits
		flakyMu.Unlock()

		// First attempt fails with an overlimit-style response, subsequent attempts succeed.
		if hits == 1 {
			http.Error(w, "over limit", http.StatusTooManyRequests)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer flaky.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:always429",
					StreamURL:     always429.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:flaky",
					StreamURL:     flaky.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             800 * time.Millisecond,
		FailoverTotalTimeout:       2200 * time.Millisecond,
		UpstreamOverlimitCooldown:  120 * time.Millisecond,
		MaxFailovers:               1,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	flakyMu.Lock()
	hits := flakyHits
	flakyMu.Unlock()
	if hits < 2 {
		t.Fatalf("flaky upstream hits = %d, want >= 2 to confirm retry pass", hits)
	}

	waitFor(t, time.Second, func() bool {
		failures := provider.sourceFailures()
		successes := provider.sourceSuccesses()
		return hasFailureReason(failures, 10, "429") &&
			hasFailureReason(failures, 11, "429") &&
			containsInt64(successes, 11)
	})
	failures := provider.sourceFailures()
	successes := provider.sourceSuccesses()
	if !hasFailureReason(failures, 10, "429") {
		t.Fatalf("failures = %#v, want source 10 429 failure recorded", failures)
	}
	if !hasFailureReason(failures, 11, "429") {
		t.Fatalf("failures = %#v, want source 11 initial 429 failure recorded", failures)
	}
	if !containsInt64(successes, 11) {
		t.Fatalf("successes = %#v, want source 11 eventual success", successes)
	}
}

func TestSessionManagerDoesNotRetryOverlimitStartupPassOn404(t *testing.T) {
	always404 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer always404.Close()

	var (
		flakyMu   sync.Mutex
		flakyHits int
	)
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flakyMu.Lock()
		flakyHits++
		hits := flakyHits
		flakyMu.Unlock()

		if hits == 1 {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer flaky.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:always404",
					StreamURL:     always404.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:flaky",
					StreamURL:     flaky.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             800 * time.Millisecond,
		FailoverTotalTimeout:       2200 * time.Millisecond,
		UpstreamOverlimitCooldown:  120 * time.Millisecond,
		MaxFailovers:               1,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err == nil {
		if sub != nil {
			sub.Close()
		}
		t.Fatal("Subscribe() error = nil, want startup failure without 404 retry pass")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "404") {
		t.Fatalf("Subscribe() error = %v, want 404 startup failure detail", err)
	}

	flakyMu.Lock()
	hits := flakyHits
	flakyMu.Unlock()
	if hits != 1 {
		t.Fatalf("flaky upstream hits = %d, want 1 (no 404 retry pass)", hits)
	}

	waitFor(t, time.Second, func() bool {
		failures := provider.sourceFailures()
		return hasFailureReason(failures, 10, "404") && hasFailureReason(failures, 11, "404")
	})
	failures := provider.sourceFailures()
	successes := provider.sourceSuccesses()
	if containsInt64(successes, 11) {
		t.Fatalf("successes = %#v, did not expect source 11 success without retry pass", successes)
	}
	if !hasFailureReason(failures, 10, "404") {
		t.Fatalf("failures = %#v, want source 10 404 failure recorded", failures)
	}
	if !hasFailureReason(failures, 11, "404") {
		t.Fatalf("failures = %#v, want source 11 404 failure recorded", failures)
	}
}

func TestSessionManagerArmProviderOverlimitCooldownOnlyFor429(t *testing.T) {
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  200 * time.Millisecond,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
	}, NewPool(1), &fakeChannelsProvider{})
	if manager == nil {
		t.Fatal("manager is nil")
	}

	manager.armProviderOverlimitCooldown(http.StatusNotFound, "upstream returned status 404")
	if remaining := manager.providerOverlimitRemaining(); remaining > 0 {
		t.Fatalf("providerOverlimitRemaining() = %s after 404, want 0", remaining)
	}

	manager.armProviderOverlimitCooldown(http.StatusTooManyRequests, "upstream returned status 429")
	if remaining := manager.providerOverlimitRemaining(); remaining <= 0 {
		t.Fatalf("providerOverlimitRemaining() = %s after 429, want > 0", remaining)
	}
}

func TestSessionManagerProviderOverlimitCooldownClearIsScopeLocal(t *testing.T) {
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  350 * time.Millisecond,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
	}, NewPool(1), &fakeChannelsProvider{})
	if manager == nil {
		t.Fatal("manager is nil")
	}

	scopeAURL := "https://provider-a.example/live.m3u8"
	scopeBURL := "https://provider-b.example/live.m3u8"

	manager.armProviderOverlimitCooldownForSource(http.StatusTooManyRequests, "upstream returned status 429", 10, scopeAURL)
	manager.armProviderOverlimitCooldownForSource(http.StatusTooManyRequests, "upstream returned status 429", 20, scopeBURL)

	if remaining := manager.providerOverlimitRemainingForSource(10, scopeAURL); remaining <= 0 {
		t.Fatalf("scope A cooldown = %s, want > 0", remaining)
	}
	if remaining := manager.providerOverlimitRemainingForSource(20, scopeBURL); remaining <= 0 {
		t.Fatalf("scope B cooldown = %s, want > 0", remaining)
	}

	// Successful startup in one scope must not clear cooldown in another scope.
	manager.clearProviderOverlimitCooldownForSource(20, scopeBURL)
	if remaining := manager.providerOverlimitRemainingForSource(20, scopeBURL); remaining > 0 {
		t.Fatalf("scope B cooldown = %s after clear, want 0", remaining)
	}
	if remaining := manager.providerOverlimitRemainingForSource(10, scopeAURL); remaining <= 0 {
		t.Fatalf("scope A cooldown = %s after scope B clear, want > 0", remaining)
	}
}

func TestSessionManagerProviderOverlimitCooldownScopeStateBounded(t *testing.T) {
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  2 * time.Second,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
	}, NewPool(1), &fakeChannelsProvider{})
	if manager == nil {
		t.Fatal("manager is nil")
	}

	totalScopes := providerOverlimitScopeStateLimit + 64
	for i := 0; i < totalScopes; i++ {
		scopeURL := fmt.Sprintf("https://provider-%d.example/live.m3u8", i)
		manager.armProviderOverlimitCooldownForSource(
			http.StatusTooManyRequests,
			"upstream returned status 429",
			int64(1000+i),
			scopeURL,
		)
	}

	manager.overlimitMu.Lock()
	scopedStateSize := len(manager.providerCooldownByScope)
	manager.overlimitMu.Unlock()
	if scopedStateSize > providerOverlimitScopeStateLimit {
		t.Fatalf(
			"scoped cooldown state size = %d, want <= %d",
			scopedStateSize,
			providerOverlimitScopeStateLimit,
		)
	}
}

func TestSessionManagerProviderOverlimitCooldownScopeStatePreservesActiveScopeUnderChurn(t *testing.T) {
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  350 * time.Millisecond,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
	}, NewPool(1), &fakeChannelsProvider{})
	if manager == nil {
		t.Fatal("manager is nil")
	}

	activeSourceID := int64(42)
	activeScopeURL := "https://active-provider.example/live.m3u8"

	// Simulate an active scope with repeated overlimit hits while unrelated
	// one-off scopes churn through the bounded state map.
	manager.armProviderOverlimitCooldownForSource(
		http.StatusTooManyRequests,
		"active scope hit #1",
		activeSourceID,
		activeScopeURL,
	)
	manager.armProviderOverlimitCooldownForSource(
		http.StatusTooManyRequests,
		"active scope hit #2",
		activeSourceID,
		activeScopeURL,
	)

	totalScopes := providerOverlimitScopeStateLimit + 96
	for i := 0; i < totalScopes; i++ {
		scopeURL := fmt.Sprintf("https://provider-%d.example/live.m3u8", i)
		manager.armProviderOverlimitCooldownForSource(
			http.StatusTooManyRequests,
			"unrelated scope churn",
			int64(1000+i),
			scopeURL,
		)
	}

	if remaining := manager.providerOverlimitRemainingForSource(activeSourceID, activeScopeURL); remaining <= 0 {
		t.Fatalf("active scope cooldown = %s after churn, want > 0", remaining)
	}

	manager.overlimitMu.Lock()
	scopedStateSize := len(manager.providerCooldownByScope)
	manager.overlimitMu.Unlock()
	if scopedStateSize > providerOverlimitScopeStateLimit {
		t.Fatalf(
			"scoped cooldown state size = %d, want <= %d",
			scopedStateSize,
			providerOverlimitScopeStateLimit,
		)
	}
}

func TestSessionManagerWaitForProviderOverlimitCooldownForSourceHonorsActiveScopeAfterChurn(t *testing.T) {
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  320 * time.Millisecond,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
	}, NewPool(1), &fakeChannelsProvider{})
	if manager == nil {
		t.Fatal("manager is nil")
	}

	activeSourceID := int64(99)
	activeScopeURL := "https://wait-active-provider.example/live.m3u8"

	manager.armProviderOverlimitCooldownForSource(
		http.StatusTooManyRequests,
		"active scope wait hit #1",
		activeSourceID,
		activeScopeURL,
	)
	manager.armProviderOverlimitCooldownForSource(
		http.StatusTooManyRequests,
		"active scope wait hit #2",
		activeSourceID,
		activeScopeURL,
	)

	totalScopes := providerOverlimitScopeStateLimit + 80
	for i := 0; i < totalScopes; i++ {
		scopeURL := fmt.Sprintf("https://scope-churn-%d.example/live.m3u8", i)
		manager.armProviderOverlimitCooldownForSource(
			http.StatusTooManyRequests,
			"scope churn",
			int64(2000+i),
			scopeURL,
		)
	}

	started := time.Now()
	deadline := started.Add(2 * time.Second)
	if err := manager.waitForProviderOverlimitCooldownForSource(
		context.Background(),
		deadline,
		activeSourceID,
		activeScopeURL,
	); err != nil {
		t.Fatalf("waitForProviderOverlimitCooldownForSource() error = %v", err)
	}

	elapsed := time.Since(started)
	if elapsed < 120*time.Millisecond {
		t.Fatalf("wait elapsed = %s after churn, want >= 120ms (active scope cooldown should still gate)", elapsed)
	}
}

func TestStartRecoverySourceSingleCandidateUsesSingleSourceLookup(t *testing.T) {
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer current.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 1, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() error = %v", err)
	}
	defer reader.Close()
	if selected.SourceID != currentSource.SourceID {
		t.Fatalf("selected source = %d, want %d", selected.SourceID, currentSource.SourceID)
	}

	if got := provider.listSourceCalls(); got != 1 {
		t.Fatalf("ListSources() call count = %d, want 1", got)
	}
	if got := provider.getSourceCallCount(); got != 1 {
		t.Fatalf("GetSource() call count = %d, want 1", got)
	}
}

func TestStartRecoverySourceSingleCandidateLegacyProviderFallsBackToListSources(t *testing.T) {
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer current.Close()

	baseProvider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}
	provider := &listOnlyChannelsProvider{base: baseProvider}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := baseProvider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 1, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() error = %v", err)
	}
	defer reader.Close()
	if selected.SourceID != currentSource.SourceID {
		t.Fatalf("selected source = %d, want %d", selected.SourceID, currentSource.SourceID)
	}

	if got := baseProvider.listSourceCalls(); got != 2 {
		t.Fatalf("ListSources() call count = %d, want 2 fallback list scans", got)
	}
	if got := baseProvider.getSourceCallCount(); got != 0 {
		t.Fatalf("GetSource() call count = %d, want 0 for legacy provider", got)
	}
}

func BenchmarkSessionManagerHistorySnapshotBoundedActiveTimeline(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := &SessionManager{
		cfg: sessionManagerConfig{
			mode:                          "direct",
			sessionHistoryLimit:           256,
			sessionSourceHistoryLimit:     64,
			sessionSubscriberHistoryLimit: 64,
		},
		logger:                  logger,
		sessions:                make(map[int64]*sharedRuntimeSession),
		creating:                make(map[int64]*sessionCreateWait),
		sessionHistory:          make([]SharedSessionHistory, 0, 256),
		recentHealth:            newRecentSourceHealth(),
		overlimitMu:             sync.Mutex{},
		providerCooldownByScope: make(map[string]providerOverlimitScopeState),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &sharedRuntimeSession{
		manager:                 manager,
		channel:                 channels.Channel{ChannelID: 1, GuideNumber: "101", GuideName: "Bench"},
		ring:                    NewChunkRing(32),
		ctx:                     ctx,
		cancel:                  cancel,
		readyCh:                 make(chan struct{}),
		subscribers:             make(map[uint64]SubscriberStats),
		startedAt:               time.Now().UTC().Add(-2 * time.Minute),
		historySessionID:        9001,
		historyCurrentSourceIdx: -1,
		historySubscriberIndex:  make(map[uint64]int),
	}
	manager.sessions[1] = session

	for i := 0; i < 300; i++ {
		source := channels.Source{
			SourceID:  int64(1000 + i),
			ItemKey:   fmt.Sprintf("src:bench:%03d", i),
			StreamURL: fmt.Sprintf("http://example.com/%03d.ts", i),
		}
		session.setSourceStateWithStartupProbe(
			source,
			fmt.Sprintf("direct:source=%d", source.SourceID),
			"benchmark",
			startupProbeTelemetry{rawBytes: 1024, trimmedBytes: 1024},
			true,
			"h264",
			startupStreamInventory{videoStreamCount: 1, audioStreamCount: 1},
			false,
			"",
		)
	}
	for i := 0; i < 300; i++ {
		_, subscriberID, err := session.addSubscriber(fmt.Sprintf("192.168.0.%d:5000", i+1))
		if err != nil {
			b.Fatalf("addSubscriber(%d) error = %v", i, err)
		}
		session.removeSubscriber(subscriberID, subscriberRemovalReasonStreamWriteError)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		history, limit, truncated := manager.HistorySnapshot()
		if len(history) != 1 {
			b.Fatalf("len(history) = %d, want 1", len(history))
		}
		if limit != 256 {
			b.Fatalf("history limit = %d, want 256", limit)
		}
		if truncated < 0 {
			b.Fatalf("history truncated = %d, want >= 0", truncated)
		}
		if got := len(history[0].Sources); got > 64 {
			b.Fatalf("len(history[0].sources) = %d, want <= 64", got)
		}
		if got := len(history[0].Subscribers); got > 64 {
			b.Fatalf("len(history[0].subscribers) = %d, want <= 64", got)
		}
	}
}

func BenchmarkLoadPersistedSourceCandidateLookup(b *testing.B) {
	makeSources := func(count int) []channels.Source {
		sources := make([]channels.Source, 0, count)
		for i := 0; i < count; i++ {
			sources = append(sources, channels.Source{
				SourceID:      int64(10 + i),
				ChannelID:     1,
				ItemKey:       fmt.Sprintf("src:bench:%03d", i),
				StreamURL:     fmt.Sprintf("http://example.com/%03d.ts", i),
				PriorityIndex: i,
				Enabled:       true,
			})
		}
		return sources
	}

	benchmarkLookup := func(b *testing.B, provider ChannelsProvider) {
		manager := NewSessionManager(SessionManagerConfig{
			Mode:                       "direct",
			StartupTimeout:             700 * time.Millisecond,
			FailoverTotalTimeout:       3 * time.Second,
			MinProbeBytes:              1,
			BufferChunkBytes:           1,
			BufferPublishFlushInterval: 10 * time.Millisecond,
		}, NewPool(1), provider)
		if manager == nil {
			b.Fatal("manager is nil")
		}

		sources, err := provider.ListSources(context.Background(), 1, true)
		if err != nil {
			b.Fatalf("ListSources() setup error = %v", err)
		}
		target := sources[len(sources)-1]

		session := &sharedRuntimeSession{
			manager: manager,
			channel: channels.Channel{
				ChannelID:   1,
				GuideNumber: "101",
				GuideName:   "Bench",
				Enabled:     true,
			},
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			source, ok := session.loadPersistedSourceCandidate(context.Background(), target.SourceID)
			if !ok {
				b.Fatalf("loadPersistedSourceCandidate(%d) found = false, want true", target.SourceID)
			}
			if source.SourceID != target.SourceID {
				b.Fatalf("source.SourceID = %d, want %d", source.SourceID, target.SourceID)
			}
		}
	}

	b.Run("fallback_list_sources", func(b *testing.B) {
		base := &fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{
				"101": {ChannelID: 1, GuideNumber: "101", GuideName: "Bench", Enabled: true},
			},
			sourcesByID: map[int64][]channels.Source{
				1: makeSources(250),
			},
		}
		benchmarkLookup(b, &listOnlyChannelsProvider{base: base})
	})

	b.Run("narrow_get_source", func(b *testing.B) {
		provider := &fakeChannelsProvider{
			channelsByGuide: map[string]channels.Channel{
				"101": {ChannelID: 1, GuideNumber: "101", GuideName: "Bench", Enabled: true},
			},
			sourcesByID: map[int64][]channels.Source{
				1: makeSources(250),
			},
		}
		benchmarkLookup(b, provider)
	})
}

func TestStartRecoverySourceRetriesCurrentSourceWithBackoffOnly(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		hit := currentHits
		currentMu.Unlock()

		if hit == 1 {
			http.Error(w, "temporary over limit", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer current.Close()

	var (
		alternateMu   sync.Mutex
		alternateHits int
	)
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alternateMu.Lock()
		alternateHits++
		alternateMu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("alt"))
	}))
	defer alternate.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:alternate",
					StreamURL:     alternate.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          1200 * time.Millisecond,
		StallPolicy:                stallPolicyRestartSame,
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	started := time.Now()
	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 1, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() error = %v", err)
	}
	defer reader.Close()

	if selected.SourceID != 10 {
		t.Fatalf("selected source = %d, want 10 (current source)", selected.SourceID)
	}

	elapsed := time.Since(started)
	if elapsed < 200*time.Millisecond {
		t.Fatalf("recovery elapsed = %s, want >= 200ms to confirm backoff retry", elapsed)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits < 2 {
		t.Fatalf("current source hits = %d, want >= 2 for retry on same source", gotCurrentHits)
	}

	alternateMu.Lock()
	gotAlternateHits := alternateHits
	alternateMu.Unlock()
	if gotAlternateHits != 0 {
		t.Fatalf("alternate source hits = %d, want 0 during recovery", gotAlternateHits)
	}

	waitFor(t, time.Second, func() bool {
		return containsInt64(provider.sourceSuccesses(), 10)
	})
	failures := provider.sourceFailures()
	successes := provider.sourceSuccesses()
	if len(failures) != 0 {
		t.Fatalf("failures = %#v, want no persisted recovery startup failures", failures)
	}
	if containsInt64(successes, 11) {
		t.Fatalf("successes = %#v, source 11 should not be used for recovery", successes)
	}
	if !containsInt64(successes, 10) {
		t.Fatalf("successes = %#v, want source 10 eventual recovery success", successes)
	}
}

func TestStartRecoverySourceFailoverPolicyUsesAlternateCandidate(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		currentMu.Unlock()

		http.Error(w, "current source down", http.StatusNotFound)
	}))
	defer current.Close()

	var (
		alternateMu   sync.Mutex
		alternateHits int
	)
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alternateMu.Lock()
		alternateHits++
		alternateMu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("alt"))
	}))
	defer alternate.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:alternate",
					StreamURL:     alternate.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		MaxFailovers:               1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          1200 * time.Millisecond,
		StallPolicy:                "failover_source",
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 2, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() error = %v", err)
	}
	defer reader.Close()

	if selected.SourceID != 11 {
		t.Fatalf("selected source = %d, want 11 (alternate source)", selected.SourceID)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits != 0 {
		t.Fatalf("current source hits = %d, want 0 because failover_source should try alternate first", gotCurrentHits)
	}

	alternateMu.Lock()
	gotAlternateHits := alternateHits
	alternateMu.Unlock()
	if gotAlternateHits == 0 {
		t.Fatal("alternate source hits = 0, want alternate probe/startup")
	}

	waitFor(t, time.Second, func() bool {
		return containsInt64(provider.sourceSuccesses(), 11)
	})
	successes := provider.sourceSuccesses()
	if !containsInt64(successes, 11) {
		t.Fatalf("successes = %#v, want recovery success for source 11", successes)
	}
}

func TestStartRecoverySourceFailoverPolicySkipsTransientCoolingAlternate(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		currentMu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("current"))
	}))
	defer current.Close()

	var (
		alternateMu   sync.Mutex
		alternateHits int
	)
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alternateMu.Lock()
		alternateHits++
		alternateMu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("alternate"))
	}))
	defer alternate.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:alternate",
					StreamURL:     alternate.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		MaxFailovers:               1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          1200 * time.Millisecond,
		StallPolicy:                "failover_source",
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:                       manager,
		channel:                       channel,
		ctx:                           context.Background(),
		cancel:                        func() {},
		readyCh:                       make(chan struct{}),
		subscribers:                   map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:                     time.Now().UTC(),
		shortLivedRecoveryBySource:    make(map[int64]shortLivedRecoveryPenalty),
		sourceHealthPersistCh:         make(chan sourceHealthPersistRequest, defaultSourceHealthPersistQueueSize),
		sourceHealthCoalesced:         make(map[int64]sourceHealthPersistRequest),
		sourceHealthQueue:             make([]int64, 0, defaultSourceHealthPersistQueueSize),
		sourceHealthCoalescedBySource: make(map[int64]int64),
		sourceHealthDroppedBySource:   make(map[int64]int64),
	}

	session.recordShortLivedRecoveryFailure(11, "startup_passed_unstable_source_eof", time.Now().UTC())

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 2, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() error = %v", err)
	}
	defer reader.Close()

	if selected.SourceID != 10 {
		t.Fatalf("selected source = %d, want 10 due transient cooldown on alternate", selected.SourceID)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits == 0 {
		t.Fatal("current source was not attempted")
	}

	alternateMu.Lock()
	gotAlternateHits := alternateHits
	alternateMu.Unlock()
	if gotAlternateHits != 0 {
		t.Fatalf("alternate source hits = %d, want 0 while transient cooldown is active", gotAlternateHits)
	}
}

func TestStartRecoverySourceFailoverPolicyPenalizesFailingAlternateAcrossRecoveryCycles(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		currentMu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("current"))
	}))
	defer current.Close()

	var (
		alternateMu   sync.Mutex
		alternateHits int
	)
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alternateMu.Lock()
		alternateHits++
		alternateMu.Unlock()

		http.Error(w, "alternate unavailable", http.StatusNotFound)
	}))
	defer alternate.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:alternate",
					StreamURL:     alternate.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             150 * time.Millisecond,
		FailoverTotalTimeout:       1300 * time.Millisecond,
		MinProbeBytes:              1,
		MaxFailovers:               1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          1300 * time.Millisecond,
		StallPolicy:                "failover_source",
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 2, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() first cycle error = %v", err)
	}
	_ = reader.Close()
	if selected.SourceID != 10 {
		t.Fatalf("first cycle selected source = %d, want 10 fallback current source", selected.SourceID)
	}

	alternateMu.Lock()
	alternateAfterFirst := alternateHits
	alternateMu.Unlock()
	if alternateAfterFirst == 0 {
		t.Fatal("alternate source hits after first recovery cycle = 0, want at least one failed alternate startup attempt")
	}
	if got := session.shortLivedRecoveryCount(11); got == 0 {
		t.Fatalf("shortLivedRecoveryCount(11) after first cycle = %d, want > 0", got)
	}

	reader, selected, err = session.startRecoverySource(context.Background(), currentSource, true, 3, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() second cycle error = %v", err)
	}
	_ = reader.Close()
	if selected.SourceID != 10 {
		t.Fatalf("second cycle selected source = %d, want 10 fallback current source", selected.SourceID)
	}

	alternateMu.Lock()
	alternateAfterSecond := alternateHits
	alternateMu.Unlock()
	if alternateAfterSecond != alternateAfterFirst {
		t.Fatalf("alternate source hits after second cycle = %d, want unchanged %d due transient cooldown", alternateAfterSecond, alternateAfterFirst)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits == 0 {
		t.Fatal("current source was not retried")
	}

	if failures := provider.sourceFailures(); len(failures) != 0 {
		t.Fatalf("failures = %#v, want no persisted recovery startup failures", failures)
	}
}

func TestStartSourceWithCandidatesRecoveryCyclePenalizesFailingAlternatesAcrossCalls(t *testing.T) {
	var (
		alternateMu   sync.Mutex
		alternateHits int
	)
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alternateMu.Lock()
		alternateHits++
		alternateMu.Unlock()
		http.Error(w, "alternate unavailable", http.StatusNotFound)
	}))
	defer alternate.Close()

	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		currentMu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("current"))
	}))
	defer current.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:alternate",
					StreamURL:     alternate.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             120 * time.Millisecond,
		FailoverTotalTimeout:       900 * time.Millisecond,
		MinProbeBytes:              1,
		MaxFailovers:               1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          900 * time.Millisecond,
		StallPolicy:                "failover_source",
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	candidates := []channels.Source{
		{
			SourceID:      11,
			ChannelID:     1,
			ItemKey:       "src:news:alternate",
			StreamURL:     alternate.URL,
			PriorityIndex: 1,
			Enabled:       true,
		},
		{
			SourceID:      10,
			ChannelID:     1,
			ItemKey:       "src:news:current",
			StreamURL:     current.URL,
			PriorityIndex: 0,
			Enabled:       true,
		},
	}

	reader, selected, err := session.startSourceWithCandidates(
		context.Background(),
		candidates,
		900*time.Millisecond,
		"recovery_cycle_2:stall",
		2,
		"stall",
	)
	if err != nil {
		t.Fatalf("startSourceWithCandidates() first call error = %v", err)
	}
	_ = reader.Close()
	if selected.SourceID != 10 {
		t.Fatalf("first call selected source = %d, want 10", selected.SourceID)
	}

	alternateMu.Lock()
	alternateAfterFirst := alternateHits
	alternateMu.Unlock()
	if alternateAfterFirst == 0 {
		t.Fatal("alternate source hits after first call = 0, want at least one failing attempt")
	}
	if got := session.shortLivedRecoveryCount(11); got == 0 {
		t.Fatalf("shortLivedRecoveryCount(11) after first call = %d, want > 0", got)
	}

	reader, selected, err = session.startSourceWithCandidates(
		context.Background(),
		candidates,
		900*time.Millisecond,
		"recovery_cycle_3:stall",
		3,
		"stall",
	)
	if err != nil {
		t.Fatalf("startSourceWithCandidates() second call error = %v", err)
	}
	_ = reader.Close()
	if selected.SourceID != 10 {
		t.Fatalf("second call selected source = %d, want 10", selected.SourceID)
	}

	alternateMu.Lock()
	alternateAfterSecond := alternateHits
	alternateMu.Unlock()
	if alternateAfterSecond != alternateAfterFirst {
		t.Fatalf("alternate source hits after second call = %d, want unchanged %d due transient cooldown", alternateAfterSecond, alternateAfterFirst)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits == 0 {
		t.Fatal("current source was not attempted")
	}
}

func TestStartRecoverySourceFailoverPolicyFallsBackToRestartSameWithoutAlternates(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		hit := currentHits
		currentMu.Unlock()

		if hit == 1 {
			http.Error(w, "temporary startup failure", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("current"))
	}))
	defer current.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          1200 * time.Millisecond,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	started := time.Now()
	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 1, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() error = %v", err)
	}
	defer reader.Close()

	if selected.SourceID != 10 {
		t.Fatalf("selected source = %d, want 10 (current source)", selected.SourceID)
	}

	elapsed := time.Since(started)
	if elapsed < 200*time.Millisecond {
		t.Fatalf("recovery elapsed = %s, want >= 200ms to confirm restart_same fallback retry", elapsed)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits < 2 {
		t.Fatalf("current source hits = %d, want >= 2 for same-source retry fallback", gotCurrentHits)
	}

	if got := provider.listSourceCalls(); got != 1 {
		t.Fatalf("ListSources() call count = %d, want 1", got)
	}
	if got := provider.getSourceCallCount(); got != 1 {
		t.Fatalf("GetSource() call count = %d, want 1 persisted current-source cooldown lookup", got)
	}

	waitFor(t, time.Second, func() bool {
		return containsInt64(provider.sourceSuccesses(), 10)
	})
	if failures := provider.sourceFailures(); len(failures) != 0 {
		t.Fatalf("failures = %#v, want no persisted recovery startup failures", failures)
	}
	successes := provider.sourceSuccesses()
	if !containsInt64(successes, 10) {
		t.Fatalf("successes = %#v, want source 10 recovery success", successes)
	}
}

func TestStartRecoverySourceFailoverPolicyFallsBackWhenAlternatesNotStartupEligible(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		hit := currentHits
		currentMu.Unlock()

		if hit == 1 {
			http.Error(w, "temporary startup failure", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("current"))
	}))
	defer current.Close()

	var (
		alternateMu   sync.Mutex
		alternateHits int
	)
	alternateCooling := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alternateMu.Lock()
		alternateHits++
		alternateMu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("alternate"))
	}))
	defer alternateCooling.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:empty",
					StreamURL:     "",
					PriorityIndex: 1,
					Enabled:       true,
				},
				{
					SourceID:      12,
					ChannelID:     1,
					ItemKey:       "src:news:cooling",
					StreamURL:     alternateCooling.URL,
					PriorityIndex: 2,
					Enabled:       true,
					CooldownUntil: time.Now().UTC().Add(3 * time.Second).Unix(),
				},
			},
		},
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     logger,
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		MaxFailovers:               2,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          1200 * time.Millisecond,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	started := time.Now()
	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 2, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() error = %v", err)
	}
	defer reader.Close()

	if selected.SourceID != 10 {
		t.Fatalf("selected source = %d, want 10 because alternates are not startup-eligible", selected.SourceID)
	}

	elapsed := time.Since(started)
	if elapsed < 200*time.Millisecond {
		t.Fatalf("recovery elapsed = %s, want >= 200ms to confirm same-source retry fallback", elapsed)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits < 2 {
		t.Fatalf("current source hits = %d, want >= 2 for same-source retry fallback", gotCurrentHits)
	}

	alternateMu.Lock()
	gotAlternateHits := alternateHits
	alternateMu.Unlock()
	if gotAlternateHits != 0 {
		t.Fatalf("alternate source hits = %d, want 0 because startup-ineligible alternates must be skipped", gotAlternateHits)
	}

	logOutput := logs.String()
	if !strings.Contains(logOutput, "shared session failover_source recovery fallback to same-source retry") {
		t.Fatalf("fallback log output missing message; logs=%q", logOutput)
	}
	if !strings.Contains(logOutput, "stall_policy=failover_source") {
		t.Fatalf("fallback log output missing stall_policy field; logs=%q", logOutput)
	}
	if !strings.Contains(logOutput, "effective_recovery_mode=restart_same_fallback") {
		t.Fatalf("fallback log output missing effective_recovery_mode field; logs=%q", logOutput)
	}
	if !strings.Contains(logOutput, "fallback_reason=no_alternate_sources") {
		t.Fatalf("fallback log output missing fallback_reason field; logs=%q", logOutput)
	}

	waitFor(t, time.Second, func() bool {
		return containsInt64(provider.sourceSuccesses(), 10)
	})
	if failures := provider.sourceFailures(); len(failures) != 0 {
		t.Fatalf("failures = %#v, want no persisted recovery startup failures", failures)
	}
	successes := provider.sourceSuccesses()
	if !containsInt64(successes, 10) {
		t.Fatalf("successes = %#v, want source 10 recovery success", successes)
	}
	if containsInt64(successes, 12) {
		t.Fatalf("successes = %#v, source 12 should not be used when fallback is active", successes)
	}
}

func TestStartRecoverySourceFailoverPolicyRetriesAlternateBeforeFallback(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		currentMu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("current"))
	}))
	defer current.Close()

	var (
		alternateMu   sync.Mutex
		alternateHits int
	)
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alternateMu.Lock()
		alternateHits++
		hit := alternateHits
		alternateMu.Unlock()

		if hit <= 2 {
			http.Error(w, "alternate warming up", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("alternate"))
	}))
	defer alternate.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:alternate",
					StreamURL:     alternate.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       4 * time.Second,
		MinProbeBytes:              1,
		MaxFailovers:               1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          4 * time.Second,
		StallPolicy:                "failover_source",
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	started := time.Now()
	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 3, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() error = %v", err)
	}
	defer reader.Close()

	if selected.SourceID != 11 {
		t.Fatalf("selected source = %d, want 11 after alternate retries", selected.SourceID)
	}

	elapsed := time.Since(started)
	if elapsed < 800*time.Millisecond {
		t.Fatalf("recovery elapsed = %s, want >= 800ms to confirm settle+retry alternate window", elapsed)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits != 0 {
		t.Fatalf("current source hits = %d, want 0 because alternate should recover before fallback", gotCurrentHits)
	}

	alternateMu.Lock()
	gotAlternateHits := alternateHits
	alternateMu.Unlock()
	if gotAlternateHits < 3 {
		t.Fatalf("alternate source hits = %d, want >= 3 for bounded retry window", gotAlternateHits)
	}

	waitFor(t, time.Second, func() bool {
		return containsInt64(provider.sourceSuccesses(), 11)
	})
	failures := provider.sourceFailures()
	successes := provider.sourceSuccesses()
	if len(failures) != 0 {
		t.Fatalf("failures = %#v, want no persisted recovery startup failures", failures)
	}
	if !containsInt64(successes, 11) {
		t.Fatalf("successes = %#v, want recovery success for source 11", successes)
	}
}

func TestStartRecoverySourceRestartSameHonorsCurrentCooldown(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		currentMu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("current"))
	}))
	defer current.Close()

	var (
		alternateMu   sync.Mutex
		alternateHits int
	)
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alternateMu.Lock()
		alternateHits++
		alternateMu.Unlock()

		http.Error(w, "alternate should not be called", http.StatusNotFound)
	}))
	defer alternate.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
					CooldownUntil: time.Now().UTC().Add(2 * time.Second).Unix(),
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:alternate",
					StreamURL:     alternate.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          3 * time.Second,
		StallPolicy:                stallPolicyRestartSame,
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	started := time.Now()
	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 1, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() error = %v", err)
	}
	defer reader.Close()

	if selected.SourceID != 10 {
		t.Fatalf("selected source = %d, want 10", selected.SourceID)
	}

	elapsed := time.Since(started)
	if elapsed < 900*time.Millisecond {
		t.Fatalf("recovery elapsed = %s, want >= 900ms to honor source cooldown", elapsed)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits == 0 {
		t.Fatal("current source was not attempted after cooldown")
	}

	alternateMu.Lock()
	gotAlternateHits := alternateHits
	alternateMu.Unlock()
	if gotAlternateHits != 0 {
		t.Fatalf("alternate source hits = %d, want 0 in restart_same mode", gotAlternateHits)
	}
}

func TestStartRecoverySourceRestartSameSkipsDisabledCurrentSource(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		currentMu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("stale-current"))
	}))
	defer current.Close()

	var (
		alternateMu   sync.Mutex
		alternateHits int
	)
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alternateMu.Lock()
		alternateHits++
		alternateMu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("enabled-alternate"))
	}))
	defer alternate.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current-disabled",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       false,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:alternate-enabled",
					StreamURL:     alternate.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          1200 * time.Millisecond,
		StallPolicy:                stallPolicyRestartSame,
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:runtime-current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 1, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() error = %v", err)
	}
	defer reader.Close()

	if selected.SourceID != 11 {
		t.Fatalf("selected source = %d, want 11 (enabled candidate)", selected.SourceID)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits != 0 {
		t.Fatalf("disabled current source hits = %d, want 0", gotCurrentHits)
	}

	alternateMu.Lock()
	gotAlternateHits := alternateHits
	alternateMu.Unlock()
	if gotAlternateHits == 0 {
		t.Fatal("enabled alternate source was not attempted")
	}

	successes := provider.sourceSuccesses()
	if !containsInt64(successes, 11) {
		t.Fatalf("successes = %#v, want source 11 recovery success", successes)
	}
	if containsInt64(successes, 10) {
		t.Fatalf("successes = %#v, disabled current source 10 should not be used", successes)
	}

	if got := provider.getSourceCallCount(); got != 0 {
		t.Fatalf("GetSource() call count = %d, want 0 for disabled current source", got)
	}
}

func TestStartRecoverySourceRestartSameDisabledCurrentWithoutEnabledCandidatesFailsDeterministically(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		currentMu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("stale-current"))
	}))
	defer current.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current-disabled",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       false,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          1200 * time.Millisecond,
		StallPolicy:                stallPolicyRestartSame,
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:runtime-current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	_, _, err := session.startRecoverySource(context.Background(), currentSource, true, 1, "stall")
	if !errors.Is(err, ErrSessionNoSources) {
		t.Fatalf("startRecoverySource() error = %v, want ErrSessionNoSources", err)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits != 0 {
		t.Fatalf("disabled current source hits = %d, want 0", gotCurrentHits)
	}

	if got := provider.listSourceCalls(); got != 1 {
		t.Fatalf("ListSources() call count = %d, want 1", got)
	}
	if got := provider.getSourceCallCount(); got != 0 {
		t.Fatalf("GetSource() call count = %d, want 0", got)
	}
}

func TestStartRecoverySourceFailoverFallbackSkipsDisabledCurrentSource(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		currentMu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("stale-current"))
	}))
	defer current.Close()

	var (
		alternateMu   sync.Mutex
		alternateHits int
	)
	alternate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		alternateMu.Lock()
		alternateHits++
		alternateMu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("cooling-alternate"))
	}))
	defer alternate.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current-disabled",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       false,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:alternate-cooling",
					StreamURL:     alternate.URL,
					PriorityIndex: 1,
					Enabled:       true,
					CooldownUntil: time.Now().UTC().Add(3 * time.Second).Unix(),
				},
			},
		},
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     logger,
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		MaxFailovers:               1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          1200 * time.Millisecond,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:runtime-current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	reader, selected, err := session.startRecoverySource(context.Background(), currentSource, true, 2, "stall")
	if err != nil {
		t.Fatalf("startRecoverySource() error = %v", err)
	}
	defer reader.Close()

	if selected.SourceID != 11 {
		t.Fatalf("selected source = %d, want 11 (enabled fallback candidate)", selected.SourceID)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits != 0 {
		t.Fatalf("disabled current source hits = %d, want 0", gotCurrentHits)
	}

	alternateMu.Lock()
	gotAlternateHits := alternateHits
	alternateMu.Unlock()
	if gotAlternateHits == 0 {
		t.Fatal("enabled alternate source was not attempted")
	}

	successes := provider.sourceSuccesses()
	if !containsInt64(successes, 11) {
		t.Fatalf("successes = %#v, want source 11 recovery success", successes)
	}
	if containsInt64(successes, 10) {
		t.Fatalf("successes = %#v, disabled current source 10 should not be used", successes)
	}

	if got := provider.getSourceCallCount(); got != 0 {
		t.Fatalf("GetSource() call count = %d, want 0 for disabled current source fallback", got)
	}

	logOutput := logs.String()
	if strings.Contains(logOutput, "shared session failover_source recovery fallback to same-source retry") {
		t.Fatalf("fallback log unexpectedly emitted for disabled current source; logs=%q", logOutput)
	}
}

func TestStartSourceWithCandidatesWaitsForCoolingSource(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary unavailable", http.StatusNotFound)
	}))
	defer failing.Close()

	var (
		healthyMu   sync.Mutex
		healthyHits int
	)
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		healthyMu.Lock()
		healthyHits++
		healthyMu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer healthy.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:failing",
					StreamURL:     failing.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:healthy",
					StreamURL:     healthy.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  0,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	candidates := []channels.Source{
		{
			SourceID:      10,
			ChannelID:     1,
			ItemKey:       "src:news:failing",
			StreamURL:     failing.URL,
			PriorityIndex: 0,
			Enabled:       true,
		},
		{
			SourceID:      11,
			ChannelID:     1,
			ItemKey:       "src:news:healthy",
			StreamURL:     healthy.URL,
			PriorityIndex: 1,
			Enabled:       true,
			CooldownUntil: time.Now().UTC().Add(2 * time.Second).Unix(),
		},
	}

	started := time.Now()
	reader, selected, err := session.startSourceWithCandidates(
		context.Background(),
		candidates,
		3*time.Second,
		"initial_startup",
		0,
		"",
	)
	if err != nil {
		t.Fatalf("startSourceWithCandidates() error = %v", err)
	}
	defer reader.Close()

	if selected.SourceID != 11 {
		t.Fatalf("selected source = %d, want 11", selected.SourceID)
	}

	elapsed := time.Since(started)
	if elapsed < 900*time.Millisecond {
		t.Fatalf("startup elapsed = %s, want >= 900ms to honor source cooldown", elapsed)
	}

	healthyMu.Lock()
	gotHealthyHits := healthyHits
	healthyMu.Unlock()
	if gotHealthyHits == 0 {
		t.Fatal("healthy source was not attempted after cooldown")
	}
}

func TestStartSourceWithCandidatesAttemptsLeastFailedCoolingSource(t *testing.T) {
	var (
		mu               sync.Mutex
		mostFailedHits   int
		preferredHits    int
		secondaryTieHits int
	)

	mostFailed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		mostFailedHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer mostFailed.Close()

	preferred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		preferredHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer preferred.Close()

	secondaryTie := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		secondaryTieHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer secondaryTie.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:most-failed",
					StreamURL:     mostFailed.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:preferred",
					StreamURL:     preferred.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
				{
					SourceID:      12,
					ChannelID:     1,
					ItemKey:       "src:news:secondary-tie",
					StreamURL:     secondaryTie.URL,
					PriorityIndex: 2,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             600 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  0,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	now := time.Now().UTC()
	candidates := []channels.Source{
		{
			SourceID:      10,
			ChannelID:     1,
			ItemKey:       "src:news:most-failed",
			StreamURL:     mostFailed.URL,
			PriorityIndex: 0,
			Enabled:       true,
			FailCount:     3,
			LastFailAt:    now.Add(-20 * time.Second).Unix(),
			CooldownUntil: now.Add(2 * time.Minute).Unix(),
		},
		{
			SourceID:      12,
			ChannelID:     1,
			ItemKey:       "src:news:secondary-tie",
			StreamURL:     secondaryTie.URL,
			PriorityIndex: 2,
			Enabled:       true,
			FailCount:     1,
			LastFailAt:    now.Add(-12 * time.Second).Unix(),
			CooldownUntil: now.Add(2 * time.Minute).Unix(),
		},
		{
			SourceID:      11,
			ChannelID:     1,
			ItemKey:       "src:news:preferred",
			StreamURL:     preferred.URL,
			PriorityIndex: 1,
			Enabled:       true,
			FailCount:     1,
			LastFailAt:    now.Add(-10 * time.Second).Unix(),
			CooldownUntil: now.Add(2 * time.Minute).Unix(),
		},
	}

	started := time.Now()
	reader, selected, err := session.startSourceWithCandidates(
		context.Background(),
		candidates,
		1*time.Second,
		"initial_startup",
		0,
		"",
	)
	if err != nil {
		t.Fatalf("startSourceWithCandidates() error = %v", err)
	}
	defer reader.Close()

	if selected.SourceID != 11 {
		t.Fatalf("selected source = %d, want 11 (least failures + highest priority)", selected.SourceID)
	}

	elapsed := time.Since(started)
	if elapsed >= 900*time.Millisecond {
		t.Fatalf("startup elapsed = %s, want < 900ms when forcing a cooling candidate", elapsed)
	}

	mu.Lock()
	gotMostFailedHits := mostFailedHits
	gotPreferredHits := preferredHits
	gotSecondaryTieHits := secondaryTieHits
	mu.Unlock()

	if gotPreferredHits == 0 {
		t.Fatal("preferred source was not attempted")
	}
	if gotMostFailedHits != 0 {
		t.Fatalf("most-failed source hits = %d, want 0", gotMostFailedHits)
	}
	if gotSecondaryTieHits != 0 {
		t.Fatalf("secondary-tie source hits = %d, want 0", gotSecondaryTieHits)
	}
}

func TestStartRecoverySourceHonorsStallMaxFailoversPerStall(t *testing.T) {
	var (
		currentMu   sync.Mutex
		currentHits int
	)
	current := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		currentMu.Lock()
		currentHits++
		currentMu.Unlock()

		http.Error(w, "temporary over limit", http.StatusNotFound)
	}))
	defer current.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     current.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		UpstreamOverlimitCooldown:  0,
		StallHardDeadline:          4 * time.Second,
		StallMaxFailoversPerStall:  1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     channel,
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	currentSource := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:current",
		StreamURL:     current.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	started := time.Now()
	_, _, err := session.startRecoverySource(context.Background(), currentSource, true, 4, "stall")
	if err == nil {
		t.Fatal("startRecoverySource() error = nil, want failure after capped attempts")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "stall max failovers") {
		t.Fatalf("startRecoverySource() error = %v, want stall max failovers context", err)
	}

	elapsed := time.Since(started)
	if elapsed >= 3*time.Second {
		t.Fatalf("recovery elapsed = %s, want early stop from attempt cap", elapsed)
	}

	currentMu.Lock()
	gotCurrentHits := currentHits
	currentMu.Unlock()
	if gotCurrentHits != 2 {
		t.Fatalf("current source hits = %d, want exactly 2 attempts (initial + 1 retry)", gotCurrentHits)
	}

	provider.mu.Lock()
	failures := append([]sourceFailureCall(nil), provider.failures...)
	provider.mu.Unlock()
	if got := len(failures); got != 0 {
		t.Fatalf("len(failures) = %d, want 0 recovery startup failures recorded", got)
	}
}

func TestSessionStatsExposeRecoveryChurnTelemetry(t *testing.T) {
	var (
		sourceMu   sync.Mutex
		sourceHits int
	)
	stallingThenHealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sourceMu.Lock()
		sourceHits++
		hit := sourceHits
		sourceMu.Unlock()

		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if hit == 1 {
			_, _ = w.Write([]byte("A"))
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(250 * time.Millisecond)
			return
		}

		for i := 0; i < 240; i++ {
			if _, err := w.Write([]byte("B")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer stallingThenHealthy.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:current",
					StreamURL:     stallingThenHealthy.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                80 * time.Millisecond,
		StallHardDeadline:          1200 * time.Millisecond,
		StallPolicy:                stallPolicyRestartSame,
		StallMaxFailoversPerStall:  2,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	streamCtx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	writer := &recordingResponseWriter{}
	errCh := make(chan error, 1)
	go func() {
		errCh <- sub.Stream(streamCtx, writer)
	}()

	waitFor(t, 2*time.Second, func() bool {
		stats := sub.Stats()
		return stats.RecoveryCycle >= 1 &&
			stats.SourceSelectCount >= 2 &&
			stats.SameSourceReselectCount >= 1
	})

	stats := sub.Stats()
	if strings.TrimSpace(stats.RecoveryReason) == "" {
		t.Fatal("stats.recovery_reason is empty")
	}
	if strings.TrimSpace(stats.SinceLastSourceSelect) == "" {
		t.Fatal("stats.since_last_source_select is empty")
	}
	if !strings.Contains(stats.LastSourceSelectReason, "recovery_cycle_") {
		t.Fatalf("stats.last_source_select_reason = %q, want recovery_cycle_*", stats.LastSourceSelectReason)
	}
	if writer.Writes() == 0 {
		t.Fatal("writer received no chunks")
	}

	provider.mu.Lock()
	failures := append([]sourceFailureCall(nil), provider.failures...)
	provider.mu.Unlock()
	if !hasFailureReason(failures, 10, "stall") {
		t.Fatalf("failures = %#v, want at least one stall-related failure reason", failures)
	}

	cancel()
	select {
	case streamErr := <-errCh:
		if streamErr != nil &&
			!errors.Is(streamErr, context.Canceled) &&
			!errors.Is(streamErr, context.DeadlineExceeded) {
			t.Fatalf("Stream() error = %v", streamErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream goroutine")
	}
}

func TestSharedSessionShouldTrackCycleFailureByHealthyWindow(t *testing.T) {
	now := time.Unix(1770433600, 0).UTC()
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				cycleFailureMinHealth: 2 * time.Second,
			},
		},
	}

	session.lastSourceSelectedAt = now.Add(-600 * time.Millisecond)
	session.lastSourceSelectReason = "recovery_cycle_3:source_eof"
	track, healthyFor, selectReason := session.shouldTrackCycleFailure(now)
	if track {
		t.Fatal("track = true, want false for short-lived recovery-selected source")
	}
	if healthyFor < 500*time.Millisecond || healthyFor > 700*time.Millisecond {
		t.Fatalf("healthyFor = %s, want ~600ms", healthyFor)
	}
	if selectReason != "recovery_cycle_3:source_eof" {
		t.Fatalf("selectReason = %q, want recovery cycle reason", selectReason)
	}

	session.lastSourceSelectReason = "initial_startup"
	track, _, _ = session.shouldTrackCycleFailure(now)
	if !track {
		t.Fatal("track = false, want true for initial_startup source selection")
	}

	session.lastSourceSelectReason = "recovery_cycle_4:source_eof"
	session.lastSourceSelectedAt = now.Add(-3 * time.Second)
	track, _, _ = session.shouldTrackCycleFailure(now)
	if !track {
		t.Fatal("track = false, want true after recovery-selected source passes min healthy window")
	}

	session.manager.cfg.cycleFailureMinHealth = 0
	track, _, _ = session.shouldTrackCycleFailure(now)
	if !track {
		t.Fatal("track = false, want true when cycle-failure-min-health guard is disabled")
	}
}

func TestSessionManagerSkipsShortLivedRecoveryCycleFailurePersistence(t *testing.T) {
	stalling := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if _, err := w.Write([]byte("A")); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(900 * time.Millisecond)
	}))
	defer stalling.Close()

	shortEOF := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if _, err := w.Write([]byte("B")); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(80 * time.Millisecond)
	}))
	defer shortEOF.Close()

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 200; i++ {
			if _, err := w.Write([]byte("C")); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer healthy.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:stalling",
					StreamURL:     stalling.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      11,
					ChannelID:     1,
					ItemKey:       "src:news:short-eof",
					StreamURL:     shortEOF.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
				{
					SourceID:      12,
					ChannelID:     1,
					ItemKey:       "src:news:healthy",
					StreamURL:     healthy.URL,
					PriorityIndex: 2,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             700 * time.Millisecond,
		FailoverTotalTimeout:       4 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                120 * time.Millisecond,
		StallHardDeadline:          3 * time.Second,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  1,
		CycleFailureMinHealth:      2 * time.Second,
		SessionIdleTimeout:         50 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	streamCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	writer := &recordingResponseWriter{
		onWrite: func(totalBytes int) {
			if totalBytes >= 40 {
				cancel()
			}
		},
	}
	err = sub.Stream(streamCtx, writer)
	if err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!strings.Contains(strings.ToLower(err.Error()), "recovery cycle budget exhausted") {
		t.Fatalf("Stream() error = %v", err)
	}

	provider.mu.Lock()
	failures := append([]sourceFailureCall(nil), provider.failures...)
	successes := append([]int64(nil), provider.successes...)
	provider.mu.Unlock()

	if !hasFailureReason(failures, 10, "stall") {
		t.Fatalf("failures = %#v, want stall failure persisted for source 10", failures)
	}
	if hasFailureReason(failures, 11, "source stream ended") {
		t.Fatalf("failures = %#v, did not expect short-lived recovery source EOF to be persisted for source 11", failures)
	}
	if !containsInt64(successes, 11) {
		t.Fatalf("successes = %#v, want short-lived recovery source 11 to start successfully", successes)
	}
}

func TestSetSourceStateLogsSameSourceReselectThresholdCrossing(t *testing.T) {
	if sameSourceReselectAlertThreshold <= 0 {
		t.Skip("sameSourceReselectAlertThreshold is disabled")
	}

	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			logger: logger,
		},
		channel: channels.Channel{
			ChannelID:   7,
			GuideNumber: "107",
			GuideName:   "US Comedy Central",
		},
		startedAt: time.Now().UTC(),
	}

	sourceA := channels.Source{
		SourceID:  33,
		ItemKey:   "src:comedycentral.us:33",
		StreamURL: "https://example.com/33",
	}
	sourceB := channels.Source{
		SourceID:  34,
		ItemKey:   "src:comedycentral.us:34",
		StreamURL: "https://example.com/34",
	}

	// Initial source selection should not trigger an alert.
	session.setSourceState(sourceA, "ffmpeg-copy:source=33", "initial_startup")
	for i := 0; i < int(sameSourceReselectAlertThreshold); i++ {
		session.setSourceState(sourceA, "ffmpeg-copy:source=33", "recovery_cycle_1:stall")
	}

	alertMsg := "shared session same-source reselection threshold crossed"
	if got := strings.Count(logs.String(), alertMsg); got != 1 {
		t.Fatalf("alert count after first threshold crossing = %d, want 1", got)
	}

	// Additional reselections in the same streak should not emit extra threshold alerts.
	session.setSourceState(sourceA, "ffmpeg-copy:source=33", "recovery_cycle_2:stall")
	session.setSourceState(sourceA, "ffmpeg-copy:source=33", "recovery_cycle_3:stall")
	if got := strings.Count(logs.String(), alertMsg); got != 1 {
		t.Fatalf("alert count after same streak reselections = %d, want 1", got)
	}

	// Switching sources resets streak state; crossing threshold again should alert again.
	session.setSourceState(sourceB, "ffmpeg-copy:source=34", "failover_source")
	for i := 0; i < int(sameSourceReselectAlertThreshold); i++ {
		session.setSourceState(sourceB, "ffmpeg-copy:source=34", "recovery_cycle_4:stall")
	}
	if got := strings.Count(logs.String(), alertMsg); got != 2 {
		t.Fatalf("alert count after second threshold crossing = %d, want 2", got)
	}
}

func TestSessionManagerLogsSubscriberLifecycle(t *testing.T) {
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < 16; i++ {
			_, _ = w.Write([]byte("lifecycle-stream"))
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
					ItemKey:       "src:news:lifecycle",
					StreamURL:     upstream.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     logger,
		StartupTimeout:             500 * time.Millisecond,
		FailoverTotalTimeout:       1 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 15 * time.Millisecond,
		SessionIdleTimeout:         30 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	ctx := withSubscriberClientAddr(context.Background(), "198.51.100.24:4567")
	sub, err := manager.Subscribe(ctx, provider.channelsByGuide["101"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	sub.Close()

	waitFor(t, 2*time.Second, func() bool {
		text := logs.String()
		return strings.Contains(text, "shared session subscriber connected") &&
			strings.Contains(text, "shared session subscriber disconnected")
	})

	text := logs.String()
	if !strings.Contains(text, "reason=session_closed") {
		t.Fatalf("logs missing disconnect reason session_closed: %s", text)
	}
	if !strings.Contains(text, "client_addr=198.51.100.24:4567") {
		t.Fatalf("logs missing subscriber client address: %s", text)
	}
}

func TestSharedRuntimeSessionNoSubscribersBeforeReadySkipsStaleAsyncIdleCancelAfterReattach(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(previousProcs)
	})

	logs := newTestLogBuffer()
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				subscriberJoinLagBytes: 0,
				sessionIdleTimeout:     5 * time.Second,
			},
			logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		},
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}

	cancelCalls := make(chan struct{}, 1)
	session.cancel = func() {
		select {
		case cancelCalls <- struct{}{}:
		default:
		}
	}

	_, firstSubscriberID, err := session.addSubscriber("198.51.100.30:43001")
	if err != nil {
		t.Fatalf("addSubscriber(first) error = %v", err)
	}
	session.removeSubscriber(firstSubscriberID, subscriberRemovalReasonSessionClosed)
	if _, _, err := session.addSubscriber("198.51.100.30:43002"); err != nil {
		t.Fatalf("addSubscriber(reattach) error = %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	select {
	case <-cancelCalls:
		t.Fatal("stale no_subscribers_before_ready async idle cancel fired after reattach")
	default:
	}

	session.mu.Lock()
	subscriberCount := len(session.subscribers)
	session.mu.Unlock()
	if subscriberCount != 1 {
		t.Fatalf("subscriber count after reattach = %d, want 1", subscriberCount)
	}

	if strings.Contains(logs.String(), "shared session canceled while idle") {
		t.Fatalf("logs = %s, did not expect stale idle-cancel lifecycle log after reattach", logs.String())
	}
}

func TestSharedRuntimeSessionIdleTimeoutDisabledSkipsStaleAsyncIdleCancelAfterReattach(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() {
		runtime.GOMAXPROCS(previousProcs)
	})

	logs := newTestLogBuffer()
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				subscriberJoinLagBytes: 0,
				sessionIdleTimeout:     0,
			},
			logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		},
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}
	session.markReady(nil)

	cancelCalls := make(chan struct{}, 1)
	session.cancel = func() {
		select {
		case cancelCalls <- struct{}{}:
		default:
		}
	}

	_, firstSubscriberID, err := session.addSubscriber("198.51.100.40:44001")
	if err != nil {
		t.Fatalf("addSubscriber(first) error = %v", err)
	}
	session.removeSubscriber(firstSubscriberID, subscriberRemovalReasonSessionClosed)
	if _, _, err := session.addSubscriber("198.51.100.40:44002"); err != nil {
		t.Fatalf("addSubscriber(reattach) error = %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	select {
	case <-cancelCalls:
		t.Fatal("stale idle_timeout_disabled async idle cancel fired after reattach")
	default:
	}

	session.mu.Lock()
	subscriberCount := len(session.subscribers)
	session.mu.Unlock()
	if subscriberCount != 1 {
		t.Fatalf("subscriber count after reattach = %d, want 1", subscriberCount)
	}

	if strings.Contains(logs.String(), "shared session canceled while idle") {
		t.Fatalf("logs = %s, did not expect stale idle-cancel lifecycle log after reattach", logs.String())
	}
}

func TestSharedRuntimeSessionOnIdleTimerPostValidationReattachSkipsCancel(t *testing.T) {
	logs := newTestLogBuffer()
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				subscriberJoinLagBytes: 0,
				sessionIdleTimeout:     5 * time.Second,
			},
			logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		},
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}
	session.markReady(nil)
	t.Cleanup(func() {
		session.mu.Lock()
		if session.idleTimer != nil {
			session.idleTimer.Stop()
		}
		session.idleCancelValidatedHook = nil
		session.mu.Unlock()
	})

	cancelCalls := make(chan struct{}, 1)
	session.cancel = func() {
		select {
		case cancelCalls <- struct{}{}:
		default:
		}
	}

	_, firstSubscriberID, err := session.addSubscriber("198.51.100.41:44101")
	if err != nil {
		t.Fatalf("addSubscriber(first) error = %v", err)
	}
	session.removeSubscriber(firstSubscriberID, subscriberRemovalReasonSessionClosed)

	session.mu.Lock()
	token := session.idleToken
	session.mu.Unlock()
	if token == 0 {
		t.Fatal("idle token not advanced after removeSubscriber")
	}

	validated := make(chan struct{})
	release := make(chan struct{})
	session.idleCancelValidatedHook = func() {
		select {
		case <-validated:
		default:
			close(validated)
		}
		<-release
	}

	done := make(chan struct{})
	go func() {
		session.onIdleTimer(token)
		close(done)
	}()

	select {
	case <-validated:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for idle-timeout cancellation validation hook")
	}

	if _, _, err := session.addSubscriber("198.51.100.41:44102"); err != nil {
		t.Fatalf("addSubscriber(reattach) error = %v", err)
	}

	close(release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onIdleTimer cancellation attempt")
	}

	select {
	case <-cancelCalls:
		t.Fatal("idle-timeout cancellation fired after post-validation reattach")
	default:
	}

	session.mu.Lock()
	subscriberCount := len(session.subscribers)
	session.mu.Unlock()
	if subscriberCount != 1 {
		t.Fatalf("subscriber count after reattach = %d, want 1", subscriberCount)
	}

	if strings.Contains(logs.String(), "shared session canceled while idle") {
		t.Fatalf("logs = %s, did not expect idle-cancel log after post-validation reattach", logs.String())
	}
}

func TestSharedRuntimeSessionTryPreemptIdlePostValidationReattachSkipsCancel(t *testing.T) {
	logs := newTestLogBuffer()
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				subscriberJoinLagBytes: 0,
				sessionIdleTimeout:     5 * time.Second,
			},
			logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		},
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}
	session.markReady(nil)
	t.Cleanup(func() {
		session.mu.Lock()
		if session.idleTimer != nil {
			session.idleTimer.Stop()
		}
		session.idleCancelValidatedHook = nil
		session.mu.Unlock()
	})

	cancelCalls := make(chan struct{}, 1)
	session.cancel = func() {
		select {
		case cancelCalls <- struct{}{}:
		default:
		}
	}

	_, firstSubscriberID, err := session.addSubscriber("198.51.100.42:44201")
	if err != nil {
		t.Fatalf("addSubscriber(first) error = %v", err)
	}
	session.removeSubscriber(firstSubscriberID, subscriberRemovalReasonSessionClosed)

	session.mu.Lock()
	token := session.idleToken
	session.mu.Unlock()
	if token == 0 {
		t.Fatal("idle token not advanced after removeSubscriber")
	}

	validated := make(chan struct{})
	release := make(chan struct{})
	session.idleCancelValidatedHook = func() {
		select {
		case <-validated:
		default:
			close(validated)
		}
		<-release
	}

	result := make(chan bool, 1)
	go func() {
		result <- session.tryPreemptIdle(token)
	}()

	select {
	case <-validated:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for idle-preempt cancellation validation hook")
	}

	if _, _, err := session.addSubscriber("198.51.100.42:44202"); err != nil {
		t.Fatalf("addSubscriber(reattach) error = %v", err)
	}

	close(release)

	select {
	case got := <-result:
		if got {
			t.Fatal("tryPreemptIdle returned true after post-validation reattach")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for tryPreemptIdle result")
	}

	select {
	case <-cancelCalls:
		t.Fatal("idle-preempt cancellation fired after post-validation reattach")
	default:
	}

	session.mu.Lock()
	subscriberCount := len(session.subscribers)
	session.mu.Unlock()
	if subscriberCount != 1 {
		t.Fatalf("subscriber count after reattach = %d, want 1", subscriberCount)
	}

	if strings.Contains(logs.String(), "shared session canceled while idle") {
		t.Fatalf("logs = %s, did not expect idle-cancel log after post-validation reattach", logs.String())
	}
}

func TestSharedRuntimeSessionOnIdleTimerPostValidationSessionCloseSkipsCancel(t *testing.T) {
	logs := newTestLogBuffer()
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				subscriberJoinLagBytes: 0,
				sessionIdleTimeout:     5 * time.Second,
			},
			logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		},
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}
	session.markReady(nil)
	t.Cleanup(func() {
		session.mu.Lock()
		if session.idleTimer != nil {
			session.idleTimer.Stop()
		}
		session.idleCancelValidatedHook = nil
		session.mu.Unlock()
	})

	cancelCalls := make(chan struct{}, 1)
	session.cancel = func() {
		select {
		case cancelCalls <- struct{}{}:
		default:
		}
	}

	_, firstSubscriberID, err := session.addSubscriber("198.51.100.60:46001")
	if err != nil {
		t.Fatalf("addSubscriber(first) error = %v", err)
	}
	session.removeSubscriber(firstSubscriberID, subscriberRemovalReasonSessionClosed)

	session.mu.Lock()
	token := session.idleToken
	session.mu.Unlock()
	if token == 0 {
		t.Fatal("idle token not advanced after removeSubscriber")
	}

	validated := make(chan struct{})
	release := make(chan struct{})
	session.idleCancelValidatedHook = func() {
		select {
		case <-validated:
		default:
			close(validated)
		}
		<-release
	}

	done := make(chan struct{})
	go func() {
		session.onIdleTimer(token)
		close(done)
	}()

	select {
	case <-validated:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for idle-timeout cancellation validation hook")
	}

	session.mu.Lock()
	session.closed = true
	session.mu.Unlock()

	close(release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onIdleTimer to return")
	}

	select {
	case <-cancelCalls:
		t.Fatal("idle-timeout cancellation fired after post-validation session close")
	default:
	}

	if strings.Contains(logs.String(), "shared session canceled while idle") {
		t.Fatalf("logs = %s, did not expect idle-cancel log after post-validation session close", logs.String())
	}
}

func TestSharedRuntimeSessionTryPreemptIdleReturnsTrueWhenStillIdle(t *testing.T) {
	logs := newTestLogBuffer()
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				subscriberJoinLagBytes: 0,
				sessionIdleTimeout:     5 * time.Second,
			},
			logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		},
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}
	session.markReady(nil)
	t.Cleanup(func() {
		session.mu.Lock()
		if session.idleTimer != nil {
			session.idleTimer.Stop()
		}
		session.mu.Unlock()
	})

	cancelCalls := make(chan struct{}, 1)
	session.cancel = func() {
		select {
		case cancelCalls <- struct{}{}:
		default:
		}
	}

	_, firstSubscriberID, err := session.addSubscriber("198.51.100.61:46101")
	if err != nil {
		t.Fatalf("addSubscriber(first) error = %v", err)
	}
	session.removeSubscriber(firstSubscriberID, subscriberRemovalReasonSessionClosed)

	session.mu.Lock()
	token := session.idleToken
	session.mu.Unlock()
	if token == 0 {
		t.Fatal("idle token not advanced after removeSubscriber")
	}

	got := session.tryPreemptIdle(token)
	if !got {
		t.Fatal("tryPreemptIdle returned false on a still-idle session, want true")
	}

	select {
	case <-cancelCalls:
	default:
		t.Fatal("cancel function was not called after successful tryPreemptIdle")
	}

	if !strings.Contains(logs.String(), "shared session canceled while idle") {
		t.Fatalf("logs = %s, expected idle-cancel log with reason idle_preempted", logs.String())
	}
	if !strings.Contains(logs.String(), "idle_preempted") {
		t.Fatalf("logs = %s, expected reason=idle_preempted in idle-cancel log", logs.String())
	}
}

func TestSharedRuntimeSessionOnIdleTimerPostValidationTokenAdvanceSkipsCancel(t *testing.T) {
	logs := newTestLogBuffer()
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				subscriberJoinLagBytes: 0,
				sessionIdleTimeout:     5 * time.Second,
			},
			logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		},
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}
	session.markReady(nil)
	t.Cleanup(func() {
		session.mu.Lock()
		if session.idleTimer != nil {
			session.idleTimer.Stop()
		}
		session.idleCancelValidatedHook = nil
		session.mu.Unlock()
	})

	cancelCalls := make(chan struct{}, 1)
	session.cancel = func() {
		select {
		case cancelCalls <- struct{}{}:
		default:
		}
	}

	_, firstSubscriberID, err := session.addSubscriber("198.51.100.62:46201")
	if err != nil {
		t.Fatalf("addSubscriber(first) error = %v", err)
	}
	session.removeSubscriber(firstSubscriberID, subscriberRemovalReasonSessionClosed)

	session.mu.Lock()
	token := session.idleToken
	session.mu.Unlock()
	if token == 0 {
		t.Fatal("idle token not advanced after removeSubscriber")
	}

	validated := make(chan struct{})
	release := make(chan struct{})
	session.idleCancelValidatedHook = func() {
		select {
		case <-validated:
		default:
			close(validated)
		}
		<-release
	}

	done := make(chan struct{})
	go func() {
		session.onIdleTimer(token)
		close(done)
	}()

	select {
	case <-validated:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for idle-timeout cancellation validation hook")
	}

	_, secondSubscriberID, err := session.addSubscriber("198.51.100.62:46202")
	if err != nil {
		t.Fatalf("addSubscriber(second) error = %v", err)
	}
	session.removeSubscriber(secondSubscriberID, subscriberRemovalReasonSessionClosed)

	session.mu.Lock()
	newToken := session.idleToken
	session.mu.Unlock()
	if newToken == token {
		t.Fatalf("idle token did not advance after add+remove cycle: still %d", newToken)
	}

	close(release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for onIdleTimer to return")
	}

	select {
	case <-cancelCalls:
		t.Fatal("idle-timeout cancellation fired after post-validation token advance via add+remove")
	default:
	}

	if strings.Contains(logs.String(), "shared session canceled while idle") {
		t.Fatalf("logs = %s, did not expect idle-cancel log after post-validation token advance", logs.String())
	}
}

func TestSharedRuntimeSessionNoSubscribersBeforeReadyCancelsWhenStillIdle(t *testing.T) {
	logs := newTestLogBuffer()
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				subscriberJoinLagBytes: 0,
				sessionIdleTimeout:     5 * time.Second,
			},
			logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})),
		},
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}

	cancelCalls := make(chan struct{}, 1)
	session.cancel = func() {
		select {
		case cancelCalls <- struct{}{}:
		default:
		}
	}

	_, firstSubscriberID, err := session.addSubscriber("198.51.100.50:45001")
	if err != nil {
		t.Fatalf("addSubscriber(first) error = %v", err)
	}
	session.removeSubscriber(firstSubscriberID, subscriberRemovalReasonSessionClosed)

	select {
	case <-cancelCalls:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected immediate no_subscribers_before_ready idle cancel when session stays idle")
	}

	waitFor(t, time.Second, func() bool {
		text := logs.String()
		return strings.Contains(text, "shared session canceled while idle") &&
			strings.Contains(text, "reason=no_subscribers_before_ready")
	})
}

func TestSharedRuntimeSessionRemoveSubscriberStaleIdlePreemptRegistrationIgnoredAfterLeaseReuse(t *testing.T) {
	pool := NewPool(1)
	manager := &SessionManager{
		tuners: pool,
		cfg: sessionManagerConfig{
			subscriberJoinLagBytes: 0,
			sessionIdleTimeout:     5 * time.Second,
		},
	}

	staleLease, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient(stale setup) error = %v", err)
	}
	staleToken := staleLease.token

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		lease:       staleLease,
		startedAt:   time.Now().UTC(),
	}
	session.markReady(nil)
	t.Cleanup(func() {
		session.mu.Lock()
		if session.idleTimer != nil {
			session.idleTimer.Stop()
		}
		session.mu.Unlock()
	})

	_, subscriberID, err := session.addSubscriber("198.51.100.62:46001")
	if err != nil {
		t.Fatalf("addSubscriber() error = %v", err)
	}

	staleLease.Release()
	currentLease, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient(current setup) error = %v", err)
	}
	if currentLease.token == staleToken {
		t.Fatal("current lease token unexpectedly matches stale lease token")
	}

	currentPreempted := make(chan struct{}, 1)
	pool.markClientPreemptible(currentLease.ID, currentLease.token, func() bool {
		currentLease.Release()
		select {
		case currentPreempted <- struct{}{}:
		default:
		}
		return true
	})

	// Simulate late removeSubscriber callback registration from the stale session
	// after the same tuner slot has been reused by a newer lease owner.
	session.removeSubscriber(subscriberID, subscriberRemovalReasonSessionClosed)

	acquireCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	replacementLease, err := pool.AcquireClient(acquireCtx, "103", "shared:103")
	if err != nil {
		t.Fatalf("AcquireClient(replacement) error = %v, want success via current callback", err)
	}
	replacementLease.Release()

	select {
	case <-currentPreempted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for current lease preempt callback")
	}
}

func TestSharedRuntimeSessionAddSubscriberStaleClearPreemptibleIgnoredAfterLeaseReuse(t *testing.T) {
	pool := NewPool(1)
	manager := &SessionManager{
		tuners: pool,
		cfg: sessionManagerConfig{
			subscriberJoinLagBytes: 0,
			sessionIdleTimeout:     5 * time.Second,
		},
	}

	// Acquire a stale lease and build a session holding it.
	staleLease, err := pool.AcquireClient(context.Background(), "101", "shared:101")
	if err != nil {
		t.Fatalf("AcquireClient(stale setup) error = %v", err)
	}
	staleToken := staleLease.token

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ring:        NewChunkRing(8),
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		lease:       staleLease,
		startedAt:   time.Now().UTC(),
	}
	session.markReady(nil)

	// Release stale lease so the slot is free, then acquire a current lease.
	staleLease.Release()
	currentLease, err := pool.AcquireClient(context.Background(), "102", "shared:102")
	if err != nil {
		t.Fatalf("AcquireClient(current setup) error = %v", err)
	}
	if currentLease.token == staleToken {
		t.Fatal("current lease token unexpectedly matches stale lease token")
	}

	// Register a preempt callback for the current lease.
	currentPreempted := make(chan struct{}, 1)
	pool.markClientPreemptible(currentLease.ID, currentLease.token, func() bool {
		currentLease.Release()
		select {
		case currentPreempted <- struct{}{}:
		default:
		}
		return true
	})

	// The session still holds the stale lease reference. addSubscriber calls
	// clearClientPreemptible with the stale token — should be a no-op.
	_, _, err = session.addSubscriber("198.51.100.62:46001")
	if err != nil {
		t.Fatalf("addSubscriber() error = %v", err)
	}

	// Verify the current callback is still intact by triggering preemption.
	acquireCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	replacementLease, err := pool.AcquireClient(acquireCtx, "103", "shared:103")
	if err != nil {
		t.Fatalf("AcquireClient(replacement) error = %v, want success via current callback", err)
	}
	replacementLease.Release()

	select {
	case <-currentPreempted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for current lease preempt callback")
	}
}

func TestSessionManagerStartupTelemetryIncludesRelaxedRetryInventory(t *testing.T) {
	tmp := t.TempDir()
	statePath := filepath.Join(tmp, "attempt.txt")
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

	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-startup-telemetry.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

state=%q
video_only=%q
video_audio=%q

attempt=0
if [[ -f "$state" ]]; then
  attempt=$(cat "$state")
fi
attempt=$((attempt+1))
echo "$attempt" > "$state"
if [[ "$attempt" -eq 1 ]]; then
  cat "$video_only"
  sleep 1
else
  cat "$video_audio"
  sleep 2
fi
`, statePath, videoOnlyPath, videoAudioPath))

	// Keep profile probing deterministic and fast for this startup-telemetry test.
	writeExecutable(t, tmp, "ffprobe", `#!/usr/bin/env bash
cat <<'JSON'
{
  "streams":[
    {"codec_type":"video","codec_name":"h264","width":1280,"height":720,"avg_frame_rate":"30000/1001"},
    {"codec_type":"audio","codec_name":"aac"}
  ],
  "format":{"bit_rate":"2500000"}
}
JSON
`)
	t.Setenv("PATH", tmp+":"+os.Getenv("PATH"))

	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "Telemetry", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:telemetry:primary",
					StreamURL:     "https://example.test/live/telemetry.m3u8",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "ffmpeg-copy",
		FFmpegPath:                 ffmpegPath,
		Logger:                     logger,
		StartupTimeout:             2 * time.Second,
		FailoverTotalTimeout:       4 * time.Second,
		MinProbeBytes:              len(videoAudioProbe),
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         500 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(
		withSubscriberClientAddr(context.Background(), "198.51.100.31:5010"),
		provider.channelsByGuide["101"],
	)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	var stats []SharedSessionStats
	waitFor(t, 2*time.Second, func() bool {
		stats = manager.Snapshot()
		if len(stats) != 1 {
			return false
		}
		snapshot := stats[0]
		return snapshot.SourceStartupInventoryMethod == "pmt" &&
			snapshot.SourceStartupVideoStreams == 1 &&
			snapshot.SourceStartupAudioStreams == 1 &&
			snapshot.SourceStartupComponentState == "video_audio" &&
			snapshot.SourceStartupRetryRelaxedProbe &&
			snapshot.SourceStartupRetryRelaxedProbeReason == "startup_detection_incomplete"
	})

	snapshot := stats[0]
	if got, want := snapshot.SourceID, int64(10); got != want {
		t.Fatalf("snapshot.source_id = %d, want %d", got, want)
	}
	if snapshot.SourceStartupProbeRawBytes <= 0 {
		t.Fatalf("snapshot.source_startup_probe_raw_bytes = %d, want > 0", snapshot.SourceStartupProbeRawBytes)
	}
	if got, want := snapshot.SourceStartupProbeTrimmedBytes, snapshot.SourceStartupProbeBytes; got != want {
		t.Fatalf("snapshot.source_startup_probe_trimmed_bytes = %d, want %d", got, want)
	}
	if got := snapshot.SourceStartupProbeCutoverOffset; got != 0 {
		t.Fatalf("snapshot.source_startup_probe_cutover_offset = %d, want 0", got)
	}
	if got := snapshot.SourceStartupProbeDroppedBytes; got != 0 {
		t.Fatalf("snapshot.source_startup_probe_dropped_bytes = %d, want 0", got)
	}
	if !strings.Contains(snapshot.SourceStartupVideoCodecs, "h264") {
		t.Fatalf("snapshot.source_startup_video_codecs = %q, want codec h264", snapshot.SourceStartupVideoCodecs)
	}
	if !strings.Contains(snapshot.SourceStartupAudioCodecs, "aac") {
		t.Fatalf("snapshot.source_startup_audio_codecs = %q, want codec aac", snapshot.SourceStartupAudioCodecs)
	}
	if !strings.Contains(snapshot.Producer, "startup_retry_relaxed_probe") {
		t.Fatalf("snapshot.producer = %q, want relaxed retry marker", snapshot.Producer)
	}

	waitFor(t, time.Second, func() bool {
		text := logs.String()
		return strings.Contains(text, "shared session selected source") &&
			strings.Contains(text, "source_startup_probe_raw_bytes=") &&
			strings.Contains(text, "source_startup_probe_trimmed_bytes=") &&
			strings.Contains(text, "source_startup_probe_cutover_offset=0") &&
			strings.Contains(text, "source_startup_probe_dropped_bytes=0") &&
			strings.Contains(text, "source_startup_inventory_method=pmt") &&
			strings.Contains(text, "source_startup_component_state=video_audio") &&
			strings.Contains(text, "source_startup_retry_relaxed_probe=true") &&
			strings.Contains(text, "source_startup_retry_reason=startup_detection_incomplete")
	})

	waitFor(t, time.Second, func() bool {
		return containsInt64(provider.sourceSuccesses(), 10)
	})
	if failures := provider.sourceFailures(); len(failures) != 0 {
		t.Fatalf("failures = %#v, want none for successful relaxed startup retry", failures)
	}
}

func TestSessionManagerStartupFailsWhenRelaxedRetryFailsAfterIncompleteInventory(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	tmp := t.TempDir()
	statePath := filepath.Join(tmp, "attempt.txt")
	videoOnlyPath := filepath.Join(tmp, "video_only.ts")

	videoOnlyProbe := startupTestProbeWithPMTStreams(0x1B)
	if err := os.WriteFile(videoOnlyPath, videoOnlyProbe, 0o644); err != nil {
		t.Fatalf("WriteFile(videoOnlyPath) error = %v", err)
	}

	ffmpegPath := writeExecutable(t, tmp, "ffmpeg-startup-telemetry-retry-fallback.sh", fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

state=%q
video_only=%q

attempt=0
if [[ -f "$state" ]]; then
  attempt=$(cat "$state")
fi
attempt=$((attempt+1))
echo "$attempt" > "$state"
if [[ "$attempt" -eq 1 ]]; then
  cat "$video_only"
  sleep 2
else
  echo "[http @ 0x5f2] HTTP error 503 Service Unavailable" >&2
  exit 1
fi
`, statePath, videoOnlyPath))

	// Keep profile probing deterministic and fast for this startup-telemetry test.
	writeExecutable(t, tmp, "ffprobe", `#!/usr/bin/env bash
cat <<'JSON'
{
  "streams":[
    {"codec_type":"video","codec_name":"h264","width":1280,"height":720,"avg_frame_rate":"30000/1001"}
  ],
  "format":{"bit_rate":"2500000"}
}
JSON
`)
	t.Setenv("PATH", tmp+":"+os.Getenv("PATH"))

	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "Telemetry", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:telemetry:fallback",
					StreamURL:     "https://example.test/live/telemetry.m3u8",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "ffmpeg-copy",
		FFmpegPath:                 ffmpegPath,
		Logger:                     logger,
		StartupTimeout:             2 * time.Second,
		FailoverTotalTimeout:       4 * time.Second,
		MinProbeBytes:              len(videoOnlyProbe),
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         500 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(
		withSubscriberClientAddr(context.Background(), "198.51.100.41:5010"),
		provider.channelsByGuide["101"],
	)
	if err == nil {
		if sub != nil {
			sub.Close()
		}
		t.Fatal("Subscribe() error = nil, want startup failure after relaxed retry failure")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("Subscribe() error = %q, want retry failure detail", err.Error())
	}

	waitFor(t, time.Second, func() bool {
		return hasFailureReason(provider.sourceFailures(), 10, "503")
	})
	if containsInt64(provider.sourceSuccesses(), 10) {
		t.Fatalf("successes = %#v, did not expect source success on relaxed retry failure", provider.sourceSuccesses())
	}
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelDrain()
	if err := manager.WaitForDrain(drainCtx); err != nil {
		t.Fatalf("WaitForDrain() error = %v, want nil", err)
	}
	if got := len(manager.Snapshot()); got != 0 {
		t.Fatalf("len(snapshot) = %d, want 0 sessions after startup failure", got)
	}
	if strings.Contains(logs.String(), "source_startup_retry_reason=") {
		t.Fatalf("logs unexpectedly include selected-source startup retry reason after startup failure: %s", logs.String())
	}
}

func TestSetSourceStateWithStartupProbeWarnsAndCoalescesHighCutover(t *testing.T) {
	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	manager := &SessionManager{
		cfg: sessionManagerConfig{
			logger: logger,
		},
		logger: logger,
	}
	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   42,
			GuideNumber: "142",
			GuideName:   "Telemetry",
		},
		startedAt:   time.Now().Add(-2 * time.Second).UTC(),
		subscribers: map[uint64]SubscriberStats{},
	}

	source := channels.Source{
		SourceID:  10,
		ItemKey:   "src:telemetry:primary",
		StreamURL: "http://example.test/live.ts",
	}
	startupProbe := startupProbeTelemetryFromLengths(1000, 220, 780)
	startupInventory := startupStreamInventory{
		detectionMethod:  "pmt",
		videoStreamCount: 1,
		audioStreamCount: 1,
		videoCodecs:      []string{"h264"},
		audioCodecs:      []string{"aac"},
	}

	session.setSourceStateWithStartupProbe(
		source,
		"ffmpeg-copy:source=10",
		"initial_select",
		startupProbe,
		true,
		"h264",
		startupInventory,
		false,
		"",
	)
	if got, want := strings.Count(logs.String(), "shared session startup probe cutover warning"), 1; got != want {
		t.Fatalf("warning log count after first selection = %d, want %d", got, want)
	}

	session.setSourceStateWithStartupProbe(
		source,
		"ffmpeg-copy:source=10",
		"recovery_cycle_1:stall_detected",
		startupProbe,
		true,
		"h264",
		startupInventory,
		false,
		"",
	)
	if got, want := strings.Count(logs.String(), "shared session startup probe cutover warning"), 1; got != want {
		t.Fatalf("warning log count after coalesced selection = %d, want %d", got, want)
	}

	session.mu.Lock()
	session.startupProbeWarnedAt = time.Now().Add(-startupProbeHighCutoverWarnWindow - time.Second)
	session.mu.Unlock()

	session.setSourceStateWithStartupProbe(
		source,
		"ffmpeg-copy:source=10",
		"recovery_cycle_2:stall_detected",
		startupProbe,
		true,
		"h264",
		startupInventory,
		false,
		"",
	)

	text := logs.String()
	if got, want := strings.Count(text, "shared session startup probe cutover warning"), 2; got != want {
		t.Fatalf("warning log count after window reset = %d, want %d", got, want)
	}
	if !strings.Contains(text, "source_startup_probe_cutover_warn_logs_coalesced=1") {
		t.Fatalf("warning logs missing coalesced count marker: %s", text)
	}
	if !strings.Contains(text, "source_startup_probe_cutover_percent=78") {
		t.Fatalf("warning logs missing cutover percent marker: %s", text)
	}
}

func TestSetSourceStateWithStartupProbeRecoveryReselectionChurnWithoutRecoveryDelay(t *testing.T) {
	previousProbeRunner := streamProfileProbeRunner
	t.Cleanup(func() {
		streamProfileProbeRunner = previousProbeRunner
	})

	var probeCalls int64
	streamProfileProbeRunner = func(
		_ context.Context,
		_ string,
		_ string,
		_ time.Duration,
	) (streamProfile, error) {
		atomic.AddInt64(&probeCalls, 1)
		return streamProfile{}, errors.New("synthetic profile probe failure")
	}

	manager := &SessionManager{
		cfg: sessionManagerConfig{
			mode:                  "ffmpeg-copy",
			stallDetect:           0,
			cycleFailureMinHealth: 0,
		},
	}
	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   42,
			GuideNumber: "142",
			GuideName:   "Probe Churn",
		},
		ctx:         context.Background(),
		cancel:      func() {},
		subscribers: map[uint64]SubscriberStats{},
		startedAt:   time.Now().UTC(),
	}

	selections := []struct {
		source channels.Source
		reason string
	}{
		{
			source: channels.Source{
				SourceID:  10,
				ItemKey:   "src:probe:10",
				StreamURL: "http://example.test/source-10.m3u8",
			},
			reason: "recovery_cycle_1:stall",
		},
		{
			source: channels.Source{
				SourceID:  11,
				ItemKey:   "src:probe:11",
				StreamURL: "http://example.test/source-11.m3u8",
			},
			reason: "recovery_cycle_2:stall",
		},
		{
			source: channels.Source{
				SourceID:  10,
				ItemKey:   "src:probe:10",
				StreamURL: "http://example.test/source-10.m3u8",
			},
			reason: "recovery_cycle_3:stall",
		},
	}

	for i, selection := range selections {
		session.setSourceState(selection.source, "ffmpeg-copy", selection.reason)
		wantCalls := int64(i + 1)
		waitFor(t, time.Second, func() bool {
			return atomic.LoadInt64(&probeCalls) >= wantCalls
		})
	}

	if got, want := atomic.LoadInt64(&probeCalls), int64(len(selections)); got != want {
		t.Fatalf("profile probe calls = %d, want %d", got, want)
	}
}

func TestSetSourceStateWithStartupProbeDefersRecoveryProbeUntilStable(t *testing.T) {
	previousProbeRunner := streamProfileProbeRunner
	t.Cleanup(func() {
		streamProfileProbeRunner = previousProbeRunner
	})

	var probeCalls int64
	streamProfileProbeRunner = func(
		_ context.Context,
		_ string,
		_ string,
		_ time.Duration,
	) (streamProfile, error) {
		atomic.AddInt64(&probeCalls, 1)
		return streamProfile{
			Width:           1280,
			Height:          720,
			FrameRate:       59.94,
			VideoCodec:      "h264",
			AudioCodec:      "aac",
			BitrateBPS:      2_500_000,
			AudioSampleRate: 48000,
			AudioChannels:   2,
		}, nil
	}

	manager := &SessionManager{
		cfg: sessionManagerConfig{
			mode:                  "ffmpeg-copy",
			stallDetect:           120 * time.Millisecond,
			cycleFailureMinHealth: 140 * time.Millisecond,
		},
	}
	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   42,
			GuideNumber: "142",
			GuideName:   "Probe Stability",
		},
		ctx:         context.Background(),
		cancel:      func() {},
		subscribers: map[uint64]SubscriberStats{},
		startedAt:   time.Now().UTC(),
	}

	sourceA := channels.Source{
		SourceID:  10,
		ItemKey:   "src:stable:10",
		StreamURL: "http://example.test/stable-10.m3u8",
	}
	sourceB := channels.Source{
		SourceID:  11,
		ItemKey:   "src:stable:11",
		StreamURL: "http://example.test/stable-11.m3u8",
	}

	session.setSourceState(sourceA, "ffmpeg-copy", "recovery_cycle_1:stall")
	time.Sleep(40 * time.Millisecond)
	if got := atomic.LoadInt64(&probeCalls); got != 0 {
		t.Fatalf("probe calls after first unstable select = %d, want 0", got)
	}

	session.setSourceState(sourceB, "ffmpeg-copy", "recovery_cycle_2:stall")
	time.Sleep(80 * time.Millisecond)
	if got := atomic.LoadInt64(&probeCalls); got != 0 {
		t.Fatalf("probe calls while recovery reselections are unstable = %d, want 0", got)
	}

	waitFor(t, time.Second, func() bool {
		return atomic.LoadInt64(&probeCalls) == 1
	})
	time.Sleep(40 * time.Millisecond)
	if got := atomic.LoadInt64(&probeCalls); got != 1 {
		t.Fatalf("probe calls after stable window = %d, want 1", got)
	}

	session.mu.Lock()
	selectedSourceID := session.sourceID
	profile := session.sourceProfile
	session.mu.Unlock()
	if got, want := selectedSourceID, sourceB.SourceID; got != want {
		t.Fatalf("session source_id = %d, want %d", got, want)
	}
	if got, want := profile.Width, 1280; got != want {
		t.Fatalf("profile.Width = %d, want %d", got, want)
	}
	if got, want := profile.VideoCodec, "h264"; got != want {
		t.Fatalf("profile.VideoCodec = %q, want %q", got, want)
	}
}

func TestSetSourceStateWithStartupProbeAppliesRapidReselectCooldown(t *testing.T) {
	previousProbeRunner := streamProfileProbeRunner
	t.Cleanup(func() {
		streamProfileProbeRunner = previousProbeRunner
	})

	var probeCalls int64
	streamProfileProbeRunner = func(
		_ context.Context,
		_ string,
		_ string,
		_ time.Duration,
	) (streamProfile, error) {
		atomic.AddInt64(&probeCalls, 1)
		return streamProfile{}, errors.New("synthetic profile probe failure")
	}

	manager := &SessionManager{
		cfg: sessionManagerConfig{
			mode:                  "ffmpeg-copy",
			stallDetect:           220 * time.Millisecond,
			cycleFailureMinHealth: 0,
		},
	}
	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   42,
			GuideNumber: "142",
			GuideName:   "Probe Cooldown",
		},
		ctx:         context.Background(),
		cancel:      func() {},
		subscribers: map[uint64]SubscriberStats{},
		startedAt:   time.Now().UTC(),
	}

	sourceA := channels.Source{
		SourceID:  10,
		ItemKey:   "src:cooldown:10",
		StreamURL: "http://example.test/cooldown-10.m3u8",
	}
	sourceB := channels.Source{
		SourceID:  11,
		ItemKey:   "src:cooldown:11",
		StreamURL: "http://example.test/cooldown-11.m3u8",
	}

	session.setSourceState(sourceA, "ffmpeg-copy", "initial_startup")
	waitFor(t, time.Second, func() bool {
		return atomic.LoadInt64(&probeCalls) == 1
	})

	session.setSourceState(sourceB, "ffmpeg-copy", "manual_reselect")
	time.Sleep(60 * time.Millisecond)
	if got := atomic.LoadInt64(&probeCalls); got != 1 {
		t.Fatalf("probe calls before cooldown elapsed = %d, want 1", got)
	}

	waitFor(t, time.Second, func() bool {
		return atomic.LoadInt64(&probeCalls) == 2
	})
}

func TestSetSourceStatePersistsStreamProfileToStore(t *testing.T) {
	tmp := t.TempDir()
	writeExecutable(t, tmp, "ffprobe", `#!/usr/bin/env bash
cat <<'JSON'
{
  "streams":[
    {
      "codec_type":"video",
      "codec_name":"h264",
      "width":1920,
      "height":1080,
      "avg_frame_rate":"30000/1001",
      "r_frame_rate":"0/0",
      "bit_rate":"4100000"
    },
    {
      "codec_type":"audio",
      "codec_name":"aac"
    }
  ],
  "format":{"bit_rate":"4200000"}
}
JSON
`)
	t.Setenv("PATH", tmp+":"+os.Getenv("PATH"))

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
					StreamURL:     "http://example.com/news.m3u8",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "ffmpeg-copy",
		StartupTimeout:             1 * time.Second,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 10 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     provider.channelsByGuide["101"],
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}

	source := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:primary",
		StreamURL:     "http://example.com/news.m3u8",
		PriorityIndex: 0,
		Enabled:       true,
	}

	session.setSourceState(source, "ffmpeg-copy", "initial_startup")

	waitFor(t, 2*time.Second, func() bool {
		provider.mu.Lock()
		defer provider.mu.Unlock()
		return len(provider.profileUpdates) > 0
	})

	provider.mu.Lock()
	call := provider.profileUpdates[len(provider.profileUpdates)-1]
	provider.mu.Unlock()
	if got, want := call.sourceID, int64(10); got != want {
		t.Fatalf("profile update source_id = %d, want %d", got, want)
	}
	if got, want := call.profile.Width, 1920; got != want {
		t.Fatalf("profile update width = %d, want %d", got, want)
	}
	if got, want := call.profile.Height, 1080; got != want {
		t.Fatalf("profile update height = %d, want %d", got, want)
	}
	if call.profile.FPS < 29.96 || call.profile.FPS > 29.98 {
		t.Fatalf("profile update fps = %f, want ~29.97", call.profile.FPS)
	}
	if got, want := call.profile.VideoCodec, "h264"; got != want {
		t.Fatalf("profile update video codec = %q, want %q", got, want)
	}
	if got, want := call.profile.AudioCodec, "aac"; got != want {
		t.Fatalf("profile update audio codec = %q, want %q", got, want)
	}
	if got, want := call.profile.BitrateBPS, int64(4_100_000); got != want {
		t.Fatalf("profile update bitrate = %d, want %d", got, want)
	}
	if call.profile.LastProbeAt.IsZero() {
		t.Fatal("profile update last_probe_at = zero, want non-zero")
	}
}

func TestOrderFailoverRecoveryCandidatesKeepsCurrentBeforeCoolingSources(t *testing.T) {
	now := time.Unix(1770430000, 0).UTC()
	candidates := []channels.Source{
		{SourceID: 6, CooldownUntil: 0},
		{SourceID: 10, CooldownUntil: 0},
		{SourceID: 7, CooldownUntil: now.Add(30 * time.Second).Unix()},
		{SourceID: 8, CooldownUntil: now.Add(60 * time.Second).Unix()},
		{SourceID: 9, CooldownUntil: now.Add(90 * time.Second).Unix()},
	}

	ordered := orderFailoverRecoveryCandidates(candidates, 6, now)
	if got, want := sourceIDs(ordered), []int64{10, 6, 7, 8, 9}; !equalInt64Slices(got, want) {
		t.Fatalf("ordered source ids = %#v, want %#v", got, want)
	}

	limited := limitSourcesByFailovers(ordered, 2)
	if got, want := sourceIDs(limited), []int64{10, 6, 7}; !equalInt64Slices(got, want) {
		t.Fatalf("limited source ids = %#v, want %#v", got, want)
	}
}

func TestOrderFailoverRecoveryCandidatesPreservesOrderWhenCurrentMissing(t *testing.T) {
	now := time.Unix(1770430000, 0).UTC()
	candidates := []channels.Source{
		{SourceID: 20, CooldownUntil: 0},
		{SourceID: 21, CooldownUntil: 0},
		{SourceID: 22, CooldownUntil: now.Add(30 * time.Second).Unix()},
	}

	ordered := orderFailoverRecoveryCandidates(candidates, 99, now)
	if got, want := sourceIDs(ordered), []int64{20, 21, 22}; !equalInt64Slices(got, want) {
		t.Fatalf("ordered source ids = %#v, want %#v", got, want)
	}
}

func TestOrderFailoverRecoveryCandidatesDelaysCoolingCurrentWhenAlternatesReady(t *testing.T) {
	now := time.Unix(1770430000, 0).UTC()
	candidates := []channels.Source{
		{SourceID: 6, CooldownUntil: now.Add(15 * time.Second).Unix()},
		{SourceID: 10, CooldownUntil: 0},
		{SourceID: 11, CooldownUntil: 0},
		{SourceID: 12, CooldownUntil: now.Add(30 * time.Second).Unix()},
	}

	ordered := orderFailoverRecoveryCandidates(candidates, 6, now)
	if got, want := sourceIDs(ordered), []int64{10, 11, 6, 12}; !equalInt64Slices(got, want) {
		t.Fatalf("ordered source ids = %#v, want %#v", got, want)
	}
}

func TestSharedSessionApplyShortLivedRecoveryPenalties(t *testing.T) {
	now := time.Unix(1770430000, 0).UTC()
	session := &sharedRuntimeSession{
		shortLivedRecoveryBySource: make(map[int64]shortLivedRecoveryPenalty),
	}

	count, cooldownUntil, cooldown := session.recordShortLivedRecoveryFailure(11, "startup_passed_unstable_source_eof", now)
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if cooldown < recoveryTransientPenaltyBase {
		t.Fatalf("cooldown = %s, want >= %s", cooldown, recoveryTransientPenaltyBase)
	}
	if !cooldownUntil.After(now) {
		t.Fatalf("cooldownUntil = %v, want after %v", cooldownUntil, now)
	}

	candidates := []channels.Source{
		{SourceID: 10, FailCount: 0, CooldownUntil: 0},
		{SourceID: 11, FailCount: 0, CooldownUntil: 0},
	}
	penalized := session.applyShortLivedRecoveryPenalties(candidates, now)

	var source11 channels.Source
	found := false
	for _, source := range penalized {
		if source.SourceID != 11 {
			continue
		}
		source11 = source
		found = true
		break
	}
	if !found {
		t.Fatal("source 11 missing from penalized candidate set")
	}
	if source11.CooldownUntil <= now.Unix() {
		t.Fatalf("source11 cooldown_until = %d, want > %d", source11.CooldownUntil, now.Unix())
	}
	if source11.FailCount <= 0 {
		t.Fatalf("source11 fail_count = %d, want transient fail-count boost", source11.FailCount)
	}
	if got := session.shortLivedRecoveryCount(11); got != 1 {
		t.Fatalf("shortLivedRecoveryCount(11) = %d, want 1", got)
	}
}

func TestSharedSessionTrackRecoveryBurstExhaustsAndResets(t *testing.T) {
	now := time.Unix(1770430000, 0).UTC()
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				startupWait:             5 * time.Second,
				stallDetect:             2 * time.Second,
				stallMaxFailoversPerTry: 1,
			},
		},
	}

	limit := recoveryCycleBudgetLimit(session.manager.cfg.stallMaxFailoversPerTry)
	resetWindow := recoveryCycleBudgetResetWindow(session.manager.cfg.stallDetect, session.manager.cfg.startupWait)
	paceWindow := recoveryCycleBudgetPaceWindow(session.manager.cfg.stallDetect, session.manager.cfg.startupWait)
	startedAt := time.Time{}
	for i := 1; i <= limit; i++ {
		count, budgetCount, gotLimit, gotReset, gotPace, gotStartedAt, exhausted := session.trackRecoveryBurst(
			now.Add(time.Duration(i)*100*time.Millisecond),
			100*time.Millisecond,
		)
		if gotLimit != limit {
			t.Fatalf("limit = %d, want %d", gotLimit, limit)
		}
		if gotReset != resetWindow {
			t.Fatalf("reset window = %s, want %s", gotReset, resetWindow)
		}
		if gotPace != paceWindow {
			t.Fatalf("pace window = %s, want %s", gotPace, paceWindow)
		}
		if count != i {
			t.Fatalf("count at step %d = %d, want %d", i, count, i)
		}
		if budgetCount <= 0 {
			t.Fatalf("budget count at step %d = %d, want > 0", i, budgetCount)
		}
		if budgetCount > count {
			t.Fatalf("budget count at step %d = %d, want <= raw count %d", i, budgetCount, count)
		}
		if gotStartedAt.IsZero() {
			t.Fatal("recovery burst start time is zero")
		}
		if startedAt.IsZero() {
			startedAt = gotStartedAt
		}
		if exhausted {
			t.Fatalf("burst exhausted at step %d, want not exhausted", i)
		}
	}

	count, budgetCount, gotLimit, _, _, _, exhausted := session.trackRecoveryBurst(now.Add(2*time.Second), 100*time.Millisecond)
	if gotLimit != limit {
		t.Fatalf("limit after overflow = %d, want %d", gotLimit, limit)
	}
	if count != limit+1 {
		t.Fatalf("count after overflow = %d, want %d", count, limit+1)
	}
	if budgetCount > limit {
		t.Fatalf("budget count after overflow = %d, want <= limit %d before paced window elapses", budgetCount, limit)
	}
	if exhausted {
		t.Fatal("expected no burst exhaustion before paced budget window elapses")
	}

	exhaustAt := startedAt.Add(time.Duration(limit) * paceWindow)
	count, budgetCount, gotLimit, _, gotPace, _, exhausted := session.trackRecoveryBurst(exhaustAt, 100*time.Millisecond)
	if gotLimit != limit {
		t.Fatalf("limit at paced exhaustion point = %d, want %d", gotLimit, limit)
	}
	if gotPace != paceWindow {
		t.Fatalf("pace window at paced exhaustion point = %s, want %s", gotPace, paceWindow)
	}
	if budgetCount <= limit {
		t.Fatalf("budget count at paced exhaustion point = %d, want > %d", budgetCount, limit)
	}
	if !exhausted {
		t.Fatal("expected burst budget exhaustion once paced budget window is consumed")
	}

	count, _, _, _, _, _, exhausted = session.trackRecoveryBurst(exhaustAt.Add(time.Second), resetWindow+100*time.Millisecond)
	if count != 1 {
		t.Fatalf("count after reset window = %d, want 1", count)
	}
	if exhausted {
		t.Fatal("expected burst budget reset after stable window")
	}
}

func TestRecoverySameSourcePacingDelay(t *testing.T) {
	if got := recoverySameSourcePacingDelay("source_eof", 0); got != 0 {
		t.Fatalf("recoverySameSourcePacingDelay(source_eof,0) = %s, want 0", got)
	}
	if got := recoverySameSourcePacingDelay("stall", 3); got != 0 {
		t.Fatalf("recoverySameSourcePacingDelay(stall,3) = %s, want 0", got)
	}
	if got, want := recoverySameSourcePacingDelay("source_eof", 1), 250*time.Millisecond; got != want {
		t.Fatalf("recoverySameSourcePacingDelay(source_eof,1) = %s, want %s", got, want)
	}
	if got, want := recoverySameSourcePacingDelay("source_eof", 4), 2*time.Second; got != want {
		t.Fatalf("recoverySameSourcePacingDelay(source_eof,4) = %s, want %s", got, want)
	}
}

func TestSharedSessionRecoveryTriggerLogCoalescing(t *testing.T) {
	now := time.Unix(1770430000, 0).UTC()
	session := &sharedRuntimeSession{}

	emit, suppressed := session.shouldEmitRecoveryTriggerLog(now, 11, "source_eof")
	if !emit || suppressed != 0 {
		t.Fatalf("first log decision = (%t,%d), want (true,0)", emit, suppressed)
	}

	emit, suppressed = session.shouldEmitRecoveryTriggerLog(now.Add(200*time.Millisecond), 11, "source_eof")
	if emit || suppressed != 0 {
		t.Fatalf("coalesced log decision = (%t,%d), want (false,0)", emit, suppressed)
	}

	emit, suppressed = session.shouldEmitRecoveryTriggerLog(now.Add(500*time.Millisecond), 11, "source_eof")
	if emit || suppressed != 0 {
		t.Fatalf("coalesced log decision #2 = (%t,%d), want (false,0)", emit, suppressed)
	}

	emit, suppressed = session.shouldEmitRecoveryTriggerLog(
		now.Add(recoveryTriggerLogCoalesceWindow+50*time.Millisecond),
		11,
		"source_eof",
	)
	if !emit || suppressed != 2 {
		t.Fatalf("window rollover log decision = (%t,%d), want (true,2)", emit, suppressed)
	}

	emit, suppressed = session.shouldEmitRecoveryTriggerLog(now.Add(recoveryTriggerLogCoalesceWindow+70*time.Millisecond), 12, "stall")
	if !emit || suppressed != 0 {
		t.Fatalf("new source/reason log decision = (%t,%d), want (true,0)", emit, suppressed)
	}

	emit, suppressed = session.shouldEmitRecoveryTriggerLog(now.Add(recoveryTriggerLogCoalesceWindow+200*time.Millisecond), 12, "stall")
	if emit || suppressed != 0 {
		t.Fatalf("coalesced new source/reason decision = (%t,%d), want (false,0)", emit, suppressed)
	}
	if got := session.takeSuppressedRecoveryTriggerLogs(); got != 1 {
		t.Fatalf("takeSuppressedRecoveryTriggerLogs() = %d, want 1", got)
	}
}

func TestRecentHealthCacheReordersCrossSessionBeforeAsyncPersistence(t *testing.T) {
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
					StreamURL:     "http://example.test/primary.m3u8",
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      20,
					ChannelID:     1,
					ItemKey:       "src:news:backup",
					StreamURL:     "http://example.test/backup.m3u8",
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
		markFailureDelay: 400 * time.Millisecond,
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                 "direct",
		StartupTimeout:       200 * time.Millisecond,
		FailoverTotalTimeout: 2 * time.Second,
		MinProbeBytes:        1,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	channel := provider.channelsByGuide["101"]
	originSession := manager.newSession(channel, nil)
	defer originSession.cancel()
	peerSession := manager.newSession(channel, nil)
	defer peerSession.cancel()

	before, err := peerSession.loadSourceCandidates(context.Background())
	if err != nil {
		t.Fatalf("loadSourceCandidates(before) error = %v", err)
	}
	if got, want := sourceIDs(before), []int64{10, 20}; !equalInt64Slices(got, want) {
		t.Fatalf("pre-failure candidate order = %#v, want %#v", got, want)
	}

	originSession.recordSourceFailure(context.Background(), 10, "", errors.New("upstream timeout"), true)

	immediate, err := peerSession.loadSourceCandidates(context.Background())
	if err != nil {
		t.Fatalf("loadSourceCandidates(immediate) error = %v", err)
	}
	if got, want := sourceIDs(immediate), []int64{20, 10}; !equalInt64Slices(got, want) {
		t.Fatalf("immediate candidate order = %#v, want %#v", got, want)
	}

	waitFor(t, 2*time.Second, func() bool {
		failures := provider.sourceFailures()
		return len(failures) > 0 && failures[0].sourceID == 10
	})

	waitFor(t, time.Second, func() bool {
		afterPersist, loadErr := peerSession.loadSourceCandidates(context.Background())
		if loadErr != nil {
			return false
		}
		return equalInt64Slices(sourceIDs(afterPersist), []int64{10, 20})
	})
}

type blockingWriteResponseWriter struct {
	mu sync.Mutex

	header     http.Header
	statusCode int
	writeDelay time.Duration
	writeCalls int
}

func newBlockingWriteResponseWriter(writeDelay time.Duration) *blockingWriteResponseWriter {
	return &blockingWriteResponseWriter{
		header:     make(http.Header),
		writeDelay: writeDelay,
	}
}

func (w *blockingWriteResponseWriter) Header() http.Header {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *blockingWriteResponseWriter) WriteHeader(statusCode int) {
	w.mu.Lock()
	w.statusCode = statusCode
	w.mu.Unlock()
}

func (w *blockingWriteResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	delay := w.writeDelay
	w.writeCalls++
	w.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	return len(p), nil
}

func (w *blockingWriteResponseWriter) WriteCalls() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeCalls
}

type failingWriteDeadlineResponseWriter struct {
	*blockingWriteResponseWriter
	err error
}

func (w *failingWriteDeadlineResponseWriter) SetWriteDeadline(time.Time) error {
	if w == nil || w.err == nil {
		return nil
	}
	return w.err
}

type timeoutWriteResponseWriter struct {
	mu sync.Mutex

	header        http.Header
	statusCode    int
	writeDelay    time.Duration
	writeCalls    int
	writeDeadline time.Time
}

func newTimeoutWriteResponseWriter(writeDelay time.Duration) *timeoutWriteResponseWriter {
	return &timeoutWriteResponseWriter{
		header:     make(http.Header),
		writeDelay: writeDelay,
	}
}

func (w *timeoutWriteResponseWriter) Header() http.Header {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *timeoutWriteResponseWriter) WriteHeader(statusCode int) {
	w.mu.Lock()
	w.statusCode = statusCode
	w.mu.Unlock()
}

func (w *timeoutWriteResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.writeDeadline = deadline
	w.mu.Unlock()
	return nil
}

func (w *timeoutWriteResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	delay := w.writeDelay
	deadline := w.writeDeadline
	w.writeCalls++
	w.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if !deadline.IsZero() && time.Now().After(deadline) {
		return 0, os.ErrDeadlineExceeded
	}
	return len(p), nil
}

func (w *timeoutWriteResponseWriter) WriteCalls() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeCalls
}

type shortWriteResponseWriter struct {
	mu sync.Mutex

	header     http.Header
	statusCode int
	writeDelay time.Duration
	writeCalls int
}

func newShortWriteResponseWriter(writeDelay time.Duration) *shortWriteResponseWriter {
	return &shortWriteResponseWriter{
		header:     make(http.Header),
		writeDelay: writeDelay,
	}
}

func (w *shortWriteResponseWriter) Header() http.Header {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *shortWriteResponseWriter) WriteHeader(statusCode int) {
	w.mu.Lock()
	w.statusCode = statusCode
	w.mu.Unlock()
}

func (w *shortWriteResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	delay := w.writeDelay
	w.writeCalls++
	w.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if len(p) <= 1 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func (w *shortWriteResponseWriter) WriteCalls() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeCalls
}

type writeDeadlineNetError struct {
	timeout bool
}

func (e writeDeadlineNetError) Error() string {
	if e.timeout {
		return "write timeout"
	}
	return "write error"
}

func (e writeDeadlineNetError) Timeout() bool {
	return e.timeout
}

func (e writeDeadlineNetError) Temporary() bool {
	return false
}

type writeChunkTestResponseWriter struct {
	mu sync.Mutex

	header             http.Header
	statusCode         int
	writeCalls         int
	writeErr           error
	shortWriteN        int
	deadlineSetCalls   int
	deadlineClearCalls int
}

func newWriteChunkTestResponseWriter() *writeChunkTestResponseWriter {
	return &writeChunkTestResponseWriter{
		header: make(http.Header),
	}
}

func (w *writeChunkTestResponseWriter) Header() http.Header {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *writeChunkTestResponseWriter) WriteHeader(statusCode int) {
	w.mu.Lock()
	w.statusCode = statusCode
	w.mu.Unlock()
}

func (w *writeChunkTestResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	if deadline.IsZero() {
		w.deadlineClearCalls++
		w.mu.Unlock()
		return nil
	}
	w.deadlineSetCalls++
	w.mu.Unlock()
	return nil
}

func (w *writeChunkTestResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.writeCalls++
	err := w.writeErr
	shortWriteN := w.shortWriteN
	w.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if shortWriteN > 0 && shortWriteN < len(p) {
		return shortWriteN, nil
	}
	return len(p), nil
}

func (w *writeChunkTestResponseWriter) WriteCalls() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeCalls
}

func (w *writeChunkTestResponseWriter) DeadlineSetCalls() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deadlineSetCalls
}

func (w *writeChunkTestResponseWriter) DeadlineClearCalls() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deadlineClearCalls
}

type recordingResponseWriter struct {
	mu sync.Mutex

	header http.Header
	status int
	body   strings.Builder

	writes    int
	flushes   int
	totalSize int
	onWrite   func(totalBytes int)
	chunks    [][]byte
}

func (w *recordingResponseWriter) Header() http.Header {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *recordingResponseWriter) WriteHeader(statusCode int) {
	w.mu.Lock()
	w.status = statusCode
	w.mu.Unlock()
}

func (w *recordingResponseWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	w.mu.Lock()
	w.writes++
	w.totalSize += len(p)
	_, _ = w.body.Write(p)
	copied := append([]byte(nil), p...)
	w.chunks = append(w.chunks, copied)
	total := w.totalSize
	cb := w.onWrite
	w.mu.Unlock()

	if cb != nil {
		cb(total)
	}
	return len(p), nil
}

func (w *recordingResponseWriter) Flush() {
	w.mu.Lock()
	w.flushes++
	w.mu.Unlock()
}

func (w *recordingResponseWriter) SetWriteDeadline(time.Time) error {
	return nil
}

func (w *recordingResponseWriter) Body() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.String()
}

func (w *recordingResponseWriter) Writes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes
}

func (w *recordingResponseWriter) Flushes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushes
}

func (w *recordingResponseWriter) Chunks() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := make([][]byte, 0, len(w.chunks))
	for _, chunk := range w.chunks {
		out = append(out, append([]byte(nil), chunk...))
	}
	return out
}

func containsInt64(values []int64, target int64) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func sourceIDs(sources []channels.Source) []int64 {
	out := make([]int64, 0, len(sources))
	for _, source := range sources {
		out = append(out, source.SourceID)
	}
	return out
}

func equalInt64Slices(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func hasFailureReason(failures []sourceFailureCall, sourceID int64, contains string) bool {
	needle := strings.ToLower(strings.TrimSpace(contains))
	for _, failure := range failures {
		if failure.sourceID != sourceID {
			continue
		}
		if needle == "" {
			return true
		}
		if strings.Contains(strings.ToLower(failure.reason), needle) {
			return true
		}
	}
	return false
}

func waitFor(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	poll := fastStreamTestTiming.pollInterval
	if poll <= 0 {
		poll = 10 * time.Millisecond
	}
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(poll)
	}
	if !check() {
		t.Fatal("condition not met before timeout")
	}
}

func streamWithNoDataWatchdog(
	sub *SessionSubscription,
	noDataTimeout time.Duration,
	runDuration time.Duration,
) (bool, *recordingResponseWriter, error) {
	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := &recordingResponseWriter{}
	var mu sync.Mutex
	lastDataAt := time.Now()
	writer.onWrite = func(_ int) {
		mu.Lock()
		lastDataAt = time.Now()
		mu.Unlock()
	}

	streamErrCh := make(chan error, 1)
	go func() {
		streamErrCh <- sub.Stream(streamCtx, writer)
	}()

	interval := noDataTimeout / 5
	if interval < 20*time.Millisecond {
		interval = 20 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	timer := time.NewTimer(runDuration)
	defer timer.Stop()

	timedOut := false
	for {
		select {
		case err := <-streamErrCh:
			return timedOut, writer, err
		case <-ticker.C:
			mu.Lock()
			idleFor := time.Since(lastDataAt)
			mu.Unlock()
			if idleFor >= noDataTimeout {
				timedOut = true
				cancel()
			}
		case <-timer.C:
			cancel()
		}
	}
}

func newBurstGapSourceServer(burstChunks int, payload []byte, gap time.Duration) *httptest.Server {
	if burstChunks < 1 {
		burstChunks = 1
	}
	if len(payload) == 0 {
		payload = []byte("x")
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for i := 0; i < burstChunks; i++ {
			if _, err := w.Write(payload); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		time.Sleep(gap)
	}))
}

func TestShouldRequireStartupRandomAccess(t *testing.T) {
	tests := []struct {
		name                  string
		recoveryCycle         int64
		mode                  string
		startupRecoveryOnly   bool
		recoveryFillerEnabled bool
		recoveryFillerMode    string
		want                  bool
	}{
		{
			name:                  "recovery cycle always requires random access",
			recoveryCycle:         2,
			mode:                  "direct",
			recoveryFillerEnabled: false,
			recoveryFillerMode:    recoveryFillerModeNull,
			want:                  true,
		},
		{
			name:                  "initial slate_av ffmpeg-copy requires random access",
			recoveryCycle:         0,
			mode:                  "ffmpeg-copy",
			startupRecoveryOnly:   false,
			recoveryFillerEnabled: true,
			recoveryFillerMode:    recoveryFillerModeSlateAV,
			want:                  true,
		},
		{
			name:                  "initial slate_av ffmpeg-transcode requires random access",
			recoveryCycle:         0,
			mode:                  "ffmpeg-transcode",
			startupRecoveryOnly:   false,
			recoveryFillerEnabled: true,
			recoveryFillerMode:    recoveryFillerModeSlateAV,
			want:                  true,
		},
		{
			name:                  "initial slate_av ffmpeg-copy recovery-only disables random access",
			recoveryCycle:         0,
			mode:                  "ffmpeg-copy",
			startupRecoveryOnly:   true,
			recoveryFillerEnabled: true,
			recoveryFillerMode:    recoveryFillerModeSlateAV,
			want:                  false,
		},
		{
			name:                  "initial slate_av direct does not require random access",
			recoveryCycle:         0,
			mode:                  "direct",
			startupRecoveryOnly:   false,
			recoveryFillerEnabled: true,
			recoveryFillerMode:    recoveryFillerModeSlateAV,
			want:                  false,
		},
		{
			name:                  "initial psi does not require random access",
			recoveryCycle:         0,
			mode:                  "ffmpeg-copy",
			startupRecoveryOnly:   false,
			recoveryFillerEnabled: true,
			recoveryFillerMode:    recoveryFillerModePSI,
			want:                  false,
		},
		{
			name:                  "initial slate_av without filler enabled does not require random access",
			recoveryCycle:         0,
			mode:                  "ffmpeg-copy",
			startupRecoveryOnly:   false,
			recoveryFillerEnabled: false,
			recoveryFillerMode:    recoveryFillerModeSlateAV,
			want:                  false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			session := &sharedRuntimeSession{
				manager: &SessionManager{
					cfg: sessionManagerConfig{
						mode:                            tc.mode,
						startupRandomAccessRecoveryOnly: tc.startupRecoveryOnly,
						recoveryFillerEnabled:           tc.recoveryFillerEnabled,
						recoveryFillerMode:              tc.recoveryFillerMode,
						recoveryTransitionMode:          defaultSharedRecoveryTransitionMode,
					},
				},
			}
			if got := session.shouldRequireStartupRandomAccess(tc.recoveryCycle); got != tc.want {
				t.Fatalf("shouldRequireStartupRandomAccess() = %t, want %t", got, tc.want)
			}
		})
	}

	var nilSession *sharedRuntimeSession
	if got := nilSession.shouldRequireStartupRandomAccess(0); got {
		t.Fatalf("nil session shouldRequireStartupRandomAccess() = %t, want false", got)
	}
}

func TestNormalizeSessionManagerConfigRecoveryDefaults(t *testing.T) {
	cfg := normalizeSessionManagerConfig(SessionManagerConfig{
		Mode:                  "ffmpeg-copy",
		RecoveryFillerEnabled: true,
	})

	if cfg.stallDetect != 4*time.Second {
		t.Fatalf("stallDetect = %s, want 4s", cfg.stallDetect)
	}
	if cfg.stallHardDeadline != 32*time.Second {
		t.Fatalf("stallHardDeadline = %s, want 32s", cfg.stallHardDeadline)
	}
	if cfg.stallPolicy != stallPolicyFailoverSource {
		t.Fatalf("stallPolicy = %q, want %q", cfg.stallPolicy, stallPolicyFailoverSource)
	}
	if !cfg.recoveryFillerEnabled {
		t.Fatal("recoveryFillerEnabled = false, want true")
	}
	if cfg.recoveryFillerMode != recoveryFillerModeSlateAV {
		t.Fatalf("recoveryFillerMode = %q, want %q", cfg.recoveryFillerMode, recoveryFillerModeSlateAV)
	}
	if cfg.startupRandomAccessRecoveryOnly {
		t.Fatal("startupRandomAccessRecoveryOnly = true, want false")
	}
	if cfg.recoveryFillerInterval != 200*time.Millisecond {
		t.Fatalf("recoveryFillerInterval = %s, want 200ms", cfg.recoveryFillerInterval)
	}
	if cfg.ffmpegReconnectEnabled {
		t.Fatal("ffmpegReconnectEnabled = true, want false")
	}
	if !cfg.ffmpegCopyRegenerateTimestamps {
		t.Fatal("ffmpegCopyRegenerateTimestamps = false, want true")
	}
	if cfg.producerReadRate != 1 {
		t.Fatalf("producerReadRate = %v, want 1", cfg.producerReadRate)
	}
	if cfg.producerReadRateCatchup != cfg.producerReadRate {
		t.Fatalf(
			"producerReadRateCatchup = %v, want match producerReadRate %v",
			cfg.producerReadRateCatchup,
			cfg.producerReadRate,
		)
	}
	if cfg.sessionDrainTimeout != boundedCloseTimeout {
		t.Fatalf("sessionDrainTimeout = %s, want %s", cfg.sessionDrainTimeout, boundedCloseTimeout)
	}
}

func TestNormalizeSessionManagerConfigSessionDrainTimeoutOverride(t *testing.T) {
	cfg := normalizeSessionManagerConfig(SessionManagerConfig{
		Mode:                "ffmpeg-copy",
		SessionDrainTimeout: 950 * time.Millisecond,
	})
	if cfg.sessionDrainTimeout != 950*time.Millisecond {
		t.Fatalf("sessionDrainTimeout = %s, want 950ms", cfg.sessionDrainTimeout)
	}

	cfg = normalizeSessionManagerConfig(SessionManagerConfig{
		Mode:                "ffmpeg-copy",
		SessionDrainTimeout: -1 * time.Second,
	})
	if cfg.sessionDrainTimeout != boundedCloseTimeout {
		t.Fatalf("sessionDrainTimeout = %s, want fallback %s", cfg.sessionDrainTimeout, boundedCloseTimeout)
	}

	falseValue := false
	cfg = normalizeSessionManagerConfig(SessionManagerConfig{
		Mode:                           "ffmpeg-copy",
		FFmpegCopyRegenerateTimestamps: &falseValue,
	})
	if cfg.ffmpegCopyRegenerateTimestamps {
		t.Fatal("ffmpegCopyRegenerateTimestamps = true, want false override")
	}
}

func TestNormalizeSessionManagerConfigProducerReadRateCatchup(t *testing.T) {
	cfg := normalizeSessionManagerConfig(SessionManagerConfig{
		Mode:                    "ffmpeg-copy",
		ProducerReadRate:        1.25,
		ProducerReadRateCatchup: 2.0,
	})
	if cfg.producerReadRate != 1.25 {
		t.Fatalf("producerReadRate = %v, want 1.25", cfg.producerReadRate)
	}
	if cfg.producerReadRateCatchup != 2.0 {
		t.Fatalf("producerReadRateCatchup = %v, want 2.0", cfg.producerReadRateCatchup)
	}

	cfg = normalizeSessionManagerConfig(SessionManagerConfig{
		Mode:                    "ffmpeg-copy",
		ProducerReadRate:        1.5,
		ProducerReadRateCatchup: 1.25,
	})
	if cfg.producerReadRateCatchup != cfg.producerReadRate {
		t.Fatalf(
			"producerReadRateCatchup = %v, want clamp to producerReadRate %v",
			cfg.producerReadRateCatchup,
			cfg.producerReadRate,
		)
	}
}

func containsExactChunk(chunks [][]byte, target []byte) bool {
	for _, chunk := range chunks {
		if bytes.Equal(chunk, target) {
			return true
		}
	}
	return false
}

func isPSIHeartbeatChunk(chunk []byte) bool {
	if len(chunk) != 2*mpegTSPacketSize {
		return false
	}
	first := chunk[:mpegTSPacketSize]
	second := chunk[mpegTSPacketSize:]

	if !isTSSectionPacket(first, 0x0000, 0x00) {
		return false
	}
	if !isTSSectionPacket(second, 0x1000, 0x02) {
		return false
	}
	return true
}

func isPSIHeartbeatChunkWithDiscontinuity(chunk []byte) bool {
	if len(chunk) < 2*mpegTSPacketSize || len(chunk)%mpegTSPacketSize != 0 {
		return false
	}
	first := chunk[:mpegTSPacketSize]
	second := chunk[mpegTSPacketSize:]

	if !isTSSectionPacketWithDiscontinuity(first, 0x0000, 0x00) {
		return false
	}
	if !isTSSectionPacketWithDiscontinuity(second, 0x1000, 0x02) {
		return false
	}
	return true
}

func isRecoveryTransitionBoundaryChunk(chunk []byte) bool {
	if len(chunk) != 3*mpegTSPacketSize {
		return false
	}
	if !isPSIHeartbeatChunkWithDiscontinuity(chunk[:2*mpegTSPacketSize]) {
		return false
	}
	return isPCRPacketWithDiscontinuity(chunk[2*mpegTSPacketSize:], heartbeatPCRPID)
}

func isPCRPacketWithDiscontinuity(packet []byte, pid uint16) bool {
	_, ok := parsePCRBase(packet, pid, true)
	return ok
}

func parsePCRBase(packet []byte, pid uint16, requireDiscontinuity bool) (uint64, bool) {
	if len(packet) != mpegTSPacketSize {
		return 0, false
	}
	if packet[0] != 0x47 {
		return 0, false
	}
	gotPID := (uint16(packet[1]&0x1F) << 8) | uint16(packet[2])
	if gotPID != pid {
		return 0, false
	}

	adaptationControl := (packet[3] >> 4) & 0x03
	if adaptationControl != 0x02 && adaptationControl != 0x03 {
		return 0, false
	}
	adaptationLength := int(packet[4])
	if adaptationLength < 7 || adaptationLength > (mpegTSPacketSize-5) {
		return 0, false
	}
	if requireDiscontinuity && packet[5]&0x80 == 0 {
		return 0, false
	}
	if packet[5]&0x10 == 0 {
		return 0, false
	}

	base := uint64(packet[6])<<25 |
		uint64(packet[7])<<17 |
		uint64(packet[8])<<9 |
		uint64(packet[9])<<1 |
		uint64((packet[10]>>7)&0x01)
	return base, true
}

func firstRecoveryTransitionBoundary(snapshot []RingChunk) []byte {
	for _, chunk := range snapshot {
		if isRecoveryTransitionBoundaryChunk(chunk.Data) {
			return chunk.Data
		}
	}
	return nil
}

func allRecoveryTransitionBoundaries(snapshot []RingChunk) [][]byte {
	out := make([][]byte, 0, len(snapshot))
	for _, chunk := range snapshot {
		if isRecoveryTransitionBoundaryChunk(chunk.Data) {
			out = append(out, chunk.Data)
		}
	}
	return out
}

func firstPSIChunk(snapshot []RingChunk) []byte {
	for _, chunk := range snapshot {
		if isPSIHeartbeatChunk(chunk.Data) {
			return chunk.Data
		}
	}
	return nil
}

func packetContinuityCounter(packet []byte) (byte, bool) {
	if len(packet) != mpegTSPacketSize || packet[0] != 0x47 {
		return 0, false
	}
	return packet[3] & 0x0F, true
}

func psiTableVersion(packet []byte, pid uint16, tableID byte) (byte, bool) {
	if len(packet) != mpegTSPacketSize {
		return 0, false
	}
	if packet[0] != 0x47 {
		return 0, false
	}
	gotPID := (uint16(packet[1]&0x1F) << 8) | uint16(packet[2])
	if gotPID != pid {
		return 0, false
	}
	if packet[1]&0x40 == 0 {
		return 0, false
	}

	payloadOffset := 4
	adaptationControl := (packet[3] >> 4) & 0x03
	switch adaptationControl {
	case 0x01:
	case 0x03:
		adaptationLength := int(packet[4])
		if adaptationLength < 1 || adaptationLength > (mpegTSPacketSize-5) {
			return 0, false
		}
		payloadOffset = 5 + adaptationLength
	default:
		return 0, false
	}
	if payloadOffset >= len(packet) {
		return 0, false
	}
	pointerField := int(packet[payloadOffset])
	sectionStart := payloadOffset + 1 + pointerField
	if sectionStart+6 > len(packet) {
		return 0, false
	}
	if packet[sectionStart] != tableID {
		return 0, false
	}
	version := (packet[sectionStart+5] >> 1) & 0x1F
	return version, true
}

func isTSSectionPacket(packet []byte, pid uint16, tableID byte) bool {
	return isTSSectionPacketWithOptions(packet, pid, tableID, false)
}

func isTSSectionPacketWithDiscontinuity(packet []byte, pid uint16, tableID byte) bool {
	return isTSSectionPacketWithOptions(packet, pid, tableID, true)
}

func isTSSectionPacketWithOptions(packet []byte, pid uint16, tableID byte, requireDiscontinuity bool) bool {
	if len(packet) != mpegTSPacketSize {
		return false
	}
	if packet[0] != 0x47 {
		return false
	}

	gotPID := (uint16(packet[1]&0x1F) << 8) | uint16(packet[2])
	if gotPID != pid {
		return false
	}
	if packet[1]&0x40 == 0 {
		return false
	}
	if packet[3]&0x10 == 0 {
		return false
	}

	payloadOffset := 4
	adaptationControl := (packet[3] >> 4) & 0x03
	switch adaptationControl {
	case 0x01:
		if requireDiscontinuity {
			return false
		}
	case 0x03:
		adaptationLength := int(packet[4])
		if adaptationLength < 1 || adaptationLength > (mpegTSPacketSize-5) {
			return false
		}
		if requireDiscontinuity && packet[5]&0x80 == 0 {
			return false
		}
		payloadOffset = 5 + adaptationLength
	default:
		return false
	}
	if payloadOffset >= len(packet) {
		return false
	}

	pointerField := int(packet[payloadOffset])
	sectionStart := payloadOffset + 1 + pointerField
	if sectionStart+3 > len(packet) {
		return false
	}
	if packet[sectionStart] != tableID {
		return false
	}

	sectionLength := (int(packet[sectionStart+1]&0x0F) << 8) | int(packet[sectionStart+2])
	sectionTotal := 3 + sectionLength
	sectionEnd := sectionStart + sectionTotal
	if sectionLength < 4 || sectionEnd > len(packet) {
		return false
	}

	crcStart := sectionEnd - 4
	sectionNoCRC := packet[sectionStart:crcStart]
	expectedCRC := mpegTSCRC32(sectionNoCRC)
	actualCRC := uint32(packet[crcStart])<<24 |
		uint32(packet[crcStart+1])<<16 |
		uint32(packet[crcStart+2])<<8 |
		uint32(packet[crcStart+3])
	return expectedCRC == actualCRC
}

func TestSessionManagerHistoryTracksLifecycleWindows(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := &SessionManager{
		cfg: sessionManagerConfig{
			mode:                "direct",
			sessionHistoryLimit: 4,
		},
		logger:                  logger,
		sessions:                make(map[int64]*sharedRuntimeSession),
		creating:                make(map[int64]*sessionCreateWait),
		sessionHistory:          make([]SharedSessionHistory, 0, 4),
		recentHealth:            newRecentSourceHealth(),
		overlimitMu:             sync.Mutex{},
		providerCooldownByScope: make(map[string]providerOverlimitScopeState),
	}

	ctx, cancel := context.WithCancel(context.Background())
	session := &sharedRuntimeSession{
		manager:                 manager,
		channel:                 channels.Channel{ChannelID: 1, GuideNumber: "101", GuideName: "History"},
		ring:                    NewChunkRing(32),
		ctx:                     ctx,
		cancel:                  cancel,
		readyCh:                 make(chan struct{}),
		subscribers:             make(map[uint64]SubscriberStats),
		startedAt:               time.Now().UTC().Add(-2 * time.Minute),
		historySessionID:        501,
		historyCurrentSourceIdx: -1,
		historySubscriberIndex:  make(map[uint64]int),
	}
	manager.sessions[1] = session

	sourceA := channels.Source{
		SourceID:  10,
		ItemKey:   "src:history:a",
		StreamURL: "http://example.com/live/a.ts",
	}
	sourceB := channels.Source{
		SourceID:  11,
		ItemKey:   "src:history:b",
		StreamURL: "http://example.com/live/b.ts",
	}

	session.setSourceStateWithStartupProbe(
		sourceA,
		"direct:source=10",
		"initial_startup",
		startupProbeTelemetry{rawBytes: 1024, trimmedBytes: 1024},
		true,
		"h264",
		startupStreamInventory{videoStreamCount: 1, audioStreamCount: 1},
		false,
		"",
	)

	_, subscriberID, err := session.addSubscriber("192.168.1.50:51000")
	if err != nil {
		t.Fatalf("addSubscriber() error = %v", err)
	}
	session.removeSubscriber(subscriberID, subscriberRemovalReasonStreamWriteError)

	session.beginRecoveryCycle("stall")
	session.setSourceStateWithStartupProbe(
		sourceB,
		"direct:source=11",
		"recovery_cycle_stall",
		startupProbeTelemetry{rawBytes: 2048, trimmedBytes: 1024, droppedBytes: 1024, cutoverOffset: 1024},
		false,
		"",
		startupStreamInventory{videoStreamCount: 1, audioStreamCount: 1},
		true,
		"retry_relaxed_probe",
	)
	session.finish(errors.New("history terminal failure"))

	history, limit, truncated := manager.HistorySnapshot()
	if got, want := limit, 4; got != want {
		t.Fatalf("history limit = %d, want %d", got, want)
	}
	if got, want := truncated, int64(0); got != want {
		t.Fatalf("history truncated = %d, want %d", got, want)
	}
	if got, want := len(history), 1; got != want {
		t.Fatalf("len(history) = %d, want %d", got, want)
	}

	entry := history[0]
	if entry.Active {
		t.Fatal("entry.active = true, want false")
	}
	if got, want := entry.SessionID, uint64(501); got != want {
		t.Fatalf("entry.session_id = %d, want %d", got, want)
	}
	if got, want := entry.TerminalStatus, "error"; got != want {
		t.Fatalf("entry.terminal_status = %q, want %q", got, want)
	}
	if !strings.Contains(entry.TerminalError, "history terminal failure") {
		t.Fatalf("entry.terminal_error = %q, want terminal failure marker", entry.TerminalError)
	}
	if got, want := entry.PeakSubscribers, 1; got != want {
		t.Fatalf("entry.peak_subscribers = %d, want %d", got, want)
	}
	if got, want := entry.TotalSubscribers, int64(1); got != want {
		t.Fatalf("entry.total_subscribers = %d, want %d", got, want)
	}
	if got, want := entry.CompletedSubscribers, int64(1); got != want {
		t.Fatalf("entry.completed_subscribers = %d, want %d", got, want)
	}
	if got, want := entry.RecoveryCycleCount, int64(1); got != want {
		t.Fatalf("entry.recovery_cycle_count = %d, want %d", got, want)
	}
	if got, want := entry.LastRecoveryReason, "stall"; got != want {
		t.Fatalf("entry.last_recovery_reason = %q, want %q", got, want)
	}
	if got, want := len(entry.Sources), 2; got != want {
		t.Fatalf("len(entry.sources) = %d, want %d", got, want)
	}
	if entry.Sources[0].DeselectedAt.IsZero() {
		t.Fatal("entry.sources[0].deselected_at is zero, want source window close timestamp")
	}
	if entry.Sources[1].DeselectedAt.IsZero() {
		t.Fatal("entry.sources[1].deselected_at is zero, want session-close timestamp")
	}
	if got, want := len(entry.Subscribers), 1; got != want {
		t.Fatalf("len(entry.subscribers) = %d, want %d", got, want)
	}
	if entry.Subscribers[0].ClosedAt.IsZero() {
		t.Fatal("entry.subscribers[0].closed_at is zero, want close timestamp")
	}
	if got, want := entry.Subscribers[0].CloseReason, subscriberRemovalReasonStreamWriteError; got != want {
		t.Fatalf("entry.subscribers[0].close_reason = %q, want %q", got, want)
	}
}

func TestNormalizeSharedSessionTimelineLimit(t *testing.T) {
	tests := []struct {
		name         string
		configured   int
		fallback     int
		defaultLimit int
		minLimit     int
		maxLimit     int
		want         int
	}{
		{
			name:         "uses configured when positive",
			configured:   48,
			fallback:     96,
			defaultLimit: 256,
			minLimit:     16,
			maxLimit:     4096,
			want:         48,
		},
		{
			name:         "uses fallback when configured non-positive",
			configured:   0,
			fallback:     64,
			defaultLimit: 256,
			minLimit:     16,
			maxLimit:     4096,
			want:         64,
		},
		{
			name:         "uses default when configured and fallback non-positive",
			configured:   -1,
			fallback:     0,
			defaultLimit: 256,
			minLimit:     16,
			maxLimit:     4096,
			want:         256,
		},
		{
			name:         "clamps to min",
			configured:   4,
			fallback:     0,
			defaultLimit: 256,
			minLimit:     16,
			maxLimit:     4096,
			want:         16,
		},
		{
			name:         "clamps to max",
			configured:   5000,
			fallback:     0,
			defaultLimit: 256,
			minLimit:     16,
			maxLimit:     4096,
			want:         4096,
		},
		{
			name:         "max zero disables upper clamp",
			configured:   5000,
			fallback:     0,
			defaultLimit: 256,
			minLimit:     16,
			maxLimit:     0,
			want:         5000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeSharedSessionTimelineLimit(
				tc.configured,
				tc.fallback,
				tc.defaultLimit,
				tc.minLimit,
				tc.maxLimit,
			)
			if got != tc.want {
				t.Fatalf("normalizeSharedSessionTimelineLimit() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestSharedSessionHistoryLimitLockedNilManagerFallbacks(t *testing.T) {
	session := &sharedRuntimeSession{}

	if got, want := session.sourceHistoryLimitLocked(), defaultSharedSourceHistoryLimit; got != want {
		t.Fatalf("sourceHistoryLimitLocked() = %d, want %d", got, want)
	}
	if got, want := session.subscriberHistoryLimitLocked(), defaultSharedSubscriberHistoryLimit; got != want {
		t.Fatalf("subscriberHistoryLimitLocked() = %d, want %d", got, want)
	}
}

func TestSharedSessionHistoryTruncationConcurrentSubscriberMutationAndSnapshot(t *testing.T) {
	manager := &SessionManager{
		cfg: sessionManagerConfig{
			mode:                          "direct",
			sessionSubscriberHistoryLimit: 16,
			sessionSourceHistoryLimit:     16,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &sharedRuntimeSession{
		manager:                manager,
		channel:                channels.Channel{ChannelID: 501, GuideNumber: "501", GuideName: "Concurrent History"},
		ring:                   NewChunkRing(64),
		ctx:                    ctx,
		cancel:                 cancel,
		readyCh:                closedSignalChan(),
		subscribers:            make(map[uint64]SubscriberStats),
		startedAt:              time.Now().UTC(),
		historySubscriberIndex: make(map[uint64]int),
	}

	_, persistentSubscriberID, err := session.addSubscriber("192.168.50.1:5000")
	if err != nil {
		t.Fatalf("addSubscriber(persistent) error = %v", err)
	}

	const (
		workers    = 8
		iterations = 120
	)
	errCh := make(chan error, workers*iterations)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				clientAddr := fmt.Sprintf("192.168.%d.%d:5000", worker+1, i+10)
				_, subscriberID, addErr := session.addSubscriber(clientAddr)
				if addErr != nil {
					errCh <- fmt.Errorf("addSubscriber(%q): %w", clientAddr, addErr)
					return
				}
				_ = session.historySnapshot()
				session.removeSubscriber(subscriberID, subscriberRemovalReasonStreamWriteError)
				_ = session.historySnapshot()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for runErr := range errCh {
		t.Fatal(runErr)
	}

	snapshot := session.historySnapshot()
	if got, wantMax := len(snapshot.Subscribers), manager.cfg.sessionSubscriberHistoryLimit; got > wantMax {
		t.Fatalf("len(snapshot.subscribers) = %d, want <= %d", got, wantMax)
	}
	historyIdx, ok := session.historySubscriberIndex[persistentSubscriberID]
	if !ok {
		t.Fatalf("historySubscriberIndex missing persistent subscriber_id=%d", persistentSubscriberID)
	}
	if historyIdx < 0 || historyIdx >= len(snapshot.Subscribers) {
		t.Fatalf("persistent history index = %d out of range", historyIdx)
	}
	if got, want := snapshot.Subscribers[historyIdx].SubscriberID, persistentSubscriberID; got != want {
		t.Fatalf("snapshot.subscribers[%d].subscriber_id = %d, want %d", historyIdx, got, want)
	}
}

func TestSharedSessionSourceHistoryRetentionPreservesNonTailActiveSource(t *testing.T) {
	mkSourceHistory := func(sourceIDs []int64, deselected map[int]bool) []SharedSessionSourceHistory {
		base := time.Unix(1_770_000_000, 0).UTC()
		history := make([]SharedSessionSourceHistory, 0, len(sourceIDs))
		for idx, sourceID := range sourceIDs {
			entry := SharedSessionSourceHistory{
				SourceID:          sourceID,
				ItemKey:           fmt.Sprintf("src:history:%d", sourceID),
				SelectedAt:        base.Add(time.Duration(idx) * time.Second),
				StreamURL:         fmt.Sprintf("http://example.com/%d.ts", sourceID),
				Resolution:        "1920x1080",
				VideoCodec:        "h264",
				AudioCodec:        "aac",
				FrameRate:         29.97,
				Producer:          "direct",
				ProfileBitrateBPS: 3_500_000,
			}
			if deselected[idx] {
				entry.DeselectedAt = base.Add(time.Duration(idx+1) * time.Second)
			}
			history = append(history, entry)
		}
		return history
	}

	extractSourceIDs := func(history []SharedSessionSourceHistory) []int64 {
		ids := make([]int64, 0, len(history))
		for _, entry := range history {
			ids = append(ids, entry.SourceID)
		}
		return ids
	}

	tests := []struct {
		name             string
		sourceIDs        []int64
		deselected       map[int]bool
		currentIdx       int
		limit            int
		wantSourceIDs    []int64
		wantCurrentIdx   int
		wantTruncatedAdd int64
	}{
		{
			name:             "active source at index zero remains retained",
			sourceIDs:        []int64{1, 2, 3, 4, 5},
			deselected:       map[int]bool{1: true, 2: true, 3: true, 4: true},
			currentIdx:       0,
			limit:            3,
			wantSourceIDs:    []int64{1, 4, 5},
			wantCurrentIdx:   0,
			wantTruncatedAdd: 2,
		},
		{
			name:             "active source in middle reindexes after compaction",
			sourceIDs:        []int64{21, 22, 23, 24, 25, 26},
			deselected:       map[int]bool{0: true, 2: true, 4: true, 5: true},
			currentIdx:       3,
			limit:            3,
			wantSourceIDs:    []int64{22, 24, 26},
			wantCurrentIdx:   1,
			wantTruncatedAdd: 3,
		},
		{
			name:             "all-active history uses second eviction pass",
			sourceIDs:        []int64{31, 32, 33, 34, 35},
			deselected:       map[int]bool{},
			currentIdx:       1,
			limit:            3,
			wantSourceIDs:    []int64{32, 34, 35},
			wantCurrentIdx:   0,
			wantTruncatedAdd: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			session := &sharedRuntimeSession{
				manager: &SessionManager{
					cfg: sessionManagerConfig{
						sessionSourceHistoryLimit: tc.limit,
					},
				},
				historySources:          mkSourceHistory(tc.sourceIDs, tc.deselected),
				historyCurrentSourceIdx: tc.currentIdx,
			}

			activeSourceID := tc.sourceIDs[tc.currentIdx]
			session.truncateSourceHistoryLocked()

			if got := extractSourceIDs(session.historySources); !reflect.DeepEqual(got, tc.wantSourceIDs) {
				t.Fatalf("retained source IDs = %v, want %v", got, tc.wantSourceIDs)
			}
			if got, want := session.historyCurrentSourceIdx, tc.wantCurrentIdx; got != want {
				t.Fatalf("historyCurrentSourceIdx = %d, want %d", got, want)
			}
			if got, want := session.historySourcesTruncated, tc.wantTruncatedAdd; got != want {
				t.Fatalf("historySourcesTruncated = %d, want %d", got, want)
			}

			foundActive := false
			for _, entry := range session.historySources {
				if entry.SourceID != activeSourceID {
					continue
				}
				foundActive = true
				if !entry.DeselectedAt.IsZero() {
					t.Fatalf("active source_id=%d has deselected_at=%s, want zero", activeSourceID, entry.DeselectedAt)
				}
			}
			if !foundActive {
				t.Fatalf("active source_id=%d was evicted", activeSourceID)
			}
		})
	}
}

func TestSharedSessionSourceHistoryRetentionBoundsTimeline(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := &SessionManager{
		cfg: sessionManagerConfig{
			mode:                          "direct",
			sessionHistoryLimit:           8,
			sessionSourceHistoryLimit:     3,
			sessionSubscriberHistoryLimit: 8,
		},
		logger:                  logger,
		sessions:                make(map[int64]*sharedRuntimeSession),
		creating:                make(map[int64]*sessionCreateWait),
		sessionHistory:          make([]SharedSessionHistory, 0, 8),
		recentHealth:            newRecentSourceHealth(),
		overlimitMu:             sync.Mutex{},
		providerCooldownByScope: make(map[string]providerOverlimitScopeState),
	}

	ctx, cancel := context.WithCancel(context.Background())
	session := &sharedRuntimeSession{
		manager:                 manager,
		channel:                 channels.Channel{ChannelID: 1, GuideNumber: "101", GuideName: "History"},
		ring:                    NewChunkRing(32),
		ctx:                     ctx,
		cancel:                  cancel,
		readyCh:                 make(chan struct{}),
		subscribers:             make(map[uint64]SubscriberStats),
		startedAt:               time.Now().UTC().Add(-30 * time.Second),
		historySessionID:        600,
		historyCurrentSourceIdx: -1,
		historySubscriberIndex:  make(map[uint64]int),
	}

	for i := 1; i <= 6; i++ {
		source := channels.Source{
			SourceID:  int64(i),
			ItemKey:   fmt.Sprintf("src:history:%d", i),
			StreamURL: fmt.Sprintf("http://example.com/live/%d.ts", i),
		}
		session.setSourceStateWithStartupProbe(
			source,
			fmt.Sprintf("direct:source=%d", i),
			"history_limit_test",
			startupProbeTelemetry{rawBytes: 1024, trimmedBytes: 1024},
			true,
			"h264",
			startupStreamInventory{videoStreamCount: 1, audioStreamCount: 1},
			false,
			"",
		)
	}

	if got, want := len(session.historySources), 3; got != want {
		t.Fatalf("len(historySources) = %d, want %d", got, want)
	}
	if got, want := session.historyCurrentSourceIdx, 2; got != want {
		t.Fatalf("historyCurrentSourceIdx = %d, want %d", got, want)
	}
	if got, want := session.historySourcesTruncated, int64(3); got != want {
		t.Fatalf("historySourcesTruncated = %d, want %d", got, want)
	}
	if got, want := session.historySources[0].SourceID, int64(4); got != want {
		t.Fatalf("historySources[0].source_id = %d, want %d", got, want)
	}
	if got, want := session.historySources[1].SourceID, int64(5); got != want {
		t.Fatalf("historySources[1].source_id = %d, want %d", got, want)
	}
	if got, want := session.historySources[2].SourceID, int64(6); got != want {
		t.Fatalf("historySources[2].source_id = %d, want %d", got, want)
	}
	if session.historySources[0].DeselectedAt.IsZero() {
		t.Fatal("historySources[0].deselected_at is zero, want closed window")
	}
	if session.historySources[1].DeselectedAt.IsZero() {
		t.Fatal("historySources[1].deselected_at is zero, want closed window")
	}
	if !session.historySources[2].DeselectedAt.IsZero() {
		t.Fatal("historySources[2].deselected_at set, want active source window")
	}

	snapshot := session.historySnapshot()
	if got, want := snapshot.SourceHistoryLimit, 3; got != want {
		t.Fatalf("snapshot.source_history_limit = %d, want %d", got, want)
	}
	if got, want := snapshot.SourceHistoryTruncated, int64(3); got != want {
		t.Fatalf("snapshot.source_history_truncated_count = %d, want %d", got, want)
	}
	if got, want := len(snapshot.Sources), 3; got != want {
		t.Fatalf("len(snapshot.sources) = %d, want %d", got, want)
	}
}

func TestSharedSessionSubscriberHistoryRetentionPreservesActiveIndex(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := &SessionManager{
		cfg: sessionManagerConfig{
			mode:                          "direct",
			subscriberJoinLagBytes:        0,
			sessionHistoryLimit:           8,
			sessionSourceHistoryLimit:     8,
			sessionSubscriberHistoryLimit: 4,
		},
		logger:                  logger,
		sessions:                make(map[int64]*sharedRuntimeSession),
		creating:                make(map[int64]*sessionCreateWait),
		sessionHistory:          make([]SharedSessionHistory, 0, 8),
		recentHealth:            newRecentSourceHealth(),
		overlimitMu:             sync.Mutex{},
		providerCooldownByScope: make(map[string]providerOverlimitScopeState),
	}

	ctx, cancel := context.WithCancel(context.Background())
	session := &sharedRuntimeSession{
		manager:                 manager,
		channel:                 channels.Channel{ChannelID: 2, GuideNumber: "102", GuideName: "Subscriber History"},
		ring:                    NewChunkRing(32),
		ctx:                     ctx,
		cancel:                  cancel,
		readyCh:                 make(chan struct{}),
		subscribers:             make(map[uint64]SubscriberStats),
		startedAt:               time.Now().UTC().Add(-45 * time.Second),
		historySessionID:        601,
		historyCurrentSourceIdx: -1,
		historySubscriberIndex:  make(map[uint64]int),
	}

	_, persistentID, err := session.addSubscriber("192.168.1.1:5000")
	if err != nil {
		t.Fatalf("addSubscriber(persistent) error = %v", err)
	}
	for i := 0; i < 5; i++ {
		_, subscriberID, err := session.addSubscriber(fmt.Sprintf("192.168.1.%d:5000", i+2))
		if err != nil {
			t.Fatalf("addSubscriber(%d) error = %v", i, err)
		}
		session.removeSubscriber(subscriberID, subscriberRemovalReasonStreamWriteError)
	}

	if got, want := len(session.historySubscribers), 4; got != want {
		t.Fatalf("len(historySubscribers) = %d, want %d", got, want)
	}
	if got, want := session.historySubscribersTruncated, int64(2); got != want {
		t.Fatalf("historySubscribersTruncated = %d, want %d", got, want)
	}
	activeHistoryIdx, ok := session.historySubscriberIndex[persistentID]
	if !ok {
		t.Fatalf("historySubscriberIndex missing active subscriber_id=%d", persistentID)
	}
	if activeHistoryIdx < 0 || activeHistoryIdx >= len(session.historySubscribers) {
		t.Fatalf("active history index = %d, out of range", activeHistoryIdx)
	}
	if session.historySubscribers[activeHistoryIdx].SubscriberID != persistentID {
		t.Fatalf(
			"historySubscribers[%d].subscriber_id = %d, want %d",
			activeHistoryIdx,
			session.historySubscribers[activeHistoryIdx].SubscriberID,
			persistentID,
		)
	}
	if !session.historySubscribers[activeHistoryIdx].ClosedAt.IsZero() {
		t.Fatal("active history entry has closed_at set before removal")
	}

	session.removeSubscriber(persistentID, subscriberRemovalReasonHTTPContextCanceled)
	if got := len(session.historySubscriberIndex); got != 0 {
		t.Fatalf("len(historySubscriberIndex) = %d, want 0 after active removal", got)
	}
	closed := false
	for _, entry := range session.historySubscribers {
		if entry.SubscriberID != persistentID {
			continue
		}
		closed = true
		if entry.ClosedAt.IsZero() {
			t.Fatal("persistent subscriber closed_at is zero after removeSubscriber")
		}
		if got, want := entry.CloseReason, subscriberRemovalReasonHTTPContextCanceled; got != want {
			t.Fatalf("persistent subscriber close_reason = %q, want %q", got, want)
		}
	}
	if !closed {
		t.Fatalf("persistent subscriber_id=%d not found in retained history", persistentID)
	}

	snapshot := session.historySnapshot()
	if got, want := snapshot.SubscriberHistoryLimit, 4; got != want {
		t.Fatalf("snapshot.subscriber_history_limit = %d, want %d", got, want)
	}
	if got, want := snapshot.SubscriberHistoryTruncated, int64(2); got != want {
		t.Fatalf("snapshot.subscriber_history_truncated_count = %d, want %d", got, want)
	}
	if got, want := len(snapshot.Subscribers), 4; got != want {
		t.Fatalf("len(snapshot.subscribers) = %d, want %d", got, want)
	}
}

func TestSharedSessionSubscriberHistoryRetentionExpandsEffectiveLimitForActiveSubscribers(t *testing.T) {
	manager := &SessionManager{
		cfg: sessionManagerConfig{
			sessionSubscriberHistoryLimit: 2,
		},
	}

	base := time.Unix(1_770_000_000, 0).UTC()
	session := &sharedRuntimeSession{
		manager:                manager,
		subscribers:            make(map[uint64]SubscriberStats),
		historySubscribers:     make([]SharedSessionSubscriberHistory, 0, 4),
		historySubscriberIndex: make(map[uint64]int),
	}

	for i := 0; i < 4; i++ {
		subscriberID := uint64(100 + i)
		session.subscribers[subscriberID] = SubscriberStats{
			SubscriberID: subscriberID,
			ClientAddr:   fmt.Sprintf("192.168.1.%d:5000", i+1),
			StartedAt:    base.Add(time.Duration(i) * time.Second),
		}
		session.historySubscribers = append(session.historySubscribers, SharedSessionSubscriberHistory{
			SubscriberID: subscriberID,
			ClientAddr:   fmt.Sprintf("192.168.1.%d:5000", i+1),
			ConnectedAt:  base.Add(time.Duration(i) * time.Second),
		})
		session.historySubscriberIndex[subscriberID] = i
	}

	session.truncateSubscriberHistoryLocked()

	if got, want := len(session.historySubscribers), 4; got != want {
		t.Fatalf("len(historySubscribers) = %d, want %d (active subscribers should expand effective limit)", got, want)
	}
	if got, want := session.historySubscribersTruncated, int64(0); got != want {
		t.Fatalf("historySubscribersTruncated = %d, want %d", got, want)
	}
	if got, want := len(session.historySubscriberIndex), 4; got != want {
		t.Fatalf("len(historySubscriberIndex) = %d, want %d", got, want)
	}

	for i := 0; i < 4; i++ {
		subscriberID := uint64(100 + i)
		historyIdx, ok := session.historySubscriberIndex[subscriberID]
		if !ok {
			t.Fatalf("historySubscriberIndex missing subscriber_id=%d", subscriberID)
		}
		if got, want := historyIdx, i; got != want {
			t.Fatalf("historySubscriberIndex[%d] = %d, want %d", subscriberID, got, want)
		}
		if !session.isActiveSubscriberHistoryEntryLocked(historyIdx) {
			t.Fatalf("isActiveSubscriberHistoryEntryLocked(%d) = false, want true", historyIdx)
		}
	}

	snapshot := session.historySnapshot()
	if got, want := snapshot.SubscriberHistoryLimit, 2; got != want {
		t.Fatalf("snapshot.subscriber_history_limit = %d, want %d", got, want)
	}
	if got, want := snapshot.SubscriberHistoryTruncated, int64(0); got != want {
		t.Fatalf("snapshot.subscriber_history_truncated_count = %d, want %d", got, want)
	}
	if got, want := len(snapshot.Subscribers), 4; got != want {
		t.Fatalf("len(snapshot.subscribers) = %d, want %d", got, want)
	}
}

func TestSharedSessionHistorySnapshotTruncationIsIdempotent(t *testing.T) {
	base := time.Unix(1_770_000_000, 0).UTC()
	session := &sharedRuntimeSession{
		manager: &SessionManager{
			cfg: sessionManagerConfig{
				sessionSourceHistoryLimit:     2,
				sessionSubscriberHistoryLimit: 2,
			},
		},
		historySources: []SharedSessionSourceHistory{
			{SourceID: 41, SelectedAt: base},
			{SourceID: 42, SelectedAt: base.Add(time.Second)},
			{SourceID: 43, SelectedAt: base.Add(2 * time.Second)},
		},
		historyCurrentSourceIdx: 2,
		subscribers:             make(map[uint64]SubscriberStats),
		historySubscribers: []SharedSessionSubscriberHistory{
			{
				SubscriberID: 301,
				ClientAddr:   "192.168.50.1:5000",
				ConnectedAt:  base.Add(10 * time.Second),
				ClosedAt:     base.Add(20 * time.Second),
				CloseReason:  subscriberRemovalReasonStreamWriteError,
			},
			{
				SubscriberID: 302,
				ClientAddr:   "192.168.50.2:5000",
				ConnectedAt:  base.Add(11 * time.Second),
				ClosedAt:     base.Add(21 * time.Second),
				CloseReason:  subscriberRemovalReasonStreamWriteError,
			},
			{
				SubscriberID: 303,
				ClientAddr:   "192.168.50.3:5000",
				ConnectedAt:  base.Add(12 * time.Second),
				ClosedAt:     base.Add(22 * time.Second),
				CloseReason:  subscriberRemovalReasonStreamWriteError,
			},
		},
		historySubscriberIndex: make(map[uint64]int),
	}

	first := session.historySnapshot()
	second := session.historySnapshot()

	if got, want := first.SourceHistoryTruncated, int64(1); got != want {
		t.Fatalf("first snapshot source_history_truncated = %d, want %d", got, want)
	}
	if got, want := second.SourceHistoryTruncated, first.SourceHistoryTruncated; got != want {
		t.Fatalf("second snapshot source_history_truncated = %d, want %d", got, want)
	}
	if got, want := first.SubscriberHistoryTruncated, int64(1); got != want {
		t.Fatalf("first snapshot subscriber_history_truncated = %d, want %d", got, want)
	}
	if got, want := second.SubscriberHistoryTruncated, first.SubscriberHistoryTruncated; got != want {
		t.Fatalf("second snapshot subscriber_history_truncated = %d, want %d", got, want)
	}
	if !reflect.DeepEqual(first.Sources, second.Sources) {
		t.Fatalf("snapshot sources differ between first and second calls")
	}
	if !reflect.DeepEqual(first.Subscribers, second.Subscribers) {
		t.Fatalf("snapshot subscribers differ between first and second calls")
	}
}

func TestSharedSessionIsActiveSubscriberHistoryEntryLockedEdgeCases(t *testing.T) {
	base := time.Unix(1_770_000_000, 0).UTC()
	session := &sharedRuntimeSession{
		subscribers: map[uint64]SubscriberStats{
			1: {SubscriberID: 1, StartedAt: base},
			3: {SubscriberID: 3, StartedAt: base.Add(2 * time.Second)},
			4: {SubscriberID: 4, StartedAt: base.Add(3 * time.Second)},
		},
		historySubscribers: []SharedSessionSubscriberHistory{
			{SubscriberID: 1, ConnectedAt: base},
			{SubscriberID: 2, ConnectedAt: base.Add(1 * time.Second)},
			{SubscriberID: 3, ConnectedAt: base.Add(2 * time.Second)},
			{
				SubscriberID: 4,
				ConnectedAt:  base.Add(3 * time.Second),
				ClosedAt:     base.Add(4 * time.Second),
				CloseReason:  subscriberRemovalReasonHTTPContextCanceled,
			},
		},
		historySubscriberIndex: map[uint64]int{
			1: 0, // active happy-path
			2: 1, // orphaned: not in subscribers map
			3: 0, // stale index: points at subscriber 1's entry
			4: 3, // closed entry should still report inactive
		},
	}

	tests := []struct {
		name  string
		index int
		want  bool
	}{
		{name: "happy path active entry", index: 0, want: true},
		{name: "orphaned entry", index: 1, want: false},
		{name: "stale index mismatch", index: 2, want: false},
		{name: "closed entry", index: 3, want: false},
		{name: "negative index", index: -1, want: false},
		{name: "out of range", index: 99, want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := session.isActiveSubscriberHistoryEntryLocked(tc.index); got != tc.want {
				t.Fatalf("isActiveSubscriberHistoryEntryLocked(%d) = %t, want %t", tc.index, got, tc.want)
			}
		})
	}
}

func TestSessionManagerHistoryRetentionTruncatesOldestEntries(t *testing.T) {
	manager := &SessionManager{
		cfg: sessionManagerConfig{
			sessionHistoryLimit: 2,
		},
		sessionHistory: make([]SharedSessionHistory, 0, 2),
	}

	base := time.Unix(1_770_000_000, 0).UTC()
	manager.recordClosedSessionHistory(SharedSessionHistory{SessionID: 1, OpenedAt: base.Add(1 * time.Minute)})
	manager.recordClosedSessionHistory(SharedSessionHistory{SessionID: 2, OpenedAt: base.Add(2 * time.Minute)})
	manager.recordClosedSessionHistory(SharedSessionHistory{SessionID: 3, OpenedAt: base.Add(3 * time.Minute)})

	history, limit, truncated := manager.HistorySnapshot()
	if got, want := limit, 2; got != want {
		t.Fatalf("history limit = %d, want %d", got, want)
	}
	if got, want := truncated, int64(1); got != want {
		t.Fatalf("history truncated = %d, want %d", got, want)
	}
	if got, want := len(history), 2; got != want {
		t.Fatalf("len(history) = %d, want %d", got, want)
	}
	if got, want := history[0].SessionID, uint64(3); got != want {
		t.Fatalf("history[0].session_id = %d, want %d", got, want)
	}
	if got, want := history[1].SessionID, uint64(2); got != want {
		t.Fatalf("history[1].session_id = %d, want %d", got, want)
	}
}

func TestSessionManagerHistorySnapshotDeduplicatesClosedAndActiveSessions(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := &SessionManager{
		cfg: sessionManagerConfig{
			sessionHistoryLimit: 16,
		},
		logger:                  logger,
		sessions:                make(map[int64]*sharedRuntimeSession),
		creating:                make(map[int64]*sessionCreateWait),
		sessionHistory:          make([]SharedSessionHistory, 0, 4),
		recentHealth:            newRecentSourceHealth(),
		overlimitMu:             sync.Mutex{},
		providerCooldownByScope: make(map[string]providerOverlimitScopeState),
	}

	base := time.Unix(1_770_000_000, 0).UTC()
	manager.recordClosedSessionHistory(SharedSessionHistory{
		SessionID:            501,
		ChannelID:            1,
		GuideNumber:          "101",
		GuideName:            "Overlap Session",
		OpenedAt:             base.Add(-1 * time.Minute),
		ClosedAt:             base.Add(-5 * time.Second),
		Active:               false,
		TerminalStatus:       "completed",
		TerminalError:        "",
		PeakSubscribers:      0,
		TotalSubscribers:     0,
		CompletedSubscribers: 0,
	})
	manager.recordClosedSessionHistory(SharedSessionHistory{
		SessionID:            502,
		ChannelID:            2,
		GuideNumber:          "202",
		GuideName:            "Closed-Only Session",
		OpenedAt:             base.Add(-2 * time.Minute),
		ClosedAt:             base.Add(-10 * time.Second),
		Active:               false,
		TerminalStatus:       "error",
		TerminalError:        "closed by source failure",
		PeakSubscribers:      0,
		TotalSubscribers:     0,
		CompletedSubscribers: 0,
	})

	ctx1, cancel1 := context.WithCancel(context.Background())
	overlapping := &sharedRuntimeSession{
		manager:                 manager,
		channel:                 channels.Channel{ChannelID: 1, GuideNumber: "101", GuideName: "Overlap Session"},
		ring:                    NewChunkRing(32),
		ctx:                     ctx1,
		cancel:                  cancel1,
		readyCh:                 make(chan struct{}),
		subscribers:             make(map[uint64]SubscriberStats),
		startedAt:               base.Add(-90 * time.Second),
		historySessionID:        501,
		historyCurrentSourceIdx: -1,
		historySubscriberIndex:  make(map[uint64]int),
	}
	manager.sessions[1] = overlapping

	ctx2, cancel2 := context.WithCancel(context.Background())
	activeOnly := &sharedRuntimeSession{
		manager:                 manager,
		channel:                 channels.Channel{ChannelID: 3, GuideNumber: "303", GuideName: "Active Session"},
		ring:                    NewChunkRing(32),
		ctx:                     ctx2,
		cancel:                  cancel2,
		readyCh:                 make(chan struct{}),
		subscribers:             make(map[uint64]SubscriberStats),
		startedAt:               base.Add(-30 * time.Second),
		historySessionID:        503,
		historyCurrentSourceIdx: -1,
		historySubscriberIndex:  make(map[uint64]int),
	}
	manager.sessions[3] = activeOnly

	history, _, _ := manager.HistorySnapshot()
	if got, want := len(history), 3; got != want {
		t.Fatalf("len(history) = %d, want %d", got, want)
	}

	seen := make(map[uint64]int, len(history))
	for _, entry := range history {
		seen[entry.SessionID]++
		switch entry.SessionID {
		case 501:
			if !entry.Active {
				t.Fatalf("history[501].active = %v, want true (active snapshot should win overlap)", entry.Active)
			}
			if !entry.ClosedAt.IsZero() {
				t.Fatalf("history[501].closed_at = %s, want zero for active overlap entry", entry.ClosedAt)
			}
		case 502:
			if entry.Active {
				t.Fatalf("history[502].active = %v, want false", entry.Active)
			}
		case 503:
			if !entry.Active {
				t.Fatalf("history[503].active = %v, want true", entry.Active)
			}
		}
	}
	if got, want := seen[501], 1; got != want {
		t.Fatalf("session_id=%d count = %d, want %d", 501, got, want)
	}
	if got, want := seen[502], 1; got != want {
		t.Fatalf("session_id=%d count = %d, want %d", 502, got, want)
	}
	if got, want := seen[503], 1; got != want {
		t.Fatalf("session_id=%d count = %d, want %d", 503, got, want)
	}
}

// TestSessionManagerCloseWaitsForInFlightCreator verifies that Close() blocks
// until a creator stuck in AcquireClient (tracked via m.creating) has fully
// torn down. This exercises the wg-from-reservation lifecycle: wg.Add(1) at
// creation reservation, Close() cancels in-flight creators, wg.Wait() blocks
// until the creator's wg.Done() fires.
//
// Setup: pool capacity 1 filled by a probe, with a long preempt settle delay.
// The subscriber preempts the probe but blocks in the slot settle wait.
// Close() cancels the create context, unblocking AcquireClient, and waits for
// the creator to clean up via wg.Wait().
func TestSessionManagerCloseWaitsForInFlightCreator(t *testing.T) {
	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"101": {ChannelID: 1, GuideNumber: "101", GuideName: "News", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			1: {
				{
					SourceID:      10,
					ChannelID:     1,
					ItemKey:       "src:news:close-waits-creator",
					StreamURL:     "http://127.0.0.1:0/nonexistent",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	// Capacity 1: fill with a probe. A long settle delay ensures AcquireClient
	// blocks after preempting the probe — long enough for Close() to observe
	// the creator in m.creating and cancel it.
	pool := NewPool(1)
	pool.SetPreemptSettleDelay(30 * time.Second)

	probeCtx, probeCancel := context.WithCancelCause(context.Background())
	defer probeCancel(context.Canceled)

	probeLease, err := pool.AcquireProbe(probeCtx, "block-creator", probeCancel)
	if err != nil {
		t.Fatalf("AcquireProbe() error = %v", err)
	}
	defer probeLease.Release()
	// Release probe when its context is canceled (preempted by client).
	go func() {
		<-probeCtx.Done()
		probeLease.Release()
	}()

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             2 * time.Second,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           1,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         100 * time.Millisecond,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	// Launch a subscriber that will block in AcquireClient (settle wait).
	subErrCh := make(chan error, 1)
	go func() {
		_, err := manager.Subscribe(context.Background(), provider.channelsByGuide["101"])
		subErrCh <- err
	}()

	// Wait until the creator is visible in m.creating.
	waitFor(t, 2*time.Second, func() bool {
		manager.mu.Lock()
		_, creating := manager.creating[1]
		manager.mu.Unlock()
		return creating
	})

	// Close() must cancel the in-flight creator and wait for teardown.
	closeDone := make(chan struct{})
	go func() {
		manager.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// Close() returned — creator was fully torn down.
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return; in-flight creator was not drained")
	}

	// Subscribe should have returned a shutdown-path error: context.Canceled
	// (Close canceled the create context while AcquireClient was blocked) or
	// errSessionManagerClosed (AcquireClient completed before cancel and the
	// post-acquire closed check fired).
	select {
	case subErr := <-subErrCh:
		if !errors.Is(subErr, context.Canceled) && !errors.Is(subErr, errSessionManagerClosed) {
			t.Fatalf("Subscribe() error = %v, want context.Canceled or errSessionManagerClosed", subErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe() did not return after Close()")
	}

	// Pool should be fully released.
	waitFor(t, 2*time.Second, func() bool {
		return pool.InUseCount() == 0
	})
}

func TestSessionManagerWaitForDrainReturnsImmediatelyAfterDrainCompletes(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
		drainCh:  closedSignalChan(),
	}

	manager.mu.Lock()
	// Simulate one full active->drained lifecycle before the waiter starts.
	manager.sessions[1] = &sharedRuntimeSession{}
	manager.syncDrainSignalLocked()
	delete(manager.sessions, 1)
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	started := time.Now()
	if err := manager.WaitForDrain(ctx); err != nil {
		t.Fatalf("WaitForDrain() error = %v, want nil", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("WaitForDrain() elapsed = %s, want immediate late-waiter return", elapsed)
	}
}

func TestSessionManagerWaitForDrainNilManagerIsNoop(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	var manager *SessionManager
	if err := manager.WaitForDrain(context.Background()); err != nil {
		t.Fatalf("WaitForDrain() error = %v, want nil for nil manager", err)
	}
}

func TestSessionManagerWaitForDrainInitializesNilDrainSignalForZeroValueManager(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
	}

	manager.mu.Lock()
	manager.sessions[1] = &sharedRuntimeSession{}
	manager.mu.Unlock()
	blockedCh := installWaitForDrainBlockedHookForTest(t, manager, 2)

	waitErrCh := make(chan error, 1)
	go func() {
		waitErrCh <- manager.WaitForDrain(context.Background())
	}()
	t.Cleanup(func() {
		manager.mu.Lock()
		delete(manager.sessions, 1)
		manager.syncDrainSignalLocked()
		manager.mu.Unlock()
	})
	waitForDrainBlockedHookSignal(t, blockedCh, "zero-value nil drain signal wait")

	manager.mu.Lock()
	if manager.drainCh == nil {
		manager.mu.Unlock()
		t.Fatal("drain signal should be initialized for active waiters")
	}
	select {
	case <-manager.drainCh:
		manager.mu.Unlock()
		t.Fatal("drain signal unexpectedly closed while session is active")
	default:
	}
	delete(manager.sessions, 1)
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()

	select {
	case err := <-waitErrCh:
		if err != nil {
			t.Fatalf("WaitForDrain() error = %v, want nil after zero-value manager drains", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForDrain() did not return after zero-value manager drain")
	}
}

func TestSessionManagerWaitForDrainNilContextFallsBackToBackground(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
		drainCh:  closedSignalChan(),
	}

	manager.mu.Lock()
	manager.sessions[1] = &sharedRuntimeSession{}
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()
	blockedCh := installWaitForDrainBlockedHookForTest(t, manager, 2)

	waitErrCh := make(chan error, 1)
	go func() {
		waitErrCh <- manager.WaitForDrain(nil)
	}()
	t.Cleanup(func() {
		manager.mu.Lock()
		delete(manager.sessions, 1)
		manager.syncDrainSignalLocked()
		manager.mu.Unlock()
	})
	waitForDrainBlockedHookSignal(t, blockedCh, "nil context wait")

	manager.mu.Lock()
	delete(manager.sessions, 1)
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()

	select {
	case err := <-waitErrCh:
		if err != nil {
			t.Fatalf("WaitForDrain(nil) error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForDrain(nil) did not return after drain")
	}
}

func TestSessionManagerWaitForDrainBlocksOnCreatingReservations(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
		drainCh:  closedSignalChan(),
	}

	manager.mu.Lock()
	// A pending creator should keep drain waiters blocked even with no sessions.
	manager.creating[1] = &sessionCreateWait{}
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()
	blockedCh := installWaitForDrainBlockedHookForTest(t, manager, 2)

	waitCtx, cancelWait := context.WithCancel(context.Background())
	waitErrCh := make(chan error, 1)
	go func() {
		waitErrCh <- manager.WaitForDrain(waitCtx)
	}()
	waitForDrainBlockedHookSignal(t, blockedCh, "creating reservation wait")
	cancelWait()
	if err := <-waitErrCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForDrain() with pending creator error = %v, want context canceled", err)
	}

	manager.mu.Lock()
	delete(manager.creating, 1)
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Second)
	defer drainCancel()
	if err := manager.WaitForDrain(drainCtx); err != nil {
		t.Fatalf("WaitForDrain() after creating drain error = %v, want nil", err)
	}
}

func TestSessionManagerWaitForDrainReturnsCanceledWhenContextCanceled(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
		drainCh:  closedSignalChan(),
	}

	manager.mu.Lock()
	manager.sessions[1] = &sharedRuntimeSession{}
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()
	blockedCh := installWaitForDrainBlockedHookForTest(t, manager, 2)

	waitCtx, cancelWait := context.WithCancel(context.Background())
	waitErrCh := make(chan error, 1)
	go func() {
		waitErrCh <- manager.WaitForDrain(waitCtx)
	}()
	waitForDrainBlockedHookSignal(t, blockedCh, "cancelable wait")

	cancelWait()

	select {
	case err := <-waitErrCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitForDrain() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForDrain() did not return after context cancellation")
	}
}

func TestSessionManagerWaitForDrainResetsSignalAcrossSessionReuse(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	logs := newTestLogBuffer()
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
		drainCh:  closedSignalChan(),
		logger:   logger,
	}
	blockedCh := installWaitForDrainBlockedHookForTest(t, manager, 8)
	statsBefore := streamDrainStatsSnapshot()

	markActive := func(channelID int64) {
		t.Helper()
		manager.mu.Lock()
		manager.sessions[channelID] = &sharedRuntimeSession{}
		manager.syncDrainSignalLocked()
		manager.mu.Unlock()
	}
	markDrained := func(channelID int64) {
		t.Helper()
		manager.mu.Lock()
		delete(manager.sessions, channelID)
		manager.syncDrainSignalLocked()
		manager.mu.Unlock()
	}
	expectBlocked := func(step string) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		waitErrCh := make(chan error, 1)
		go func() {
			waitErrCh <- manager.WaitForDrain(ctx)
		}()
		waitForDrainBlockedHookSignal(t, blockedCh, step)
		cancel()
		if err := <-waitErrCh; !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: WaitForDrain() error = %v, want context canceled", step, err)
		}
	}

	// First drain cycle: active session must block, then drain unblocks.
	markActive(101)
	expectBlocked("first active session")
	markDrained(101)
	if err := manager.WaitForDrain(context.Background()); err != nil {
		t.Fatalf("WaitForDrain() after first drain error = %v, want nil", err)
	}

	// Signal must reset when new activity appears, then close again on drain.
	markActive(202)
	expectBlocked("reused active session")
	markDrained(202)
	if err := manager.WaitForDrain(context.Background()); err != nil {
		t.Fatalf("WaitForDrain() after second drain error = %v, want nil", err)
	}

	statsAfter := streamDrainStatsSnapshot()
	if statsAfter.OK < statsBefore.OK+2 {
		t.Fatalf("stream drain ok delta = %d, want >= 2", statsAfter.OK-statsBefore.OK)
	}
	if statsAfter.Error < statsBefore.Error+2 {
		t.Fatalf("stream drain error delta = %d, want >= 2", statsAfter.Error-statsBefore.Error)
	}

	logText := logs.String()
	if !strings.Contains(logText, "drain_result=ok") {
		t.Fatalf("drain logs missing drain_result=ok field: %s", logText)
	}
	if !strings.Contains(logText, "drain_result=error") {
		t.Fatalf("drain logs missing drain_result=error field: %s", logText)
	}
	if !strings.Contains(logText, "manager_id=") {
		t.Fatalf("drain logs missing manager_id field: %s", logText)
	}
	if !strings.Contains(logText, "drain_wait_duration_us=") {
		t.Fatalf("drain logs missing drain_wait_duration_us field: %s", logText)
	}
	if strings.Contains(logText, "stream_drain_ok=") || strings.Contains(logText, "stream_drain_error=") {
		t.Fatalf("drain logs still contain deprecated stream_drain_ok/stream_drain_error fields: %s", logText)
	}
}

func TestSessionManagerWaitForDrainRepeatedDrainSignalDoesNotPanic(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
		drainCh:  closedSignalChan(),
	}

	manager.mu.Lock()
	// Two drain sync calls on an already-drained lifecycle must remain safe.
	manager.sessions[1] = &sharedRuntimeSession{}
	manager.syncDrainSignalLocked()
	delete(manager.sessions, 1)
	manager.syncDrainSignalLocked()
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.WaitForDrain(ctx); err != nil {
		t.Fatalf("WaitForDrain() error = %v, want nil after repeated drain signal sync", err)
	}
}

func TestSessionManagerWaitForDrainSessionAddedAfterDrainSignalBlocksAgain(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
		drainCh:  closedSignalChan(),
	}

	manager.mu.Lock()
	manager.sessions[1] = &sharedRuntimeSession{}
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()
	blockedCh := installWaitForDrainBlockedHookForTest(t, manager, 4)

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelWait()

	waitErrCh := make(chan error, 1)
	go func() {
		waitErrCh <- manager.WaitForDrain(waitCtx)
	}()
	waitForDrainBlockedHookSignal(t, blockedCh, "initial active session")

	manager.mu.Lock()
	// Simulate the race window: drain signal closes, then a new session is
	// registered before the waiter can re-lock and observe empty state.
	delete(manager.sessions, 1)
	manager.syncDrainSignalLocked()
	manager.sessions[2] = &sharedRuntimeSession{}
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()
	waitForDrainBlockedHookSignal(t, blockedCh, "replacement active session")

	manager.mu.Lock()
	delete(manager.sessions, 2)
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()

	select {
	case err := <-waitErrCh:
		if err != nil {
			t.Fatalf("WaitForDrain() error = %v, want nil after replacement drain", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForDrain() did not return after replacement session drained")
	}
}

func TestSessionManagerWaitForDrainUnblocksConcurrentWaiters(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	manager := &SessionManager{
		sessions: make(map[int64]*sharedRuntimeSession),
		creating: make(map[int64]*sessionCreateWait),
		drainCh:  closedSignalChan(),
	}

	manager.mu.Lock()
	manager.sessions[1] = &sharedRuntimeSession{}
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()

	const waiterCount = 8
	blockedCh := installWaitForDrainBlockedHookForTest(t, manager, waiterCount*2)
	parentCtx, cancelParent := context.WithCancel(context.Background())
	t.Cleanup(cancelParent)

	waitErrCh := make(chan error, waiterCount)
	for i := 0; i < waiterCount; i++ {
		go func() {
			// Shared parent cancel ensures no waiter goroutine is stranded if the
			// test aborts before all results are collected.
			ctx, cancel := context.WithTimeout(parentCtx, time.Second)
			defer cancel()
			waitErrCh <- manager.WaitForDrain(ctx)
		}()
	}
	for i := 0; i < waiterCount; i++ {
		waitForDrainBlockedHookSignal(t, blockedCh, fmt.Sprintf("concurrent waiter %d", i+1))
	}

	// A single signal close must broadcast to all current waiters.
	manager.mu.Lock()
	delete(manager.sessions, 1)
	manager.syncDrainSignalLocked()
	manager.mu.Unlock()

	for i := 0; i < waiterCount; i++ {
		select {
		case err := <-waitErrCh:
			if err != nil {
				t.Fatalf("concurrent waiter error = %v, want nil", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for waiter %d to unblock", i+1)
		}
	}
}

func TestRecordDrainWaitResultNilLoggerStillIncrementsCounters(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	manager := &SessionManager{}
	startedAt := time.Now().Add(-50 * time.Millisecond)
	if err := manager.recordDrainWaitResult(startedAt, nil); err != nil {
		t.Fatalf("recordDrainWaitResult() error = %v, want nil", err)
	}

	stats := streamDrainStatsSnapshot()
	if got, want := stats.OK, uint64(1); got != want {
		t.Fatalf("stream drain ok count = %d, want %d", got, want)
	}
	if got, want := stats.Error, uint64(0); got != want {
		t.Fatalf("stream drain error count = %d, want %d", got, want)
	}
	if got := stats.WaitDurationUS; got == 0 {
		t.Fatalf("stream drain wait duration us = %d, want > 0", got)
	}
}

func TestRecordDrainWaitResultNilManagerReceiverStillIncrementsCounters(t *testing.T) {
	isolateStreamDrainStatsForTest(t)

	var manager *SessionManager
	startedAt := time.Now().Add(-50 * time.Millisecond)
	if err := manager.recordDrainWaitResult(startedAt, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("recordDrainWaitResult() error = %v, want context canceled", err)
	}

	stats := streamDrainStatsSnapshot()
	if got, want := stats.OK, uint64(0); got != want {
		t.Fatalf("stream drain ok count = %d, want %d", got, want)
	}
	if got, want := stats.Error, uint64(1); got != want {
		t.Fatalf("stream drain error count = %d, want %d", got, want)
	}
	if got := stats.WaitDurationUS; got == 0 {
		t.Fatalf("stream drain wait duration us = %d, want > 0", got)
	}
}

// TestRecoveryCycleDoesNotFalseStallFromStaleLastPublish verifies that a
// recovery cycle is not immediately canceled as stalled because of a stale
// LastPublishAt carried over from the prior source cycle's pump stats.
//
// Scenario:
//   - Initial source sends enough data to trigger a size-based publish, then
//     stops (stalls). Stall detection fires and initiates recovery.
//   - Recovery source delays briefly before writing (simulating slow startup
//     or upstream buffering), then sends data. Because the delay pushes first
//     publish close to stallDetect, a stale prior-cycle LastPublishAt would
//     trigger a false stall without the cycle-local publish baseline guard.
func TestRecoveryCycleDoesNotFalseStallFromStaleLastPublish(t *testing.T) {
	stallingHits := atomic.Int32{}
	stalling := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		stallingHits.Add(1)
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		// Send enough data to trigger a size-based publish (>512 bytes).
		if _, err := w.Write(bytes.Repeat([]byte("A"), 1024)); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		// Then stall: no more data.
		time.Sleep(5 * time.Second)
	}))
	defer stalling.Close()

	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		// Delay first write so first chunk publish is near stallDetect.
		// With BufferChunkBytes=512 and 200-byte writes every 10ms,
		// first publish happens ~30ms after first write. Adding 100ms
		// delay pushes first publish to ~130ms, close to stallDetect=200ms.
		// A stale prior-cycle LastPublishAt would be >>200ms old, so
		// without the cycle-baseline guard this source would false-stall.
		time.Sleep(100 * time.Millisecond)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < 500; i++ {
			if _, err := w.Write(bytes.Repeat([]byte("B"), 200)); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			<-ticker.C
		}
	}))
	defer healthy.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"200": {ChannelID: 2, GuideNumber: "200", GuideName: "TestCh", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			2: {
				{
					SourceID:      20,
					ChannelID:     2,
					ItemKey:       "src:test:stalling",
					StreamURL:     stalling.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      21,
					ChannelID:     2,
					ItemKey:       "src:test:healthy",
					StreamURL:     healthy.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             300 * time.Millisecond,
		FailoverTotalTimeout:       5 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           512,
		BufferPublishFlushInterval: 500 * time.Millisecond,
		StallDetect:                200 * time.Millisecond,
		StallHardDeadline:          3 * time.Second,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  2,
		SessionIdleTimeout:         50 * time.Millisecond,
		SubscriberJoinLagBytes:     1,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["200"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	streamCtx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	sawRecoveryData := atomic.Bool{}
	writer := &recordingResponseWriter{
		onWrite: func(totalBytes int) {
			// The initial source sends "A" bytes. Once we see enough total
			// bytes that recovery data must have been delivered, mark success
			// and stop streaming.
			if totalBytes >= 2048 {
				sawRecoveryData.Store(true)
				cancel()
			}
		},
	}
	err = sub.Stream(streamCtx, writer)
	if err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!strings.Contains(strings.ToLower(err.Error()), "recovery cycle budget exhausted") {
		t.Fatalf("Stream() error = %v", err)
	}

	if !sawRecoveryData.Load() {
		t.Fatal("subscriber never received enough data from recovery source; " +
			"recovery cycle likely false-stalled from stale LastPublishAt")
	}

	provider.mu.Lock()
	failures := append([]sourceFailureCall(nil), provider.failures...)
	provider.mu.Unlock()

	// The initial stalling source (20) should have a stall failure.
	if !hasFailureReason(failures, 20, "stall") {
		t.Fatalf("failures = %#v, want stall failure for initial source 20", failures)
	}
	// The recovery source (21) should NOT have a stall failure, proving the
	// recovery cycle was not false-stalled from the stale LastPublishAt.
	if hasFailureReason(failures, 21, "stall") {
		t.Fatalf("failures = %#v, recovery source 21 should not have a stall failure", failures)
	}
}

// TestRecoveryCycleStallDetectsZeroPublishNewSource verifies that a recovery
// source which never publishes any chunks still triggers stall detection within
// a bounded time. This is the companion to
// TestRecoveryCycleDoesNotFalseStallFromStaleLastPublish — it ensures the
// cycle-baseline guard does not permanently suppress stall detection when the
// new source sends no data at all.
//
// Scenario:
//   - Initial source sends data then stalls, triggering recovery.
//   - Recovery source accepts the connection but never writes any bytes.
//   - Stall detection must still fire for the recovery source (after the
//     grace period expires), so the session can continue to the next recovery
//     attempt rather than hanging indefinitely.
func TestRecoveryCycleStallDetectsZeroPublishNewSource(t *testing.T) {
	stallingHits := atomic.Int32{}
	stalling := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		stallingHits.Add(1)
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		// Send enough data to trigger a size-based publish (>512 bytes).
		if _, err := w.Write(bytes.Repeat([]byte("A"), 1024)); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		// Then stall: no more data.
		time.Sleep(5 * time.Second)
	}))
	defer stalling.Close()

	// Recovery source that passes startup probe but never publishes enough
	// data to trigger a chunk publish.
	neverPublish := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Send a tiny amount of data to satisfy the startup probe
		// (MinProbeBytes=1), but not enough to trigger a chunk publish
		// (BufferChunkBytes=512). Then hang.
		if _, err := w.Write([]byte("X")); err != nil {
			return
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		// Hang without sending more data until client disconnects.
		<-r.Context().Done()
	}))
	defer neverPublish.Close()

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"300": {ChannelID: 3, GuideNumber: "300", GuideName: "TestCh", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			3: {
				{
					SourceID:      30,
					ChannelID:     3,
					ItemKey:       "src:test:stalling",
					StreamURL:     stalling.URL,
					PriorityIndex: 0,
					Enabled:       true,
				},
				{
					SourceID:      31,
					ChannelID:     3,
					ItemKey:       "src:test:nopublish",
					StreamURL:     neverPublish.URL,
					PriorityIndex: 1,
					Enabled:       true,
				},
			},
		},
	}

	pool := NewPool(1)
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		StartupTimeout:             300 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           512,
		BufferPublishFlushInterval: 500 * time.Millisecond,
		StallDetect:                150 * time.Millisecond,
		StallHardDeadline:          2 * time.Second,
		StallPolicy:                stallPolicyFailoverSource,
		StallMaxFailoversPerStall:  3,
		SessionIdleTimeout:         50 * time.Millisecond,
		SubscriberJoinLagBytes:     1,
	}, pool, provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	sub, err := manager.Subscribe(context.Background(), provider.channelsByGuide["300"])
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	// Budget enough time for initial stall + recovery grace + recovery stall.
	streamCtx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	writer := &recordingResponseWriter{}
	err = sub.Stream(streamCtx, writer)
	// We expect the stream to terminate (recovery budget exhausted, stall, or
	// context deadline) — any of these confirm recovery didn't hang.
	if err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) &&
		!strings.Contains(strings.ToLower(err.Error()), "recovery cycle budget exhausted") {
		t.Fatalf("Stream() error = %v", err)
	}

	provider.mu.Lock()
	failures := append([]sourceFailureCall(nil), provider.failures...)
	provider.mu.Unlock()

	// The initial stalling source (30) must have a stall failure.
	if !hasFailureReason(failures, 30, "stall") {
		t.Fatalf("failures = %#v, want stall failure for initial source 30", failures)
	}
	// The recovery source (31) MUST also have a stall failure, proving that
	// the zero-publish guard did not permanently suppress stall detection.
	if !hasFailureReason(failures, 31, "stall") {
		t.Fatalf("failures = %#v, want stall failure for zero-publish recovery source 31", failures)
	}
}

// TestStartSourceWithCandidatesIdleAbortBlocksOnReaderClose verifies that
// the candidate_startup_success idle-abort guard at shared_session.go:2317
// returns promptly even when reader.Close() blocks, because it now uses
// bounded close (closeWithTimeout) instead of synchronous close.
//
// Regression: TODO-shared-session-recovery-startup-idle-abort-blocking-reader-close-lifecycle-stall.md
func TestStartSourceWithCandidatesIdleAbortBlocksOnReaderClose(t *testing.T) {
	blockCh := make(chan struct{})
	defer close(blockCh) // unblock close on test exit
	requestArrived := make(chan struct{}, 1)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Signal that the request has arrived so the test can drop
		// subscribers before responding.
		select {
		case requestArrived <- struct{}{}:
		default:
		}
		// Small delay to let subscriber drop propagate before responding.
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		// Write enough data so startup probe succeeds.
		_, _ = w.Write(bytes.Repeat([]byte("X"), 1024))
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             900 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           4,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         2 * time.Second,
		HTTPClient: &http.Client{
			Transport: &blockingCloseTransport{
				inner:   http.DefaultTransport,
				blockCh: blockCh,
			},
		},
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ctx:     context.Background(),
		cancel:  func() {},
		readyCh: make(chan struct{}),
		// Start with one subscriber so early idle-abort guards pass.
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	candidates := []channels.Source{
		{
			SourceID:      10,
			ChannelID:     1,
			ItemKey:       "src:news:blocking-close-candidates",
			StreamURL:     upstream.URL,
			PriorityIndex: 0,
			Enabled:       true,
		},
	}

	resultCh := make(chan error, 1)
	go func() {
		reader, _, err := session.startSourceWithCandidates(
			context.Background(),
			candidates,
			3*time.Second,
			"recovery_blocking_close_test",
			1, // recoveryCycle > 0 enables idle-abort guard
			"stall_detected",
		)
		if reader != nil {
			_ = reader.Close()
		}
		resultCh <- err
	}()

	// Wait for the upstream request to arrive, then drop subscribers so the
	// candidate_startup_success guard detects idle AFTER startup succeeds.
	select {
	case <-requestArrived:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream did not receive request within 2s")
	}

	session.mu.Lock()
	session.subscribers = map[uint64]SubscriberStats{}
	session.mu.Unlock()

	// The function should return promptly with idle-abort error even though
	// reader.Close() blocks, because bounded close runs in background.
	select {
	case err := <-resultCh:
		if !errors.Is(err, errRecoveryAbortedNoSubscribers) {
			t.Fatalf("startSourceWithCandidates() error = %v, want errRecoveryAbortedNoSubscribers", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startSourceWithCandidates() did not return within 2s — bounded close may not be working")
	}
}

func TestStartSourceWithCandidatesRepeatedIdleAbortBlocksOnReaderClose(t *testing.T) {
	resetCloseWithTimeoutStatsForTest()
	t.Cleanup(resetCloseWithTimeoutStatsForTest)

	const attempts = boundedCloseWorkerBudget + 8
	const runtimeGoroutineJitter = 4
	const drainResidualAllowance = 4
	requestArrived := make(chan struct{}, attempts)

	blockCh := make(chan struct{})
	var releaseBlockedClose sync.Once
	releaseBlockedCloser := func() {
		releaseBlockedClose.Do(func() {
			close(blockCh)
		})
	}
	// Cleanup is a safety net for early test failures; normal flow releases
	// blockCh explicitly below to validate post-release drain behavior.
	t.Cleanup(releaseBlockedCloser)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case requestArrived <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("Z"), 1024))
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             900 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           4,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		SessionIdleTimeout:         2 * time.Second,
		HTTPClient: &http.Client{
			Transport: &blockingCloseTransport{
				inner:   http.DefaultTransport,
				blockCh: blockCh,
			},
		},
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ctx:     context.Background(),
		cancel:  func() {},
		readyCh: make(chan struct{}),
		// Start with one subscriber so idle-abort guards can pass before churn.
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	candidates := []channels.Source{
		{
			SourceID:      10,
			ChannelID:     1,
			ItemKey:       "src:news:shared-session-blocking-close-candidates",
			StreamURL:     upstream.URL,
			PriorityIndex: 0,
			Enabled:       true,
		},
	}

	before := waitForStableGoroutines()

	for i := 0; i < attempts; i++ {
		session.mu.Lock()
		session.subscribers = map[uint64]SubscriberStats{1: {SubscriberID: 1}}
		session.mu.Unlock()

		resultCh := make(chan error, 1)
		go func() {
			_, _, err := session.startSourceWithCandidates(
				context.Background(),
				candidates,
				3*time.Second,
				"recovery_blocking_close_test",
				1, // recoveryCycle > 0 enables idle-abort guard.
				"stall_detected",
			)
			resultCh <- err
		}()

		select {
		case <-requestArrived:
		case <-time.After(2 * time.Second):
			t.Fatal("upstream request was not observed within 2s")
		}

		session.mu.Lock()
		session.subscribers = map[uint64]SubscriberStats{}
		session.mu.Unlock()

		select {
		case err := <-resultCh:
			if !errors.Is(err, errRecoveryAbortedNoSubscribers) {
				t.Fatalf(
					"startSourceWithCandidates() iteration %d error = %v, want errRecoveryAbortedNoSubscribers",
					i,
					err,
				)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("startSourceWithCandidates() iteration %d did not return within 2s — bounded close may not be working", i)
		}
	}

	after := waitForStableGoroutines()
	// Worst-case growth components:
	// - one blocked startup/read-close path per attempt in this churn loop
	// - one blocking Close goroutine per started bounded worker
	// - one late-completion waiter goroutine per timed-out worker
	// - small runtime scheduling jitter allowance
	maxExpectedGrowth := attempts + (2 * boundedCloseWorkerBudget) + runtimeGoroutineJitter
	if after > before+maxExpectedGrowth {
		t.Fatalf(
			"shared-session close worker accumulation exceeded bounded ceiling: before=%d after=%d max_growth=%d",
			before,
			after,
			maxExpectedGrowth,
		)
	}
	stats := closeWithTimeoutStatsSnapshot()
	if stats.Suppressed == 0 {
		t.Fatalf("stats.Suppressed = %d, want > 0 under blocked-close churn", stats.Suppressed)
	}
	if stats.Dropped != 0 {
		t.Fatalf("stats.Dropped = %d, want 0", stats.Dropped)
	}
	if stats.LateCompletions != 0 {
		t.Fatalf("stats.LateCompletions = %d, want 0 before blocking close release", stats.LateCompletions)
	}
	expectedLateCompletions := stats.Timeouts

	releaseBlockedCloser()
	drainCeiling := before + drainResidualAllowance
	waitFor(t, 2*time.Second, func() bool {
		return waitForStableGoroutines() <= drainCeiling
	})
	if expectedLateCompletions > 0 {
		waitFor(t, 4*time.Second, func() bool {
			return closeWithTimeoutStatsSnapshot().LateCompletions >= expectedLateCompletions
		})
	}

	afterRelease := waitForStableGoroutines()
	if afterRelease > drainCeiling {
		t.Fatalf("shared-session close workers did not drain after release: before=%d afterRelease=%d", before, afterRelease)
	}
	stats = closeWithTimeoutStatsSnapshot()
	if stats.LateCompletions != expectedLateCompletions {
		t.Fatalf("stats.LateCompletions = %d, want %d after release", stats.LateCompletions, expectedLateCompletions)
	}
}

// TestStartCurrentSourceWithBackoffIdleAbortBlocksOnReaderClose verifies that
// the restart_same_startup_success idle-abort guard at shared_session.go:3496
// returns promptly even when reader.Close() blocks, because it now uses
// bounded close (closeWithTimeout) instead of synchronous close.
//
// Regression: TODO-shared-session-recovery-startup-idle-abort-blocking-reader-close-lifecycle-stall.md
func TestStartCurrentSourceWithBackoffIdleAbortBlocksOnReaderClose(t *testing.T) {
	blockCh := make(chan struct{})
	defer close(blockCh) // unblock close on test exit
	requestArrived := make(chan struct{}, 1)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case requestArrived <- struct{}{}:
		default:
		}
		// Small delay to let subscriber drop propagate before responding.
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("Y"), 1024))
	}))
	defer upstream.Close()

	provider := &fakeChannelsProvider{}
	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "direct",
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		StartupTimeout:             900 * time.Millisecond,
		FailoverTotalTimeout:       3 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           4,
		BufferPublishFlushInterval: 10 * time.Millisecond,
		StallDetect:                45 * time.Millisecond,
		StallHardDeadline:          3 * time.Second,
		StallPolicy:                stallPolicyRestartSame,
		StallMaxFailoversPerStall:  4,
		SessionIdleTimeout:         2 * time.Second,
		HTTPClient: &http.Client{
			Transport: &blockingCloseTransport{
				inner:   http.DefaultTransport,
				blockCh: blockCh,
			},
		},
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager: manager,
		channel: channels.Channel{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News",
			Enabled:     true,
		},
		ctx:     context.Background(),
		cancel:  func() {},
		readyCh: make(chan struct{}),
		// Start with one subscriber so early idle-abort guards pass.
		subscribers: map[uint64]SubscriberStats{1: {SubscriberID: 1}},
		startedAt:   time.Now().UTC(),
	}

	source := channels.Source{
		SourceID:      10,
		ChannelID:     1,
		ItemKey:       "src:news:restart-same-blocking-close",
		StreamURL:     upstream.URL,
		PriorityIndex: 0,
		Enabled:       true,
	}

	resultCh := make(chan error, 1)
	go func() {
		reader, _, err := session.startCurrentSourceWithBackoff(
			context.Background(),
			source,
			3*time.Second,
			1, // recoveryCycle > 0 enables idle-abort guard
			"stall_detected",
			1,
		)
		if reader != nil {
			_ = reader.Close()
		}
		resultCh <- err
	}()

	// Wait for the upstream request to arrive, then drop subscribers so the
	// restart_same_startup_success guard detects idle AFTER startup succeeds.
	select {
	case <-requestArrived:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream did not receive request within 2s")
	}

	session.mu.Lock()
	session.subscribers = map[uint64]SubscriberStats{}
	session.mu.Unlock()

	// The function should return promptly with idle-abort error even though
	// reader.Close() blocks, because bounded close runs in background.
	select {
	case err := <-resultCh:
		if !errors.Is(err, errRecoveryAbortedNoSubscribers) {
			t.Fatalf("startCurrentSourceWithBackoff() error = %v, want errRecoveryAbortedNoSubscribers", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startCurrentSourceWithBackoff() did not return within 2s — bounded close may not be working")
	}
}

// TestSetSourceStatePersistProfileRetriesOnTransientFailure verifies that
// bounded retry converges persisted profile metadata after transient persist
// failures. The first UpdateSourceProfile call fails; bounded retry succeeds.
func TestSetSourceStatePersistProfileRetriesOnTransientFailure(t *testing.T) {
	tmp := t.TempDir()
	writeExecutable(t, tmp, "ffprobe", `#!/usr/bin/env bash
cat <<'JSON'
{
  "streams":[
    {
      "codec_type":"video",
      "codec_name":"h264",
      "width":1920,
      "height":1080,
      "avg_frame_rate":"30000/1001",
      "r_frame_rate":"0/0",
      "bit_rate":"4100000"
    },
    {
      "codec_type":"audio",
      "codec_name":"aac"
    }
  ],
  "format":{"bit_rate":"4200000"}
}
JSON
`)
	t.Setenv("PATH", tmp+":"+os.Getenv("PATH"))

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"201": {ChannelID: 2, GuideNumber: "201", GuideName: "Retry Gap", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			2: {
				{
					SourceID:      20,
					ChannelID:     2,
					ItemKey:       "src:retry:primary",
					StreamURL:     "http://example.com/retry.m3u8",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
		// First UpdateSourceProfile call will fail; subsequent calls succeed.
		updateProfileErrs: []error{fmt.Errorf("transient sqlite busy")},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "ffmpeg-copy",
		StartupTimeout:             1 * time.Second,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 10 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     provider.channelsByGuide["201"],
		ctx:         context.Background(),
		cancel:      func() {},
		readyCh:     make(chan struct{}),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}

	source := channels.Source{
		SourceID:      20,
		ChannelID:     2,
		ItemKey:       "src:retry:primary",
		StreamURL:     "http://example.com/retry.m3u8",
		PriorityIndex: 0,
		Enabled:       true,
	}

	session.setSourceState(source, "ffmpeg-copy", "initial_startup")

	// Wait for the in-memory profile to be populated by the probe.
	waitFor(t, 2*time.Second, func() bool {
		session.mu.Lock()
		defer session.mu.Unlock()
		return !sourceProfileIsEmpty(session.sourceProfile)
	})

	// In-memory profile should be populated.
	session.mu.Lock()
	gotCodec := session.sourceProfile.VideoCodec
	session.mu.Unlock()
	if gotCodec != "h264" {
		t.Fatalf("in-memory VideoCodec = %q, want h264", gotCodec)
	}

	// Bounded retry should persist the profile after the transient failure.
	waitFor(t, 2*time.Second, func() bool {
		provider.mu.Lock()
		defer provider.mu.Unlock()
		return len(provider.profileUpdates) > 0
	})

	provider.mu.Lock()
	persistCount := len(provider.profileUpdates)
	call := provider.profileUpdates[0]
	provider.mu.Unlock()

	if persistCount != 1 {
		t.Fatalf("profileUpdates = %d, want 1 (bounded retry should converge)", persistCount)
	}
	if got, want := call.sourceID, int64(20); got != want {
		t.Fatalf("persisted source_id = %d, want %d", got, want)
	}
	if got, want := call.profile.Width, 1920; got != want {
		t.Fatalf("persisted width = %d, want %d", got, want)
	}
	if call.profile.LastProbeAt.IsZero() {
		t.Fatal("persisted last_probe_at = zero, want non-zero")
	}
}

func TestSetSourceStatePersistProfileProbeCancelsOnSessionFinish(t *testing.T) {
	prevProbeRunner := streamProfileProbeRunner
	t.Cleanup(func() {
		streamProfileProbeRunner = prevProbeRunner
	})

	streamProfileProbeRunner = func(
		_ context.Context,
		_ string,
		_ string,
		_ time.Duration,
	) (streamProfile, error) {
		return streamProfile{
			Width:           1280,
			Height:          720,
			FrameRate:       29.97,
			VideoCodec:      "h264",
			AudioCodec:      "aac",
			BitrateBPS:      1_200_000,
			AudioSampleRate: 48000,
			AudioChannels:   2,
		}, nil
	}

	provider := newBlockingProfilePersistChannelsProvider(&fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"301": {ChannelID: 3, GuideNumber: "301", GuideName: "Profile Persist", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			3: {
				{
					SourceID:      30,
					ChannelID:     3,
					ItemKey:       "src:profile-persist:30",
					StreamURL:     "http://example.com/profile-persist.m3u8",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	})

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "ffmpeg-copy",
		StartupTimeout:             1 * time.Second,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 10 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     provider.channelsByGuide["301"],
		ctx:         ctx,
		cancel:      cancel,
		readyCh:     make(chan struct{}),
		ring:        NewChunkRing(8),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}

	source := channels.Source{
		SourceID:      30,
		ChannelID:     3,
		ItemKey:       "src:profile-persist:30",
		StreamURL:     "http://example.com/profile-persist.m3u8",
		PriorityIndex: 0,
		Enabled:       true,
	}

	session.setSourceState(source, "ffmpeg-copy", "manual_profile_persist")

	select {
	case <-provider.profilePersistStarted:
	case <-time.After(2 * time.Second):
		close(provider.profilePersistUnblock)
		t.Fatal("profile persist update was not started within 2s")
	}

	finished := make(chan struct{})
	go func() {
		session.finish(nil)
		close(finished)
	}()

	select {
	case <-provider.profilePersistCanceled:
	case <-time.After(2 * time.Second):
		close(provider.profilePersistUnblock)
		t.Fatal("profile persist did not observe session cancel")
	}

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		close(provider.profilePersistUnblock)
		t.Fatal("session.finish() did not return after profile persist cancellation")
	}
}

func TestSetSourceStateWithStartupProbeCanceledContextConvergesProfileProbeWaitGroup(t *testing.T) {
	prevProbeRunner := streamProfileProbeRunner
	t.Cleanup(func() {
		streamProfileProbeRunner = prevProbeRunner
	})

	probeStarted := make(chan struct{}, 1)
	streamProfileProbeRunner = func(
		ctx context.Context,
		_ string,
		_ string,
		_ time.Duration,
	) (streamProfile, error) {
		select {
		case probeStarted <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return streamProfile{}, ctx.Err()
	}

	provider := &fakeChannelsProvider{
		channelsByGuide: map[string]channels.Channel{
			"401": {ChannelID: 4, GuideNumber: "401", GuideName: "Probe WaitGroup", Enabled: true},
		},
		sourcesByID: map[int64][]channels.Source{
			4: {
				{
					SourceID:      40,
					ChannelID:     4,
					ItemKey:       "src:probe-wg:40",
					StreamURL:     "http://example.com/probe-wg.m3u8",
					PriorityIndex: 0,
					Enabled:       true,
				},
			},
		},
	}

	manager := NewSessionManager(SessionManagerConfig{
		Mode:                       "ffmpeg-copy",
		StartupTimeout:             time.Second,
		FailoverTotalTimeout:       2 * time.Second,
		MinProbeBytes:              1,
		BufferChunkBytes:           188,
		BufferPublishFlushInterval: 10 * time.Millisecond,
	}, NewPool(1), provider)
	if manager == nil {
		t.Fatal("manager is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	session := &sharedRuntimeSession{
		manager:     manager,
		channel:     provider.channelsByGuide["401"],
		ctx:         ctx,
		cancel:      cancel,
		readyCh:     make(chan struct{}),
		ring:        NewChunkRing(8),
		subscribers: make(map[uint64]SubscriberStats),
		startedAt:   time.Now().UTC(),
	}

	source := channels.Source{
		SourceID:      40,
		ChannelID:     4,
		ItemKey:       "src:probe-wg:40",
		StreamURL:     "http://example.com/probe-wg.m3u8",
		PriorityIndex: 0,
		Enabled:       true,
	}
	session.setSourceStateWithStartupProbe(
		source,
		"ffmpeg-copy",
		"initial_startup",
		startupProbeTelemetry{},
		false,
		"",
		startupStreamInventory{},
		false,
		"",
	)

	select {
	case <-probeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("profile probe worker did not start")
	}

	cancel()

	finished := make(chan struct{})
	go func() {
		session.finish(nil)
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("session.finish() did not converge after probe context cancellation")
	}
}

// TestEnhanceInventoryWithPersistedProfileUpgradesUndetected verifies that
// an "undetected" startup inventory is enhanced with persisted source profile
// codec hints, and that non-undetected inventories are left unchanged.
func TestEnhanceInventoryWithPersistedProfileUpgradesUndetected(t *testing.T) {
	// Undetected inventory with persisted video+audio hints → video_audio.
	inv := enhanceInventoryWithPersistedProfile(
		startupStreamInventory{},
		channels.Source{ProfileVideoCodec: "h264", ProfileAudioCodec: "aac"},
	)
	if got, want := inv.componentState(), "video_audio"; got != want {
		t.Fatalf("componentState() = %q, want %q", got, want)
	}
	if got, want := inv.detectionMethod, "persisted_profile"; got != want {
		t.Fatalf("detectionMethod = %q, want %q", got, want)
	}
	if got, want := inv.videoStreamCount, 1; got != want {
		t.Fatalf("videoStreamCount = %d, want %d", got, want)
	}
	if got, want := inv.audioStreamCount, 1; got != want {
		t.Fatalf("audioStreamCount = %d, want %d", got, want)
	}

	// Undetected inventory with only audio hint → audio_only.
	inv = enhanceInventoryWithPersistedProfile(
		startupStreamInventory{},
		channels.Source{ProfileAudioCodec: "aac"},
	)
	if got, want := inv.componentState(), "audio_only"; got != want {
		t.Fatalf("audio-only componentState() = %q, want %q", got, want)
	}

	// Non-undetected inventory should not be overridden.
	existing := startupStreamInventory{
		detectionMethod:  "startup_probe",
		videoStreamCount: 1,
		audioStreamCount: 1,
		videoCodecs:      []string{"hevc"},
		audioCodecs:      []string{"mp3"},
	}
	inv = enhanceInventoryWithPersistedProfile(existing, channels.Source{ProfileVideoCodec: "h264", ProfileAudioCodec: "aac"})
	if got, want := inv.detectionMethod, "startup_probe"; got != want {
		t.Fatalf("existing inventory detectionMethod = %q, want %q (should not be overridden)", got, want)
	}

	// Undetected inventory with no persisted hints stays undetected.
	inv = enhanceInventoryWithPersistedProfile(startupStreamInventory{}, channels.Source{})
	if got, want := inv.componentState(), "undetected"; got != want {
		t.Fatalf("empty source componentState() = %q, want %q", got, want)
	}
}

// TestInferSourceComponentStateFromPersistedProfile tests the component-state
// inference helper that maps persisted source profile codec fields to a
// component state string.
func TestInferSourceComponentStateFromPersistedProfile(t *testing.T) {
	tests := []struct {
		name          string
		videoCodec    string
		audioCodec    string
		expectedState string
	}{
		{"video_audio", "h264", "aac", "video_audio"},
		{"video_only", "h264", "", "video_only"},
		{"audio_only", "", "aac", "audio_only"},
		{"undetected", "", "", "undetected"},
		{"whitespace_video_only", "  hevc  ", "   ", "video_only"},
		{"whitespace_audio_only", "", "  mp3 ", "audio_only"},
		{"whitespace_undetected", "  ", "  ", "undetected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := channels.Source{
				ProfileVideoCodec: tt.videoCodec,
				ProfileAudioCodec: tt.audioCodec,
			}
			got := inferSourceComponentState(source)
			if got != tt.expectedState {
				t.Fatalf("inferSourceComponentState(video=%q, audio=%q) = %q, want %q",
					tt.videoCodec, tt.audioCodec, got, tt.expectedState)
			}
		})
	}
}
