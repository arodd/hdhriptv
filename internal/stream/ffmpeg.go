package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type streamFailure struct {
	status          int
	responseStarted bool
	err             error
}

func (e *streamFailure) Error() string {
	return e.err.Error()
}

func (e *streamFailure) Unwrap() error {
	return e.err
}

type sourceStartupError struct {
	reason string
	err    error
}

func (e *sourceStartupError) Error() string {
	return e.reason
}

func (e *sourceStartupError) Unwrap() error {
	return e.err
}

var upstreamStatusPattern = regexp.MustCompile(`(?i)(?:upstream returned status|http error|server returned)\s+([45][0-9]{2})(?:\b|:)`)

const (
	defaultFFmpegReconnectEnabled         = false
	defaultFFmpegReconnectDelayMax        = 3 * time.Second
	defaultFFmpegReconnectMaxRetries      = 1
	defaultFFmpegReconnectHTTPErrors      = ""
	defaultFFmpegCopyRegenerateTimestamps = true
	ffmpegReadrateInitialBurstOption      = "-readrate_initial_burst"
	ffmpegReadrateCatchupOption           = "-readrate_catchup"
	ffmpegInputFlagsOption                = "-fflags"
	ffmpegOutputTSOffsetOption            = "-output_ts_offset"
	ffmpegGenPTSFlag                      = "+genpts"
	minFFmpegStartupProbeSize             = 128_000
	minFFmpegStartupAnalyzeDelay          = 1 * time.Second
	defaultFFmpegStartupProbeSize         = 1_000_000
	defaultFFmpegStartupAnalyzeDelay      = 1500 * time.Millisecond
	ffmpegRetryStartupProbeSize           = 2_000_000
	ffmpegRetryStartupAnalyzeDelay        = 3 * time.Second
	ffmpegRetryStartupTimeoutFloor        = 250 * time.Millisecond
	ffmpegStreamStderrCaptureMax          = 64 * 1024
	startupProbeReadBufferSize            = 32 * 1024
	startupRandomAccessProbeMaxBytes      = 2 * 1024 * 1024
	previewCancelFlushWait                = 10 * time.Millisecond
	previewRetryInitialBackoff            = 1 * time.Millisecond
	previewRetryMaxBackoff                = 50 * time.Millisecond

	startupRetryReasonIncomplete = "startup_detection_incomplete"

	// boundedCloseTimeout is the default deadline for bounded close operations.
	// If a close exceeds this duration, it is abandoned in a background goroutine.
	boundedCloseTimeout = 2 * time.Second
	// boundedCloseWorkerBudget caps in-flight close workers started by
	// closeWithTimeout across all callers.
	boundedCloseWorkerBudget = 16
	// closeWithTimeoutWorkerBudgetEnv can override boundedCloseWorkerBudget at
	// process startup for high-churn deployments.
	closeWithTimeoutWorkerBudgetEnv = "CLOSE_WITH_TIMEOUT_WORKER_BUDGET"
	// closeWithTimeoutWorkerBudgetMax bounds budget overrides to avoid accidental
	// runaway goroutine budgets.
	closeWithTimeoutWorkerBudgetMax = 256
	// closeWithTimeoutRetryPollInterval provides a backstop for retry-queue
	// progress when retry notifications are not observed.
	closeWithTimeoutRetryPollInterval = 250 * time.Millisecond
	// closeWithTimeoutSuppressionLogEvery emits suppression warnings for the
	// first event and every Nth event thereafter to avoid log spam.
	closeWithTimeoutSuppressionLogEvery = 64
	// closeWithTimeoutLateCompletionReleaseTimeout bounds how long timed-out
	// close workers keep occupying a worker slot while waiting for Close to
	// return. The in-flight dedupe key is still retained until completion.
	closeWithTimeoutLateCompletionReleaseTimeout = 30 * time.Second
	// closeWithTimeoutLateCompletionAbandonTimeout bounds how long a post-timeout
	// waiter keeps waiting after the worker slot is force-released. If the close
	// still has not completed after this budget, in-flight dedupe ownership is
	// cleared so future close attempts are no longer pinned indefinitely.
	closeWithTimeoutLateCompletionAbandonTimeout = 2 * time.Minute
)

var (
	closeWithTimeoutWorkerBudget      = configuredCloseWithTimeoutWorkerBudget()
	closeWithTimeoutRetryQueueCap     = closeWithTimeoutWorkerBudget * 8
	closeWithTimeoutSuppressionLogged uint64
	closeWithTimeoutInFlightMu        sync.Mutex
	closeWithTimeoutInFlightByCloser  = make(map[uintptr]struct{})
	closeWithTimeoutWorkers           = make(chan struct{}, closeWithTimeoutWorkerBudget)
	closeWithTimeoutRetryMu           sync.Mutex
	closeWithTimeoutRetryQueue        []closeWithTimeoutRetryRequest
	closeWithTimeoutRetryByCloser     = make(map[uintptr]struct{})
	closeWithTimeoutRetryNotify       = make(chan struct{}, 1)
	closeWithTimeoutRetryOnce         sync.Once
	closeWithTimeoutTimerPool         = sync.Pool{
		New: func() any {
			timer := time.NewTimer(boundedCloseTimeout)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return timer
		},
	}
	closeWithTimeoutStarted             uint64
	closeWithTimeoutRetried             uint64
	closeWithTimeoutSuppressed          uint64
	closeWithTimeoutSuppressedDuplicate uint64
	closeWithTimeoutSuppressedBudget    uint64
	closeWithTimeoutDropped             uint64
	closeWithTimeoutTimedOut            uint64
	closeWithTimeoutLateCompletions     uint64
	closeWithTimeoutLateAbandoned       uint64
	closeWithTimeoutReleaseUnderflow    uint64

	closeWithTimeoutStartedMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_close_with_timeout_started_total",
		Help: "Total number of bounded close workers started immediately.",
	})
	closeWithTimeoutRetriedMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_close_with_timeout_retried_total",
		Help: "Total number of deferred bounded close attempts started from the retry queue.",
	})
	closeWithTimeoutSuppressedMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_close_with_timeout_suppressed_total",
		Help: "Total number of bounded close attempts suppressed due to dedupe or budget pressure.",
	})
	closeWithTimeoutSuppressedDuplicateMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_close_with_timeout_suppressed_duplicate_total",
		Help: "Total number of bounded close attempts suppressed because the closer is already in-flight.",
	})
	closeWithTimeoutSuppressedBudgetMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_close_with_timeout_suppressed_budget_total",
		Help: "Total number of bounded close attempts suppressed because the worker budget is exhausted.",
	})
	closeWithTimeoutDroppedMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_close_with_timeout_dropped_total",
		Help: "Total number of bounded close retry requests dropped because the retry queue is full.",
	})
	closeWithTimeoutTimedOutMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_close_with_timeout_timeouts_total",
		Help: "Total number of bounded close workers that exceeded their timeout budget.",
	})
	closeWithTimeoutLateCompletionsMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_close_with_timeout_late_completions_total",
		Help: "Total number of bounded close operations that completed after timeout in detached waiters.",
	})
	closeWithTimeoutLateAbandonedMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_close_with_timeout_late_abandoned_total",
		Help: "Total number of bounded close waiters abandoned after exceeding the late-abandon timeout.",
	})
	closeWithTimeoutReleaseUnderflowMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_close_with_timeout_release_underflow_total",
		Help: "Total number of bounded close worker-slot release attempts that underflowed the worker budget.",
	})
)

type closeWithTimeoutStats struct {
	Started             uint64
	Retried             uint64
	Suppressed          uint64
	SuppressedDuplicate uint64
	SuppressedBudget    uint64
	Dropped             uint64
	Queued              uint64
	Timeouts            uint64
	LateCompletions     uint64
	LateAbandoned       uint64
	ReleaseUnderflow    uint64
}

type closeWithTimeoutStartResult uint8

const (
	closeWithTimeoutStartSuccess closeWithTimeoutStartResult = iota
	closeWithTimeoutStartSuppressedDuplicate
	closeWithTimeoutStartSuppressedBudget
)

type closeWithTimeoutRetryRequest struct {
	closer    io.Closer
	timeout   time.Duration
	closerKey uintptr
}

func closeWithTimeoutRecordStarted() {
	atomic.AddUint64(&closeWithTimeoutStarted, 1)
	closeWithTimeoutStartedMetric.Inc()
}

func closeWithTimeoutRecordRetried() {
	atomic.AddUint64(&closeWithTimeoutRetried, 1)
	closeWithTimeoutRetriedMetric.Inc()
}

func closeWithTimeoutRecordSuppressed() {
	atomic.AddUint64(&closeWithTimeoutSuppressed, 1)
	closeWithTimeoutSuppressedMetric.Inc()
}

func closeWithTimeoutRecordSuppressedDuplicate() {
	atomic.AddUint64(&closeWithTimeoutSuppressedDuplicate, 1)
	closeWithTimeoutSuppressedDuplicateMetric.Inc()
}

func closeWithTimeoutRecordSuppressedBudget() {
	atomic.AddUint64(&closeWithTimeoutSuppressedBudget, 1)
	closeWithTimeoutSuppressedBudgetMetric.Inc()
}

func closeWithTimeoutRecordDropped() {
	atomic.AddUint64(&closeWithTimeoutDropped, 1)
	closeWithTimeoutDroppedMetric.Inc()
}

func closeWithTimeoutRecordTimedOut() {
	atomic.AddUint64(&closeWithTimeoutTimedOut, 1)
	closeWithTimeoutTimedOutMetric.Inc()
}

func closeWithTimeoutRecordLateCompletion() {
	atomic.AddUint64(&closeWithTimeoutLateCompletions, 1)
	closeWithTimeoutLateCompletionsMetric.Inc()
}

func closeWithTimeoutRecordLateAbandoned() {
	atomic.AddUint64(&closeWithTimeoutLateAbandoned, 1)
	closeWithTimeoutLateAbandonedMetric.Inc()
}

func closeWithTimeoutRecordReleaseUnderflow() {
	atomic.AddUint64(&closeWithTimeoutReleaseUnderflow, 1)
	closeWithTimeoutReleaseUnderflowMetric.Inc()
}

func parseCloseWithTimeoutWorkerBudget(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return boundedCloseWorkerBudget, nil
	}
	budget, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("must be an integer: %w", err)
	}
	if budget < 1 || budget > closeWithTimeoutWorkerBudgetMax {
		return 0, fmt.Errorf("must be between 1 and %d", closeWithTimeoutWorkerBudgetMax)
	}
	return budget, nil
}

func configuredCloseWithTimeoutWorkerBudget() int {
	raw := os.Getenv(closeWithTimeoutWorkerBudgetEnv)
	budget, err := parseCloseWithTimeoutWorkerBudget(raw)
	if err != nil {
		slog.Warn(
			"invalid closeWithTimeout worker budget; using default",
			"env", closeWithTimeoutWorkerBudgetEnv,
			"value", raw,
			"default_budget", boundedCloseWorkerBudget,
			"error", err,
		)
		return boundedCloseWorkerBudget
	}
	return budget
}

func closeWithTimeoutCloserKey(c io.Closer) uintptr {
	if c == nil {
		return 0
	}
	rv := reflect.ValueOf(c)
	if !rv.IsValid() {
		return 0
	}

	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
		return rv.Pointer()
	default:
		// Value-type closers do not expose stable pointer identity, so they
		// intentionally bypass in-flight deduplication.
		return 0
	}
}

func closeWithTimeoutTryStartWorker(c io.Closer, countSuppression bool) closeWithTimeoutStartResult {
	closerKey := closeWithTimeoutCloserKey(c)

	closeWithTimeoutInFlightMu.Lock()
	defer closeWithTimeoutInFlightMu.Unlock()

	if closerKey != 0 {
		if _, found := closeWithTimeoutInFlightByCloser[closerKey]; found {
			closeWithTimeoutRecordSuppression(closeWithTimeoutStartSuppressedDuplicate, countSuppression)
			return closeWithTimeoutStartSuppressedDuplicate
		}
	}

	select {
	case closeWithTimeoutWorkers <- struct{}{}:
	default:
		closeWithTimeoutRecordSuppression(closeWithTimeoutStartSuppressedBudget, countSuppression)
		return closeWithTimeoutStartSuppressedBudget
	}

	if closerKey != 0 {
		closeWithTimeoutInFlightByCloser[closerKey] = struct{}{}
	}
	closeWithTimeoutRecordStarted()

	return closeWithTimeoutStartSuccess
}

func closeWithTimeoutStartWorker(c io.Closer) closeWithTimeoutStartResult {
	return closeWithTimeoutTryStartWorker(c, true)
}

func closeWithTimeoutRecordSuppression(result closeWithTimeoutStartResult, countSuppression bool) {
	if !countSuppression {
		return
	}

	switch result {
	case closeWithTimeoutStartSuppressedDuplicate:
		closeWithTimeoutRecordSuppressedDuplicate()
	case closeWithTimeoutStartSuppressedBudget:
		closeWithTimeoutRecordSuppressedBudget()
	default:
		return
	}
	closeWithTimeoutRecordSuppressed()
}

func closeWithTimeoutSuppressionReason(duplicate, budget uint64) string {
	switch {
	case duplicate == 0 && budget > 0:
		return "budget"
	case budget == 0 && duplicate > 0:
		return "duplicate"
	case duplicate > 0 && budget > 0:
		return "mixed"
	default:
		return "unknown"
	}
}

func closeWithTimeoutMaybeLogSuppression() {
	total := atomic.LoadUint64(&closeWithTimeoutSuppressed)
	if total == 0 {
		return
	}
	if total != 1 && total%closeWithTimeoutSuppressionLogEvery != 0 {
		return
	}

	for {
		last := atomic.LoadUint64(&closeWithTimeoutSuppressionLogged)
		if total <= last {
			return
		}
		if atomic.CompareAndSwapUint64(&closeWithTimeoutSuppressionLogged, last, total) {
			break
		}
	}

	suppressedDuplicate := atomic.LoadUint64(&closeWithTimeoutSuppressedDuplicate)
	suppressedBudget := atomic.LoadUint64(&closeWithTimeoutSuppressedBudget)
	stats := closeWithTimeoutStatsSnapshot()
	slog.Warn(
		"closeWithTimeout suppression observed",
		"reason", closeWithTimeoutSuppressionReason(suppressedDuplicate, suppressedBudget),
		"close_worker_budget", closeWithTimeoutWorkerBudget,
		"close_retry_queue_budget", closeWithTimeoutRetryQueueCap,
		"close_started", stats.Started,
		"close_retried", stats.Retried,
		"close_suppressed", stats.Suppressed,
		"close_suppressed_duplicate", suppressedDuplicate,
		"close_suppressed_budget", suppressedBudget,
		"close_queued", stats.Queued,
		"close_dropped", stats.Dropped,
	)
}

func closeWithTimeoutEnsureRetryLoop() {
	closeWithTimeoutRetryOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(closeWithTimeoutRetryPollInterval)
			defer ticker.Stop()

			for {
				select {
				case <-closeWithTimeoutRetryNotify:
					closeWithTimeoutDrainRetryQueue()
				case <-ticker.C:
					if closeWithTimeoutRetryQueueHasItems() {
						closeWithTimeoutDrainRetryQueue()
					}
				}
			}
		}()
	})
}

func closeWithTimeoutSignalRetryLoop() {
	closeWithTimeoutEnsureRetryLoop()
	select {
	case closeWithTimeoutRetryNotify <- struct{}{}:
	default:
	}
}

func closeWithTimeoutRetryQueueHasItems() bool {
	closeWithTimeoutRetryMu.Lock()
	hasItems := len(closeWithTimeoutRetryQueue) > 0
	closeWithTimeoutRetryMu.Unlock()
	return hasItems
}

func closeWithTimeoutQueueRetry(c io.Closer, timeout time.Duration) {
	closeWithTimeoutQueueRetryWithSignal(c, timeout, true)
}

func closeWithTimeoutQueueRetryWithSignal(c io.Closer, timeout time.Duration, signal bool) {
	req := closeWithTimeoutRetryRequest{
		closer:    c,
		timeout:   timeout,
		closerKey: closeWithTimeoutCloserKey(c),
	}

	closeWithTimeoutRetryMu.Lock()
	defer closeWithTimeoutRetryMu.Unlock()

	if req.closerKey != 0 {
		if _, queued := closeWithTimeoutRetryByCloser[req.closerKey]; queued {
			return
		}
	}
	if len(closeWithTimeoutRetryQueue) >= closeWithTimeoutRetryQueueCap {
		closeWithTimeoutRecordDropped()
		return
	}

	closeWithTimeoutRetryQueue = append(closeWithTimeoutRetryQueue, req)
	if req.closerKey != 0 {
		closeWithTimeoutRetryByCloser[req.closerKey] = struct{}{}
	}

	if signal {
		closeWithTimeoutSignalRetryLoop()
	}
}

func closeWithTimeoutDequeueRetry() (closeWithTimeoutRetryRequest, bool) {
	closeWithTimeoutRetryMu.Lock()
	defer closeWithTimeoutRetryMu.Unlock()

	if len(closeWithTimeoutRetryQueue) == 0 {
		return closeWithTimeoutRetryRequest{}, false
	}
	req := closeWithTimeoutRetryQueue[0]
	closeWithTimeoutRetryQueue[0] = closeWithTimeoutRetryRequest{}
	closeWithTimeoutRetryQueue = closeWithTimeoutRetryQueue[1:]
	if req.closerKey != 0 {
		delete(closeWithTimeoutRetryByCloser, req.closerKey)
	}
	return req, true
}

func closeWithTimeoutRequeueRetry(req closeWithTimeoutRetryRequest) {
	closeWithTimeoutRetryMu.Lock()
	defer closeWithTimeoutRetryMu.Unlock()

	if req.closerKey != 0 {
		if _, queued := closeWithTimeoutRetryByCloser[req.closerKey]; queued {
			return
		}
	}
	if len(closeWithTimeoutRetryQueue) >= closeWithTimeoutRetryQueueCap {
		closeWithTimeoutRecordDropped()
		return
	}

	closeWithTimeoutRetryQueue = append(closeWithTimeoutRetryQueue, req)
	if req.closerKey != 0 {
		closeWithTimeoutRetryByCloser[req.closerKey] = struct{}{}
	}
}

func closeWithTimeoutDrainRetryQueue() {
	for {
		req, ok := closeWithTimeoutDequeueRetry()
		if !ok {
			return
		}

		switch closeWithTimeoutTryStartWorker(req.closer, false) {
		case closeWithTimeoutStartSuccess:
			closeWithTimeoutRecordRetried()
			go closeWithTimeoutRunWorker(req.closer, req.timeout)
		case closeWithTimeoutStartSuppressedDuplicate:
			// Another close attempt is already in-flight for this closer.
		case closeWithTimeoutStartSuppressedBudget:
			closeWithTimeoutRequeueRetry(req)
			return
		default:
			closeWithTimeoutRequeueRetry(req)
			return
		}
	}
}

func closeWithTimeoutAcquireTimer(timeout time.Duration) *time.Timer {
	timer, _ := closeWithTimeoutTimerPool.Get().(*time.Timer)
	if timer == nil {
		return time.NewTimer(timeout)
	}
	closeWithTimeoutStopAndDrainTimer(timer)
	timer.Reset(timeout)
	return timer
}

func closeWithTimeoutReleaseTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	closeWithTimeoutStopAndDrainTimer(timer)
	closeWithTimeoutTimerPool.Put(timer)
}

func closeWithTimeoutStopAndDrainTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func closeWithTimeoutRunWorker(c io.Closer, timeout time.Duration) {
	closeWithTimeoutRunWorkerWithLateCompletionTimeouts(
		c,
		timeout,
		closeWithTimeoutLateCompletionReleaseTimeout,
		closeWithTimeoutLateCompletionAbandonTimeout,
	)
}

func closeWithTimeoutRunWorkerWithLateCompletionTimeout(c io.Closer, timeout, lateReleaseTimeout time.Duration) {
	closeWithTimeoutRunWorkerWithLateCompletionTimeouts(
		c,
		timeout,
		lateReleaseTimeout,
		closeWithTimeoutLateCompletionAbandonTimeout,
	)
}

func closeWithTimeoutRunWorkerWithLateCompletionTimeouts(
	c io.Closer,
	timeout time.Duration,
	lateReleaseTimeout time.Duration,
	lateAbandonTimeout time.Duration,
) {
	done := make(chan struct{})
	go func() {
		_ = c.Close()
		close(done)
	}()
	timer := closeWithTimeoutAcquireTimer(timeout)
	defer closeWithTimeoutReleaseTimer(timer)

	select {
	case <-done:
		closeWithTimeoutFinishWorker(c)
	case <-timer.C:
		closeWithTimeoutRecordTimedOut()
		go closeWithTimeoutAwaitLateCompletion(c, done, lateReleaseTimeout, lateAbandonTimeout)
	}
}

func closeWithTimeoutAwaitLateCompletion(
	c io.Closer,
	done <-chan struct{},
	lateReleaseTimeout time.Duration,
	lateAbandonTimeout time.Duration,
) {
	if lateReleaseTimeout <= 0 {
		<-done
		closeWithTimeoutRecordLateCompletion()
		closeWithTimeoutFinishWorker(c)
		return
	}

	timer := closeWithTimeoutAcquireTimer(lateReleaseTimeout)
	defer closeWithTimeoutReleaseTimer(timer)

	select {
	case <-done:
		closeWithTimeoutRecordLateCompletion()
		closeWithTimeoutFinishWorker(c)
	case <-timer.C:
		// Force-release the worker slot after the late-completion hold timeout
		// so unrelated closers can still make progress under stuck Close calls.
		// Keep in-flight dedupe ownership until Close eventually returns.
		closeWithTimeoutReleaseWorkerSlot()
		closeWithTimeoutSignalRetryLoop()
		go closeWithTimeoutAwaitLateCompletionAfterRelease(c, done, lateAbandonTimeout)
	}
}

func closeWithTimeoutAwaitLateCompletionAfterRelease(
	c io.Closer,
	done <-chan struct{},
	lateAbandonTimeout time.Duration,
) {
	if lateAbandonTimeout <= 0 {
		<-done
		closeWithTimeoutRecordLateCompletion()
		closeWithTimeoutClearInFlight(c)
		closeWithTimeoutSignalRetryLoop()
		return
	}

	timer := closeWithTimeoutAcquireTimer(lateAbandonTimeout)
	defer closeWithTimeoutReleaseTimer(timer)

	select {
	case <-done:
		closeWithTimeoutRecordLateCompletion()
	case <-timer.C:
		closeWithTimeoutRecordLateAbandoned()
	}
	closeWithTimeoutClearInFlight(c)
	closeWithTimeoutSignalRetryLoop()
}

func closeWithTimeoutReleaseWorkerSlot() bool {
	select {
	case <-closeWithTimeoutWorkers:
		return true
	default:
		closeWithTimeoutRecordReleaseUnderflow()
		return false
	}
}

func closeWithTimeoutClearInFlight(c io.Closer) {
	closerKey := closeWithTimeoutCloserKey(c)
	if closerKey == 0 {
		return
	}
	closeWithTimeoutInFlightMu.Lock()
	delete(closeWithTimeoutInFlightByCloser, closerKey)
	closeWithTimeoutInFlightMu.Unlock()
}

func closeWithTimeoutIsInFlight(c io.Closer) bool {
	closerKey := closeWithTimeoutCloserKey(c)
	if closerKey == 0 {
		return false
	}
	closeWithTimeoutInFlightMu.Lock()
	_, exists := closeWithTimeoutInFlightByCloser[closerKey]
	closeWithTimeoutInFlightMu.Unlock()
	return exists
}

func closeWithTimeoutFinishWorker(c io.Closer) {
	if released := closeWithTimeoutReleaseWorkerSlot(); !released {
		stats := closeWithTimeoutStatsSnapshot()
		slog.Warn(
			"closeWithTimeout worker slot release underflow",
			"close_release_underflow", stats.ReleaseUnderflow,
			"close_started", stats.Started,
			"close_retried", stats.Retried,
			"close_suppressed", stats.Suppressed,
			"close_queued", stats.Queued,
			"close_timeouts", stats.Timeouts,
			"close_late_completions", stats.LateCompletions,
			"close_late_abandoned", stats.LateAbandoned,
			"close_dropped", stats.Dropped,
		)
	}
	closeWithTimeoutClearInFlight(c)
	closeWithTimeoutSignalRetryLoop()
}

func closeWithTimeoutStatsSnapshot() closeWithTimeoutStats {
	closeWithTimeoutRetryMu.Lock()
	queued := len(closeWithTimeoutRetryQueue)
	closeWithTimeoutRetryMu.Unlock()

	return closeWithTimeoutStats{
		Started:             atomic.LoadUint64(&closeWithTimeoutStarted),
		Retried:             atomic.LoadUint64(&closeWithTimeoutRetried),
		Suppressed:          atomic.LoadUint64(&closeWithTimeoutSuppressed),
		SuppressedDuplicate: atomic.LoadUint64(&closeWithTimeoutSuppressedDuplicate),
		SuppressedBudget:    atomic.LoadUint64(&closeWithTimeoutSuppressedBudget),
		Dropped:             atomic.LoadUint64(&closeWithTimeoutDropped),
		Queued:              uint64(queued),
		Timeouts:            atomic.LoadUint64(&closeWithTimeoutTimedOut),
		LateCompletions:     atomic.LoadUint64(&closeWithTimeoutLateCompletions),
		LateAbandoned:       atomic.LoadUint64(&closeWithTimeoutLateAbandoned),
		ReleaseUnderflow:    atomic.LoadUint64(&closeWithTimeoutReleaseUnderflow),
	}
}

func resetCloseWithTimeoutStatsForTest() {
	atomic.StoreUint64(&closeWithTimeoutStarted, 0)
	atomic.StoreUint64(&closeWithTimeoutRetried, 0)
	atomic.StoreUint64(&closeWithTimeoutSuppressed, 0)
	atomic.StoreUint64(&closeWithTimeoutSuppressedDuplicate, 0)
	atomic.StoreUint64(&closeWithTimeoutSuppressedBudget, 0)
	atomic.StoreUint64(&closeWithTimeoutDropped, 0)
	atomic.StoreUint64(&closeWithTimeoutTimedOut, 0)
	atomic.StoreUint64(&closeWithTimeoutLateCompletions, 0)
	atomic.StoreUint64(&closeWithTimeoutLateAbandoned, 0)
	atomic.StoreUint64(&closeWithTimeoutReleaseUnderflow, 0)
	atomic.StoreUint64(&closeWithTimeoutSuppressionLogged, 0)

	closeWithTimeoutRetryMu.Lock()
	closeWithTimeoutRetryQueue = nil
	for closerKey := range closeWithTimeoutRetryByCloser {
		delete(closeWithTimeoutRetryByCloser, closerKey)
	}
	closeWithTimeoutRetryMu.Unlock()

	closeWithTimeoutInFlightMu.Lock()
	for closerKey := range closeWithTimeoutInFlightByCloser {
		delete(closeWithTimeoutInFlightByCloser, closerKey)
	}
	closeWithTimeoutInFlightMu.Unlock()

drainWorkers:
	for {
		select {
		case <-closeWithTimeoutWorkers:
		default:
			break drainWorkers
		}
	}

drainRetrySignals:
	for {
		select {
		case <-closeWithTimeoutRetryNotify:
		default:
			break drainRetrySignals
		}
	}
}

func nextPreviewRetryBackoff(delay time.Duration) time.Duration {
	if delay < previewRetryInitialBackoff {
		return previewRetryInitialBackoff
	}
	next := delay * 2
	if next > previewRetryMaxBackoff {
		return previewRetryMaxBackoff
	}
	return next
}

type streamSession struct {
	reader                           io.Reader
	startupProbeBytes                int
	startupProbeRawBytes             int
	startupProbeTrimmedBytes         int
	startupProbeCutoverOffset        int
	startupProbeDroppedBytes         int
	startupProbeTelemetry            startupProbeTelemetry
	startupProbeRA                   bool
	startupProbeCodec                string
	startupInventory                 startupStreamInventory
	startupNoInitialBurstFallback    bool
	startupNoReadrateCatchupFallback bool
	startupRetryRelaxedProbe         bool
	startupRetryRelaxedProbeToken    string
	abortFn                          func()
	waitFn                           func() error
	closeFn                          func() error
}

type startupProbeTelemetry struct {
	rawBytes      int
	trimmedBytes  int
	cutoverOffset int
	droppedBytes  int
}

func startupProbeTelemetryFromLengths(rawBytes, trimmedBytes, cutoverOffset int) startupProbeTelemetry {
	if rawBytes < 0 {
		rawBytes = 0
	}
	if trimmedBytes < 0 {
		trimmedBytes = 0
	}
	if rawBytes < trimmedBytes {
		rawBytes = trimmedBytes
	}
	if cutoverOffset < 0 {
		cutoverOffset = 0
	}
	if cutoverOffset > rawBytes {
		cutoverOffset = rawBytes
	}
	droppedBytes := rawBytes - trimmedBytes
	if droppedBytes < 0 {
		droppedBytes = 0
	}
	if cutoverOffset > droppedBytes {
		cutoverOffset = droppedBytes
	}
	return startupProbeTelemetry{
		rawBytes:      rawBytes,
		trimmedBytes:  trimmedBytes,
		cutoverOffset: cutoverOffset,
		droppedBytes:  droppedBytes,
	}
}

func (t startupProbeTelemetry) normalized() startupProbeTelemetry {
	return startupProbeTelemetryFromLengths(t.rawBytes, t.trimmedBytes, t.cutoverOffset)
}

func (t startupProbeTelemetry) cutoverPercent() int {
	t = t.normalized()
	if t.rawBytes <= 0 || t.droppedBytes <= 0 {
		return 0
	}
	percent := (t.droppedBytes*100 + (t.rawBytes / 2)) / t.rawBytes
	if percent < 0 {
		return 0
	}
	if percent > 100 {
		return 100
	}
	return percent
}

type startupStreamInventory struct {
	detectionMethod  string
	videoStreamCount int
	audioStreamCount int
	videoCodecs      []string
	audioCodecs      []string
}

func (s startupStreamInventory) normalized() startupStreamInventory {
	s.detectionMethod = strings.TrimSpace(strings.ToLower(s.detectionMethod))
	if s.detectionMethod == "" {
		s.detectionMethod = "none"
	}
	if s.videoStreamCount < 0 {
		s.videoStreamCount = 0
	}
	if s.audioStreamCount < 0 {
		s.audioStreamCount = 0
	}
	s.videoCodecs = normalizeCodecList(s.videoCodecs)
	s.audioCodecs = normalizeCodecList(s.audioCodecs)
	return s
}

func (s startupStreamInventory) componentState() string {
	s = s.normalized()
	switch {
	case s.videoStreamCount > 0 && s.audioStreamCount > 0:
		return "video_audio"
	case s.videoStreamCount > 0:
		return "video_only"
	case s.audioStreamCount > 0:
		return "audio_only"
	default:
		return "undetected"
	}
}

func startupInventoryRequiresVideoAudio(inventory startupStreamInventory) error {
	inventory = inventory.normalized()
	componentState := inventory.componentState()
	if componentState == "video_audio" || componentState == "undetected" {
		return nil
	}
	return &sourceStartupError{
		reason: fmt.Sprintf(
			"startup stream inventory missing required audio+video components (state=%s method=%s video_streams=%d audio_streams=%d)",
			componentState,
			inventory.detectionMethod,
			inventory.videoStreamCount,
			inventory.audioStreamCount,
		),
		err: errors.New("startup stream inventory requires both audio and video"),
	}
}

func (s startupStreamInventory) withVideoCodecHint(codec string) startupStreamInventory {
	codec = strings.TrimSpace(strings.ToLower(codec))
	if codec == "" {
		return s.normalized()
	}
	s = s.normalized()
	if s.videoStreamCount <= 0 {
		return s
	}
	for _, existing := range s.videoCodecs {
		if existing == codec {
			return s
		}
	}
	s.videoCodecs = append(s.videoCodecs, codec)
	s.videoCodecs = normalizeCodecList(s.videoCodecs)
	return s
}

func normalizeCodecList(codecs []string) []string {
	if len(codecs) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(codecs))
	out := make([]string, 0, len(codecs))
	for _, codec := range codecs {
		normalized := strings.TrimSpace(strings.ToLower(codec))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}

func (s *streamSession) stream(ctx context.Context, w io.Writer) error {
	if s == nil || s.reader == nil {
		return &streamFailure{
			status:          http.StatusInternalServerError,
			responseStarted: true,
			err:             errors.New("stream session is not initialized"),
		}
	}

	_, copyErr := io.Copy(w, s.reader)
	if copyErr != nil && s.abortFn != nil {
		s.abortFn()
	}

	var waitErr error
	if s.waitFn != nil {
		waitErr = s.waitFn()
	}
	if s.closeFn != nil {
		_ = s.closeFn()
	}

	if copyErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: true,
			err:             fmt.Errorf("copy stream: %w", copyErr),
		}
	}

	if waitErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: true,
			err:             fmt.Errorf("stream process failed: %w", waitErr),
		}
	}

	return nil
}

func (s *streamSession) close() {
	if s == nil {
		return
	}
	if s.abortFn != nil {
		s.abortFn()
	}
	if s.waitFn != nil {
		_ = s.waitFn()
	}
	if s.closeFn != nil {
		_ = s.closeFn()
	}
}

func normalizeStartupAndStreamContexts(startupCtx, streamCtx context.Context) (context.Context, context.Context) {
	if startupCtx == nil && streamCtx == nil {
		startupCtx = context.Background()
		streamCtx = context.Background()
		return startupCtx, streamCtx
	}
	if startupCtx == nil {
		startupCtx = streamCtx
	}
	if streamCtx == nil {
		streamCtx = startupCtx
	}
	return startupCtx, streamCtx
}

func startDirect(
	ctx context.Context,
	client *http.Client,
	upstreamURL string,
	startupTimeout time.Duration,
	minProbeBytes int,
	requireRandomAccess bool,
) (*streamSession, error) {
	return startDirectWithContexts(
		ctx,
		ctx,
		client,
		upstreamURL,
		startupTimeout,
		minProbeBytes,
		requireRandomAccess,
	)
}

func startDirectWithContexts(
	startupCtx context.Context,
	streamCtx context.Context,
	client *http.Client,
	upstreamURL string,
	startupTimeout time.Duration,
	minProbeBytes int,
	requireRandomAccess bool,
) (*streamSession, error) {
	startupCtx, streamCtx = normalizeStartupAndStreamContexts(startupCtx, streamCtx)

	if startupTimeout <= 0 {
		startupTimeout = defaultStartupTimeout
	}

	attemptCtx, attemptCancel := context.WithCancel(streamCtx)

	req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, upstreamURL, nil)
	if err != nil {
		attemptCancel()
		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err:             fmt.Errorf("create upstream request: %w", err),
		}
	}

	startedAt := time.Now()
	resp, err := doDirectStartupRequest(startupCtx, attemptCancel, client, req, startupTimeout)
	if err != nil {
		if startupCtx.Err() != nil {
			return nil, startupCtx.Err()
		}
		if streamCtx.Err() != nil {
			return nil, streamCtx.Err()
		}

		var startupErr *sourceStartupError
		if errors.As(err, &startupErr) {
			return nil, &streamFailure{
				status:          http.StatusBadGateway,
				responseStarted: false,
				err:             startupErr,
			}
		}

		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err: &sourceStartupError{
				reason: "request upstream stream failed",
				err:    err,
			},
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Bound the body preview read by remaining startup budget so a
		// stalled non-2xx body does not pin startup beyond the timeout.
		previewBudget := startupTimeout - time.Since(startedAt)
		if previewBudget <= 0 {
			previewBudget = 50 * time.Millisecond
		}
		previewCtx, previewCancel := context.WithTimeout(attemptCtx, previewBudget)
		previewBytes := readBodyPreviewBounded(previewCtx, resp.Body, 256)
		previewCancel()
		// Keep the request context alive through bounded body close; canceling the
		// context at this point eagerly tears down the underlying keep-alive
		// connection and defeats connection reuse across non-2xx startup churn.
		go func() {
			closeWithTimeout(resp.Body, boundedCloseTimeout)
			// Cancel the child context after bounded close finishes so repeated
			// non-2xx startup churn does not accumulate leaked attempt contexts.
			attemptCancel()
		}()

		preview := strings.TrimSpace(string(previewBytes))
		msg := fmt.Sprintf("upstream returned status %d", resp.StatusCode)
		if preview != "" {
			msg += ": " + preview
		}

		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err: &sourceStartupError{
				reason: msg,
				err:    errors.New(msg),
			},
		}
	}

	probeTimeout := startupTimeout
	if probeTimeout > 0 {
		probeTimeout -= time.Since(startedAt)
	}
	if probeTimeout <= 0 {
		attemptCancel()
		go closeWithTimeout(resp.Body, boundedCloseTimeout)
		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err: &sourceStartupError{
				reason: "startup timeout waiting for source bytes",
				err:    context.DeadlineExceeded,
			},
		}
	}

	// Wrap resp.Body close in onceCloser so the abort callback (fired
	// asynchronously from abortProbeRead) and the post-probe error path
	// cannot race on concurrent Close() calls.
	bodyCloser := &onceCloser{c: resp.Body}

	probe, randomAccessReady, randomAccessCodec, inventoryProbe, probeTelemetry, err := readStartupProbeWithRandomAccess(
		startupCtx,
		resp.Body,
		minProbeBytes,
		probeTimeout,
		requireRandomAccess,
		func() {
			attemptCancel()
			go closeWithTimeout(bodyCloser, boundedCloseTimeout)
		},
	)
	if err != nil {
		attemptCancel()
		go closeWithTimeout(bodyCloser, boundedCloseTimeout)
		if startupCtx.Err() != nil {
			return nil, startupCtx.Err()
		}
		if streamCtx.Err() != nil {
			return nil, streamCtx.Err()
		}
		if errors.Is(err, context.DeadlineExceeded) {
			err = &sourceStartupError{
				reason: "startup timeout waiting for source bytes",
				err:    context.DeadlineExceeded,
			}
		}
		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err:             fmt.Errorf("open upstream stream: %w", err),
		}
	}
	startupInventory := startupProbeStreamInventory(inventoryProbe).withVideoCodecHint(randomAccessCodec)
	probeTelemetry = probeTelemetry.normalized()
	if inventoryErr := startupInventoryRequiresVideoAudio(startupInventory); inventoryErr != nil {
		attemptCancel()
		go closeWithTimeout(bodyCloser, boundedCloseTimeout)
		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err:             inventoryErr,
		}
	}

	return &streamSession{
		reader:                    io.MultiReader(bytes.NewReader(probe), resp.Body),
		startupProbeBytes:         probeTelemetry.trimmedBytes,
		startupProbeRawBytes:      probeTelemetry.rawBytes,
		startupProbeTrimmedBytes:  probeTelemetry.trimmedBytes,
		startupProbeCutoverOffset: probeTelemetry.cutoverOffset,
		startupProbeDroppedBytes:  probeTelemetry.droppedBytes,
		startupProbeTelemetry:     probeTelemetry,
		startupProbeRA:            randomAccessReady,
		startupProbeCodec:         randomAccessCodec,
		startupInventory:          startupInventory.normalized(),
		abortFn: func() {
			attemptCancel()
			go closeWithTimeout(bodyCloser, boundedCloseTimeout)
		},
		closeFn: func() error {
			attemptCancel()
			go closeWithTimeout(bodyCloser, boundedCloseTimeout)
			return nil
		},
	}, nil
}

func startFFmpeg(
	ctx context.Context,
	ffmpegPath, upstreamURL, mode string,
	startupTimeout time.Duration,
	minProbeBytes int,
	readRate float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	requireRandomAccess bool,
) (*streamSession, error) {
	return startFFmpegWithContextsConfigured(
		ctx,
		ctx,
		ffmpegPath,
		upstreamURL,
		mode,
		startupTimeout,
		minProbeBytes,
		readRate,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		defaultFFmpegCopyRegenerateTimestamps,
		0,
		requireRandomAccess,
	)
}

func startFFmpegWithContexts(
	startupCtx context.Context,
	streamCtx context.Context,
	ffmpegPath, upstreamURL, mode string,
	startupTimeout time.Duration,
	minProbeBytes int,
	readRate float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	requireRandomAccess bool,
) (*streamSession, error) {
	return startFFmpegWithContextsConfigured(
		startupCtx,
		streamCtx,
		ffmpegPath,
		upstreamURL,
		mode,
		startupTimeout,
		minProbeBytes,
		readRate,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		defaultFFmpegCopyRegenerateTimestamps,
		0,
		requireRandomAccess,
	)
}

func startFFmpegWithContextsConfigured(
	startupCtx context.Context,
	streamCtx context.Context,
	ffmpegPath, upstreamURL, mode string,
	startupTimeout time.Duration,
	minProbeBytes int,
	readRate float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	copyRegenerateTimestamps bool,
	outputTSOffset time.Duration,
	requireRandomAccess bool,
) (*streamSession, error) {
	return startFFmpegWithContextsConfiguredReadrateCatchup(
		startupCtx,
		streamCtx,
		ffmpegPath,
		upstreamURL,
		mode,
		startupTimeout,
		minProbeBytes,
		readRate,
		readRate,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		copyRegenerateTimestamps,
		outputTSOffset,
		requireRandomAccess,
	)
}

func startFFmpegWithContextsConfiguredReadrateCatchup(
	startupCtx context.Context,
	streamCtx context.Context,
	ffmpegPath, upstreamURL, mode string,
	startupTimeout time.Duration,
	minProbeBytes int,
	readRate float64,
	readRateCatchup float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	copyRegenerateTimestamps bool,
	outputTSOffset time.Duration,
	requireRandomAccess bool,
) (*streamSession, error) {
	startupCtx, streamCtx = normalizeStartupAndStreamContexts(startupCtx, streamCtx)

	startupProbeSize, startupAnalyzeDuration = normalizeFFmpegStartupDetection(startupProbeSize, startupAnalyzeDuration)

	startedAt := time.Now()
	session, err := startFFmpegOnceWithContextsConfiguredReadrateCatchup(
		startupCtx,
		streamCtx,
		ffmpegPath,
		upstreamURL,
		mode,
		startupTimeout,
		minProbeBytes,
		readRate,
		readRateCatchup,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		copyRegenerateTimestamps,
		outputTSOffset,
		requireRandomAccess,
	)
	if err != nil {
		return nil, err
	}

	inventoryErr := startupInventoryRequiresVideoAudio(session.startupInventory)
	if inventoryErr == nil {
		return session, nil
	}

	relaxedProbeSize, relaxedAnalyzeDuration := relaxedFFmpegStartupDetection(startupProbeSize, startupAnalyzeDuration)
	if relaxedProbeSize <= startupProbeSize && relaxedAnalyzeDuration <= startupAnalyzeDuration {
		session.close()
		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err:             inventoryErr,
		}
	}

	remaining := startupTimeout - time.Since(startedAt)
	if remaining <= ffmpegRetryStartupTimeoutFloor {
		session.close()
		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err:             inventoryErr,
		}
	}

	retrySession, retryErr := startFFmpegOnceWithContextsConfiguredReadrateCatchup(
		startupCtx,
		streamCtx,
		ffmpegPath,
		upstreamURL,
		mode,
		remaining,
		minProbeBytes,
		readRate,
		readRateCatchup,
		initialBurst,
		relaxedProbeSize,
		relaxedAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		copyRegenerateTimestamps,
		outputTSOffset,
		requireRandomAccess,
	)
	if retryErr != nil {
		session.close()
		return nil, retryErr
	}

	session.close()
	if retryInventoryErr := startupInventoryRequiresVideoAudio(retrySession.startupInventory); retryInventoryErr != nil {
		retrySession.close()
		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err:             retryInventoryErr,
		}
	}
	retrySession.startupRetryRelaxedProbe = true
	retrySession.startupRetryRelaxedProbeToken = startupRetryReasonIncomplete
	return retrySession, nil
}

func startFFmpegOnce(
	ctx context.Context,
	ffmpegPath, upstreamURL, mode string,
	startupTimeout time.Duration,
	minProbeBytes int,
	readRate float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	requireRandomAccess bool,
) (*streamSession, error) {
	return startFFmpegOnceWithContextsConfigured(
		ctx,
		ctx,
		ffmpegPath,
		upstreamURL,
		mode,
		startupTimeout,
		minProbeBytes,
		readRate,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		defaultFFmpegCopyRegenerateTimestamps,
		0,
		requireRandomAccess,
	)
}

func startFFmpegOnceWithContexts(
	startupCtx context.Context,
	streamCtx context.Context,
	ffmpegPath, upstreamURL, mode string,
	startupTimeout time.Duration,
	minProbeBytes int,
	readRate float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	requireRandomAccess bool,
) (*streamSession, error) {
	return startFFmpegOnceWithContextsConfigured(
		startupCtx,
		streamCtx,
		ffmpegPath,
		upstreamURL,
		mode,
		startupTimeout,
		minProbeBytes,
		readRate,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		defaultFFmpegCopyRegenerateTimestamps,
		0,
		requireRandomAccess,
	)
}

func startFFmpegOnceWithContextsConfigured(
	startupCtx context.Context,
	streamCtx context.Context,
	ffmpegPath, upstreamURL, mode string,
	startupTimeout time.Duration,
	minProbeBytes int,
	readRate float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	copyRegenerateTimestamps bool,
	outputTSOffset time.Duration,
	requireRandomAccess bool,
) (*streamSession, error) {
	return startFFmpegOnceWithContextsConfiguredReadrateCatchup(
		startupCtx,
		streamCtx,
		ffmpegPath,
		upstreamURL,
		mode,
		startupTimeout,
		minProbeBytes,
		readRate,
		readRate,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		copyRegenerateTimestamps,
		outputTSOffset,
		requireRandomAccess,
	)
}

func startFFmpegOnceWithContextsConfiguredReadrateCatchup(
	startupCtx context.Context,
	streamCtx context.Context,
	ffmpegPath, upstreamURL, mode string,
	startupTimeout time.Duration,
	minProbeBytes int,
	readRate float64,
	readRateCatchup float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	copyRegenerateTimestamps bool,
	outputTSOffset time.Duration,
	requireRandomAccess bool,
) (*streamSession, error) {
	startupCtx, streamCtx = normalizeStartupAndStreamContexts(startupCtx, streamCtx)
	startedAt := time.Now()

	args := ffmpegArgsWithCopyTimestampRegenerationReadrateCatchupAndOutputTSOffset(
		mode,
		upstreamURL,
		readRate,
		readRateCatchup,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		copyRegenerateTimestamps,
		outputTSOffset,
	)
	if len(args) == 0 {
		return nil, &streamFailure{
			status:          http.StatusInternalServerError,
			responseStarted: false,
			err:             fmt.Errorf("unsupported ffmpeg mode %q", mode),
		}
	}

	cmd := exec.CommandContext(streamCtx, ffmpegPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err:             fmt.Errorf("create ffmpeg stdout pipe: %w", err),
		}
	}

	stderr := newBoundedTailBuffer(ffmpegStreamStderrCaptureMax)
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		if startupCtx.Err() != nil {
			return nil, startupCtx.Err()
		}
		if streamCtx.Err() != nil {
			return nil, streamCtx.Err()
		}
		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err:             fmt.Errorf("start ffmpeg: %w", err),
		}
	}

	probe, randomAccessReady, randomAccessCodec, inventoryProbe, probeTelemetry, err := readStartupProbeWithRandomAccess(
		startupCtx,
		stdout,
		minProbeBytes,
		startupTimeout,
		requireRandomAccess,
		func() {
			killCommand(cmd)
		},
	)
	if err != nil {
		killCommand(cmd)
		waitErr := waitFFmpeg(cmd, stderr)
		if waitErr != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			if ffmpegReadrateInitialBurstUnsupported(waitErr) && ffmpegArgsContains(args, ffmpegReadrateInitialBurstOption) {
				remaining := startupTimeout
				if startupTimeout > 0 {
					remaining = startupTimeout - time.Since(startedAt)
				}
				if remaining > 0 || startupTimeout <= 0 {
					fallbackSession, fallbackErr := startFFmpegOnceWithContextsConfiguredReadrateCatchup(
						startupCtx,
						streamCtx,
						ffmpegPath,
						upstreamURL,
						mode,
						remaining,
						minProbeBytes,
						readRate,
						readRateCatchup,
						-1,
						startupProbeSize,
						startupAnalyzeDuration,
						reconnectEnabled,
						reconnectDelayMax,
						reconnectMaxRetries,
						reconnectHTTPErrors,
						copyRegenerateTimestamps,
						outputTSOffset,
						requireRandomAccess,
					)
					if fallbackErr == nil {
						fallbackSession.startupNoInitialBurstFallback = true
						return fallbackSession, nil
					}
					return nil, fallbackErr
				}
			}
			if ffmpegReadrateCatchupUnsupported(waitErr) && ffmpegArgsContains(args, ffmpegReadrateCatchupOption) {
				remaining := startupTimeout
				if startupTimeout > 0 {
					remaining = startupTimeout - time.Since(startedAt)
				}
				if remaining > 0 || startupTimeout <= 0 {
					fallbackSession, fallbackErr := startFFmpegOnceWithContextsConfiguredReadrateCatchup(
						startupCtx,
						streamCtx,
						ffmpegPath,
						upstreamURL,
						mode,
						remaining,
						minProbeBytes,
						readRate,
						-1,
						initialBurst,
						startupProbeSize,
						startupAnalyzeDuration,
						reconnectEnabled,
						reconnectDelayMax,
						reconnectMaxRetries,
						reconnectHTTPErrors,
						copyRegenerateTimestamps,
						outputTSOffset,
						requireRandomAccess,
					)
					if fallbackErr == nil {
						fallbackSession.startupNoReadrateCatchupFallback = true
						return fallbackSession, nil
					}
					return nil, fallbackErr
				}
			}
			err = fmt.Errorf("%w (ffmpeg: %v)", err, waitErr)
		}
		if startupCtx.Err() != nil {
			return nil, startupCtx.Err()
		}
		if streamCtx.Err() != nil {
			return nil, streamCtx.Err()
		}
		return nil, &streamFailure{
			status:          http.StatusBadGateway,
			responseStarted: false,
			err:             fmt.Errorf("open ffmpeg stream: %w", err),
		}
	}
	startupInventory := startupProbeStreamInventory(inventoryProbe).withVideoCodecHint(randomAccessCodec)
	probeTelemetry = probeTelemetry.normalized()

	return &streamSession{
		reader:                    io.MultiReader(bytes.NewReader(probe), stdout),
		startupProbeBytes:         probeTelemetry.trimmedBytes,
		startupProbeRawBytes:      probeTelemetry.rawBytes,
		startupProbeTrimmedBytes:  probeTelemetry.trimmedBytes,
		startupProbeCutoverOffset: probeTelemetry.cutoverOffset,
		startupProbeDroppedBytes:  probeTelemetry.droppedBytes,
		startupProbeTelemetry:     probeTelemetry,
		startupProbeRA:            randomAccessReady,
		startupProbeCodec:         randomAccessCodec,
		startupInventory:          startupInventory.normalized(),
		abortFn: func() {
			killCommand(cmd)
		},
		waitFn: func() error {
			return waitFFmpeg(cmd, stderr)
		},
	}, nil
}

func startupFailureReason(err error) string {
	if err == nil {
		return "startup failed"
	}

	var startupErr *sourceStartupError
	if errors.As(err, &startupErr) {
		reason := strings.TrimSpace(startupErr.reason)
		if reason != "" {
			// Keep explicit direct-source reasons (which can include response previews),
			// but prefer extracted HTTP status details for wrapped ffmpeg startup errors.
			if statusReason := upstreamStatusFailureReason(reason); statusReason != "" {
				return reason
			}
			if statusReason := upstreamStatusFailureReason(err.Error()); statusReason != "" {
				return statusReason
			}
			return reason
		}
		if statusReason := upstreamStatusFailureReason(err.Error()); statusReason != "" {
			return statusReason
		}
	}

	if statusReason := upstreamStatusFailureReason(err.Error()); statusReason != "" {
		return statusReason
	}

	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return "startup failed"
	}
	return msg
}

func upstreamStatusFailureReason(msg string) string {
	matches := upstreamStatusPattern.FindStringSubmatch(msg)
	if len(matches) != 2 {
		return ""
	}
	return "upstream returned status " + matches[1]
}

func upstreamStatusCode(msg string) int {
	matches := upstreamStatusPattern.FindStringSubmatch(msg)
	if len(matches) != 2 {
		return 0
	}
	code, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0
	}
	return code
}

func isLikelyUpstreamOverlimitStatus(code int) bool {
	return code == http.StatusTooManyRequests
}

type directStartupRequestResult struct {
	resp *http.Response
	err  error
}

func doDirectStartupRequest(
	startupCtx context.Context,
	cancel context.CancelFunc,
	client *http.Client,
	req *http.Request,
	startupTimeout time.Duration,
) (*http.Response, error) {
	if startupCtx == nil {
		startupCtx = context.Background()
	}

	resultCh := make(chan directStartupRequestResult)
	abortCh := make(chan struct{})
	go func() {
		resp, err := client.Do(req)
		result := directStartupRequestResult{resp: resp, err: err}
		select {
		case resultCh <- result:
		case <-abortCh:
			closeDirectResponseBody(result.resp)
		}
	}()

	if startupTimeout <= 0 {
		select {
		case <-startupCtx.Done():
			abortDirectStartupRequest(cancel, abortCh)
			return nil, startupCtx.Err()
		case result := <-resultCh:
			return result.resp, result.err
		}
	}

	timer := time.NewTimer(startupTimeout)
	defer timer.Stop()

	select {
	case <-startupCtx.Done():
		abortDirectStartupRequest(cancel, abortCh)
		return nil, startupCtx.Err()
	case <-timer.C:
		abortDirectStartupRequest(cancel, abortCh)
		return nil, &sourceStartupError{
			reason: "startup timeout waiting for upstream response headers",
			err:    context.DeadlineExceeded,
		}
	case result := <-resultCh:
		return result.resp, result.err
	}
}

func abortDirectStartupRequest(cancel context.CancelFunc, abortCh chan struct{}) {
	if cancel != nil {
		cancel()
	}
	if abortCh != nil {
		close(abortCh)
	}
}

func closeDirectResponseBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	// Use bounded close so late-response startup abort paths remain
	// non-blocking while preserving a bounded lifecycle for delayed close behavior.
	go closeWithTimeout(resp.Body, boundedCloseTimeout)
}

// closeWithTimeout attempts to close the given closer within the specified
// timeout. If the close does not complete in time, it is abandoned in a
// detached background goroutine so the caller is not blocked indefinitely.
func closeWithTimeout(c io.Closer, timeout time.Duration) {
	if c == nil {
		return
	}
	if timeout <= 0 {
		timeout = boundedCloseTimeout
	}

	switch result := closeWithTimeoutStartWorker(c); result {
	case closeWithTimeoutStartSuccess:
		closeWithTimeoutRunWorker(c, timeout)
	case closeWithTimeoutStartSuppressedDuplicate:
		closeWithTimeoutMaybeLogSuppression()
		return
	case closeWithTimeoutStartSuppressedBudget:
		closeWithTimeoutQueueRetry(c, timeout)
		closeWithTimeoutMaybeLogSuppression()
		return
	default:
		return
	}
}

// onceCloser wraps an io.Closer so that Close is called at most once,
// making it safe for concurrent use from multiple goroutines.
type onceCloser struct {
	c    io.Closer
	once sync.Once
	err  error
}

func (o *onceCloser) Close() error {
	o.once.Do(func() { o.err = o.c.Close() })
	return o.err
}

// readBodyPreviewBounded reads up to maxBytes from r, bounded by the given
// context. Returns whatever bytes were read before the context expired.
func readBodyPreviewBounded(ctx context.Context, r io.ReadCloser, maxBytes int64) []byte {
	if r == nil || maxBytes <= 0 {
		return nil
	}
	if maxBytes > int64(^uint(0)>>1) {
		maxBytes = int64(^uint(0) >> 1)
	}

	maxPreviewBytes := int(maxBytes)
	previewCtx, previewCancel := context.WithCancel(ctx)
	defer previewCancel()

	resultCh := make(chan []byte, 1)

	go func() {
		bufCap := startupProbeReadBufferSize
		if bufCap > maxPreviewBytes {
			bufCap = maxPreviewBytes
		}
		buf := make([]byte, 0, bufCap)
		tmpSize := startupProbeReadBufferSize
		if tmpSize > maxPreviewBytes {
			tmpSize = maxPreviewBytes
		}
		tmp := make([]byte, tmpSize)
		retryDelay := previewRetryInitialBackoff
		retryTimer := time.NewTimer(previewRetryInitialBackoff)
		closeWithTimeoutStopAndDrainTimer(retryTimer)
		defer retryTimer.Stop()

		for {
			select {
			case <-previewCtx.Done():
				resultCh <- buf
				return
			default:
			}

			if len(buf) >= maxPreviewBytes {
				resultCh <- buf
				return
			}

			remaining := maxPreviewBytes - len(buf)
			readSize := startupProbeReadBufferSize
			if readSize > remaining {
				readSize = remaining
			}

			readDone := make(chan struct {
				n   int
				err error
			}, 1)
			localTmp := tmp[:readSize]
			go func(data []byte) {
				n, err := r.Read(data)
				readDone <- struct {
					n   int
					err error
				}{n: n, err: err}
			}(localTmp)

			select {
			case <-previewCtx.Done():
				resultCh <- buf
				return
			case result := <-readDone:
				if result.n > 0 {
					buf = append(buf, localTmp[:result.n]...)
					retryDelay = previewRetryInitialBackoff
				}
				if result.err != nil {
					resultCh <- buf
					return
				}
				if result.n == 0 {
					// Reader made no progress and returned no error. Retry until
					// context cancellation or EOF/error to follow io.Reader contract.
					closeWithTimeoutStopAndDrainTimer(retryTimer)
					retryTimer.Reset(retryDelay)
					retryDelay = nextPreviewRetryBackoff(retryDelay)
					select {
					case <-previewCtx.Done():
						resultCh <- buf
						return
					case <-retryTimer.C:
					}
					continue
				}
			}
		}
	}()

	select {
	case preview := <-resultCh:
		return preview
	case <-ctx.Done():
		// Caller owns r close on timeout/cancel. Keep read-body helper focused on
		// bounded preview collection to avoid implicit double-close behavior.
		previewCancel()
		select {
		case preview := <-resultCh:
			return preview
		case <-time.After(previewCancelFlushWait):
			return nil
		}
	}
}

func readStartupProbe(
	ctx context.Context,
	reader io.Reader,
	minProbeBytes int,
	startupTimeout time.Duration,
	abort func(),
) ([]byte, error) {
	probe, _, _, _, _, err := readStartupProbeWithRandomAccess(
		ctx,
		reader,
		minProbeBytes,
		startupTimeout,
		false,
		abort,
	)
	return probe, err
}

type startupProbeOutcome struct {
	buf               []byte
	randomAccessReady bool
	randomAccessCodec string
	inventoryProbe    []byte
	telemetry         startupProbeTelemetry
	err               error
}

func readStartupProbeWithRandomAccess(
	ctx context.Context,
	reader io.Reader,
	minProbeBytes int,
	startupTimeout time.Duration,
	requireRandomAccess bool,
	abort func(),
) ([]byte, bool, string, []byte, startupProbeTelemetry, error) {
	required := minProbeBytes
	if required < 1 {
		required = 1
	}

	maxProbeBytes := required
	if requireRandomAccess && maxProbeBytes < startupRandomAccessProbeMaxBytes {
		maxProbeBytes = startupRandomAccessProbeMaxBytes
	}

	// probeCtx is canceled when timeout/parent-cancel fires so the goroutine
	// can observe the cancellation between reads and exit promptly.
	probeCtx, probeCancel := context.WithCancel(ctx)

	resultCh := make(chan startupProbeOutcome, 1)
	go func() {
		buf := make([]byte, 0, required)
		tmp := make([]byte, startupProbeReadBufferSize)

		for {
			// Check for cancellation before each read so the goroutine
			// exits promptly when startup timeout or parent cancel fires.
			select {
			case <-probeCtx.Done():
				resultCh <- startupProbeOutcome{buf: buf, err: probeCtx.Err()}
				return
			default:
			}

			if len(buf) >= required {
				if !requireRandomAccess {
					resultCh <- startupProbeOutcome{
						buf:            buf,
						inventoryProbe: buf,
						telemetry: startupProbeTelemetryFromLengths(
							len(buf),
							len(buf),
							0,
						),
					}
					return
				}
				if marker, ok := startupProbeRandomAccessInfo(buf); ok {
					cutover := marker.cutoverOffset
					if cutover < 0 {
						cutover = 0
					}
					if cutover > len(buf) {
						cutover = len(buf)
					}
					if cutover >= len(buf) {
						// The cutover marker landed exactly at the current buffer tail.
						// Keep reading so the returned probe includes stream payload.
						if len(buf) >= maxProbeBytes {
							resultCh <- startupProbeOutcome{
								buf: buf,
								err: &sourceStartupError{
									reason: fmt.Sprintf(
										"startup random-access marker was not observed in first %d bytes",
										len(buf),
									),
									err: errors.New("startup random access marker missing"),
								},
							}
							return
						}
					} else {
						trimmed := buf[cutover:]
						if len(trimmed) == 0 {
							trimmed = buf
							cutover = 0
						}
						codec := strings.TrimSpace(marker.codec)
						if codec == "" {
							codec = "unknown"
						}
						resultCh <- startupProbeOutcome{
							buf:               trimmed,
							randomAccessReady: true,
							randomAccessCodec: codec,
							inventoryProbe:    buf,
							telemetry: startupProbeTelemetryFromLengths(
								len(buf),
								len(trimmed),
								cutover,
							),
						}
						return
					}
				}
				if len(buf) >= maxProbeBytes {
					resultCh <- startupProbeOutcome{
						buf: buf,
						err: &sourceStartupError{
							reason: fmt.Sprintf(
								"startup random-access marker was not observed in first %d bytes",
								len(buf),
							),
							err: errors.New("startup random access marker missing"),
						},
					}
					return
				}
			}

			remaining := maxProbeBytes - len(buf)
			if remaining <= 0 {
				resultCh <- startupProbeOutcome{
					buf: buf,
					err: &sourceStartupError{
						reason: fmt.Sprintf(
							"startup random-access marker was not observed in first %d bytes",
							len(buf),
						),
						err: errors.New("startup random access marker missing"),
					},
				}
				return
			}
			readSize := startupProbeReadBufferSize
			if readSize > remaining {
				readSize = remaining
			}

			// Run Read in a sub-goroutine so the probe goroutine can exit
			// promptly when probeCtx is canceled, even if Read blocks
			// (e.g. unresponsive upstream or misbehaving transport).
			readDone := make(chan struct {
				n   int
				err error
			}, 1)
			go func() {
				n, err := reader.Read(tmp[:readSize])
				readDone <- struct {
					n   int
					err error
				}{n, err}
			}()

			select {
			case <-probeCtx.Done():
				resultCh <- startupProbeOutcome{buf: buf, err: probeCtx.Err()}
				return
			case r := <-readDone:
				if r.n > 0 {
					buf = append(buf, tmp[:r.n]...)
				}
				if r.err != nil {
					resultCh <- startupProbeOutcome{buf: buf, err: r.err}
					return
				}
			}
		}
	}()

	timer := time.NewTimer(startupTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		abortProbeRead(probeCancel, abort, resultCh)
		return nil, false, "", nil, startupProbeTelemetry{}, ctx.Err()
	case <-timer.C:
		abortProbeRead(probeCancel, abort, resultCh)
		reason := "startup timeout waiting for source bytes"
		if requireRandomAccess {
			reason = "startup timeout waiting for random-access source bytes"
		}
		return nil, false, "", nil, startupProbeTelemetry{}, &sourceStartupError{
			reason: reason,
			err:    context.DeadlineExceeded,
		}
	case result := <-resultCh:
		probeCancel()
		if result.err == nil {
			inventoryProbe := result.inventoryProbe
			if len(inventoryProbe) == 0 {
				inventoryProbe = result.buf
			}
			telemetry := result.telemetry.normalized()
			if telemetry.trimmedBytes == 0 && len(result.buf) > 0 {
				telemetry = startupProbeTelemetryFromLengths(
					len(inventoryProbe),
					len(result.buf),
					0,
				)
			}
			return result.buf, result.randomAccessReady, result.randomAccessCodec, inventoryProbe, telemetry, nil
		}
		if errors.Is(result.err, io.EOF) {
			switch {
			case len(result.buf) == 0:
				return nil, false, "", nil, startupProbeTelemetry{}, &sourceStartupError{
					reason: "source closed before startup bytes were received",
					err:    result.err,
				}
			case len(result.buf) < required:
				return nil, false, "", nil, startupProbeTelemetry{}, &sourceStartupError{
					reason: fmt.Sprintf("source closed early after %d of %d startup bytes", len(result.buf), required),
					err:    result.err,
				}
			case requireRandomAccess:
				return nil, false, "", nil, startupProbeTelemetry{}, &sourceStartupError{
					reason: fmt.Sprintf(
						"source closed before random-access startup bytes were received (%d bytes)",
						len(result.buf),
					),
					err: result.err,
				}
			default:
				telemetry := startupProbeTelemetryFromLengths(len(result.buf), len(result.buf), 0)
				return result.buf, false, "", result.buf, telemetry, nil
			}
		}
		return nil, false, "", nil, startupProbeTelemetry{}, &sourceStartupError{
			reason: "failed reading startup bytes",
			err:    result.err,
		}
	}
}

func abortProbeRead(probeCancel context.CancelFunc, abort func(), resultCh <-chan startupProbeOutcome) {
	// Cancel probe context first so the goroutine can observe cancellation
	// via the context-aware read select and exit promptly.
	if probeCancel != nil {
		probeCancel()
	}
	// Fire-and-forget abort. With probeCancel() already called, the probe
	// goroutine exits via its context check. The abort callback (canceling
	// request context, closing body) runs async so a blocking close/kill
	// does not delay the caller.
	if abort != nil {
		go abort()
	}
	// Wait briefly for the goroutine result. With the context-aware read,
	// the goroutine should exit almost immediately after probeCancel().
	select {
	case <-resultCh:
	case <-time.After(boundedCloseTimeout):
	}
}

func startupProbeHasRandomAccess(data []byte) (bool, string) {
	marker, ok := startupProbeRandomAccessInfo(data)
	if !ok {
		return false, ""
	}
	return true, marker.codec
}

func startupProbeStreamInventory(data []byte) startupStreamInventory {
	alignment, ok := startupFindTSPacketAlignment(data)
	if !ok {
		return startupStreamInventory{detectionMethod: "none"}
	}

	pmtPIDs := make(map[uint16]struct{})
	pmtVideoPIDs := make(map[uint16]string)
	pmtAudioPIDs := make(map[uint16]string)
	pesVideoPIDs := make(map[uint16]struct{})
	pesAudioPIDs := make(map[uint16]struct{})

	for packetOffset := alignment; packetOffset+mpegTSPacketSize <= len(data); packetOffset += mpegTSPacketSize {
		packet := data[packetOffset : packetOffset+mpegTSPacketSize]
		if len(packet) != mpegTSPacketSize || packet[0] != 0x47 {
			continue
		}

		payloadUnitStart := packet[1]&0x40 != 0
		pid := (uint16(packet[1]&0x1F) << 8) | uint16(packet[2])
		adaptationControl := (packet[3] >> 4) & 0x03
		if adaptationControl == 0 || adaptationControl == 2 {
			continue
		}

		payloadStart := 4
		if adaptationControl == 3 {
			if payloadStart >= len(packet) {
				continue
			}
			adaptationLength := int(packet[payloadStart])
			payloadStart += 1 + adaptationLength
			if payloadStart > len(packet) {
				continue
			}
		}
		if payloadStart >= len(packet) {
			continue
		}

		payload := packet[payloadStart:]
		if len(payload) == 0 {
			continue
		}

		if payloadUnitStart {
			if section, tableID, ok := startupTSSectionFromPayload(payload); ok {
				switch tableID {
				case 0x00:
					startupParsePATSection(section, pmtPIDs)
				case 0x02:
					if len(pmtPIDs) == 0 {
						startupParsePMTSection(section, pmtVideoPIDs, pmtAudioPIDs)
					} else if _, allowed := pmtPIDs[pid]; allowed {
						startupParsePMTSection(section, pmtVideoPIDs, pmtAudioPIDs)
					}
				}
			}
			if kind, ok := startupPESStreamKind(payload); ok {
				switch kind {
				case "video":
					pesVideoPIDs[pid] = struct{}{}
				case "audio":
					pesAudioPIDs[pid] = struct{}{}
				}
			}
		}
	}

	if len(pmtVideoPIDs) > 0 || len(pmtAudioPIDs) > 0 {
		videoCodecs := startupSortedCodecsByPID(pmtVideoPIDs)
		audioCodecs := startupSortedCodecsByPID(pmtAudioPIDs)
		return startupStreamInventory{
			detectionMethod:  "pmt",
			videoStreamCount: len(pmtVideoPIDs),
			audioStreamCount: len(pmtAudioPIDs),
			videoCodecs:      videoCodecs,
			audioCodecs:      audioCodecs,
		}.normalized()
	}

	if len(pesVideoPIDs) > 0 || len(pesAudioPIDs) > 0 {
		return startupStreamInventory{
			detectionMethod:  "pes",
			videoStreamCount: len(pesVideoPIDs),
			audioStreamCount: len(pesAudioPIDs),
		}.normalized()
	}

	return startupStreamInventory{detectionMethod: "none"}
}

func startupTSSectionFromPayload(payload []byte) ([]byte, byte, bool) {
	if len(payload) < 4 {
		return nil, 0, false
	}
	pointer := int(payload[0])
	start := 1 + pointer
	if start < 0 || start+3 > len(payload) {
		return nil, 0, false
	}
	if payload[start+1]&0x80 == 0 {
		return nil, 0, false
	}
	sectionLength := int(payload[start+1]&0x0F)<<8 | int(payload[start+2])
	if sectionLength < 4 {
		return nil, 0, false
	}
	totalLength := 3 + sectionLength
	if start+totalLength > len(payload) {
		return nil, 0, false
	}
	section := payload[start : start+totalLength]
	if len(section) < 3 {
		return nil, 0, false
	}
	return section, section[0], true
}

func startupParsePATSection(section []byte, pmtPIDs map[uint16]struct{}) {
	if len(section) < 12 || section[0] != 0x00 || pmtPIDs == nil {
		return
	}
	sectionLength := int(section[1]&0x0F)<<8 | int(section[2])
	end := 3 + sectionLength - 4 // exclude CRC32
	if end > len(section) || end <= 8 {
		return
	}
	for i := 8; i+4 <= end; i += 4 {
		programNumber := (uint16(section[i]) << 8) | uint16(section[i+1])
		if programNumber == 0 {
			continue
		}
		pmtPID := (uint16(section[i+2]&0x1F) << 8) | uint16(section[i+3])
		pmtPIDs[pmtPID] = struct{}{}
	}
}

func startupParsePMTSection(section []byte, videoPIDs map[uint16]string, audioPIDs map[uint16]string) {
	if len(section) < 16 || section[0] != 0x02 {
		return
	}
	sectionLength := int(section[1]&0x0F)<<8 | int(section[2])
	end := 3 + sectionLength - 4 // exclude CRC32
	if end > len(section) || end <= 12 {
		return
	}
	programInfoLength := int(section[10]&0x0F)<<8 | int(section[11])
	idx := 12 + programInfoLength
	if idx > end {
		return
	}

	for idx+5 <= end {
		streamType := section[idx]
		elementaryPID := (uint16(section[idx+1]&0x1F) << 8) | uint16(section[idx+2])
		esInfoLength := int(section[idx+3]&0x0F)<<8 | int(section[idx+4])

		kind, codec := startupCodecFromPMTStreamType(streamType)
		switch kind {
		case "video":
			videoPIDs[elementaryPID] = codec
		case "audio":
			audioPIDs[elementaryPID] = codec
		}

		idx += 5 + esInfoLength
	}
}

func startupCodecFromPMTStreamType(streamType byte) (string, string) {
	switch streamType {
	case 0x01:
		return "video", "mpeg1video"
	case 0x02:
		return "video", "mpeg2video"
	case 0x10:
		return "video", "mpeg4video"
	case 0x1B:
		return "video", "h264"
	case 0x24:
		return "video", "hevc"
	case 0x03:
		return "audio", "mpeg1audio"
	case 0x04:
		return "audio", "mpeg2audio"
	case 0x0F:
		return "audio", "aac"
	case 0x11:
		return "audio", "aac-latm"
	case 0x81:
		return "audio", "ac3"
	default:
		return "", ""
	}
}

func startupPESStreamKind(payload []byte) (string, bool) {
	if len(payload) < 6 {
		return "", false
	}
	if payload[0] != 0x00 || payload[1] != 0x00 || payload[2] != 0x01 {
		return "", false
	}
	streamID := payload[3]
	switch {
	case streamID >= 0xE0 && streamID <= 0xEF:
		return "video", true
	case streamID >= 0xC0 && streamID <= 0xDF:
		return "audio", true
	default:
		return "", false
	}
}

func startupSortedCodecsByPID(byPID map[uint16]string) []string {
	if len(byPID) == 0 {
		return nil
	}
	out := make([]string, 0, len(byPID))
	for _, codec := range byPID {
		out = append(out, codec)
	}
	return normalizeCodecList(out)
}

func annexBStartCodeLen(data []byte) int {
	if len(data) >= 4 &&
		data[0] == 0x00 &&
		data[1] == 0x00 &&
		data[2] == 0x00 &&
		data[3] == 0x01 {
		return 4
	}
	if len(data) >= 3 &&
		data[0] == 0x00 &&
		data[1] == 0x00 &&
		data[2] == 0x01 {
		return 3
	}
	return 0
}

type startupRandomAccessMarker struct {
	codec         string
	cutoverOffset int
}

type startupPIDPayloadSpan struct {
	start           int
	end             int
	packetOffset    int
	pesPacketOffset int
}

type startupPIDPayload struct {
	data  []byte
	spans []startupPIDPayloadSpan
}

func startupProbeRandomAccessInfo(data []byte) (startupRandomAccessMarker, bool) {
	alignment, ok := startupFindTSPacketAlignment(data)
	if !ok {
		return startupRandomAccessMarker{}, false
	}

	pidPayloads := map[uint16]*startupPIDPayload{}
	pidVideoPES := map[uint16]bool{}
	pidPESStartPacketOffset := map[uint16]int{}

	for packetOffset := alignment; packetOffset+mpegTSPacketSize <= len(data); packetOffset += mpegTSPacketSize {
		packet := data[packetOffset : packetOffset+mpegTSPacketSize]
		if len(packet) != mpegTSPacketSize || packet[0] != 0x47 {
			continue
		}

		payloadUnitStart := packet[1]&0x40 != 0
		pid := (uint16(packet[1]&0x1F) << 8) | uint16(packet[2])
		adaptationControl := (packet[3] >> 4) & 0x03
		if adaptationControl == 0 || adaptationControl == 2 {
			continue
		}

		payloadStart := 4
		if adaptationControl == 3 {
			if payloadStart >= len(packet) {
				continue
			}
			adaptationLength := int(packet[payloadStart])
			payloadStart += 1 + adaptationLength
			if payloadStart > len(packet) {
				continue
			}
		}
		if payloadStart >= len(packet) {
			continue
		}

		payload := packet[payloadStart:]
		if len(payload) == 0 {
			continue
		}

		if payloadUnitStart {
			pesPayloadStart, isVideoPES := startupPESPayloadStart(payload)
			if !isVideoPES {
				pidVideoPES[pid] = false
				delete(pidPESStartPacketOffset, pid)
				continue
			}
			pidVideoPES[pid] = true
			pidPESStartPacketOffset[pid] = packetOffset
			if pesPayloadStart >= len(payload) {
				continue
			}
			payload = payload[pesPayloadStart:]
		} else if !pidVideoPES[pid] {
			continue
		}

		if len(payload) == 0 {
			continue
		}

		entry := pidPayloads[pid]
		if entry == nil {
			entry = &startupPIDPayload{}
			pidPayloads[pid] = entry
		}
		pesPacketOffset := packetOffset
		if startOffset, ok := pidPESStartPacketOffset[pid]; ok && startOffset >= 0 {
			pesPacketOffset = startOffset
		}
		spanStart := len(entry.data)
		entry.data = append(entry.data, payload...)
		entry.spans = append(entry.spans, startupPIDPayloadSpan{
			start:           spanStart,
			end:             len(entry.data),
			packetOffset:    packetOffset,
			pesPacketOffset: pesPacketOffset,
		})
	}

	var (
		best  startupRandomAccessMarker
		found bool
	)
	for _, entry := range pidPayloads {
		codec, cutoverOffset, ok := startupFindPIDRandomAccessCutover(entry)
		if !ok {
			continue
		}
		if !found || cutoverOffset < best.cutoverOffset {
			best = startupRandomAccessMarker{
				codec:         codec,
				cutoverOffset: cutoverOffset,
			}
			found = true
		}
	}
	if !found {
		return startupRandomAccessMarker{}, false
	}
	if best.cutoverOffset < 0 {
		best.cutoverOffset = 0
	}
	if best.cutoverOffset > len(data) {
		best.cutoverOffset = len(data)
	}
	best.codec = strings.TrimSpace(best.codec)
	if best.codec == "" {
		return startupRandomAccessMarker{}, false
	}
	return best, true
}

func startupFindTSPacketAlignment(data []byte) (int, bool) {
	if len(data) < mpegTSPacketSize {
		return 0, false
	}

	requiredSyncPackets := 1
	if len(data) >= mpegTSPacketSize*2 {
		requiredSyncPackets = 2
	}
	if len(data) >= mpegTSPacketSize*3 {
		requiredSyncPackets = 3
	}

	maxOffset := mpegTSPacketSize
	if len(data) < maxOffset {
		maxOffset = len(data)
	}
	for offset := 0; offset < maxOffset; offset++ {
		matches := 0
		for pos := offset; pos < len(data); pos += mpegTSPacketSize {
			if data[pos] != 0x47 {
				break
			}
			matches++
			if matches >= requiredSyncPackets {
				return offset, true
			}
		}
	}
	return 0, false
}

func startupPESPayloadStart(payload []byte) (int, bool) {
	if len(payload) < 6 {
		return 0, false
	}
	if payload[0] != 0x00 || payload[1] != 0x00 || payload[2] != 0x01 {
		return 0, false
	}
	streamID := payload[3]
	if streamID < 0xE0 || streamID > 0xEF {
		return 0, false
	}
	if len(payload) < 9 {
		return len(payload), true
	}
	pesHeaderLength := int(payload[8])
	start := 9 + pesHeaderLength
	if start > len(payload) {
		start = len(payload)
	}
	return start, true
}

func startupFindPIDRandomAccessCutover(pidPayload *startupPIDPayload) (string, int, bool) {
	if pidPayload == nil || len(pidPayload.data) == 0 || len(pidPayload.spans) == 0 {
		return "", 0, false
	}

	var (
		h264SPSOffset = -1
		h264PPSOffset = -1

		hevcVPSOffset = -1
		hevcSPSOffset = -1
		hevcPPSOffset = -1
	)

	for scan := 0; ; {
		start, startCodeLen := startupFindNextAnnexBStartCode(pidPayload.data, scan)
		if start < 0 {
			break
		}
		nalStart := start + startCodeLen
		if nalStart >= len(pidPayload.data) {
			break
		}
		nextStart, _ := startupFindNextAnnexBStartCode(pidPayload.data, nalStart)
		if nextStart < 0 {
			nextStart = len(pidPayload.data)
		}

		_, pesPacketOffset := startupPacketOffsetsForPayloadIndex(pidPayload.spans, start)
		nalHeader := pidPayload.data[nalStart]

		switch nalHeader & 0x1F {
		case 7: // SPS
			h264SPSOffset = pesPacketOffset
		case 8: // PPS
			h264PPSOffset = pesPacketOffset
		case 5: // IDR
			if h264SPSOffset >= 0 && h264PPSOffset >= 0 {
				cutover := startupMinOffset(h264SPSOffset, h264PPSOffset)
				return "h264", cutover, true
			}
		}

		nalTypeHEVC := (nalHeader >> 1) & 0x3F
		switch nalTypeHEVC {
		case 32: // VPS
			hevcVPSOffset = pesPacketOffset
		case 33: // SPS
			hevcSPSOffset = pesPacketOffset
		case 34: // PPS
			hevcPPSOffset = pesPacketOffset
		case 19, 20: // IDR_W_RADL / IDR_N_LP
			if hevcVPSOffset >= 0 && hevcSPSOffset >= 0 && hevcPPSOffset >= 0 {
				cutover := startupMinOffset(hevcVPSOffset, hevcSPSOffset, hevcPPSOffset)
				return "hevc", cutover, true
			}
		}

		scan = nextStart
	}

	return "", 0, false
}

func startupMinOffset(first int, rest ...int) int {
	min := first
	for _, v := range rest {
		if v < min {
			min = v
		}
	}
	return min
}

func startupFindNextAnnexBStartCode(data []byte, from int) (int, int) {
	if from < 0 {
		from = 0
	}
	for i := from; i+3 < len(data); i++ {
		if data[i] != 0x00 || data[i+1] != 0x00 {
			continue
		}
		if data[i+2] == 0x01 {
			return i, 3
		}
		if i+3 < len(data) && data[i+2] == 0x00 && data[i+3] == 0x01 {
			return i, 4
		}
	}
	return -1, 0
}

func startupPacketOffsetsForPayloadIndex(spans []startupPIDPayloadSpan, index int) (int, int) {
	if len(spans) == 0 {
		return 0, 0
	}
	for _, span := range spans {
		if index < span.start {
			break
		}
		if index < span.end {
			return span.packetOffset, span.pesPacketOffset
		}
	}
	last := spans[len(spans)-1]
	return last.packetOffset, last.pesPacketOffset
}

func killCommand(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	// Kill the entire process group (negative PID) so wrapper-script children
	// that inherited pipe FDs are terminated together with the parent.
	// This prevents cmd.Wait() from blocking on pipe-copy goroutines when
	// orphaned children hold stdout/stderr FDs open.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	// Also kill the direct process in case Setpgid was not effective.
	_ = cmd.Process.Kill()
}

func waitFFmpeg(cmd *exec.Cmd, stderr *boundedTailBuffer) error {
	if cmd == nil {
		return nil
	}
	err := cmd.Wait()
	if err == nil {
		return nil
	}

	msg := ""
	if stderr != nil {
		msg = strings.TrimSpace(string(stderr.Bytes()))
	}
	if msg != "" {
		return fmt.Errorf("%w: %s", err, msg)
	}
	return err
}

func ffmpegReadrateInitialBurstUnsupported(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if msg == "" || !strings.Contains(msg, "readrate_initial_burst") {
		return false
	}
	return strings.Contains(msg, "unrecognized option") || strings.Contains(msg, "option not found")
}

func ffmpegReadrateCatchupUnsupported(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if msg == "" || !strings.Contains(msg, "readrate_catchup") {
		return false
	}
	return strings.Contains(msg, "unrecognized option") || strings.Contains(msg, "option not found")
}

func ffmpegArgsContains(args []string, target string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	for _, arg := range args {
		if arg == target {
			return true
		}
	}
	return false
}

type boundedTailBuffer struct {
	max       int
	buf       []byte
	truncated bool
}

func newBoundedTailBuffer(max int) *boundedTailBuffer {
	if max < 0 {
		max = 0
	}
	return &boundedTailBuffer{max: max}
}

func (b *boundedTailBuffer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b == nil {
		return len(p), nil
	}
	if b.max == 0 {
		b.truncated = true
		return len(p), nil
	}
	if len(p) >= b.max {
		if len(p) > b.max || len(b.buf) > 0 {
			b.truncated = true
		}
		b.buf = append(b.buf[:0], p[len(p)-b.max:]...)
		return len(p), nil
	}

	if len(b.buf)+len(p) > b.max {
		overflow := len(b.buf) + len(p) - b.max
		b.truncated = true
		if overflow >= len(b.buf) {
			b.buf = b.buf[:0]
		} else {
			copy(b.buf, b.buf[overflow:])
			b.buf = b.buf[:len(b.buf)-overflow]
		}
	}

	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *boundedTailBuffer) Bytes() []byte {
	if b == nil {
		return nil
	}
	if !b.truncated {
		return b.buf
	}

	prefix := fmt.Sprintf("[stderr truncated to last %d bytes] ", len(b.buf))
	out := make([]byte, 0, len(prefix)+len(b.buf))
	out = append(out, prefix...)
	out = append(out, b.buf...)
	return out
}

func ffmpegArgs(
	mode, upstreamURL string,
	readRate float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
) []string {
	return ffmpegArgsWithCopyTimestampRegeneration(
		mode,
		upstreamURL,
		readRate,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		defaultFFmpegCopyRegenerateTimestamps,
	)
}

func ffmpegArgsWithCopyTimestampRegeneration(
	mode, upstreamURL string,
	readRate float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	copyRegenerateTimestamps bool,
) []string {
	return ffmpegArgsWithCopyTimestampRegenerationAndOutputTSOffset(
		mode,
		upstreamURL,
		readRate,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		copyRegenerateTimestamps,
		0,
	)
}

func ffmpegArgsWithCopyTimestampRegenerationAndOutputTSOffset(
	mode, upstreamURL string,
	readRate float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	copyRegenerateTimestamps bool,
	outputTSOffset time.Duration,
) []string {
	return ffmpegArgsWithCopyTimestampRegenerationReadrateCatchupAndOutputTSOffset(
		mode,
		upstreamURL,
		readRate,
		readRate,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		copyRegenerateTimestamps,
		outputTSOffset,
	)
}

func ffmpegArgsWithCopyTimestampRegenerationReadrateCatchupAndOutputTSOffset(
	mode, upstreamURL string,
	readRate float64,
	readRateCatchup float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	copyRegenerateTimestamps bool,
	outputTSOffset time.Duration,
) []string {
	normalizedMode := strings.ToLower(strings.TrimSpace(mode))
	readRateArg := formatReadRate(readRate)
	includeReadrateCatchup := true
	if readRateCatchup < 0 {
		includeReadrateCatchup = false
	}
	readRateCatchupArg := formatFFmpegReadRateCatchup(readRate, readRateCatchup)
	includeInitialBurst := true
	if initialBurst < 0 {
		includeInitialBurst = false
	}
	if initialBurst == 0 {
		initialBurst = defaultProducerInitialBurstSeconds
	}
	startupProbeSize, startupAnalyzeDuration = normalizeFFmpegStartupDetection(startupProbeSize, startupAnalyzeDuration)

	inputArgs := []string{
		"-hide_banner",
		"-loglevel", "error",
		"-nostdin",
		"-readrate", readRateArg,
	}
	if includeReadrateCatchup {
		inputArgs = append(inputArgs, ffmpegReadrateCatchupOption, readRateCatchupArg)
	}
	if includeInitialBurst {
		inputArgs = append(inputArgs, ffmpegReadrateInitialBurstOption, strconv.Itoa(initialBurst))
	}
	if startupProbeSize > 0 {
		inputArgs = append(inputArgs, "-probesize", strconv.Itoa(startupProbeSize))
	}
	analyzeMicros := ffmpegAnalyzeDurationMicroseconds(startupAnalyzeDuration)
	if analyzeMicros > 0 {
		inputArgs = append(inputArgs, "-analyzeduration", strconv.FormatInt(analyzeMicros, 10))
	}
	inputArgs = appendFFmpegReconnectInputArgs(
		inputArgs,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
	)
	if normalizedMode == "ffmpeg-copy" && copyRegenerateTimestamps {
		inputArgs = append(inputArgs, ffmpegInputFlagsOption, ffmpegGenPTSFlag)
	}
	inputArgs = append(inputArgs, "-i", upstreamURL)

	switch normalizedMode {
	case "ffmpeg-copy":
		outputArgs := append([]string{
			"-c", "copy",
		}, appendFFmpegOutputTSOffsetArg(nil, outputTSOffset)...)
		outputArgs = append(outputArgs, "-f", "mpegts", "pipe:1")
		return append(inputArgs, outputArgs...)
	case "ffmpeg-transcode":
		outputArgs := []string{
			"-map", "0:v:0?",
			"-map", "0:a:0?",
			"-c:v", "libx264",
			"-preset", "veryfast",
			"-tune", "zerolatency",
			"-c:a", "aac",
			"-ar", "48000",
			"-ac", "2",
		}
		outputArgs = append(outputArgs, appendFFmpegOutputTSOffsetArg(nil, outputTSOffset)...)
		outputArgs = append(outputArgs, "-f", "mpegts", "pipe:1")
		return append(inputArgs, outputArgs...)
	default:
		return nil
	}
}

func formatFFmpegReadRateCatchup(readRate, readRateCatchup float64) string {
	baseRate := readRate
	if baseRate <= 0 {
		baseRate = defaultProducerReadRate
	}
	catchup := readRateCatchup
	if catchup <= 0 {
		catchup = baseRate
	}
	return strconv.FormatFloat(catchup, 'f', -1, 64)
}

func appendFFmpegOutputTSOffsetArg(args []string, outputTSOffset time.Duration) []string {
	formatted := formatFFmpegOutputTSOffset(outputTSOffset)
	if formatted == "" {
		return args
	}
	return append(args, ffmpegOutputTSOffsetOption, formatted)
}

func formatFFmpegOutputTSOffset(outputTSOffset time.Duration) string {
	if outputTSOffset <= 0 {
		return ""
	}
	seconds := outputTSOffset.Seconds()
	if seconds <= 0 {
		return ""
	}
	return strconv.FormatFloat(seconds, 'f', 6, 64)
}

func appendFFmpegReconnectInputArgs(args []string, enabled bool, delayMax time.Duration, maxRetries int, httpErrors string) []string {
	if !enabled {
		return args
	}

	args = append(args,
		"-reconnect", "1",
		"-reconnect_streamed", "1",
		"-reconnect_at_eof", "1",
		"-reconnect_on_network_error", "1",
	)

	trimmedHTTPErrors := strings.TrimSpace(httpErrors)
	if trimmedHTTPErrors != "" {
		args = append(args, "-reconnect_on_http_error", trimmedHTTPErrors)
	}
	if maxRetries >= 0 {
		args = append(args, "-reconnect_max_retries", strconv.Itoa(maxRetries))
	}
	args = append(args, "-reconnect_delay_max", strconv.Itoa(ffmpegReconnectDelayMaxSeconds(delayMax)))
	return args
}

func ffmpegReconnectDelayMaxSeconds(delayMax time.Duration) int {
	if delayMax <= 0 {
		return 0
	}
	seconds := int(delayMax / time.Second)
	if delayMax%time.Second != 0 {
		seconds++
	}
	return seconds
}

func ffmpegAnalyzeDurationMicroseconds(v time.Duration) int64 {
	if v <= 0 {
		return 0
	}
	micros := v / time.Microsecond
	if v%time.Microsecond != 0 {
		micros++
	}
	if micros <= 0 {
		return 1
	}
	return int64(micros)
}

func normalizeFFmpegStartupDetection(probeSize int, analyzeDuration time.Duration) (int, time.Duration) {
	if probeSize <= 0 {
		probeSize = defaultFFmpegStartupProbeSize
	}
	if probeSize < minFFmpegStartupProbeSize {
		probeSize = minFFmpegStartupProbeSize
	}
	if analyzeDuration <= 0 {
		analyzeDuration = defaultFFmpegStartupAnalyzeDelay
	}
	if analyzeDuration < minFFmpegStartupAnalyzeDelay {
		analyzeDuration = minFFmpegStartupAnalyzeDelay
	}
	return probeSize, analyzeDuration
}

func relaxedFFmpegStartupDetection(probeSize int, analyzeDuration time.Duration) (int, time.Duration) {
	probeSize, analyzeDuration = normalizeFFmpegStartupDetection(probeSize, analyzeDuration)
	if probeSize < ffmpegRetryStartupProbeSize {
		probeSize = ffmpegRetryStartupProbeSize
	}
	if analyzeDuration < ffmpegRetryStartupAnalyzeDelay {
		analyzeDuration = ffmpegRetryStartupAnalyzeDelay
	}
	return probeSize, analyzeDuration
}

func normalizeFFmpegReconnectSettings(mode string, enabled bool, delayMax time.Duration, maxRetries int, httpErrors string) (bool, time.Duration, int, string) {
	normalizedMode := strings.ToLower(strings.TrimSpace(mode))
	trimmedHTTPErrors := strings.TrimSpace(httpErrors)
	if delayMax < 0 {
		delayMax = 0
	}
	if maxRetries < -1 {
		maxRetries = -1
	}
	if normalizedMode == "direct" {
		return false, 0, -1, ""
	}

	switch normalizedMode {
	case "ffmpeg-copy", "ffmpeg-transcode":
		// Preserve v1 behavior for tests and direct constructors that did not pass reconnect config.
		if !enabled && delayMax == 0 && maxRetries == 0 && trimmedHTTPErrors == "" {
			return defaultFFmpegReconnectEnabled, defaultFFmpegReconnectDelayMax, defaultFFmpegReconnectMaxRetries, defaultFFmpegReconnectHTTPErrors
		}
	}

	return enabled, delayMax, maxRetries, trimmedHTTPErrors
}

func resolveFFmpegCopyRegenerateTimestamps(mode string, configured *bool) bool {
	if configured != nil {
		return *configured
	}
	if strings.EqualFold(strings.TrimSpace(mode), "ffmpeg-copy") {
		return defaultFFmpegCopyRegenerateTimestamps
	}
	return false
}

func startSourceSession(
	ctx context.Context,
	mode string,
	client *http.Client,
	ffmpegPath string,
	streamURL string,
	startupTimeout time.Duration,
	minProbeBytes int,
	readRate float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	requireRandomAccess bool,
) (*streamSession, error) {
	return startSourceSessionWithContextsConfigured(
		ctx,
		ctx,
		mode,
		client,
		ffmpegPath,
		streamURL,
		startupTimeout,
		minProbeBytes,
		readRate,
		readRate,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		defaultFFmpegCopyRegenerateTimestamps,
		0,
		requireRandomAccess,
	)
}

func startSourceSessionWithContexts(
	startupCtx context.Context,
	streamCtx context.Context,
	mode string,
	client *http.Client,
	ffmpegPath string,
	streamURL string,
	startupTimeout time.Duration,
	minProbeBytes int,
	readRate float64,
	readRateCatchup float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	requireRandomAccess bool,
) (*streamSession, error) {
	return startSourceSessionWithContextsConfigured(
		startupCtx,
		streamCtx,
		mode,
		client,
		ffmpegPath,
		streamURL,
		startupTimeout,
		minProbeBytes,
		readRate,
		readRateCatchup,
		initialBurst,
		startupProbeSize,
		startupAnalyzeDuration,
		reconnectEnabled,
		reconnectDelayMax,
		reconnectMaxRetries,
		reconnectHTTPErrors,
		defaultFFmpegCopyRegenerateTimestamps,
		0,
		requireRandomAccess,
	)
}

func startSourceSessionWithContextsConfigured(
	startupCtx context.Context,
	streamCtx context.Context,
	mode string,
	client *http.Client,
	ffmpegPath string,
	streamURL string,
	startupTimeout time.Duration,
	minProbeBytes int,
	readRate float64,
	readRateCatchup float64,
	initialBurst int,
	startupProbeSize int,
	startupAnalyzeDuration time.Duration,
	reconnectEnabled bool,
	reconnectDelayMax time.Duration,
	reconnectMaxRetries int,
	reconnectHTTPErrors string,
	copyRegenerateTimestamps bool,
	outputTSOffset time.Duration,
	requireRandomAccess bool,
) (*streamSession, error) {
	switch mode {
	case "direct":
		return startDirectWithContexts(
			startupCtx,
			streamCtx,
			client,
			streamURL,
			startupTimeout,
			minProbeBytes,
			false,
		)
	case "ffmpeg-copy":
		return startFFmpegWithContextsConfiguredReadrateCatchup(
			startupCtx,
			streamCtx,
			ffmpegPath,
			streamURL,
			"ffmpeg-copy",
			startupTimeout,
			minProbeBytes,
			readRate,
			readRateCatchup,
			initialBurst,
			startupProbeSize,
			startupAnalyzeDuration,
			reconnectEnabled,
			reconnectDelayMax,
			reconnectMaxRetries,
			reconnectHTTPErrors,
			copyRegenerateTimestamps,
			outputTSOffset,
			requireRandomAccess,
		)
	case "ffmpeg-transcode":
		return startFFmpegWithContextsConfiguredReadrateCatchup(
			startupCtx,
			streamCtx,
			ffmpegPath,
			streamURL,
			"ffmpeg-transcode",
			startupTimeout,
			minProbeBytes,
			readRate,
			readRateCatchup,
			initialBurst,
			startupProbeSize,
			startupAnalyzeDuration,
			reconnectEnabled,
			reconnectDelayMax,
			reconnectMaxRetries,
			reconnectHTTPErrors,
			copyRegenerateTimestamps,
			outputTSOffset,
			requireRandomAccess,
		)
	default:
		return nil, &streamFailure{
			status:          http.StatusInternalServerError,
			responseStarted: false,
			err:             fmt.Errorf("unsupported stream mode %q", mode),
		}
	}
}
