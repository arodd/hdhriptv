package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestScheduledJobCallbackDefersOverlapAndStartsCatchUp(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	blockFirstRun := make(chan struct{})
	var invocationCount atomic.Int32
	jobFn := func(ctx context.Context, _ *RunContext) error {
		runNumber := invocationCount.Add(1)
		if runNumber == 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-blockFirstRun:
			}
		}
		return nil
	}

	firstRunID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, jobFn)
	if err != nil {
		t.Fatalf("Start(first run) error = %v", err)
	}

	callback, err := NewScheduledJobCallback(
		runner,
		store,
		slog.Default(),
		JobPlaylistSync,
		jobFn,
		ScheduledCatchUpOptions{
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     80 * time.Millisecond,
			FreshnessLimit: 16,
		},
	)
	if err != nil {
		t.Fatalf("NewScheduledJobCallback() error = %v", err)
	}

	if err := callback.Callback(context.Background(), JobPlaylistSync); err != nil {
		t.Fatalf("Callback() error = %v", err)
	}
	if got := testutil.ToFloat64(jobSchedulerDeferredPendingMetric.WithLabelValues(JobPlaylistSync)); got != 1 {
		t.Fatalf("deferred pending gauge = %.0f, want 1", got)
	}

	close(blockFirstRun)
	_ = waitForRunDone(t, runner, firstRunID)

	scheduledRun := waitForFinishedScheduledRun(t, runner, JobPlaylistSync, 2*time.Second)
	if scheduledRun.Status != StatusSuccess {
		t.Fatalf("scheduled run status = %q, want %q", scheduledRun.Status, StatusSuccess)
	}
	if got := invocationCount.Load(); got < 2 {
		t.Fatalf("job invocation count = %d, want at least 2", got)
	}
	if got := testutil.ToFloat64(jobSchedulerDeferredPendingMetric.WithLabelValues(JobPlaylistSync)); got != 0 {
		t.Fatalf("deferred pending gauge = %.0f, want 0 after catch-up start", got)
	}
}

func TestScheduledJobCallbackCoalescesRepeatedOverlapTicks(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	blockFirstRun := make(chan struct{})
	jobFn := func(ctx context.Context, _ *RunContext) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-blockFirstRun:
			return nil
		}
	}

	firstRunID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, jobFn)
	if err != nil {
		t.Fatalf("Start(first run) error = %v", err)
	}

	callback, err := NewScheduledJobCallback(
		runner,
		store,
		slog.Default(),
		JobPlaylistSync,
		func(context.Context, *RunContext) error { return nil },
		ScheduledCatchUpOptions{
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     80 * time.Millisecond,
			FreshnessLimit: 16,
		},
	)
	if err != nil {
		t.Fatalf("NewScheduledJobCallback() error = %v", err)
	}

	coalescedBefore := testutil.ToFloat64(jobSchedulerEventsMetric.WithLabelValues(JobPlaylistSync, "deferred_coalesced"))
	for i := 0; i < 3; i++ {
		if err := callback.Callback(context.Background(), JobPlaylistSync); err != nil {
			t.Fatalf("Callback(%d) error = %v", i, err)
		}
	}
	coalescedDelta := testutil.ToFloat64(jobSchedulerEventsMetric.WithLabelValues(JobPlaylistSync, "deferred_coalesced")) - coalescedBefore
	if coalescedDelta < 2 {
		t.Fatalf("coalesced event delta = %.0f, want at least 2", coalescedDelta)
	}

	close(blockFirstRun)
	_ = waitForRunDone(t, runner, firstRunID)
	_ = waitForFinishedScheduledRun(t, runner, JobPlaylistSync, 2*time.Second)

	runs, err := runner.ListRuns(context.Background(), JobPlaylistSync, 10, 0)
	if err != nil {
		t.Fatalf("ListRuns() error = %v", err)
	}
	scheduledCount := 0
	for _, run := range runs {
		if run.TriggeredBy == TriggerSchedule {
			scheduledCount++
		}
	}
	if scheduledCount != 1 {
		t.Fatalf("schedule-triggered runs = %d, want exactly 1 coalesced catch-up run", scheduledCount)
	}
}

func TestScheduledJobCallbackCancelsDeferredCatchUpWhenContextCanceled(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	blockFirstRun := make(chan struct{})
	var invocationCount atomic.Int32
	jobFn := func(ctx context.Context, _ *RunContext) error {
		runNumber := invocationCount.Add(1)
		if runNumber == 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-blockFirstRun:
			}
		}
		return nil
	}

	firstRunID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, jobFn)
	if err != nil {
		t.Fatalf("Start(first run) error = %v", err)
	}

	callback, err := NewScheduledJobCallback(
		runner,
		store,
		slog.Default(),
		JobPlaylistSync,
		jobFn,
		ScheduledCatchUpOptions{
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     80 * time.Millisecond,
			FreshnessLimit: 16,
		},
	)
	if err != nil {
		t.Fatalf("NewScheduledJobCallback() error = %v", err)
	}

	callbackCtx, cancelCallback := context.WithCancel(context.Background())
	if err := callback.Callback(callbackCtx, JobPlaylistSync); err != nil {
		t.Fatalf("Callback() error = %v", err)
	}
	cancelCallback()
	close(blockFirstRun)
	_ = waitForRunDone(t, runner, firstRunID)

	time.Sleep(200 * time.Millisecond)
	runs, err := runner.ListRuns(context.Background(), JobPlaylistSync, 10, 0)
	if err != nil {
		t.Fatalf("ListRuns() error = %v", err)
	}
	scheduledCount := 0
	for _, run := range runs {
		if run.TriggeredBy == TriggerSchedule {
			scheduledCount++
		}
	}
	if scheduledCount != 0 {
		t.Fatalf("schedule-triggered runs after callback cancellation = %d, want 0", scheduledCount)
	}
}

func TestScheduledJobCallbackObservesScheduledFreshness(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	seedRunID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerSchedule, func(context.Context, *RunContext) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Start(seed schedule run) error = %v", err)
	}
	_ = waitForRunDone(t, runner, seedRunID)

	callback, err := NewScheduledJobCallback(
		runner,
		store,
		slog.Default(),
		JobPlaylistSync,
		func(context.Context, *RunContext) error { return nil },
		ScheduledCatchUpOptions{
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     80 * time.Millisecond,
			FreshnessLimit: 16,
		},
	)
	if err != nil {
		t.Fatalf("NewScheduledJobCallback() error = %v", err)
	}

	if err := callback.Callback(context.Background(), JobPlaylistSync); err != nil {
		t.Fatalf("Callback() error = %v", err)
	}

	if got := testutil.ToFloat64(jobSchedulerLastSuccessMetric.WithLabelValues(JobPlaylistSync)); got <= 0 {
		t.Fatalf("last_success metric = %.0f, want > 0", got)
	}
	if got := testutil.ToFloat64(jobSchedulerFreshnessMetric.WithLabelValues(JobPlaylistSync)); got < 0 {
		t.Fatalf("freshness metric = %.6f, want >= 0", got)
	}
}

func TestScheduledJobCallbackFreshnessUsesDurableLastSuccessHistory(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	jobName := "playlist_sync_freshness_history"
	successRunID, err := runner.Start(
		context.Background(),
		jobName,
		TriggerSchedule,
		func(context.Context, *RunContext) error { return nil },
	)
	if err != nil {
		t.Fatalf("Start(seed success run) error = %v", err)
	}
	successRun := waitForRunDone(t, runner, successRunID)
	if successRun.Status != StatusSuccess {
		t.Fatalf("seed run status = %q, want %q", successRun.Status, StatusSuccess)
	}

	for i := 0; i < 6; i++ {
		idx := i
		failureRunID, err := runner.Start(
			context.Background(),
			jobName,
			TriggerSchedule,
			func(context.Context, *RunContext) error {
				return fmt.Errorf("forced failure %d", idx)
			},
		)
		if err != nil {
			t.Fatalf("Start(failure run %d) error = %v", i, err)
		}
		failureRun := waitForRunDone(t, runner, failureRunID)
		if failureRun.Status != StatusError {
			t.Fatalf("failure run %d status = %q, want %q", i, failureRun.Status, StatusError)
		}
	}

	callback, err := NewScheduledJobCallback(
		runner,
		store,
		slog.Default(),
		jobName,
		func(context.Context, *RunContext) error { return nil },
		ScheduledCatchUpOptions{
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     80 * time.Millisecond,
			FreshnessLimit: 2,
		},
	)
	if err != nil {
		t.Fatalf("NewScheduledJobCallback() error = %v", err)
	}

	now := time.Unix(successRun.FinishedAt+120, 0).UTC()
	callback.observeFreshness(context.Background(), now)

	if got := int64(testutil.ToFloat64(jobSchedulerLastSuccessMetric.WithLabelValues(jobName))); got != successRun.FinishedAt {
		t.Fatalf("last_success metric = %d, want %d from durable history", got, successRun.FinishedAt)
	}
	if got := testutil.ToFloat64(jobSchedulerFreshnessMetric.WithLabelValues(jobName)); got < 120 {
		t.Fatalf("freshness metric = %.6f, want >= 120 seconds", got)
	}
}

func waitForFinishedScheduledRun(t *testing.T, runner *Runner, jobName string, timeout time.Duration) Run {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		runs, err := runner.ListRuns(context.Background(), jobName, 10, 0)
		if err != nil {
			t.Fatalf("ListRuns() error = %v", err)
		}
		for _, run := range runs {
			if run.TriggeredBy != TriggerSchedule {
				continue
			}
			if run.Status == StatusRunning {
				continue
			}
			return run
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no finished schedule-triggered %s run before timeout", jobName)
	return Run{}
}
