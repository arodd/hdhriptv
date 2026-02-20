package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type memoryStore struct {
	mu                 sync.Mutex
	nextID             int64
	runs               map[int64]Run
	progressWriteCount map[int64]int
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		nextID:             1,
		runs:               make(map[int64]Run),
		progressWriteCount: make(map[int64]int),
	}
}

func (m *memoryStore) CreateRun(_ context.Context, jobName, triggeredBy string, startedAt int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := m.nextID
	m.nextID++
	m.runs[id] = Run{
		RunID:       id,
		JobName:     jobName,
		TriggeredBy: triggeredBy,
		StartedAt:   startedAt,
		Status:      StatusRunning,
	}
	return id, nil
}

func (m *memoryStore) UpdateRunProgress(_ context.Context, runID int64, progressCur, progressMax int, summary string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	run, ok := m.runs[runID]
	if !ok {
		return fmt.Errorf("run %d not found", runID)
	}
	if run.Status != StatusRunning {
		return ErrRunNotRunning
	}
	run.ProgressCur = progressCur
	run.ProgressMax = progressMax
	run.Summary = summary
	m.runs[runID] = run
	m.progressWriteCount[runID]++
	return nil
}

func (m *memoryStore) FinishRun(_ context.Context, runID int64, status, errText, summary string, finishedAt int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	run, ok := m.runs[runID]
	if !ok {
		return fmt.Errorf("run %d not found", runID)
	}
	run.Status = status
	run.FinishedAt = finishedAt
	run.ErrorMessage = errText
	run.Summary = summary
	m.runs[runID] = run
	return nil
}

func (m *memoryStore) GetRun(_ context.Context, runID int64) (Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	run, ok := m.runs[runID]
	if !ok {
		return Run{}, fmt.Errorf("run %d not found", runID)
	}
	return run, nil
}

func (m *memoryStore) ListRuns(_ context.Context, jobName string, limit, offset int) ([]Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	all := make([]Run, 0, len(m.runs))
	for _, run := range m.runs {
		if jobName != "" && run.JobName != jobName {
			continue
		}
		all = append(all, run)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].StartedAt == all[j].StartedAt {
			return all[i].RunID > all[j].RunID
		}
		return all[i].StartedAt > all[j].StartedAt
	})

	if offset < 0 {
		offset = 0
	}
	if offset >= len(all) {
		return []Run{}, nil
	}
	if limit <= 0 {
		limit = len(all)
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end], nil
}

func (m *memoryStore) progressWrites(runID int64) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.progressWriteCount[runID]
}

func TestRunnerStartAndPersistProgress(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, func(ctx context.Context, run *RunContext) error {
		if err := run.SetProgress(ctx, 1, 3); err != nil {
			return err
		}
		if err := run.SetSummary(ctx, "phase 1 complete"); err != nil {
			return err
		}
		if err := run.IncrementProgress(ctx, 2); err != nil {
			return err
		}
		return run.SetSummary(ctx, "done")
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("status = %q, want %q", run.Status, StatusSuccess)
	}
	if run.ProgressCur != 3 || run.ProgressMax != 3 {
		t.Fatalf("progress = %d/%d, want 3/3", run.ProgressCur, run.ProgressMax)
	}
	if run.Summary != "done" {
		t.Fatalf("summary = %q, want done", run.Summary)
	}
	if run.FinishedAt == 0 {
		t.Fatal("finished_at was not set")
	}
}

func TestRunnerRejectsLateRunContextUpdatesAfterFinish(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runContextCh := make(chan *RunContext, 1)
	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, func(ctx context.Context, run *RunContext) error {
		runContextCh <- run
		if err := run.SetProgress(ctx, 1, 2); err != nil {
			return err
		}
		return run.SetSummary(ctx, "initial summary")
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	var runCtx *RunContext
	select {
	case runCtx = <-runContextCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for run context")
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("status = %q, want %q", run.Status, StatusSuccess)
	}
	if run.Summary != "initial summary" {
		t.Fatalf("summary = %q, want %q", run.Summary, "initial summary")
	}

	if err := runCtx.SetSummary(context.Background(), "late summary"); !errors.Is(err, ErrRunFinalized) {
		t.Fatalf("SetSummary(late) error = %v, want ErrRunFinalized", err)
	}
	if err := runCtx.SetProgress(context.Background(), 2, 2); !errors.Is(err, ErrRunFinalized) {
		t.Fatalf("SetProgress(late) error = %v, want ErrRunFinalized", err)
	}
	if writes := store.progressWrites(runID); writes != 2 {
		t.Fatalf("progress writes = %d, want 2", writes)
	}

	finalRun, err := runner.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if finalRun.Summary != "initial summary" {
		t.Fatalf("final summary = %q, want %q", finalRun.Summary, "initial summary")
	}
	if finalRun.ProgressCur != 1 || finalRun.ProgressMax != 2 {
		t.Fatalf("final progress = %d/%d, want 1/2", finalRun.ProgressCur, finalRun.ProgressMax)
	}
}

func TestRunnerEnforcesPerJobAndGlobalLocks(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	block := make(chan struct{})
	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, func(_ context.Context, _ *RunContext) error {
		<-block
		return nil
	})
	if err != nil {
		t.Fatalf("Start(first) error = %v", err)
	}

	if _, err := runner.Start(context.Background(), JobPlaylistSync, TriggerSchedule, func(context.Context, *RunContext) error { return nil }); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("Start(second same job) error = %v, want ErrAlreadyRunning", err)
	}
	if _, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerSchedule, func(context.Context, *RunContext) error { return nil }); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("Start(different job while global lock active) error = %v, want ErrAlreadyRunning", err)
	}

	close(block)
	_ = waitForRunDone(t, runner, runID)

	runner.SetGlobalLock(false)
	blockA := make(chan struct{})
	blockB := make(chan struct{})

	idA, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, func(_ context.Context, _ *RunContext) error {
		<-blockA
		return nil
	})
	if err != nil {
		t.Fatalf("Start(idA) error = %v", err)
	}
	if _, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, func(_ context.Context, _ *RunContext) error {
		<-blockB
		return nil
	}); err != nil {
		t.Fatalf("Start(idB) error = %v, want nil with global lock disabled", err)
	}

	close(blockA)
	close(blockB)
	_ = waitForRunDone(t, runner, idA)
}

func TestRunnerMarksPanicsAsFailedRuns(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, func(_ context.Context, _ *RunContext) error {
		panic("boom")
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusError {
		t.Fatalf("status = %q, want %q", run.Status, StatusError)
	}
	if run.ErrorMessage == "" {
		t.Fatal("error message was not recorded for panic")
	}
}

func TestRunnerMarksContextCancellationAsCanceled(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	startCtx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()

	runCanceled := make(chan error, 1)
	runID, err := runner.Start(startCtx, JobPlaylistSync, TriggerManual, func(ctx context.Context, _ *RunContext) error {
		<-ctx.Done()
		runCanceled <- ctx.Err()
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	select {
	case canceledErr := <-runCanceled:
		if !errors.Is(canceledErr, context.Canceled) && !errors.Is(canceledErr, context.DeadlineExceeded) {
			t.Fatalf("job context cancellation error = %v, want canceled/deadline", canceledErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for job context cancellation")
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusCanceled {
		t.Fatalf("status = %q, want %q", run.Status, StatusCanceled)
	}
	if strings.TrimSpace(run.ErrorMessage) == "" {
		t.Fatal("expected canceled run to persist error message")
	}
}

func TestRunnerCloseCancelsActiveRunsAndPreventsNewStarts(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runCanceled := make(chan error, 1)
	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, func(ctx context.Context, _ *RunContext) error {
		<-ctx.Done()
		runCanceled <- ctx.Err()
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	runner.Close()

	select {
	case canceledErr := <-runCanceled:
		if !errors.Is(canceledErr, context.Canceled) {
			t.Fatalf("job context cancellation error = %v, want context.Canceled", canceledErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Close() to cancel active run")
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusCanceled {
		t.Fatalf("status = %q, want %q", run.Status, StatusCanceled)
	}
	if runner.IsRunning(JobPlaylistSync) {
		t.Fatal("runner still reports playlist_sync as running after canceled shutdown")
	}

	if _, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, func(context.Context, *RunContext) error {
		return nil
	}); !errors.Is(err, ErrRunnerClosed) {
		t.Fatalf("Start() after Close error = %v, want ErrRunnerClosed", err)
	}
}

func TestRunnerLogsJobLifecycle(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	logs := newTestLogBuffer()
	runner.SetLogger(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})))

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, func(ctx context.Context, run *RunContext) error {
		return run.SetSummary(ctx, "lifecycle test")
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("status = %q, want %q", run.Status, StatusSuccess)
	}

	text := logs.String()
	if !strings.Contains(text, "job started") {
		t.Fatalf("logs missing job started event: %s", text)
	}
	if !strings.Contains(text, "job finished") {
		t.Fatalf("logs missing job finished event: %s", text)
	}
	if !strings.Contains(text, fmt.Sprintf("run_id=%d", runID)) {
		t.Fatalf("logs missing run_id=%d: %s", runID, text)
	}
	if !strings.Contains(text, "job_name=playlist_sync") {
		t.Fatalf("logs missing job_name field: %s", text)
	}
	if !strings.Contains(text, "triggered_by=manual") {
		t.Fatalf("logs missing triggered_by field: %s", text)
	}
	if !strings.Contains(text, "status=success") {
		t.Fatalf("logs missing success status: %s", text)
	}
}

func TestRunnerInjectsRunMetadataIntoJobContext(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, func(ctx context.Context, run *RunContext) error {
		meta, ok := RunMetadataFromContext(ctx)
		if !ok {
			return fmt.Errorf("run metadata was not present in job context")
		}
		if meta.RunID != run.RunID() {
			return fmt.Errorf("run metadata run_id = %d, want %d", meta.RunID, run.RunID())
		}
		if meta.JobName != JobPlaylistSync {
			return fmt.Errorf("run metadata job_name = %q, want %q", meta.JobName, JobPlaylistSync)
		}
		if meta.TriggeredBy != TriggerManual {
			return fmt.Errorf("run metadata triggered_by = %q, want %q", meta.TriggeredBy, TriggerManual)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("status = %q, want %q (error=%q)", run.Status, StatusSuccess, run.ErrorMessage)
	}
}

// closableMemoryStore wraps memoryStore with a Close() method that rejects
// writes after close, simulating the real SQLite store behavior during shutdown.
type closableMemoryStore struct {
	*memoryStore
	closeMu sync.Mutex
	closed  bool
}

func newClosableMemoryStore() *closableMemoryStore {
	return &closableMemoryStore{memoryStore: newMemoryStore()}
}

func (c *closableMemoryStore) closeStore() {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	c.closed = true
}

func (c *closableMemoryStore) isClosed() bool {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closed
}

func (c *closableMemoryStore) FinishRun(ctx context.Context, runID int64, status, errText, summary string, finishedAt int64) error {
	if c.isClosed() {
		return fmt.Errorf("sql: database is closed")
	}
	return c.memoryStore.FinishRun(ctx, runID, status, errText, summary, finishedAt)
}

func (c *closableMemoryStore) CreateRun(ctx context.Context, jobName, triggeredBy string, startedAt int64) (int64, error) {
	if c.isClosed() {
		return 0, fmt.Errorf("sql: database is closed")
	}
	return c.memoryStore.CreateRun(ctx, jobName, triggeredBy, startedAt)
}

func (c *closableMemoryStore) UpdateRunProgress(ctx context.Context, runID int64, progressCur, progressMax int, summary string) error {
	if c.isClosed() {
		return fmt.Errorf("sql: database is closed")
	}
	return c.memoryStore.UpdateRunProgress(ctx, runID, progressCur, progressMax, summary)
}

// TestRunnerCloseWaitsForTerminalPersistence proves that runner.Close()
// must wait for active run goroutines to complete their terminal FinishRun()
// persistence before returning. When Close() returns immediately, the main
// defer ordering (runner.Close() then store.Close()) creates a race where
// the run goroutine's FinishRun() call hits a closed database.
func TestRunnerCloseWaitsForTerminalPersistence(t *testing.T) {
	store := newClosableMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	logs := newTestLogBuffer()
	runner.SetLogger(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// Start a job that blocks until its context is canceled.
	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, func(ctx context.Context, _ *RunContext) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Simulate main.go defer ordering: runner.Close() followed immediately
	// by store.Close(). If Close() doesn't wait for the run goroutine's
	// terminal persistence, FinishRun() will race with or execute after
	// the store is closed.
	runner.Close()
	store.closeStore()

	// Give the run goroutine time to attempt its deferred FinishRun().
	time.Sleep(200 * time.Millisecond)

	// The run should have reached terminal status (canceled) with its
	// FinishRun() completing before the store was closed. If Close()
	// returned before the run goroutine finished, the status will be
	// stuck as "running" because FinishRun() failed on the closed store.
	run, err := store.memoryStore.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun(%d) error = %v", runID, err)
	}
	if run.Status == StatusRunning {
		t.Fatalf("run %d stuck in %q after runner.Close() + store close: "+
			"terminal persistence was lost because Close() did not wait "+
			"for FinishRun() to complete", runID, StatusRunning)
	}
	if run.Status != StatusCanceled {
		t.Fatalf("run %d status = %q, want %q", runID, run.Status, StatusCanceled)
	}

	// Verify no persistence failure was logged.
	logText := logs.String()
	if strings.Contains(logText, "job run persistence failed") {
		t.Fatalf("FinishRun() failed during shutdown, indicating Close() "+
			"returned before terminal persistence completed: %s", logText)
	}
}

// TestRunnerCloseMultipleRunsWaitsForAll proves that Close() waits for
// all active run goroutines to complete terminal persistence, not just
// the first one.
func TestRunnerCloseMultipleRunsWaitsForAll(t *testing.T) {
	store := newClosableMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}
	runner.SetGlobalLock(false)

	logs := newTestLogBuffer()
	runner.SetLogger(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// Start two jobs that block until canceled.
	runID1, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, func(ctx context.Context, _ *RunContext) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Start(job1) error = %v", err)
	}

	runID2, err := runner.Start(context.Background(), JobAutoPrioritize, TriggerManual, func(ctx context.Context, _ *RunContext) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatalf("Start(job2) error = %v", err)
	}

	runner.Close()
	store.closeStore()

	time.Sleep(200 * time.Millisecond)

	for _, id := range []int64{runID1, runID2} {
		run, err := store.memoryStore.GetRun(context.Background(), id)
		if err != nil {
			t.Fatalf("GetRun(%d) error = %v", id, err)
		}
		if run.Status == StatusRunning {
			t.Fatalf("run %d stuck in %q: terminal persistence lost", id, StatusRunning)
		}
		if run.Status != StatusCanceled {
			t.Fatalf("run %d status = %q, want %q", id, run.Status, StatusCanceled)
		}
	}

	logText := logs.String()
	if strings.Contains(logText, "job run persistence failed") {
		t.Fatalf("FinishRun() failed during shutdown: %s", logText)
	}
}

func waitForRunDone(t *testing.T, runner *Runner, runID int64) Run {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := runner.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatalf("GetRun(%d) error = %v", runID, err)
		}
		if run.Status != StatusRunning {
			return run
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run %d did not finish before timeout", runID)
	return Run{}
}
