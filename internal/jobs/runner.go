package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

var (
	ErrAlreadyRunning = errors.New("job already running")
	ErrRunnerClosed   = errors.New("job runner is closed")
)

// JobFunc executes one asynchronous job run.
type JobFunc func(ctx context.Context, run *RunContext) error

// Runner coordinates asynchronous job execution with persisted run state.
type Runner struct {
	store  Store
	logger *slog.Logger

	mu            sync.Mutex
	runningByName map[string]struct{}
	runCancels    map[int64]context.CancelCauseFunc
	closed        bool
	globalLock    bool

	wg sync.WaitGroup // tracks in-flight run goroutines through FinishRun persistence
}

// NewRunner builds a runner with global overlap locking enabled.
func NewRunner(store Store) (*Runner, error) {
	if store == nil {
		return nil, fmt.Errorf("jobs store is required")
	}
	return &Runner{
		store:         store,
		logger:        slog.Default(),
		runningByName: make(map[string]struct{}),
		runCancels:    make(map[int64]context.CancelCauseFunc),
		globalLock:    true,
	}, nil
}

func (r *Runner) SetLogger(logger *slog.Logger) {
	if r == nil {
		return
	}
	if logger == nil {
		r.logger = slog.Default()
		return
	}
	r.logger = logger
}

// SetGlobalLock controls whether different jobs may run concurrently.
// When enabled (default), only one job run may be active at a time.
func (r *Runner) SetGlobalLock(enabled bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.globalLock = enabled
}

// Close prevents new runs from being started, cancels active runs, and waits
// for all in-flight run goroutines to finish their terminal persistence
// (FinishRun) before returning.
func (r *Runner) Close() {
	r.mu.Lock()
	cancels := make([]context.CancelCauseFunc, 0, len(r.runCancels))
	for _, cancel := range r.runCancels {
		cancels = append(cancels, cancel)
	}
	r.closed = true
	r.mu.Unlock()

	for _, cancel := range cancels {
		cancel(ErrRunnerClosed)
	}

	r.wg.Wait()
}

// Start creates a run row and executes fn asynchronously.
func (r *Runner) Start(ctx context.Context, jobName, triggeredBy string, fn JobFunc) (int64, error) {
	if fn == nil {
		return 0, fmt.Errorf("job function is required")
	}

	jobName = strings.TrimSpace(jobName)
	if jobName == "" {
		return 0, fmt.Errorf("job name is required")
	}

	triggeredBy = strings.TrimSpace(triggeredBy)
	if triggeredBy == "" {
		return 0, fmt.Errorf("triggered_by is required")
	}

	locked, err := r.tryLock(jobName)
	if err != nil {
		return 0, err
	}
	if !locked {
		return 0, ErrAlreadyRunning
	}

	startedAt := time.Now().UTC().Unix()
	runID, err := r.store.CreateRun(ctx, jobName, triggeredBy, startedAt)
	if err != nil {
		r.wg.Done() // balance Add from tryLock
		r.unlock(jobName)
		return 0, err
	}
	runExecCtx, runCancel := context.WithCancelCause(ctx)
	if isClosed := r.setRunCancel(runID, runCancel); isClosed {
		runCancel(ErrRunnerClosed)
	}

	r.logInfo(
		"job started",
		"run_id", runID,
		"job_name", jobName,
		"triggered_by", triggeredBy,
		"started_at", time.Unix(startedAt, 0).UTC(),
	)
	go r.run(runExecCtx, jobName, triggeredBy, runID, startedAt, fn)
	return runID, nil
}

func (r *Runner) run(ctx context.Context, jobName, triggeredBy string, runID, startedAtUnix int64, fn JobFunc) {
	defer r.wg.Done()

	runCtx := &RunContext{
		runID:  runID,
		runner: r,
	}
	ctx = WithRunMetadata(ctx, runID, jobName, triggeredBy)
	startedAt := time.Unix(startedAtUnix, 0).UTC()
	if startedAtUnix <= 0 {
		startedAt = time.Now().UTC()
	}

	status := StatusSuccess
	errText := ""

	defer func() {
		if recovered := recover(); recovered != nil {
			status = StatusError
			errText = fmt.Sprintf("panic: %v", recovered)
			r.logError(
				"job panic recovered",
				"run_id", runID,
				"job_name", jobName,
				"triggered_by", triggeredBy,
				"error", errText,
			)
		}

		_, _, summary := runCtx.beginFinalization()

		// Release runner locks before persisting terminal state so IsRunning()
		// cannot outlive a non-running persisted status.
		r.clearRunCancel(runID)
		r.unlock(jobName)

		finishedAt := time.Now().UTC()
		finishErr := r.store.FinishRun(
			context.Background(),
			runID,
			status,
			errText,
			summary,
			finishedAt.Unix(),
		)
		duration := finishedAt.Sub(startedAt)
		if duration < 0 {
			duration = 0
		}
		if finishErr != nil {
			r.logError(
				"job run persistence failed",
				"run_id", runID,
				"job_name", jobName,
				"triggered_by", triggeredBy,
				"status", status,
				"error", finishErr,
			)
			return
		}
		fields := []any{
			"run_id", runID,
			"job_name", jobName,
			"triggered_by", triggeredBy,
			"status", status,
			"duration", duration.String(),
		}
		if summary != "" {
			fields = append(fields, "summary", summary)
		}
		if errText != "" {
			fields = append(fields, "error", errText)
		}
		if status == StatusSuccess {
			r.logInfo("job finished", fields...)
			return
		}
		r.logWarn("job finished", fields...)
	}()

	if err := fn(ctx, runCtx); err != nil {
		if isCancellationError(err) {
			status = StatusCanceled
		} else {
			status = StatusError
		}
		errText = err.Error()
	}
}

func (r *Runner) logInfo(msg string, args ...any) {
	if r == nil || r.logger == nil {
		return
	}
	r.logger.Info(msg, args...)
}

func (r *Runner) logWarn(msg string, args ...any) {
	if r == nil || r.logger == nil {
		return
	}
	r.logger.Warn(msg, args...)
}

func (r *Runner) logError(msg string, args ...any) {
	if r == nil || r.logger == nil {
		return
	}
	r.logger.Error(msg, args...)
}

func (r *Runner) tryLock(jobName string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return false, ErrRunnerClosed
	}

	if _, exists := r.runningByName[jobName]; exists {
		return false, nil
	}

	if r.globalLock && len(r.runningByName) > 0 {
		return false, nil
	}

	r.runningByName[jobName] = struct{}{}
	r.wg.Add(1) // under mu: happens-before Close's wg.Wait
	return true, nil
}

func (r *Runner) unlock(jobName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.runningByName, jobName)
}

func (r *Runner) setRunCancel(runID int64, cancel context.CancelCauseFunc) (closed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runCancels[runID] = cancel
	return r.closed
}

func (r *Runner) clearRunCancel(runID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.runCancels, runID)
}

// IsRunning returns whether a job currently has an active run.
func (r *Runner) IsRunning(jobName string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, exists := r.runningByName[strings.TrimSpace(jobName)]
	return exists
}

// GetRun reads one run by ID.
func (r *Runner) GetRun(ctx context.Context, runID int64) (Run, error) {
	return r.store.GetRun(ctx, runID)
}

// ListRuns lists runs by optional job name.
func (r *Runner) ListRuns(ctx context.Context, jobName string, limit, offset int) ([]Run, error) {
	return r.store.ListRuns(ctx, jobName, limit, offset)
}

func isCancellationError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// RunContext exposes progress and summary updates for a running job.
type RunContext struct {
	runID  int64
	runner *Runner

	mu          sync.Mutex
	finalizing  bool
	progressCur int
	progressMax int
	summary     string
}

// RunID returns the persisted run identifier.
func (r *RunContext) RunID() int64 {
	if r == nil {
		return 0
	}
	return r.runID
}

// SetProgress persists run progress counters.
func (r *RunContext) SetProgress(ctx context.Context, progressCur, progressMax int) error {
	if err := r.setProgressInMemory(progressCur, progressMax); err != nil {
		return err
	}
	return r.persistProgress(ctx)
}

func (r *RunContext) setProgressInMemory(progressCur, progressMax int) error {
	if r == nil || r.runner == nil {
		return fmt.Errorf("run context is not initialized")
	}
	if progressCur < 0 {
		return fmt.Errorf("progress_cur must be non-negative")
	}
	if progressMax < 0 {
		return fmt.Errorf("progress_max must be non-negative")
	}
	if progressMax > 0 && progressCur > progressMax {
		return fmt.Errorf("progress_cur cannot exceed progress_max")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finalizing {
		return ErrRunFinalized
	}
	r.progressCur = progressCur
	r.progressMax = progressMax

	return nil
}

func (r *RunContext) persistProgress(ctx context.Context) error {
	if r == nil || r.runner == nil {
		return fmt.Errorf("run context is not initialized")
	}

	r.mu.Lock()
	if r.finalizing {
		r.mu.Unlock()
		return ErrRunFinalized
	}
	cur := r.progressCur
	max := r.progressMax
	summary := r.summary
	r.mu.Unlock()

	err := r.runner.store.UpdateRunProgress(ctx, r.runID, cur, max, summary)
	if errors.Is(err, ErrRunNotRunning) {
		return ErrRunFinalized
	}
	return err
}

// IncrementProgress increments progress_cur by delta.
func (r *RunContext) IncrementProgress(ctx context.Context, delta int) error {
	if delta < 0 {
		return fmt.Errorf("delta must be non-negative")
	}

	cur, max, _ := r.Snapshot()
	return r.SetProgress(ctx, cur+delta, max)
}

// SetSummary persists an updated summary string.
func (r *RunContext) SetSummary(ctx context.Context, summary string) error {
	if r == nil || r.runner == nil {
		return fmt.Errorf("run context is not initialized")
	}

	summary = strings.TrimSpace(summary)
	r.mu.Lock()
	if r.finalizing {
		r.mu.Unlock()
		return ErrRunFinalized
	}
	r.summary = summary
	cur := r.progressCur
	max := r.progressMax
	r.mu.Unlock()

	err := r.runner.store.UpdateRunProgress(ctx, r.runID, cur, max, summary)
	if errors.Is(err, ErrRunNotRunning) {
		return ErrRunFinalized
	}
	return err
}

// Snapshot returns the current in-memory progress and summary.
func (r *RunContext) Snapshot() (progressCur, progressMax int, summary string) {
	if r == nil {
		return 0, 0, ""
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.progressCur, r.progressMax, r.summary
}

func (r *RunContext) beginFinalization() (progressCur, progressMax int, summary string) {
	if r == nil {
		return 0, 0, ""
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.finalizing = true
	return r.progressCur, r.progressMax, r.summary
}
