package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	defaultScheduledCatchUpInitialBackoff = 2 * time.Second
	defaultScheduledCatchUpMaxBackoff     = 30 * time.Second
	defaultScheduledFreshnessScanLimit    = 64
)

var (
	jobSchedulerEventsMetric = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "job_scheduler_events_total",
		Help: "Scheduler callback lifecycle events by job and event type.",
	}, []string{"job_name", "event"})
	jobSchedulerDeferredPendingMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "job_scheduler_deferred_pending",
		Help: "Whether a coalesced deferred scheduled run is pending for a job (1=true, 0=false).",
	}, []string{"job_name"})
	jobSchedulerDeferredBackoffMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "job_scheduler_deferred_backoff_seconds",
		Help: "Current deferred scheduled-run retry backoff in seconds.",
	}, []string{"job_name"})
	jobSchedulerDeferredAgeMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "job_scheduler_deferred_age_seconds",
		Help: "Age in seconds of the currently pending deferred scheduled run.",
	}, []string{"job_name"})
	jobSchedulerLastSuccessMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "job_scheduler_last_success_timestamp_seconds",
		Help: "Unix timestamp for the last successful schedule-triggered run.",
	}, []string{"job_name"})
	jobSchedulerFreshnessMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "job_scheduler_freshness_seconds",
		Help: "Seconds since the last successful schedule-triggered run.",
	}, []string{"job_name"})
)

// ScheduledJobStarter starts asynchronous jobs from scheduler callbacks.
type ScheduledJobStarter interface {
	Start(ctx context.Context, jobName, triggeredBy string, fn JobFunc) (int64, error)
}

// ScheduledCatchUpOptions controls overlap catch-up behavior for scheduled
// callbacks.
type ScheduledCatchUpOptions struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	FreshnessLimit int
}

// ScheduledJobCallback applies overlap catch-up/backoff policy for scheduled
// job starts while preserving runner-level overlap enforcement.
type ScheduledJobCallback struct {
	starter ScheduledJobStarter
	store   Store
	logger  *slog.Logger
	jobName string
	jobFn   JobFunc

	initialBackoff time.Duration
	maxBackoff     time.Duration
	freshnessLimit int

	mu sync.Mutex

	deferredPending bool
	deferredSince   time.Time
	deferredBackoff time.Duration
	deferredAttempt int
}

func NewScheduledJobCallback(
	starter ScheduledJobStarter,
	store Store,
	logger *slog.Logger,
	jobName string,
	jobFn JobFunc,
	options ScheduledCatchUpOptions,
) (*ScheduledJobCallback, error) {
	if starter == nil {
		return nil, fmt.Errorf("scheduled job starter is required")
	}
	if jobFn == nil {
		return nil, fmt.Errorf("scheduled job function is required")
	}
	if jobName = strings.TrimSpace(jobName); jobName == "" {
		return nil, fmt.Errorf("job name is required")
	}

	initialBackoff := options.InitialBackoff
	if initialBackoff <= 0 {
		initialBackoff = defaultScheduledCatchUpInitialBackoff
	}
	maxBackoff := options.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultScheduledCatchUpMaxBackoff
	}
	if maxBackoff < initialBackoff {
		maxBackoff = initialBackoff
	}
	freshnessLimit := options.FreshnessLimit
	if freshnessLimit < 1 {
		freshnessLimit = defaultScheduledFreshnessScanLimit
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &ScheduledJobCallback{
		starter:        starter,
		store:          store,
		logger:         logger,
		jobName:        jobName,
		jobFn:          jobFn,
		initialBackoff: initialBackoff,
		maxBackoff:     maxBackoff,
		freshnessLimit: freshnessLimit,
	}, nil
}

// Callback is the scheduler-facing callback that starts runs and manages
// deferred catch-up when overlaps occur.
func (c *ScheduledJobCallback) Callback(ctx context.Context, _ string) error {
	if c == nil {
		return fmt.Errorf("scheduled callback is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		c.clearDeferred()
		jobSchedulerEventsMetric.WithLabelValues(c.jobName, "skipped_canceled").Inc()
		c.logWarn(
			"scheduled job callback skipped due to canceled callback context",
			"job_name", c.jobName,
			"error", ctx.Err(),
		)
		return nil
	}

	c.observeFreshness(ctx, time.Now().UTC())

	startCtx := context.WithoutCancel(ctx)
	runID, err := c.starter.Start(startCtx, c.jobName, TriggerSchedule, c.jobFn)
	if err == nil {
		jobSchedulerEventsMetric.WithLabelValues(c.jobName, "started").Inc()
		c.logInfo(
			"scheduled job started",
			"job_name", c.jobName,
			"run_id", runID,
		)
		return nil
	}

	if !errors.Is(err, ErrAlreadyRunning) {
		jobSchedulerEventsMetric.WithLabelValues(c.jobName, "start_error").Inc()
		return err
	}

	jobSchedulerEventsMetric.WithLabelValues(c.jobName, "skipped_already_running").Inc()
	enqueued, deferredSince, backoff := c.enqueueDeferred(time.Now().UTC())
	if !enqueued {
		jobSchedulerEventsMetric.WithLabelValues(c.jobName, "deferred_coalesced").Inc()
		c.logWarn(
			"scheduled job overlap coalesced into existing deferred catch-up",
			"job_name", c.jobName,
			"deferred_since", deferredSince,
		)
		return nil
	}

	jobSchedulerEventsMetric.WithLabelValues(c.jobName, "deferred_enqueued").Inc()
	c.logWarn(
		"scheduled job overlap deferred for catch-up",
		"job_name", c.jobName,
		"initial_backoff", backoff.String(),
	)
	go c.runDeferred(ctx)
	return nil
}

func (c *ScheduledJobCallback) runDeferred(ctx context.Context) {
	for {
		pending, attempt, backoff, deferredSince := c.prepareDeferredAttempt(time.Now().UTC())
		if !pending {
			return
		}

		c.logInfo(
			"scheduled deferred catch-up waiting",
			"job_name", c.jobName,
			"attempt", attempt,
			"backoff", backoff.String(),
			"deferred_age", time.Since(deferredSince).String(),
		)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			c.clearDeferred()
			jobSchedulerEventsMetric.WithLabelValues(c.jobName, "deferred_canceled").Inc()
			c.logWarn("scheduled deferred catch-up canceled", "job_name", c.jobName, "error", ctx.Err())
			return
		case <-timer.C:
		}

		startCtx := context.WithoutCancel(ctx)
		runID, err := c.starter.Start(startCtx, c.jobName, TriggerSchedule, c.jobFn)
		c.observeFreshness(ctx, time.Now().UTC())
		if err == nil {
			c.clearDeferred()
			jobSchedulerEventsMetric.WithLabelValues(c.jobName, "deferred_started").Inc()
			c.logInfo(
				"scheduled deferred catch-up started",
				"job_name", c.jobName,
				"run_id", runID,
			)
			return
		}

		if errors.Is(err, ErrAlreadyRunning) {
			jobSchedulerEventsMetric.WithLabelValues(c.jobName, "deferred_retry_already_running").Inc()
			c.bumpDeferredBackoff(time.Now().UTC())
			continue
		}

		jobSchedulerEventsMetric.WithLabelValues(c.jobName, "deferred_retry_start_error").Inc()
		c.logWarn(
			"scheduled deferred catch-up start failed; retrying with backoff",
			"job_name", c.jobName,
			"error", err,
		)
		c.bumpDeferredBackoff(time.Now().UTC())
	}
}

func (c *ScheduledJobCallback) observeFreshness(ctx context.Context, now time.Time) {
	if c == nil || c.store == nil {
		return
	}

	lastSuccess, err := c.latestScheduledSuccessTimestamp(ctx)
	if err != nil {
		return
	}

	if lastSuccess <= 0 {
		jobSchedulerLastSuccessMetric.WithLabelValues(c.jobName).Set(0)
		jobSchedulerFreshnessMetric.WithLabelValues(c.jobName).Set(0)
		return
	}

	lastSuccessTime := time.Unix(lastSuccess, 0).UTC()
	age := now.Sub(lastSuccessTime)
	if age < 0 {
		age = 0
	}
	jobSchedulerLastSuccessMetric.WithLabelValues(c.jobName).Set(float64(lastSuccess))
	jobSchedulerFreshnessMetric.WithLabelValues(c.jobName).Set(age.Seconds())
}

func (c *ScheduledJobCallback) latestScheduledSuccessTimestamp(ctx context.Context) (int64, error) {
	if c == nil || c.store == nil {
		return 0, nil
	}

	limit := c.freshnessLimit
	if limit < 1 {
		limit = defaultScheduledFreshnessScanLimit
	}

	offset := 0
	for {
		runs, err := c.store.ListRuns(ctx, c.jobName, limit, offset)
		if err != nil {
			return 0, err
		}
		if len(runs) == 0 {
			return 0, nil
		}
		for _, run := range runs {
			if run.TriggeredBy != TriggerSchedule || run.Status != StatusSuccess || run.FinishedAt <= 0 {
				continue
			}
			// ListRuns returns newest-first; the first matching success is the
			// most recent successful schedule-triggered run.
			return run.FinishedAt, nil
		}
		if len(runs) < limit {
			return 0, nil
		}
		offset += len(runs)
	}
}

func (c *ScheduledJobCallback) enqueueDeferred(now time.Time) (bool, time.Time, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.deferredPending {
		return false, c.deferredSince, c.deferredBackoff
	}
	c.deferredPending = true
	c.deferredSince = now
	c.deferredBackoff = c.initialBackoff
	c.deferredAttempt = 0

	jobSchedulerDeferredPendingMetric.WithLabelValues(c.jobName).Set(1)
	jobSchedulerDeferredBackoffMetric.WithLabelValues(c.jobName).Set(c.deferredBackoff.Seconds())
	jobSchedulerDeferredAgeMetric.WithLabelValues(c.jobName).Set(0)
	return true, c.deferredSince, c.deferredBackoff
}

func (c *ScheduledJobCallback) prepareDeferredAttempt(now time.Time) (bool, int, time.Duration, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.deferredPending {
		return false, 0, 0, time.Time{}
	}
	c.deferredAttempt++
	age := now.Sub(c.deferredSince)
	if age < 0 {
		age = 0
	}
	jobSchedulerDeferredPendingMetric.WithLabelValues(c.jobName).Set(1)
	jobSchedulerDeferredBackoffMetric.WithLabelValues(c.jobName).Set(c.deferredBackoff.Seconds())
	jobSchedulerDeferredAgeMetric.WithLabelValues(c.jobName).Set(age.Seconds())
	return true, c.deferredAttempt, c.deferredBackoff, c.deferredSince
}

func (c *ScheduledJobCallback) bumpDeferredBackoff(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.deferredPending {
		return
	}
	next := c.deferredBackoff * 2
	if next < c.initialBackoff {
		next = c.initialBackoff
	}
	if next > c.maxBackoff {
		next = c.maxBackoff
	}
	c.deferredBackoff = next
	age := now.Sub(c.deferredSince)
	if age < 0 {
		age = 0
	}

	jobSchedulerDeferredPendingMetric.WithLabelValues(c.jobName).Set(1)
	jobSchedulerDeferredBackoffMetric.WithLabelValues(c.jobName).Set(c.deferredBackoff.Seconds())
	jobSchedulerDeferredAgeMetric.WithLabelValues(c.jobName).Set(age.Seconds())
}

func (c *ScheduledJobCallback) clearDeferred() {
	c.mu.Lock()
	c.deferredPending = false
	c.deferredSince = time.Time{}
	c.deferredBackoff = 0
	c.deferredAttempt = 0
	c.mu.Unlock()

	jobSchedulerDeferredPendingMetric.WithLabelValues(c.jobName).Set(0)
	jobSchedulerDeferredBackoffMetric.WithLabelValues(c.jobName).Set(0)
	jobSchedulerDeferredAgeMetric.WithLabelValues(c.jobName).Set(0)
}

func (c *ScheduledJobCallback) logInfo(msg string, args ...any) {
	if c == nil || c.logger == nil {
		return
	}
	c.logger.Info(msg, args...)
}

func (c *ScheduledJobCallback) logWarn(msg string, args ...any) {
	if c == nil || c.logger == nil {
		return
	}
	c.logger.Warn(msg, args...)
}
