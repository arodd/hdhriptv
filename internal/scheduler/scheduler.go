package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/arodd/hdhriptv/internal/jobs"
	"github.com/robfig/cron/v3"
)

const (
	defaultTimezone             = "America/Chicago"
	settingJobsTimezone         = "jobs.timezone"
	settingPlaylistSyncEnabled  = "jobs.playlist_sync.enabled"
	settingPlaylistSyncCron     = "jobs.playlist_sync.cron"
	settingAutoPrioritizeEnable = "jobs.auto_prioritize.enabled"
	settingAutoPrioritizeCron   = "jobs.auto_prioritize.cron"
	settingDVRLineupSyncEnable  = "jobs.dvr_lineup_sync.enabled"
	settingDVRLineupSyncCron    = "jobs.dvr_lineup_sync.cron"
)

type scheduleConfig struct {
	enabledKey    string
	cronKey       string
	defaultEnable bool
	defaultCron   string
}

var jobScheduleConfigs = map[string]scheduleConfig{
	jobs.JobPlaylistSync: {
		enabledKey:    settingPlaylistSyncEnabled,
		cronKey:       settingPlaylistSyncCron,
		defaultEnable: true,
		defaultCron:   "*/30 * * * *",
	},
	jobs.JobAutoPrioritize: {
		enabledKey:    settingAutoPrioritizeEnable,
		cronKey:       settingAutoPrioritizeCron,
		defaultEnable: false,
		defaultCron:   "30 3 * * *",
	},
	jobs.JobDVRLineupSync: {
		enabledKey:    settingDVRLineupSyncEnable,
		cronKey:       settingDVRLineupSyncCron,
		defaultEnable: false,
		defaultCron:   "*/30 * * * *",
	},
}

// SettingsStore reads and writes automation settings.
type SettingsStore interface {
	GetSetting(ctx context.Context, key string) (string, error)
	SetSettings(ctx context.Context, values map[string]string) error
}

// JobCallback is invoked by scheduler ticks.
type JobCallback func(ctx context.Context, jobName string) error

// JobSchedule is one persisted scheduler configuration with runtime next-run state.
type JobSchedule struct {
	JobName   string    `json:"job_name"`
	Enabled   bool      `json:"enabled"`
	CronSpec  string    `json:"cron_spec"`
	NextRun   time.Time `json:"next_run,omitempty"`
	LastError string    `json:"last_error,omitempty"`
}

// Service loads, validates, and executes cron schedules backed by settings.
type Service struct {
	store  SettingsStore
	logger *slog.Logger
	parser cron.Parser

	lifecycleMu sync.Mutex
	mu          sync.Mutex
	engine      *cron.Cron
	entries     map[string]cron.EntryID
	callbacks   map[string]JobCallback
	started     bool
	timezone    string
	runCtx      context.Context
	runCancel   context.CancelFunc
}

func New(store SettingsStore, logger *slog.Logger) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("settings store is required")
	}
	runCtx, runCancel := context.WithCancel(context.Background())

	return &Service{
		store:     store,
		logger:    logger,
		parser:    cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		entries:   make(map[string]cron.EntryID),
		callbacks: make(map[string]JobCallback),
		timezone:  defaultTimezone,
		runCtx:    runCtx,
		runCancel: runCancel,
	}, nil
}

// RegisterJob registers a runtime callback for a supported job name.
func (s *Service) RegisterJob(jobName string, callback JobCallback) error {
	if callback == nil {
		return fmt.Errorf("job callback is required")
	}
	if _, ok := jobScheduleConfigs[jobName]; !ok {
		return fmt.Errorf("unsupported scheduled job %q", jobName)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.callbacks[jobName] = callback
	return nil
}

// Start begins cron execution for currently loaded entries.
func (s *Service) Start() {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return
	}
	if s.engine == nil {
		location, _ := time.LoadLocation(defaultTimezone)
		if location == nil {
			location = time.UTC
		}
		s.engine = cron.New(
			cron.WithParser(s.parser),
			cron.WithLocation(location),
		)
	}
	if s.runCtx == nil || s.runCtx.Err() != nil || s.runCancel == nil {
		s.runCtx, s.runCancel = context.WithCancel(context.Background())
	}
	s.engine.Start()
	s.started = true
}

// Stop halts scheduler execution.
func (s *Service) Stop() context.Context {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.mu.Lock()
	runCancel := s.runCancel
	s.runCancel = nil
	oldEngine := s.engine
	wasStarted := s.started
	s.started = false
	s.mu.Unlock()

	if runCancel != nil {
		runCancel()
	}
	if !wasStarted || oldEngine == nil {
		return canceledContext()
	}
	return oldEngine.Stop()
}

// LoadFromSettings rebuilds entries using persisted settings.
func (s *Service) LoadFromSettings(ctx context.Context) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	locationName, err := s.readSetting(ctx, settingJobsTimezone, defaultTimezone)
	if err != nil {
		return err
	}
	location, normalizedLocation, fallbackErr := resolveLocation(locationName)
	if fallbackErr != nil {
		s.logWarn("invalid scheduler timezone; falling back to UTC", "timezone", locationName, "error", fallbackErr)
	}

	schedules := make([]JobSchedule, 0, len(jobScheduleConfigs))
	for jobName, cfg := range jobScheduleConfigs {
		enabledStr, err := s.readSetting(ctx, cfg.enabledKey, boolToString(cfg.defaultEnable))
		if err != nil {
			return err
		}
		cronSpec, err := s.readSetting(ctx, cfg.cronKey, cfg.defaultCron)
		if err != nil {
			return err
		}
		enabled := parseBool(enabledStr, cfg.defaultEnable)
		cronSpec = strings.TrimSpace(cronSpec)
		if enabled {
			if err := s.ValidateCron(cronSpec); err != nil {
				return fmt.Errorf("invalid cron for %s: %w", jobName, err)
			}
		}
		schedules = append(schedules, JobSchedule{
			JobName:  jobName,
			Enabled:  enabled,
			CronSpec: cronSpec,
		})
	}
	activeJobs := make([]string, 0, len(schedules))
	for _, schedule := range schedules {
		if schedule.Enabled {
			activeJobs = append(activeJobs, schedule.JobName)
		}
	}
	sort.Strings(activeJobs)

	newEngine := cron.New(
		cron.WithParser(s.parser),
		cron.WithLocation(location),
	)
	newEntries := make(map[string]cron.EntryID)

	for _, schedule := range schedules {
		if !schedule.Enabled {
			continue
		}
		callback, ok := s.getCallback(schedule.JobName)
		if !ok {
			return fmt.Errorf("no callback registered for scheduled job %q", schedule.JobName)
		}

		jobName := schedule.JobName
		id, err := newEngine.AddFunc(schedule.CronSpec, func() {
			if callbackErr := callback(s.callbackContext(), jobName); callbackErr != nil {
				s.logError("scheduled job callback failed", "job_name", jobName, "error", callbackErr)
			}
		})
		if err != nil {
			return fmt.Errorf("add cron entry for %s: %w", schedule.JobName, err)
		}
		newEntries[schedule.JobName] = id
	}

	s.mu.Lock()
	oldEngine := s.engine
	wasStarted := s.started
	s.mu.Unlock()

	if oldEngine != nil {
		<-oldEngine.Stop().Done()
	}

	s.mu.Lock()
	s.engine = newEngine
	s.entries = newEntries
	s.timezone = normalizedLocation
	if wasStarted {
		s.engine.Start()
	}
	s.mu.Unlock()

	s.logInfo(
		"scheduler loaded schedules",
		"timezone", normalizedLocation,
		"active_count", len(activeJobs),
		"active_jobs", activeJobs,
	)

	return nil
}

// UpdateJobSchedule validates, persists, and applies one job schedule at runtime.
func (s *Service) UpdateJobSchedule(ctx context.Context, jobName string, enabled bool, cronSpec string) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	cfg, ok := jobScheduleConfigs[jobName]
	if !ok {
		return fmt.Errorf("unsupported scheduled job %q", jobName)
	}

	cronSpec = strings.TrimSpace(cronSpec)
	if cronSpec == "" {
		cronSpec = cfg.defaultCron
	}
	if enabled {
		if err := s.ValidateCron(cronSpec); err != nil {
			return err
		}
		if _, exists := s.getCallback(jobName); !exists {
			return fmt.Errorf("no callback registered for scheduled job %q", jobName)
		}
	}

	if err := s.store.SetSettings(ctx, map[string]string{
		cfg.enabledKey: boolToString(enabled),
		cfg.cronKey:    cronSpec,
	}); err != nil {
		return fmt.Errorf("persist job schedule %q: %w", jobName, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.engine == nil {
		location, _ := time.LoadLocation(defaultTimezone)
		if location == nil {
			location = time.UTC
		}
		s.engine = cron.New(
			cron.WithParser(s.parser),
			cron.WithLocation(location),
		)
	}

	if oldEntry, exists := s.entries[jobName]; exists {
		s.engine.Remove(oldEntry)
		delete(s.entries, jobName)
	}
	if !enabled {
		s.logInfo(
			"scheduler schedule updated",
			"job_name", jobName,
			"enabled", false,
			"cron_spec", cronSpec,
		)
		return nil
	}

	callback, exists := s.callbacks[jobName]
	if !exists {
		return fmt.Errorf("no callback registered for scheduled job %q", jobName)
	}

	id, err := s.engine.AddFunc(cronSpec, func() {
		if callbackErr := callback(s.callbackContext(), jobName); callbackErr != nil {
			s.logError("scheduled job callback failed", "job_name", jobName, "error", callbackErr)
		}
	})
	if err != nil {
		return fmt.Errorf("add schedule %q: %w", jobName, err)
	}
	s.entries[jobName] = id
	s.logInfo(
		"scheduler schedule updated",
		"job_name", jobName,
		"enabled", true,
		"cron_spec", cronSpec,
	)
	return nil
}

func (s *Service) callbackContext() context.Context {
	s.mu.Lock()
	ctx := s.runCtx
	runCancel := s.runCancel
	s.mu.Unlock()
	if runCancel != nil && ctx != nil {
		return ctx
	}
	return canceledContext()
}

// UpdateTimezone persists timezone and reloads schedule entries.
func (s *Service) UpdateTimezone(ctx context.Context, timezone string) error {
	timezone = strings.TrimSpace(timezone)
	if timezone == "" {
		return fmt.Errorf("timezone is required")
	}
	if err := s.store.SetSettings(ctx, map[string]string{
		settingJobsTimezone: timezone,
	}); err != nil {
		return fmt.Errorf("persist timezone: %w", err)
	}
	if err := s.LoadFromSettings(ctx); err != nil {
		return err
	}
	s.logInfo("scheduler timezone updated", "timezone", timezone)
	return nil
}

// ValidateCron validates cron spec with support for 5-field and optional-seconds format.
func (s *Service) ValidateCron(spec string) error {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return fmt.Errorf("cron expression is required")
	}
	if _, err := s.parser.Parse(spec); err != nil {
		return fmt.Errorf("parse cron expression: %w", err)
	}
	return nil
}

// Timezone returns the active scheduler timezone name.
func (s *Service) Timezone() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.timezone
}

// NextRun returns the next run time for a job when enabled.
func (s *Service) NextRun(jobName string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.engine == nil {
		return time.Time{}, false
	}
	entryID, ok := s.entries[jobName]
	if !ok {
		return time.Time{}, false
	}
	next := s.engine.Entry(entryID).Next
	if next.IsZero() {
		return time.Time{}, false
	}
	return next, true
}

// ListSchedules returns persisted schedule configuration plus runtime next-run values.
func (s *Service) ListSchedules(ctx context.Context) ([]JobSchedule, error) {
	out := make([]JobSchedule, 0, len(jobScheduleConfigs))

	for jobName, cfg := range jobScheduleConfigs {
		enabledRaw, err := s.readSetting(ctx, cfg.enabledKey, boolToString(cfg.defaultEnable))
		if err != nil {
			return nil, err
		}
		cronSpec, err := s.readSetting(ctx, cfg.cronKey, cfg.defaultCron)
		if err != nil {
			return nil, err
		}
		schedule := JobSchedule{
			JobName:  jobName,
			Enabled:  parseBool(enabledRaw, cfg.defaultEnable),
			CronSpec: strings.TrimSpace(cronSpec),
		}
		if next, ok := s.NextRun(jobName); ok {
			schedule.NextRun = next
		}
		out = append(out, schedule)
	}
	return out, nil
}

func (s *Service) readSetting(ctx context.Context, key, def string) (string, error) {
	value, err := s.store.GetSetting(ctx, key)
	if err == sql.ErrNoRows {
		return def, nil
	}
	if err != nil {
		return "", fmt.Errorf("read setting %q: %w", key, err)
	}

	value = strings.TrimSpace(value)
	if value == "" {
		return def, nil
	}
	return value, nil
}

func (s *Service) getCallback(jobName string) (JobCallback, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	callback, ok := s.callbacks[jobName]
	return callback, ok
}

func (s *Service) logWarn(msg string, args ...any) {
	if s.logger == nil {
		return
	}
	s.logger.Warn(msg, args...)
}

func (s *Service) logInfo(msg string, args ...any) {
	if s.logger == nil {
		return
	}
	s.logger.Info(msg, args...)
}

func (s *Service) logError(msg string, args ...any) {
	if s.logger == nil {
		return
	}
	s.logger.Error(msg, args...)
}

func boolToString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func parseBool(raw string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func resolveLocation(name string) (*time.Location, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = defaultTimezone
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return time.UTC, "UTC", err
	}
	return location, name, nil
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
