package httpapi

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/arodd/hdhriptv/internal/analyzer"
	"github.com/arodd/hdhriptv/internal/jobs"
	"github.com/arodd/hdhriptv/internal/scheduler"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
)

// AutomationSettingsStore reads and writes automation settings.
type AutomationSettingsStore interface {
	GetSetting(ctx context.Context, key string) (string, error)
	SetSettings(ctx context.Context, values map[string]string) error
	DeleteAllStreamMetrics(ctx context.Context) (int64, error)
}

// AutomationScheduler manages schedule state and runtime updates.
type AutomationScheduler interface {
	ListSchedules(ctx context.Context) ([]scheduler.JobSchedule, error)
	UpdateJobSchedule(ctx context.Context, jobName string, enabled bool, cronSpec string) error
	UpdateTimezone(ctx context.Context, timezone string) error
	ValidateCron(spec string) error
	Timezone() string
	LoadFromSettings(ctx context.Context) error
}

// AutomationRunner starts and queries asynchronous job runs.
type AutomationRunner interface {
	Start(ctx context.Context, jobName, triggeredBy string, fn jobs.JobFunc) (int64, error)
	GetRun(ctx context.Context, runID int64) (jobs.Run, error)
	ListRuns(ctx context.Context, jobName string, limit, offset int) ([]jobs.Run, error)
}

// AutomationDeps contains services required by automation API/UI handlers.
type AutomationDeps struct {
	Settings  AutomationSettingsStore
	Scheduler AutomationScheduler
	Runner    AutomationRunner
	JobFuncs  map[string]jobs.JobFunc
}

func (d AutomationDeps) validate() error {
	if d.Settings == nil {
		return fmt.Errorf("automation settings store is required")
	}
	if d.Scheduler == nil {
		return fmt.Errorf("automation scheduler is required")
	}
	if d.Runner == nil {
		return fmt.Errorf("automation runner is required")
	}
	if d.JobFuncs == nil {
		return fmt.Errorf("automation job funcs map is required")
	}
	if d.JobFuncs[jobs.JobPlaylistSync] == nil {
		return fmt.Errorf("automation playlist sync job function is required")
	}
	if d.JobFuncs[jobs.JobAutoPrioritize] == nil {
		return fmt.Errorf("automation auto-prioritize job function is required")
	}
	return nil
}

type automationStateResponse struct {
	PlaylistURL    string                     `json:"playlist_url"`
	Timezone       string                     `json:"timezone"`
	PlaylistSync   automationScheduleState    `json:"playlist_sync"`
	AutoPrioritize automationScheduleState    `json:"auto_prioritize"`
	Analyzer       automationAnalyzerSettings `json:"analyzer"`
}

type automationScheduleState struct {
	Enabled  bool      `json:"enabled"`
	CronSpec string    `json:"cron_spec"`
	NextRun  int64     `json:"next_run,omitempty"`
	LastRun  *jobs.Run `json:"last_run,omitempty"`
}

type automationAnalyzerSettings struct {
	ProbeTimeoutMS    int    `json:"probe_timeout_ms"`
	AnalyzeDurationUS int64  `json:"analyzeduration_us"`
	ProbeSizeBytes    int64  `json:"probesize_bytes"`
	BitrateMode       string `json:"bitrate_mode"`
	SampleSeconds     int    `json:"sample_seconds"`
	EnabledOnly       bool   `json:"enabled_only"`
	TopNPerChannel    int    `json:"top_n_per_channel"`
}

func (h *AdminHandler) handleGetAutomation(w http.ResponseWriter, r *http.Request) {
	payload, err := h.automationState(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("get automation state: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func (h *AdminHandler) handlePutAutomation(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PlaylistURL *string `json:"playlist_url"`
		Timezone    *string `json:"timezone"`

		PlaylistSync *struct {
			Enabled  *bool   `json:"enabled"`
			CronSpec *string `json:"cron_spec"`
		} `json:"playlist_sync"`

		AutoPrioritize *struct {
			Enabled  *bool   `json:"enabled"`
			CronSpec *string `json:"cron_spec"`
		} `json:"auto_prioritize"`

		Analyzer *struct {
			ProbeTimeoutMS    *int    `json:"probe_timeout_ms"`
			AnalyzeDurationUS *int64  `json:"analyzeduration_us"`
			ProbeSizeBytes    *int64  `json:"probesize_bytes"`
			BitrateMode       *string `json:"bitrate_mode"`
			SampleSeconds     *int    `json:"sample_seconds"`
			EnabledOnly       *bool   `json:"enabled_only"`
			TopNPerChannel    *int    `json:"top_n_per_channel"`
		} `json:"analyzer"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	h.adminConfigMutationMu.Lock()
	defer h.adminConfigMutationMu.Unlock()

	schedules, err := h.automation.Scheduler.ListSchedules(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("list schedules: %v", err), http.StatusInternalServerError)
		return
	}
	scheduleByName := map[string]scheduler.JobSchedule{}
	for _, schedule := range schedules {
		scheduleByName[schedule.JobName] = schedule
	}

	type scheduleUpdate struct {
		apply   bool
		enabled bool
		cron    string
	}

	playlistUpdate, err := parseScheduleUpdate(
		jobs.JobPlaylistSync,
		scheduleByName[jobs.JobPlaylistSync],
		req.PlaylistSync,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	autoUpdate, err := parseScheduleUpdate(
		jobs.JobAutoPrioritize,
		scheduleByName[jobs.JobAutoPrioritize],
		req.AutoPrioritize,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if playlistUpdate.apply && playlistUpdate.enabled && strings.TrimSpace(playlistUpdate.cron) != "" {
		if err := h.automation.Scheduler.ValidateCron(playlistUpdate.cron); err != nil {
			http.Error(w, fmt.Sprintf("invalid playlist_sync cron: %v", err), http.StatusBadRequest)
			return
		}
	}
	if autoUpdate.apply && autoUpdate.enabled && strings.TrimSpace(autoUpdate.cron) != "" {
		if err := h.automation.Scheduler.ValidateCron(autoUpdate.cron); err != nil {
			http.Error(w, fmt.Sprintf("invalid auto_prioritize cron: %v", err), http.StatusBadRequest)
			return
		}
	}

	settingsUpdates := map[string]string{}
	if req.PlaylistURL != nil {
		settingsUpdates[sqlite.SettingPlaylistURL] = strings.TrimSpace(*req.PlaylistURL)
	}
	if req.Analyzer != nil {
		if req.Analyzer.ProbeTimeoutMS != nil {
			if *req.Analyzer.ProbeTimeoutMS <= 0 {
				http.Error(w, "analyzer.probe_timeout_ms must be greater than zero", http.StatusBadRequest)
				return
			}
			settingsUpdates[sqlite.SettingAnalyzerProbeTimeoutMS] = intToString(*req.Analyzer.ProbeTimeoutMS)
		}
		if req.Analyzer.AnalyzeDurationUS != nil {
			if *req.Analyzer.AnalyzeDurationUS <= 0 {
				http.Error(w, "analyzer.analyzeduration_us must be greater than zero", http.StatusBadRequest)
				return
			}
			settingsUpdates[sqlite.SettingAnalyzerAnalyzeDurationUS] = int64ToString(*req.Analyzer.AnalyzeDurationUS)
		}
		if req.Analyzer.ProbeSizeBytes != nil {
			if *req.Analyzer.ProbeSizeBytes <= 0 {
				http.Error(w, "analyzer.probesize_bytes must be greater than zero", http.StatusBadRequest)
				return
			}
			settingsUpdates[sqlite.SettingAnalyzerProbeSizeBytes] = int64ToString(*req.Analyzer.ProbeSizeBytes)
		}
		if req.Analyzer.BitrateMode != nil {
			mode := strings.ToLower(strings.TrimSpace(*req.Analyzer.BitrateMode))
			switch mode {
			case analyzer.BitrateModeMetadata, analyzer.BitrateModeSample, analyzer.BitrateModeMetadataThenSample:
			default:
				http.Error(w, "analyzer.bitrate_mode must be metadata, sample, or metadata_then_sample", http.StatusBadRequest)
				return
			}
			settingsUpdates[sqlite.SettingAnalyzerBitrateMode] = mode
		}
		if req.Analyzer.SampleSeconds != nil {
			if *req.Analyzer.SampleSeconds <= 0 {
				http.Error(w, "analyzer.sample_seconds must be greater than zero", http.StatusBadRequest)
				return
			}
			settingsUpdates[sqlite.SettingAnalyzerSampleSeconds] = intToString(*req.Analyzer.SampleSeconds)
		}
		if req.Analyzer.EnabledOnly != nil {
			settingsUpdates[sqlite.SettingAutoPrioritizeEnabledOnly] = boolToSetting(*req.Analyzer.EnabledOnly)
		}
		if req.Analyzer.TopNPerChannel != nil {
			if *req.Analyzer.TopNPerChannel < 0 {
				http.Error(w, "analyzer.top_n_per_channel must be zero or greater", http.StatusBadRequest)
				return
			}
			settingsUpdates[sqlite.SettingAutoPrioritizeTopNPerChannel] = intToString(*req.Analyzer.TopNPerChannel)
		}
	}

	timezoneUpdated := false
	timezoneValue := ""
	if req.Timezone != nil {
		timezoneValue = strings.TrimSpace(*req.Timezone)
		if timezoneValue == "" {
			http.Error(w, "timezone is required", http.StatusBadRequest)
			return
		}
		timezoneUpdated = true
		settingsUpdates[sqlite.SettingJobsTimezone] = timezoneValue
	}
	if playlistUpdate.apply {
		settingsUpdates[sqlite.SettingJobsPlaylistSyncEnabled] = boolToSetting(playlistUpdate.enabled)
		settingsUpdates[sqlite.SettingJobsPlaylistSyncCron] = strings.TrimSpace(playlistUpdate.cron)
	}
	if autoUpdate.apply {
		settingsUpdates[sqlite.SettingJobsAutoPrioritizeEnabled] = boolToSetting(autoUpdate.enabled)
		settingsUpdates[sqlite.SettingJobsAutoPrioritizeCron] = strings.TrimSpace(autoUpdate.cron)
	}

	schedulerSettingsUpdated := timezoneUpdated || playlistUpdate.apply || autoUpdate.apply
	mutationCtx := r.Context()
	if len(settingsUpdates) > 0 {
		var cancelMutation context.CancelFunc
		mutationCtx, cancelMutation = h.detachedAutomationMutationContext(r.Context())
		defer cancelMutation()
	}

	if len(settingsUpdates) > 0 {
		var rollbackSettings map[string]string
		if schedulerSettingsUpdated {
			rollbackSettings, err = h.snapshotAutomationSettings(mutationCtx, settingsUpdates)
			if err != nil {
				http.Error(w, fmt.Sprintf("snapshot automation settings: %v", err), http.StatusInternalServerError)
				return
			}
		}

		if err := h.automation.Settings.SetSettings(mutationCtx, settingsUpdates); err != nil {
			http.Error(w, fmt.Sprintf("update automation settings: %v", err), http.StatusInternalServerError)
			return
		}
		if schedulerSettingsUpdated {
			if err := h.automation.Scheduler.LoadFromSettings(mutationCtx); err != nil {
				restoreErr := h.restoreAutomationSettings(mutationCtx, rollbackSettings)
				if restoreErr != nil {
					http.Error(
						w,
						fmt.Sprintf("apply automation runtime settings: %v (rollback failed: %v)", err, restoreErr),
						http.StatusInternalServerError,
					)
					return
				}
				http.Error(w, fmt.Sprintf("apply automation runtime settings: %v", err), http.StatusInternalServerError)
				return
			}
		}
	}

	if timezoneUpdated {
		h.logAdminMutation(
			r,
			"admin automation timezone updated",
			"timezone", timezoneValue,
		)
	}

	if playlistUpdate.apply {
		h.logAdminMutation(
			r,
			"admin automation schedule updated",
			"job_name", jobs.JobPlaylistSync,
			"enabled", playlistUpdate.enabled,
			"cron_spec", playlistUpdate.cron,
		)
	}
	if autoUpdate.apply {
		h.logAdminMutation(
			r,
			"admin automation schedule updated",
			"job_name", jobs.JobAutoPrioritize,
			"enabled", autoUpdate.enabled,
			"cron_spec", autoUpdate.cron,
		)
	}

	settingKeys := nonSchedulerAutomationSettingKeys(settingsUpdates)
	if len(settingKeys) > 0 {
		h.logAdminMutation(
			r,
			"admin automation settings updated",
			"updated_keys", settingKeys,
			"updated_count", len(settingKeys),
		)
	}

	payload, err := h.automationState(mutationCtx)
	if err != nil {
		http.Error(w, fmt.Sprintf("get automation state: %v", err), http.StatusInternalServerError)
		return
	}
	h.logAdminMutation(
		r,
		"admin automation updated",
		"playlist_schedule_updated", playlistUpdate.apply,
		"auto_prioritize_schedule_updated", autoUpdate.apply,
		"settings_updated", len(settingKeys),
		"timezone_updated", timezoneUpdated,
	)
	writeJSON(w, http.StatusOK, payload)
}

func (h *AdminHandler) handleRunPlaylistSync(w http.ResponseWriter, r *http.Request) {
	h.startJobRun(w, r, jobs.JobPlaylistSync)
}

func (h *AdminHandler) handleRunAutoPrioritize(w http.ResponseWriter, r *http.Request) {
	h.startJobRun(w, r, jobs.JobAutoPrioritize)
}

func (h *AdminHandler) handleClearAutoPrioritizeCache(w http.ResponseWriter, r *http.Request) {
	deleted, err := h.automation.Settings.DeleteAllStreamMetrics(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("clear auto-prioritize cache: %v", err), http.StatusInternalServerError)
		return
	}
	h.logAdminMutation(
		r,
		"admin auto-prioritize cache cleared",
		"deleted", deleted,
	)
	writeJSON(w, http.StatusOK, map[string]any{
		"deleted": deleted,
	})
}

func (h *AdminHandler) startJobRun(w http.ResponseWriter, r *http.Request, jobName string) {
	fn := h.automation.JobFuncs[jobName]
	if fn == nil {
		http.Error(w, fmt.Sprintf("job %q is not configured", jobName), http.StatusInternalServerError)
		return
	}

	runID, err := h.automation.Runner.Start(context.WithoutCancel(r.Context()), jobName, jobs.TriggerManual, fn)
	if err != nil {
		if errors.Is(err, jobs.ErrAlreadyRunning) {
			http.Error(w, fmt.Sprintf("job %q is already running", jobName), http.StatusConflict)
			return
		}
		if isInputError(err) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, fmt.Sprintf("start job %q: %v", jobName, err), http.StatusInternalServerError)
		return
	}
	h.logAdminMutation(
		r,
		"admin manual job run started",
		"job_name", jobName,
		"run_id", runID,
		"triggered_by", jobs.TriggerManual,
	)

	writeJSON(w, http.StatusAccepted, map[string]any{
		"run_id": runID,
		"status": "queued",
	})
}

func (h *AdminHandler) handleGetJobRun(w http.ResponseWriter, r *http.Request) {
	runID, err := parsePathInt64(r, "runID")
	if err != nil {
		http.Error(w, "invalid run id", http.StatusBadRequest)
		return
	}

	run, err := h.automation.Runner.GetRun(r.Context(), runID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "job run not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("get run: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (h *AdminHandler) handleListJobRuns(w http.ResponseWriter, r *http.Request) {
	jobName := strings.TrimSpace(r.URL.Query().Get("name"))
	switch jobName {
	case "", jobs.JobPlaylistSync, jobs.JobAutoPrioritize, jobs.JobDVRLineupSync:
	default:
		http.Error(w, "name must be playlist_sync, auto_prioritize, or dvr_lineup_sync", http.StatusBadRequest)
		return
	}

	limit := parseInt(r.URL.Query().Get("limit"), 50)
	if limit < 1 {
		limit = 1
	}
	if limit > 500 {
		limit = 500
	}
	offset := parseInt(r.URL.Query().Get("offset"), 0)
	if offset < 0 {
		offset = 0
	}

	runs, err := h.automation.Runner.ListRuns(r.Context(), jobName, limit, offset)
	if err != nil {
		http.Error(w, fmt.Sprintf("list runs: %v", err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"name":   jobName,
		"limit":  limit,
		"offset": offset,
		"runs":   runs,
	})
}

func (h *AdminHandler) automationState(ctx context.Context) (automationStateResponse, error) {
	schedules, err := h.automation.Scheduler.ListSchedules(ctx)
	if err != nil {
		return automationStateResponse{}, err
	}
	scheduleByName := map[string]scheduler.JobSchedule{}
	for _, schedule := range schedules {
		scheduleByName[schedule.JobName] = schedule
	}

	playlistURL, err := h.settingOrDefault(ctx, sqlite.SettingPlaylistURL, "")
	if err != nil {
		return automationStateResponse{}, err
	}

	probeTimeoutMS, err := h.intSettingOrDefault(ctx, sqlite.SettingAnalyzerProbeTimeoutMS, 7000)
	if err != nil {
		return automationStateResponse{}, err
	}
	analyzeDurationUS, err := h.int64SettingOrDefault(ctx, sqlite.SettingAnalyzerAnalyzeDurationUS, 1_500_000)
	if err != nil {
		return automationStateResponse{}, err
	}
	probeSizeBytes, err := h.int64SettingOrDefault(ctx, sqlite.SettingAnalyzerProbeSizeBytes, 1_000_000)
	if err != nil {
		return automationStateResponse{}, err
	}
	bitrateMode, err := h.settingOrDefault(ctx, sqlite.SettingAnalyzerBitrateMode, analyzer.BitrateModeMetadataThenSample)
	if err != nil {
		return automationStateResponse{}, err
	}
	sampleSeconds, err := h.intSettingOrDefault(ctx, sqlite.SettingAnalyzerSampleSeconds, 3)
	if err != nil {
		return automationStateResponse{}, err
	}
	enabledOnly, err := h.boolSettingOrDefault(ctx, sqlite.SettingAutoPrioritizeEnabledOnly, true)
	if err != nil {
		return automationStateResponse{}, err
	}
	topN, err := h.intSettingOrDefault(ctx, sqlite.SettingAutoPrioritizeTopNPerChannel, 0)
	if err != nil {
		return automationStateResponse{}, err
	}

	playlistSchedule := scheduleByName[jobs.JobPlaylistSync]
	autoSchedule := scheduleByName[jobs.JobAutoPrioritize]

	playlistLastRun, err := h.latestRun(ctx, jobs.JobPlaylistSync)
	if err != nil {
		return automationStateResponse{}, err
	}
	autoLastRun, err := h.latestRun(ctx, jobs.JobAutoPrioritize)
	if err != nil {
		return automationStateResponse{}, err
	}

	payload := automationStateResponse{
		PlaylistURL: playlistURL,
		Timezone:    h.automation.Scheduler.Timezone(),
		PlaylistSync: automationScheduleState{
			Enabled:  playlistSchedule.Enabled,
			CronSpec: playlistSchedule.CronSpec,
			LastRun:  playlistLastRun,
		},
		AutoPrioritize: automationScheduleState{
			Enabled:  autoSchedule.Enabled,
			CronSpec: autoSchedule.CronSpec,
			LastRun:  autoLastRun,
		},
		Analyzer: automationAnalyzerSettings{
			ProbeTimeoutMS:    probeTimeoutMS,
			AnalyzeDurationUS: analyzeDurationUS,
			ProbeSizeBytes:    probeSizeBytes,
			BitrateMode:       strings.ToLower(strings.TrimSpace(bitrateMode)),
			SampleSeconds:     sampleSeconds,
			EnabledOnly:       enabledOnly,
			TopNPerChannel:    topN,
		},
	}

	if !playlistSchedule.NextRun.IsZero() {
		payload.PlaylistSync.NextRun = playlistSchedule.NextRun.UTC().Unix()
	}
	if !autoSchedule.NextRun.IsZero() {
		payload.AutoPrioritize.NextRun = autoSchedule.NextRun.UTC().Unix()
	}
	return payload, nil
}

func (h *AdminHandler) latestRun(ctx context.Context, jobName string) (*jobs.Run, error) {
	runs, err := h.automation.Runner.ListRuns(ctx, jobName, 1, 0)
	if err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return nil, nil
	}
	return &runs[0], nil
}

func (h *AdminHandler) settingOrDefault(ctx context.Context, key, fallback string) (string, error) {
	value, err := h.automation.Settings.GetSetting(ctx, key)
	if err == sql.ErrNoRows {
		return fallback, nil
	}
	if err != nil {
		return "", fmt.Errorf("read setting %q: %w", key, err)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback, nil
	}
	return value, nil
}

func (h *AdminHandler) intSettingOrDefault(ctx context.Context, key string, fallback int) (int, error) {
	raw, err := h.settingOrDefault(ctx, key, intToString(fallback))
	if err != nil {
		return 0, err
	}
	value, ok := parseIntStrict(raw)
	if !ok {
		return 0, fmt.Errorf("invalid integer setting %q=%q", key, raw)
	}
	return value, nil
}

func (h *AdminHandler) int64SettingOrDefault(ctx context.Context, key string, fallback int64) (int64, error) {
	raw, err := h.settingOrDefault(ctx, key, int64ToString(fallback))
	if err != nil {
		return 0, err
	}
	value, ok := parseInt64Strict(raw)
	if !ok {
		return 0, fmt.Errorf("invalid integer setting %q=%q", key, raw)
	}
	return value, nil
}

func (h *AdminHandler) boolSettingOrDefault(ctx context.Context, key string, fallback bool) (bool, error) {
	raw, err := h.settingOrDefault(ctx, key, boolToSetting(fallback))
	if err != nil {
		return false, err
	}
	return parseBoolSetting(raw, fallback), nil
}

func parseScheduleUpdate(
	jobName string,
	current scheduler.JobSchedule,
	req *struct {
		Enabled  *bool   `json:"enabled"`
		CronSpec *string `json:"cron_spec"`
	},
) (struct {
	apply   bool
	enabled bool
	cron    string
}, error) {
	update := struct {
		apply   bool
		enabled bool
		cron    string
	}{
		apply:   false,
		enabled: current.Enabled,
		cron:    strings.TrimSpace(current.CronSpec),
	}
	if req == nil {
		return update, nil
	}

	if req.Enabled != nil {
		update.apply = true
		update.enabled = *req.Enabled
	}
	if req.CronSpec != nil {
		update.apply = true
		update.cron = strings.TrimSpace(*req.CronSpec)
	}

	if update.apply && update.enabled && update.cron == "" {
		return update, fmt.Errorf("%s cron_spec is required when schedule is enabled", jobName)
	}

	return update, nil
}

func isCronError(err error) bool {
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "cron")
}

func (h *AdminHandler) snapshotAutomationSettings(ctx context.Context, updates map[string]string) (map[string]string, error) {
	snapshot := make(map[string]string, len(updates))
	for key := range updates {
		value, err := h.automation.Settings.GetSetting(ctx, key)
		if errors.Is(err, sql.ErrNoRows) {
			snapshot[key] = automationDefaultSettingValue(key)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read setting %q: %w", key, err)
		}
		snapshot[key] = value
	}
	return snapshot, nil
}

func (h *AdminHandler) restoreAutomationSettings(ctx context.Context, values map[string]string) error {
	if len(values) == 0 {
		return nil
	}
	if err := h.automation.Settings.SetSettings(ctx, values); err != nil {
		return fmt.Errorf("restore settings: %w", err)
	}
	if err := h.automation.Scheduler.LoadFromSettings(ctx); err != nil {
		return fmt.Errorf("reload scheduler after restore: %w", err)
	}
	return nil
}

func nonSchedulerAutomationSettingKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		if isSchedulerAutomationSettingKey(key) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func isSchedulerAutomationSettingKey(key string) bool {
	switch key {
	case sqlite.SettingJobsTimezone,
		sqlite.SettingJobsPlaylistSyncEnabled,
		sqlite.SettingJobsPlaylistSyncCron,
		sqlite.SettingJobsAutoPrioritizeEnabled,
		sqlite.SettingJobsAutoPrioritizeCron:
		return true
	default:
		return false
	}
}

func automationDefaultSettingValue(key string) string {
	switch key {
	case sqlite.SettingPlaylistURL:
		return ""
	case sqlite.SettingJobsTimezone:
		return "America/Chicago"
	case sqlite.SettingJobsPlaylistSyncEnabled:
		return boolToSetting(true)
	case sqlite.SettingJobsPlaylistSyncCron:
		return "*/30 * * * *"
	case sqlite.SettingJobsAutoPrioritizeEnabled:
		return boolToSetting(false)
	case sqlite.SettingJobsAutoPrioritizeCron:
		return "30 3 * * *"
	case sqlite.SettingAnalyzerProbeTimeoutMS:
		return intToString(7000)
	case sqlite.SettingAnalyzerAnalyzeDurationUS:
		return int64ToString(1_500_000)
	case sqlite.SettingAnalyzerProbeSizeBytes:
		return int64ToString(1_000_000)
	case sqlite.SettingAnalyzerBitrateMode:
		return analyzer.BitrateModeMetadataThenSample
	case sqlite.SettingAnalyzerSampleSeconds:
		return intToString(3)
	case sqlite.SettingAutoPrioritizeEnabledOnly:
		return boolToSetting(true)
	case sqlite.SettingAutoPrioritizeTopNPerChannel:
		return intToString(0)
	default:
		return ""
	}
}

func parseBoolSetting(raw string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func parseIntStrict(raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return value, true
}

func parseInt64Strict(raw string) (int64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

func boolToSetting(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func intToString(value int) string {
	return strconv.Itoa(value)
}

func int64ToString(value int64) string {
	return strconv.FormatInt(value, 10)
}
