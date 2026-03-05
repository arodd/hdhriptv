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
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/scheduler"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
)

// AutomationSettingsStore reads and writes automation settings.
type AutomationSettingsStore interface {
	GetSetting(ctx context.Context, key string) (string, error)
	SetSettings(ctx context.Context, values map[string]string) error
	DeleteAllStreamMetrics(ctx context.Context) (int64, error)
	GetPlaylistSource(ctx context.Context, sourceID int64) (playlist.PlaylistSource, error)
	ListPlaylistSources(ctx context.Context) ([]playlist.PlaylistSource, error)
	CreatePlaylistSource(ctx context.Context, create playlist.PlaylistSourceCreate) (playlist.PlaylistSource, error)
	UpdatePlaylistSource(ctx context.Context, sourceID int64, update playlist.PlaylistSourceUpdate) (playlist.PlaylistSource, error)
	BulkUpdatePlaylistSources(ctx context.Context, updates []playlist.PlaylistSourceBulkUpdate) error
	DeletePlaylistSource(ctx context.Context, sourceID int64) error
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
	PlaylistURL     string                     `json:"playlist_url"`
	PlaylistSources []playlistSourceResponse   `json:"playlist_sources"`
	Timezone        string                     `json:"timezone"`
	PlaylistSync    automationScheduleState    `json:"playlist_sync"`
	AutoPrioritize  automationScheduleState    `json:"auto_prioritize"`
	Analyzer        automationAnalyzerSettings `json:"analyzer"`
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

type playlistSourceResponse struct {
	SourceID    int64  `json:"source_id"`
	SourceKey   string `json:"source_key"`
	Name        string `json:"name"`
	PlaylistURL string `json:"playlist_url"`
	TunerCount  int    `json:"tuner_count"`
	Enabled     bool   `json:"enabled"`
	OrderIndex  int    `json:"order_index"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

type playlistSourceUpsertRequest struct {
	SourceID    *int64 `json:"source_id,omitempty"`
	Name        string `json:"name"`
	PlaylistURL string `json:"playlist_url"`
	TunerCount  int    `json:"tuner_count"`
	Enabled     *bool  `json:"enabled,omitempty"`
}

type normalizedPlaylistSourceUpsert struct {
	SourceID    int64
	Name        string
	PlaylistURL string
	TunerCount  int
	Enabled     bool
}

func playlistSourceResponseFromModel(source playlist.PlaylistSource) playlistSourceResponse {
	return playlistSourceResponse{
		SourceID:    source.SourceID,
		SourceKey:   source.SourceKey,
		Name:        source.Name,
		PlaylistURL: source.PlaylistURL,
		TunerCount:  source.TunerCount,
		Enabled:     source.Enabled,
		OrderIndex:  source.OrderIndex,
		CreatedAt:   source.CreatedAt,
		UpdatedAt:   source.UpdatedAt,
	}
}

func playlistSourceResponsesFromModels(sources []playlist.PlaylistSource) []playlistSourceResponse {
	out := make([]playlistSourceResponse, 0, len(sources))
	for _, source := range sources {
		out = append(out, playlistSourceResponseFromModel(source))
	}
	return out
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
		PlaylistURL     *string                        `json:"playlist_url"`
		PlaylistSources *[]playlistSourceUpsertRequest `json:"playlist_sources"`
		Timezone        *string                        `json:"timezone"`

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

	var playlistSourceUpserts []normalizedPlaylistSourceUpsert
	playlistSourcesUpdated := false
	playlistURLUpdated := false
	if req.PlaylistSources != nil {
		playlistSourceUpserts, err = h.normalizeAutomationPlaylistSourceUpserts(r.Context(), *req.PlaylistSources, req.PlaylistURL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		playlistSourcesUpdated = true
	} else if req.PlaylistURL != nil {
		playlistURLUpdated = true
	}

	settingsUpdates := map[string]string{}
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
		normalizedTimezone, timezoneErr := scheduler.ValidateTimezone(timezoneValue)
		if timezoneErr != nil {
			http.Error(w, timezoneErr.Error(), http.StatusBadRequest)
			return
		}
		timezoneValue = normalizedTimezone
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
	if len(settingsUpdates) > 0 || playlistSourcesUpdated || playlistURLUpdated {
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

	if playlistSourcesUpdated {
		if err := h.applyAutomationPlaylistSourceUpserts(mutationCtx, playlistSourceUpserts); err != nil {
			writePlaylistSourceMutationError(w, err, "update playlist_sources")
			return
		}
		if err := h.reloadPlaylistSourceRuntime(mutationCtx); err != nil {
			writePlaylistSourceRuntimeApplyError(
				w,
				"update_playlist_sources",
				err,
				map[string]any{"source_ids": playlistSourceIDsFromNormalizedUpserts(playlistSourceUpserts)},
			)
			return
		}
		playlistURLUpdated = true
	} else if req.PlaylistURL != nil {
		playlistURL := strings.TrimSpace(*req.PlaylistURL)
		update := playlist.PlaylistSourceUpdate{
			PlaylistURL: &playlistURL,
		}
		if _, err := h.automation.Settings.UpdatePlaylistSource(mutationCtx, 1, update); err != nil {
			writePlaylistSourceMutationError(w, err, "update playlist_url")
			return
		}
	}

	if timezoneUpdated {
		h.logAdminMutation(
			r,
			"admin automation timezone updated",
			"timezone", timezoneValue,
		)
	}

	if playlistSourcesUpdated {
		h.logAdminMutation(
			r,
			"admin automation playlist sources updated",
			"source_count", len(playlistSourceUpserts),
		)
	} else if playlistURLUpdated {
		h.logAdminMutation(
			r,
			"admin automation playlist url updated",
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
		"playlist_sources_updated", playlistSourcesUpdated,
		"playlist_url_updated", playlistURLUpdated,
		"settings_updated", len(settingKeys),
		"timezone_updated", timezoneUpdated,
	)
	writeJSON(w, http.StatusOK, payload)
}

func (h *AdminHandler) handleListPlaylistSources(w http.ResponseWriter, r *http.Request) {
	sources, err := h.automation.Settings.ListPlaylistSources(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("list playlist sources: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"playlist_sources": playlistSourceResponsesFromModels(sources),
	})
}

func (h *AdminHandler) handleGetPlaylistSource(w http.ResponseWriter, r *http.Request) {
	sourceID, err := parsePathInt64(r, "sourceID")
	if err != nil {
		http.Error(w, "invalid source id", http.StatusBadRequest)
		return
	}

	source, err := h.automation.Settings.GetPlaylistSource(r.Context(), sourceID)
	if err != nil {
		writePlaylistSourceMutationError(w, err, fmt.Sprintf("get playlist source %d", sourceID))
		return
	}
	writeJSON(w, http.StatusOK, playlistSourceResponseFromModel(source))
}

func (h *AdminHandler) handleCreatePlaylistSource(w http.ResponseWriter, r *http.Request) {
	var req playlistSourceUpsertRequest
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}
	if req.SourceID != nil {
		http.Error(w, "source_id is not allowed in create payload", http.StatusBadRequest)
		return
	}

	create := playlist.PlaylistSourceCreate{
		Name:        strings.TrimSpace(req.Name),
		PlaylistURL: strings.TrimSpace(req.PlaylistURL),
		TunerCount:  req.TunerCount,
		Enabled:     req.Enabled,
	}
	mutationCtx, cancelMutation := h.detachedAutomationMutationContext(r.Context())
	defer cancelMutation()

	source, err := h.automation.Settings.CreatePlaylistSource(mutationCtx, create)
	if err != nil {
		writePlaylistSourceMutationError(w, err, "create playlist source")
		return
	}
	if err := h.reloadPlaylistSourceRuntime(mutationCtx); err != nil {
		writePlaylistSourceRuntimeApplyError(
			w,
			"create_playlist_source",
			err,
			map[string]any{"source_id": source.SourceID},
		)
		return
	}

	h.logAdminMutation(
		r,
		"admin playlist source created",
		"source_id", source.SourceID,
		"name", source.Name,
		"enabled", source.Enabled,
		"tuner_count", source.TunerCount,
	)
	writeJSON(w, http.StatusCreated, playlistSourceResponseFromModel(source))
}

func (h *AdminHandler) handleUpdatePlaylistSource(w http.ResponseWriter, r *http.Request) {
	sourceID, err := parsePathInt64(r, "sourceID")
	if err != nil {
		http.Error(w, "invalid source id", http.StatusBadRequest)
		return
	}

	var req struct {
		Name        *string `json:"name"`
		PlaylistURL *string `json:"playlist_url"`
		TunerCount  *int    `json:"tuner_count"`
		Enabled     *bool   `json:"enabled"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}
	if req.Name == nil && req.PlaylistURL == nil && req.TunerCount == nil && req.Enabled == nil {
		http.Error(w, "at least one field is required", http.StatusBadRequest)
		return
	}

	update := playlist.PlaylistSourceUpdate{
		Name:        req.Name,
		PlaylistURL: req.PlaylistURL,
		TunerCount:  req.TunerCount,
		Enabled:     req.Enabled,
	}
	mutationCtx, cancelMutation := h.detachedAutomationMutationContext(r.Context())
	defer cancelMutation()

	source, err := h.automation.Settings.UpdatePlaylistSource(mutationCtx, sourceID, update)
	if err != nil {
		writePlaylistSourceMutationError(w, err, fmt.Sprintf("update playlist source %d", sourceID))
		return
	}
	if err := h.reloadPlaylistSourceRuntime(mutationCtx); err != nil {
		writePlaylistSourceRuntimeApplyError(
			w,
			"update_playlist_source",
			err,
			map[string]any{"source_id": sourceID},
		)
		return
	}

	h.logAdminMutation(
		r,
		"admin playlist source updated",
		"source_id", source.SourceID,
		"name", source.Name,
		"enabled", source.Enabled,
		"tuner_count", source.TunerCount,
	)
	writeJSON(w, http.StatusOK, playlistSourceResponseFromModel(source))
}

func (h *AdminHandler) handleDeletePlaylistSource(w http.ResponseWriter, r *http.Request) {
	sourceID, err := parsePathInt64(r, "sourceID")
	if err != nil {
		http.Error(w, "invalid source id", http.StatusBadRequest)
		return
	}

	mutationCtx, cancelMutation := h.detachedAutomationMutationContext(r.Context())
	defer cancelMutation()

	if err := h.automation.Settings.DeletePlaylistSource(mutationCtx, sourceID); err != nil {
		writePlaylistSourceMutationError(w, err, fmt.Sprintf("delete playlist source %d", sourceID))
		return
	}
	if err := h.reloadPlaylistSourceRuntime(mutationCtx); err != nil {
		writePlaylistSourceRuntimeApplyError(
			w,
			"delete_playlist_source",
			err,
			map[string]any{"source_id": sourceID},
		)
		return
	}

	h.logAdminMutation(
		r,
		"admin playlist source deleted",
		"source_id", sourceID,
	)
	writeJSON(w, http.StatusOK, map[string]any{
		"deleted":   true,
		"source_id": sourceID,
	})
}

func (h *AdminHandler) normalizeAutomationPlaylistSourceUpserts(
	ctx context.Context,
	reqSources []playlistSourceUpsertRequest,
	legacyPlaylistURL *string,
) ([]normalizedPlaylistSourceUpsert, error) {
	if len(reqSources) == 0 {
		return nil, fmt.Errorf("playlist_sources must include at least one source")
	}

	existingSources, err := h.automation.Settings.ListPlaylistSources(ctx)
	if err != nil {
		return nil, fmt.Errorf("list playlist sources: %w", err)
	}
	if len(existingSources) == 0 {
		return nil, fmt.Errorf("no playlist sources are configured")
	}
	if len(reqSources) != len(existingSources) {
		return nil, fmt.Errorf("playlist_sources must include all existing sources; use /api/admin/playlist-sources CRUD endpoints for add/remove")
	}

	existingByID := make(map[int64]playlist.PlaylistSource, len(existingSources))
	for _, source := range existingSources {
		existingByID[source.SourceID] = source
	}

	out := make([]normalizedPlaylistSourceUpsert, 0, len(reqSources))
	seenIDs := make(map[int64]struct{}, len(reqSources))
	seenNames := make(map[string]int, len(reqSources))
	seenURLs := make(map[string]int, len(reqSources))
	enabledCount := 0

	for i, reqSource := range reqSources {
		if reqSource.SourceID == nil {
			return nil, fmt.Errorf("playlist_sources[%d].source_id is required", i)
		}
		sourceID := *reqSource.SourceID
		if sourceID <= 0 {
			return nil, fmt.Errorf("playlist_sources[%d].source_id must be a positive integer", i)
		}
		if _, exists := seenIDs[sourceID]; exists {
			return nil, fmt.Errorf("playlist_sources contains duplicate source_id %d", sourceID)
		}
		existing, ok := existingByID[sourceID]
		if !ok {
			return nil, fmt.Errorf("playlist_sources[%d].source_id=%d not found", i, sourceID)
		}
		if expectedSourceID := existingSources[i].SourceID; sourceID != expectedSourceID {
			return nil, fmt.Errorf(
				"playlist_sources[%d].source_id must be %d to preserve existing source order (got %d)",
				i,
				expectedSourceID,
				sourceID,
			)
		}
		seenIDs[sourceID] = struct{}{}

		name := strings.TrimSpace(reqSource.Name)
		if name == "" {
			return nil, fmt.Errorf("playlist_sources[%d].name is required", i)
		}
		playlistURL := playlist.CanonicalPlaylistSourceURL(reqSource.PlaylistURL)
		if playlistURL == "" && sourceID != 1 {
			return nil, fmt.Errorf("playlist_sources[%d].playlist_url is required", i)
		}
		if reqSource.TunerCount < 1 {
			return nil, fmt.Errorf("playlist_sources[%d].tuner_count must be at least 1", i)
		}

		nameKey := playlist.CanonicalPlaylistSourceName(name)
		if prev, exists := seenNames[nameKey]; exists {
			return nil, fmt.Errorf("playlist_sources[%d].name duplicates playlist_sources[%d].name", i, prev)
		}
		seenNames[nameKey] = i

		urlKey := playlist.CanonicalPlaylistSourceURL(playlistURL)
		if prev, exists := seenURLs[urlKey]; exists {
			return nil, fmt.Errorf("playlist_sources[%d].playlist_url duplicates playlist_sources[%d].playlist_url", i, prev)
		}
		seenURLs[urlKey] = i

		enabled := existing.Enabled
		if reqSource.Enabled != nil {
			enabled = *reqSource.Enabled
		}
		if enabled {
			enabledCount++
		}

		out = append(out, normalizedPlaylistSourceUpsert{
			SourceID:    sourceID,
			Name:        name,
			PlaylistURL: playlistURL,
			TunerCount:  reqSource.TunerCount,
			Enabled:     enabled,
		})
	}

	if _, ok := seenIDs[1]; !ok {
		return nil, playlist.ErrPrimaryPlaylistSourceOrderChange
	}
	if enabledCount == 0 {
		return nil, playlist.ErrNoEnabledPlaylistSources
	}

	if legacyPlaylistURL != nil {
		legacyURL := playlist.CanonicalPlaylistSourceURL(*legacyPlaylistURL)
		primaryURL := ""
		for _, source := range out {
			if source.SourceID == 1 {
				primaryURL = source.PlaylistURL
				break
			}
		}
		if legacyURL != primaryURL {
			return nil, fmt.Errorf("playlist_url must match playlist_sources primary source URL")
		}
	}

	return out, nil
}

func (h *AdminHandler) applyAutomationPlaylistSourceUpserts(ctx context.Context, upserts []normalizedPlaylistSourceUpsert) error {
	if len(upserts) == 0 {
		return nil
	}

	updates := make([]playlist.PlaylistSourceBulkUpdate, 0, len(upserts))
	for _, upsert := range upserts {
		updates = append(updates, playlist.PlaylistSourceBulkUpdate{
			SourceID:    upsert.SourceID,
			Name:        upsert.Name,
			PlaylistURL: upsert.PlaylistURL,
			TunerCount:  upsert.TunerCount,
			Enabled:     upsert.Enabled,
		})
	}
	return h.automation.Settings.BulkUpdatePlaylistSources(ctx, updates)
}

func playlistSourceIDsFromNormalizedUpserts(upserts []normalizedPlaylistSourceUpsert) []int64 {
	if len(upserts) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(upserts))
	for _, upsert := range upserts {
		if upsert.SourceID <= 0 {
			continue
		}
		ids = append(ids, upsert.SourceID)
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

func (h *AdminHandler) reloadPlaylistSourceRuntime(ctx context.Context) error {
	if h == nil || h.playlistSourceRuntime == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return h.playlistSourceRuntime.ReloadPlaylistSources(ctx)
}

func writePlaylistSourceMutationError(w http.ResponseWriter, err error, action string) {
	switch {
	case errors.Is(err, playlist.ErrPlaylistSourceNotFound), errors.Is(err, sql.ErrNoRows):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case errors.Is(err, playlist.ErrPlaylistSourceOrderDrift):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case errors.Is(err, playlist.ErrPrimaryPlaylistSourceDelete),
		errors.Is(err, playlist.ErrPrimaryPlaylistSourceOrderChange),
		errors.Is(err, playlist.ErrNoEnabledPlaylistSources):
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if isInputError(err) || strings.Contains(message, "already exists") {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	http.Error(w, fmt.Sprintf("%s: %v", action, err), http.StatusInternalServerError)
}

func writePlaylistSourceRuntimeApplyError(w http.ResponseWriter, operation string, runtimeErr error, extras map[string]any) {
	payload := map[string]any{
		"error":           "playlist_source_runtime_apply_failed",
		"message":         "playlist source mutation persisted but runtime source reload failed",
		"operation":       strings.TrimSpace(operation),
		"persisted":       true,
		"runtime_applied": false,
		"consistency":     "eventual",
	}
	detail := ""
	if runtimeErr != nil {
		detail = strings.TrimSpace(runtimeErr.Error())
	}
	if detail != "" {
		payload["runtime_error"] = detail
	}
	for key, value := range extras {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		payload[trimmed] = value
	}
	writeJSON(w, http.StatusInternalServerError, payload)
}

func (h *AdminHandler) handleRunPlaylistSync(w http.ResponseWriter, r *http.Request) {
	sourceID, sourceScope, err := parsePlaylistSyncSourceIDQueryParam(r.URL.Query().Get("source_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	runCtx := context.WithoutCancel(r.Context())
	responseExtras := map[string]any{}
	logExtras := make([]any, 0, 2)

	if sourceScope {
		source, err := h.automation.Settings.GetPlaylistSource(r.Context(), sourceID)
		if err != nil {
			if errors.Is(err, playlist.ErrPlaylistSourceNotFound) || errors.Is(err, sql.ErrNoRows) {
				http.Error(w, "playlist source not found", http.StatusNotFound)
				return
			}
			http.Error(w, fmt.Sprintf("get playlist source %d: %v", sourceID, err), http.StatusInternalServerError)
			return
		}
		if !source.Enabled {
			http.Error(w, fmt.Sprintf("playlist source %d is disabled", sourceID), http.StatusBadRequest)
			return
		}

		runCtx = jobs.WithPlaylistSyncSourceID(runCtx, sourceID)
		responseExtras["source_id"] = sourceID
		logExtras = append(logExtras, "source_id", sourceID)
	}

	h.startJobRunWithContext(w, r, runCtx, jobs.JobPlaylistSync, logExtras, responseExtras)
}

func (h *AdminHandler) handleRunAutoPrioritize(w http.ResponseWriter, r *http.Request) {
	h.startJobRunWithContext(w, r, context.WithoutCancel(r.Context()), jobs.JobAutoPrioritize, nil, nil)
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

func (h *AdminHandler) startJobRunWithContext(
	w http.ResponseWriter,
	r *http.Request,
	runCtx context.Context,
	jobName string,
	logExtras []any,
	responseExtras map[string]any,
) {
	if runCtx == nil {
		runCtx = context.WithoutCancel(r.Context())
	}

	fn := h.automation.JobFuncs[jobName]
	if fn == nil {
		http.Error(w, fmt.Sprintf("job %q is not configured", jobName), http.StatusInternalServerError)
		return
	}

	runID, err := h.automation.Runner.Start(runCtx, jobName, jobs.TriggerManual, fn)
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
	logFields := []any{
		"job_name", jobName,
		"run_id", runID,
		"triggered_by", jobs.TriggerManual,
	}
	if len(logExtras) > 0 {
		logFields = append(logFields, logExtras...)
	}
	h.logAdminMutation(r, "admin manual job run started", logFields...)

	payload := map[string]any{
		"run_id": runID,
		"status": "queued",
	}
	for key, value := range responseExtras {
		payload[key] = value
	}
	writeJSON(w, http.StatusAccepted, payload)
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

	playlistSources, err := h.automation.Settings.ListPlaylistSources(ctx)
	if err != nil {
		return automationStateResponse{}, err
	}
	playlistURL := ""
	for _, source := range playlistSources {
		if source.SourceID == 1 {
			playlistURL = strings.TrimSpace(source.PlaylistURL)
			break
		}
	}
	if playlistURL == "" && len(playlistSources) > 0 {
		playlistURL = strings.TrimSpace(playlistSources[0].PlaylistURL)
	}
	if playlistURL == "" {
		playlistURL, err = h.settingOrDefault(ctx, sqlite.SettingPlaylistURL, "")
		if err != nil {
			return automationStateResponse{}, err
		}
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
		PlaylistURL:     playlistURL,
		PlaylistSources: playlistSourceResponsesFromModels(playlistSources),
		Timezone:        h.automation.Scheduler.Timezone(),
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

func parsePlaylistSyncSourceIDQueryParam(raw string) (sourceID int64, hasValue bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false, nil
	}
	sourceID, ok := parseInt64Strict(raw)
	if !ok || sourceID <= 0 {
		return 0, false, fmt.Errorf("source_id must be a positive integer")
	}
	return sourceID, true, nil
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
