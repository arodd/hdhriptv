package httpapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/dvr"
	"github.com/arodd/hdhriptv/internal/jobs"
)

func (h *AdminHandler) handleGetDVR(w http.ResponseWriter, r *http.Request) {
	state, err := h.dvr.GetState(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("get dvr state: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, redactDVRConfigStateForAPI(state))
}

func (h *AdminHandler) handlePutDVR(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Provider              *string   `json:"provider"`
		ActiveProviders       *[]string `json:"active_providers"`
		BaseURL               *string   `json:"base_url"`
		ChannelsBaseURL       *string   `json:"channels_base_url"`
		JellyfinBaseURL       *string   `json:"jellyfin_base_url"`
		DefaultLineupID       *string   `json:"default_lineup_id"`
		SyncEnabled           *bool     `json:"sync_enabled"`
		SyncCron              *string   `json:"sync_cron"`
		SyncMode              *string   `json:"sync_mode"`
		PreSyncRefreshDevices *bool     `json:"pre_sync_refresh_devices"`
		JellyfinAPIToken      *string   `json:"jellyfin_api_token"`
		JellyfinTunerHostID   *string   `json:"jellyfin_tuner_host_id"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	h.adminConfigMutationMu.Lock()
	defer h.adminConfigMutationMu.Unlock()

	current, err := h.dvr.GetState(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("get current dvr state: %v", err), http.StatusInternalServerError)
		return
	}
	previous := current.Instance

	next := current.Instance
	if req.Provider != nil {
		next.Provider = dvr.ProviderType(strings.TrimSpace(*req.Provider))
	}
	if req.ActiveProviders != nil {
		next.ActiveProviders = make([]dvr.ProviderType, 0, len(*req.ActiveProviders))
		for _, provider := range *req.ActiveProviders {
			next.ActiveProviders = append(next.ActiveProviders, dvr.ProviderType(strings.TrimSpace(provider)))
		}
	}
	if req.BaseURL != nil {
		next.BaseURL = strings.TrimSpace(*req.BaseURL)
	}
	if req.ChannelsBaseURL != nil {
		next.ChannelsBaseURL = strings.TrimSpace(*req.ChannelsBaseURL)
	}
	if req.JellyfinBaseURL != nil {
		next.JellyfinBaseURL = strings.TrimSpace(*req.JellyfinBaseURL)
	}
	if req.DefaultLineupID != nil {
		next.DefaultLineupID = strings.TrimSpace(*req.DefaultLineupID)
	}
	if req.SyncEnabled != nil {
		next.SyncEnabled = *req.SyncEnabled
	}
	if req.SyncCron != nil {
		next.SyncCron = strings.TrimSpace(*req.SyncCron)
	}
	if req.SyncMode != nil {
		next.SyncMode = dvr.SyncMode(strings.TrimSpace(*req.SyncMode))
	}
	if req.PreSyncRefreshDevices != nil {
		next.PreSyncRefreshDevices = *req.PreSyncRefreshDevices
	}
	if req.JellyfinAPIToken != nil {
		next.JellyfinAPIToken = strings.TrimSpace(*req.JellyfinAPIToken)
	}
	if req.JellyfinTunerHostID != nil {
		next.JellyfinTunerHostID = strings.TrimSpace(*req.JellyfinTunerHostID)
	}

	normalizedPrimaryProvider := dvr.NormalizeProviderType(next.Provider)
	if normalizedPrimaryProvider != dvr.ProviderChannels {
		http.Error(
			w,
			fmt.Sprintf(
				`invalid primary provider %q: only %q is supported for sync/mapping`,
				strings.TrimSpace(string(next.Provider)),
				dvr.ProviderChannels,
			),
			http.StatusBadRequest,
		)
		return
	}
	next.Provider = normalizedPrimaryProvider

	// Fail fast on invalid cron before persisting config to avoid partial updates
	// where the request errors but DVR config has already been mutated.
	if h.dvrScheduler != nil && next.SyncEnabled {
		cronSpec := strings.TrimSpace(next.SyncCron)
		if cronSpec == "" {
			cronSpec = strings.TrimSpace(current.Instance.SyncCron)
		}
		if cronSpec != "" {
			if err := h.dvrScheduler.ValidateCron(cronSpec); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
	}

	mutationCtx, cancelMutation := h.detachedDVRMutationContext(r.Context())
	defer cancelMutation()

	updated, err := h.dvr.UpdateConfig(mutationCtx, next)
	if err != nil {
		writeDVRError(w, err, "update dvr config")
		return
	}
	updatedResponse := redactDVRConfigStateForAPI(updated)

	if h.dvrScheduler != nil {
		if scheduleErr := h.dvrScheduler.UpdateJobSchedule(
			mutationCtx,
			jobs.JobDVRLineupSync,
			updated.Instance.SyncEnabled,
			strings.TrimSpace(updated.Instance.SyncCron),
		); scheduleErr != nil {
			rollbackErr := h.restoreDVRConfig(mutationCtx, previous)
			if rollbackErr != nil {
				http.Error(
					w,
					fmt.Sprintf("update dvr sync schedule: %v (rollback dvr config failed: %v)", scheduleErr, rollbackErr),
					http.StatusInternalServerError,
				)
				return
			}
			status := http.StatusInternalServerError
			if isCronError(scheduleErr) {
				status = http.StatusBadRequest
			}
			http.Error(w, fmt.Sprintf("update dvr sync schedule: %v (dvr config rolled back)", scheduleErr), status)
			return
		}
		h.logAdminMutation(
			r,
			"admin dvr schedule updated",
			"job_name", jobs.JobDVRLineupSync,
			"enabled", updated.Instance.SyncEnabled,
			"cron_spec", strings.TrimSpace(updated.Instance.SyncCron),
		)
	}

	h.logAdminMutation(
		r,
		"admin dvr config updated",
		"provider", updated.Instance.Provider,
		"active_providers", strings.Join(providerTypesToStrings(updated.Instance.ActiveProviders), ","),
		"base_url", updated.Instance.BaseURL,
		"channels_base_url", updated.Instance.ChannelsBaseURL,
		"jellyfin_base_url", updated.Instance.JellyfinBaseURL,
		"default_lineup_id", updated.Instance.DefaultLineupID,
		"sync_enabled", updated.Instance.SyncEnabled,
		"sync_mode", updated.Instance.SyncMode,
		"jellyfin_api_token_configured", updatedResponse.Instance.JellyfinAPITokenConfigured,
		"jellyfin_tuner_host_id", updated.Instance.JellyfinTunerHostID,
	)
	writeJSON(w, http.StatusOK, updatedResponse)
}

func (h *AdminHandler) restoreDVRConfig(ctx context.Context, instance dvr.InstanceConfig) error {
	if _, err := h.dvr.UpdateConfig(ctx, instance); err != nil {
		return fmt.Errorf("restore dvr config: %w", err)
	}
	return nil
}

func (h *AdminHandler) handleTestDVR(w http.ResponseWriter, r *http.Request) {
	result, err := h.dvr.TestConnection(r.Context())
	if err != nil {
		writeDVRError(w, err, "test dvr connection")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *AdminHandler) handleDVRLineups(w http.ResponseWriter, r *http.Request) {
	refresh := parseBoolQuery(r.URL.Query().Get("refresh"))
	lineups, err := h.dvr.ListLineups(r.Context(), refresh)
	if err != nil {
		writeDVRError(w, err, "list dvr lineups")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"lineups": lineups,
		"refresh": refresh,
	})
}

func (h *AdminHandler) handleDVRSync(w http.ResponseWriter, r *http.Request) {
	req := dvr.SyncRequest{}
	if err := h.decodeOptionalJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}
	timeout := h.dvrSyncRequestTimeout()
	h.logAdminMutation(
		r,
		"admin dvr sync requested",
		"dry_run", req.DryRun,
		"include_dynamic", req.IncludeDynamic,
		"timeout", timeout.String(),
	)

	ctx, cancel := h.detachedDVRContext(r.Context())
	defer cancel()

	result, err := h.dvr.Sync(ctx, req)
	if err != nil {
		writeDVRError(w, err, "run dvr sync")
		return
	}
	h.logAdminMutation(
		r,
		"admin dvr sync completed",
		"dry_run", result.DryRun,
		"include_dynamic", req.IncludeDynamic,
		"updated", result.UpdatedCount,
		"cleared", result.ClearedCount,
		"unchanged", result.UnchangedCount,
		"unresolved", result.UnresolvedCount,
		"warnings", len(result.Warnings),
		"duration_ms", result.DurationMS,
	)
	writeJSON(w, http.StatusOK, result)
}

func (h *AdminHandler) handleDVRReverseSync(w http.ResponseWriter, r *http.Request) {
	req := dvr.ReverseSyncRequest{}
	if err := h.decodeOptionalJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}
	timeout := h.dvrSyncRequestTimeout()
	h.logAdminMutation(
		r,
		"admin dvr reverse-sync requested",
		"dry_run", req.DryRun,
		"lineup_id", strings.TrimSpace(req.LineupID),
		"include_dynamic", req.IncludeDynamic,
		"timeout", timeout.String(),
	)

	ctx, cancel := h.detachedDVRContext(r.Context())
	defer cancel()

	result, err := h.dvr.ReverseSync(ctx, req)
	if err != nil {
		writeDVRError(w, err, "run dvr reverse sync")
		return
	}
	h.logAdminMutation(
		r,
		"admin dvr reverse-sync completed",
		"dry_run", result.DryRun,
		"lineup_id", result.LineupID,
		"include_dynamic", req.IncludeDynamic,
		"imported", result.ImportedCount,
		"unchanged", result.UnchangedCount,
		"warnings", len(result.Warnings),
		"duration_ms", result.DurationMS,
	)
	writeJSON(w, http.StatusOK, result)
}

func (h *AdminHandler) handleChannelDVRReverseSync(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}

	req := dvr.ReverseSyncRequest{}
	if err := h.decodeOptionalJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}
	timeout := h.dvrSyncRequestTimeout()
	h.logAdminMutation(
		r,
		"admin channel dvr reverse-sync requested",
		"channel_id", channelID,
		"dry_run", req.DryRun,
		"lineup_id", strings.TrimSpace(req.LineupID),
		"include_dynamic", req.IncludeDynamic,
		"timeout", timeout.String(),
	)

	ctx, cancel := h.detachedDVRContext(r.Context())
	defer cancel()

	result, err := h.dvr.ReverseSyncChannel(ctx, channelID, req)
	if err != nil {
		writeDVRError(w, err, "run channel dvr reverse sync")
		return
	}
	h.logAdminMutation(
		r,
		"admin channel dvr reverse-sync completed",
		"channel_id", channelID,
		"dry_run", result.DryRun,
		"lineup_id", result.LineupID,
		"include_dynamic", req.IncludeDynamic,
		"imported", result.ImportedCount,
		"unchanged", result.UnchangedCount,
		"warnings", len(result.Warnings),
		"duration_ms", result.DurationMS,
	)
	writeJSON(w, http.StatusOK, result)
}

func (h *AdminHandler) handleListChannelDVRMappings(w http.ResponseWriter, r *http.Request) {
	enabledOnly := parseBoolQuery(r.URL.Query().Get("enabled_only"))
	includeDynamic := parseBoolQuery(r.URL.Query().Get("include_dynamic"))
	limit, offset, err := parsePaginationQuery(
		r,
		defaultDVRChannelMappingsListLimit,
		maxDVRChannelMappingsListLimit,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	mappings, total, err := h.dvr.ListChannelMappingsPaged(
		r.Context(),
		enabledOnly,
		includeDynamic,
		limit,
		offset,
	)
	if err != nil {
		writeDVRError(w, err, "list channel dvr mappings")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"mappings":        mappings,
		"total":           total,
		"limit":           limit,
		"offset":          offset,
		"enabled_only":    enabledOnly,
		"include_dynamic": includeDynamic,
	})
}

func (h *AdminHandler) handleGetChannelDVRMapping(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}

	mapping, err := h.dvr.GetChannelMapping(r.Context(), channelID)
	if err != nil {
		writeDVRError(w, err, "get channel dvr mapping")
		return
	}
	writeJSON(w, http.StatusOK, mapping)
}

func (h *AdminHandler) handlePutChannelDVRMapping(w http.ResponseWriter, r *http.Request) {
	channelID, err := parsePathInt64(r, "channelID")
	if err != nil {
		http.Error(w, "invalid channel id", http.StatusBadRequest)
		return
	}

	var req dvr.ChannelMappingUpdate
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSONDecodeError(w, err)
		return
	}

	mapping, err := h.dvr.UpdateChannelMapping(r.Context(), channelID, req)
	if err != nil {
		writeDVRError(w, err, "update channel dvr mapping")
		return
	}
	h.logAdminMutation(
		r,
		"admin channel dvr mapping updated",
		"channel_id", channelID,
		"dvr_lineup_id", mapping.DVRLineupID,
		"dvr_lineup_channel", mapping.DVRLineupChannel,
		"dvr_station_ref", mapping.DVRStationRef,
	)
	writeJSON(w, http.StatusOK, mapping)
}

func writeDVRError(w http.ResponseWriter, err error, action string) {
	switch {
	case errors.Is(err, channels.ErrChannelNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	case errors.Is(err, dvr.ErrDVRSyncConfig):
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	case errors.Is(err, dvr.ErrSyncAlreadyRunning):
		http.Error(w, err.Error(), http.StatusConflict)
		return
	case errors.Is(err, dvr.ErrUnsupportedProvider):
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if isInputError(err) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	http.Error(w, fmt.Sprintf("%s: %v", action, err), http.StatusInternalServerError)
}

func parseBoolQuery(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	switch value {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func redactDVRConfigStateForAPI(state dvr.ConfigState) dvr.ConfigState {
	state.Instance = redactDVRInstanceForAPI(state.Instance)
	return state
}

func redactDVRInstanceForAPI(instance dvr.InstanceConfig) dvr.InstanceConfig {
	tokenPresent := strings.TrimSpace(instance.JellyfinAPIToken) != ""
	instance.JellyfinAPIToken = ""
	instance.JellyfinAPITokenConfigured = tokenPresent
	return instance
}

func providerTypesToStrings(providers []dvr.ProviderType) []string {
	if len(providers) == 0 {
		return nil
	}
	out := make([]string, 0, len(providers))
	for _, provider := range providers {
		value := strings.TrimSpace(string(provider))
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

func (h *AdminHandler) decodeOptionalJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if r == nil {
		return fmt.Errorf("request is required")
	}
	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 {
		return nil
	}
	err := h.decodeJSON(w, r, dst)
	if err == nil {
		return nil
	}
	// Preserve the prior "empty body is allowed" behavior for optional payload
	// routes when content length is unknown (e.g. chunked transfer encoding).
	if r.ContentLength < 0 && errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func parseIntQuery(value string, def int) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return def
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return def
	}
	return parsed
}
