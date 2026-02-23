package dvr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	maxSyncWarnings = 200
	syncBatchSize   = 75
	defaultSyncCron = "*/30 * * * *"
)

var (
	ErrDVRSyncConfig      = errors.New("dvr sync configuration error")
	ErrSyncAlreadyRunning = errors.New("dvr sync already running")
)

// Store describes persistence operations required by DVR integration.
type Store interface {
	GetDVRInstance(ctx context.Context) (InstanceConfig, error)
	UpsertDVRInstance(ctx context.Context, instance InstanceConfig) (InstanceConfig, error)
	ListChannelsForDVRSync(ctx context.Context, dvrInstanceID int64, enabledOnly bool, includeDynamic bool) ([]ChannelMapping, error)
	ListChannelsForDVRSyncPaged(ctx context.Context, dvrInstanceID int64, enabledOnly bool, includeDynamic bool, limit, offset int) ([]ChannelMapping, int, error)
	GetChannelDVRMapping(ctx context.Context, dvrInstanceID, channelID int64) (ChannelMapping, error)
	UpsertChannelDVRMapping(ctx context.Context, mapping ChannelMapping) (ChannelMapping, error)
	DeleteChannelDVRMapping(ctx context.Context, dvrInstanceID, channelID int64) error
}

type lineupReloadProviderFactory func(instance InstanceConfig, client *http.Client) (LineupReloadProvider, error)
type mappingProviderFactory func(instance InstanceConfig, client *http.Client) (MappingProvider, error)

// Service coordinates provider access, sync operations, and mapping persistence.
type Service struct {
	store                Store
	httpClient           *http.Client
	hdhrDeviceID         string
	lineupProviderBuild  lineupReloadProviderFactory
	mappingProviderBuild mappingProviderFactory
	syncRunLock          sync.Mutex

	mu              sync.RWMutex
	lastSync        *SyncResult
	cachedLineups   []DVRLineup
	cachedLineupsAt int64
}

// ConfigState is the API view of DVR integration state.
type ConfigState struct {
	Instance        InstanceConfig `json:"instance"`
	CachedLineups   []DVRLineup    `json:"cached_lineups,omitempty"`
	CachedLineupsAt int64          `json:"cached_lineups_at,omitempty"`
	LastSync        *SyncResult    `json:"last_sync,omitempty"`
}

// TestResult summarizes provider connectivity.
type TestResult struct {
	Reachable              bool   `json:"reachable"`
	Provider               string `json:"provider"`
	BaseURL                string `json:"base_url"`
	DeviceChannelCount     int    `json:"device_channel_count"`
	FilteredChannelCount   int    `json:"filtered_channel_count"`
	HDHRDeviceID           string `json:"hdhr_device_id,omitempty"`
	HDHRDeviceFilterActive bool   `json:"hdhr_device_filter_active"`
}

// SyncRequest controls sync execution behavior.
type SyncRequest struct {
	DryRun         bool `json:"dry_run"`
	IncludeDynamic bool `json:"include_dynamic,omitempty"`
}

// LineupSyncResult captures one lineup's sync work.
type LineupSyncResult struct {
	LineupID           string `json:"lineup_id"`
	ConfiguredChannels int    `json:"configured_channels"`
	ResolvedChannels   int    `json:"resolved_channels"`
	UpdatedCount       int    `json:"updated_count"`
	ClearedCount       int    `json:"cleared_count"`
	UnchangedCount     int    `json:"unchanged_count"`
	AppliedCount       int    `json:"applied_count"`
}

// SyncResult summarizes one sync run.
type SyncResult struct {
	StartedAt            int64                        `json:"started_at"`
	FinishedAt           int64                        `json:"finished_at"`
	DurationMS           int64                        `json:"duration_ms"`
	DryRun               bool                         `json:"dry_run"`
	Provider             string                       `json:"provider"`
	BaseURL              string                       `json:"base_url"`
	HDHRDeviceID         string                       `json:"hdhr_device_id,omitempty"`
	SyncMode             SyncMode                     `json:"sync_mode"`
	DeviceChannelCount   int                          `json:"device_channel_count"`
	FilteredChannelCount int                          `json:"filtered_channel_count"`
	UpdatedCount         int                          `json:"updated_count"`
	ClearedCount         int                          `json:"cleared_count"`
	UnchangedCount       int                          `json:"unchanged_count"`
	MissingTunerCount    int                          `json:"missing_tuner_count"`
	UnresolvedCount      int                          `json:"unresolved_count"`
	Lineups              []LineupSyncResult           `json:"lineups"`
	Warnings             []string                     `json:"warnings,omitempty"`
	PatchPreview         map[string]map[string]string `json:"patch_preview,omitempty"`
}

type deviceChannelIndex struct {
	DeviceKeys        []string
	TunerToDeviceKeys map[string][]string
	FilteredCount     int
	Warnings          []string
}

type syncPlan struct {
	Lineups           []syncLineupPlan
	UpdatedCount      int
	ClearedCount      int
	UnchangedCount    int
	MissingTunerCount int
	UnresolvedCount   int
	Warnings          []string
}

type syncLineupPlan struct {
	Result              LineupSyncResult
	ChannelsForLineup   []ChannelMapping
	Patch               map[string]string
	ResolvedByChannelID map[int64]string
}

type syncApplyStats struct {
	Lineups      []LineupSyncResult
	PatchPreview map[string]map[string]string
}

// ChannelMappingUpdate is a write payload for one published channel mapping.
type ChannelMappingUpdate struct {
	DVRLineupID      string `json:"dvr_lineup_id"`
	DVRLineupChannel string `json:"dvr_lineup_channel"`
	DVRStationRef    string `json:"dvr_station_ref"`
	DVRCallsignHint  string `json:"dvr_callsign_hint"`
}

func NewService(store Store, hdhrDeviceID string, client *http.Client) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("dvr store is required")
	}
	return &Service{
		store:                store,
		httpClient:           client,
		hdhrDeviceID:         strings.ToUpper(strings.TrimSpace(hdhrDeviceID)),
		lineupProviderBuild:  newLineupReloadProvider,
		mappingProviderBuild: newMappingProvider,
	}, nil
}

func (s *Service) GetState(ctx context.Context) (ConfigState, error) {
	instance, err := s.store.GetDVRInstance(ctx)
	if err != nil {
		return ConfigState{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	state := ConfigState{
		Instance:        instance,
		CachedLineupsAt: s.cachedLineupsAt,
	}
	if len(s.cachedLineups) > 0 {
		state.CachedLineups = append([]DVRLineup(nil), s.cachedLineups...)
	}
	if s.lastSync != nil {
		copied := cloneSyncResult(*s.lastSync)
		state.LastSync = &copied
	}
	return state, nil
}

func (s *Service) UpdateConfig(ctx context.Context, instance InstanceConfig) (ConfigState, error) {
	current, err := s.store.GetDVRInstance(ctx)
	if err != nil {
		return ConfigState{}, err
	}

	instance = NormalizeInstanceConfig(instance, current)

	if _, err := s.buildLineupReloadProvider(instance); err != nil {
		return ConfigState{}, err
	}
	if _, err := s.buildMappingProvider(instance); err != nil {
		return ConfigState{}, err
	}

	saved, err := s.store.UpsertDVRInstance(ctx, instance)
	if err != nil {
		return ConfigState{}, err
	}

	state, err := s.GetState(ctx)
	if err != nil {
		return ConfigState{}, err
	}
	state.Instance = saved
	return state, nil
}

func (s *Service) ListLineups(ctx context.Context, refresh bool) ([]DVRLineup, error) {
	if !refresh {
		s.mu.RLock()
		if len(s.cachedLineups) > 0 {
			lineups := append([]DVRLineup(nil), s.cachedLineups...)
			s.mu.RUnlock()
			return lineups, nil
		}
		s.mu.RUnlock()
	}

	instance, provider, err := s.lineupProviderForCurrentConfig(ctx)
	if err != nil {
		return nil, err
	}
	_ = instance

	lineups, err := provider.ListLineups(ctx)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.cachedLineups = append([]DVRLineup(nil), lineups...)
	s.cachedLineupsAt = time.Now().UTC().Unix()
	s.mu.Unlock()
	return lineups, nil
}

func (s *Service) TestConnection(ctx context.Context) (TestResult, error) {
	instance, provider, err := s.mappingProviderForCurrentConfig(ctx)
	if err != nil {
		return TestResult{}, err
	}

	deviceChannels, err := provider.ListDeviceChannels(ctx)
	if err != nil {
		return TestResult{}, err
	}

	index := s.buildDeviceChannelIndex(deviceChannels)

	return TestResult{
		Reachable:              true,
		Provider:               string(instance.Provider),
		BaseURL:                instance.BaseURL,
		DeviceChannelCount:     len(deviceChannels),
		FilteredChannelCount:   index.FilteredCount,
		HDHRDeviceID:           s.hdhrDeviceID,
		HDHRDeviceFilterActive: strings.TrimSpace(s.hdhrDeviceID) != "",
	}, nil
}

func (s *Service) ReloadLineup(ctx context.Context) error {
	instance, provider, err := s.lineupProviderForCurrentConfig(ctx)
	if err != nil {
		return err
	}

	return s.reloadLineup(ctx, instance, provider)
}

// ReloadLineupForPlaylistSyncOutcome is the typed variant used by playlist-sync
// orchestration paths and summary rendering.
func (s *Service) ReloadLineupForPlaylistSyncOutcome(ctx context.Context) (ReloadOutcome, error) {
	instance, err := s.store.GetDVRInstance(ctx)
	if err != nil {
		return ReloadOutcome{}, err
	}

	activeProviders := resolvedActiveProviders(instance)
	if len(activeProviders) == 0 {
		return ReloadOutcome{
			Skipped:     true,
			Status:      ReloadStatusSkipped,
			SkipReasons: []string{"no_active_providers"},
		}, nil
	}

	outcome := ReloadOutcome{
		Status:           ReloadStatusUnknown,
		SkippedProviders: make(map[ProviderType]string),
	}
	for _, providerType := range activeProviders {
		configured := instanceForProvider(instance, providerType)
		if providerType == ProviderChannels {
			if reason := channelsReloadSkipReason(configured); reason != "" {
				outcome.SkippedProviders[providerType] = reason
				continue
			}
		}
		if providerType == ProviderJellyfin {
			if reason := jellyfinReloadSkipReason(configured); reason != "" {
				outcome.SkippedProviders[providerType] = reason
				continue
			}
		}

		provider, buildErr := s.buildLineupReloadProvider(configured)
		if buildErr != nil {
			return ReloadOutcome{}, fmt.Errorf("build %s dvr provider: %w", providerType, buildErr)
		}
		if err := s.reloadLineup(ctx, configured, provider); err != nil {
			return ReloadOutcome{}, fmt.Errorf("reload lineup for provider %s: %w", providerType, err)
		}
		outcome.Reloaded = true
		outcome.ReloadedProviders = append(outcome.ReloadedProviders, providerType)
	}

	if len(outcome.SkippedProviders) > 0 {
		outcome.Skipped = true
		outcome.SkipReasons = make([]string, 0, len(outcome.SkippedProviders))
		for _, providerType := range activeProviders {
			reason, ok := outcome.SkippedProviders[providerType]
			if !ok || strings.TrimSpace(reason) == "" {
				continue
			}
			outcome.SkipReasons = append(outcome.SkipReasons, fmt.Sprintf("%s:%s", providerType, reason))
		}
	} else {
		outcome.SkippedProviders = nil
	}

	switch {
	case outcome.Reloaded && outcome.Skipped:
		outcome.Status = ReloadStatusPartial
	case outcome.Skipped:
		outcome.Status = ReloadStatusSkipped
	case outcome.Reloaded:
		outcome.Status = ReloadStatusReloaded
	default:
		outcome.Status = ReloadStatusUnknown
	}

	return outcome, nil
}

func (s *Service) reloadLineup(ctx context.Context, instance InstanceConfig, provider LineupReloadProvider) error {
	if provider == nil {
		return fmt.Errorf("dvr provider is required")
	}

	deviceID := strings.TrimSpace(s.hdhrDeviceID)
	if deviceID == "" {
		return fmt.Errorf("%w: hdhr device id is required for lineup reload", ErrDVRSyncConfig)
	}
	if err := provider.ReloadDeviceLineup(ctx, deviceID); err != nil {
		return fmt.Errorf("reload device lineup %q: %w", deviceID, err)
	}
	if err := provider.RefreshGuideStations(ctx); err != nil {
		return fmt.Errorf("refresh guide stations after lineup reload %q: %w", deviceID, err)
	}
	guideLineupIDs, err := resolveGuideRedownloadLineupIDs(ctx, instance, provider)
	if err != nil {
		return fmt.Errorf("resolve lineup ids for guide redownload: %w", err)
	}
	for _, lineupID := range guideLineupIDs {
		if err := provider.RedownloadGuideLineup(ctx, lineupID); err != nil {
			return fmt.Errorf("redownload guide lineup %q after lineup reload %q: %w", lineupID, deviceID, err)
		}
	}
	return nil
}

func resolveGuideRedownloadLineupIDs(ctx context.Context, instance InstanceConfig, provider LineupReloadProvider) ([]string, error) {
	if provider == nil {
		return nil, fmt.Errorf("dvr provider is required")
	}
	if lineupID := strings.TrimSpace(instance.DefaultLineupID); lineupID != "" {
		return []string{lineupID}, nil
	}

	lineups, err := provider.ListLineups(ctx)
	if err != nil {
		if errors.Is(err, ErrUnsupportedProvider) {
			return nil, nil
		}
		return nil, err
	}

	ids := make([]string, 0, len(lineups))
	seen := make(map[string]struct{}, len(lineups))
	for _, lineup := range lineups {
		lineupID := strings.TrimSpace(lineup.ID)
		if lineupID == "" {
			continue
		}
		if _, exists := seen[lineupID]; exists {
			continue
		}
		seen[lineupID] = struct{}{}
		ids = append(ids, lineupID)
	}
	sort.Strings(ids)
	return ids, nil
}

func jellyfinReloadSkipReason(instance InstanceConfig) string {
	if reason := reloadBaseURLSkipReason(instance.BaseURL, "missing_jellyfin_base_url", "invalid_jellyfin_base_url"); reason != "" {
		return reason
	}
	if strings.TrimSpace(instance.JellyfinAPIToken) == "" {
		return "missing_jellyfin_api_token"
	}
	return ""
}

func channelsReloadSkipReason(instance InstanceConfig) string {
	return reloadBaseURLSkipReason(instance.BaseURL, "missing_channels_base_url", "invalid_channels_base_url")
}

func reloadBaseURLSkipReason(baseURL, missingReason, invalidReason string) string {
	if strings.TrimSpace(baseURL) == "" {
		return missingReason
	}
	if !ProviderBaseURLConfigured(baseURL) {
		return invalidReason
	}
	return ""
}

func (s *Service) ListChannelMappings(ctx context.Context, enabledOnly bool, includeDynamic bool) ([]ChannelMapping, error) {
	instance, err := s.store.GetDVRInstance(ctx)
	if err != nil {
		return nil, err
	}
	return s.store.ListChannelsForDVRSync(ctx, instance.ID, enabledOnly, includeDynamic)
}

func (s *Service) ListChannelMappingsPaged(
	ctx context.Context,
	enabledOnly bool,
	includeDynamic bool,
	limit int,
	offset int,
) ([]ChannelMapping, int, error) {
	instance, err := s.store.GetDVRInstance(ctx)
	if err != nil {
		return nil, 0, err
	}
	return s.store.ListChannelsForDVRSyncPaged(ctx, instance.ID, enabledOnly, includeDynamic, limit, offset)
}

func (s *Service) GetChannelMapping(ctx context.Context, channelID int64) (ChannelMapping, error) {
	instance, err := s.store.GetDVRInstance(ctx)
	if err != nil {
		return ChannelMapping{}, err
	}
	return s.store.GetChannelDVRMapping(ctx, instance.ID, channelID)
}

func (s *Service) UpdateChannelMapping(
	ctx context.Context,
	channelID int64,
	update ChannelMappingUpdate,
) (ChannelMapping, error) {
	instance, err := s.store.GetDVRInstance(ctx)
	if err != nil {
		return ChannelMapping{}, err
	}
	existing, err := s.store.GetChannelDVRMapping(ctx, instance.ID, channelID)
	if err != nil {
		return ChannelMapping{}, err
	}

	lineupID := strings.TrimSpace(update.DVRLineupID)
	lineupChannel := strings.TrimSpace(update.DVRLineupChannel)
	stationRef := strings.TrimSpace(update.DVRStationRef)
	callsignHint := strings.TrimSpace(update.DVRCallsignHint)

	if lineupID == "" {
		lineupID = strings.TrimSpace(instance.DefaultLineupID)
	}
	if lineupChannel == "" && stationRef == "" {
		if err := s.store.DeleteChannelDVRMapping(ctx, instance.ID, channelID); err != nil {
			return ChannelMapping{}, err
		}
		return s.store.GetChannelDVRMapping(ctx, instance.ID, channelID)
	}
	if lineupID == "" {
		return ChannelMapping{}, fmt.Errorf("%w: dvr_lineup_id is required when default_lineup_id is unset", ErrDVRSyncConfig)
	}
	if stationRef != "" &&
		mappingIdentityChanged(existing, lineupID, lineupChannel, callsignHint) &&
		strings.EqualFold(stationRef, strings.TrimSpace(existing.DVRStationRef)) {
		// Station ref is a cache. If identity fields changed but caller echoed old cached ref,
		// clear it so sync resolves from the updated lineup/channel configuration.
		stationRef = ""
	}

	return s.store.UpsertChannelDVRMapping(ctx, ChannelMapping{
		ChannelID:        channelID,
		DVRInstanceID:    instance.ID,
		DVRLineupID:      lineupID,
		DVRLineupChannel: lineupChannel,
		DVRStationRef:    stationRef,
		DVRCallsignHint:  callsignHint,
	})
}

func (s *Service) ReverseSync(ctx context.Context, req ReverseSyncRequest) (ReverseSyncResult, error) {
	if !s.syncRunLock.TryLock() {
		return ReverseSyncResult{}, ErrSyncAlreadyRunning
	}
	defer s.syncRunLock.Unlock()

	instance, provider, err := s.mappingProviderForCurrentConfig(ctx)
	if err != nil {
		return ReverseSyncResult{}, err
	}

	channels, err := s.store.ListChannelsForDVRSync(ctx, instance.ID, true, req.IncludeDynamic)
	if err != nil {
		return ReverseSyncResult{}, err
	}

	lineupID := strings.TrimSpace(req.LineupID)
	if lineupID == "" {
		lineupID = strings.TrimSpace(instance.DefaultLineupID)
	}
	if lineupID == "" {
		inferred, candidates := inferLineupIDFromMappings(channels)
		if inferred != "" {
			lineupID = inferred
		} else if len(candidates) > 1 {
			return ReverseSyncResult{}, fmt.Errorf(
				"%w: lineup_id is required when default_lineup_id is unset (multiple lineup ids present: %s)",
				ErrDVRSyncConfig,
				strings.Join(candidates, ", "),
			)
		}
	}
	if lineupID == "" {
		return ReverseSyncResult{}, fmt.Errorf("%w: lineup_id is required when default_lineup_id is unset", ErrDVRSyncConfig)
	}

	return s.reverseSyncMappings(ctx, instance, provider, lineupID, channels, req.DryRun)
}

func (s *Service) ReverseSyncChannel(
	ctx context.Context,
	channelID int64,
	req ReverseSyncRequest,
) (ReverseSyncResult, error) {
	if !s.syncRunLock.TryLock() {
		return ReverseSyncResult{}, ErrSyncAlreadyRunning
	}
	defer s.syncRunLock.Unlock()

	instance, provider, err := s.mappingProviderForCurrentConfig(ctx)
	if err != nil {
		return ReverseSyncResult{}, err
	}

	mapping, err := s.store.GetChannelDVRMapping(ctx, instance.ID, channelID)
	if err != nil {
		return ReverseSyncResult{}, err
	}

	lineupID := strings.TrimSpace(req.LineupID)
	if lineupID == "" {
		lineupID = strings.TrimSpace(mapping.DVRLineupID)
	}
	if lineupID == "" {
		lineupID = strings.TrimSpace(instance.DefaultLineupID)
	}
	if lineupID == "" {
		channels, err := s.store.ListChannelsForDVRSync(ctx, instance.ID, true, req.IncludeDynamic)
		if err != nil {
			return ReverseSyncResult{}, err
		}
		inferred, candidates := inferLineupIDFromMappings(channels)
		if inferred != "" {
			lineupID = inferred
		} else if len(candidates) > 1 {
			return ReverseSyncResult{}, fmt.Errorf(
				"%w: lineup_id is required when channel and default lineup ids are unset (multiple lineup ids present: %s)",
				ErrDVRSyncConfig,
				strings.Join(candidates, ", "),
			)
		}
	}
	if lineupID == "" {
		return ReverseSyncResult{}, fmt.Errorf("%w: lineup_id is required when channel and default lineup ids are unset", ErrDVRSyncConfig)
	}

	return s.reverseSyncMappings(ctx, instance, provider, lineupID, []ChannelMapping{mapping}, req.DryRun)
}

func (s *Service) reverseSyncMappings(
	ctx context.Context,
	instance InstanceConfig,
	provider MappingProvider,
	lineupID string,
	channels []ChannelMapping,
	dryRun bool,
) (ReverseSyncResult, error) {
	start := time.Now().UTC()
	lineupID = strings.TrimSpace(lineupID)

	result := ReverseSyncResult{
		StartedAt:    start.Unix(),
		DryRun:       dryRun,
		Provider:     string(instance.Provider),
		BaseURL:      instance.BaseURL,
		HDHRDeviceID: s.hdhrDeviceID,
		LineupID:     lineupID,
	}

	deviceChannels, err := provider.ListDeviceChannels(ctx)
	if err != nil {
		return ReverseSyncResult{}, err
	}
	result.DeviceChannelCount = len(deviceChannels)

	index := s.buildDeviceChannelIndex(deviceChannels)
	result.FilteredChannelCount = index.FilteredCount
	for _, warning := range index.Warnings {
		appendWarning(&result.Warnings, warning)
	}
	tunerToDeviceKeys := index.TunerToDeviceKeys

	if len(index.DeviceKeys) == 0 {
		return ReverseSyncResult{}, fmt.Errorf("%w: no provider device channels found for hdhr device id %q", ErrDVRSyncConfig, s.hdhrDeviceID)
	}

	stations, err := provider.ListLineupStations(ctx, lineupID)
	if err != nil {
		return ReverseSyncResult{}, fmt.Errorf("fetch lineup stations %q: %w", lineupID, err)
	}
	stationByRef := make(map[string]DVRStation, len(stations))
	for _, station := range stations {
		stationRef := strings.TrimSpace(station.StationRef)
		if stationRef == "" {
			continue
		}
		stationByRef[stationRef] = station
	}

	customByDevice, err := provider.GetCustomMapping(ctx, lineupID)
	if err != nil {
		return ReverseSyncResult{}, fmt.Errorf("fetch lineup custom mapping %q: %w", lineupID, err)
	}

	for _, mapping := range channels {
		result.CandidateCount++

		deviceKeysForTuner := tunerToDeviceKeys[strings.TrimSpace(mapping.GuideNumber)]
		if len(deviceKeysForTuner) == 0 {
			result.MissingTunerCount++
			appendWarning(&result.Warnings, fmt.Sprintf("missing provider tuner channel for guide_number=%s channel=%s", mapping.GuideNumber, mapping.GuideName))
			continue
		}

		deviceKey := deviceKeysForTuner[0]
		stationRef := strings.TrimSpace(customByDevice[deviceKey])
		if stationRef == "" {
			result.MissingMappingCount++
			appendWarning(&result.Warnings, fmt.Sprintf("no provider mapping for guide_number=%s channel=%s device_key=%s lineup=%s", mapping.GuideNumber, mapping.GuideName, deviceKey, lineupID))
			continue
		}

		station, ok := stationByRef[stationRef]
		if !ok {
			result.MissingStationRefCount++
			appendWarning(&result.Warnings, fmt.Sprintf("provider station_ref=%s not found in lineup=%s for guide_number=%s", stationRef, lineupID, mapping.GuideNumber))
			continue
		}

		lineupChannel := strings.TrimSpace(station.LineupChannel)
		if lineupChannel == "" {
			lineupChannel = strings.TrimSpace(mapping.DVRLineupChannel)
			appendWarning(
				&result.Warnings,
				fmt.Sprintf("lineup=%s station_ref=%s has empty lineup channel; using station_ref-only mapping", lineupID, stationRef),
			)
		}

		callsignHint := strings.TrimSpace(mapping.DVRCallsignHint)
		if callsignHint == "" {
			callsignHint = strings.TrimSpace(station.CallSign)
		}

		next := ChannelMapping{
			ChannelID:        mapping.ChannelID,
			DVRInstanceID:    instance.ID,
			DVRLineupID:      lineupID,
			DVRLineupChannel: lineupChannel,
			DVRStationRef:    stationRef,
			DVRCallsignHint:  callsignHint,
		}

		if strings.TrimSpace(mapping.DVRLineupID) == next.DVRLineupID &&
			strings.TrimSpace(mapping.DVRLineupChannel) == next.DVRLineupChannel &&
			strings.TrimSpace(mapping.DVRStationRef) == next.DVRStationRef &&
			strings.TrimSpace(mapping.DVRCallsignHint) == next.DVRCallsignHint {
			result.UnchangedCount++
			continue
		}

		result.ImportedCount++
		if dryRun {
			continue
		}

		if _, err := s.store.UpsertChannelDVRMapping(ctx, next); err != nil {
			return ReverseSyncResult{}, fmt.Errorf("persist reverse mapping for channel_id=%d lineup=%s: %w", mapping.ChannelID, lineupID, err)
		}
	}

	result.FinishedAt = time.Now().UTC().Unix()
	result.DurationMS = time.Since(start).Milliseconds()
	return result, nil
}

func (s *Service) Sync(ctx context.Context, req SyncRequest) (SyncResult, error) {
	if !s.syncRunLock.TryLock() {
		return SyncResult{}, ErrSyncAlreadyRunning
	}
	defer s.syncRunLock.Unlock()

	start := time.Now().UTC()
	instance, mappingProvider, err := s.mappingProviderForCurrentConfig(ctx)
	if err != nil {
		return SyncResult{}, err
	}
	lineupProvider, err := s.buildLineupReloadProvider(instance)
	if err != nil {
		return SyncResult{}, err
	}

	result := SyncResult{
		StartedAt:    start.Unix(),
		DryRun:       req.DryRun,
		Provider:     string(instance.Provider),
		BaseURL:      instance.BaseURL,
		HDHRDeviceID: s.hdhrDeviceID,
		SyncMode:     normalizeSyncMode(instance.SyncMode),
	}
	if result.SyncMode == "" {
		result.SyncMode = SyncModeConfiguredOnly
	}

	deviceID := strings.TrimSpace(s.hdhrDeviceID)
	if deviceID == "" {
		return SyncResult{}, fmt.Errorf("%w: hdhr device id is required for lineup reload preflight", ErrDVRSyncConfig)
	}
	if err := lineupProvider.ReloadDeviceLineup(ctx, deviceID); err != nil {
		return SyncResult{}, fmt.Errorf("reload device lineup %q: %w", deviceID, err)
	}

	if instance.PreSyncRefreshDevices {
		if err := mappingProvider.RefreshDevices(ctx); err != nil {
			appendWarning(&result.Warnings, fmt.Sprintf("pre-sync refresh failed: %v", err))
		}
	}

	deviceChannels, err := mappingProvider.ListDeviceChannels(ctx)
	if err != nil {
		return SyncResult{}, err
	}
	result.DeviceChannelCount = len(deviceChannels)

	index := s.buildDeviceChannelIndex(deviceChannels)
	deviceKeys := index.DeviceKeys
	result.FilteredChannelCount = index.FilteredCount
	for _, warning := range index.Warnings {
		appendWarning(&result.Warnings, warning)
	}

	if len(deviceKeys) == 0 {
		return SyncResult{}, fmt.Errorf("%w: no provider device channels found for hdhr device id %q", ErrDVRSyncConfig, s.hdhrDeviceID)
	}

	plan, err := s.buildSyncPlan(ctx, instance, mappingProvider, req, index, result.SyncMode)
	if err != nil {
		return SyncResult{}, err
	}
	result.UpdatedCount = plan.UpdatedCount
	result.ClearedCount = plan.ClearedCount
	result.UnchangedCount = plan.UnchangedCount
	result.MissingTunerCount = plan.MissingTunerCount
	result.UnresolvedCount = plan.UnresolvedCount
	for _, warning := range plan.Warnings {
		appendWarning(&result.Warnings, warning)
	}

	applied, err := s.applySyncPlan(ctx, instance.ID, mappingProvider, plan, req.DryRun)
	if err != nil {
		return SyncResult{}, err
	}
	result.Lineups = applied.Lineups
	result.PatchPreview = applied.PatchPreview

	result.FinishedAt = time.Now().UTC().Unix()
	result.DurationMS = time.Since(start).Milliseconds()
	s.setLastSync(result)
	return result, nil
}

func (s *Service) buildSyncPlan(
	ctx context.Context,
	instance InstanceConfig,
	provider MappingProvider,
	req SyncRequest,
	index deviceChannelIndex,
	syncMode SyncMode,
) (syncPlan, error) {
	plan := syncPlan{}
	channels, err := s.store.ListChannelsForDVRSync(ctx, instance.ID, true, req.IncludeDynamic)
	if err != nil {
		return syncPlan{}, err
	}

	groupedByLineup := map[string][]ChannelMapping{}
	for _, mapping := range channels {
		lineupID := strings.TrimSpace(mapping.DVRLineupID)
		if lineupID == "" {
			lineupID = strings.TrimSpace(instance.DefaultLineupID)
		}

		hasLineupChannel := strings.TrimSpace(mapping.DVRLineupChannel) != ""
		hasStationRef := strings.TrimSpace(mapping.DVRStationRef) != ""
		if !hasLineupChannel && !hasStationRef {
			continue
		}
		if lineupID == "" {
			plan.UnresolvedCount++
			appendWarning(
				&plan.Warnings,
				fmt.Sprintf(
					"channel %s (%s) has dvr mapping (lineup_channel=%q station_ref=%q) but no lineup id/default_lineup_id",
					mapping.GuideNumber,
					mapping.GuideName,
					mapping.DVRLineupChannel,
					mapping.DVRStationRef,
				),
			)
			continue
		}

		mapping.DVRLineupID = lineupID
		groupedByLineup[lineupID] = append(groupedByLineup[lineupID], mapping)
	}

	lineupIDs := make([]string, 0, len(groupedByLineup))
	for lineupID := range groupedByLineup {
		lineupIDs = append(lineupIDs, lineupID)
	}
	sort.Strings(lineupIDs)

	for _, lineupID := range lineupIDs {
		channelsForLineup := groupedByLineup[lineupID]
		lineupPlan := syncLineupPlan{
			Result: LineupSyncResult{
				LineupID:           lineupID,
				ConfiguredChannels: len(channelsForLineup),
			},
			ChannelsForLineup:   channelsForLineup,
			Patch:               map[string]string{},
			ResolvedByChannelID: map[int64]string{},
		}

		stations, err := provider.ListLineupStations(ctx, lineupID)
		if err != nil {
			return syncPlan{}, fmt.Errorf("fetch lineup stations %q: %w", lineupID, err)
		}
		currentMapping, err := provider.GetCustomMapping(ctx, lineupID)
		if err != nil {
			return syncPlan{}, fmt.Errorf("fetch lineup custom mapping %q: %w", lineupID, err)
		}

		stationByRef, stationByLineupChannel := buildStationIndex(stations)
		desiredByDevice := map[string]string{}
		for _, mapping := range channelsForLineup {
			deviceKeysForTuner := index.TunerToDeviceKeys[strings.TrimSpace(mapping.GuideNumber)]
			if len(deviceKeysForTuner) == 0 {
				plan.MissingTunerCount++
				appendWarning(&plan.Warnings, fmt.Sprintf("missing provider tuner channel for guide_number=%s channel=%s", mapping.GuideNumber, mapping.GuideName))
				continue
			}

			stationRef, warning, ok := resolveStationRef(mapping, stationByRef, stationByLineupChannel)
			if warning != "" {
				appendWarning(&plan.Warnings, warning)
			}
			if !ok {
				plan.UnresolvedCount++
				continue
			}

			deviceKey := deviceKeysForTuner[0]
			desiredByDevice[deviceKey] = stationRef
			lineupPlan.ResolvedByChannelID[mapping.ChannelID] = stationRef
			lineupPlan.Result.ResolvedChannels++
		}

		if syncMode == SyncModeMirrorDevice {
			for _, deviceKey := range index.DeviceKeys {
				desired := strings.TrimSpace(desiredByDevice[deviceKey])
				current := strings.TrimSpace(currentMapping[deviceKey])
				if current == desired {
					lineupPlan.Result.UnchangedCount++
					plan.UnchangedCount++
					continue
				}
				lineupPlan.Patch[deviceKey] = desired
				if desired == "" {
					lineupPlan.Result.ClearedCount++
					plan.ClearedCount++
				} else {
					lineupPlan.Result.UpdatedCount++
					plan.UpdatedCount++
				}
			}
		} else {
			deviceKeysToUpdate := make([]string, 0, len(desiredByDevice))
			for deviceKey := range desiredByDevice {
				deviceKeysToUpdate = append(deviceKeysToUpdate, deviceKey)
			}
			sort.Strings(deviceKeysToUpdate)
			for _, deviceKey := range deviceKeysToUpdate {
				desired := strings.TrimSpace(desiredByDevice[deviceKey])
				current := strings.TrimSpace(currentMapping[deviceKey])
				if current == desired {
					lineupPlan.Result.UnchangedCount++
					plan.UnchangedCount++
					continue
				}
				lineupPlan.Patch[deviceKey] = desired
				lineupPlan.Result.UpdatedCount++
				plan.UpdatedCount++
			}
		}

		if len(lineupPlan.Patch) == 0 {
			lineupPlan.Patch = nil
		}
		if len(lineupPlan.ResolvedByChannelID) == 0 {
			lineupPlan.ResolvedByChannelID = nil
		}
		plan.Lineups = append(plan.Lineups, lineupPlan)
	}

	return plan, nil
}

func (s *Service) applySyncPlan(
	ctx context.Context,
	instanceID int64,
	provider MappingProvider,
	plan syncPlan,
	dryRun bool,
) (syncApplyStats, error) {
	applied := syncApplyStats{
		Lineups:      make([]LineupSyncResult, 0, len(plan.Lineups)),
		PatchPreview: map[string]map[string]string{},
	}

	for _, lineupPlan := range plan.Lineups {
		lineupResult := lineupPlan.Result
		if len(lineupPlan.Patch) > 0 {
			applied.PatchPreview[lineupResult.LineupID] = sortedPatchCopy(lineupPlan.Patch)
			if !dryRun {
				for _, chunk := range chunkPatch(lineupPlan.Patch, syncBatchSize) {
					if err := provider.PutCustomMapping(ctx, lineupResult.LineupID, chunk); err != nil {
						return syncApplyStats{}, fmt.Errorf("apply lineup patch %q: %w", lineupResult.LineupID, err)
					}
					lineupResult.AppliedCount += len(chunk)
				}
			}
		}

		if !dryRun && len(lineupPlan.ResolvedByChannelID) > 0 {
			for _, mapping := range lineupPlan.ChannelsForLineup {
				resolvedRef := strings.TrimSpace(lineupPlan.ResolvedByChannelID[mapping.ChannelID])
				if resolvedRef == "" {
					continue
				}
				if strings.TrimSpace(mapping.DVRStationRef) == resolvedRef {
					continue
				}

				if _, err := s.store.UpsertChannelDVRMapping(ctx, ChannelMapping{
					ChannelID:        mapping.ChannelID,
					DVRInstanceID:    instanceID,
					DVRLineupID:      strings.TrimSpace(mapping.DVRLineupID),
					DVRLineupChannel: strings.TrimSpace(mapping.DVRLineupChannel),
					DVRStationRef:    resolvedRef,
					DVRCallsignHint:  strings.TrimSpace(mapping.DVRCallsignHint),
				}); err != nil {
					return syncApplyStats{}, fmt.Errorf("persist resolved station ref for channel_id=%d lineup=%s: %w", mapping.ChannelID, lineupResult.LineupID, err)
				}
			}
		}

		applied.Lineups = append(applied.Lineups, lineupResult)
	}

	if len(applied.PatchPreview) == 0 {
		applied.PatchPreview = nil
	}
	return applied, nil
}

func (s *Service) buildDeviceChannelIndex(deviceChannels map[string]DVRDeviceChannel) deviceChannelIndex {
	index := deviceChannelIndex{TunerToDeviceKeys: map[string][]string{}}
	deviceKeySet := map[string]struct{}{}

	for key, channel := range deviceChannels {
		if !s.matchDeviceID(channel.DeviceID) {
			continue
		}

		deviceKey := strings.TrimSpace(key)
		if deviceKey == "" {
			deviceKey = strings.TrimSpace(channel.Key)
		}
		if deviceKey == "" {
			continue
		}

		tunerNumber := strings.TrimSpace(channel.Number)
		if tunerNumber == "" {
			tunerNumber = deviceKey
		}

		deviceKeySet[deviceKey] = struct{}{}
		index.TunerToDeviceKeys[tunerNumber] = append(index.TunerToDeviceKeys[tunerNumber], deviceKey)
	}

	index.DeviceKeys = sortedKeys(deviceKeySet)
	index.FilteredCount = len(index.DeviceKeys)

	tunerNumbers := make([]string, 0, len(index.TunerToDeviceKeys))
	for tunerNumber := range index.TunerToDeviceKeys {
		tunerNumbers = append(tunerNumbers, tunerNumber)
	}
	sort.Strings(tunerNumbers)
	for _, tunerNumber := range tunerNumbers {
		keys := index.TunerToDeviceKeys[tunerNumber]
		sort.Strings(keys)
		index.TunerToDeviceKeys[tunerNumber] = keys
		if len(keys) > 1 {
			index.Warnings = append(index.Warnings, fmt.Sprintf("multiple device keys for tuner %s; using %s", tunerNumber, keys[0]))
		}
	}

	return index
}

func buildStationIndex(stations []DVRStation) (map[string]DVRStation, map[string][]DVRStation) {
	stationByRef := make(map[string]DVRStation, len(stations))
	stationByLineupChannel := make(map[string][]DVRStation)
	for _, station := range stations {
		stationRef := strings.TrimSpace(station.StationRef)
		if stationRef == "" {
			continue
		}
		stationByRef[stationRef] = station

		lineupChannel := strings.TrimSpace(station.LineupChannel)
		if lineupChannel == "" {
			continue
		}
		stationByLineupChannel[lineupChannel] = append(stationByLineupChannel[lineupChannel], station)
	}
	return stationByRef, stationByLineupChannel
}

func resolveStationRef(
	mapping ChannelMapping,
	stationByRef map[string]DVRStation,
	stationByLineupChannel map[string][]DVRStation,
) (stationRef string, warning string, ok bool) {
	cachedRef := strings.TrimSpace(mapping.DVRStationRef)
	lineupChannel := strings.TrimSpace(mapping.DVRLineupChannel)
	cachedMismatchWarning := ""
	if cachedRef != "" {
		if station, exists := stationByRef[cachedRef]; exists {
			if lineupChannel == "" {
				return cachedRef, "", true
			}

			cachedLineupChannel := strings.TrimSpace(station.LineupChannel)
			if cachedLineupChannel == "" || cachedLineupChannel == lineupChannel {
				return cachedRef, "", true
			}
			cachedMismatchWarning = fmt.Sprintf(
				"cached station_ref=%s points to lineup_channel=%s but configured lineup_channel=%s for guide_number=%s; resolving by lineup_channel",
				cachedRef,
				cachedLineupChannel,
				lineupChannel,
				mapping.GuideNumber,
			)
		}
	}

	if lineupChannel == "" {
		return "", joinWarnings(cachedMismatchWarning, fmt.Sprintf("channel %s (%s) has empty dvr_lineup_channel", mapping.GuideNumber, mapping.GuideName)), false
	}
	candidates := stationByLineupChannel[lineupChannel]
	if len(candidates) == 0 {
		return "", joinWarnings(cachedMismatchWarning, fmt.Sprintf("no station in lineup %s for channel %s (%s) lineup_channel=%s", mapping.DVRLineupID, mapping.GuideNumber, mapping.GuideName, lineupChannel)), false
	}
	if len(candidates) == 1 {
		return strings.TrimSpace(candidates[0].StationRef), cachedMismatchWarning, true
	}

	callsignHint := strings.TrimSpace(mapping.DVRCallsignHint)
	if callsignHint != "" {
		for _, candidate := range candidates {
			if strings.EqualFold(strings.TrimSpace(candidate.CallSign), callsignHint) {
				return strings.TrimSpace(candidate.StationRef), cachedMismatchWarning, true
			}
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		return strings.TrimSpace(candidates[i].StationRef) < strings.TrimSpace(candidates[j].StationRef)
	})
	return strings.TrimSpace(candidates[0].StationRef),
		joinWarnings(
			cachedMismatchWarning,
			fmt.Sprintf("ambiguous lineup match for guide_number=%s lineup_channel=%s (using first station_ref=%s)", mapping.GuideNumber, lineupChannel, strings.TrimSpace(candidates[0].StationRef)),
		),
		true
}

func mappingIdentityChanged(existing ChannelMapping, lineupID, lineupChannel, callsignHint string) bool {
	return !strings.EqualFold(strings.TrimSpace(existing.DVRLineupID), strings.TrimSpace(lineupID)) ||
		strings.TrimSpace(existing.DVRLineupChannel) != strings.TrimSpace(lineupChannel) ||
		!strings.EqualFold(strings.TrimSpace(existing.DVRCallsignHint), strings.TrimSpace(callsignHint))
}

func chunkPatch(patch map[string]string, chunkSize int) []map[string]string {
	if len(patch) == 0 {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = len(patch)
	}

	keys := make([]string, 0, len(patch))
	for key := range patch {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	chunks := make([]map[string]string, 0, (len(keys)+chunkSize-1)/chunkSize)
	for i := 0; i < len(keys); i += chunkSize {
		end := i + chunkSize
		if end > len(keys) {
			end = len(keys)
		}
		part := make(map[string]string, end-i)
		for _, key := range keys[i:end] {
			part[key] = patch[key]
		}
		chunks = append(chunks, part)
	}
	return chunks
}

func sortedPatchCopy(patch map[string]string) map[string]string {
	keys := make([]string, 0, len(patch))
	for key := range patch {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make(map[string]string, len(patch))
	for _, key := range keys {
		out[key] = patch[key]
	}
	return out
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func appendWarning(warnings *[]string, message string) {
	message = strings.TrimSpace(message)
	if message == "" || warnings == nil {
		return
	}
	if len(*warnings) >= maxSyncWarnings {
		return
	}
	*warnings = append(*warnings, message)
}

func joinWarnings(messages ...string) string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		trimmed := strings.TrimSpace(message)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return strings.Join(out, "; ")
}

func inferLineupIDFromMappings(channels []ChannelMapping) (lineupID string, candidates []string) {
	seen := map[string]struct{}{}
	for _, mapping := range channels {
		id := strings.TrimSpace(mapping.DVRLineupID)
		if id == "" {
			continue
		}
		seen[id] = struct{}{}
	}

	if len(seen) == 0 {
		return "", nil
	}

	candidates = make([]string, 0, len(seen))
	for id := range seen {
		candidates = append(candidates, id)
	}
	sort.Strings(candidates)
	if len(candidates) == 1 {
		return candidates[0], candidates
	}
	return "", candidates
}

func (s *Service) matchDeviceID(deviceID string) bool {
	if strings.TrimSpace(s.hdhrDeviceID) == "" {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(deviceID), strings.TrimSpace(s.hdhrDeviceID))
}

func (s *Service) currentProviderConfig(ctx context.Context) (InstanceConfig, error) {
	instance, err := s.store.GetDVRInstance(ctx)
	if err != nil {
		return InstanceConfig{}, err
	}
	return instanceForProvider(instance, instance.Provider), nil
}

func (s *Service) buildLineupReloadProvider(instance InstanceConfig) (LineupReloadProvider, error) {
	if s.lineupProviderBuild == nil {
		return nil, fmt.Errorf("lineup provider factory is not configured")
	}
	provider, err := s.lineupProviderBuild(instance, s.httpClient)
	if err != nil {
		return nil, err
	}
	return provider, nil
}

func (s *Service) buildMappingProvider(instance InstanceConfig) (MappingProvider, error) {
	if s.mappingProviderBuild == nil {
		return nil, fmt.Errorf("mapping provider factory is not configured")
	}
	provider, err := s.mappingProviderBuild(instance, s.httpClient)
	if err != nil {
		return nil, err
	}
	return provider, nil
}

func (s *Service) lineupProviderForCurrentConfig(ctx context.Context) (InstanceConfig, LineupReloadProvider, error) {
	instance, err := s.currentProviderConfig(ctx)
	if err != nil {
		return InstanceConfig{}, nil, err
	}
	provider, err := s.buildLineupReloadProvider(instance)
	if err != nil {
		return InstanceConfig{}, nil, err
	}
	return instance, provider, nil
}

func (s *Service) mappingProviderForCurrentConfig(ctx context.Context) (InstanceConfig, MappingProvider, error) {
	instance, err := s.currentProviderConfig(ctx)
	if err != nil {
		return InstanceConfig{}, nil, err
	}
	provider, err := s.buildMappingProvider(instance)
	if err != nil {
		return InstanceConfig{}, nil, err
	}
	return instance, provider, nil
}

func (s *Service) setLastSync(result SyncResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := cloneSyncResult(result)
	s.lastSync = &copied
}

func cloneSyncResult(result SyncResult) SyncResult {
	copied := result
	if result.Lineups != nil {
		copied.Lineups = append([]LineupSyncResult(nil), result.Lineups...)
	}
	if result.Warnings != nil {
		copied.Warnings = append([]string(nil), result.Warnings...)
	}
	if result.PatchPreview != nil {
		copied.PatchPreview = make(map[string]map[string]string, len(result.PatchPreview))
		for lineupID, patch := range result.PatchPreview {
			copied.PatchPreview[lineupID] = cloneStringMap(patch)
		}
	}
	return copied
}

func cloneStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	out := make(map[string]string, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}
