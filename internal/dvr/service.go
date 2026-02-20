package dvr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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

type providerFactory func(instance InstanceConfig, client *http.Client) (DVRProvider, error)

// Service coordinates provider access, sync operations, and mapping persistence.
type Service struct {
	store         Store
	httpClient    *http.Client
	hdhrDeviceID  string
	providerBuild providerFactory
	syncRunLock   sync.Mutex

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
		store:         store,
		httpClient:    client,
		hdhrDeviceID:  strings.ToUpper(strings.TrimSpace(hdhrDeviceID)),
		providerBuild: newProvider,
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

	instance.ID = current.ID
	instance.Provider = normalizeProviderType(instance.Provider)
	if instance.Provider == "" {
		instance.Provider = ProviderChannels
	}
	instance.BaseURL = strings.TrimSpace(instance.BaseURL)
	instance.ChannelsBaseURL = strings.TrimSpace(instance.ChannelsBaseURL)
	instance.JellyfinBaseURL = strings.TrimSpace(instance.JellyfinBaseURL)
	if instance.ChannelsBaseURL == "" {
		instance.ChannelsBaseURL = strings.TrimSpace(current.ChannelsBaseURL)
	}
	if instance.JellyfinBaseURL == "" {
		instance.JellyfinBaseURL = strings.TrimSpace(current.JellyfinBaseURL)
	}
	if instance.BaseURL != "" {
		switch instance.Provider {
		case ProviderJellyfin:
			instance.JellyfinBaseURL = instance.BaseURL
		case ProviderChannels:
			instance.ChannelsBaseURL = instance.BaseURL
		}
	}
	if strings.TrimSpace(instance.ChannelsBaseURL) == "" {
		instance.ChannelsBaseURL = providerBaseURL(current, ProviderChannels)
	}
	if strings.TrimSpace(instance.ChannelsBaseURL) == "" {
		instance.ChannelsBaseURL = defaultChannelsBaseURL
	}
	if strings.TrimSpace(instance.JellyfinBaseURL) == "" {
		instance.JellyfinBaseURL = providerBaseURL(current, ProviderJellyfin)
	}
	instance.ActiveProviders = normalizeActiveProviders(instance.Provider, instance.ActiveProviders)
	instance.DefaultLineupID = strings.TrimSpace(instance.DefaultLineupID)
	instance.SyncCron = strings.TrimSpace(instance.SyncCron)
	if instance.SyncEnabled && instance.SyncCron == "" {
		instance.SyncCron = defaultSyncCron
	}
	instance.SyncMode = normalizeSyncMode(instance.SyncMode)
	if instance.SyncMode == "" {
		instance.SyncMode = SyncModeConfiguredOnly
	}
	instance.JellyfinAPIToken = strings.TrimSpace(instance.JellyfinAPIToken)
	instance.JellyfinTunerHostID = strings.TrimSpace(instance.JellyfinTunerHostID)
	instance.JellyfinAPITokenConfigured = false
	instance = instanceForProvider(instance, instance.Provider)

	if _, err := newProvider(instance, s.httpClient); err != nil {
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

	instance, provider, err := s.providerForCurrentConfig(ctx)
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
	instance, provider, err := s.providerForCurrentConfig(ctx)
	if err != nil {
		return TestResult{}, err
	}

	deviceChannels, err := provider.ListDeviceChannels(ctx)
	if err != nil {
		return TestResult{}, err
	}

	filteredCount := 0
	for _, channel := range deviceChannels {
		if s.matchDeviceID(channel.DeviceID) {
			filteredCount++
		}
	}

	return TestResult{
		Reachable:              true,
		Provider:               string(instance.Provider),
		BaseURL:                instance.BaseURL,
		DeviceChannelCount:     len(deviceChannels),
		FilteredChannelCount:   filteredCount,
		HDHRDeviceID:           s.hdhrDeviceID,
		HDHRDeviceFilterActive: strings.TrimSpace(s.hdhrDeviceID) != "",
	}, nil
}

func (s *Service) ReloadLineup(ctx context.Context) error {
	instance, provider, err := s.providerForCurrentConfig(ctx)
	if err != nil {
		return err
	}

	return s.reloadLineup(ctx, instance, provider)
}

// ReloadLineupForPlaylistSync performs playlist-sync-triggered lineup refresh
// with provider-aware gating and optional multi-provider fan-out. Missing
// Jellyfin credentials are treated as explicit non-fatal skips.
func (s *Service) ReloadLineupForPlaylistSync(ctx context.Context) (bool, bool, string, error) {
	instance, err := s.store.GetDVRInstance(ctx)
	if err != nil {
		return false, false, "", err
	}

	activeProviders := normalizeActiveProviders(instance.Provider, instance.ActiveProviders)
	skippedReasons := make([]string, 0, len(activeProviders))
	reloadedAny := false
	for _, providerType := range activeProviders {
		configured := instanceForProvider(instance, providerType)
		if providerType == ProviderJellyfin {
			if reason := jellyfinReloadSkipReason(configured); reason != "" {
				skippedReasons = append(skippedReasons, fmt.Sprintf("%s:%s", providerType, reason))
				continue
			}
		}
		provider, buildErr := s.providerBuild(configured, s.httpClient)
		if buildErr != nil {
			return false, false, "", fmt.Errorf("build %s dvr provider: %w", providerType, buildErr)
		}
		if err := s.reloadLineup(ctx, configured, provider); err != nil {
			return false, false, "", fmt.Errorf("reload lineup for provider %s: %w", providerType, err)
		}
		reloadedAny = true
	}

	if len(skippedReasons) == 0 {
		return reloadedAny, false, "", nil
	}
	return reloadedAny, true, strings.Join(skippedReasons, ","), nil
}

func (s *Service) reloadLineup(ctx context.Context, instance InstanceConfig, provider DVRProvider) error {
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

func resolveGuideRedownloadLineupIDs(ctx context.Context, instance InstanceConfig, provider DVRProvider) ([]string, error) {
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
	if strings.TrimSpace(instance.BaseURL) == "" {
		return "missing_jellyfin_base_url"
	}
	parsedBase, err := url.ParseRequestURI(strings.TrimSpace(instance.BaseURL))
	if err != nil || parsedBase.Scheme == "" || parsedBase.Host == "" {
		return "invalid_jellyfin_base_url"
	}
	if strings.TrimSpace(instance.JellyfinAPIToken) == "" {
		return "missing_jellyfin_api_token"
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

	instance, provider, err := s.providerForCurrentConfig(ctx)
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

	instance, provider, err := s.providerForCurrentConfig(ctx)
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
	provider DVRProvider,
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

	tunerToDeviceKeys := map[string][]string{}
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
		tunerToDeviceKeys[tunerNumber] = append(tunerToDeviceKeys[tunerNumber], deviceKey)
	}

	deviceKeys := sortedKeys(deviceKeySet)
	result.FilteredChannelCount = len(deviceKeys)
	for tunerNumber, keys := range tunerToDeviceKeys {
		sort.Strings(keys)
		tunerToDeviceKeys[tunerNumber] = keys
		if len(keys) > 1 {
			appendWarning(&result.Warnings, fmt.Sprintf("multiple device keys for tuner %s; using %s", tunerNumber, keys[0]))
		}
	}

	if len(deviceKeys) == 0 {
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
	instance, provider, err := s.providerForCurrentConfig(ctx)
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
		PatchPreview: map[string]map[string]string{},
	}
	if result.SyncMode == "" {
		result.SyncMode = SyncModeConfiguredOnly
	}

	deviceID := strings.TrimSpace(s.hdhrDeviceID)
	if deviceID == "" {
		return SyncResult{}, fmt.Errorf("%w: hdhr device id is required for lineup reload preflight", ErrDVRSyncConfig)
	}
	if err := provider.ReloadDeviceLineup(ctx, deviceID); err != nil {
		return SyncResult{}, fmt.Errorf("reload device lineup %q: %w", deviceID, err)
	}

	if instance.PreSyncRefreshDevices {
		if err := provider.RefreshDevices(ctx); err != nil {
			appendWarning(&result.Warnings, fmt.Sprintf("pre-sync refresh failed: %v", err))
		}
	}

	deviceChannels, err := provider.ListDeviceChannels(ctx)
	if err != nil {
		return SyncResult{}, err
	}
	result.DeviceChannelCount = len(deviceChannels)

	tunerToDeviceKeys := map[string][]string{}
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
		tunerToDeviceKeys[tunerNumber] = append(tunerToDeviceKeys[tunerNumber], deviceKey)
	}

	deviceKeys := sortedKeys(deviceKeySet)
	result.FilteredChannelCount = len(deviceKeys)
	for tunerNumber, keys := range tunerToDeviceKeys {
		sort.Strings(keys)
		tunerToDeviceKeys[tunerNumber] = keys
		if len(keys) > 1 {
			appendWarning(&result.Warnings, fmt.Sprintf("multiple device keys for tuner %s; using %s", tunerNumber, keys[0]))
		}
	}

	if len(deviceKeys) == 0 {
		return SyncResult{}, fmt.Errorf("%w: no provider device channels found for hdhr device id %q", ErrDVRSyncConfig, s.hdhrDeviceID)
	}

	channels, err := s.store.ListChannelsForDVRSync(ctx, instance.ID, true, req.IncludeDynamic)
	if err != nil {
		return SyncResult{}, err
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
			result.UnresolvedCount++
			appendWarning(
				&result.Warnings,
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
		lineupResult := LineupSyncResult{
			LineupID:           lineupID,
			ConfiguredChannels: len(channelsForLineup),
		}

		stations, err := provider.ListLineupStations(ctx, lineupID)
		if err != nil {
			return SyncResult{}, fmt.Errorf("fetch lineup stations %q: %w", lineupID, err)
		}
		currentMapping, err := provider.GetCustomMapping(ctx, lineupID)
		if err != nil {
			return SyncResult{}, fmt.Errorf("fetch lineup custom mapping %q: %w", lineupID, err)
		}

		stationByRef := make(map[string]DVRStation, len(stations))
		stationByLineupChannel := make(map[string][]DVRStation)
		for _, station := range stations {
			stationRef := strings.TrimSpace(station.StationRef)
			if stationRef == "" {
				continue
			}
			stationByRef[stationRef] = station

			lineupChannel := strings.TrimSpace(station.LineupChannel)
			if lineupChannel != "" {
				stationByLineupChannel[lineupChannel] = append(stationByLineupChannel[lineupChannel], station)
			}
		}

		desiredByDevice := map[string]string{}
		resolvedByChannelID := map[int64]string{}
		for _, mapping := range channelsForLineup {
			deviceKeysForTuner := tunerToDeviceKeys[strings.TrimSpace(mapping.GuideNumber)]
			if len(deviceKeysForTuner) == 0 {
				result.MissingTunerCount++
				appendWarning(&result.Warnings, fmt.Sprintf("missing provider tuner channel for guide_number=%s channel=%s", mapping.GuideNumber, mapping.GuideName))
				continue
			}

			stationRef, warning, ok := resolveStationRef(mapping, stationByRef, stationByLineupChannel)
			if warning != "" {
				appendWarning(&result.Warnings, warning)
			}
			if !ok {
				result.UnresolvedCount++
				continue
			}

			deviceKey := deviceKeysForTuner[0]
			desiredByDevice[deviceKey] = stationRef
			resolvedByChannelID[mapping.ChannelID] = stationRef
			lineupResult.ResolvedChannels++
		}

		patch := map[string]string{}
		if result.SyncMode == SyncModeMirrorDevice {
			for _, deviceKey := range deviceKeys {
				desired := strings.TrimSpace(desiredByDevice[deviceKey])
				current := strings.TrimSpace(currentMapping[deviceKey])
				if current == desired {
					lineupResult.UnchangedCount++
					result.UnchangedCount++
					continue
				}
				patch[deviceKey] = desired
				if desired == "" {
					lineupResult.ClearedCount++
					result.ClearedCount++
				} else {
					lineupResult.UpdatedCount++
					result.UpdatedCount++
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
					lineupResult.UnchangedCount++
					result.UnchangedCount++
					continue
				}
				patch[deviceKey] = desired
				lineupResult.UpdatedCount++
				result.UpdatedCount++
			}
		}

		if len(patch) > 0 {
			result.PatchPreview[lineupID] = sortedPatchCopy(patch)
			if !req.DryRun {
				for _, chunk := range chunkPatch(patch, syncBatchSize) {
					if err := provider.PutCustomMapping(ctx, lineupID, chunk); err != nil {
						return SyncResult{}, fmt.Errorf("apply lineup patch %q: %w", lineupID, err)
					}
					lineupResult.AppliedCount += len(chunk)
				}
			}
		}

		if !req.DryRun && len(resolvedByChannelID) > 0 {
			for _, mapping := range channelsForLineup {
				resolvedRef := strings.TrimSpace(resolvedByChannelID[mapping.ChannelID])
				if resolvedRef == "" {
					continue
				}
				if strings.TrimSpace(mapping.DVRStationRef) == resolvedRef {
					continue
				}

				if _, err := s.store.UpsertChannelDVRMapping(ctx, ChannelMapping{
					ChannelID:        mapping.ChannelID,
					DVRInstanceID:    instance.ID,
					DVRLineupID:      strings.TrimSpace(mapping.DVRLineupID),
					DVRLineupChannel: strings.TrimSpace(mapping.DVRLineupChannel),
					DVRStationRef:    resolvedRef,
					DVRCallsignHint:  strings.TrimSpace(mapping.DVRCallsignHint),
				}); err != nil {
					return SyncResult{}, fmt.Errorf("persist resolved station ref for channel_id=%d lineup=%s: %w", mapping.ChannelID, lineupID, err)
				}
			}
		}

		result.Lineups = append(result.Lineups, lineupResult)
	}

	if len(result.PatchPreview) == 0 {
		result.PatchPreview = nil
	}

	result.FinishedAt = time.Now().UTC().Unix()
	result.DurationMS = time.Since(start).Milliseconds()
	s.setLastSync(result)
	return result, nil
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

func normalizeSyncMode(mode SyncMode) SyncMode {
	switch SyncMode(strings.ToLower(strings.TrimSpace(string(mode)))) {
	case SyncModeMirrorDevice:
		return SyncModeMirrorDevice
	case SyncModeConfiguredOnly:
		return SyncModeConfiguredOnly
	default:
		return SyncModeConfiguredOnly
	}
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

func (s *Service) providerForCurrentConfig(ctx context.Context) (InstanceConfig, DVRProvider, error) {
	instance, err := s.store.GetDVRInstance(ctx)
	if err != nil {
		return InstanceConfig{}, nil, err
	}
	instance = instanceForProvider(instance, instance.Provider)

	provider, err := s.providerBuild(instance, s.httpClient)
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
