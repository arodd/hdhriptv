package dvr

// ProviderType identifies one DVR provider implementation.
type ProviderType string

const (
	ProviderChannels ProviderType = "channels"
	ProviderJellyfin ProviderType = "jellyfin"
)

// SyncMode controls how sync applies and clears mappings.
type SyncMode string

const (
	SyncModeConfiguredOnly SyncMode = "configured_only"
	SyncModeMirrorDevice   SyncMode = "mirror_device"
)

// InstanceConfig stores one DVR integration endpoint configuration.
type InstanceConfig struct {
	ID                         int64          `json:"id"`
	Provider                   ProviderType   `json:"provider"`
	ActiveProviders            []ProviderType `json:"active_providers,omitempty"`
	BaseURL                    string         `json:"base_url"`
	ChannelsBaseURL            string         `json:"channels_base_url,omitempty"`
	JellyfinBaseURL            string         `json:"jellyfin_base_url,omitempty"`
	DefaultLineupID            string         `json:"default_lineup_id,omitempty"`
	SyncEnabled                bool           `json:"sync_enabled"`
	SyncCron                   string         `json:"sync_cron,omitempty"`
	SyncMode                   SyncMode       `json:"sync_mode"`
	PreSyncRefreshDevices      bool           `json:"pre_sync_refresh_devices"`
	JellyfinAPIToken           string         `json:"jellyfin_api_token,omitempty"`
	JellyfinAPITokenConfigured bool           `json:"jellyfin_api_token_configured,omitempty"`
	JellyfinTunerHostID        string         `json:"jellyfin_tuner_host_id,omitempty"`
	UpdatedAt                  int64          `json:"updated_at"`
}

// ChannelMapping stores per-channel DVR lineup association metadata.
type ChannelMapping struct {
	ChannelID        int64  `json:"channel_id"`
	GuideNumber      string `json:"guide_number"`
	GuideName        string `json:"guide_name"`
	Enabled          bool   `json:"enabled"`
	DVRInstanceID    int64  `json:"dvr_instance_id"`
	DVRLineupID      string `json:"dvr_lineup_id,omitempty"`
	DVRLineupChannel string `json:"dvr_lineup_channel,omitempty"`
	DVRStationRef    string `json:"dvr_station_ref,omitempty"`
	DVRCallsignHint  string `json:"dvr_callsign_hint,omitempty"`
}

// ReverseSyncRequest controls pull direction sync (DVR -> hdhriptv mappings).
type ReverseSyncRequest struct {
	DryRun         bool   `json:"dry_run"`
	LineupID       string `json:"lineup_id,omitempty"`
	IncludeDynamic bool   `json:"include_dynamic,omitempty"`
}

// ReverseSyncResult summarizes a reverse sync run.
type ReverseSyncResult struct {
	StartedAt              int64    `json:"started_at"`
	FinishedAt             int64    `json:"finished_at"`
	DurationMS             int64    `json:"duration_ms"`
	DryRun                 bool     `json:"dry_run"`
	Provider               string   `json:"provider"`
	BaseURL                string   `json:"base_url"`
	HDHRDeviceID           string   `json:"hdhr_device_id,omitempty"`
	LineupID               string   `json:"lineup_id"`
	DeviceChannelCount     int      `json:"device_channel_count"`
	FilteredChannelCount   int      `json:"filtered_channel_count"`
	CandidateCount         int      `json:"candidate_count"`
	ImportedCount          int      `json:"imported_count"`
	UnchangedCount         int      `json:"unchanged_count"`
	MissingTunerCount      int      `json:"missing_tuner_count"`
	MissingMappingCount    int      `json:"missing_mapping_count"`
	MissingStationRefCount int      `json:"missing_station_ref_count"`
	Warnings               []string `json:"warnings,omitempty"`
}

const (
	ReloadStatusDisabled = "disabled"
	ReloadStatusFailed   = "failed"
	ReloadStatusReloaded = "reloaded"
	ReloadStatusPartial  = "partial"
	ReloadStatusSkipped  = "skipped"
	ReloadStatusUnknown  = "unknown"
)

// ReloadOutcome captures provider-aware DVR lineup reload state.
type ReloadOutcome struct {
	Reloaded          bool                    `json:"reloaded"`
	Skipped           bool                    `json:"skipped"`
	Failed            bool                    `json:"failed"`
	Status            string                  `json:"status"`
	SkipReasons       []string                `json:"skip_reasons,omitempty"`
	FailureReasons    []string                `json:"failure_reasons,omitempty"`
	ReloadedProviders []ProviderType          `json:"reloaded_providers,omitempty"`
	SkippedProviders  map[ProviderType]string `json:"skipped_providers,omitempty"`
	FailedProviders   map[ProviderType]string `json:"failed_providers,omitempty"`
}
