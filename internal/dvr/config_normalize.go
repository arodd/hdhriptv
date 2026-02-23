package dvr

import (
	"net/url"
	"strings"
)

// NormalizeProviderType canonicalizes provider values while preserving unknown
// inputs for upstream validation.
func NormalizeProviderType(provider ProviderType) ProviderType {
	switch ProviderType(strings.ToLower(strings.TrimSpace(string(provider)))) {
	case ProviderChannels:
		return ProviderChannels
	case ProviderJellyfin:
		return ProviderJellyfin
	default:
		return provider
	}
}

func normalizeProviderType(provider ProviderType) ProviderType {
	return NormalizeProviderType(provider)
}

// NormalizeSyncMode canonicalizes sync mode values and falls back to the
// configured-only mode for unknown or empty inputs.
func NormalizeSyncMode(mode SyncMode) SyncMode {
	switch SyncMode(strings.ToLower(strings.TrimSpace(string(mode)))) {
	case SyncModeMirrorDevice:
		return SyncModeMirrorDevice
	case SyncModeConfiguredOnly:
		return SyncModeConfiguredOnly
	default:
		return SyncModeConfiguredOnly
	}
}

func normalizeSyncMode(mode SyncMode) SyncMode {
	return NormalizeSyncMode(mode)
}

func normalizeActiveProviders(primary ProviderType, configured []ProviderType) []ProviderType {
	normalized := make([]ProviderType, 0, len(configured))
	seen := map[ProviderType]struct{}{}
	for _, provider := range configured {
		value := normalizeProviderType(provider)
		if value != ProviderChannels && value != ProviderJellyfin {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	if len(normalized) > 0 {
		return normalized
	}
	value := normalizeProviderType(primary)
	if value != ProviderChannels && value != ProviderJellyfin {
		value = ProviderChannels
	}
	return []ProviderType{value}
}

func resolvedActiveProviders(instance InstanceConfig) []ProviderType {
	normalized := normalizeActiveProviders(instance.Provider, instance.ActiveProviders)
	return filterConfiguredActiveProviders(instance, normalized)
}

func filterConfiguredActiveProviders(instance InstanceConfig, providers []ProviderType) []ProviderType {
	if len(providers) == 0 {
		return nil
	}

	channelsConfigured := providerBaseURLConfigured(providerBaseURL(instance, ProviderChannels))
	jellyfinConfigured := providerBaseURLConfigured(providerBaseURL(instance, ProviderJellyfin))

	filtered := make([]ProviderType, 0, len(providers))
	for _, provider := range providers {
		value := normalizeProviderType(provider)
		switch value {
		case ProviderChannels:
			if !channelsConfigured {
				continue
			}
		case ProviderJellyfin:
			if !jellyfinConfigured {
				continue
			}
		default:
			continue
		}
		filtered = append(filtered, value)
	}
	return filtered
}

// ProviderBaseURLConfigured returns true when the value is a non-empty absolute
// URL with scheme and host.
func ProviderBaseURLConfigured(raw string) bool {
	baseURL := strings.TrimSpace(raw)
	if baseURL == "" {
		return false
	}
	parsed, err := url.ParseRequestURI(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return false
	}
	return true
}

func providerBaseURLConfigured(raw string) bool {
	return ProviderBaseURLConfigured(raw)
}

// ProviderBaseURL resolves the provider-scoped base URL from the merged
// BaseURL/ChannelsBaseURL/JellyfinBaseURL fields.
func ProviderBaseURL(instance InstanceConfig, provider ProviderType) string {
	provider = normalizeProviderType(provider)

	channelsURL := strings.TrimSpace(instance.ChannelsBaseURL)
	jellyfinURL := strings.TrimSpace(instance.JellyfinBaseURL)
	baseURL := strings.TrimSpace(instance.BaseURL)

	if channelsURL == "" && provider == ProviderChannels {
		channelsURL = baseURL
	}
	if jellyfinURL == "" && provider == ProviderJellyfin {
		jellyfinURL = baseURL
	}

	switch provider {
	case ProviderJellyfin:
		if jellyfinURL != "" {
			return jellyfinURL
		}
		return ""
	case ProviderChannels:
		if channelsURL != "" {
			return channelsURL
		}
		return ""
	default:
		return baseURL
	}
}

func providerBaseURL(instance InstanceConfig, provider ProviderType) string {
	return ProviderBaseURL(instance, provider)
}

// NormalizeForProvider returns a canonical provider-scoped view of the instance.
func NormalizeForProvider(instance InstanceConfig, provider ProviderType) InstanceConfig {
	configured := NormalizeStoredInstance(instance)
	configured.Provider = normalizeProviderType(provider)
	if configured.Provider == "" {
		configured.Provider = ProviderChannels
	}
	configured.ActiveProviders = resolvedActiveProviders(configured)
	configured.BaseURL = providerBaseURL(configured, configured.Provider)
	if strings.TrimSpace(configured.ChannelsBaseURL) == "" {
		configured.ChannelsBaseURL = providerBaseURL(configured, ProviderChannels)
	}
	if strings.TrimSpace(configured.JellyfinBaseURL) == "" {
		configured.JellyfinBaseURL = providerBaseURL(configured, ProviderJellyfin)
	}
	return configured
}

func instanceForProvider(instance InstanceConfig, provider ProviderType) InstanceConfig {
	return NormalizeForProvider(instance, provider)
}

// NormalizeStoredInstance canonicalizes persisted/read-back config shape.
func NormalizeStoredInstance(instance InstanceConfig) InstanceConfig {
	normalized := instance
	normalized.Provider = normalizeProviderType(normalized.Provider)
	if normalized.Provider == "" {
		normalized.Provider = ProviderChannels
	}
	normalized.BaseURL = strings.TrimSpace(normalized.BaseURL)
	normalized.ChannelsBaseURL = strings.TrimSpace(normalized.ChannelsBaseURL)
	normalized.JellyfinBaseURL = strings.TrimSpace(normalized.JellyfinBaseURL)
	if normalized.BaseURL != "" {
		switch normalized.Provider {
		case ProviderJellyfin:
			normalized.JellyfinBaseURL = normalized.BaseURL
		case ProviderChannels:
			normalized.ChannelsBaseURL = normalized.BaseURL
		}
	}
	if normalized.ChannelsBaseURL == "" {
		normalized.ChannelsBaseURL = providerBaseURL(normalized, ProviderChannels)
	}
	if normalized.JellyfinBaseURL == "" {
		normalized.JellyfinBaseURL = providerBaseURL(normalized, ProviderJellyfin)
	}
	normalized.BaseURL = providerBaseURL(normalized, normalized.Provider)
	normalized.ActiveProviders = resolvedActiveProviders(normalized)
	normalized.DefaultLineupID = strings.TrimSpace(normalized.DefaultLineupID)
	normalized.SyncCron = strings.TrimSpace(normalized.SyncCron)
	normalized.SyncMode = normalizeSyncMode(normalized.SyncMode)
	normalized.JellyfinAPIToken = strings.TrimSpace(normalized.JellyfinAPIToken)
	normalized.JellyfinTunerHostID = strings.TrimSpace(normalized.JellyfinTunerHostID)
	normalized.JellyfinAPITokenConfigured = strings.TrimSpace(normalized.JellyfinAPIToken) != ""
	return normalized
}

// NormalizeInstanceConfig canonicalizes API update payloads against the current
// stored instance while preserving existing provider-specific URLs when omitted.
func NormalizeInstanceConfig(instance InstanceConfig, current InstanceConfig) InstanceConfig {
	current = NormalizeStoredInstance(current)

	normalized := instance
	normalized.ID = current.ID
	normalized.Provider = normalizeProviderType(normalized.Provider)
	if normalized.Provider == "" {
		normalized.Provider = ProviderChannels
	}
	normalized.BaseURL = strings.TrimSpace(normalized.BaseURL)
	normalized.ChannelsBaseURL = strings.TrimSpace(normalized.ChannelsBaseURL)
	normalized.JellyfinBaseURL = strings.TrimSpace(normalized.JellyfinBaseURL)

	if normalized.ChannelsBaseURL == "" {
		normalized.ChannelsBaseURL = strings.TrimSpace(current.ChannelsBaseURL)
	}
	if normalized.JellyfinBaseURL == "" {
		normalized.JellyfinBaseURL = strings.TrimSpace(current.JellyfinBaseURL)
	}
	if normalized.BaseURL != "" {
		switch normalized.Provider {
		case ProviderJellyfin:
			normalized.JellyfinBaseURL = normalized.BaseURL
		case ProviderChannels:
			normalized.ChannelsBaseURL = normalized.BaseURL
		}
	}
	if strings.TrimSpace(normalized.ChannelsBaseURL) == "" {
		normalized.ChannelsBaseURL = providerBaseURL(current, ProviderChannels)
	}
	if strings.TrimSpace(normalized.JellyfinBaseURL) == "" {
		normalized.JellyfinBaseURL = providerBaseURL(current, ProviderJellyfin)
	}

	normalized.ActiveProviders = resolvedActiveProviders(normalized)
	normalized.DefaultLineupID = strings.TrimSpace(normalized.DefaultLineupID)
	normalized.SyncCron = strings.TrimSpace(normalized.SyncCron)
	if normalized.SyncEnabled && normalized.SyncCron == "" {
		normalized.SyncCron = defaultSyncCron
	}
	normalized.SyncMode = normalizeSyncMode(normalized.SyncMode)
	normalized.JellyfinAPIToken = strings.TrimSpace(normalized.JellyfinAPIToken)
	normalized.JellyfinTunerHostID = strings.TrimSpace(normalized.JellyfinTunerHostID)
	normalized = NormalizeForProvider(normalized, normalized.Provider)
	normalized.JellyfinAPITokenConfigured = false
	return normalized
}
