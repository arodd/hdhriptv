package dvr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

var (
	ErrUnsupportedProvider = errors.New("unsupported dvr provider")
)

const (
	defaultChannelsBaseURL = "http://channels.lan:8089"
	defaultJellyfinBaseURL = "http://jellyfin.lan:8096"
)

// DVRProvider encapsulates one provider implementation.
type DVRProvider interface {
	Type() ProviderType
	ListLineups(ctx context.Context) ([]DVRLineup, error)
	ReloadDeviceLineup(ctx context.Context, deviceID string) error
	RefreshGuideStations(ctx context.Context) error
	RedownloadGuideLineup(ctx context.Context, lineupID string) error
	ListDeviceChannels(ctx context.Context) (map[string]DVRDeviceChannel, error)
	ListLineupStations(ctx context.Context, lineupID string) ([]DVRStation, error)
	GetCustomMapping(ctx context.Context, lineupID string) (map[string]string, error)
	PutCustomMapping(ctx context.Context, lineupID string, patch map[string]string) error
	RefreshDevices(ctx context.Context) error
}

// DVRLineup describes one guide lineup/source.
type DVRLineup struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
}

// DVRDeviceChannel represents one provider device channel entry.
type DVRDeviceChannel struct {
	Key        string `json:"key"`
	DeviceID   string `json:"device_id,omitempty"`
	Number     string `json:"number,omitempty"`
	Name       string `json:"name,omitempty"`
	CallSign   string `json:"call_sign,omitempty"`
	StationRef string `json:"station_ref,omitempty"`
}

// DVRStation represents one station entry inside a lineup.
type DVRStation struct {
	StationRef    string `json:"station_ref"`
	LineupChannel string `json:"lineup_channel,omitempty"`
	CallSign      string `json:"call_sign,omitempty"`
	Name          string `json:"name,omitempty"`
}

func newProvider(instance InstanceConfig, httpClient *http.Client) (DVRProvider, error) {
	configured := instanceForProvider(instance, instance.Provider)
	switch normalizeProviderType(configured.Provider) {
	case ProviderChannels:
		return NewChannelsProvider(configured.BaseURL, httpClient), nil
	case ProviderJellyfin:
		return NewJellyfinProvider(configured, httpClient), nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedProvider, configured.Provider)
	}
}

func normalizeProviderType(provider ProviderType) ProviderType {
	switch ProviderType(strings.ToLower(strings.TrimSpace(string(provider)))) {
	case ProviderChannels:
		return ProviderChannels
	case ProviderJellyfin:
		return ProviderJellyfin
	default:
		return provider
	}
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

func providerBaseURL(instance InstanceConfig, provider ProviderType) string {
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
		return defaultChannelsBaseURL
	default:
		return baseURL
	}
}

func instanceForProvider(instance InstanceConfig, provider ProviderType) InstanceConfig {
	configured := instance
	configured.Provider = normalizeProviderType(provider)
	if configured.Provider == "" {
		configured.Provider = ProviderChannels
	}
	configured.ActiveProviders = normalizeActiveProviders(configured.Provider, configured.ActiveProviders)
	configured.BaseURL = providerBaseURL(configured, configured.Provider)
	if strings.TrimSpace(configured.ChannelsBaseURL) == "" {
		configured.ChannelsBaseURL = providerBaseURL(configured, ProviderChannels)
	}
	if strings.TrimSpace(configured.JellyfinBaseURL) == "" {
		configured.JellyfinBaseURL = providerBaseURL(configured, ProviderJellyfin)
	}
	return configured
}
