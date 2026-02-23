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
	defaultJellyfinBaseURL = "http://jellyfin.lan:8096"
)

// ProviderBase exposes common provider identity behavior.
type ProviderBase interface {
	Type() ProviderType
}

// LineupReloadProvider supports lineup reload + guide refresh flows.
type LineupReloadProvider interface {
	ProviderBase
	ListLineups(ctx context.Context) ([]DVRLineup, error)
	ReloadDeviceLineup(ctx context.Context, deviceID string) error
	RefreshGuideStations(ctx context.Context) error
	RedownloadGuideLineup(ctx context.Context, lineupID string) error
}

// MappingProvider supports forward/reverse mapping sync behavior.
type MappingProvider interface {
	ProviderBase
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

func newLineupReloadProvider(instance InstanceConfig, httpClient *http.Client) (LineupReloadProvider, error) {
	configured := instanceForProvider(instance, instance.Provider)
	switch normalizeProviderType(configured.Provider) {
	case ProviderChannels:
		if strings.TrimSpace(configured.BaseURL) == "" {
			return nil, fmt.Errorf("%w: channels base url is required", ErrDVRSyncConfig)
		}
		return NewChannelsProvider(configured.BaseURL, httpClient), nil
	case ProviderJellyfin:
		return NewJellyfinProvider(configured, httpClient), nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedProvider, configured.Provider)
	}
}

func newMappingProvider(instance InstanceConfig, httpClient *http.Client) (MappingProvider, error) {
	configured := instanceForProvider(instance, instance.Provider)
	switch normalizeProviderType(configured.Provider) {
	case ProviderChannels:
		if strings.TrimSpace(configured.BaseURL) == "" {
			return nil, fmt.Errorf("%w: channels base url is required", ErrDVRSyncConfig)
		}
		return NewChannelsProvider(configured.BaseURL, httpClient), nil
	case ProviderJellyfin:
		return nil, unsupportedProviderOperationError(ProviderJellyfin, "mapping operations")
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedProvider, configured.Provider)
	}
}

func unsupportedProviderOperationError(provider ProviderType, operation string) error {
	operation = strings.TrimSpace(operation)
	if operation == "" {
		operation = "operation"
	}
	return fmt.Errorf("%w: provider=%q does not support %s", ErrUnsupportedProvider, provider, operation)
}
