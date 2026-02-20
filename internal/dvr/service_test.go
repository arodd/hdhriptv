package dvr

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestServiceSyncConfiguredOnlyReorderPatch(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:       1,
			Provider: ProviderChannels,
			BaseURL:  "http://channels.lan:8089",
			SyncMode: SyncModeConfiguredOnly,
		},
		channels: []ChannelMapping{
			{
				ChannelID:        1,
				GuideNumber:      "100",
				GuideName:        "CBS",
				Enabled:          true,
				DVRInstanceID:    1,
				DVRLineupID:      "USA-TEST",
				DVRLineupChannel: "2",
			},
			{
				ChannelID:        2,
				GuideNumber:      "101",
				GuideName:        "FOX",
				Enabled:          true,
				DVRInstanceID:    1,
				DVRLineupID:      "USA-TEST",
				DVRLineupChannel: "3",
			},
		},
	}

	provider := &fakeProvider{
		deviceChannels: map[string]DVRDeviceChannel{
			"A100": {Key: "A100", DeviceID: "8F07FDC6", Number: "100"},
			"A101": {Key: "A101", DeviceID: "8F07FDC6", Number: "101"},
		},
		stationsByLineup: map[string][]DVRStation{
			"USA-TEST": {
				{StationRef: "21234", LineupChannel: "2", CallSign: "WCCO"},
				{StationRef: "24504", LineupChannel: "3", CallSign: "KMSP"},
			},
		},
		customByLineup: map[string]map[string]string{
			"USA-TEST": {
				"A100": "99999",
				"A101": "24504",
			},
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	result, err := svc.Sync(context.Background(), SyncRequest{DryRun: true})
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	if result.UpdatedCount != 1 {
		t.Fatalf("result.UpdatedCount = %d, want 1", result.UpdatedCount)
	}
	if result.ClearedCount != 0 {
		t.Fatalf("result.ClearedCount = %d, want 0", result.ClearedCount)
	}
	if result.UnchangedCount != 1 {
		t.Fatalf("result.UnchangedCount = %d, want 1", result.UnchangedCount)
	}
	if len(provider.reloadCalls) != 1 {
		t.Fatalf("len(provider.reloadCalls) = %d, want 1", len(provider.reloadCalls))
	}
	if provider.reloadCalls[0] != "8F07FDC6" {
		t.Fatalf("provider.reloadCalls[0] = %q, want 8F07FDC6", provider.reloadCalls[0])
	}

	wantPatch := map[string]map[string]string{
		"USA-TEST": {
			"A100": "21234",
		},
	}
	if !reflect.DeepEqual(result.PatchPreview, wantPatch) {
		t.Fatalf("result.PatchPreview = %#v, want %#v", result.PatchPreview, wantPatch)
	}
	if len(provider.putCalls) != 0 {
		t.Fatalf("len(provider.putCalls) = %d, want 0 for dry-run", len(provider.putCalls))
	}
}

func TestServiceSyncFailsWhenReloadLineupPreflightFails(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:       1,
			Provider: ProviderChannels,
			BaseURL:  "http://channels.lan:8089",
			SyncMode: SyncModeConfiguredOnly,
		},
		channels: []ChannelMapping{
			{
				ChannelID:        1,
				GuideNumber:      "100",
				GuideName:        "CBS",
				Enabled:          true,
				DVRInstanceID:    1,
				DVRLineupID:      "USA-TEST",
				DVRLineupChannel: "2",
			},
		},
	}

	provider := &fakeProvider{
		deviceChannels: map[string]DVRDeviceChannel{
			"A100": {Key: "A100", DeviceID: "8F07FDC6", Number: "100"},
		},
		stationsByLineup: map[string][]DVRStation{
			"USA-TEST": {
				{StationRef: "21234", LineupChannel: "2", CallSign: "WCCO"},
			},
		},
		customByLineup: map[string]map[string]string{
			"USA-TEST": {
				"A100": "99999",
			},
		},
		reloadErr: fmt.Errorf("reload failed"),
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	_, err = svc.Sync(context.Background(), SyncRequest{DryRun: true})
	if err == nil {
		t.Fatal("Sync() error = nil, want non-nil")
	}
	if len(provider.reloadCalls) != 1 {
		t.Fatalf("len(provider.reloadCalls) = %d, want 1", len(provider.reloadCalls))
	}
}

func TestServiceReloadLineupRefreshesGuideStationsAndRedownloadsGuide(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:              1,
			Provider:        ProviderChannels,
			BaseURL:         "http://channels.lan:8089",
			DefaultLineupID: "USA-TEST",
		},
	}
	provider := &fakeProvider{}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	if err := svc.ReloadLineup(context.Background()); err != nil {
		t.Fatalf("ReloadLineup() error = %v", err)
	}
	if len(provider.reloadCalls) != 1 {
		t.Fatalf("len(provider.reloadCalls) = %d, want 1", len(provider.reloadCalls))
	}
	if provider.refreshGuideStationsCalls != 1 {
		t.Fatalf("provider.refreshGuideStationsCalls = %d, want 1", provider.refreshGuideStationsCalls)
	}
	if got, want := len(provider.redownloadGuideLineupCalls), 1; got != want {
		t.Fatalf("len(provider.redownloadGuideLineupCalls) = %d, want %d", got, want)
	}
	if got, want := provider.redownloadGuideLineupCalls[0], "USA-TEST"; got != want {
		t.Fatalf("provider.redownloadGuideLineupCalls[0] = %q, want %q", got, want)
	}
}

func TestServiceReloadLineupReturnsGuideRefreshError(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:       1,
			Provider: ProviderChannels,
			BaseURL:  "http://channels.lan:8089",
		},
	}
	provider := &fakeProvider{
		refreshGuideStationsErr: fmt.Errorf("refresh failed"),
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	if err := svc.ReloadLineup(context.Background()); err == nil {
		t.Fatal("ReloadLineup() error = nil, want non-nil")
	}
	if len(provider.reloadCalls) != 1 {
		t.Fatalf("len(provider.reloadCalls) = %d, want 1", len(provider.reloadCalls))
	}
	if provider.refreshGuideStationsCalls != 1 {
		t.Fatalf("provider.refreshGuideStationsCalls = %d, want 1", provider.refreshGuideStationsCalls)
	}
	if got := len(provider.redownloadGuideLineupCalls); got != 0 {
		t.Fatalf("len(provider.redownloadGuideLineupCalls) = %d, want 0 after guide refresh failure", got)
	}
}

func TestServiceReloadLineupReturnsGuideRedownloadError(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:              1,
			Provider:        ProviderChannels,
			BaseURL:         "http://channels.lan:8089",
			DefaultLineupID: "USA-TEST",
		},
	}
	provider := &fakeProvider{
		redownloadGuideLineupErr: fmt.Errorf("redownload failed"),
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	if err := svc.ReloadLineup(context.Background()); err == nil {
		t.Fatal("ReloadLineup() error = nil, want non-nil")
	}
	if got, want := len(provider.reloadCalls), 1; got != want {
		t.Fatalf("len(provider.reloadCalls) = %d, want %d", got, want)
	}
	if got, want := provider.refreshGuideStationsCalls, 1; got != want {
		t.Fatalf("provider.refreshGuideStationsCalls = %d, want %d", got, want)
	}
	if got, want := len(provider.redownloadGuideLineupCalls), 1; got != want {
		t.Fatalf("len(provider.redownloadGuideLineupCalls) = %d, want %d", got, want)
	}
}

func TestServiceReloadLineupRedownloadsGuideForDiscoveredLineups(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:       1,
			Provider: ProviderChannels,
			BaseURL:  "http://channels.lan:8089",
		},
	}
	provider := &fakeProvider{
		lineups: []DVRLineup{
			{ID: "USA-B"},
			{ID: "USA-A"},
			{ID: "USA-B"},
			{ID: "   "},
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	if err := svc.ReloadLineup(context.Background()); err != nil {
		t.Fatalf("ReloadLineup() error = %v", err)
	}
	if got, want := provider.redownloadGuideLineupCalls, []string{"USA-A", "USA-B"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("provider.redownloadGuideLineupCalls = %#v, want %#v", got, want)
	}
}

func TestServiceReloadLineupForPlaylistSyncSkipsIncompleteJellyfinConfig(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:               1,
			Provider:         ProviderJellyfin,
			BaseURL:          "http://jellyfin.example.invalid:8096",
			JellyfinAPIToken: "",
		},
	}
	provider := &fakeProvider{providerType: ProviderJellyfin}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	reloaded, skipped, skipReason, err := svc.ReloadLineupForPlaylistSync(context.Background())
	if err != nil {
		t.Fatalf("ReloadLineupForPlaylistSync() error = %v", err)
	}
	if !skipped {
		t.Fatalf("skipped = %v, want true", skipped)
	}
	if reloaded {
		t.Fatalf("reloaded = %v, want false", reloaded)
	}
	if skipReason != "jellyfin:missing_jellyfin_api_token" {
		t.Fatalf("skipReason = %q, want jellyfin:missing_jellyfin_api_token", skipReason)
	}
	if len(provider.reloadCalls) != 0 {
		t.Fatalf("len(provider.reloadCalls) = %d, want 0 on skipped run", len(provider.reloadCalls))
	}
}

func TestServiceReloadLineupForPlaylistSyncReloadsConfiguredJellyfin(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:               1,
			Provider:         ProviderJellyfin,
			BaseURL:          "http://jellyfin.example.invalid:8096",
			JellyfinAPIToken: "token-abc",
		},
	}
	provider := &fakeProvider{providerType: ProviderJellyfin}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	reloaded, skipped, skipReason, err := svc.ReloadLineupForPlaylistSync(context.Background())
	if err != nil {
		t.Fatalf("ReloadLineupForPlaylistSync() error = %v", err)
	}
	if skipped {
		t.Fatalf("skipped = %v, want false", skipped)
	}
	if !reloaded {
		t.Fatalf("reloaded = %v, want true", reloaded)
	}
	if skipReason != "" {
		t.Fatalf("skipReason = %q, want empty", skipReason)
	}
	if got, want := len(provider.reloadCalls), 1; got != want {
		t.Fatalf("len(provider.reloadCalls) = %d, want %d", got, want)
	}
}

func TestServiceReloadLineupForPlaylistSyncReloadsAllActiveProviders(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:               1,
			Provider:         ProviderChannels,
			ActiveProviders:  []ProviderType{ProviderChannels, ProviderJellyfin},
			ChannelsBaseURL:  "http://channels.example.invalid:8089",
			JellyfinBaseURL:  "http://jellyfin.example.invalid:8096",
			JellyfinAPIToken: "token-abc",
		},
	}
	channelsProvider := &fakeProvider{providerType: ProviderChannels}
	jellyfinProvider := &fakeProvider{providerType: ProviderJellyfin}
	builtBaseURLByProvider := map[ProviderType]string{}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(instance InstanceConfig, _ *http.Client) (DVRProvider, error) {
		builtBaseURLByProvider[instance.Provider] = instance.BaseURL
		switch instance.Provider {
		case ProviderChannels:
			return channelsProvider, nil
		case ProviderJellyfin:
			return jellyfinProvider, nil
		default:
			return nil, fmt.Errorf("unexpected provider %q", instance.Provider)
		}
	}

	reloaded, skipped, skipReason, err := svc.ReloadLineupForPlaylistSync(context.Background())
	if err != nil {
		t.Fatalf("ReloadLineupForPlaylistSync() error = %v", err)
	}
	if !reloaded {
		t.Fatalf("reloaded = %v, want true", reloaded)
	}
	if skipped {
		t.Fatalf("skipped = %v, want false", skipped)
	}
	if skipReason != "" {
		t.Fatalf("skipReason = %q, want empty", skipReason)
	}
	if got, want := len(channelsProvider.reloadCalls), 1; got != want {
		t.Fatalf("len(channelsProvider.reloadCalls) = %d, want %d", got, want)
	}
	if got, want := len(jellyfinProvider.reloadCalls), 1; got != want {
		t.Fatalf("len(jellyfinProvider.reloadCalls) = %d, want %d", got, want)
	}
	if got, want := builtBaseURLByProvider[ProviderChannels], "http://channels.example.invalid:8089"; got != want {
		t.Fatalf("channels provider BaseURL = %q, want %q", got, want)
	}
	if got, want := builtBaseURLByProvider[ProviderJellyfin], "http://jellyfin.example.invalid:8096"; got != want {
		t.Fatalf("jellyfin provider BaseURL = %q, want %q", got, want)
	}
}

func TestServiceReloadLineupForPlaylistSyncSkipsIncompleteActiveProvidersAndReloadsRest(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:               1,
			Provider:         ProviderChannels,
			ActiveProviders:  []ProviderType{ProviderChannels, ProviderJellyfin},
			ChannelsBaseURL:  "http://channels.example.invalid:8089",
			JellyfinBaseURL:  "http://jellyfin.example.invalid:8096",
			JellyfinAPIToken: "",
		},
	}
	channelsProvider := &fakeProvider{providerType: ProviderChannels}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(instance InstanceConfig, _ *http.Client) (DVRProvider, error) {
		switch instance.Provider {
		case ProviderChannels:
			return channelsProvider, nil
		case ProviderJellyfin:
			return &fakeProvider{providerType: ProviderJellyfin}, nil
		default:
			return nil, fmt.Errorf("unexpected provider %q", instance.Provider)
		}
	}

	reloaded, skipped, skipReason, err := svc.ReloadLineupForPlaylistSync(context.Background())
	if err != nil {
		t.Fatalf("ReloadLineupForPlaylistSync() error = %v", err)
	}
	if !reloaded {
		t.Fatalf("reloaded = %v, want true", reloaded)
	}
	if !skipped {
		t.Fatalf("skipped = %v, want true", skipped)
	}
	if skipReason != "jellyfin:missing_jellyfin_api_token" {
		t.Fatalf("skipReason = %q, want jellyfin:missing_jellyfin_api_token", skipReason)
	}
	if got, want := len(channelsProvider.reloadCalls), 1; got != want {
		t.Fatalf("len(channelsProvider.reloadCalls) = %d, want %d", got, want)
	}
}

func TestServiceGetStateIncludesStoredJellyfinToken(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:                         1,
			Provider:                   ProviderJellyfin,
			BaseURL:                    "http://jellyfin.example.invalid:8096",
			JellyfinAPIToken:           "super-secret-token",
			JellyfinAPITokenConfigured: true,
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	state, err := svc.GetState(context.Background())
	if err != nil {
		t.Fatalf("GetState() error = %v", err)
	}
	if state.Instance.JellyfinAPIToken != "super-secret-token" {
		t.Fatalf("GetState() jellyfin_api_token = %q, want stored token", state.Instance.JellyfinAPIToken)
	}
	if !state.Instance.JellyfinAPITokenConfigured {
		t.Fatal("GetState() jellyfin_api_token_configured = false, want true")
	}
}

func TestServiceSyncMirrorDeviceClearsUnconfigured(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:       1,
			Provider: ProviderChannels,
			BaseURL:  "http://channels.lan:8089",
			SyncMode: SyncModeMirrorDevice,
		},
		channels: []ChannelMapping{
			{
				ChannelID:        1,
				GuideNumber:      "100",
				GuideName:        "CBS",
				Enabled:          true,
				DVRInstanceID:    1,
				DVRLineupID:      "USA-TEST",
				DVRLineupChannel: "2",
			},
		},
	}

	provider := &fakeProvider{
		deviceChannels: map[string]DVRDeviceChannel{
			"A100": {Key: "A100", DeviceID: "8F07FDC6", Number: "100"},
			"A101": {Key: "A101", DeviceID: "8F07FDC6", Number: "101"},
		},
		stationsByLineup: map[string][]DVRStation{
			"USA-TEST": {
				{StationRef: "21234", LineupChannel: "2", CallSign: "WCCO"},
			},
		},
		customByLineup: map[string]map[string]string{
			"USA-TEST": {
				"A100": "99999",
				"A101": "11111",
			},
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	result, err := svc.Sync(context.Background(), SyncRequest{DryRun: false})
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	if result.UpdatedCount != 1 {
		t.Fatalf("result.UpdatedCount = %d, want 1", result.UpdatedCount)
	}
	if result.ClearedCount != 1 {
		t.Fatalf("result.ClearedCount = %d, want 1", result.ClearedCount)
	}
	if len(provider.putCalls) != 1 {
		t.Fatalf("len(provider.putCalls) = %d, want 1", len(provider.putCalls))
	}

	final := provider.customByLineup["USA-TEST"]
	if final["A100"] != "21234" {
		t.Fatalf("custom mapping A100 = %q, want 21234", final["A100"])
	}
	if final["A101"] != "" {
		t.Fatalf("custom mapping A101 = %q, want empty string", final["A101"])
	}
	if len(store.upsertCalls) != 1 {
		t.Fatalf("len(store.upsertCalls) = %d, want 1 resolved-station persistence update", len(store.upsertCalls))
	}
	if store.upsertCalls[0].DVRStationRef != "21234" {
		t.Fatalf("store.upsertCalls[0].DVRStationRef = %q, want 21234", store.upsertCalls[0].DVRStationRef)
	}
}

func TestServiceSyncDryRunDoesNotPersistResolvedStationRef(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:       1,
			Provider: ProviderChannels,
			BaseURL:  "http://channels.lan:8089",
			SyncMode: SyncModeConfiguredOnly,
		},
		channels: []ChannelMapping{
			{
				ChannelID:        1,
				GuideNumber:      "100",
				GuideName:        "CBS",
				Enabled:          true,
				DVRInstanceID:    1,
				DVRLineupID:      "USA-TEST",
				DVRLineupChannel: "2",
			},
		},
	}

	provider := &fakeProvider{
		deviceChannels: map[string]DVRDeviceChannel{
			"A100": {Key: "A100", DeviceID: "8F07FDC6", Number: "100"},
		},
		stationsByLineup: map[string][]DVRStation{
			"USA-TEST": {
				{StationRef: "21234", LineupChannel: "2", CallSign: "WCCO"},
			},
		},
		customByLineup: map[string]map[string]string{
			"USA-TEST": {},
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	result, err := svc.Sync(context.Background(), SyncRequest{DryRun: true})
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if result.UpdatedCount != 1 {
		t.Fatalf("result.UpdatedCount = %d, want 1", result.UpdatedCount)
	}
	if len(store.upsertCalls) != 0 {
		t.Fatalf("len(store.upsertCalls) = %d, want 0 during dry-run", len(store.upsertCalls))
	}
}

func TestServiceReverseSyncImportsMappings(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:              1,
			Provider:        ProviderChannels,
			BaseURL:         "http://channels.lan:8089",
			DefaultLineupID: "USA-TEST",
		},
		channels: []ChannelMapping{
			{
				ChannelID:   10,
				GuideNumber: "100",
				GuideName:   "CBS",
				Enabled:     true,
			},
			{
				ChannelID:   11,
				GuideNumber: "101",
				GuideName:   "FOX",
				Enabled:     true,
			},
		},
	}

	provider := &fakeProvider{
		deviceChannels: map[string]DVRDeviceChannel{
			"A100": {Key: "A100", DeviceID: "8F07FDC6", Number: "100"},
			"A101": {Key: "A101", DeviceID: "8F07FDC6", Number: "101"},
		},
		stationsByLineup: map[string][]DVRStation{
			"USA-TEST": {
				{StationRef: "21234", LineupChannel: "2", CallSign: "WCCO"},
				{StationRef: "24504", LineupChannel: "3", CallSign: "KMSP"},
			},
		},
		customByLineup: map[string]map[string]string{
			"USA-TEST": {
				"A100": "21234",
				"A101": "24504",
			},
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	result, err := svc.ReverseSync(context.Background(), ReverseSyncRequest{})
	if err != nil {
		t.Fatalf("ReverseSync() error = %v", err)
	}

	if result.CandidateCount != 2 {
		t.Fatalf("result.CandidateCount = %d, want 2", result.CandidateCount)
	}
	if result.ImportedCount != 2 {
		t.Fatalf("result.ImportedCount = %d, want 2", result.ImportedCount)
	}
	if result.MissingTunerCount != 0 {
		t.Fatalf("result.MissingTunerCount = %d, want 0", result.MissingTunerCount)
	}
	if len(store.upsertCalls) != 2 {
		t.Fatalf("len(store.upsertCalls) = %d, want 2", len(store.upsertCalls))
	}

	byChannelID := map[int64]ChannelMapping{}
	for _, mapping := range store.upsertCalls {
		byChannelID[mapping.ChannelID] = mapping
	}
	if got := byChannelID[10].DVRLineupChannel; got != "2" {
		t.Fatalf("channel 10 DVRLineupChannel = %q, want 2", got)
	}
	if got := byChannelID[11].DVRLineupChannel; got != "3" {
		t.Fatalf("channel 11 DVRLineupChannel = %q, want 3", got)
	}
	if got := byChannelID[10].DVRStationRef; got != "21234" {
		t.Fatalf("channel 10 DVRStationRef = %q, want 21234", got)
	}
	if got := byChannelID[11].DVRStationRef; got != "24504" {
		t.Fatalf("channel 11 DVRStationRef = %q, want 24504", got)
	}
}

func TestServiceReverseSyncInfersLineupWhenDefaultUnset(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:       1,
			Provider: ProviderChannels,
			BaseURL:  "http://channels.lan:8089",
		},
		channels: []ChannelMapping{
			{
				ChannelID:   10,
				GuideNumber: "100",
				GuideName:   "CBS",
				Enabled:     true,
			},
			{
				ChannelID:        11,
				GuideNumber:      "101",
				GuideName:        "FOX",
				Enabled:          true,
				DVRLineupID:      "USA-TEST",
				DVRLineupChannel: "3",
			},
		},
	}

	provider := &fakeProvider{
		deviceChannels: map[string]DVRDeviceChannel{
			"A100": {Key: "A100", DeviceID: "8F07FDC6", Number: "100"},
			"A101": {Key: "A101", DeviceID: "8F07FDC6", Number: "101"},
		},
		stationsByLineup: map[string][]DVRStation{
			"USA-TEST": {
				{StationRef: "21234", LineupChannel: "2", CallSign: "WCCO"},
				{StationRef: "24504", LineupChannel: "3", CallSign: "KMSP"},
			},
		},
		customByLineup: map[string]map[string]string{
			"USA-TEST": {
				"A100": "21234",
				"A101": "24504",
			},
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	result, err := svc.ReverseSync(context.Background(), ReverseSyncRequest{DryRun: true})
	if err != nil {
		t.Fatalf("ReverseSync() error = %v", err)
	}
	if result.LineupID != "USA-TEST" {
		t.Fatalf("result.LineupID = %q, want USA-TEST", result.LineupID)
	}
	if result.ImportedCount != 2 {
		t.Fatalf("result.ImportedCount = %d, want 2", result.ImportedCount)
	}
}

func TestServiceReverseSyncChannelImportsOnlyTarget(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:              1,
			Provider:        ProviderChannels,
			BaseURL:         "http://channels.lan:8089",
			DefaultLineupID: "USA-TEST",
		},
		channels: []ChannelMapping{
			{
				ChannelID:        10,
				GuideNumber:      "100",
				GuideName:        "CBS",
				Enabled:          true,
				DVRLineupID:      "USA-TEST",
				DVRLineupChannel: "2",
				DVRStationRef:    "21234",
			},
			{
				ChannelID:   11,
				GuideNumber: "101",
				GuideName:   "FOX",
				Enabled:     true,
			},
		},
	}

	provider := &fakeProvider{
		deviceChannels: map[string]DVRDeviceChannel{
			"A100": {Key: "A100", DeviceID: "8F07FDC6", Number: "100"},
			"A101": {Key: "A101", DeviceID: "8F07FDC6", Number: "101"},
		},
		stationsByLineup: map[string][]DVRStation{
			"USA-TEST": {
				{StationRef: "21234", LineupChannel: "2", CallSign: "WCCO"},
				{StationRef: "24504", LineupChannel: "3", CallSign: "KMSP"},
			},
		},
		customByLineup: map[string]map[string]string{
			"USA-TEST": {
				"A100": "21234",
				"A101": "24504",
			},
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	result, err := svc.ReverseSyncChannel(context.Background(), 11, ReverseSyncRequest{})
	if err != nil {
		t.Fatalf("ReverseSyncChannel() error = %v", err)
	}

	if result.CandidateCount != 1 {
		t.Fatalf("result.CandidateCount = %d, want 1", result.CandidateCount)
	}
	if result.ImportedCount != 1 {
		t.Fatalf("result.ImportedCount = %d, want 1", result.ImportedCount)
	}
	if len(store.upsertCalls) != 1 {
		t.Fatalf("len(store.upsertCalls) = %d, want 1", len(store.upsertCalls))
	}
	if got := store.upsertCalls[0].ChannelID; got != 11 {
		t.Fatalf("upsert channel_id = %d, want 11", got)
	}
	if got := store.upsertCalls[0].DVRLineupChannel; got != "3" {
		t.Fatalf("upsert DVRLineupChannel = %q, want 3", got)
	}
}

func TestServiceReverseSyncChannelInfersLineupWhenUnset(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:       1,
			Provider: ProviderChannels,
			BaseURL:  "http://channels.lan:8089",
		},
		channels: []ChannelMapping{
			{
				ChannelID:   10,
				GuideNumber: "100",
				GuideName:   "CBS",
				Enabled:     true,
			},
			{
				ChannelID:        11,
				GuideNumber:      "101",
				GuideName:        "FOX",
				Enabled:          true,
				DVRLineupID:      "USA-TEST",
				DVRLineupChannel: "3",
			},
		},
	}

	provider := &fakeProvider{
		deviceChannels: map[string]DVRDeviceChannel{
			"A100": {Key: "A100", DeviceID: "8F07FDC6", Number: "100"},
			"A101": {Key: "A101", DeviceID: "8F07FDC6", Number: "101"},
		},
		stationsByLineup: map[string][]DVRStation{
			"USA-TEST": {
				{StationRef: "21234", LineupChannel: "2", CallSign: "WCCO"},
				{StationRef: "24504", LineupChannel: "3", CallSign: "KMSP"},
			},
		},
		customByLineup: map[string]map[string]string{
			"USA-TEST": {
				"A100": "21234",
				"A101": "24504",
			},
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	result, err := svc.ReverseSyncChannel(context.Background(), 10, ReverseSyncRequest{DryRun: true})
	if err != nil {
		t.Fatalf("ReverseSyncChannel() error = %v", err)
	}
	if result.LineupID != "USA-TEST" {
		t.Fatalf("result.LineupID = %q, want USA-TEST", result.LineupID)
	}
	if result.ImportedCount != 1 {
		t.Fatalf("result.ImportedCount = %d, want 1", result.ImportedCount)
	}
}

func TestServiceReverseSyncImportsStationRefOnlyWhenLineupChannelMissing(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:              1,
			Provider:        ProviderChannels,
			BaseURL:         "http://channels.lan:8089",
			DefaultLineupID: "USA-TEST",
		},
		channels: []ChannelMapping{
			{
				ChannelID:   12,
				GuideNumber: "111",
				GuideName:   "US Al Jazeera",
				Enabled:     true,
			},
		},
	}

	provider := &fakeProvider{
		deviceChannels: map[string]DVRDeviceChannel{
			"111": {Key: "111", DeviceID: "8F07FDC6", Number: "111"},
		},
		stationsByLineup: map[string][]DVRStation{
			"USA-TEST": {
				{StationRef: "72078", LineupChannel: "", CallSign: "RTENEWS"},
			},
		},
		customByLineup: map[string]map[string]string{
			"USA-TEST": {"111": "72078"},
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	result, err := svc.ReverseSync(context.Background(), ReverseSyncRequest{})
	if err != nil {
		t.Fatalf("ReverseSync() error = %v", err)
	}
	if result.ImportedCount != 1 {
		t.Fatalf("result.ImportedCount = %d, want 1", result.ImportedCount)
	}
	if len(store.upsertCalls) != 1 {
		t.Fatalf("len(store.upsertCalls) = %d, want 1", len(store.upsertCalls))
	}
	if got := store.upsertCalls[0].DVRStationRef; got != "72078" {
		t.Fatalf("upsert DVRStationRef = %q, want 72078", got)
	}
	if got := store.upsertCalls[0].DVRLineupChannel; got != "" {
		t.Fatalf("upsert DVRLineupChannel = %q, want empty", got)
	}
}

func TestServiceSyncUsesStationRefOnlyMapping(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:       1,
			Provider: ProviderChannels,
			BaseURL:  "http://channels.lan:8089",
			SyncMode: SyncModeConfiguredOnly,
		},
		channels: []ChannelMapping{
			{
				ChannelID:        12,
				GuideNumber:      "111",
				GuideName:        "US Al Jazeera",
				Enabled:          true,
				DVRInstanceID:    1,
				DVRLineupID:      "USA-TEST",
				DVRLineupChannel: "",
				DVRStationRef:    "72078",
			},
		},
	}

	provider := &fakeProvider{
		deviceChannels: map[string]DVRDeviceChannel{
			"111": {Key: "111", DeviceID: "8F07FDC6", Number: "111"},
		},
		stationsByLineup: map[string][]DVRStation{
			"USA-TEST": {
				{StationRef: "72078", LineupChannel: "", CallSign: "RTENEWS"},
			},
		},
		customByLineup: map[string]map[string]string{
			"USA-TEST": {"111": "99999"},
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.providerBuild = func(_ InstanceConfig, _ *http.Client) (DVRProvider, error) {
		return provider, nil
	}

	result, err := svc.Sync(context.Background(), SyncRequest{DryRun: true})
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if result.UpdatedCount != 1 {
		t.Fatalf("result.UpdatedCount = %d, want 1", result.UpdatedCount)
	}
	if got := result.PatchPreview["USA-TEST"]["111"]; got != "72078" {
		t.Fatalf("patch preview 111 = %q, want 72078", got)
	}
}

func TestServiceSetLastSyncStoresDeepCopy(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:       1,
			Provider: ProviderChannels,
			BaseURL:  "http://channels.lan:8089",
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	result := SyncResult{
		Warnings: []string{"initial warning"},
		Lineups: []LineupSyncResult{
			{
				LineupID:     "USA-TEST",
				UpdatedCount: 1,
			},
		},
		PatchPreview: map[string]map[string]string{
			"USA-TEST": {
				"A100": "21234",
			},
		},
	}

	svc.setLastSync(result)

	result.Warnings[0] = "mutated warning"
	result.Lineups[0].LineupID = "MUTATED"
	result.Lineups[0].UpdatedCount = 99
	result.PatchPreview["USA-TEST"]["A100"] = "99999"
	result.PatchPreview["USA-TEST"]["A101"] = "24504"
	result.PatchPreview["NEW"] = map[string]string{"A102": "11111"}

	state, err := svc.GetState(context.Background())
	if err != nil {
		t.Fatalf("GetState() error = %v", err)
	}
	if state.LastSync == nil {
		t.Fatal("state.LastSync = nil, want non-nil")
	}

	if got := state.LastSync.Warnings[0]; got != "initial warning" {
		t.Fatalf("state.LastSync.Warnings[0] = %q, want initial warning", got)
	}
	if got := state.LastSync.Lineups[0].LineupID; got != "USA-TEST" {
		t.Fatalf("state.LastSync.Lineups[0].LineupID = %q, want USA-TEST", got)
	}
	if got := state.LastSync.Lineups[0].UpdatedCount; got != 1 {
		t.Fatalf("state.LastSync.Lineups[0].UpdatedCount = %d, want 1", got)
	}
	if got := state.LastSync.PatchPreview["USA-TEST"]["A100"]; got != "21234" {
		t.Fatalf("state.LastSync.PatchPreview[USA-TEST][A100] = %q, want 21234", got)
	}
	if got := state.LastSync.PatchPreview["USA-TEST"]["A101"]; got != "" {
		t.Fatalf("state.LastSync.PatchPreview[USA-TEST][A101] = %q, want empty", got)
	}
	if _, exists := state.LastSync.PatchPreview["NEW"]; exists {
		t.Fatalf("state.LastSync.PatchPreview[NEW] exists, want absent")
	}
}

func TestServiceGetStateLastSyncReturnsDeepCopy(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		instance: InstanceConfig{
			ID:       1,
			Provider: ProviderChannels,
			BaseURL:  "http://channels.lan:8089",
		},
	}

	svc, err := NewService(store, "8F07FDC6", nil)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	svc.setLastSync(SyncResult{
		Warnings: []string{"initial warning"},
		Lineups: []LineupSyncResult{
			{
				LineupID:     "USA-TEST",
				UpdatedCount: 1,
			},
		},
		PatchPreview: map[string]map[string]string{
			"USA-TEST": {
				"A100": "21234",
			},
		},
	})

	state, err := svc.GetState(context.Background())
	if err != nil {
		t.Fatalf("GetState() error = %v", err)
	}
	if state.LastSync == nil {
		t.Fatal("state.LastSync = nil, want non-nil")
	}

	state.LastSync.Warnings[0] = "mutated warning"
	state.LastSync.Lineups[0].LineupID = "MUTATED"
	state.LastSync.Lineups[0].UpdatedCount = 99
	state.LastSync.PatchPreview["USA-TEST"]["A100"] = "99999"
	state.LastSync.PatchPreview["NEW"] = map[string]string{"A102": "11111"}

	next, err := svc.GetState(context.Background())
	if err != nil {
		t.Fatalf("GetState() second call error = %v", err)
	}
	if next.LastSync == nil {
		t.Fatal("next.LastSync = nil, want non-nil")
	}

	if got := next.LastSync.Warnings[0]; got != "initial warning" {
		t.Fatalf("next.LastSync.Warnings[0] = %q, want initial warning", got)
	}
	if got := next.LastSync.Lineups[0].LineupID; got != "USA-TEST" {
		t.Fatalf("next.LastSync.Lineups[0].LineupID = %q, want USA-TEST", got)
	}
	if got := next.LastSync.Lineups[0].UpdatedCount; got != 1 {
		t.Fatalf("next.LastSync.Lineups[0].UpdatedCount = %d, want 1", got)
	}
	if got := next.LastSync.PatchPreview["USA-TEST"]["A100"]; got != "21234" {
		t.Fatalf("next.LastSync.PatchPreview[USA-TEST][A100] = %q, want 21234", got)
	}
	if _, exists := next.LastSync.PatchPreview["NEW"]; exists {
		t.Fatalf("next.LastSync.PatchPreview[NEW] exists, want absent")
	}
}

type fakeStore struct {
	instance      InstanceConfig
	channels      []ChannelMapping
	upsertCalls   []ChannelMapping
	mappingByChan map[int64]ChannelMapping
}

func (s *fakeStore) GetDVRInstance(context.Context) (InstanceConfig, error) {
	return s.instance, nil
}

func (s *fakeStore) UpsertDVRInstance(context.Context, InstanceConfig) (InstanceConfig, error) {
	return InstanceConfig{}, fmt.Errorf("not implemented")
}

func (s *fakeStore) ListChannelsForDVRSync(_ context.Context, _ int64, _ bool, _ bool) ([]ChannelMapping, error) {
	out := make([]ChannelMapping, len(s.channels))
	copy(out, s.channels)
	return out, nil
}

func (s *fakeStore) ListChannelsForDVRSyncPaged(
	_ context.Context,
	_ int64,
	_ bool,
	_ bool,
	limit int,
	offset int,
) ([]ChannelMapping, int, error) {
	out := make([]ChannelMapping, len(s.channels))
	copy(out, s.channels)
	total := len(out)
	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return []ChannelMapping{}, total, nil
	}
	if limit < 1 {
		limit = 1
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return out[offset:end], total, nil
}

func (s *fakeStore) GetChannelDVRMapping(_ context.Context, _ int64, channelID int64) (ChannelMapping, error) {
	if s.mappingByChan != nil {
		if mapping, ok := s.mappingByChan[channelID]; ok {
			return mapping, nil
		}
	}
	for _, mapping := range s.channels {
		if mapping.ChannelID == channelID {
			return mapping, nil
		}
	}
	return ChannelMapping{}, fmt.Errorf("not implemented")
}

func (s *fakeStore) UpsertChannelDVRMapping(_ context.Context, mapping ChannelMapping) (ChannelMapping, error) {
	s.upsertCalls = append(s.upsertCalls, mapping)
	if s.mappingByChan == nil {
		s.mappingByChan = map[int64]ChannelMapping{}
	}
	s.mappingByChan[mapping.ChannelID] = mapping
	for i := range s.channels {
		if s.channels[i].ChannelID == mapping.ChannelID {
			s.channels[i] = mapping
		}
	}
	return mapping, nil
}

func (s *fakeStore) DeleteChannelDVRMapping(context.Context, int64, int64) error {
	return fmt.Errorf("not implemented")
}

type fakePutCall struct {
	lineupID string
	patch    map[string]string
}

type fakeProvider struct {
	deviceChannels             map[string]DVRDeviceChannel
	lineups                    []DVRLineup
	listLineupsErr             error
	stationsByLineup           map[string][]DVRStation
	customByLineup             map[string]map[string]string
	putCalls                   []fakePutCall
	reloadCalls                []string
	reloadErr                  error
	refreshGuideStationsCalls  int
	refreshGuideStationsErr    error
	redownloadGuideLineupCalls []string
	redownloadGuideLineupErr   error
	providerType               ProviderType
}

func (p *fakeProvider) Type() ProviderType {
	if p.providerType != "" {
		return p.providerType
	}
	return ProviderChannels
}

func (p *fakeProvider) ListLineups(context.Context) ([]DVRLineup, error) {
	if p.listLineupsErr != nil {
		return nil, p.listLineupsErr
	}
	out := make([]DVRLineup, len(p.lineups))
	copy(out, p.lineups)
	return out, nil
}

func (p *fakeProvider) ReloadDeviceLineup(_ context.Context, deviceID string) error {
	p.reloadCalls = append(p.reloadCalls, deviceID)
	return p.reloadErr
}

func (p *fakeProvider) RefreshGuideStations(context.Context) error {
	p.refreshGuideStationsCalls++
	return p.refreshGuideStationsErr
}

func (p *fakeProvider) RedownloadGuideLineup(_ context.Context, lineupID string) error {
	p.redownloadGuideLineupCalls = append(p.redownloadGuideLineupCalls, strings.TrimSpace(lineupID))
	return p.redownloadGuideLineupErr
}

func (p *fakeProvider) ListDeviceChannels(context.Context) (map[string]DVRDeviceChannel, error) {
	out := make(map[string]DVRDeviceChannel, len(p.deviceChannels))
	for key, value := range p.deviceChannels {
		out[key] = value
	}
	return out, nil
}

func (p *fakeProvider) ListLineupStations(_ context.Context, lineupID string) ([]DVRStation, error) {
	out := make([]DVRStation, len(p.stationsByLineup[lineupID]))
	copy(out, p.stationsByLineup[lineupID])
	return out, nil
}

func (p *fakeProvider) GetCustomMapping(_ context.Context, lineupID string) (map[string]string, error) {
	src := p.customByLineup[lineupID]
	out := make(map[string]string, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out, nil
}

func (p *fakeProvider) PutCustomMapping(_ context.Context, lineupID string, patch map[string]string) error {
	copyPatch := make(map[string]string, len(patch))
	for key, value := range patch {
		copyPatch[key] = value
	}
	p.putCalls = append(p.putCalls, fakePutCall{lineupID: lineupID, patch: copyPatch})

	if p.customByLineup[lineupID] == nil {
		p.customByLineup[lineupID] = map[string]string{}
	}
	for key, value := range patch {
		p.customByLineup[lineupID][key] = value
	}
	return nil
}

func (p *fakeProvider) RefreshDevices(context.Context) error {
	return nil
}
