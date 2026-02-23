package dvr

import (
	"reflect"
	"testing"
)

func TestNormalizeInstanceConfigUsesCurrentProviderURLsAndDefaults(t *testing.T) {
	t.Parallel()

	current := InstanceConfig{
		ID:              7,
		Provider:        ProviderChannels,
		ChannelsBaseURL: "http://channels.current.invalid:8089",
		JellyfinBaseURL: "http://jellyfin.current.invalid:8096",
		BaseURL:         "http://channels.current.invalid:8089",
	}

	normalized := NormalizeInstanceConfig(InstanceConfig{
		Provider:            ProviderChannels,
		SyncEnabled:         true,
		SyncCron:            "",
		SyncMode:            "",
		JellyfinAPIToken:    "  token-abc  ",
		JellyfinTunerHostID: "  host-1  ",
	}, current)

	if got, want := normalized.ID, int64(7); got != want {
		t.Fatalf("normalized.ID = %d, want %d", got, want)
	}
	if got, want := normalized.ChannelsBaseURL, "http://channels.current.invalid:8089"; got != want {
		t.Fatalf("normalized.ChannelsBaseURL = %q, want %q", got, want)
	}
	if got, want := normalized.JellyfinBaseURL, "http://jellyfin.current.invalid:8096"; got != want {
		t.Fatalf("normalized.JellyfinBaseURL = %q, want %q", got, want)
	}
	if got, want := normalized.BaseURL, "http://channels.current.invalid:8089"; got != want {
		t.Fatalf("normalized.BaseURL = %q, want %q", got, want)
	}
	if got, want := normalized.SyncCron, defaultSyncCron; got != want {
		t.Fatalf("normalized.SyncCron = %q, want %q", got, want)
	}
	if got, want := normalized.SyncMode, SyncModeConfiguredOnly; got != want {
		t.Fatalf("normalized.SyncMode = %q, want %q", got, want)
	}
	if got, want := normalized.ActiveProviders, []ProviderType{ProviderChannels}; !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized.ActiveProviders = %#v, want %#v", got, want)
	}
	if got, want := normalized.JellyfinAPIToken, "token-abc"; got != want {
		t.Fatalf("normalized.JellyfinAPIToken = %q, want %q", got, want)
	}
	if got, want := normalized.JellyfinTunerHostID, "host-1"; got != want {
		t.Fatalf("normalized.JellyfinTunerHostID = %q, want %q", got, want)
	}
	if normalized.JellyfinAPITokenConfigured {
		t.Fatal("normalized.JellyfinAPITokenConfigured = true, want false for update payload")
	}
}

func TestNormalizeStoredInstanceFiltersUnconfiguredChannelsActiveProvider(t *testing.T) {
	t.Parallel()

	normalized := NormalizeStoredInstance(InstanceConfig{
		Provider:        ProviderType("CHANNELS"),
		ActiveProviders: []ProviderType{ProviderChannels, ProviderJellyfin},
		ChannelsBaseURL: "",
		JellyfinBaseURL: "http://jellyfin.example.invalid:8096",
	})

	if got, want := normalized.Provider, ProviderChannels; got != want {
		t.Fatalf("normalized.Provider = %q, want %q", got, want)
	}
	if got, want := normalized.ActiveProviders, []ProviderType{ProviderJellyfin}; !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized.ActiveProviders = %#v, want %#v", got, want)
	}
	if got, want := normalized.BaseURL, ""; got != want {
		t.Fatalf("normalized.BaseURL = %q, want %q", got, want)
	}
}

func TestNormalizeStoredInstanceFiltersUnconfiguredJellyfinActiveProvider(t *testing.T) {
	t.Parallel()

	normalized := NormalizeStoredInstance(InstanceConfig{
		Provider:        ProviderType("JELLYFIN"),
		ActiveProviders: []ProviderType{ProviderChannels, ProviderJellyfin},
		ChannelsBaseURL: "http://channels.example.invalid:8089",
		JellyfinBaseURL: "",
		BaseURL:         "",
	})

	if got, want := normalized.Provider, ProviderJellyfin; got != want {
		t.Fatalf("normalized.Provider = %q, want %q", got, want)
	}
	if got, want := normalized.ActiveProviders, []ProviderType{ProviderChannels}; !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized.ActiveProviders = %#v, want %#v", got, want)
	}
	if got, want := normalized.BaseURL, ""; got != want {
		t.Fatalf("normalized.BaseURL = %q, want %q", got, want)
	}
}

func TestNormalizeForProviderUsesProviderScopedBaseURL(t *testing.T) {
	t.Parallel()

	normalized := NormalizeForProvider(InstanceConfig{
		Provider:        ProviderChannels,
		ChannelsBaseURL: "http://channels.example.invalid:8089",
		JellyfinBaseURL: "http://jellyfin.example.invalid:8096",
		ActiveProviders: []ProviderType{ProviderChannels, ProviderJellyfin},
	}, ProviderJellyfin)

	if got, want := normalized.Provider, ProviderJellyfin; got != want {
		t.Fatalf("normalized.Provider = %q, want %q", got, want)
	}
	if got, want := normalized.BaseURL, "http://jellyfin.example.invalid:8096"; got != want {
		t.Fatalf("normalized.BaseURL = %q, want %q", got, want)
	}
	if got, want := normalized.ChannelsBaseURL, "http://channels.example.invalid:8089"; got != want {
		t.Fatalf("normalized.ChannelsBaseURL = %q, want %q", got, want)
	}
	if got, want := normalized.JellyfinBaseURL, "http://jellyfin.example.invalid:8096"; got != want {
		t.Fatalf("normalized.JellyfinBaseURL = %q, want %q", got, want)
	}
}
