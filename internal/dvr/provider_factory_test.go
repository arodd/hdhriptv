package dvr

import (
	"errors"
	"net/http"
	"testing"
)

var (
	_ LineupReloadProvider = (*ChannelsProvider)(nil)
	_ MappingProvider      = (*ChannelsProvider)(nil)
	_ LineupReloadProvider = (*JellyfinProvider)(nil)
)

func TestNewLineupReloadProviderSupportsJellyfin(t *testing.T) {
	t.Parallel()

	provider, err := newLineupReloadProvider(InstanceConfig{
		Provider:         ProviderType("JeLlYfIn"),
		BaseURL:          "http://jellyfin.lan:8096",
		JellyfinAPIToken: "token-abc",
	}, &http.Client{})
	if err != nil {
		t.Fatalf("newLineupReloadProvider(jellyfin) error = %v", err)
	}
	if got, want := provider.Type(), ProviderJellyfin; got != want {
		t.Fatalf("provider.Type() = %q, want %q", got, want)
	}
}

func TestNewMappingProviderRejectsJellyfin(t *testing.T) {
	t.Parallel()

	_, err := newMappingProvider(InstanceConfig{
		Provider:         ProviderJellyfin,
		BaseURL:          "http://jellyfin.lan:8096",
		JellyfinAPIToken: "token-abc",
	}, &http.Client{})
	if err == nil {
		t.Fatal("newMappingProvider(jellyfin) error = nil, want non-nil")
	}
	if !errors.Is(err, ErrUnsupportedProvider) {
		t.Fatalf("newMappingProvider(jellyfin) error = %v, want ErrUnsupportedProvider", err)
	}
}

func TestNewLineupReloadProviderRejectsUnknownProvider(t *testing.T) {
	t.Parallel()

	_, err := newLineupReloadProvider(InstanceConfig{
		Provider: "unsupported-provider",
		BaseURL:  "http://example.invalid",
	}, &http.Client{})
	if err == nil {
		t.Fatal("newLineupReloadProvider() error = nil, want non-nil")
	}
	if !errors.Is(err, ErrUnsupportedProvider) {
		t.Fatalf("newLineupReloadProvider() error = %v, want ErrUnsupportedProvider", err)
	}
}

func TestNewMappingProviderRejectsChannelsWithoutBaseURL(t *testing.T) {
	t.Parallel()

	_, err := newMappingProvider(InstanceConfig{
		Provider: ProviderChannels,
		BaseURL:  "",
	}, &http.Client{})
	if err == nil {
		t.Fatal("newMappingProvider(channels blank base URL) error = nil, want non-nil")
	}
	if !errors.Is(err, ErrDVRSyncConfig) {
		t.Fatalf("newMappingProvider(channels blank base URL) error = %v, want ErrDVRSyncConfig", err)
	}
}

func TestNormalizeProviderType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   ProviderType
		want ProviderType
	}{
		{in: ProviderType("channels"), want: ProviderChannels},
		{in: ProviderType("CHannels"), want: ProviderChannels},
		{in: ProviderType("jellyfin"), want: ProviderJellyfin},
		{in: ProviderType("JELLYFIN"), want: ProviderJellyfin},
		{in: ProviderType("unknown"), want: ProviderType("unknown")},
	}
	for _, tc := range tests {
		if got := normalizeProviderType(tc.in); got != tc.want {
			t.Fatalf("normalizeProviderType(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
