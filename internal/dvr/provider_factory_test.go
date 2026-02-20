package dvr

import (
	"errors"
	"net/http"
	"testing"
)

func TestNewProviderSupportsJellyfin(t *testing.T) {
	t.Parallel()

	provider, err := newProvider(InstanceConfig{
		Provider:         ProviderType("JeLlYfIn"),
		BaseURL:          "http://jellyfin.lan:8096",
		JellyfinAPIToken: "token-abc",
	}, &http.Client{})
	if err != nil {
		t.Fatalf("newProvider(jellyfin) error = %v", err)
	}
	if got, want := provider.Type(), ProviderJellyfin; got != want {
		t.Fatalf("provider.Type() = %q, want %q", got, want)
	}
}

func TestNewProviderRejectsUnknownProvider(t *testing.T) {
	t.Parallel()

	_, err := newProvider(InstanceConfig{
		Provider: "unsupported-provider",
		BaseURL:  "http://example.invalid",
	}, &http.Client{})
	if err == nil {
		t.Fatal("newProvider() error = nil, want non-nil")
	}
	if !errors.Is(err, ErrUnsupportedProvider) {
		t.Fatalf("newProvider() error = %v, want ErrUnsupportedProvider", err)
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
