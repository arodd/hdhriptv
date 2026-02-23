package dvr

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestJellyfinProviderReloadDeviceLineupByDeviceID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var (
		receivedAuthHeaders []string
		receivedQueryAuth   []string
		postedHost          map[string]any
		scheduledTasksCalls int
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuthHeaders = append(receivedAuthHeaders, r.Header.Get("X-Emby-Token"))
		receivedQueryAuth = append(receivedQueryAuth, r.URL.Query().Get("api_key"))

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/System/Configuration/livetv":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"TunerHosts": [
					{"Id":"host-m3u","Type":"m3u","DeviceId":"","FriendlyName":"M3U"},
					{"Id":"host-hdhr","Type":"hdhomerun","DeviceId":"8F07FDC6","FriendlyName":"HDHR IPTV","Url":"http://10.0.0.2","TunerCount":6}
				]
			}`))
		case r.Method == http.MethodPost && r.URL.Path == "/LiveTv/TunerHosts":
			defer r.Body.Close()
			if err := json.NewDecoder(r.Body).Decode(&postedHost); err != nil {
				t.Fatalf("decode POST /LiveTv/TunerHosts body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"Id":"host-hdhr"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/ScheduledTasks":
			scheduledTasksCalls++
			if got := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("isHidden"))); got != "false" {
				t.Fatalf("ScheduledTasks isHidden query = %q, want false", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"Key":"RefreshGuide","State":"Running"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	provider := NewJellyfinProvider(InstanceConfig{
		Provider:         ProviderJellyfin,
		BaseURL:          srv.URL,
		JellyfinAPIToken: "token-abc",
	}, srv.Client())

	if err := provider.ReloadDeviceLineup(ctx, "8F07FDC6"); err != nil {
		t.Fatalf("ReloadDeviceLineup() error = %v", err)
	}

	if postedHost == nil {
		t.Fatal("expected POST /LiveTv/TunerHosts payload")
	}
	if got, want := strings.TrimSpace(stringValue(postedHost["Id"])), "host-hdhr"; got != want {
		t.Fatalf("posted host id = %q, want %q", got, want)
	}
	if got, want := strings.TrimSpace(stringValue(postedHost["DeviceId"])), "8F07FDC6"; got != want {
		t.Fatalf("posted host device id = %q, want %q", got, want)
	}
	if scheduledTasksCalls != 1 {
		t.Fatalf("ScheduledTasks call count = %d, want 1", scheduledTasksCalls)
	}

	for i, headerValue := range receivedAuthHeaders {
		if headerValue != "token-abc" {
			t.Fatalf("X-Emby-Token header[%d] = %q, want token-abc", i, headerValue)
		}
	}
	for i, queryToken := range receivedQueryAuth {
		if queryToken != "" {
			t.Fatalf("api_key query auth[%d] = %q, want empty", i, queryToken)
		}
	}
}

func TestJellyfinProviderReloadDeviceLineupUsesHostOverride(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var postedHostID string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/System/Configuration/livetv":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"TunerHosts": [
					{"Id":"override-target","Type":"hdhomerun","DeviceId":"OTHER"},
					{"Id":"other-host","Type":"hdhomerun","DeviceId":"NOT-MATCHED"}
				]
			}`))
		case r.Method == http.MethodPost && r.URL.Path == "/LiveTv/TunerHosts":
			defer r.Body.Close()
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode POST payload: %v", err)
			}
			postedHostID = strings.TrimSpace(stringValue(payload["Id"]))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"Id":"override-target"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/ScheduledTasks":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	provider := NewJellyfinProvider(InstanceConfig{
		Provider:            ProviderJellyfin,
		BaseURL:             srv.URL,
		JellyfinAPIToken:    "token-abc",
		JellyfinTunerHostID: "override-target",
	}, srv.Client())

	if err := provider.ReloadDeviceLineup(ctx, "8F07FDC6"); err != nil {
		t.Fatalf("ReloadDeviceLineup() error = %v", err)
	}
	if got, want := postedHostID, "override-target"; got != want {
		t.Fatalf("POST host ID = %q, want %q", got, want)
	}
}

func TestJellyfinProviderReloadDeviceLineupFailsWhenHostAmbiguous(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/System/Configuration/livetv":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"TunerHosts": [
					{"Id":"h1","Type":"hdhomerun","DeviceId":"NOT-MATCHED-1"},
					{"Id":"h2","Type":"hdhomerun","DeviceId":"NOT-MATCHED-2"}
				]
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	provider := NewJellyfinProvider(InstanceConfig{
		Provider:         ProviderJellyfin,
		BaseURL:          srv.URL,
		JellyfinAPIToken: "token-abc",
	}, srv.Client())

	err := provider.ReloadDeviceLineup(ctx, "8F07FDC6")
	if err == nil {
		t.Fatal("ReloadDeviceLineup() error = nil, want non-nil")
	}
	if !errors.Is(err, ErrDVRSyncConfig) {
		t.Fatalf("ReloadDeviceLineup() error = %v, want ErrDVRSyncConfig", err)
	}
}

func TestJellyfinProviderRequiresToken(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	provider := NewJellyfinProvider(InstanceConfig{
		Provider: ProviderJellyfin,
		BaseURL:  "http://jellyfin.example.invalid:8096",
	}, nil)

	err := provider.ReloadDeviceLineup(ctx, "8F07FDC6")
	if err == nil {
		t.Fatal("ReloadDeviceLineup() error = nil, want non-nil")
	}
	if !errors.Is(err, ErrDVRSyncConfig) {
		t.Fatalf("ReloadDeviceLineup() error = %v, want ErrDVRSyncConfig", err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "jellyfin_api_token") {
		t.Fatalf("ReloadDeviceLineup() error = %q, want jellyfin_api_token context", err.Error())
	}
}

func TestJellyfinProviderListLineupsUnsupported(t *testing.T) {
	t.Parallel()

	provider := NewJellyfinProvider(InstanceConfig{
		Provider:         ProviderJellyfin,
		BaseURL:          "http://jellyfin.example.invalid:8096",
		JellyfinAPIToken: "token-abc",
	}, nil)

	ctx := context.Background()
	_, err := provider.ListLineups(ctx)
	if !errors.Is(err, ErrUnsupportedProvider) {
		t.Fatalf("ListLineups() error = %v, want ErrUnsupportedProvider", err)
	}
}

func TestJellyfinProviderDoesNotImplementMappingProvider(t *testing.T) {
	t.Parallel()

	provider := NewJellyfinProvider(InstanceConfig{
		Provider:         ProviderJellyfin,
		BaseURL:          "http://jellyfin.example.invalid:8096",
		JellyfinAPIToken: "token-abc",
	}, nil)

	if _, ok := any(provider).(MappingProvider); ok {
		t.Fatal("JellyfinProvider unexpectedly satisfies MappingProvider")
	}
}

func TestJellyfinProviderRequestDoesNotUseQueryAPIKeyAuth(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var rawQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"TunerHosts":[{"Id":"h1","Type":"hdhomerun","DeviceId":"8F07FDC6"}]}`))
	}))
	defer srv.Close()

	provider := NewJellyfinProvider(InstanceConfig{
		Provider:         ProviderJellyfin,
		BaseURL:          srv.URL,
		JellyfinAPIToken: "token-abc",
	}, srv.Client())

	_, err := provider.getLiveTVConfig(ctx)
	if err != nil {
		t.Fatalf("getLiveTVConfig() error = %v", err)
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		t.Fatalf("ParseQuery(rawQuery) error = %v", err)
	}
	if got := values.Get("api_key"); got != "" {
		t.Fatalf("api_key query parameter = %q, want empty", got)
	}
}
