package dvr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestChannelsProviderGuideEndpointsAndPutMapping(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var (
		receivedPatchContentType string
		receivedPatch            map[string]string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/dvr/guide/channels":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"100": {"DeviceID":"8F07FDC6","Number":"100","Name":"CBS","CallSign":"WCCO","Station":"21234"},
				"101": {"DeviceID":"8F07FDC6","Number":"101","Name":"News","CallSign":"NEWS","Station":"97047"}
			}`))
		case r.Method == http.MethodGet && r.URL.Path == "/dvr/guide/stations/USA-TEST":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[
				{"channel":"100","callSign":"WCCO","stationId":"21234","name":"CBS"},
				{"channel":"101","callSign":"TNCKHD","stationId":"97047","name":"TeenNick"}
			]`))
		case r.Method == http.MethodGet && r.URL.Path == "/dvr/guide/stations/USA-TEST/custom":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"100":"21234","101":"97047"}`))
		case r.Method == http.MethodPut && r.URL.Path == "/dvr/guide/stations/USA-TEST":
			receivedPatchContentType = r.Header.Get("Content-Type")
			defer r.Body.Close()
			if err := json.NewDecoder(r.Body).Decode(&receivedPatch); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`true`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	provider := NewChannelsProvider(srv.URL, srv.Client())

	channelsMap, err := provider.ListDeviceChannels(ctx)
	if err != nil {
		t.Fatalf("ListDeviceChannels() error = %v", err)
	}
	if len(channelsMap) != 2 {
		t.Fatalf("len(ListDeviceChannels) = %d, want 2", len(channelsMap))
	}
	if channelsMap["100"].StationRef != "21234" {
		t.Fatalf("channelsMap[100].StationRef = %q, want 21234", channelsMap["100"].StationRef)
	}
	if channelsMap["101"].DeviceID != "8F07FDC6" {
		t.Fatalf("channelsMap[101].DeviceID = %q, want 8F07FDC6", channelsMap["101"].DeviceID)
	}

	stations, err := provider.ListLineupStations(ctx, "USA-TEST")
	if err != nil {
		t.Fatalf("ListLineupStations() error = %v", err)
	}
	if len(stations) != 2 {
		t.Fatalf("len(ListLineupStations) = %d, want 2", len(stations))
	}
	if stations[1].StationRef != "97047" {
		t.Fatalf("stations[1].StationRef = %q, want 97047", stations[1].StationRef)
	}

	mapping, err := provider.GetCustomMapping(ctx, "USA-TEST")
	if err != nil {
		t.Fatalf("GetCustomMapping() error = %v", err)
	}
	if mapping["100"] != "21234" || mapping["101"] != "97047" {
		t.Fatalf("mapping = %#v, want 100->21234 and 101->97047", mapping)
	}

	if err := provider.PutCustomMapping(ctx, "USA-TEST", map[string]string{"101": ""}); err != nil {
		t.Fatalf("PutCustomMapping() error = %v", err)
	}
	if receivedPatchContentType != "text/plain;charset=UTF-8" {
		t.Fatalf("received Content-Type = %q, want text/plain;charset=UTF-8", receivedPatchContentType)
	}
	if receivedPatch["101"] != "" {
		t.Fatalf("received patch = %#v, want 101->\"\"", receivedPatch)
	}
}

func TestChannelsProviderListLineupsFallbackToDevices(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/dvr/lineups":
			http.Error(w, "not found", http.StatusNotFound)
		case r.Method == http.MethodGet && r.URL.Path == "/devices":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[
				{"Name":"HDHR IPTV","Lineup":"USA-MN22577-X"},
				{"Name":"Another Device","Sources":[{"Lineup":"USA-MN22577-X"},{"Lineup":"USA-WI99999-X"}]}
			]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	provider := NewChannelsProvider(srv.URL, srv.Client())
	lineups, err := provider.ListLineups(ctx)
	if err != nil {
		t.Fatalf("ListLineups() error = %v", err)
	}
	if len(lineups) != 2 {
		t.Fatalf("len(ListLineups) = %d, want 2", len(lineups))
	}

	ids := []string{lineups[0].ID, lineups[1].ID}
	got := strings.Join(ids, ",")
	if got != "USA-MN22577-X,USA-WI99999-X" {
		t.Fatalf("lineup ids = %q, want %q", got, "USA-MN22577-X,USA-WI99999-X")
	}
}

func TestChannelsProviderListLineupsFromLineupsMap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/dvr/lineups":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"8F07FDC6":"USA-MN22577-X",
					"A1B2C3D4":"USA-OTA55369",
				"VIRTUAL":"X-VIRTUAL"
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	provider := NewChannelsProvider(srv.URL, srv.Client())
	lineups, err := provider.ListLineups(ctx)
	if err != nil {
		t.Fatalf("ListLineups() error = %v", err)
	}
	if len(lineups) != 3 {
		t.Fatalf("len(ListLineups) = %d, want 3", len(lineups))
	}
	ids := []string{lineups[0].ID, lineups[1].ID, lineups[2].ID}
	got := strings.Join(ids, ",")
	if got != "USA-MN22577-X,USA-OTA55369,X-VIRTUAL" {
		t.Fatalf("lineup ids = %q, want %q", got, "USA-MN22577-X,USA-OTA55369,X-VIRTUAL")
	}
}

func TestChannelsProviderRefreshDevicesFallback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var (
		scanCalled    bool
		devicesCalled bool
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/dvr/scanner/scan":
			scanCalled = true
			http.Error(w, "not found", http.StatusNotFound)
		case r.Method == http.MethodPost && r.URL.Path == "/devices":
			devicesCalled = true
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	provider := NewChannelsProvider(srv.URL, srv.Client())
	if err := provider.RefreshDevices(ctx); err != nil {
		t.Fatalf("RefreshDevices() error = %v", err)
	}
	if !scanCalled {
		t.Fatal("expected PUT /dvr/scanner/scan to be called")
	}
	if !devicesCalled {
		t.Fatal("expected POST /devices fallback to be called")
	}
}

func TestChannelsProviderRefreshGuideStations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	called := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/dvr/guide/stations":
			called = true
			if r.ContentLength != 0 {
				t.Fatalf("PUT /dvr/guide/stations ContentLength = %d, want 0", r.ContentLength)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`true`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	provider := NewChannelsProvider(srv.URL, srv.Client())
	if err := provider.RefreshGuideStations(ctx); err != nil {
		t.Fatalf("RefreshGuideStations() error = %v", err)
	}
	if !called {
		t.Fatal("expected PUT /dvr/guide/stations to be called")
	}
}

func TestChannelsProviderRedownloadGuideLineup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	called := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/dvr/lineups/USA-MN22577-X":
			called = true
			if r.ContentLength != 0 {
				t.Fatalf("PUT /dvr/lineups/{id} ContentLength = %d, want 0", r.ContentLength)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`true`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	provider := NewChannelsProvider(srv.URL, srv.Client())
	if err := provider.RedownloadGuideLineup(ctx, "USA-MN22577-X"); err != nil {
		t.Fatalf("RedownloadGuideLineup() error = %v", err)
	}
	if !called {
		t.Fatal("expected PUT /dvr/lineups/{lineupID} to be called")
	}
}

func TestChannelsProviderReloadDeviceLineupAcceptsChannelList400(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	jsonBody := `[{"GuideNumber":"100","GuideName":"US CBS 4","Station":"21234"}]`
	tests := []struct {
		name string
		body string
	}{
		{name: "json", body: jsonBody},
		{name: "base64", body: base64.StdEncoding.EncodeToString([]byte(jsonBody))},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/devices/8F07FDC6/channels":
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(tc.body))
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			provider := NewChannelsProvider(srv.URL, srv.Client())
			if err := provider.ReloadDeviceLineup(ctx, "8F07FDC6"); err != nil {
				t.Fatalf("ReloadDeviceLineup() error = %v", err)
			}
		})
	}
}

func TestChannelsProviderReloadDeviceLineupFailsOnNonChannelList400(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name string
		body string
	}{
		{name: "plain text", body: "bad request"},
		{name: "json string array", body: `["bad request"]`},
		{name: "json error object array", body: `[{"error":"bad request"}]`},
		{name: "base64 json string array", body: base64.StdEncoding.EncodeToString([]byte(`["bad request"]`))},
		{name: "base64 json error object array", body: base64.StdEncoding.EncodeToString([]byte(`[{"error":"bad request"}]`))},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/devices/8F07FDC6/channels":
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(tc.body))
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			provider := NewChannelsProvider(srv.URL, srv.Client())
			if err := provider.ReloadDeviceLineup(ctx, "8F07FDC6"); err == nil {
				t.Fatal("ReloadDeviceLineup() error = nil, want non-nil")
			}
		})
	}
}
