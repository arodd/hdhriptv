package hdhr

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/arodd/hdhriptv/internal/channels"
)

type fakeChannelsProvider struct {
	items []channels.Channel
}

func (f fakeChannelsProvider) ListEnabled(context.Context) ([]channels.Channel, error) {
	return f.items, nil
}

func TestDiscoverJSON(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{})

	r := httptest.NewRequest(http.MethodGet, "http://example.local/discover.json", nil)
	r.Host = "example.local:5004"
	w := httptest.NewRecorder()

	h.DiscoverJSON(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", w.Code, http.StatusOK)
	}

	var out DiscoverResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.BaseURL != "http://example.local:5004" {
		t.Fatalf("BaseURL = %q, want http://example.local:5004", out.BaseURL)
	}
	if out.LineupURL != "http://example.local:5004/lineup.json" {
		t.Fatalf("LineupURL = %q, want http://example.local:5004/lineup.json", out.LineupURL)
	}
	if out.DeviceID != "1234ABCD" {
		t.Fatalf("DeviceID = %q, want 1234ABCD", out.DeviceID)
	}
}

func TestLineupJSON(t *testing.T) {
	h := NewHandler(Config{FriendlyName: "x", DeviceID: "ABCD1234", DeviceAuth: "a", TunerCount: 2}, fakeChannelsProvider{
		items: []channels.Channel{{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News One",
			Enabled:     true,
		}},
	})

	r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/lineup.json", nil)
	r.Host = "127.0.0.1:5004"
	w := httptest.NewRecorder()

	h.LineupJSON(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", w.Code, http.StatusOK)
	}

	var out []LineupEntry
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	if out[0].URL != "http://127.0.0.1:5004/auto/v101" {
		t.Fatalf("URL = %q, want http://127.0.0.1:5004/auto/v101", out[0].URL)
	}
	if out[0].Tags != "favorite" {
		t.Fatalf("Tags = %q, want favorite", out[0].Tags)
	}
}

func TestLineupJSONIncludesDynamicGeneratedGuideRange(t *testing.T) {
	h := NewHandler(Config{FriendlyName: "x", DeviceID: "ABCD1234", DeviceAuth: "a", TunerCount: 2}, fakeChannelsProvider{
		items: []channels.Channel{
			{
				ChannelID:    1,
				ChannelClass: channels.ChannelClassTraditional,
				GuideNumber:  "101",
				GuideName:    "News One",
				Enabled:      true,
			},
			{
				ChannelID:      2,
				ChannelClass:   channels.ChannelClassDynamicGenerated,
				GuideNumber:    "10000",
				GuideName:      "Dynamic Match",
				Enabled:        true,
				DynamicQueryID: 9,
				DynamicItemKey: "src:dyn:one",
			},
		},
	})

	r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/lineup.json", nil)
	r.Host = "127.0.0.1:5004"
	w := httptest.NewRecorder()

	h.LineupJSON(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", w.Code, http.StatusOK)
	}

	var out []LineupEntry
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("len(out) = %d, want 2", len(out))
	}
	if out[1].GuideNumber != "10000" {
		t.Fatalf("out[1].GuideNumber = %q, want 10000", out[1].GuideNumber)
	}
	if out[1].URL != "http://127.0.0.1:5004/auto/v10000" {
		t.Fatalf("out[1].URL = %q, want http://127.0.0.1:5004/auto/v10000", out[1].URL)
	}
}

func TestBaseURLPrefersForwardedHeaders(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://localhost/discover.json", nil)
	r.Host = "localhost:5004"
	r.Header.Set("X-Forwarded-Proto", "https")
	r.Header.Set("X-Forwarded-Host", "hdhr.local")

	got := BaseURL(r)
	want := "https://hdhr.local"
	if got != want {
		t.Fatalf("BaseURL() = %q, want %q", got, want)
	}
}

func TestDeviceDescriptionXML(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{})

	r := httptest.NewRequest(http.MethodGet, "http://example.local/upnp/device.xml", nil)
	r.Host = "example.local:5004"
	w := httptest.NewRecorder()

	h.DeviceDescriptionXML(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get("Content-Type"); got != "application/xml" {
		t.Fatalf("content type = %q, want application/xml", got)
	}

	var out UPnPRootDescription
	if err := xml.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode xml response: %v", err)
	}

	if out.Device.FriendlyName != "HDHR IPTV" {
		t.Fatalf("friendlyName = %q, want HDHR IPTV", out.Device.FriendlyName)
	}
	if out.Device.DeviceType != UPnPDeviceTypeMediaServer {
		t.Fatalf("deviceType = %q, want %q", out.Device.DeviceType, UPnPDeviceTypeMediaServer)
	}
	if out.Device.UDN != DeviceUDN("1234ABCD") {
		t.Fatalf("UDN = %q, want %q", out.Device.UDN, DeviceUDN("1234ABCD"))
	}
	if out.URLBase != "http://example.local:5004" {
		t.Fatalf("URLBase = %q, want http://example.local:5004", out.URLBase)
	}
	if len(out.Device.ServiceList) != 2 {
		t.Fatalf("len(serviceList) = %d, want 2", len(out.Device.ServiceList))
	}
	if out.Device.ServiceList[0].ServiceType != UPnPServiceTypeConnection {
		t.Fatalf("serviceList[0].serviceType = %q, want %q", out.Device.ServiceList[0].ServiceType, UPnPServiceTypeConnection)
	}
	if out.Device.ServiceList[0].SCPDURL != UPnPConnectionSCPDPath {
		t.Fatalf("serviceList[0].SCPDURL = %q, want %q", out.Device.ServiceList[0].SCPDURL, UPnPConnectionSCPDPath)
	}
	if out.Device.ServiceList[1].ServiceType != UPnPServiceTypeContentDir {
		t.Fatalf("serviceList[1].serviceType = %q, want %q", out.Device.ServiceList[1].ServiceType, UPnPServiceTypeContentDir)
	}
	if out.Device.ServiceList[1].ControlURL != UPnPContentDirectoryControlPath {
		t.Fatalf("serviceList[1].controlURL = %q, want %q", out.Device.ServiceList[1].ControlURL, UPnPContentDirectoryControlPath)
	}
}

func TestRegisterRoutesDeviceDescriptionAlias(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{})

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	r := httptest.NewRequest(http.MethodGet, "http://example.local/device.xml", nil)
	r.Host = "example.local:5004"
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", w.Code, http.StatusOK)
	}

	var out UPnPRootDescription
	if err := xml.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode xml response: %v", err)
	}
	if out.Device.UDN != DeviceUDN("1234ABCD") {
		t.Fatalf("UDN = %q, want %q", out.Device.UDN, DeviceUDN("1234ABCD"))
	}
	if out.Device.DeviceType != UPnPDeviceTypeMediaServer {
		t.Fatalf("deviceType = %q, want %q", out.Device.DeviceType, UPnPDeviceTypeMediaServer)
	}
}

func TestDeviceUDNDeterministic(t *testing.T) {
	first := DeviceUDN("1234ABCD")
	second := DeviceUDN("1234abcd")
	if first != second {
		t.Fatalf("DeviceUDN case-insensitive mismatch: %q vs %q", first, second)
	}
	if !strings.HasPrefix(first, "uuid:") {
		t.Fatalf("DeviceUDN prefix = %q, want uuid:*", first)
	}
	if len(first) != len("uuid:00000000-0000-0000-0000-000000000000") {
		t.Fatalf("DeviceUDN length = %d, want %d", len(first), len("uuid:00000000-0000-0000-0000-000000000000"))
	}
}
