package hdhr

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

type fakeChannelsProvider struct {
	items              []channels.Channel
	sourcesByChannelID map[int64][]channels.Source
	listSourcesErr     error
}

func (f fakeChannelsProvider) ListEnabled(context.Context) ([]channels.Channel, error) {
	return f.items, nil
}

func (f fakeChannelsProvider) ListSourcesByChannelIDs(_ context.Context, channelIDs []int64, enabledOnly bool) (map[int64][]channels.Source, error) {
	if f.listSourcesErr != nil {
		return nil, f.listSourcesErr
	}
	out := make(map[int64][]channels.Source, len(channelIDs))
	for _, channelID := range channelIDs {
		sources := f.sourcesByChannelID[channelID]
		for _, source := range sources {
			if enabledOnly && !source.Enabled {
				continue
			}
			out[channelID] = append(out[channelID], source)
		}
	}
	return out, nil
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
	if out.TunerCount != 2 {
		t.Fatalf("TunerCount = %d, want 2", out.TunerCount)
	}
}

func TestDiscoverJSONCapsAdvertisedTunerCount(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   999,
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
	if out.TunerCount != 255 {
		t.Fatalf("TunerCount = %d, want 255 cap", out.TunerCount)
	}
}

func TestDiscoverJSONReflectsRuntimeAdvertisedTunerCountUpdates(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{})

	h.SetDiscoveryAdvertisedTunerCount(370)

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
	if out.TunerCount != 255 {
		t.Fatalf("TunerCount after runtime update = %d, want 255 cap", out.TunerCount)
	}

	h.SetDiscoveryAdvertisedTunerCount(7)
	w = httptest.NewRecorder()
	h.DiscoverJSON(w, r)
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response (second call): %v", err)
	}
	if out.TunerCount != 7 {
		t.Fatalf("TunerCount after second runtime update = %d, want 7", out.TunerCount)
	}
}

func TestNewHandlerContentDirectoryUpdateIDCacheTTL(t *testing.T) {
	customTTL := 3 * time.Second
	h := NewHandler(Config{
		FriendlyName:                     "HDHR IPTV",
		DeviceID:                         "1234ABCD",
		DeviceAuth:                       "token",
		TunerCount:                       2,
		ContentDirectoryUpdateIDCacheTTL: customTTL,
	}, fakeChannelsProvider{})
	if h.contentDirectoryUpdateIDCacheTTL != customTTL {
		t.Fatalf("contentDirectoryUpdateIDCacheTTL = %s, want %s", h.contentDirectoryUpdateIDCacheTTL, customTTL)
	}

	h = NewHandler(Config{
		FriendlyName:                     "HDHR IPTV",
		DeviceID:                         "1234ABCD",
		DeviceAuth:                       "token",
		TunerCount:                       2,
		ContentDirectoryUpdateIDCacheTTL: 0,
	}, fakeChannelsProvider{})
	if h.contentDirectoryUpdateIDCacheTTL != defaultContentDirectoryUpdateIDCacheTTL {
		t.Fatalf("contentDirectoryUpdateIDCacheTTL(0) = %s, want %s", h.contentDirectoryUpdateIDCacheTTL, defaultContentDirectoryUpdateIDCacheTTL)
	}

	h = NewHandler(Config{
		FriendlyName:                     "HDHR IPTV",
		DeviceID:                         "1234ABCD",
		DeviceAuth:                       "token",
		TunerCount:                       2,
		ContentDirectoryUpdateIDCacheTTL: -time.Second,
	}, fakeChannelsProvider{})
	if h.contentDirectoryUpdateIDCacheTTL != defaultContentDirectoryUpdateIDCacheTTL {
		t.Fatalf("contentDirectoryUpdateIDCacheTTL(-1s) = %s, want %s", h.contentDirectoryUpdateIDCacheTTL, defaultContentDirectoryUpdateIDCacheTTL)
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

	var out []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	if got, _ := out[0]["URL"].(string); got != "http://127.0.0.1:5004/auto/v101" {
		t.Fatalf("URL = %q, want http://127.0.0.1:5004/auto/v101", got)
	}
	if got, _ := out[0]["Tags"].(string); got != "favorite" {
		t.Fatalf("Tags = %q, want favorite", got)
	}
}

func TestLineupJSONOmitsExtendedFieldsForCompatibility(t *testing.T) {
	h := NewHandler(Config{FriendlyName: "x", DeviceID: "ABCD1234", DeviceAuth: "a", TunerCount: 2}, fakeChannelsProvider{
		items: []channels.Channel{
			{
				ChannelID:   1,
				GuideNumber: "101",
				GuideName:   "News One",
				Enabled:     true,
			},
			{
				ChannelID:   2,
				GuideNumber: "102",
				GuideName:   "Sports Two",
				Enabled:     true,
			},
		},
		sourcesByChannelID: map[int64][]channels.Source{
			1: {
				{
					SourceID:          10,
					ChannelID:         1,
					Enabled:           true,
					ProfileVideoCodec: "",
					ProfileAudioCodec: "",
				},
				{
					SourceID:          11,
					ChannelID:         1,
					Enabled:           true,
					ProfileVideoCodec: "hevc",
					ProfileAudioCodec: "eac3",
				},
			},
			2: {
				{
					SourceID:          20,
					ChannelID:         2,
					Enabled:           true,
					ProfileVideoCodec: "avc",
					ProfileAudioCodec: "ac3",
				},
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

	var out []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("len(out) = %d, want 2", len(out))
	}
	for _, key := range []string{"VideoCodec", "AudioCodec", "Subscribed", "Favorite", "HD", "DRM"} {
		if _, ok := out[0][key]; ok {
			t.Fatalf("lineup json unexpectedly included %s", key)
		}
	}
	if got, _ := out[0]["Tags"].(string); got != "favorite" {
		t.Fatalf("Tags = %q, want favorite", got)
	}
}

func TestPreferredLineupCodecsPrefersCompletePairOverEarlierPartial(t *testing.T) {
	got := preferredLineupCodecs([]channels.Source{
		{
			Enabled:           true,
			ProfileVideoCodec: "hevc",
			ProfileAudioCodec: "",
		},
		{
			Enabled:           true,
			ProfileVideoCodec: "avc",
			ProfileAudioCodec: "ac3",
		},
	})

	if got.Video != "H264" {
		t.Fatalf("Video = %q, want H264", got.Video)
	}
	if got.Audio != "AC3" {
		t.Fatalf("Audio = %q, want AC3", got.Audio)
	}
}

func TestPreferredLineupCodecsFallsBackToFirstPartialPair(t *testing.T) {
	got := preferredLineupCodecs([]channels.Source{
		{
			Enabled:           true,
			ProfileVideoCodec: "",
			ProfileAudioCodec: "eac3",
		},
		{
			Enabled:           true,
			ProfileVideoCodec: "avc",
			ProfileAudioCodec: "",
		},
	})

	if got.Video != "" {
		t.Fatalf("Video = %q, want empty", got.Video)
	}
	if got.Audio != "EAC3" {
		t.Fatalf("Audio = %q, want EAC3", got.Audio)
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

func TestLineupJSONShowDemoReturnsEmpty(t *testing.T) {
	h := NewHandler(Config{FriendlyName: "x", DeviceID: "ABCD1234", DeviceAuth: "a", TunerCount: 2}, fakeChannelsProvider{
		items: []channels.Channel{{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News One",
			Enabled:     true,
		}},
	})

	r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/lineup.json?show=demo", nil)
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
	if len(out) != 0 {
		t.Fatalf("len(out) = %d, want 0", len(out))
	}
}

func TestLineupJSONIgnoresListSourcesErrorForCompatibility(t *testing.T) {
	h := NewHandler(Config{FriendlyName: "x", DeviceID: "ABCD1234", DeviceAuth: "a", TunerCount: 2}, fakeChannelsProvider{
		items: []channels.Channel{{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News One",
			Enabled:     true,
		}},
		listSourcesErr: errors.New("boom"),
	})

	r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/lineup.json", nil)
	r.Host = "127.0.0.1:5004"
	w := httptest.NewRecorder()

	h.LineupJSON(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", w.Code, http.StatusOK)
	}

	var out []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	if got, _ := out[0]["GuideNumber"].(string); got != "101" {
		t.Fatalf("GuideNumber = %q, want 101", got)
	}
}

func TestLineupXMLListSourcesErrorReturnsInternalServerError(t *testing.T) {
	h := NewHandler(Config{FriendlyName: "x", DeviceID: "ABCD1234", DeviceAuth: "a", TunerCount: 2}, fakeChannelsProvider{
		items: []channels.Channel{{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News One",
			Enabled:     true,
		}},
		listSourcesErr: errors.New("boom"),
	})

	r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/lineup.xml", nil)
	r.Host = "127.0.0.1:5004"
	w := httptest.NewRecorder()

	h.LineupXML(w, r)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status code = %d, want %d", w.Code, http.StatusInternalServerError)
	}
	if body := w.Body.String(); !strings.Contains(body, "list channel sources for lineup: boom") {
		t.Fatalf("response body = %q, want source listing error details", body)
	}
}

func TestLineupXMLOmitsEmptyOptionalFields(t *testing.T) {
	h := NewHandler(Config{FriendlyName: "x", DeviceID: "ABCD1234", DeviceAuth: "a", TunerCount: 2}, fakeChannelsProvider{
		items: []channels.Channel{{
			ChannelID:   1,
			GuideNumber: "101",
			GuideName:   "News One",
			Enabled:     true,
		}},
	})

	r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/lineup.xml", nil)
	r.Host = "127.0.0.1:5004"
	w := httptest.NewRecorder()

	h.LineupXML(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if strings.Contains(body, "<Favorite>") {
		t.Fatalf("lineup xml unexpectedly included Favorite when zero-value; body=%q", body)
	}
	if strings.Contains(body, "<VideoCodec>") {
		t.Fatalf("lineup xml unexpectedly included VideoCodec when empty; body=%q", body)
	}
	if strings.Contains(body, "<AudioCodec>") {
		t.Fatalf("lineup xml unexpectedly included AudioCodec when empty; body=%q", body)
	}
	if !strings.Contains(body, "<Subscribed>1</Subscribed>") {
		t.Fatalf("lineup xml missing Subscribed element; body=%q", body)
	}
	if !strings.Contains(body, "<HD>1</HD>") {
		t.Fatalf("lineup xml missing HD element; body=%q", body)
	}
	if !strings.Contains(body, "<DRM>0</DRM>") {
		t.Fatalf("lineup xml missing DRM element; body=%q", body)
	}
}

func TestLineupStatusJSON(t *testing.T) {
	h := NewHandler(Config{FriendlyName: "x", DeviceID: "ABCD1234", DeviceAuth: "a", TunerCount: 2}, fakeChannelsProvider{})

	r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/lineup_status.json", nil)
	r.Host = "127.0.0.1:5004"
	w := httptest.NewRecorder()

	h.LineupStatusJSON(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", w.Code, http.StatusOK)
	}

	var out LineupStatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Source != "Antenna" {
		t.Fatalf("Source = %q, want Antenna", out.Source)
	}
	if out.ScanPossible != 1 {
		t.Fatalf("ScanPossible = %d, want 1", out.ScanPossible)
	}
	if len(out.SourceList) != 2 || out.SourceList[0] != "Antenna" || out.SourceList[1] != "Cable" {
		t.Fatalf("SourceList = %#v, want [Antenna Cable]", out.SourceList)
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
	if got := w.Header().Get("Content-Type"); got != `text/xml; charset="utf-8"` {
		t.Fatalf(`content type = %q, want text/xml; charset="utf-8"`, got)
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
