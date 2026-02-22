package hdhr

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

// ChannelsProvider provides enabled published channels for lineup responses.
type ChannelsProvider interface {
	ListEnabled(ctx context.Context) ([]channels.Channel, error)
}

type channelSourcesProvider interface {
	ListSourcesByChannelIDs(ctx context.Context, channelIDs []int64, enabledOnly bool) (map[int64][]channels.Source, error)
}

// Config carries static values used by HDHomeRun emulation handlers.
type Config struct {
	FriendlyName string
	DeviceID     string
	DeviceAuth   string
	TunerCount   int
	// ContentDirectoryUpdateIDCacheTTL controls how long GetSystemUpdateID values are cached.
	ContentDirectoryUpdateIDCacheTTL time.Duration
}

// Handler serves HDHomeRun-compatible HTTP endpoints.
type Handler struct {
	channelsProvider ChannelsProvider

	friendlyName string
	deviceID     string
	deviceAuth   string
	tunerCount   int

	contentDirectoryUpdateIDCacheMu    sync.Mutex
	contentDirectoryUpdateIDCacheValue int
	contentDirectoryUpdateIDCacheAt    time.Time
	contentDirectoryUpdateIDCacheTTL   time.Duration

	contentDirectoryUpdateIDRefreshMu    sync.Mutex
	contentDirectoryUpdateIDRefreshWait  chan struct{}
	contentDirectoryUpdateIDRefreshValue int
	contentDirectoryUpdateIDRefreshErr   error
}

func NewHandler(cfg Config, channelsProvider ChannelsProvider) *Handler {
	friendlyName := strings.TrimSpace(cfg.FriendlyName)
	if friendlyName == "" {
		friendlyName = "HDHR IPTV"
	}
	contentDirectoryUpdateIDCacheTTL := cfg.ContentDirectoryUpdateIDCacheTTL
	if contentDirectoryUpdateIDCacheTTL <= 0 {
		contentDirectoryUpdateIDCacheTTL = defaultContentDirectoryUpdateIDCacheTTL
	}

	return &Handler{
		channelsProvider:                 channelsProvider,
		friendlyName:                     friendlyName,
		deviceID:                         strings.ToUpper(strings.TrimSpace(cfg.DeviceID)),
		deviceAuth:                       strings.TrimSpace(cfg.DeviceAuth),
		tunerCount:                       cfg.TunerCount,
		contentDirectoryUpdateIDCacheTTL: contentDirectoryUpdateIDCacheTTL,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /discover.json", h.DiscoverJSON)
	mux.HandleFunc("GET /lineup.json", h.LineupJSON)
	mux.HandleFunc("GET /lineup.m3u", h.LineupM3U)
	mux.HandleFunc("GET /lineup.xml", h.LineupXML)
	mux.HandleFunc("GET /lineup_status.json", h.LineupStatusJSON)
	mux.HandleFunc("GET "+UPnPDescriptionPath, h.DeviceDescriptionXML)
	mux.HandleFunc("GET "+UPnPDescriptionAliasPath, h.DeviceDescriptionXML)
	mux.HandleFunc("GET "+UPnPConnectionSCPDPath, h.ConnectionManagerSCPDXML)
	mux.HandleFunc("GET "+UPnPContentDirectorySCPDPath, h.ContentDirectorySCPDXML)
	mux.HandleFunc("POST "+UPnPConnectionControlPath, h.ConnectionManagerControl)
	mux.HandleFunc("POST "+UPnPContentDirectoryControlPath, h.ContentDirectoryControl)
	mux.HandleFunc("GET /lineup.html", h.LineupHTML)
}

func (h *Handler) DiscoverJSON(w http.ResponseWriter, r *http.Request) {
	baseURL := BaseURL(r)
	resp := DiscoverResponse{
		FriendlyName: h.friendlyName,
		DeviceID:     h.deviceID,
		DeviceAuth:   h.deviceAuth,
		BaseURL:      baseURL,
		LineupURL:    baseURL + "/lineup.json",
		ModelNumber:  "HDFX-4K",
		FirmwareName: "hdhomerun_dvr_atsc3",
		// Keep firmware stable across restarts to mirror physical HDHomeRun payloads.
		FirmwareVersion: "20250815",
		TunerCount:      h.tunerCount,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) LineupJSON(w http.ResponseWriter, r *http.Request) {
	entries, err := h.lineupEntriesForRequest(r.Context(), r, BaseURL(r))
	if err != nil {
		http.Error(w, fmt.Sprintf("build lineup: %v", err), http.StatusInternalServerError)
		return
	}

	// Keep JSON lineup shape conservative for DVR compatibility.
	legacy := make([]lineupJSONEntry, 0, len(entries))
	for _, entry := range entries {
		legacy = append(legacy, lineupJSONEntry{
			GuideNumber: entry.GuideNumber,
			GuideName:   entry.GuideName,
			URL:         entry.URL,
			Tags:        "favorite",
		})
	}

	writeJSON(w, http.StatusOK, legacy)
}

func (h *Handler) LineupM3U(w http.ResponseWriter, r *http.Request) {
	entries, err := h.lineupEntriesForRequest(r.Context(), r, BaseURL(r))
	if err != nil {
		http.Error(w, fmt.Sprintf("build lineup: %v", err), http.StatusInternalServerError)
		return
	}

	setHDHRResponseHeaders(w)
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("#EXTM3U\n"))
	for _, entry := range entries {
		_, _ = w.Write([]byte(fmt.Sprintf("#EXTINF:-1 tvg-chno=\"%s\",%s\n", entry.GuideNumber, entry.GuideName)))
		_, _ = w.Write([]byte(entry.URL + "\n"))
	}
}

func (h *Handler) LineupXML(w http.ResponseWriter, r *http.Request) {
	entries, err := h.lineupEntriesForRequest(r.Context(), r, BaseURL(r))
	if err != nil {
		http.Error(w, fmt.Sprintf("build lineup: %v", err), http.StatusInternalServerError)
		return
	}

	setHDHRResponseHeaders(w)
	w.Header().Set("Content-Type", `text/xml; charset="utf-8"`)
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(LineupXMLResponse{Items: entries}); err != nil {
		http.Error(w, fmt.Sprintf("encode lineup xml: %v", err), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) LineupStatusJSON(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, LineupStatusResponse{
		ScanInProgress: 0,
		ScanPossible:   1,
		Source:         "Antenna",
		SourceList:     []string{"Antenna", "Cable"},
	})
}

func (h *Handler) DeviceDescriptionXML(w http.ResponseWriter, r *http.Request) {
	baseURL := BaseURL(r)
	payload := UPnPRootDescription{
		XMLNS:   "urn:schemas-upnp-org:device-1-0",
		Spec:    UPnPSpecVersion{Major: 1, Minor: 0},
		URLBase: baseURL,
		Device: UPnPDeviceDescription{
			DeviceType:      UPnPDeviceTypeMediaServer,
			FriendlyName:    h.friendlyName,
			Manufacturer:    "hdhriptv",
			ModelName:       "HDHR-EMULATOR",
			ModelNumber:     "HDHR-EMULATOR",
			SerialNumber:    h.deviceID,
			UDN:             DeviceUDN(h.deviceID),
			PresentationURL: baseURL + "/ui/",
			ServiceList: []UPnPServiceDescription{
				{
					ServiceType: UPnPServiceTypeConnection,
					ServiceID:   "urn:upnp-org:serviceId:ConnectionManager",
					SCPDURL:     UPnPConnectionSCPDPath,
					ControlURL:  UPnPConnectionControlPath,
					EventSubURL: UPnPConnectionEventPath,
				},
				{
					ServiceType: UPnPServiceTypeContentDir,
					ServiceID:   "urn:upnp-org:serviceId:ContentDirectory",
					SCPDURL:     UPnPContentDirectorySCPDPath,
					ControlURL:  UPnPContentDirectoryControlPath,
					EventSubURL: UPnPContentDirectoryEventPath,
				},
			},
		},
	}

	setHDHRResponseHeaders(w)
	w.Header().Set("Content-Type", `text/xml; charset="utf-8"`)
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(payload); err != nil {
		http.Error(w, fmt.Sprintf("encode upnp device xml: %v", err), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) LineupHTML(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/ui/", http.StatusFound)
}

func (h *Handler) lineupEntries(ctx context.Context, baseURL string) ([]LineupEntry, error) {
	if h.channelsProvider == nil {
		return nil, fmt.Errorf("channels provider is not configured")
	}

	publishedChannels, err := h.channelsProvider.ListEnabled(ctx)
	if err != nil {
		return nil, err
	}

	codecByChannelID := make(map[int64]lineupCodecs, len(publishedChannels))
	if sourcesProvider, ok := h.channelsProvider.(channelSourcesProvider); ok && len(publishedChannels) > 0 {
		channelIDs := make([]int64, 0, len(publishedChannels))
		for _, channel := range publishedChannels {
			channelIDs = append(channelIDs, channel.ChannelID)
		}
		sourcesByChannelID, err := sourcesProvider.ListSourcesByChannelIDs(ctx, channelIDs, true)
		if err != nil {
			return nil, fmt.Errorf("list channel sources for lineup: %w", err)
		}
		for _, channel := range publishedChannels {
			codecByChannelID[channel.ChannelID] = preferredLineupCodecs(sourcesByChannelID[channel.ChannelID])
		}
	}

	entries := make([]LineupEntry, 0, len(publishedChannels))
	for _, channel := range publishedChannels {
		codecs := codecByChannelID[channel.ChannelID]
		entries = append(entries, LineupEntry{
			GuideNumber: channel.GuideNumber,
			GuideName:   channel.GuideName,
			URL:         baseURL + "/auto/v" + channel.GuideNumber,
			Favorite:    0,
			Subscribed:  1,
			VideoCodec:  codecs.Video,
			AudioCodec:  codecs.Audio,
			HD:          1,
			DRM:         0,
		})
	}

	return entries, nil
}

type lineupCodecs struct {
	Video string
	Audio string
}

type lineupJSONEntry struct {
	GuideNumber string `json:"GuideNumber"`
	GuideName   string `json:"GuideName"`
	URL         string `json:"URL"`
	Tags        string `json:"Tags,omitempty"`
}

func preferredLineupCodecs(sources []channels.Source) lineupCodecs {
	fallback := lineupCodecs{}
	for _, source := range sources {
		video := normalizeLineupVideoCodec(source.ProfileVideoCodec)
		audio := normalizeLineupAudioCodec(source.ProfileAudioCodec)
		if video == "" && audio == "" {
			continue
		}
		if video != "" && audio != "" {
			return lineupCodecs{
				Video: video,
				Audio: audio,
			}
		}
		if fallback.Video == "" && fallback.Audio == "" {
			fallback = lineupCodecs{
				Video: video,
				Audio: audio,
			}
		}
	}
	return fallback
}

func normalizeLineupVideoCodec(codec string) string {
	switch strings.ToLower(strings.TrimSpace(codec)) {
	case "":
		return ""
	case "h264", "avc":
		return "H264"
	case "hevc", "h265":
		return "HEVC"
	case "mpeg2video", "mpeg2", "mpeg-2video":
		return "MPEG2"
	case "av1":
		return "AV1"
	default:
		return strings.ToUpper(strings.TrimSpace(codec))
	}
}

func normalizeLineupAudioCodec(codec string) string {
	switch strings.ToLower(strings.TrimSpace(codec)) {
	case "":
		return ""
	case "aac":
		return "AAC"
	case "ac3":
		return "AC3"
	case "eac3":
		return "EAC3"
	case "mp2":
		return "MP2"
	case "mp3":
		return "MP3"
	default:
		return strings.ToUpper(strings.TrimSpace(codec))
	}
}

func (h *Handler) lineupEntriesForRequest(ctx context.Context, r *http.Request, baseURL string) ([]LineupEntry, error) {
	entries, err := h.lineupEntries(ctx, baseURL)
	if err != nil {
		return nil, err
	}

	show := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("show")))
	if show == "demo" {
		return []LineupEntry{}, nil
	}
	return entries, nil
}

func BaseURL(r *http.Request) string {
	scheme := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	if scheme == "" {
		scheme = "http"
		if r.TLS != nil {
			scheme = "https"
		}
	}

	host := strings.TrimSpace(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = strings.TrimSpace(r.Host)
	}
	if host == "" {
		host = "localhost"
	}

	return scheme + "://" + host
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	setHDHRResponseHeaders(w)
	w.Header().Set("Content-Type", `application/json; charset="utf-8"`)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func setHDHRResponseHeaders(w http.ResponseWriter) {
	w.Header().Set("Server", "HDHomeRun/1.0")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Connection", "close")
}
