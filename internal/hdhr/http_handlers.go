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
		FriendlyName:    h.friendlyName,
		DeviceID:        h.deviceID,
		DeviceAuth:      h.deviceAuth,
		BaseURL:         baseURL,
		LineupURL:       baseURL + "/lineup.json",
		ModelNumber:     "HDHR-EMULATOR",
		FirmwareName:    "hdhomerun_iptv",
		FirmwareVersion: time.Now().UTC().Format("20060102"),
		TunerCount:      h.tunerCount,
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) LineupJSON(w http.ResponseWriter, r *http.Request) {
	entries, err := h.lineupEntries(r.Context(), BaseURL(r))
	if err != nil {
		http.Error(w, fmt.Sprintf("build lineup: %v", err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, entries)
}

func (h *Handler) LineupM3U(w http.ResponseWriter, r *http.Request) {
	entries, err := h.lineupEntries(r.Context(), BaseURL(r))
	if err != nil {
		http.Error(w, fmt.Sprintf("build lineup: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-mpegURL")
	_, _ = w.Write([]byte("#EXTM3U\n"))
	for _, entry := range entries {
		_, _ = w.Write([]byte(fmt.Sprintf("#EXTINF:-1 tvg-chno=\"%s\",%s\n", entry.GuideNumber, entry.GuideName)))
		_, _ = w.Write([]byte(entry.URL + "\n"))
	}
}

func (h *Handler) LineupXML(w http.ResponseWriter, r *http.Request) {
	entries, err := h.lineupEntries(r.Context(), BaseURL(r))
	if err != nil {
		http.Error(w, fmt.Sprintf("build lineup: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
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
		Source:         "Cable",
		SourceList:     []string{"Cable"},
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

	w.Header().Set("Content-Type", "application/xml")
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

	entries := make([]LineupEntry, 0, len(publishedChannels))
	for _, channel := range publishedChannels {
		entries = append(entries, LineupEntry{
			GuideNumber: channel.GuideNumber,
			GuideName:   channel.GuideName,
			URL:         baseURL + "/auto/v" + channel.GuideNumber,
			Tags:        "favorite",
		})
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}
