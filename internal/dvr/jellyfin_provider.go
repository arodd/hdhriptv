package dvr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	jellyfinTaskRefreshKey = "RefreshGuide"
)

// JellyfinProvider implements DVRProvider against Jellyfin Live TV APIs.
type JellyfinProvider struct {
	baseURL         string
	apiToken        string
	tunerHostIDHint string
	client          *http.Client
}

func NewJellyfinProvider(instance InstanceConfig, client *http.Client) *JellyfinProvider {
	baseURL := strings.TrimSpace(instance.BaseURL)
	if baseURL == "" {
		baseURL = defaultJellyfinBaseURL
	}
	baseURL = strings.TrimSuffix(baseURL, "/")
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &JellyfinProvider{
		baseURL:         baseURL,
		apiToken:        strings.TrimSpace(instance.JellyfinAPIToken),
		tunerHostIDHint: strings.TrimSpace(instance.JellyfinTunerHostID),
		client:          client,
	}
}

func (p *JellyfinProvider) Type() ProviderType {
	return ProviderJellyfin
}

func (p *JellyfinProvider) ListLineups(context.Context) ([]DVRLineup, error) {
	return nil, unsupportedOperationError(ProviderJellyfin, "list lineups")
}

func (p *JellyfinProvider) ReloadDeviceLineup(ctx context.Context, deviceID string) error {
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		return fmt.Errorf("%w: hdhr device id is required", ErrDVRSyncConfig)
	}

	liveTVConfig, err := p.getLiveTVConfig(ctx)
	if err != nil {
		return err
	}

	targetHost, err := p.selectTunerHost(deviceID, liveTVConfig.TunerHosts)
	if err != nil {
		return err
	}

	body, err := json.Marshal(targetHost)
	if err != nil {
		return fmt.Errorf("marshal jellyfin tuner host upsert payload: %w", err)
	}
	if err := p.request(ctx, http.MethodPost, "/LiveTv/TunerHosts", nil, bytes.NewReader(body), "application/json", nil); err != nil {
		return fmt.Errorf("jellyfin tuner-host refresh trigger failed: %w", err)
	}

	// Best-effort observability request; this endpoint may be permission-gated
	// on some Jellyfin deployments and is not required for refresh success.
	_, _ = p.refreshGuideTaskState(ctx)
	return nil
}

func (p *JellyfinProvider) RefreshGuideStations(context.Context) error {
	// Jellyfin lineup refresh is triggered by re-posting a tuner host object.
	return nil
}

func (p *JellyfinProvider) RedownloadGuideLineup(context.Context, string) error {
	// Jellyfin has no lineup-scoped guide redownload endpoint analogous to
	// Channels DVR's PUT /dvr/lineups/{lineupID}.
	return nil
}

func (p *JellyfinProvider) ListDeviceChannels(context.Context) (map[string]DVRDeviceChannel, error) {
	return nil, unsupportedOperationError(ProviderJellyfin, "list device channels")
}

func (p *JellyfinProvider) ListLineupStations(context.Context, string) ([]DVRStation, error) {
	return nil, unsupportedOperationError(ProviderJellyfin, "list lineup stations")
}

func (p *JellyfinProvider) GetCustomMapping(context.Context, string) (map[string]string, error) {
	return nil, unsupportedOperationError(ProviderJellyfin, "get custom mapping")
}

func (p *JellyfinProvider) PutCustomMapping(context.Context, string, map[string]string) error {
	return unsupportedOperationError(ProviderJellyfin, "put custom mapping")
}

func (p *JellyfinProvider) RefreshDevices(context.Context) error {
	return unsupportedOperationError(ProviderJellyfin, "refresh devices")
}

type jellyfinLiveTVConfig struct {
	TunerHosts []map[string]any `json:"TunerHosts"`
}

func (p *JellyfinProvider) getLiveTVConfig(ctx context.Context) (jellyfinLiveTVConfig, error) {
	var config jellyfinLiveTVConfig
	if err := p.request(ctx, http.MethodGet, "/System/Configuration/livetv", nil, nil, "", &config); err != nil {
		return jellyfinLiveTVConfig{}, fmt.Errorf("jellyfin live tv config request failed: %w", err)
	}
	if len(config.TunerHosts) == 0 {
		return jellyfinLiveTVConfig{}, fmt.Errorf("%w: jellyfin live tv config contains no tuner hosts", ErrDVRSyncConfig)
	}
	return config, nil
}

func (p *JellyfinProvider) refreshGuideTaskState(ctx context.Context) (string, error) {
	var tasks []struct {
		Key   string `json:"Key"`
		State string `json:"State"`
	}
	query := url.Values{}
	query.Set("isHidden", "false")
	if err := p.request(ctx, http.MethodGet, "/ScheduledTasks", query, nil, "", &tasks); err != nil {
		return "", fmt.Errorf("jellyfin scheduled tasks request failed: %w", err)
	}
	for _, task := range tasks {
		if strings.EqualFold(strings.TrimSpace(task.Key), jellyfinTaskRefreshKey) {
			return strings.TrimSpace(task.State), nil
		}
	}
	return "", nil
}

func (p *JellyfinProvider) selectTunerHost(deviceID string, tunerHosts []map[string]any) (map[string]any, error) {
	if len(tunerHosts) == 0 {
		return nil, fmt.Errorf("%w: no jellyfin tuner hosts configured", ErrDVRSyncConfig)
	}

	if overrideID := strings.TrimSpace(p.tunerHostIDHint); overrideID != "" {
		for _, host := range tunerHosts {
			if strings.EqualFold(strings.TrimSpace(stringValue(host["Id"])), overrideID) {
				return host, nil
			}
		}
		return nil, fmt.Errorf("%w: jellyfin tuner host override id %q was not found", ErrDVRSyncConfig, overrideID)
	}

	deviceID = strings.TrimSpace(deviceID)
	hdhrByDeviceID := make([]map[string]any, 0, 1)
	hdhrHosts := make([]map[string]any, 0, len(tunerHosts))
	for _, host := range tunerHosts {
		hostType := strings.ToLower(strings.TrimSpace(stringValue(host["Type"])))
		if hostType != "hdhomerun" {
			continue
		}
		hdhrHosts = append(hdhrHosts, host)
		if strings.EqualFold(strings.TrimSpace(stringValue(host["DeviceId"])), deviceID) {
			hdhrByDeviceID = append(hdhrByDeviceID, host)
		}
	}

	if len(hdhrByDeviceID) == 1 {
		return hdhrByDeviceID[0], nil
	}
	if len(hdhrByDeviceID) > 1 {
		return nil, fmt.Errorf(
			"%w: multiple jellyfin hdhomerun hosts matched device id %q; set jellyfin_tuner_host_id override",
			ErrDVRSyncConfig,
			deviceID,
		)
	}
	if len(hdhrHosts) == 1 {
		return hdhrHosts[0], nil
	}
	if len(hdhrHosts) > 1 {
		return nil, fmt.Errorf(
			"%w: unable to uniquely select jellyfin hdhomerun host for device id %q; set jellyfin_tuner_host_id override",
			ErrDVRSyncConfig,
			deviceID,
		)
	}

	return nil, fmt.Errorf("%w: no jellyfin hdhomerun tuner host found", ErrDVRSyncConfig)
}

func (p *JellyfinProvider) request(
	ctx context.Context,
	method, path string,
	query url.Values,
	body io.Reader,
	contentType string,
	out any,
) error {
	if strings.TrimSpace(p.apiToken) == "" {
		return fmt.Errorf("%w: jellyfin_api_token is required", ErrDVRSyncConfig)
	}

	fullURL := p.baseURL + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, body)
	if err != nil {
		return err
	}
	req.Header.Set("X-Emby-Token", p.apiToken)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return fmt.Errorf(
			"%s %s failed: %s: %s",
			method,
			path,
			resp.Status,
			strings.TrimSpace(string(respBody)),
		)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, path, err)
	}
	return nil
}

func unsupportedOperationError(provider ProviderType, operation string) error {
	return fmt.Errorf("%w: provider=%q does not support %s", ErrUnsupportedProvider, provider, strings.TrimSpace(operation))
}
