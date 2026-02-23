package dvr

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ChannelsProvider implements LineupReloadProvider and MappingProvider against
// Channels DVR server APIs.
type ChannelsProvider struct {
	baseURL string
	client  *http.Client
}

func NewChannelsProvider(baseURL string, client *http.Client) *ChannelsProvider {
	baseURL = strings.TrimSpace(baseURL)
	baseURL = strings.TrimSuffix(baseURL, "/")
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &ChannelsProvider{
		baseURL: baseURL,
		client:  client,
	}
}

func (p *ChannelsProvider) Type() ProviderType {
	return ProviderChannels
}

func (p *ChannelsProvider) ListDeviceChannels(ctx context.Context) (map[string]DVRDeviceChannel, error) {
	var raw map[string]struct {
		ID        string `json:"ID"`
		ChannelID string `json:"ChannelID"`
		DeviceID  string `json:"DeviceID"`
		Number    string `json:"Number"`
		Name      string `json:"Name"`
		CallSign  string `json:"CallSign"`
		Station   any    `json:"Station"`
	}
	if err := p.getJSON(ctx, "/dvr/guide/channels", &raw); err != nil {
		return nil, fmt.Errorf("channels guide channels request failed: %w", err)
	}

	out := make(map[string]DVRDeviceChannel, len(raw))
	for mapKey, entry := range raw {
		key := strings.TrimSpace(mapKey)
		if key == "" {
			key = strings.TrimSpace(entry.ID)
		}
		if key == "" {
			key = strings.TrimSpace(entry.ChannelID)
		}
		if key == "" {
			continue
		}

		out[key] = DVRDeviceChannel{
			Key:        key,
			DeviceID:   strings.TrimSpace(entry.DeviceID),
			Number:     strings.TrimSpace(entry.Number),
			Name:       strings.TrimSpace(entry.Name),
			CallSign:   strings.TrimSpace(entry.CallSign),
			StationRef: stringValue(entry.Station),
		}
	}
	return out, nil
}

func (p *ChannelsProvider) ListLineupStations(ctx context.Context, lineupID string) ([]DVRStation, error) {
	lineupID = strings.TrimSpace(lineupID)
	if lineupID == "" {
		return nil, fmt.Errorf("lineup id is required")
	}

	path := "/dvr/guide/stations/" + url.PathEscape(lineupID)
	var raw []map[string]any
	if err := p.getJSON(ctx, path, &raw); err != nil {
		return nil, fmt.Errorf("lineup stations request failed: %w", err)
	}

	out := make([]DVRStation, 0, len(raw))
	for _, entry := range raw {
		stationRef := stringValue(entry["stationId"])
		if stationRef == "" {
			continue
		}
		out = append(out, DVRStation{
			StationRef:    stationRef,
			LineupChannel: strings.TrimSpace(stringValue(entry["channel"])),
			CallSign:      strings.TrimSpace(stringValue(entry["callSign"])),
			Name:          strings.TrimSpace(stringValue(entry["name"])),
		})
	}
	return out, nil
}

func (p *ChannelsProvider) GetCustomMapping(ctx context.Context, lineupID string) (map[string]string, error) {
	lineupID = strings.TrimSpace(lineupID)
	if lineupID == "" {
		return nil, fmt.Errorf("lineup id is required")
	}

	path := "/dvr/guide/stations/" + url.PathEscape(lineupID) + "/custom"
	var raw map[string]any
	if err := p.getJSON(ctx, path, &raw); err != nil {
		return nil, fmt.Errorf("custom mapping request failed: %w", err)
	}

	out := make(map[string]string, len(raw))
	for key, value := range raw {
		out[strings.TrimSpace(key)] = stringValue(value)
	}
	return out, nil
}

func (p *ChannelsProvider) PutCustomMapping(ctx context.Context, lineupID string, patch map[string]string) error {
	lineupID = strings.TrimSpace(lineupID)
	if lineupID == "" {
		return fmt.Errorf("lineup id is required")
	}
	if len(patch) == 0 {
		return nil
	}

	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal custom mapping patch: %w", err)
	}
	path := "/dvr/guide/stations/" + url.PathEscape(lineupID)
	if err := p.request(ctx, http.MethodPut, path, bytes.NewReader(body), "text/plain;charset=UTF-8", nil); err != nil {
		return fmt.Errorf("mapping update request failed: %w", err)
	}
	return nil
}

func (p *ChannelsProvider) ListLineups(ctx context.Context) ([]DVRLineup, error) {
	lineups, err := p.listLineupsFromLineupsEndpoint(ctx)
	if err == nil && len(lineups) > 0 {
		return lineups, nil
	}
	firstErr := err

	fallbackLineups, fallbackErr := p.listLineupsFromDevices(ctx)
	if fallbackErr == nil {
		return fallbackLineups, nil
	}

	if firstErr != nil {
		return nil, fmt.Errorf("lineups endpoint failed: %w; devices fallback failed: %v", firstErr, fallbackErr)
	}
	return nil, fallbackErr
}

func (p *ChannelsProvider) ReloadDeviceLineup(ctx context.Context, deviceID string) error {
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		return fmt.Errorf("device id is required")
	}

	path := "/devices/" + url.PathEscape(deviceID) + "/channels"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+path, nil)
	if err != nil {
		return err
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	// Channels DVR reload endpoint currently returns 400 with the refreshed lineup body.
	if resp.StatusCode == http.StatusBadRequest && isReloadLineupChannelListResponse(body) {
		return nil
	}

	return fmt.Errorf("%s %s failed: %s: %s", http.MethodPost, path, resp.Status, strings.TrimSpace(string(body)))
}

func (p *ChannelsProvider) RefreshGuideStations(ctx context.Context) error {
	if err := p.request(ctx, http.MethodPut, "/dvr/guide/stations", nil, "", nil); err != nil {
		return fmt.Errorf("guide stations refresh request failed: %w", err)
	}
	return nil
}

func (p *ChannelsProvider) RedownloadGuideLineup(ctx context.Context, lineupID string) error {
	lineupID = strings.TrimSpace(lineupID)
	if lineupID == "" {
		return fmt.Errorf("lineup id is required")
	}

	path := "/dvr/lineups/" + url.PathEscape(lineupID)
	if err := p.request(ctx, http.MethodPut, path, nil, "", nil); err != nil {
		return fmt.Errorf("lineup guide redownload request failed: %w", err)
	}
	return nil
}

func (p *ChannelsProvider) RefreshDevices(ctx context.Context) error {
	firstErr := p.request(ctx, http.MethodPut, "/dvr/scanner/scan", nil, "", nil)
	if firstErr == nil {
		return nil
	}

	secondErr := p.request(ctx, http.MethodPost, "/devices", nil, "", nil)
	if secondErr == nil {
		return nil
	}

	return fmt.Errorf("scan request failed: %w; devices refresh fallback failed: %v", firstErr, secondErr)
}

func (p *ChannelsProvider) listLineupsFromLineupsEndpoint(ctx context.Context) ([]DVRLineup, error) {
	var payload any
	if err := p.getJSON(ctx, "/dvr/lineups", &payload); err != nil {
		return nil, err
	}
	lineups := collectLineups(payload)
	if len(lineups) == 0 {
		return nil, fmt.Errorf("no lineups found in /dvr/lineups response")
	}
	return lineups, nil
}

func (p *ChannelsProvider) listLineupsFromDevices(ctx context.Context) ([]DVRLineup, error) {
	var payload any
	if err := p.getJSON(ctx, "/devices", &payload); err != nil {
		return nil, err
	}
	lineups := collectLineups(payload)
	if len(lineups) == 0 {
		return nil, fmt.Errorf("no lineup ids found in /devices response")
	}
	return lineups, nil
}

func isReloadLineupChannelListResponse(body []byte) bool {
	tryDecode := func(candidate []byte) bool {
		candidate = bytes.TrimSpace(candidate)
		if len(candidate) == 0 {
			return false
		}

		var entries []map[string]any
		if err := json.Unmarshal(candidate, &entries); err != nil {
			return false
		}
		return isLikelyReloadLineupChannelList(entries)
	}

	trimmed := bytes.TrimSpace(body)
	if tryDecode(trimmed) {
		return true
	}

	decoded, err := base64.StdEncoding.DecodeString(string(trimmed))
	if err != nil {
		return false
	}
	return tryDecode(decoded)
}

func isLikelyReloadLineupChannelList(entries []map[string]any) bool {
	if len(entries) == 0 {
		return false
	}
	for _, entry := range entries {
		if !isLikelyReloadLineupChannelEntry(entry) {
			return false
		}
	}
	return true
}

func isLikelyReloadLineupChannelEntry(entry map[string]any) bool {
	identifier := firstNonEmpty(
		stringValue(entry["GuideNumber"]),
		stringValue(entry["Number"]),
		stringValue(entry["Station"]),
		stringValue(entry["ChannelID"]),
	)
	name := firstNonEmpty(
		stringValue(entry["GuideName"]),
		stringValue(entry["Name"]),
		stringValue(entry["CallSign"]),
	)
	return identifier != "" && name != ""
}

func collectLineups(payload any) []DVRLineup {
	found := map[string]string{}
	var walk func(value any)

	walk = func(value any) {
		switch typed := value.(type) {
		case map[string]any:
			id := firstNonEmpty(
				stringValue(typed["id"]),
				stringValue(typed["ID"]),
				stringValue(typed["lineup"]),
				stringValue(typed["Lineup"]),
			)
			name := firstNonEmpty(
				stringValue(typed["name"]),
				stringValue(typed["Name"]),
				stringValue(typed["title"]),
				stringValue(typed["Title"]),
			)
			if looksLikeLineupID(id) {
				if _, exists := found[id]; !exists {
					found[id] = name
				} else if found[id] == "" && name != "" {
					found[id] = name
				}
			}

			for key, child := range typed {
				if looksLikeLineupID(strings.TrimSpace(key)) {
					keyID := strings.TrimSpace(key)
					if _, exists := found[keyID]; !exists {
						found[keyID] = ""
					}
				}
				walk(child)
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		case string:
			candidate := strings.TrimSpace(typed)
			if looksLikeLineupID(candidate) {
				if _, exists := found[candidate]; !exists {
					found[candidate] = ""
				}
			}
		}
	}

	walk(payload)

	lineups := make([]DVRLineup, 0, len(found))
	for id, name := range found {
		name = strings.TrimSpace(name)
		if name == "" {
			name = id
		}
		lineups = append(lineups, DVRLineup{
			ID:   id,
			Name: name,
		})
	}

	sort.Slice(lineups, func(i, j int) bool {
		return lineups[i].ID < lineups[j].ID
	})
	return lineups
}

func looksLikeLineupID(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if len(value) > 96 {
		return false
	}
	if strings.Count(value, "-") < 1 {
		return false
	}
	if strings.HasPrefix(value, "-") || strings.HasSuffix(value, "-") {
		return false
	}

	hasLetter := false
	for _, r := range value {
		switch {
		case r >= 'A' && r <= 'Z':
			hasLetter = true
		case r >= 'a' && r <= 'z':
			hasLetter = true
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return false
		}
	}
	return hasLetter
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return strings.TrimSpace(typed.String())
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strings.TrimSpace(strconv.FormatFloat(typed, 'f', -1, 64))
	case float32:
		f := float64(typed)
		if f == float64(int64(f)) {
			return strconv.FormatInt(int64(f), 10)
		}
		return strings.TrimSpace(strconv.FormatFloat(f, 'f', -1, 64))
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case uint:
		return strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	}
}

func (p *ChannelsProvider) getJSON(ctx context.Context, path string, out any) error {
	return p.request(ctx, http.MethodGet, path, nil, "", out)
}

func (p *ChannelsProvider) request(
	ctx context.Context,
	method, path string,
	body io.Reader,
	contentType string,
	out any,
) error {
	req, err := http.NewRequestWithContext(ctx, method, p.baseURL+path, body)
	if err != nil {
		return err
	}
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
		return fmt.Errorf("%s %s failed: %s: %s", method, path, resp.Status, strings.TrimSpace(string(respBody)))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, path, err)
	}
	return nil
}
