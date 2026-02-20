package dvr_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/dvr"
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
)

type putCapture struct {
	LineupID    string
	Patch       map[string]string
	ContentType string
}

type channelsGuideServer struct {
	deviceChannels   map[string]map[string]string
	stationsByLineup map[string][]map[string]string
	customByLineup   map[string]map[string]string

	mu              sync.Mutex
	putCalls        []putCapture
	reloadDeviceIDs []string
}

func newChannelsGuideServer(
	t *testing.T,
	deviceChannels map[string]map[string]string,
	stationsByLineup map[string][]map[string]string,
	customByLineup map[string]map[string]string,
) (*httptest.Server, *channelsGuideServer) {
	t.Helper()

	state := &channelsGuideServer{
		deviceChannels:   deviceChannels,
		stationsByLineup: stationsByLineup,
		customByLineup:   customByLineup,
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/devices/") && strings.HasSuffix(r.URL.Path, "/channels"):
			handleReloadLineupRequest(t, w, r, state)
			return
		case r.Method == http.MethodGet && r.URL.Path == "/dvr/guide/channels":
			writeJSONResponse(t, w, state.deviceChannels)
			return
		case strings.HasPrefix(r.URL.Path, "/dvr/guide/stations/"):
			handleGuideStationsRequest(t, w, r, state)
			return
		default:
			http.NotFound(w, r)
			return
		}
	}))

	return srv, state
}

func handleReloadLineupRequest(t *testing.T, w http.ResponseWriter, r *http.Request, state *channelsGuideServer) {
	t.Helper()

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "devices" || parts[2] != "channels" {
		http.NotFound(w, r)
		return
	}
	deviceID := strings.TrimSpace(parts[1])
	if deviceID == "" {
		http.NotFound(w, r)
		return
	}

	state.mu.Lock()
	state.reloadDeviceIDs = append(state.reloadDeviceIDs, deviceID)

	out := make([]map[string]any, 0)
	for _, channel := range state.deviceChannels {
		if !strings.EqualFold(strings.TrimSpace(channel["DeviceID"]), deviceID) {
			continue
		}
		out = append(out, map[string]any{
			"GuideNumber": channel["Number"],
			"GuideName":   channel["Name"],
			"Station":     channel["Station"],
		})
	}
	state.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	// Channels DVR currently returns 400 on this reload endpoint while still returning fresh lineup data.
	w.WriteHeader(http.StatusBadRequest)
	if err := json.NewEncoder(w).Encode(out); err != nil {
		t.Fatalf("encode reload response: %v", err)
	}
}

func handleGuideStationsRequest(t *testing.T, w http.ResponseWriter, r *http.Request, state *channelsGuideServer) {
	t.Helper()

	rest := strings.TrimPrefix(r.URL.Path, "/dvr/guide/stations/")
	rest = strings.Trim(rest, "/")
	if rest == "" {
		http.NotFound(w, r)
		return
	}

	if strings.HasSuffix(rest, "/custom") {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		lineupID := strings.TrimSuffix(rest, "/custom")
		lineupID = strings.Trim(lineupID, "/")

		state.mu.Lock()
		custom := copyStringMap(state.customByLineup[lineupID])
		state.mu.Unlock()
		writeJSONResponse(t, w, custom)
		return
	}

	lineupID := rest
	if r.Method == http.MethodGet {
		stations := state.stationsByLineup[lineupID]
		writeJSONResponse(t, w, stations)
		return
	}
	if r.Method != http.MethodPut {
		http.NotFound(w, r)
		return
	}

	var patch map[string]string
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.customByLineup[lineupID] == nil {
		state.customByLineup[lineupID] = map[string]string{}
	}
	for key, value := range patch {
		state.customByLineup[lineupID][key] = value
	}
	state.putCalls = append(state.putCalls, putCapture{
		LineupID:    lineupID,
		Patch:       copyStringMap(patch),
		ContentType: r.Header.Get("Content-Type"),
	})
	writeJSONResponse(t, w, true)
}

func writeJSONResponse(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("encode json response: %v", err)
	}
}

func copyStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func TestServiceSyncIntegrationReorderAppliesPatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:cbs:primary",
			ChannelKey: "tvg:cbs",
			Name:       "CBS",
			Group:      "Local",
			StreamURL:  "http://example.com/cbs.ts",
		},
		{
			ItemKey:    "src:fox:primary",
			ChannelKey: "tvg:fox",
			Name:       "FOX",
			Group:      "Local",
			StreamURL:  "http://example.com/fox.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	cbs, err := channelsSvc.Create(ctx, "src:cbs:primary", "CBS", "", nil)
	if err != nil {
		t.Fatalf("Create(CBS) error = %v", err)
	}
	fox, err := channelsSvc.Create(ctx, "src:fox:primary", "FOX", "", nil)
	if err != nil {
		t.Fatalf("Create(FOX) error = %v", err)
	}

	deviceChannels := map[string]map[string]string{
		"A100": {"DeviceID": "8F07FDC6", "Number": "100", "Name": "guide-100", "Station": "21234"},
		"A101": {"DeviceID": "8F07FDC6", "Number": "101", "Name": "guide-101", "Station": "24504"},
	}
	stationsByLineup := map[string][]map[string]string{
		"USA-TEST": {
			{"channel": "2", "callSign": "WCCO", "stationId": "21234", "name": "CBS"},
			{"channel": "3", "callSign": "KMSP", "stationId": "24504", "name": "FOX"},
		},
	}
	customByLineup := map[string]map[string]string{
		"USA-TEST": {
			"A100": "21234",
			"A101": "24504",
		},
	}

	srv, guideState := newChannelsGuideServer(t, deviceChannels, stationsByLineup, customByLineup)
	defer srv.Close()

	instance, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance() error = %v", err)
	}
	instance.Provider = dvr.ProviderChannels
	instance.BaseURL = srv.URL
	instance.DefaultLineupID = "USA-TEST"
	instance.SyncMode = dvr.SyncModeConfiguredOnly
	instance.SyncEnabled = false
	instance.SyncCron = ""
	if _, err := store.UpsertDVRInstance(ctx, instance); err != nil {
		t.Fatalf("UpsertDVRInstance() error = %v", err)
	}

	if _, err := store.UpsertChannelDVRMapping(ctx, dvr.ChannelMapping{
		ChannelID:        cbs.ChannelID,
		DVRInstanceID:    instance.ID,
		DVRLineupID:      "USA-TEST",
		DVRLineupChannel: "2",
	}); err != nil {
		t.Fatalf("UpsertChannelDVRMapping(CBS) error = %v", err)
	}
	if _, err := store.UpsertChannelDVRMapping(ctx, dvr.ChannelMapping{
		ChannelID:        fox.ChannelID,
		DVRInstanceID:    instance.ID,
		DVRLineupID:      "USA-TEST",
		DVRLineupChannel: "3",
	}); err != nil {
		t.Fatalf("UpsertChannelDVRMapping(FOX) error = %v", err)
	}

	if err := channelsSvc.Reorder(ctx, []int64{fox.ChannelID, cbs.ChannelID}); err != nil {
		t.Fatalf("Reorder() error = %v", err)
	}

	svc, err := dvr.NewService(store, "8F07FDC6", srv.Client())
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	result, err := svc.Sync(ctx, dvr.SyncRequest{DryRun: false})
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	if result.UpdatedCount != 2 {
		t.Fatalf("result.UpdatedCount = %d, want 2", result.UpdatedCount)
	}
	if result.ClearedCount != 0 {
		t.Fatalf("result.ClearedCount = %d, want 0", result.ClearedCount)
	}

	guideState.mu.Lock()
	defer guideState.mu.Unlock()
	if len(guideState.reloadDeviceIDs) != 1 {
		t.Fatalf("len(reloadDeviceIDs) = %d, want 1", len(guideState.reloadDeviceIDs))
	}
	if guideState.reloadDeviceIDs[0] != "8F07FDC6" {
		t.Fatalf("reload device id = %q, want 8F07FDC6", guideState.reloadDeviceIDs[0])
	}
	if len(guideState.putCalls) != 1 {
		t.Fatalf("len(putCalls) = %d, want 1", len(guideState.putCalls))
	}

	wantPatch := map[string]string{
		"A100": "24504",
		"A101": "21234",
	}
	if got := guideState.putCalls[0].Patch; !reflect.DeepEqual(got, wantPatch) {
		t.Fatalf("patch = %#v, want %#v", got, wantPatch)
	}
	if ct := guideState.putCalls[0].ContentType; ct != "text/plain;charset=UTF-8" {
		t.Fatalf("PUT Content-Type = %q, want text/plain;charset=UTF-8", ct)
	}

	foxMapping, err := store.GetChannelDVRMapping(ctx, instance.ID, fox.ChannelID)
	if err != nil {
		t.Fatalf("GetChannelDVRMapping(FOX) error = %v", err)
	}
	if foxMapping.DVRStationRef != "24504" {
		t.Fatalf("FOX DVRStationRef = %q, want 24504", foxMapping.DVRStationRef)
	}

	cbsMapping, err := store.GetChannelDVRMapping(ctx, instance.ID, cbs.ChannelID)
	if err != nil {
		t.Fatalf("GetChannelDVRMapping(CBS) error = %v", err)
	}
	if cbsMapping.DVRStationRef != "21234" {
		t.Fatalf("CBS DVRStationRef = %q, want 21234", cbsMapping.DVRStationRef)
	}
}

func TestServiceSyncIntegrationMirrorDeviceClearsUnconfigured(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{{
		ItemKey:    "src:cbs:primary",
		ChannelKey: "tvg:cbs",
		Name:       "CBS",
		Group:      "Local",
		StreamURL:  "http://example.com/cbs.ts",
	}}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	cbs, err := channelsSvc.Create(ctx, "src:cbs:primary", "CBS", "", nil)
	if err != nil {
		t.Fatalf("Create(CBS) error = %v", err)
	}

	deviceChannels := map[string]map[string]string{
		"A100": {"DeviceID": "8F07FDC6", "Number": "100", "Name": "guide-100", "Station": "99999"},
		"A101": {"DeviceID": "8F07FDC6", "Number": "101", "Name": "guide-101", "Station": "11111"},
	}
	stationsByLineup := map[string][]map[string]string{
		"USA-TEST": {
			{"channel": "2", "callSign": "WCCO", "stationId": "21234", "name": "CBS"},
		},
	}
	customByLineup := map[string]map[string]string{
		"USA-TEST": {
			"A100": "99999",
			"A101": "11111",
		},
	}

	srv, guideState := newChannelsGuideServer(t, deviceChannels, stationsByLineup, customByLineup)
	defer srv.Close()

	instance, err := store.GetDVRInstance(ctx)
	if err != nil {
		t.Fatalf("GetDVRInstance() error = %v", err)
	}
	instance.Provider = dvr.ProviderChannels
	instance.BaseURL = srv.URL
	instance.DefaultLineupID = "USA-TEST"
	instance.SyncMode = dvr.SyncModeMirrorDevice
	if _, err := store.UpsertDVRInstance(ctx, instance); err != nil {
		t.Fatalf("UpsertDVRInstance() error = %v", err)
	}

	if _, err := store.UpsertChannelDVRMapping(ctx, dvr.ChannelMapping{
		ChannelID:        cbs.ChannelID,
		DVRInstanceID:    instance.ID,
		DVRLineupID:      "USA-TEST",
		DVRLineupChannel: "2",
	}); err != nil {
		t.Fatalf("UpsertChannelDVRMapping(CBS) error = %v", err)
	}

	svc, err := dvr.NewService(store, "8F07FDC6", srv.Client())
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	result, err := svc.Sync(ctx, dvr.SyncRequest{DryRun: false})
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	if result.UpdatedCount != 1 {
		t.Fatalf("result.UpdatedCount = %d, want 1", result.UpdatedCount)
	}
	if result.ClearedCount != 1 {
		t.Fatalf("result.ClearedCount = %d, want 1", result.ClearedCount)
	}

	guideState.mu.Lock()
	defer guideState.mu.Unlock()
	if len(guideState.reloadDeviceIDs) != 1 {
		t.Fatalf("len(reloadDeviceIDs) = %d, want 1", len(guideState.reloadDeviceIDs))
	}
	if guideState.reloadDeviceIDs[0] != "8F07FDC6" {
		t.Fatalf("reload device id = %q, want 8F07FDC6", guideState.reloadDeviceIDs[0])
	}
	if len(guideState.putCalls) != 1 {
		t.Fatalf("len(putCalls) = %d, want 1", len(guideState.putCalls))
	}

	wantPatch := map[string]string{
		"A100": "21234",
		"A101": "",
	}
	if got := guideState.putCalls[0].Patch; !reflect.DeepEqual(got, wantPatch) {
		t.Fatalf("patch = %#v, want %#v", got, wantPatch)
	}
}
