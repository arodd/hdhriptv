package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/dvr"
	"github.com/arodd/hdhriptv/internal/jobs"
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/scheduler"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
	"github.com/arodd/hdhriptv/internal/stream"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestAdminRoutesUIChannelsIncludesDVRMappingToggle(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	sourceHealthRuntimeClearer := &fakeSourceHealthClearRuntime{}
	handler.SetSourceHealthClearRuntime(sourceHealthRuntimeClearer)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ui/channels", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /ui/channels status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	for _, marker := range []string{
		`id="dvr-toggle-mappings-btn"`,
		`aria-pressed="false"`,
		`Show DVR Mappings`,
		`id="dvr-mapping-visibility-note"`,
		`DVR mappings hidden by default.`,
		`dvr-mappings-hidden`,
		`Dynamic Channel Blocks (10000+)`,
		`id="dynamic-query-create-form"`,
		`id="dynamic-group-chips"`,
		`id="dynamic-group-input" type="text" list="dynamic-group-options"`,
		`id="dynamic-group-add"`,
		`id="dynamic-group-clear"`,
		`id="dynamic-group-options"`,
		`dynamicCatalogGroupsURL()`,
		`id="dynamic-query-list"`,
		`group_names: groupNames`,
		`source_ids: sourceIDs`,
		`id="dynamic-source-toggle"`,
		`id="dynamic-source-dropdown"`,
		`loadPagedCollection("/api/dynamic-channels", "queries")`,
		`formatChannelDynamicRuleStatus(ch)`,
		`formatChannelSourceSummary(ch)`,
		`dynamic query enabled`,
		`dynamic-managed`,
	} {
		if !strings.Contains(body, marker) {
			t.Fatalf("GET /ui/channels missing marker %q", marker)
		}
	}
}

func TestAdminRoutesUIBrandIncludesVersion(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.SetUIVersion("v9.9.9-test")

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	for _, path := range []string{"/ui/catalog", "/ui/channels", "/ui/tuners"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d", path, rec.Code, http.StatusOK)
		}

		if !strings.Contains(rec.Body.String(), `HDHR IPTV Admin - v9.9.9-test`) {
			t.Fatalf("GET %s body missing versioned app title marker", path)
		}
	}
}

func TestAdminRoutesUICapabilityAwareNavigation(t *testing.T) {
	t.Run("hides unavailable conditional links", func(t *testing.T) {
		store, err := sqlite.Open(":memory:")
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer store.Close()

		channelsSvc := channels.NewService(store)
		handler, err := NewAdminHandler(store, channelsSvc)
		if err != nil {
			t.Fatalf("NewAdminHandler() error = %v", err)
		}

		mux := http.NewServeMux()
		handler.RegisterRoutes(mux, "")

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/ui/catalog", nil)
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /ui/catalog status = %d, want %d", rec.Code, http.StatusOK)
		}

		body := rec.Body.String()
		if strings.Contains(body, `href="/ui/dvr"`) {
			t.Fatalf("GET /ui/catalog unexpectedly contains DVR nav link when DVR route is unavailable")
		}
		if strings.Contains(body, `href="/ui/automation"`) {
			t.Fatalf("GET /ui/catalog unexpectedly contains Automation nav link when automation route is unavailable")
		}
	})

	t.Run("shows available conditional links", func(t *testing.T) {
		store, err := sqlite.Open(":memory:")
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer store.Close()

		channelsSvc := channels.NewService(store)
		runner, err := jobs.NewRunner(store)
		if err != nil {
			t.Fatalf("jobs.NewRunner() error = %v", err)
		}
		defer runner.Close()

		schedulerSvc, err := scheduler.New(store, nil)
		if err != nil {
			t.Fatalf("scheduler.New() error = %v", err)
		}

		handler, err := NewAdminHandler(store, channelsSvc, AutomationDeps{
			Settings:  store,
			Scheduler: schedulerSvc,
			Runner:    runner,
			JobFuncs: map[string]jobs.JobFunc{
				jobs.JobPlaylistSync:   func(context.Context, *jobs.RunContext) error { return nil },
				jobs.JobAutoPrioritize: func(context.Context, *jobs.RunContext) error { return nil },
			},
		})
		if err != nil {
			t.Fatalf("NewAdminHandler() error = %v", err)
		}
		handler.SetDVRService(&fakeDVRService{})

		mux := http.NewServeMux()
		handler.RegisterRoutes(mux, "")

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/ui/catalog", nil)
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /ui/catalog status = %d, want %d", rec.Code, http.StatusOK)
		}

		body := rec.Body.String()
		if !strings.Contains(body, `href="/ui/dvr"`) {
			t.Fatalf("GET /ui/catalog missing DVR nav link when DVR route is available")
		}
		if !strings.Contains(body, `href="/ui/automation"`) {
			t.Fatalf("GET /ui/catalog missing Automation nav link when automation route is available")
		}
	})
}

func TestAdminRoutesUIDynamicChannelDetailIncludesQueryID(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ui/dynamic-channels/42", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /ui/dynamic-channels/{queryID} status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	for _, marker := range []string{
		`Dynamic Block Configuration`,
		`Generated Channels (Block Sorting)`,
		`const queryID = Number(`,
		`id="query-group-chips"`,
		`id="query-group-input" type="text" list="query-group-options"`,
		`id="query-group-add"`,
		`id="query-group-clear"`,
		`id="query-group-options"`,
		`catalogGroupsURL()`,
		`/api/groups?`,
		`loadCatalogGroups`,
		`group_names: groupNames`,
		`/api/dynamic-channels/${queryID}`,
	} {
		if !strings.Contains(body, marker) {
			t.Fatalf("GET /ui/dynamic-channels/{queryID} missing marker %q", marker)
		}
	}
}

func TestAdminRoutesUICatalogIncludesRapidSourceAddToolbar(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ui/catalog", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /ui/catalog status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	for _, marker := range []string{
		`badge.className = "source-badge"`,
		`const srcName = itemSourceName(item);`,
		`id="group-chip-input" type="text" list="group-options"`,
		`id="group-chip-add"`,
		`id="group-chip-clear"`,
		`id="selected-groups"`,
		`id="target-channel-select"`,
		`No target channel selected`,
		`Create Dynamic Channel From Filters`,
		`Rapid source-add mode`,
		`params.append("group", groupName)`,
		`dynamic_rule: dynamicRule`,
		`id="group-options"`,
	} {
		if !strings.Contains(body, marker) {
			t.Fatalf("GET /ui/catalog missing marker %q", marker)
		}
	}

	rootRec := httptest.NewRecorder()
	rootReq := httptest.NewRequest(http.MethodGet, "/ui/", nil)
	mux.ServeHTTP(rootRec, rootReq)
	if rootRec.Code != http.StatusFound {
		t.Fatalf("GET /ui/ status = %d, want %d", rootRec.Code, http.StatusFound)
	}
	if location := rootRec.Header().Get("Location"); location != "/ui/catalog" {
		t.Fatalf("GET /ui/ redirect location = %q, want %q", location, "/ui/catalog")
	}
}

func TestAdminRoutesUIChannelDetailIncludesDynamicGroupChips(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ui/channels/42", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /ui/channels/{channelID} status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	for _, marker := range []string{
		`id="dynamic-group-chips"`,
		`id="dynamic-group-input" type="text" list="dynamic-group-options"`,
		`id="dynamic-group-add"`,
		`id="dynamic-group-clear"`,
		`id="dynamic-group-options"`,
		`catalogGroupsURL()`,
		`/api/groups?`,
		`loadCatalogGroups`,
		`group_names: groupNames`,
		`dynamic_rule: dynamicRule`,
	} {
		if !strings.Contains(body, marker) {
			t.Fatalf("GET /ui/channels/{channelID} missing marker %q", marker)
		}
	}
}

func TestAdminRoutesUIReorderScriptsDoNotDependOnReorderListPayloads(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	channelsRec := httptest.NewRecorder()
	channelsReq := httptest.NewRequest(http.MethodGet, "/ui/channels", nil)
	mux.ServeHTTP(channelsRec, channelsReq)
	if channelsRec.Code != http.StatusOK {
		t.Fatalf("GET /ui/channels status = %d, want %d", channelsRec.Code, http.StatusOK)
	}
	channelsBody := channelsRec.Body.String()
	if strings.Contains(channelsBody, "payload.channels") {
		t.Fatal("ui/channels script still depends on reorder response list payload")
	}
	if strings.Contains(channelsBody, "state.traditionalChannels = next;") {
		t.Fatal("ui/channels reorder script still relies on optimistic post-success assignment")
	}
	for _, marker := range []string{
		`await api("/api/channels/reorder"`,
		`await loadTraditionalChannels();`,
	} {
		if !strings.Contains(channelsBody, marker) {
			t.Fatalf("GET /ui/channels missing marker %q", marker)
		}
	}

	dynamicRec := httptest.NewRecorder()
	dynamicReq := httptest.NewRequest(http.MethodGet, "/ui/dynamic-channels/42", nil)
	mux.ServeHTTP(dynamicRec, dynamicReq)
	if dynamicRec.Code != http.StatusOK {
		t.Fatalf("GET /ui/dynamic-channels/{queryID} status = %d, want %d", dynamicRec.Code, http.StatusOK)
	}
	dynamicBody := dynamicRec.Body.String()
	if strings.Contains(dynamicBody, "payload.channels") {
		t.Fatal("ui/dynamic-channels script still depends on reorder response list payload")
	}
	for _, marker := range []string{
		"await api(`/api/dynamic-channels/${queryID}/channels/reorder`",
		`state.channels = next;`,
	} {
		if !strings.Contains(dynamicBody, marker) {
			t.Fatalf("GET /ui/dynamic-channels/{queryID} missing marker %q", marker)
		}
	}
}

func TestAdminRoutesReorderEndpointsReturnNoContentAndBoundedPayloads(t *testing.T) {
	const traditionalChannelCount = 120
	const sourceCount = 80
	const dynamicItemCount = 120

	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	items := make([]playlist.Item, 0, traditionalChannelCount+sourceCount+dynamicItemCount)
	for i := range traditionalChannelCount {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:reorder:channel:%03d", i),
			ChannelKey: fmt.Sprintf("tvg:reorder-channel-%03d", i),
			Name:       fmt.Sprintf("Reorder Channel %03d", i),
			Group:      "Reorder Traditional",
			StreamURL:  fmt.Sprintf("http://example.com/reorder/channel/%03d.ts", i),
			TVGID:      fmt.Sprintf("reorder-channel-%03d", i),
		})
	}
	for i := range sourceCount {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:reorder:source:%03d", i),
			ChannelKey: "tvg:reorder-source-shared",
			Name:       "Reorder Source Shared",
			Group:      "Reorder Sources",
			StreamURL:  fmt.Sprintf("http://example.com/reorder/source/%03d.ts", i),
			TVGID:      "reorder-source-shared",
		})
	}
	for i := range dynamicItemCount {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:reorder:dynamic:%03d", i),
			ChannelKey: fmt.Sprintf("tvg:reorder-dynamic-%03d", i),
			Name:       fmt.Sprintf("Dynamic Load %03d", i),
			Group:      "Dynamic Load",
			StreamURL:  fmt.Sprintf("http://example.com/reorder/dynamic/%03d.ts", i),
			TVGID:      fmt.Sprintf("reorder-dynamic-%03d", i),
		})
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	for i := range traditionalChannelCount {
		if _, err := channelsSvc.Create(ctx, fmt.Sprintf("src:reorder:channel:%03d", i), "", "", nil); err != nil {
			t.Fatalf("Create(traditional channel %d) error = %v", i, err)
		}
	}

	sourceChannel, err := channelsSvc.Create(ctx, "src:reorder:source:000", "", "", nil)
	if err != nil {
		t.Fatalf("Create(source channel) error = %v", err)
	}
	for i := 1; i < sourceCount; i++ {
		if _, err := channelsSvc.AddSource(ctx, sourceChannel.ChannelID, fmt.Sprintf("src:reorder:source:%03d", i), true); err != nil {
			t.Fatalf("AddSource(%d) error = %v", i, err)
		}
	}

	dynamicEnabled := true
	dynamicQuery, err := channelsSvc.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
		Enabled:     &dynamicEnabled,
		Name:        "Reorder Dynamic Load",
		GroupName:   "Dynamic Load",
		SearchQuery: "Dynamic Load",
	})
	if err != nil {
		t.Fatalf("CreateDynamicChannelQuery() error = %v", err)
	}
	if _, err := channelsSvc.SyncDynamicChannelBlocks(ctx); err != nil {
		t.Fatalf("SyncDynamicChannelBlocks() error = %v", err)
	}

	channelsPath := fmt.Sprintf("/api/channels?limit=%d&offset=0", maxChannelsListLimit)
	var traditionalList struct {
		Channels []channels.Channel `json:"channels"`
	}
	doJSON(t, mux, http.MethodGet, channelsPath, nil, http.StatusOK, &traditionalList)
	if len(traditionalList.Channels) < traditionalChannelCount+1 {
		t.Fatalf("len(traditional channels) = %d, want >= %d", len(traditionalList.Channels), traditionalChannelCount+1)
	}

	traditionalIDs := make([]int64, len(traditionalList.Channels))
	for i, ch := range traditionalList.Channels {
		traditionalIDs[i] = ch.ChannelID
	}
	reorderedTraditionalIDs := make([]int64, len(traditionalIDs))
	for i := range traditionalIDs {
		reorderedTraditionalIDs[i] = traditionalIDs[len(traditionalIDs)-1-i]
	}

	traditionalListRec := doRaw(t, mux, http.MethodGet, channelsPath, nil)
	if traditionalListRec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d", channelsPath, traditionalListRec.Code, http.StatusOK)
	}
	traditionalListBytes := traditionalListRec.Body.Len()

	startedAt := time.Now()
	traditionalReorderRec := doRaw(t, mux, http.MethodPatch, "/api/channels/reorder", map[string]any{
		"channel_ids": reorderedTraditionalIDs,
	})
	traditionalReorderLatency := time.Since(startedAt)
	if traditionalReorderRec.Code != http.StatusNoContent {
		t.Fatalf("PATCH /api/channels/reorder status = %d, want %d", traditionalReorderRec.Code, http.StatusNoContent)
	}
	if traditionalReorderRec.Body.Len() != 0 {
		t.Fatalf("PATCH /api/channels/reorder body length = %d, want 0", traditionalReorderRec.Body.Len())
	}
	doJSON(t, mux, http.MethodGet, channelsPath, nil, http.StatusOK, &traditionalList)
	if traditionalList.Channels[0].ChannelID != reorderedTraditionalIDs[0] {
		t.Fatalf("reordered traditional first channel_id = %d, want %d", traditionalList.Channels[0].ChannelID, reorderedTraditionalIDs[0])
	}
	if traditionalListBytes <= 1500 {
		t.Fatalf("traditional channels list payload bytes = %d, want > 1500 for high-cardinality guardrail", traditionalListBytes)
	}

	sourcesPath := fmt.Sprintf("/api/channels/%d/sources?limit=%d&offset=0", sourceChannel.ChannelID, maxChannelSourcesListLimit)
	var sourcesList struct {
		Sources []channels.Source `json:"sources"`
	}
	doJSON(t, mux, http.MethodGet, sourcesPath, nil, http.StatusOK, &sourcesList)
	if len(sourcesList.Sources) != sourceCount {
		t.Fatalf("len(source list) = %d, want %d", len(sourcesList.Sources), sourceCount)
	}
	sourceIDs := make([]int64, len(sourcesList.Sources))
	for i, src := range sourcesList.Sources {
		sourceIDs[i] = src.SourceID
	}
	reorderedSourceIDs := make([]int64, len(sourceIDs))
	for i := range sourceIDs {
		reorderedSourceIDs[i] = sourceIDs[len(sourceIDs)-1-i]
	}

	sourcesListRec := doRaw(t, mux, http.MethodGet, sourcesPath, nil)
	if sourcesListRec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d", sourcesPath, sourcesListRec.Code, http.StatusOK)
	}
	sourcesListBytes := sourcesListRec.Body.Len()

	startedAt = time.Now()
	sourcesReorderRec := doRaw(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d/sources/reorder", sourceChannel.ChannelID), map[string]any{
		"source_ids": reorderedSourceIDs,
	})
	sourcesReorderLatency := time.Since(startedAt)
	if sourcesReorderRec.Code != http.StatusNoContent {
		t.Fatalf("PATCH /api/channels/{channelID}/sources/reorder status = %d, want %d", sourcesReorderRec.Code, http.StatusNoContent)
	}
	if sourcesReorderRec.Body.Len() != 0 {
		t.Fatalf("PATCH /api/channels/{channelID}/sources/reorder body length = %d, want 0", sourcesReorderRec.Body.Len())
	}
	doJSON(t, mux, http.MethodGet, sourcesPath, nil, http.StatusOK, &sourcesList)
	if sourcesList.Sources[0].SourceID != reorderedSourceIDs[0] {
		t.Fatalf("reordered first source_id = %d, want %d", sourcesList.Sources[0].SourceID, reorderedSourceIDs[0])
	}
	if sourcesListBytes <= 1500 {
		t.Fatalf("channel sources list payload bytes = %d, want > 1500 for high-cardinality guardrail", sourcesListBytes)
	}

	dynamicPath := fmt.Sprintf("/api/dynamic-channels/%d/channels?limit=%d&offset=0", dynamicQuery.QueryID, maxChannelsListLimit)
	var dynamicList struct {
		Channels []channels.Channel `json:"channels"`
	}
	doJSON(t, mux, http.MethodGet, dynamicPath, nil, http.StatusOK, &dynamicList)
	if len(dynamicList.Channels) != dynamicItemCount {
		t.Fatalf("len(dynamic generated channels) = %d, want %d", len(dynamicList.Channels), dynamicItemCount)
	}
	dynamicIDs := make([]int64, len(dynamicList.Channels))
	for i, ch := range dynamicList.Channels {
		dynamicIDs[i] = ch.ChannelID
	}
	reorderedDynamicIDs := make([]int64, len(dynamicIDs))
	for i := range dynamicIDs {
		reorderedDynamicIDs[i] = dynamicIDs[len(dynamicIDs)-1-i]
	}

	dynamicListRec := doRaw(t, mux, http.MethodGet, dynamicPath, nil)
	if dynamicListRec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d", dynamicPath, dynamicListRec.Code, http.StatusOK)
	}
	dynamicListBytes := dynamicListRec.Body.Len()

	startedAt = time.Now()
	dynamicReorderRec := doRaw(t, mux, http.MethodPatch, fmt.Sprintf("/api/dynamic-channels/%d/channels/reorder", dynamicQuery.QueryID), map[string]any{
		"channel_ids": reorderedDynamicIDs,
	})
	dynamicReorderLatency := time.Since(startedAt)
	if dynamicReorderRec.Code != http.StatusNoContent {
		t.Fatalf("PATCH /api/dynamic-channels/{queryID}/channels/reorder status = %d, want %d", dynamicReorderRec.Code, http.StatusNoContent)
	}
	if dynamicReorderRec.Body.Len() != 0 {
		t.Fatalf("PATCH /api/dynamic-channels/{queryID}/channels/reorder body length = %d, want 0", dynamicReorderRec.Body.Len())
	}
	doJSON(t, mux, http.MethodGet, dynamicPath, nil, http.StatusOK, &dynamicList)
	if dynamicList.Channels[0].ChannelID != reorderedDynamicIDs[0] {
		t.Fatalf("reordered first dynamic channel_id = %d, want %d", dynamicList.Channels[0].ChannelID, reorderedDynamicIDs[0])
	}
	if dynamicListBytes <= 1500 {
		t.Fatalf("dynamic generated channels list payload bytes = %d, want > 1500 for high-cardinality guardrail", dynamicListBytes)
	}

	t.Logf(
		"high-cardinality reorder evidence: channels list_bytes=%d reorder_bytes=%d reorder_latency_ms=%d",
		traditionalListBytes,
		traditionalReorderRec.Body.Len(),
		traditionalReorderLatency.Milliseconds(),
	)
	t.Logf(
		"high-cardinality reorder evidence: sources list_bytes=%d reorder_bytes=%d reorder_latency_ms=%d",
		sourcesListBytes,
		sourcesReorderRec.Body.Len(),
		sourcesReorderLatency.Milliseconds(),
	)
	t.Logf(
		"high-cardinality reorder evidence: dynamic list_bytes=%d reorder_bytes=%d reorder_latency_ms=%d",
		dynamicListBytes,
		dynamicReorderRec.Body.Len(),
		dynamicReorderLatency.Milliseconds(),
	)
}

func TestAdminRoutesReorderChannelsQueuesDVRLineupReload(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:queue:one",
			ChannelKey: "name:queue one",
			Name:       "Queue One",
			Group:      "Queue",
			StreamURL:  "http://example.com/queue-one.ts",
		},
		{
			ItemKey:    "src:queue:two",
			ChannelKey: "name:queue two",
			Name:       "Queue Two",
			Group:      "Queue",
			StreamURL:  "http://example.com/queue-two.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	defer handler.Close()

	fakeDVR := &fakeDVRService{}
	handler.SetDVRService(fakeDVR)
	handler.dvrLineupReloadDebounce = 20 * time.Millisecond
	handler.dvrLineupReloadMaxWait = 150 * time.Millisecond

	chOne, err := channelsSvc.Create(ctx, "src:queue:one", "", "", nil)
	if err != nil {
		t.Fatalf("Create(channel one) error = %v", err)
	}
	chTwo, err := channelsSvc.Create(ctx, "src:queue:two", "", "", nil)
	if err != nil {
		t.Fatalf("Create(channel two) error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodPatch, "/api/channels/reorder", map[string]any{
		"channel_ids": []int64{chTwo.ChannelID, chOne.ChannelID},
	})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("PATCH /api/channels/reorder status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("PATCH /api/channels/reorder body length = %d, want 0", rec.Body.Len())
	}

	waitForCondition(t, 2*time.Second, "traditional reorder queued dvr lineup reload", func() bool {
		return fakeDVR.ReloadCallCount() >= 1
	})
}

func TestAdminRoutesChannelSourceFlow(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(context.Background(), []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
			TVGID:      "news.us",
			Attrs:      map[string]string{"tvg-name": "News Primary TVG"},
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "TVG:NEWS",
			Name:       "News Backup",
			Group:      "News",
			StreamURL:  "http://example.com/news-backup.ts",
			TVGID:      "NEWS.US",
			Attrs:      map[string]string{"tvg-name": "News Backup TVG"},
		},
		{
			ItemKey:    "src:sports:primary",
			ChannelKey: "tvg:sports",
			Name:       "Sports Primary",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-primary.ts",
			TVGID:      "sports.us",
			Attrs:      map[string]string{"tvg-name": "Sports Primary TVG"},
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	uiRec := httptest.NewRecorder()
	uiReq := httptest.NewRequest(http.MethodGet, "/ui/catalog", nil)
	mux.ServeHTTP(uiRec, uiReq)
	if uiRec.Code != http.StatusOK {
		t.Fatalf("GET /ui/catalog status = %d, want %d", uiRec.Code, http.StatusOK)
	}

	tunerUIRec := httptest.NewRecorder()
	tunerUIReq := httptest.NewRequest(http.MethodGet, "/ui/tuners", nil)
	mux.ServeHTTP(tunerUIRec, tunerUIReq)
	if tunerUIRec.Code != http.StatusOK {
		t.Fatalf("GET /ui/tuners status = %d, want %d", tunerUIRec.Code, http.StatusOK)
	}
	tunerUIBody := tunerUIRec.Body.String()
	for _, marker := range []string{
		"Shared Session History",
		`id="tuner-command-bar"`,
		`id="command-bar-toggle"`,
		`id="command-bar-controls"`,
		`id="quick-filter-active"`,
		`id="quick-filter-recovering"`,
		`id="quick-filter-errors"`,
		`id="quick-filter-idle"`,
		`id="tuner-search-input"`,
		`id="active-sort-key"`,
		`id="history-sort-key"`,
		`id="history-status-filter"`,
		`id="history-errors-only"`,
		`id="history-recovery-only"`,
		`id="history-search-input"`,
		`id="history-list"`,
		`id="history-tablist"`,
		`id="history-tab-summary"`,
		`id="history-tabpanel-recovery"`,
		`id="summary-card-recovering"`,
		`id="summary-recovering-severity"`,
		`id="summary-card-reselect-alert"`,
		`id="summary-max-reselect-severity"`,
		`id="client-groups"`,
		`id="resolve-client-ip"`,
		`id="selected-session-detail"`,
	} {
		if !strings.Contains(tunerUIBody, marker) {
			t.Fatalf("GET /ui/tuners body missing marker %q", marker)
		}
	}
	if strings.Contains(tunerUIBody, `id="alerts-list"`) {
		t.Fatal(`GET /ui/tuners body unexpectedly contains deprecated marker "alerts-list"`)
	}

	tunerAPIRec := doRaw(t, mux, http.MethodGet, "/api/admin/tuners", nil)
	if tunerAPIRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /api/admin/tuners status = %d, want %d", tunerAPIRec.Code, http.StatusServiceUnavailable)
	}

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:news:primary",
	}, http.StatusOK, &created)
	if created.GuideNumber != "100" {
		t.Fatalf("created guide_number = %q, want 100", created.GuideNumber)
	}

	var added channels.Source
	doJSON(t, mux, http.MethodPost, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), map[string]any{
		"item_key": "src:news:backup",
	}, http.StatusOK, &added)
	if added.AssociationType != "channel_key" {
		t.Fatalf("association_type = %q, want channel_key", added.AssociationType)
	}

	rec := doRaw(t, mux, http.MethodPost, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), map[string]any{
		"item_key": "src:sports:primary",
	})
	if rec.Code != http.StatusConflict {
		t.Fatalf("POST /api/channels/{id}/sources non-matching status = %d, want %d", rec.Code, http.StatusConflict)
	}

	var manual channels.Source
	doJSON(t, mux, http.MethodPost, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), map[string]any{
		"item_key":            "src:sports:primary",
		"allow_cross_channel": true,
	}, http.StatusOK, &manual)
	if manual.AssociationType != "manual" {
		t.Fatalf("manual association_type = %q, want manual", manual.AssociationType)
	}

	var updated channels.Channel
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"guide_name": "News Super",
		"enabled":    false,
	}, http.StatusOK, &updated)
	if updated.Enabled {
		t.Fatal("updated channel enabled = true, want false")
	}

	var sourcesResp struct {
		Sources []channels.Source `json:"sources"`
	}
	doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), nil, http.StatusOK, &sourcesResp)
	if len(sourcesResp.Sources) != 3 {
		t.Fatalf("len(sources) = %d, want 3", len(sourcesResp.Sources))
	}
	gotTVGNames := map[string]string{}
	for _, src := range sourcesResp.Sources {
		gotTVGNames[src.ItemKey] = src.TVGName
	}
	if gotTVGNames["src:news:primary"] != "News Primary TVG" {
		t.Fatalf("source tvg_name for src:news:primary = %q, want %q", gotTVGNames["src:news:primary"], "News Primary TVG")
	}
	if gotTVGNames["src:news:backup"] != "News Backup TVG" {
		t.Fatalf("source tvg_name for src:news:backup = %q, want %q", gotTVGNames["src:news:backup"], "News Backup TVG")
	}
	if gotTVGNames["src:sports:primary"] != "Sports Primary TVG" {
		t.Fatalf("source tvg_name for src:sports:primary = %q, want %q", gotTVGNames["src:sports:primary"], "Sports Primary TVG")
	}

	var sourceUpdated channels.Source
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d/sources/%d", created.ChannelID, added.SourceID), map[string]any{
		"enabled": false,
	}, http.StatusOK, &sourceUpdated)
	if sourceUpdated.Enabled {
		t.Fatal("updated source enabled = true, want false")
	}

	var suggestions struct {
		Groups []channels.DuplicateGroup `json:"groups"`
		Total  int                       `json:"total"`
		Limit  int                       `json:"limit"`
		Offset int                       `json:"offset"`
	}
	doJSON(t, mux, http.MethodGet, "/api/suggestions/duplicates?min=2&q=tvg:news", nil, http.StatusOK, &suggestions)
	if suggestions.Total != 1 {
		t.Fatalf("suggestions total = %d, want 1", suggestions.Total)
	}
	if suggestions.Limit != defaultDuplicateSuggestionsLimit {
		t.Fatalf("suggestions limit = %d, want %d", suggestions.Limit, defaultDuplicateSuggestionsLimit)
	}
	if suggestions.Offset != 0 {
		t.Fatalf("suggestions offset = %d, want 0", suggestions.Offset)
	}
	if len(suggestions.Groups) != 1 {
		t.Fatalf("len(suggestion groups) = %d, want 1", len(suggestions.Groups))
	}
	if suggestions.Groups[0].ChannelKey != "tvg:news" {
		t.Fatalf("suggestion channel_key = %q, want tvg:news", suggestions.Groups[0].ChannelKey)
	}
	if len(suggestions.Groups[0].Items) == 0 {
		t.Fatal("suggestion group items = empty, want at least one duplicate item")
	}
	for _, item := range suggestions.Groups[0].Items {
		if item.PlaylistSourceID <= 0 {
			t.Fatalf("suggestion item %q playlist_source_id = %d, want positive source id", item.ItemKey, item.PlaylistSourceID)
		}
		if strings.TrimSpace(item.PlaylistSourceName) == "" {
			t.Fatalf("suggestion item %q playlist_source_name = empty, want non-empty source name", item.ItemKey)
		}
	}
}

func TestAdminRoutesItemsFiltersPaginationAndActiveParity(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:fox9:0001",
			ChannelKey: "name:fox 9 kmsp",
			Name:       "FOX 9 KMSP",
			Group:      "Catalog",
			StreamURL:  "http://example.com/fox9.ts",
			TVGLogo:    "http://img.example.com/fox9.png",
		},
		{
			ItemKey:    "src:legacy:0004",
			ChannelKey: "name:legacy feed",
			Name:       "Legacy Feed",
			Group:      "Catalog",
			StreamURL:  "http://example.com/legacy.ts",
			TVGLogo:    "http://img.example.com/legacy.png",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(initial) error = %v", err)
	}

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:fox9:0001",
			ChannelKey: "name:fox 9 kmsp",
			Name:       "FOX 9 KMSP",
			Group:      "Catalog",
			StreamURL:  "http://example.com/fox9.ts",
			TVGLogo:    "http://img.example.com/fox9.png",
		},
		{
			ItemKey:    "src:fox32:0002",
			ChannelKey: "name:fox 32 wfld",
			Name:       "FOX 32 WFLD",
			Group:      "Catalog",
			StreamURL:  "http://example.com/fox32.ts",
			TVGLogo:    "http://img.example.com/fox32.png",
		},
		{
			ItemKey:    "src:sports:0003",
			ChannelKey: "name:sports now",
			Name:       "Sports Now",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports.ts",
			TVGLogo:    "http://img.example.com/sports.png",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems(refreshed) error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	type itemResponse struct {
		ItemKey    string `json:"item_key"`
		ChannelKey string `json:"channel_key"`
		Name       string `json:"name"`
		Group      string `json:"group"`
		StreamURL  string `json:"stream_url"`
		Logo       string `json:"logo"`
		Active     bool   `json:"active"`
	}
	type itemsResponse struct {
		Items         []itemResponse               `json:"items"`
		Total         int                          `json:"total"`
		Limit         int                          `json:"limit"`
		Offset        int                          `json:"offset"`
		QRegex        bool                         `json:"q_regex"`
		SearchWarning *sqlite.CatalogSearchWarning `json:"search_warning,omitempty"`
	}
	fetch := func(path string) itemsResponse {
		t.Helper()
		var out itemsResponse
		doJSON(t, mux, http.MethodGet, path, nil, http.StatusOK, &out)
		return out
	}

	pageOne := fetch("/api/items?limit=2&offset=0")
	if pageOne.Total != 3 {
		t.Fatalf("pageOne total = %d, want 3", pageOne.Total)
	}
	if pageOne.QRegex {
		t.Fatalf("pageOne q_regex = true, want false")
	}
	if pageOne.Limit != 2 || pageOne.Offset != 0 {
		t.Fatalf("pageOne limit/offset = %d/%d, want 2/0", pageOne.Limit, pageOne.Offset)
	}
	if len(pageOne.Items) != 2 {
		t.Fatalf("len(pageOne.Items) = %d, want 2", len(pageOne.Items))
	}
	if pageOne.Items[0].ItemKey != "src:fox32:0002" || pageOne.Items[1].ItemKey != "src:fox9:0001" {
		t.Fatalf("pageOne item order = %#v, want [src:fox32:0002 src:fox9:0001]", []string{pageOne.Items[0].ItemKey, pageOne.Items[1].ItemKey})
	}
	if pageOne.SearchWarning != nil {
		t.Fatalf("pageOne search_warning = %#v, want nil when q is empty", pageOne.SearchWarning)
	}
	if pageOne.Items[0].Logo != "http://img.example.com/fox32.png" {
		t.Fatalf("pageOne first logo = %q, want %q", pageOne.Items[0].Logo, "http://img.example.com/fox32.png")
	}
	for i, item := range pageOne.Items {
		if !item.Active {
			t.Fatalf("pageOne item[%d] active = false, want true", i)
		}
		if item.StreamURL == "" {
			t.Fatalf("pageOne item[%d] stream_url = empty, want non-empty", i)
		}
		if item.ItemKey == "src:legacy:0004" {
			t.Fatal("pageOne unexpectedly included inactive stale item src:legacy:0004")
		}
	}

	pageTwo := fetch("/api/items?limit=2&offset=2")
	if pageTwo.Total != 3 || pageTwo.Limit != 2 || pageTwo.Offset != 2 {
		t.Fatalf("pageTwo total/limit/offset = %d/%d/%d, want 3/2/2", pageTwo.Total, pageTwo.Limit, pageTwo.Offset)
	}
	if len(pageTwo.Items) != 1 || pageTwo.Items[0].ItemKey != "src:sports:0003" {
		t.Fatalf("pageTwo items = %#v, want [src:sports:0003]", pageTwo.Items)
	}

	groupFiltered := fetch("/api/items?group=Catalog&limit=10&offset=0")
	if groupFiltered.Total != 2 || len(groupFiltered.Items) != 2 {
		t.Fatalf("groupFiltered total/len = %d/%d, want 2/2", groupFiltered.Total, len(groupFiltered.Items))
	}

	multiGroupFiltered := fetch("/api/items?group=Catalog&group=Sports&limit=10&offset=0")
	if multiGroupFiltered.Total != 3 || len(multiGroupFiltered.Items) != 3 {
		t.Fatalf("multiGroupFiltered total/len = %d/%d, want 3/3", multiGroupFiltered.Total, len(multiGroupFiltered.Items))
	}

	commaGroupFiltered := fetch("/api/items?group=Catalog,Sports&limit=10&offset=0")
	if commaGroupFiltered.Total != 3 || len(commaGroupFiltered.Items) != 3 {
		t.Fatalf("commaGroupFiltered total/len = %d/%d, want 3/3", commaGroupFiltered.Total, len(commaGroupFiltered.Items))
	}

	searchFiltered := fetch("/api/items?q=fox%20kmsp&limit=10&offset=0")
	if searchFiltered.Total != 1 || len(searchFiltered.Items) != 1 {
		t.Fatalf("searchFiltered total/len = %d/%d, want 1/1", searchFiltered.Total, len(searchFiltered.Items))
	}
	if searchFiltered.Items[0].ItemKey != "src:fox9:0001" {
		t.Fatalf("searchFiltered item key = %q, want src:fox9:0001", searchFiltered.Items[0].ItemKey)
	}
	if searchFiltered.SearchWarning == nil {
		t.Fatal("searchFiltered search_warning is nil, want token search warning metadata")
	}

	searchRegexFiltered := fetch("/api/items?q=fox%20kmsp&q_regex=1&limit=10&offset=0")
	if !searchRegexFiltered.QRegex {
		t.Fatalf("searchRegexFiltered q_regex = false, want true")
	}
	if searchRegexFiltered.Total != 0 || len(searchRegexFiltered.Items) != 0 {
		t.Fatalf("searchRegexFiltered total/len = %d/%d, want 0/0 (regex mode uses full query pattern)", searchRegexFiltered.Total, len(searchRegexFiltered.Items))
	}

	regexPatternFiltered := fetch("/api/items?q=fox%5Cs%2B3%5B0-9%5D&q_regex=1&limit=10&offset=0")
	if regexPatternFiltered.Total != 1 || len(regexPatternFiltered.Items) != 1 {
		t.Fatalf("regexPatternFiltered total/len = %d/%d, want 1/1", regexPatternFiltered.Total, len(regexPatternFiltered.Items))
	}
	if regexPatternFiltered.Items[0].ItemKey != "src:fox32:0002" {
		t.Fatalf("regexPatternFiltered item key = %q, want src:fox32:0002", regexPatternFiltered.Items[0].ItemKey)
	}

	invalidRegexFlag := doRaw(t, mux, http.MethodGet, "/api/items?q=fox&q_regex=maybe", nil)
	if invalidRegexFlag.Code != http.StatusBadRequest {
		t.Fatalf("GET /api/items invalid q_regex status = %d, want %d", invalidRegexFlag.Code, http.StatusBadRequest)
	}

	invalidRegexPattern := doRaw(t, mux, http.MethodGet, "/api/items?q=%28%5B&q_regex=1", nil)
	if invalidRegexPattern.Code != http.StatusBadRequest {
		t.Fatalf("GET /api/items invalid regex pattern status = %d, want %d", invalidRegexPattern.Code, http.StatusBadRequest)
	}
}

func TestAdminRoutesItemsSourceIDFiltering(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Secondary Items Source",
		PlaylistURL: "http://example.com/secondary-items.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}

	if err := store.UpsertPlaylistItemsForSource(ctx, 1, []playlist.Item{
		{ItemKey: "src:items:primary:news", ChannelKey: "name:items-primary-news", Name: "Primary News", Group: "News", StreamURL: "http://example.com/items-primary-news.ts"},
		{ItemKey: "src:items:primary:sports", ChannelKey: "name:items-primary-sports", Name: "Primary Sports", Group: "Sports", StreamURL: "http://example.com/items-primary-sports.ts"},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(primary) error = %v", err)
	}
	if err := store.UpsertPlaylistItemsForSource(ctx, secondary.SourceID, []playlist.Item{
		{ItemKey: "src:items:secondary:news", ChannelKey: "name:items-secondary-news", Name: "Secondary News", Group: "News", StreamURL: "http://example.com/items-secondary-news.ts"},
		{ItemKey: "src:items:secondary:movies", ChannelKey: "name:items-secondary-movies", Name: "Secondary Movies", Group: "Movies", StreamURL: "http://example.com/items-secondary-movies.ts"},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(secondary) error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	primary, err := store.GetPlaylistSource(ctx, 1)
	if err != nil {
		t.Fatalf("GetPlaylistSource(primary) error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	type itemResponse struct {
		ItemKey            string `json:"item_key"`
		PlaylistSourceID   int64  `json:"playlist_source_id"`
		PlaylistSourceName string `json:"playlist_source_name"`
	}
	type itemsResponse struct {
		Items []itemResponse `json:"items"`
		Total int            `json:"total"`
	}
	fetch := func(path string) itemsResponse {
		t.Helper()
		var out itemsResponse
		doJSON(t, mux, http.MethodGet, path, nil, http.StatusOK, &out)
		return out
	}

	secondaryOnly := fetch(fmt.Sprintf("/api/items?source_ids=%d&limit=20&offset=0", secondary.SourceID))
	if secondaryOnly.Total != 2 || len(secondaryOnly.Items) != 2 {
		t.Fatalf("secondaryOnly total/len = %d/%d, want 2/2", secondaryOnly.Total, len(secondaryOnly.Items))
	}
	if secondaryOnly.Items[0].ItemKey != "src:items:secondary:movies" || secondaryOnly.Items[1].ItemKey != "src:items:secondary:news" {
		t.Fatalf("secondaryOnly item order = %#v, want [src:items:secondary:movies src:items:secondary:news]", []string{secondaryOnly.Items[0].ItemKey, secondaryOnly.Items[1].ItemKey})
	}
	for _, item := range secondaryOnly.Items {
		if item.PlaylistSourceID != secondary.SourceID {
			t.Fatalf("secondaryOnly item %q playlist_source_id = %d, want %d", item.ItemKey, item.PlaylistSourceID, secondary.SourceID)
		}
		if item.PlaylistSourceName != secondary.Name {
			t.Fatalf("secondaryOnly item %q playlist_source_name = %q, want %q", item.ItemKey, item.PlaylistSourceName, secondary.Name)
		}
	}

	combined := fetch(fmt.Sprintf("/api/items?source_ids=1,%d&limit=20&offset=0", secondary.SourceID))
	if combined.Total != 4 || len(combined.Items) != 4 {
		t.Fatalf("combined total/len = %d/%d, want 4/4", combined.Total, len(combined.Items))
	}
	wantCombined := []string{
		"src:items:secondary:movies",
		"src:items:primary:news",
		"src:items:secondary:news",
		"src:items:primary:sports",
	}
	gotCombined := []string{
		combined.Items[0].ItemKey,
		combined.Items[1].ItemKey,
		combined.Items[2].ItemKey,
		combined.Items[3].ItemKey,
	}
	if strings.Join(gotCombined, ",") != strings.Join(wantCombined, ",") {
		t.Fatalf("combined item order = %#v, want %#v", gotCombined, wantCombined)
	}
	for _, item := range combined.Items {
		switch item.ItemKey {
		case "src:items:secondary:movies", "src:items:secondary:news":
			if item.PlaylistSourceID != secondary.SourceID {
				t.Fatalf("combined item %q playlist_source_id = %d, want %d", item.ItemKey, item.PlaylistSourceID, secondary.SourceID)
			}
			if item.PlaylistSourceName != secondary.Name {
				t.Fatalf("combined item %q playlist_source_name = %q, want %q", item.ItemKey, item.PlaylistSourceName, secondary.Name)
			}
		case "src:items:primary:news", "src:items:primary:sports":
			if item.PlaylistSourceID != primary.SourceID {
				t.Fatalf("combined item %q playlist_source_id = %d, want %d", item.ItemKey, item.PlaylistSourceID, primary.SourceID)
			}
			if item.PlaylistSourceName != primary.Name {
				t.Fatalf("combined item %q playlist_source_name = %q, want %q", item.ItemKey, item.PlaylistSourceName, primary.Name)
			}
		default:
			t.Fatalf("unexpected item in combined response: %q", item.ItemKey)
		}
	}

	emptySourceFilter := fetch("/api/items?source_ids=&limit=20&offset=0")
	if emptySourceFilter.Total != 4 || len(emptySourceFilter.Items) != 4 {
		t.Fatalf("emptySourceFilter total/len = %d/%d, want 4/4", emptySourceFilter.Total, len(emptySourceFilter.Items))
	}

	newsScoped := fetch(fmt.Sprintf("/api/items?source_ids=%d&group=News&limit=20&offset=0", secondary.SourceID))
	if newsScoped.Total != 1 || len(newsScoped.Items) != 1 || newsScoped.Items[0].ItemKey != "src:items:secondary:news" {
		t.Fatalf("newsScoped = %+v, want only src:items:secondary:news", newsScoped)
	}

	for _, path := range []string{
		"/api/items?source_ids=0",
		"/api/items?source_ids=abc",
		"/api/items?source_ids=1,not-a-number",
	} {
		rec := doRaw(t, mux, http.MethodGet, path, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want %d", path, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestAdminRoutesItemsSearchSupportsExclusionTokens(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:fox9:english",
			ChannelKey: "name:fox 9 kmsp",
			Name:       "FOX 9 KMSP",
			Group:      "Catalog",
			StreamURL:  "http://example.com/fox9.ts",
		},
		{
			ItemKey:    "src:fox9:spanish",
			ChannelKey: "name:fox 9 spanish",
			Name:       "FOX 9 Spanish",
			Group:      "Catalog",
			StreamURL:  "http://example.com/fox9-spanish.ts",
		},
		{
			ItemKey:    "src:foxsports:720p",
			ChannelKey: "name:fox sports 720p",
			Name:       "FOX Sports 720p",
			Group:      "Catalog",
			StreamURL:  "http://example.com/fox-sports-720p.ts",
		},
		{
			ItemKey:    "src:sports:1080p",
			ChannelKey: "name:sports 1080p",
			Name:       "Sports 1080p",
			Group:      "Catalog",
			StreamURL:  "http://example.com/sports-1080p.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	type itemResponse struct {
		ItemKey string `json:"item_key"`
		Name    string `json:"name"`
	}
	type itemsResponse struct {
		Items []itemResponse `json:"items"`
		Total int            `json:"total"`
	}
	fetch := func(path string) itemsResponse {
		t.Helper()
		var out itemsResponse
		doJSON(t, mux, http.MethodGet, path, nil, http.StatusOK, &out)
		return out
	}

	filtered := fetch("/api/items?q=fox%20-spanish%20!720p&limit=10&offset=0")
	if filtered.Total != 1 || len(filtered.Items) != 1 {
		t.Fatalf("filtered total/len = %d/%d, want 1/1", filtered.Total, len(filtered.Items))
	}
	if filtered.Items[0].ItemKey != "src:fox9:english" {
		t.Fatalf("filtered item key = %q, want src:fox9:english", filtered.Items[0].ItemKey)
	}

	exclusionOnly := fetch("/api/items?q=-spanish&limit=10&offset=0")
	if exclusionOnly.Total != 3 || len(exclusionOnly.Items) != 3 {
		t.Fatalf("exclusion-only total/len = %d/%d, want 3/3", exclusionOnly.Total, len(exclusionOnly.Items))
	}
	for _, item := range exclusionOnly.Items {
		if item.ItemKey == "src:fox9:spanish" {
			t.Fatalf("exclusion-only results unexpectedly contain src:fox9:spanish: %#v", exclusionOnly.Items)
		}
	}
}

func TestAdminRoutesItemsWarningWhenCatalogSearchTokensAreTruncated(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.OpenWithOptions(":memory:", sqlite.SQLiteOptions{
		CatalogSearchLimits: sqlite.CatalogSearchLimits{
			MaxTerms: 1,
		},
	})
	if err != nil {
		t.Fatalf("OpenWithOptions() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:warning:one",
			ChannelKey: "name:alpha beta gamma",
			Name:       "Alpha Beta Gamma",
			Group:      "Catalog",
			StreamURL:  "http://example.com/alpha-beta-gamma.ts",
		},
		{
			ItemKey:    "src:warning:two",
			ChannelKey: "name:delta epsilon",
			Name:       "Delta Epsilon",
			Group:      "Catalog",
			StreamURL:  "http://example.com/delta-epsilon.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	type itemResponse struct {
		ItemKey string `json:"item_key"`
		Name    string `json:"name"`
	}
	type itemsResponse struct {
		Items         []itemResponse               `json:"items"`
		Total         int                          `json:"total"`
		Limit         int                          `json:"limit"`
		Offset        int                          `json:"offset"`
		QRegex        bool                         `json:"q_regex"`
		SearchWarning *sqlite.CatalogSearchWarning `json:"search_warning,omitempty"`
	}

	var out itemsResponse
	doJSON(
		t,
		mux,
		http.MethodGet,
		"/api/items?q=alpha%20beta%20gamma&limit=10&offset=0",
		nil,
		http.StatusOK,
		&out,
	)
	if out.SearchWarning == nil {
		t.Fatal("search_warning is nil for over-limit query")
	}
	if !out.SearchWarning.Truncated {
		t.Fatal("search_warning.truncated = false, want true")
	}
	if out.SearchWarning.Mode != "token" {
		t.Fatalf("search_warning.mode = %q, want token", out.SearchWarning.Mode)
	}
	if out.SearchWarning.TermsApplied != 1 || out.SearchWarning.TermsDropped != 2 {
		t.Fatalf(
			"search_warning terms applied/dropped = %d/%d, want 1/2",
			out.SearchWarning.TermsApplied,
			out.SearchWarning.TermsDropped,
		)
	}
}

func TestAdminRoutesItemsWarningLookupErrorIsNonFatal(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:warning:error",
			ChannelKey: "name:warning error",
			Name:       "Warning Error",
			Group:      "Catalog",
			StreamURL:  "http://example.com/warning-error.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	catalog := &catalogWarningErrorStore{
		CatalogStore: store,
		warningErr:   fmt.Errorf("warning backend unavailable"),
	}
	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(catalog, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	var logBuffer bytes.Buffer
	handler.SetLogger(slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelInfo})))

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	type itemResponse struct {
		ItemKey string `json:"item_key"`
	}
	type itemsResponse struct {
		Items         []itemResponse               `json:"items"`
		Total         int                          `json:"total"`
		SearchWarning *sqlite.CatalogSearchWarning `json:"search_warning,omitempty"`
	}

	var out itemsResponse
	doJSON(
		t,
		mux,
		http.MethodGet,
		"/api/items?q=warning&limit=10&offset=0",
		nil,
		http.StatusOK,
		&out,
	)
	if out.Total != 1 || len(out.Items) != 1 || out.Items[0].ItemKey != "src:warning:error" {
		t.Fatalf("GET /api/items warning lookup fallback response = %+v, want one matching item", out)
	}
	if out.SearchWarning != nil {
		t.Fatalf("search_warning = %#v, want nil when warning lookup fails", out.SearchWarning)
	}
	if catalog.warningCalls != 1 {
		t.Fatalf("catalog warning call count = %d, want 1", catalog.warningCalls)
	}
	if !strings.Contains(logBuffer.String(), "catalog search warning check failed") {
		t.Fatalf("logs = %q, want warning lookup failure log", logBuffer.String())
	}
}

func TestAdminRoutesGroupsPaginationAndValidation(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	items := []playlist.Item{
		{ItemKey: "src:groups:movies:001", ChannelKey: "name:movies", Name: "Movies 1", Group: "Movies", StreamURL: "http://example.com/groups/movies-1.ts"},
		{ItemKey: "src:groups:news:001", ChannelKey: "name:news", Name: "News 1", Group: "News", StreamURL: "http://example.com/groups/news-1.ts"},
		{ItemKey: "src:groups:news:002", ChannelKey: "name:news", Name: "News 2", Group: "News", StreamURL: "http://example.com/groups/news-2.ts"},
		{ItemKey: "src:groups:sports:001", ChannelKey: "name:sports", Name: "Sports 1", Group: "Sports", StreamURL: "http://example.com/groups/sports-1.ts"},
		{ItemKey: "src:groups:sports:002", ChannelKey: "name:sports", Name: "Sports 2", Group: "Sports", StreamURL: "http://example.com/groups/sports-2.ts"},
		{ItemKey: "src:groups:sports:003", ChannelKey: "name:sports", Name: "Sports 3", Group: "Sports", StreamURL: "http://example.com/groups/sports-3.ts"},
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	type group struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	type groupsResponse struct {
		Groups        []group `json:"groups"`
		Total         int     `json:"total"`
		Limit         int     `json:"limit"`
		Offset        int     `json:"offset"`
		IncludeCounts bool    `json:"include_counts"`
	}

	var paged groupsResponse
	doJSON(t, mux, http.MethodGet, "/api/groups?limit=2&offset=1", nil, http.StatusOK, &paged)
	if paged.Total != 3 {
		t.Fatalf("paged total = %d, want 3", paged.Total)
	}
	if paged.Limit != 2 || paged.Offset != 1 {
		t.Fatalf("paged limit/offset = %d/%d, want 2/1", paged.Limit, paged.Offset)
	}
	if !paged.IncludeCounts {
		t.Fatal("paged include_counts = false, want true default")
	}
	if len(paged.Groups) != 2 {
		t.Fatalf("len(paged groups) = %d, want 2", len(paged.Groups))
	}
	if paged.Groups[0].Name != "News" || paged.Groups[1].Name != "Sports" {
		t.Fatalf("paged group names = [%q,%q], want [News,Sports]", paged.Groups[0].Name, paged.Groups[1].Name)
	}
	if paged.Groups[0].Count != 2 || paged.Groups[1].Count != 3 {
		t.Fatalf("paged group counts = [%d,%d], want [2,3]", paged.Groups[0].Count, paged.Groups[1].Count)
	}

	var defaults groupsResponse
	doJSON(t, mux, http.MethodGet, "/api/groups", nil, http.StatusOK, &defaults)
	if defaults.Total != 3 {
		t.Fatalf("default total = %d, want 3", defaults.Total)
	}
	if defaults.Limit != defaultGroupsListLimit || defaults.Offset != 0 {
		t.Fatalf("default limit/offset = %d/%d, want %d/0", defaults.Limit, defaults.Offset, defaultGroupsListLimit)
	}
	if len(defaults.Groups) != 3 {
		t.Fatalf("len(default groups) = %d, want 3", len(defaults.Groups))
	}

	var capped groupsResponse
	doJSON(
		t,
		mux,
		http.MethodGet,
		fmt.Sprintf("/api/groups?limit=%d", maxGroupsListLimit+250),
		nil,
		http.StatusOK,
		&capped,
	)
	if capped.Limit != maxGroupsListLimit {
		t.Fatalf("capped limit = %d, want %d", capped.Limit, maxGroupsListLimit)
	}
	if len(capped.Groups) != 3 {
		t.Fatalf("len(capped groups) = %d, want 3", len(capped.Groups))
	}

	var normalized groupsResponse
	doJSON(t, mux, http.MethodGet, "/api/groups?limit=0", nil, http.StatusOK, &normalized)
	if normalized.Limit != defaultGroupsListLimit {
		t.Fatalf("normalized limit = %d, want %d", normalized.Limit, defaultGroupsListLimit)
	}

	var namesOnly struct {
		Groups        []map[string]any `json:"groups"`
		Total         int              `json:"total"`
		Limit         int              `json:"limit"`
		Offset        int              `json:"offset"`
		IncludeCounts bool             `json:"include_counts"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		"/api/groups?limit=2&offset=0&include_counts=0",
		nil,
		http.StatusOK,
		&namesOnly,
	)
	if namesOnly.IncludeCounts {
		t.Fatal("namesOnly include_counts = true, want false")
	}
	if namesOnly.Total != 3 || namesOnly.Limit != 2 || namesOnly.Offset != 0 {
		t.Fatalf(
			"namesOnly total/limit/offset = %d/%d/%d, want 3/2/0",
			namesOnly.Total,
			namesOnly.Limit,
			namesOnly.Offset,
		)
	}
	if len(namesOnly.Groups) != 2 {
		t.Fatalf("len(namesOnly groups) = %d, want 2", len(namesOnly.Groups))
	}
	for idx, row := range namesOnly.Groups {
		if _, ok := row["count"]; ok {
			t.Fatalf("namesOnly group[%d] unexpectedly contains count field", idx)
		}
	}

	for _, path := range []string{
		"/api/groups?limit=-1",
		"/api/groups?limit=abc",
		"/api/groups?offset=-1",
		"/api/groups?offset=nan",
		"/api/groups?include_counts=maybe",
	} {
		rec := doRaw(t, mux, http.MethodGet, path, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want %d", path, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestAdminRoutesGroupsSourceIDFiltering(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	secondary, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Secondary Groups Source",
		PlaylistURL: "http://example.com/secondary-groups.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(secondary) error = %v", err)
	}

	if err := store.UpsertPlaylistItemsForSource(ctx, 1, []playlist.Item{
		{ItemKey: "src:groups:primary:news", ChannelKey: "name:primary-news", Name: "Primary News", Group: "News", StreamURL: "http://example.com/primary-news.ts"},
		{ItemKey: "src:groups:primary:sports", ChannelKey: "name:primary-sports", Name: "Primary Sports", Group: "Sports", StreamURL: "http://example.com/primary-sports.ts"},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(primary) error = %v", err)
	}
	if err := store.UpsertPlaylistItemsForSource(ctx, secondary.SourceID, []playlist.Item{
		{ItemKey: "src:groups:secondary:news", ChannelKey: "name:secondary-news", Name: "Secondary News", Group: "News", StreamURL: "http://example.com/secondary-news.ts"},
		{ItemKey: "src:groups:secondary:movies", ChannelKey: "name:secondary-movies", Name: "Secondary Movies", Group: "Movies", StreamURL: "http://example.com/secondary-movies.ts"},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItemsForSource(secondary) error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	type group struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	type groupsResponse struct {
		Groups []group `json:"groups"`
		Total  int     `json:"total"`
	}

	var secondaryOnly groupsResponse
	doJSON(
		t,
		mux,
		http.MethodGet,
		fmt.Sprintf("/api/groups?source_ids=%d", secondary.SourceID),
		nil,
		http.StatusOK,
		&secondaryOnly,
	)
	if secondaryOnly.Total != 2 {
		t.Fatalf("secondaryOnly total = %d, want 2", secondaryOnly.Total)
	}
	if len(secondaryOnly.Groups) != 2 {
		t.Fatalf("len(secondaryOnly groups) = %d, want 2", len(secondaryOnly.Groups))
	}
	if secondaryOnly.Groups[0].Name != "Movies" || secondaryOnly.Groups[1].Name != "News" {
		t.Fatalf("secondaryOnly group names = [%q,%q], want [Movies,News]", secondaryOnly.Groups[0].Name, secondaryOnly.Groups[1].Name)
	}
	if secondaryOnly.Groups[0].Count != 1 || secondaryOnly.Groups[1].Count != 1 {
		t.Fatalf("secondaryOnly counts = [%d,%d], want [1,1]", secondaryOnly.Groups[0].Count, secondaryOnly.Groups[1].Count)
	}

	var combined groupsResponse
	doJSON(
		t,
		mux,
		http.MethodGet,
		fmt.Sprintf("/api/groups?source_ids=1,%d", secondary.SourceID),
		nil,
		http.StatusOK,
		&combined,
	)
	if combined.Total != 3 {
		t.Fatalf("combined total = %d, want 3", combined.Total)
	}
	if len(combined.Groups) != 3 {
		t.Fatalf("len(combined groups) = %d, want 3", len(combined.Groups))
	}
	if combined.Groups[0].Name != "Movies" || combined.Groups[1].Name != "News" || combined.Groups[2].Name != "Sports" {
		t.Fatalf("combined group names = %#v, want [Movies News Sports]", combined.Groups)
	}

	for _, path := range []string{
		"/api/groups?source_ids=0",
		"/api/groups?source_ids=abc",
		"/api/groups?source_ids=1,not-a-number",
	} {
		rec := doRaw(t, mux, http.MethodGet, path, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want %d", path, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestAdminRoutesGroupsSourceIDFilteringUnsupportedReturnsContractError(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.catalog = catalogWithoutSourceScopedSupport{CatalogStore: handler.catalog}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var payload struct {
		Error     string `json:"error"`
		Message   string `json:"message"`
		Operation string `json:"operation"`
		Parameter string `json:"parameter"`
		Detail    string `json:"detail"`
	}
	doJSON(t, mux, http.MethodGet, "/api/groups?source_ids=1", nil, http.StatusNotImplemented, &payload)
	if payload.Error != "source_scoped_operation_unsupported" {
		t.Fatalf("error = %q, want source_scoped_operation_unsupported", payload.Error)
	}
	if payload.Parameter != "source_ids" {
		t.Fatalf("parameter = %q, want source_ids", payload.Parameter)
	}
	if payload.Operation != "groups_list" {
		t.Fatalf("operation = %q, want groups_list", payload.Operation)
	}
	if !strings.Contains(payload.Detail, "source-scoped group backend support") {
		t.Fatalf("detail = %q, want source-scoped group backend support message", payload.Detail)
	}
}

func TestAdminRoutesGroupsDefaultPaginationBoundsHighCardinality(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const totalGroups = 560
	items := make([]playlist.Item, 0, totalGroups)
	for i := range totalGroups {
		items = append(items, playlist.Item{
			ItemKey:    fmt.Sprintf("src:group-load:%03d", i),
			ChannelKey: fmt.Sprintf("name:group-load-%03d", i),
			Name:       fmt.Sprintf("Group Load %03d", i),
			Group:      fmt.Sprintf("Group %03d", i),
			StreamURL:  fmt.Sprintf("http://example.com/group-load/%03d.ts", i),
		})
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var defaultPage struct {
		Groups        []map[string]any `json:"groups"`
		Total         int              `json:"total"`
		Limit         int              `json:"limit"`
		Offset        int              `json:"offset"`
		IncludeCounts bool             `json:"include_counts"`
	}
	doJSON(t, mux, http.MethodGet, "/api/groups", nil, http.StatusOK, &defaultPage)
	if defaultPage.Total != totalGroups {
		t.Fatalf("default total = %d, want %d", defaultPage.Total, totalGroups)
	}
	if defaultPage.Limit != defaultGroupsListLimit {
		t.Fatalf("default limit = %d, want %d", defaultPage.Limit, defaultGroupsListLimit)
	}
	if defaultPage.Offset != 0 {
		t.Fatalf("default offset = %d, want 0", defaultPage.Offset)
	}
	if !defaultPage.IncludeCounts {
		t.Fatal("default include_counts = false, want true")
	}
	if len(defaultPage.Groups) != defaultGroupsListLimit {
		t.Fatalf("len(default groups) = %d, want %d", len(defaultPage.Groups), defaultGroupsListLimit)
	}

	countsPath := fmt.Sprintf(
		"/api/groups?limit=%d&offset=0&include_counts=1",
		defaultGroupsListLimit,
	)
	namesPath := fmt.Sprintf(
		"/api/groups?limit=%d&offset=0&include_counts=0",
		defaultGroupsListLimit,
	)

	startedAt := time.Now()
	countsRec := doRaw(t, mux, http.MethodGet, countsPath, nil)
	countsLatency := time.Since(startedAt)
	if countsRec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d", countsPath, countsRec.Code, http.StatusOK)
	}

	startedAt = time.Now()
	namesRec := doRaw(t, mux, http.MethodGet, namesPath, nil)
	namesLatency := time.Since(startedAt)
	if namesRec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d", namesPath, namesRec.Code, http.StatusOK)
	}

	countsBytes := countsRec.Body.Len()
	namesBytes := namesRec.Body.Len()
	if countsBytes <= 5000 {
		t.Fatalf("counted page payload bytes = %d, want > 5000", countsBytes)
	}
	if namesBytes >= countsBytes {
		t.Fatalf("names-only payload bytes = %d, want < counted payload bytes %d", namesBytes, countsBytes)
	}

	t.Logf(
		"/api/groups high-cardinality evidence: total=%d default_limit=%d counts_bytes=%d names_only_bytes=%d counts_latency_ms=%d names_only_latency_ms=%d",
		totalGroups,
		defaultGroupsListLimit,
		countsBytes,
		namesBytes,
		countsLatency.Milliseconds(),
		namesLatency.Milliseconds(),
	)
}

func TestAdminRoutesChannelsPaginationAndValidation(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	items := []playlist.Item{
		{ItemKey: "src:chan:1", ChannelKey: "name:chan-1", Name: "Channel 1", Group: "News", StreamURL: "http://example.com/chan-1.ts"},
		{ItemKey: "src:chan:2", ChannelKey: "name:chan-2", Name: "Channel 2", Group: "News", StreamURL: "http://example.com/chan-2.ts"},
		{ItemKey: "src:chan:3", ChannelKey: "name:chan-3", Name: "Channel 3", Group: "News", StreamURL: "http://example.com/chan-3.ts"},
		{ItemKey: "src:chan:4", ChannelKey: "name:chan-4", Name: "Channel 4", Group: "News", StreamURL: "http://example.com/chan-4.ts"},
		{ItemKey: "src:chan:5", ChannelKey: "name:chan-5", Name: "Channel 5", Group: "News", StreamURL: "http://example.com/chan-5.ts"},
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	fakeDVR := &fakeDVRService{}
	handler.SetDVRService(fakeDVR)
	handler.dvrLineupReloadDebounce = 25 * time.Millisecond
	handler.dvrLineupReloadMaxWait = 125 * time.Millisecond
	defer handler.Close()
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	createdChannels := make([]channels.Channel, 0, len(items))
	for i, item := range items {
		req := map[string]any{
			"item_key": item.ItemKey,
		}
		if i == 0 {
			req["dynamic_rule"] = map[string]any{
				"enabled":      true,
				"group_name":   "News",
				"search_query": "Channel 1",
			}
		}

		var created channels.Channel
		doJSON(t, mux, http.MethodPost, "/api/channels", req, http.StatusOK, &created)
		createdChannels = append(createdChannels, created)
	}

	var manualSource channels.Source
	doJSON(
		t,
		mux,
		http.MethodPost,
		fmt.Sprintf("/api/channels/%d/sources", createdChannels[0].ChannelID),
		map[string]any{
			"item_key":            "src:chan:2",
			"allow_cross_channel": true,
		},
		http.StatusOK,
		&manualSource,
	)

	disabled := false
	doJSON(
		t,
		mux,
		http.MethodPatch,
		fmt.Sprintf("/api/channels/%d/sources/%d", createdChannels[0].ChannelID, manualSource.SourceID),
		map[string]any{"enabled": disabled},
		http.StatusOK,
		nil,
	)

	var paged struct {
		Channels []channels.Channel `json:"channels"`
		Total    int                `json:"total"`
		Limit    int                `json:"limit"`
		Offset   int                `json:"offset"`
	}
	doJSON(t, mux, http.MethodGet, "/api/channels?limit=2&offset=1", nil, http.StatusOK, &paged)
	if paged.Total != len(items) {
		t.Fatalf("paged total = %d, want %d", paged.Total, len(items))
	}
	if paged.Limit != 2 {
		t.Fatalf("paged limit = %d, want 2", paged.Limit)
	}
	if paged.Offset != 1 {
		t.Fatalf("paged offset = %d, want 1", paged.Offset)
	}
	if len(paged.Channels) != 2 {
		t.Fatalf("len(paged channels) = %d, want 2", len(paged.Channels))
	}
	if paged.Channels[0].GuideNumber != "101" || paged.Channels[1].GuideNumber != "102" {
		t.Fatalf("paged guide numbers = [%q,%q], want [101,102]", paged.Channels[0].GuideNumber, paged.Channels[1].GuideNumber)
	}

	var defaults struct {
		Channels []channels.Channel `json:"channels"`
		Total    int                `json:"total"`
		Limit    int                `json:"limit"`
		Offset   int                `json:"offset"`
	}
	doJSON(t, mux, http.MethodGet, "/api/channels", nil, http.StatusOK, &defaults)
	if defaults.Limit != defaultChannelsListLimit {
		t.Fatalf("default limit = %d, want %d", defaults.Limit, defaultChannelsListLimit)
	}
	if defaults.Offset != 0 {
		t.Fatalf("default offset = %d, want 0", defaults.Offset)
	}
	if defaults.Total != len(items) {
		t.Fatalf("default total = %d, want %d", defaults.Total, len(items))
	}
	if len(defaults.Channels) != len(items) {
		t.Fatalf("len(default channels) = %d, want %d", len(defaults.Channels), len(items))
	}
	if !defaults.Channels[0].DynamicRule.Enabled {
		t.Fatal("defaults.Channels[0].DynamicRule.Enabled = false, want true")
	}
	if defaults.Channels[0].SourceTotal != 2 {
		t.Fatalf("defaults.Channels[0].SourceTotal = %d, want 2", defaults.Channels[0].SourceTotal)
	}
	if defaults.Channels[0].SourceEnabled != 1 {
		t.Fatalf("defaults.Channels[0].SourceEnabled = %d, want 1", defaults.Channels[0].SourceEnabled)
	}
	if defaults.Channels[0].SourceDynamic != 1 {
		t.Fatalf("defaults.Channels[0].SourceDynamic = %d, want 1", defaults.Channels[0].SourceDynamic)
	}
	if defaults.Channels[0].SourceManual != 1 {
		t.Fatalf("defaults.Channels[0].SourceManual = %d, want 1", defaults.Channels[0].SourceManual)
	}

	var capped struct {
		Channels []channels.Channel `json:"channels"`
		Limit    int                `json:"limit"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		fmt.Sprintf("/api/channels?limit=%d", maxChannelsListLimit+250),
		nil,
		http.StatusOK,
		&capped,
	)
	if capped.Limit != maxChannelsListLimit {
		t.Fatalf("capped limit = %d, want %d", capped.Limit, maxChannelsListLimit)
	}
	if len(capped.Channels) != len(items) {
		t.Fatalf("len(capped channels) = %d, want %d", len(capped.Channels), len(items))
	}

	var normalized struct {
		Channels []channels.Channel `json:"channels"`
		Limit    int                `json:"limit"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		"/api/channels?limit=0",
		nil,
		http.StatusOK,
		&normalized,
	)
	if normalized.Limit != defaultChannelsListLimit {
		t.Fatalf("normalized limit = %d, want %d", normalized.Limit, defaultChannelsListLimit)
	}
	if len(normalized.Channels) != len(items) {
		t.Fatalf("len(normalized channels) = %d, want %d", len(normalized.Channels), len(items))
	}

	for _, path := range []string{
		"/api/channels?limit=-1",
		"/api/channels?limit=abc",
		"/api/channels?offset=-1",
		"/api/channels?offset=nan",
	} {
		rec := doRaw(t, mux, http.MethodGet, path, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want %d", path, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestAdminRoutesDuplicateSuggestionsPaginationAndValidation(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{ItemKey: "src:dup:alpha:1", ChannelKey: "tvg:alpha", Name: "Alpha 1", Group: "Dupes", StreamURL: "http://example.com/alpha-1.ts"},
		{ItemKey: "src:dup:alpha:2", ChannelKey: "tvg:alpha", Name: "Alpha 2", Group: "Dupes", StreamURL: "http://example.com/alpha-2.ts"},
		{ItemKey: "src:dup:bravo:1", ChannelKey: "tvg:bravo", Name: "Bravo 1", Group: "Dupes", StreamURL: "http://example.com/bravo-1.ts"},
		{ItemKey: "src:dup:bravo:2", ChannelKey: "tvg:bravo", Name: "Bravo 2", Group: "Dupes", StreamURL: "http://example.com/bravo-2.ts"},
		{ItemKey: "src:dup:charlie:1", ChannelKey: "tvg:charlie", Name: "Charlie 1", Group: "Dupes", StreamURL: "http://example.com/charlie-1.ts"},
		{ItemKey: "src:dup:charlie:2", ChannelKey: "tvg:charlie", Name: "Charlie 2", Group: "Dupes", StreamURL: "http://example.com/charlie-2.ts"},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var paged struct {
		Groups []channels.DuplicateGroup `json:"groups"`
		Total  int                       `json:"total"`
		Limit  int                       `json:"limit"`
		Offset int                       `json:"offset"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		"/api/suggestions/duplicates?min=2&limit=2&offset=1",
		nil,
		http.StatusOK,
		&paged,
	)
	if paged.Total != 3 {
		t.Fatalf("paged total = %d, want 3", paged.Total)
	}
	if paged.Limit != 2 {
		t.Fatalf("paged limit = %d, want 2", paged.Limit)
	}
	if paged.Offset != 1 {
		t.Fatalf("paged offset = %d, want 1", paged.Offset)
	}
	if len(paged.Groups) != 2 {
		t.Fatalf("len(paged groups) = %d, want 2", len(paged.Groups))
	}
	if paged.Groups[0].ChannelKey != "tvg:bravo" || paged.Groups[1].ChannelKey != "tvg:charlie" {
		t.Fatalf("paged channel keys = [%q,%q], want [tvg:bravo,tvg:charlie]", paged.Groups[0].ChannelKey, paged.Groups[1].ChannelKey)
	}

	var capped struct {
		Limit int `json:"limit"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		fmt.Sprintf("/api/suggestions/duplicates?limit=%d", maxDuplicateSuggestionsLimit+500),
		nil,
		http.StatusOK,
		&capped,
	)
	if capped.Limit != maxDuplicateSuggestionsLimit {
		t.Fatalf("capped limit = %d, want %d", capped.Limit, maxDuplicateSuggestionsLimit)
	}

	var normalized struct {
		Limit int `json:"limit"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		"/api/suggestions/duplicates?limit=0",
		nil,
		http.StatusOK,
		&normalized,
	)
	if normalized.Limit != defaultDuplicateSuggestionsLimit {
		t.Fatalf("normalized limit = %d, want %d", normalized.Limit, defaultDuplicateSuggestionsLimit)
	}

	for _, path := range []string{
		"/api/suggestions/duplicates?limit=-1",
		"/api/suggestions/duplicates?limit=abc",
		"/api/suggestions/duplicates?offset=-1",
		"/api/suggestions/duplicates?offset=nan",
	} {
		rec := doRaw(t, mux, http.MethodGet, path, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want %d", path, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestAdminRoutesDynamicChannelQueryFlowAndImmediateSync(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:dyn:news:one",
			ChannelKey: "name:dyn news one",
			Name:       "Dynamic News One",
			Group:      "News",
			StreamURL:  "http://example.com/dyn-news-one.ts",
		},
		{
			ItemKey:    "src:dyn:news:two",
			ChannelKey: "name:dyn news two",
			Name:       "Dynamic News Two",
			Group:      "News",
			StreamURL:  "http://example.com/dyn-news-two.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	fakeDVR := &fakeDVRService{}
	handler.SetDVRService(fakeDVR)
	handler.dvrLineupReloadDebounce = 25 * time.Millisecond
	handler.dvrLineupReloadMaxWait = 125 * time.Millisecond
	defer handler.Close()
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created struct {
		QueryID     int64    `json:"query_id"`
		BlockStart  int      `json:"block_start"`
		BlockEnd    int      `json:"block_end"`
		Name        string   `json:"name"`
		GroupName   string   `json:"group_name"`
		GroupNames  []string `json:"group_names"`
		SearchRegex bool     `json:"search_regex"`
	}
	doJSON(t, mux, http.MethodPost, "/api/dynamic-channels", map[string]any{
		"name":         "News Block",
		"group_names":  []string{"Sports", "News", "sports"},
		"search_query": "news",
	}, http.StatusOK, &created)
	if created.QueryID <= 0 {
		t.Fatalf("created.QueryID = %d, want > 0", created.QueryID)
	}
	if created.BlockStart != 10000 || created.BlockEnd != 10999 {
		t.Fatalf("created block range = [%d,%d], want [10000,10999]", created.BlockStart, created.BlockEnd)
	}
	if created.GroupName != "News" {
		t.Fatalf("created.GroupName = %q, want News", created.GroupName)
	}
	if len(created.GroupNames) != 2 || created.GroupNames[0] != "News" || created.GroupNames[1] != "Sports" {
		t.Fatalf("created.GroupNames = %#v, want [News Sports]", created.GroupNames)
	}
	if created.SearchRegex {
		t.Fatalf("created.SearchRegex = true, want false")
	}

	waitForCondition(t, 2*time.Second, "dynamic block immediate sync", func() bool {
		var resp struct {
			Total int `json:"total"`
		}
		rec := doRaw(t, mux, http.MethodGet, fmt.Sprintf("/api/dynamic-channels/%d/channels", created.QueryID), nil)
		if rec.Code != http.StatusOK {
			return false
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return false
		}
		return resp.Total == 2
	})
	waitForCondition(t, 2*time.Second, "dynamic block lineup reload", func() bool {
		return fakeDVR.ReloadCallCount() >= 1
	})

	var traditional struct {
		Channels []channels.Channel `json:"channels"`
		Total    int                `json:"total"`
	}
	doJSON(t, mux, http.MethodGet, "/api/channels", nil, http.StatusOK, &traditional)
	if traditional.Total != 0 || len(traditional.Channels) != 0 {
		t.Fatalf("traditional channel list should exclude dynamic rows, got total=%d len=%d", traditional.Total, len(traditional.Channels))
	}

	var generated struct {
		Channels []channels.Channel `json:"channels"`
		Total    int                `json:"total"`
		Limit    int                `json:"limit"`
		Offset   int                `json:"offset"`
	}
	basePath := fmt.Sprintf("/api/dynamic-channels/%d/channels", created.QueryID)
	doJSON(t, mux, http.MethodGet, basePath, nil, http.StatusOK, &generated)
	if generated.Total != 2 || len(generated.Channels) != 2 {
		t.Fatalf("generated channels total=%d len=%d, want total=2 len=2", generated.Total, len(generated.Channels))
	}
	if generated.Limit != defaultChannelsListLimit || generated.Offset != 0 {
		t.Fatalf("generated default paging limit=%d offset=%d, want limit=%d offset=0", generated.Limit, generated.Offset, defaultChannelsListLimit)
	}

	var pagedGenerated struct {
		Channels []channels.Channel `json:"channels"`
		Total    int                `json:"total"`
		Limit    int                `json:"limit"`
		Offset   int                `json:"offset"`
	}
	doJSON(t, mux, http.MethodGet, basePath+"?limit=1&offset=1", nil, http.StatusOK, &pagedGenerated)
	if pagedGenerated.Total != 2 {
		t.Fatalf("generated paged total=%d, want 2", pagedGenerated.Total)
	}
	if pagedGenerated.Limit != 1 || pagedGenerated.Offset != 1 {
		t.Fatalf("generated paged metadata limit=%d offset=%d, want limit=1 offset=1", pagedGenerated.Limit, pagedGenerated.Offset)
	}
	if len(pagedGenerated.Channels) != 1 {
		t.Fatalf("len(generated paged channels) = %d, want 1", len(pagedGenerated.Channels))
	}

	var normalizedGenerated struct {
		Channels []channels.Channel `json:"channels"`
		Limit    int                `json:"limit"`
	}
	doJSON(t, mux, http.MethodGet, basePath+"?limit=0", nil, http.StatusOK, &normalizedGenerated)
	if normalizedGenerated.Limit != defaultChannelsListLimit {
		t.Fatalf("generated normalized limit = %d, want %d", normalizedGenerated.Limit, defaultChannelsListLimit)
	}
	if len(normalizedGenerated.Channels) != 2 {
		t.Fatalf("len(generated normalized channels) = %d, want 2", len(normalizedGenerated.Channels))
	}

	var cappedGenerated struct {
		Channels []channels.Channel `json:"channels"`
		Limit    int                `json:"limit"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		fmt.Sprintf("%s?limit=%d", basePath, maxChannelsListLimit+250),
		nil,
		http.StatusOK,
		&cappedGenerated,
	)
	if cappedGenerated.Limit != maxChannelsListLimit {
		t.Fatalf("generated capped limit = %d, want %d", cappedGenerated.Limit, maxChannelsListLimit)
	}
	if len(cappedGenerated.Channels) != 2 {
		t.Fatalf("len(generated capped channels) = %d, want 2", len(cappedGenerated.Channels))
	}

	for _, suffix := range []string{
		"?limit=-1",
		"?limit=abc",
		"?offset=-1",
		"?offset=nan",
	} {
		rec := doRaw(t, mux, http.MethodGet, basePath+suffix, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want %d", basePath+suffix, rec.Code, http.StatusBadRequest)
		}
	}

	reorderedIDs := []int64{generated.Channels[1].ChannelID, generated.Channels[0].ChannelID}
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/dynamic-channels/%d/channels/reorder", created.QueryID), map[string]any{
		"channel_ids": reorderedIDs,
	}, http.StatusNoContent, nil)
	waitForCondition(t, 2*time.Second, "dynamic generated reorder lineup reload", func() bool {
		return fakeDVR.ReloadCallCount() >= 2
	})

	doJSON(t, mux, http.MethodGet, basePath, nil, http.StatusOK, &generated)
	if generated.Channels[0].ChannelID != reorderedIDs[0] || generated.Channels[1].ChannelID != reorderedIDs[1] {
		t.Fatalf("generated channel reorder mismatch, got [%d,%d], want [%d,%d]", generated.Channels[0].ChannelID, generated.Channels[1].ChannelID, reorderedIDs[0], reorderedIDs[1])
	}
	if generated.Channels[0].GuideNumber != "10000" || generated.Channels[1].GuideNumber != "10001" {
		t.Fatalf("generated guide numbers after reorder = [%q,%q], want [10000,10001]", generated.Channels[0].GuideNumber, generated.Channels[1].GuideNumber)
	}

	noMatch := "no-such-news"
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/dynamic-channels/%d", created.QueryID), map[string]any{
		"search_query": noMatch,
	}, http.StatusOK, nil)

	waitForCondition(t, 2*time.Second, "dynamic block sync after query update", func() bool {
		var resp struct {
			Total int `json:"total"`
		}
		rec := doRaw(t, mux, http.MethodGet, basePath, nil)
		if rec.Code != http.StatusOK {
			return false
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			return false
		}
		return resp.Total == 0
	})

	doJSON(t, mux, http.MethodDelete, fmt.Sprintf("/api/dynamic-channels/%d", created.QueryID), nil, http.StatusNoContent, nil)
	rec := doRaw(t, mux, http.MethodGet, basePath, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET deleted dynamic query channels status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestAdminDVRLineupReloadQueueSemantics(t *testing.T) {
	newHarness := func(t *testing.T) (*AdminHandler, *fakeDVRService, func()) {
		t.Helper()

		store, err := sqlite.Open(":memory:")
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}

		channelsSvc := channels.NewService(store)
		handler, err := NewAdminHandler(store, channelsSvc)
		if err != nil {
			store.Close()
			t.Fatalf("NewAdminHandler() error = %v", err)
		}
		handler.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

		fakeDVR := &fakeDVRService{}
		handler.SetDVRService(fakeDVR)

		cleanup := func() {
			handler.Close()
			store.Close()
		}
		return handler, fakeDVR, cleanup
	}

	t.Run("BurstCoalescing", func(t *testing.T) {
		handler, fakeDVR, cleanup := newHarness(t)
		defer cleanup()

		handler.dvrLineupReloadDebounce = 35 * time.Millisecond
		handler.dvrLineupReloadMaxWait = 200 * time.Millisecond
		fakeDVR.reloadStartedCh = make(chan struct{}, 4)

		handler.enqueueDVRLineupReload("burst_a")
		handler.enqueueDVRLineupReload("burst_b")
		handler.enqueueDVRLineupReload("burst_c")

		select {
		case <-fakeDVR.reloadStartedCh:
		case <-time.After(time.Second):
			t.Fatal("coalesced reload did not start")
		}
		select {
		case <-fakeDVR.reloadStartedCh:
			t.Fatal("burst coalescing started an unexpected second reload")
		case <-time.After(4 * handler.dvrLineupReloadDebounce):
		}
		if got := fakeDVR.ReloadCallCount(); got != 1 {
			t.Fatalf("coalesced burst reload calls = %d, want 1", got)
		}
	})

	t.Run("RescheduleExtendsButCapsAtMaxWait", func(t *testing.T) {
		handler, fakeDVR, cleanup := newHarness(t)
		defer cleanup()

		handler.dvrLineupReloadDebounce = 90 * time.Millisecond
		handler.dvrLineupReloadMaxWait = 150 * time.Millisecond

		startedAt := time.Now()
		handler.enqueueDVRLineupReload("max_wait_1")
		time.Sleep(60 * time.Millisecond)
		handler.enqueueDVRLineupReload("max_wait_2")
		time.Sleep(60 * time.Millisecond)
		handler.enqueueDVRLineupReload("max_wait_3")

		waitForCondition(t, time.Second, "max-wait capped reload execution", func() bool {
			return fakeDVR.ReloadCallCount() >= 1
		})

		firstReloadAt := fakeDVR.FirstReloadStartedAt()
		if firstReloadAt.IsZero() {
			t.Fatal("first reload start time was not recorded")
		}
		elapsed := firstReloadAt.Sub(startedAt)
		if elapsed < 120*time.Millisecond {
			t.Fatalf("first reload elapsed = %s, want >= 120ms to confirm debounce extension", elapsed)
		}
		if elapsed > 260*time.Millisecond {
			t.Fatalf("first reload elapsed = %s, want <= 260ms with max-wait cap", elapsed)
		}
	})

	t.Run("EnqueueWhileRunningSchedulesSingleFollowUp", func(t *testing.T) {
		handler, fakeDVR, cleanup := newHarness(t)
		defer cleanup()

		handler.dvrLineupReloadDebounce = 20 * time.Millisecond
		handler.dvrLineupReloadMaxWait = 120 * time.Millisecond
		fakeDVR.reloadStartedCh = make(chan struct{}, 4)
		fakeDVR.reloadReleaseCh = make(chan struct{})

		handler.enqueueDVRLineupReload("running_first")
		select {
		case <-fakeDVR.reloadStartedCh:
		case <-time.After(time.Second):
			t.Fatal("first queued reload did not start")
		}

		handler.enqueueDVRLineupReload("running_followup_1")
		handler.enqueueDVRLineupReload("running_followup_2")
		close(fakeDVR.reloadReleaseCh)

		select {
		case <-fakeDVR.reloadStartedCh:
		case <-time.After(time.Second):
			t.Fatal("follow-up reload did not start after in-flight enqueue")
		}
		select {
		case <-fakeDVR.reloadStartedCh:
			t.Fatal("in-flight enqueue scheduled more than one follow-up reload")
		case <-time.After(handler.dvrLineupReloadDebounce + handler.dvrLineupReloadMaxWait):
		}
		if got := fakeDVR.ReloadCallCount(); got != 2 {
			t.Fatalf("reload calls after in-flight enqueue = %d, want 2", got)
		}
	})

	t.Run("CloseCancelsPendingAndRunning", func(t *testing.T) {
		t.Run("PendingTimer", func(t *testing.T) {
			handler, fakeDVR, cleanup := newHarness(t)
			defer cleanup()

			handler.dvrLineupReloadDebounce = 200 * time.Millisecond
			handler.dvrLineupReloadMaxWait = 400 * time.Millisecond
			fakeDVR.reloadStartedCh = make(chan struct{}, 1)

			handler.enqueueDVRLineupReload("close_pending")
			handler.Close()
			select {
			case <-fakeDVR.reloadStartedCh:
				t.Fatal("reload started after Close canceled pending timer")
			case <-time.After(handler.dvrLineupReloadDebounce + 150*time.Millisecond):
			}
			if got := fakeDVR.ReloadCallCount(); got != 0 {
				t.Fatalf("reload calls after close with pending timer = %d, want 0", got)
			}
		})

		t.Run("RunningReload", func(t *testing.T) {
			handler, fakeDVR, cleanup := newHarness(t)
			defer cleanup()

			handler.dvrLineupReloadDebounce = 10 * time.Millisecond
			handler.dvrLineupReloadMaxWait = 80 * time.Millisecond
			fakeDVR.reloadStartedCh = make(chan struct{}, 1)
			fakeDVR.reloadReleaseCh = make(chan struct{})

			handler.enqueueDVRLineupReload("close_running")
			select {
			case <-fakeDVR.reloadStartedCh:
			case <-time.After(time.Second):
				t.Fatal("running reload did not start before close")
			}

			closeDone := make(chan struct{})
			go func() {
				handler.Close()
				close(closeDone)
			}()

			select {
			case <-closeDone:
			case <-time.After(2 * time.Second):
				t.Fatal("handler.Close() did not return while reload was in-flight")
			}

			if got := fakeDVR.ReloadCallCount(); got != 1 {
				t.Fatalf("reload calls during close-running scenario = %d, want 1", got)
			}
			if err := fakeDVR.LastReloadContextErr(); err == nil {
				t.Fatal("LastReloadContextErr() = nil, want canceled/deadline error after close")
			}
		})
	})
}

func TestFormatDVRLineupReloadReasons(t *testing.T) {
	tests := []struct {
		name         string
		reasonCounts map[string]int
		want         string
	}{
		{
			name:         "EmptyMapUsesUnspecifiedFallback",
			reasonCounts: nil,
			want:         "unspecified(1)",
		},
		{
			name:         "InitializedEmptyMapUsesUnspecifiedFallback",
			reasonCounts: map[string]int{},
			want:         "unspecified(1)",
		},
		{
			name: "SingleReason",
			reasonCounts: map[string]int{
				"traditional_reorder": 2,
			},
			want: "traditional_reorder(2)",
		},
		{
			name: "MultipleReasonsSorted",
			reasonCounts: map[string]int{
				"zeta":  1,
				"alpha": 3,
				"beta":  2,
			},
			want: "alpha(3),beta(2),zeta(1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatDVRLineupReloadReasons(tt.reasonCounts); got != tt.want {
				t.Fatalf("formatDVRLineupReloadReasons() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAdminDVRLineupReloadRunUsesDedicatedTimeout(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	defer handler.Close()

	handler.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	handler.dynamicBlockSyncTimeout = 5 * time.Millisecond
	handler.SetDVRLineupReloadTimeout(150 * time.Millisecond)

	fakeDVR := &fakeDVRService{
		reloadDelay: 40 * time.Millisecond,
	}
	handler.SetDVRService(fakeDVR)

	handler.runDVRLineupReloadOnce(dvrLineupReloadBatch{
		reasonSummary:  "timeout_test",
		coalescedCount: 1,
	})

	if got := fakeDVR.ReloadCallCount(); got != 1 {
		t.Fatalf("ReloadCallCount() = %d, want 1", got)
	}
	if err := fakeDVR.LastReloadContextErr(); err != nil {
		t.Fatalf("LastReloadContextErr() = %v, want nil when dedicated lineup timeout exceeds reload delay", err)
	}
}

func TestAdminDVRLineupReloadRunTimeoutExpiryLogsFailure(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	defer handler.Close()

	var logBuffer bytes.Buffer
	handler.SetLogger(slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelDebug})))
	handler.SetDVRLineupReloadTimeout(20 * time.Millisecond)

	fakeDVR := &fakeDVRService{
		reloadDelay: 80 * time.Millisecond,
	}
	handler.SetDVRService(fakeDVR)

	handler.runDVRLineupReloadOnce(dvrLineupReloadBatch{
		reasonSummary:  "timeout_expiry",
		coalescedCount: 1,
	})

	if got := fakeDVR.ReloadCallCount(); got != 1 {
		t.Fatalf("ReloadCallCount() = %d, want 1", got)
	}
	if err := fakeDVR.LastReloadContextErr(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("LastReloadContextErr() = %v, want context deadline exceeded", err)
	}

	logs := logBuffer.String()
	if !strings.Contains(logs, "admin dvr lineup reload failed") {
		t.Fatalf("logs = %q, want admin dvr lineup reload failed entry", logs)
	}
	if strings.Contains(logs, "admin dvr lineup reload canceled") {
		t.Fatalf("logs = %q, did not expect admin dvr lineup reload canceled entry", logs)
	}
	if !strings.Contains(logs, "context deadline exceeded") {
		t.Fatalf("logs = %q, want context deadline exceeded detail", logs)
	}
}

func TestAdminRoutesDynamicChannelQueryValidation(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:optional:one",
			ChannelKey: "name:news optional one",
			Name:       "News Optional One",
			Group:      "News",
			StreamURL:  "http://example.com/news-optional-one.ts",
		},
		{
			ItemKey:    "src:news:optional:two",
			ChannelKey: "name:news optional two",
			Name:       "News Optional Two",
			Group:      "News",
			StreamURL:  "http://example.com/news-optional-two.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created struct {
		QueryID     int64  `json:"query_id"`
		SearchQuery string `json:"search_query"`
		SearchRegex bool   `json:"search_regex"`
	}
	doJSON(t, mux, http.MethodPost, "/api/dynamic-channels", map[string]any{
		"name":         "Invalid",
		"group_name":   "News",
		"search_query": "   ",
	}, http.StatusOK, &created)
	if created.QueryID <= 0 {
		t.Fatalf("created.QueryID = %d, want > 0", created.QueryID)
	}
	if created.SearchQuery != "" {
		t.Fatalf("created.SearchQuery = %q, want empty string", created.SearchQuery)
	}
	if created.SearchRegex {
		t.Fatalf("created.SearchRegex = true, want false")
	}

	invalidRegexCreate := doRaw(t, mux, http.MethodPost, "/api/dynamic-channels", map[string]any{
		"name":         "Invalid Regex",
		"group_name":   "News",
		"search_query": "([",
		"search_regex": true,
	})
	if invalidRegexCreate.Code != http.StatusBadRequest {
		t.Fatalf("POST /api/dynamic-channels invalid regex status = %d, want %d", invalidRegexCreate.Code, http.StatusBadRequest)
	}

	waitForCondition(t, 2*time.Second, "dynamic block immediate sync with empty search query", func() bool {
		var generated struct {
			Total int `json:"total"`
		}
		rec := doRaw(t, mux, http.MethodGet, fmt.Sprintf("/api/dynamic-channels/%d/channels", created.QueryID), nil)
		if rec.Code != http.StatusOK {
			return false
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &generated); err != nil {
			return false
		}
		return generated.Total == 2
	})
}

func TestAdminRoutesDynamicChannelQueryWarningForTruncatedSearch(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.OpenWithOptions(":memory:", sqlite.SQLiteOptions{
		CatalogSearchLimits: sqlite.CatalogSearchLimits{
			MaxTerms: 1,
		},
	})
	if err != nil {
		t.Fatalf("OpenWithOptions() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:dyn:one",
			ChannelKey: "name:dyn one",
			Name:       "Dynamic One",
			Group:      "News",
			StreamURL:  "http://example.com/dyn-one.ts",
		},
		{
			ItemKey:    "src:dyn:two",
			ChannelKey: "name:dyn two",
			Name:       "Dynamic Two",
			Group:      "News",
			StreamURL:  "http://example.com/dyn-two.ts",
		},
		{
			ItemKey:    "src:dyn:three",
			ChannelKey: "name:dyn three",
			Name:       "Dynamic Three",
			Group:      "News",
			StreamURL:  "http://example.com/dyn-three.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created dynamicChannelQueryResponse
	doJSON(t, mux, http.MethodPost, "/api/dynamic-channels", map[string]any{
		"name":         "Long Query",
		"group_name":   "News",
		"search_query": "dynamic query truncation test",
	}, http.StatusOK, &created)
	if created.SearchWarning == nil {
		t.Fatal("create dynamic channel query search_warning is nil")
	}
	if !created.SearchWarning.Truncated {
		t.Fatalf("create dynamic channel query search_warning.truncated = %v, want true", created.SearchWarning.Truncated)
	}
	if created.SearchWarning.TermsApplied != 1 || created.SearchWarning.TermsDropped != 3 {
		t.Fatalf(
			"create dynamic channel query terms applied/dropped = %d/%d, want 1/3",
			created.SearchWarning.TermsApplied,
			created.SearchWarning.TermsDropped,
		)
	}

	var detail dynamicChannelQueryResponse
	doJSON(
		t,
		mux,
		http.MethodGet,
		fmt.Sprintf("/api/dynamic-channels/%d", created.QueryID),
		nil,
		http.StatusOK,
		&detail,
	)
	if detail.SearchWarning == nil {
		t.Fatal("detail dynamic channel query search_warning is nil")
	}
	if !detail.SearchWarning.Truncated {
		t.Fatalf("detail dynamic channel query search_warning.truncated = %v, want true", detail.SearchWarning.Truncated)
	}

	var listed struct {
		Queries []dynamicChannelQueryResponse `json:"queries"`
		Total   int                           `json:"total"`
	}
	doJSON(t, mux, http.MethodGet, "/api/dynamic-channels", nil, http.StatusOK, &listed)
	if listed.Total != 1 {
		t.Fatalf("listed total = %d, want 1", listed.Total)
	}
	if len(listed.Queries) != 1 {
		t.Fatalf("len(listed.queries) = %d, want 1", len(listed.Queries))
	}
	if listed.Queries[0].SearchWarning == nil {
		t.Fatal("list dynamic channel query search_warning is nil")
	}
	if !listed.Queries[0].SearchWarning.Truncated {
		t.Fatalf("list dynamic channel query search_warning.truncated = %v, want true", listed.Queries[0].SearchWarning.Truncated)
	}

	var updated dynamicChannelQueryResponse
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/dynamic-channels/%d", created.QueryID), map[string]any{
		"name":         "Long Query Updated",
		"search_query": "more fields in truncated query",
	}, http.StatusOK, &updated)
	if updated.SearchWarning == nil {
		t.Fatal("update dynamic channel query search_warning is nil")
	}
	if !updated.SearchWarning.Truncated {
		t.Fatalf("update dynamic channel query search_warning.truncated = %v, want true", updated.SearchWarning.Truncated)
	}
	if updated.SearchWarning.TermsApplied != 1 || updated.SearchWarning.TermsDropped != 4 {
		t.Fatalf(
			"update dynamic channel query terms applied/dropped = %d/%d, want 1/4",
			updated.SearchWarning.TermsApplied,
			updated.SearchWarning.TermsDropped,
		)
	}
}

func TestAdminRoutesDynamicChannelQuerySearchRegexRoundTripAndOmittedUpdateCompatibility(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:regex:one",
			ChannelKey: "name:news regex one",
			Name:       "News Regex One",
			Group:      "News",
			StreamURL:  "http://example.com/news-regex-one.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created struct {
		QueryID     int64  `json:"query_id"`
		SearchQuery string `json:"search_query"`
		SearchRegex bool   `json:"search_regex"`
	}
	doJSON(t, mux, http.MethodPost, "/api/dynamic-channels", map[string]any{
		"name":         "Regex Block",
		"group_name":   "News",
		"search_query": "news",
		"search_regex": true,
	}, http.StatusOK, &created)
	if created.QueryID <= 0 {
		t.Fatalf("created.QueryID = %d, want > 0", created.QueryID)
	}
	if !created.SearchRegex {
		t.Fatal("created.SearchRegex = false, want true")
	}

	var updatedOmitted struct {
		SearchQuery string `json:"search_query"`
		SearchRegex bool   `json:"search_regex"`
	}
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/dynamic-channels/%d", created.QueryID), map[string]any{
		"search_query": "news updated",
	}, http.StatusOK, &updatedOmitted)
	if updatedOmitted.SearchQuery != "news updated" {
		t.Fatalf("updatedOmitted.SearchQuery = %q, want %q", updatedOmitted.SearchQuery, "news updated")
	}
	if !updatedOmitted.SearchRegex {
		t.Fatal("updatedOmitted.SearchRegex = false, want true when field is omitted")
	}

	var updatedExplicit struct {
		SearchRegex bool `json:"search_regex"`
	}
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/dynamic-channels/%d", created.QueryID), map[string]any{
		"search_regex": false,
	}, http.StatusOK, &updatedExplicit)
	if updatedExplicit.SearchRegex {
		t.Fatal("updatedExplicit.SearchRegex = true, want false")
	}
}

func TestAdminRoutesDynamicChannelQueriesPaginationAndDetailValidation(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	createdIDs := make([]int64, 0, 6)
	for i := 0; i < 6; i++ {
		created, createErr := channelsSvc.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
			Name:        fmt.Sprintf("Query %d", i),
			GroupName:   "News",
			SearchQuery: fmt.Sprintf("term-%d", i),
		})
		if createErr != nil {
			t.Fatalf("CreateDynamicChannelQuery(%d) error = %v", i, createErr)
		}
		createdIDs = append(createdIDs, created.QueryID)
	}

	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var defaultPage struct {
		Queries []dynamicChannelQueryResponse `json:"queries"`
		Total   int                           `json:"total"`
		Limit   int                           `json:"limit"`
		Offset  int                           `json:"offset"`
	}
	doJSON(t, mux, http.MethodGet, "/api/dynamic-channels", nil, http.StatusOK, &defaultPage)
	if defaultPage.Total != 6 || len(defaultPage.Queries) != 6 {
		t.Fatalf("default list total=%d len=%d, want total=6 len=6", defaultPage.Total, len(defaultPage.Queries))
	}
	if defaultPage.Limit != defaultDynamicChannelQueriesListLimit || defaultPage.Offset != 0 {
		t.Fatalf(
			"default list pagination limit=%d offset=%d, want limit=%d offset=0",
			defaultPage.Limit,
			defaultPage.Offset,
			defaultDynamicChannelQueriesListLimit,
		)
	}

	var paged struct {
		Queries []dynamicChannelQueryResponse `json:"queries"`
		Total   int                           `json:"total"`
		Limit   int                           `json:"limit"`
		Offset  int                           `json:"offset"`
	}
	doJSON(t, mux, http.MethodGet, "/api/dynamic-channels?limit=2&offset=1", nil, http.StatusOK, &paged)
	if paged.Total != 6 || paged.Limit != 2 || paged.Offset != 1 {
		t.Fatalf("paged metadata total=%d limit=%d offset=%d, want total=6 limit=2 offset=1", paged.Total, paged.Limit, paged.Offset)
	}
	if len(paged.Queries) != 2 {
		t.Fatalf("len(paged queries) = %d, want 2", len(paged.Queries))
	}
	if paged.Queries[0].QueryID != createdIDs[1] || paged.Queries[1].QueryID != createdIDs[2] {
		t.Fatalf(
			"paged query IDs = [%d,%d], want [%d,%d]",
			paged.Queries[0].QueryID,
			paged.Queries[1].QueryID,
			createdIDs[1],
			createdIDs[2],
		)
	}

	var normalized struct {
		Queries []dynamicChannelQueryResponse `json:"queries"`
		Limit   int                           `json:"limit"`
	}
	doJSON(t, mux, http.MethodGet, "/api/dynamic-channels?limit=0", nil, http.StatusOK, &normalized)
	if normalized.Limit != defaultDynamicChannelQueriesListLimit {
		t.Fatalf("normalized limit = %d, want %d", normalized.Limit, defaultDynamicChannelQueriesListLimit)
	}
	if len(normalized.Queries) != 6 {
		t.Fatalf("len(normalized queries) = %d, want 6", len(normalized.Queries))
	}

	var capped struct {
		Queries []dynamicChannelQueryResponse `json:"queries"`
		Limit   int                           `json:"limit"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		fmt.Sprintf("/api/dynamic-channels?limit=%d", maxDynamicChannelQueriesListLimit+250),
		nil,
		http.StatusOK,
		&capped,
	)
	if capped.Limit != maxDynamicChannelQueriesListLimit {
		t.Fatalf("capped limit = %d, want %d", capped.Limit, maxDynamicChannelQueriesListLimit)
	}
	if len(capped.Queries) != 6 {
		t.Fatalf("len(capped queries) = %d, want 6", len(capped.Queries))
	}

	for _, path := range []string{
		"/api/dynamic-channels?limit=-1",
		"/api/dynamic-channels?limit=abc",
		"/api/dynamic-channels?offset=-1",
		"/api/dynamic-channels?offset=nan",
	} {
		rec := doRaw(t, mux, http.MethodGet, path, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want %d", path, rec.Code, http.StatusBadRequest)
		}
	}

	var detail dynamicChannelQueryResponse
	doJSON(
		t,
		mux,
		http.MethodGet,
		fmt.Sprintf("/api/dynamic-channels/%d", createdIDs[4]),
		nil,
		http.StatusOK,
		&detail,
	)
	if detail.QueryID != createdIDs[4] {
		t.Fatalf("detail query_id = %d, want %d", detail.QueryID, createdIDs[4])
	}
	if detail.Name != "Query 4" {
		t.Fatalf("detail name = %q, want Query 4", detail.Name)
	}

	rec := doRaw(t, mux, http.MethodGet, "/api/dynamic-channels/999999", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /api/dynamic-channels/{queryID} for missing row status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	rec = doRaw(t, mux, http.MethodGet, "/api/dynamic-channels/not-a-number", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("GET /api/dynamic-channels/{queryID} for invalid id status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestAdminRoutesDynamicChannelQueriesDefaultPaginationBoundsHighCardinality(t *testing.T) {
	const queryCount = 560

	ctx := context.Background()
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	createdIDs := make([]int64, 0, queryCount)
	for i := 0; i < queryCount; i++ {
		created, createErr := channelsSvc.CreateDynamicChannelQuery(ctx, channels.DynamicChannelQueryCreate{
			Name:        fmt.Sprintf("High Cardinality Query %03d", i),
			GroupName:   "Load",
			SearchQuery: fmt.Sprintf("load-%03d", i),
		})
		if createErr != nil {
			t.Fatalf("CreateDynamicChannelQuery(%d) error = %v", i, createErr)
		}
		createdIDs = append(createdIDs, created.QueryID)
	}

	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var firstPage struct {
		Queries []dynamicChannelQueryResponse `json:"queries"`
		Total   int                           `json:"total"`
		Limit   int                           `json:"limit"`
		Offset  int                           `json:"offset"`
	}
	doJSON(t, mux, http.MethodGet, "/api/dynamic-channels", nil, http.StatusOK, &firstPage)
	if firstPage.Total != queryCount {
		t.Fatalf("first page total = %d, want %d", firstPage.Total, queryCount)
	}
	if firstPage.Limit != defaultDynamicChannelQueriesListLimit || firstPage.Offset != 0 {
		t.Fatalf(
			"first page pagination limit=%d offset=%d, want limit=%d offset=0",
			firstPage.Limit,
			firstPage.Offset,
			defaultDynamicChannelQueriesListLimit,
		)
	}
	if len(firstPage.Queries) != defaultDynamicChannelQueriesListLimit {
		t.Fatalf("first page len = %d, want %d", len(firstPage.Queries), defaultDynamicChannelQueriesListLimit)
	}
	if firstPage.Queries[0].QueryID != createdIDs[0] {
		t.Fatalf("first page first query_id = %d, want %d", firstPage.Queries[0].QueryID, createdIDs[0])
	}

	var secondPage struct {
		Queries []dynamicChannelQueryResponse `json:"queries"`
		Total   int                           `json:"total"`
		Limit   int                           `json:"limit"`
		Offset  int                           `json:"offset"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		fmt.Sprintf("/api/dynamic-channels?limit=%d&offset=%d", defaultDynamicChannelQueriesListLimit, defaultDynamicChannelQueriesListLimit),
		nil,
		http.StatusOK,
		&secondPage,
	)
	if secondPage.Total != queryCount {
		t.Fatalf("second page total = %d, want %d", secondPage.Total, queryCount)
	}
	if secondPage.Offset != defaultDynamicChannelQueriesListLimit || secondPage.Limit != defaultDynamicChannelQueriesListLimit {
		t.Fatalf(
			"second page pagination limit=%d offset=%d, want limit=%d offset=%d",
			secondPage.Limit,
			secondPage.Offset,
			defaultDynamicChannelQueriesListLimit,
			defaultDynamicChannelQueriesListLimit,
		)
	}
	if len(secondPage.Queries) != defaultDynamicChannelQueriesListLimit {
		t.Fatalf("second page len = %d, want %d", len(secondPage.Queries), defaultDynamicChannelQueriesListLimit)
	}

	listRec := doRaw(t, mux, http.MethodGet, "/api/dynamic-channels", nil)
	if listRec.Code != http.StatusOK {
		t.Fatalf("GET /api/dynamic-channels status = %d, want %d", listRec.Code, http.StatusOK)
	}
	listBytes := listRec.Body.Len()
	if listBytes <= 1500 {
		t.Fatalf("dynamic query list payload bytes = %d, want > 1500 for high-cardinality guardrail", listBytes)
	}

	startedAt := time.Now()
	detailRec := doRaw(t, mux, http.MethodGet, fmt.Sprintf("/api/dynamic-channels/%d", createdIDs[len(createdIDs)-1]), nil)
	detailLatency := time.Since(startedAt)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("GET /api/dynamic-channels/{queryID} status = %d, want %d", detailRec.Code, http.StatusOK)
	}
	detailBytes := detailRec.Body.Len()
	if detailBytes <= 0 {
		t.Fatalf("dynamic query detail payload bytes = %d, want > 0", detailBytes)
	}
	if detailBytes >= listBytes {
		t.Fatalf("dynamic query detail payload bytes = %d, want < list payload bytes %d", detailBytes, listBytes)
	}

	t.Logf(
		"high-cardinality dynamic query metadata evidence: total=%d list_returned=%d list_bytes=%d detail_bytes=%d detail_latency_ms=%d",
		queryCount,
		len(firstPage.Queries),
		listBytes,
		detailBytes,
		detailLatency.Milliseconds(),
	)
}

func TestAdminRoutesSourcesPaginationAndValidation(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	items := []playlist.Item{
		{
			ItemKey:    "src:sources:1",
			ChannelKey: "tvg:sources",
			Name:       "Sources 1",
			Group:      "Sources",
			StreamURL:  "http://example.com/sources-1.ts",
		},
		{
			ItemKey:    "src:sources:2",
			ChannelKey: "tvg:sources",
			Name:       "Sources 2",
			Group:      "Sources",
			StreamURL:  "http://example.com/sources-2.ts",
		},
		{
			ItemKey:    "src:sources:3",
			ChannelKey: "tvg:sources",
			Name:       "Sources 3",
			Group:      "Sources",
			StreamURL:  "http://example.com/sources-3.ts",
		},
		{
			ItemKey:    "src:sources:4",
			ChannelKey: "tvg:sources",
			Name:       "Sources 4",
			Group:      "Sources",
			StreamURL:  "http://example.com/sources-4.ts",
		},
		{
			ItemKey:    "src:sources:5",
			ChannelKey: "tvg:sources",
			Name:       "Sources 5",
			Group:      "Sources",
			StreamURL:  "http://example.com/sources-5.ts",
		},
	}
	if err := store.UpsertPlaylistItems(ctx, items); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": items[0].ItemKey,
	}, http.StatusOK, &created)
	for _, item := range items[1:] {
		doJSON(t, mux, http.MethodPost, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), map[string]any{
			"item_key": item.ItemKey,
		}, http.StatusOK, nil)
	}

	basePath := fmt.Sprintf("/api/channels/%d/sources", created.ChannelID)
	var paged struct {
		Sources []channels.Source `json:"sources"`
		Total   int               `json:"total"`
		Limit   int               `json:"limit"`
		Offset  int               `json:"offset"`
	}
	doJSON(t, mux, http.MethodGet, basePath+"?limit=2&offset=1", nil, http.StatusOK, &paged)
	if paged.Total != len(items) {
		t.Fatalf("paged total = %d, want %d", paged.Total, len(items))
	}
	if paged.Limit != 2 {
		t.Fatalf("paged limit = %d, want 2", paged.Limit)
	}
	if paged.Offset != 1 {
		t.Fatalf("paged offset = %d, want 1", paged.Offset)
	}
	if len(paged.Sources) != 2 {
		t.Fatalf("len(paged sources) = %d, want 2", len(paged.Sources))
	}
	if paged.Sources[0].PriorityIndex != 1 || paged.Sources[1].PriorityIndex != 2 {
		t.Fatalf("paged priority indices = [%d,%d], want [1,2]", paged.Sources[0].PriorityIndex, paged.Sources[1].PriorityIndex)
	}

	var defaults struct {
		Sources []channels.Source `json:"sources"`
		Total   int               `json:"total"`
		Limit   int               `json:"limit"`
		Offset  int               `json:"offset"`
	}
	doJSON(t, mux, http.MethodGet, basePath, nil, http.StatusOK, &defaults)
	if defaults.Limit != defaultChannelSourcesListLimit {
		t.Fatalf("default limit = %d, want %d", defaults.Limit, defaultChannelSourcesListLimit)
	}
	if defaults.Offset != 0 {
		t.Fatalf("default offset = %d, want 0", defaults.Offset)
	}
	if defaults.Total != len(items) {
		t.Fatalf("default total = %d, want %d", defaults.Total, len(items))
	}
	if len(defaults.Sources) != len(items) {
		t.Fatalf("len(default sources) = %d, want %d", len(defaults.Sources), len(items))
	}

	var capped struct {
		Sources []channels.Source `json:"sources"`
		Limit   int               `json:"limit"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		fmt.Sprintf("%s?limit=%d", basePath, maxChannelSourcesListLimit+250),
		nil,
		http.StatusOK,
		&capped,
	)
	if capped.Limit != maxChannelSourcesListLimit {
		t.Fatalf("capped limit = %d, want %d", capped.Limit, maxChannelSourcesListLimit)
	}
	if len(capped.Sources) != len(items) {
		t.Fatalf("len(capped sources) = %d, want %d", len(capped.Sources), len(items))
	}

	var normalized struct {
		Sources []channels.Source `json:"sources"`
		Limit   int               `json:"limit"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		basePath+"?limit=0",
		nil,
		http.StatusOK,
		&normalized,
	)
	if normalized.Limit != defaultChannelSourcesListLimit {
		t.Fatalf("normalized limit = %d, want %d", normalized.Limit, defaultChannelSourcesListLimit)
	}
	if len(normalized.Sources) != len(items) {
		t.Fatalf("len(normalized sources) = %d, want %d", len(normalized.Sources), len(items))
	}

	for _, suffix := range []string{
		"?limit=-1",
		"?limit=abc",
		"?offset=-1",
		"?offset=nan",
	} {
		rec := doRaw(t, mux, http.MethodGet, basePath+suffix, nil)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want %d", basePath+suffix, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestAdminRoutesDynamicRuleCreateAndUpdateValidation(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX KMSP Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
		{
			ItemKey:    "src:sports:primary",
			ChannelKey: "tvg:sports",
			Name:       "Sports Primary",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:news:primary",
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "  US News  ",
			"search_query": "  fox kmsp  ",
		},
	}, http.StatusOK, &created)
	if !created.DynamicRule.Enabled {
		t.Fatal("created.DynamicRule.Enabled = false, want true")
	}
	if created.DynamicRule.GroupName != "US News" {
		t.Fatalf("created.DynamicRule.GroupName = %q, want US News", created.DynamicRule.GroupName)
	}
	if len(created.DynamicRule.GroupNames) != 1 || created.DynamicRule.GroupNames[0] != "US News" {
		t.Fatalf("created.DynamicRule.GroupNames = %#v, want [US News]", created.DynamicRule.GroupNames)
	}
	if created.DynamicRule.SearchQuery != "fox kmsp" {
		t.Fatalf("created.DynamicRule.SearchQuery = %q, want fox kmsp", created.DynamicRule.SearchQuery)
	}
	if created.DynamicRule.SearchRegex {
		t.Fatal("created.DynamicRule.SearchRegex = true, want false by default")
	}
	var createdSources struct {
		Sources []channels.Source `json:"sources"`
	}
	waitForCondition(t, 2*time.Second, "dynamic channel source synchronization", func() bool {
		doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), nil, http.StatusOK, &createdSources)
		return len(createdSources.Sources) == 2
	})
	createdByItem := make(map[string]channels.Source, len(createdSources.Sources))
	for _, src := range createdSources.Sources {
		createdByItem[src.ItemKey] = src
	}
	if got := createdByItem["src:news:primary"].AssociationType; got != "dynamic_query" {
		t.Fatalf("src:news:primary association_type = %q, want dynamic_query", got)
	}
	if got := createdByItem["src:news:backup"].AssociationType; got != "dynamic_query" {
		t.Fatalf("src:news:backup association_type = %q, want dynamic_query", got)
	}

	var createdNoSeed channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"guide_name": "US News Dynamic",
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "",
			"search_query": "fox",
		},
	}, http.StatusOK, &createdNoSeed)
	if !createdNoSeed.DynamicRule.Enabled {
		t.Fatal("createdNoSeed.DynamicRule.Enabled = false, want true")
	}
	if createdNoSeed.DynamicRule.GroupName != "" {
		t.Fatalf("createdNoSeed.DynamicRule.GroupName = %q, want empty", createdNoSeed.DynamicRule.GroupName)
	}
	if len(createdNoSeed.DynamicRule.GroupNames) != 0 {
		t.Fatalf("createdNoSeed.DynamicRule.GroupNames = %#v, want empty", createdNoSeed.DynamicRule.GroupNames)
	}
	if createdNoSeed.DynamicRule.SearchQuery != "fox" {
		t.Fatalf("createdNoSeed.DynamicRule.SearchQuery = %q, want fox", createdNoSeed.DynamicRule.SearchQuery)
	}
	if createdNoSeed.DynamicRule.SearchRegex {
		t.Fatal("createdNoSeed.DynamicRule.SearchRegex = true, want false by default")
	}
	waitForCondition(t, 2*time.Second, "dynamic channel source synchronization without seed item", func() bool {
		doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", createdNoSeed.ChannelID), nil, http.StatusOK, &createdSources)
		return len(createdSources.Sources) == 2
	})
	createdNoSeedByItem := make(map[string]channels.Source, len(createdSources.Sources))
	for _, src := range createdSources.Sources {
		createdNoSeedByItem[src.ItemKey] = src
	}
	if got := createdNoSeedByItem["src:news:primary"].AssociationType; got != "dynamic_query" {
		t.Fatalf("src:news:primary association_type without seed item = %q, want dynamic_query", got)
	}
	if got := createdNoSeedByItem["src:news:backup"].AssociationType; got != "dynamic_query" {
		t.Fatalf("src:news:backup association_type without seed item = %q, want dynamic_query", got)
	}

	badCreate := doRaw(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"guide_name": "Missing Source",
	})
	if badCreate.Code != http.StatusBadRequest {
		t.Fatalf("POST /api/channels without item_key or enabled dynamic_rule status = %d, want %d", badCreate.Code, http.StatusBadRequest)
	}

	var updated channels.Channel
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"dynamic_rule": map[string]any{
			"enabled":      false,
			"group_name":   "US News",
			"search_query": "fox",
		},
	}, http.StatusOK, &updated)
	if updated.DynamicRule.Enabled {
		t.Fatal("updated.DynamicRule.Enabled = true, want false")
	}
	if updated.DynamicRule.GroupName != "US News" || updated.DynamicRule.SearchQuery != "fox" {
		t.Fatalf("updated dynamic_rule = %+v, want group=US News query=fox", updated.DynamicRule)
	}
	if len(updated.DynamicRule.GroupNames) != 1 || updated.DynamicRule.GroupNames[0] != "US News" {
		t.Fatalf("updated.DynamicRule.GroupNames = %#v, want [US News]", updated.DynamicRule.GroupNames)
	}
	if updated.DynamicRule.SearchRegex {
		t.Fatal("updated.DynamicRule.SearchRegex = true, want false when omitted")
	}

	var updatedRegex channels.Channel
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "US News",
			"search_query": "fox",
			"search_regex": true,
		},
	}, http.StatusOK, &updatedRegex)
	if !updatedRegex.DynamicRule.SearchRegex {
		t.Fatal("updatedRegex.DynamicRule.SearchRegex = false, want true")
	}

	invalidRegexPatch := doRaw(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "US News",
			"search_query": "([",
			"search_regex": true,
		},
	})
	if invalidRegexPatch.Code != http.StatusBadRequest {
		t.Fatalf("PATCH /api/channels/{id} invalid dynamic regex status = %d, want %d", invalidRegexPatch.Code, http.StatusBadRequest)
	}

	badPatch := doRaw(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "US News",
			"search_query": "",
		},
	})
	if badPatch.Code != http.StatusBadRequest {
		t.Fatalf("PATCH /api/channels/{id} invalid dynamic_rule status = %d, want %d", badPatch.Code, http.StatusBadRequest)
	}
}

func TestAdminRoutesDynamicRuleCreateAndUpdateWarningForTruncatedQuery(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.OpenWithOptions(":memory:", sqlite.SQLiteOptions{
		CatalogSearchLimits: sqlite.CatalogSearchLimits{
			MaxTerms: 1,
		},
	})
	if err != nil {
		t.Fatalf("OpenWithOptions() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:sports:primary",
			ChannelKey: "tvg:sports",
			Name:       "Sports Primary",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	type channelWithWarning struct {
		channels.Channel
		SearchWarning *sqlite.CatalogSearchWarning `json:"search_warning,omitempty"`
	}

	var created channelWithWarning
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:news:primary",
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "US News",
			"search_query": "news local fox",
		},
	}, http.StatusOK, &created)
	if created.SearchWarning == nil {
		t.Fatal("create channel search_warning is nil")
	}
	if !created.SearchWarning.Truncated {
		t.Fatalf("create channel search_warning.truncated = %v, want true", created.SearchWarning.Truncated)
	}
	if created.SearchWarning.TermsApplied != 1 || created.SearchWarning.TermsDropped != 2 {
		t.Fatalf(
			"create channel search_warning terms applied/dropped = %d/%d, want 1/2",
			created.SearchWarning.TermsApplied,
			created.SearchWarning.TermsDropped,
		)
	}

	var updated channelWithWarning
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "US News",
			"search_query": "sports live event",
		},
	}, http.StatusOK, &updated)
	if updated.SearchWarning == nil {
		t.Fatal("update channel search_warning is nil")
	}
	if !updated.SearchWarning.Truncated {
		t.Fatalf("update channel search_warning.truncated = %v, want true", updated.SearchWarning.Truncated)
	}
	if updated.SearchWarning.TermsApplied != 1 || updated.SearchWarning.TermsDropped != 2 {
		t.Fatalf(
			"update channel search_warning terms applied/dropped = %d/%d, want 1/2",
			updated.SearchWarning.TermsApplied,
			updated.SearchWarning.TermsDropped,
		)
	}
}

func TestAdminRoutesChannelDynamicRuleSourceScopeUnsupportedReturnsContractError(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:scope:primary",
			ChannelKey: "tvg:scope",
			Name:       "Scope Primary",
			Group:      "Scope",
			StreamURL:  "http://example.com/scope-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.catalog = catalogWithoutSourceScopedSupport{CatalogStore: handler.catalog}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	type unsupportedResponse struct {
		Error     string `json:"error"`
		Message   string `json:"message"`
		Operation string `json:"operation"`
		Parameter string `json:"parameter"`
		Detail    string `json:"detail"`
	}

	var createUnsupported unsupportedResponse
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:scope:primary",
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"search_query": "scope",
			"source_ids":   []int64{1},
		},
	}, http.StatusNotImplemented, &createUnsupported)
	if createUnsupported.Operation != "channel_dynamic_rule_create" {
		t.Fatalf("create operation = %q, want channel_dynamic_rule_create", createUnsupported.Operation)
	}
	if createUnsupported.Error != "source_scoped_operation_unsupported" {
		t.Fatalf("create error = %q, want source_scoped_operation_unsupported", createUnsupported.Error)
	}
	if createUnsupported.Parameter != "source_ids" {
		t.Fatalf("create parameter = %q, want source_ids", createUnsupported.Parameter)
	}

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:scope:primary",
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"search_query": "scope",
		},
	}, http.StatusOK, &created)

	var updateUnsupported unsupportedResponse
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"search_query": "scope",
			"source_ids":   []int64{1},
		},
	}, http.StatusNotImplemented, &updateUnsupported)
	if updateUnsupported.Operation != "channel_dynamic_rule_update" {
		t.Fatalf("update operation = %q, want channel_dynamic_rule_update", updateUnsupported.Operation)
	}
	if updateUnsupported.Error != "source_scoped_operation_unsupported" {
		t.Fatalf("update error = %q, want source_scoped_operation_unsupported", updateUnsupported.Error)
	}
}

func TestAdminRoutesDynamicChannelQuerySourceScopeUnsupportedReturnsContractError(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:scope:primary",
			ChannelKey: "tvg:scope",
			Name:       "Scope Primary",
			Group:      "Scope",
			StreamURL:  "http://example.com/scope-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	baseChannels := channels.NewService(store)
	unsupportedChannels := dynamicBlockCapabilityUnsupportedChannelsService{ChannelsService: baseChannels}

	handler, err := NewAdminHandler(store, unsupportedChannels)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	type unsupportedResponse struct {
		Error     string `json:"error"`
		Operation string `json:"operation"`
		Parameter string `json:"parameter"`
	}

	var createUnsupported unsupportedResponse
	doJSON(t, mux, http.MethodPost, "/api/dynamic-channels", map[string]any{
		"name":         "Scoped Query",
		"enabled":      true,
		"search_query": "scope",
		"source_ids":   []int64{1},
	}, http.StatusNotImplemented, &createUnsupported)
	if createUnsupported.Operation != "dynamic_channel_query_create" {
		t.Fatalf("create operation = %q, want dynamic_channel_query_create", createUnsupported.Operation)
	}
	if createUnsupported.Error != "source_scoped_operation_unsupported" {
		t.Fatalf("create error = %q, want source_scoped_operation_unsupported", createUnsupported.Error)
	}
	if createUnsupported.Parameter != "source_ids" {
		t.Fatalf("create parameter = %q, want source_ids", createUnsupported.Parameter)
	}

	var created struct {
		QueryID int64 `json:"query_id"`
	}
	doJSON(t, mux, http.MethodPost, "/api/dynamic-channels", map[string]any{
		"name":         "Unscoped Query",
		"enabled":      true,
		"search_query": "scope",
	}, http.StatusOK, &created)
	if created.QueryID <= 0 {
		t.Fatalf("created query_id = %d, want > 0", created.QueryID)
	}

	var updateUnsupported unsupportedResponse
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/dynamic-channels/%d", created.QueryID), map[string]any{
		"source_ids": []int64{1},
	}, http.StatusNotImplemented, &updateUnsupported)
	if updateUnsupported.Operation != "dynamic_channel_query_update" {
		t.Fatalf("update operation = %q, want dynamic_channel_query_update", updateUnsupported.Operation)
	}
	if updateUnsupported.Error != "source_scoped_operation_unsupported" {
		t.Fatalf("update error = %q, want source_scoped_operation_unsupported", updateUnsupported.Error)
	}
	if updateUnsupported.Parameter != "source_ids" {
		t.Fatalf("update parameter = %q, want source_ids", updateUnsupported.Parameter)
	}
}

func TestAdminRoutesDynamicChannelQuerySourceScopeUsesBlockCapability(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:scope:primary",
			ChannelKey: "tvg:scope",
			Name:       "Scope Primary",
			Group:      "Scope",
			StreamURL:  "http://example.com/scope-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	observedChannels := &syncObservedChannelsService{
		base: channels.NewService(store),
	}

	handler, err := NewAdminHandler(store, observedChannels)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created struct {
		QueryID    int64   `json:"query_id"`
		SourceIDs  []int64 `json:"source_ids"`
		SearchTerm string  `json:"search_query"`
	}
	doJSON(t, mux, http.MethodPost, "/api/dynamic-channels", map[string]any{
		"name":         "Scoped Query",
		"enabled":      true,
		"search_query": "scope",
		"source_ids":   []int64{1},
	}, http.StatusOK, &created)
	if created.QueryID <= 0 {
		t.Fatalf("created query_id = %d, want > 0", created.QueryID)
	}
	if len(created.SourceIDs) != 1 || created.SourceIDs[0] != 1 {
		t.Fatalf("created source_ids = %#v, want [1]", created.SourceIDs)
	}
	if created.SearchTerm != "scope" {
		t.Fatalf("created search_query = %q, want scope", created.SearchTerm)
	}
}

func TestAdminRoutesDynamicChannelQueryRejectsInvalidBodySourceIDs(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:scope:primary",
			ChannelKey: "tvg:scope",
			Name:       "Scope Primary",
			Group:      "Scope",
			StreamURL:  "http://example.com/scope-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	createInvalid := doRaw(t, mux, http.MethodPost, "/api/dynamic-channels", map[string]any{
		"name":         "Scoped Query",
		"enabled":      true,
		"search_query": "scope",
		"source_ids":   []int64{0, -1},
	})
	if createInvalid.Code != http.StatusBadRequest {
		t.Fatalf("POST /api/dynamic-channels invalid source_ids status = %d, want %d", createInvalid.Code, http.StatusBadRequest)
	}

	var created struct {
		QueryID int64 `json:"query_id"`
	}
	doJSON(t, mux, http.MethodPost, "/api/dynamic-channels", map[string]any{
		"name":         "Scoped Query",
		"enabled":      true,
		"search_query": "scope",
		"source_ids":   []int64{1},
	}, http.StatusOK, &created)
	if created.QueryID <= 0 {
		t.Fatalf("created query_id = %d, want > 0", created.QueryID)
	}

	updateInvalid := doRaw(t, mux, http.MethodPatch, fmt.Sprintf("/api/dynamic-channels/%d", created.QueryID), map[string]any{
		"source_ids": []int64{-7},
	})
	if updateInvalid.Code != http.StatusBadRequest {
		t.Fatalf("PATCH /api/dynamic-channels/{queryID} invalid source_ids status = %d, want %d", updateInvalid.Code, http.StatusBadRequest)
	}
}

func TestAdminRoutesChannelDynamicRuleRejectsInvalidBodySourceIDs(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:scope:primary",
			ChannelKey: "tvg:scope",
			Name:       "Scope Primary",
			Group:      "Scope",
			StreamURL:  "http://example.com/scope-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	createInvalid := doRaw(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:scope:primary",
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"search_query": "scope",
			"source_ids":   []int64{0},
		},
	})
	if createInvalid.Code != http.StatusBadRequest {
		t.Fatalf("POST /api/channels invalid dynamic_rule.source_ids status = %d, want %d", createInvalid.Code, http.StatusBadRequest)
	}

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:scope:primary",
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"search_query": "scope",
			"source_ids":   []int64{1},
		},
	}, http.StatusOK, &created)
	if created.ChannelID <= 0 {
		t.Fatalf("created channel_id = %d, want > 0", created.ChannelID)
	}

	updateInvalid := doRaw(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"search_query": "scope",
			"source_ids":   []int64{-9},
		},
	})
	if updateInvalid.Code != http.StatusBadRequest {
		t.Fatalf("PATCH /api/channels/{channelID} invalid dynamic_rule.source_ids status = %d, want %d", updateInvalid.Code, http.StatusBadRequest)
	}
}

func TestAdminRoutesReorderSourcesMapsWrappedSourceOrderDriftToBadRequest(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	baseChannels := channels.NewService(store)
	channelsSvc := sourceOrderDriftReorderChannelsService{
		ChannelsService: baseChannels,
		reorderErr:      fmt.Errorf("reorder guard rejected request: %w", channels.ErrSourceOrderDrift),
	}

	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodPatch, "/api/channels/12/sources/reorder", map[string]any{
		"source_ids": []int64{3, 2, 1},
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PATCH /api/channels/{channelID}/sources/reorder status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if !strings.Contains(rec.Body.String(), channels.ErrSourceOrderDrift.Error()) {
		t.Fatalf("PATCH /api/channels/{channelID}/sources/reorder body = %q, want source-order drift sentinel text", rec.Body.String())
	}
}

func TestAdminDynamicChannelImmediateSyncUnsupportedSourceScopeIncrementsMetric(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.catalog = catalogWithoutSourceScopedSupport{CatalogStore: handler.catalog}

	metric := dynamicChannelImmediateSyncFailuresMetric.WithLabelValues("list_active_item_keys", "source_scoped_catalog_unsupported")
	before := testutil.ToFloat64(metric)

	channel := channels.Channel{
		ChannelID:   42,
		GuideNumber: "10042",
		DynamicRule: channels.DynamicSourceRule{
			Enabled:     true,
			SourceIDs:   []int64{2},
			SearchQuery: "scope",
		},
	}
	queuedWhileRunning, canceledRunning := handler.enqueueDynamicChannelSync(channel, true)
	if queuedWhileRunning {
		t.Fatal("enqueueDynamicChannelSync queued_while_running = true, want false on first run")
	}
	if canceledRunning {
		t.Fatal("enqueueDynamicChannelSync canceled_running = true, want false on first run")
	}

	waitForCondition(t, 2*time.Second, "dynamic sync unsupported-source metric increment", func() bool {
		return testutil.ToFloat64(metric) >= before+1
	})

	handler.Close()
}

func TestAdminRoutesDynamicSyncCreateReturnsPromptlyWhileSyncWorkerBlocked(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	release := make(chan struct{})
	catalog := &blockingCatalogStore{
		itemKeys:   []string{"src:news:primary"},
		blockUntil: release,
		blockCalls: 1,
	}
	observedChannels := &syncObservedChannelsService{
		base: channels.NewService(store),
	}

	handler, err := NewAdminHandler(catalog, observedChannels)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	responseCh := make(chan *httptest.ResponseRecorder, 1)
	startedAt := time.Now()
	go func() {
		responseCh <- doRaw(t, mux, http.MethodPost, "/api/channels", map[string]any{
			"item_key": "src:news:primary",
			"dynamic_rule": map[string]any{
				"enabled":      true,
				"group_name":   "US News",
				"search_query": "fox",
			},
		})
	}()

	var rec *httptest.ResponseRecorder
	select {
	case rec = <-responseCh:
	case <-time.After(300 * time.Millisecond):
		close(release)
		t.Fatal("POST /api/channels did not return promptly while dynamic sync worker was blocked")
	}
	if rec.Code != http.StatusOK {
		close(release)
		t.Fatalf("POST /api/channels status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		close(release)
		t.Fatalf("POST /api/channels latency = %v, want <= 250ms when sync worker is blocked", elapsed)
	}

	close(release)
	waitForCondition(t, 2*time.Second, "dynamic sync invocation", func() bool {
		return observedChannels.SyncCallCount() == 1
	})
}

func TestAdminRoutesDynamicSyncDetachedFromRequestCancellation(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	release := make(chan struct{})
	catalog := &blockingCatalogStore{
		itemKeys:   []string{"src:news:primary"},
		blockUntil: release,
		blockCalls: 1,
	}
	observedChannels := &syncObservedChannelsService{
		base: channels.NewService(store),
	}

	handler, err := NewAdminHandler(catalog, observedChannels)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	reqCtx, cancel := context.WithCancel(context.Background())
	responseCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		payload, marshalErr := json.Marshal(map[string]any{
			"item_key": "src:news:primary",
			"dynamic_rule": map[string]any{
				"enabled":      true,
				"group_name":   "US News",
				"search_query": "fox",
			},
		})
		if marshalErr != nil {
			t.Errorf("marshal request body: %v", marshalErr)
			return
		}
		req := httptest.NewRequest(http.MethodPost, "/api/channels", bytes.NewReader(payload)).WithContext(reqCtx)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		responseCh <- rec
	}()

	var rec *httptest.ResponseRecorder
	select {
	case rec = <-responseCh:
	case <-time.After(300 * time.Millisecond):
		close(release)
		cancel()
		t.Fatal("POST /api/channels did not return promptly while dynamic sync worker was blocked")
	}
	if rec.Code != http.StatusOK {
		close(release)
		cancel()
		t.Fatalf("POST /api/channels status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	waitForCondition(t, time.Second, "dynamic sync catalog lookup start", func() bool {
		return catalog.CallCount() >= 1
	})
	cancel()
	close(release)

	waitForCondition(t, 2*time.Second, "dynamic sync invocation after request cancellation", func() bool {
		return observedChannels.SyncCallCount() == 1
	})
	if canceled := catalog.CanceledCount(); canceled != 0 {
		t.Fatalf("catalog lookup canceled %d times, want 0", canceled)
	}
}

func TestAdminRoutesDynamicSyncCoalescesBurstUpdates(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	release := make(chan struct{})
	releaseOnce := sync.Once{}
	releaseCatalog := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releaseCatalog()

	catalog := &blockingCatalogStore{
		itemKeys:   []string{"src:news:primary", "src:news:backup"},
		blockUntil: release,
		blockCalls: 3,
	}
	observedChannels := &syncObservedChannelsService{
		base: channels.NewService(store),
	}

	handler, err := NewAdminHandler(catalog, observedChannels)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:news:primary",
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "US News",
			"search_query": "fox",
		},
	}, http.StatusOK, &created)

	waitForCondition(t, time.Second, "first dynamic sync catalog lookup start", func() bool {
		return catalog.CallCount() >= 1
	})

	path := fmt.Sprintf("/api/channels/%d", created.ChannelID)
	supersedingQueries := []string{"fox one", "fox two", "fox three"}
	for idx, query := range supersedingQueries {
		doJSON(t, mux, http.MethodPatch, path, map[string]any{
			"dynamic_rule": map[string]any{
				"enabled":      true,
				"group_name":   "US News",
				"search_query": query,
			},
		}, http.StatusOK, nil)

		wantCanceled := idx + 1
		waitForCondition(t, time.Second, fmt.Sprintf("canceled superseded dynamic sync lookup #%d", wantCanceled), func() bool {
			return catalog.CanceledCount() >= wantCanceled
		})
		if idx < len(supersedingQueries)-1 {
			wantCalls := idx + 2
			waitForCondition(t, time.Second, fmt.Sprintf("next dynamic sync lookup #%d started", wantCalls), func() bool {
				return catalog.CallCount() >= wantCalls
			})
		}
	}

	releaseCatalog()
	waitForCondition(t, 2*time.Second, "coalesced dynamic sync completion", func() bool {
		return observedChannels.SyncCallCount() == 1
	})
	if got := catalog.CallCount(); got != 4 {
		t.Fatalf("catalog lookup call count = %d, want 4 (3 canceled superseded runs + 1 final apply)", got)
	}
	if canceled := catalog.CanceledCount(); canceled != 3 {
		t.Fatalf("catalog lookup canceled count = %d, want 3", canceled)
	}
	if maxConcurrent := observedChannels.MaxActiveSyncs(); maxConcurrent > 1 {
		t.Fatalf("max concurrent sync runs = %d, want <= 1", maxConcurrent)
	}

	var sourcesResp struct {
		Sources []channels.Source `json:"sources"`
	}
	doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), nil, http.StatusOK, &sourcesResp)
	if len(sourcesResp.Sources) != 2 {
		t.Fatalf("len(channel sources) = %d, want 2 after latest-only coalesced apply", len(sourcesResp.Sources))
	}
}

func TestAdminRoutesDynamicSyncDisableCancelsInFlightAndPreventsStaleApply(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	release := make(chan struct{})
	releaseOnce := sync.Once{}
	releaseCatalog := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releaseCatalog()

	catalog := &blockingCatalogStore{
		itemKeys:   []string{"src:news:primary", "src:news:backup"},
		blockUntil: release,
		blockCalls: 1,
	}
	observedChannels := &syncObservedChannelsService{
		base: channels.NewService(store),
	}

	handler, err := NewAdminHandler(catalog, observedChannels)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:news:primary",
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "US News",
			"search_query": "fox",
		},
	}, http.StatusOK, &created)

	waitForCondition(t, time.Second, "first dynamic sync catalog lookup start", func() bool {
		return catalog.CallCount() >= 1
	})

	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"dynamic_rule": map[string]any{
			"enabled":      false,
			"group_name":   "US News",
			"search_query": "fox",
		},
	}, http.StatusOK, nil)

	waitForCondition(t, time.Second, "in-flight dynamic sync cancellation", func() bool {
		return catalog.CanceledCount() >= 1
	})
	releaseCatalog()

	if got := observedChannels.SyncCallCount(); got != 0 {
		t.Fatalf("sync call count = %d, want 0 after disable while sync in-flight", got)
	}

	var sourcesResp struct {
		Sources []channels.Source `json:"sources"`
	}
	doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), nil, http.StatusOK, &sourcesResp)
	if len(sourcesResp.Sources) != 1 {
		t.Fatalf("len(channel sources) = %d, want 1 (no stale dynamic apply)", len(sourcesResp.Sources))
	}
	if got := sourcesResp.Sources[0].ItemKey; got != "src:news:primary" {
		t.Fatalf("channel source item_key = %q, want src:news:primary", got)
	}
}

func TestAdminRoutesDynamicSyncDisableWinsOverQueuedEnabledUpdates(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	release := make(chan struct{})
	releaseOnce := sync.Once{}
	releaseCatalog := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releaseCatalog()

	catalog := &blockingCatalogStore{
		itemKeys:   []string{"src:news:primary", "src:news:backup"},
		blockUntil: release,
		blockCalls: 3,
	}
	observedChannels := &syncObservedChannelsService{
		base: channels.NewService(store),
	}

	handler, err := NewAdminHandler(catalog, observedChannels)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:news:primary",
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "US News",
			"search_query": "fox",
		},
	}, http.StatusOK, &created)

	waitForCondition(t, time.Second, "first dynamic sync catalog lookup start", func() bool {
		return catalog.CallCount() >= 1
	})

	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "US News",
			"search_query": "fox one",
		},
	}, http.StatusOK, nil)
	waitForCondition(t, time.Second, "in-flight dynamic sync cancellation after first enabled supersession", func() bool {
		return catalog.CanceledCount() >= 1
	})
	waitForCondition(t, time.Second, "second dynamic sync lookup start", func() bool {
		return catalog.CallCount() >= 2
	})

	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "US News",
			"search_query": "fox two",
		},
	}, http.StatusOK, nil)
	waitForCondition(t, time.Second, "in-flight dynamic sync cancellation after second enabled supersession", func() bool {
		return catalog.CanceledCount() >= 2
	})
	waitForCondition(t, time.Second, "third dynamic sync lookup start", func() bool {
		return catalog.CallCount() >= 3
	})

	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"dynamic_rule": map[string]any{
			"enabled":      false,
			"group_name":   "US News",
			"search_query": "fox two",
		},
	}, http.StatusOK, nil)

	waitForCondition(t, time.Second, "in-flight dynamic sync cancellation on disable", func() bool {
		return catalog.CanceledCount() >= 3
	})
	releaseCatalog()

	stableUntil := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(stableUntil) {
		if got := catalog.CallCount(); got != 3 {
			t.Fatalf("catalog call count = %d, want 3 (only canceled in-flight superseded runs before final disable)", got)
		}
		if got := observedChannels.SyncCallCount(); got != 0 {
			t.Fatalf("sync call count = %d, want 0 after final disable", got)
		}
		time.Sleep(10 * time.Millisecond)
	}

	var sourcesResp struct {
		Sources []channels.Source `json:"sources"`
	}
	doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), nil, http.StatusOK, &sourcesResp)
	if len(sourcesResp.Sources) != 1 {
		t.Fatalf("len(channel sources) = %d, want 1 (disabled rule must win)", len(sourcesResp.Sources))
	}
	if got := sourcesResp.Sources[0].ItemKey; got != "src:news:primary" {
		t.Fatalf("channel source item_key = %q, want src:news:primary", got)
	}
}

func TestAdminRoutesDynamicSyncDeleteCancelsInFlightAndCleansState(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "FOX 9 KMSP",
			Group:      "US News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "FOX Backup",
			Group:      "US News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	release := make(chan struct{})
	releaseOnce := sync.Once{}
	releaseCatalog := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer releaseCatalog()

	catalog := &blockingCatalogStore{
		itemKeys:   []string{"src:news:primary", "src:news:backup"},
		blockUntil: release,
		blockCalls: 1,
	}
	observedChannels := &syncObservedChannelsService{
		base: channels.NewService(store),
	}

	handler, err := NewAdminHandler(catalog, observedChannels)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:news:primary",
		"dynamic_rule": map[string]any{
			"enabled":      true,
			"group_name":   "US News",
			"search_query": "fox",
		},
	}, http.StatusOK, &created)

	waitForCondition(t, time.Second, "first dynamic sync catalog lookup start", func() bool {
		return catalog.CallCount() >= 1
	})

	deletePath := fmt.Sprintf("/api/channels/%d", created.ChannelID)
	deleteRec := doRaw(t, mux, http.MethodDelete, deletePath, nil)
	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("DELETE /api/channels/{id} status = %d, want %d, body=%s", deleteRec.Code, http.StatusNoContent, deleteRec.Body.String())
	}

	waitForCondition(t, time.Second, "in-flight dynamic sync cancellation on delete", func() bool {
		return catalog.CanceledCount() >= 1
	})
	releaseCatalog()

	if got := observedChannels.SyncCallCount(); got != 0 {
		t.Fatalf("sync call count = %d, want 0 after delete while sync in-flight", got)
	}

	waitForCondition(t, time.Second, "dynamic sync state cleanup after delete", func() bool {
		handler.dynamicSyncMu.Lock()
		defer handler.dynamicSyncMu.Unlock()
		_, ok := handler.dynamicSyncStates[created.ChannelID]
		return !ok
	})

	var channelsResp struct {
		Channels []channels.Channel `json:"channels"`
	}
	doJSON(t, mux, http.MethodGet, "/api/channels", nil, http.StatusOK, &channelsResp)
	if len(channelsResp.Channels) != 0 {
		t.Fatalf("len(channels) = %d, want 0 after delete", len(channelsResp.Channels))
	}
}

func TestAdminRoutesClearSourceHealth(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "News Backup",
			Group:      "News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
		{
			ItemKey:    "src:sports:primary",
			ChannelKey: "tvg:sports",
			Name:       "Sports Primary",
			Group:      "Sports",
			StreamURL:  "http://example.com/sports-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	sourceHealthRuntimeClearer := &fakeSourceHealthClearRuntime{}
	handler.SetSourceHealthClearRuntime(sourceHealthRuntimeClearer)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var newsChannel channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:news:primary",
	}, http.StatusOK, &newsChannel)

	doJSON(t, mux, http.MethodPost, fmt.Sprintf("/api/channels/%d/sources", newsChannel.ChannelID), map[string]any{
		"item_key": "src:news:backup",
	}, http.StatusOK, nil)

	var sportsChannel channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:sports:primary",
	}, http.StatusOK, &sportsChannel)

	var newsSources struct {
		Sources []channels.Source `json:"sources"`
	}
	doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", newsChannel.ChannelID), nil, http.StatusOK, &newsSources)
	if len(newsSources.Sources) != 2 {
		t.Fatalf("len(news sources) = %d, want 2", len(newsSources.Sources))
	}
	for _, src := range newsSources.Sources {
		if err := store.MarkSourceFailure(ctx, src.SourceID, "startup timeout", time.Unix(1_700_200_000, 0).UTC()); err != nil {
			t.Fatalf("MarkSourceFailure(news source %d) error = %v", src.SourceID, err)
		}
	}

	var sportsSources struct {
		Sources []channels.Source `json:"sources"`
	}
	doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", sportsChannel.ChannelID), nil, http.StatusOK, &sportsSources)
	if len(sportsSources.Sources) != 1 {
		t.Fatalf("len(sports sources) = %d, want 1", len(sportsSources.Sources))
	}
	if err := store.MarkSourceSuccess(ctx, sportsSources.Sources[0].SourceID, time.Unix(1_700_300_000, 0).UTC()); err != nil {
		t.Fatalf("MarkSourceSuccess(sports source) error = %v", err)
	}

	var clearChannelResp struct {
		Cleared int64 `json:"cleared"`
	}
	doJSON(t, mux, http.MethodPost, fmt.Sprintf("/api/channels/%d/sources/health/clear", newsChannel.ChannelID), nil, http.StatusOK, &clearChannelResp)
	if clearChannelResp.Cleared < 1 {
		t.Fatalf("channel clear response cleared = %d, want at least 1", clearChannelResp.Cleared)
	}
	if got := sourceHealthRuntimeClearer.clearSourceHealthCallCount(); got != 1 {
		t.Fatalf("runtime clear source health call count = %d, want 1", got)
	}
	if got := sourceHealthRuntimeClearer.lastClearSourceHealthChannel(); got != newsChannel.ChannelID {
		t.Fatalf("runtime clear source health channel = %d, want %d", got, newsChannel.ChannelID)
	}

	newsSources = struct {
		Sources []channels.Source `json:"sources"`
	}{}
	doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", newsChannel.ChannelID), nil, http.StatusOK, &newsSources)
	for _, src := range newsSources.Sources {
		if src.SuccessCount != 0 || src.FailCount != 0 {
			t.Fatalf("news source %d counts after channel clear = ok:%d fail:%d, want 0", src.SourceID, src.SuccessCount, src.FailCount)
		}
		if src.LastOKAt != 0 || src.LastFailAt != 0 || src.CooldownUntil != 0 {
			t.Fatalf("news source %d timestamps after channel clear = ok:%d fail:%d cooldown:%d, want 0", src.SourceID, src.LastOKAt, src.LastFailAt, src.CooldownUntil)
		}
		if src.LastFailReason != "" {
			t.Fatalf("news source %d last_fail_reason after channel clear = %q, want empty", src.SourceID, src.LastFailReason)
		}
	}

	sportsSources = struct {
		Sources []channels.Source `json:"sources"`
	}{}
	doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", sportsChannel.ChannelID), nil, http.StatusOK, &sportsSources)
	if sportsSources.Sources[0].SuccessCount != 1 {
		t.Fatalf("sports source success_count after news clear = %d, want 1", sportsSources.Sources[0].SuccessCount)
	}

	var clearAllResp struct {
		Cleared int64 `json:"cleared"`
	}
	doJSON(t, mux, http.MethodPost, "/api/channels/sources/health/clear", nil, http.StatusOK, &clearAllResp)
	if clearAllResp.Cleared < 1 {
		t.Fatalf("clear all response cleared = %d, want at least 1", clearAllResp.Cleared)
	}
	if got := sourceHealthRuntimeClearer.clearAllSourceHealthCallCount(); got != 1 {
		t.Fatalf("runtime clear all source health call count = %d, want 1", got)
	}

	sportsSources = struct {
		Sources []channels.Source `json:"sources"`
	}{}
	doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", sportsChannel.ChannelID), nil, http.StatusOK, &sportsSources)
	src := sportsSources.Sources[0]
	if src.SuccessCount != 0 || src.FailCount != 0 {
		t.Fatalf("sports source counts after clear all = ok:%d fail:%d, want 0", src.SuccessCount, src.FailCount)
	}
	if src.LastOKAt != 0 || src.LastFailAt != 0 || src.CooldownUntil != 0 {
		t.Fatalf("sports source timestamps after clear all = ok:%d fail:%d cooldown:%d, want 0", src.LastOKAt, src.LastFailAt, src.CooldownUntil)
	}
	if src.LastFailReason != "" {
		t.Fatalf("sports source last_fail_reason after clear all = %q, want empty", src.LastFailReason)
	}
}

func TestAdminRoutesUpdateSourceDisabledActiveSourceTriggersSessionRecovery(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "News Backup",
			Group:      "News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{"item_key": "src:news:primary"}, http.StatusOK, &created)

	var backup channels.Source
	doJSON(t, mux, http.MethodPost, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), map[string]any{
		"item_key": "src:news:backup",
	}, http.StatusOK, &backup)

	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: created.ChannelID,
					SourceID:  backup.SourceID,
					TunerID:   1,
					Kind:      "client",
				},
			},
		},
	}
	handler.SetTunerStatusProvider(provider)

	var updated channels.Source
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d/sources/%d", created.ChannelID, backup.SourceID), map[string]any{
		"enabled": false,
	}, http.StatusOK, &updated)
	if updated.Enabled {
		t.Fatal("updated.Enabled = true, want false")
	}
	if updated.SourceID != backup.SourceID {
		t.Fatalf("updated source_id = %d, want %d", updated.SourceID, backup.SourceID)
	}
	if got := provider.triggerCalls; got != 1 {
		t.Fatalf("triggerCalls = %d, want 1", got)
	}
	if got := provider.lastTriggerChannelID; got != created.ChannelID {
		t.Fatalf("lastTriggerChannelID = %d, want %d", got, created.ChannelID)
	}
	if provider.lastTriggerReason != "admin_source_disabled" {
		t.Fatalf("lastTriggerReason = %q, want admin_source_disabled", provider.lastTriggerReason)
	}
}

func TestAdminRoutesUpdateSourceDisabledNonActiveSourceSkipsSessionRecovery(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "News Backup",
			Group:      "News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{"item_key": "src:news:primary"}, http.StatusOK, &created)

	var backup channels.Source
	doJSON(t, mux, http.MethodPost, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), map[string]any{
		"item_key": "src:news:backup",
	}, http.StatusOK, &backup)

	var sources struct {
		Sources []channels.Source `json:"sources"`
	}
	doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), nil, http.StatusOK, &sources)
	if len(sources.Sources) != 2 {
		t.Fatalf("len(sources) = %d, want 2", len(sources.Sources))
	}

	var primaryID int64
	for _, src := range sources.Sources {
		if src.SourceID != backup.SourceID {
			primaryID = src.SourceID
			break
		}
	}
	if primaryID == 0 {
		t.Fatal("could not identify primary source id")
	}

	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: created.ChannelID,
					SourceID:  primaryID,
					TunerID:   1,
					Kind:      "client",
				},
			},
		},
	}
	handler.SetTunerStatusProvider(provider)

	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d/sources/%d", created.ChannelID, backup.SourceID), map[string]any{
		"enabled": false,
	}, http.StatusOK, nil)
	if got := provider.triggerCalls; got != 0 {
		t.Fatalf("triggerCalls = %d, want 0 for non-active source", got)
	}
}

func TestAdminRoutesDeleteSourceActiveSourceTriggersSessionRecovery(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "News Backup",
			Group:      "News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{"item_key": "src:news:primary"}, http.StatusOK, &created)

	var backup channels.Source
	doJSON(t, mux, http.MethodPost, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), map[string]any{
		"item_key": "src:news:backup",
	}, http.StatusOK, &backup)

	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: created.ChannelID,
					SourceID:  backup.SourceID,
					TunerID:   1,
					Kind:      "client",
				},
			},
		},
	}
	handler.SetTunerStatusProvider(provider)

	rec := doRaw(t, mux, http.MethodDelete, fmt.Sprintf("/api/channels/%d/sources/%d", created.ChannelID, backup.SourceID), nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if got := provider.triggerCalls; got != 1 {
		t.Fatalf("triggerCalls = %d, want 1", got)
	}
	if got := provider.lastTriggerChannelID; got != created.ChannelID {
		t.Fatalf("lastTriggerChannelID = %d, want %d", got, created.ChannelID)
	}
	if provider.lastTriggerReason != "admin_source_deleted" {
		t.Fatalf("lastTriggerReason = %q, want admin_source_deleted", provider.lastTriggerReason)
	}
}

func TestAdminRoutesSourceMutationRecoverySkipsNoActiveSessionFromProvider(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
		{
			ItemKey:    "src:news:backup",
			ChannelKey: "tvg:news",
			Name:       "News Backup",
			Group:      "News",
			StreamURL:  "http://example.com/news-backup.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{"item_key": "src:news:primary"}, http.StatusOK, &created)

	var backup channels.Source
	doJSON(t, mux, http.MethodPost, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), map[string]any{
		"item_key": "src:news:backup",
	}, http.StatusOK, &backup)

	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: created.ChannelID,
					SourceID:  backup.SourceID,
					TunerID:   1,
					Kind:      "client",
				},
			},
		},
		triggerErr: stream.ErrSessionNotFound,
	}
	handler.SetTunerStatusProvider(provider)

	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d/sources/%d", created.ChannelID, backup.SourceID), map[string]any{
		"enabled": false,
	}, http.StatusOK, nil)
	if got := provider.triggerCalls; got != 1 {
		t.Fatalf("triggerCalls = %d, want 1", got)
	}
}

func TestAdminRoutesUpdateChannelDisabledActiveChannelTriggersSessionRecovery(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{"item_key": "src:news:primary"}, http.StatusOK, &created)

	var sources struct {
		Sources []channels.Source `json:"sources"`
	}
	doJSON(t, mux, http.MethodGet, fmt.Sprintf("/api/channels/%d/sources", created.ChannelID), nil, http.StatusOK, &sources)
	if len(sources.Sources) == 0 {
		t.Fatal("len(channel sources) = 0, want at least 1")
	}

	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: created.ChannelID,
					SourceID:  sources.Sources[0].SourceID,
					TunerID:   1,
					Kind:      "client",
				},
			},
		},
	}
	handler.SetTunerStatusProvider(provider)

	var updated channels.Channel
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"enabled": false,
	}, http.StatusOK, &updated)
	if updated.Enabled {
		t.Fatal("updated.Enabled = true, want false")
	}
	if got := provider.triggerCalls; got != 1 {
		t.Fatalf("triggerCalls = %d, want 1", got)
	}
	if got := provider.lastTriggerChannelID; got != created.ChannelID {
		t.Fatalf("lastTriggerChannelID = %d, want %d", got, created.ChannelID)
	}
	if provider.lastTriggerReason != "admin_channel_disabled" {
		t.Fatalf("lastTriggerReason = %q, want admin_channel_disabled", provider.lastTriggerReason)
	}
}

func TestAdminRoutesUpdateChannelDisabledNonActiveChannelSkipsSessionRecovery(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{"item_key": "src:news:primary"}, http.StatusOK, &created)

	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: created.ChannelID + 100,
					SourceID:  999,
					TunerID:   1,
					Kind:      "client",
				},
			},
		},
	}
	handler.SetTunerStatusProvider(provider)

	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"enabled": false,
	}, http.StatusOK, nil)
	if got := provider.triggerCalls; got != 0 {
		t.Fatalf("triggerCalls = %d, want 0 for non-active channel", got)
	}
}

func TestAdminRoutesUpdateChannelMetadataOnlySkipsSessionRecovery(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:news:primary",
	}, http.StatusOK, &created)

	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: created.ChannelID,
					SourceID:  12,
					TunerID:   1,
					Kind:      "client",
				},
			},
		},
	}
	handler.SetTunerStatusProvider(provider)

	var updated channels.Channel
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"guide_name": "News + Live",
	}, http.StatusOK, &updated)
	if updated.GuideName != "News + Live" {
		t.Fatalf("updated.GuideName = %q, want %q", updated.GuideName, "News + Live")
	}
	if got := provider.triggerCalls; got != 0 {
		t.Fatalf("triggerCalls = %d, want 0 for metadata-only update", got)
	}
}

func TestAdminRoutesUpdateChannelDisabledRecoveryInputErrorIsNonFatal(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	var logBuffer bytes.Buffer
	handler.SetLogger(slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelInfo})))

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:news:primary",
	}, http.StatusOK, &created)

	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: created.ChannelID,
					SourceID:  12,
					TunerID:   1,
					Kind:      "client",
				},
			},
		},
		triggerErr: fmt.Errorf("recovery reason must be non-empty"),
	}
	handler.SetTunerStatusProvider(provider)

	var updated channels.Channel
	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"enabled": false,
	}, http.StatusOK, &updated)
	if updated.Enabled {
		t.Fatal("updated.Enabled = true, want false")
	}
	if got := provider.triggerCalls; got != 1 {
		t.Fatalf("triggerCalls = %d, want 1", got)
	}
	logText := logBuffer.String()
	if !strings.Contains(logText, "admin channel mutation recovery request rejected") {
		t.Fatalf("logs = %q, want input-rejected recovery event", logText)
	}
	if strings.Contains(logText, "admin channel mutation recovery request failed") {
		t.Fatalf("logs = %q, want no generic-failure recovery event for input error", logText)
	}
}

func TestAdminRoutesDeleteChannelActiveChannelTriggersSessionRecovery(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{"item_key": "src:news:primary"}, http.StatusOK, &created)

	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: created.ChannelID,
					SourceID:  12,
					TunerID:   1,
					Kind:      "client",
				},
			},
		},
	}
	handler.SetTunerStatusProvider(provider)

	rec := doRaw(t, mux, http.MethodDelete, fmt.Sprintf("/api/channels/%d", created.ChannelID), nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if got := provider.triggerCalls; got != 1 {
		t.Fatalf("triggerCalls = %d, want 1", got)
	}
	if got := provider.lastTriggerChannelID; got != created.ChannelID {
		t.Fatalf("lastTriggerChannelID = %d, want %d", got, created.ChannelID)
	}
	if provider.lastTriggerReason != "admin_channel_deleted" {
		t.Fatalf("lastTriggerReason = %q, want admin_channel_deleted", provider.lastTriggerReason)
	}
}

func TestAdminRoutesChannelMutationRecoverySkipsNoActiveSessionFromProvider(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{"item_key": "src:news:primary"}, http.StatusOK, &created)

	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: created.ChannelID,
					SourceID:  12,
					TunerID:   1,
					Kind:      "client",
				},
			},
		},
		triggerErr: stream.ErrSessionNotFound,
	}
	handler.SetTunerStatusProvider(provider)

	doJSON(t, mux, http.MethodPatch, fmt.Sprintf("/api/channels/%d", created.ChannelID), map[string]any{
		"enabled": false,
	}, http.StatusOK, nil)
	if got := provider.triggerCalls; got != 1 {
		t.Fatalf("triggerCalls = %d, want 1", got)
	}
}

func TestAdminRoutesTunerStatusSnapshot(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	now := time.Unix(1_770_000_000, 0).UTC()
	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			GeneratedAt: now,
			TunerCount:  2,
			InUseCount:  1,
			IdleCount:   1,
			VirtualTuners: []stream.VirtualTunerStatus{
				{
					PlaylistSourceID:    1,
					PlaylistSourceName:  "Primary",
					PlaylistSourceOrder: 0,
					TunerCount:          1,
					InUseCount:          0,
					IdleCount:           1,
					ActiveSessionCount:  0,
				},
				{
					PlaylistSourceID:    2,
					PlaylistSourceName:  "Backup A",
					PlaylistSourceOrder: 1,
					TunerCount:          1,
					InUseCount:          1,
					IdleCount:           0,
					ActiveSessionCount:  1,
				},
			},
			Tuners: []stream.TunerStatus{
				{
					TunerID:            0,
					PlaylistSourceID:   2,
					PlaylistSourceName: "Backup A",
					VirtualTunerSlot:   0,
					Kind:               "client",
					GuideNumber:        "101",
					GuideName:          "News",
					SourceID:           10,
					SourceItemKey:      "src:news:primary",
					SourceStreamURL:    "http://example.com/news.ts",
					Subscribers: []stream.SubscriberStats{
						{
							SubscriberID: 1,
							ClientAddr:   "192.168.1.100:50000",
							StartedAt:    now.Add(-30 * time.Second),
						},
					},
				},
			},
			ClientStreams: []stream.ClientStreamStatus{
				{
					TunerID:            0,
					PlaylistSourceID:   2,
					PlaylistSourceName: "Backup A",
					VirtualTunerSlot:   0,
					GuideNumber:        "101",
					SubscriberID:       1,
					ClientAddr:         "192.168.1.100:50000",
					ConnectedAt:        now.Add(-30 * time.Second),
				},
			},
			SessionHistory: []stream.SharedSessionHistory{
				{
					SessionID:   501,
					ChannelID:   1,
					GuideNumber: "101",
					GuideName:   "News",
					OpenedAt:    now.Add(-2 * time.Minute),
					ClosedAt:    now.Add(-1 * time.Minute),
					Active:      false,
				},
			},
			SessionHistoryLimit:          256,
			SessionHistoryTruncatedCount: 0,
		},
	}
	handler.SetTunerStatusProvider(provider)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var payload stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners", nil, http.StatusOK, &payload)
	if payload.TunerCount != 2 {
		t.Fatalf("payload.tuner_count = %d, want 2", payload.TunerCount)
	}
	if len(payload.Tuners) != 1 {
		t.Fatalf("len(payload.tuners) = %d, want 1", len(payload.Tuners))
	}
	if payload.Tuners[0].GuideNumber != "101" {
		t.Fatalf("payload.tuners[0].guide_number = %q, want 101", payload.Tuners[0].GuideNumber)
	}
	if got, want := payload.Tuners[0].PlaylistSourceID, int64(2); got != want {
		t.Fatalf("payload.tuners[0].playlist_source_id = %d, want %d", got, want)
	}
	if got, want := payload.Tuners[0].PlaylistSourceName, "Backup A"; got != want {
		t.Fatalf("payload.tuners[0].playlist_source_name = %q, want %q", got, want)
	}
	if got, want := payload.Tuners[0].VirtualTunerSlot, 0; got != want {
		t.Fatalf("payload.tuners[0].virtual_tuner_slot = %d, want %d", got, want)
	}
	if got := len(payload.ClientStreams); got != 1 {
		t.Fatalf("len(payload.client_streams) = %d, want 1", got)
	}
	if got, want := payload.ClientStreams[0].PlaylistSourceID, int64(2); got != want {
		t.Fatalf("payload.client_streams[0].playlist_source_id = %d, want %d", got, want)
	}
	if got, want := payload.ClientStreams[0].PlaylistSourceName, "Backup A"; got != want {
		t.Fatalf("payload.client_streams[0].playlist_source_name = %q, want %q", got, want)
	}
	if got, want := payload.ClientStreams[0].VirtualTunerSlot, 0; got != want {
		t.Fatalf("payload.client_streams[0].virtual_tuner_slot = %d, want %d", got, want)
	}
	if got, want := len(payload.VirtualTuners), 2; got != want {
		t.Fatalf("len(payload.virtual_tuners) = %d, want %d", got, want)
	}
	if got, want := payload.VirtualTuners[1].PlaylistSourceID, int64(2); got != want {
		t.Fatalf("payload.virtual_tuners[1].playlist_source_id = %d, want %d", got, want)
	}
	if got, want := payload.VirtualTuners[1].ActiveSessionCount, 1; got != want {
		t.Fatalf("payload.virtual_tuners[1].active_session_count = %d, want %d", got, want)
	}
	if got := len(payload.SessionHistory); got != 1 {
		t.Fatalf("len(payload.session_history) = %d, want 1", got)
	}
	if got, want := payload.SessionHistory[0].SessionID, uint64(501); got != want {
		t.Fatalf("payload.session_history[0].session_id = %d, want %d", got, want)
	}
}

func TestAdminRoutesTunerStatusResolveIPEnabled(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			ClientStreams: []stream.ClientStreamStatus{
				{
					SubscriberID: 11,
					ClientAddr:   "10.13.0.165:49084",
				},
				{
					SubscriberID: 12,
					ClientAddr:   "10.13.0.165:49085",
				},
				{
					SubscriberID: 13,
					ClientAddr:   "unknown-client",
				},
			},
			SessionHistory: []stream.SharedSessionHistory{
				{
					SessionID: 7001,
					Subscribers: []stream.SharedSessionSubscriberHistory{
						{
							SubscriberID: 22,
							ClientAddr:   "10.13.0.165:59084",
						},
						{
							SubscriberID: 23,
							ClientAddr:   "10.13.0.166:59085",
						},
					},
				},
			},
		},
	}
	handler.SetTunerStatusProvider(provider)

	lookupCalls := make(map[string]int)
	handler.lookupAddr = func(_ context.Context, addr string) ([]string, error) {
		lookupCalls[addr]++
		if addr == "10.13.0.165" {
			return []string{"living-room.local."}, nil
		}
		if addr == "10.13.0.166" {
			return []string{"kitchen-tablet.local."}, nil
		}
		return nil, fmt.Errorf("no PTR record")
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var payload stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &payload)

	if got := len(payload.ClientStreams); got != 3 {
		t.Fatalf("len(payload.client_streams) = %d, want 3", got)
	}

	if got, want := payload.ClientStreams[0].ClientHost, "living-room.local"; got != want {
		t.Fatalf("payload.client_streams[0].client_host = %q, want %q", got, want)
	}
	if got, want := payload.ClientStreams[1].ClientHost, "living-room.local"; got != want {
		t.Fatalf("payload.client_streams[1].client_host = %q, want %q", got, want)
	}
	if got := payload.ClientStreams[2].ClientHost; got != "" {
		t.Fatalf("payload.client_streams[2].client_host = %q, want empty", got)
	}
	if got, want := payload.SessionHistory[0].Subscribers[0].ClientHost, "living-room.local"; got != want {
		t.Fatalf("payload.session_history[0].subscribers[0].client_host = %q, want %q", got, want)
	}
	if got, want := payload.SessionHistory[0].Subscribers[1].ClientHost, "kitchen-tablet.local"; got != want {
		t.Fatalf("payload.session_history[0].subscribers[1].client_host = %q, want %q", got, want)
	}

	// Duplicate IP lookups should be memoized per response.
	if got, want := lookupCalls["10.13.0.165"], 1; got != want {
		t.Fatalf("lookupCalls[10.13.0.165] = %d, want %d", got, want)
	}
	if got, want := lookupCalls["10.13.0.166"], 1; got != want {
		t.Fatalf("lookupCalls[10.13.0.166] = %d, want %d", got, want)
	}

	// Provider snapshot should remain unchanged.
	if got := provider.snapshot.ClientStreams[0].ClientHost; got != "" {
		t.Fatalf("provider snapshot mutated client_host = %q, want empty", got)
	}
	if got := provider.snapshot.SessionHistory[0].Subscribers[0].ClientHost; got != "" {
		t.Fatalf("provider snapshot mutated session_history subscriber client_host = %q, want empty", got)
	}
}

func TestAdminRoutesTunerStatusResolveIPUsesPerLookupTimeout(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	handler.SetTunerStatusProvider(&fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			ClientStreams: []stream.ClientStreamStatus{
				{
					SubscriberID: 41,
					ClientAddr:   "10.99.0.1:40000",
				},
			},
		},
	})

	lookupCalls := 0
	handler.lookupAddr = func(ctx context.Context, addr string) ([]string, error) {
		lookupCalls++
		deadline, hasDeadline := ctx.Deadline()
		if !hasDeadline {
			t.Fatal("lookup context missing deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("lookup context deadline remaining = %s, want positive", remaining)
		}
		if remaining > defaultResolveClientHostLookupTimeout {
			t.Fatalf(
				"lookup context deadline remaining = %s, want <= %s",
				remaining,
				defaultResolveClientHostLookupTimeout,
			)
		}
		if addr != "10.99.0.1" {
			t.Fatalf("lookup address = %q, want 10.99.0.1", addr)
		}
		return []string{"garage-tv.local."}, nil
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var payload stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &payload)

	if lookupCalls != 1 {
		t.Fatalf("lookupCalls = %d, want 1", lookupCalls)
	}
	if got, want := payload.ClientStreams[0].ClientHost, "garage-tv.local"; got != want {
		t.Fatalf("payload.client_streams[0].client_host = %q, want %q", got, want)
	}
}

func TestAdminRoutesTunerStatusResolveIPUsesCrossRequestCache(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.resolveClientHostCacheTTL = 2 * time.Minute

	handler.SetTunerStatusProvider(&fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			ClientStreams: []stream.ClientStreamStatus{
				{
					SubscriberID: 51,
					ClientAddr:   "10.88.0.7:41000",
				},
			},
		},
	})

	lookupCalls := 0
	handler.lookupAddr = func(_ context.Context, addr string) ([]string, error) {
		lookupCalls++
		if addr != "10.88.0.7" {
			t.Fatalf("lookup address = %q, want 10.88.0.7", addr)
		}
		return []string{"bedroom-tv.local."}, nil
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var first stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &first)
	if got, want := first.ClientStreams[0].ClientHost, "bedroom-tv.local"; got != want {
		t.Fatalf("first payload client_host = %q, want %q", got, want)
	}

	var second stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &second)
	if got, want := second.ClientStreams[0].ClientHost, "bedroom-tv.local"; got != want {
		t.Fatalf("second payload client_host = %q, want %q", got, want)
	}

	if lookupCalls != 1 {
		t.Fatalf("lookupCalls = %d, want 1 across repeated requests", lookupCalls)
	}
}

func TestAdminRoutesTunerStatusResolveIPCacheExpiresAfterTTL(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.resolveClientHostCacheTTL = 2 * time.Minute
	handler.resolveClientHostCacheSweepInterval = 0

	now := time.Unix(1_700_000_000, 0).UTC()
	handler.resolveClientHostCacheNow = func() time.Time {
		return now
	}

	handler.SetTunerStatusProvider(&fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			ClientStreams: []stream.ClientStreamStatus{
				{
					SubscriberID: 53,
					ClientAddr:   "10.88.0.9:41000",
				},
			},
		},
	})

	lookupCalls := 0
	handler.lookupAddr = func(_ context.Context, addr string) ([]string, error) {
		lookupCalls++
		if addr != "10.88.0.9" {
			t.Fatalf("lookup address = %q, want 10.88.0.9", addr)
		}
		if lookupCalls == 1 {
			return []string{"bedroom-tv-v1.local."}, nil
		}
		return []string{"bedroom-tv-v2.local."}, nil
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var first stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &first)
	if got, want := first.ClientStreams[0].ClientHost, "bedroom-tv-v1.local"; got != want {
		t.Fatalf("first payload client_host = %q, want %q", got, want)
	}

	now = now.Add(3 * time.Minute)
	var second stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &second)
	if got, want := second.ClientStreams[0].ClientHost, "bedroom-tv-v2.local"; got != want {
		t.Fatalf("second payload client_host = %q, want %q", got, want)
	}

	if lookupCalls != 2 {
		t.Fatalf("lookupCalls = %d, want 2 after TTL expiry", lookupCalls)
	}
}

func TestAdminRoutesTunerStatusResolveIPCacheDisabledWhenTTLZero(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.resolveClientHostCacheTTL = 0

	handler.SetTunerStatusProvider(&fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			ClientStreams: []stream.ClientStreamStatus{
				{
					SubscriberID: 54,
					ClientAddr:   "10.88.0.10:41000",
				},
			},
		},
	})

	lookupCalls := 0
	handler.lookupAddr = func(_ context.Context, addr string) ([]string, error) {
		lookupCalls++
		if addr != "10.88.0.10" {
			t.Fatalf("lookup address = %q, want 10.88.0.10", addr)
		}
		return []string{"garage-tv.local."}, nil
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var first stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &first)
	var second stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &second)

	if lookupCalls != 2 {
		t.Fatalf("lookupCalls = %d, want 2 when cache TTL is disabled", lookupCalls)
	}
	if got, want := first.ClientStreams[0].ClientHost, "garage-tv.local"; got != want {
		t.Fatalf("first payload client_host = %q, want %q", got, want)
	}
	if got, want := second.ClientStreams[0].ClientHost, "garage-tv.local"; got != want {
		t.Fatalf("second payload client_host = %q, want %q", got, want)
	}

	handler.resolveClientHostCacheMu.Lock()
	cacheLen := len(handler.resolveClientHostCache)
	handler.resolveClientHostCacheMu.Unlock()
	if cacheLen != 0 {
		t.Fatalf("cache length = %d, want 0 when cache TTL is disabled", cacheLen)
	}
}

func TestAdminRoutesTunerStatusResolveIPUsesShortNegativeCacheTTL(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.resolveClientHostCacheTTL = 2 * time.Minute
	handler.resolveClientHostCacheNegativeTTL = 5 * time.Second
	handler.resolveClientHostCacheSweepInterval = 0

	now := time.Unix(1_700_000_100, 0).UTC()
	handler.resolveClientHostCacheNow = func() time.Time {
		return now
	}

	handler.SetTunerStatusProvider(&fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			ClientStreams: []stream.ClientStreamStatus{
				{
					SubscriberID: 55,
					ClientAddr:   "10.88.0.11:41000",
				},
			},
		},
	})

	lookupCalls := 0
	handler.lookupAddr = func(_ context.Context, addr string) ([]string, error) {
		lookupCalls++
		if addr != "10.88.0.11" {
			t.Fatalf("lookup address = %q, want 10.88.0.11", addr)
		}
		if lookupCalls == 1 {
			return nil, context.DeadlineExceeded
		}
		return []string{"retry-host.local."}, nil
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var first stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &first)
	if got := first.ClientStreams[0].ClientHost; got != "" {
		t.Fatalf("first payload client_host = %q, want empty after initial lookup failure", got)
	}

	var second stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &second)
	if got := second.ClientStreams[0].ClientHost; got != "" {
		t.Fatalf("second payload client_host = %q, want empty while negative cache entry is fresh", got)
	}
	if lookupCalls != 1 {
		t.Fatalf("lookupCalls = %d, want 1 while negative cache entry is within short TTL", lookupCalls)
	}

	now = now.Add(6 * time.Second)
	var third stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &third)
	if got, want := third.ClientStreams[0].ClientHost, "retry-host.local"; got != want {
		t.Fatalf("third payload client_host = %q, want %q after short negative TTL expiry", got, want)
	}
	if lookupCalls != 2 {
		t.Fatalf("lookupCalls = %d, want 2 after negative cache TTL expiry", lookupCalls)
	}
}

func TestAdminRoutesTunerStatusResolveIPConcurrentRequests(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.resolveClientHostCacheTTL = 2 * time.Minute
	handler.resolveClientHostCacheNegativeTTL = 10 * time.Second
	handler.resolveClientHostCacheMaxEntries = 8

	handler.SetTunerStatusProvider(&fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			ClientStreams: []stream.ClientStreamStatus{
				{
					SubscriberID: 56,
					ClientAddr:   "10.88.0.12:41000",
				},
				{
					SubscriberID: 57,
					ClientAddr:   "10.88.0.13:41001",
				},
			},
		},
	})

	var lookupMu sync.Mutex
	lookupCalls := 0
	handler.lookupAddr = func(_ context.Context, addr string) ([]string, error) {
		time.Sleep(2 * time.Millisecond)
		lookupMu.Lock()
		lookupCalls++
		lookupMu.Unlock()

		switch addr {
		case "10.88.0.12":
			return []string{"tablet.local."}, nil
		case "10.88.0.13":
			return []string{"phone.local."}, nil
		default:
			return nil, fmt.Errorf("unexpected lookup ip %q", addr)
		}
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	const requestCount = 24
	errCh := make(chan string, requestCount)
	var wg sync.WaitGroup
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil)
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				errCh <- fmt.Sprintf("status = %d, want %d", rec.Code, http.StatusOK)
				return
			}

			var payload stream.TunerStatusSnapshot
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				errCh <- fmt.Sprintf("decode payload: %v", err)
				return
			}
			if len(payload.ClientStreams) != 2 {
				errCh <- fmt.Sprintf("len(payload.client_streams) = %d, want 2", len(payload.ClientStreams))
				return
			}
			if payload.ClientStreams[0].ClientHost == "" || payload.ClientStreams[1].ClientHost == "" {
				errCh <- "expected resolved client_host fields for both client_streams entries"
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for errMsg := range errCh {
		t.Fatal(errMsg)
	}

	lookupMu.Lock()
	gotLookupCalls := lookupCalls
	lookupMu.Unlock()
	if gotLookupCalls <= 0 {
		t.Fatalf("lookupCalls = %d, want > 0", gotLookupCalls)
	}

	handler.resolveClientHostCacheMu.Lock()
	cacheLen := len(handler.resolveClientHostCache)
	handler.resolveClientHostCacheMu.Unlock()
	if cacheLen <= 0 {
		t.Fatalf("cache length = %d, want > 0 after concurrent requests", cacheLen)
	}
	if handler.resolveClientHostCacheMaxEntries > 0 && cacheLen > handler.resolveClientHostCacheMaxEntries {
		t.Fatalf(
			"cache length = %d, want <= max entries %d",
			cacheLen,
			handler.resolveClientHostCacheMaxEntries,
		)
	}
}

func TestAdminResolveClientHostCacheCapacityAndCloseClear(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.resolveClientHostCacheTTL = 2 * time.Minute
	handler.resolveClientHostCacheMaxEntries = 2
	handler.resolveClientHostCacheSweepInterval = 0

	now := time.Unix(1_700_000_200, 0).UTC()
	handler.resolveClientHostCacheNow = func() time.Time {
		return now
	}

	handler.storeCachedResolvedClientHost("10.0.0.1", "one.local")
	now = now.Add(time.Second)
	handler.storeCachedResolvedClientHost("10.0.0.2", "two.local")
	now = now.Add(time.Second)
	handler.storeCachedResolvedClientHost("10.0.0.3", "three.local")

	if _, ok := handler.loadCachedResolvedClientHost("10.0.0.1"); ok {
		t.Fatal("10.0.0.1 cache entry should have been evicted at capacity")
	}
	if got, ok := handler.loadCachedResolvedClientHost("10.0.0.2"); !ok || got != "two.local" {
		t.Fatalf("10.0.0.2 cache entry = (%q, %v), want (two.local, true)", got, ok)
	}
	if got, ok := handler.loadCachedResolvedClientHost("10.0.0.3"); !ok || got != "three.local" {
		t.Fatalf("10.0.0.3 cache entry = (%q, %v), want (three.local, true)", got, ok)
	}

	handler.Close()

	handler.resolveClientHostCacheMu.Lock()
	cacheIsNil := handler.resolveClientHostCache == nil
	lastSweepZero := handler.resolveClientHostCacheLastSweep.IsZero()
	handler.resolveClientHostCacheMu.Unlock()
	if !cacheIsNil {
		t.Fatal("resolve client host cache should be cleared on Close()")
	}
	if !lastSweepZero {
		t.Fatal("resolve client host cache sweep timestamp should reset on Close()")
	}
}

func TestAdminResolveClientHostCacheSweepIntervalThrottleDefersExpiredSweep(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.resolveClientHostCacheTTL = 2 * time.Minute
	handler.resolveClientHostCacheSweepInterval = 30 * time.Second

	now := time.Unix(1_700_000_300, 0).UTC()
	handler.resolveClientHostCacheNow = func() time.Time {
		return now
	}

	handler.storeCachedResolvedClientHost("10.0.0.1", "expired.local")
	handler.storeCachedResolvedClientHost("10.0.0.2", "fresh.local")

	handler.resolveClientHostCacheMu.Lock()
	expiredEntry := handler.resolveClientHostCache["10.0.0.1"]
	expiredEntry.expiresAt = now.Add(-time.Second)
	handler.resolveClientHostCache["10.0.0.1"] = expiredEntry
	freshEntry := handler.resolveClientHostCache["10.0.0.2"]
	freshEntry.expiresAt = now.Add(time.Minute)
	handler.resolveClientHostCache["10.0.0.2"] = freshEntry
	handler.resolveClientHostCacheLastSweep = now
	handler.resolveClientHostCacheMu.Unlock()

	if got, ok := handler.loadCachedResolvedClientHost("10.0.0.2"); !ok || got != "fresh.local" {
		t.Fatalf("first load = (%q, %v), want (fresh.local, true)", got, ok)
	}
	handler.resolveClientHostCacheMu.Lock()
	_, expiredStillPresent := handler.resolveClientHostCache["10.0.0.1"]
	handler.resolveClientHostCacheMu.Unlock()
	if !expiredStillPresent {
		t.Fatal("expired cache entry should remain while sweep interval is throttled")
	}

	now = now.Add(31 * time.Second)
	if got, ok := handler.loadCachedResolvedClientHost("10.0.0.2"); !ok || got != "fresh.local" {
		t.Fatalf("second load = (%q, %v), want (fresh.local, true)", got, ok)
	}
	handler.resolveClientHostCacheMu.Lock()
	_, expiredStillPresent = handler.resolveClientHostCache["10.0.0.1"]
	handler.resolveClientHostCacheMu.Unlock()
	if expiredStillPresent {
		t.Fatal("expired cache entry should be removed after sweep interval elapses")
	}
}

func TestAdminResolveClientHostCacheCapacityEvictionIgnoresStaleHeapEntries(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.resolveClientHostCacheTTL = 2 * time.Minute
	handler.resolveClientHostCacheMaxEntries = 2
	handler.resolveClientHostCacheSweepInterval = 0

	now := time.Unix(1_700_000_500, 0).UTC()
	handler.resolveClientHostCacheNow = func() time.Time {
		return now
	}

	handler.storeCachedResolvedClientHost("10.0.0.1", "one-v1.local")
	now = now.Add(time.Second)
	handler.storeCachedResolvedClientHost("10.0.0.2", "two.local")
	now = now.Add(time.Second)
	handler.storeCachedResolvedClientHost("10.0.0.1", "one-v2.local")
	now = now.Add(time.Second)
	handler.storeCachedResolvedClientHost("10.0.0.3", "three.local")

	if _, ok := handler.loadCachedResolvedClientHost("10.0.0.2"); ok {
		t.Fatal("10.0.0.2 cache entry should be evicted as the oldest current expiry")
	}
	if got, ok := handler.loadCachedResolvedClientHost("10.0.0.1"); !ok || got != "one-v2.local" {
		t.Fatalf("10.0.0.1 cache entry = (%q, %v), want (one-v2.local, true)", got, ok)
	}
	if got, ok := handler.loadCachedResolvedClientHost("10.0.0.3"); !ok || got != "three.local" {
		t.Fatalf("10.0.0.3 cache entry = (%q, %v), want (three.local, true)", got, ok)
	}
}

func TestAdminRoutesTunerStatusResolveIPLogsCacheStats(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.resolveClientHostCacheTTL = 2 * time.Minute
	handler.resolveClientHostSummaryLogInterval = 10 * time.Second

	now := time.Unix(1_700_000_600, 0).UTC()
	handler.resolveClientHostSummaryNow = func() time.Time {
		return now
	}

	var logBuffer bytes.Buffer
	handler.SetLogger(slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelDebug})))

	handler.SetTunerStatusProvider(&fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			ClientStreams: []stream.ClientStreamStatus{
				{
					SubscriberID: 58,
					ClientAddr:   "10.88.0.14:41000",
				},
			},
		},
	})

	handler.lookupAddr = func(_ context.Context, addr string) ([]string, error) {
		if addr != "10.88.0.14" {
			return nil, fmt.Errorf("unexpected lookup ip %q", addr)
		}
		return []string{"office.local."}, nil
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var first stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &first)
	now = now.Add(5 * time.Second)
	var second stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &second)
	now = now.Add(6 * time.Second)
	var third stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &third)

	logText := logBuffer.String()
	if strings.Count(logText, "admin tuner resolve_ip summary") != 1 {
		t.Fatalf("logs = %q, want exactly one periodic resolve_ip summary event", logText)
	}
	if strings.Contains(logText, "admin tuner resolve_ip completed") {
		t.Fatalf("logs = %q, did not expect per-request resolve_ip completion event", logText)
	}
	if !strings.Contains(logText, "resolve_requests=3") {
		t.Fatalf("logs = %q, want resolve_requests=3 in periodic summary", logText)
	}
	if !strings.Contains(logText, "cache_hit_rate=") {
		t.Fatalf("logs = %q, want cache hit-rate field", logText)
	}
	if !strings.Contains(logText, "cache_hits=2") {
		t.Fatalf("logs = %q, want cache_hits=2 across aggregated summary window", logText)
	}
	if !strings.Contains(logText, "cache_misses=1") {
		t.Fatalf("logs = %q, want cache_misses=1 for initial cold-cache request", logText)
	}
	if !strings.Contains(logText, "lookup_calls=1") {
		t.Fatalf("logs = %q, want lookup_calls=1 across aggregated summary window", logText)
	}
}

func TestAdminRoutesTunerStatusResolveIPUsesTotalTimeoutCap(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.resolveClientHostsTimeout = 150 * time.Millisecond

	handler.SetTunerStatusProvider(&fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			ClientStreams: []stream.ClientStreamStatus{
				{
					SubscriberID: 61,
					ClientAddr:   "10.88.0.8:41000",
				},
			},
		},
	})

	lookupCalls := 0
	handler.lookupAddr = func(ctx context.Context, addr string) ([]string, error) {
		lookupCalls++
		if addr != "10.88.0.8" {
			t.Fatalf("lookup address = %q, want 10.88.0.8", addr)
		}
		deadline, hasDeadline := ctx.Deadline()
		if !hasDeadline {
			t.Fatal("lookup context missing deadline")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("lookup context deadline remaining = %s, want positive", remaining)
		}
		if remaining > 250*time.Millisecond {
			t.Fatalf("lookup context deadline remaining = %s, want <= 250ms", remaining)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	start := time.Now()
	var payload stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &payload)
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("resolve_ip request elapsed = %s, want <= 1s", elapsed)
	}
	if lookupCalls != 1 {
		t.Fatalf("lookupCalls = %d, want 1", lookupCalls)
	}
	if got := payload.ClientStreams[0].ClientHost; got != "" {
		t.Fatalf("payload.client_streams[0].client_host = %q, want empty after timeout", got)
	}
}

func TestAdminRoutesTunerStatusResolveIPMemoizesTimeoutLookupErrors(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	handler.SetTunerStatusProvider(&fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			ClientStreams: []stream.ClientStreamStatus{
				{
					SubscriberID: 71,
					ClientAddr:   "10.77.0.42:41000",
				},
				{
					SubscriberID: 72,
					ClientAddr:   "10.77.0.42:41001",
				},
			},
			SessionHistory: []stream.SharedSessionHistory{
				{
					SessionID: 9001,
					Subscribers: []stream.SharedSessionSubscriberHistory{
						{
							SubscriberID: 73,
							ClientAddr:   "10.77.0.42:41002",
						},
					},
				},
			},
		},
	})

	lookupCalls := 0
	handler.lookupAddr = func(_ context.Context, addr string) ([]string, error) {
		lookupCalls++
		if addr != "10.77.0.42" {
			t.Fatalf("lookup address = %q, want 10.77.0.42", addr)
		}
		return nil, context.DeadlineExceeded
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var payload stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=1", nil, http.StatusOK, &payload)

	if lookupCalls != 1 {
		t.Fatalf("lookupCalls = %d, want 1 after timeout memoization", lookupCalls)
	}
	if got := payload.ClientStreams[0].ClientHost; got != "" {
		t.Fatalf("payload.client_streams[0].client_host = %q, want empty after timeout", got)
	}
	if got := payload.ClientStreams[1].ClientHost; got != "" {
		t.Fatalf("payload.client_streams[1].client_host = %q, want empty after timeout", got)
	}
	if got := payload.SessionHistory[0].Subscribers[0].ClientHost; got != "" {
		t.Fatalf("payload.session_history[0].subscribers[0].client_host = %q, want empty after timeout", got)
	}
}

func TestAdminRoutesTunerStatusResolveIPDisabledByDefault(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	handler.SetTunerStatusProvider(&fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			ClientStreams: []stream.ClientStreamStatus{
				{
					SubscriberID: 21,
					ClientAddr:   "10.13.0.99:41000",
				},
			},
			SessionHistory: []stream.SharedSessionHistory{
				{
					SessionID: 8100,
					Subscribers: []stream.SharedSessionSubscriberHistory{
						{
							SubscriberID: 31,
							ClientAddr:   "10.13.0.99:41001",
						},
					},
				},
			},
		},
	})

	lookupCalls := 0
	handler.lookupAddr = func(_ context.Context, _ string) ([]string, error) {
		lookupCalls++
		return []string{"should-not-be-used.local."}, nil
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var payload stream.TunerStatusSnapshot
	doJSON(t, mux, http.MethodGet, "/api/admin/tuners", nil, http.StatusOK, &payload)

	if lookupCalls != 0 {
		t.Fatalf("lookup calls = %d, want 0 when resolve_ip is omitted", lookupCalls)
	}
	if got := payload.ClientStreams[0].ClientHost; got != "" {
		t.Fatalf("payload.client_streams[0].client_host = %q, want empty when resolve_ip is omitted", got)
	}
	if got := payload.SessionHistory[0].Subscribers[0].ClientHost; got != "" {
		t.Fatalf("payload.session_history[0].subscribers[0].client_host = %q, want empty when resolve_ip is omitted", got)
	}
}

func TestAdminRoutesTunerStatusResolveIPRejectsInvalidBoolean(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	handler.SetTunerStatusProvider(&fakeTunerStatusProvider{})
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodGet, "/api/admin/tuners?resolve_ip=banana", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid resolve_ip status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if !strings.Contains(rec.Body.String(), "resolve_ip must be a boolean") {
		t.Fatalf("invalid resolve_ip body = %q, want boolean validation error", rec.Body.String())
	}
}

func TestAdminRoutesTriggerTunerRecovery(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	provider := &fakeTunerStatusProvider{}
	handler.SetTunerStatusProvider(provider)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var resp struct {
		ChannelID      int64  `json:"channel_id"`
		RecoveryReason string `json:"recovery_reason"`
		Accepted       bool   `json:"accepted"`
	}
	doJSON(t, mux, http.MethodPost, "/api/admin/tuners/recovery", map[string]any{
		"channel_id": 42,
		"reason":     "ui_manual_trigger",
	}, http.StatusOK, &resp)

	if !resp.Accepted {
		t.Fatal("resp.accepted = false, want true")
	}
	if resp.ChannelID != 42 {
		t.Fatalf("resp.channel_id = %d, want 42", resp.ChannelID)
	}
	if resp.RecoveryReason != "ui_manual_trigger" {
		t.Fatalf("resp.recovery_reason = %q, want ui_manual_trigger", resp.RecoveryReason)
	}
	if provider.triggerCalls != 1 {
		t.Fatalf("provider.triggerCalls = %d, want 1", provider.triggerCalls)
	}
	if provider.lastTriggerChannelID != 42 {
		t.Fatalf("provider.lastTriggerChannelID = %d, want 42", provider.lastTriggerChannelID)
	}
	if provider.lastTriggerReason != "ui_manual_trigger" {
		t.Fatalf("provider.lastTriggerReason = %q, want ui_manual_trigger", provider.lastTriggerReason)
	}

	provider.resetTriggers()
	doJSON(t, mux, http.MethodPost, "/api/admin/tuners/recovery", map[string]any{
		"channel_id": 77,
	}, http.StatusOK, &resp)
	if provider.lastTriggerReason != "ui_manual_trigger" {
		t.Fatalf("provider.lastTriggerReason default = %q, want ui_manual_trigger", provider.lastTriggerReason)
	}
}

func TestAdminRoutesTriggerTunerRecoveryErrorPaths(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodPost, "/api/admin/tuners/recovery", map[string]any{"channel_id": 1})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing provider status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	handler.SetTunerStatusProvider(snapshotOnlyTunerStatusProvider{})
	mux = http.NewServeMux()
	handler.RegisterRoutes(mux, "")
	rec = doRaw(t, mux, http.MethodPost, "/api/admin/tuners/recovery", map[string]any{"channel_id": 1})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unsupported provider status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	provider := &fakeTunerStatusProvider{}
	handler.SetTunerStatusProvider(provider)
	mux = http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec = doRaw(t, mux, http.MethodPost, "/api/admin/tuners/recovery", map[string]any{"channel_id": 0})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid channel_id status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	provider.triggerErr = stream.ErrSessionNotFound
	rec = doRaw(t, mux, http.MethodPost, "/api/admin/tuners/recovery", map[string]any{"channel_id": 10})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("session not found status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	provider.triggerErr = stream.ErrSessionRecoveryAlreadyPending
	rec = doRaw(t, mux, http.MethodPost, "/api/admin/tuners/recovery", map[string]any{"channel_id": 10})
	if rec.Code != http.StatusConflict {
		t.Fatalf("session recovery pending status = %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestAdminRoutesDVRFlow(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	fakeDVR := &fakeDVRService{
		state: dvr.ConfigState{
			Instance: dvr.InstanceConfig{
				ID:              1,
				Provider:        dvr.ProviderChannels,
				BaseURL:         "http://channels.lan:8089",
				DefaultLineupID: "USA-MN22577-X",
				SyncMode:        dvr.SyncModeConfiguredOnly,
			},
		},
		testResult: dvr.TestResult{
			Reachable:              true,
			Provider:               "channels",
			BaseURL:                "http://channels.lan:8089",
			DeviceChannelCount:     18,
			FilteredChannelCount:   18,
			HDHRDeviceID:           "8F07FDC6",
			HDHRDeviceFilterActive: true,
		},
		lineups: []dvr.DVRLineup{
			{ID: "USA-MN22577-X", Name: "USA-MN22577-X"},
		},
		syncResult: dvr.SyncResult{
			DryRun:       true,
			Provider:     "channels",
			BaseURL:      "http://channels.lan:8089",
			SyncMode:     dvr.SyncModeConfiguredOnly,
			UpdatedCount: 2,
			PatchPreview: map[string]map[string]string{
				"USA-MN22577-X": {"100": "21234"},
			},
		},
		reverseSyncResult: dvr.ReverseSyncResult{
			DryRun:         false,
			Provider:       "channels",
			BaseURL:        "http://channels.lan:8089",
			LineupID:       "USA-MN22577-X",
			CandidateCount: 2,
			ImportedCount:  2,
		},
		reverseChannelResult: dvr.ReverseSyncResult{
			DryRun:         false,
			Provider:       "channels",
			BaseURL:        "http://channels.lan:8089",
			LineupID:       "USA-MN22577-X",
			CandidateCount: 1,
			ImportedCount:  1,
		},
		channelMapping: dvr.ChannelMapping{
			ChannelID:        123,
			GuideNumber:      "100",
			GuideName:        "CBS",
			Enabled:          true,
			DVRInstanceID:    1,
			DVRLineupID:      "USA-MN22577-X",
			DVRLineupChannel: "2",
			DVRStationRef:    "21234",
		},
		channelMappings: []dvr.ChannelMapping{
			{
				ChannelID:        123,
				GuideNumber:      "100",
				GuideName:        "CBS",
				Enabled:          true,
				DVRInstanceID:    1,
				DVRLineupID:      "USA-MN22577-X",
				DVRLineupChannel: "2",
				DVRStationRef:    "21234",
			},
			{
				ChannelID:        124,
				GuideNumber:      "101",
				GuideName:        "NBC",
				Enabled:          true,
				DVRInstanceID:    1,
				DVRLineupID:      "USA-MN22577-X",
				DVRLineupChannel: "4",
				DVRStationRef:    "30001",
			},
		},
	}
	handler.SetDVRService(fakeDVR)
	fakeScheduler := &fakeDVRScheduler{}
	handler.SetDVRScheduler(fakeScheduler)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	dvrUIRec := httptest.NewRecorder()
	dvrUIReq := httptest.NewRequest(http.MethodGet, "/ui/dvr", nil)
	mux.ServeHTTP(dvrUIRec, dvrUIReq)
	if dvrUIRec.Code != http.StatusOK {
		t.Fatalf("GET /ui/dvr status = %d, want %d", dvrUIRec.Code, http.StatusOK)
	}

	var state dvr.ConfigState
	doJSON(t, mux, http.MethodGet, "/api/admin/dvr", nil, http.StatusOK, &state)
	if state.Instance.BaseURL != "http://channels.lan:8089" {
		t.Fatalf("state.instance.base_url = %q, want http://channels.lan:8089", state.Instance.BaseURL)
	}

	doJSON(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
		"base_url":     "http://example.invalid:8089",
		"sync_mode":    "mirror_device",
		"sync_enabled": true,
		"sync_cron":    "*/15 * * * *",
	}, http.StatusOK, &state)
	if state.Instance.BaseURL != "http://example.invalid:8089" {
		t.Fatalf("updated base_url = %q, want http://example.invalid:8089", state.Instance.BaseURL)
	}
	if !fakeScheduler.updated {
		t.Fatal("expected DVR scheduler update to be invoked")
	}
	if fakeScheduler.jobName != "dvr_lineup_sync" {
		t.Fatalf("scheduler jobName = %q, want dvr_lineup_sync", fakeScheduler.jobName)
	}
	if !fakeScheduler.enabled {
		t.Fatal("scheduler enabled = false, want true")
	}
	if fakeScheduler.cronSpec != "*/15 * * * *" {
		t.Fatalf("scheduler cronSpec = %q, want */15 * * * *", fakeScheduler.cronSpec)
	}

	var testResult dvr.TestResult
	doJSON(t, mux, http.MethodPost, "/api/admin/dvr/test", nil, http.StatusOK, &testResult)
	if !testResult.Reachable {
		t.Fatal("testResult.reachable = false, want true")
	}

	var lineupResp struct {
		Lineups []dvr.DVRLineup `json:"lineups"`
		Refresh bool            `json:"refresh"`
	}
	doJSON(t, mux, http.MethodGet, "/api/admin/dvr/lineups?refresh=1", nil, http.StatusOK, &lineupResp)
	if !lineupResp.Refresh {
		t.Fatal("lineupResp.refresh = false, want true")
	}
	if len(lineupResp.Lineups) != 1 {
		t.Fatalf("len(lineupResp.lineups) = %d, want 1", len(lineupResp.Lineups))
	}

	var syncResult dvr.SyncResult
	doJSON(t, mux, http.MethodPost, "/api/admin/dvr/sync", map[string]any{"dry_run": true}, http.StatusOK, &syncResult)
	if !syncResult.DryRun {
		t.Fatal("syncResult.dry_run = false, want true")
	}
	if fakeDVR.lastSyncIncludeDynamic {
		t.Fatal("sync include_dynamic default = true, want false")
	}
	doJSON(t, mux, http.MethodPost, "/api/admin/dvr/sync", map[string]any{"dry_run": true, "include_dynamic": true}, http.StatusOK, &syncResult)
	if !fakeDVR.lastSyncIncludeDynamic {
		t.Fatal("sync include_dynamic = false, want true")
	}

	var reverseResult dvr.ReverseSyncResult
	doJSON(t, mux, http.MethodPost, "/api/admin/dvr/reverse-sync", map[string]any{
		"lineup_id": "USA-MN22577-X",
	}, http.StatusOK, &reverseResult)
	if reverseResult.ImportedCount != 2 {
		t.Fatalf("reverseResult.imported_count = %d, want 2", reverseResult.ImportedCount)
	}
	if fakeDVR.lastReverseIncludeDynamic {
		t.Fatal("reverse sync include_dynamic default = true, want false")
	}
	doJSON(t, mux, http.MethodPost, "/api/admin/dvr/reverse-sync", map[string]any{
		"lineup_id":       "USA-MN22577-X",
		"include_dynamic": true,
	}, http.StatusOK, &reverseResult)
	if !fakeDVR.lastReverseIncludeDynamic {
		t.Fatal("reverse sync include_dynamic = false, want true")
	}

	var channelMapping dvr.ChannelMapping
	doJSON(t, mux, http.MethodGet, "/api/channels/123/dvr", nil, http.StatusOK, &channelMapping)
	if channelMapping.DVRLineupChannel != "2" {
		t.Fatalf("channelMapping.dvr_lineup_channel = %q, want 2", channelMapping.DVRLineupChannel)
	}

	var channelMappingsResp struct {
		Mappings       []dvr.ChannelMapping `json:"mappings"`
		Total          int                  `json:"total"`
		Limit          int                  `json:"limit"`
		Offset         int                  `json:"offset"`
		EnabledOnly    bool                 `json:"enabled_only"`
		IncludeDynamic bool                 `json:"include_dynamic"`
	}
	doJSON(t, mux, http.MethodGet, "/api/channels/dvr", nil, http.StatusOK, &channelMappingsResp)
	if len(channelMappingsResp.Mappings) != 2 {
		t.Fatalf("len(channelMappingsResp.mappings) = %d, want 2", len(channelMappingsResp.Mappings))
	}
	if channelMappingsResp.Mappings[1].ChannelID != 124 {
		t.Fatalf("channelMappingsResp.mappings[1].channel_id = %d, want 124", channelMappingsResp.Mappings[1].ChannelID)
	}
	if channelMappingsResp.EnabledOnly {
		t.Fatal("channelMappingsResp.enabled_only = true, want false")
	}
	if channelMappingsResp.IncludeDynamic {
		t.Fatal("channelMappingsResp.include_dynamic = true, want false")
	}
	if channelMappingsResp.Total != 2 {
		t.Fatalf("channelMappingsResp.total = %d, want 2", channelMappingsResp.Total)
	}
	if channelMappingsResp.Limit != defaultDVRChannelMappingsListLimit {
		t.Fatalf("channelMappingsResp.limit = %d, want %d", channelMappingsResp.Limit, defaultDVRChannelMappingsListLimit)
	}
	if channelMappingsResp.Offset != 0 {
		t.Fatalf("channelMappingsResp.offset = %d, want 0", channelMappingsResp.Offset)
	}
	if fakeDVR.lastListIncludeDynamic {
		t.Fatal("ListChannelMappings include_dynamic default = true, want false")
	}
	if fakeDVR.lastListLimit != defaultDVRChannelMappingsListLimit {
		t.Fatalf("ListChannelMappings default limit = %d, want %d", fakeDVR.lastListLimit, defaultDVRChannelMappingsListLimit)
	}
	if fakeDVR.lastListOffset != 0 {
		t.Fatalf("ListChannelMappings default offset = %d, want 0", fakeDVR.lastListOffset)
	}
	doJSON(t, mux, http.MethodGet, "/api/channels/dvr?include_dynamic=1", nil, http.StatusOK, &channelMappingsResp)
	if !channelMappingsResp.IncludeDynamic {
		t.Fatal("channelMappingsResp.include_dynamic = false, want true")
	}
	if !fakeDVR.lastListIncludeDynamic {
		t.Fatal("ListChannelMappings include_dynamic = false, want true")
	}

	doJSON(t, mux, http.MethodPut, "/api/channels/123/dvr", map[string]any{
		"dvr_lineup_id":      "USA-MN22577-X",
		"dvr_lineup_channel": "1740",
		"dvr_station_ref":    "97047",
		"dvr_callsign_hint":  "TNCKHD",
	}, http.StatusOK, &channelMapping)
	if channelMapping.DVRLineupChannel != "1740" {
		t.Fatalf("updated channelMapping.dvr_lineup_channel = %q, want 1740", channelMapping.DVRLineupChannel)
	}

	doJSON(t, mux, http.MethodPost, "/api/channels/123/dvr/reverse-sync", map[string]any{
		"lineup_id": "USA-MN22577-X",
	}, http.StatusOK, &reverseResult)
	if reverseResult.CandidateCount != 1 {
		t.Fatalf("per-channel reverse candidate_count = %d, want 1", reverseResult.CandidateCount)
	}
}

func TestAdminRoutesDVRJellyfinTokenWriteOnly(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	fakeDVR := &fakeDVRService{
		state: dvr.ConfigState{
			Instance: dvr.InstanceConfig{
				ID:       1,
				Provider: dvr.ProviderChannels,
				BaseURL:  "http://channels.lan:8089",
			},
		},
	}
	handler.SetDVRService(fakeDVR)

	var logBuffer bytes.Buffer
	handler.SetLogger(slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelInfo})))

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	const token = "jellyfin-super-secret-token"
	var updated dvr.ConfigState
	doJSON(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
		"provider":                 "channels",
		"active_providers":         []string{"channels", "jellyfin"},
		"base_url":                 "http://channels.lan:8089",
		"channels_base_url":        "http://channels.lan:8089",
		"jellyfin_base_url":        "http://jellyfin.lan:8096",
		"jellyfin_api_token":       token,
		"jellyfin_tuner_host_id":   "db45abff86f1417ea5ff462aee703e24",
		"default_lineup_id":        "",
		"sync_mode":                "configured_only",
		"pre_sync_refresh_devices": false,
	}, http.StatusOK, &updated)

	if updated.Instance.Provider != dvr.ProviderChannels {
		t.Fatalf("updated provider = %q, want channels", updated.Instance.Provider)
	}
	if updated.Instance.JellyfinAPIToken != "" {
		t.Fatalf("updated jellyfin_api_token = %q, want empty", updated.Instance.JellyfinAPIToken)
	}
	if !updated.Instance.JellyfinAPITokenConfigured {
		t.Fatal("updated jellyfin_api_token_configured = false, want true")
	}
	if got, want := updated.Instance.JellyfinTunerHostID, "db45abff86f1417ea5ff462aee703e24"; got != want {
		t.Fatalf("updated jellyfin_tuner_host_id = %q, want %q", got, want)
	}
	if got, want := updated.Instance.ChannelsBaseURL, "http://channels.lan:8089"; got != want {
		t.Fatalf("updated channels_base_url = %q, want %q", got, want)
	}
	if got, want := updated.Instance.JellyfinBaseURL, "http://jellyfin.lan:8096"; got != want {
		t.Fatalf("updated jellyfin_base_url = %q, want %q", got, want)
	}
	if got, want := updated.Instance.ActiveProviders, []dvr.ProviderType{dvr.ProviderChannels, dvr.ProviderJellyfin}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("updated active_providers = %v, want %v", got, want)
	}
	if got := fakeDVR.state.Instance.JellyfinAPIToken; got != token {
		t.Fatalf("persisted jellyfin token = %q, want %q", got, token)
	}

	var state dvr.ConfigState
	doJSON(t, mux, http.MethodGet, "/api/admin/dvr", nil, http.StatusOK, &state)
	if state.Instance.JellyfinAPIToken != "" {
		t.Fatalf("GET /api/admin/dvr jellyfin_api_token = %q, want empty", state.Instance.JellyfinAPIToken)
	}
	if !state.Instance.JellyfinAPITokenConfigured {
		t.Fatal("GET /api/admin/dvr jellyfin_api_token_configured = false, want true")
	}

	logText := logBuffer.String()
	if strings.Contains(logText, token) {
		t.Fatalf("admin mutation logs leaked jellyfin token: %q", logText)
	}
}

func TestAdminRoutesDVRPutPreservesStoredJellyfinTokenWhenOmitted(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	const token = "persisted-token-value"
	fakeDVR := &fakeDVRService{
		state: dvr.ConfigState{
			Instance: dvr.InstanceConfig{
				ID:                         1,
				Provider:                   dvr.ProviderChannels,
				BaseURL:                    "http://channels.lan:8089",
				ChannelsBaseURL:            "http://channels.lan:8089",
				JellyfinBaseURL:            "http://jellyfin.lan:8096",
				JellyfinAPIToken:           token,
				JellyfinAPITokenConfigured: true,
			},
		},
	}
	handler.SetDVRService(fakeDVR)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var updated dvr.ConfigState
	doJSON(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
		"sync_enabled": true,
		"sync_cron":    "*/30 * * * *",
	}, http.StatusOK, &updated)

	if got := fakeDVR.state.Instance.JellyfinAPIToken; got != token {
		t.Fatalf("persisted jellyfin token after partial update = %q, want %q", got, token)
	}
	if updated.Instance.JellyfinAPIToken != "" {
		t.Fatalf("updated response jellyfin_api_token = %q, want empty", updated.Instance.JellyfinAPIToken)
	}
	if !updated.Instance.JellyfinAPITokenConfigured {
		t.Fatal("updated response jellyfin_api_token_configured = false, want true")
	}
}

func TestAdminRoutesDVRPutRejectsUnsupportedPrimaryProvider(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	fakeDVR := &fakeDVRService{
		state: dvr.ConfigState{
			Instance: dvr.InstanceConfig{
				ID:       1,
				Provider: dvr.ProviderChannels,
				BaseURL:  "http://channels.lan:8089",
			},
		},
	}
	handler.SetDVRService(fakeDVR)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
		"provider":          "jellyfin",
		"jellyfin_base_url": "http://jellyfin.lan:8096",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT /api/admin/dvr status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `only "channels" is supported for sync/mapping`) {
		t.Fatalf("PUT /api/admin/dvr body = %q, want primary provider validation message", rec.Body.String())
	}
	if fakeDVR.updateCalls != 0 {
		t.Fatalf("UpdateConfig called %d times, want 0", fakeDVR.updateCalls)
	}
}

func TestAdminRoutesDVRPutRejectsLegacyPrimaryProviderWhenProviderOmitted(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	fakeDVR := &fakeDVRService{
		state: dvr.ConfigState{
			Instance: dvr.InstanceConfig{
				ID:       1,
				Provider: dvr.ProviderJellyfin,
				BaseURL:  "http://jellyfin.lan:8096",
			},
		},
	}
	handler.SetDVRService(fakeDVR)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
		"sync_enabled": true,
		"sync_cron":    "*/30 * * * *",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT /api/admin/dvr status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `invalid primary provider "jellyfin"`) {
		t.Fatalf("PUT /api/admin/dvr body = %q, want jellyfin primary-provider rejection", rec.Body.String())
	}
	if fakeDVR.updateCalls != 0 {
		t.Fatalf("UpdateConfig called %d times, want 0", fakeDVR.updateCalls)
	}
}

func TestAdminRoutesDVRPutLogsSanitizedBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		want    string
		blocked []string
	}{
		{
			name:    "credentialed URL strips userinfo query and fragment",
			baseURL: "https://user:pass@channels.example:8089/path?token=abc#frag",
			want:    "https://channels.example:8089/path",
			blocked: []string{"user:pass@", "token=abc", "#frag"},
		},
		{
			name:    "malformed query URL falls back to deterministic sanitization",
			baseURL: "http://user:secret@example.org/live/index.m3u8?sig=%ZZ#frag",
			want:    "http://example.org/live/index.m3u8",
			blocked: []string{"user:secret@", "sig=%ZZ", "#frag"},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			store, err := sqlite.Open(":memory:")
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			defer store.Close()

			channelsSvc := channels.NewService(store)
			handler, err := NewAdminHandler(store, channelsSvc)
			if err != nil {
				t.Fatalf("NewAdminHandler() error = %v", err)
			}

			fakeDVR := &fakeDVRService{
				state: dvr.ConfigState{
					Instance: dvr.InstanceConfig{
						ID:       1,
						Provider: dvr.ProviderChannels,
						BaseURL:  "http://channels.lan:8089",
					},
				},
			}
			handler.SetDVRService(fakeDVR)

			var logBuffer bytes.Buffer
			handler.SetLogger(slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelInfo})))

			mux := http.NewServeMux()
			handler.RegisterRoutes(mux, "")

			var updated dvr.ConfigState
			doJSON(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
				"base_url": tc.baseURL,
			}, http.StatusOK, &updated)

			if updated.Instance.BaseURL != tc.baseURL {
				t.Fatalf("updated base_url = %q, want %q", updated.Instance.BaseURL, tc.baseURL)
			}

			logText := logBuffer.String()
			if !strings.Contains(logText, "admin dvr config updated") {
				t.Fatalf("expected dvr mutation log event, got %q", logText)
			}
			if !strings.Contains(logText, "base_url="+tc.want) {
				t.Fatalf("expected sanitized base_url %q in logs, got %q", tc.want, logText)
			}
			for _, blocked := range tc.blocked {
				if blocked != "" && strings.Contains(logText, blocked) {
					t.Fatalf("mutation log leaked %q in %q", blocked, logText)
				}
			}
		})
	}
}

func TestSanitizeAdminMutationFieldsURLAwareRedaction(t *testing.T) {
	fields := []any{
		"base_url", "https://user:pass@example.test/dvr/path?token=abc#frag",
		"sync_mode", "mirror_device",
		"duration_ms", 42,
		"url", "http://user:secret@example.org/live/index.m3u8?sig=%ZZ#frag",
	}

	sanitized := sanitizeAdminMutationFields(fields)
	if got, want := len(sanitized), len(fields); got != want {
		t.Fatalf("len(sanitized fields) = %d, want %d", got, want)
	}

	if got, want := sanitized[1], "https://example.test/dvr/path"; got != want {
		t.Fatalf("sanitized base_url = %#v, want %#v", got, want)
	}
	if got, want := sanitized[3], "mirror_device"; got != want {
		t.Fatalf("sync_mode value = %#v, want %#v", got, want)
	}
	if got, want := sanitized[5], 42; got != want {
		t.Fatalf("duration_ms value = %#v, want %#v", got, want)
	}
	if got, want := sanitized[7], "http://example.org/live/index.m3u8"; got != want {
		t.Fatalf("sanitized url = %#v, want %#v", got, want)
	}

	if got, want := fields[1], "https://user:pass@example.test/dvr/path?token=abc#frag"; got != want {
		t.Fatalf("original fields mutated at base_url: got %#v, want %#v", got, want)
	}
	if got, want := fields[7], "http://user:secret@example.org/live/index.m3u8?sig=%ZZ#frag"; got != want {
		t.Fatalf("original fields mutated at url: got %#v, want %#v", got, want)
	}
}

func TestAdminRoutesDVRSyncChunkedOptionalBodyHonored(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	fakeDVR := &fakeDVRService{
		syncResult: dvr.SyncResult{
			Provider: "channels",
			BaseURL:  "http://channels.lan:8089",
		},
		reverseSyncResult: dvr.ReverseSyncResult{
			Provider: "channels",
			BaseURL:  "http://channels.lan:8089",
			LineupID: "DEFAULT-LINEUP",
		},
		reverseChannelResult: dvr.ReverseSyncResult{
			Provider: "channels",
			BaseURL:  "http://channels.lan:8089",
			LineupID: "DEFAULT-LINEUP",
		},
	}
	handler.SetDVRService(fakeDVR)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var defaultSync dvr.SyncResult
	rec := doRawChunkedBody(t, mux, http.MethodPost, "/api/admin/dvr/sync", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/admin/dvr/sync chunked empty body status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &defaultSync); err != nil {
		t.Fatalf("decode chunked empty /api/admin/dvr/sync response: %v", err)
	}
	if defaultSync.DryRun {
		t.Fatal("chunked empty /api/admin/dvr/sync dry_run = true, want false")
	}

	var syncResult dvr.SyncResult
	doChunkedJSON(t, mux, http.MethodPost, "/api/admin/dvr/sync", map[string]any{
		"dry_run": true,
	}, http.StatusOK, &syncResult)
	if !syncResult.DryRun {
		t.Fatal("chunked /api/admin/dvr/sync dry_run = false, want true")
	}

	const lineupID = "USA-MN22577-X"

	var reverseResult dvr.ReverseSyncResult
	doChunkedJSON(t, mux, http.MethodPost, "/api/admin/dvr/reverse-sync", map[string]any{
		"dry_run":   true,
		"lineup_id": lineupID,
	}, http.StatusOK, &reverseResult)
	if !reverseResult.DryRun {
		t.Fatal("chunked /api/admin/dvr/reverse-sync dry_run = false, want true")
	}
	if reverseResult.LineupID != lineupID {
		t.Fatalf("chunked /api/admin/dvr/reverse-sync lineup_id = %q, want %q", reverseResult.LineupID, lineupID)
	}

	var channelReverseResult dvr.ReverseSyncResult
	doChunkedJSON(t, mux, http.MethodPost, "/api/channels/123/dvr/reverse-sync", map[string]any{
		"dry_run":   true,
		"lineup_id": lineupID,
	}, http.StatusOK, &channelReverseResult)
	if !channelReverseResult.DryRun {
		t.Fatal("chunked /api/channels/{channelID}/dvr/reverse-sync dry_run = false, want true")
	}
	if channelReverseResult.LineupID != lineupID {
		t.Fatalf(
			"chunked /api/channels/{channelID}/dvr/reverse-sync lineup_id = %q, want %q",
			channelReverseResult.LineupID,
			lineupID,
		)
	}
}

func TestAdminRoutesDVRMappingsPaginationAndValidation(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	fakeDVR := &fakeDVRService{
		channelMappings: []dvr.ChannelMapping{
			{ChannelID: 11, GuideNumber: "101", GuideName: "Trad Enabled", Enabled: true, DVRInstanceID: 1},
			{ChannelID: 12, GuideNumber: "102", GuideName: "Trad Disabled", Enabled: false, DVRInstanceID: 1},
			{ChannelID: 13, GuideNumber: "10000", GuideName: "Dynamic Enabled", Enabled: true, DVRInstanceID: 1},
		},
	}
	handler.SetDVRService(fakeDVR)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	type mappingsResp struct {
		Mappings       []dvr.ChannelMapping `json:"mappings"`
		Total          int                  `json:"total"`
		Limit          int                  `json:"limit"`
		Offset         int                  `json:"offset"`
		EnabledOnly    bool                 `json:"enabled_only"`
		IncludeDynamic bool                 `json:"include_dynamic"`
	}

	var defaultResp mappingsResp
	doJSON(t, mux, http.MethodGet, "/api/channels/dvr", nil, http.StatusOK, &defaultResp)
	if got, want := len(defaultResp.Mappings), 2; got != want {
		t.Fatalf("default len(mappings) = %d, want %d", got, want)
	}
	if got, want := defaultResp.Total, 2; got != want {
		t.Fatalf("default total = %d, want %d", got, want)
	}
	if got, want := defaultResp.Limit, defaultDVRChannelMappingsListLimit; got != want {
		t.Fatalf("default limit = %d, want %d", got, want)
	}
	if defaultResp.Offset != 0 {
		t.Fatalf("default offset = %d, want 0", defaultResp.Offset)
	}
	if defaultResp.EnabledOnly {
		t.Fatal("default enabled_only = true, want false")
	}
	if defaultResp.IncludeDynamic {
		t.Fatal("default include_dynamic = true, want false")
	}
	if fakeDVR.lastListEnabledOnly {
		t.Fatal("ListChannelMappings default enabled_only = true, want false")
	}
	if fakeDVR.lastListIncludeDynamic {
		t.Fatal("ListChannelMappings default include_dynamic = true, want false")
	}

	var enabledOnlyResp mappingsResp
	doJSON(t, mux, http.MethodGet, "/api/channels/dvr?enabled_only=1", nil, http.StatusOK, &enabledOnlyResp)
	if got, want := len(enabledOnlyResp.Mappings), 1; got != want {
		t.Fatalf("enabled_only len(mappings) = %d, want %d", got, want)
	}
	if got, want := enabledOnlyResp.Total, 1; got != want {
		t.Fatalf("enabled_only total = %d, want %d", got, want)
	}
	if !enabledOnlyResp.EnabledOnly {
		t.Fatal("enabled_only response flag = false, want true")
	}

	var includeDynamicResp mappingsResp
	doJSON(t, mux, http.MethodGet, "/api/channels/dvr?enabled_only=1&include_dynamic=1", nil, http.StatusOK, &includeDynamicResp)
	if got, want := len(includeDynamicResp.Mappings), 2; got != want {
		t.Fatalf("enabled_only+include_dynamic len(mappings) = %d, want %d", got, want)
	}
	if got, want := includeDynamicResp.Total, 2; got != want {
		t.Fatalf("enabled_only+include_dynamic total = %d, want %d", got, want)
	}
	if !includeDynamicResp.EnabledOnly || !includeDynamicResp.IncludeDynamic {
		t.Fatalf(
			"enabled_only/include_dynamic flags = %t/%t, want true/true",
			includeDynamicResp.EnabledOnly,
			includeDynamicResp.IncludeDynamic,
		)
	}

	var pagedResp mappingsResp
	doJSON(t, mux, http.MethodGet, "/api/channels/dvr?include_dynamic=1&limit=1&offset=1", nil, http.StatusOK, &pagedResp)
	if got, want := len(pagedResp.Mappings), 1; got != want {
		t.Fatalf("paged len(mappings) = %d, want %d", got, want)
	}
	if got, want := pagedResp.Total, 3; got != want {
		t.Fatalf("paged total = %d, want %d", got, want)
	}
	if got, want := pagedResp.Limit, 1; got != want {
		t.Fatalf("paged limit = %d, want %d", got, want)
	}
	if got, want := pagedResp.Offset, 1; got != want {
		t.Fatalf("paged offset = %d, want %d", got, want)
	}
	if got, want := pagedResp.Mappings[0].ChannelID, int64(12); got != want {
		t.Fatalf("paged first channel_id = %d, want %d", got, want)
	}

	var zeroLimitResp mappingsResp
	doJSON(t, mux, http.MethodGet, "/api/channels/dvr?include_dynamic=1&limit=0", nil, http.StatusOK, &zeroLimitResp)
	if got, want := zeroLimitResp.Limit, defaultDVRChannelMappingsListLimit; got != want {
		t.Fatalf("limit=0 normalized limit = %d, want %d", got, want)
	}
	if got, want := zeroLimitResp.Total, 3; got != want {
		t.Fatalf("limit=0 total = %d, want %d", got, want)
	}

	var clampedResp mappingsResp
	doJSON(t, mux, http.MethodGet, "/api/channels/dvr?include_dynamic=1&limit=99999", nil, http.StatusOK, &clampedResp)
	if got, want := clampedResp.Limit, maxDVRChannelMappingsListLimit; got != want {
		t.Fatalf("clamped limit = %d, want %d", got, want)
	}

	invalidLimit := doRaw(t, mux, http.MethodGet, "/api/channels/dvr?limit=nope", nil)
	if invalidLimit.Code != http.StatusBadRequest {
		t.Fatalf("invalid limit status = %d, want %d", invalidLimit.Code, http.StatusBadRequest)
	}

	negativeOffset := doRaw(t, mux, http.MethodGet, "/api/channels/dvr?offset=-1", nil)
	if negativeOffset.Code != http.StatusBadRequest {
		t.Fatalf("negative offset status = %d, want %d", negativeOffset.Code, http.StatusBadRequest)
	}
}

func TestAdminRoutesDVRMappingsDefaultPaginationBoundsHighCardinality(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	const totalMappings = 450
	channelMappings := make([]dvr.ChannelMapping, 0, totalMappings)
	for i := 0; i < totalMappings; i++ {
		channelMappings = append(channelMappings, dvr.ChannelMapping{
			ChannelID:        int64(i + 1),
			GuideNumber:      strconv.Itoa(100 + i),
			GuideName:        fmt.Sprintf("Mapping %d", i+1),
			Enabled:          true,
			DVRInstanceID:    1,
			DVRLineupID:      "USA-BENCH",
			DVRLineupChannel: strconv.Itoa(100 + i),
		})
	}
	handler.SetDVRService(&fakeDVRService{channelMappings: channelMappings})

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var resp struct {
		Mappings []dvr.ChannelMapping `json:"mappings"`
		Total    int                  `json:"total"`
		Limit    int                  `json:"limit"`
		Offset   int                  `json:"offset"`
	}
	doJSON(t, mux, http.MethodGet, "/api/channels/dvr", nil, http.StatusOK, &resp)

	if got, want := resp.Total, totalMappings; got != want {
		t.Fatalf("response total = %d, want %d", got, want)
	}
	if got, want := resp.Limit, defaultDVRChannelMappingsListLimit; got != want {
		t.Fatalf("response limit = %d, want %d", got, want)
	}
	if got, want := resp.Offset, 0; got != want {
		t.Fatalf("response offset = %d, want %d", got, want)
	}
	if got, want := len(resp.Mappings), defaultDVRChannelMappingsListLimit; got != want {
		t.Fatalf("len(response mappings) = %d, want %d", got, want)
	}
	if got, want := resp.Mappings[0].ChannelID, int64(1); got != want {
		t.Fatalf("first channel_id = %d, want %d", got, want)
	}
	if got, want := resp.Mappings[len(resp.Mappings)-1].ChannelID, int64(defaultDVRChannelMappingsListLimit); got != want {
		t.Fatalf("last channel_id on default page = %d, want %d", got, want)
	}
}

func TestAdminRoutesDVRSyncChunkedInvalidJSONReturnsBadRequest(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.SetDVRService(&fakeDVRService{})

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRawChunkedBody(t, mux, http.MethodPost, "/api/admin/dvr/sync", `{"dry_run":`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST /api/admin/dvr/sync chunked malformed body status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestAdminRoutesDVRSyncDetachedFromClientCancellation(t *testing.T) {
	fakeDVR := &fakeDVRService{
		syncResult: dvr.SyncResult{
			Provider: "channels",
			BaseURL:  "http://channels.lan:8089",
		},
		syncDelay:     250 * time.Millisecond,
		syncStartedCh: make(chan struct{}, 1),
		syncDoneCh:    make(chan struct{}, 1),
		syncCtxErrCh:  make(chan error, 1),
	}
	server := newAdminDVRHTTPServer(t, fakeDVR)

	reqCtx, cancelReq := context.WithCancel(context.Background())
	defer cancelReq()

	req, err := http.NewRequestWithContext(
		reqCtx,
		http.MethodPost,
		server.URL+"/api/admin/dvr/sync",
		strings.NewReader(`{"dry_run":true}`),
	)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	responseErrCh := make(chan error, 1)
	go func() {
		resp, reqErr := server.Client().Do(req)
		if reqErr == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		responseErrCh <- reqErr
	}()

	select {
	case <-fakeDVR.syncStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for DVR sync start")
	}

	cancelReq()

	select {
	case reqErr := <-responseErrCh:
		if reqErr == nil {
			t.Fatal("request error = nil, want cancellation error after client cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for canceled client request result")
	}

	select {
	case <-fakeDVR.syncDoneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for detached DVR sync completion")
	}

	select {
	case ctxErr := <-fakeDVR.syncCtxErrCh:
		if ctxErr != nil {
			t.Fatalf("sync context error after client cancellation = %v, want nil", ctxErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DVR sync context observation")
	}
}

func TestAdminRoutesDVRSyncUsesRunnerOverlapLockDomainWhenAutomationConfigured(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	runner, err := jobs.NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}
	defer runner.Close()

	schedulerSvc, err := scheduler.New(store, nil)
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}

	handler, err := NewAdminHandler(store, channelsSvc, AutomationDeps{
		Settings:  store,
		Scheduler: schedulerSvc,
		Runner:    runner,
		JobFuncs: map[string]jobs.JobFunc{
			jobs.JobPlaylistSync:   func(context.Context, *jobs.RunContext) error { return nil },
			jobs.JobAutoPrioritize: func(context.Context, *jobs.RunContext) error { return nil },
		},
	})
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	fakeDVR := &fakeDVRService{
		syncResult: dvr.SyncResult{
			Provider: "channels",
			BaseURL:  "http://channels.lan:8089",
		},
		syncDelay:     300 * time.Millisecond,
		syncStartedCh: make(chan struct{}, 4),
		syncDoneCh:    make(chan struct{}, 4),
	}
	handler.SetDVRService(fakeDVR)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	firstRespCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstRespCh <- doRaw(t, mux, http.MethodPost, "/api/admin/dvr/sync", map[string]any{
			"dry_run": false,
		})
	}()

	select {
	case <-fakeDVR.syncStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first runner-backed DVR sync start")
	}

	conflictRec := doRaw(t, mux, http.MethodPost, "/api/admin/dvr/sync", map[string]any{
		"dry_run": true,
	})
	if conflictRec.Code != http.StatusConflict {
		t.Fatalf("overlap POST /api/admin/dvr/sync status = %d, want %d, body=%s", conflictRec.Code, http.StatusConflict, conflictRec.Body.String())
	}

	select {
	case <-fakeDVR.syncStartedCh:
		t.Fatal("overlapping request invoked dvr.Sync(), want runner overlap rejection before service call")
	case <-time.After(150 * time.Millisecond):
	}

	var firstRec *httptest.ResponseRecorder
	select {
	case firstRec = <-firstRespCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first DVR sync response")
	}
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first POST /api/admin/dvr/sync status = %d, want %d, body=%s", firstRec.Code, http.StatusOK, firstRec.Body.String())
	}

	runs, err := runner.ListRuns(ctx, jobs.JobDVRLineupSync, 5, 0)
	if err != nil {
		t.Fatalf("ListRuns(dvr_lineup_sync) error = %v", err)
	}
	if len(runs) == 0 {
		t.Fatal("dvr_lineup_sync runs = 0, want manual run persisted via runner")
	}
	if runs[0].TriggeredBy != jobs.TriggerManual {
		t.Fatalf("dvr_lineup_sync triggered_by = %q, want %q", runs[0].TriggeredBy, jobs.TriggerManual)
	}
}

func TestAdminRoutesDVRReverseSyncDetachedFromClientCancellation(t *testing.T) {
	fakeDVR := &fakeDVRService{
		reverseSyncResult: dvr.ReverseSyncResult{
			Provider: "channels",
			BaseURL:  "http://channels.lan:8089",
			LineupID: "USA-MN22577-X",
		},
		reverseSyncDelay:     250 * time.Millisecond,
		reverseSyncStartedCh: make(chan struct{}, 1),
		reverseSyncDoneCh:    make(chan struct{}, 1),
		reverseSyncCtxErrCh:  make(chan error, 1),
	}
	server := newAdminDVRHTTPServer(t, fakeDVR)

	reqCtx, cancelReq := context.WithCancel(context.Background())
	defer cancelReq()

	req, err := http.NewRequestWithContext(
		reqCtx,
		http.MethodPost,
		server.URL+"/api/admin/dvr/reverse-sync",
		strings.NewReader(`{"lineup_id":"USA-MN22577-X"}`),
	)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	responseErrCh := make(chan error, 1)
	go func() {
		resp, reqErr := server.Client().Do(req)
		if reqErr == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		responseErrCh <- reqErr
	}()

	select {
	case <-fakeDVR.reverseSyncStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for DVR reverse-sync start")
	}

	cancelReq()

	select {
	case reqErr := <-responseErrCh:
		if reqErr == nil {
			t.Fatal("request error = nil, want cancellation error after client cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for canceled client request result")
	}

	select {
	case <-fakeDVR.reverseSyncDoneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for detached DVR reverse-sync completion")
	}

	select {
	case ctxErr := <-fakeDVR.reverseSyncCtxErrCh:
		if ctxErr != nil {
			t.Fatalf("reverse-sync context error after client cancellation = %v, want nil", ctxErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DVR reverse-sync context observation")
	}
}

func TestAdminRoutesChannelDVRReverseSyncDetachedFromClientCancellation(t *testing.T) {
	fakeDVR := &fakeDVRService{
		reverseChannelResult: dvr.ReverseSyncResult{
			Provider:       "channels",
			BaseURL:        "http://channels.lan:8089",
			LineupID:       "USA-MN22577-X",
			CandidateCount: 1,
		},
		reverseChannelDelay:     250 * time.Millisecond,
		reverseChannelStartedCh: make(chan struct{}, 1),
		reverseChannelDoneCh:    make(chan struct{}, 1),
		reverseChannelCtxErrCh:  make(chan error, 1),
	}
	server := newAdminDVRHTTPServer(t, fakeDVR)

	reqCtx, cancelReq := context.WithCancel(context.Background())
	defer cancelReq()

	req, err := http.NewRequestWithContext(
		reqCtx,
		http.MethodPost,
		server.URL+"/api/channels/123/dvr/reverse-sync",
		strings.NewReader(`{"lineup_id":"USA-MN22577-X"}`),
	)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	responseErrCh := make(chan error, 1)
	go func() {
		resp, reqErr := server.Client().Do(req)
		if reqErr == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		responseErrCh <- reqErr
	}()

	select {
	case <-fakeDVR.reverseChannelStartedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for channel DVR reverse-sync start")
	}

	cancelReq()

	select {
	case reqErr := <-responseErrCh:
		if reqErr == nil {
			t.Fatal("request error = nil, want cancellation error after client cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for canceled client request result")
	}

	select {
	case <-fakeDVR.reverseChannelDoneCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for detached channel DVR reverse-sync completion")
	}

	select {
	case ctxErr := <-fakeDVR.reverseChannelCtxErrCh:
		if ctxErr != nil {
			t.Fatalf("channel reverse-sync context error after client cancellation = %v, want nil", ctxErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel DVR reverse-sync context observation")
	}
}

func TestAdminRoutesDVRPutRejectsInvalidCronBeforePersist(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	fakeDVR := &fakeDVRService{
		state: dvr.ConfigState{
			Instance: dvr.InstanceConfig{
				ID:              1,
				Provider:        dvr.ProviderChannels,
				BaseURL:         "http://channels.lan:8089",
				DefaultLineupID: "USA-MN22577-X",
				SyncMode:        dvr.SyncModeConfiguredOnly,
			},
		},
	}
	handler.SetDVRService(fakeDVR)

	fakeScheduler := &fakeDVRScheduler{
		validateErr: fmt.Errorf("parse cron expression: expected exactly 5 fields, found 2"),
	}
	handler.SetDVRScheduler(fakeScheduler)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
		"base_url":     "http://example.invalid:8089",
		"sync_enabled": true,
		"sync_cron":    "bad cron",
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT /api/admin/dvr status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if fakeDVR.updateCalls != 0 {
		t.Fatalf("UpdateConfig called %d times, want 0", fakeDVR.updateCalls)
	}
	if fakeDVR.state.Instance.BaseURL != "http://channels.lan:8089" {
		t.Fatalf("base_url mutated on invalid cron: got %q, want %q", fakeDVR.state.Instance.BaseURL, "http://channels.lan:8089")
	}
	if !fakeScheduler.validateCalled {
		t.Fatal("expected scheduler ValidateCron to be called")
	}
	if fakeScheduler.validateSpec != "bad cron" {
		t.Fatalf("ValidateCron called with %q, want %q", fakeScheduler.validateSpec, "bad cron")
	}
	if fakeScheduler.updated {
		t.Fatal("UpdateJobSchedule called on invalid cron, want not called")
	}
}

func TestAdminRoutesDVRPutSchedulerFailureRollsBackConfig(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	initial := dvr.InstanceConfig{
		ID:              1,
		Provider:        dvr.ProviderChannels,
		BaseURL:         "http://channels.lan:8089",
		DefaultLineupID: "USA-MN22577-X",
		SyncEnabled:     false,
		SyncCron:        "",
		SyncMode:        dvr.SyncModeConfiguredOnly,
	}
	fakeDVR := &fakeDVRService{
		state: dvr.ConfigState{
			Instance: initial,
		},
	}
	handler.SetDVRService(fakeDVR)

	fakeScheduler := &fakeDVRScheduler{
		updateErr: fmt.Errorf("scheduler storage unavailable"),
	}
	handler.SetDVRScheduler(fakeScheduler)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
		"base_url":     "http://example.invalid:8089",
		"sync_enabled": true,
		"sync_cron":    "*/15 * * * *",
	})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("PUT /api/admin/dvr scheduler failure status = %d, want %d, body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "update dvr sync schedule: scheduler storage unavailable") {
		t.Fatalf("scheduler failure body = %q, want scheduler update error", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "dvr config rolled back") {
		t.Fatalf("scheduler failure body = %q, want rollback marker", rec.Body.String())
	}
	if fakeDVR.updateCalls != 2 {
		t.Fatalf("UpdateConfig called %d times, want 2 (update + rollback)", fakeDVR.updateCalls)
	}
	if fakeDVR.state.Instance.BaseURL != initial.BaseURL {
		t.Fatalf("base_url after rollback = %q, want %q", fakeDVR.state.Instance.BaseURL, initial.BaseURL)
	}
	if fakeDVR.state.Instance.SyncEnabled != initial.SyncEnabled {
		t.Fatalf("sync_enabled after rollback = %v, want %v", fakeDVR.state.Instance.SyncEnabled, initial.SyncEnabled)
	}
	if fakeScheduler.updateCalls != 1 {
		t.Fatalf("UpdateJobSchedule called %d times, want 1", fakeScheduler.updateCalls)
	}
}

func TestAdminRoutesDVRPutSchedulerFailureRollbackFailureIncludesBothErrors(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	initial := dvr.InstanceConfig{
		ID:              1,
		Provider:        dvr.ProviderChannels,
		BaseURL:         "http://channels.lan:8089",
		DefaultLineupID: "USA-MN22577-X",
		SyncEnabled:     false,
		SyncCron:        "",
		SyncMode:        dvr.SyncModeConfiguredOnly,
	}
	fakeDVR := &fakeDVRService{
		state: dvr.ConfigState{
			Instance: initial,
		},
		updateErrByCall: map[int]error{
			2: fmt.Errorf("database rollback write failed"),
		},
	}
	handler.SetDVRService(fakeDVR)

	fakeScheduler := &fakeDVRScheduler{
		updateErr: fmt.Errorf("scheduler apply failed"),
	}
	handler.SetDVRScheduler(fakeScheduler)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
		"base_url":     "http://example.invalid:8089",
		"sync_enabled": true,
		"sync_cron":    "*/15 * * * *",
	})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("PUT /api/admin/dvr rollback failure status = %d, want %d, body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "update dvr sync schedule: scheduler apply failed") {
		t.Fatalf("rollback failure body = %q, want scheduler update error", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "rollback dvr config failed: restore dvr config: database rollback write failed") {
		t.Fatalf("rollback failure body = %q, want rollback failure details", rec.Body.String())
	}
	if fakeDVR.updateCalls != 2 {
		t.Fatalf("UpdateConfig called %d times, want 2 (update + rollback)", fakeDVR.updateCalls)
	}
	if fakeDVR.state.Instance.BaseURL != "http://example.invalid:8089" {
		t.Fatalf("base_url after rollback failure = %q, want mutated update value", fakeDVR.state.Instance.BaseURL)
	}
}

func TestAdminRoutesDVRPutSchedulerFailureAfterClientCancelRollsBackUsingDetachedContext(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	initial := dvr.InstanceConfig{
		ID:              1,
		Provider:        dvr.ProviderChannels,
		BaseURL:         "http://channels.lan:8089",
		DefaultLineupID: "USA-MN22577-X",
		SyncEnabled:     false,
		SyncCron:        "",
		SyncMode:        dvr.SyncModeConfiguredOnly,
	}

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()

	fakeDVR := &fakeDVRService{
		state: dvr.ConfigState{
			Instance: initial,
		},
		cancelRequest:          cancelRequest,
		cancelAfterUpdateCalls: 1,
		failOnCanceledContext:  true,
	}
	handler.SetDVRService(fakeDVR)

	contextAwareScheduler := &contextAwareDVRScheduler{
		firstUpdateErr:        fmt.Errorf("injected scheduler apply failure"),
		failOnCanceledContext: true,
	}
	handler.SetDVRScheduler(contextAwareScheduler)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	payload, err := json.Marshal(map[string]any{
		"base_url":     "http://mutated.example.invalid:8089",
		"sync_enabled": true,
		"sync_cron":    "*/15 * * * *",
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/api/admin/dvr", bytes.NewReader(payload)).WithContext(requestCtx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("PUT /api/admin/dvr status = %d, want %d, body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "update dvr sync schedule: injected scheduler apply failure") {
		t.Fatalf("PUT /api/admin/dvr body = %q, want scheduler apply failure marker", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "dvr config rolled back") {
		t.Fatalf("PUT /api/admin/dvr body = %q, want rollback marker", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "rollback dvr config failed") {
		t.Fatalf("PUT /api/admin/dvr body = %q, rollback should succeed under detached context", rec.Body.String())
	}

	if fakeDVR.updateCalls != 2 {
		t.Fatalf("UpdateConfig() calls = %d, want 2 (persist + rollback)", fakeDVR.updateCalls)
	}
	if fakeDVR.canceledUpdateCalls != 0 {
		t.Fatalf("UpdateConfig() canceled calls = %d, want 0", fakeDVR.canceledUpdateCalls)
	}

	scheduleCalls, canceledScheduleCalls := contextAwareScheduler.UpdateCounts()
	if scheduleCalls != 1 {
		t.Fatalf("UpdateJobSchedule() calls = %d, want 1", scheduleCalls)
	}
	if canceledScheduleCalls != 0 {
		t.Fatalf("UpdateJobSchedule() canceled calls = %d, want 0", canceledScheduleCalls)
	}

	if fakeDVR.state.Instance.BaseURL != initial.BaseURL {
		t.Fatalf("base_url after rollback = %q, want %q", fakeDVR.state.Instance.BaseURL, initial.BaseURL)
	}
	if fakeDVR.state.Instance.SyncEnabled != initial.SyncEnabled {
		t.Fatalf("sync_enabled after rollback = %v, want %v", fakeDVR.state.Instance.SyncEnabled, initial.SyncEnabled)
	}
	if fakeDVR.state.Instance.SyncCron != initial.SyncCron {
		t.Fatalf("sync_cron after rollback = %q, want %q", fakeDVR.state.Instance.SyncCron, initial.SyncCron)
	}
}

func TestAdminRoutesDVRPutConcurrentRollbackDoesNotClobberWinner(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	initial := dvr.InstanceConfig{
		ID:              1,
		Provider:        dvr.ProviderChannels,
		BaseURL:         "http://channels.lan:8089",
		DefaultLineupID: "USA-MN22577-X",
		SyncEnabled:     false,
		SyncCron:        "",
		SyncMode:        dvr.SyncModeConfiguredOnly,
	}
	fakeDVR := &fakeDVRService{
		state: dvr.ConfigState{
			Instance: initial,
		},
	}
	handler.SetDVRService(fakeDVR)

	releaseFirstUpdate := make(chan struct{})
	blockingScheduler := &blockingDVRScheduler{
		firstUpdateStarted: make(chan struct{}),
		releaseFirstUpdate: releaseFirstUpdate,
		firstUpdateErr:     fmt.Errorf("scheduler apply failed"),
	}
	handler.SetDVRScheduler(blockingScheduler)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	losingRespCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		losingRespCh <- doRaw(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
			"base_url":     "http://losing.example.invalid:8089",
			"sync_enabled": true,
			"sync_cron":    "*/15 * * * *",
		})
	}()

	select {
	case <-blockingScheduler.firstUpdateStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first scheduler update attempt to block")
	}

	winningRespCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		winningRespCh <- doRaw(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
			"base_url": "http://winning.example.invalid:8089",
		})
	}()

	select {
	case rec := <-winningRespCh:
		t.Fatalf(
			"winning request completed before first request finished; admin config mutations were not serialized (status=%d body=%s)",
			rec.Code,
			rec.Body.String(),
		)
	case <-time.After(150 * time.Millisecond):
	}

	close(releaseFirstUpdate)

	var losingRec *httptest.ResponseRecorder
	select {
	case losingRec = <-losingRespCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for losing request response")
	}
	if losingRec.Code != http.StatusInternalServerError {
		t.Fatalf("losing PUT /api/admin/dvr status = %d, want %d, body=%s", losingRec.Code, http.StatusInternalServerError, losingRec.Body.String())
	}
	if !strings.Contains(losingRec.Body.String(), "update dvr sync schedule: scheduler apply failed") {
		t.Fatalf("losing request body = %q, want scheduler failure marker", losingRec.Body.String())
	}
	if !strings.Contains(losingRec.Body.String(), "dvr config rolled back") {
		t.Fatalf("losing request body = %q, want rollback marker", losingRec.Body.String())
	}

	var winningRec *httptest.ResponseRecorder
	select {
	case winningRec = <-winningRespCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for winning request response")
	}
	if winningRec.Code != http.StatusOK {
		t.Fatalf("winning PUT /api/admin/dvr status = %d, want %d, body=%s", winningRec.Code, http.StatusOK, winningRec.Body.String())
	}
	if fakeDVR.state.Instance.BaseURL != "http://winning.example.invalid:8089" {
		t.Fatalf("base_url after concurrent rollback = %q, want winner value", fakeDVR.state.Instance.BaseURL)
	}
}

func TestAdminRoutesRejectOversizeJSONBodyWith413(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:news:primary",
			ChannelKey: "tvg:news",
			Name:       "News Primary",
			Group:      "News",
			StreamURL:  "http://example.com/news-primary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.SetJSONBodyLimitBytes(128)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key":   "src:news:primary",
		"guide_name": strings.Repeat("n", 512),
	})
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("POST /api/channels oversize status = %d, want %d, body=%s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "request body too large") {
		t.Fatalf("oversize response body = %q, want request body too large marker", rec.Body.String())
	}

	var listResp struct {
		Channels []channels.Channel `json:"channels"`
	}
	doJSON(t, mux, http.MethodGet, "/api/channels", nil, http.StatusOK, &listResp)
	if len(listResp.Channels) != 0 {
		t.Fatalf("len(channels) after oversize request = %d, want 0", len(listResp.Channels))
	}

	var created channels.Channel
	doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
		"item_key": "src:news:primary",
	}, http.StatusOK, &created)
	if created.ChannelID <= 0 {
		t.Fatalf("created.ChannelID = %d, want > 0", created.ChannelID)
	}
}

func TestAdminRoutesDVRPutOversizeJSONDoesNotMutateConfig(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	channelsSvc := channels.NewService(store)
	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.SetJSONBodyLimitBytes(128)

	fakeDVR := &fakeDVRService{
		state: dvr.ConfigState{
			Instance: dvr.InstanceConfig{
				ID:       1,
				Provider: dvr.ProviderChannels,
				BaseURL:  "http://channels.lan:8089",
			},
		},
	}
	handler.SetDVRService(fakeDVR)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodPut, "/api/admin/dvr", map[string]any{
		"base_url": "http://example.invalid/" + strings.Repeat("a", 512),
	})
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("PUT /api/admin/dvr oversize status = %d, want %d, body=%s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
	}
	if fakeDVR.updateCalls != 0 {
		t.Fatalf("UpdateConfig called %d times, want 0", fakeDVR.updateCalls)
	}
}

type fakeDVRService struct {
	state                     dvr.ConfigState
	testResult                dvr.TestResult
	lineups                   []dvr.DVRLineup
	syncResult                dvr.SyncResult
	syncErr                   error
	syncDelay                 time.Duration
	syncStartedCh             chan struct{}
	syncDoneCh                chan struct{}
	syncCtxErrCh              chan error
	reverseSyncResult         dvr.ReverseSyncResult
	reverseSyncErr            error
	reverseSyncDelay          time.Duration
	reverseSyncStartedCh      chan struct{}
	reverseSyncDoneCh         chan struct{}
	reverseSyncCtxErrCh       chan error
	reverseChannelResult      dvr.ReverseSyncResult
	reverseChannelErr         error
	reverseChannelDelay       time.Duration
	reverseChannelStartedCh   chan struct{}
	reverseChannelDoneCh      chan struct{}
	reverseChannelCtxErrCh    chan error
	channelMapping            dvr.ChannelMapping
	channelMappings           []dvr.ChannelMapping
	updateCalls               int
	updateErr                 error
	updateErrByCall           map[int]error
	cancelRequest             context.CancelFunc
	cancelAfterUpdateCalls    int
	failOnCanceledContext     bool
	canceledUpdateCalls       int
	getStateErr               error
	reloadErr                 error
	reloadDelay               time.Duration
	reloadStartedCh           chan struct{}
	reloadDoneCh              chan struct{}
	reloadReleaseCh           chan struct{}
	reloadMu                  sync.RWMutex
	reloadCalls               int
	reloadStartedAt           []time.Time
	lastReloadCtxErr          error
	lastListEnabledOnly       bool
	lastSyncIncludeDynamic    bool
	lastReverseIncludeDynamic bool
	lastListIncludeDynamic    bool
	lastListLimit             int
	lastListOffset            int
}

type fakeDVRScheduler struct {
	updated        bool
	updateCalls    int
	jobName        string
	enabled        bool
	cronSpec       string
	validateCalled bool
	validateSpec   string
	validateErr    error
	updateErr      error
}

func (f *fakeDVRScheduler) ValidateCron(spec string) error {
	f.validateCalled = true
	f.validateSpec = spec
	return f.validateErr
}

func (f *fakeDVRScheduler) UpdateJobSchedule(_ context.Context, jobName string, enabled bool, cronSpec string) error {
	f.updateCalls++
	f.updated = true
	f.jobName = jobName
	f.enabled = enabled
	f.cronSpec = cronSpec
	return f.updateErr
}

type contextAwareDVRScheduler struct {
	firstUpdateErr        error
	failOnCanceledContext bool

	mu                 sync.Mutex
	updateCalls        int
	canceledUpdateRuns int
}

func (s *contextAwareDVRScheduler) ValidateCron(string) error {
	return nil
}

func (s *contextAwareDVRScheduler) UpdateJobSchedule(ctx context.Context, _ string, _ bool, _ string) error {
	s.mu.Lock()
	s.updateCalls++
	call := s.updateCalls
	s.mu.Unlock()

	var canceledErr error
	if s.failOnCanceledContext {
		if err := ctx.Err(); err != nil {
			s.mu.Lock()
			s.canceledUpdateRuns++
			s.mu.Unlock()
			canceledErr = err
		}
	}
	if call == 1 && s.firstUpdateErr != nil {
		return s.firstUpdateErr
	}
	if canceledErr != nil {
		return canceledErr
	}
	return nil
}

func (s *contextAwareDVRScheduler) UpdateCounts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateCalls, s.canceledUpdateRuns
}

type blockingDVRScheduler struct {
	firstUpdateStarted chan struct{}
	releaseFirstUpdate <-chan struct{}
	firstUpdateErr     error
	firstSignalOnce    sync.Once
	mu                 sync.Mutex
	updateCalls        int
}

func (s *blockingDVRScheduler) ValidateCron(string) error {
	return nil
}

func (s *blockingDVRScheduler) UpdateJobSchedule(_ context.Context, _ string, _ bool, _ string) error {
	s.mu.Lock()
	s.updateCalls++
	call := s.updateCalls
	s.mu.Unlock()

	if call == 1 {
		s.firstSignalOnce.Do(func() {
			close(s.firstUpdateStarted)
		})
		<-s.releaseFirstUpdate
		if s.firstUpdateErr != nil {
			return s.firstUpdateErr
		}
	}
	return nil
}

func (f *fakeDVRService) GetState(context.Context) (dvr.ConfigState, error) {
	if f.getStateErr != nil {
		return dvr.ConfigState{}, f.getStateErr
	}
	return f.state, nil
}

func (f *fakeDVRService) UpdateConfig(ctx context.Context, instance dvr.InstanceConfig) (dvr.ConfigState, error) {
	if f.failOnCanceledContext {
		if err := ctx.Err(); err != nil {
			f.canceledUpdateCalls++
			return f.state, err
		}
	}
	f.updateCalls++
	if err, ok := f.updateErrByCall[f.updateCalls]; ok && err != nil {
		return f.state, err
	}
	if f.updateErr != nil {
		return f.state, f.updateErr
	}
	f.state.Instance = instance
	if f.cancelRequest != nil &&
		f.cancelAfterUpdateCalls > 0 &&
		f.updateCalls >= f.cancelAfterUpdateCalls {
		f.cancelRequest()
	}
	return f.state, nil
}

func (f *fakeDVRService) TestConnection(context.Context) (dvr.TestResult, error) {
	return f.testResult, nil
}

func (f *fakeDVRService) ListLineups(context.Context, bool) ([]dvr.DVRLineup, error) {
	return f.lineups, nil
}

func signalDVRCall(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func signalDVRContextErr(ch chan error, err error) {
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
}

func (f *fakeDVRService) Sync(ctx context.Context, req dvr.SyncRequest) (dvr.SyncResult, error) {
	f.lastSyncIncludeDynamic = req.IncludeDynamic
	signalDVRCall(f.syncStartedCh)
	if f.syncDelay > 0 {
		timer := time.NewTimer(f.syncDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			signalDVRContextErr(f.syncCtxErrCh, ctx.Err())
			signalDVRCall(f.syncDoneCh)
			return dvr.SyncResult{}, ctx.Err()
		case <-timer.C:
		}
	}
	signalDVRContextErr(f.syncCtxErrCh, ctx.Err())
	signalDVRCall(f.syncDoneCh)
	if f.syncErr != nil {
		return dvr.SyncResult{}, f.syncErr
	}
	result := f.syncResult
	result.DryRun = req.DryRun
	return result, nil
}

func (f *fakeDVRService) ReverseSync(ctx context.Context, req dvr.ReverseSyncRequest) (dvr.ReverseSyncResult, error) {
	f.lastReverseIncludeDynamic = req.IncludeDynamic
	signalDVRCall(f.reverseSyncStartedCh)
	if f.reverseSyncDelay > 0 {
		timer := time.NewTimer(f.reverseSyncDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			signalDVRContextErr(f.reverseSyncCtxErrCh, ctx.Err())
			signalDVRCall(f.reverseSyncDoneCh)
			return dvr.ReverseSyncResult{}, ctx.Err()
		case <-timer.C:
		}
	}
	signalDVRContextErr(f.reverseSyncCtxErrCh, ctx.Err())
	signalDVRCall(f.reverseSyncDoneCh)
	if f.reverseSyncErr != nil {
		return dvr.ReverseSyncResult{}, f.reverseSyncErr
	}
	result := f.reverseSyncResult
	result.DryRun = req.DryRun
	if req.LineupID != "" {
		result.LineupID = req.LineupID
	}
	return result, nil
}

func (f *fakeDVRService) ReverseSyncChannel(ctx context.Context, channelID int64, req dvr.ReverseSyncRequest) (dvr.ReverseSyncResult, error) {
	signalDVRCall(f.reverseChannelStartedCh)
	if f.reverseChannelDelay > 0 {
		timer := time.NewTimer(f.reverseChannelDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			signalDVRContextErr(f.reverseChannelCtxErrCh, ctx.Err())
			signalDVRCall(f.reverseChannelDoneCh)
			return dvr.ReverseSyncResult{}, ctx.Err()
		case <-timer.C:
		}
	}
	signalDVRContextErr(f.reverseChannelCtxErrCh, ctx.Err())
	signalDVRCall(f.reverseChannelDoneCh)
	if f.reverseChannelErr != nil {
		return dvr.ReverseSyncResult{}, f.reverseChannelErr
	}
	result := f.reverseChannelResult
	result.DryRun = req.DryRun
	if req.LineupID != "" {
		result.LineupID = req.LineupID
	}
	if result.CandidateCount == 0 {
		result.CandidateCount = 1
	}
	if channelID == 0 {
		result.ImportedCount = 0
	}
	return result, nil
}

func (f *fakeDVRService) ListChannelMappings(_ context.Context, enabledOnly bool, includeDynamic bool) ([]dvr.ChannelMapping, error) {
	rows, _, err := f.ListChannelMappingsPaged(context.Background(), enabledOnly, includeDynamic, int(^uint(0)>>1), 0)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (f *fakeDVRService) ListChannelMappingsPaged(
	_ context.Context,
	enabledOnly bool,
	includeDynamic bool,
	limit int,
	offset int,
) ([]dvr.ChannelMapping, int, error) {
	f.lastListEnabledOnly = enabledOnly
	f.lastListIncludeDynamic = includeDynamic
	f.lastListLimit = limit
	f.lastListOffset = offset

	source := f.channelMappings
	if len(source) == 0 {
		source = []dvr.ChannelMapping{f.channelMapping}
	}
	out := make([]dvr.ChannelMapping, 0, len(source))
	for _, mapping := range source {
		if !includeDynamic {
			guideNumber, err := strconv.Atoi(strings.TrimSpace(mapping.GuideNumber))
			if err == nil && guideNumber >= channels.DynamicGuideStart {
				continue
			}
		}
		if enabledOnly && !mapping.Enabled {
			continue
		}
		out = append(out, mapping)
	}
	total := len(out)
	if limit < 1 {
		limit = 1
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return []dvr.ChannelMapping{}, total, nil
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return out[offset:end], total, nil
}

func (f *fakeDVRService) ReloadLineup(ctx context.Context) error {
	f.reloadMu.Lock()
	f.reloadCalls++
	f.reloadStartedAt = append(f.reloadStartedAt, time.Now())
	startedCh := f.reloadStartedCh
	doneCh := f.reloadDoneCh
	releaseCh := f.reloadReleaseCh
	delay := f.reloadDelay
	reloadErr := f.reloadErr
	f.reloadMu.Unlock()

	if startedCh != nil {
		select {
		case startedCh <- struct{}{}:
		default:
		}
	}

	recordAndNotify := func(err error) {
		f.reloadMu.Lock()
		f.lastReloadCtxErr = err
		f.reloadMu.Unlock()
		if doneCh != nil {
			select {
			case doneCh <- struct{}{}:
			default:
			}
		}
	}

	if releaseCh != nil {
		select {
		case <-releaseCh:
		case <-ctx.Done():
			err := ctx.Err()
			recordAndNotify(err)
			return err
		}
	}

	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			err := ctx.Err()
			recordAndNotify(err)
			return err
		}
	}

	if err := ctx.Err(); err != nil {
		recordAndNotify(err)
		return err
	}
	if reloadErr != nil {
		recordAndNotify(nil)
		return reloadErr
	}
	recordAndNotify(nil)
	return nil
}

func (f *fakeDVRService) ReloadCallCount() int {
	f.reloadMu.RLock()
	defer f.reloadMu.RUnlock()
	return f.reloadCalls
}

func (f *fakeDVRService) LastReloadContextErr() error {
	f.reloadMu.RLock()
	defer f.reloadMu.RUnlock()
	return f.lastReloadCtxErr
}

func (f *fakeDVRService) FirstReloadStartedAt() time.Time {
	f.reloadMu.RLock()
	defer f.reloadMu.RUnlock()
	if len(f.reloadStartedAt) == 0 {
		return time.Time{}
	}
	return f.reloadStartedAt[0]
}

func (f *fakeDVRService) GetChannelMapping(_ context.Context, channelID int64) (dvr.ChannelMapping, error) {
	mapping := f.channelMapping
	mapping.ChannelID = channelID
	return mapping, nil
}

func (f *fakeDVRService) UpdateChannelMapping(_ context.Context, channelID int64, update dvr.ChannelMappingUpdate) (dvr.ChannelMapping, error) {
	f.channelMapping.ChannelID = channelID
	f.channelMapping.DVRLineupID = update.DVRLineupID
	f.channelMapping.DVRLineupChannel = update.DVRLineupChannel
	f.channelMapping.DVRStationRef = update.DVRStationRef
	f.channelMapping.DVRCallsignHint = update.DVRCallsignHint
	return f.channelMapping, nil
}

type fakeTunerStatusProvider struct {
	snapshot             stream.TunerStatusSnapshot
	triggerErr           error
	triggerCalls         int
	lastTriggerChannelID int64
	lastTriggerReason    string
}

func (f *fakeTunerStatusProvider) TunerStatusSnapshot() stream.TunerStatusSnapshot {
	return f.snapshot
}

func (f *fakeTunerStatusProvider) TriggerSessionRecovery(channelID int64, reason string) error {
	f.triggerCalls++
	f.lastTriggerChannelID = channelID
	f.lastTriggerReason = reason
	if f.triggerErr != nil {
		return f.triggerErr
	}
	return nil
}

func (f *fakeTunerStatusProvider) resetTriggers() {
	f.triggerCalls = 0
	f.lastTriggerChannelID = 0
	f.lastTriggerReason = ""
}

type fakeSourceHealthClearRuntime struct {
	mu                             sync.Mutex
	clearSourceHealthCalls         int
	lastClearSourceHealthChannelID int64
	clearAllSourceHealthCalls      int
	clearSourceHealthErr           error
	clearAllSourceHealthErr        error
}

func (f *fakeSourceHealthClearRuntime) ClearSourceHealth(channelID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.clearSourceHealthCalls++
	f.lastClearSourceHealthChannelID = channelID
	if f.clearSourceHealthErr != nil {
		return f.clearSourceHealthErr
	}
	return nil
}

func (f *fakeSourceHealthClearRuntime) ClearAllSourceHealth() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.clearAllSourceHealthCalls++
	if f.clearAllSourceHealthErr != nil {
		return f.clearAllSourceHealthErr
	}
	return nil
}

func (f *fakeSourceHealthClearRuntime) clearSourceHealthCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clearSourceHealthCalls
}

func (f *fakeSourceHealthClearRuntime) clearAllSourceHealthCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clearAllSourceHealthCalls
}

func (f *fakeSourceHealthClearRuntime) lastClearSourceHealthChannel() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastClearSourceHealthChannelID
}

type snapshotOnlyTunerStatusProvider struct{}

func (snapshotOnlyTunerStatusProvider) TunerStatusSnapshot() stream.TunerStatusSnapshot {
	return stream.TunerStatusSnapshot{}
}

type catalogWithoutSourceScopedSupport struct {
	CatalogStore
}

type dynamicBlockCapabilityUnsupportedChannelsService struct {
	ChannelsService
}

func (dynamicBlockCapabilityUnsupportedChannelsService) SupportsSourceScopedDynamicChannelBlocks() bool {
	return false
}

type sourceOrderDriftReorderChannelsService struct {
	ChannelsService
	reorderErr error
}

func (s sourceOrderDriftReorderChannelsService) ReorderSources(ctx context.Context, channelID int64, sourceIDs []int64) error {
	if s.reorderErr != nil {
		return s.reorderErr
	}
	return s.ChannelsService.ReorderSources(ctx, channelID, sourceIDs)
}

type catalogWarningErrorStore struct {
	CatalogStore
	warningErr   error
	warningCalls int
}

func (c *catalogWarningErrorStore) CatalogSearchWarningForQuery(search string, searchRegex bool) (sqlite.CatalogSearchWarning, error) {
	c.warningCalls++
	if c.warningErr != nil {
		return sqlite.CatalogSearchWarning{}, c.warningErr
	}
	return c.CatalogStore.CatalogSearchWarningForQuery(search, searchRegex)
}

type blockingCatalogStore struct {
	itemKeys   []string
	blockUntil <-chan struct{}
	blockCalls int

	mu            sync.Mutex
	callCount     int
	canceledCount int
}

func (b *blockingCatalogStore) GetGroups(context.Context) ([]playlist.Group, error) {
	return nil, nil
}

func (b *blockingCatalogStore) ListGroupsPaged(context.Context, int, int, bool) ([]playlist.Group, int, error) {
	return nil, 0, nil
}

func (b *blockingCatalogStore) ListItems(context.Context, playlist.Query) ([]playlist.Item, int, error) {
	return nil, 0, nil
}

func (b *blockingCatalogStore) ListCatalogItems(context.Context, playlist.Query) ([]playlist.Item, int, error) {
	return nil, 0, nil
}

func (b *blockingCatalogStore) CatalogSearchWarningForQuery(string, bool) (sqlite.CatalogSearchWarning, error) {
	return sqlite.CatalogSearchWarning{}, nil
}

func (b *blockingCatalogStore) ListActiveItemKeysByCatalogFilter(ctx context.Context, _ []string, _ string, _ bool) ([]string, error) {
	b.mu.Lock()
	b.callCount++
	callNumber := b.callCount
	b.mu.Unlock()

	shouldBlock := b.blockUntil != nil && (b.blockCalls <= 0 || callNumber <= b.blockCalls)
	if shouldBlock {
		select {
		case <-b.blockUntil:
		case <-ctx.Done():
			b.mu.Lock()
			b.canceledCount++
			b.mu.Unlock()
			return nil, ctx.Err()
		}
	}

	itemKeys := append([]string(nil), b.itemKeys...)
	return itemKeys, nil
}

func (b *blockingCatalogStore) CallCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.callCount
}

func (b *blockingCatalogStore) CanceledCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.canceledCount
}

type syncObservedChannelsService struct {
	base ChannelsService

	mu             sync.Mutex
	syncCallCount  int
	activeSyncs    int
	maxActiveSyncs int
}

func (s *syncObservedChannelsService) Create(ctx context.Context, itemKey, guideName, channelKey string, dynamicRule *channels.DynamicSourceRule) (channels.Channel, error) {
	return s.base.Create(ctx, itemKey, guideName, channelKey, dynamicRule)
}

func (s *syncObservedChannelsService) Delete(ctx context.Context, channelID int64) error {
	return s.base.Delete(ctx, channelID)
}

func (s *syncObservedChannelsService) List(ctx context.Context) ([]channels.Channel, error) {
	return s.base.List(ctx)
}

func (s *syncObservedChannelsService) ListPaged(ctx context.Context, limit, offset int) ([]channels.Channel, int, error) {
	return s.base.ListPaged(ctx, limit, offset)
}

func (s *syncObservedChannelsService) Reorder(ctx context.Context, channelIDs []int64) error {
	return s.base.Reorder(ctx, channelIDs)
}

func (s *syncObservedChannelsService) Update(ctx context.Context, channelID int64, guideName *string, enabled *bool, dynamicRule *channels.DynamicSourceRule) (channels.Channel, error) {
	return s.base.Update(ctx, channelID, guideName, enabled, dynamicRule)
}

func (s *syncObservedChannelsService) SyncDynamicSources(ctx context.Context, channelID int64, matchedItemKeys []string) (channels.DynamicSourceSyncResult, error) {
	s.mu.Lock()
	s.syncCallCount++
	s.activeSyncs++
	if s.activeSyncs > s.maxActiveSyncs {
		s.maxActiveSyncs = s.activeSyncs
	}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.activeSyncs--
		s.mu.Unlock()
	}()
	return s.base.SyncDynamicSources(ctx, channelID, matchedItemKeys)
}

func (s *syncObservedChannelsService) ListDynamicChannelQueries(ctx context.Context) ([]channels.DynamicChannelQuery, error) {
	return s.base.ListDynamicChannelQueries(ctx)
}

func (s *syncObservedChannelsService) ListDynamicChannelQueriesPaged(ctx context.Context, limit, offset int) ([]channels.DynamicChannelQuery, int, error) {
	return s.base.ListDynamicChannelQueriesPaged(ctx, limit, offset)
}

func (s *syncObservedChannelsService) GetDynamicChannelQuery(ctx context.Context, queryID int64) (channels.DynamicChannelQuery, error) {
	return s.base.GetDynamicChannelQuery(ctx, queryID)
}

func (s *syncObservedChannelsService) CreateDynamicChannelQuery(ctx context.Context, create channels.DynamicChannelQueryCreate) (channels.DynamicChannelQuery, error) {
	return s.base.CreateDynamicChannelQuery(ctx, create)
}

func (s *syncObservedChannelsService) UpdateDynamicChannelQuery(ctx context.Context, queryID int64, update channels.DynamicChannelQueryUpdate) (channels.DynamicChannelQuery, error) {
	return s.base.UpdateDynamicChannelQuery(ctx, queryID, update)
}

func (s *syncObservedChannelsService) DeleteDynamicChannelQuery(ctx context.Context, queryID int64) error {
	return s.base.DeleteDynamicChannelQuery(ctx, queryID)
}

func (s *syncObservedChannelsService) ListDynamicGeneratedChannelsPaged(ctx context.Context, queryID int64, limit, offset int) ([]channels.Channel, int, error) {
	return s.base.ListDynamicGeneratedChannelsPaged(ctx, queryID, limit, offset)
}

func (s *syncObservedChannelsService) ReorderDynamicGeneratedChannels(ctx context.Context, queryID int64, channelIDs []int64) error {
	return s.base.ReorderDynamicGeneratedChannels(ctx, queryID, channelIDs)
}

func (s *syncObservedChannelsService) SyncDynamicChannelBlocks(ctx context.Context) (channels.DynamicChannelSyncResult, error) {
	return s.base.SyncDynamicChannelBlocks(ctx)
}

func (s *syncObservedChannelsService) SupportsSourceScopedDynamicChannelBlocks() bool {
	capability, ok := s.base.(sourceScopedDynamicChannelBlocksCapability)
	if !ok {
		return false
	}
	return capability.SupportsSourceScopedDynamicChannelBlocks()
}

func (s *syncObservedChannelsService) ListSources(ctx context.Context, channelID int64, enabledOnly bool) ([]channels.Source, error) {
	return s.base.ListSources(ctx, channelID, enabledOnly)
}

func (s *syncObservedChannelsService) ListSourcesPaged(ctx context.Context, channelID int64, enabledOnly bool, limit, offset int) ([]channels.Source, int, error) {
	return s.base.ListSourcesPaged(ctx, channelID, enabledOnly, limit, offset)
}

func (s *syncObservedChannelsService) AddSource(ctx context.Context, channelID int64, itemKey string, allowCrossChannel bool) (channels.Source, error) {
	return s.base.AddSource(ctx, channelID, itemKey, allowCrossChannel)
}

func (s *syncObservedChannelsService) DeleteSource(ctx context.Context, channelID, sourceID int64) error {
	return s.base.DeleteSource(ctx, channelID, sourceID)
}

func (s *syncObservedChannelsService) ReorderSources(ctx context.Context, channelID int64, sourceIDs []int64) error {
	return s.base.ReorderSources(ctx, channelID, sourceIDs)
}

func (s *syncObservedChannelsService) UpdateSource(ctx context.Context, channelID, sourceID int64, enabled *bool) (channels.Source, error) {
	return s.base.UpdateSource(ctx, channelID, sourceID, enabled)
}

func (s *syncObservedChannelsService) ClearSourceHealth(ctx context.Context, channelID int64) (int64, error) {
	return s.base.ClearSourceHealth(ctx, channelID)
}

func (s *syncObservedChannelsService) ClearAllSourceHealth(ctx context.Context) (int64, error) {
	return s.base.ClearAllSourceHealth(ctx)
}

func (s *syncObservedChannelsService) DuplicateSuggestions(ctx context.Context, minItems int, searchQuery string, limit, offset int) ([]channels.DuplicateGroup, int, error) {
	return s.base.DuplicateSuggestions(ctx, minItems, searchQuery, limit, offset)
}

func (s *syncObservedChannelsService) SyncCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncCallCount
}

func (s *syncObservedChannelsService) MaxActiveSyncs() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxActiveSyncs
}

func waitForCondition(t *testing.T, timeout time.Duration, description string, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s after %v", description, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newAdminDVRHTTPServer(t *testing.T, fakeDVR *fakeDVRService) *httptest.Server {
	t.Helper()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	channelsSvc := channels.NewService(store)

	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		store.Close()
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	handler.SetDVRService(fakeDVR)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	server := httptest.NewServer(mux)
	t.Cleanup(func() {
		server.Close()
		store.Close()
	})
	return server
}

func doJSON(t *testing.T, mux *http.ServeMux, method, path string, body any, wantStatus int, out any) {
	t.Helper()

	rec := doRaw(t, mux, method, path, body)
	if rec.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d, body = %s", method, path, rec.Code, wantStatus, rec.Body.String())
	}
	if out == nil {
		return
	}
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode %s %s: %v", method, path, err)
	}
}

func doChunkedJSON(t *testing.T, mux *http.ServeMux, method, path string, body any, wantStatus int, out any) {
	t.Helper()

	rec := doRawChunkedJSON(t, mux, method, path, body)
	if rec.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d, body = %s", method, path, rec.Code, wantStatus, rec.Body.String())
	}
	if out == nil {
		return
	}
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatalf("decode %s %s: %v", method, path, err)
	}
}

func doRaw(t *testing.T, mux *http.ServeMux, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body for %s %s: %v", method, path, err)
		}
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func doRawChunkedJSON(t *testing.T, mux *http.ServeMux, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal chunked request body for %s %s: %v", method, path, err)
		}
	}
	return doRawChunkedBodyBytes(t, mux, method, path, payload)
}

func doRawChunkedBody(t *testing.T, mux *http.ServeMux, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	return doRawChunkedBodyBytes(t, mux, method, path, []byte(body))
}

func doRawChunkedBodyBytes(t *testing.T, mux *http.ServeMux, method, path string, payload []byte) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, io.NopCloser(bytes.NewReader(payload)))
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// TestAdminDynamicSyncWorkerConvergesBeforeStoreClose verifies that
// AdminHandler.Close() cancels in-flight dynamic sync background workers and
// blocks until they complete, so store.Close() in the deferred shutdown
// sequence never races with active database work.
//
// Two subtests cover both background worker paths:
//   - BlockSync: triggered by dynamic channel query create/update/delete
//   - ChannelSync: triggered by channel create/update with dynamic rules
func TestAdminDynamicSyncWorkerConvergesBeforeStoreClose(t *testing.T) {
	t.Run("BlockSync", func(t *testing.T) {
		ctx := context.Background()

		store, err := sqlite.Open(":memory:")
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer store.Close()

		if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
			{
				ItemKey:    "src:shutdown:news:one",
				ChannelKey: "name:shutdown news one",
				Name:       "Shutdown News One",
				Group:      "News",
				StreamURL:  "http://example.com/shutdown-news-one.ts",
			},
		}); err != nil {
			t.Fatalf("UpsertPlaylistItems() error = %v", err)
		}

		realSvc := channels.NewService(store)
		blocking := &blockingSyncChannelsService{
			ChannelsService:  realSvc,
			blockSyncStarted: make(chan struct{}, 1),
			blockSyncRelease: make(chan struct{}),
		}

		handler, err := NewAdminHandler(store, blocking)
		if err != nil {
			t.Fatalf("NewAdminHandler() error = %v", err)
		}
		handler.SetDVRService(&fakeDVRService{})
		handler.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

		mux := http.NewServeMux()
		handler.RegisterRoutes(mux, "")

		// POST /api/dynamic-channels triggers enqueueDynamicBlockSync,
		// which launches runDynamicBlockSyncLoop → runDynamicBlockSyncOnce
		// → SyncDynamicChannelBlocks.
		doJSON(t, mux, http.MethodPost, "/api/dynamic-channels", map[string]any{
			"name":         "Shutdown Test Block",
			"group_names":  []string{"News"},
			"search_query": "shutdown",
		}, http.StatusOK, &struct{}{})

		// Wait for the worker to enter SyncDynamicChannelBlocks.
		select {
		case <-blocking.blockSyncStarted:
		case <-time.After(3 * time.Second):
			t.Fatal("block sync worker did not start")
		}

		// Worker is blocked inside SyncDynamicChannelBlocks. Close() should
		// cancel its context (via workerContext) and wait for it to exit.
		closeDone := make(chan struct{})
		go func() {
			handler.Close()
			close(closeDone)
		}()

		select {
		case <-closeDone:
		case <-time.After(5 * time.Second):
			t.Fatal("handler.Close() did not return; block sync worker was not drained")
		}
	})

	t.Run("ChannelSync", func(t *testing.T) {
		ctx := context.Background()

		store, err := sqlite.Open(":memory:")
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		defer store.Close()

		if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
			{
				ItemKey:    "src:shutdown:sports:one",
				ChannelKey: "name:shutdown sports one",
				Name:       "Shutdown Sports One",
				Group:      "Sports",
				StreamURL:  "http://example.com/shutdown-sports-one.ts",
			},
		}); err != nil {
			t.Fatalf("UpsertPlaylistItems() error = %v", err)
		}

		realSvc := channels.NewService(store)
		blocking := &blockingSyncChannelsService{
			ChannelsService:        realSvc,
			blockSourceSyncStarted: make(chan struct{}, 1),
			blockSourceSyncRelease: make(chan struct{}),
		}

		handler, err := NewAdminHandler(store, blocking)
		if err != nil {
			t.Fatalf("NewAdminHandler() error = %v", err)
		}
		handler.SetDVRService(&fakeDVRService{})
		handler.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

		mux := http.NewServeMux()
		handler.RegisterRoutes(mux, "")

		// POST /api/channels with a dynamic rule triggers
		// enqueueDynamicChannelSync, which launches
		// runDynamicChannelSyncLoop → runDynamicChannelSyncOnce
		// → SyncDynamicSources.
		doJSON(t, mux, http.MethodPost, "/api/channels", map[string]any{
			"item_key":   "src:shutdown:sports:one",
			"guide_name": "Shutdown Sports",
			"dynamic_rule": map[string]any{
				"enabled":      true,
				"group_name":   "Sports",
				"search_query": "shutdown",
			},
		}, http.StatusOK, &struct{}{})

		// Wait for the worker to enter SyncDynamicSources.
		select {
		case <-blocking.blockSourceSyncStarted:
		case <-time.After(3 * time.Second):
			t.Fatal("channel sync worker did not start")
		}

		closeDone := make(chan struct{})
		go func() {
			handler.Close()
			close(closeDone)
		}()

		select {
		case <-closeDone:
		case <-time.After(5 * time.Second):
			t.Fatal("handler.Close() did not return; channel sync worker was not drained")
		}
	})
}

// blockingSyncChannelsService wraps a real ChannelsService, blocking on
// SyncDynamicChannelBlocks and/or SyncDynamicSources until signaled. This lets
// shutdown tests synchronize with background worker entry and verify that
// Close() properly drains them.
type blockingSyncChannelsService struct {
	ChannelsService

	// Block sync (SyncDynamicChannelBlocks) control.
	blockSyncStarted chan struct{}
	blockSyncRelease chan struct{}

	// Channel source sync (SyncDynamicSources) control.
	blockSourceSyncStarted chan struct{}
	blockSourceSyncRelease chan struct{}
}

func (b *blockingSyncChannelsService) SupportsSourceScopedDynamicChannelBlocks() bool {
	capability, ok := b.ChannelsService.(sourceScopedDynamicChannelBlocksCapability)
	if !ok {
		return false
	}
	return capability.SupportsSourceScopedDynamicChannelBlocks()
}

func (b *blockingSyncChannelsService) SyncDynamicChannelBlocks(ctx context.Context) (channels.DynamicChannelSyncResult, error) {
	if b.blockSyncStarted != nil {
		select {
		case b.blockSyncStarted <- struct{}{}:
		default:
		}
	}
	if b.blockSyncRelease != nil {
		select {
		case <-b.blockSyncRelease:
		case <-ctx.Done():
			return channels.DynamicChannelSyncResult{}, ctx.Err()
		}
	}
	return b.ChannelsService.SyncDynamicChannelBlocks(ctx)
}

func (b *blockingSyncChannelsService) SyncDynamicSources(ctx context.Context, channelID int64, matchedItemKeys []string) (channels.DynamicSourceSyncResult, error) {
	if b.blockSourceSyncStarted != nil {
		select {
		case b.blockSourceSyncStarted <- struct{}{}:
		default:
		}
	}
	if b.blockSourceSyncRelease != nil {
		select {
		case <-b.blockSourceSyncRelease:
		case <-ctx.Done():
			return channels.DynamicSourceSyncResult{}, ctx.Err()
		}
	}
	return b.ChannelsService.SyncDynamicSources(ctx, channelID, matchedItemKeys)
}
