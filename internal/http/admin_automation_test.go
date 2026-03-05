package httpapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/jobs"
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/scheduler"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
)

func TestAdminAutomationRoutesLifecycle(t *testing.T) {
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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

	playlistJob := func(ctx context.Context, run *jobs.RunContext) error {
		if err := run.SetProgress(ctx, 1, 1); err != nil {
			return err
		}
		return run.SetSummary(ctx, "playlist sync complete")
	}
	autoJob := func(ctx context.Context, run *jobs.RunContext) error {
		if err := run.SetProgress(ctx, 1, 1); err != nil {
			return err
		}
		return run.SetSummary(ctx, "channels=1 analyzed=3 cache_hits=0 reordered=1 analysis_errors=2 analysis_error_buckets=http_429:1,decode_ffprobe_json:1 enabled_only=true top_n_per_channel=0 limited_channels=0")
	}

	handler, err := NewAdminHandler(store, channelsSvc, AutomationDeps{
		Settings:  store,
		Scheduler: schedulerSvc,
		Runner:    runner,
		JobFuncs: map[string]jobs.JobFunc{
			jobs.JobPlaylistSync:   playlistJob,
			jobs.JobAutoPrioritize: autoJob,
		},
	})
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	playlistRuntime := &capturingPlaylistSourceRuntime{}
	handler.SetPlaylistSourceRuntime(playlistRuntime)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	uiRec := httptest.NewRecorder()
	uiReq := httptest.NewRequest(http.MethodGet, "/ui/automation", nil)
	mux.ServeHTTP(uiRec, uiReq)
	if uiRec.Code != http.StatusOK {
		t.Fatalf("GET /ui/automation status = %d, want %d", uiRec.Code, http.StatusOK)
	}
	if !strings.Contains(uiRec.Body.String(), "Automation Settings") {
		t.Fatalf("GET /ui/automation body missing Automation Settings content")
	}
	if strings.Contains(uiRec.Body.String(), "Analyzer Workers") {
		t.Fatalf("GET /ui/automation body still exposes deprecated Analyzer Workers setting")
	}
	if !strings.Contains(uiRec.Body.String(), "Clear Auto-prioritize Cache") {
		t.Fatalf("GET /ui/automation body missing clear cache control")
	}
	if !strings.Contains(uiRec.Body.String(), "Clear All Source Health + Cooldowns") {
		t.Fatalf("GET /ui/automation body missing clear source health control")
	}
	if !strings.Contains(uiRec.Body.String(), "source-draft-url-input") {
		t.Fatalf("GET /ui/automation body missing always-visible source draft URL input")
	}
	if !strings.Contains(uiRec.Body.String(), "Fill out this card, then click Add Source.") {
		t.Fatalf("GET /ui/automation body missing add-source draft guidance text")
	}
	if !strings.Contains(uiRec.Body.String(), "let sourceDraft = buildEmptySourceDraft();") {
		t.Fatalf("GET /ui/automation body missing empty source draft initialization")
	}
	if !strings.Contains(uiRec.Body.String(), "sourceDraft = buildEmptySourceDraft();") {
		t.Fatalf("GET /ui/automation body missing empty source draft reset after add")
	}

	var initialState map[string]any
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &initialState)
	if initialState["timezone"] != "America/Chicago" {
		t.Fatalf("initial timezone = %#v, want America/Chicago", initialState["timezone"])
	}

	invalidRec := doRaw(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_sync": map[string]any{
			"enabled":   true,
			"cron_spec": "not a cron",
		},
	})
	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("PUT /api/admin/automation invalid cron status = %d, want %d", invalidRec.Code, http.StatusBadRequest)
	}

	// Disabling a schedule should not require a valid cron expression.
	var disabledState map[string]any
	doJSON(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_sync": map[string]any{
			"enabled":   false,
			"cron_spec": "still not a cron",
		},
	}, http.StatusOK, &disabledState)
	playlistState, ok := disabledState["playlist_sync"].(map[string]any)
	if !ok {
		t.Fatalf("disabled playlist_sync payload type = %T, want map[string]any", disabledState["playlist_sync"])
	}
	if enabled, _ := playlistState["enabled"].(bool); enabled {
		t.Fatalf("playlist_sync.enabled = %v, want false", enabled)
	}

	unknownFieldRec := doRaw(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"analyzer": map[string]any{
			"workers": 3,
		},
	})
	if unknownFieldRec.Code != http.StatusBadRequest {
		t.Fatalf("PUT /api/admin/automation unknown analyzer.workers status = %d, want %d", unknownFieldRec.Code, http.StatusBadRequest)
	}

	invalidTimezoneRec := doRaw(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"timezone": "Not/A_Real_Timezone",
	})
	if invalidTimezoneRec.Code != http.StatusBadRequest {
		t.Fatalf(
			"PUT /api/admin/automation invalid timezone status = %d, want %d, body=%s",
			invalidTimezoneRec.Code,
			http.StatusBadRequest,
			invalidTimezoneRec.Body.String(),
		)
	}

	var updatedState map[string]any
	doJSON(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_url": "http://example.com/playlist.m3u",
		"timezone":     "UTC",
		"playlist_sync": map[string]any{
			"enabled":   true,
			"cron_spec": "*/20 * * * *",
		},
		"auto_prioritize": map[string]any{
			"enabled":   true,
			"cron_spec": "15 2 * * *",
		},
		"analyzer": map[string]any{
			"enabled_only":       true,
			"top_n_per_channel":  2,
			"probe_timeout_ms":   9000,
			"analyzeduration_us": 1600000,
			"probesize_bytes":    1200000,
			"bitrate_mode":       "metadata",
			"sample_seconds":     4,
		},
	}, http.StatusOK, &updatedState)

	if updatedState["timezone"] != "UTC" {
		t.Fatalf("updated timezone = %#v, want UTC", updatedState["timezone"])
	}
	analyzerState, ok := updatedState["analyzer"].(map[string]any)
	if !ok {
		t.Fatalf("updated analyzer payload type = %T, want map[string]any", updatedState["analyzer"])
	}
	if _, exists := analyzerState["workers"]; exists {
		t.Fatalf("updated analyzer payload still includes deprecated workers field: %#v", analyzerState["workers"])
	}

	playlistURL, err := store.GetSetting(ctx, sqlite.SettingPlaylistURL)
	if err != nil {
		t.Fatalf("GetSetting(playlist.url) error = %v", err)
	}
	if playlistURL != "http://example.com/playlist.m3u" {
		t.Fatalf("playlist.url = %q, want updated URL", playlistURL)
	}

	topNSetting, err := store.GetSetting(ctx, sqlite.SettingAutoPrioritizeTopNPerChannel)
	if err != nil {
		t.Fatalf("GetSetting(top_n_per_channel) error = %v", err)
	}
	if topNSetting != "2" {
		t.Fatalf("top_n_per_channel = %q, want 2", topNSetting)
	}

	for _, metric := range []jobs.StreamMetric{
		{ItemKey: "src:test:1", AnalyzedAt: time.Now().UTC().Unix()},
		{ItemKey: "src:test:2", AnalyzedAt: time.Now().UTC().Unix()},
	} {
		if err := store.UpsertStreamMetric(ctx, metric); err != nil {
			t.Fatalf("UpsertStreamMetric(%q) error = %v", metric.ItemKey, err)
		}
	}
	var clearResp struct {
		Deleted int64 `json:"deleted"`
	}
	doJSON(t, mux, http.MethodPost, "/api/admin/jobs/auto-prioritize/cache/clear", nil, http.StatusOK, &clearResp)
	if clearResp.Deleted != 2 {
		t.Fatalf("POST /api/admin/jobs/auto-prioritize/cache/clear deleted = %d, want 2", clearResp.Deleted)
	}
	_, err = store.GetStreamMetric(ctx, "src:test:1")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetStreamMetric(src:test:1) error = %v, want sql.ErrNoRows", err)
	}
	_, err = store.GetStreamMetric(ctx, "src:test:2")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetStreamMetric(src:test:2) error = %v, want sql.ErrNoRows", err)
	}

	rec := doRaw(t, mux, http.MethodPost, "/api/admin/jobs/playlist-sync/run", nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /api/admin/jobs/playlist-sync/run status = %d, want %d, body = %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	var runQueued struct {
		RunID int64 `json:"run_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &runQueued); err != nil {
		t.Fatalf("decode run queue response: %v", err)
	}
	if runQueued.RunID <= 0 {
		t.Fatalf("queued run id = %d, expected > 0", runQueued.RunID)
	}

	run := waitForJobRun(t, mux, runQueued.RunID)
	if run.Status != jobs.StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, jobs.StatusSuccess)
	}

	autoRec := doRaw(t, mux, http.MethodPost, "/api/admin/jobs/auto-prioritize/run", nil)
	if autoRec.Code != http.StatusAccepted {
		t.Fatalf("POST /api/admin/jobs/auto-prioritize/run status = %d, want %d, body = %s", autoRec.Code, http.StatusAccepted, autoRec.Body.String())
	}
	var autoRunQueued struct {
		RunID int64 `json:"run_id"`
	}
	if err := json.Unmarshal(autoRec.Body.Bytes(), &autoRunQueued); err != nil {
		t.Fatalf("decode auto run queue response: %v", err)
	}
	if autoRunQueued.RunID <= 0 {
		t.Fatalf("queued auto run id = %d, expected > 0", autoRunQueued.RunID)
	}

	autoRun := waitForJobRun(t, mux, autoRunQueued.RunID)
	if autoRun.Status != jobs.StatusSuccess {
		t.Fatalf("auto run status = %q, want %q", autoRun.Status, jobs.StatusSuccess)
	}
	if autoRun.AnalysisErrorBuckets["http_429"] != 1 {
		t.Fatalf("auto run analysis_error_buckets[http_429] = %d, want 1", autoRun.AnalysisErrorBuckets["http_429"])
	}
	if autoRun.AnalysisErrorBuckets["decode_ffprobe_json"] != 1 {
		t.Fatalf("auto run analysis_error_buckets[decode_ffprobe_json] = %d, want 1", autoRun.AnalysisErrorBuckets["decode_ffprobe_json"])
	}

	var runList struct {
		Runs []jobs.Run `json:"runs"`
	}
	doJSON(t, mux, http.MethodGet, "/api/admin/jobs?name=playlist_sync&limit=5", nil, http.StatusOK, &runList)
	if len(runList.Runs) == 0 {
		t.Fatal("GET /api/admin/jobs expected at least one run")
	}
	if runList.Runs[0].JobName != jobs.JobPlaylistSync {
		t.Fatalf("listed run job_name = %q, want %q", runList.Runs[0].JobName, jobs.JobPlaylistSync)
	}

	dvrRunID, err := store.CreateRun(ctx, jobs.JobDVRLineupSync, jobs.TriggerSchedule, time.Now().UTC().Unix())
	if err != nil {
		t.Fatalf("CreateRun(dvr_lineup_sync) error = %v", err)
	}
	if err := store.FinishRun(
		ctx,
		dvrRunID,
		jobs.StatusSuccess,
		"",
		"dvr lineup sync complete",
		time.Now().UTC().Unix(),
	); err != nil {
		t.Fatalf("FinishRun(dvr_lineup_sync) error = %v", err)
	}

	var dvrRunList struct {
		Runs []jobs.Run `json:"runs"`
	}
	doJSON(t, mux, http.MethodGet, "/api/admin/jobs?name=dvr_lineup_sync&limit=5", nil, http.StatusOK, &dvrRunList)
	if len(dvrRunList.Runs) == 0 {
		t.Fatal("GET /api/admin/jobs name=dvr_lineup_sync expected at least one run")
	}
	if dvrRunList.Runs[0].JobName != jobs.JobDVRLineupSync {
		t.Fatalf("listed dvr run job_name = %q, want %q", dvrRunList.Runs[0].JobName, jobs.JobDVRLineupSync)
	}

	badName := doRaw(t, mux, http.MethodGet, "/api/admin/jobs?name=nope", nil)
	if badName.Code != http.StatusBadRequest {
		t.Fatalf("GET /api/admin/jobs invalid name status = %d, want %d", badName.Code, http.StatusBadRequest)
	}
}

func TestAdminRunPlaylistSyncSourceScopeValidation(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	disabled := false
	disabledSource, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Disabled Source",
		PlaylistURL: "http://disabled.example/playlist.m3u",
		TunerCount:  1,
		Enabled:     &disabled,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(disabled) error = %v", err)
	}
	if disabledSource.Enabled {
		t.Fatalf("disabled source enabled = %v, want false", disabledSource.Enabled)
	}

	enabled := true
	scopedSource, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Scoped Source",
		PlaylistURL: "http://scoped.example/playlist.m3u",
		TunerCount:  1,
		Enabled:     &enabled,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(scoped) error = %v", err)
	}

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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

	playlistJob := func(ctx context.Context, run *jobs.RunContext) error {
		if sourceID, ok := jobs.PlaylistSyncSourceIDFromContext(ctx); ok {
			return run.SetSummary(ctx, fmt.Sprintf("scoped source_id=%d", sourceID))
		}
		return run.SetSummary(ctx, "scoped source_id=all")
	}

	handler, err := NewAdminHandler(store, channelsSvc, AutomationDeps{
		Settings:  store,
		Scheduler: schedulerSvc,
		Runner:    runner,
		JobFuncs: map[string]jobs.JobFunc{
			jobs.JobPlaylistSync:   playlistJob,
			jobs.JobAutoPrioritize: func(context.Context, *jobs.RunContext) error { return nil },
		},
	})
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	playlistRuntime := &capturingPlaylistSourceRuntime{}
	handler.SetPlaylistSourceRuntime(playlistRuntime)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	invalid := doRaw(t, mux, http.MethodPost, "/api/admin/jobs/playlist-sync/run?source_id=abc", nil)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("POST playlist-sync with invalid source_id status = %d, want %d", invalid.Code, http.StatusBadRequest)
	}

	missing := doRaw(t, mux, http.MethodPost, "/api/admin/jobs/playlist-sync/run?source_id=9999", nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("POST playlist-sync with missing source_id status = %d, want %d", missing.Code, http.StatusNotFound)
	}

	disabledRec := doRaw(t, mux, http.MethodPost, "/api/admin/jobs/playlist-sync/run?source_id="+strconv.FormatInt(disabledSource.SourceID, 10), nil)
	if disabledRec.Code != http.StatusBadRequest {
		t.Fatalf("POST playlist-sync with disabled source status = %d, want %d", disabledRec.Code, http.StatusBadRequest)
	}

	scopedRec := doRaw(t, mux, http.MethodPost, "/api/admin/jobs/playlist-sync/run?source_id="+strconv.FormatInt(scopedSource.SourceID, 10), nil)
	if scopedRec.Code != http.StatusAccepted {
		t.Fatalf("POST playlist-sync with scoped source status = %d, want %d, body = %s", scopedRec.Code, http.StatusAccepted, scopedRec.Body.String())
	}
	var queued struct {
		RunID    int64 `json:"run_id"`
		SourceID int64 `json:"source_id"`
	}
	if err := json.Unmarshal(scopedRec.Body.Bytes(), &queued); err != nil {
		t.Fatalf("decode scoped queue response: %v", err)
	}
	if queued.SourceID != scopedSource.SourceID {
		t.Fatalf("queued source_id = %d, want %d", queued.SourceID, scopedSource.SourceID)
	}
	if queued.RunID <= 0 {
		t.Fatalf("queued run_id = %d, want > 0", queued.RunID)
	}

	run := waitForJobRun(t, mux, queued.RunID)
	if run.Status != jobs.StatusSuccess {
		t.Fatalf("scoped run status = %q, want %q", run.Status, jobs.StatusSuccess)
	}
	if !strings.Contains(run.Summary, fmt.Sprintf("source_id=%d", scopedSource.SourceID)) {
		t.Fatalf("scoped run summary = %q, want source_id marker", run.Summary)
	}
}

func TestAdminPlaylistSourceCRUDRoutes(t *testing.T) {
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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

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
	playlistRuntime := &capturingPlaylistSourceRuntime{}
	handler.SetPlaylistSourceRuntime(playlistRuntime)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var initial struct {
		PlaylistSources []playlistSourceResponse `json:"playlist_sources"`
	}
	doJSON(t, mux, http.MethodGet, "/api/admin/playlist-sources", nil, http.StatusOK, &initial)
	if len(initial.PlaylistSources) != 1 {
		t.Fatalf("initial playlist source count = %d, want 1", len(initial.PlaylistSources))
	}
	if initial.PlaylistSources[0].SourceID != 1 {
		t.Fatalf("initial primary source_id = %d, want 1", initial.PlaylistSources[0].SourceID)
	}
	if strings.TrimSpace(initial.PlaylistSources[0].SourceKey) == "" {
		t.Fatal("initial primary source_key = empty, want non-empty source_key in response")
	}

	var created playlistSourceResponse
	doJSON(t, mux, http.MethodPost, "/api/admin/playlist-sources", map[string]any{
		"name":         "Backup Source",
		"playlist_url": "http://example.com/backup.m3u",
		"tuner_count":  3,
		"enabled":      true,
	}, http.StatusCreated, &created)
	if created.SourceID <= 1 {
		t.Fatalf("created source_id = %d, want > 1", created.SourceID)
	}
	if strings.TrimSpace(created.SourceKey) == "" {
		t.Fatal("created source_key = empty, want non-empty source_key in response")
	}
	if got, want := playlistRuntime.ReloadCalls(), 1; got != want {
		t.Fatalf("runtime reload calls after create = %d, want %d", got, want)
	}

	var fetched playlistSourceResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/playlist-sources/"+strconv.FormatInt(created.SourceID, 10), nil, http.StatusOK, &fetched)
	if fetched.Name != "Backup Source" {
		t.Fatalf("fetched source name = %q, want Backup Source", fetched.Name)
	}
	if strings.TrimSpace(fetched.SourceKey) == "" {
		t.Fatal("fetched source_key = empty, want non-empty source_key in response")
	}

	var updated playlistSourceResponse
	doJSON(t, mux, http.MethodPut, "/api/admin/playlist-sources/"+strconv.FormatInt(created.SourceID, 10), map[string]any{
		"name":        "Backup Source Updated",
		"tuner_count": 5,
		"enabled":     false,
	}, http.StatusOK, &updated)
	if updated.Name != "Backup Source Updated" || updated.TunerCount != 5 || updated.Enabled {
		t.Fatalf("updated source = %+v, want name/tuners/enabled updates applied", updated)
	}
	if got, want := playlistRuntime.ReloadCalls(), 2; got != want {
		t.Fatalf("runtime reload calls after update = %d, want %d", got, want)
	}

	reorderRemoved := doRaw(t, mux, http.MethodPut, "/api/admin/playlist-sources/order", map[string]any{
		"source_ids": []int64{created.SourceID, 1},
	})
	if reorderRemoved.Code != http.StatusBadRequest {
		t.Fatalf("PUT /api/admin/playlist-sources/order status = %d, want %d (reorder endpoint removed)", reorderRemoved.Code, http.StatusBadRequest)
	}

	rec := doRaw(t, mux, http.MethodDelete, "/api/admin/playlist-sources/1", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("DELETE primary source status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	lastEnabledDisable := doRaw(t, mux, http.MethodPut, "/api/admin/playlist-sources/1", map[string]any{
		"enabled": false,
	})
	if lastEnabledDisable.Code != http.StatusBadRequest {
		t.Fatalf("PUT disable last enabled source status = %d, want %d", lastEnabledDisable.Code, http.StatusBadRequest)
	}
	if got, want := playlistRuntime.ReloadCalls(), 2; got != want {
		t.Fatalf("runtime reload calls after failed mutation = %d, want %d", got, want)
	}

	var deleteResp struct {
		Deleted  bool  `json:"deleted"`
		SourceID int64 `json:"source_id"`
	}
	doJSON(t, mux, http.MethodDelete, "/api/admin/playlist-sources/"+strconv.FormatInt(created.SourceID, 10), nil, http.StatusOK, &deleteResp)
	if !deleteResp.Deleted || deleteResp.SourceID != created.SourceID {
		t.Fatalf("delete response = %+v, want deleted=true source_id=%d", deleteResp, created.SourceID)
	}
	if got, want := playlistRuntime.ReloadCalls(), 3; got != want {
		t.Fatalf("runtime reload calls after delete = %d, want %d", got, want)
	}

	missing := doRaw(t, mux, http.MethodGet, "/api/admin/playlist-sources/"+strconv.FormatInt(created.SourceID, 10), nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("GET deleted source status = %d, want %d", missing.Code, http.StatusNotFound)
	}
}

func TestAdminAutomationPlaylistSourcesPayloadRoundTrip(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	created, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Second Source",
		PlaylistURL: "http://example.com/second.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(second) error = %v", err)
	}

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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

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
	playlistRuntime := &capturingPlaylistSourceRuntime{}
	handler.SetPlaylistSourceRuntime(playlistRuntime)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var state automationStateResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &state)
	if len(state.PlaylistSources) != 2 {
		t.Fatalf("initial automation playlist source count = %d, want 2", len(state.PlaylistSources))
	}

	var updated automationStateResponse
	doJSON(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_url": "http://example.com/primary-updated.m3u",
		"playlist_sources": []map[string]any{
			{
				"source_id":    int64(1),
				"name":         "Primary",
				"playlist_url": "http://example.com/primary-updated.m3u",
				"tuner_count":  6,
				"enabled":      true,
			},
			{
				"source_id":    created.SourceID,
				"name":         "Second Source Updated",
				"playlist_url": "http://example.com/second-updated.m3u",
				"tuner_count":  4,
				"enabled":      false,
			},
		},
	}, http.StatusOK, &updated)
	if updated.PlaylistURL != "http://example.com/primary-updated.m3u" {
		t.Fatalf("updated playlist_url = %q, want primary URL alias", updated.PlaylistURL)
	}
	if len(updated.PlaylistSources) != 2 {
		t.Fatalf("updated playlist source count = %d, want 2", len(updated.PlaylistSources))
	}
	if updated.PlaylistSources[0].SourceID != 1 || updated.PlaylistSources[1].SourceID != created.SourceID {
		t.Fatalf("updated playlist source order = [%d,%d], want [1,%d]", updated.PlaylistSources[0].SourceID, updated.PlaylistSources[1].SourceID, created.SourceID)
	}
	if updated.PlaylistSources[1].Name != "Second Source Updated" || updated.PlaylistSources[1].Enabled {
		t.Fatalf("updated secondary source = %+v, want renamed and disabled", updated.PlaylistSources[1])
	}
	if updated.PlaylistSources[0].TunerCount != 6 {
		t.Fatalf("updated primary tuner_count = %d, want 6", updated.PlaylistSources[0].TunerCount)
	}
	if got, want := playlistRuntime.ReloadCalls(), 1; got != want {
		t.Fatalf("runtime reload calls after automation playlist_sources update = %d, want %d", got, want)
	}

	legacyPlaylistURL, err := store.GetSetting(ctx, sqlite.SettingPlaylistURL)
	if err != nil {
		t.Fatalf("GetSetting(playlist.url) error = %v", err)
	}
	if legacyPlaylistURL != "http://example.com/primary-updated.m3u" {
		t.Fatalf("playlist.url setting = %q, want mirrored primary URL", legacyPlaylistURL)
	}

	mismatch := doRaw(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_url": "http://example.com/legacy-mismatch.m3u",
		"playlist_sources": []map[string]any{
			{
				"source_id":    int64(1),
				"name":         "Primary",
				"playlist_url": "http://example.com/primary-updated.m3u",
				"tuner_count":  6,
				"enabled":      true,
			},
			{
				"source_id":    created.SourceID,
				"name":         "Second Source Updated",
				"playlist_url": "http://example.com/second-updated.m3u",
				"tuner_count":  4,
				"enabled":      false,
			},
		},
	})
	if mismatch.Code != http.StatusBadRequest {
		t.Fatalf("PUT /api/admin/automation with playlist_url mismatch status = %d, want %d", mismatch.Code, http.StatusBadRequest)
	}

	var canonicalLegacyMatch automationStateResponse
	doJSON(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_url": "HTTP://EXAMPLE.COM/primary-updated.m3u",
		"playlist_sources": []map[string]any{
			{
				"source_id":    int64(1),
				"name":         "Primary",
				"playlist_url": "http://example.com/primary-updated.m3u",
				"tuner_count":  6,
				"enabled":      true,
			},
			{
				"source_id":    created.SourceID,
				"name":         "Second Source Updated",
				"playlist_url": "http://example.com/second-updated.m3u",
				"tuner_count":  4,
				"enabled":      false,
			},
		},
	}, http.StatusOK, &canonicalLegacyMatch)

	duplicateCanonicalURL := doRaw(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_sources": []map[string]any{
			{
				"source_id":    int64(1),
				"name":         "Primary",
				"playlist_url": "http://example.com/primary-updated.m3u",
				"tuner_count":  6,
				"enabled":      true,
			},
			{
				"source_id":    created.SourceID,
				"name":         "Second Source Updated",
				"playlist_url": "HTTP://EXAMPLE.COM/primary-updated.m3u",
				"tuner_count":  4,
				"enabled":      false,
			},
		},
	})
	if duplicateCanonicalURL.Code != http.StatusBadRequest {
		t.Fatalf("PUT /api/admin/automation duplicate canonical playlist_url status = %d, want %d", duplicateCanonicalURL.Code, http.StatusBadRequest)
	}

	reorderAttempt := doRaw(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_sources": []map[string]any{
			{
				"source_id":    created.SourceID,
				"name":         "Second Source Updated",
				"playlist_url": "http://example.com/second-updated.m3u",
				"tuner_count":  4,
				"enabled":      false,
			},
			{
				"source_id":    int64(1),
				"name":         "Primary",
				"playlist_url": "http://example.com/primary-updated.m3u",
				"tuner_count":  6,
				"enabled":      true,
			},
		},
	})
	if reorderAttempt.Code != http.StatusBadRequest {
		t.Fatalf("PUT /api/admin/automation reorder attempt status = %d, want %d", reorderAttempt.Code, http.StatusBadRequest)
	}
	if !strings.Contains(strings.ToLower(reorderAttempt.Body.String()), "preserve existing source order") {
		t.Fatalf("PUT /api/admin/automation reorder attempt body = %q, want preserve-order error", reorderAttempt.Body.String())
	}
}

func TestAdminPlaylistSourceCRUDMutationsReturnEventualConsistencyContractWhenRuntimeReloadFails(t *testing.T) {
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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

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
	playlistRuntime := &capturingPlaylistSourceRuntime{reloadErr: fmt.Errorf("injected runtime reload failure")}
	handler.SetPlaylistSourceRuntime(playlistRuntime)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	assertRuntimeApplyFailure := func(rec *httptest.ResponseRecorder, operation string, sourceID int64) map[string]any {
		t.Helper()
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("runtime apply failure status = %d, want %d, body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
		}
		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode runtime apply failure response: %v; body=%s", err, rec.Body.String())
		}
		if got, want := payload["error"], "playlist_source_runtime_apply_failed"; got != want {
			t.Fatalf("error = %#v, want %q", got, want)
		}
		if got, want := payload["operation"], operation; got != want {
			t.Fatalf("operation = %#v, want %q", got, want)
		}
		if got, want := payload["persisted"], true; got != want {
			t.Fatalf("persisted = %#v, want %v", got, want)
		}
		if got, want := payload["runtime_applied"], false; got != want {
			t.Fatalf("runtime_applied = %#v, want %v", got, want)
		}
		if got, want := payload["consistency"], "eventual"; got != want {
			t.Fatalf("consistency = %#v, want %q", got, want)
		}
		runtimeErr, ok := payload["runtime_error"].(string)
		if !ok || !strings.Contains(runtimeErr, "injected runtime reload failure") {
			t.Fatalf("runtime_error = %#v, want injected runtime failure details", payload["runtime_error"])
		}
		if sourceID > 0 {
			gotID, ok := payload["source_id"].(float64)
			if !ok || int64(gotID) != sourceID {
				t.Fatalf("source_id = %#v, want %d", payload["source_id"], sourceID)
			}
		}
		return payload
	}

	createRec := doRaw(t, mux, http.MethodPost, "/api/admin/playlist-sources", map[string]any{
		"name":         "Runtime Fail Source",
		"playlist_url": "http://example.com/runtime-fail.m3u",
		"tuner_count":  2,
		"enabled":      true,
	})
	assertRuntimeApplyFailure(createRec, "create_playlist_source", 0)
	if got, want := playlistRuntime.ReloadCalls(), 1; got != want {
		t.Fatalf("runtime reload calls = %d, want %d", got, want)
	}

	sources, err := store.ListPlaylistSources(ctx)
	if err != nil {
		t.Fatalf("ListPlaylistSources() error = %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("len(ListPlaylistSources) after failed runtime reload = %d, want 2 (mutation persisted)", len(sources))
	}
	var created playlist.PlaylistSource
	for _, source := range sources {
		if source.SourceID == 1 {
			continue
		}
		created = source
		break
	}
	if created.SourceID <= 1 {
		t.Fatalf("created source_id = %d, want > 1", created.SourceID)
	}

	updateRec := doRaw(
		t,
		mux,
		http.MethodPut,
		"/api/admin/playlist-sources/"+strconv.FormatInt(created.SourceID, 10),
		map[string]any{
			"name":        "Runtime Fail Source Updated",
			"tuner_count": 7,
			"enabled":     true,
		},
	)
	assertRuntimeApplyFailure(updateRec, "update_playlist_source", created.SourceID)
	if got, want := playlistRuntime.ReloadCalls(), 2; got != want {
		t.Fatalf("runtime reload calls after update = %d, want %d", got, want)
	}
	updated, err := store.GetPlaylistSource(ctx, created.SourceID)
	if err != nil {
		t.Fatalf("GetPlaylistSource(updated) error = %v", err)
	}
	if updated.Name != "Runtime Fail Source Updated" || updated.TunerCount != 7 {
		t.Fatalf("updated source after runtime apply failure = %+v, want persisted update", updated)
	}

	deleteRec := doRaw(
		t,
		mux,
		http.MethodDelete,
		"/api/admin/playlist-sources/"+strconv.FormatInt(created.SourceID, 10),
		nil,
	)
	assertRuntimeApplyFailure(deleteRec, "delete_playlist_source", created.SourceID)
	if got, want := playlistRuntime.ReloadCalls(), 3; got != want {
		t.Fatalf("runtime reload calls after delete = %d, want %d", got, want)
	}
	if _, err := store.GetPlaylistSource(ctx, created.SourceID); !errors.Is(err, playlist.ErrPlaylistSourceNotFound) {
		t.Fatalf("GetPlaylistSource(deleted) error = %v, want ErrPlaylistSourceNotFound", err)
	}
}

func TestAdminAutomationPlaylistSourcesMutationReturnsEventualConsistencyContractWhenRuntimeReloadFails(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	created, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Runtime Bulk Source",
		PlaylistURL: "http://example.com/runtime-bulk.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(runtime bulk) error = %v", err)
	}

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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

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
	playlistRuntime := &capturingPlaylistSourceRuntime{reloadErr: fmt.Errorf("injected runtime reload failure")}
	handler.SetPlaylistSourceRuntime(playlistRuntime)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	rec := doRaw(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_url": "http://example.com/primary-runtime-failed.m3u",
		"playlist_sources": []map[string]any{
			{
				"source_id":    int64(1),
				"name":         "Primary Runtime Updated",
				"playlist_url": "http://example.com/primary-runtime-failed.m3u",
				"tuner_count":  6,
				"enabled":      true,
			},
			{
				"source_id":    created.SourceID,
				"name":         "Runtime Bulk Source Updated",
				"playlist_url": "http://example.com/runtime-bulk-updated.m3u",
				"tuner_count":  5,
				"enabled":      true,
			},
		},
	})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("PUT /api/admin/automation status = %d, want %d, body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if got, want := playlistRuntime.ReloadCalls(), 1; got != want {
		t.Fatalf("runtime reload calls = %d, want %d", got, want)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode runtime apply failure response: %v; body=%s", err, rec.Body.String())
	}
	if got, want := payload["error"], "playlist_source_runtime_apply_failed"; got != want {
		t.Fatalf("error = %#v, want %q", got, want)
	}
	if got, want := payload["operation"], "update_playlist_sources"; got != want {
		t.Fatalf("operation = %#v, want %q", got, want)
	}
	if got, want := payload["persisted"], true; got != want {
		t.Fatalf("persisted = %#v, want %v", got, want)
	}
	if got, want := payload["runtime_applied"], false; got != want {
		t.Fatalf("runtime_applied = %#v, want %v", got, want)
	}
	if got, want := payload["consistency"], "eventual"; got != want {
		t.Fatalf("consistency = %#v, want %q", got, want)
	}
	runtimeErr, ok := payload["runtime_error"].(string)
	if !ok || !strings.Contains(runtimeErr, "injected runtime reload failure") {
		t.Fatalf("runtime_error = %#v, want injected runtime failure details", payload["runtime_error"])
	}
	sourceIDs, ok := payload["source_ids"].([]any)
	if !ok {
		t.Fatalf("source_ids payload type = %T, want []any", payload["source_ids"])
	}
	if got, want := len(sourceIDs), 2; got != want {
		t.Fatalf("len(source_ids) = %d, want %d", got, want)
	}
	if got, want := int64(sourceIDs[0].(float64)), int64(1); got != want {
		t.Fatalf("source_ids[0] = %d, want %d", got, want)
	}
	if got, want := int64(sourceIDs[1].(float64)), created.SourceID; got != want {
		t.Fatalf("source_ids[1] = %d, want %d", got, want)
	}

	var after automationStateResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &after)
	if after.PlaylistURL != "http://example.com/primary-runtime-failed.m3u" {
		t.Fatalf("playlist_url after runtime apply failure = %q, want persisted primary URL", after.PlaylistURL)
	}
	if len(after.PlaylistSources) != 2 {
		t.Fatalf("playlist source count after runtime apply failure = %d, want 2", len(after.PlaylistSources))
	}
	if after.PlaylistSources[0].Name != "Primary Runtime Updated" || after.PlaylistSources[0].TunerCount != 6 {
		t.Fatalf("primary source after runtime apply failure = %+v, want persisted updates", after.PlaylistSources[0])
	}
	if after.PlaylistSources[1].Name != "Runtime Bulk Source Updated" || after.PlaylistSources[1].TunerCount != 5 {
		t.Fatalf("secondary source after runtime apply failure = %+v, want persisted updates", after.PlaylistSources[1])
	}
}

func TestAdminAutomationPlaylistSourcesBulkApplyFailureRollsBack(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	sourceTwo, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Bulk Source Two",
		PlaylistURL: "http://example.com/bulk-two.m3u",
		TunerCount:  2,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(source two) error = %v", err)
	}
	sourceThree, err := store.CreatePlaylistSource(ctx, playlist.PlaylistSourceCreate{
		Name:        "Bulk Source Three",
		PlaylistURL: "http://example.com/bulk-three.m3u",
		TunerCount:  3,
	})
	if err != nil {
		t.Fatalf("CreatePlaylistSource(source three) error = %v", err)
	}

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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

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

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var before automationStateResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &before)
	if len(before.PlaylistSources) != 3 {
		t.Fatalf("initial playlist source count = %d, want 3", len(before.PlaylistSources))
	}

	rec := doRaw(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_url": "http://example.com/primary-rolled-back.m3u",
		"playlist_sources": []map[string]any{
			{
				"source_id":    int64(1),
				"name":         "Primary Updated",
				"playlist_url": "http://example.com/primary-rolled-back.m3u",
				"tuner_count":  8,
				"enabled":      true,
			},
			{
				"source_id":    sourceTwo.SourceID,
				"name":         "Bulk Source Two Updated",
				"playlist_url": "http://example.com/bulk-three.m3u",
				"tuner_count":  4,
				"enabled":      true,
			},
			{
				"source_id":    sourceThree.SourceID,
				"name":         "Bulk Source Three Updated",
				"playlist_url": "http://example.com/bulk-two.m3u",
				"tuner_count":  5,
				"enabled":      true,
			},
		},
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("PUT /api/admin/automation bulk conflict status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	var after automationStateResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &after)

	if after.PlaylistURL != before.PlaylistURL {
		t.Fatalf("playlist_url after failed bulk update = %q, want %q", after.PlaylistURL, before.PlaylistURL)
	}
	if len(after.PlaylistSources) != len(before.PlaylistSources) {
		t.Fatalf("playlist source count after failed bulk update = %d, want %d", len(after.PlaylistSources), len(before.PlaylistSources))
	}
	for i := range before.PlaylistSources {
		if after.PlaylistSources[i] != before.PlaylistSources[i] {
			t.Fatalf("playlist source[%d] changed after failed bulk update = %+v, want %+v", i, after.PlaylistSources[i], before.PlaylistSources[i])
		}
	}
}

func TestAdminAutomationListJobRunsHighOffsetPagination(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	jobNames := []string{
		jobs.JobPlaylistSync,
		jobs.JobAutoPrioritize,
		jobs.JobDVRLineupSync,
	}
	countsByJob := map[string]int{
		jobs.JobPlaylistSync:   0,
		jobs.JobAutoPrioritize: 0,
		jobs.JobDVRLineupSync:  0,
	}
	const totalRuns = 2400
	const startedBase int64 = 1_700_100_000
	for i := 0; i < totalRuns; i++ {
		jobName := jobNames[i%len(jobNames)]
		countsByJob[jobName]++

		runID, err := store.CreateRun(ctx, jobName, jobs.TriggerSchedule, startedBase+int64(i/2))
		if err != nil {
			t.Fatalf("CreateRun(%d) error = %v", i, err)
		}
		if i%5 != 0 {
			if err := store.FinishRun(ctx, runID, jobs.StatusSuccess, "", "", startedBase+int64(i/2)+1); err != nil {
				t.Fatalf("FinishRun(%d) error = %v", i, err)
			}
		}
	}

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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

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

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	const (
		unfilteredLimit  = 75
		unfilteredOffset = 1800
	)
	var unfilteredResp struct {
		Name   string     `json:"name"`
		Limit  int        `json:"limit"`
		Offset int        `json:"offset"`
		Runs   []jobs.Run `json:"runs"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		"/api/admin/jobs?limit="+strconv.Itoa(unfilteredLimit)+"&offset="+strconv.Itoa(unfilteredOffset),
		nil,
		http.StatusOK,
		&unfilteredResp,
	)
	if unfilteredResp.Name != "" {
		t.Fatalf("unfiltered response name = %q, want empty", unfilteredResp.Name)
	}
	if unfilteredResp.Limit != unfilteredLimit {
		t.Fatalf("unfiltered response limit = %d, want %d", unfilteredResp.Limit, unfilteredLimit)
	}
	if unfilteredResp.Offset != unfilteredOffset {
		t.Fatalf("unfiltered response offset = %d, want %d", unfilteredResp.Offset, unfilteredOffset)
	}
	if len(unfilteredResp.Runs) != unfilteredLimit {
		t.Fatalf("unfiltered response len(runs) = %d, want %d", len(unfilteredResp.Runs), unfilteredLimit)
	}
	assertSortedRunsResponse(t, unfilteredResp.Runs)

	const filteredLimit = 60
	filteredOffset := countsByJob[jobs.JobPlaylistSync] - (filteredLimit + 15)
	if filteredOffset < 0 {
		filteredOffset = 0
	}
	var filteredResp struct {
		Name   string     `json:"name"`
		Limit  int        `json:"limit"`
		Offset int        `json:"offset"`
		Runs   []jobs.Run `json:"runs"`
	}
	doJSON(
		t,
		mux,
		http.MethodGet,
		"/api/admin/jobs?name="+jobs.JobPlaylistSync+"&limit="+strconv.Itoa(filteredLimit)+"&offset="+strconv.Itoa(filteredOffset),
		nil,
		http.StatusOK,
		&filteredResp,
	)
	if filteredResp.Name != jobs.JobPlaylistSync {
		t.Fatalf("filtered response name = %q, want %q", filteredResp.Name, jobs.JobPlaylistSync)
	}
	if filteredResp.Limit != filteredLimit {
		t.Fatalf("filtered response limit = %d, want %d", filteredResp.Limit, filteredLimit)
	}
	if filteredResp.Offset != filteredOffset {
		t.Fatalf("filtered response offset = %d, want %d", filteredResp.Offset, filteredOffset)
	}
	if len(filteredResp.Runs) != filteredLimit {
		t.Fatalf("filtered response len(runs) = %d, want %d", len(filteredResp.Runs), filteredLimit)
	}
	for _, run := range filteredResp.Runs {
		if run.JobName != jobs.JobPlaylistSync {
			t.Fatalf("filtered response run job_name = %q, want %q", run.JobName, jobs.JobPlaylistSync)
		}
	}
	assertSortedRunsResponse(t, filteredResp.Runs)
}

func TestAdminAutomationPutRollsBackOnSchedulerReloadFailure(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.SetSettings(ctx, map[string]string{
		sqlite.SettingPlaylistURL:           "http://initial.example.com/playlist.m3u",
		sqlite.SettingAnalyzerSampleSeconds: "3",
	}); err != nil {
		t.Fatalf("SetSettings(seed) error = %v", err)
	}

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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	// Intentionally do not register auto_prioritize callback so reloading an enabled
	// auto_prioritize schedule fails after persistence and exercises rollback.
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

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

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var before automationStateResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &before)

	rec := doRaw(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_url": "http://updated.example.com/playlist.m3u",
		"timezone":     "UTC",
		"playlist_sync": map[string]any{
			"enabled":   true,
			"cron_spec": "*/20 * * * *",
		},
		"auto_prioritize": map[string]any{
			"enabled":   true,
			"cron_spec": "15 2 * * *",
		},
		"analyzer": map[string]any{
			"sample_seconds": 9,
		},
	})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("PUT /api/admin/automation status = %d, want %d, body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}

	var after automationStateResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &after)

	if after.PlaylistURL != before.PlaylistURL {
		t.Fatalf("playlist_url after rollback = %q, want %q", after.PlaylistURL, before.PlaylistURL)
	}
	if after.Timezone != before.Timezone {
		t.Fatalf("timezone after rollback = %q, want %q", after.Timezone, before.Timezone)
	}
	if after.PlaylistSync.Enabled != before.PlaylistSync.Enabled {
		t.Fatalf("playlist_sync.enabled after rollback = %v, want %v", after.PlaylistSync.Enabled, before.PlaylistSync.Enabled)
	}
	if after.PlaylistSync.CronSpec != before.PlaylistSync.CronSpec {
		t.Fatalf("playlist_sync.cron_spec after rollback = %q, want %q", after.PlaylistSync.CronSpec, before.PlaylistSync.CronSpec)
	}
	if after.AutoPrioritize.Enabled != before.AutoPrioritize.Enabled {
		t.Fatalf("auto_prioritize.enabled after rollback = %v, want %v", after.AutoPrioritize.Enabled, before.AutoPrioritize.Enabled)
	}
	if after.AutoPrioritize.CronSpec != before.AutoPrioritize.CronSpec {
		t.Fatalf("auto_prioritize.cron_spec after rollback = %q, want %q", after.AutoPrioritize.CronSpec, before.AutoPrioritize.CronSpec)
	}
	if after.Analyzer.SampleSeconds != before.Analyzer.SampleSeconds {
		t.Fatalf("analyzer.sample_seconds after rollback = %d, want %d", after.Analyzer.SampleSeconds, before.Analyzer.SampleSeconds)
	}
}

func TestAdminAutomationPutDetachesRuntimeApplyFromRequestCancellation(t *testing.T) {
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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()

	cancelingStore := &cancelingAutomationSettingsStore{
		AutomationSettingsStore: store,
		cancelRequest:           cancelRequest,
		cancelAfterSetCalls:     1,
		failOnCanceledContext:   true,
	}
	contextAwareScheduler := &contextAwareAutomationScheduler{
		AutomationScheduler:   schedulerSvc,
		failOnCanceledContext: true,
	}

	handler, err := NewAdminHandler(store, channelsSvc, AutomationDeps{
		Settings:  cancelingStore,
		Scheduler: contextAwareScheduler,
		Runner:    runner,
		JobFuncs: map[string]jobs.JobFunc{
			jobs.JobPlaylistSync:   func(context.Context, *jobs.RunContext) error { return nil },
			jobs.JobAutoPrioritize: func(context.Context, *jobs.RunContext) error { return nil },
		},
	})
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	payload, err := json.Marshal(map[string]any{
		"timezone": "UTC",
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/api/admin/automation", bytes.NewReader(payload)).WithContext(requestCtx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("PUT /api/admin/automation status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	setCalls := cancelingStore.SetCalls()
	if setCalls != 1 {
		t.Fatalf("SetSettings() calls = %d, want 1", setCalls)
	}
	loadCalls, canceledLoadCalls := contextAwareScheduler.LoadCounts()
	if loadCalls != 1 {
		t.Fatalf("LoadFromSettings() calls = %d, want 1", loadCalls)
	}
	if canceledLoadCalls != 0 {
		t.Fatalf("canceled LoadFromSettings() calls = %d, want 0", canceledLoadCalls)
	}

	var state automationStateResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &state)
	if state.Timezone != "UTC" {
		t.Fatalf("timezone after canceled request apply = %q, want UTC", state.Timezone)
	}
}

func TestAdminAutomationPutRollbackDetachesFromRequestCancellation(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.SetSettings(ctx, map[string]string{
		sqlite.SettingPlaylistURL: "http://initial.example.com/playlist.m3u",
	}); err != nil {
		t.Fatalf("SetSettings(seed) error = %v", err)
	}

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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()

	cancelingStore := &cancelingAutomationSettingsStore{
		AutomationSettingsStore: store,
		cancelRequest:           cancelRequest,
		cancelAfterSetCalls:     1,
		failOnCanceledContext:   true,
	}
	contextAwareScheduler := &contextAwareAutomationScheduler{
		AutomationScheduler:   schedulerSvc,
		firstLoadErr:          fmt.Errorf("injected scheduler reload failure"),
		failOnCanceledContext: true,
	}

	handler, err := NewAdminHandler(store, channelsSvc, AutomationDeps{
		Settings:  cancelingStore,
		Scheduler: contextAwareScheduler,
		Runner:    runner,
		JobFuncs: map[string]jobs.JobFunc{
			jobs.JobPlaylistSync:   func(context.Context, *jobs.RunContext) error { return nil },
			jobs.JobAutoPrioritize: func(context.Context, *jobs.RunContext) error { return nil },
		},
	})
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var before automationStateResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &before)

	payload, err := json.Marshal(map[string]any{
		"playlist_url": "http://updated.example.com/playlist.m3u",
		"timezone":     "UTC",
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/api/admin/automation", bytes.NewReader(payload)).WithContext(requestCtx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("PUT /api/admin/automation status = %d, want %d, body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "apply automation runtime settings: injected scheduler reload failure") {
		t.Fatalf("PUT /api/admin/automation body = %q, want apply failure marker", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "rollback failed") {
		t.Fatalf("PUT /api/admin/automation body = %q, rollback should succeed under detached context", rec.Body.String())
	}

	setCalls := cancelingStore.SetCalls()
	if setCalls != 2 {
		t.Fatalf("SetSettings() calls = %d, want 2 (persist + rollback)", setCalls)
	}
	loadCalls, canceledLoadCalls := contextAwareScheduler.LoadCounts()
	if loadCalls != 2 {
		t.Fatalf("LoadFromSettings() calls = %d, want 2 (apply + rollback)", loadCalls)
	}
	if canceledLoadCalls != 0 {
		t.Fatalf("canceled LoadFromSettings() calls = %d, want 0", canceledLoadCalls)
	}

	var after automationStateResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &after)
	if after.PlaylistURL != before.PlaylistURL {
		t.Fatalf("playlist_url after rollback = %q, want %q", after.PlaylistURL, before.PlaylistURL)
	}
	if after.Timezone != before.Timezone {
		t.Fatalf("timezone after rollback = %q, want %q", after.Timezone, before.Timezone)
	}
}

func TestAdminAutomationPutConcurrentRollbackDoesNotClobberWinner(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.SetSettings(ctx, map[string]string{
		sqlite.SettingPlaylistURL: "http://initial.example.com/playlist.m3u",
	}); err != nil {
		t.Fatalf("SetSettings(seed) error = %v", err)
	}

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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

	releaseFirstLoad := make(chan struct{})
	blockingScheduler := &blockingAutomationScheduler{
		AutomationScheduler: schedulerSvc,
		firstLoadStarted:    make(chan struct{}),
		releaseFirstLoad:    releaseFirstLoad,
		firstLoadErr:        fmt.Errorf("injected scheduler reload failure"),
	}

	handler, err := NewAdminHandler(store, channelsSvc, AutomationDeps{
		Settings:  store,
		Scheduler: blockingScheduler,
		Runner:    runner,
		JobFuncs: map[string]jobs.JobFunc{
			jobs.JobPlaylistSync:   func(context.Context, *jobs.RunContext) error { return nil },
			jobs.JobAutoPrioritize: func(context.Context, *jobs.RunContext) error { return nil },
		},
	})
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	mustPayload := func(body any) []byte {
		t.Helper()
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("json.Marshal() error = %v", err)
		}
		return payload
	}
	doPut := func(payload []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPut, "/api/admin/automation", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	losingPayload := mustPayload(map[string]any{
		"playlist_url": "http://losing.example.com/playlist.m3u",
		"timezone":     "UTC",
	})
	winningPayload := mustPayload(map[string]any{
		"playlist_url": "http://winning.example.com/playlist.m3u",
	})

	losingRespCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		losingRespCh <- doPut(losingPayload)
	}()

	select {
	case <-blockingScheduler.firstLoadStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first scheduler reload attempt to block")
	}

	winningRespCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		winningRespCh <- doPut(winningPayload)
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

	close(releaseFirstLoad)

	var losingRec *httptest.ResponseRecorder
	select {
	case losingRec = <-losingRespCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for losing request response")
	}
	if losingRec.Code != http.StatusInternalServerError {
		t.Fatalf("losing PUT /api/admin/automation status = %d, want %d, body=%s", losingRec.Code, http.StatusInternalServerError, losingRec.Body.String())
	}
	if !strings.Contains(losingRec.Body.String(), "apply automation runtime settings: injected scheduler reload failure") {
		t.Fatalf("losing PUT /api/admin/automation body = %q, want scheduler reload failure marker", losingRec.Body.String())
	}

	var winningRec *httptest.ResponseRecorder
	select {
	case winningRec = <-winningRespCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for winning request response")
	}
	if winningRec.Code != http.StatusOK {
		t.Fatalf("winning PUT /api/admin/automation status = %d, want %d, body=%s", winningRec.Code, http.StatusOK, winningRec.Body.String())
	}

	playlistURL, err := store.GetSetting(ctx, sqlite.SettingPlaylistURL)
	if err != nil {
		t.Fatalf("GetSetting(playlist.url) error = %v", err)
	}
	if playlistURL != "http://winning.example.com/playlist.m3u" {
		t.Fatalf("playlist.url after concurrent rollback = %q, want winner value", playlistURL)
	}
}

func TestAdminAutomationPutSetSettingsFailureDoesNotMutateState(t *testing.T) {
	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if err := store.SetSettings(ctx, map[string]string{
		sqlite.SettingPlaylistURL:           "http://initial.example.com/playlist.m3u",
		sqlite.SettingAnalyzerSampleSeconds: "3",
	}); err != nil {
		t.Fatalf("SetSettings(seed) error = %v", err)
	}

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
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()
	defer func() { <-schedulerSvc.Stop().Done() }()

	failingStore := &failingAutomationSettingsStore{
		AutomationSettingsStore: store,
		failSet:                 true,
	}

	handler, err := NewAdminHandler(store, channelsSvc, AutomationDeps{
		Settings:  failingStore,
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

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var before automationStateResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &before)

	rec := doRaw(t, mux, http.MethodPut, "/api/admin/automation", map[string]any{
		"playlist_url": "http://updated.example.com/playlist.m3u",
		"timezone":     "UTC",
		"playlist_sync": map[string]any{
			"enabled":   true,
			"cron_spec": "*/20 * * * *",
		},
		"analyzer": map[string]any{
			"sample_seconds": 9,
		},
	})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("PUT /api/admin/automation status = %d, want %d, body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if failingStore.setCalls != 1 {
		t.Fatalf("SetSettings() calls = %d, want 1", failingStore.setCalls)
	}

	var after automationStateResponse
	doJSON(t, mux, http.MethodGet, "/api/admin/automation", nil, http.StatusOK, &after)

	if after.PlaylistURL != before.PlaylistURL {
		t.Fatalf("playlist_url after failed persist = %q, want %q", after.PlaylistURL, before.PlaylistURL)
	}
	if after.Timezone != before.Timezone {
		t.Fatalf("timezone after failed persist = %q, want %q", after.Timezone, before.Timezone)
	}
	if after.PlaylistSync.Enabled != before.PlaylistSync.Enabled {
		t.Fatalf("playlist_sync.enabled after failed persist = %v, want %v", after.PlaylistSync.Enabled, before.PlaylistSync.Enabled)
	}
	if after.PlaylistSync.CronSpec != before.PlaylistSync.CronSpec {
		t.Fatalf("playlist_sync.cron_spec after failed persist = %q, want %q", after.PlaylistSync.CronSpec, before.PlaylistSync.CronSpec)
	}
	if after.Analyzer.SampleSeconds != before.Analyzer.SampleSeconds {
		t.Fatalf("analyzer.sample_seconds after failed persist = %d, want %d", after.Analyzer.SampleSeconds, before.Analyzer.SampleSeconds)
	}
}

type failingAutomationSettingsStore struct {
	AutomationSettingsStore
	failSet  bool
	setCalls int
}

func (s *failingAutomationSettingsStore) SetSettings(ctx context.Context, values map[string]string) error {
	s.setCalls++
	if s.failSet {
		return fmt.Errorf("injected SetSettings failure")
	}
	return s.AutomationSettingsStore.SetSettings(ctx, values)
}

type cancelingAutomationSettingsStore struct {
	AutomationSettingsStore
	cancelRequest         context.CancelFunc
	cancelAfterSetCalls   int
	failOnCanceledContext bool

	mu       sync.Mutex
	setCalls int
	canceled bool
}

func (s *cancelingAutomationSettingsStore) SetSettings(ctx context.Context, values map[string]string) error {
	if s.failOnCanceledContext {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if err := s.AutomationSettingsStore.SetSettings(ctx, values); err != nil {
		return err
	}

	shouldCancel := false
	s.mu.Lock()
	s.setCalls++
	if s.cancelRequest != nil &&
		s.cancelAfterSetCalls > 0 &&
		!s.canceled &&
		s.setCalls >= s.cancelAfterSetCalls {
		s.canceled = true
		shouldCancel = true
	}
	s.mu.Unlock()

	if shouldCancel {
		s.cancelRequest()
	}
	return nil
}

func (s *cancelingAutomationSettingsStore) SetCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setCalls
}

type contextAwareAutomationScheduler struct {
	AutomationScheduler
	firstLoadErr          error
	failOnCanceledContext bool

	mu                sync.Mutex
	loadCalls         int
	canceledLoadCalls int
}

func (s *contextAwareAutomationScheduler) LoadFromSettings(ctx context.Context) error {
	s.mu.Lock()
	s.loadCalls++
	call := s.loadCalls
	s.mu.Unlock()

	if call == 1 && s.firstLoadErr != nil {
		return s.firstLoadErr
	}

	if s.failOnCanceledContext {
		if err := ctx.Err(); err != nil {
			s.mu.Lock()
			s.canceledLoadCalls++
			s.mu.Unlock()
			return err
		}
	}

	return s.AutomationScheduler.LoadFromSettings(ctx)
}

func (s *contextAwareAutomationScheduler) LoadCounts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadCalls, s.canceledLoadCalls
}

type blockingAutomationScheduler struct {
	AutomationScheduler
	firstLoadStarted    chan struct{}
	releaseFirstLoad    <-chan struct{}
	firstLoadErr        error
	firstLoadSignalOnce sync.Once
	loadMu              sync.Mutex
	loadCalls           int
}

func (s *blockingAutomationScheduler) LoadFromSettings(ctx context.Context) error {
	s.loadMu.Lock()
	s.loadCalls++
	call := s.loadCalls
	s.loadMu.Unlock()

	if call == 1 {
		s.firstLoadSignalOnce.Do(func() {
			close(s.firstLoadStarted)
		})
		select {
		case <-s.releaseFirstLoad:
		case <-ctx.Done():
			return ctx.Err()
		}
		if s.firstLoadErr != nil {
			return s.firstLoadErr
		}
	}

	return s.AutomationScheduler.LoadFromSettings(ctx)
}

type capturingPlaylistSourceRuntime struct {
	reloadErr error

	mu          sync.Mutex
	reloadCalls int
}

func (r *capturingPlaylistSourceRuntime) ReloadPlaylistSources(context.Context) error {
	r.mu.Lock()
	r.reloadCalls++
	err := r.reloadErr
	r.mu.Unlock()
	return err
}

func (r *capturingPlaylistSourceRuntime) ReloadCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reloadCalls
}

func TestAdminManualRunDetachesFromRequestLifecycle(t *testing.T) {
	const jobRuntime = 200 * time.Millisecond

	runStarted := make(chan struct{}, 1)
	runFinished := make(chan struct{}, 1)
	playlistJob := func(ctx context.Context, _ *jobs.RunContext) error {
		select {
		case runStarted <- struct{}{}:
		default:
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jobRuntime):
		}

		select {
		case runFinished <- struct{}{}:
		default:
		}
		return nil
	}

	server, runner := newAutomationHTTPServer(t, playlistJob, func(context.Context, *jobs.RunContext) error { return nil })

	resp, err := http.Post(server.URL+"/api/admin/jobs/playlist-sync/run", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST /api/admin/jobs/playlist-sync/run error = %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("POST /api/admin/jobs/playlist-sync/run status = %d, want %d, body = %s", resp.StatusCode, http.StatusAccepted, string(body))
	}

	var queued struct {
		RunID int64 `json:"run_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&queued); err != nil {
		resp.Body.Close()
		t.Fatalf("decode queued run response: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close response body: %v", err)
	}
	if queued.RunID <= 0 {
		t.Fatalf("queued run id = %d, want > 0", queued.RunID)
	}

	select {
	case <-runStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for manual run to start")
	}

	run := waitForRunnerRun(t, runner, queued.RunID)
	if run.Status != jobs.StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, jobs.StatusSuccess)
	}

	select {
	case <-runFinished:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for detached manual run to finish")
	}
}

func TestAdminManualRunSurvivesClientDisconnect(t *testing.T) {
	const jobRuntime = 200 * time.Millisecond

	playlistJob := func(ctx context.Context, _ *jobs.RunContext) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jobRuntime):
			return nil
		}
	}

	server, runner := newAutomationHTTPServer(t, playlistJob, func(context.Context, *jobs.RunContext) error { return nil })

	addr := strings.TrimPrefix(server.URL, "http://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial test server: %v", err)
	}
	request := fmt.Sprintf("POST /api/admin/jobs/playlist-sync/run HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}", addr)
	if _, err := conn.Write([]byte(request)); err != nil {
		conn.Close()
		t.Fatalf("write raw request: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close raw client connection: %v", err)
	}

	runID := waitForLatestRunID(t, runner, jobs.JobPlaylistSync)
	run := waitForRunnerRun(t, runner, runID)
	if run.Status != jobs.StatusSuccess {
		t.Fatalf("run status after client disconnect = %q, want %q", run.Status, jobs.StatusSuccess)
	}
}

func newAutomationHTTPServer(t *testing.T, playlistJob jobs.JobFunc, autoJob jobs.JobFunc) (*httptest.Server, *jobs.Runner) {
	t.Helper()

	ctx := context.Background()

	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	channelsSvc := channels.NewService(store)

	runner, err := jobs.NewRunner(store)
	if err != nil {
		store.Close()
		t.Fatalf("NewRunner() error = %v", err)
	}

	schedulerSvc, err := scheduler.New(store, nil)
	if err != nil {
		runner.Close()
		store.Close()
		t.Fatalf("scheduler.New() error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobPlaylistSync, func(context.Context, string) error { return nil }); err != nil {
		runner.Close()
		store.Close()
		t.Fatalf("RegisterJob(playlist_sync) error = %v", err)
	}
	if err := schedulerSvc.RegisterJob(jobs.JobAutoPrioritize, func(context.Context, string) error { return nil }); err != nil {
		runner.Close()
		store.Close()
		t.Fatalf("RegisterJob(auto_prioritize) error = %v", err)
	}
	if err := schedulerSvc.LoadFromSettings(ctx); err != nil {
		runner.Close()
		store.Close()
		t.Fatalf("LoadFromSettings() error = %v", err)
	}
	schedulerSvc.Start()

	handler, err := NewAdminHandler(store, channelsSvc, AutomationDeps{
		Settings:  store,
		Scheduler: schedulerSvc,
		Runner:    runner,
		JobFuncs: map[string]jobs.JobFunc{
			jobs.JobPlaylistSync:   playlistJob,
			jobs.JobAutoPrioritize: autoJob,
		},
	})
	if err != nil {
		<-schedulerSvc.Stop().Done()
		runner.Close()
		store.Close()
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	server := httptest.NewServer(mux)
	t.Cleanup(func() {
		server.Close()
		<-schedulerSvc.Stop().Done()
		runner.Close()
		store.Close()
	})

	return server, runner
}

func waitForLatestRunID(t *testing.T, runner *jobs.Runner, jobName string) int64 {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runs, err := runner.ListRuns(context.Background(), jobName, 1, 0)
		if err != nil {
			t.Fatalf("ListRuns(%q) error = %v", jobName, err)
		}
		if len(runs) > 0 {
			return runs[0].RunID
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q run to be created", jobName)
	return 0
}

func waitForRunnerRun(t *testing.T, runner *jobs.Runner, runID int64) jobs.Run {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := runner.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatalf("GetRun(%d) error = %v", runID, err)
		}
		if run.Status != jobs.StatusRunning {
			return run
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for run %d to finish", runID)
	return jobs.Run{}
}

func waitForJobRun(t *testing.T, mux *http.ServeMux, runID int64) jobs.Run {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var run jobs.Run
		doJSON(t, mux, http.MethodGet, "/api/admin/jobs/"+strconv.FormatInt(runID, 10), nil, http.StatusOK, &run)
		if run.Status != jobs.StatusRunning {
			return run
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for run %d to finish", runID)
	return jobs.Run{}
}

func assertSortedRunsResponse(t *testing.T, runs []jobs.Run) {
	t.Helper()

	for i := 1; i < len(runs); i++ {
		prev := runs[i-1]
		curr := runs[i]
		if prev.StartedAt < curr.StartedAt {
			t.Fatalf(
				"runs out of order at %d: prev(started_at=%d run_id=%d) < curr(started_at=%d run_id=%d)",
				i,
				prev.StartedAt,
				prev.RunID,
				curr.StartedAt,
				curr.RunID,
			)
		}
		if prev.StartedAt == curr.StartedAt && prev.RunID < curr.RunID {
			t.Fatalf(
				"runs tie-order mismatch at %d: prev(run_id=%d) < curr(run_id=%d) with started_at=%d",
				i,
				prev.RunID,
				curr.RunID,
				prev.StartedAt,
			)
		}
	}
}
