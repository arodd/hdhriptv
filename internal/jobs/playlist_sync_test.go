package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/arodd/hdhriptv/internal/reconcile"
)

type fakePlaylistSettings struct {
	value string
	err   error
}

func (f *fakePlaylistSettings) GetSetting(context.Context, string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return f.value, nil
}

type fakeRefresher struct {
	lastURL string
	count   int
	err     error
}

func (f *fakeRefresher) Refresh(_ context.Context, playlistURL string) (int, error) {
	f.lastURL = playlistURL
	if f.err != nil {
		return 0, f.err
	}
	return f.count, nil
}

type fakeReconciler struct {
	count  int
	result reconcile.Result
	err    error
}

func (f *fakeReconciler) CountChannels(context.Context) (int, error) {
	return f.count, nil
}

func (f *fakeReconciler) Reconcile(_ context.Context, onProgress func(cur, max int) error) (reconcile.Result, error) {
	for i := 1; i <= f.count; i++ {
		if onProgress != nil {
			if err := onProgress(i, f.count); err != nil {
				return reconcile.Result{}, err
			}
		}
	}
	if f.err != nil {
		return reconcile.Result{}, f.err
	}
	return f.result, nil
}

type fakeLineupReloader struct {
	calls int
	err   error
}

func (f *fakeLineupReloader) ReloadLineup(context.Context) error {
	f.calls++
	return f.err
}

type fakeLineupReloaderWithStatus struct {
	calls      int
	err        error
	reloaded   bool
	skipped    bool
	skipReason string
}

func (f *fakeLineupReloaderWithStatus) ReloadLineup(context.Context) error {
	f.calls++
	return f.err
}

func (f *fakeLineupReloaderWithStatus) ReloadLineupForPlaylistSync(context.Context) (bool, bool, string, error) {
	f.calls++
	if f.err != nil {
		return false, false, "", f.err
	}
	return f.reloaded, f.skipped, f.skipReason, nil
}

func TestPlaylistSyncJobRunSuccess(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{value: "http://example.com/playlist.m3u"}
	refresher := &fakeRefresher{count: 17}
	reconciler := &fakeReconciler{
		count: 2,
		result: reconcile.Result{
			ChannelsTotal:            2,
			ChannelsProcessed:        2,
			SourcesAdded:             4,
			SourcesAlreadySeen:       6,
			DynamicChannelsProcessed: 1,
			DynamicSourcesAdded:      2,
			DynamicSourcesRemoved:    1,
			DynamicGuideNamesUpdated: 3,
		},
	}
	job, err := NewPlaylistSyncJob(settings, refresher, reconciler)
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if refresher.lastURL != "http://example.com/playlist.m3u" {
		t.Fatalf("refresher lastURL = %q, want playlist URL", refresher.lastURL)
	}
	if run.ProgressCur != 2 || run.ProgressMax != 2 {
		t.Fatalf("run progress = %d/%d, want 2/2", run.ProgressCur, run.ProgressMax)
	}
	if !strings.Contains(run.Summary, "items=17") ||
		!strings.Contains(run.Summary, "added_sources=4") ||
		!strings.Contains(run.Summary, "dynamic_channels=1") ||
		!strings.Contains(run.Summary, "dynamic_added=2") ||
		!strings.Contains(run.Summary, "dynamic_removed=1") ||
		!strings.Contains(run.Summary, "dynamic_name_updates=3") ||
		!strings.Contains(run.Summary, "dvr_lineup_reloaded=false") ||
		!strings.Contains(run.Summary, "dvr_lineup_reload_status=disabled") ||
		!strings.Contains(run.Summary, "dvr_lineup_reload_skip_reason=none") {
		t.Fatalf("run summary = %q, expected refresh + reconcile counts", run.Summary)
	}
}

func TestPlaylistSyncJobRunTriggersDVRLineupReload(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{value: "http://example.com/playlist.m3u"}
	refresher := &fakeRefresher{count: 11}
	reconciler := &fakeReconciler{
		count:  1,
		result: reconcile.Result{ChannelsTotal: 1, ChannelsProcessed: 1},
	}
	job, err := NewPlaylistSyncJob(settings, refresher, reconciler)
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}
	reloader := &fakeLineupReloader{}
	job.SetPostSyncLineupReloader(reloader)

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if reloader.calls != 1 {
		t.Fatalf("reloader.calls = %d, want 1", reloader.calls)
	}
	if !strings.Contains(run.Summary, "dvr_lineup_reloaded=true") {
		t.Fatalf("run summary = %q, expected dvr_lineup_reloaded=true", run.Summary)
	}
	if !strings.Contains(run.Summary, "dvr_lineup_reload_status=reloaded") {
		t.Fatalf("run summary = %q, expected dvr_lineup_reload_status=reloaded", run.Summary)
	}
	if !strings.Contains(run.Summary, "dvr_lineup_reload_skip_reason=none") {
		t.Fatalf("run summary = %q, expected dvr_lineup_reload_skip_reason=none", run.Summary)
	}
}

func TestPlaylistSyncJobRunFailsWhenDVRLineupReloadFails(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{value: "http://example.com/playlist.m3u"}
	refresher := &fakeRefresher{count: 11}
	reconciler := &fakeReconciler{
		count:  1,
		result: reconcile.Result{ChannelsTotal: 1, ChannelsProcessed: 1},
	}
	job, err := NewPlaylistSyncJob(settings, refresher, reconciler)
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}
	job.SetPostSyncLineupReloader(&fakeLineupReloader{err: fmt.Errorf("reload failed")})

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusError {
		t.Fatalf("run status = %q, want %q", run.Status, StatusError)
	}
	if !strings.Contains(run.ErrorMessage, "reload dvr lineup after playlist sync") {
		t.Fatalf("run.ErrorMessage = %q, want lineup reload failure context", run.ErrorMessage)
	}
}

func TestPlaylistSyncJobRunSkipsDVRLineupReloadWhenReloaderReportsSkipped(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{value: "http://example.com/playlist.m3u"}
	refresher := &fakeRefresher{count: 11}
	reconciler := &fakeReconciler{
		count:  1,
		result: reconcile.Result{ChannelsTotal: 1, ChannelsProcessed: 1},
	}
	job, err := NewPlaylistSyncJob(settings, refresher, reconciler)
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}
	reloader := &fakeLineupReloaderWithStatus{
		reloaded:   false,
		skipped:    true,
		skipReason: "missing_jellyfin_api_token",
	}
	job.SetPostSyncLineupReloader(reloader)

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if reloader.calls != 1 {
		t.Fatalf("reloader.calls = %d, want 1", reloader.calls)
	}
	if !strings.Contains(run.Summary, "dvr_lineup_reloaded=false") {
		t.Fatalf("run summary = %q, expected dvr_lineup_reloaded=false", run.Summary)
	}
	if !strings.Contains(run.Summary, "dvr_lineup_reload_status=skipped") {
		t.Fatalf("run summary = %q, expected dvr_lineup_reload_status=skipped", run.Summary)
	}
	if !strings.Contains(run.Summary, "dvr_lineup_reload_skip_reason=missing_jellyfin_api_token") {
		t.Fatalf("run summary = %q, expected missing_jellyfin_api_token skip reason", run.Summary)
	}
}

func TestPlaylistSyncJobRunMarksPartialDVRLineupReloadStatus(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{value: "http://example.com/playlist.m3u"}
	refresher := &fakeRefresher{count: 11}
	reconciler := &fakeReconciler{
		count:  1,
		result: reconcile.Result{ChannelsTotal: 1, ChannelsProcessed: 1},
	}
	job, err := NewPlaylistSyncJob(settings, refresher, reconciler)
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}
	reloader := &fakeLineupReloaderWithStatus{
		reloaded:   true,
		skipped:    true,
		skipReason: "jellyfin:missing_jellyfin_api_token",
	}
	job.SetPostSyncLineupReloader(reloader)

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if reloader.calls != 1 {
		t.Fatalf("reloader.calls = %d, want 1", reloader.calls)
	}
	if !strings.Contains(run.Summary, "dvr_lineup_reloaded=true") {
		t.Fatalf("run summary = %q, expected dvr_lineup_reloaded=true", run.Summary)
	}
	if !strings.Contains(run.Summary, "dvr_lineup_reload_status=partial") {
		t.Fatalf("run summary = %q, expected dvr_lineup_reload_status=partial", run.Summary)
	}
	if !strings.Contains(run.Summary, "dvr_lineup_reload_skip_reason=jellyfin:missing_jellyfin_api_token") {
		t.Fatalf("run summary = %q, expected jellyfin skip reason", run.Summary)
	}
}

func TestPlaylistSyncJobThrottlesProgressPersistence(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{value: "http://example.com/playlist.m3u"}
	refresher := &fakeRefresher{count: 42}
	reconciler := &fakeReconciler{
		count: 25,
		result: reconcile.Result{
			ChannelsTotal:      25,
			ChannelsProcessed:  25,
			SourcesAdded:       9,
			SourcesAlreadySeen: 14,
		},
	}
	job, err := NewPlaylistSyncJob(settings, refresher, reconciler)
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if run.ProgressCur != 25 || run.ProgressMax != 25 {
		t.Fatalf("run progress = %d/%d, want 25/25", run.ProgressCur, run.ProgressMax)
	}
	if !strings.Contains(run.Summary, "channels processed=25/25") {
		t.Fatalf("run summary = %q, expected final reconcile summary", run.Summary)
	}

	progressWrites := store.progressWrites(runID)
	if progressWrites >= reconciler.count {
		t.Fatalf("UpdateRunProgress writes = %d, want less than channel count %d", progressWrites, reconciler.count)
	}
	if progressWrites > 10 {
		t.Fatalf("UpdateRunProgress writes = %d, expected throttled write volume", progressWrites)
	}
}

func TestPlaylistSyncJobRequiresPlaylistURL(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	job, err := NewPlaylistSyncJob(
		&fakePlaylistSettings{err: sql.ErrNoRows},
		&fakeRefresher{count: 1},
		&fakeReconciler{count: 0},
	)
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusError {
		t.Fatalf("run status = %q, want %q", run.Status, StatusError)
	}
	if !strings.Contains(strings.ToLower(run.ErrorMessage), "playlist url") {
		t.Fatalf("error message = %q, want playlist URL failure", run.ErrorMessage)
	}
}
