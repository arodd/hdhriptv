package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/dvr"
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/reconcile"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakePlaylistSettings struct {
	value    string
	valueErr error

	sources    []playlist.PlaylistSource
	sourcesErr error
}

func (f *fakePlaylistSettings) GetSetting(context.Context, string) (string, error) {
	if f.valueErr != nil {
		return "", f.valueErr
	}
	return f.value, nil
}

func (f *fakePlaylistSettings) ListPlaylistSources(context.Context) ([]playlist.PlaylistSource, error) {
	if f.sourcesErr != nil {
		return nil, f.sourcesErr
	}
	return append([]playlist.PlaylistSource(nil), f.sources...), nil
}

type fakeRefresher struct {
	mu sync.Mutex

	calls []playlist.PlaylistSource
	errs  map[int64]error
	items map[int64]int

	startedCh chan int64
	blockCh   <-chan struct{}

	inFlight    int
	maxInFlight int
}

func (f *fakeRefresher) RefreshForSource(ctx context.Context, source playlist.PlaylistSource) (int, error) {
	f.mu.Lock()
	f.calls = append(f.calls, source)
	f.inFlight++
	if f.inFlight > f.maxInFlight {
		f.maxInFlight = f.inFlight
	}
	startedCh := f.startedCh
	blockCh := f.blockCh
	f.mu.Unlock()

	if startedCh != nil {
		select {
		case startedCh <- source.SourceID:
		default:
		}
	}
	if blockCh != nil {
		select {
		case <-ctx.Done():
		case <-blockCh:
		}
	}

	defer func() {
		f.mu.Lock()
		if f.inFlight > 0 {
			f.inFlight--
		}
		f.mu.Unlock()
	}()

	if err := f.errs[source.SourceID]; err != nil {
		return 0, err
	}
	return f.items[source.SourceID], nil
}

func (f *fakeRefresher) snapshotCalls() []playlist.PlaylistSource {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]playlist.PlaylistSource(nil), f.calls...)
}

func (f *fakeRefresher) maxConcurrentCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxInFlight
}

type fakeReconciler struct {
	count    int
	countErr error
	result   reconcile.Result
	err      error

	countCalls     int
	reconcileCalls int
}

func (f *fakeReconciler) CountChannels(context.Context) (int, error) {
	f.countCalls++
	if f.countErr != nil {
		return 0, f.countErr
	}
	return f.count, nil
}

func (f *fakeReconciler) Reconcile(_ context.Context, onProgress func(cur, max int) error) (reconcile.Result, error) {
	f.reconcileCalls++
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
	calls      int
	err        error
	outcome    dvr.ReloadOutcome
	hasOutcome bool
}

func (f *fakeLineupReloader) ReloadLineupForPlaylistSyncOutcome(context.Context) (dvr.ReloadOutcome, error) {
	f.calls++
	if f.err != nil {
		return dvr.ReloadOutcome{}, f.err
	}
	if !f.hasOutcome {
		return dvr.ReloadOutcome{
			Reloaded: true,
			Status:   dvr.ReloadStatusReloaded,
		}, nil
	}
	return f.outcome, nil
}

func decodePlaylistSyncRunSummary(t *testing.T, raw string) playlistSyncRunSummary {
	t.Helper()

	var summary playlistSyncRunSummary
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &summary); err != nil {
		t.Fatalf("decode playlist sync summary %q: %v", raw, err)
	}
	return summary
}

func TestPlaylistSyncJobRunSuccessSequentialSources(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		value: "http://legacy.example/playlist.m3u",
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://stale-primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
			{
				SourceID:    2,
				SourceKey:   "backup01",
				Name:        "Backup",
				PlaylistURL: "http://backup.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  1,
			},
		},
	}
	refresher := &fakeRefresher{
		items: map[int64]int{
			1: 11,
			2: 6,
		},
	}
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
	if run.ProgressCur != 2 || run.ProgressMax != 2 {
		t.Fatalf("run progress = %d/%d, want 2/2", run.ProgressCur, run.ProgressMax)
	}
	refresherCalls := refresher.snapshotCalls()
	if len(refresherCalls) != 2 {
		t.Fatalf("refresher calls = %d, want 2", len(refresherCalls))
	}
	if refresherCalls[0].SourceID != 1 || refresherCalls[1].SourceID != 2 {
		t.Fatalf("refresher source order = [%d,%d], want [1,2]", refresherCalls[0].SourceID, refresherCalls[1].SourceID)
	}
	if got, want := refresherCalls[0].PlaylistURL, "http://legacy.example/playlist.m3u"; got != want {
		t.Fatalf("primary source playlist_url = %q, want %q", got, want)
	}
	summary := decodePlaylistSyncRunSummary(t, run.Summary)
	if summary.SchemaVersion != playlistSyncSummarySchemaVersion {
		t.Fatalf("summary schema_version = %d, want %d", summary.SchemaVersion, playlistSyncSummarySchemaVersion)
	}
	if summary.RequestedSourceID != "all" {
		t.Fatalf("summary requested_source_id = %q, want all", summary.RequestedSourceID)
	}
	if summary.Sources.Attempted != 2 || summary.Sources.Succeeded != 2 || summary.Sources.Failed != 0 {
		t.Fatalf(
			"summary source counts = attempted:%d succeeded:%d failed:%d, want 2/2/0",
			summary.Sources.Attempted,
			summary.Sources.Succeeded,
			summary.Sources.Failed,
		)
	}
	if summary.Refresh.Items != 17 {
		t.Fatalf("summary refresh.items = %d, want 17", summary.Refresh.Items)
	}
}

func TestPlaylistSyncJobRunUsesBoundedSourceRefreshConcurrency(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
			{
				SourceID:    2,
				SourceKey:   "backup01",
				Name:        "Backup A",
				PlaylistURL: "http://backup-a.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  1,
			},
			{
				SourceID:    3,
				SourceKey:   "backup02",
				Name:        "Backup B",
				PlaylistURL: "http://backup-b.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  2,
			},
		},
	}

	startedCh := make(chan int64, 8)
	blockCh := make(chan struct{})
	refresher := &fakeRefresher{
		items: map[int64]int{
			1: 3,
			2: 4,
			3: 5,
		},
		startedCh: startedCh,
		blockCh:   blockCh,
	}
	reconciler := &fakeReconciler{
		count:  1,
		result: reconcile.Result{ChannelsTotal: 1, ChannelsProcessed: 1},
	}

	job, err := NewPlaylistSyncJob(settings, refresher, reconciler)
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}
	job.SetSourceRefreshConcurrency(2)

	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-startedCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for concurrent source refresh starts")
		}
	}
	select {
	case <-startedCh:
		t.Fatal("third source refresh started before worker slots were released")
	case <-time.After(50 * time.Millisecond):
	}
	if got, want := refresher.maxConcurrentCalls(), 2; got != want {
		t.Fatalf("max concurrent source refreshes = %d, want %d", got, want)
	}

	close(blockCh)

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	summary := decodePlaylistSyncRunSummary(t, run.Summary)
	if summary.Sources.Attempted != 3 || summary.Sources.Succeeded != 3 || summary.Sources.Failed != 0 {
		t.Fatalf(
			"summary source counts = attempted:%d succeeded:%d failed:%d, want 3/3/0",
			summary.Sources.Attempted,
			summary.Sources.Succeeded,
			summary.Sources.Failed,
		)
	}
}

func TestNormalizeSourceRefreshConcurrencyClampsBounds(t *testing.T) {
	t.Parallel()

	if got, want := normalizeSourceRefreshConcurrency(0), 1; got != want {
		t.Fatalf("normalizeSourceRefreshConcurrency(0) = %d, want %d", got, want)
	}
	if got, want := normalizeSourceRefreshConcurrency(-3), 1; got != want {
		t.Fatalf("normalizeSourceRefreshConcurrency(-3) = %d, want %d", got, want)
	}
	if got, want := normalizeSourceRefreshConcurrency(999), maxPlaylistSyncSourceConcurrency; got != want {
		t.Fatalf("normalizeSourceRefreshConcurrency(999) = %d, want %d", got, want)
	}
}

func TestPlaylistSyncJobRunPartialSourceFailureStillSucceeds(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		value: "",
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
			{
				SourceID:    2,
				SourceKey:   "backup01",
				Name:        "Backup",
				PlaylistURL: "http://backup.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  1,
			},
		},
	}
	refresher := &fakeRefresher{
		items: map[int64]int{
			1: 9,
		},
		errs: map[int64]error{
			2: fmt.Errorf("source fetch failed"),
		},
	}
	reconciler := &fakeReconciler{
		count:  1,
		result: reconcile.Result{ChannelsTotal: 1, ChannelsProcessed: 1},
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
	if reconciler.reconcileCalls != 1 {
		t.Fatalf("reconciler.Reconcile calls = %d, want 1", reconciler.reconcileCalls)
	}
	summary := decodePlaylistSyncRunSummary(t, run.Summary)
	if summary.Sources.Attempted != 2 || summary.Sources.Succeeded != 1 || summary.Sources.Failed != 1 {
		t.Fatalf(
			"summary source counts = attempted:%d succeeded:%d failed:%d, want 2/1/1",
			summary.Sources.Attempted,
			summary.Sources.Succeeded,
			summary.Sources.Failed,
		)
	}
	if summary.Refresh.Items != 9 {
		t.Fatalf("summary refresh.items = %d, want 9", summary.Refresh.Items)
	}
	failedFound := false
	for _, outcome := range summary.Sources.Outcomes {
		if outcome.SourceID != 2 {
			continue
		}
		failedFound = true
		if outcome.Status != "error" {
			t.Fatalf("source 2 status = %q, want error", outcome.Status)
		}
		if !strings.Contains(outcome.Error, "source fetch failed") {
			t.Fatalf("source 2 error = %q, want source fetch failed", outcome.Error)
		}
	}
	if !failedFound {
		t.Fatalf("summary outcomes missing failed source 2: %+v", summary.Sources.Outcomes)
	}
}

func TestPlaylistSyncJobRunUpdatesPerSourcePrometheusMetrics(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	primaryKey := "metrics_primary_source"
	backupKey := "metrics_backup_source"
	settings := &fakePlaylistSettings{
		value: "",
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   primaryKey,
				Name:        "Primary Metrics",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
			{
				SourceID:    2,
				SourceKey:   backupKey,
				Name:        "Backup Metrics",
				PlaylistURL: "http://backup.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  1,
			},
		},
	}
	refresher := &fakeRefresher{
		items: map[int64]int{
			1: 13,
		},
		errs: map[int64]error{
			2: fmt.Errorf("backup source failed"),
		},
	}
	reconciler := &fakeReconciler{
		count:  1,
		result: reconcile.Result{ChannelsTotal: 1, ChannelsProcessed: 1},
	}
	job, err := NewPlaylistSyncJob(settings, refresher, reconciler)
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}

	backupErrorsBefore := testutil.ToFloat64(playlistSyncSourceErrorsMetric.WithLabelValues(backupKey))
	runID, err := runner.Start(context.Background(), JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	if got := testutil.ToFloat64(playlistSyncSourceItemsMetric.WithLabelValues(primaryKey)); got != 13 {
		t.Fatalf("primary items gauge = %.0f, want 13", got)
	}
	if got := testutil.ToFloat64(playlistSyncSourceItemsMetric.WithLabelValues(backupKey)); got != 0 {
		t.Fatalf("backup items gauge = %.0f, want 0 after failure", got)
	}
	if got := testutil.ToFloat64(playlistSyncSourceErrorsMetric.WithLabelValues(backupKey)) - backupErrorsBefore; got != 1 {
		t.Fatalf("backup errors counter delta = %.0f, want 1", got)
	}
}

func TestPlaylistSyncJobRunAllSourceFailuresReturnError(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		value: "",
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
			{
				SourceID:    2,
				SourceKey:   "backup01",
				Name:        "Backup",
				PlaylistURL: "http://backup.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  1,
			},
		},
	}
	refresher := &fakeRefresher{
		errs: map[int64]error{
			1: fmt.Errorf("primary failed"),
			2: fmt.Errorf("backup failed"),
		},
	}
	reconciler := &fakeReconciler{
		count:  1,
		result: reconcile.Result{ChannelsTotal: 1, ChannelsProcessed: 1},
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
	if run.Status != StatusError {
		t.Fatalf("run status = %q, want %q", run.Status, StatusError)
	}
	if reconciler.reconcileCalls != 0 {
		t.Fatalf("reconciler.Reconcile calls = %d, want 0", reconciler.reconcileCalls)
	}
	summary := decodePlaylistSyncRunSummary(t, run.Summary)
	if summary.Sources.Attempted != 2 || summary.Sources.Succeeded != 0 || summary.Sources.Failed != 2 {
		t.Fatalf(
			"summary source counts = attempted:%d succeeded:%d failed:%d, want 2/0/2",
			summary.Sources.Attempted,
			summary.Sources.Succeeded,
			summary.Sources.Failed,
		)
	}
	if summary.DVRLineup.Status != dvr.ReloadStatusSkipped {
		t.Fatalf("summary dvr_lineup.status = %q, want %q", summary.DVRLineup.Status, dvr.ReloadStatusSkipped)
	}
	if summary.DVRLineup.SkipReason != "no_sources_succeeded" {
		t.Fatalf("summary dvr_lineup.skip_reason = %q, want no_sources_succeeded", summary.DVRLineup.SkipReason)
	}
	if summary.Failure == nil {
		t.Fatal("summary failure metadata is nil, want refresh phase failure")
	}
	if summary.Failure.Phase != playlistSyncFailurePhaseRefresh {
		t.Fatalf("summary failure.phase = %q, want %q", summary.Failure.Phase, playlistSyncFailurePhaseRefresh)
	}
}

func TestPlaylistSyncJobRunCountChannelsFailurePersistsStructuredSummary(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
		},
	}
	refresher := &fakeRefresher{
		items: map[int64]int{1: 5},
	}
	reconciler := &fakeReconciler{
		countErr: fmt.Errorf("count unavailable"),
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
	if run.Status != StatusError {
		t.Fatalf("run status = %q, want %q", run.Status, StatusError)
	}
	if !strings.Contains(run.ErrorMessage, "count channels for reconcile") {
		t.Fatalf("run error = %q, want count channels for reconcile", run.ErrorMessage)
	}

	summary := decodePlaylistSyncRunSummary(t, run.Summary)
	if summary.Failure == nil {
		t.Fatal("summary failure metadata = nil, want count_channels failure")
	}
	if summary.Failure.Phase != playlistSyncFailurePhaseCount {
		t.Fatalf("summary failure.phase = %q, want %q", summary.Failure.Phase, playlistSyncFailurePhaseCount)
	}
	if !strings.Contains(summary.Failure.Error, "count unavailable") {
		t.Fatalf("summary failure.error = %q, want count unavailable", summary.Failure.Error)
	}
	if summary.Sources.Attempted != 1 || summary.Sources.Succeeded != 1 || summary.Sources.Failed != 0 {
		t.Fatalf(
			"summary source counts = attempted:%d succeeded:%d failed:%d, want 1/1/0",
			summary.Sources.Attempted,
			summary.Sources.Succeeded,
			summary.Sources.Failed,
		)
	}
}

func TestPlaylistSyncJobRunReconcileFailurePersistsStructuredSummary(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
		},
	}
	refresher := &fakeRefresher{
		items: map[int64]int{1: 5},
	}
	reconciler := &fakeReconciler{
		count: 3,
		err:   fmt.Errorf("reconcile blew up"),
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
	if run.Status != StatusError {
		t.Fatalf("run status = %q, want %q", run.Status, StatusError)
	}
	if !strings.Contains(run.ErrorMessage, "reconcile channels") {
		t.Fatalf("run error = %q, want reconcile channels", run.ErrorMessage)
	}

	summary := decodePlaylistSyncRunSummary(t, run.Summary)
	if summary.Failure == nil {
		t.Fatal("summary failure metadata = nil, want reconcile failure")
	}
	if summary.Failure.Phase != playlistSyncFailurePhaseReconcile {
		t.Fatalf("summary failure.phase = %q, want %q", summary.Failure.Phase, playlistSyncFailurePhaseReconcile)
	}
	if !strings.Contains(summary.Failure.Error, "reconcile blew up") {
		t.Fatalf("summary failure.error = %q, want reconcile blew up", summary.Failure.Error)
	}
	if summary.Reconcile == nil {
		t.Fatal("summary reconcile metrics = nil, want partial progress snapshot")
	}
	if summary.Reconcile.ChannelsProcessed != 3 || summary.Reconcile.ChannelsTotal != 3 {
		t.Fatalf(
			"summary reconcile channels = %d/%d, want 3/3",
			summary.Reconcile.ChannelsProcessed,
			summary.Reconcile.ChannelsTotal,
		)
	}
}

func TestPlaylistSyncJobRunRequestedSourceOnly(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		value: "",
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
			{
				SourceID:    2,
				SourceKey:   "backup01",
				Name:        "Backup",
				PlaylistURL: "http://backup.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  1,
			},
		},
	}
	refresher := &fakeRefresher{
		items: map[int64]int{
			1: 11,
			2: 4,
		},
	}
	reconciler := &fakeReconciler{
		count:  1,
		result: reconcile.Result{ChannelsTotal: 1, ChannelsProcessed: 1},
	}
	job, err := NewPlaylistSyncJob(settings, refresher, reconciler)
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}

	jobCtx := WithPlaylistSyncSourceID(context.Background(), 2)
	runID, err := runner.Start(jobCtx, JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	refresherCalls := refresher.snapshotCalls()
	if len(refresherCalls) != 1 {
		t.Fatalf("refresher calls = %d, want 1", len(refresherCalls))
	}
	if refresherCalls[0].SourceID != 2 {
		t.Fatalf("refresher source_id = %d, want 2", refresherCalls[0].SourceID)
	}
	summary := decodePlaylistSyncRunSummary(t, run.Summary)
	if summary.Sources.Attempted != 1 || summary.Sources.Succeeded != 1 || summary.Sources.Failed != 0 {
		t.Fatalf(
			"summary source counts = attempted:%d succeeded:%d failed:%d, want 1/1/0",
			summary.Sources.Attempted,
			summary.Sources.Succeeded,
			summary.Sources.Failed,
		)
	}
	if summary.RequestedSourceID != "2" {
		t.Fatalf("summary requested_source_id = %q, want 2", summary.RequestedSourceID)
	}
}

func TestPlaylistSyncJobRunRequestedSourceDisabled(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		value: "",
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
			{
				SourceID:    2,
				SourceKey:   "backup01",
				Name:        "Backup",
				PlaylistURL: "http://backup.example/playlist.m3u",
				Enabled:     false,
				OrderIndex:  1,
			},
		},
	}
	refresher := &fakeRefresher{}
	reconciler := &fakeReconciler{}
	job, err := NewPlaylistSyncJob(settings, refresher, reconciler)
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}

	jobCtx := WithPlaylistSyncSourceID(context.Background(), 2)
	runID, err := runner.Start(jobCtx, JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusError {
		t.Fatalf("run status = %q, want %q", run.Status, StatusError)
	}
	if !strings.Contains(run.ErrorMessage, "disabled") {
		t.Fatalf("run error = %q, want disabled source message", run.ErrorMessage)
	}
	if got := len(refresher.snapshotCalls()); got != 0 {
		t.Fatalf("refresher calls = %d, want 0", got)
	}
}

func TestPlaylistSyncJobRunTriggersDVRLineupReload(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		value: "",
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
		},
	}
	refresher := &fakeRefresher{
		items: map[int64]int{
			1: 11,
		},
	}
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
	summary := decodePlaylistSyncRunSummary(t, run.Summary)
	if !summary.DVRLineup.Reloaded {
		t.Fatalf("summary dvr_lineup.reloaded = %v, want true", summary.DVRLineup.Reloaded)
	}
	if summary.DVRLineup.Status != dvr.ReloadStatusReloaded {
		t.Fatalf("summary dvr_lineup.status = %q, want %q", summary.DVRLineup.Status, dvr.ReloadStatusReloaded)
	}
}

func TestPlaylistSyncJobRunSucceedsWhenDVRLineupReloadFails(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		value: "",
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
		},
	}
	refresher := &fakeRefresher{
		items: map[int64]int{1: 1},
	}
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
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
	summary := decodePlaylistSyncRunSummary(t, run.Summary)
	if summary.DVRLineup.Reloaded {
		t.Fatalf("summary dvr_lineup.reloaded = %v, want false", summary.DVRLineup.Reloaded)
	}
	if summary.DVRLineup.Status != dvr.ReloadStatusUnknown {
		t.Fatalf("summary dvr_lineup.status = %q, want %q", summary.DVRLineup.Status, dvr.ReloadStatusUnknown)
	}
	if summary.DVRLineup.SkipReason != "reload_error" {
		t.Fatalf("summary dvr_lineup.skip_reason = %q, want reload_error", summary.DVRLineup.SkipReason)
	}
}

func TestPlaylistSyncJobThrottlesProgressPersistence(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		value: "",
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
		},
	}
	refresher := &fakeRefresher{
		items: map[int64]int{1: 42},
	}
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
	summary := decodePlaylistSyncRunSummary(t, run.Summary)
	if summary.Reconcile == nil {
		t.Fatalf("summary reconcile metrics = nil, want final reconcile totals")
	}
	if summary.Reconcile.ChannelsProcessed != 25 || summary.Reconcile.ChannelsTotal != 25 {
		t.Fatalf(
			"summary reconcile channels = %d/%d, want 25/25",
			summary.Reconcile.ChannelsProcessed,
			summary.Reconcile.ChannelsTotal,
		)
	}

	progressWrites := store.progressWrites(runID)
	if progressWrites >= reconciler.count {
		t.Fatalf("UpdateRunProgress writes = %d, want less than channel count %d", progressWrites, reconciler.count)
	}
	if progressWrites > 10 {
		t.Fatalf("UpdateRunProgress writes = %d, expected throttled write volume", progressWrites)
	}
}

func TestPlaylistSyncJobRequiresPlaylistURLOnlyWhenPrimaryURLMissing(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	job, err := NewPlaylistSyncJob(
		&fakePlaylistSettings{
			valueErr: sql.ErrNoRows,
			sources: []playlist.PlaylistSource{
				{
					SourceID:    1,
					SourceKey:   "primary",
					Name:        "Primary",
					PlaylistURL: "http://primary.example/playlist.m3u",
					Enabled:     true,
					OrderIndex:  0,
				},
			},
		},
		&fakeRefresher{items: map[int64]int{1: 1}},
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
	if run.Status != StatusSuccess {
		t.Fatalf("run status = %q, want %q", run.Status, StatusSuccess)
	}
}

func TestNormalizeReloadOutcomeForSummaryDefaultsStatusAndSkipReason(t *testing.T) {
	t.Parallel()

	outcome := dvr.ReloadOutcome{
		Reloaded:    false,
		Skipped:     true,
		SkipReasons: []string{"  missing_jellyfin_api_token  "},
	}
	reloaded, status, skipReason := normalizeReloadOutcomeForSummary(outcome)
	if reloaded {
		t.Fatalf("reloaded = %v, want false", reloaded)
	}
	if got, want := status, dvr.ReloadStatusSkipped; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := skipReason, "missing_jellyfin_api_token"; got != want {
		t.Fatalf("skipReason = %q, want %q", got, want)
	}
}

func TestNormalizeReloadOutcomeForSummaryNormalizesKnownStatus(t *testing.T) {
	t.Parallel()

	outcome := dvr.ReloadOutcome{
		Reloaded:    true,
		Skipped:     true,
		Status:      "  PARTIAL  ",
		SkipReasons: []string{"  channels:missing_channels_base_url  "},
	}
	reloaded, status, skipReason := normalizeReloadOutcomeForSummary(outcome)
	if !reloaded {
		t.Fatalf("reloaded = %v, want true", reloaded)
	}
	if got, want := status, dvr.ReloadStatusPartial; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := skipReason, "channels:missing_channels_base_url"; got != want {
		t.Fatalf("skipReason = %q, want %q", got, want)
	}
}

func TestNormalizeReloadOutcomeForSummaryFallsBackForUnknownStatus(t *testing.T) {
	t.Parallel()

	outcome := dvr.ReloadOutcome{
		Reloaded: true,
		Status:   "not_a_real_status",
	}
	reloaded, status, skipReason := normalizeReloadOutcomeForSummary(outcome)
	if !reloaded {
		t.Fatalf("reloaded = %v, want true", reloaded)
	}
	if got, want := status, dvr.ReloadStatusReloaded; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := skipReason, "none"; got != want {
		t.Fatalf("skipReason = %q, want %q", got, want)
	}
}

func TestNormalizeReloadOutcomeForSummarySupportsFailedStatusAndFailureReasons(t *testing.T) {
	t.Parallel()

	outcome := dvr.ReloadOutcome{
		Reloaded:       false,
		Failed:         true,
		Status:         "  FAILED  ",
		FailureReasons: []string{"  channels:reload_lineup_failed: reload device lineup \"8F07FDC6\": upstream timeout  "},
	}
	reloaded, status, skipReason := normalizeReloadOutcomeForSummary(outcome)
	if reloaded {
		t.Fatalf("reloaded = %v, want false", reloaded)
	}
	if got, want := status, dvr.ReloadStatusFailed; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	wantReason := sanitizeReloadReasonSummaryValue(outcome.FailureReasons[0])
	if got, want := skipReason, wantReason; got != want {
		t.Fatalf("skipReason = %q, want %q", got, want)
	}
}

func TestNormalizeReloadOutcomeForSummaryCombinesSkipAndFailureReasons(t *testing.T) {
	t.Parallel()

	outcome := dvr.ReloadOutcome{
		Reloaded:       true,
		Skipped:        true,
		Failed:         true,
		SkipReasons:    []string{"  jellyfin:missing_jellyfin_api_token  "},
		FailureReasons: []string{"  channels:reload_lineup_failed: reload device lineup \"8F07FDC6\": upstream timeout  "},
	}
	reloaded, status, skipReason := normalizeReloadOutcomeForSummary(outcome)
	if !reloaded {
		t.Fatalf("reloaded = %v, want true", reloaded)
	}
	if got, want := status, dvr.ReloadStatusPartial; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	wantReason := strings.Join(
		[]string{
			sanitizeReloadReasonSummaryValue(outcome.SkipReasons[0]),
			sanitizeReloadReasonSummaryValue(outcome.FailureReasons[0]),
		},
		",",
	)
	if got, want := skipReason, wantReason; got != want {
		t.Fatalf("skipReason = %q, want %q", got, want)
	}
}

func TestPlaylistSyncJobRunRequestedSourceNotFound(t *testing.T) {
	store := newMemoryStore()
	runner, err := NewRunner(store)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}

	settings := &fakePlaylistSettings{
		value: "",
		sources: []playlist.PlaylistSource{
			{
				SourceID:    1,
				SourceKey:   "primary",
				Name:        "Primary",
				PlaylistURL: "http://primary.example/playlist.m3u",
				Enabled:     true,
				OrderIndex:  0,
			},
		},
	}
	job, err := NewPlaylistSyncJob(settings, &fakeRefresher{}, &fakeReconciler{})
	if err != nil {
		t.Fatalf("NewPlaylistSyncJob() error = %v", err)
	}

	jobCtx := WithPlaylistSyncSourceID(context.Background(), 99)
	runID, err := runner.Start(jobCtx, JobPlaylistSync, TriggerManual, job.Run)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	run := waitForRunDone(t, runner, runID)
	if run.Status != StatusError {
		t.Fatalf("run status = %q, want %q", run.Status, StatusError)
	}
	if !strings.Contains(run.ErrorMessage, "playlist source not found") &&
		!strings.Contains(run.ErrorMessage, "source_id 99") {
		t.Fatalf("run error = %q, want playlist source not found", run.ErrorMessage)
	}
}
