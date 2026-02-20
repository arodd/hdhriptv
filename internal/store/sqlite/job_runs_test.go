package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/jobs"
)

func TestJobRunLifecycle(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	startedAt := time.Now().Add(-2 * time.Minute).UTC().Unix()
	runID, err := store.CreateRun(ctx, jobs.JobPlaylistSync, jobs.TriggerManual, startedAt)
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if runID <= 0 {
		t.Fatalf("runID = %d, want > 0", runID)
	}

	if err := store.UpdateRunProgress(ctx, runID, 3, 10, "phase 1 complete"); err != nil {
		t.Fatalf("UpdateRunProgress() error = %v", err)
	}

	finishedAt := time.Now().UTC().Unix()
	if err := store.FinishRun(ctx, runID, jobs.StatusSuccess, "", "all good", finishedAt); err != nil {
		t.Fatalf("FinishRun() error = %v", err)
	}

	run, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if run.Status != jobs.StatusSuccess {
		t.Fatalf("status = %q, want %q", run.Status, jobs.StatusSuccess)
	}
	if run.ProgressCur != 3 || run.ProgressMax != 10 {
		t.Fatalf("progress = %d/%d, want 3/10", run.ProgressCur, run.ProgressMax)
	}
	if run.Summary != "all good" {
		t.Fatalf("summary = %q, want %q", run.Summary, "all good")
	}
	if len(run.AnalysisErrorBuckets) != 0 {
		t.Fatalf("analysis_error_buckets = %#v, want empty", run.AnalysisErrorBuckets)
	}
	if run.FinishedAt != finishedAt {
		t.Fatalf("finished_at = %d, want %d", run.FinishedAt, finishedAt)
	}
}

func TestListRunsAndValidation(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	runA, err := store.CreateRun(ctx, jobs.JobPlaylistSync, jobs.TriggerManual, 100)
	if err != nil {
		t.Fatalf("CreateRun(runA) error = %v", err)
	}
	runB, err := store.CreateRun(ctx, jobs.JobAutoPrioritize, jobs.TriggerSchedule, 200)
	if err != nil {
		t.Fatalf("CreateRun(runB) error = %v", err)
	}

	if err := store.FinishRun(ctx, runA, jobs.StatusError, "failed", "bad stream", 101); err != nil {
		t.Fatalf("FinishRun(runA) error = %v", err)
	}
	if err := store.FinishRun(
		ctx,
		runB,
		jobs.StatusSuccess,
		"",
		"channels=1 analyzed=1 analysis_errors=2 analysis_error_buckets=http_429:2",
		201,
	); err != nil {
		t.Fatalf("FinishRun(runB) error = %v", err)
	}

	allRuns, err := store.ListRuns(ctx, "", 10, 0)
	if err != nil {
		t.Fatalf("ListRuns(all) error = %v", err)
	}
	if len(allRuns) != 2 {
		t.Fatalf("len(allRuns) = %d, want 2", len(allRuns))
	}
	if allRuns[0].RunID != runB {
		t.Fatalf("ListRuns(all)[0].RunID = %d, want %d", allRuns[0].RunID, runB)
	}
	if allRuns[0].AnalysisErrorBuckets["http_429"] != 2 {
		t.Fatalf("ListRuns(all)[0].analysis_error_buckets[http_429] = %d, want 2", allRuns[0].AnalysisErrorBuckets["http_429"])
	}

	playlistRuns, err := store.ListRuns(ctx, jobs.JobPlaylistSync, 10, 0)
	if err != nil {
		t.Fatalf("ListRuns(playlist_sync) error = %v", err)
	}
	if len(playlistRuns) != 1 || playlistRuns[0].RunID != runA {
		t.Fatalf("playlist runs = %+v, want only runA", playlistRuns)
	}

	if err := store.FinishRun(ctx, runA, "invalid_status", "", "", 999); err == nil {
		t.Fatal("FinishRun(invalid_status) expected error")
	}
	if err := store.UpdateRunProgress(ctx, 999999, 1, 2, "x"); err != sql.ErrNoRows {
		t.Fatalf("UpdateRunProgress(missing) error = %v, want sql.ErrNoRows", err)
	}
}

func TestUpdateRunProgressRejectsTerminalRun(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	runID, err := store.CreateRun(ctx, jobs.JobPlaylistSync, jobs.TriggerManual, time.Now().UTC().Unix())
	if err != nil {
		t.Fatalf("CreateRun() error = %v", err)
	}
	if err := store.UpdateRunProgress(ctx, runID, 2, 5, "in-flight summary"); err != nil {
		t.Fatalf("UpdateRunProgress(running) error = %v", err)
	}
	if err := store.FinishRun(ctx, runID, jobs.StatusError, "failure", "terminal summary", time.Now().UTC().Unix()); err != nil {
		t.Fatalf("FinishRun() error = %v", err)
	}

	err = store.UpdateRunProgress(ctx, runID, 5, 5, "late summary")
	if !errors.Is(err, jobs.ErrRunNotRunning) {
		t.Fatalf("UpdateRunProgress(terminal) error = %v, want ErrRunNotRunning", err)
	}

	run, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun() error = %v", err)
	}
	if run.Status != jobs.StatusError {
		t.Fatalf("status = %q, want %q", run.Status, jobs.StatusError)
	}
	if run.ProgressCur != 2 || run.ProgressMax != 5 {
		t.Fatalf("progress = %d/%d, want 2/5", run.ProgressCur, run.ProgressMax)
	}
	if run.Summary != "terminal summary" {
		t.Fatalf("summary = %q, want %q", run.Summary, "terminal summary")
	}
	if run.ErrorMessage != "failure" {
		t.Fatalf("error = %q, want %q", run.ErrorMessage, "failure")
	}
}

func TestListRunsQueryPlanAvoidsTempBTreeForOrderedPagination(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if _, err := seedJobRunsHistory(t, ctx, store, 12000); err != nil {
		t.Fatalf("seedJobRunsHistory() error = %v", err)
	}

	unfilteredPlan, err := explainListRunsQueryPlan(ctx, store.db, "", 100, 9000)
	if err != nil {
		t.Fatalf("explainListRunsQueryPlan(unfiltered) error = %v", err)
	}
	if len(unfilteredPlan) == 0 {
		t.Fatal("expected unfiltered query-plan rows")
	}
	t.Logf("job_runs unfiltered query plan: %s", strings.Join(unfilteredPlan, " | "))
	if !planContains(unfilteredPlan, "idx_job_runs_started_run_id_desc") {
		t.Fatalf("unfiltered plan missing ordered index: %q", strings.Join(unfilteredPlan, " | "))
	}
	if planContains(unfilteredPlan, "use temp b-tree for order by") {
		t.Fatalf("unfiltered plan unexpectedly includes temp sort: %q", strings.Join(unfilteredPlan, " | "))
	}

	filteredPlan, err := explainListRunsQueryPlan(ctx, store.db, jobs.JobPlaylistSync, 100, 2500)
	if err != nil {
		t.Fatalf("explainListRunsQueryPlan(filtered) error = %v", err)
	}
	if len(filteredPlan) == 0 {
		t.Fatal("expected filtered query-plan rows")
	}
	t.Logf("job_runs filtered query plan: %s", strings.Join(filteredPlan, " | "))
	if !planContains(filteredPlan, "idx_job_runs_name_started_run_id_desc") {
		t.Fatalf("filtered plan missing ordered index: %q", strings.Join(filteredPlan, " | "))
	}
	if planContains(filteredPlan, "use temp b-tree for order by") {
		t.Fatalf("filtered plan unexpectedly includes temp sort: %q", strings.Join(filteredPlan, " | "))
	}
	if planContains(filteredPlan, "use temp b-tree for last term of order by") {
		t.Fatalf("filtered plan unexpectedly includes partial temp sort: %q", strings.Join(filteredPlan, " | "))
	}
}

func TestListRunsHighOffsetLargeHistory(t *testing.T) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const totalRows = 30000
	countsByJob, err := seedJobRunsHistory(t, ctx, store, totalRows)
	if err != nil {
		t.Fatalf("seedJobRunsHistory() error = %v", err)
	}

	const pageLimit = 100
	unfilteredOffset := totalRows - (pageLimit + 25)
	startUnfiltered := time.Now()
	unfilteredRuns, err := store.ListRuns(ctx, "", pageLimit, unfilteredOffset)
	unfilteredDuration := time.Since(startUnfiltered)
	if err != nil {
		t.Fatalf("ListRuns(unfiltered high offset) error = %v", err)
	}
	if len(unfilteredRuns) != pageLimit {
		t.Fatalf("ListRuns(unfiltered high offset) len = %d, want %d", len(unfilteredRuns), pageLimit)
	}
	assertRunsSortedDescending(t, unfilteredRuns)

	filteredTotal := countsByJob[jobs.JobPlaylistSync]
	filteredOffset := filteredTotal - (pageLimit + 25)
	if filteredOffset < 0 {
		filteredOffset = 0
	}
	startFiltered := time.Now()
	filteredRuns, err := store.ListRuns(ctx, jobs.JobPlaylistSync, pageLimit, filteredOffset)
	filteredDuration := time.Since(startFiltered)
	if err != nil {
		t.Fatalf("ListRuns(filtered high offset) error = %v", err)
	}
	if len(filteredRuns) == 0 {
		t.Fatal("ListRuns(filtered high offset) returned no rows")
	}
	for _, run := range filteredRuns {
		if run.JobName != jobs.JobPlaylistSync {
			t.Fatalf("filtered run job_name = %q, want %q", run.JobName, jobs.JobPlaylistSync)
		}
	}
	assertRunsSortedDescending(t, filteredRuns)

	t.Logf(
		"job_runs high-offset timings unfiltered(offset=%d):%s filtered(offset=%d):%s",
		unfilteredOffset,
		unfilteredDuration,
		filteredOffset,
		filteredDuration,
	)
}

func BenchmarkListRunsHighOffsetUnfiltered(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const totalRows = 100000
	if _, err := seedJobRunsHistory(b, ctx, store, totalRows); err != nil {
		b.Fatalf("seedJobRunsHistory() error = %v", err)
	}

	const (
		pageLimit = 100
		offset    = totalRows - (pageLimit + 50)
	)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		runs, err := store.ListRuns(ctx, "", pageLimit, offset)
		if err != nil {
			b.Fatalf("ListRuns(unfiltered) error = %v", err)
		}
		if len(runs) != pageLimit {
			b.Fatalf("ListRuns(unfiltered) len = %d, want %d", len(runs), pageLimit)
		}
	}
}

func BenchmarkListRunsHighOffsetFiltered(b *testing.B) {
	ctx := context.Background()

	store, err := Open(":memory:")
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	const totalRows = 100000
	countsByJob, err := seedJobRunsHistory(b, ctx, store, totalRows)
	if err != nil {
		b.Fatalf("seedJobRunsHistory() error = %v", err)
	}

	const pageLimit = 100
	offset := countsByJob[jobs.JobPlaylistSync] - (pageLimit + 50)
	if offset < 0 {
		offset = 0
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		runs, err := store.ListRuns(ctx, jobs.JobPlaylistSync, pageLimit, offset)
		if err != nil {
			b.Fatalf("ListRuns(filtered) error = %v", err)
		}
		if len(runs) != pageLimit {
			b.Fatalf("ListRuns(filtered) len = %d, want %d", len(runs), pageLimit)
		}
		for _, run := range runs {
			if run.JobName != jobs.JobPlaylistSync {
				b.Fatalf("ListRuns(filtered) unexpected job name %q", run.JobName)
			}
		}
	}
}

func seedJobRunsHistory(tb testing.TB, ctx context.Context, store *Store, totalRows int) (map[string]int, error) {
	tb.Helper()

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(
		ctx,
		`INSERT INTO job_runs (
			job_name,
			triggered_by,
			started_at,
			finished_at,
			status,
			progress_cur,
			progress_max,
			summary,
			error
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

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
	const baseStartedAt int64 = 1_700_000_000
	for i := 0; i < totalRows; i++ {
		jobName := jobNames[i%len(jobNames)]
		countsByJob[jobName]++

		startedAt := baseStartedAt + int64(i/2)
		finishedAt := startedAt + 1
		summary := ""
		if i%17 == 0 {
			summary = "analysis_errors=1 analysis_error_buckets=http_429:1"
		}
		if _, err := stmt.ExecContext(
			ctx,
			jobName,
			jobs.TriggerSchedule,
			startedAt,
			finishedAt,
			jobs.StatusSuccess,
			1,
			1,
			summary,
			"",
		); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return countsByJob, nil
}

func explainListRunsQueryPlan(ctx context.Context, db *sql.DB, jobName string, limit, offset int) ([]string, error) {
	jobName = strings.TrimSpace(jobName)
	query := listRunsUnfilteredQuery
	args := make([]any, 0, 3)
	if jobName != "" {
		query = listRunsByNameQuery
		args = append(args, jobName)
	}
	args = append(args, limit, offset)

	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	details := make([]string, 0, 4)
	for rows.Next() {
		var (
			id       int
			parentID int
			unused   int
			detail   string
		)
		if err := rows.Scan(&id, &parentID, &unused, &detail); err != nil {
			return nil, err
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return details, nil
}

func assertRunsSortedDescending(t *testing.T, runs []jobs.Run) {
	t.Helper()

	for i := 1; i < len(runs); i++ {
		prev := runs[i-1]
		curr := runs[i]
		if prev.StartedAt < curr.StartedAt {
			t.Fatalf(
				"runs out of order at index %d: prev(started_at=%d run_id=%d) < curr(started_at=%d run_id=%d)",
				i,
				prev.StartedAt,
				prev.RunID,
				curr.StartedAt,
				curr.RunID,
			)
		}
		if prev.StartedAt == curr.StartedAt && prev.RunID < curr.RunID {
			t.Fatalf(
				"runs tie-order mismatch at index %d: prev(run_id=%d) < curr(run_id=%d) with same started_at=%d",
				i,
				prev.RunID,
				curr.RunID,
				prev.StartedAt,
			)
		}
	}
}
