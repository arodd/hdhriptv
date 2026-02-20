package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/jobs"
)

var _ jobs.Store = (*Store)(nil)

const (
	listRunsSelectColumns = `
		SELECT
			run_id,
			job_name,
			triggered_by,
			started_at,
			finished_at,
			status,
			progress_cur,
			progress_max,
			COALESCE(summary, ''),
			COALESCE(error, '')
	`

	listRunsUnfilteredQuery = listRunsSelectColumns + `
		FROM job_runs INDEXED BY idx_job_runs_started_run_id_desc
		ORDER BY started_at DESC, run_id DESC
		LIMIT ? OFFSET ?
	`

	listRunsByNameQuery = listRunsSelectColumns + `
		FROM job_runs INDEXED BY idx_job_runs_name_started_run_id_desc
		WHERE job_name = ?
		ORDER BY started_at DESC, run_id DESC
		LIMIT ? OFFSET ?
	`
)

func (s *Store) CreateRun(ctx context.Context, jobName, triggeredBy string, startedAt int64) (int64, error) {
	jobName = strings.TrimSpace(jobName)
	if jobName == "" {
		return 0, fmt.Errorf("job_name is required")
	}

	triggeredBy = strings.TrimSpace(triggeredBy)
	if triggeredBy == "" {
		return 0, fmt.Errorf("triggered_by is required")
	}

	if startedAt <= 0 {
		startedAt = time.Now().UTC().Unix()
	}

	createTrace := s.beginSQLiteIOERRTrace("job_run_write", "create_run")
	result, err := s.db.ExecContext(
		ctx,
		`INSERT INTO job_runs (
			job_name,
			triggered_by,
			started_at,
			status,
			progress_cur,
			progress_max
		) VALUES (?, ?, ?, ?, 0, 0)`,
		jobName,
		triggeredBy,
		startedAt,
		jobs.StatusRunning,
	)
	s.endSQLiteIOERRTrace(createTrace, err)
	if err != nil {
		return 0, fmt.Errorf(
			"insert job run: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "create_run",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}

	runID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("read inserted run id: %w", err)
	}
	return runID, nil
}

func (s *Store) UpdateRunProgress(ctx context.Context, runID int64, progressCur, progressMax int, summary string) error {
	if runID <= 0 {
		return fmt.Errorf("run_id must be greater than zero")
	}
	if progressCur < 0 {
		return fmt.Errorf("progress_cur must be non-negative")
	}
	if progressMax < 0 {
		return fmt.Errorf("progress_max must be non-negative")
	}
	if progressMax > 0 && progressCur > progressMax {
		return fmt.Errorf("progress_cur cannot exceed progress_max")
	}

	progressTrace := s.beginSQLiteIOERRTrace("job_run_write", "update_run_progress")
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE job_runs
		 SET progress_cur = ?,
		     progress_max = ?,
		     summary = ?
		 WHERE run_id = ?
		   AND status = ?`,
		progressCur,
		progressMax,
		strings.TrimSpace(summary),
		runID,
		jobs.StatusRunning,
	)
	s.endSQLiteIOERRTrace(progressTrace, err)
	if err != nil {
		return fmt.Errorf(
			"update run progress: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "update_run_progress",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("run progress rows affected: %w", err)
	}
	if rows == 0 {
		var exists int
		lookupErr := s.db.QueryRowContext(
			ctx,
			`SELECT 1 FROM job_runs WHERE run_id = ?`,
			runID,
		).Scan(&exists)
		if lookupErr == sql.ErrNoRows {
			return sql.ErrNoRows
		}
		if lookupErr != nil {
			return fmt.Errorf("lookup run for progress update: %w", lookupErr)
		}
		return jobs.ErrRunNotRunning
	}
	return nil
}

func (s *Store) FinishRun(ctx context.Context, runID int64, status, errText, summary string, finishedAt int64) error {
	if runID <= 0 {
		return fmt.Errorf("run_id must be greater than zero")
	}

	status = strings.TrimSpace(status)
	switch status {
	case jobs.StatusSuccess, jobs.StatusError, jobs.StatusCanceled:
	default:
		return fmt.Errorf("invalid run status: %q", status)
	}

	if finishedAt <= 0 {
		finishedAt = time.Now().UTC().Unix()
	}

	finishTrace := s.beginSQLiteIOERRTrace("job_run_write", "finish_run")
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE job_runs
		 SET status = ?,
		     finished_at = ?,
		     error = ?,
		     summary = ?
		 WHERE run_id = ?`,
		status,
		finishedAt,
		strings.TrimSpace(errText),
		strings.TrimSpace(summary),
		runID,
	)
	s.endSQLiteIOERRTrace(finishTrace, err)
	if err != nil {
		return fmt.Errorf(
			"finish run: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "finish_run",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("finish run rows affected: %w", err)
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) GetRun(ctx context.Context, runID int64) (jobs.Run, error) {
	if runID <= 0 {
		return jobs.Run{}, fmt.Errorf("run_id must be greater than zero")
	}

	var (
		run            jobs.Run
		finishedAtNull sql.NullInt64
	)
	err := s.db.QueryRowContext(
		ctx,
		`SELECT
			run_id,
			job_name,
			triggered_by,
			started_at,
			finished_at,
			status,
			progress_cur,
			progress_max,
			COALESCE(summary, ''),
			COALESCE(error, '')
		FROM job_runs
		WHERE run_id = ?`,
		runID,
	).Scan(
		&run.RunID,
		&run.JobName,
		&run.TriggeredBy,
		&run.StartedAt,
		&finishedAtNull,
		&run.Status,
		&run.ProgressCur,
		&run.ProgressMax,
		&run.Summary,
		&run.ErrorMessage,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return jobs.Run{}, sql.ErrNoRows
		}
		return jobs.Run{}, fmt.Errorf("query run: %w", err)
	}
	if finishedAtNull.Valid {
		run.FinishedAt = finishedAtNull.Int64
	}
	run.AnalysisErrorBuckets = jobs.ParseAnalysisErrorBuckets(run.Summary)
	return run, nil
}

func (s *Store) ListRuns(ctx context.Context, jobName string, limit, offset int) ([]jobs.Run, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}

	query := listRunsUnfilteredQuery
	args := make([]any, 0, 3)

	jobName = strings.TrimSpace(jobName)
	if jobName != "" {
		query = listRunsByNameQuery
		args = append(args, jobName)
	}

	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query runs: %w", err)
	}
	defer rows.Close()

	out := make([]jobs.Run, 0)
	for rows.Next() {
		var (
			run            jobs.Run
			finishedAtNull sql.NullInt64
		)
		if err := rows.Scan(
			&run.RunID,
			&run.JobName,
			&run.TriggeredBy,
			&run.StartedAt,
			&finishedAtNull,
			&run.Status,
			&run.ProgressCur,
			&run.ProgressMax,
			&run.Summary,
			&run.ErrorMessage,
		); err != nil {
			return nil, fmt.Errorf("scan run row: %w", err)
		}
		if finishedAtNull.Valid {
			run.FinishedAt = finishedAtNull.Int64
		}
		run.AnalysisErrorBuckets = jobs.ParseAnalysisErrorBuckets(run.Summary)
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runs: %w", err)
	}

	return out, nil
}
