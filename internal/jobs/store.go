package jobs

import (
	"context"
	"errors"
)

const (
	JobPlaylistSync   = "playlist_sync"
	JobAutoPrioritize = "auto_prioritize"
	JobDVRLineupSync  = "dvr_lineup_sync"

	TriggerManual   = "manual"
	TriggerSchedule = "schedule"

	StatusRunning  = "running"
	StatusSuccess  = "success"
	StatusError    = "error"
	StatusCanceled = "canceled"
)

var (
	ErrRunNotRunning = errors.New("job run is not running")
	ErrRunFinalized  = errors.New("job run is finalized")
)

// Run captures persisted job execution metadata.
type Run struct {
	RunID        int64  `json:"run_id"`
	JobName      string `json:"job_name"`
	TriggeredBy  string `json:"triggered_by"`
	StartedAt    int64  `json:"started_at"`
	FinishedAt   int64  `json:"finished_at,omitempty"`
	Status       string `json:"status"`
	ProgressCur  int    `json:"progress_cur"`
	ProgressMax  int    `json:"progress_max"`
	Summary      string `json:"summary,omitempty"`
	ErrorMessage string `json:"error,omitempty"`
	// AnalysisErrorBuckets is derived from auto-prioritize run summary text when present.
	AnalysisErrorBuckets map[string]int `json:"analysis_error_buckets,omitempty"`
}

// Store persists and queries job run lifecycle state.
type Store interface {
	CreateRun(ctx context.Context, jobName, triggeredBy string, startedAt int64) (int64, error)
	UpdateRunProgress(ctx context.Context, runID int64, progressCur, progressMax int, summary string) error
	FinishRun(ctx context.Context, runID int64, status, errText, summary string, finishedAt int64) error
	GetRun(ctx context.Context, runID int64) (Run, error)
	ListRuns(ctx context.Context, jobName string, limit, offset int) ([]Run, error)
}
