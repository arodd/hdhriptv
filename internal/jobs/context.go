package jobs

import (
	"context"
	"strings"
)

type runMetadataContextKey struct{}
type playlistSyncSourceIDContextKey struct{}

// RunMetadata identifies a persisted job run in downstream call chains.
type RunMetadata struct {
	RunID       int64
	JobName     string
	TriggeredBy string
}

// WithRunMetadata annotates ctx with job-run correlation identifiers.
func WithRunMetadata(ctx context.Context, runID int64, jobName, triggeredBy string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	jobName = strings.TrimSpace(jobName)
	triggeredBy = strings.TrimSpace(triggeredBy)
	if runID <= 0 || jobName == "" || triggeredBy == "" {
		return ctx
	}

	meta := RunMetadata{
		RunID:       runID,
		JobName:     jobName,
		TriggeredBy: triggeredBy,
	}
	return context.WithValue(ctx, runMetadataContextKey{}, meta)
}

// RunMetadataFromContext returns run correlation metadata if present.
func RunMetadataFromContext(ctx context.Context) (RunMetadata, bool) {
	if ctx == nil {
		return RunMetadata{}, false
	}

	meta, ok := ctx.Value(runMetadataContextKey{}).(RunMetadata)
	if !ok || meta.RunID <= 0 || meta.JobName == "" || meta.TriggeredBy == "" {
		return RunMetadata{}, false
	}
	return meta, true
}

// WithPlaylistSyncSourceID annotates ctx with an optional source scope for
// playlist_sync runs triggered via the admin API.
func WithPlaylistSyncSourceID(ctx context.Context, sourceID int64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if sourceID <= 0 {
		return ctx
	}
	return context.WithValue(ctx, playlistSyncSourceIDContextKey{}, sourceID)
}

// PlaylistSyncSourceIDFromContext returns an optional playlist source scope.
func PlaylistSyncSourceIDFromContext(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	sourceID, ok := ctx.Value(playlistSyncSourceIDContextKey{}).(int64)
	if !ok || sourceID <= 0 {
		return 0, false
	}
	return sourceID, true
}
