package jobs

import (
	"context"
	"testing"
)

func TestWithPlaylistSyncSourceID(t *testing.T) {
	t.Parallel()

	ctx := WithPlaylistSyncSourceID(context.Background(), 42)
	sourceID, ok := PlaylistSyncSourceIDFromContext(ctx)
	if !ok {
		t.Fatal("PlaylistSyncSourceIDFromContext() ok = false, want true")
	}
	if sourceID != 42 {
		t.Fatalf("PlaylistSyncSourceIDFromContext() sourceID = %d, want 42", sourceID)
	}
}

func TestWithPlaylistSyncSourceIDInvalidIgnored(t *testing.T) {
	t.Parallel()

	ctx := WithPlaylistSyncSourceID(context.Background(), 0)
	if _, ok := PlaylistSyncSourceIDFromContext(ctx); ok {
		t.Fatal("PlaylistSyncSourceIDFromContext() ok = true, want false")
	}
}
