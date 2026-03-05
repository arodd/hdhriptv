package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/playlist"
	"github.com/arodd/hdhriptv/internal/store/sqlite"
	"github.com/arodd/hdhriptv/internal/stream"
)

func TestAdminRoutesUpdateSourceRecoveryForActiveSourceAndNoRecoveryForInactiveSource(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:src-mutation-primary",
			ChannelKey: "tvg:source-mutation",
			Name:       "Source Mutation Source",
			Group:      "Source Mutation",
			StreamURL:  "http://example.com/source-mutation-primary.ts",
		},
		{
			ItemKey:    "src:src-mutation-secondary",
			ChannelKey: "tvg:source-mutation",
			Name:       "Source Mutation Backup",
			Group:      "Source Mutation",
			StreamURL:  "http://example.com/source-mutation-secondary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	channel, err := channelsSvc.Create(ctx, "src:src-mutation-primary", "", "", nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	source, err := channelsSvc.AddSource(ctx, channel.ChannelID, "src:src-mutation-secondary", true)
	if err != nil {
		t.Fatalf("AddSource() error = %v", err)
	}

	inactiveSource, err := channelsSvc.AddSource(ctx, channel.ChannelID, "src:src-mutation-primary", true)
	if err != nil {
		t.Fatalf("AddSource() error = %v", err)
	}

	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: channel.ChannelID,
					SourceID:  source.SourceID,
				},
			},
		},
	}
	handler.SetTunerStatusProvider(provider)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	var updated channels.Source
	path := fmt.Sprintf("/api/channels/%d/sources/%d", channel.ChannelID, source.SourceID)
	doJSON(t, mux, http.MethodPatch, path, map[string]any{"enabled": false}, http.StatusOK, &updated)
	if updated.SourceID != source.SourceID {
		t.Fatalf("updated source_id = %d, want %d", updated.SourceID, source.SourceID)
	}
	if updated.Enabled {
		t.Fatal("updated source should be disabled")
	}

	if provider.triggerCalls != 1 {
		t.Fatalf("triggerCalls = %d, want 1", provider.triggerCalls)
	}
	if provider.lastTriggerChannelID != channel.ChannelID {
		t.Fatalf("lastTriggerChannelID = %d, want %d", provider.lastTriggerChannelID, channel.ChannelID)
	}
	if provider.lastTriggerReason != "admin_source_disabled" {
		t.Fatalf("lastTriggerReason = %q, want %q", provider.lastTriggerReason, "admin_source_disabled")
	}

	provider.resetTriggers()
	provider.snapshot = stream.TunerStatusSnapshot{
		Tuners: []stream.TunerStatus{
			{
				ChannelID: channel.ChannelID,
				SourceID:  inactiveSource.SourceID,
			},
		},
	}

	doJSON(t, mux, http.MethodPatch, path, map[string]any{"enabled": true}, http.StatusOK, &updated)
	if updated.Enabled != true {
		t.Fatal("updated source should be enabled again")
	}
	if provider.triggerCalls != 0 {
		t.Fatalf("triggerCalls = %d, want 0 for non-active source", provider.triggerCalls)
	}
}

func TestAdminRoutesDeleteSourceRecoveryForActiveSource(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:src-delete-primary",
			ChannelKey: "tvg:source-delete",
			Name:       "Delete Source Mutation Primary",
			Group:      "Source Mutation",
			StreamURL:  "http://example.com/source-delete-primary.ts",
		},
		{
			ItemKey:    "src:src-delete-secondary",
			ChannelKey: "tvg:source-delete",
			Name:       "Delete Source Mutation Backup",
			Group:      "Source Mutation",
			StreamURL:  "http://example.com/source-delete-secondary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	channel, err := channelsSvc.Create(ctx, "src:src-delete-primary", "", "", nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	source, err := channelsSvc.AddSource(ctx, channel.ChannelID, "src:src-delete-secondary", true)
	if err != nil {
		t.Fatalf("AddSource() error = %v", err)
	}

	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}
	provider := &fakeTunerStatusProvider{
		snapshot: stream.TunerStatusSnapshot{
			Tuners: []stream.TunerStatus{
				{
					ChannelID: channel.ChannelID,
					SourceID:  source.SourceID,
				},
			},
		},
	}
	handler.SetTunerStatusProvider(provider)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	path := fmt.Sprintf("/api/channels/%d/sources/%d", channel.ChannelID, source.SourceID)
	rec := doRaw(t, mux, http.MethodDelete, path, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE %s status = %d, want %d", path, rec.Code, http.StatusNoContent)
	}

	if provider.triggerCalls != 1 {
		t.Fatalf("triggerCalls = %d, want 1", provider.triggerCalls)
	}
	if provider.lastTriggerReason != "admin_source_deleted" {
		t.Fatalf("lastTriggerReason = %q, want %q", provider.lastTriggerReason, "admin_source_deleted")
	}
}

func TestAdminRoutesSourceMutationsReturnChannelNotFoundForMissingChannel(t *testing.T) {
	store, err := sqlite.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.UpsertPlaylistItems(ctx, []playlist.Item{
		{
			ItemKey:    "src:src-missing-primary",
			ChannelKey: "tvg:source-missing",
			Name:       "Source Missing Primary",
			Group:      "Source Mutation",
			StreamURL:  "http://example.com/source-missing-primary.ts",
		},
		{
			ItemKey:    "src:src-missing-secondary",
			ChannelKey: "tvg:source-missing",
			Name:       "Source Missing Secondary",
			Group:      "Source Mutation",
			StreamURL:  "http://example.com/source-missing-secondary.ts",
		},
	}); err != nil {
		t.Fatalf("UpsertPlaylistItems() error = %v", err)
	}

	channelsSvc := channels.NewService(store)
	channel, err := channelsSvc.Create(ctx, "src:src-missing-primary", "", "", nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	source, err := channelsSvc.AddSource(ctx, channel.ChannelID, "src:src-missing-secondary", true)
	if err != nil {
		t.Fatalf("AddSource() error = %v", err)
	}

	handler, err := NewAdminHandler(store, channelsSvc)
	if err != nil {
		t.Fatalf("NewAdminHandler() error = %v", err)
	}

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux, "")

	const missingChannelID int64 = 999999

	updateRec := doRaw(
		t,
		mux,
		http.MethodPatch,
		fmt.Sprintf("/api/channels/%d/sources/%d", missingChannelID, source.SourceID),
		map[string]any{"enabled": true},
	)
	if updateRec.Code != http.StatusNotFound {
		t.Fatalf("PATCH source on missing channel status = %d, want %d", updateRec.Code, http.StatusNotFound)
	}
	if !strings.Contains(updateRec.Body.String(), channels.ErrChannelNotFound.Error()) {
		t.Fatalf("PATCH source on missing channel body = %q, want channel not found", updateRec.Body.String())
	}

	deleteRec := doRaw(
		t,
		mux,
		http.MethodDelete,
		fmt.Sprintf("/api/channels/%d/sources/%d", missingChannelID, source.SourceID),
		nil,
	)
	if deleteRec.Code != http.StatusNotFound {
		t.Fatalf("DELETE source on missing channel status = %d, want %d", deleteRec.Code, http.StatusNotFound)
	}
	if !strings.Contains(deleteRec.Body.String(), channels.ErrChannelNotFound.Error()) {
		t.Fatalf("DELETE source on missing channel body = %q, want channel not found", deleteRec.Body.String())
	}

	reorderRec := doRaw(
		t,
		mux,
		http.MethodPatch,
		fmt.Sprintf("/api/channels/%d/sources/reorder", missingChannelID),
		map[string]any{"source_ids": []int64{}},
	)
	if reorderRec.Code != http.StatusNotFound {
		t.Fatalf("PATCH reorder on missing channel status = %d, want %d", reorderRec.Code, http.StatusNotFound)
	}
	if !strings.Contains(reorderRec.Body.String(), channels.ErrChannelNotFound.Error()) {
		t.Fatalf("PATCH reorder on missing channel body = %q, want channel not found", reorderRec.Body.String())
	}
}
