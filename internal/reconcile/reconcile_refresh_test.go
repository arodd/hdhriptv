package reconcile_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/reconcile"
)

type staticCatalogStore struct{}

func (staticCatalogStore) ListActiveItemKeysByChannelKey(context.Context, string) ([]string, error) {
	return nil, nil
}

func (staticCatalogStore) ListActiveItemKeysByCatalogFilter(context.Context, []string, string, bool) ([]string, error) {
	return nil, nil
}

type refreshGuideNamesChannelsService struct {
	listChannels             []channels.Channel
	dynamicBlocks            channels.DynamicChannelSyncResult
	dynamicGuideNamesUpdated int
	refreshErr               error
	refreshCalls             int
}

func (s *refreshGuideNamesChannelsService) List(context.Context) ([]channels.Channel, error) {
	return append([]channels.Channel(nil), s.listChannels...), nil
}

func (s *refreshGuideNamesChannelsService) ListSources(context.Context, int64, bool) ([]channels.Source, error) {
	return nil, nil
}

func (s *refreshGuideNamesChannelsService) AddSource(context.Context, int64, string, bool) (channels.Source, error) {
	return channels.Source{}, nil
}

func (s *refreshGuideNamesChannelsService) SyncDynamicSources(context.Context, int64, []string) (channels.DynamicSourceSyncResult, error) {
	return channels.DynamicSourceSyncResult{}, nil
}

func (s *refreshGuideNamesChannelsService) SyncDynamicChannelBlocks(context.Context) (channels.DynamicChannelSyncResult, error) {
	return s.dynamicBlocks, nil
}

func (s *refreshGuideNamesChannelsService) RefreshDynamicGeneratedGuideNames(context.Context) (int, error) {
	s.refreshCalls++
	if s.refreshErr != nil {
		return 0, s.refreshErr
	}
	return s.dynamicGuideNamesUpdated, nil
}

func TestReconcileSetsDynamicGuideNamesUpdatedFromRefresh(t *testing.T) {
	channelSvc := &refreshGuideNamesChannelsService{
		listChannels: nil,
		dynamicBlocks: channels.DynamicChannelSyncResult{
			QueriesProcessed: 1,
		},
		dynamicGuideNamesUpdated: 4,
	}

	reconciler, err := reconcile.New(staticCatalogStore{}, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := reconciler.Reconcile(context.Background(), nil)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.DynamicGuideNamesUpdated != 4 {
		t.Fatalf("result.DynamicGuideNamesUpdated = %d, want 4", result.DynamicGuideNamesUpdated)
	}
	if channelSvc.refreshCalls != 1 {
		t.Fatalf("RefreshDynamicGeneratedGuideNames() calls = %d, want 1", channelSvc.refreshCalls)
	}
}

func TestReconcileReturnsErrorWhenRefreshDynamicGuideNamesFails(t *testing.T) {
	channelSvc := &refreshGuideNamesChannelsService{
		listChannels:             nil,
		dynamicGuideNamesUpdated: 9,
		refreshErr:               errors.New("refresh failed"),
	}

	reconciler, err := reconcile.New(staticCatalogStore{}, channelSvc)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	result, err := reconciler.Reconcile(context.Background(), nil)
	if err == nil {
		t.Fatal("Reconcile() error = nil, want refresh dynamic generated guide names failure")
	}
	if !strings.Contains(err.Error(), "refresh dynamic generated guide names: refresh failed") {
		t.Fatalf("Reconcile() error = %v, want wrapped refresh error", err)
	}
	if result.DynamicGuideNamesUpdated != 0 {
		t.Fatalf("result.DynamicGuideNamesUpdated = %d, want 0 on refresh failure", result.DynamicGuideNamesUpdated)
	}
	if channelSvc.refreshCalls != 1 {
		t.Fatalf("RefreshDynamicGeneratedGuideNames() calls = %d, want 1", channelSvc.refreshCalls)
	}
}
