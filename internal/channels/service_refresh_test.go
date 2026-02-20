package channels

import (
	"context"
	"errors"
	"testing"
)

type refreshGuideNameUnsupportedStore struct {
	Store
}

type refreshGuideNameDelegatingStore struct {
	Store
	calls   int
	gotCtx  context.Context
	updated int
	err     error
}

func (s *refreshGuideNameDelegatingStore) RefreshDynamicGeneratedGuideNames(ctx context.Context) (int, error) {
	s.calls++
	s.gotCtx = ctx
	return s.updated, s.err
}

func TestServiceRefreshDynamicGeneratedGuideNamesNilService(t *testing.T) {
	var svc *Service
	_, err := svc.RefreshDynamicGeneratedGuideNames(context.Background())
	if err == nil {
		t.Fatal("RefreshDynamicGeneratedGuideNames() error = nil, want service-not-configured error")
	}
	if err.Error() != "channels service is not configured" {
		t.Fatalf("RefreshDynamicGeneratedGuideNames() error = %q, want channels service is not configured", err)
	}
}

func TestServiceRefreshDynamicGeneratedGuideNamesStoreNotSupported(t *testing.T) {
	svc := &Service{
		store:            refreshGuideNameUnsupportedStore{},
		startGuideNumber: TraditionalGuideStart,
	}

	_, err := svc.RefreshDynamicGeneratedGuideNames(context.Background())
	if err == nil {
		t.Fatal("RefreshDynamicGeneratedGuideNames() error = nil, want unsupported-store error")
	}
	if err.Error() != "channels store does not support dynamic generated guide-name refresh" {
		t.Fatalf(
			"RefreshDynamicGeneratedGuideNames() error = %q, want channels store does not support dynamic generated guide-name refresh",
			err,
		)
	}
}

func TestServiceRefreshDynamicGeneratedGuideNamesDelegatesToStore(t *testing.T) {
	refresher := &refreshGuideNameDelegatingStore{updated: 7}
	svc := &Service{
		store:            refresher,
		startGuideNumber: TraditionalGuideStart,
	}

	ctx := context.WithValue(context.Background(), struct{}{}, "marker")
	updated, err := svc.RefreshDynamicGeneratedGuideNames(ctx)
	if err != nil {
		t.Fatalf("RefreshDynamicGeneratedGuideNames() error = %v", err)
	}
	if updated != 7 {
		t.Fatalf("RefreshDynamicGeneratedGuideNames() updated = %d, want 7", updated)
	}
	if refresher.calls != 1 {
		t.Fatalf("RefreshDynamicGeneratedGuideNames() calls = %d, want 1", refresher.calls)
	}
	if refresher.gotCtx != ctx {
		t.Fatal("RefreshDynamicGeneratedGuideNames() did not pass through request context")
	}
}

func TestServiceRefreshDynamicGeneratedGuideNamesPropagatesStoreError(t *testing.T) {
	wantErr := errors.New("refresh failed")
	refresher := &refreshGuideNameDelegatingStore{err: wantErr}
	svc := &Service{
		store:            refresher,
		startGuideNumber: TraditionalGuideStart,
	}

	updated, err := svc.RefreshDynamicGeneratedGuideNames(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("RefreshDynamicGeneratedGuideNames() error = %v, want %v", err, wantErr)
	}
	if updated != 0 {
		t.Fatalf("RefreshDynamicGeneratedGuideNames() updated = %d, want 0", updated)
	}
	if refresher.calls != 1 {
		t.Fatalf("RefreshDynamicGeneratedGuideNames() calls = %d, want 1", refresher.calls)
	}
}
