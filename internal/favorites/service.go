package favorites

import (
	"context"
	"errors"
	"strings"
)

var (
	ErrItemNotFound = errors.New("playlist item not found")
	ErrNotFound     = errors.New("favorite not found")
)

// Favorite is a curated channel exposed in HDHomeRun lineup responses.
type Favorite struct {
	FavID       int64  `json:"fav_id"`
	ItemKey     string `json:"item_key"`
	OrderIndex  int    `json:"order_index"`
	GuideNumber string `json:"guide_number"`
	GuideName   string `json:"guide_name"`
	Enabled     bool   `json:"enabled"`
	StreamURL   string `json:"stream_url,omitempty"`
	TVGLogo     string `json:"tvg_logo,omitempty"`
	GroupName   string `json:"group_name,omitempty"`
}

// Store captures persistence operations required by favorites service.
type Store interface {
	AddFavorite(ctx context.Context, itemKey string, startGuideNumber int) (Favorite, error)
	RemoveFavorite(ctx context.Context, favID int64, startGuideNumber int) error
	ListFavorites(ctx context.Context, enabledOnly bool) ([]Favorite, error)
	ReorderFavorites(ctx context.Context, favIDs []int64, startGuideNumber int) error
}

// Service provides business operations for favorites CRUD + reorder.
type Service struct {
	store            Store
	startGuideNumber int
}

func NewService(store Store) *Service {
	return &Service{store: store, startGuideNumber: 100}
}

func (s *Service) Add(ctx context.Context, itemKey string) (Favorite, error) {
	if s == nil || s.store == nil {
		return Favorite{}, errors.New("favorites service is not configured")
	}
	itemKey = strings.TrimSpace(itemKey)
	if itemKey == "" {
		return Favorite{}, errors.New("item_key is required")
	}
	return s.store.AddFavorite(ctx, itemKey, s.startGuideNumber)
}

func (s *Service) Remove(ctx context.Context, favID int64) error {
	if s == nil || s.store == nil {
		return errors.New("favorites service is not configured")
	}
	if favID <= 0 {
		return errors.New("fav_id must be greater than zero")
	}
	return s.store.RemoveFavorite(ctx, favID, s.startGuideNumber)
}

func (s *Service) List(ctx context.Context) ([]Favorite, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("favorites service is not configured")
	}
	return s.store.ListFavorites(ctx, false)
}

func (s *Service) ListEnabled(ctx context.Context) ([]Favorite, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("favorites service is not configured")
	}
	return s.store.ListFavorites(ctx, true)
}

func (s *Service) Reorder(ctx context.Context, favIDs []int64) error {
	if s == nil || s.store == nil {
		return errors.New("favorites service is not configured")
	}
	return s.store.ReorderFavorites(ctx, favIDs, s.startGuideNumber)
}
