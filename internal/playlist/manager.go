package playlist

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/m3u"
)

// Manager fetches and parses remote playlists.
type Manager struct {
	client *http.Client
}

func NewManager(client *http.Client) *Manager {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Manager{client: client}
}

func (m *Manager) FetchAndParse(ctx context.Context, playlistURL string) ([]Item, error) {
	items := make([]Item, 0)
	_, err := m.FetchAndParseEach(ctx, playlistURL, func(item Item) error {
		items = append(items, item)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// FetchAndParseEach fetches a remote playlist and emits normalized entries incrementally.
func (m *Manager) FetchAndParseEach(ctx context.Context, playlistURL string, onItem func(Item) error) (int, error) {
	playlistURL = strings.TrimSpace(playlistURL)
	if playlistURL == "" {
		return 0, fmt.Errorf("playlist URL is required")
	}
	if onItem == nil {
		return 0, fmt.Errorf("item callback is required")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, playlistURL, nil)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetch playlist: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return 0, fmt.Errorf("fetch playlist: status %d", resp.StatusCode)
	}

	count, err := m.ParseEach(resp.Body, onItem)
	if err != nil {
		return 0, fmt.Errorf("parse playlist: %w", err)
	}

	return count, nil
}

func (m *Manager) Parse(r io.Reader) ([]Item, error) {
	items := make([]Item, 0)
	_, err := m.ParseEach(r, func(item Item) error {
		items = append(items, item)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// ParseEach parses playlist content and emits normalized entries incrementally.
func (m *Manager) ParseEach(r io.Reader, onItem func(Item) error) (int, error) {
	if onItem == nil {
		return 0, fmt.Errorf("item callback is required")
	}

	count, err := m3u.ParseEach(r, func(it m3u.Item) error {
		if sinkErr := onItem(Item{
			ItemKey:    it.ItemKey,
			ChannelKey: it.ChannelKey,
			Name:       it.Name,
			Group:      it.Group,
			StreamURL:  it.StreamURL,
			TVGID:      it.TVGID,
			TVGLogo:    it.TVGLogo,
			Attrs:      it.Attrs,
		}); sinkErr != nil {
			return &itemSinkError{err: sinkErr}
		}
		return nil
	})
	if err != nil {
		var sinkErr *itemSinkError
		if errors.As(err, &sinkErr) {
			return count, sinkErr.err
		}
	}
	return count, err
}

type itemSinkError struct {
	err error
}

func (e *itemSinkError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *itemSinkError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}
