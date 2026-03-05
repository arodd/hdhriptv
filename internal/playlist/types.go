package playlist

import "errors"

var (
	ErrPlaylistSourceNotFound           = errors.New("playlist source not found")
	ErrPlaylistSourceOrderDrift         = errors.New("playlist source set changed")
	ErrPrimaryPlaylistSourceDelete      = errors.New("primary playlist source cannot be deleted")
	ErrPrimaryPlaylistSourceOrderChange = errors.New("primary playlist source cannot be removed from reorder set")
	ErrNoEnabledPlaylistSources         = errors.New("at least one enabled playlist source is required")
)

// Item is a normalized playlist entry used by catalog and favorites workflows.
type Item struct {
	ItemKey            string
	ChannelKey         string
	Name               string
	Group              string
	StreamURL          string
	TVGID              string
	TVGLogo            string
	PlaylistSourceID   int64
	PlaylistSourceName string
	Attrs              map[string]string
	FirstSeenAt        int64
	LastSeenAt         int64
	Active             bool
}

// ItemStream incrementally emits playlist items to a callback.
type ItemStream func(yield func(Item) error) error

// Group represents a playlist group with item count.
type Group struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// Query controls catalog filtering and pagination.
type Query struct {
	SourceIDs   []int64
	Group       string
	GroupNames  []string
	Search      string
	SearchRegex bool
	Limit       int
	Offset      int
}

// PlaylistSource describes one configured playlist ingest source.
type PlaylistSource struct {
	SourceID    int64  `json:"source_id"`
	SourceKey   string `json:"source_key"`
	Name        string `json:"name"`
	PlaylistURL string `json:"playlist_url"`
	TunerCount  int    `json:"tuner_count"`
	Enabled     bool   `json:"enabled"`
	OrderIndex  int    `json:"order_index"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

// PlaylistSourceCreate carries create arguments for one playlist source.
type PlaylistSourceCreate struct {
	Name        string `json:"name"`
	PlaylistURL string `json:"playlist_url"`
	TunerCount  int    `json:"tuner_count"`
	Enabled     *bool  `json:"enabled,omitempty"`
}

// PlaylistSourceUpdate carries mutable source fields.
type PlaylistSourceUpdate struct {
	Name        *string `json:"name,omitempty"`
	PlaylistURL *string `json:"playlist_url,omitempty"`
	TunerCount  *int    `json:"tuner_count,omitempty"`
	Enabled     *bool   `json:"enabled,omitempty"`
}

// PlaylistSourceBulkUpdate carries one ordered source mutation entry for atomic
// multi-source update operations.
type PlaylistSourceBulkUpdate struct {
	SourceID    int64
	Name        string
	PlaylistURL string
	TunerCount  int
	Enabled     bool
}
