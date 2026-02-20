package playlist

// Item is a normalized playlist entry used by catalog and favorites workflows.
type Item struct {
	ItemKey     string
	ChannelKey  string
	Name        string
	Group       string
	StreamURL   string
	TVGID       string
	TVGLogo     string
	Attrs       map[string]string
	FirstSeenAt int64
	LastSeenAt  int64
	Active      bool
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
	Group       string
	GroupNames  []string
	Search      string
	SearchRegex bool
	Limit       int
	Offset      int
}
