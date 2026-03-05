package channels

import (
	"context"
	"errors"
	"strings"
	"time"
)

var (
	ErrItemNotFound         = errors.New("playlist item not found")
	ErrChannelNotFound      = errors.New("channel not found")
	ErrSourceNotFound       = errors.New("channel source not found")
	ErrSourceOrderDrift     = errors.New("channel source set changed")
	ErrDynamicQueryNotFound = errors.New("dynamic channel query not found")
	ErrAssociationMismatch  = errors.New("source metadata does not match channel metadata")
)

// Channel is a published HDHomeRun channel entry.
type Channel struct {
	ChannelID      int64             `json:"channel_id"`
	ChannelClass   string            `json:"channel_class,omitempty"`
	ChannelKey     string            `json:"channel_key,omitempty"`
	GuideNumber    string            `json:"guide_number"`
	GuideName      string            `json:"guide_name"`
	OrderIndex     int               `json:"order_index"`
	Enabled        bool              `json:"enabled"`
	DynamicQueryID int64             `json:"dynamic_query_id,omitempty"`
	DynamicItemKey string            `json:"dynamic_item_key,omitempty"`
	DynamicRule    DynamicSourceRule `json:"dynamic_rule"`
	SourceTotal    int               `json:"source_total"`
	SourceEnabled  int               `json:"source_enabled"`
	SourceDynamic  int               `json:"source_dynamic"`
	SourceManual   int               `json:"source_manual"`
}

// DynamicSourceRule defines one catalog-filter rule used to manage dynamic channel sources.
type DynamicSourceRule struct {
	Enabled     bool     `json:"enabled"`
	GroupName   string   `json:"group_name"`
	GroupNames  []string `json:"group_names,omitempty"`
	SourceIDs   []int64  `json:"source_ids"`
	SearchQuery string   `json:"search_query"`
	SearchRegex bool     `json:"search_regex,omitempty"`
}

// DynamicSourceSyncResult summarizes one dynamic source synchronization run.
type DynamicSourceSyncResult struct {
	Added    int `json:"added"`
	Removed  int `json:"removed"`
	Retained int `json:"retained"`
}

// Source is an ordered failover source attached to a channel.
type Source struct {
	SourceID             int64   `json:"source_id"`
	ChannelID            int64   `json:"channel_id"`
	ItemKey              string  `json:"item_key"`
	TVGName              string  `json:"tvg_name,omitempty"`
	StreamURL            string  `json:"stream_url,omitempty"`
	PlaylistSourceID     int64   `json:"playlist_source_id,omitempty"`
	PlaylistSourceName   string  `json:"playlist_source_name,omitempty"`
	PlaylistSourceTuners int     `json:"playlist_source_tuner_count,omitempty"`
	PriorityIndex        int     `json:"priority_index"`
	Enabled              bool    `json:"enabled"`
	AssociationType      string  `json:"association_type"`
	LastOKAt             int64   `json:"last_ok_at,omitempty"`
	LastFailAt           int64   `json:"last_fail_at,omitempty"`
	LastFailReason       string  `json:"last_fail_reason,omitempty"`
	SuccessCount         int     `json:"success_count"`
	FailCount            int     `json:"fail_count"`
	CooldownUntil        int64   `json:"cooldown_until"`
	LastProbeAt          int64   `json:"last_probe_at,omitempty"`
	ProfileWidth         int     `json:"profile_width,omitempty"`
	ProfileHeight        int     `json:"profile_height,omitempty"`
	ProfileFPS           float64 `json:"profile_fps,omitempty"`
	ProfileVideoCodec    string  `json:"profile_video_codec,omitempty"`
	ProfileAudioCodec    string  `json:"profile_audio_codec,omitempty"`
	ProfileBitrateBPS    int64   `json:"profile_bitrate_bps,omitempty"`
}

// SourceProfileUpdate carries persisted stream-profile metadata for one source.
type SourceProfileUpdate struct {
	LastProbeAt time.Time
	Width       int
	Height      int
	FPS         float64
	VideoCodec  string
	AudioCodec  string
	BitrateBPS  int64
}

// DuplicateItem is one catalog item included in a duplicate suggestion group.
type DuplicateItem struct {
	ItemKey            string `json:"item_key"`
	Name               string `json:"name"`
	GroupName          string `json:"group_name"`
	TVGID              string `json:"tvg_id,omitempty"`
	TVGLogo            string `json:"tvg_logo,omitempty"`
	StreamURL          string `json:"stream_url,omitempty"`
	PlaylistSourceID   int64  `json:"playlist_source_id,omitempty"`
	PlaylistSourceName string `json:"playlist_source_name,omitempty"`
	Active             bool   `json:"active"`
}

// DuplicateGroup is a grouped set of catalog items sharing the same channel key.
type DuplicateGroup struct {
	ChannelKey string          `json:"channel_key"`
	Count      int             `json:"count"`
	Items      []DuplicateItem `json:"items"`
}

// Store captures persistence operations required by channel management.
type Store interface {
	CreateChannelFromItem(ctx context.Context, itemKey, guideName, channelKey string, dynamicRule *DynamicSourceRule, startGuideNumber int) (Channel, error)
	DeleteChannel(ctx context.Context, channelID int64, startGuideNumber int) error
	ListChannels(ctx context.Context, enabledOnly bool) ([]Channel, error)
	ListChannelsPaged(ctx context.Context, enabledOnly bool, limit, offset int) ([]Channel, int, error)
	ListLineupChannels(ctx context.Context) ([]Channel, error)
	ReorderChannels(ctx context.Context, channelIDs []int64, startGuideNumber int) error
	UpdateChannel(ctx context.Context, channelID int64, guideName *string, enabled *bool, dynamicRule *DynamicSourceRule) (Channel, error)
	GetChannelByGuideNumber(ctx context.Context, guideNumber string) (Channel, error)
	SyncDynamicChannelBlocks(ctx context.Context) (DynamicChannelSyncResult, error)
	ListDynamicChannelQueries(ctx context.Context) ([]DynamicChannelQuery, error)
	ListDynamicChannelQueriesPaged(ctx context.Context, limit, offset int) ([]DynamicChannelQuery, int, error)
	GetDynamicChannelQuery(ctx context.Context, queryID int64) (DynamicChannelQuery, error)
	CreateDynamicChannelQuery(ctx context.Context, create DynamicChannelQueryCreate) (DynamicChannelQuery, error)
	UpdateDynamicChannelQuery(ctx context.Context, queryID int64, update DynamicChannelQueryUpdate) (DynamicChannelQuery, error)
	DeleteDynamicChannelQuery(ctx context.Context, queryID int64) error
	ListDynamicGeneratedChannelsPaged(ctx context.Context, queryID int64, limit, offset int) ([]Channel, int, error)
	ReorderDynamicGeneratedChannels(ctx context.Context, queryID int64, channelIDs []int64) error
	GetSource(ctx context.Context, channelID, sourceID int64, enabledOnly bool) (Source, error)
	ListSources(ctx context.Context, channelID int64, enabledOnly bool) ([]Source, error)
	ListSourcesPaged(ctx context.Context, channelID int64, enabledOnly bool, limit, offset int) ([]Source, int, error)
	AddSource(ctx context.Context, channelID int64, itemKey string, allowCrossChannel bool) (Source, error)
	DeleteSource(ctx context.Context, channelID, sourceID int64) error
	ReorderSources(ctx context.Context, channelID int64, sourceIDs []int64) error
	UpdateSource(ctx context.Context, channelID, sourceID int64, enabled *bool) (Source, error)
	SyncDynamicSources(ctx context.Context, channelID int64, matchedItemKeys []string) (DynamicSourceSyncResult, error)
	MarkSourceFailure(ctx context.Context, sourceID int64, reason string, failedAt time.Time) error
	MarkSourceSuccess(ctx context.Context, sourceID int64, succeededAt time.Time) error
	UpdateSourceProfile(ctx context.Context, sourceID int64, profile SourceProfileUpdate) error
	ClearSourceHealth(ctx context.Context, channelID int64) (int64, error)
	ClearAllSourceHealth(ctx context.Context) (int64, error)
	ListDuplicateSuggestions(ctx context.Context, minItems int, searchQuery string, limit, offset int) ([]DuplicateGroup, int, error)
}

type bulkSourceLister interface {
	ListSourcesByChannelIDs(ctx context.Context, channelIDs []int64, enabledOnly bool) (map[int64][]Source, error)
}

type dynamicCatalogFilterSyncStore interface {
	SyncDynamicSourcesByCatalogFilter(ctx context.Context, channelID int64, sourceIDs []int64, groupNames []string, searchQuery string, searchRegex bool, pageSize int, maxMatches int) (DynamicSourceSyncResult, int, error)
}

type dynamicGeneratedGuideNameRefresher interface {
	RefreshDynamicGeneratedGuideNames(ctx context.Context) (int, error)
}

type sourceScopedDynamicChannelBlocksCapabilityStore interface {
	SupportsSourceScopedDynamicChannelBlocks() bool
}

// Service provides business operations for published channels and source mappings.
type Service struct {
	store            Store
	startGuideNumber int
}

func NewService(store Store) *Service {
	return NewServiceWithStartGuideNumber(store, TraditionalGuideStart)
}

func NewServiceWithStartGuideNumber(store Store, startGuideNumber int) *Service {
	if startGuideNumber < 1 || startGuideNumber >= DynamicGuideStart {
		startGuideNumber = TraditionalGuideStart
	}
	return &Service{store: store, startGuideNumber: startGuideNumber}
}

func (s *Service) Create(ctx context.Context, itemKey, guideName, channelKey string, dynamicRule *DynamicSourceRule) (Channel, error) {
	if s == nil || s.store == nil {
		return Channel{}, errors.New("channels service is not configured")
	}
	itemKey = strings.TrimSpace(itemKey)
	normalizedRule, err := normalizeDynamicRuleInput(dynamicRule)
	if err != nil {
		return Channel{}, err
	}
	if itemKey == "" && (normalizedRule == nil || !normalizedRule.Enabled) {
		return Channel{}, errors.New("item_key is required unless dynamic_rule.enabled is true")
	}
	return s.store.CreateChannelFromItem(ctx, itemKey, guideName, channelKey, normalizedRule, s.startGuideNumber)
}

func (s *Service) Delete(ctx context.Context, channelID int64) error {
	if s == nil || s.store == nil {
		return errors.New("channels service is not configured")
	}
	if channelID <= 0 {
		return errors.New("channel_id must be greater than zero")
	}
	return s.store.DeleteChannel(ctx, channelID, s.startGuideNumber)
}

func (s *Service) List(ctx context.Context) ([]Channel, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("channels service is not configured")
	}
	return s.store.ListChannels(ctx, false)
}

func (s *Service) ListEnabled(ctx context.Context) ([]Channel, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("channels service is not configured")
	}
	return s.store.ListLineupChannels(ctx)
}

func (s *Service) ListPaged(ctx context.Context, limit, offset int) ([]Channel, int, error) {
	if s == nil || s.store == nil {
		return nil, 0, errors.New("channels service is not configured")
	}
	if limit < 0 {
		return nil, 0, errors.New("limit must be zero or greater")
	}
	if offset < 0 {
		return nil, 0, errors.New("offset must be zero or greater")
	}
	return s.store.ListChannelsPaged(ctx, false, limit, offset)
}

func (s *Service) Reorder(ctx context.Context, channelIDs []int64) error {
	if s == nil || s.store == nil {
		return errors.New("channels service is not configured")
	}
	return s.store.ReorderChannels(ctx, channelIDs, s.startGuideNumber)
}

func (s *Service) Update(ctx context.Context, channelID int64, guideName *string, enabled *bool, dynamicRule *DynamicSourceRule) (Channel, error) {
	if s == nil || s.store == nil {
		return Channel{}, errors.New("channels service is not configured")
	}
	if channelID <= 0 {
		return Channel{}, errors.New("channel_id must be greater than zero")
	}
	if guideName != nil {
		trimmed := strings.TrimSpace(*guideName)
		if trimmed == "" {
			return Channel{}, errors.New("guide_name must not be empty")
		}
		guideName = &trimmed
	}
	normalizedRule, err := normalizeDynamicRuleInput(dynamicRule)
	if err != nil {
		return Channel{}, err
	}
	if guideName == nil && enabled == nil && normalizedRule == nil {
		return Channel{}, errors.New("at least one field must be provided")
	}
	return s.store.UpdateChannel(ctx, channelID, guideName, enabled, normalizedRule)
}

func (s *Service) GetByGuideNumber(ctx context.Context, guideNumber string) (Channel, error) {
	if s == nil || s.store == nil {
		return Channel{}, errors.New("channels service is not configured")
	}
	guideNumber = strings.TrimSpace(guideNumber)
	if guideNumber == "" {
		return Channel{}, errors.New("guide_number is required")
	}
	return s.store.GetChannelByGuideNumber(ctx, guideNumber)
}

func (s *Service) ListSources(ctx context.Context, channelID int64, enabledOnly bool) ([]Source, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("channels service is not configured")
	}
	if channelID <= 0 {
		return nil, errors.New("channel_id must be greater than zero")
	}
	return s.store.ListSources(ctx, channelID, enabledOnly)
}

func (s *Service) ListSourcesByChannelIDs(ctx context.Context, channelIDs []int64, enabledOnly bool) (map[int64][]Source, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("channels service is not configured")
	}

	deduped := make([]int64, 0, len(channelIDs))
	seen := make(map[int64]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		if channelID <= 0 {
			return nil, errors.New("channel_id must be greater than zero")
		}
		if _, ok := seen[channelID]; ok {
			continue
		}
		seen[channelID] = struct{}{}
		deduped = append(deduped, channelID)
	}

	if len(deduped) == 0 {
		return map[int64][]Source{}, nil
	}

	if bulkStore, ok := s.store.(bulkSourceLister); ok {
		return bulkStore.ListSourcesByChannelIDs(ctx, deduped, enabledOnly)
	}

	out := make(map[int64][]Source, len(deduped))
	for _, channelID := range deduped {
		sources, err := s.store.ListSources(ctx, channelID, enabledOnly)
		if err != nil {
			return nil, err
		}
		out[channelID] = sources
	}
	return out, nil
}

func (s *Service) GetSource(ctx context.Context, channelID, sourceID int64, enabledOnly bool) (Source, error) {
	if s == nil || s.store == nil {
		return Source{}, errors.New("channels service is not configured")
	}
	if channelID <= 0 || sourceID <= 0 {
		return Source{}, errors.New("channel_id and source_id must be greater than zero")
	}
	return s.store.GetSource(ctx, channelID, sourceID, enabledOnly)
}

func (s *Service) ListSourcesPaged(ctx context.Context, channelID int64, enabledOnly bool, limit, offset int) ([]Source, int, error) {
	if s == nil || s.store == nil {
		return nil, 0, errors.New("channels service is not configured")
	}
	if channelID <= 0 {
		return nil, 0, errors.New("channel_id must be greater than zero")
	}
	if limit < 0 {
		return nil, 0, errors.New("limit must be zero or greater")
	}
	if offset < 0 {
		return nil, 0, errors.New("offset must be zero or greater")
	}
	return s.store.ListSourcesPaged(ctx, channelID, enabledOnly, limit, offset)
}

func (s *Service) AddSource(ctx context.Context, channelID int64, itemKey string, allowCrossChannel bool) (Source, error) {
	if s == nil || s.store == nil {
		return Source{}, errors.New("channels service is not configured")
	}
	if channelID <= 0 {
		return Source{}, errors.New("channel_id must be greater than zero")
	}
	itemKey = strings.TrimSpace(itemKey)
	if itemKey == "" {
		return Source{}, errors.New("item_key is required")
	}
	return s.store.AddSource(ctx, channelID, itemKey, allowCrossChannel)
}

func (s *Service) DeleteSource(ctx context.Context, channelID, sourceID int64) error {
	if s == nil || s.store == nil {
		return errors.New("channels service is not configured")
	}
	if channelID <= 0 || sourceID <= 0 {
		return errors.New("channel_id and source_id must be greater than zero")
	}
	return s.store.DeleteSource(ctx, channelID, sourceID)
}

func (s *Service) ReorderSources(ctx context.Context, channelID int64, sourceIDs []int64) error {
	if s == nil || s.store == nil {
		return errors.New("channels service is not configured")
	}
	if channelID <= 0 {
		return errors.New("channel_id must be greater than zero")
	}
	return s.store.ReorderSources(ctx, channelID, sourceIDs)
}

func (s *Service) UpdateSource(ctx context.Context, channelID, sourceID int64, enabled *bool) (Source, error) {
	if s == nil || s.store == nil {
		return Source{}, errors.New("channels service is not configured")
	}
	if channelID <= 0 || sourceID <= 0 {
		return Source{}, errors.New("channel_id and source_id must be greater than zero")
	}
	if enabled == nil {
		return Source{}, errors.New("at least one field must be provided")
	}
	return s.store.UpdateSource(ctx, channelID, sourceID, enabled)
}

func (s *Service) SyncDynamicSources(ctx context.Context, channelID int64, matchedItemKeys []string) (DynamicSourceSyncResult, error) {
	if s == nil || s.store == nil {
		return DynamicSourceSyncResult{}, errors.New("channels service is not configured")
	}
	if channelID <= 0 {
		return DynamicSourceSyncResult{}, errors.New("channel_id must be greater than zero")
	}

	deduped := make([]string, 0, len(matchedItemKeys))
	seen := make(map[string]struct{}, len(matchedItemKeys))
	for _, itemKey := range matchedItemKeys {
		itemKey = strings.TrimSpace(itemKey)
		if itemKey == "" {
			continue
		}
		if _, ok := seen[itemKey]; ok {
			continue
		}
		seen[itemKey] = struct{}{}
		deduped = append(deduped, itemKey)
	}
	return s.store.SyncDynamicSources(ctx, channelID, deduped)
}

// SyncDynamicSourcesByCatalogFilter synchronizes dynamic sources for one channel by
// streaming catalog-filter matches in bounded pages when supported by the backing store.
func (s *Service) SyncDynamicSourcesByCatalogFilter(
	ctx context.Context,
	channelID int64,
	groupNames []string,
	searchQuery string,
	searchRegex bool,
	pageSize int,
	maxMatches int,
) (DynamicSourceSyncResult, int, error) {
	return s.SyncDynamicSourcesByCatalogFilterWithSourceIDs(ctx, channelID, nil, groupNames, searchQuery, searchRegex, pageSize, maxMatches)
}

// SyncDynamicSourcesByCatalogFilterWithSourceIDs synchronizes dynamic sources
// for one channel by streaming catalog-filter matches in bounded pages when
// supported by the backing store, optionally scoping matches to playlist
// source IDs.
func (s *Service) SyncDynamicSourcesByCatalogFilterWithSourceIDs(
	ctx context.Context,
	channelID int64,
	sourceIDs []int64,
	groupNames []string,
	searchQuery string,
	searchRegex bool,
	pageSize int,
	maxMatches int,
) (DynamicSourceSyncResult, int, error) {
	if s == nil || s.store == nil {
		return DynamicSourceSyncResult{}, 0, errors.New("channels service is not configured")
	}
	if channelID <= 0 {
		return DynamicSourceSyncResult{}, 0, errors.New("channel_id must be greater than zero")
	}
	syncStore, ok := s.store.(dynamicCatalogFilterSyncStore)
	if !ok {
		return DynamicSourceSyncResult{}, 0, errors.New("channels store does not support catalog-filter dynamic sync")
	}

	groupNames = NormalizeGroupNames("", groupNames)
	sourceIDs = NormalizeSourceIDs(sourceIDs)
	searchQuery = strings.TrimSpace(searchQuery)
	return syncStore.SyncDynamicSourcesByCatalogFilter(ctx, channelID, sourceIDs, groupNames, searchQuery, searchRegex, pageSize, maxMatches)
}

func (s *Service) SyncDynamicChannelBlocks(ctx context.Context) (DynamicChannelSyncResult, error) {
	if s == nil || s.store == nil {
		return DynamicChannelSyncResult{}, errors.New("channels service is not configured")
	}
	return s.store.SyncDynamicChannelBlocks(ctx)
}

// SupportsSourceScopedDynamicChannelBlocks reports whether the backing store
// can materialize dynamic channel blocks with source-scoped query filters.
func (s *Service) SupportsSourceScopedDynamicChannelBlocks() bool {
	if s == nil || s.store == nil {
		return false
	}
	capability, ok := s.store.(sourceScopedDynamicChannelBlocksCapabilityStore)
	if !ok {
		return false
	}
	return capability.SupportsSourceScopedDynamicChannelBlocks()
}

func (s *Service) RefreshDynamicGeneratedGuideNames(ctx context.Context) (int, error) {
	if s == nil || s.store == nil {
		return 0, errors.New("channels service is not configured")
	}
	refresher, ok := s.store.(dynamicGeneratedGuideNameRefresher)
	if !ok {
		return 0, errors.New("channels store does not support dynamic generated guide-name refresh")
	}
	return refresher.RefreshDynamicGeneratedGuideNames(ctx)
}

func (s *Service) ListDynamicChannelQueries(ctx context.Context) ([]DynamicChannelQuery, error) {
	if s == nil || s.store == nil {
		return nil, errors.New("channels service is not configured")
	}
	return s.store.ListDynamicChannelQueries(ctx)
}

func (s *Service) ListDynamicChannelQueriesPaged(ctx context.Context, limit, offset int) ([]DynamicChannelQuery, int, error) {
	if s == nil || s.store == nil {
		return nil, 0, errors.New("channels service is not configured")
	}
	if limit < 0 {
		return nil, 0, errors.New("limit must be zero or greater")
	}
	if offset < 0 {
		return nil, 0, errors.New("offset must be zero or greater")
	}
	return s.store.ListDynamicChannelQueriesPaged(ctx, limit, offset)
}

func (s *Service) GetDynamicChannelQuery(ctx context.Context, queryID int64) (DynamicChannelQuery, error) {
	if s == nil || s.store == nil {
		return DynamicChannelQuery{}, errors.New("channels service is not configured")
	}
	if queryID <= 0 {
		return DynamicChannelQuery{}, errors.New("query_id must be greater than zero")
	}
	return s.store.GetDynamicChannelQuery(ctx, queryID)
}

func (s *Service) CreateDynamicChannelQuery(ctx context.Context, create DynamicChannelQueryCreate) (DynamicChannelQuery, error) {
	if s == nil || s.store == nil {
		return DynamicChannelQuery{}, errors.New("channels service is not configured")
	}

	normalized, err := normalizeDynamicChannelQueryCreateInput(create)
	if err != nil {
		return DynamicChannelQuery{}, err
	}
	return s.store.CreateDynamicChannelQuery(ctx, normalized)
}

func (s *Service) UpdateDynamicChannelQuery(ctx context.Context, queryID int64, update DynamicChannelQueryUpdate) (DynamicChannelQuery, error) {
	if s == nil || s.store == nil {
		return DynamicChannelQuery{}, errors.New("channels service is not configured")
	}
	if queryID <= 0 {
		return DynamicChannelQuery{}, errors.New("query_id must be greater than zero")
	}
	normalized, err := normalizeDynamicChannelQueryUpdateInput(update)
	if err != nil {
		return DynamicChannelQuery{}, err
	}
	return s.store.UpdateDynamicChannelQuery(ctx, queryID, normalized)
}

func (s *Service) DeleteDynamicChannelQuery(ctx context.Context, queryID int64) error {
	if s == nil || s.store == nil {
		return errors.New("channels service is not configured")
	}
	if queryID <= 0 {
		return errors.New("query_id must be greater than zero")
	}
	return s.store.DeleteDynamicChannelQuery(ctx, queryID)
}

func (s *Service) ListDynamicGeneratedChannelsPaged(ctx context.Context, queryID int64, limit, offset int) ([]Channel, int, error) {
	if s == nil || s.store == nil {
		return nil, 0, errors.New("channels service is not configured")
	}
	if queryID <= 0 {
		return nil, 0, errors.New("query_id must be greater than zero")
	}
	if limit < 0 {
		return nil, 0, errors.New("limit must be zero or greater")
	}
	if offset < 0 {
		return nil, 0, errors.New("offset must be zero or greater")
	}
	return s.store.ListDynamicGeneratedChannelsPaged(ctx, queryID, limit, offset)
}

func (s *Service) ReorderDynamicGeneratedChannels(ctx context.Context, queryID int64, channelIDs []int64) error {
	if s == nil || s.store == nil {
		return errors.New("channels service is not configured")
	}
	if queryID <= 0 {
		return errors.New("query_id must be greater than zero")
	}
	return s.store.ReorderDynamicGeneratedChannels(ctx, queryID, channelIDs)
}

func (s *Service) MarkSourceFailure(ctx context.Context, sourceID int64, reason string, failedAt time.Time) error {
	if s == nil || s.store == nil {
		return errors.New("channels service is not configured")
	}
	if sourceID <= 0 {
		return errors.New("source_id must be greater than zero")
	}
	if failedAt.IsZero() {
		failedAt = time.Now().UTC()
	}
	return s.store.MarkSourceFailure(ctx, sourceID, reason, failedAt)
}

func (s *Service) MarkSourceSuccess(ctx context.Context, sourceID int64, succeededAt time.Time) error {
	if s == nil || s.store == nil {
		return errors.New("channels service is not configured")
	}
	if sourceID <= 0 {
		return errors.New("source_id must be greater than zero")
	}
	if succeededAt.IsZero() {
		succeededAt = time.Now().UTC()
	}
	return s.store.MarkSourceSuccess(ctx, sourceID, succeededAt)
}

func (s *Service) UpdateSourceProfile(ctx context.Context, sourceID int64, profile SourceProfileUpdate) error {
	if s == nil || s.store == nil {
		return errors.New("channels service is not configured")
	}
	if sourceID <= 0 {
		return errors.New("source_id must be greater than zero")
	}
	if profile.Width < 0 || profile.Height < 0 {
		return errors.New("profile width and height must be non-negative")
	}
	if profile.FPS < 0 {
		return errors.New("profile fps must be non-negative")
	}
	if profile.BitrateBPS < 0 {
		return errors.New("profile bitrate must be non-negative")
	}
	profile.VideoCodec = strings.TrimSpace(profile.VideoCodec)
	profile.AudioCodec = strings.TrimSpace(profile.AudioCodec)
	return s.store.UpdateSourceProfile(ctx, sourceID, profile)
}

func (s *Service) ClearSourceHealth(ctx context.Context, channelID int64) (int64, error) {
	if s == nil || s.store == nil {
		return 0, errors.New("channels service is not configured")
	}
	if channelID <= 0 {
		return 0, errors.New("channel_id must be greater than zero")
	}
	return s.store.ClearSourceHealth(ctx, channelID)
}

func (s *Service) ClearAllSourceHealth(ctx context.Context) (int64, error) {
	if s == nil || s.store == nil {
		return 0, errors.New("channels service is not configured")
	}
	return s.store.ClearAllSourceHealth(ctx)
}

func (s *Service) DuplicateSuggestions(ctx context.Context, minItems int, searchQuery string, limit, offset int) ([]DuplicateGroup, int, error) {
	if s == nil || s.store == nil {
		return nil, 0, errors.New("channels service is not configured")
	}
	if minItems < 2 {
		minItems = 2
	}
	if limit < 1 {
		limit = 1
	}
	if offset < 0 {
		offset = 0
	}
	return s.store.ListDuplicateSuggestions(ctx, minItems, strings.TrimSpace(searchQuery), limit, offset)
}

func normalizeDynamicRuleInput(rule *DynamicSourceRule) (*DynamicSourceRule, error) {
	if rule == nil {
		return nil, nil
	}
	if err := ValidateSourceIDs(rule.SourceIDs, "dynamic_rule.source_ids"); err != nil {
		return nil, err
	}
	groupNames := NormalizeGroupNames(rule.GroupName, rule.GroupNames)
	sourceIDs := NormalizeSourceIDs(rule.SourceIDs)
	normalized := DynamicSourceRule{
		Enabled:     rule.Enabled,
		GroupName:   GroupNameAlias(groupNames),
		GroupNames:  groupNames,
		SourceIDs:   sourceIDs,
		SearchQuery: strings.TrimSpace(rule.SearchQuery),
		SearchRegex: rule.SearchRegex,
	}
	if normalized.Enabled {
		if normalized.SearchQuery == "" {
			return nil, errors.New("dynamic_rule.search_query is required when enabled")
		}
	}
	return &normalized, nil
}

func normalizeDynamicChannelQueryCreateInput(create DynamicChannelQueryCreate) (DynamicChannelQueryCreate, error) {
	if err := ValidateSourceIDs(create.SourceIDs, "source_ids"); err != nil {
		return DynamicChannelQueryCreate{}, err
	}
	groupNames := NormalizeGroupNames(create.GroupName, create.GroupNames)
	sourceIDs := NormalizeSourceIDs(create.SourceIDs)
	normalized := DynamicChannelQueryCreate{
		Enabled:     create.Enabled,
		Name:        strings.TrimSpace(create.Name),
		GroupName:   GroupNameAlias(groupNames),
		GroupNames:  groupNames,
		SourceIDs:   sourceIDs,
		SearchQuery: strings.TrimSpace(create.SearchQuery),
		SearchRegex: create.SearchRegex,
	}
	return normalized, nil
}

func normalizeDynamicChannelQueryUpdateInput(update DynamicChannelQueryUpdate) (DynamicChannelQueryUpdate, error) {
	normalized := DynamicChannelQueryUpdate{
		Enabled: update.Enabled,
	}
	if update.Name != nil {
		value := strings.TrimSpace(*update.Name)
		normalized.Name = &value
	}
	if update.GroupName != nil || update.GroupNames != nil {
		legacyGroup := ""
		if update.GroupName != nil {
			legacyGroup = *update.GroupName
		}
		var rawGroups []string
		if update.GroupNames != nil {
			rawGroups = append(rawGroups, (*update.GroupNames)...)
		}
		groupNames := NormalizeGroupNames(legacyGroup, rawGroups)
		alias := GroupNameAlias(groupNames)
		groupNamesCopy := append([]string(nil), groupNames...)
		normalized.GroupName = &alias
		normalized.GroupNames = &groupNamesCopy
	}
	if update.SourceIDs != nil {
		if err := ValidateSourceIDs(*update.SourceIDs, "source_ids"); err != nil {
			return DynamicChannelQueryUpdate{}, err
		}
		sourceIDs := NormalizeSourceIDs(*update.SourceIDs)
		normalized.SourceIDs = &sourceIDs
	}
	if update.SearchQuery != nil {
		value := strings.TrimSpace(*update.SearchQuery)
		normalized.SearchQuery = &value
	}
	if update.SearchRegex != nil {
		value := *update.SearchRegex
		normalized.SearchRegex = &value
	}
	if normalized.Enabled == nil && normalized.Name == nil && normalized.GroupName == nil && normalized.GroupNames == nil && normalized.SourceIDs == nil && normalized.SearchQuery == nil && normalized.SearchRegex == nil {
		return DynamicChannelQueryUpdate{}, errors.New("at least one field must be provided")
	}
	return normalized, nil
}
