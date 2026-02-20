package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/arodd/hdhriptv/internal/channels"
)

// CatalogStore provides active source lookups by channel key.
type CatalogStore interface {
	ListActiveItemKeysByChannelKey(ctx context.Context, channelKey string) ([]string, error)
	ListActiveItemKeysByCatalogFilter(ctx context.Context, groupNames []string, searchQuery string, searchRegex bool) ([]string, error)
}

type catalogFilterIterator interface {
	IterateActiveItemKeysByCatalogFilter(ctx context.Context, groupNames []string, searchQuery string, searchRegex bool, pageSize int, visit func(itemKey string) error) (int, error)
}

// ChannelsService provides channel and source operations needed for reconciliation.
type ChannelsService interface {
	List(ctx context.Context) ([]channels.Channel, error)
	ListSources(ctx context.Context, channelID int64, enabledOnly bool) ([]channels.Source, error)
	AddSource(ctx context.Context, channelID int64, itemKey string, allowCrossChannel bool) (channels.Source, error)
	// SyncDynamicSources must treat matchedItemKeys as read-only input.
	SyncDynamicSources(ctx context.Context, channelID int64, matchedItemKeys []string) (channels.DynamicSourceSyncResult, error)
	SyncDynamicChannelBlocks(ctx context.Context) (channels.DynamicChannelSyncResult, error)
	RefreshDynamicGeneratedGuideNames(ctx context.Context) (int, error)
}

type catalogFilterDynamicSourceSyncer interface {
	SyncDynamicSourcesByCatalogFilter(ctx context.Context, channelID int64, groupNames []string, searchQuery string, searchRegex bool, pageSize int, maxMatches int) (channels.DynamicSourceSyncResult, int, error)
}

// Result summarizes one reconciliation run.
type Result struct {
	ChannelsTotal            int `json:"channels_total"`
	ChannelsProcessed        int `json:"channels_processed"`
	ChannelsSkipped          int `json:"channels_skipped"`
	SourcesAdded             int `json:"sources_added"`
	SourcesAlreadySeen       int `json:"sources_existing"`
	DynamicBlocksProcessed   int `json:"dynamic_blocks_processed"`
	DynamicBlocksEnabled     int `json:"dynamic_blocks_enabled"`
	DynamicChannelsAdded     int `json:"dynamic_channels_added"`
	DynamicChannelsUpdated   int `json:"dynamic_channels_updated"`
	DynamicChannelsRetained  int `json:"dynamic_channels_retained"`
	DynamicChannelsRemoved   int `json:"dynamic_channels_removed"`
	DynamicChannelsTruncated int `json:"dynamic_channels_truncated"`
	DynamicChannelsProcessed int `json:"dynamic_channels_processed"`
	DynamicSourcesAdded      int `json:"dynamic_sources_added"`
	DynamicSourcesRemoved    int `json:"dynamic_sources_removed"`
	DynamicGuideNamesUpdated int `json:"dynamic_guide_names_updated"`
}

// Service reconciles published channels against active catalog sources.
type Service struct {
	catalog               CatalogStore
	channels              ChannelsService
	logger                *slog.Logger
	dynamicRulePagedMode  bool
	dynamicRuleMatchLimit int
}

type dynamicCatalogFilterKey struct {
	GroupNames  string
	SearchQuery string
	SearchRegex bool
}

const dynamicCatalogFilterPageSize = 512

func New(catalog CatalogStore, channelsSvc ChannelsService) (*Service, error) {
	if catalog == nil {
		return nil, fmt.Errorf("catalog store is required")
	}
	if channelsSvc == nil {
		return nil, fmt.Errorf("channels service is required")
	}
	return &Service{
		catalog:               catalog,
		channels:              channelsSvc,
		logger:                slog.Default(),
		dynamicRulePagedMode:  false,
		dynamicRuleMatchLimit: channels.DynamicGuideBlockMaxLen,
	}, nil
}

func (s *Service) SetLogger(logger *slog.Logger) {
	if s == nil {
		return
	}
	if logger == nil {
		s.logger = slog.Default()
		return
	}
	s.logger = logger
}

func (s *Service) loggerOrDefault() *slog.Logger {
	if s == nil || s.logger == nil {
		return slog.Default()
	}
	return s.logger
}

// SetDynamicRulePagedMode toggles paged catalog-filter sync for dynamic rules.
// When disabled, reconcile uses the legacy in-memory slice path.
func (s *Service) SetDynamicRulePagedMode(enabled bool) {
	if s == nil {
		return
	}
	s.dynamicRulePagedMode = enabled
}

// SetDynamicRuleMatchLimit sets the maximum allowed matches for one dynamic
// rule sync pass. Values <= 0 disable the guard.
func (s *Service) SetDynamicRuleMatchLimit(limit int) {
	if s == nil {
		return
	}
	s.dynamicRuleMatchLimit = limit
}

// CountChannels returns the number of reconcilable channels.
func (s *Service) CountChannels(ctx context.Context) (int, error) {
	channelList, err := s.channels.List(ctx)
	if err != nil {
		return 0, fmt.Errorf("list channels: %w", err)
	}

	total := 0
	for _, channel := range channelList {
		if isReconcilable(channel) {
			total++
		}
	}
	return total, nil
}

// Reconcile updates channel sources by mode:
//   - static channels append newly discovered channel_key matches
//   - dynamic channels synchronize association_type=dynamic_query to catalog filter output
func (s *Service) Reconcile(ctx context.Context, onProgress func(cur, max int) error) (Result, error) {
	dynamicResult, err := s.channels.SyncDynamicChannelBlocks(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("sync dynamic channel blocks: %w", err)
	}
	if dynamicResult.TruncatedCount > 0 {
		s.loggerOrDefault().Warn(
			"dynamic channel block matches exceeded cap",
			"cap", channels.DynamicGuideBlockMaxLen,
			"truncated_total", dynamicResult.TruncatedCount,
		)
	}

	channelList, err := s.channels.List(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("list channels: %w", err)
	}

	reconcilable := make([]channels.Channel, 0, len(channelList))
	result := Result{
		ChannelsTotal:            len(channelList),
		DynamicBlocksProcessed:   dynamicResult.QueriesProcessed,
		DynamicBlocksEnabled:     dynamicResult.QueriesEnabled,
		DynamicChannelsAdded:     dynamicResult.ChannelsAdded,
		DynamicChannelsUpdated:   dynamicResult.ChannelsUpdated,
		DynamicChannelsRetained:  dynamicResult.ChannelsRetained,
		DynamicChannelsRemoved:   dynamicResult.ChannelsRemoved,
		DynamicChannelsTruncated: dynamicResult.TruncatedCount,
	}

	for _, channel := range channelList {
		if !isReconcilable(channel) {
			result.ChannelsSkipped++
			continue
		}
		reconcilable = append(reconcilable, channel)
	}

	progressMax := len(reconcilable)
	dynamicRuleUsage := make(map[dynamicCatalogFilterKey]int, progressMax)
	for _, channel := range reconcilable {
		if !channel.DynamicRule.Enabled {
			continue
		}
		groupNames := channels.NormalizeGroupNames(channel.DynamicRule.GroupName, channel.DynamicRule.GroupNames)
		ruleKey := newDynamicCatalogFilterKey(groupNames, channel.DynamicRule.SearchQuery, channel.DynamicRule.SearchRegex)
		dynamicRuleUsage[ruleKey]++
	}
	dynamicLookupCache := make(map[dynamicCatalogFilterKey][]string, len(dynamicRuleUsage))
	for idx, channel := range reconcilable {
		if err := s.reconcileOneChannel(ctx, channel, &result, dynamicLookupCache, dynamicRuleUsage); err != nil {
			return result, fmt.Errorf("reconcile channel %d (%s): %w", channel.ChannelID, channel.ChannelKey, err)
		}
		result.ChannelsProcessed++
		if onProgress != nil {
			if err := onProgress(idx+1, progressMax); err != nil {
				return result, err
			}
		}
	}

	updatedGuideNames, err := s.channels.RefreshDynamicGeneratedGuideNames(ctx)
	if err != nil {
		return result, fmt.Errorf("refresh dynamic generated guide names: %w", err)
	}
	result.DynamicGuideNamesUpdated = updatedGuideNames

	return result, nil
}

func (s *Service) reconcileOneChannel(
	ctx context.Context,
	channel channels.Channel,
	result *Result,
	dynamicLookupCache map[dynamicCatalogFilterKey][]string,
	dynamicRuleUsage map[dynamicCatalogFilterKey]int,
) error {
	if channel.DynamicRule.Enabled {
		groupNames := channels.NormalizeGroupNames(channel.DynamicRule.GroupName, channel.DynamicRule.GroupNames)
		ruleKey := newDynamicCatalogFilterKey(groupNames, channel.DynamicRule.SearchQuery, channel.DynamicRule.SearchRegex)

		// Keep shared-rule cache mode for multi-channel rules to avoid repeated
		// full filter scans for each channel using the same dynamic rule.
		if s.dynamicRulePagedMode && dynamicRuleUsage[ruleKey] <= 1 {
			if _, catalogSupportsIterator := s.catalog.(catalogFilterIterator); catalogSupportsIterator {
				if pagedSyncer, ok := s.channels.(catalogFilterDynamicSourceSyncer); ok {
					syncResult, matchedCount, err := pagedSyncer.SyncDynamicSourcesByCatalogFilter(
						ctx,
						channel.ChannelID,
						groupNames,
						ruleKey.SearchQuery,
						ruleKey.SearchRegex,
						dynamicCatalogFilterPageSize,
						s.dynamicRuleMatchLimit,
					)
					if err != nil {
						return err
					}
					if err := s.enforceDynamicRuleMatchLimit(channel, ruleKey, matchedCount); err != nil {
						return err
					}
					s.recordDynamicSyncResult(channel, result, groupNames, ruleKey, syncResult, matchedCount)
					return nil
				}
			}
		}

		itemKeys := []string(nil)
		if dynamicRuleUsage[ruleKey] > 1 {
			if cached, found := dynamicLookupCache[ruleKey]; found {
				itemKeys = cached
			}
		}
		if itemKeys == nil {
			lookupKeys, err := s.catalog.ListActiveItemKeysByCatalogFilter(ctx, groupNames, ruleKey.SearchQuery, ruleKey.SearchRegex)
			if err != nil {
				return err
			}
			itemKeys = lookupKeys
			if err := s.enforceDynamicRuleMatchLimit(channel, ruleKey, len(itemKeys)); err != nil {
				return err
			}
			if dynamicRuleUsage[ruleKey] > 1 {
				// Cache only when at least two channels share a rule to avoid retaining
				// large dynamic key lists for one-off rules.
				dynamicLookupCache[ruleKey] = itemKeys
			}
		}

		syncResult, err := s.channels.SyncDynamicSources(ctx, channel.ChannelID, itemKeys)
		if err != nil {
			return err
		}
		s.recordDynamicSyncResult(channel, result, groupNames, ruleKey, syncResult, len(itemKeys))
		return nil
	}

	return s.reconcileStaticChannel(ctx, channel, result)
}

func (s *Service) enforceDynamicRuleMatchLimit(channel channels.Channel, ruleKey dynamicCatalogFilterKey, matchedCount int) error {
	limit := s.dynamicRuleMatchLimit
	if limit <= 0 || matchedCount <= limit {
		return nil
	}
	s.loggerOrDefault().Warn(
		"dynamic rule match count exceeded configured limit",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"guide_name", channel.GuideName,
		"query", ruleKey.SearchQuery,
		"query_regex", ruleKey.SearchRegex,
		"matched_items", matchedCount,
		"match_limit", limit,
	)
	return fmt.Errorf("dynamic rule channel %d matched %d items; limit is %d", channel.ChannelID, matchedCount, limit)
}

func (s *Service) recordDynamicSyncResult(
	channel channels.Channel,
	result *Result,
	groupNames []string,
	ruleKey dynamicCatalogFilterKey,
	syncResult channels.DynamicSourceSyncResult,
	matchedCount int,
) {
	s.loggerOrDefault().Info(
		"dynamic reconcile synced",
		"channel_id", channel.ChannelID,
		"guide_number", channel.GuideNumber,
		"guide_name", channel.GuideName,
		"groups", groupNames,
		"query", ruleKey.SearchQuery,
		"query_regex", ruleKey.SearchRegex,
		"matched_items", matchedCount,
		"added_sources", syncResult.Added,
		"removed_sources", syncResult.Removed,
		"retained_sources", syncResult.Retained,
	)
	result.DynamicChannelsProcessed++
	result.DynamicSourcesAdded += syncResult.Added
	result.DynamicSourcesRemoved += syncResult.Removed
	result.SourcesAdded += syncResult.Added
	result.SourcesAlreadySeen += syncResult.Retained
}

func newDynamicCatalogFilterKey(groupNames []string, searchQuery string, searchRegex bool) dynamicCatalogFilterKey {
	return dynamicCatalogFilterKey{
		GroupNames:  strings.Join(groupNames, "\x1f"),
		SearchQuery: strings.TrimSpace(searchQuery),
		SearchRegex: searchRegex,
	}
}

func (s *Service) reconcileStaticChannel(ctx context.Context, channel channels.Channel, result *Result) error {
	itemKeys, err := s.catalog.ListActiveItemKeysByChannelKey(ctx, channel.ChannelKey)
	if err != nil {
		return err
	}

	existingSources, err := s.channels.ListSources(ctx, channel.ChannelID, false)
	if err != nil {
		return err
	}
	existing := make(map[string]struct{}, len(existingSources))
	for _, source := range existingSources {
		existing[source.ItemKey] = struct{}{}
	}

	for _, itemKey := range itemKeys {
		itemKey = strings.TrimSpace(itemKey)
		if itemKey == "" {
			continue
		}
		if _, already := existing[itemKey]; already {
			result.SourcesAlreadySeen++
			continue
		}

		if _, err := s.channels.AddSource(ctx, channel.ChannelID, itemKey, false); err != nil {
			if errors.Is(err, channels.ErrAssociationMismatch) {
				result.ChannelsSkipped++
				continue
			}
			return err
		}

		existing[itemKey] = struct{}{}
		result.SourcesAdded++
	}

	return nil
}

func isReconcilable(channel channels.Channel) bool {
	if channel.ChannelClass == channels.ChannelClassDynamicGenerated {
		return false
	}
	if channel.DynamicRule.Enabled {
		return strings.TrimSpace(channel.DynamicRule.SearchQuery) != ""
	}
	return strings.TrimSpace(channel.ChannelKey) != ""
}
