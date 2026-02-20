package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

// testBeforeProfileExistenceCheck is a test-only hook called after the
// monotonic-guard UPDATE returns rows == 0 but before the source existence
// check SELECT. Nil in production; set in tests to inject context
// cancellation or other failures between the two queries.
var testBeforeProfileExistenceCheck func()

const maxChannelIDsPerSourceBatch = 900
const dynamicSyncMatchedItemTempTable = "temp_dynamic_sync_matched_item_keys"

func (s *Store) CreateChannelFromItem(ctx context.Context, itemKey, guideName, channelKey string, dynamicRule *channels.DynamicSourceRule, startGuideNumber int) (channels.Channel, error) {
	itemKey = strings.TrimSpace(itemKey)
	if dynamicRule == nil {
		dynamicRule = &channels.DynamicSourceRule{}
	}
	dynamicRule.SearchQuery = strings.TrimSpace(dynamicRule.SearchQuery)
	if dynamicRule.SearchRegex {
		if err := validateCatalogSearchRegexQuery(dynamicRule.SearchQuery); err != nil {
			return channels.Channel{}, err
		}
	}
	hasSeedItem := itemKey != ""
	if !hasSeedItem && !dynamicRule.Enabled {
		return channels.Channel{}, fmt.Errorf("item_key is required unless dynamic_rule.enabled is true")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return channels.Channel{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	item := playlistItemInfo{}
	if hasSeedItem {
		item, err = lookupItemInfoTx(ctx, tx, itemKey)
		if err != nil {
			if err == sql.ErrNoRows {
				return channels.Channel{}, channels.ErrItemNotFound
			}
			return channels.Channel{}, fmt.Errorf("lookup playlist item: %w", err)
		}
	}

	dynamicGroupNames := channels.NormalizeGroupNames(dynamicRule.GroupName, dynamicRule.GroupNames)
	dynamicGroupName := channels.GroupNameAlias(dynamicGroupNames)
	dynamicGroupNamesJSON := marshalGroupNamesJSON(dynamicGroupNames)

	guideName = strings.TrimSpace(guideName)
	if guideName == "" {
		if item.GuideName != "" {
			guideName = item.GuideName
		} else if dynamicRule.Enabled {
			guideName = strings.TrimSpace(dynamicRule.SearchQuery)
		} else if hasSeedItem {
			guideName = itemKey
		}
	}
	if guideName == "" {
		guideName = "Dynamic Channel"
	}

	channelKey = strings.TrimSpace(channelKey)
	if channelKey == "" && hasSeedItem {
		channelKey = strings.TrimSpace(item.ChannelKey)
	}
	channelKey = normalizeChannelKey(channelKey)

	var nextOrder int
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(order_index) + 1, 0)
		FROM published_channels
		WHERE channel_class = ?
	`, channels.ChannelClassTraditional).Scan(&nextOrder); err != nil {
		return channels.Channel{}, fmt.Errorf("query next channel order: %w", err)
	}

	now := time.Now().Unix()
	guideNumber := strconv.Itoa(startGuideNumber + nextOrder)
	result, err := tx.ExecContext(ctx, `
		INSERT INTO published_channels (
			channel_class,
			channel_key,
			guide_number,
			guide_name,
			order_index,
			enabled,
			dynamic_query_id,
			dynamic_item_key,
			dynamic_sources_enabled,
			dynamic_group_name,
			dynamic_group_names_json,
			dynamic_search_query,
			dynamic_search_regex,
			created_at,
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, 1, NULL, '', ?, ?, ?, ?, ?, ?, ?)
	`, channels.ChannelClassTraditional, channelKey, guideNumber, guideName, nextOrder, boolToInt(dynamicRule.Enabled), dynamicGroupName, dynamicGroupNamesJSON, dynamicRule.SearchQuery, boolToInt(dynamicRule.SearchRegex), now, now)
	if err != nil {
		return channels.Channel{}, fmt.Errorf("insert published channel: %w", err)
	}

	channelID, err := result.LastInsertId()
	if err != nil {
		return channels.Channel{}, fmt.Errorf("read inserted channel id: %w", err)
	}

	if hasSeedItem {
		associationType := "manual"
		if dynamicRule.Enabled {
			associationType = "dynamic_query"
		} else if channelKey != "" && channelKeysEqual(channelKey, item.ChannelKey) {
			associationType = "channel_key"
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO channel_sources (
				channel_id,
				item_key,
				priority_index,
				enabled,
				association_type,
				created_at,
				updated_at
			)
			VALUES (?, ?, 0, 1, ?, ?, ?)
		`, channelID, itemKey, associationType, now, now); err != nil {
			return channels.Channel{}, fmt.Errorf("insert channel source: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return channels.Channel{}, fmt.Errorf("commit create channel: %w", err)
	}

	sourceTotal := 0
	sourceEnabled := 0
	sourceDynamic := 0
	sourceManual := 0
	if hasSeedItem {
		sourceTotal = 1
		sourceEnabled = 1
		if dynamicRule.Enabled {
			sourceDynamic = 1
		} else {
			sourceManual = 1
		}
	}

	return channels.Channel{
		ChannelID:    channelID,
		ChannelClass: channels.ChannelClassTraditional,
		ChannelKey:   channelKey,
		GuideNumber:  guideNumber,
		GuideName:    guideName,
		OrderIndex:   nextOrder,
		Enabled:      true,
		DynamicRule: channels.DynamicSourceRule{
			Enabled:     dynamicRule.Enabled,
			GroupName:   dynamicGroupName,
			GroupNames:  dynamicGroupNames,
			SearchQuery: dynamicRule.SearchQuery,
			SearchRegex: dynamicRule.SearchRegex,
		},
		SourceTotal:   sourceTotal,
		SourceEnabled: sourceEnabled,
		SourceDynamic: sourceDynamic,
		SourceManual:  sourceManual,
	}, nil
}

func (s *Store) DeleteChannel(ctx context.Context, channelID int64, startGuideNumber int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	channel, err := getChannelByIDTx(ctx, tx, channelID)
	if err != nil {
		if err == sql.ErrNoRows {
			return channels.ErrChannelNotFound
		}
		return fmt.Errorf("lookup channel: %w", err)
	}
	if channel.ChannelClass != channels.ChannelClassTraditional {
		return fmt.Errorf("only traditional channels can be deleted")
	}

	result, err := tx.ExecContext(ctx, `DELETE FROM published_channels WHERE channel_id = ?`, channelID)
	if err != nil {
		return fmt.Errorf("delete channel: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return channels.ErrChannelNotFound
	}

	if err := renumberChannelsTx(ctx, tx, startGuideNumber); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete channel: %w", err)
	}
	return nil
}

func (s *Store) ListChannels(ctx context.Context, enabledOnly bool) ([]channels.Channel, error) {
	query := `
		SELECT
			pc.channel_id,
			COALESCE(pc.channel_class, ''),
			COALESCE(pc.channel_key, ''),
			pc.guide_number,
			pc.guide_name,
			pc.order_index,
			pc.enabled,
			COALESCE(pc.dynamic_query_id, 0),
			COALESCE(pc.dynamic_item_key, ''),
			COALESCE(pc.dynamic_sources_enabled, 0),
			COALESCE(pc.dynamic_group_name, ''),
			COALESCE(pc.dynamic_group_names_json, '[]'),
			COALESCE(pc.dynamic_search_query, ''),
			COALESCE(pc.dynamic_search_regex, 0),
			COALESCE(source_summary.source_total, 0),
			COALESCE(source_summary.source_enabled, 0),
			COALESCE(source_summary.source_dynamic, 0),
			COALESCE(source_summary.source_manual, 0)
		FROM published_channels pc
		LEFT JOIN (
			SELECT
				channel_id,
				COUNT(*) AS source_total,
				SUM(CASE WHEN enabled <> 0 THEN 1 ELSE 0 END) AS source_enabled,
				SUM(CASE WHEN association_type = 'dynamic_query' THEN 1 ELSE 0 END) AS source_dynamic,
				SUM(CASE WHEN association_type <> 'dynamic_query' THEN 1 ELSE 0 END) AS source_manual
			FROM channel_sources
			GROUP BY channel_id
		) AS source_summary ON source_summary.channel_id = pc.channel_id
		WHERE pc.channel_class = ?
	`
	args := []any{channels.ChannelClassTraditional}
	if enabledOnly {
		query += `AND pc.enabled = 1 `
	}
	query += `ORDER BY pc.order_index ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query channels: %w", err)
	}
	defer rows.Close()

	out := make([]channels.Channel, 0)
	for rows.Next() {
		var (
			channel               channels.Channel
			enabledInt            int
			dynamicEnabledInt     int
			dynamicSearchRegexInt int
			dynamicGroupNamesJSON string
		)
		if err := rows.Scan(
			&channel.ChannelID,
			&channel.ChannelClass,
			&channel.ChannelKey,
			&channel.GuideNumber,
			&channel.GuideName,
			&channel.OrderIndex,
			&enabledInt,
			&channel.DynamicQueryID,
			&channel.DynamicItemKey,
			&dynamicEnabledInt,
			&channel.DynamicRule.GroupName,
			&dynamicGroupNamesJSON,
			&channel.DynamicRule.SearchQuery,
			&dynamicSearchRegexInt,
			&channel.SourceTotal,
			&channel.SourceEnabled,
			&channel.SourceDynamic,
			&channel.SourceManual,
		); err != nil {
			return nil, fmt.Errorf("scan channel: %w", err)
		}
		channel.Enabled = enabledInt != 0
		channel.DynamicRule.Enabled = dynamicEnabledInt != 0
		channel.DynamicRule.SearchRegex = dynamicSearchRegexInt != 0
		channel.DynamicRule.GroupName, channel.DynamicRule.GroupNames = normalizeStoredGroupNames(channel.DynamicRule.GroupName, dynamicGroupNamesJSON)
		out = append(out, channel)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channels: %w", err)
	}
	return out, nil
}

func (s *Store) ListChannelsPaged(ctx context.Context, enabledOnly bool, limit, offset int) ([]channels.Channel, int, error) {
	totalQuery := `SELECT COUNT(*) FROM published_channels WHERE channel_class = ?`
	totalArgs := []any{channels.ChannelClassTraditional}
	if enabledOnly {
		totalQuery += ` AND enabled = 1`
	}

	var total int
	if err := s.db.QueryRowContext(ctx, totalQuery, totalArgs...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count channels: %w", err)
	}
	if total == 0 || offset >= total {
		return []channels.Channel{}, total, nil
	}

	query := `
		SELECT
			pc.channel_id,
			COALESCE(pc.channel_class, ''),
			COALESCE(pc.channel_key, ''),
			pc.guide_number,
			pc.guide_name,
			pc.order_index,
			pc.enabled,
			COALESCE(pc.dynamic_query_id, 0),
			COALESCE(pc.dynamic_item_key, ''),
			COALESCE(pc.dynamic_sources_enabled, 0),
			COALESCE(pc.dynamic_group_name, ''),
			COALESCE(pc.dynamic_group_names_json, '[]'),
			COALESCE(pc.dynamic_search_query, ''),
			COALESCE(pc.dynamic_search_regex, 0),
			COALESCE(source_summary.source_total, 0),
			COALESCE(source_summary.source_enabled, 0),
			COALESCE(source_summary.source_dynamic, 0),
			COALESCE(source_summary.source_manual, 0)
		FROM published_channels pc
		LEFT JOIN (
			SELECT
				channel_id,
				COUNT(*) AS source_total,
				SUM(CASE WHEN enabled <> 0 THEN 1 ELSE 0 END) AS source_enabled,
				SUM(CASE WHEN association_type = 'dynamic_query' THEN 1 ELSE 0 END) AS source_dynamic,
				SUM(CASE WHEN association_type <> 'dynamic_query' THEN 1 ELSE 0 END) AS source_manual
			FROM channel_sources
			GROUP BY channel_id
		) AS source_summary ON source_summary.channel_id = pc.channel_id
		WHERE pc.channel_class = ?
	`
	args := []any{channels.ChannelClassTraditional}
	if enabledOnly {
		query += `AND pc.enabled = 1 `
	}
	query += `ORDER BY pc.order_index ASC`

	query, args = applyLimitOffset(query, args, limit, offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query channels page: %w", err)
	}
	defer rows.Close()

	out := make([]channels.Channel, expectedPageCapacity(total, limit, offset))
	rowCount := 0
	var (
		enabledInt            int
		dynamicEnabledInt     int
		dynamicSearchRegexInt int
		dynamicGroupNamesJSON string
	)
	for rows.Next() {
		if rowCount >= len(out) {
			out = append(out, channels.Channel{})
		}
		channel := &out[rowCount]
		if err := rows.Scan(
			&channel.ChannelID,
			&channel.ChannelClass,
			&channel.ChannelKey,
			&channel.GuideNumber,
			&channel.GuideName,
			&channel.OrderIndex,
			&enabledInt,
			&channel.DynamicQueryID,
			&channel.DynamicItemKey,
			&dynamicEnabledInt,
			&channel.DynamicRule.GroupName,
			&dynamicGroupNamesJSON,
			&channel.DynamicRule.SearchQuery,
			&dynamicSearchRegexInt,
			&channel.SourceTotal,
			&channel.SourceEnabled,
			&channel.SourceDynamic,
			&channel.SourceManual,
		); err != nil {
			return nil, 0, fmt.Errorf("scan channel page row: %w", err)
		}
		channel.Enabled = enabledInt != 0
		channel.DynamicRule.Enabled = dynamicEnabledInt != 0
		channel.DynamicRule.SearchRegex = dynamicSearchRegexInt != 0
		channel.DynamicRule.GroupName, channel.DynamicRule.GroupNames = normalizeStoredGroupNames(channel.DynamicRule.GroupName, dynamicGroupNamesJSON)
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate channels page: %w", err)
	}
	return out[:rowCount], total, nil
}

func (s *Store) ListLineupChannels(ctx context.Context) ([]channels.Channel, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT
			channel_id,
			COALESCE(channel_class, ''),
			COALESCE(channel_key, ''),
			guide_number,
			guide_name,
			order_index,
			enabled,
			COALESCE(dynamic_query_id, 0),
			COALESCE(dynamic_item_key, ''),
			COALESCE(dynamic_sources_enabled, 0),
			COALESCE(dynamic_group_name, ''),
			COALESCE(dynamic_group_names_json, '[]'),
			COALESCE(dynamic_search_query, ''),
			COALESCE(dynamic_search_regex, 0)
		FROM published_channels
		WHERE enabled = 1
		ORDER BY order_index ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("query lineup channels: %w", err)
	}
	defer rows.Close()

	out := make([]channels.Channel, 0)
	for rows.Next() {
		var (
			channel               channels.Channel
			enabledInt            int
			dynamicEnabledInt     int
			dynamicSearchRegexInt int
			dynamicGroupNamesJSON string
		)
		if err := rows.Scan(
			&channel.ChannelID,
			&channel.ChannelClass,
			&channel.ChannelKey,
			&channel.GuideNumber,
			&channel.GuideName,
			&channel.OrderIndex,
			&enabledInt,
			&channel.DynamicQueryID,
			&channel.DynamicItemKey,
			&dynamicEnabledInt,
			&channel.DynamicRule.GroupName,
			&dynamicGroupNamesJSON,
			&channel.DynamicRule.SearchQuery,
			&dynamicSearchRegexInt,
		); err != nil {
			return nil, fmt.Errorf("scan lineup channel: %w", err)
		}
		channel.Enabled = enabledInt != 0
		channel.DynamicRule.Enabled = dynamicEnabledInt != 0
		channel.DynamicRule.SearchRegex = dynamicSearchRegexInt != 0
		channel.DynamicRule.GroupName, channel.DynamicRule.GroupNames = normalizeStoredGroupNames(channel.DynamicRule.GroupName, dynamicGroupNamesJSON)
		out = append(out, channel)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate lineup channels: %w", err)
	}
	return out, nil
}

func (s *Store) ReorderChannels(ctx context.Context, channelIDs []int64, startGuideNumber int) error {
	seen := make(map[int64]struct{}, len(channelIDs))
	for _, id := range channelIDs {
		if id <= 0 {
			return fmt.Errorf("channel_ids must contain positive ids")
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("channel_ids contains duplicate id %d", id)
		}
		seen[id] = struct{}{}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	currentIDs, err := listChannelIDsTx(ctx, tx)
	if err != nil {
		return err
	}
	if len(currentIDs) != len(channelIDs) {
		return fmt.Errorf("channel_ids count mismatch: got %d, want %d", len(channelIDs), len(currentIDs))
	}
	for _, id := range currentIDs {
		if _, ok := seen[id]; !ok {
			return fmt.Errorf("channel_ids missing id %d", id)
		}
	}

	now := time.Now().Unix()
	if err := applyChannelOrderTx(ctx, tx, channelIDs, startGuideNumber, now); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reorder channels: %w", err)
	}
	return nil
}

func (s *Store) UpdateChannel(ctx context.Context, channelID int64, guideName *string, enabled *bool, dynamicRule *channels.DynamicSourceRule) (channels.Channel, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return channels.Channel{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	channel, err := getChannelByIDTx(ctx, tx, channelID)
	if err != nil {
		if err == sql.ErrNoRows {
			return channels.Channel{}, channels.ErrChannelNotFound
		}
		return channels.Channel{}, fmt.Errorf("lookup channel: %w", err)
	}
	if channel.ChannelClass != channels.ChannelClassTraditional {
		return channels.Channel{}, fmt.Errorf("only traditional channels can be updated")
	}

	if guideName != nil {
		channel.GuideName = strings.TrimSpace(*guideName)
	}
	if enabled != nil {
		channel.Enabled = *enabled
	}
	if dynamicRule != nil {
		channel.DynamicRule = *dynamicRule
	}
	channel.DynamicRule.GroupNames = channels.NormalizeGroupNames(channel.DynamicRule.GroupName, channel.DynamicRule.GroupNames)
	channel.DynamicRule.GroupName = channels.GroupNameAlias(channel.DynamicRule.GroupNames)
	channel.DynamicRule.SearchQuery = strings.TrimSpace(channel.DynamicRule.SearchQuery)
	if channel.DynamicRule.SearchRegex {
		if err := validateCatalogSearchRegexQuery(channel.DynamicRule.SearchQuery); err != nil {
			return channels.Channel{}, err
		}
	}
	dynamicGroupNamesJSON := marshalGroupNamesJSON(channel.DynamicRule.GroupNames)

	if guideName != nil || enabled != nil || dynamicRule != nil {
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE published_channels
			 SET guide_name = ?, enabled = ?, dynamic_sources_enabled = ?, dynamic_group_name = ?, dynamic_group_names_json = ?, dynamic_search_query = ?, dynamic_search_regex = ?, updated_at = ?
			 WHERE channel_id = ?`,
			channel.GuideName,
			boolToInt(channel.Enabled),
			boolToInt(channel.DynamicRule.Enabled),
			channel.DynamicRule.GroupName,
			dynamicGroupNamesJSON,
			channel.DynamicRule.SearchQuery,
			boolToInt(channel.DynamicRule.SearchRegex),
			time.Now().Unix(),
			channelID,
		); err != nil {
			return channels.Channel{}, fmt.Errorf("update channel: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return channels.Channel{}, fmt.Errorf("commit update channel: %w", err)
	}
	return channel, nil
}

func (s *Store) GetChannelByGuideNumber(ctx context.Context, guideNumber string) (channels.Channel, error) {
	var (
		channel               channels.Channel
		enabledInt            int
		dynamicEnabledInt     int
		dynamicSearchRegexInt int
		dynamicGroupNamesJSON string
	)
	err := s.db.QueryRowContext(
		ctx,
		`SELECT
			channel_id,
			COALESCE(channel_class, ''),
			COALESCE(channel_key, ''),
			guide_number,
			guide_name,
			order_index,
			enabled,
			COALESCE(dynamic_query_id, 0),
			COALESCE(dynamic_item_key, ''),
			COALESCE(dynamic_sources_enabled, 0),
			COALESCE(dynamic_group_name, ''),
			COALESCE(dynamic_group_names_json, '[]'),
			COALESCE(dynamic_search_query, ''),
			COALESCE(dynamic_search_regex, 0),
			COALESCE((
				SELECT COUNT(*)
				FROM channel_sources cs
				WHERE cs.channel_id = published_channels.channel_id
			), 0),
			COALESCE((
				SELECT COUNT(*)
				FROM channel_sources cs
				WHERE cs.channel_id = published_channels.channel_id
				  AND cs.enabled <> 0
			), 0),
			COALESCE((
				SELECT COUNT(*)
				FROM channel_sources cs
				WHERE cs.channel_id = published_channels.channel_id
				  AND cs.association_type = 'dynamic_query'
			), 0),
			COALESCE((
				SELECT COUNT(*)
				FROM channel_sources cs
				WHERE cs.channel_id = published_channels.channel_id
				  AND cs.association_type <> 'dynamic_query'
			), 0)
		 FROM published_channels
		 WHERE guide_number = ?`,
		strings.TrimSpace(guideNumber),
	).Scan(
		&channel.ChannelID,
		&channel.ChannelClass,
		&channel.ChannelKey,
		&channel.GuideNumber,
		&channel.GuideName,
		&channel.OrderIndex,
		&enabledInt,
		&channel.DynamicQueryID,
		&channel.DynamicItemKey,
		&dynamicEnabledInt,
		&channel.DynamicRule.GroupName,
		&dynamicGroupNamesJSON,
		&channel.DynamicRule.SearchQuery,
		&dynamicSearchRegexInt,
		&channel.SourceTotal,
		&channel.SourceEnabled,
		&channel.SourceDynamic,
		&channel.SourceManual,
	)
	if err == sql.ErrNoRows {
		return channels.Channel{}, channels.ErrChannelNotFound
	}
	if err != nil {
		return channels.Channel{}, fmt.Errorf("query channel by guide number: %w", err)
	}
	channel.Enabled = enabledInt != 0
	channel.DynamicRule.Enabled = dynamicEnabledInt != 0
	channel.DynamicRule.SearchRegex = dynamicSearchRegexInt != 0
	channel.DynamicRule.GroupName, channel.DynamicRule.GroupNames = normalizeStoredGroupNames(channel.DynamicRule.GroupName, dynamicGroupNamesJSON)
	return channel, nil
}

func (s *Store) GetSource(ctx context.Context, channelID, sourceID int64, enabledOnly bool) (channels.Source, error) {
	query := `
		SELECT
			cs.source_id,
			cs.channel_id,
			cs.item_key,
			COALESCE(p.stream_url, ''),
			cs.priority_index,
			cs.enabled,
			cs.association_type,
			COALESCE(cs.last_ok_at, 0),
			COALESCE(cs.last_fail_at, 0),
			COALESCE(cs.last_fail_reason, ''),
			cs.success_count,
			cs.fail_count,
			cs.cooldown_until,
			COALESCE(cs.last_probe_at, 0),
			COALESCE(cs.profile_width, 0),
			COALESCE(cs.profile_height, 0),
			COALESCE(cs.profile_fps, 0),
			COALESCE(cs.profile_video_codec, ''),
			COALESCE(cs.profile_audio_codec, ''),
			COALESCE(cs.profile_bitrate_bps, 0),
			COALESCE(p.tvg_name, '')
		FROM channel_sources cs
		LEFT JOIN playlist_items p ON p.item_key = cs.item_key
		WHERE cs.channel_id = ? AND cs.source_id = ?
	`
	if enabledOnly {
		query += `AND cs.enabled = 1 `
	}

	var (
		src        channels.Source
		enabledInt int
		tvgName    string
	)
	err := s.db.QueryRowContext(ctx, query, channelID, sourceID).Scan(
		&src.SourceID,
		&src.ChannelID,
		&src.ItemKey,
		&src.StreamURL,
		&src.PriorityIndex,
		&enabledInt,
		&src.AssociationType,
		&src.LastOKAt,
		&src.LastFailAt,
		&src.LastFailReason,
		&src.SuccessCount,
		&src.FailCount,
		&src.CooldownUntil,
		&src.LastProbeAt,
		&src.ProfileWidth,
		&src.ProfileHeight,
		&src.ProfileFPS,
		&src.ProfileVideoCodec,
		&src.ProfileAudioCodec,
		&src.ProfileBitrateBPS,
		&tvgName,
	)
	if err == sql.ErrNoRows {
		exists, existsErr := s.channelExists(ctx, channelID)
		if existsErr != nil {
			return channels.Source{}, existsErr
		}
		if !exists {
			return channels.Source{}, channels.ErrChannelNotFound
		}
		return channels.Source{}, channels.ErrSourceNotFound
	}
	if err != nil {
		return channels.Source{}, fmt.Errorf("query channel source: %w", err)
	}

	src.Enabled = enabledInt != 0
	src.TVGName = strings.TrimSpace(tvgName)
	return src, nil
}

func (s *Store) ListSources(ctx context.Context, channelID int64, enabledOnly bool) ([]channels.Source, error) {
	query := `
		SELECT
			cs.source_id,
			cs.channel_id,
			cs.item_key,
			COALESCE(p.stream_url, ''),
			cs.priority_index,
			cs.enabled,
			cs.association_type,
			COALESCE(cs.last_ok_at, 0),
			COALESCE(cs.last_fail_at, 0),
			COALESCE(cs.last_fail_reason, ''),
			cs.success_count,
			cs.fail_count,
			cs.cooldown_until,
			COALESCE(cs.last_probe_at, 0),
			COALESCE(cs.profile_width, 0),
			COALESCE(cs.profile_height, 0),
			COALESCE(cs.profile_fps, 0),
			COALESCE(cs.profile_video_codec, ''),
			COALESCE(cs.profile_audio_codec, ''),
			COALESCE(cs.profile_bitrate_bps, 0),
			COALESCE(p.tvg_name, '')
		FROM channel_sources cs
		LEFT JOIN playlist_items p ON p.item_key = cs.item_key
		WHERE cs.channel_id = ?
	`
	if enabledOnly {
		query += `AND cs.enabled = 1 `
	}
	query += `ORDER BY cs.priority_index ASC`

	rows, err := s.db.QueryContext(ctx, query, channelID)
	if err != nil {
		return nil, fmt.Errorf("query channel sources: %w", err)
	}
	defer rows.Close()

	out := make([]channels.Source, 0, 16)
	var (
		enabledInt int
		tvgName    string
	)
	for rows.Next() {
		out = append(out, channels.Source{})
		src := &out[len(out)-1]
		if err := rows.Scan(
			&src.SourceID,
			&src.ChannelID,
			&src.ItemKey,
			&src.StreamURL,
			&src.PriorityIndex,
			&enabledInt,
			&src.AssociationType,
			&src.LastOKAt,
			&src.LastFailAt,
			&src.LastFailReason,
			&src.SuccessCount,
			&src.FailCount,
			&src.CooldownUntil,
			&src.LastProbeAt,
			&src.ProfileWidth,
			&src.ProfileHeight,
			&src.ProfileFPS,
			&src.ProfileVideoCodec,
			&src.ProfileAudioCodec,
			&src.ProfileBitrateBPS,
			&tvgName,
		); err != nil {
			return nil, fmt.Errorf("scan channel source: %w", err)
		}
		src.Enabled = enabledInt != 0
		src.TVGName = strings.TrimSpace(tvgName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel sources: %w", err)
	}

	if len(out) == 0 {
		exists, err := s.channelExists(ctx, channelID)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, channels.ErrChannelNotFound
		}
	}

	return out, nil
}

func (s *Store) ListSourcesByChannelIDs(ctx context.Context, channelIDs []int64, enabledOnly bool) (map[int64][]channels.Source, error) {
	normalizedIDs, err := normalizeChannelIDList(channelIDs)
	if err != nil {
		return nil, err
	}
	out := make(map[int64][]channels.Source, len(normalizedIDs))
	if len(normalizedIDs) == 0 {
		return out, nil
	}

	existingByID, err := s.listExistingChannelIDs(ctx, normalizedIDs)
	if err != nil {
		return nil, err
	}
	for _, channelID := range normalizedIDs {
		if _, ok := existingByID[channelID]; !ok {
			return nil, channels.ErrChannelNotFound
		}
		// Preserve explicit empty-source channels in the output map.
		out[channelID] = []channels.Source{}
	}

	for start := 0; start < len(normalizedIDs); start += maxChannelIDsPerSourceBatch {
		end := start + maxChannelIDsPerSourceBatch
		if end > len(normalizedIDs) {
			end = len(normalizedIDs)
		}
		batchIDs := normalizedIDs[start:end]

		query, args := listSourcesByChannelIDsQuery(batchIDs, enabledOnly)
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("query channel sources batch: %w", err)
		}

		var (
			enabledInt int
			tvgName    string
		)
		for rows.Next() {
			var src channels.Source
			if err := rows.Scan(
				&src.ChannelID,
				&src.SourceID,
				&src.ItemKey,
				&src.StreamURL,
				&src.PriorityIndex,
				&enabledInt,
				&src.AssociationType,
				&src.LastOKAt,
				&src.LastFailAt,
				&src.LastFailReason,
				&src.SuccessCount,
				&src.FailCount,
				&src.CooldownUntil,
				&src.LastProbeAt,
				&src.ProfileWidth,
				&src.ProfileHeight,
				&src.ProfileFPS,
				&src.ProfileVideoCodec,
				&src.ProfileAudioCodec,
				&src.ProfileBitrateBPS,
				&tvgName,
			); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan channel source batch row: %w", err)
			}
			src.Enabled = enabledInt != 0
			src.TVGName = strings.TrimSpace(tvgName)
			out[src.ChannelID] = append(out[src.ChannelID], src)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate channel source batch rows: %w", err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close channel source batch rows: %w", err)
		}
	}

	return out, nil
}

func (s *Store) ListSourcesPaged(ctx context.Context, channelID int64, enabledOnly bool, limit, offset int) ([]channels.Source, int, error) {
	totalQuery := `
		SELECT
			COUNT(DISTINCT pc.channel_id),
			COUNT(cs.source_id)
		FROM published_channels pc
		LEFT JOIN channel_sources cs ON cs.channel_id = pc.channel_id
		WHERE pc.channel_id = ?
	`
	if enabledOnly {
		totalQuery = `
			SELECT
				COUNT(DISTINCT pc.channel_id),
				COUNT(cs.source_id)
			FROM published_channels pc
			LEFT JOIN channel_sources cs ON cs.channel_id = pc.channel_id AND cs.enabled = 1
			WHERE pc.channel_id = ?
		`
	}

	var (
		channelCount int
		total        int
	)
	if err := s.db.QueryRowContext(ctx, totalQuery, channelID).Scan(&channelCount, &total); err != nil {
		return nil, 0, fmt.Errorf("count channel sources: %w", err)
	}
	if channelCount == 0 {
		return nil, 0, channels.ErrChannelNotFound
	}
	if total == 0 || offset >= total {
		return []channels.Source{}, total, nil
	}

	query := `
		SELECT
			cs.source_id,
			cs.channel_id,
			cs.item_key,
			COALESCE(p.stream_url, ''),
			cs.priority_index,
			cs.enabled,
			cs.association_type,
			COALESCE(cs.last_ok_at, 0),
			COALESCE(cs.last_fail_at, 0),
			COALESCE(cs.last_fail_reason, ''),
			cs.success_count,
			cs.fail_count,
			cs.cooldown_until,
			COALESCE(cs.last_probe_at, 0),
			COALESCE(cs.profile_width, 0),
			COALESCE(cs.profile_height, 0),
			COALESCE(cs.profile_fps, 0),
			COALESCE(cs.profile_video_codec, ''),
			COALESCE(cs.profile_audio_codec, ''),
			COALESCE(cs.profile_bitrate_bps, 0),
			COALESCE(p.tvg_name, '')
		FROM channel_sources cs
		LEFT JOIN playlist_items p ON p.item_key = cs.item_key
		WHERE cs.channel_id = ?
	`
	args := []any{channelID}
	if enabledOnly {
		query += `AND cs.enabled = 1 `
	}
	query += `ORDER BY cs.priority_index ASC`

	query, args = applyLimitOffset(query, args, limit, offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query channel sources page: %w", err)
	}
	defer rows.Close()

	out := make([]channels.Source, expectedPageCapacity(total, limit, offset))
	rowCount := 0
	var (
		enabledInt int
		tvgName    string
	)
	for rows.Next() {
		if rowCount >= len(out) {
			out = append(out, channels.Source{})
		}
		src := &out[rowCount]
		if err := rows.Scan(
			&src.SourceID,
			&src.ChannelID,
			&src.ItemKey,
			&src.StreamURL,
			&src.PriorityIndex,
			&enabledInt,
			&src.AssociationType,
			&src.LastOKAt,
			&src.LastFailAt,
			&src.LastFailReason,
			&src.SuccessCount,
			&src.FailCount,
			&src.CooldownUntil,
			&src.LastProbeAt,
			&src.ProfileWidth,
			&src.ProfileHeight,
			&src.ProfileFPS,
			&src.ProfileVideoCodec,
			&src.ProfileAudioCodec,
			&src.ProfileBitrateBPS,
			&tvgName,
		); err != nil {
			return nil, 0, fmt.Errorf("scan channel source page row: %w", err)
		}
		src.Enabled = enabledInt != 0
		src.TVGName = strings.TrimSpace(tvgName)
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate channel sources page: %w", err)
	}

	return out[:rowCount], total, nil
}

func (s *Store) AddSource(ctx context.Context, channelID int64, itemKey string, allowCrossChannel bool) (channels.Source, error) {
	itemKey = strings.TrimSpace(itemKey)
	if itemKey == "" {
		return channels.Source{}, fmt.Errorf("item_key is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return channels.Source{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var channelKey string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(channel_key, '') FROM published_channels WHERE channel_id = ?`, channelID).Scan(&channelKey); err != nil {
		if err == sql.ErrNoRows {
			return channels.Source{}, channels.ErrChannelNotFound
		}
		return channels.Source{}, fmt.Errorf("lookup channel: %w", err)
	}
	channelKey = normalizeChannelKey(channelKey)

	item, err := lookupItemInfoTx(ctx, tx, itemKey)
	if err != nil {
		if err == sql.ErrNoRows {
			return channels.Source{}, channels.ErrItemNotFound
		}
		return channels.Source{}, fmt.Errorf("lookup playlist item: %w", err)
	}

	associationType := "manual"
	if channelKey != "" && item.ChannelKey != "" && channelKeysEqual(channelKey, item.ChannelKey) {
		associationType = "channel_key"
	} else if !allowCrossChannel {
		return channels.Source{}, channels.ErrAssociationMismatch
	}

	if existing, err := getChannelSourceByItemKeyTx(ctx, tx, channelID, itemKey); err == nil {
		if err := tx.Commit(); err != nil {
			return channels.Source{}, fmt.Errorf("commit existing source lookup: %w", err)
		}
		return existing, nil
	} else if err != sql.ErrNoRows {
		return channels.Source{}, fmt.Errorf("lookup existing source: %w", err)
	}

	var nextPriority int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(priority_index) + 1, 0) FROM channel_sources WHERE channel_id = ?`, channelID).Scan(&nextPriority); err != nil {
		return channels.Source{}, fmt.Errorf("query next source priority: %w", err)
	}

	now := time.Now().Unix()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO channel_sources (
			channel_id,
			item_key,
			priority_index,
			enabled,
			association_type,
			created_at,
			updated_at
		)
		VALUES (?, ?, ?, 1, ?, ?, ?)
	`, channelID, itemKey, nextPriority, associationType, now, now)
	if err != nil {
		return channels.Source{}, fmt.Errorf("insert channel source: %w", err)
	}

	sourceID, err := result.LastInsertId()
	if err != nil {
		return channels.Source{}, fmt.Errorf("read inserted source id: %w", err)
	}

	source, err := getSourceByIDTx(ctx, tx, channelID, sourceID)
	if err != nil {
		return channels.Source{}, fmt.Errorf("lookup inserted source: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return channels.Source{}, fmt.Errorf("commit add source: %w", err)
	}
	return source, nil
}

func (s *Store) DeleteSource(ctx context.Context, channelID, sourceID int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `DELETE FROM channel_sources WHERE channel_id = ? AND source_id = ?`, channelID, sourceID)
	if err != nil {
		return fmt.Errorf("delete channel source: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return channels.ErrSourceNotFound
	}

	ids, err := listSourceIDsTx(ctx, tx, channelID)
	if err != nil {
		return err
	}
	if err := applySourceOrderTx(ctx, tx, channelID, ids, time.Now().Unix()); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete source: %w", err)
	}
	return nil
}

func (s *Store) ReorderSources(ctx context.Context, channelID int64, sourceIDs []int64) error {
	seen := make(map[int64]struct{}, len(sourceIDs))
	for _, id := range sourceIDs {
		if id <= 0 {
			return fmt.Errorf("source_ids must contain positive ids")
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("source_ids contains duplicate id %d", id)
		}
		seen[id] = struct{}{}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	currentIDs, err := listSourceIDsTx(ctx, tx, channelID)
	if err != nil {
		return err
	}
	if len(currentIDs) != len(sourceIDs) {
		return fmt.Errorf("%w: source_ids count mismatch: got %d, want %d", channels.ErrSourceOrderDrift, len(sourceIDs), len(currentIDs))
	}
	for _, id := range currentIDs {
		if _, ok := seen[id]; !ok {
			return fmt.Errorf("%w: source_ids missing id %d", channels.ErrSourceOrderDrift, id)
		}
	}

	if err := applySourceOrderTx(ctx, tx, channelID, sourceIDs, time.Now().Unix()); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reorder sources: %w", err)
	}
	return nil
}

func (s *Store) UpdateSource(ctx context.Context, channelID, sourceID int64, enabled *bool) (channels.Source, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return channels.Source{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	source, err := getSourceByIDTx(ctx, tx, channelID, sourceID)
	if err != nil {
		if err == sql.ErrNoRows {
			return channels.Source{}, channels.ErrSourceNotFound
		}
		return channels.Source{}, fmt.Errorf("lookup source: %w", err)
	}

	if enabled != nil {
		source.Enabled = *enabled
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE channel_sources
			 SET enabled = ?, updated_at = ?
			 WHERE channel_id = ? AND source_id = ?`,
			boolToInt(source.Enabled),
			time.Now().Unix(),
			channelID,
			sourceID,
		); err != nil {
			return channels.Source{}, fmt.Errorf("update source: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return channels.Source{}, fmt.Errorf("commit update source: %w", err)
	}
	return source, nil
}

// SyncDynamicSourcesByCatalogFilter synchronizes one channel's dynamic sources by
// iterating catalog matches in bounded pages and staging matched keys in a temp table.
func (s *Store) SyncDynamicSourcesByCatalogFilter(
	ctx context.Context,
	channelID int64,
	groupNames []string,
	searchQuery string,
	searchRegex bool,
	pageSize int,
	maxMatches int,
) (channels.DynamicSourceSyncResult, int, error) {
	spec, err := buildCatalogFilterQuerySpec(groupNames, searchQuery, searchRegex)
	if err != nil {
		return channels.DynamicSourceSyncResult{}, 0, err
	}
	pageSize = normalizeCatalogFilterPageSize(pageSize)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return channels.DynamicSourceSyncResult{}, 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := validateDynamicSourcesChannelTx(ctx, tx, channelID); err != nil {
		return channels.DynamicSourceSyncResult{}, 0, err
	}
	if err := ensureDynamicSyncMatchedItemTempTableTx(ctx, tx); err != nil {
		return channels.DynamicSourceSyncResult{}, 0, err
	}
	if err := clearDynamicSyncMatchedItemTempTableTx(ctx, tx); err != nil {
		return channels.DynamicSourceSyncResult{}, 0, err
	}

	insertStmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO `+dynamicSyncMatchedItemTempTable+` (item_key) VALUES (?)`)
	if err != nil {
		return channels.DynamicSourceSyncResult{}, 0, fmt.Errorf("prepare temp matched item insert: %w", err)
	}
	defer insertStmt.Close()

	matchedCount := 0
	afterName := ""
	afterItemKey := ""
	for {
		page, err := listActiveItemKeyRowsByCatalogFilterPage(ctx, tx, spec, pageSize, afterName, afterItemKey)
		if err != nil {
			return channels.DynamicSourceSyncResult{}, 0, err
		}
		if len(page) == 0 {
			break
		}
		for _, row := range page {
			insertResult, err := insertStmt.ExecContext(ctx, row.ItemKey)
			if err != nil {
				return channels.DynamicSourceSyncResult{}, 0, fmt.Errorf("insert matched item %q into temp table: %w", row.ItemKey, err)
			}
			rowsAffected, err := insertResult.RowsAffected()
			if err != nil {
				return channels.DynamicSourceSyncResult{}, 0, fmt.Errorf("rows affected inserting matched item %q: %w", row.ItemKey, err)
			}
			if rowsAffected > 0 {
				matchedCount++
				if maxMatches > 0 && matchedCount > maxMatches {
					return channels.DynamicSourceSyncResult{}, 0, fmt.Errorf("dynamic rule channel %d matched %d items; limit is %d", channelID, matchedCount, maxMatches)
				}
			}
			afterName = row.Name
			afterItemKey = row.ItemKey
		}
		if len(page) < pageSize {
			break
		}
	}

	now := time.Now().Unix()
	result, err := syncDynamicSourcesFromMatchedItemTempTableTx(ctx, tx, channelID, now)
	if err != nil {
		return channels.DynamicSourceSyncResult{}, 0, err
	}
	if err := clearDynamicSyncMatchedItemTempTableTx(ctx, tx); err != nil {
		return channels.DynamicSourceSyncResult{}, 0, err
	}
	if err := tx.Commit(); err != nil {
		return channels.DynamicSourceSyncResult{}, 0, fmt.Errorf("commit catalog-filter dynamic source sync: %w", err)
	}

	return result, matchedCount, nil
}

func validateDynamicSourcesChannelTx(ctx context.Context, tx *sql.Tx, channelID int64) error {
	var channelClass string
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(channel_class, '')
		 FROM published_channels
		 WHERE channel_id = ?`,
		channelID,
	).Scan(&channelClass); err != nil {
		if err == sql.ErrNoRows {
			return channels.ErrChannelNotFound
		}
		return fmt.Errorf("lookup channel: %w", err)
	}
	if strings.TrimSpace(channelClass) == channels.ChannelClassDynamicGenerated {
		return fmt.Errorf("dynamic source sync is not supported for generated dynamic channels")
	}
	return nil
}

func ensureDynamicSyncMatchedItemTempTableTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(
		ctx,
		`CREATE TEMP TABLE IF NOT EXISTS `+dynamicSyncMatchedItemTempTable+` (
			item_key TEXT PRIMARY KEY
		)`,
	); err != nil {
		return fmt.Errorf("create dynamic sync temp table: %w", err)
	}
	return nil
}

func clearDynamicSyncMatchedItemTempTableTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+dynamicSyncMatchedItemTempTable); err != nil {
		return fmt.Errorf("clear dynamic sync temp table: %w", err)
	}
	return nil
}

func syncDynamicSourcesFromMatchedItemTempTableTx(
	ctx context.Context,
	tx *sql.Tx,
	channelID int64,
	now int64,
) (channels.DynamicSourceSyncResult, error) {
	result := channels.DynamicSourceSyncResult{}

	deleteResult, err := tx.ExecContext(
		ctx,
		`DELETE FROM channel_sources
		 WHERE channel_id = ?
		   AND association_type = 'dynamic_query'
		   AND item_key NOT IN (SELECT item_key FROM `+dynamicSyncMatchedItemTempTable+`)`,
		channelID,
	)
	if err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("delete stale dynamic sources: %w", err)
	}
	removedRows, err := deleteResult.RowsAffected()
	if err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("rows affected deleting stale dynamic sources: %w", err)
	}
	if removedRows > 0 {
		result.Removed = int(removedRows)
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE channel_sources
		 SET association_type = 'dynamic_query',
		     updated_at = ?
		 WHERE channel_id = ?
		   AND association_type = 'channel_key'
		   AND item_key IN (SELECT item_key FROM `+dynamicSyncMatchedItemTempTable+`)`,
		now,
		channelID,
	); err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("promote channel_key sources for dynamic matches: %w", err)
	}

	type sourceRow struct {
		SourceID        int64
		ItemKey         string
		AssociationType string
	}

	existingRows, err := tx.QueryContext(
		ctx,
		`SELECT source_id, item_key, COALESCE(association_type, 'manual')
		 FROM channel_sources
		 WHERE channel_id = ?
		 ORDER BY priority_index ASC`,
		channelID,
	)
	if err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("query channel sources: %w", err)
	}
	defer existingRows.Close()

	existingByItem := make(map[string]sourceRow)
	for existingRows.Next() {
		var row sourceRow
		if err := existingRows.Scan(&row.SourceID, &row.ItemKey, &row.AssociationType); err != nil {
			return channels.DynamicSourceSyncResult{}, fmt.Errorf("scan channel source row: %w", err)
		}
		row.ItemKey = strings.TrimSpace(row.ItemKey)
		if row.ItemKey == "" {
			continue
		}
		existingByItem[row.ItemKey] = row
	}
	if err := existingRows.Err(); err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("iterate channel source rows: %w", err)
	}

	var nextPriority int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(MAX(priority_index) + 1, 0)
		 FROM channel_sources
		 WHERE channel_id = ?`,
		channelID,
	).Scan(&nextPriority); err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("query next source priority: %w", err)
	}

	matchedRows, err := tx.QueryContext(
		ctx,
		`SELECT p.item_key
		 FROM `+dynamicSyncMatchedItemTempTable+` m
		 JOIN playlist_items p ON p.item_key = m.item_key
		 ORDER BY p.name ASC, p.item_key ASC`,
	)
	if err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("query ordered matched item keys: %w", err)
	}
	defer matchedRows.Close()

	for matchedRows.Next() {
		var itemKey string
		if err := matchedRows.Scan(&itemKey); err != nil {
			return channels.DynamicSourceSyncResult{}, fmt.Errorf("scan matched item key row: %w", err)
		}
		itemKey = strings.TrimSpace(itemKey)
		if itemKey == "" {
			continue
		}

		if _, ok := existingByItem[itemKey]; ok {
			result.Retained++
			continue
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO channel_sources (
				channel_id,
				item_key,
				priority_index,
				enabled,
				association_type,
				created_at,
				updated_at
			)
			VALUES (?, ?, ?, 1, 'dynamic_query', ?, ?)`,
			channelID,
			itemKey,
			nextPriority,
			now,
			now,
		); err != nil {
			return channels.DynamicSourceSyncResult{}, fmt.Errorf("insert dynamic source for item %q: %w", itemKey, err)
		}
		existingByItem[itemKey] = sourceRow{
			ItemKey:         itemKey,
			AssociationType: "dynamic_query",
		}
		nextPriority++
		result.Added++
	}
	if err := matchedRows.Err(); err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("iterate matched item key rows: %w", err)
	}

	if result.Removed > 0 {
		ids, err := listSourceIDsTx(ctx, tx, channelID)
		if err != nil {
			return channels.DynamicSourceSyncResult{}, err
		}
		if err := applySourceOrderTx(ctx, tx, channelID, ids, now); err != nil {
			return channels.DynamicSourceSyncResult{}, err
		}
	}

	return result, nil
}

func (s *Store) SyncDynamicSources(ctx context.Context, channelID int64, matchedItemKeys []string) (channels.DynamicSourceSyncResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := validateDynamicSourcesChannelTx(ctx, tx, channelID); err != nil {
		return channels.DynamicSourceSyncResult{}, err
	}

	orderedMatches := make([]string, 0, len(matchedItemKeys))
	matchedSet := make(map[string]struct{}, len(matchedItemKeys))
	for _, itemKey := range matchedItemKeys {
		itemKey = strings.TrimSpace(itemKey)
		if itemKey == "" {
			continue
		}
		if _, ok := matchedSet[itemKey]; ok {
			continue
		}
		matchedSet[itemKey] = struct{}{}
		orderedMatches = append(orderedMatches, itemKey)
	}

	type sourceRow struct {
		SourceID        int64
		ItemKey         string
		AssociationType string
	}
	rows, err := tx.QueryContext(
		ctx,
		`SELECT source_id, item_key, COALESCE(association_type, 'manual')
		 FROM channel_sources
		 WHERE channel_id = ?
		 ORDER BY priority_index ASC`,
		channelID,
	)
	if err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("query channel sources: %w", err)
	}
	defer rows.Close()

	result := channels.DynamicSourceSyncResult{}
	existingByItem := make(map[string]sourceRow)
	dynamicByItem := make(map[string]sourceRow)
	for rows.Next() {
		var row sourceRow
		if err := rows.Scan(&row.SourceID, &row.ItemKey, &row.AssociationType); err != nil {
			return channels.DynamicSourceSyncResult{}, fmt.Errorf("scan channel source row: %w", err)
		}
		existingByItem[row.ItemKey] = row
		if row.AssociationType == "dynamic_query" {
			dynamicByItem[row.ItemKey] = row
		}
	}
	if err := rows.Err(); err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("iterate channel source rows: %w", err)
	}

	now := time.Now().Unix()
	for itemKey, row := range dynamicByItem {
		if _, ok := matchedSet[itemKey]; ok {
			continue
		}
		delete(existingByItem, itemKey)
		if _, err := tx.ExecContext(
			ctx,
			`DELETE FROM channel_sources
			 WHERE channel_id = ? AND source_id = ? AND association_type = 'dynamic_query'`,
			channelID,
			row.SourceID,
		); err != nil {
			return channels.DynamicSourceSyncResult{}, fmt.Errorf("delete dynamic source %d: %w", row.SourceID, err)
		}
		result.Removed++
	}

	var nextPriority int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(priority_index) + 1, 0) FROM channel_sources WHERE channel_id = ?`, channelID).Scan(&nextPriority); err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("query next source priority: %w", err)
	}

	for _, itemKey := range orderedMatches {
		if existingRow, ok := existingByItem[itemKey]; ok {
			if existingRow.AssociationType == "channel_key" {
				if _, err := tx.ExecContext(
					ctx,
					`UPDATE channel_sources
					 SET association_type = 'dynamic_query', updated_at = ?
					 WHERE channel_id = ? AND source_id = ? AND association_type = 'channel_key'`,
					now,
					channelID,
					existingRow.SourceID,
				); err != nil {
					return channels.DynamicSourceSyncResult{}, fmt.Errorf("promote matched source %d to dynamic_query: %w", existingRow.SourceID, err)
				}
				existingRow.AssociationType = "dynamic_query"
				existingByItem[itemKey] = existingRow
			}
			result.Retained++
			continue
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO channel_sources (
				channel_id,
				item_key,
				priority_index,
				enabled,
				association_type,
				created_at,
				updated_at
			)
			VALUES (?, ?, ?, 1, 'dynamic_query', ?, ?)`,
			channelID,
			itemKey,
			nextPriority,
			now,
			now,
		); err != nil {
			return channels.DynamicSourceSyncResult{}, fmt.Errorf("insert dynamic source for item %q: %w", itemKey, err)
		}

		existingByItem[itemKey] = sourceRow{
			ItemKey:         itemKey,
			AssociationType: "dynamic_query",
		}
		nextPriority++
		result.Added++
	}

	if result.Removed > 0 {
		ids, err := listSourceIDsTx(ctx, tx, channelID)
		if err != nil {
			return channels.DynamicSourceSyncResult{}, err
		}
		if err := applySourceOrderTx(ctx, tx, channelID, ids, now); err != nil {
			return channels.DynamicSourceSyncResult{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return channels.DynamicSourceSyncResult{}, fmt.Errorf("commit sync dynamic sources: %w", err)
	}
	return result, nil
}

func (s *Store) MarkSourceFailure(ctx context.Context, sourceID int64, reason string, failedAt time.Time) error {
	beginTrace := s.beginSQLiteIOERRTrace("channel_source_health_write", "mark_source_failure_begin_tx")
	tx, err := s.db.BeginTx(ctx, nil)
	s.endSQLiteIOERRTrace(beginTrace, err)
	if err != nil {
		return fmt.Errorf(
			"begin tx: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "mark_source_failure_begin_tx",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	defer tx.Rollback()

	var failCount int
	var lastOKAt int64
	lookupTrace := s.beginSQLiteIOERRTrace("channel_source_health_write", "mark_source_failure_lookup")
	if err := tx.QueryRowContext(ctx, `SELECT fail_count, COALESCE(last_ok_at, 0) FROM channel_sources WHERE source_id = ?`, sourceID).Scan(&failCount, &lastOKAt); err != nil {
		s.endSQLiteIOERRTrace(lookupTrace, err)
		if err == sql.ErrNoRows {
			return channels.ErrSourceNotFound
		}
		return fmt.Errorf(
			"lookup source fail count: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "mark_source_failure_lookup",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	s.endSQLiteIOERRTrace(lookupTrace, nil)

	failedUnix := failedAt.UTC().Unix()

	// Ordering guard: skip stale failure if a newer success is already persisted.
	if failedUnix > 0 && lastOKAt > 0 && failedUnix < lastOKAt {
		tx.Commit()
		return nil
	}

	failCount++
	cooldownUntil := failedUnix + int64(sourceBackoffDuration(failCount).Seconds())
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "startup failed"
	}

	updateTrace := s.beginSQLiteIOERRTrace("channel_source_health_write", "mark_source_failure")
	updateResult, err := tx.ExecContext(
		ctx,
		`UPDATE channel_sources
		 SET fail_count = ?, last_fail_at = ?, last_fail_reason = ?, cooldown_until = ?, updated_at = ?
		 WHERE source_id = ?`,
		failCount,
		failedUnix,
		reason,
		cooldownUntil,
		failedUnix,
		sourceID,
	)
	if err != nil {
		s.endSQLiteIOERRTrace(updateTrace, err)
		return fmt.Errorf(
			"update source failure state: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "mark_source_failure",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	s.endSQLiteIOERRTrace(updateTrace, nil)
	if affected, err := updateResult.RowsAffected(); err != nil {
		return fmt.Errorf("rows affected: %w", err)
	} else if affected == 0 {
		return channels.ErrSourceNotFound
	}

	commitTrace := s.beginSQLiteIOERRTrace("channel_source_health_write", "mark_source_failure_commit")
	if err := tx.Commit(); err != nil {
		s.endSQLiteIOERRTrace(commitTrace, err)
		return fmt.Errorf(
			"commit source failure state: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "mark_source_failure_commit",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	s.endSQLiteIOERRTrace(commitTrace, nil)
	return nil
}

func (s *Store) MarkSourceSuccess(ctx context.Context, sourceID int64, succeededAt time.Time) error {
	beginTrace := s.beginSQLiteIOERRTrace("channel_source_health_write", "mark_source_success_begin_tx")
	tx, err := s.db.BeginTx(ctx, nil)
	s.endSQLiteIOERRTrace(beginTrace, err)
	if err != nil {
		return fmt.Errorf(
			"begin tx: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "mark_source_success_begin_tx",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	defer tx.Rollback()

	var lastFailAt int64
	lookupTrace := s.beginSQLiteIOERRTrace("channel_source_health_write", "mark_source_success_lookup")
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(last_fail_at, 0) FROM channel_sources WHERE source_id = ?`, sourceID).Scan(&lastFailAt); err != nil {
		s.endSQLiteIOERRTrace(lookupTrace, err)
		if err == sql.ErrNoRows {
			return channels.ErrSourceNotFound
		}
		return fmt.Errorf(
			"lookup source for success: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "mark_source_success_lookup",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	s.endSQLiteIOERRTrace(lookupTrace, nil)

	succeededUnix := succeededAt.UTC().Unix()

	// Ordering guard: skip stale success if a newer failure is already persisted.
	if succeededUnix > 0 && lastFailAt > 0 && succeededUnix < lastFailAt {
		tx.Commit()
		return nil
	}

	successTrace := s.beginSQLiteIOERRTrace("channel_source_health_write", "mark_source_success")
	result, err := tx.ExecContext(
		ctx,
		`UPDATE channel_sources
		 SET success_count = success_count + 1,
		     fail_count = 0,
		     last_fail_at = 0,
		     last_fail_reason = '',
		     last_ok_at = ?,
		     cooldown_until = 0,
		     updated_at = ?
		 WHERE source_id = ?`,
		succeededUnix,
		succeededUnix,
		sourceID,
	)
	s.endSQLiteIOERRTrace(successTrace, err)
	if err != nil {
		return fmt.Errorf(
			"update source success state: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "mark_source_success",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return channels.ErrSourceNotFound
	}

	commitTrace := s.beginSQLiteIOERRTrace("channel_source_health_write", "mark_source_success_commit")
	if err := tx.Commit(); err != nil {
		s.endSQLiteIOERRTrace(commitTrace, err)
		return fmt.Errorf(
			"commit source success state: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "mark_source_success_commit",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	s.endSQLiteIOERRTrace(commitTrace, nil)
	return nil
}

func (s *Store) UpdateSourceProfile(ctx context.Context, sourceID int64, profile channels.SourceProfileUpdate) error {
	if sourceID <= 0 {
		return channels.ErrSourceNotFound
	}

	if profile.Width < 0 {
		profile.Width = 0
	}
	if profile.Height < 0 {
		profile.Height = 0
	}
	if profile.FPS < 0 {
		profile.FPS = 0
	}
	if profile.BitrateBPS < 0 {
		profile.BitrateBPS = 0
	}

	probeAtUnix := int64(0)
	if !profile.LastProbeAt.IsZero() {
		probeAtUnix = profile.LastProbeAt.UTC().Unix()
	}

	now := time.Now().UTC().Unix()
	videoCodec := strings.TrimSpace(profile.VideoCodec)
	audioCodec := strings.TrimSpace(profile.AudioCodec)

	profileTrace := s.beginSQLiteIOERRTrace("channel_source_profile_write", "update_source_profile")

	var result sql.Result
	var err error

	if probeAtUnix > 0 {
		// Monotonic guard: reject stale writes when the persisted profile
		// already has a newer last_probe_at timestamp. Same-timestamp
		// updates are allowed (last-writer-wins for equal timestamps).
		result, err = s.db.ExecContext(
			ctx,
			`UPDATE channel_sources
			 SET last_probe_at = ?,
			     profile_width = ?,
			     profile_height = ?,
			     profile_fps = ?,
			     profile_video_codec = ?,
			     profile_audio_codec = ?,
			     profile_bitrate_bps = ?,
			     updated_at = ?
			 WHERE source_id = ?
			   AND (last_probe_at IS NULL OR last_probe_at <= ?)`,
			probeAtUnix,
			profile.Width,
			profile.Height,
			profile.FPS,
			videoCodec,
			audioCodec,
			profile.BitrateBPS,
			now,
			sourceID,
			probeAtUnix,
		)
	} else {
		// Clear/reset (LastProbeAt is zero): unconditional update with no
		// monotonic guard so that explicit profile resets always apply.
		result, err = s.db.ExecContext(
			ctx,
			`UPDATE channel_sources
			 SET last_probe_at = ?,
			     profile_width = ?,
			     profile_height = ?,
			     profile_fps = ?,
			     profile_video_codec = ?,
			     profile_audio_codec = ?,
			     profile_bitrate_bps = ?,
			     updated_at = ?
			 WHERE source_id = ?`,
			probeAtUnix,
			profile.Width,
			profile.Height,
			profile.FPS,
			videoCodec,
			audioCodec,
			profile.BitrateBPS,
			now,
			sourceID,
		)
	}

	s.endSQLiteIOERRTrace(profileTrace, err)
	if err != nil {
		return fmt.Errorf(
			"update source profile: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "update_source_profile",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		if probeAtUnix > 0 {
			// rows == 0 could mean either stale-write rejection by the
			// monotonic guard OR a truly missing/deleted source. Disambiguate
			// with a lightweight existence check so callers can still detect
			// source drift/deletion.
			if testBeforeProfileExistenceCheck != nil {
				testBeforeProfileExistenceCheck()
			}
			var exists int
			if err := s.db.QueryRowContext(ctx,
				`SELECT 1 FROM channel_sources WHERE source_id = ?`,
				sourceID,
			).Scan(&exists); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return channels.ErrSourceNotFound
				}
				// Propagate real DB/context errors (deadline, cancel, IO).
				return fmt.Errorf("source existence check: %w", err)
			}
			// Source exists but write was rejected by monotonic guard:
			// stale write, treat as successful no-op.
			return nil
		}
		return channels.ErrSourceNotFound
	}
	return nil
}

func (s *Store) ClearSourceHealth(ctx context.Context, channelID int64) (int64, error) {
	exists, err := s.channelExists(ctx, channelID)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, channels.ErrChannelNotFound
	}

	result, err := s.db.ExecContext(
		ctx,
		`UPDATE channel_sources
		 SET success_count = 0,
		     fail_count = 0,
		     last_ok_at = 0,
		     last_fail_at = 0,
		     last_fail_reason = '',
		     cooldown_until = 0,
		     updated_at = ?
		 WHERE channel_id = ?`,
		time.Now().UTC().Unix(),
		channelID,
	)
	if err != nil {
		return 0, fmt.Errorf("clear source health: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return rows, nil
}

func (s *Store) ClearAllSourceHealth(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE channel_sources
		 SET success_count = 0,
		     fail_count = 0,
		     last_ok_at = 0,
		     last_fail_at = 0,
		     last_fail_reason = '',
		     cooldown_until = 0,
		     updated_at = ?`,
		time.Now().UTC().Unix(),
	)
	if err != nil {
		return 0, fmt.Errorf("clear all source health: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return rows, nil
}

func (s *Store) ListDuplicateSuggestions(ctx context.Context, minItems int, searchQuery string, limit, offset int) ([]channels.DuplicateGroup, int, error) {
	if minItems < 2 {
		minItems = 2
	}
	if limit < 1 {
		limit = 1
	}
	if offset < 0 {
		offset = 0
	}

	searchFilterSQL, searchFilterArgs := duplicateSuggestionsSearchFilter(strings.TrimSpace(searchQuery))

	totalQueryArgs := append([]any{}, searchFilterArgs...)
	totalQueryArgs = append(totalQueryArgs, minItems)
	var total int
	if err := s.db.QueryRowContext(
		ctx,
		`
			SELECT COUNT(*)
			FROM (
				SELECT channel_key
				FROM playlist_items
				WHERE active = 1
				  AND channel_key <> ''
			`+searchFilterSQL+`
				GROUP BY channel_key
				HAVING COUNT(*) >= ?
			) grouped
		`,
		totalQueryArgs...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count duplicate suggestions: %w", err)
	}

	groupQueryArgs := append([]any{}, searchFilterArgs...)
	groupQueryArgs = append(groupQueryArgs, minItems, limit, offset)
	groupRows, err := s.db.QueryContext(
		ctx,
		`
			SELECT channel_key, COUNT(*) AS item_count
			FROM playlist_items
			WHERE active = 1
			  AND channel_key <> ''
		`+searchFilterSQL+`
			GROUP BY channel_key
			HAVING COUNT(*) >= ?
			ORDER BY channel_key ASC
			LIMIT ? OFFSET ?
		`,
		groupQueryArgs...,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("query duplicate suggestion groups: %w", err)
	}
	defer groupRows.Close()

	type duplicateGroupRow struct {
		channelKey string
		count      int
	}

	selectedGroups := make([]duplicateGroupRow, 0, limit)
	for groupRows.Next() {
		var row duplicateGroupRow
		if err := groupRows.Scan(&row.channelKey, &row.count); err != nil {
			return nil, 0, fmt.Errorf("scan duplicate suggestion group row: %w", err)
		}
		selectedGroups = append(selectedGroups, row)
	}
	if err := groupRows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate duplicate suggestion group rows: %w", err)
	}
	if len(selectedGroups) == 0 {
		return []channels.DuplicateGroup{}, total, nil
	}

	groups := make([]channels.DuplicateGroup, len(selectedGroups))
	groupIndexByKey := make(map[string]int, len(selectedGroups))
	itemQueryArgs := make([]any, 0, len(selectedGroups))
	placeholders := make([]string, 0, len(selectedGroups))
	for idx, row := range selectedGroups {
		groups[idx] = channels.DuplicateGroup{
			ChannelKey: row.channelKey,
			Count:      row.count,
			Items:      make([]channels.DuplicateItem, 0, row.count),
		}
		groupIndexByKey[row.channelKey] = idx
		itemQueryArgs = append(itemQueryArgs, row.channelKey)
		placeholders = append(placeholders, "?")
	}

	itemRows, err := s.db.QueryContext(
		ctx,
		`
			SELECT channel_key,
			       item_key,
			       name,
			       group_name,
			       COALESCE(tvg_id, ''),
			       COALESCE(tvg_logo, ''),
			       stream_url,
			       active
			FROM playlist_items
			WHERE active = 1
			  AND channel_key IN (`+strings.Join(placeholders, ",")+`)
			ORDER BY channel_key ASC, name ASC, item_key ASC
		`,
		itemQueryArgs...,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("query duplicate suggestion items: %w", err)
	}
	defer itemRows.Close()

	for itemRows.Next() {
		var (
			channelKey string
			item       channels.DuplicateItem
			activeInt  int
		)
		if err := itemRows.Scan(
			&channelKey,
			&item.ItemKey,
			&item.Name,
			&item.GroupName,
			&item.TVGID,
			&item.TVGLogo,
			&item.StreamURL,
			&activeInt,
		); err != nil {
			return nil, 0, fmt.Errorf("scan duplicate suggestion item row: %w", err)
		}
		groupIdx, ok := groupIndexByKey[channelKey]
		if !ok {
			continue
		}
		item.Active = activeInt != 0
		groups[groupIdx].Items = append(groups[groupIdx].Items, item)
	}
	if err := itemRows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate duplicate suggestion item rows: %w", err)
	}

	return groups, total, nil
}

func duplicateSuggestionsSearchFilter(searchQuery string) (string, []any) {
	normalized := strings.TrimSpace(searchQuery)
	if normalized == "" {
		return "", nil
	}

	likePattern := sqlLikeContainsPattern(normalized)
	return `
			AND channel_key IN (
				SELECT DISTINCT channel_key
				FROM playlist_items
				WHERE active = 1
				  AND channel_key <> ''
				  AND (
					channel_key COLLATE NOCASE LIKE ? ESCAPE '!'
					OR TRIM(COALESCE(tvg_id, '')) COLLATE NOCASE LIKE ? ESCAPE '!'
				  )
			)
	`, []any{likePattern, likePattern}
}

func normalizeChannelIDList(channelIDs []int64) ([]int64, error) {
	normalized := make([]int64, 0, len(channelIDs))
	seen := make(map[int64]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		if channelID <= 0 {
			return nil, fmt.Errorf("channel_ids must contain positive ids")
		}
		if _, ok := seen[channelID]; ok {
			continue
		}
		seen[channelID] = struct{}{}
		normalized = append(normalized, channelID)
	}
	return normalized, nil
}

func listSourcesByChannelIDsQuery(channelIDs []int64, enabledOnly bool) (string, []any) {
	args := make([]any, 0, len(channelIDs))
	for _, channelID := range channelIDs {
		args = append(args, channelID)
	}

	query := `
		SELECT
			cs.channel_id,
			cs.source_id,
			cs.item_key,
			COALESCE(p.stream_url, ''),
			cs.priority_index,
			cs.enabled,
			cs.association_type,
			COALESCE(cs.last_ok_at, 0),
			COALESCE(cs.last_fail_at, 0),
			COALESCE(cs.last_fail_reason, ''),
			cs.success_count,
			cs.fail_count,
			cs.cooldown_until,
			COALESCE(cs.last_probe_at, 0),
			COALESCE(cs.profile_width, 0),
			COALESCE(cs.profile_height, 0),
			COALESCE(cs.profile_fps, 0),
			COALESCE(cs.profile_video_codec, ''),
			COALESCE(cs.profile_audio_codec, ''),
			COALESCE(cs.profile_bitrate_bps, 0),
			COALESCE(p.tvg_name, '')
		FROM channel_sources cs
		LEFT JOIN playlist_items p ON p.item_key = cs.item_key
		WHERE cs.channel_id IN (` + inPlaceholders(len(channelIDs)) + `)
	`
	if enabledOnly {
		query += `AND cs.enabled = 1 `
	}
	query += `ORDER BY cs.channel_id ASC, cs.priority_index ASC`
	return query, args
}

func inPlaceholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", count), ",")
}

func (s *Store) listExistingChannelIDs(ctx context.Context, channelIDs []int64) (map[int64]struct{}, error) {
	existing := make(map[int64]struct{}, len(channelIDs))
	for start := 0; start < len(channelIDs); start += maxChannelIDsPerSourceBatch {
		end := start + maxChannelIDsPerSourceBatch
		if end > len(channelIDs) {
			end = len(channelIDs)
		}
		batchIDs := channelIDs[start:end]

		query := `SELECT channel_id FROM published_channels WHERE channel_id IN (` + inPlaceholders(len(batchIDs)) + `)`
		args := make([]any, 0, len(batchIDs))
		for _, channelID := range batchIDs {
			args = append(args, channelID)
		}

		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("query channel existence: %w", err)
		}

		for rows.Next() {
			var channelID int64
			if err := rows.Scan(&channelID); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan channel existence row: %w", err)
			}
			existing[channelID] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate channel existence rows: %w", err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close channel existence rows: %w", err)
		}
	}
	return existing, nil
}

func (s *Store) channelExists(ctx context.Context, channelID int64) (bool, error) {
	var found int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM published_channels WHERE channel_id = ?`, channelID).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check channel exists: %w", err)
	}
	return found == 1, nil
}

func getChannelByIDTx(ctx context.Context, tx *sql.Tx, channelID int64) (channels.Channel, error) {
	var (
		channel               channels.Channel
		enabledInt            int
		dynamicEnabledInt     int
		dynamicSearchRegexInt int
		dynamicGroupNamesJSON string
	)
	err := tx.QueryRowContext(
		ctx,
		`SELECT
			channel_id,
			COALESCE(channel_class, ''),
			COALESCE(channel_key, ''),
			guide_number,
			guide_name,
			order_index,
			enabled,
			COALESCE(dynamic_query_id, 0),
			COALESCE(dynamic_item_key, ''),
			COALESCE(dynamic_sources_enabled, 0),
			COALESCE(dynamic_group_name, ''),
			COALESCE(dynamic_group_names_json, '[]'),
			COALESCE(dynamic_search_query, ''),
			COALESCE(dynamic_search_regex, 0),
			COALESCE((
				SELECT COUNT(*)
				FROM channel_sources cs
				WHERE cs.channel_id = published_channels.channel_id
			), 0),
			COALESCE((
				SELECT COUNT(*)
				FROM channel_sources cs
				WHERE cs.channel_id = published_channels.channel_id
				  AND cs.enabled <> 0
			), 0),
			COALESCE((
				SELECT COUNT(*)
				FROM channel_sources cs
				WHERE cs.channel_id = published_channels.channel_id
				  AND cs.association_type = 'dynamic_query'
			), 0),
			COALESCE((
				SELECT COUNT(*)
				FROM channel_sources cs
				WHERE cs.channel_id = published_channels.channel_id
				  AND cs.association_type <> 'dynamic_query'
			), 0)
		 FROM published_channels
		 WHERE channel_id = ?`,
		channelID,
	).Scan(
		&channel.ChannelID,
		&channel.ChannelClass,
		&channel.ChannelKey,
		&channel.GuideNumber,
		&channel.GuideName,
		&channel.OrderIndex,
		&enabledInt,
		&channel.DynamicQueryID,
		&channel.DynamicItemKey,
		&dynamicEnabledInt,
		&channel.DynamicRule.GroupName,
		&dynamicGroupNamesJSON,
		&channel.DynamicRule.SearchQuery,
		&dynamicSearchRegexInt,
		&channel.SourceTotal,
		&channel.SourceEnabled,
		&channel.SourceDynamic,
		&channel.SourceManual,
	)
	if err != nil {
		return channels.Channel{}, err
	}
	channel.Enabled = enabledInt != 0
	channel.DynamicRule.Enabled = dynamicEnabledInt != 0
	channel.DynamicRule.SearchRegex = dynamicSearchRegexInt != 0
	channel.DynamicRule.GroupName, channel.DynamicRule.GroupNames = normalizeStoredGroupNames(channel.DynamicRule.GroupName, dynamicGroupNamesJSON)
	return channel, nil
}

func getChannelSourceByItemKeyTx(ctx context.Context, tx *sql.Tx, channelID int64, itemKey string) (channels.Source, error) {
	var (
		src        channels.Source
		enabledInt int
		tvgName    string
	)
	err := tx.QueryRowContext(
		ctx,
		`SELECT
			cs.source_id,
			cs.channel_id,
			cs.item_key,
			COALESCE(p.stream_url, ''),
			cs.priority_index,
			cs.enabled,
			cs.association_type,
			COALESCE(cs.last_ok_at, 0),
			COALESCE(cs.last_fail_at, 0),
			COALESCE(cs.last_fail_reason, ''),
			cs.success_count,
			cs.fail_count,
			cs.cooldown_until,
			COALESCE(cs.last_probe_at, 0),
			COALESCE(cs.profile_width, 0),
			COALESCE(cs.profile_height, 0),
			COALESCE(cs.profile_fps, 0),
			COALESCE(cs.profile_video_codec, ''),
			COALESCE(cs.profile_audio_codec, ''),
			COALESCE(cs.profile_bitrate_bps, 0),
			COALESCE(p.tvg_name, '')
		 FROM channel_sources cs
		 LEFT JOIN playlist_items p ON p.item_key = cs.item_key
		 WHERE cs.channel_id = ? AND cs.item_key = ?`,
		channelID,
		itemKey,
	).Scan(
		&src.SourceID,
		&src.ChannelID,
		&src.ItemKey,
		&src.StreamURL,
		&src.PriorityIndex,
		&enabledInt,
		&src.AssociationType,
		&src.LastOKAt,
		&src.LastFailAt,
		&src.LastFailReason,
		&src.SuccessCount,
		&src.FailCount,
		&src.CooldownUntil,
		&src.LastProbeAt,
		&src.ProfileWidth,
		&src.ProfileHeight,
		&src.ProfileFPS,
		&src.ProfileVideoCodec,
		&src.ProfileAudioCodec,
		&src.ProfileBitrateBPS,
		&tvgName,
	)
	if err != nil {
		return channels.Source{}, err
	}
	src.Enabled = enabledInt != 0
	src.TVGName = strings.TrimSpace(tvgName)
	return src, nil
}

func getSourceByIDTx(ctx context.Context, tx *sql.Tx, channelID, sourceID int64) (channels.Source, error) {
	var (
		src        channels.Source
		enabledInt int
		tvgName    string
	)
	err := tx.QueryRowContext(
		ctx,
		`SELECT
			cs.source_id,
			cs.channel_id,
			cs.item_key,
			COALESCE(p.stream_url, ''),
			cs.priority_index,
			cs.enabled,
			cs.association_type,
			COALESCE(cs.last_ok_at, 0),
			COALESCE(cs.last_fail_at, 0),
			COALESCE(cs.last_fail_reason, ''),
			cs.success_count,
			cs.fail_count,
			cs.cooldown_until,
			COALESCE(cs.last_probe_at, 0),
			COALESCE(cs.profile_width, 0),
			COALESCE(cs.profile_height, 0),
			COALESCE(cs.profile_fps, 0),
			COALESCE(cs.profile_video_codec, ''),
			COALESCE(cs.profile_audio_codec, ''),
			COALESCE(cs.profile_bitrate_bps, 0),
			COALESCE(p.tvg_name, '')
		FROM channel_sources cs
		LEFT JOIN playlist_items p ON p.item_key = cs.item_key
		WHERE cs.channel_id = ? AND cs.source_id = ?`,
		channelID,
		sourceID,
	).Scan(
		&src.SourceID,
		&src.ChannelID,
		&src.ItemKey,
		&src.StreamURL,
		&src.PriorityIndex,
		&enabledInt,
		&src.AssociationType,
		&src.LastOKAt,
		&src.LastFailAt,
		&src.LastFailReason,
		&src.SuccessCount,
		&src.FailCount,
		&src.CooldownUntil,
		&src.LastProbeAt,
		&src.ProfileWidth,
		&src.ProfileHeight,
		&src.ProfileFPS,
		&src.ProfileVideoCodec,
		&src.ProfileAudioCodec,
		&src.ProfileBitrateBPS,
		&tvgName,
	)
	if err != nil {
		return channels.Source{}, err
	}
	src.Enabled = enabledInt != 0
	src.TVGName = strings.TrimSpace(tvgName)
	return src, nil
}

func listSourceIDsTx(ctx context.Context, tx *sql.Tx, channelID int64) ([]int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT source_id FROM channel_sources WHERE channel_id = ? ORDER BY priority_index ASC`, channelID)
	if err != nil {
		return nil, fmt.Errorf("query source ids: %w", err)
	}
	defer rows.Close()

	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan source id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source ids: %w", err)
	}
	return ids, nil
}

func applySourceOrderTx(ctx context.Context, tx *sql.Tx, channelID int64, orderedIDs []int64, updatedAt int64) error {
	tempStmt, err := tx.PrepareContext(ctx, `UPDATE channel_sources SET priority_index = ?, updated_at = ? WHERE source_id = ? AND channel_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare temp source reorder statement: %w", err)
	}
	defer tempStmt.Close()

	for i, id := range orderedIDs {
		tempPriority := -(i + 1)
		if _, err := tempStmt.ExecContext(ctx, tempPriority, updatedAt, id, channelID); err != nil {
			return fmt.Errorf("set temporary priority for source %d: %w", id, err)
		}
	}

	finalStmt, err := tx.PrepareContext(ctx, `UPDATE channel_sources SET priority_index = ?, updated_at = ? WHERE source_id = ? AND channel_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare final source reorder statement: %w", err)
	}
	defer finalStmt.Close()

	for i, id := range orderedIDs {
		if _, err := finalStmt.ExecContext(ctx, i, updatedAt, id, channelID); err != nil {
			return fmt.Errorf("set final priority for source %d: %w", id, err)
		}
	}

	return nil
}

func applyLimitOffset(query string, args []any, limit, offset int) (string, []any) {
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
		if offset > 0 {
			query += ` OFFSET ?`
			args = append(args, offset)
		}
		return query, args
	}

	if offset > 0 {
		query += ` LIMIT -1 OFFSET ?`
		args = append(args, offset)
	}
	return query, args
}

func expectedPageCapacity(total, limit, offset int) int {
	if total <= 0 {
		return 0
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= total {
		return 0
	}
	remaining := total - offset
	if limit <= 0 || limit > remaining {
		return remaining
	}
	return limit
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func extractTVGNameFromAttrsRaw(attrsRaw string) string {
	attrsRaw = strings.TrimSpace(attrsRaw)
	if attrsRaw == "" {
		return ""
	}

	var attrs map[string]string
	if err := json.Unmarshal([]byte(attrsRaw), &attrs); err != nil {
		return ""
	}
	if attrs == nil {
		return ""
	}

	if tvgName := strings.TrimSpace(attrs["tvg-name"]); tvgName != "" {
		return tvgName
	}
	return strings.TrimSpace(attrs["tvg_name"])
}

func sourceBackoffDuration(failCount int) time.Duration {
	switch {
	case failCount <= 1:
		return 10 * time.Second
	case failCount == 2:
		return 30 * time.Second
	case failCount == 3:
		return 2 * time.Minute
	case failCount == 4:
		return 10 * time.Minute
	default:
		return 1 * time.Hour
	}
}
