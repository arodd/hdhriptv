package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

const dynamicChannelQuerySelectColumns = `
		query_id,
		enabled,
		COALESCE(name, ''),
		COALESCE(group_name, ''),
		COALESCE(group_names_json, '[]'),
		COALESCE(search_query, ''),
		COALESCE(search_regex, 0),
		order_index,
		COALESCE(last_count, 0),
		COALESCE(truncated_by, 0),
		COALESCE(next_slot_cursor, 0),
		created_at,
		updated_at
`

func (s *Store) ListDynamicChannelQueries(ctx context.Context) ([]channels.DynamicChannelQuery, error) {
	rows, _, err := s.ListDynamicChannelQueriesPaged(ctx, 0, 0)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *Store) ListDynamicChannelQueriesPaged(ctx context.Context, limit, offset int) ([]channels.DynamicChannelQuery, int, error) {
	if limit < 0 {
		return nil, 0, fmt.Errorf("limit must be zero or greater")
	}
	if offset < 0 {
		return nil, 0, fmt.Errorf("offset must be zero or greater")
	}

	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dynamic_channel_queries`).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count dynamic channel queries: %w", err)
	}

	query := `
		SELECT
` + dynamicChannelQuerySelectColumns + `
		FROM dynamic_channel_queries
		ORDER BY order_index ASC, query_id ASC
	`
	args := make([]any, 0, 2)
	query, args = applyLimitOffset(query, args, limit, offset)

	sqlRows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query dynamic channel queries: %w", err)
	}
	defer sqlRows.Close()

	out := make([]channels.DynamicChannelQuery, 0, expectedPageCapacity(total, limit, offset))
	for sqlRows.Next() {
		row, err := scanDynamicChannelQueryRow(sqlRows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan dynamic channel query row: %w", err)
		}
		out = append(out, row)
	}
	if err := sqlRows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate dynamic channel query rows: %w", err)
	}

	return out, total, nil
}

func (s *Store) GetDynamicChannelQuery(ctx context.Context, queryID int64) (channels.DynamicChannelQuery, error) {
	return s.getDynamicChannelQueryByID(ctx, queryID)
}

func (s *Store) CreateDynamicChannelQuery(ctx context.Context, create channels.DynamicChannelQueryCreate) (channels.DynamicChannelQuery, error) {
	create.SearchQuery = strings.TrimSpace(create.SearchQuery)
	if create.SearchRegex {
		if err := validateCatalogSearchRegexQuery(create.SearchQuery); err != nil {
			return channels.DynamicChannelQuery{}, err
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return channels.DynamicChannelQuery{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var maxOrder sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(order_index) FROM dynamic_channel_queries`).Scan(&maxOrder); err != nil {
		return channels.DynamicChannelQuery{}, fmt.Errorf("lookup max dynamic query order: %w", err)
	}

	nextOrderIndex := 0
	if maxOrder.Valid {
		if maxOrder.Int64 >= int64(int(^uint(0)>>1)) {
			return channels.DynamicChannelQuery{}, fmt.Errorf("allocate dynamic guide block: dynamic guide range exhausted")
		}
		nextOrderIndex = int(maxOrder.Int64) + 1
	}
	if _, err := channels.DynamicGuideBlockStart(nextOrderIndex); err != nil {
		return channels.DynamicChannelQuery{}, fmt.Errorf("allocate dynamic guide block: %w", err)
	}

	enabled := true
	if create.Enabled != nil {
		enabled = *create.Enabled
	}

	now := time.Now().UTC().Unix()
	groupNames := channels.NormalizeGroupNames(create.GroupName, create.GroupNames)
	groupNamesJSON := marshalGroupNamesJSON(groupNames)
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO dynamic_channel_queries (
				enabled,
				name,
				group_name,
				group_names_json,
				search_query,
				search_regex,
				order_index,
				last_count,
				truncated_by,
				next_slot_cursor,
				created_at,
				updated_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, ?)`,
		boolToInt(enabled),
		strings.TrimSpace(create.Name),
		channels.GroupNameAlias(groupNames),
		groupNamesJSON,
		create.SearchQuery,
		boolToInt(create.SearchRegex),
		nextOrderIndex,
		now,
		now,
	)
	if err != nil {
		return channels.DynamicChannelQuery{}, fmt.Errorf("insert dynamic channel query: %w", err)
	}
	queryID, err := result.LastInsertId()
	if err != nil {
		return channels.DynamicChannelQuery{}, fmt.Errorf("lookup inserted dynamic channel query id: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return channels.DynamicChannelQuery{}, fmt.Errorf("commit create dynamic channel query: %w", err)
	}
	return s.getDynamicChannelQueryByID(ctx, queryID)
}

func (s *Store) UpdateDynamicChannelQuery(ctx context.Context, queryID int64, update channels.DynamicChannelQueryUpdate) (channels.DynamicChannelQuery, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return channels.DynamicChannelQuery{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	current, err := getDynamicChannelQueryByIDTx(ctx, tx, queryID)
	if err != nil {
		if err == sql.ErrNoRows {
			return channels.DynamicChannelQuery{}, channels.ErrDynamicQueryNotFound
		}
		return channels.DynamicChannelQuery{}, fmt.Errorf("lookup dynamic channel query %d: %w", queryID, err)
	}

	if update.Enabled != nil {
		current.Enabled = *update.Enabled
	}
	if update.Name != nil {
		current.Name = strings.TrimSpace(*update.Name)
	}
	if update.GroupName != nil || update.GroupNames != nil {
		legacy := ""
		if update.GroupName != nil {
			legacy = *update.GroupName
		}
		var raw []string
		if update.GroupNames != nil {
			raw = append([]string(nil), (*update.GroupNames)...)
		}
		current.GroupNames = channels.NormalizeGroupNames(legacy, raw)
		current.GroupName = channels.GroupNameAlias(current.GroupNames)
	}
	if update.SearchQuery != nil {
		current.SearchQuery = strings.TrimSpace(*update.SearchQuery)
	}
	if update.SearchRegex != nil {
		current.SearchRegex = *update.SearchRegex
	}
	if current.SearchRegex {
		if err := validateCatalogSearchRegexQuery(current.SearchQuery); err != nil {
			return channels.DynamicChannelQuery{}, err
		}
	}

	updatedAt := time.Now().UTC().Unix()
	groupNamesJSON := marshalGroupNamesJSON(current.GroupNames)
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE dynamic_channel_queries
		 SET enabled = ?,
		     name = ?,
		     group_name = ?,
		     group_names_json = ?,
		     search_query = ?,
		     search_regex = ?,
		     updated_at = ?
		 WHERE query_id = ?`,
		boolToInt(current.Enabled),
		current.Name,
		current.GroupName,
		groupNamesJSON,
		current.SearchQuery,
		boolToInt(current.SearchRegex),
		updatedAt,
		queryID,
	); err != nil {
		return channels.DynamicChannelQuery{}, fmt.Errorf("update dynamic channel query %d: %w", queryID, err)
	}

	if err := tx.Commit(); err != nil {
		return channels.DynamicChannelQuery{}, fmt.Errorf("commit update dynamic channel query %d: %w", queryID, err)
	}
	return s.getDynamicChannelQueryByID(ctx, queryID)
}

func (s *Store) DeleteDynamicChannelQuery(ctx context.Context, queryID int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	deleteResult, err := tx.ExecContext(ctx, `DELETE FROM dynamic_channel_queries WHERE query_id = ?`, queryID)
	if err != nil {
		return fmt.Errorf("delete dynamic channel query %d: %w", queryID, err)
	}
	rowsAffected, err := deleteResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected deleting dynamic channel query %d: %w", queryID, err)
	}
	if rowsAffected == 0 {
		return channels.ErrDynamicQueryNotFound
	}

	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM published_channels
		 WHERE channel_class = ?
		   AND dynamic_query_id = ?`,
		channels.ChannelClassDynamicGenerated,
		queryID,
	); err != nil {
		return fmt.Errorf("delete generated dynamic channels for query %d: %w", queryID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete dynamic channel query %d: %w", queryID, err)
	}
	return nil
}

func (s *Store) ListDynamicGeneratedChannelsPaged(ctx context.Context, queryID int64, limit, offset int) ([]channels.Channel, int, error) {
	exists, err := s.dynamicChannelQueryExists(ctx, queryID)
	if err != nil {
		return nil, 0, err
	}
	if !exists {
		return nil, 0, channels.ErrDynamicQueryNotFound
	}

	var total int
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		 FROM published_channels
		 WHERE channel_class = ?
		   AND dynamic_query_id = ?`,
		channels.ChannelClassDynamicGenerated,
		queryID,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count generated dynamic channels for query %d: %w", queryID, err)
	}

	query := `
		SELECT
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
		WHERE channel_class = ?
		  AND dynamic_query_id = ?
		ORDER BY order_index ASC
	`
	args := []any{channels.ChannelClassDynamicGenerated, queryID}
	query, args = applyLimitOffset(query, args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query generated dynamic channels for query %d: %w", queryID, err)
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
			return nil, 0, fmt.Errorf("scan generated dynamic channel row for query %d: %w", queryID, err)
		}
		channel.Enabled = enabledInt != 0
		channel.DynamicRule.Enabled = dynamicEnabledInt != 0
		channel.DynamicRule.SearchRegex = dynamicSearchRegexInt != 0
		channel.DynamicRule.GroupName, channel.DynamicRule.GroupNames = normalizeStoredGroupNames(channel.DynamicRule.GroupName, dynamicGroupNamesJSON)
		out = append(out, channel)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate generated dynamic channel rows for query %d: %w", queryID, err)
	}
	return out, total, nil
}

func (s *Store) ReorderDynamicGeneratedChannels(ctx context.Context, queryID int64, channelIDs []int64) error {
	seen := make(map[int64]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		if channelID <= 0 {
			return fmt.Errorf("channel_ids must contain positive ids")
		}
		if _, ok := seen[channelID]; ok {
			return fmt.Errorf("channel_ids contains duplicate id %d", channelID)
		}
		seen[channelID] = struct{}{}
	}
	if len(channelIDs) > channels.DynamicGuideBlockMaxLen {
		return fmt.Errorf("channel_ids count exceeds max %d", channels.DynamicGuideBlockMaxLen)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var queryOrderIndex int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT order_index
		 FROM dynamic_channel_queries
		 WHERE query_id = ?`,
		queryID,
	).Scan(&queryOrderIndex); err != nil {
		if err == sql.ErrNoRows {
			return channels.ErrDynamicQueryNotFound
		}
		return fmt.Errorf("lookup dynamic channel query %d order: %w", queryID, err)
	}

	rows, err := tx.QueryContext(
		ctx,
		`SELECT channel_id
		 FROM published_channels
		 WHERE channel_class = ?
		   AND dynamic_query_id = ?
		 ORDER BY order_index ASC`,
		channels.ChannelClassDynamicGenerated,
		queryID,
	)
	if err != nil {
		return fmt.Errorf("query generated channel ids for query %d: %w", queryID, err)
	}

	currentIDs := make([]int64, 0)
	for rows.Next() {
		var channelID int64
		if err := rows.Scan(&channelID); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan generated channel id for query %d: %w", queryID, err)
		}
		currentIDs = append(currentIDs, channelID)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close generated channel ids for query %d: %w", queryID, err)
	}

	if len(currentIDs) != len(channelIDs) {
		return fmt.Errorf("channel_ids count mismatch: got %d, want %d", len(channelIDs), len(currentIDs))
	}
	for _, channelID := range currentIDs {
		if _, ok := seen[channelID]; !ok {
			return fmt.Errorf("channel_ids missing id %d", channelID)
		}
	}

	now := time.Now().UTC().Unix()
	tempStmt, err := tx.PrepareContext(ctx, `UPDATE published_channels SET order_index = ?, guide_number = ?, updated_at = ? WHERE channel_id = ? AND channel_class = ? AND dynamic_query_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare temporary generated reorder statement: %w", err)
	}
	defer tempStmt.Close()

	for idx, channelID := range channelIDs {
		tempOrder := -(idx + 1)
		tempGuide := fmt.Sprintf("__tmp_dynamic__%d__%d", now, channelID)
		if _, err := tempStmt.ExecContext(
			ctx,
			tempOrder,
			tempGuide,
			now,
			channelID,
			channels.ChannelClassDynamicGenerated,
			queryID,
		); err != nil {
			return fmt.Errorf("set temporary generated order for channel %d: %w", channelID, err)
		}
	}

	finalStmt, err := tx.PrepareContext(ctx, `UPDATE published_channels SET order_index = ?, guide_number = ?, updated_at = ? WHERE channel_id = ? AND channel_class = ? AND dynamic_query_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare final generated reorder statement: %w", err)
	}
	defer finalStmt.Close()

	for idx, channelID := range channelIDs {
		guideNumber, err := channels.DynamicGuideNumber(queryOrderIndex, idx)
		if err != nil {
			return fmt.Errorf("compute guide number for query %d channel %d: %w", queryID, channelID, err)
		}
		orderIndex, err := channels.DynamicChannelOrderIndex(queryOrderIndex, idx)
		if err != nil {
			return fmt.Errorf("compute order index for query %d channel %d: %w", queryID, channelID, err)
		}
		if _, err := finalStmt.ExecContext(
			ctx,
			orderIndex,
			strconv.Itoa(guideNumber),
			now,
			channelID,
			channels.ChannelClassDynamicGenerated,
			queryID,
		); err != nil {
			return fmt.Errorf("set final generated order for channel %d: %w", channelID, err)
		}
	}
	nextCursor := len(channelIDs) % channels.DynamicGuideBlockMaxLen
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE dynamic_channel_queries
		 SET next_slot_cursor = ?
		 WHERE query_id = ?
		   AND next_slot_cursor <> ?`,
		nextCursor,
		queryID,
		nextCursor,
	); err != nil {
		return fmt.Errorf("update next slot cursor for query %d after reorder: %w", queryID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit generated channel reorder for query %d: %w", queryID, err)
	}
	return nil
}

func (s *Store) dynamicChannelQueryExists(ctx context.Context, queryID int64) (bool, error) {
	var count int
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM dynamic_channel_queries WHERE query_id = ?`,
		queryID,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("lookup dynamic channel query %d: %w", queryID, err)
	}
	return count > 0, nil
}

func (s *Store) getDynamicChannelQueryByID(ctx context.Context, queryID int64) (channels.DynamicChannelQuery, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT
`+dynamicChannelQuerySelectColumns+`
		FROM dynamic_channel_queries
		WHERE query_id = ?`,
		queryID,
	)
	query, err := scanDynamicChannelQueryRow(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return channels.DynamicChannelQuery{}, channels.ErrDynamicQueryNotFound
		}
		return channels.DynamicChannelQuery{}, fmt.Errorf("lookup dynamic channel query %d: %w", queryID, err)
	}
	return query, nil
}

func getDynamicChannelQueryByIDTx(ctx context.Context, tx *sql.Tx, queryID int64) (channels.DynamicChannelQuery, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT
`+dynamicChannelQuerySelectColumns+`
		FROM dynamic_channel_queries
		WHERE query_id = ?`,
		queryID,
	)
	return scanDynamicChannelQueryRow(row)
}

type dynamicChannelQueryScanner interface {
	Scan(dest ...any) error
}

func scanDynamicChannelQueryRow(scanner dynamicChannelQueryScanner) (channels.DynamicChannelQuery, error) {
	var (
		query          channels.DynamicChannelQuery
		enabledInt     int
		searchRegexInt int
		groupNamesJSON string
	)
	if err := scanner.Scan(
		&query.QueryID,
		&enabledInt,
		&query.Name,
		&query.GroupName,
		&groupNamesJSON,
		&query.SearchQuery,
		&searchRegexInt,
		&query.OrderIndex,
		&query.LastCount,
		&query.TruncatedBy,
		&query.NextSlotCursor,
		&query.CreatedAt,
		&query.UpdatedAt,
	); err != nil {
		return channels.DynamicChannelQuery{}, err
	}
	query.Enabled = enabledInt != 0
	query.SearchRegex = searchRegexInt != 0
	query.Name = strings.TrimSpace(query.Name)
	query.GroupName, query.GroupNames = normalizeStoredGroupNames(query.GroupName, groupNamesJSON)
	query.SearchQuery = strings.TrimSpace(query.SearchQuery)
	if query.LastCount < 0 {
		query.LastCount = 0
	}
	if query.TruncatedBy < 0 {
		query.TruncatedBy = 0
	}
	query.NextSlotCursor = normalizeDynamicSlotCursor(query.NextSlotCursor)
	return query, nil
}
