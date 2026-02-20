package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

type dynamicQueryRow struct {
	QueryID        int64
	Enabled        bool
	GroupName      string
	GroupNames     []string
	SearchQuery    string
	SearchRegex    bool
	NextSlotCursor int
	OrderIndex     int
}

type dynamicMatchRow struct {
	ItemKey    string
	ChannelKey string
	GuideName  string
	TVGID      string
	StreamURL  string
}

type generatedChannelRow struct {
	ChannelID      int64
	ItemKey        string
	SourceIdentity string
	GuideNumber    string
	GuideName      string
	ChannelKey     string
	OrderIndex     int
}

const maxCatalogFilterItemKeyBatch = 900

func (s *Store) SyncDynamicChannelBlocks(ctx context.Context) (channels.DynamicChannelSyncResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return channels.DynamicChannelSyncResult{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	queries, err := listDynamicQueryRowsTx(ctx, tx)
	if err != nil {
		return channels.DynamicChannelSyncResult{}, err
	}

	now := time.Now().UTC().Unix()
	result := channels.DynamicChannelSyncResult{
		QueriesProcessed: len(queries),
	}

	for _, query := range queries {
		if !query.Enabled {
			removed, err := deleteDynamicGeneratedChannelsForQueryTx(ctx, tx, query.QueryID)
			if err != nil {
				return channels.DynamicChannelSyncResult{}, err
			}
			if err := updateDynamicQuerySyncStateTx(ctx, tx, query.QueryID, 0, 0, query.NextSlotCursor); err != nil {
				return channels.DynamicChannelSyncResult{}, err
			}
			result.ChannelsRemoved += removed
			continue
		}

		result.QueriesEnabled++
		queryResult, nextSlotCursor, err := syncOneDynamicQueryBlockTx(ctx, tx, query, now)
		if err != nil {
			return channels.DynamicChannelSyncResult{}, err
		}
		lastCount := queryResult.ChannelsAdded + queryResult.ChannelsUpdated + queryResult.ChannelsRetained
		if err := updateDynamicQuerySyncStateTx(ctx, tx, query.QueryID, lastCount, queryResult.TruncatedCount, nextSlotCursor); err != nil {
			return channels.DynamicChannelSyncResult{}, err
		}
		result.ChannelsAdded += queryResult.ChannelsAdded
		result.ChannelsUpdated += queryResult.ChannelsUpdated
		result.ChannelsRetained += queryResult.ChannelsRetained
		result.ChannelsRemoved += queryResult.ChannelsRemoved
		result.TruncatedCount += queryResult.TruncatedCount
	}

	removedOrphans, err := deleteOrphanDynamicGeneratedChannelsTx(ctx, tx)
	if err != nil {
		return channels.DynamicChannelSyncResult{}, err
	}
	result.ChannelsRemoved += removedOrphans

	if err := tx.Commit(); err != nil {
		return channels.DynamicChannelSyncResult{}, fmt.Errorf("commit dynamic channel block sync: %w", err)
	}
	return result, nil
}

// RefreshDynamicGeneratedGuideNames re-synchronizes dynamic generated channel
// guide names from active catalog items without mutating slot/order assignments.
func (s *Store) RefreshDynamicGeneratedGuideNames(ctx context.Context) (int, error) {
	now := time.Now().UTC().Unix()
	updateResult, err := s.db.ExecContext(
		ctx,
		`UPDATE published_channels
		 SET guide_name = (
			 SELECT CASE
					 WHEN TRIM(COALESCE(p.name, '')) <> '' THEN TRIM(COALESCE(p.name, ''))
					 ELSE p.item_key
				 END
			 FROM playlist_items p
			 WHERE p.item_key = published_channels.dynamic_item_key
			   AND p.active <> 0
		 ),
		     updated_at = ?
		 WHERE channel_class = ?
		   AND COALESCE(dynamic_item_key, '') <> ''
		   AND EXISTS (
			 SELECT 1
			 FROM playlist_items p
			 WHERE p.item_key = published_channels.dynamic_item_key
			   AND p.active <> 0
			   AND (
				 CASE
					 WHEN TRIM(COALESCE(p.name, '')) <> '' THEN TRIM(COALESCE(p.name, ''))
					 ELSE p.item_key
				 END
			   ) <> published_channels.guide_name
		   )`,
		now,
		channels.ChannelClassDynamicGenerated,
	)
	if err != nil {
		return 0, fmt.Errorf("refresh dynamic generated guide names: %w", err)
	}
	rowsAffected, err := updateResult.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected refreshing dynamic generated guide names: %w", err)
	}
	return int(rowsAffected), nil
}

func updateDynamicQuerySyncStateTx(ctx context.Context, tx *sql.Tx, queryID int64, lastCount, truncatedBy, nextSlotCursor int) error {
	if lastCount < 0 {
		lastCount = 0
	}
	if truncatedBy < 0 {
		truncatedBy = 0
	}
	nextSlotCursor = normalizeDynamicSlotCursor(nextSlotCursor)
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE dynamic_channel_queries
		 SET last_count = ?,
		     truncated_by = ?,
		     next_slot_cursor = ?
		 WHERE query_id = ?
		   AND (last_count <> ? OR truncated_by <> ? OR next_slot_cursor <> ?)`,
		lastCount,
		truncatedBy,
		nextSlotCursor,
		queryID,
		lastCount,
		truncatedBy,
		nextSlotCursor,
	); err != nil {
		return fmt.Errorf("update dynamic sync state for query %d: %w", queryID, err)
	}
	return nil
}

func listDynamicQueryRowsTx(ctx context.Context, tx *sql.Tx) ([]dynamicQueryRow, error) {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT
			query_id,
			enabled,
			COALESCE(group_name, ''),
			COALESCE(group_names_json, '[]'),
			COALESCE(search_query, ''),
			COALESCE(search_regex, 0),
			COALESCE(next_slot_cursor, 0),
			order_index
		FROM dynamic_channel_queries
		ORDER BY order_index ASC, query_id ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("query dynamic channel queries: %w", err)
	}
	defer rows.Close()

	out := make([]dynamicQueryRow, 0)
	for rows.Next() {
		var (
			row            dynamicQueryRow
			enabledInt     int
			searchRegexInt int
			groupNamesJSON string
		)
		if err := rows.Scan(
			&row.QueryID,
			&enabledInt,
			&row.GroupName,
			&groupNamesJSON,
			&row.SearchQuery,
			&searchRegexInt,
			&row.NextSlotCursor,
			&row.OrderIndex,
		); err != nil {
			return nil, fmt.Errorf("scan dynamic channel query row: %w", err)
		}
		row.Enabled = enabledInt != 0
		row.SearchRegex = searchRegexInt != 0
		row.GroupName, row.GroupNames = normalizeStoredGroupNames(row.GroupName, groupNamesJSON)
		row.SearchQuery = strings.TrimSpace(row.SearchQuery)
		row.NextSlotCursor = normalizeDynamicSlotCursor(row.NextSlotCursor)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dynamic channel query rows: %w", err)
	}
	return out, nil
}

func syncOneDynamicQueryBlockTx(ctx context.Context, tx *sql.Tx, query dynamicQueryRow, now int64) (channels.DynamicChannelSyncResult, int, error) {
	existingRows, err := listGeneratedChannelsForQueryTx(ctx, tx, query.QueryID)
	if err != nil {
		return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, err
	}

	spec, err := buildCatalogFilterQuerySpec(query.GroupNames, query.SearchQuery, query.SearchRegex)
	if err != nil {
		return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, err
	}
	totalMatches, err := countDynamicQueryMatchesTx(ctx, tx, spec)
	if err != nil {
		return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, err
	}

	matchURLIdentityCounts, err := countDynamicMatchURLIdentityOccurrencesTx(ctx, tx, spec)
	if err != nil {
		return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, err
	}

	result := channels.DynamicChannelSyncResult{}
	if totalMatches > channels.DynamicGuideBlockMaxLen {
		result.TruncatedCount = totalMatches - channels.DynamicGuideBlockMaxLen
	}

	type matchWithIdentity struct {
		Match          dynamicMatchRow
		SourceIdentity string
	}
	type retainedGeneratedRow struct {
		Existing generatedChannelRow
		Matched  matchWithIdentity
	}

	existingByIdentity := make(map[string][]generatedChannelRow, len(existingRows))
	for _, existing := range existingRows {
		sourceIdentity := normalizeDynamicSourceIdentity(existing.SourceIdentity)
		if sourceIdentity == "" {
			sourceIdentity = dynamicSourceIdentityFromItemKey(existing.ItemKey)
		}
		if sourceIdentity == "" {
			continue
		}
		existingByIdentity[sourceIdentity] = append(existingByIdentity[sourceIdentity], existing)
	}

	const queryPageSize = 256
	offset := 0
	matchedExistingByChannelID := make(map[int64]retainedGeneratedRow, len(existingRows))
	newCandidates := make([]matchWithIdentity, 0, channels.DynamicGuideBlockMaxLen)
	newSeenIdentities := make(map[string]struct{}, channels.DynamicGuideBlockMaxLen)

	for {
		page, err := listDynamicQueryMatchesPageTx(ctx, tx, spec, queryPageSize, offset)
		if err != nil {
			return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, err
		}
		if len(page) == 0 {
			break
		}
		offset += len(page)

		for _, match := range page {
			sourceIdentity := dynamicSourceIdentityForMatch(match, matchURLIdentityCounts)
			if sourceIdentity == "" {
				continue
			}

			if existingForIdentity, hasExisting := existingByIdentity[sourceIdentity]; hasExisting {
				matched := false
				for _, existing := range existingForIdentity {
					if _, alreadyMatched := matchedExistingByChannelID[existing.ChannelID]; alreadyMatched {
						continue
					}
					matchedExistingByChannelID[existing.ChannelID] = retainedGeneratedRow{
						Existing: existing,
						Matched: matchWithIdentity{
							Match:          match,
							SourceIdentity: sourceIdentity,
						},
					}
					matched = true
					break
				}
				if matched {
					continue
				}
				// This identity already maps to a retained row; skip duplicates.
				continue
			}

			if _, seen := newSeenIdentities[sourceIdentity]; seen {
				continue
			}
			newSeenIdentities[sourceIdentity] = struct{}{}
			newCandidates = append(newCandidates, matchWithIdentity{
				Match:          match,
				SourceIdentity: sourceIdentity,
			})
		}

		if len(page) < queryPageSize {
			break
		}
	}

	freeSlots := make([]bool, channels.DynamicGuideBlockMaxLen)
	for idx := range freeSlots {
		freeSlots[idx] = true
	}
	retainedBySlot := make(map[int]retainedGeneratedRow, len(existingRows))
	pendingRetained := make([]retainedGeneratedRow, 0)
	keepExistingChannelIDs := make(map[int64]struct{}, len(existingRows))
	for _, existing := range existingRows {
		retained, ok := matchedExistingByChannelID[existing.ChannelID]
		if !ok {
			continue
		}
		keepExistingChannelIDs[existing.ChannelID] = struct{}{}
		slot, slotOK := dynamicSlotForGeneratedRow(query.OrderIndex, existing)
		if !slotOK || !freeSlots[slot] {
			pendingRetained = append(pendingRetained, retained)
			continue
		}
		retainedBySlot[slot] = retained
		freeSlots[slot] = false
	}

	freeCount := 0
	for _, free := range freeSlots {
		if free {
			freeCount++
		}
	}

	nextSlotCursor := normalizeDynamicSlotCursor(query.NextSlotCursor)
	allocationOccurred := false
	allocateSlot := func() (int, bool) {
		slot, nextCursor, ok := nextFreeDynamicSlot(freeSlots, nextSlotCursor)
		if !ok {
			return 0, false
		}
		nextSlotCursor = nextCursor
		allocationOccurred = true
		freeCount--
		return slot, true
	}

	for _, retained := range pendingRetained {
		slot, ok := allocateSlot()
		if !ok {
			delete(keepExistingChannelIDs, retained.Existing.ChannelID)
			continue
		}
		retainedBySlot[slot] = retained
	}

	newBySlot := make(map[int]matchWithIdentity, freeCount)
	if freeCount > 0 {
		for _, candidate := range newCandidates {
			slot, ok := allocateSlot()
			if !ok {
				break
			}
			newBySlot[slot] = candidate
			if freeCount == 0 {
				break
			}
		}
	}
	if !allocationOccurred {
		nextSlotCursor = normalizeDynamicSlotCursor(query.NextSlotCursor)
	}

	for _, existing := range existingRows {
		if _, ok := keepExistingChannelIDs[existing.ChannelID]; ok {
			continue
		}
		deleteResult, err := tx.ExecContext(
			ctx,
			`DELETE FROM published_channels
			 WHERE channel_id = ?
			   AND channel_class = ?
			   AND dynamic_query_id = ?`,
			existing.ChannelID,
			channels.ChannelClassDynamicGenerated,
			query.QueryID,
		)
		if err != nil {
			return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, fmt.Errorf("delete stale generated channel %d: %w", existing.ChannelID, err)
		}
		rowsAffected, err := deleteResult.RowsAffected()
		if err != nil {
			return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, fmt.Errorf("rows affected deleting stale generated channel %d: %w", existing.ChannelID, err)
		}
		if rowsAffected > 0 {
			result.ChannelsRemoved++
		}
	}

	for slot := 0; slot < channels.DynamicGuideBlockMaxLen; slot++ {
		retained, hasRetained := retainedBySlot[slot]
		match, hasNew := newBySlot[slot]
		if !hasRetained && !hasNew {
			continue
		}
		if hasRetained {
			match = retained.Matched
		}

		itemKey := strings.TrimSpace(match.Match.ItemKey)
		if itemKey == "" {
			continue
		}
		sourceIdentity := normalizeDynamicSourceIdentity(match.SourceIdentity)
		if sourceIdentity == "" {
			sourceIdentity = dynamicSourceIdentityFromItemKey(itemKey)
		}
		if sourceIdentity == "" {
			continue
		}
		guideNumber, err := channels.DynamicGuideNumber(query.OrderIndex, slot)
		if err != nil {
			return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, fmt.Errorf("compute guide number for query %d slot %d: %w", query.QueryID, slot, err)
		}
		orderIndex, err := channels.DynamicChannelOrderIndex(query.OrderIndex, slot)
		if err != nil {
			return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, fmt.Errorf("compute channel order index for query %d slot %d: %w", query.QueryID, slot, err)
		}

		guideName := strings.TrimSpace(match.Match.GuideName)
		if guideName == "" {
			guideName = itemKey
		}
		channelKey := normalizeChannelKey(match.Match.ChannelKey)
		guideNumberStr := strconv.Itoa(guideNumber)

		if hasRetained {
			existing := retained.Existing
			if existing.GuideNumber == guideNumberStr &&
				existing.GuideName == guideName &&
				existing.ChannelKey == channelKey &&
				existing.ItemKey == itemKey &&
				existing.SourceIdentity == sourceIdentity &&
				existing.OrderIndex == orderIndex {
				result.ChannelsRetained++
			} else {
				if _, err := tx.ExecContext(
					ctx,
					`UPDATE published_channels
					 SET channel_key = ?,
					     guide_number = ?,
					     guide_name = ?,
					     order_index = ?,
					     enabled = 1,
					     dynamic_query_id = ?,
					     dynamic_item_key = ?,
					     dynamic_source_identity = ?,
					     dynamic_sources_enabled = 0,
					     dynamic_group_name = '',
					     dynamic_group_names_json = '[]',
					     dynamic_search_query = '',
					     dynamic_search_regex = 0,
					     updated_at = ?
					 WHERE channel_id = ?`,
					channelKey,
					guideNumberStr,
					guideName,
					orderIndex,
					query.QueryID,
					itemKey,
					sourceIdentity,
					now,
					existing.ChannelID,
				); err != nil {
					return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, fmt.Errorf("update generated channel %d: %w", existing.ChannelID, err)
				}
				result.ChannelsUpdated++
			}
			if err := ensureGeneratedChannelSourceTx(ctx, tx, existing.ChannelID, itemKey, now); err != nil {
				return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, err
			}
			continue
		}

		insertResult, err := tx.ExecContext(
			ctx,
			`INSERT INTO published_channels (
				channel_class,
				channel_key,
				guide_number,
				guide_name,
				order_index,
				enabled,
				dynamic_query_id,
				dynamic_item_key,
				dynamic_source_identity,
				dynamic_sources_enabled,
				dynamic_group_name,
				dynamic_group_names_json,
				dynamic_search_query,
				dynamic_search_regex,
				created_at,
				updated_at
			)
			VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, 0, '', '[]', '', 0, ?, ?)`,
			channels.ChannelClassDynamicGenerated,
			channelKey,
			guideNumberStr,
			guideName,
			orderIndex,
			query.QueryID,
			itemKey,
			sourceIdentity,
			now,
			now,
		)
		if err != nil {
			return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, fmt.Errorf("insert generated channel for item %q: %w", itemKey, err)
		}
		channelID, err := insertResult.LastInsertId()
		if err != nil {
			return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, fmt.Errorf("read generated channel id for item %q: %w", itemKey, err)
		}
		if err := ensureGeneratedChannelSourceTx(ctx, tx, channelID, itemKey, now); err != nil {
			return channels.DynamicChannelSyncResult{}, query.NextSlotCursor, err
		}
		result.ChannelsAdded++
	}

	return result, nextSlotCursor, nil
}

func normalizeDynamicSlotCursor(cursor int) int {
	if cursor < 0 {
		return 0
	}
	if channels.DynamicGuideBlockMaxLen <= 0 {
		return 0
	}
	if cursor >= channels.DynamicGuideBlockMaxLen {
		return cursor % channels.DynamicGuideBlockMaxLen
	}
	return cursor
}

func nextFreeDynamicSlot(freeSlots []bool, cursor int) (slot int, nextCursor int, ok bool) {
	if len(freeSlots) == 0 {
		return 0, 0, false
	}
	cursor = normalizeDynamicSlotCursor(cursor)
	for attempt := 0; attempt < len(freeSlots); attempt++ {
		candidate := (cursor + attempt) % len(freeSlots)
		if !freeSlots[candidate] {
			continue
		}
		freeSlots[candidate] = false
		return candidate, (candidate + 1) % len(freeSlots), true
	}
	return 0, cursor, false
}

func dynamicSlotForGeneratedRow(queryOrderIndex int, row generatedChannelRow) (int, bool) {
	blockStart, err := channels.DynamicGuideBlockStart(queryOrderIndex)
	if err == nil {
		guideNumber, parseErr := strconv.Atoi(strings.TrimSpace(row.GuideNumber))
		if parseErr == nil {
			slot := guideNumber - blockStart
			if slot >= 0 && slot < channels.DynamicGuideBlockMaxLen {
				return slot, true
			}
		}
	}

	baseOrder, err := channels.DynamicChannelOrderIndex(queryOrderIndex, 0)
	if err != nil {
		return 0, false
	}
	slot := row.OrderIndex - baseOrder
	if slot >= 0 && slot < channels.DynamicGuideBlockMaxLen {
		return slot, true
	}
	return 0, false
}

func countDynamicQueryMatchesTx(ctx context.Context, tx *sql.Tx, spec catalogFilterQuerySpec) (int, error) {
	query := `SELECT COUNT(*)
		FROM playlist_items INDEXED BY ` + spec.OrderIndex + `
		WHERE ` + spec.WhereClause

	var count int
	if err := tx.QueryRowContext(ctx, query, spec.Args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count dynamic channel matches groups=%v search=%q: %w", spec.GroupNames, spec.SearchQuery, err)
	}
	return count, nil
}

func countDynamicMatchURLIdentityOccurrencesTx(ctx context.Context, tx *sql.Tx, spec catalogFilterQuerySpec) (map[string]int, error) {
	counts := make(map[string]int)
	const queryPageSize = 512
	offset := 0

	for {
		page, err := listDynamicQueryMatchesPageTx(ctx, tx, spec, queryPageSize, offset)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		offset += len(page)

		for _, match := range page {
			if normalizeDynamicTVGID(match.TVGID) != "" {
				continue
			}
			normalizedURL := normalizeDynamicSourceURLIdentity(match.StreamURL)
			if normalizedURL == "" {
				continue
			}
			if counts[normalizedURL] < 2 {
				counts[normalizedURL]++
			}
		}
		if len(page) < queryPageSize {
			break
		}
	}

	return counts, nil
}

func dynamicSourceIdentityForMatch(match dynamicMatchRow, urlIdentityCounts map[string]int) string {
	if tvgID := normalizeDynamicTVGID(match.TVGID); tvgID != "" {
		return "tvg:" + tvgID
	}
	if normalizedURL := normalizeDynamicSourceURLIdentity(match.StreamURL); normalizedURL != "" {
		if urlIdentityCounts[normalizedURL] == 1 {
			return "url:" + normalizedURL
		}
	}
	return dynamicSourceIdentityFromItemKey(match.ItemKey)
}

func normalizeDynamicSourceIdentity(raw string) string {
	identity := strings.TrimSpace(raw)
	if identity == "" {
		return ""
	}
	lower := strings.ToLower(identity)
	switch {
	case strings.HasPrefix(lower, "tvg:"):
		if tvgID := normalizeDynamicTVGID(identity[4:]); tvgID != "" {
			return "tvg:" + tvgID
		}
		return ""
	case strings.HasPrefix(lower, "url:"):
		if normalizedURL := normalizeDynamicSourceURLIdentity(identity[4:]); normalizedURL != "" {
			return "url:" + normalizedURL
		}
		return ""
	case strings.HasPrefix(lower, "item:"):
		return dynamicSourceIdentityFromItemKey(identity[5:])
	default:
		return lower
	}
}

func normalizeDynamicTVGID(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func normalizeDynamicSourceURLIdentity(raw string) string {
	parsedURL, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return ""
	}
	parsedURL.RawQuery = ""
	parsedURL.Fragment = ""
	parsedURL.User = nil
	path := parsedURL.Path
	return strings.ToLower(parsedURL.Scheme) + "://" + strings.ToLower(parsedURL.Host) + path
}

func dynamicSourceIdentityFromItemKey(itemKey string) string {
	itemKey = strings.ToLower(strings.TrimSpace(itemKey))
	if itemKey == "" {
		return ""
	}
	return "item:" + itemKey
}

func listDynamicQueryMatchesByItemKeysTx(ctx context.Context, tx *sql.Tx, spec catalogFilterQuerySpec, itemKeys []string) (map[string]dynamicMatchRow, error) {
	out := make(map[string]dynamicMatchRow, len(itemKeys))
	if len(itemKeys) == 0 {
		return out, nil
	}

	normalized := make([]string, 0, len(itemKeys))
	seen := make(map[string]struct{}, len(itemKeys))
	for _, itemKey := range itemKeys {
		itemKey = strings.TrimSpace(itemKey)
		if itemKey == "" {
			continue
		}
		if _, exists := seen[itemKey]; exists {
			continue
		}
		seen[itemKey] = struct{}{}
		normalized = append(normalized, itemKey)
	}
	if len(normalized) == 0 {
		return out, nil
	}

	for start := 0; start < len(normalized); start += maxCatalogFilterItemKeyBatch {
		end := start + maxCatalogFilterItemKeyBatch
		if end > len(normalized) {
			end = len(normalized)
		}
		batch := normalized[start:end]

		query := `SELECT item_key, COALESCE(channel_key, ''), name
			FROM playlist_items INDEXED BY ` + spec.OrderIndex + `
			WHERE ` + spec.WhereClause + `
			  AND item_key IN (` + inPlaceholders(len(batch)) + `)`

		args := make([]any, 0, len(spec.Args)+len(batch))
		args = append(args, spec.Args...)
		for _, itemKey := range batch {
			args = append(args, itemKey)
		}

		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("query dynamic channel match keys groups=%v search=%q: %w", spec.GroupNames, spec.SearchQuery, err)
		}

		for rows.Next() {
			var row dynamicMatchRow
			if err := rows.Scan(&row.ItemKey, &row.ChannelKey, &row.GuideName); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan dynamic channel keyed match row: %w", err)
			}
			row.ItemKey = strings.TrimSpace(row.ItemKey)
			if row.ItemKey == "" {
				continue
			}
			row.ChannelKey = strings.TrimSpace(row.ChannelKey)
			row.GuideName = strings.TrimSpace(row.GuideName)
			if _, exists := out[row.ItemKey]; exists {
				continue
			}
			out[row.ItemKey] = row
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("iterate dynamic channel keyed match rows: %w", err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close dynamic channel keyed match rows: %w", err)
		}
	}

	return out, nil
}

func listDynamicQueryMatchesPageTx(ctx context.Context, tx *sql.Tx, spec catalogFilterQuerySpec, limit, offset int) ([]dynamicMatchRow, error) {
	if limit <= 0 {
		return []dynamicMatchRow{}, nil
	}
	if offset < 0 {
		offset = 0
	}

	query := `SELECT item_key, COALESCE(channel_key, ''), name, COALESCE(tvg_id, ''), COALESCE(stream_url, '')
		FROM playlist_items INDEXED BY ` + spec.OrderIndex + `
		WHERE ` + spec.WhereClause + `
		ORDER BY name ASC, item_key ASC
		LIMIT ? OFFSET ?`

	args := make([]any, 0, len(spec.Args)+2)
	args = append(args, spec.Args...)
	args = append(args, limit, offset)

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query dynamic channel match page groups=%v search=%q: %w", spec.GroupNames, spec.SearchQuery, err)
	}
	defer rows.Close()

	out := make([]dynamicMatchRow, 0)
	for rows.Next() {
		var row dynamicMatchRow
		if err := rows.Scan(&row.ItemKey, &row.ChannelKey, &row.GuideName, &row.TVGID, &row.StreamURL); err != nil {
			return nil, fmt.Errorf("scan dynamic channel match row: %w", err)
		}
		row.ItemKey = strings.TrimSpace(row.ItemKey)
		if row.ItemKey == "" {
			continue
		}
		row.ChannelKey = strings.TrimSpace(row.ChannelKey)
		row.GuideName = strings.TrimSpace(row.GuideName)
		row.TVGID = strings.TrimSpace(row.TVGID)
		row.StreamURL = strings.TrimSpace(row.StreamURL)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dynamic channel match rows: %w", err)
	}
	return out, nil
}

func listGeneratedChannelsForQueryTx(ctx context.Context, tx *sql.Tx, queryID int64) ([]generatedChannelRow, error) {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT
			channel_id,
			COALESCE(dynamic_item_key, ''),
			COALESCE(dynamic_source_identity, ''),
			guide_number,
			guide_name,
			COALESCE(channel_key, ''),
			order_index
		FROM published_channels
		WHERE channel_class = ?
		  AND dynamic_query_id = ?
		ORDER BY order_index ASC, channel_id ASC`,
		channels.ChannelClassDynamicGenerated,
		queryID,
	)
	if err != nil {
		return nil, fmt.Errorf("query generated channels for dynamic query %d: %w", queryID, err)
	}
	defer rows.Close()

	out := make([]generatedChannelRow, 0)
	for rows.Next() {
		var row generatedChannelRow
		if err := rows.Scan(
			&row.ChannelID,
			&row.ItemKey,
			&row.SourceIdentity,
			&row.GuideNumber,
			&row.GuideName,
			&row.ChannelKey,
			&row.OrderIndex,
		); err != nil {
			return nil, fmt.Errorf("scan generated channel row: %w", err)
		}
		row.ItemKey = strings.TrimSpace(row.ItemKey)
		if row.ItemKey == "" {
			continue
		}
		row.SourceIdentity = normalizeDynamicSourceIdentity(row.SourceIdentity)
		row.GuideNumber = strings.TrimSpace(row.GuideNumber)
		row.GuideName = strings.TrimSpace(row.GuideName)
		row.ChannelKey = normalizeChannelKey(row.ChannelKey)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate generated channel rows: %w", err)
	}
	return out, nil
}

func ensureGeneratedChannelSourceTx(ctx context.Context, tx *sql.Tx, channelID int64, itemKey string, now int64) error {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT source_id, item_key
		FROM channel_sources
		WHERE channel_id = ?
		ORDER BY priority_index ASC, source_id ASC`,
		channelID,
	)
	if err != nil {
		return fmt.Errorf("query generated channel %d sources: %w", channelID, err)
	}

	type sourceRow struct {
		SourceID int64
		ItemKey  string
	}
	sources := make([]sourceRow, 0)
	for rows.Next() {
		var source sourceRow
		if err := rows.Scan(&source.SourceID, &source.ItemKey); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan generated channel %d source row: %w", channelID, err)
		}
		source.ItemKey = strings.TrimSpace(source.ItemKey)
		sources = append(sources, source)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close generated channel %d source rows: %w", channelID, err)
	}

	keepSourceID := int64(0)
	for _, source := range sources {
		if source.ItemKey == itemKey {
			keepSourceID = source.SourceID
			break
		}
	}

	for _, source := range sources {
		if source.SourceID == keepSourceID {
			continue
		}
		if _, err := tx.ExecContext(
			ctx,
			`DELETE FROM channel_sources
			WHERE channel_id = ? AND source_id = ?`,
			channelID,
			source.SourceID,
		); err != nil {
			return fmt.Errorf("delete generated channel %d stale source %d: %w", channelID, source.SourceID, err)
		}
	}

	if keepSourceID <= 0 {
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
			VALUES (?, ?, 0, 1, 'dynamic_channel_item', ?, ?)`,
			channelID,
			itemKey,
			now,
			now,
		); err != nil {
			return fmt.Errorf("insert generated channel %d source: %w", channelID, err)
		}
		return nil
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE channel_sources
		 SET item_key = ?,
		     priority_index = 0,
		     enabled = 1,
		     association_type = 'dynamic_channel_item',
		     updated_at = ?
		 WHERE channel_id = ? AND source_id = ?
		   AND (
		     item_key <> ? OR
		     priority_index <> 0 OR
		     enabled <> 1 OR
		     association_type <> 'dynamic_channel_item'
		   )`,
		itemKey,
		now,
		channelID,
		keepSourceID,
		itemKey,
	); err != nil {
		return fmt.Errorf("normalize generated channel %d source %d: %w", channelID, keepSourceID, err)
	}

	return nil
}

func deleteDynamicGeneratedChannelsForQueryTx(ctx context.Context, tx *sql.Tx, queryID int64) (int, error) {
	result, err := tx.ExecContext(
		ctx,
		`DELETE FROM published_channels
		 WHERE channel_class = ?
		   AND dynamic_query_id = ?`,
		channels.ChannelClassDynamicGenerated,
		queryID,
	)
	if err != nil {
		return 0, fmt.Errorf("delete generated channels for query %d: %w", queryID, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected deleting generated channels for query %d: %w", queryID, err)
	}
	return int(rowsAffected), nil
}

func deleteOrphanDynamicGeneratedChannelsTx(ctx context.Context, tx *sql.Tx) (int, error) {
	result, err := tx.ExecContext(
		ctx,
		`DELETE FROM published_channels
		 WHERE channel_class = ?
		   AND (
			 dynamic_query_id IS NULL OR
			 NOT EXISTS (
				 SELECT 1
				 FROM dynamic_channel_queries q
				 WHERE q.query_id = published_channels.dynamic_query_id
			 )
		   )`,
		channels.ChannelClassDynamicGenerated,
	)
	if err != nil {
		return 0, fmt.Errorf("delete orphan generated channels: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected deleting orphan generated channels: %w", err)
	}
	return int(rowsAffected), nil
}
