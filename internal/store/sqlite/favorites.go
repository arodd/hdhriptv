package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/favorites"
)

func (s *Store) AddFavorite(ctx context.Context, itemKey string, startGuideNumber int) (favorites.Favorite, error) {
	itemKey = strings.TrimSpace(itemKey)
	if itemKey == "" {
		return favorites.Favorite{}, fmt.Errorf("item_key is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return favorites.Favorite{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	info, err := lookupItemInfoTx(ctx, tx, itemKey)
	if err != nil {
		if err == sql.ErrNoRows {
			return favorites.Favorite{}, favorites.ErrItemNotFound
		}
		return favorites.Favorite{}, fmt.Errorf("lookup playlist item: %w", err)
	}

	existing, err := getFavoriteByItemKeyTx(ctx, tx, itemKey)
	if err == nil {
		if err := tx.Commit(); err != nil {
			return favorites.Favorite{}, fmt.Errorf("commit existing favorite lookup: %w", err)
		}
		return existing, nil
	}
	if err != sql.ErrNoRows {
		return favorites.Favorite{}, fmt.Errorf("lookup existing source mapping: %w", err)
	}

	var nextOrder int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(MAX(order_index) + 1, 0)
		 FROM published_channels
		 WHERE channel_class = ?`,
		channels.ChannelClassTraditional,
	).Scan(&nextOrder); err != nil {
		return favorites.Favorite{}, fmt.Errorf("query next channel order: %w", err)
	}

	now := time.Now().Unix()
	guideNumber := strconv.Itoa(startGuideNumber + nextOrder)
	channelKey := normalizeChannelKey(info.ChannelKey)
	associationType := "manual"
	if channelKey != "" {
		associationType = "channel_key"
	}

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
			created_at,
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, 1, NULL, '', ?, ?)
	`, channels.ChannelClassTraditional, channelKey, guideNumber, info.GuideName, nextOrder, now, now)
	if err != nil {
		return favorites.Favorite{}, fmt.Errorf("insert published channel: %w", err)
	}

	channelID, err := result.LastInsertId()
	if err != nil {
		return favorites.Favorite{}, fmt.Errorf("read inserted channel id: %w", err)
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
		return favorites.Favorite{}, fmt.Errorf("insert channel source: %w", err)
	}

	out := favorites.Favorite{
		FavID:       channelID,
		ItemKey:     itemKey,
		OrderIndex:  nextOrder,
		GuideNumber: guideNumber,
		GuideName:   info.GuideName,
		Enabled:     true,
		StreamURL:   info.StreamURL,
		TVGLogo:     info.TVGLogo,
		GroupName:   info.GroupName,
	}

	if err := tx.Commit(); err != nil {
		return favorites.Favorite{}, fmt.Errorf("commit add favorite: %w", err)
	}
	return out, nil
}

func (s *Store) RemoveFavorite(ctx context.Context, favID int64, startGuideNumber int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `DELETE FROM published_channels WHERE channel_id = ?`, favID)
	if err != nil {
		return fmt.Errorf("delete published channel: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return favorites.ErrNotFound
	}

	if err := renumberChannelsTx(ctx, tx, startGuideNumber); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit remove favorite: %w", err)
	}
	return nil
}

func (s *Store) ListFavorites(ctx context.Context, enabledOnly bool) ([]favorites.Favorite, error) {
	query := `
		SELECT
			pc.channel_id,
			COALESCE(cs.item_key, ''),
			pc.order_index,
			pc.guide_number,
			pc.guide_name,
			pc.enabled,
			COALESCE(p.stream_url, ''),
			COALESCE(p.tvg_logo, ''),
			COALESCE(p.group_name, '')
		FROM published_channels pc
		LEFT JOIN channel_sources cs ON cs.source_id = (
			SELECT cs2.source_id
			FROM channel_sources cs2
			WHERE cs2.channel_id = pc.channel_id AND cs2.enabled = 1
			ORDER BY cs2.priority_index ASC
			LIMIT 1
		)
		LEFT JOIN playlist_items p ON p.item_key = cs.item_key
		WHERE pc.channel_class = ?
	`
	args := []any{channels.ChannelClassTraditional}
	if enabledOnly {
		query += `AND pc.enabled = 1 `
	}
	query += `ORDER BY pc.order_index ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query favorites: %w", err)
	}
	defer rows.Close()

	out := make([]favorites.Favorite, 0)
	for rows.Next() {
		var (
			fav        favorites.Favorite
			enabledInt int
		)
		if err := rows.Scan(
			&fav.FavID,
			&fav.ItemKey,
			&fav.OrderIndex,
			&fav.GuideNumber,
			&fav.GuideName,
			&enabledInt,
			&fav.StreamURL,
			&fav.TVGLogo,
			&fav.GroupName,
		); err != nil {
			return nil, fmt.Errorf("scan favorite: %w", err)
		}
		fav.Enabled = enabledInt != 0
		out = append(out, fav)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate favorites: %w", err)
	}
	return out, nil
}

func (s *Store) ReorderFavorites(ctx context.Context, favIDs []int64, startGuideNumber int) error {
	seen := make(map[int64]struct{}, len(favIDs))
	for _, id := range favIDs {
		if id <= 0 {
			return fmt.Errorf("fav_ids must contain positive ids")
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("fav_ids contains duplicate id %d", id)
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
	if len(currentIDs) != len(favIDs) {
		return fmt.Errorf("fav_ids count mismatch: got %d, want %d", len(favIDs), len(currentIDs))
	}

	for _, id := range currentIDs {
		if _, ok := seen[id]; !ok {
			return fmt.Errorf("fav_ids missing id %d", id)
		}
	}

	now := time.Now().Unix()
	if err := applyChannelOrderTx(ctx, tx, favIDs, startGuideNumber, now); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reorder favorites: %w", err)
	}
	return nil
}

type playlistItemInfo struct {
	ChannelKey string
	GuideName  string
	StreamURL  string
	TVGLogo    string
	GroupName  string
}

func lookupItemInfoTx(ctx context.Context, tx *sql.Tx, itemKey string) (playlistItemInfo, error) {
	var info playlistItemInfo
	err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(channel_key, ''), name, stream_url, COALESCE(tvg_logo, ''), group_name FROM playlist_items WHERE item_key = ?`,
		itemKey,
	).Scan(&info.ChannelKey, &info.GuideName, &info.StreamURL, &info.TVGLogo, &info.GroupName)
	if err != nil {
		return playlistItemInfo{}, err
	}
	return info, nil
}

func getFavoriteByItemKeyTx(ctx context.Context, tx *sql.Tx, itemKey string) (favorites.Favorite, error) {
	var (
		fav        favorites.Favorite
		enabledInt int
	)
	err := tx.QueryRowContext(
		ctx,
		`SELECT
			pc.channel_id,
			cs.item_key,
			pc.order_index,
			pc.guide_number,
			pc.guide_name,
			pc.enabled,
			COALESCE(p.stream_url, ''),
			COALESCE(p.tvg_logo, ''),
			COALESCE(p.group_name, '')
		 FROM channel_sources cs
		 JOIN published_channels pc ON pc.channel_id = cs.channel_id
		 LEFT JOIN playlist_items p ON p.item_key = cs.item_key
		 WHERE cs.item_key = ? AND pc.channel_class = ?
		 ORDER BY cs.priority_index ASC
		 LIMIT 1`,
		itemKey,
		channels.ChannelClassTraditional,
	).Scan(
		&fav.FavID,
		&fav.ItemKey,
		&fav.OrderIndex,
		&fav.GuideNumber,
		&fav.GuideName,
		&enabledInt,
		&fav.StreamURL,
		&fav.TVGLogo,
		&fav.GroupName,
	)
	if err != nil {
		return favorites.Favorite{}, err
	}
	fav.Enabled = enabledInt != 0
	return fav, nil
}

func listChannelIDsTx(ctx context.Context, tx *sql.Tx) ([]int64, error) {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT channel_id
		 FROM published_channels
		 WHERE channel_class = ?
		 ORDER BY order_index ASC`,
		channels.ChannelClassTraditional,
	)
	if err != nil {
		return nil, fmt.Errorf("query channel ids: %w", err)
	}
	defer rows.Close()

	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan channel id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel ids: %w", err)
	}
	return ids, nil
}

func renumberChannelsTx(ctx context.Context, tx *sql.Tx, startGuideNumber int) error {
	ids, err := listChannelIDsTx(ctx, tx)
	if err != nil {
		return err
	}

	return applyChannelOrderTx(ctx, tx, ids, startGuideNumber, time.Now().Unix())
}

func applyChannelOrderTx(
	ctx context.Context,
	tx *sql.Tx,
	orderedIDs []int64,
	startGuideNumber int,
	updatedAt int64,
) error {
	tempStmt, err := tx.PrepareContext(ctx, `UPDATE published_channels SET order_index = ?, guide_number = ?, updated_at = ? WHERE channel_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare temp reorder statement: %w", err)
	}
	defer tempStmt.Close()

	for i, id := range orderedIDs {
		tempOrder := -(i + 1)
		tempGuide := fmt.Sprintf("__tmp__%d__%d", updatedAt, id)
		if _, err := tempStmt.ExecContext(ctx, tempOrder, tempGuide, updatedAt, id); err != nil {
			return fmt.Errorf("set temporary order for channel %d: %w", id, err)
		}
	}

	finalStmt, err := tx.PrepareContext(ctx, `UPDATE published_channels SET order_index = ?, guide_number = ?, updated_at = ? WHERE channel_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare final reorder statement: %w", err)
	}
	defer finalStmt.Close()

	for i, id := range orderedIDs {
		guideNumber := strconv.Itoa(startGuideNumber + i)
		if _, err := finalStmt.ExecContext(ctx, i, guideNumber, updatedAt, id); err != nil {
			return fmt.Errorf("set final order for channel %d: %w", id, err)
		}
	}

	return nil
}
