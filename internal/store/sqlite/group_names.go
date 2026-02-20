package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/arodd/hdhriptv/internal/channels"
)

const emptyGroupNamesJSON = "[]"

func parseGroupNamesJSON(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil
	}
	return values
}

func marshalGroupNamesJSON(groupNames []string) string {
	normalized := channels.NormalizeGroupNames("", groupNames)
	if len(normalized) == 0 {
		return emptyGroupNamesJSON
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return emptyGroupNamesJSON
	}
	return string(encoded)
}

func normalizeStoredGroupNames(groupName, groupNamesJSON string) (string, []string) {
	normalized := channels.NormalizeGroupNames(groupName, parseGroupNamesJSON(groupNamesJSON))
	return channels.GroupNameAlias(normalized), normalized
}

func (s *Store) normalizeDynamicQueryGroupNames(ctx context.Context) error {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT query_id, COALESCE(group_name, ''), COALESCE(group_names_json, '') FROM dynamic_channel_queries`,
	)
	if err != nil {
		return fmt.Errorf("query dynamic query group names: %w", err)
	}
	defer rows.Close()

	type row struct {
		queryID        int64
		groupName      string
		groupNamesJSON string
	}
	pending := make([]row, 0)
	for rows.Next() {
		var current row
		if err := rows.Scan(&current.queryID, &current.groupName, &current.groupNamesJSON); err != nil {
			return fmt.Errorf("scan dynamic query group names row: %w", err)
		}
		pending = append(pending, current)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate dynamic query group names rows: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin normalize dynamic query group names tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(
		ctx,
		`UPDATE dynamic_channel_queries
		 SET group_name = ?,
		     group_names_json = ?
		 WHERE query_id = ?`,
	)
	if err != nil {
		return fmt.Errorf("prepare normalize dynamic query group names statement: %w", err)
	}
	defer stmt.Close()

	for _, current := range pending {
		alias, normalized := normalizeStoredGroupNames(current.groupName, current.groupNamesJSON)
		normalizedJSON := marshalGroupNamesJSON(normalized)
		if strings.TrimSpace(current.groupName) == alias && strings.TrimSpace(current.groupNamesJSON) == normalizedJSON {
			continue
		}
		if _, err := stmt.ExecContext(ctx, alias, normalizedJSON, current.queryID); err != nil {
			return fmt.Errorf("normalize dynamic query %d groups: %w", current.queryID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit normalize dynamic query group names: %w", err)
	}
	return nil
}

func (s *Store) normalizePublishedChannelGroupNames(ctx context.Context) error {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT channel_id, COALESCE(dynamic_group_name, ''), COALESCE(dynamic_group_names_json, '') FROM published_channels`,
	)
	if err != nil {
		return fmt.Errorf("query published channel dynamic group names: %w", err)
	}
	defer rows.Close()

	type row struct {
		channelID      int64
		groupName      string
		groupNamesJSON string
	}
	pending := make([]row, 0)
	for rows.Next() {
		var current row
		if err := rows.Scan(&current.channelID, &current.groupName, &current.groupNamesJSON); err != nil {
			return fmt.Errorf("scan published channel dynamic group names row: %w", err)
		}
		pending = append(pending, current)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate published channel dynamic group names rows: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin normalize published channel dynamic group names tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(
		ctx,
		`UPDATE published_channels
		 SET dynamic_group_name = ?,
		     dynamic_group_names_json = ?
		 WHERE channel_id = ?`,
	)
	if err != nil {
		return fmt.Errorf("prepare normalize published channel dynamic group names statement: %w", err)
	}
	defer stmt.Close()

	for _, current := range pending {
		alias, normalized := normalizeStoredGroupNames(current.groupName, current.groupNamesJSON)
		normalizedJSON := marshalGroupNamesJSON(normalized)
		if strings.TrimSpace(current.groupName) == alias && strings.TrimSpace(current.groupNamesJSON) == normalizedJSON {
			continue
		}
		if _, err := stmt.ExecContext(ctx, alias, normalizedJSON, current.channelID); err != nil {
			return fmt.Errorf("normalize published channel %d dynamic groups: %w", current.channelID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit normalize published channel dynamic group names: %w", err)
	}
	return nil
}
