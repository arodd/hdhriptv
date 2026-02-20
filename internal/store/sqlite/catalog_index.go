package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/arodd/hdhriptv/internal/channels"
)

const (
	playlistActiveGroupNameOrderIndex = "idx_playlist_active_group_name_name_item"
	playlistActiveNameOrderIndex      = "idx_playlist_active_name_item"
)

type catalogFilterQuerySpec struct {
	GroupNames  []string
	SearchQuery string
	SearchRegex bool
	WhereClause string
	Args        []any
	OrderIndex  string
}

type catalogFilterItemKeyRow struct {
	ItemKey string
	Name    string
}

type catalogFilterQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ListActiveItemKeysByChannelKey returns active playlist item keys for one channel_key.
func (s *Store) ListActiveItemKeysByChannelKey(ctx context.Context, channelKey string) ([]string, error) {
	channelKey = normalizeChannelKey(channelKey)
	if channelKey == "" {
		return []string{}, nil
	}

	rows, err := s.db.QueryContext(
		ctx,
		`SELECT item_key
		 FROM playlist_items
		 WHERE active = 1 AND channel_key = ?
		 ORDER BY name ASC, item_key ASC`,
		channelKey,
	)
	if err != nil {
		return nil, fmt.Errorf("query active item keys for channel_key %q: %w", channelKey, err)
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var itemKey string
		if err := rows.Scan(&itemKey); err != nil {
			return nil, fmt.Errorf("scan active item key row: %w", err)
		}
		out = append(out, itemKey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active item key rows: %w", err)
	}

	return out, nil
}

// ListActiveItemKeysByCatalogFilter returns active playlist item keys matching
// catalog group+search filters.
// Matching semantics intentionally mirror the /api/items query behavior.
func (s *Store) ListActiveItemKeysByCatalogFilter(ctx context.Context, groupNames []string, searchQuery string, searchRegex bool) ([]string, error) {
	spec, err := buildCatalogFilterQuerySpecWithLimits(groupNames, searchQuery, searchRegex, s.catalogSearchLimits)
	if err != nil {
		return nil, err
	}
	query := `SELECT item_key
		FROM playlist_items INDEXED BY ` + spec.OrderIndex + `
		WHERE ` + spec.WhereClause + `
		ORDER BY name ASC, item_key ASC`
	rows, err := s.db.QueryContext(ctx, query, spec.Args...)
	if err != nil {
		return nil, fmt.Errorf("query active item keys by catalog filter groups=%v search=%q search_regex=%t: %w", spec.GroupNames, spec.SearchQuery, spec.SearchRegex, err)
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var itemKey string
		if err := rows.Scan(&itemKey); err != nil {
			return nil, fmt.Errorf("scan active item key row: %w", err)
		}
		out = append(out, itemKey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active item key rows: %w", err)
	}

	return out, nil
}

func buildCatalogFilterQuerySpec(groupNames []string, searchQuery string, searchRegex bool) (catalogFilterQuerySpec, error) {
	return buildCatalogFilterQuerySpecWithLimits(groupNames, searchQuery, searchRegex, defaultCatalogSearchLimits())
}

func buildCatalogFilterQuerySpecWithLimits(groupNames []string, searchQuery string, searchRegex bool, limits CatalogSearchLimits) (catalogFilterQuerySpec, error) {
	spec := catalogFilterQuerySpec{
		GroupNames:  channels.NormalizeGroupNames("", groupNames),
		SearchRegex: searchRegex,
		Args:        make([]any, 0, len(groupNames)+4),
		OrderIndex:  playlistActiveNameOrderIndex,
	}

	clauses := []string{"active = 1"}
	if len(spec.GroupNames) == 1 {
		clauses = append(clauses, "group_name = ?")
		spec.Args = append(spec.Args, spec.GroupNames[0])
		spec.OrderIndex = playlistActiveGroupNameOrderIndex
	} else if len(spec.GroupNames) > 1 {
		clauses = append(clauses, "group_name IN ("+inPlaceholders(len(spec.GroupNames))+")")
		for _, groupName := range spec.GroupNames {
			spec.Args = append(spec.Args, groupName)
		}
	}

	spec.SearchQuery = strings.TrimSpace(searchQuery)
	if spec.SearchQuery != "" {
		var err error
		clauses, spec.Args, _, err = appendCatalogSearchClausesWithLimits(clauses, spec.Args, spec.SearchQuery, spec.SearchRegex, limits)
		if err != nil {
			return catalogFilterQuerySpec{}, err
		}
	}

	spec.WhereClause = strings.Join(clauses, " AND ")
	return spec, nil
}

// IterateActiveItemKeysByCatalogFilter iterates active catalog item keys matching
// the provided group+search filter in deterministic (name,item_key) order.
func (s *Store) IterateActiveItemKeysByCatalogFilter(
	ctx context.Context,
	groupNames []string,
	searchQuery string,
	searchRegex bool,
	pageSize int,
	visit func(itemKey string) error,
) (int, error) {
	if visit == nil {
		return 0, nil
	}

	spec, err := buildCatalogFilterQuerySpecWithLimits(groupNames, searchQuery, searchRegex, s.catalogSearchLimits)
	if err != nil {
		return 0, err
	}

	pageSize = normalizeCatalogFilterPageSize(pageSize)
	afterName := ""
	afterItemKey := ""
	total := 0

	for {
		page, err := listActiveItemKeyRowsByCatalogFilterPage(ctx, s.db, spec, pageSize, afterName, afterItemKey)
		if err != nil {
			return total, err
		}
		if len(page) == 0 {
			break
		}
		for _, row := range page {
			if err := visit(row.ItemKey); err != nil {
				return total, err
			}
			total++
			afterName = row.Name
			afterItemKey = row.ItemKey
		}
		if len(page) < pageSize {
			break
		}
	}

	return total, nil
}

func normalizeCatalogFilterPageSize(pageSize int) int {
	if pageSize <= 0 {
		return 512
	}
	return pageSize
}

func listActiveItemKeyRowsByCatalogFilterPage(
	ctx context.Context,
	queryer catalogFilterQueryer,
	spec catalogFilterQuerySpec,
	pageSize int,
	afterName string,
	afterItemKey string,
) ([]catalogFilterItemKeyRow, error) {
	pageSize = normalizeCatalogFilterPageSize(pageSize)
	afterName = strings.TrimSpace(afterName)
	afterItemKey = strings.TrimSpace(afterItemKey)

	query := `SELECT item_key, name
		FROM playlist_items INDEXED BY ` + spec.OrderIndex + `
		WHERE ` + spec.WhereClause
	args := make([]any, 0, len(spec.Args)+4)
	args = append(args, spec.Args...)
	if afterName != "" || afterItemKey != "" {
		query += `
		  AND (name > ? OR (name = ? AND item_key > ?))`
		args = append(args, afterName, afterName, afterItemKey)
	}
	query += `
		ORDER BY name ASC, item_key ASC
		LIMIT ?`
	args = append(args, pageSize)

	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query active item key page by catalog filter groups=%v search=%q search_regex=%t: %w", spec.GroupNames, spec.SearchQuery, spec.SearchRegex, err)
	}
	defer rows.Close()

	out := make([]catalogFilterItemKeyRow, 0, pageSize)
	for rows.Next() {
		var row catalogFilterItemKeyRow
		if err := rows.Scan(&row.ItemKey, &row.Name); err != nil {
			return nil, fmt.Errorf("scan active item key page row: %w", err)
		}
		row.ItemKey = strings.TrimSpace(row.ItemKey)
		if row.ItemKey == "" {
			continue
		}
		row.Name = strings.TrimSpace(row.Name)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active item key page rows: %w", err)
	}
	return out, nil
}
