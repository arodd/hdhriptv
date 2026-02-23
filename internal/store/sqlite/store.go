package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"path"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/playlist"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

const (
	catalogSearchLikeClause        = "LOWER(name) LIKE ? ESCAPE '!'"
	catalogSearchNotLikeClause     = "LOWER(name) NOT LIKE ? ESCAPE '!'"
	maxCatalogSearchTerms          = 12
	maxCatalogSearchDisjuncts      = 6
	maxCatalogSearchTermRunes      = 64
	maxCatalogSearchTermsLimit     = 256
	maxCatalogSearchDisjunctsLimit = 128
	maxCatalogSearchTermRunesLimit = 256
)

type CatalogSearchLimits struct {
	MaxTerms     int
	MaxDisjuncts int
	MaxTermRunes int
}

type CatalogSearchWarning struct {
	Mode                    string `json:"mode"`
	Truncated               bool   `json:"truncated"`
	MaxTerms                int    `json:"max_terms"`
	MaxDisjuncts            int    `json:"max_disjuncts"`
	MaxTermRunes            int    `json:"max_term_runes"`
	TermsApplied            int    `json:"terms_applied"`
	TermsDropped            int    `json:"terms_dropped"`
	DisjunctsApplied        int    `json:"disjuncts_applied"`
	DisjunctsDropped        int    `json:"disjuncts_dropped"`
	TermRuneTruncationCount int    `json:"term_rune_truncations"`
}

type SQLiteOptions struct {
	CatalogSearchLimits CatalogSearchLimits
}

// CatalogSearchWarningForQuery returns token/regex parse warnings for a catalog search
// expression using the provided limits. It is useful for response metadata and
// logging without rebuilding store query SQL.
func CatalogSearchWarningForQuery(search string, searchRegex bool, limits CatalogSearchLimits) (CatalogSearchWarning, error) {
	_, _, warning, err := appendCatalogSearchClausesWithLimits(nil, nil, search, searchRegex, limits)
	return warning, err
}

func defaultCatalogSearchLimits() CatalogSearchLimits {
	return CatalogSearchLimits{
		MaxTerms:     maxCatalogSearchTerms,
		MaxDisjuncts: maxCatalogSearchDisjuncts,
		MaxTermRunes: maxCatalogSearchTermRunes,
	}
}

func normalizeCatalogSearchLimits(limits *CatalogSearchLimits) error {
	if limits == nil {
		return nil
	}

	if limits.MaxTerms < 0 {
		return fmt.Errorf("catalog search max terms must be zero or greater")
	}
	if limits.MaxDisjuncts < 0 {
		return fmt.Errorf("catalog search max disjuncts must be zero or greater")
	}
	if limits.MaxTermRunes < 0 {
		return fmt.Errorf("catalog search max term runes must be zero or greater")
	}
	if limits.MaxTerms > maxCatalogSearchTermsLimit {
		return fmt.Errorf("catalog search max terms cannot exceed %d", maxCatalogSearchTermsLimit)
	}
	if limits.MaxDisjuncts > maxCatalogSearchDisjunctsLimit {
		return fmt.Errorf("catalog search max disjuncts cannot exceed %d", maxCatalogSearchDisjunctsLimit)
	}
	if limits.MaxTermRunes > maxCatalogSearchTermRunesLimit {
		return fmt.Errorf("catalog search max term runes cannot exceed %d", maxCatalogSearchTermRunesLimit)
	}
	return nil
}

func effectiveCatalogSearchLimits(limits CatalogSearchLimits) CatalogSearchLimits {
	effective := defaultCatalogSearchLimits()
	if limits.MaxTerms > 0 {
		effective.MaxTerms = limits.MaxTerms
	}
	if limits.MaxDisjuncts > 0 {
		effective.MaxDisjuncts = limits.MaxDisjuncts
	}
	if limits.MaxTermRunes > 0 {
		effective.MaxTermRunes = limits.MaxTermRunes
	}
	return effective
}

const upsertItemSQL = `
INSERT INTO playlist_items (
  item_key,
  channel_key,
  name,
  group_name,
  stream_url,
  tvg_id,
  tvg_name,
  tvg_logo,
  attrs_json,
  first_seen_at,
  last_seen_at,
  active
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
ON CONFLICT(item_key) DO UPDATE SET
  channel_key=excluded.channel_key,
  name=excluded.name,
  group_name=excluded.group_name,
  stream_url=excluded.stream_url,
  tvg_id=excluded.tvg_id,
  tvg_name=excluded.tvg_name,
  tvg_logo=excluded.tvg_logo,
  attrs_json=excluded.attrs_json,
  last_seen_at=excluded.last_seen_at,
  active=1;
`

// Store provides SQLite-backed catalog persistence.
type Store struct {
	db                  *sql.DB
	dbPath              string
	dbIOERRTrace        *sqliteIOERRTraceRing
	catalogSearchLimits CatalogSearchLimits
}

func Open(dbPath string) (*Store, error) {
	return OpenWithOptions(dbPath, SQLiteOptions{})
}

func OpenWithOptions(dbPath string, options SQLiteOptions) (*Store, error) {
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		return nil, fmt.Errorf("db path is required")
	}

	limits := defaultCatalogSearchLimits()
	if err := normalizeCatalogSearchLimits(&options.CatalogSearchLimits); err != nil {
		return nil, err
	}
	if options.CatalogSearchLimits.MaxTerms != 0 {
		limits.MaxTerms = options.CatalogSearchLimits.MaxTerms
	}
	if options.CatalogSearchLimits.MaxDisjuncts != 0 {
		limits.MaxDisjuncts = options.CatalogSearchLimits.MaxDisjuncts
	}
	if options.CatalogSearchLimits.MaxTermRunes != 0 {
		limits.MaxTermRunes = options.CatalogSearchLimits.MaxTermRunes
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	runtimePragmasStart := time.Now()
	if err := applyRuntimePragmas(context.Background(), db); err != nil {
		_ = db.Close()
		return nil, err
	}
	slog.Info(
		"startup phase complete",
		"phase", "sqlite_runtime_pragmas",
		"duration", time.Since(runtimePragmasStart),
	)

	traceCfg := loadSQLiteIOERRTraceConfigFromEnv()
	s := &Store{
		db:                  db,
		dbPath:              dbPath,
		dbIOERRTrace:        newSQLiteIOERRTraceRing(traceCfg),
		catalogSearchLimits: limits,
	}
	if s.dbIOERRTrace != nil {
		slog.Info(
			"sqlite ioerr trace ring enabled",
			"ring_size", traceCfg.RingSize,
			"dump_limit", traceCfg.DumpLimit,
			"dump_interval", traceCfg.DumpInterval.String(),
		)
	}
	migrateStart := time.Now()
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	slog.Info(
		"startup phase complete",
		"phase", "sqlite_migrate",
		"duration", time.Since(migrateStart),
	)
	return s, nil
}

// CatalogSearchWarningForQuery returns catalog-search truncation metadata for a query
// using the store's effective limits.
func (s *Store) CatalogSearchWarningForQuery(search string, searchRegex bool) (CatalogSearchWarning, error) {
	if s == nil {
		return CatalogSearchWarningForQuery(search, searchRegex, CatalogSearchLimits{})
	}
	return CatalogSearchWarningForQuery(search, searchRegex, s.catalogSearchLimits)
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) UpsertPlaylistItems(ctx context.Context, items []playlist.Item) error {
	_, err := s.upsertPlaylistItemsStream(ctx, len(items), func(yield func(playlist.Item) error) error {
		for _, item := range items {
			if err := yield(item); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// UpsertPlaylistItemsStream upserts playlist items from an incremental stream in one transaction.
func (s *Store) UpsertPlaylistItemsStream(ctx context.Context, stream playlist.ItemStream) (int, error) {
	return s.upsertPlaylistItemsStream(ctx, 0, stream)
}

func (s *Store) upsertPlaylistItemsStream(ctx context.Context, itemTotal int, stream playlist.ItemStream) (int, error) {
	if stream == nil {
		return 0, fmt.Errorf("playlist item stream is required")
	}

	beginTrace := s.beginSQLiteIOERRTrace("playlist_sync_write", "begin_tx")
	tx, err := s.db.BeginTx(ctx, nil)
	s.endSQLiteIOERRTrace(beginTrace, err)
	if err != nil {
		return 0, fmt.Errorf(
			"playlist write failed at begin_tx: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "begin_tx",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	defer tx.Rollback()

	prepareTrace := s.beginSQLiteIOERRTrace("playlist_sync_write", "prepare_upsert")
	stmt, err := tx.PrepareContext(ctx, upsertItemSQL)
	s.endSQLiteIOERRTrace(prepareTrace, err)
	if err != nil {
		return 0, fmt.Errorf(
			"playlist write failed at prepare_upsert: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "prepare_upsert",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	defer stmt.Close()

	refreshMark := time.Now().UnixNano()
	itemCount := 0
	if err := stream(func(item playlist.Item) error {
		itemCount++
		return s.execUpsertPlaylistItem(
			ctx,
			stmt,
			refreshMark,
			item,
			itemCount,
			itemTotal,
		)
	}); err != nil {
		return 0, fmt.Errorf("playlist stream failed after %d items: %w", itemCount, err)
	}

	markInactiveTrace := s.beginSQLiteIOERRTrace("playlist_sync_write", "mark_inactive")
	if _, err := tx.ExecContext(ctx, `UPDATE playlist_items SET active = 0 WHERE last_seen_at <> ? AND active <> 0`, refreshMark); err != nil {
		s.endSQLiteIOERRTrace(markInactiveTrace, err)
		return 0, fmt.Errorf(
			"playlist write failed at mark_inactive: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "mark_inactive",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	s.endSQLiteIOERRTrace(markInactiveTrace, nil)

	commitTrace := s.beginSQLiteIOERRTrace("playlist_sync_write", "commit")
	if err := tx.Commit(); err != nil {
		s.endSQLiteIOERRTrace(commitTrace, err)
		return 0, fmt.Errorf(
			"playlist write failed at commit: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "commit",
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	s.endSQLiteIOERRTrace(commitTrace, nil)
	return itemCount, nil
}

func (s *Store) execUpsertPlaylistItem(
	ctx context.Context,
	stmt *sql.Stmt,
	refreshMark int64,
	item playlist.Item,
	itemIndex int,
	itemTotal int,
) error {
	attrsJSON, err := json.Marshal(item.Attrs)
	if err != nil {
		return fmt.Errorf("marshal attrs for %q: %w", item.ItemKey, err)
	}
	if len(attrsJSON) == 0 {
		attrsJSON = []byte("{}")
	}

	upsertTrace := s.beginSQLiteIOERRTrace("playlist_sync_write", "upsert_item")
	if _, err := stmt.ExecContext(
		ctx,
		strings.TrimSpace(item.ItemKey),
		normalizeChannelKey(item.ChannelKey),
		strings.TrimSpace(item.Name),
		strings.TrimSpace(item.Group),
		strings.TrimSpace(item.StreamURL),
		strings.TrimSpace(item.TVGID),
		tvgNameFromAttrs(item.Attrs),
		strings.TrimSpace(item.TVGLogo),
		string(attrsJSON),
		refreshMark,
		refreshMark,
	); err != nil {
		s.endSQLiteIOERRTrace(upsertTrace, err)
		return fmt.Errorf(
			"playlist write failed at upsert_item: %w%s",
			err,
			sqliteDiagSuffix(ctx, s.db, err, sqliteDiagOptions{
				Phase:     "upsert_item",
				ItemIndex: itemIndex,
				ItemTotal: itemTotal,
				ItemKey:   item.ItemKey,
				DBPath:    s.dbPath,
				TraceRing: s.dbIOERRTrace,
			}),
		)
	}
	s.endSQLiteIOERRTrace(upsertTrace, nil)
	return nil
}

func (s *Store) GetGroups(ctx context.Context) ([]playlist.Group, error) {
	groups, _, err := s.ListGroupsPaged(ctx, 0, 0, true)
	if err != nil {
		return nil, err
	}
	return groups, nil
}

func (s *Store) ListGroupsPaged(ctx context.Context, limit, offset int, includeCounts bool) ([]playlist.Group, int, error) {
	if limit < 0 {
		return nil, 0, fmt.Errorf("limit must be zero or greater")
	}
	if offset < 0 {
		return nil, 0, fmt.Errorf("offset must be zero or greater")
	}

	var total int
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM (SELECT group_name FROM playlist_items WHERE active = 1 GROUP BY group_name)`,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count groups: %w", err)
	}

	baseQuery := `
		SELECT group_name
		FROM playlist_items
		WHERE active = 1
		GROUP BY group_name
	`
	if includeCounts {
		baseQuery = `
			SELECT group_name, COUNT(*)
			FROM playlist_items
			WHERE active = 1
			GROUP BY group_name
		`
	}
	baseQuery += `
		ORDER BY group_name ASC
	`
	args := make([]any, 0, 2)
	baseQuery, args = applyLimitOffset(baseQuery, args, limit, offset)

	rows, err := s.db.QueryContext(ctx, baseQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query groups: %w", err)
	}
	defer rows.Close()

	groups := make([]playlist.Group, 0, expectedPageCapacity(total, limit, offset))
	for rows.Next() {
		var group playlist.Group
		if includeCounts {
			if err := rows.Scan(&group.Name, &group.Count); err != nil {
				return nil, 0, fmt.Errorf("scan group with count: %w", err)
			}
		} else {
			if err := rows.Scan(&group.Name); err != nil {
				return nil, 0, fmt.Errorf("scan group name: %w", err)
			}
		}
		groups = append(groups, group)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate groups: %w", err)
	}
	return groups, total, nil
}

func (s *Store) ListItems(ctx context.Context, q playlist.Query) ([]playlist.Item, int, error) {
	q = normalizeCatalogQuery(q)

	where, args, err := buildWhereWithLimits(q, s.catalogSearchLimits)
	if err != nil {
		return nil, 0, err
	}
	total, err := s.countItems(ctx, where, args)
	if err != nil {
		return nil, 0, err
	}

	listSQL := `
		SELECT item_key, channel_key, name, group_name, stream_url, tvg_id, tvg_logo,
		       COALESCE(first_seen_at, 0), COALESCE(last_seen_at, 0), active
		FROM playlist_items
	` + where + `
		ORDER BY group_name ASC, name ASC, item_key ASC
		LIMIT ? OFFSET ?
	`
	listArgs := buildListArgs(args, q)

	rows, err := s.db.QueryContext(ctx, listSQL, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query items: %w", err)
	}
	defer rows.Close()

	items := make([]playlist.Item, 0, expectedListCapacity(total, q))
	for rows.Next() {
		var (
			item      playlist.Item
			activeInt int
		)
		if err := rows.Scan(
			&item.ItemKey,
			&item.ChannelKey,
			&item.Name,
			&item.Group,
			&item.StreamURL,
			&item.TVGID,
			&item.TVGLogo,
			&item.FirstSeenAt,
			&item.LastSeenAt,
			&activeInt,
		); err != nil {
			return nil, 0, fmt.Errorf("scan item: %w", err)
		}
		item.Active = activeInt != 0
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate items: %w", err)
	}

	return items, total, nil
}

// ListCatalogItems returns only the catalog list fields needed by /api/items.
func (s *Store) ListCatalogItems(ctx context.Context, q playlist.Query) ([]playlist.Item, int, error) {
	q = normalizeCatalogQuery(q)

	where, args, err := buildWhereWithLimits(q, s.catalogSearchLimits)
	if err != nil {
		return nil, 0, err
	}
	total, err := s.countItems(ctx, where, args)
	if err != nil {
		return nil, 0, err
	}

	listSQL := `
		SELECT item_key, channel_key, name, group_name, stream_url, tvg_logo, active
		FROM playlist_items
	` + where + `
		ORDER BY group_name ASC, name ASC, item_key ASC
		LIMIT ? OFFSET ?
	`
	listArgs := buildListArgs(args, q)

	rows, err := s.db.QueryContext(ctx, listSQL, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("query catalog items: %w", err)
	}
	defer rows.Close()

	items := make([]playlist.Item, 0, expectedListCapacity(total, q))
	for rows.Next() {
		var (
			item      playlist.Item
			activeInt int
		)
		if err := rows.Scan(
			&item.ItemKey,
			&item.ChannelKey,
			&item.Name,
			&item.Group,
			&item.StreamURL,
			&item.TVGLogo,
			&activeInt,
		); err != nil {
			return nil, 0, fmt.Errorf("scan catalog item: %w", err)
		}
		item.Active = activeInt != 0
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate catalog items: %w", err)
	}

	return items, total, nil
}

func normalizeCatalogQuery(q playlist.Query) playlist.Query {
	if q.Limit <= 0 {
		q.Limit = 100
	}
	if q.Offset < 0 {
		q.Offset = 0
	}
	q.GroupNames = channels.NormalizeGroupNames(q.Group, q.GroupNames)
	q.Group = channels.GroupNameAlias(q.GroupNames)
	return q
}

func buildListArgs(args []any, q playlist.Query) []any {
	listArgs := make([]any, len(args)+2)
	copy(listArgs, args)
	listArgs[len(args)] = q.Limit
	listArgs[len(args)+1] = q.Offset
	return listArgs
}

func expectedListCapacity(total int, q playlist.Query) int {
	if total <= q.Offset {
		return 0
	}

	remaining := total - q.Offset
	if remaining < q.Limit {
		return remaining
	}
	return q.Limit
}

func (s *Store) countItems(ctx context.Context, where string, args []any) (int, error) {
	countSQL := `SELECT COUNT(*) FROM playlist_items ` + where
	var total int
	if err := s.db.QueryRowContext(ctx, countSQL, args...).Scan(&total); err != nil {
		return 0, fmt.Errorf("count items: %w", err)
	}
	return total, nil
}

func buildWhere(q playlist.Query) (string, []any, error) {
	return buildWhereWithLimits(q, defaultCatalogSearchLimits())
}

func buildWhereWithLimits(q playlist.Query, limits CatalogSearchLimits) (string, []any, error) {
	limits = effectiveCatalogSearchLimits(limits)
	groupNames := channels.NormalizeGroupNames(q.Group, q.GroupNames)
	clauses := []string{"active = 1"}
	args := make([]any, 0, len(groupNames)+2)

	switch len(groupNames) {
	case 1:
		clauses = append(clauses, "group_name = ?")
		args = append(args, groupNames[0])
	case 2:
		clauses = append(clauses, "group_name IN (?, ?)")
		args = append(args, groupNames[0], groupNames[1])
	default:
		if len(groupNames) > 2 {
			clauses = append(clauses, "group_name IN ("+inPlaceholders(len(groupNames))+")")
			for _, groupName := range groupNames {
				args = append(args, groupName)
			}
		}
	}
	if search := strings.TrimSpace(q.Search); search != "" {
		var err error
		clauses, args, _, err = appendCatalogSearchClausesWithLimits(clauses, args, search, q.SearchRegex, limits)
		if err != nil {
			return "", nil, err
		}
	}

	return "WHERE " + strings.Join(clauses, " AND "), args, nil
}

func appendCatalogSearchClauses(clauses []string, args []any, search string, searchRegex bool) ([]string, []any, error) {
	clauses, args, _, err := appendCatalogSearchClausesWithLimits(clauses, args, search, searchRegex, defaultCatalogSearchLimits())
	if err != nil {
		return nil, nil, err
	}
	return clauses, args, nil
}

func appendCatalogSearchClausesWithLimits(clauses []string, args []any, search string, searchRegex bool, limits CatalogSearchLimits) ([]string, []any, CatalogSearchWarning, error) {
	limits = effectiveCatalogSearchLimits(limits)
	warning := CatalogSearchWarning{
		Mode:         "token",
		MaxTerms:     limits.MaxTerms,
		MaxDisjuncts: limits.MaxDisjuncts,
		MaxTermRunes: limits.MaxTermRunes,
	}

	if searchRegex {
		pattern, err := buildCatalogRegexPattern(search)
		if err != nil {
			return nil, nil, warning, err
		}
		if pattern == "" {
			warning.Mode = "regex"
			return clauses, args, warning, nil
		}
		warning.Mode = "regex"
		clauses = append(clauses, catalogSearchRegexClause)
		args = append(args, pattern)
		return clauses, args, warning, nil
	}

	expr, parseWarning := parseCatalogSearchExprWithWarning(search, limits)
	clauses, args = appendCatalogSearchExprClauses(clauses, args, expr)
	warning.DisjunctsApplied = parseWarning.DisjunctsApplied
	warning.DisjunctsDropped = parseWarning.DisjunctsDropped
	warning.TermsApplied = parseWarning.TermsApplied
	warning.TermsDropped = parseWarning.TermsDropped
	warning.TermRuneTruncationCount = parseWarning.TermRuneTruncationCount
	if parseWarning.Truncated {
		warning.Truncated = true
	}
	return clauses, args, warning, nil
}

type catalogSearchSpec struct {
	Includes []string
	Excludes []string
}

type catalogSearchExpr struct {
	Disjuncts []catalogSearchSpec
}

func appendCatalogSearchExprClauses(clauses []string, args []any, expr catalogSearchExpr) ([]string, []any) {
	switch len(expr.Disjuncts) {
	case 0:
		return clauses, args
	case 1:
		disjunctClause, disjunctArgs := buildCatalogSearchDisjunctClause(expr.Disjuncts[0])
		if disjunctClause == "" {
			return clauses, args
		}
		clauses = append(clauses, disjunctClause)
		args = append(args, disjunctArgs...)
		return clauses, args
	}

	disjunctClauses := make([]string, 0, len(expr.Disjuncts))
	for _, disjunct := range expr.Disjuncts {
		disjunctClause, disjunctArgs := buildCatalogSearchDisjunctClause(disjunct)
		if disjunctClause == "" {
			continue
		}
		disjunctClauses = append(disjunctClauses, "("+disjunctClause+")")
		args = append(args, disjunctArgs...)
	}
	if len(disjunctClauses) == 0 {
		return clauses, args
	}

	clauses = append(clauses, "("+strings.Join(disjunctClauses, " OR ")+")")
	return clauses, args
}

func buildCatalogSearchDisjunctClause(spec catalogSearchSpec) (string, []any) {
	disjunctClauses := make([]string, 0, len(spec.Includes)+len(spec.Excludes))
	disjunctArgs := make([]any, 0, len(spec.Includes)+len(spec.Excludes))
	for _, term := range spec.Includes {
		disjunctClauses = append(disjunctClauses, catalogSearchLikeClause)
		disjunctArgs = append(disjunctArgs, sqlLikeContainsPattern(term))
	}
	for _, term := range spec.Excludes {
		disjunctClauses = append(disjunctClauses, catalogSearchNotLikeClause)
		disjunctArgs = append(disjunctArgs, sqlLikeContainsPattern(term))
	}
	if len(disjunctClauses) == 0 {
		return "", nil
	}
	return strings.Join(disjunctClauses, " AND "), disjunctArgs
}

func catalogSearchLikeTerms(raw string) []string {
	return parseCatalogSearchSpec(raw).Includes
}

func catalogSearchExcludeTerms(raw string) []string {
	return parseCatalogSearchSpec(raw).Excludes
}

func parseCatalogSearchExpr(raw string) catalogSearchExpr {
	expr, _ := parseCatalogSearchExprWithWarning(raw, defaultCatalogSearchLimits())
	return expr
}

func parseCatalogSearchExprWithWarning(raw string, limits CatalogSearchLimits) (catalogSearchExpr, CatalogSearchWarning) {
	limits = effectiveCatalogSearchLimits(limits)
	warning := CatalogSearchWarning{
		Mode:         "token",
		MaxTerms:     limits.MaxTerms,
		MaxDisjuncts: limits.MaxDisjuncts,
		MaxTermRunes: limits.MaxTermRunes,
	}

	search := strings.TrimSpace(raw)
	if search == "" {
		return catalogSearchExpr{}, warning
	}

	rawDisjuncts := splitCatalogSearchDisjunctClauses(search)
	if len(rawDisjuncts) == 0 {
		spec, termsApplied, termsDropped, runeTrunc := parseCatalogSearchSpecWithLimitAndWarning(search, limits.MaxTerms, limits.MaxTermRunes)
		if len(spec.Includes) == 0 && len(spec.Excludes) == 0 {
			return catalogSearchExpr{}, warning
		}

		warning.DisjunctsApplied = 1
		warning.TermsApplied = termsApplied
		warning.TermsDropped = termsDropped
		warning.TermRuneTruncationCount = runeTrunc
		if termsDropped > 0 || runeTrunc > 0 {
			warning.Truncated = true
		}
		return catalogSearchExpr{Disjuncts: []catalogSearchSpec{spec}}, warning
	}

	expr := catalogSearchExpr{
		Disjuncts: make([]catalogSearchSpec, 0, len(rawDisjuncts)),
	}
	remainingTerms := limits.MaxTerms
	for disjunctIndex, disjunctRaw := range rawDisjuncts {
		if len(expr.Disjuncts) >= limits.MaxDisjuncts {
			for _, droppedRaw := range rawDisjuncts[disjunctIndex:] {
				_, droppedTerms, droppedTrunc := parseCatalogSearchSpecAllTerms(droppedRaw, limits.MaxTermRunes)
				warning.DisjunctsDropped++
				warning.TermsDropped += droppedTerms
				warning.TermRuneTruncationCount += droppedTrunc
			}
			break
		}
		if remainingTerms <= 0 {
			for _, droppedRaw := range rawDisjuncts[disjunctIndex:] {
				_, droppedTerms, droppedTrunc := parseCatalogSearchSpecAllTerms(droppedRaw, limits.MaxTermRunes)
				warning.DisjunctsDropped++
				warning.TermsDropped += droppedTerms
				warning.TermRuneTruncationCount += droppedTrunc
			}
			break
		}

		spec, termsApplied, termsDropped, runeTrunc := parseCatalogSearchSpecWithLimitAndWarning(disjunctRaw, remainingTerms, limits.MaxTermRunes)
		if len(spec.Includes) == 0 && len(spec.Excludes) == 0 {
			continue
		}
		expr.Disjuncts = append(expr.Disjuncts, spec)
		warning.DisjunctsApplied++
		warning.TermsApplied += termsApplied
		warning.TermsDropped += termsDropped
		warning.TermRuneTruncationCount += runeTrunc
		remainingTerms -= termsApplied
	}
	if len(expr.Disjuncts) == 0 {
		if warning.TermsDropped > 0 || warning.DisjunctsDropped > 0 {
			warning.Truncated = true
		}
		return catalogSearchExpr{}, warning
	}
	if warning.TermsDropped > 0 || warning.DisjunctsDropped > 0 {
		warning.Truncated = true
	}
	return expr, warning
}

func splitCatalogSearchDisjunctClauses(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}

	clauses := make([]string, 0, 2)
	clauseTokens := make([]string, 0, 4)

	var token strings.Builder
	token.Grow(len(raw))

	flushClause := func() {
		if len(clauseTokens) == 0 {
			return
		}
		clauses = append(clauses, strings.Join(clauseTokens, " "))
		clauseTokens = clauseTokens[:0]
	}

	flushToken := func() {
		if token.Len() == 0 {
			return
		}
		part := strings.TrimSpace(token.String())
		token.Reset()
		if part == "" {
			return
		}
		if strings.EqualFold(part, "or") {
			flushClause()
			return
		}
		clauseTokens = append(clauseTokens, part)
	}

	for _, r := range raw {
		switch {
		case r == '|':
			flushToken()
			flushClause()
		case unicode.IsSpace(r):
			flushToken()
		default:
			token.WriteRune(r)
		}
	}
	flushToken()
	flushClause()

	return clauses
}

func parseCatalogSearchSpec(raw string) catalogSearchSpec {
	spec, _ := parseCatalogSearchSpecWithLimit(raw, maxCatalogSearchTerms)
	return spec
}

func parseCatalogSearchSpecWithLimit(raw string, maxTerms int) (catalogSearchSpec, int) {
	spec, termsApplied, termsDropped, _ := parseCatalogSearchSpecWithLimitAndWarning(raw, maxTerms, maxCatalogSearchTermRunes)
	_ = termsDropped
	return spec, termsApplied
}

func parseCatalogSearchSpecWithLimitAndWarning(raw string, maxTerms int, maxTermRunes int) (catalogSearchSpec, int, int, int) {
	terms, termRuneTrunc := parseCatalogSearchTermSequence(raw, maxTermRunes)
	if maxTerms <= 0 || len(terms) == 0 {
		return catalogSearchSpec{}, 0, 0, termRuneTrunc
	}

	termsApplied := len(terms)
	if termsApplied > maxTerms {
		termsApplied = maxTerms
	}
	termsDropped := len(terms) - termsApplied

	spec := catalogSearchSpec{
		Includes: make([]string, 0, termsApplied),
		Excludes: make([]string, 0, termsApplied),
	}
	for _, term := range terms[:termsApplied] {
		if term.exclude {
			spec.Excludes = append(spec.Excludes, term.value)
		} else {
			spec.Includes = append(spec.Includes, term.value)
		}
	}

	return spec, termsApplied, termsDropped, termRuneTrunc
}

func parseCatalogSearchSpecAllTerms(raw string, maxTermRunes int) (catalogSearchSpec, int, int) {
	terms, termRuneTrunc := parseCatalogSearchTermSequence(raw, maxTermRunes)
	if len(terms) == 0 {
		return catalogSearchSpec{}, 0, termRuneTrunc
	}

	spec := catalogSearchSpec{
		Includes: make([]string, 0, len(terms)),
		Excludes: make([]string, 0, len(terms)),
	}
	for _, term := range terms {
		if term.exclude {
			spec.Excludes = append(spec.Excludes, term.value)
		} else {
			spec.Includes = append(spec.Includes, term.value)
		}
	}
	return spec, len(terms), termRuneTrunc
}

type catalogSearchTerm struct {
	value   string
	exclude bool
}

func parseCatalogSearchTermSequence(raw string, maxTermRunes int) ([]catalogSearchTerm, int) {
	search := strings.TrimSpace(raw)
	if search == "" || maxTermRunes <= 0 {
		return nil, 0
	}

	if maxTermRunes <= 0 {
		return nil, 0
	}

	seenIncludes := make(map[string]struct{}, 4)
	seenExcludes := make(map[string]struct{}, 4)
	terms := make([]catalogSearchTerm, 0, 8)
	runeTruncations := 0

	for _, token := range strings.Fields(search) {
		exclude := false
		switch {
		case strings.HasPrefix(token, "-"), strings.HasPrefix(token, "!"):
			exclude = true
			token = strings.TrimSpace(token[1:])
		}

		parts, partsTruncations := catalogSearchTokenPartsWithLimitsAndTruncation(token, maxTermRunes)
		runeTruncations += partsTruncations
		for _, part := range parts {
			if exclude {
				if _, ok := seenExcludes[part]; ok {
					continue
				}
				seenExcludes[part] = struct{}{}
				terms = append(terms, catalogSearchTerm{value: part, exclude: true})
			} else {
				if _, ok := seenIncludes[part]; ok {
					continue
				}
				seenIncludes[part] = struct{}{}
				terms = append(terms, catalogSearchTerm{value: part})
			}
		}
	}

	if len(terms) > 0 {
		return terms, runeTruncations
	}

	// Preserve literal wildcard semantics for punctuation-only queries while
	// still bounding bind-arg growth.
	fallbackExclude := false
	fallback := search
	if len(fallback) > 1 {
		switch fallback[0] {
		case '-', '!':
			fallbackExclude = true
			fallback = strings.TrimSpace(fallback[1:])
		}
	}
	fallback, fallbackTruncated := trimToMaxRunesWithTruncation(strings.ToLower(strings.TrimSpace(fallback)), maxTermRunes)
	if fallback == "" {
		return nil, 0
	}
	if fallbackExclude {
		if fallbackTruncated {
			runeTruncations++
		}
		return []catalogSearchTerm{{value: fallback, exclude: true}}, runeTruncations
	}
	if fallbackTruncated {
		runeTruncations++
	}
	return []catalogSearchTerm{{value: fallback}}, runeTruncations
}

func catalogSearchTokenParts(raw string) []string {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return nil
	}

	var builder strings.Builder
	builder.Grow(len(normalized))
	for _, r := range normalized {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte(' ')
	}

	parts := strings.Fields(builder.String())
	if len(parts) == 0 {
		return nil
	}

	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = trimToMaxRunes(part, maxCatalogSearchTermRunes)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func catalogSearchTokenPartsWithLimitsAndTruncation(raw string, maxTermRunes int) ([]string, int) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return nil, 0
	}

	var builder strings.Builder
	builder.Grow(len(normalized))
	for _, r := range normalized {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte(' ')
	}

	parts := strings.Fields(builder.String())
	if len(parts) == 0 {
		return nil, 0
	}

	out := make([]string, 0, len(parts))
	termRuneTruncations := 0
	for _, part := range parts {
		trimmed, truncated := trimToMaxRunesWithTruncation(part, maxTermRunes)
		if truncated {
			termRuneTruncations++
		}
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out, termRuneTruncations
}

func trimToMaxRunesWithTruncation(raw string, maxRunes int) (string, bool) {
	if maxRunes <= 0 || raw == "" {
		return "", false
	}

	var out strings.Builder
	out.Grow(len(raw))
	runes := 0
	for _, r := range raw {
		if runes >= maxRunes {
			return out.String(), true
		}
		out.WriteRune(r)
		runes++
	}
	return out.String(), false
}

func trimToMaxRunes(raw string, maxRunes int) string {
	if maxRunes <= 0 || raw == "" {
		return ""
	}

	var out strings.Builder
	out.Grow(len(raw))
	runes := 0
	for _, r := range raw {
		if runes >= maxRunes {
			break
		}
		out.WriteRune(r)
		runes++
	}
	return out.String()
}

func (s *Store) migrate(ctx context.Context) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || path.Ext(e.Name()) != ".sql" {
			continue
		}
		files = append(files, e.Name())
	}
	sort.Strings(files)

	for _, name := range files {
		body, err := migrationFS.ReadFile(path.Join("migrations", name))
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := s.db.ExecContext(ctx, string(body)); err != nil {
			return fmt.Errorf("execute migration %s: %w", name, err)
		}
	}

	if err := s.ensureFailoverSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureMetricsSchema(ctx); err != nil {
		return err
	}
	if err := s.ensureDVRSchema(ctx); err != nil {
		return err
	}
	if err := s.migrateLegacyFavorites(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureFailoverSchema(ctx context.Context) error {
	if err := s.addColumnIfMissing(ctx, "playlist_items", "channel_key", `ALTER TABLE playlist_items ADD COLUMN channel_key TEXT`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "playlist_items", "tvg_name", `ALTER TABLE playlist_items ADD COLUMN tvg_name TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "playlist_items", "first_seen_at", `ALTER TABLE playlist_items ADD COLUMN first_seen_at INTEGER`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "playlist_items", "last_seen_at", `ALTER TABLE playlist_items ADD COLUMN last_seen_at INTEGER`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "playlist_items", "active", `ALTER TABLE playlist_items ADD COLUMN active INTEGER NOT NULL DEFAULT 1`); err != nil {
		return err
	}

	if _, err := s.db.ExecContext(ctx, `
		UPDATE playlist_items
		SET channel_key = CASE
			WHEN TRIM(COALESCE(tvg_id, '')) <> '' THEN 'tvg:' || LOWER(TRIM(tvg_id))
			ELSE 'name:' || LOWER(TRIM(name))
		END
		WHERE channel_key IS NULL OR TRIM(channel_key) = ''
	`); err != nil {
		return fmt.Errorf("backfill playlist_items.channel_key: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE playlist_items
		SET channel_key = `+normalizedChannelKeySQLExpr+`
		WHERE TRIM(COALESCE(channel_key, '')) <> ''
	`); err != nil {
		return fmt.Errorf("normalize playlist_items.channel_key: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		UPDATE playlist_items
		SET first_seen_at = COALESCE(first_seen_at, COALESCE(updated_at, CAST(strftime('%s','now') AS INTEGER))),
		    last_seen_at  = COALESCE(last_seen_at,  COALESCE(updated_at, CAST(strftime('%s','now') AS INTEGER))),
		    active        = COALESCE(active, 1)
	`); err != nil {
		return fmt.Errorf("backfill playlist_items lifecycle columns: %w", err)
	}
	if err := s.backfillPlaylistItemTVGNames(ctx); err != nil {
		return err
	}

	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_playlist_channel_key ON playlist_items(channel_key)`); err != nil {
		return fmt.Errorf("create idx_playlist_channel_key: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_playlist_active ON playlist_items(active)`); err != nil {
		return fmt.Errorf("create idx_playlist_active: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_playlist_active_group_name_name_item ON playlist_items(active, group_name, name, item_key)`); err != nil {
		return fmt.Errorf("create idx_playlist_active_group_name_name_item: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_playlist_active_name_item ON playlist_items(active, name, item_key)`); err != nil {
		return fmt.Errorf("create idx_playlist_active_name_item: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_playlist_active_key_name_item ON playlist_items(active, channel_key, name, item_key)`); err != nil {
		return fmt.Errorf("create idx_playlist_active_key_name_item: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS dynamic_channel_queries (
		  query_id      INTEGER PRIMARY KEY AUTOINCREMENT,
		  enabled       INTEGER NOT NULL DEFAULT 1,
		  name          TEXT NOT NULL DEFAULT '',
		  group_name    TEXT NOT NULL DEFAULT '',
		  group_names_json TEXT NOT NULL DEFAULT '[]',
		  search_query  TEXT NOT NULL DEFAULT '',
		  search_regex  INTEGER NOT NULL DEFAULT 0,
		  order_index   INTEGER NOT NULL,
		  last_count    INTEGER NOT NULL DEFAULT 0,
		  truncated_by  INTEGER NOT NULL DEFAULT 0,
		  next_slot_cursor INTEGER NOT NULL DEFAULT 0,
		  created_at    INTEGER NOT NULL,
		  updated_at    INTEGER NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create dynamic_channel_queries: %w", err)
	}
	if err := s.addColumnIfMissing(ctx, "dynamic_channel_queries", "last_count", `ALTER TABLE dynamic_channel_queries ADD COLUMN last_count INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dynamic_channel_queries", "truncated_by", `ALTER TABLE dynamic_channel_queries ADD COLUMN truncated_by INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dynamic_channel_queries", "group_names_json", `ALTER TABLE dynamic_channel_queries ADD COLUMN group_names_json TEXT NOT NULL DEFAULT '[]'`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dynamic_channel_queries", "search_regex", `ALTER TABLE dynamic_channel_queries ADD COLUMN search_regex INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dynamic_channel_queries", "next_slot_cursor", `ALTER TABLE dynamic_channel_queries ADD COLUMN next_slot_cursor INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS idx_dynamic_channel_queries_order
		  ON dynamic_channel_queries(order_index)
	`); err != nil {
		return fmt.Errorf("create idx_dynamic_channel_queries_order: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE dynamic_channel_queries
		SET enabled = COALESCE(enabled, 1),
		    name = TRIM(COALESCE(name, '')),
		    group_name = TRIM(COALESCE(group_name, '')),
		    group_names_json = TRIM(COALESCE(group_names_json, '[]')),
		    search_query = TRIM(COALESCE(search_query, '')),
		    search_regex = CASE
				WHEN COALESCE(search_regex, 0) <> 0 THEN 1
				ELSE 0
			END,
		    last_count = CASE
				WHEN COALESCE(last_count, 0) < 0 THEN 0
				ELSE COALESCE(last_count, 0)
			END,
		    truncated_by = CASE
				WHEN COALESCE(truncated_by, 0) < 0 THEN 0
				ELSE COALESCE(truncated_by, 0)
			END,
		    next_slot_cursor = CASE
				WHEN COALESCE(next_slot_cursor, 0) < 0 THEN 0
				WHEN COALESCE(next_slot_cursor, 0) >= 1000 THEN 999
				ELSE COALESCE(next_slot_cursor, 0)
			END
	`); err != nil {
		return fmt.Errorf("normalize dynamic_channel_queries rows: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS published_channels (
		  channel_id    INTEGER PRIMARY KEY AUTOINCREMENT,
		  channel_class TEXT NOT NULL DEFAULT 'traditional',
		  channel_key   TEXT,
		  guide_number  TEXT NOT NULL,
		  guide_name    TEXT NOT NULL,
		  order_index   INTEGER NOT NULL,
		  enabled       INTEGER NOT NULL DEFAULT 1,
		  dynamic_query_id INTEGER,
		  dynamic_item_key TEXT,
		  dynamic_source_identity TEXT NOT NULL DEFAULT '',
		  dynamic_sources_enabled INTEGER NOT NULL DEFAULT 0,
		  dynamic_group_name TEXT NOT NULL DEFAULT '',
		  dynamic_group_names_json TEXT NOT NULL DEFAULT '[]',
		  dynamic_search_query TEXT NOT NULL DEFAULT '',
		  dynamic_search_regex INTEGER NOT NULL DEFAULT 0,
		  created_at    INTEGER NOT NULL,
		  updated_at    INTEGER NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create published_channels: %w", err)
	}
	if err := s.addColumnIfMissing(ctx, "published_channels", "channel_class", `ALTER TABLE published_channels ADD COLUMN channel_class TEXT NOT NULL DEFAULT 'traditional'`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "published_channels", "dynamic_query_id", `ALTER TABLE published_channels ADD COLUMN dynamic_query_id INTEGER`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "published_channels", "dynamic_item_key", `ALTER TABLE published_channels ADD COLUMN dynamic_item_key TEXT`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "published_channels", "dynamic_source_identity", `ALTER TABLE published_channels ADD COLUMN dynamic_source_identity TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "published_channels", "dynamic_sources_enabled", `ALTER TABLE published_channels ADD COLUMN dynamic_sources_enabled INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "published_channels", "dynamic_group_name", `ALTER TABLE published_channels ADD COLUMN dynamic_group_name TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "published_channels", "dynamic_group_names_json", `ALTER TABLE published_channels ADD COLUMN dynamic_group_names_json TEXT NOT NULL DEFAULT '[]'`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "published_channels", "dynamic_search_query", `ALTER TABLE published_channels ADD COLUMN dynamic_search_query TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "published_channels", "dynamic_search_regex", `ALTER TABLE published_channels ADD COLUMN dynamic_search_regex INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_published_channels_guide_number ON published_channels(guide_number)`); err != nil {
		return fmt.Errorf("create idx_published_channels_guide_number: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_published_channels_order ON published_channels(order_index)`); err != nil {
		return fmt.Errorf("create idx_published_channels_order: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_published_channels_channel_key ON published_channels(channel_key)`); err != nil {
		return fmt.Errorf("create idx_published_channels_channel_key: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_published_channels_dynamic_query ON published_channels(dynamic_query_id)`); err != nil {
		return fmt.Errorf("create idx_published_channels_dynamic_query: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS idx_published_channels_dynamic_query_item
		  ON published_channels(dynamic_query_id, dynamic_item_key)
		  WHERE dynamic_query_id IS NOT NULL AND TRIM(COALESCE(dynamic_item_key, '')) <> ''
	`); err != nil {
		return fmt.Errorf("create idx_published_channels_dynamic_query_item: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS idx_published_channels_dynamic_query_source_identity
		  ON published_channels(dynamic_query_id, dynamic_source_identity)
		  WHERE dynamic_query_id IS NOT NULL AND TRIM(COALESCE(dynamic_source_identity, '')) <> ''
	`); err != nil {
		return fmt.Errorf("create idx_published_channels_dynamic_query_source_identity: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE published_channels
		SET channel_key = `+normalizedChannelKeySQLExpr+`
		WHERE TRIM(COALESCE(channel_key, '')) <> ''
	`); err != nil {
		return fmt.Errorf("normalize published_channels.channel_key: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE published_channels
		SET channel_class = CASE
			WHEN LOWER(TRIM(COALESCE(channel_class, ''))) = 'dynamic_generated' THEN 'dynamic_generated'
			ELSE 'traditional'
		END,
		    dynamic_query_id = CASE
			WHEN LOWER(TRIM(COALESCE(channel_class, ''))) = 'dynamic_generated' AND COALESCE(dynamic_query_id, 0) > 0
				THEN dynamic_query_id
			ELSE NULL
		END,
		    dynamic_item_key = TRIM(COALESCE(dynamic_item_key, '')),
		    dynamic_source_identity = CASE
				WHEN LOWER(TRIM(COALESCE(channel_class, ''))) = 'dynamic_generated' THEN
					CASE
						WHEN TRIM(COALESCE(dynamic_source_identity, '')) <> '' THEN TRIM(COALESCE(dynamic_source_identity, ''))
						WHEN TRIM(COALESCE(dynamic_item_key, '')) <> '' THEN 'item:' || LOWER(TRIM(COALESCE(dynamic_item_key, '')))
						ELSE ''
					END
				ELSE ''
			END,
		    dynamic_sources_enabled = COALESCE(dynamic_sources_enabled, 0),
		    dynamic_group_name = TRIM(COALESCE(dynamic_group_name, '')),
		    dynamic_group_names_json = TRIM(COALESCE(dynamic_group_names_json, '[]')),
		    dynamic_search_query = TRIM(COALESCE(dynamic_search_query, '')),
		    dynamic_search_regex = CASE
				WHEN COALESCE(dynamic_search_regex, 0) <> 0 THEN 1
				ELSE 0
			END
	`); err != nil {
		return fmt.Errorf("normalize published_channels dynamic columns: %w", err)
	}
	if err := s.normalizeDynamicQueryGroupNames(ctx); err != nil {
		return err
	}
	if err := s.normalizePublishedChannelGroupNames(ctx); err != nil {
		return err
	}

	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS channel_sources (
		  source_id         INTEGER PRIMARY KEY AUTOINCREMENT,
		  channel_id        INTEGER NOT NULL REFERENCES published_channels(channel_id) ON DELETE CASCADE,
		  item_key          TEXT NOT NULL REFERENCES playlist_items(item_key) ON DELETE RESTRICT,
		  priority_index    INTEGER NOT NULL,
		  enabled           INTEGER NOT NULL DEFAULT 1,
		  association_type  TEXT NOT NULL DEFAULT 'manual',
		  last_ok_at        INTEGER,
		  last_fail_at      INTEGER,
		  last_fail_reason  TEXT,
		  success_count     INTEGER NOT NULL DEFAULT 0,
		  fail_count        INTEGER NOT NULL DEFAULT 0,
		  cooldown_until    INTEGER NOT NULL DEFAULT 0,
		  last_probe_at     INTEGER NOT NULL DEFAULT 0,
		  profile_width     INTEGER NOT NULL DEFAULT 0,
		  profile_height    INTEGER NOT NULL DEFAULT 0,
		  profile_fps       REAL NOT NULL DEFAULT 0,
		  profile_video_codec TEXT NOT NULL DEFAULT '',
		  profile_audio_codec TEXT NOT NULL DEFAULT '',
		  profile_bitrate_bps INTEGER NOT NULL DEFAULT 0,
		  created_at        INTEGER NOT NULL,
		  updated_at        INTEGER NOT NULL,
		  UNIQUE(channel_id, item_key),
		  UNIQUE(channel_id, priority_index)
		)
	`); err != nil {
		return fmt.Errorf("create channel_sources: %w", err)
	}
	if err := s.addColumnIfMissing(ctx, "channel_sources", "last_probe_at", `ALTER TABLE channel_sources ADD COLUMN last_probe_at INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "channel_sources", "profile_width", `ALTER TABLE channel_sources ADD COLUMN profile_width INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "channel_sources", "profile_height", `ALTER TABLE channel_sources ADD COLUMN profile_height INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "channel_sources", "profile_fps", `ALTER TABLE channel_sources ADD COLUMN profile_fps REAL NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "channel_sources", "profile_video_codec", `ALTER TABLE channel_sources ADD COLUMN profile_video_codec TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "channel_sources", "profile_audio_codec", `ALTER TABLE channel_sources ADD COLUMN profile_audio_codec TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "channel_sources", "profile_bitrate_bps", `ALTER TABLE channel_sources ADD COLUMN profile_bitrate_bps INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE channel_sources
		SET last_probe_at = COALESCE(last_probe_at, 0),
		    profile_width = COALESCE(profile_width, 0),
		    profile_height = COALESCE(profile_height, 0),
		    profile_fps = COALESCE(profile_fps, 0),
		    profile_video_codec = TRIM(COALESCE(profile_video_codec, '')),
		    profile_audio_codec = TRIM(COALESCE(profile_audio_codec, '')),
		    profile_bitrate_bps = COALESCE(profile_bitrate_bps, 0)
	`); err != nil {
		return fmt.Errorf("backfill channel_sources profile columns: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_channel_sources_channel ON channel_sources(channel_id)`); err != nil {
		return fmt.Errorf("create idx_channel_sources_channel: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_channel_sources_cooldown ON channel_sources(cooldown_until)`); err != nil {
		return fmt.Errorf("create idx_channel_sources_cooldown: %w", err)
	}

	return nil
}

func (s *Store) ensureMetricsSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS stream_metrics (
		  item_key     TEXT PRIMARY KEY,
		  analyzed_at  INTEGER NOT NULL,
		  width        INTEGER,
		  height       INTEGER,
		  fps          REAL,
		  bitrate_bps  INTEGER,
		  variant_bps  INTEGER,
		  video_codec  TEXT,
		  audio_codec  TEXT,
		  score_hint   REAL,
		  error        TEXT
		)
	`); err != nil {
		return fmt.Errorf("create stream_metrics: %w", err)
	}
	if err := s.addColumnIfMissing(ctx, "stream_metrics", "video_codec", `ALTER TABLE stream_metrics ADD COLUMN video_codec TEXT`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "stream_metrics", "audio_codec", `ALTER TABLE stream_metrics ADD COLUMN audio_codec TEXT`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE stream_metrics
		SET video_codec = TRIM(COALESCE(video_codec, '')),
		    audio_codec = TRIM(COALESCE(audio_codec, ''))
	`); err != nil {
		return fmt.Errorf("backfill stream_metrics codec columns: %w", err)
	}
	return nil
}

func (s *Store) ensureDVRSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS dvr_instances (
		  id                       INTEGER PRIMARY KEY AUTOINCREMENT,
		  singleton_key            INTEGER NOT NULL DEFAULT 1,
		  provider                 TEXT NOT NULL,
		  active_providers         TEXT NOT NULL DEFAULT '',
		  base_url                 TEXT NOT NULL DEFAULT '',
		  channels_base_url        TEXT NOT NULL DEFAULT '',
		  jellyfin_base_url        TEXT NOT NULL DEFAULT '',
		  default_lineup_id        TEXT,
		  sync_enabled             INTEGER NOT NULL DEFAULT 0,
		  sync_cron                TEXT,
		  sync_mode                TEXT NOT NULL DEFAULT 'configured_only',
		  pre_sync_refresh_devices INTEGER NOT NULL DEFAULT 0,
		  jellyfin_api_token       TEXT NOT NULL DEFAULT '',
		  jellyfin_tuner_host_id   TEXT NOT NULL DEFAULT '',
		  updated_at               INTEGER NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create dvr_instances: %w", err)
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "provider", `ALTER TABLE dvr_instances ADD COLUMN provider TEXT NOT NULL DEFAULT 'channels'`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "active_providers", `ALTER TABLE dvr_instances ADD COLUMN active_providers TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "singleton_key", `ALTER TABLE dvr_instances ADD COLUMN singleton_key INTEGER NOT NULL DEFAULT 1`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "base_url", `ALTER TABLE dvr_instances ADD COLUMN base_url TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "channels_base_url", `ALTER TABLE dvr_instances ADD COLUMN channels_base_url TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "jellyfin_base_url", `ALTER TABLE dvr_instances ADD COLUMN jellyfin_base_url TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "default_lineup_id", `ALTER TABLE dvr_instances ADD COLUMN default_lineup_id TEXT`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "sync_enabled", `ALTER TABLE dvr_instances ADD COLUMN sync_enabled INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "sync_cron", `ALTER TABLE dvr_instances ADD COLUMN sync_cron TEXT`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "sync_mode", `ALTER TABLE dvr_instances ADD COLUMN sync_mode TEXT NOT NULL DEFAULT 'configured_only'`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "pre_sync_refresh_devices", `ALTER TABLE dvr_instances ADD COLUMN pre_sync_refresh_devices INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "jellyfin_api_token", `ALTER TABLE dvr_instances ADD COLUMN jellyfin_api_token TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "jellyfin_tuner_host_id", `ALTER TABLE dvr_instances ADD COLUMN jellyfin_tuner_host_id TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "dvr_instances", "updated_at", `ALTER TABLE dvr_instances ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE dvr_instances
		SET provider = CASE
			WHEN TRIM(COALESCE(provider, '')) = '' THEN 'channels'
			WHEN LOWER(TRIM(provider)) = 'jellyfin' THEN 'jellyfin'
			ELSE 'channels'
		END,
		    active_providers = CASE
			WHEN TRIM(COALESCE(active_providers, '')) = '' THEN ''
			ELSE TRIM(COALESCE(active_providers, ''))
		END,
		    channels_base_url = CASE
			WHEN TRIM(COALESCE(channels_base_url, '')) <> '' THEN TRIM(channels_base_url)
			WHEN LOWER(TRIM(COALESCE(provider, ''))) = 'channels' AND TRIM(COALESCE(base_url, '')) <> '' THEN TRIM(base_url)
			ELSE ''
		END,
		    jellyfin_base_url = CASE
			WHEN TRIM(COALESCE(jellyfin_base_url, '')) <> '' THEN TRIM(jellyfin_base_url)
			WHEN LOWER(TRIM(COALESCE(provider, ''))) = 'jellyfin' AND TRIM(COALESCE(base_url, '')) <> '' THEN TRIM(base_url)
			ELSE ''
		END,
		    base_url = CASE
			WHEN TRIM(COALESCE(base_url, '')) <> '' THEN TRIM(base_url)
			WHEN LOWER(TRIM(COALESCE(provider, ''))) = 'channels' THEN TRIM(COALESCE(channels_base_url, ''))
			WHEN LOWER(TRIM(COALESCE(provider, ''))) = 'jellyfin' THEN TRIM(COALESCE(jellyfin_base_url, ''))
			ELSE ''
		END,
		    default_lineup_id = TRIM(COALESCE(default_lineup_id, '')),
		    sync_enabled = COALESCE(sync_enabled, 0),
		    sync_cron = TRIM(COALESCE(sync_cron, '')),
		    sync_mode = CASE
			WHEN LOWER(TRIM(COALESCE(sync_mode, ''))) = 'mirror_device' THEN 'mirror_device'
			ELSE 'configured_only'
		END,
		    pre_sync_refresh_devices = COALESCE(pre_sync_refresh_devices, 0),
		    jellyfin_api_token = TRIM(COALESCE(jellyfin_api_token, '')),
		    jellyfin_tuner_host_id = TRIM(COALESCE(jellyfin_tuner_host_id, '')),
		    singleton_key = 1,
		    updated_at = CASE
			WHEN COALESCE(updated_at, 0) <= 0 THEN CAST(strftime('%s','now') AS INTEGER)
			ELSE updated_at
		END
	`); err != nil {
		return fmt.Errorf("normalize dvr_instances rows: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS published_channel_dvr_map (
		  channel_id         INTEGER NOT NULL REFERENCES published_channels(channel_id) ON DELETE CASCADE,
		  dvr_instance_id    INTEGER NOT NULL REFERENCES dvr_instances(id) ON DELETE CASCADE,
		  dvr_lineup_id      TEXT NOT NULL,
		  dvr_lineup_channel TEXT NOT NULL,
		  dvr_station_ref    TEXT,
		  dvr_callsign_hint  TEXT,
		  PRIMARY KEY(channel_id, dvr_instance_id)
		)
	`); err != nil {
		return fmt.Errorf("create published_channel_dvr_map: %w", err)
	}
	if err := s.addColumnIfMissing(ctx, "published_channel_dvr_map", "dvr_station_ref", `ALTER TABLE published_channel_dvr_map ADD COLUMN dvr_station_ref TEXT`); err != nil {
		return err
	}
	if err := s.addColumnIfMissing(ctx, "published_channel_dvr_map", "dvr_callsign_hint", `ALTER TABLE published_channel_dvr_map ADD COLUMN dvr_callsign_hint TEXT`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE published_channel_dvr_map
		SET dvr_lineup_id = TRIM(COALESCE(dvr_lineup_id, '')),
		    dvr_lineup_channel = TRIM(COALESCE(dvr_lineup_channel, '')),
		    dvr_station_ref = TRIM(COALESCE(dvr_station_ref, '')),
		    dvr_callsign_hint = TRIM(COALESCE(dvr_callsign_hint, ''))
	`); err != nil {
		return fmt.Errorf("normalize published_channel_dvr_map rows: %w", err)
	}
	if err := s.enforceDVRInstanceSingleton(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS idx_dvr_instances_singleton_key
		  ON dvr_instances(singleton_key)
	`); err != nil {
		return fmt.Errorf("create idx_dvr_instances_singleton_key: %w", err)
	}
	if err := s.ensureDVRInstanceSingleton(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_channel_dvr_map_instance_lineup
		  ON published_channel_dvr_map(dvr_instance_id, dvr_lineup_id)
	`); err != nil {
		return fmt.Errorf("create idx_channel_dvr_map_instance_lineup: %w", err)
	}
	return nil
}

func (s *Store) enforceDVRInstanceSingleton(ctx context.Context) error {
	var canonicalID int64
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT id FROM dvr_instances ORDER BY id ASC LIMIT 1`,
	).Scan(&canonicalID); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("select canonical dvr instance: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin dvr singleton normalization tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `UPDATE dvr_instances SET singleton_key = 1`); err != nil {
		return fmt.Errorf("normalize dvr singleton keys: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE OR IGNORE published_channel_dvr_map SET dvr_instance_id = ? WHERE dvr_instance_id <> ?`,
		canonicalID,
		canonicalID,
	); err != nil {
		return fmt.Errorf("repoint dvr map rows to canonical instance: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM published_channel_dvr_map WHERE dvr_instance_id <> ?`,
		canonicalID,
	); err != nil {
		return fmt.Errorf("delete duplicate dvr map rows: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM dvr_instances WHERE id <> ?`, canonicalID); err != nil {
		return fmt.Errorf("delete duplicate dvr instances: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit dvr singleton normalization tx: %w", err)
	}
	return nil
}

func (s *Store) addColumnIfMissing(ctx context.Context, table, column, alterSQL string) error {
	exists, err := s.columnExists(ctx, table, column)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, alterSQL); err != nil {
		return fmt.Errorf("add column %s.%s: %w", table, column, err)
	}
	return nil
}

func (s *Store) backfillPlaylistItemTVGNames(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT item_key, COALESCE(attrs_json, '')
		FROM playlist_items
		WHERE TRIM(COALESCE(tvg_name, '')) = ''
	`)
	if err != nil {
		return fmt.Errorf("query playlist items for tvg_name backfill: %w", err)
	}

	type tvgNameUpdate struct {
		itemKey string
		tvgName string
	}
	updates := make([]tvgNameUpdate, 0)
	for rows.Next() {
		var (
			itemKey  string
			attrsRaw string
		)
		if err := rows.Scan(&itemKey, &attrsRaw); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan playlist item for tvg_name backfill: %w", err)
		}
		tvgName := extractTVGNameFromAttrsRaw(attrsRaw)
		if tvgName == "" {
			continue
		}
		updates = append(updates, tvgNameUpdate{
			itemKey: itemKey,
			tvgName: tvgName,
		})
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close playlist item tvg_name backfill rows: %w", err)
	}
	if len(updates) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin playlist tvg_name backfill tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `UPDATE playlist_items SET tvg_name = ? WHERE item_key = ?`)
	if err != nil {
		return fmt.Errorf("prepare playlist tvg_name backfill update: %w", err)
	}
	defer stmt.Close()

	for _, update := range updates {
		if _, err := stmt.ExecContext(ctx, update.tvgName, update.itemKey); err != nil {
			return fmt.Errorf("backfill playlist item %q tvg_name: %w", update.itemKey, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit playlist tvg_name backfill tx: %w", err)
	}
	return nil
}

func tvgNameFromAttrs(attrs map[string]string) string {
	if attrs == nil {
		return ""
	}
	if tvgName := strings.TrimSpace(attrs["tvg-name"]); tvgName != "" {
		return tvgName
	}
	return strings.TrimSpace(attrs["tvg_name"])
}

func (s *Store) columnExists(ctx context.Context, table, column string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, fmt.Errorf("table_info(%s): %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid       int
			name      string
			colType   string
			notNull   int
			defaultV  any
			primaryPK int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultV, &primaryPK); err != nil {
			return false, fmt.Errorf("scan table_info(%s): %w", table, err)
		}
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate table_info(%s): %w", table, err)
	}
	return false, nil
}

func (s *Store) tableExists(ctx context.Context, table string) (bool, error) {
	var found string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`,
		table,
	).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check table %q: %w", table, err)
	}
	return found == table, nil
}

func (s *Store) migrateLegacyFavorites(ctx context.Context) error {
	exists, err := s.tableExists(ctx, "favorites")
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	var publishedCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM published_channels`).Scan(&publishedCount); err != nil {
		return fmt.Errorf("count published channels before legacy migration: %w", err)
	}
	if publishedCount > 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin legacy favorites migration tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO published_channels (
		  channel_key,
		  guide_number,
		  guide_name,
		  order_index,
		  enabled,
		  created_at,
		  updated_at
		)
		SELECT
		  p.channel_key,
		  f.guide_number,
		  f.guide_name,
		  f.order_index,
		  f.enabled,
		  f.created_at,
		  f.updated_at
		FROM favorites f
		LEFT JOIN playlist_items p ON p.item_key = f.item_key
	`); err != nil {
		return fmt.Errorf("migrate favorites to published_channels: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO channel_sources (
		  channel_id,
		  item_key,
		  priority_index,
		  enabled,
		  association_type,
		  created_at,
		  updated_at
		)
		SELECT
		  pc.channel_id,
		  f.item_key,
		  0,
		  1,
		  'manual',
		  f.created_at,
		  f.updated_at
		FROM favorites f
		JOIN published_channels pc ON pc.guide_number = f.guide_number
	`); err != nil {
		return fmt.Errorf("migrate favorites to channel_sources: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit legacy favorites migration: %w", err)
	}
	return nil
}
