package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/playlist"
)

const (
	primaryPlaylistSourceID        int64 = 1
	primaryPlaylistSourceKey             = "primary"
	primaryPlaylistSourceName            = "Primary"
	defaultPrimarySourceTunerCount       = 2
	playlistSourceKeyBytes               = 8
	maxPlaylistSourceKeyAttempts         = 8
)

type playlistSourceScanner interface {
	Scan(dest ...any) error
}

func (s *Store) ensurePlaylistSourcesSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS playlist_sources (
		  source_id    INTEGER PRIMARY KEY AUTOINCREMENT,
		  source_key   TEXT NOT NULL UNIQUE,
		  name         TEXT NOT NULL UNIQUE,
		  playlist_url TEXT NOT NULL DEFAULT '' UNIQUE,
		  tuner_count  INTEGER NOT NULL,
		  enabled      INTEGER NOT NULL DEFAULT 1,
		  order_index  INTEGER NOT NULL,
		  created_at   INTEGER NOT NULL,
		  updated_at   INTEGER NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create playlist_sources: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_playlist_sources_order ON playlist_sources(order_index)`); err != nil {
		return fmt.Errorf("create idx_playlist_sources_order: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_playlist_sources_enabled_order ON playlist_sources(enabled, order_index)`); err != nil {
		return fmt.Errorf("create idx_playlist_sources_enabled_order: %w", err)
	}

	if err := s.addColumnIfMissing(ctx, "playlist_items", "playlist_source_id", `ALTER TABLE playlist_items ADD COLUMN playlist_source_id INTEGER NOT NULL DEFAULT 1`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE playlist_items
		SET playlist_source_id = 1
		WHERE COALESCE(playlist_source_id, 0) <= 0
	`); err != nil {
		return fmt.Errorf("backfill playlist_items.playlist_source_id: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_playlist_source_active_name_item ON playlist_items(playlist_source_id, active, name, item_key)`); err != nil {
		return fmt.Errorf("create idx_playlist_source_active_name_item: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_playlist_source_active_key_name_item ON playlist_items(playlist_source_id, active, channel_key, name, item_key)`); err != nil {
		return fmt.Errorf("create idx_playlist_source_active_key_name_item: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_playlist_source_active_group_name ON playlist_items(playlist_source_id, active, group_name)`); err != nil {
		return fmt.Errorf("create idx_playlist_source_active_group_name: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_playlist_active_source_group_name_name_item ON playlist_items(active, playlist_source_id, group_name, name, item_key)`); err != nil {
		return fmt.Errorf("create idx_playlist_active_source_group_name_name_item: %w", err)
	}

	if err := s.ensurePrimaryPlaylistSource(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensurePrimaryPlaylistSource(ctx context.Context) error {
	playlistURL, err := s.readPlaylistURLSetting(ctx)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin playlist source bootstrap tx: %w", err)
	}
	defer tx.Rollback()

	source, err := getPlaylistSourceByIDTx(ctx, tx, primaryPlaylistSourceID)
	switch {
	case err == nil:
		if err := normalizePrimaryPlaylistSourceTx(ctx, tx, source, playlistURL); err != nil {
			return err
		}
	case err == sql.ErrNoRows:
		if err := shiftPlaylistSourceOrderIndexesTx(ctx, tx, 1); err != nil {
			return err
		}
		name, err := nextAvailablePlaylistSourceNameTx(ctx, tx, primaryPlaylistSourceName)
		if err != nil {
			return err
		}
		urlForInsert := playlist.CanonicalPlaylistSourceURL(playlistURL)
		if urlForInsert != "" {
			available, err := playlistSourceURLAvailableTx(ctx, tx, urlForInsert, 0)
			if err != nil {
				return err
			}
			if !available {
				urlForInsert = ""
			}
		}

		now := time.Now().UTC().Unix()
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO playlist_sources (
				source_id,
				source_key,
				name,
				playlist_url,
				tuner_count,
				enabled,
				order_index,
				created_at,
				updated_at
			)
			VALUES (?, ?, ?, ?, ?, 1, 0, ?, ?)`,
			primaryPlaylistSourceID,
			primaryPlaylistSourceKey,
			name,
			urlForInsert,
			defaultPrimarySourceTunerCount,
			now,
			now,
		); err != nil {
			return fmt.Errorf("insert primary playlist source: %w", err)
		}
	default:
		return fmt.Errorf("lookup primary playlist source: %w", err)
	}

	primary, err := getPlaylistSourceByIDTx(ctx, tx, primaryPlaylistSourceID)
	if err != nil {
		return fmt.Errorf("lookup primary playlist source after bootstrap: %w", err)
	}
	if err := upsertSettingTx(ctx, tx, SettingPlaylistURL, primary.PlaylistURL); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit playlist source bootstrap tx: %w", err)
	}
	return nil
}

func normalizePrimaryPlaylistSourceTx(ctx context.Context, tx *sql.Tx, source playlist.PlaylistSource, fallbackURL string) error {
	sourceKey := strings.TrimSpace(source.SourceKey)
	if sourceKey == "" {
		sourceKey = primaryPlaylistSourceKey
	}

	name := strings.TrimSpace(source.Name)
	if name == "" {
		fallbackName, err := nextAvailablePlaylistSourceNameTx(ctx, tx, primaryPlaylistSourceName)
		if err != nil {
			return err
		}
		name = fallbackName
	}

	playlistURL := playlist.CanonicalPlaylistSourceURL(source.PlaylistURL)
	canonicalFallbackURL := playlist.CanonicalPlaylistSourceURL(fallbackURL)
	if playlistURL == "" && canonicalFallbackURL != "" {
		available, err := playlistSourceURLAvailableTx(ctx, tx, canonicalFallbackURL, source.SourceID)
		if err != nil {
			return err
		}
		if available {
			playlistURL = canonicalFallbackURL
		}
	}

	tunerCount := source.TunerCount
	if tunerCount < 1 {
		tunerCount = defaultPrimarySourceTunerCount
	}
	orderIndex := source.OrderIndex
	if orderIndex < 0 {
		orderIndex = 0
	}
	enabled := source.Enabled

	if sourceKey == source.SourceKey &&
		name == source.Name &&
		playlistURL == source.PlaylistURL &&
		tunerCount == source.TunerCount &&
		orderIndex == source.OrderIndex &&
		enabled == source.Enabled {
		return nil
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE playlist_sources
		 SET source_key = ?,
		     name = ?,
		     playlist_url = ?,
		     tuner_count = ?,
		     enabled = ?,
		     order_index = ?,
		     updated_at = ?
		 WHERE source_id = ?`,
		sourceKey,
		name,
		playlistURL,
		tunerCount,
		boolToInt(enabled),
		orderIndex,
		time.Now().UTC().Unix(),
		source.SourceID,
	); err != nil {
		return fmt.Errorf("normalize primary playlist source: %w", err)
	}
	return nil
}

func shiftPlaylistSourceOrderIndexesTx(ctx context.Context, tx *sql.Tx, shiftBy int) error {
	if shiftBy <= 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE playlist_sources SET order_index = -(order_index + ?)`, shiftBy); err != nil {
		return fmt.Errorf("stage playlist source order shift: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE playlist_sources SET order_index = -order_index`); err != nil {
		return fmt.Errorf("finalize playlist source order shift: %w", err)
	}
	return nil
}

func nextAvailablePlaylistSourceNameTx(ctx context.Context, tx *sql.Tx, base string) (string, error) {
	base = strings.TrimSpace(base)
	if base == "" {
		base = primaryPlaylistSourceName
	}

	candidate := base
	for i := 0; i < 1024; i++ {
		available, err := playlistSourceNameAvailableTx(ctx, tx, candidate, 0)
		if err != nil {
			return "", err
		}
		if available {
			return candidate, nil
		}
		candidate = fmt.Sprintf("%s %d", base, i+2)
	}
	return "", fmt.Errorf("allocate playlist source name: exhausted attempts")
}

func playlistSourceNameAvailableTx(ctx context.Context, tx *sql.Tx, name string, excludeSourceID int64) (bool, error) {
	nameKey := playlist.CanonicalPlaylistSourceName(name)
	if nameKey == "" {
		return false, nil
	}

	query := `SELECT source_id, COALESCE(name, '') FROM playlist_sources`
	args := make([]any, 0, 1)
	if excludeSourceID > 0 {
		query += ` WHERE source_id <> ?`
		args = append(args, excludeSourceID)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("check playlist source name %q availability: %w", name, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			ignoredID    int64
			existingName string
		)
		if err := rows.Scan(&ignoredID, &existingName); err != nil {
			return false, fmt.Errorf("scan playlist source name while checking availability for %q: %w", name, err)
		}
		if playlist.CanonicalPlaylistSourceName(existingName) == nameKey {
			return false, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate playlist source names while checking availability for %q: %w", name, err)
	}
	return true, nil
}

func playlistSourceURLAvailableTx(ctx context.Context, tx *sql.Tx, playlistURL string, excludeSourceID int64) (bool, error) {
	urlKey := playlist.CanonicalPlaylistSourceURL(playlistURL)
	if urlKey == "" {
		return false, nil
	}

	query := `SELECT source_id, COALESCE(playlist_url, '') FROM playlist_sources`
	args := make([]any, 0, 1)
	if excludeSourceID > 0 {
		query += ` WHERE source_id <> ?`
		args = append(args, excludeSourceID)
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("check playlist source url %q availability: %w", playlistURL, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			ignoredID   int64
			existingURL string
		)
		if err := rows.Scan(&ignoredID, &existingURL); err != nil {
			return false, fmt.Errorf("scan playlist source url while checking availability for %q: %w", playlistURL, err)
		}
		if playlist.CanonicalPlaylistSourceURL(existingURL) == urlKey {
			return false, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate playlist source urls while checking availability for %q: %w", playlistURL, err)
	}
	return true, nil
}

func (s *Store) readPlaylistURLSetting(ctx context.Context) (string, error) {
	value, err := s.GetSetting(ctx, SettingPlaylistURL)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read setting %q for playlist source bootstrap: %w", SettingPlaylistURL, err)
	}
	return strings.TrimSpace(value), nil
}

func scanPlaylistSourceRow(scanner playlistSourceScanner) (playlist.PlaylistSource, error) {
	var (
		source     playlist.PlaylistSource
		enabledInt int
	)
	if err := scanner.Scan(
		&source.SourceID,
		&source.SourceKey,
		&source.Name,
		&source.PlaylistURL,
		&source.TunerCount,
		&enabledInt,
		&source.OrderIndex,
		&source.CreatedAt,
		&source.UpdatedAt,
	); err != nil {
		return playlist.PlaylistSource{}, err
	}
	source.SourceKey = strings.TrimSpace(source.SourceKey)
	source.Name = strings.TrimSpace(source.Name)
	source.PlaylistURL = strings.TrimSpace(source.PlaylistURL)
	source.Enabled = enabledInt != 0
	return source, nil
}

func (s *Store) ListPlaylistSources(ctx context.Context) ([]playlist.PlaylistSource, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT source_id, COALESCE(source_key, ''), COALESCE(name, ''), COALESCE(playlist_url, ''), COALESCE(tuner_count, 0), COALESCE(enabled, 0), COALESCE(order_index, 0), COALESCE(created_at, 0), COALESCE(updated_at, 0)
		 FROM playlist_sources
		 ORDER BY order_index ASC, source_id ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("query playlist sources: %w", err)
	}
	defer rows.Close()

	out := make([]playlist.PlaylistSource, 0)
	for rows.Next() {
		source, err := scanPlaylistSourceRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan playlist source row: %w", err)
		}
		out = append(out, source)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate playlist source rows: %w", err)
	}
	return out, nil
}

func (s *Store) GetPlaylistSource(ctx context.Context, sourceID int64) (playlist.PlaylistSource, error) {
	if sourceID <= 0 {
		return playlist.PlaylistSource{}, fmt.Errorf("source_id must be greater than zero")
	}
	row := s.db.QueryRowContext(
		ctx,
		`SELECT source_id, COALESCE(source_key, ''), COALESCE(name, ''), COALESCE(playlist_url, ''), COALESCE(tuner_count, 0), COALESCE(enabled, 0), COALESCE(order_index, 0), COALESCE(created_at, 0), COALESCE(updated_at, 0)
		 FROM playlist_sources
		 WHERE source_id = ?`,
		sourceID,
	)
	source, err := scanPlaylistSourceRow(row)
	if err == sql.ErrNoRows {
		return playlist.PlaylistSource{}, playlist.ErrPlaylistSourceNotFound
	}
	if err != nil {
		return playlist.PlaylistSource{}, fmt.Errorf("query playlist source %d: %w", sourceID, err)
	}
	return source, nil
}

func (s *Store) ensurePlaylistSourceExists(ctx context.Context, sourceID int64) error {
	var found int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM playlist_sources WHERE source_id = ?`, sourceID).Scan(&found)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: source_id %d", playlist.ErrPlaylistSourceNotFound, sourceID)
	}
	if err != nil {
		return fmt.Errorf("query playlist source %d existence: %w", sourceID, err)
	}
	return nil
}

func ensurePlaylistSourceExistsTx(ctx context.Context, tx *sql.Tx, sourceID int64) error {
	var found int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM playlist_sources WHERE source_id = ?`, sourceID).Scan(&found)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: source_id %d", playlist.ErrPlaylistSourceNotFound, sourceID)
	}
	if err != nil {
		return fmt.Errorf("query playlist source %d existence: %w", sourceID, err)
	}
	return nil
}

func countEnabledPlaylistSourcesTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var enabledCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM playlist_sources WHERE enabled <> 0`).Scan(&enabledCount); err != nil {
		return 0, fmt.Errorf("count enabled playlist sources: %w", err)
	}
	return enabledCount, nil
}

func ensureEnabledPlaylistSourcesTx(ctx context.Context, tx *sql.Tx) error {
	enabledCount, err := countEnabledPlaylistSourcesTx(ctx, tx)
	if err != nil {
		return err
	}
	if enabledCount < 1 {
		return playlist.ErrNoEnabledPlaylistSources
	}
	return nil
}

func getPlaylistSourceByIDTx(ctx context.Context, tx *sql.Tx, sourceID int64) (playlist.PlaylistSource, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT source_id, COALESCE(source_key, ''), COALESCE(name, ''), COALESCE(playlist_url, ''), COALESCE(tuner_count, 0), COALESCE(enabled, 0), COALESCE(order_index, 0), COALESCE(created_at, 0), COALESCE(updated_at, 0)
		 FROM playlist_sources
		 WHERE source_id = ?`,
		sourceID,
	)
	return scanPlaylistSourceRow(row)
}

func (s *Store) CreatePlaylistSource(ctx context.Context, create playlist.PlaylistSourceCreate) (playlist.PlaylistSource, error) {
	return s.createPlaylistSourceWithKeyGenerator(ctx, create, randomPlaylistSourceKey)
}

func (s *Store) createPlaylistSourceWithKeyGenerator(ctx context.Context, create playlist.PlaylistSourceCreate, keyGenerator func() (string, error)) (playlist.PlaylistSource, error) {
	if keyGenerator == nil {
		return playlist.PlaylistSource{}, fmt.Errorf("create playlist source: source key generator is required")
	}

	name := strings.TrimSpace(create.Name)
	if name == "" {
		return playlist.PlaylistSource{}, fmt.Errorf("name is required")
	}
	playlistURL := playlist.CanonicalPlaylistSourceURL(create.PlaylistURL)
	if playlistURL == "" {
		return playlist.PlaylistSource{}, fmt.Errorf("playlist_url is required")
	}
	if create.TunerCount < 1 {
		return playlist.PlaylistSource{}, fmt.Errorf("tuner_count must be at least 1")
	}
	enabled := true
	if create.Enabled != nil {
		enabled = *create.Enabled
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return playlist.PlaylistSource{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var maxOrder sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(order_index) FROM playlist_sources`).Scan(&maxOrder); err != nil {
		return playlist.PlaylistSource{}, fmt.Errorf("lookup max playlist source order: %w", err)
	}
	orderIndex := 0
	if maxOrder.Valid {
		orderIndex = int(maxOrder.Int64) + 1
	}
	nameAvailable, err := playlistSourceNameAvailableTx(ctx, tx, name, 0)
	if err != nil {
		return playlist.PlaylistSource{}, err
	}
	if !nameAvailable {
		return playlist.PlaylistSource{}, fmt.Errorf("create playlist source: playlist source name already exists")
	}
	urlAvailable, err := playlistSourceURLAvailableTx(ctx, tx, playlistURL, 0)
	if err != nil {
		return playlist.PlaylistSource{}, err
	}
	if !urlAvailable {
		return playlist.PlaylistSource{}, fmt.Errorf("create playlist source: playlist source URL already exists")
	}

	now := time.Now().UTC().Unix()
	var sourceID int64
	for attempt := 0; attempt < maxPlaylistSourceKeyAttempts; attempt++ {
		sourceKey, err := keyGenerator()
		if err != nil {
			return playlist.PlaylistSource{}, err
		}

		result, err := tx.ExecContext(
			ctx,
			`INSERT INTO playlist_sources (
				source_key,
				name,
				playlist_url,
				tuner_count,
				enabled,
				order_index,
				created_at,
				updated_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			sourceKey,
			name,
			playlistURL,
			create.TunerCount,
			boolToInt(enabled),
			orderIndex,
			now,
			now,
		)
		if err != nil {
			if playlistSourceConstraintField(err) == "source_key" {
				continue
			}
			return playlist.PlaylistSource{}, mapPlaylistSourceMutationError(err, "create playlist source")
		}
		sourceID, err = result.LastInsertId()
		if err != nil {
			return playlist.PlaylistSource{}, fmt.Errorf("lookup inserted playlist source id: %w", err)
		}
		break
	}
	if sourceID <= 0 {
		return playlist.PlaylistSource{}, fmt.Errorf("create playlist source: failed to generate unique source_key")
	}

	if err := tx.Commit(); err != nil {
		return playlist.PlaylistSource{}, fmt.Errorf("commit create playlist source: %w", err)
	}
	return s.GetPlaylistSource(ctx, sourceID)
}

func (s *Store) UpdatePlaylistSource(ctx context.Context, sourceID int64, update playlist.PlaylistSourceUpdate) (playlist.PlaylistSource, error) {
	if sourceID <= 0 {
		return playlist.PlaylistSource{}, fmt.Errorf("source_id must be greater than zero")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return playlist.PlaylistSource{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	current, err := getPlaylistSourceByIDTx(ctx, tx, sourceID)
	if err == sql.ErrNoRows {
		return playlist.PlaylistSource{}, playlist.ErrPlaylistSourceNotFound
	}
	if err != nil {
		return playlist.PlaylistSource{}, fmt.Errorf("lookup playlist source %d: %w", sourceID, err)
	}

	validateNameUniqueness := false
	validateURLUniqueness := false
	if update.Name != nil {
		trimmed := strings.TrimSpace(*update.Name)
		if trimmed == "" {
			return playlist.PlaylistSource{}, fmt.Errorf("name is required")
		}
		if trimmed != current.Name {
			validateNameUniqueness = true
		}
		current.Name = trimmed
	}
	if update.PlaylistURL != nil {
		canonicalURL := playlist.CanonicalPlaylistSourceURL(*update.PlaylistURL)
		if canonicalURL == "" && sourceID != primaryPlaylistSourceID {
			return playlist.PlaylistSource{}, fmt.Errorf("playlist_url is required")
		}
		if canonicalURL != current.PlaylistURL {
			validateURLUniqueness = true
		}
		current.PlaylistURL = canonicalURL
	}
	if update.TunerCount != nil {
		if *update.TunerCount < 1 {
			return playlist.PlaylistSource{}, fmt.Errorf("tuner_count must be at least 1")
		}
		current.TunerCount = *update.TunerCount
	}
	if update.Enabled != nil {
		current.Enabled = *update.Enabled
	}
	if validateNameUniqueness {
		nameAvailable, err := playlistSourceNameAvailableTx(ctx, tx, current.Name, sourceID)
		if err != nil {
			return playlist.PlaylistSource{}, err
		}
		if !nameAvailable {
			return playlist.PlaylistSource{}, fmt.Errorf("update playlist source %d: playlist source name already exists", sourceID)
		}
	}
	if validateURLUniqueness && current.PlaylistURL != "" {
		urlAvailable, err := playlistSourceURLAvailableTx(ctx, tx, current.PlaylistURL, sourceID)
		if err != nil {
			return playlist.PlaylistSource{}, err
		}
		if !urlAvailable {
			return playlist.PlaylistSource{}, fmt.Errorf("update playlist source %d: playlist source URL already exists", sourceID)
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE playlist_sources
		 SET name = ?,
		     playlist_url = ?,
		     tuner_count = ?,
		     enabled = ?,
		     updated_at = ?
		 WHERE source_id = ?`,
		current.Name,
		current.PlaylistURL,
		current.TunerCount,
		boolToInt(current.Enabled),
		time.Now().UTC().Unix(),
		sourceID,
	); err != nil {
		return playlist.PlaylistSource{}, mapPlaylistSourceMutationError(err, fmt.Sprintf("update playlist source %d", sourceID))
	}
	if sourceID == primaryPlaylistSourceID {
		if err := upsertSettingTx(ctx, tx, SettingPlaylistURL, current.PlaylistURL); err != nil {
			return playlist.PlaylistSource{}, err
		}
	}
	if err := ensureEnabledPlaylistSourcesTx(ctx, tx); err != nil {
		return playlist.PlaylistSource{}, err
	}

	if err := tx.Commit(); err != nil {
		return playlist.PlaylistSource{}, fmt.Errorf("commit update playlist source %d: %w", sourceID, err)
	}
	return s.GetPlaylistSource(ctx, sourceID)
}

func (s *Store) BulkUpdatePlaylistSources(ctx context.Context, updates []playlist.PlaylistSourceBulkUpdate) error {
	if len(updates) == 0 {
		return fmt.Errorf("playlist_sources must include at least one source")
	}

	type normalizedBulkUpdate struct {
		SourceID    int64
		Name        string
		PlaylistURL string
		TunerCount  int
		Enabled     bool
	}

	normalized := make([]normalizedBulkUpdate, 0, len(updates))
	seenIDs := make(map[int64]struct{}, len(updates))
	seenNames := make(map[string]int, len(updates))
	seenURLs := make(map[string]int, len(updates))
	enabledCount := 0

	for i, update := range updates {
		if update.SourceID <= 0 {
			return fmt.Errorf("playlist_sources[%d].source_id must be a positive integer", i)
		}
		if _, exists := seenIDs[update.SourceID]; exists {
			return fmt.Errorf("playlist_sources contains duplicate source_id %d", update.SourceID)
		}
		seenIDs[update.SourceID] = struct{}{}

		name := strings.TrimSpace(update.Name)
		if name == "" {
			return fmt.Errorf("playlist_sources[%d].name is required", i)
		}
		playlistURL := playlist.CanonicalPlaylistSourceURL(update.PlaylistURL)
		if playlistURL == "" && update.SourceID != primaryPlaylistSourceID {
			return fmt.Errorf("playlist_sources[%d].playlist_url is required", i)
		}
		if update.TunerCount < 1 {
			return fmt.Errorf("playlist_sources[%d].tuner_count must be at least 1", i)
		}

		nameKey := playlist.CanonicalPlaylistSourceName(name)
		if prev, exists := seenNames[nameKey]; exists {
			return fmt.Errorf("playlist_sources[%d].name duplicates playlist_sources[%d].name", i, prev)
		}
		seenNames[nameKey] = i

		urlKey := playlist.CanonicalPlaylistSourceURL(playlistURL)
		if prev, exists := seenURLs[urlKey]; exists {
			return fmt.Errorf("playlist_sources[%d].playlist_url duplicates playlist_sources[%d].playlist_url", i, prev)
		}
		seenURLs[urlKey] = i

		if update.Enabled {
			enabledCount++
		}

		normalized = append(normalized, normalizedBulkUpdate{
			SourceID:    update.SourceID,
			Name:        name,
			PlaylistURL: playlistURL,
			TunerCount:  update.TunerCount,
			Enabled:     update.Enabled,
		})
	}

	if _, ok := seenIDs[primaryPlaylistSourceID]; !ok {
		return playlist.ErrPrimaryPlaylistSourceOrderChange
	}
	if enabledCount == 0 {
		return playlist.ErrNoEnabledPlaylistSources
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	currentIDs, err := listPlaylistSourceIDsTx(ctx, tx)
	if err != nil {
		return err
	}
	if len(currentIDs) != len(normalized) {
		return fmt.Errorf("%w: source_ids count mismatch: got %d, want %d", playlist.ErrPlaylistSourceOrderDrift, len(normalized), len(currentIDs))
	}
	for i, sourceID := range currentIDs {
		if _, ok := seenIDs[sourceID]; !ok {
			return fmt.Errorf("%w: source_ids missing id %d", playlist.ErrPlaylistSourceOrderDrift, sourceID)
		}
		if normalized[i].SourceID != sourceID {
			return fmt.Errorf(
				"%w: source_ids order mismatch at index %d: got %d, want %d",
				playlist.ErrPlaylistSourceOrderDrift,
				i,
				normalized[i].SourceID,
				sourceID,
			)
		}
	}

	now := time.Now().UTC().Unix()
	primaryURL := ""
	for _, update := range normalized {
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE playlist_sources
			 SET name = ?,
			     playlist_url = ?,
			     tuner_count = ?,
			     enabled = ?,
			     updated_at = ?
			 WHERE source_id = ?`,
			update.Name,
			update.PlaylistURL,
			update.TunerCount,
			boolToInt(update.Enabled),
			now,
			update.SourceID,
		); err != nil {
			return mapPlaylistSourceMutationError(err, fmt.Sprintf("update playlist source %d", update.SourceID))
		}
		if update.SourceID == primaryPlaylistSourceID {
			primaryURL = update.PlaylistURL
		}
	}

	if err := ensureEnabledPlaylistSourcesTx(ctx, tx); err != nil {
		return err
	}
	if err := upsertSettingTx(ctx, tx, SettingPlaylistURL, primaryURL); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bulk playlist source update: %w", err)
	}
	return nil
}

func (s *Store) DeletePlaylistSource(ctx context.Context, sourceID int64) error {
	if sourceID <= 0 {
		return fmt.Errorf("source_id must be greater than zero")
	}
	if sourceID == primaryPlaylistSourceID {
		return playlist.ErrPrimaryPlaylistSourceDelete
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := getPlaylistSourceByIDTx(ctx, tx, sourceID); err == sql.ErrNoRows {
		return playlist.ErrPlaylistSourceNotFound
	} else if err != nil {
		return fmt.Errorf("lookup playlist source %d: %w", sourceID, err)
	}

	now := time.Now().UTC().Unix()

	generatedChannelIDs, err := listGeneratedChannelIDsByPlaylistSourceTx(ctx, tx, sourceID)
	if err != nil {
		return err
	}
	affectedChannelIDs, err := listChannelIDsByPlaylistSourceTx(ctx, tx, sourceID)
	if err != nil {
		return err
	}

	if err := deletePublishedChannelDVRMappingsByChannelIDsTx(ctx, tx, generatedChannelIDs); err != nil {
		return err
	}
	if err := deleteGeneratedChannelsByPlaylistSourceTx(ctx, tx, sourceID); err != nil {
		return err
	}
	if err := deleteChannelSourcesByPlaylistSourceTx(ctx, tx, sourceID); err != nil {
		return err
	}
	if err := deleteStreamMetricsByPlaylistSourceTx(ctx, tx, sourceID); err != nil {
		return err
	}
	if err := deletePlaylistItemsBySourceTx(ctx, tx, sourceID); err != nil {
		return err
	}
	if err := pruneDeletedPlaylistSourceReferencesTx(ctx, tx, sourceID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM playlist_sources WHERE source_id = ?`, sourceID); err != nil {
		return fmt.Errorf("delete playlist source %d: %w", sourceID, err)
	}

	remainingIDs, err := listPlaylistSourceIDsTx(ctx, tx)
	if err != nil {
		return err
	}
	if len(remainingIDs) == 0 {
		return fmt.Errorf("delete playlist source %d: no playlist sources remain", sourceID)
	}
	if err := applyPlaylistSourceOrderTx(ctx, tx, remainingIDs, now); err != nil {
		return err
	}
	if err := ensureEnabledPlaylistSourcesTx(ctx, tx); err != nil {
		return err
	}
	if err := reorderChannelSourcesForChannelsTx(ctx, tx, affectedChannelIDs, now); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete playlist source %d: %w", sourceID, err)
	}
	return nil
}

func listGeneratedChannelIDsByPlaylistSourceTx(ctx context.Context, tx *sql.Tx, sourceID int64) ([]int64, error) {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT channel_id
		 FROM published_channels
		 WHERE channel_class = ?
		   AND dynamic_item_key IN (
			 SELECT item_key
			 FROM playlist_items
			 WHERE playlist_source_id = ?
		   )
		 ORDER BY channel_id ASC`,
		channels.ChannelClassDynamicGenerated,
		sourceID,
	)
	if err != nil {
		return nil, fmt.Errorf("query generated channel ids for playlist source %d: %w", sourceID, err)
	}
	defer rows.Close()

	channelIDs := make([]int64, 0)
	for rows.Next() {
		var channelID int64
		if err := rows.Scan(&channelID); err != nil {
			return nil, fmt.Errorf("scan generated channel id for playlist source %d: %w", sourceID, err)
		}
		channelIDs = append(channelIDs, channelID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate generated channel ids for playlist source %d: %w", sourceID, err)
	}
	return channelIDs, nil
}

func deletePublishedChannelDVRMappingsByChannelIDsTx(ctx context.Context, tx *sql.Tx, channelIDs []int64) error {
	seen := make(map[int64]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		if channelID <= 0 {
			continue
		}
		if _, exists := seen[channelID]; exists {
			continue
		}
		seen[channelID] = struct{}{}
		if _, err := tx.ExecContext(ctx, `DELETE FROM published_channel_dvr_map WHERE channel_id = ?`, channelID); err != nil {
			return fmt.Errorf("delete dvr mapping for channel %d: %w", channelID, err)
		}
	}
	return nil
}

func listChannelIDsByPlaylistSourceTx(ctx context.Context, tx *sql.Tx, sourceID int64) ([]int64, error) {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT DISTINCT cs.channel_id
		 FROM channel_sources cs
		 JOIN playlist_items p ON p.item_key = cs.item_key
		 WHERE p.playlist_source_id = ?
		 ORDER BY cs.channel_id ASC`,
		sourceID,
	)
	if err != nil {
		return nil, fmt.Errorf("query affected channel ids for playlist source %d: %w", sourceID, err)
	}
	defer rows.Close()

	channelIDs := make([]int64, 0)
	for rows.Next() {
		var channelID int64
		if err := rows.Scan(&channelID); err != nil {
			return nil, fmt.Errorf("scan affected channel id for playlist source %d: %w", sourceID, err)
		}
		channelIDs = append(channelIDs, channelID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate affected channel ids for playlist source %d: %w", sourceID, err)
	}
	return channelIDs, nil
}

func deleteGeneratedChannelsByPlaylistSourceTx(ctx context.Context, tx *sql.Tx, sourceID int64) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM published_channels
		 WHERE channel_class = ?
		   AND dynamic_item_key IN (
			 SELECT item_key
			 FROM playlist_items
			 WHERE playlist_source_id = ?
		   )`,
		channels.ChannelClassDynamicGenerated,
		sourceID,
	); err != nil {
		return fmt.Errorf("delete generated channels for playlist source %d: %w", sourceID, err)
	}
	return nil
}

func deleteChannelSourcesByPlaylistSourceTx(ctx context.Context, tx *sql.Tx, sourceID int64) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM channel_sources
		 WHERE item_key IN (
			 SELECT item_key
			 FROM playlist_items
			 WHERE playlist_source_id = ?
		   )`,
		sourceID,
	); err != nil {
		return fmt.Errorf("delete channel sources for playlist source %d: %w", sourceID, err)
	}
	return nil
}

func deleteStreamMetricsByPlaylistSourceTx(ctx context.Context, tx *sql.Tx, sourceID int64) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM stream_metrics
		 WHERE item_key IN (
			 SELECT item_key
			 FROM playlist_items
			 WHERE playlist_source_id = ?
		   )`,
		sourceID,
	); err != nil {
		return fmt.Errorf("delete stream metrics for playlist source %d: %w", sourceID, err)
	}
	return nil
}

func deletePlaylistItemsBySourceTx(ctx context.Context, tx *sql.Tx, sourceID int64) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM playlist_items WHERE playlist_source_id = ?`, sourceID); err != nil {
		return fmt.Errorf("delete playlist items for source %d: %w", sourceID, err)
	}
	return nil
}

func pruneDeletedPlaylistSourceReferencesTx(ctx context.Context, tx *sql.Tx, sourceID int64) error {
	if err := pruneDynamicChannelQuerySourceIDsTx(ctx, tx, sourceID); err != nil {
		return err
	}
	if err := prunePublishedChannelDynamicSourceIDsTx(ctx, tx, sourceID); err != nil {
		return err
	}
	return nil
}

func pruneDynamicChannelQuerySourceIDsTx(ctx context.Context, tx *sql.Tx, sourceID int64) error {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT query_id, enabled, COALESCE(source_ids_json, '[]')
		 FROM dynamic_channel_queries`,
	)
	if err != nil {
		return fmt.Errorf("query dynamic query source ids for playlist source cleanup %d: %w", sourceID, err)
	}
	defer rows.Close()

	type pending struct {
		queryID   int64
		enabled   bool
		sourceIDs []int64
	}
	updates := make([]pending, 0)

	for rows.Next() {
		var (
			queryID       int64
			enabledInt    int
			sourceIDsJSON string
		)
		if err := rows.Scan(&queryID, &enabledInt, &sourceIDsJSON); err != nil {
			return fmt.Errorf("scan dynamic query source ids for playlist source cleanup %d: %w", sourceID, err)
		}
		nextSourceIDs, changed := removeSourceIDFromJSONList(sourceIDsJSON, sourceID)
		if !changed {
			continue
		}
		enabled := enabledInt != 0
		if len(nextSourceIDs) == 0 {
			enabled = false
		}
		updates = append(updates, pending{queryID: queryID, enabled: enabled, sourceIDs: nextSourceIDs})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate dynamic query source ids for playlist source cleanup %d: %w", sourceID, err)
	}
	if len(updates) == 0 {
		return nil
	}

	stmt, err := tx.PrepareContext(ctx, `
		UPDATE dynamic_channel_queries
		SET source_ids_json = ?,
		    enabled = ?,
		    updated_at = ?
		WHERE query_id = ?
	`)
	if err != nil {
		return fmt.Errorf("prepare dynamic query source-id prune for playlist source %d: %w", sourceID, err)
	}
	defer stmt.Close()

	updatedAt := time.Now().UTC().Unix()
	for _, update := range updates {
		if _, err := stmt.ExecContext(
			ctx,
			marshalSourceIDsJSON(update.sourceIDs),
			boolToInt(update.enabled),
			updatedAt,
			update.queryID,
		); err != nil {
			return fmt.Errorf(
				"prune source_id %d from dynamic query %d source_ids_json: %w",
				sourceID,
				update.queryID,
				err,
			)
		}
	}
	return nil
}

func prunePublishedChannelDynamicSourceIDsTx(ctx context.Context, tx *sql.Tx, sourceID int64) error {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT channel_id, COALESCE(dynamic_sources_enabled, 0), COALESCE(dynamic_source_ids_json, '[]')
		 FROM published_channels`,
	)
	if err != nil {
		return fmt.Errorf("query published channel dynamic source ids for playlist source cleanup %d: %w", sourceID, err)
	}
	defer rows.Close()

	type pending struct {
		channelID int64
		enabled   bool
		sourceIDs []int64
	}
	updates := make([]pending, 0)

	for rows.Next() {
		var (
			channelID     int64
			enabledInt    int
			sourceIDsJSON string
		)
		if err := rows.Scan(&channelID, &enabledInt, &sourceIDsJSON); err != nil {
			return fmt.Errorf("scan published channel dynamic source ids for playlist source cleanup %d: %w", sourceID, err)
		}
		nextSourceIDs, changed := removeSourceIDFromJSONList(sourceIDsJSON, sourceID)
		if !changed {
			continue
		}
		enabled := enabledInt != 0
		if len(nextSourceIDs) == 0 {
			enabled = false
		}
		updates = append(updates, pending{channelID: channelID, enabled: enabled, sourceIDs: nextSourceIDs})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate published channel dynamic source ids for playlist source cleanup %d: %w", sourceID, err)
	}
	if len(updates) == 0 {
		return nil
	}

	stmt, err := tx.PrepareContext(ctx, `
		UPDATE published_channels
		SET dynamic_source_ids_json = ?,
		    dynamic_sources_enabled = ?,
		    updated_at = ?
		WHERE channel_id = ?
	`)
	if err != nil {
		return fmt.Errorf("prepare published channel source-id prune for playlist source %d: %w", sourceID, err)
	}
	defer stmt.Close()

	updatedAt := time.Now().UTC().Unix()
	for _, update := range updates {
		if _, err := stmt.ExecContext(
			ctx,
			marshalSourceIDsJSON(update.sourceIDs),
			boolToInt(update.enabled),
			updatedAt,
			update.channelID,
		); err != nil {
			return fmt.Errorf(
				"prune source_id %d from published channel %d dynamic_source_ids_json: %w",
				sourceID,
				update.channelID,
				err,
			)
		}
	}
	return nil
}

func removeSourceIDFromJSONList(sourceIDsJSON string, targetSourceID int64) ([]int64, bool) {
	sourceIDs := normalizeStoredSourceIDs(sourceIDsJSON)
	if len(sourceIDs) == 0 {
		return sourceIDs, false
	}

	normalized := make([]int64, 0, len(sourceIDs))
	removed := false
	for _, current := range sourceIDs {
		if current == targetSourceID {
			removed = true
			continue
		}
		normalized = append(normalized, current)
	}
	if !removed {
		return sourceIDs, false
	}
	return channels.NormalizeSourceIDs(normalized), true
}

func reorderChannelSourcesForChannelsTx(ctx context.Context, tx *sql.Tx, channelIDs []int64, updatedAt int64) error {
	seen := make(map[int64]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		if channelID <= 0 {
			continue
		}
		if _, exists := seen[channelID]; exists {
			continue
		}
		seen[channelID] = struct{}{}

		exists, err := channelExistsTx(ctx, tx, channelID)
		if err != nil {
			return fmt.Errorf("check channel %d before source reorder: %w", channelID, err)
		}
		if !exists {
			continue
		}

		sourceIDs, err := listSourceIDsTx(ctx, tx, channelID)
		if err != nil {
			return err
		}
		if err := applySourceOrderTx(ctx, tx, channelID, sourceIDs, updatedAt); err != nil {
			return err
		}
	}
	return nil
}

func listPlaylistSourceIDsTx(ctx context.Context, tx *sql.Tx) ([]int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT source_id FROM playlist_sources ORDER BY order_index ASC, source_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("query playlist source ids: %w", err)
	}
	defer rows.Close()

	out := make([]int64, 0)
	for rows.Next() {
		var sourceID int64
		if err := rows.Scan(&sourceID); err != nil {
			return nil, fmt.Errorf("scan playlist source id: %w", err)
		}
		out = append(out, sourceID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate playlist source ids: %w", err)
	}
	return out, nil
}

func applyPlaylistSourceOrderTx(ctx context.Context, tx *sql.Tx, sourceIDs []int64, updatedAt int64) error {
	tempStmt, err := tx.PrepareContext(ctx, `UPDATE playlist_sources SET order_index = ?, updated_at = ? WHERE source_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare temp playlist source reorder statement: %w", err)
	}
	defer tempStmt.Close()

	for i, sourceID := range sourceIDs {
		tempOrderIndex := -(i + 1)
		if _, err := tempStmt.ExecContext(ctx, tempOrderIndex, updatedAt, sourceID); err != nil {
			return fmt.Errorf("set temporary order index for playlist source %d: %w", sourceID, err)
		}
	}

	finalStmt, err := tx.PrepareContext(ctx, `UPDATE playlist_sources SET order_index = ?, updated_at = ? WHERE source_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare final playlist source reorder statement: %w", err)
	}
	defer finalStmt.Close()

	for i, sourceID := range sourceIDs {
		if _, err := finalStmt.ExecContext(ctx, i, updatedAt, sourceID); err != nil {
			return fmt.Errorf("set final order index for playlist source %d: %w", sourceID, err)
		}
	}
	return nil
}

func randomPlaylistSourceKey() (string, error) {
	raw := make([]byte, playlistSourceKeyBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate playlist source key: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

func upsertSettingTx(ctx context.Context, tx *sql.Tx, key, value string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("setting key is required")
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO settings(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key,
		value,
	); err != nil {
		return fmt.Errorf("upsert setting %q: %w", key, err)
	}
	return nil
}

func playlistSourceConstraintField(err error) string {
	if err == nil {
		return ""
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "playlist_sources.source_key"):
		return "source_key"
	case strings.Contains(text, "playlist_sources.name"):
		return "name"
	case strings.Contains(text, "playlist_sources.playlist_url"):
		return "playlist_url"
	case strings.Contains(text, "playlist_sources.order_index"):
		return "order_index"
	default:
		return ""
	}
}

func mapPlaylistSourceMutationError(err error, operation string) error {
	field := playlistSourceConstraintField(err)
	switch field {
	case "name":
		return fmt.Errorf("%s: playlist source name already exists", operation)
	case "playlist_url":
		return fmt.Errorf("%s: playlist source URL already exists", operation)
	case "order_index":
		return fmt.Errorf("%s: playlist source order conflict", operation)
	case "source_key":
		return fmt.Errorf("%s: playlist source key collision", operation)
	default:
		return fmt.Errorf("%s: %w", operation, err)
	}
}

func normalizePlaylistSourceIDs(sourceIDs []int64) ([]int64, error) {
	if len(sourceIDs) == 0 {
		return nil, nil
	}
	out := make([]int64, 0, len(sourceIDs))
	seen := make(map[int64]struct{}, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		if sourceID <= 0 {
			return nil, fmt.Errorf("source_ids must contain positive ids")
		}
		if _, ok := seen[sourceID]; ok {
			continue
		}
		seen[sourceID] = struct{}{}
		out = append(out, sourceID)
	}
	return out, nil
}

func (s *Store) ListActiveGroupNamesBySourceIDs(ctx context.Context, sourceIDs []int64) ([]string, error) {
	normalizedSourceIDs, err := normalizePlaylistSourceIDs(sourceIDs)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT DISTINCT group_name
		FROM playlist_items
		WHERE active = 1
	`
	args := make([]any, 0, len(normalizedSourceIDs))
	if len(normalizedSourceIDs) > 0 {
		query += ` AND playlist_source_id IN (` + inPlaceholders(len(normalizedSourceIDs)) + `)`
		for _, sourceID := range normalizedSourceIDs {
			args = append(args, sourceID)
		}
	}
	query += `
		ORDER BY group_name ASC
	`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query active group names by source ids: %w", err)
	}
	defer rows.Close()

	groups := make([]string, 0)
	for rows.Next() {
		var groupName string
		if err := rows.Scan(&groupName); err != nil {
			return nil, fmt.Errorf("scan active group name by source ids row: %w", err)
		}
		groups = append(groups, strings.TrimSpace(groupName))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active group names by source ids rows: %w", err)
	}
	return groups, nil
}
