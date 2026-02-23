package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
	"github.com/arodd/hdhriptv/internal/dvr"
)

const (
	legacyDefaultDVRBaseURL = "http://channels.lan:8089"
	dvrInstanceSingletonKey = 1
)

func (s *Store) ensureDVRInstanceSingleton(ctx context.Context) error {
	now := time.Now().UTC().Unix()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO dvr_instances (
			singleton_key,
			provider,
			active_providers,
			base_url,
			channels_base_url,
			jellyfin_base_url,
			default_lineup_id,
			sync_enabled,
			sync_cron,
			sync_mode,
			pre_sync_refresh_devices,
			jellyfin_api_token,
			jellyfin_tuner_host_id,
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, '', 0, '', ?, 0, '', '', ?)
		ON CONFLICT(singleton_key) DO NOTHING
	`,
		dvrInstanceSingletonKey,
		string(dvr.ProviderChannels),
		"",
		"",
		"",
		"",
		string(dvr.SyncModeConfiguredOnly),
		now,
	); err != nil {
		return fmt.Errorf("ensure default dvr instance: %w", err)
	}
	return nil
}

func (s *Store) GetDVRInstance(ctx context.Context) (dvr.InstanceConfig, error) {
	instance, err := s.getDVRInstanceByID(ctx, 0)
	if err != nil {
		if err == sql.ErrNoRows {
			return dvr.InstanceConfig{}, fmt.Errorf("query canonical dvr instance (singleton missing): %w", err)
		}
		return dvr.InstanceConfig{}, fmt.Errorf("query dvr instance: %w", err)
	}
	return instance, nil
}

func (s *Store) UpsertDVRInstance(ctx context.Context, instance dvr.InstanceConfig) (dvr.InstanceConfig, error) {
	instance = dvr.NormalizeStoredInstance(instance)

	now := time.Now().UTC().Unix()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO dvr_instances (
			singleton_key,
			provider,
			active_providers,
			base_url,
			channels_base_url,
			jellyfin_base_url,
			default_lineup_id,
			sync_enabled,
			sync_cron,
			sync_mode,
			pre_sync_refresh_devices,
			jellyfin_api_token,
			jellyfin_tuner_host_id,
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(singleton_key) DO UPDATE SET
			provider = excluded.provider,
			active_providers = excluded.active_providers,
			base_url = excluded.base_url,
			channels_base_url = excluded.channels_base_url,
			jellyfin_base_url = excluded.jellyfin_base_url,
			default_lineup_id = excluded.default_lineup_id,
			sync_enabled = excluded.sync_enabled,
			sync_cron = excluded.sync_cron,
			sync_mode = excluded.sync_mode,
			pre_sync_refresh_devices = excluded.pre_sync_refresh_devices,
			jellyfin_api_token = excluded.jellyfin_api_token,
			jellyfin_tuner_host_id = excluded.jellyfin_tuner_host_id,
			updated_at = excluded.updated_at
	`,
		dvrInstanceSingletonKey,
		string(instance.Provider),
		joinDVRProvidersCSV(instance.ActiveProviders),
		instance.BaseURL,
		instance.ChannelsBaseURL,
		instance.JellyfinBaseURL,
		instance.DefaultLineupID,
		boolToInt(instance.SyncEnabled),
		instance.SyncCron,
		string(instance.SyncMode),
		boolToInt(instance.PreSyncRefreshDevices),
		instance.JellyfinAPIToken,
		instance.JellyfinTunerHostID,
		now,
	); err != nil {
		return dvr.InstanceConfig{}, fmt.Errorf("upsert dvr instance: %w", err)
	}

	return s.getDVRInstanceByID(ctx, 0)
}

func (s *Store) ListChannelsForDVRSync(
	ctx context.Context,
	dvrInstanceID int64,
	enabledOnly bool,
	includeDynamic bool,
) ([]dvr.ChannelMapping, error) {
	filterSQL, filterArgs := dvrSyncChannelFilterSQL(enabledOnly, includeDynamic)
	query := `
		SELECT
			pc.channel_id,
			pc.guide_number,
			pc.guide_name,
			pc.enabled,
			COALESCE(m.dvr_lineup_id, ''),
			COALESCE(m.dvr_lineup_channel, ''),
			COALESCE(m.dvr_station_ref, ''),
			COALESCE(m.dvr_callsign_hint, '')
		FROM published_channels pc
		LEFT JOIN published_channel_dvr_map m
		  ON m.channel_id = pc.channel_id
		 AND m.dvr_instance_id = ?
	`
	args := make([]any, 0, 1+len(filterArgs))
	args = append(args, dvrInstanceID)
	args = append(args, filterArgs...)
	query += filterSQL
	query += `ORDER BY pc.order_index ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query channels for dvr sync: %w", err)
	}
	defer rows.Close()

	out := make([]dvr.ChannelMapping, 0)
	for rows.Next() {
		var (
			mapping    dvr.ChannelMapping
			enabledInt int
		)
		if err := rows.Scan(
			&mapping.ChannelID,
			&mapping.GuideNumber,
			&mapping.GuideName,
			&enabledInt,
			&mapping.DVRLineupID,
			&mapping.DVRLineupChannel,
			&mapping.DVRStationRef,
			&mapping.DVRCallsignHint,
		); err != nil {
			return nil, fmt.Errorf("scan channel for dvr sync: %w", err)
		}

		mapping.Enabled = enabledInt != 0
		mapping.DVRInstanceID = dvrInstanceID
		out = append(out, mapping)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channels for dvr sync: %w", err)
	}
	return out, nil
}

func (s *Store) ListChannelsForDVRSyncPaged(
	ctx context.Context,
	dvrInstanceID int64,
	enabledOnly bool,
	includeDynamic bool,
	limit int,
	offset int,
) ([]dvr.ChannelMapping, int, error) {
	if limit < 1 {
		limit = 1
	}
	if offset < 0 {
		offset = 0
	}

	filterSQL, filterArgs := dvrSyncChannelFilterSQL(enabledOnly, includeDynamic)
	total, err := s.listDVRSyncChannelTotal(ctx, filterSQL, filterArgs)
	if err != nil {
		return nil, 0, err
	}

	selectArgs := make([]any, 0, 1+len(filterArgs)+2)
	selectArgs = append(selectArgs, dvrInstanceID)
	selectArgs = append(selectArgs, filterArgs...)
	selectArgs = append(selectArgs, limit, offset)

	rows, err := s.db.QueryContext(
		ctx,
		`
		SELECT
			pc.channel_id,
			pc.guide_number,
			pc.guide_name,
			pc.enabled,
			COALESCE(m.dvr_lineup_id, ''),
			COALESCE(m.dvr_lineup_channel, ''),
			COALESCE(m.dvr_station_ref, ''),
			COALESCE(m.dvr_callsign_hint, '')
		FROM published_channels pc
		LEFT JOIN published_channel_dvr_map m
		  ON m.channel_id = pc.channel_id
		 AND m.dvr_instance_id = ?
		`+filterSQL+`
		ORDER BY pc.order_index ASC
		LIMIT ? OFFSET ?
	`,
		selectArgs...,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("query paged channels for dvr sync: %w", err)
	}
	defer rows.Close()

	out := make([]dvr.ChannelMapping, 0, limit)
	for rows.Next() {
		var (
			mapping    dvr.ChannelMapping
			enabledInt int
		)
		if err := rows.Scan(
			&mapping.ChannelID,
			&mapping.GuideNumber,
			&mapping.GuideName,
			&enabledInt,
			&mapping.DVRLineupID,
			&mapping.DVRLineupChannel,
			&mapping.DVRStationRef,
			&mapping.DVRCallsignHint,
		); err != nil {
			return nil, 0, fmt.Errorf("scan paged channel for dvr sync: %w", err)
		}
		mapping.Enabled = enabledInt != 0
		mapping.DVRInstanceID = dvrInstanceID
		out = append(out, mapping)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate paged channels for dvr sync: %w", err)
	}
	return out, total, nil
}

func (s *Store) GetChannelDVRMapping(
	ctx context.Context,
	dvrInstanceID,
	channelID int64,
) (dvr.ChannelMapping, error) {
	var (
		mapping    dvr.ChannelMapping
		enabledInt int
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT
			pc.channel_id,
			pc.guide_number,
			pc.guide_name,
			pc.enabled,
			COALESCE(m.dvr_lineup_id, ''),
			COALESCE(m.dvr_lineup_channel, ''),
			COALESCE(m.dvr_station_ref, ''),
			COALESCE(m.dvr_callsign_hint, '')
		FROM published_channels pc
		LEFT JOIN published_channel_dvr_map m
		  ON m.channel_id = pc.channel_id
		 AND m.dvr_instance_id = ?
		WHERE pc.channel_id = ?
	`,
		dvrInstanceID,
		channelID,
	).Scan(
		&mapping.ChannelID,
		&mapping.GuideNumber,
		&mapping.GuideName,
		&enabledInt,
		&mapping.DVRLineupID,
		&mapping.DVRLineupChannel,
		&mapping.DVRStationRef,
		&mapping.DVRCallsignHint,
	)
	if err == sql.ErrNoRows {
		return dvr.ChannelMapping{}, channels.ErrChannelNotFound
	}
	if err != nil {
		return dvr.ChannelMapping{}, fmt.Errorf("query channel dvr mapping: %w", err)
	}

	mapping.Enabled = enabledInt != 0
	mapping.DVRInstanceID = dvrInstanceID
	return mapping, nil
}

func (s *Store) UpsertChannelDVRMapping(
	ctx context.Context,
	mapping dvr.ChannelMapping,
) (dvr.ChannelMapping, error) {
	if mapping.ChannelID <= 0 {
		return dvr.ChannelMapping{}, fmt.Errorf("channel_id must be greater than zero")
	}
	if mapping.DVRInstanceID <= 0 {
		return dvr.ChannelMapping{}, fmt.Errorf("dvr_instance_id must be greater than zero")
	}

	mapping.DVRLineupID = strings.TrimSpace(mapping.DVRLineupID)
	mapping.DVRLineupChannel = strings.TrimSpace(mapping.DVRLineupChannel)
	mapping.DVRStationRef = strings.TrimSpace(mapping.DVRStationRef)
	mapping.DVRCallsignHint = strings.TrimSpace(mapping.DVRCallsignHint)
	if mapping.DVRLineupID == "" {
		return dvr.ChannelMapping{}, fmt.Errorf("dvr_lineup_id is required")
	}
	if mapping.DVRLineupChannel == "" && mapping.DVRStationRef == "" {
		return dvr.ChannelMapping{}, fmt.Errorf("dvr_lineup_channel or dvr_station_ref is required")
	}

	var exists int
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT 1 FROM published_channels WHERE channel_id = ?`,
		mapping.ChannelID,
	).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return dvr.ChannelMapping{}, channels.ErrChannelNotFound
		}
		return dvr.ChannelMapping{}, fmt.Errorf("validate channel exists: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO published_channel_dvr_map (
			channel_id,
			dvr_instance_id,
			dvr_lineup_id,
			dvr_lineup_channel,
			dvr_station_ref,
			dvr_callsign_hint
		)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(channel_id, dvr_instance_id) DO UPDATE SET
			dvr_lineup_id = excluded.dvr_lineup_id,
			dvr_lineup_channel = excluded.dvr_lineup_channel,
			dvr_station_ref = excluded.dvr_station_ref,
			dvr_callsign_hint = excluded.dvr_callsign_hint
	`,
		mapping.ChannelID,
		mapping.DVRInstanceID,
		mapping.DVRLineupID,
		mapping.DVRLineupChannel,
		mapping.DVRStationRef,
		mapping.DVRCallsignHint,
	); err != nil {
		return dvr.ChannelMapping{}, fmt.Errorf("upsert channel dvr mapping: %w", err)
	}

	return s.GetChannelDVRMapping(ctx, mapping.DVRInstanceID, mapping.ChannelID)
}

func (s *Store) DeleteChannelDVRMapping(ctx context.Context, dvrInstanceID, channelID int64) error {
	if channelID <= 0 {
		return fmt.Errorf("channel_id must be greater than zero")
	}
	if dvrInstanceID <= 0 {
		return fmt.Errorf("dvr_instance_id must be greater than zero")
	}

	var exists int
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT 1 FROM published_channels WHERE channel_id = ?`,
		channelID,
	).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return channels.ErrChannelNotFound
		}
		return fmt.Errorf("validate channel exists: %w", err)
	}

	if _, err := s.db.ExecContext(
		ctx,
		`DELETE FROM published_channel_dvr_map WHERE channel_id = ? AND dvr_instance_id = ?`,
		channelID,
		dvrInstanceID,
	); err != nil {
		return fmt.Errorf("delete channel dvr mapping: %w", err)
	}
	return nil
}

func (s *Store) getDVRInstanceByID(ctx context.Context, instanceID int64) (dvr.InstanceConfig, error) {
	query := `
		SELECT
			id,
			provider,
			COALESCE(active_providers, ''),
			base_url,
			COALESCE(channels_base_url, ''),
			COALESCE(jellyfin_base_url, ''),
			COALESCE(default_lineup_id, ''),
			sync_enabled,
			COALESCE(sync_cron, ''),
			sync_mode,
			pre_sync_refresh_devices,
			COALESCE(jellyfin_api_token, ''),
			COALESCE(jellyfin_tuner_host_id, ''),
			updated_at
		FROM dvr_instances
	`
	args := []any{}
	if instanceID > 0 {
		query += `WHERE id = ? `
		args = append(args, instanceID)
	} else {
		query += `WHERE singleton_key = ? `
		args = append(args, dvrInstanceSingletonKey)
	}
	query += `ORDER BY id ASC LIMIT 1`

	var (
		instance      dvr.InstanceConfig
		providerRaw   string
		activeRaw     string
		syncModeRaw   string
		syncEnabled   int
		preSyncEnable int
	)
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(
		&instance.ID,
		&providerRaw,
		&activeRaw,
		&instance.BaseURL,
		&instance.ChannelsBaseURL,
		&instance.JellyfinBaseURL,
		&instance.DefaultLineupID,
		&syncEnabled,
		&instance.SyncCron,
		&syncModeRaw,
		&preSyncEnable,
		&instance.JellyfinAPIToken,
		&instance.JellyfinTunerHostID,
		&instance.UpdatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return dvr.InstanceConfig{}, sql.ErrNoRows
		}
		return dvr.InstanceConfig{}, err
	}

	instance.Provider = dvr.NormalizeProviderType(dvr.ProviderType(providerRaw))
	parsedActiveProviders := parseDVRProvidersCSV(activeRaw)
	instance.ActiveProviders = parsedActiveProviders
	instance.SyncMode = dvr.SyncMode(syncModeRaw)
	instance.SyncEnabled = syncEnabled != 0
	instance.PreSyncRefreshDevices = preSyncEnable != 0

	// Legacy cleanup: older builds persisted channels.lan as a default sentinel.
	// Keep trimming that placeholder when the row still looks untouched while
	// avoiding configured rows that explicitly selected active providers.
	if instance.Provider == dvr.ProviderChannels &&
		len(parsedActiveProviders) == 0 &&
		instance.BaseURL == legacyDefaultDVRBaseURL &&
		instance.ChannelsBaseURL == legacyDefaultDVRBaseURL &&
		!instance.SyncEnabled &&
		strings.TrimSpace(instance.SyncCron) == "" &&
		!instance.PreSyncRefreshDevices &&
		instance.DefaultLineupID == "" &&
		instance.JellyfinBaseURL == "" &&
		instance.JellyfinAPIToken == "" {
		instance.BaseURL = ""
		instance.ChannelsBaseURL = ""
	}
	instance = dvr.NormalizeStoredInstance(instance)
	return instance, nil
}

func dvrSyncChannelFilterSQL(enabledOnly bool, includeDynamic bool) (string, []any) {
	clauses := make([]string, 0, 2)
	args := make([]any, 0, 1)
	if enabledOnly {
		clauses = append(clauses, "pc.enabled = 1")
	}
	if !includeDynamic {
		clauses = append(clauses, "COALESCE(pc.channel_class, '') <> ?")
		args = append(args, channels.ChannelClassDynamicGenerated)
	}
	if len(clauses) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(clauses, " AND ") + " ", args
}

func (s *Store) listDVRSyncChannelTotal(ctx context.Context, filterSQL string, filterArgs []any) (int, error) {
	args := append([]any{}, filterArgs...)
	var total int
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM published_channels pc `+filterSQL,
		args...,
	).Scan(&total); err != nil {
		return 0, fmt.Errorf("count channels for dvr sync: %w", err)
	}
	return total, nil
}

func parseDVRProvidersCSV(raw string) []dvr.ProviderType {
	parts := strings.Split(strings.TrimSpace(raw), ",")
	out := make([]dvr.ProviderType, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		out = append(out, dvr.NormalizeProviderType(dvr.ProviderType(value)))
	}
	return out
}

func joinDVRProvidersCSV(providers []dvr.ProviderType) string {
	if len(providers) == 0 {
		return ""
	}
	values := make([]string, 0, len(providers))
	for _, provider := range providers {
		values = append(values, string(dvr.NormalizeProviderType(provider)))
	}
	return strings.Join(values, ",")
}
