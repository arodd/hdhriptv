CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS playlist_items (
  item_key      TEXT PRIMARY KEY,
  channel_key   TEXT,
  name          TEXT NOT NULL,
  group_name    TEXT NOT NULL,
  stream_url    TEXT NOT NULL,
  tvg_id        TEXT,
  tvg_logo      TEXT,
  attrs_json    TEXT NOT NULL,
  first_seen_at INTEGER,
  last_seen_at  INTEGER,
  active        INTEGER NOT NULL DEFAULT 1,
  -- kept for compatibility with prior schema versions
  updated_at    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_playlist_channel_key ON playlist_items(channel_key);
CREATE INDEX IF NOT EXISTS idx_playlist_group ON playlist_items(group_name);
CREATE INDEX IF NOT EXISTS idx_playlist_name ON playlist_items(name);
CREATE INDEX IF NOT EXISTS idx_playlist_active ON playlist_items(active);
CREATE INDEX IF NOT EXISTS idx_playlist_active_group_name_name_item ON playlist_items(active, group_name, name, item_key);
CREATE INDEX IF NOT EXISTS idx_playlist_active_key_name_item ON playlist_items(active, channel_key, name, item_key);

CREATE TABLE IF NOT EXISTS dynamic_channel_queries (
  query_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  enabled       INTEGER NOT NULL DEFAULT 1,
  name          TEXT NOT NULL DEFAULT '',
  group_name    TEXT NOT NULL DEFAULT '',
  group_names_json TEXT NOT NULL DEFAULT '[]',
  search_query  TEXT NOT NULL DEFAULT '',
  order_index   INTEGER NOT NULL,
  last_count    INTEGER NOT NULL DEFAULT 0,
  truncated_by  INTEGER NOT NULL DEFAULT 0,
  next_slot_cursor INTEGER NOT NULL DEFAULT 0,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dynamic_channel_queries_order
  ON dynamic_channel_queries(order_index);

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
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_published_channels_guide_number ON published_channels(guide_number);
CREATE UNIQUE INDEX IF NOT EXISTS idx_published_channels_order ON published_channels(order_index);
CREATE INDEX IF NOT EXISTS idx_published_channels_channel_key ON published_channels(channel_key);

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
);

CREATE INDEX IF NOT EXISTS idx_channel_sources_channel ON channel_sources(channel_id);
CREATE INDEX IF NOT EXISTS idx_channel_sources_cooldown ON channel_sources(cooldown_until);

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
);

CREATE TABLE IF NOT EXISTS published_channel_dvr_map (
  channel_id         INTEGER NOT NULL REFERENCES published_channels(channel_id) ON DELETE CASCADE,
  dvr_instance_id    INTEGER NOT NULL REFERENCES dvr_instances(id) ON DELETE CASCADE,
  dvr_lineup_id      TEXT NOT NULL,
  dvr_lineup_channel TEXT NOT NULL,
  dvr_station_ref    TEXT,
  dvr_callsign_hint  TEXT,
  PRIMARY KEY(channel_id, dvr_instance_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_dvr_map_instance_lineup
  ON published_channel_dvr_map(dvr_instance_id, dvr_lineup_id);

-- Legacy table retained so existing databases continue to open without destructive migration.
-- Rows are migrated into published_channels/channel_sources at runtime.
CREATE TABLE IF NOT EXISTS favorites (
  fav_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  item_key      TEXT NOT NULL REFERENCES playlist_items(item_key) ON DELETE CASCADE,
  order_index   INTEGER NOT NULL,
  guide_number  TEXT NOT NULL,
  guide_name    TEXT NOT NULL,
  enabled       INTEGER NOT NULL DEFAULT 1,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_favorites_order ON favorites(order_index);
CREATE UNIQUE INDEX IF NOT EXISTS idx_favorites_guidenum ON favorites(guide_number);
CREATE UNIQUE INDEX IF NOT EXISTS idx_favorites_item_key ON favorites(item_key);
