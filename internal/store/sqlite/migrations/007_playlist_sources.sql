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
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_playlist_sources_order
  ON playlist_sources(order_index);

CREATE INDEX IF NOT EXISTS idx_playlist_sources_enabled_order
  ON playlist_sources(enabled, order_index);
