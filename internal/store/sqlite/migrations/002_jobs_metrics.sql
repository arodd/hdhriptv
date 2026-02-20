CREATE TABLE IF NOT EXISTS job_runs (
  run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  job_name     TEXT NOT NULL,
  triggered_by TEXT NOT NULL,
  started_at   INTEGER NOT NULL,
  finished_at  INTEGER,
  status       TEXT NOT NULL,
  progress_cur INTEGER NOT NULL DEFAULT 0,
  progress_max INTEGER NOT NULL DEFAULT 0,
  summary      TEXT,
  error        TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_runs_name_time ON job_runs(job_name, started_at DESC);

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
);

INSERT OR IGNORE INTO settings(key, value) VALUES
  ('playlist.url', ''),
  ('jobs.timezone', 'America/Chicago'),
  ('jobs.playlist_sync.enabled', 'true'),
  ('jobs.playlist_sync.cron', '*/30 * * * *'),
  ('jobs.auto_prioritize.enabled', 'false'),
  ('jobs.auto_prioritize.cron', '30 3 * * *'),
  ('jobs.dvr_lineup_sync.enabled', 'false'),
  ('jobs.dvr_lineup_sync.cron', '*/30 * * * *'),
  ('analyzer.probe.timeout_ms', '7000'),
  ('analyzer.probe.analyzeduration_us', '1500000'),
  ('analyzer.probe.probesize_bytes', '1000000'),
  ('analyzer.bitrate_mode', 'metadata_then_sample'),
  ('analyzer.sample_seconds', '3');
