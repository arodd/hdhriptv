CREATE INDEX IF NOT EXISTS idx_playlist_active_name_item
  ON playlist_items(active, name, item_key);
