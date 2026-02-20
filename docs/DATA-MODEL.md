# Data Model & Persistence

This document describes the SQLite-backed data model that powers HDHomeRun IPTV. All persistent state lives in a single SQLite database managed by the `internal/store/sqlite` package.

## Dual-Key Identity System

Every M3U playlist entry receives two deterministic keys at parse time (`internal/m3u/parser.go`):

### item_key (stream-level identity)

Uniquely identifies a specific stream URL within the catalog. Two items can share the same channel but have different stream URLs (and therefore different item_keys).

Generation logic:

1. **With tvg-id**: `src:<tvg_id_lower>:<sha1(normalized_url)[:12]>`
2. **Without tvg-id**: `src:<sha1(name_lower + "\n" + normalized_url)[:16]>`

### channel_key (channel-level identity)

Groups multiple streams that represent the same logical channel. Used for failover source matching and duplicate detection.

Generation logic:

1. **With tvg-id**: `tvg:<tvg_id_lower>`
2. **Without tvg-id**: `name:<name_lower_normalized>`

### URL Normalization

Before hashing, stream URLs are normalized (`normalizedURLForKey`):

- Parse the URL; if valid, strip query parameters and fragment
- Reconstruct as `scheme://host/path`
- If unparseable, fall back to whitespace-trimmed raw value

### channel_key Normalization (Store Layer)

The store layer (`channel_key.go`) normalizes channel_key values on write to ensure consistent lowercase prefixes:

- `tvg:` prefix: lowercased value after prefix
- `name:` prefix: lowercased value after prefix
- Normalization is applied both in Go code (`normalizeChannelKey`) and via SQL expression (`normalizedChannelKeySQLExpr`) during migrations

## SQLite Configuration

Configured in `store.go:Open()` and `runtime_pragmas.go`:

| Setting | Value | Purpose |
|---------|-------|---------|
| `MaxOpenConns` | 1 | Single connection serializes all access |
| `MaxIdleConns` | 1 | Keep the single connection alive |
| `journal_mode` | WAL | Write-Ahead Logging for concurrent reads during writes |
| `busy_timeout` | 5000 ms | Wait up to 5 s for lock instead of failing immediately |
| `synchronous` | NORMAL | Balance between safety and write performance |
| Driver | `modernc.org/sqlite` | Pure-Go SQLite (no CGO required) |

Foreign key constraints are declared in the schema DDL for several tables (see below), but **are not enforced at runtime** — the application does not execute `PRAGMA foreign_keys = ON`, and `modernc.org/sqlite` defaults this pragma to OFF. `PRAGMA table_info()` is used for runtime column introspection during migrations.

> **Design Decision — Foreign Key Enforcement Intentionally OFF**
>
> FK enforcement is deliberately kept disabled at runtime. `modernc.org/sqlite` defaults `PRAGMA foreign_keys` to OFF, and the application does not enable it. Referential integrity is maintained through compensating controls:
>
> - **Application-level cascade**: service code in handlers and jobs performs ordered deletions (e.g., delete channel sources before channels, delete DVR mappings before DVR instances) replicating the CASCADE/RESTRICT intent declared in DDL.
> - **Non-destructive refresh**: playlist items are never deleted — they are marked `active=0`, preserving FK references from `channel_sources`.
> - **Test coverage**: targeted and full-suite tests validate referential consistency across entity lifecycle operations.
> - Tables with DDL FK declarations: `channel_sources`, `published_channel_dvr_map`, `favorites`.
> - Tables with logical references only (no DDL FK): `stream_metrics`, `published_channels.dynamic_query_id`.
>
> Enabling `PRAGMA foreign_keys = ON` is a future consideration but would require migration testing to verify all existing data satisfies declared constraints and that ordered-operation patterns remain compatible.

## Schema Tables

All tables are defined across `migrations/001_init.sql` through `migrations/006_*.sql`, with additional columns added programmatically via `addColumnIfMissing` in `ensureFailoverSchema`, `ensureMetricsSchema`, and `ensureDVRSchema`.

### settings

Key-value store for application configuration.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `key` | TEXT | PRIMARY KEY | Setting identifier (dotted path) |
| `value` | TEXT | NOT NULL | Setting value (always stored as text) |

Default settings seeded by `002_jobs_metrics.sql` and `003_autoprioritize_scope.sql`:

- `playlist.url` -- M3U playlist URL
- `jobs.timezone` -- Scheduler timezone (default: `America/Chicago`)
- `jobs.playlist_sync.enabled` / `.cron` -- Playlist refresh schedule
- `jobs.auto_prioritize.enabled` / `.cron` -- Auto-prioritize schedule
- `jobs.dvr_lineup_sync.enabled` / `.cron` -- DVR sync schedule
- `analyzer.probe.*` -- FFprobe tuning (timeout, analyzeduration, probesize)
- `analyzer.bitrate_mode` / `analyzer.sample_seconds` -- Bitrate analysis config
- `analyzer.autoprioritize.enabled_only` / `.top_n_per_channel` -- Auto-prioritize scope

Additional identity settings managed at runtime:

- `identity.friendly_name` / `identity.device_id` / `identity.device_auth`

### playlist_items

The catalog of parsed M3U entries. Primary table for the channel catalog.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `item_key` | TEXT | PRIMARY KEY | Deterministic stream identity (see Dual-Key above) |
| `channel_key` | TEXT | | Channel-level grouping key |
| `name` | TEXT | NOT NULL | Display name from M3U `#EXTINF` |
| `group_name` | TEXT | NOT NULL | Group/category from `group-title` attribute |
| `stream_url` | TEXT | NOT NULL | Stream URL |
| `tvg_id` | TEXT | | EPG identifier from `tvg-id` attribute |
| `tvg_name` | TEXT | NOT NULL DEFAULT '' | Display name from `tvg-name` attribute |
| `tvg_logo` | TEXT | | Logo URL from `tvg-logo` attribute |
| `attrs_json` | TEXT | NOT NULL | Full M3U attributes as JSON object |
| `first_seen_at` | INTEGER | | Unix nanos when item first appeared |
| `last_seen_at` | INTEGER | | Unix nanos of most recent refresh containing this item |
| `active` | INTEGER | NOT NULL DEFAULT 1 | 1 = present in latest refresh, 0 = absent |
| `updated_at` | INTEGER | NOT NULL DEFAULT 0 | Legacy compatibility column |

**Indexes:**

- `idx_playlist_channel_key` -- `(channel_key)`
- `idx_playlist_group` -- `(group_name)`
- `idx_playlist_name` -- `(name)`
- `idx_playlist_active` -- `(active)`
- `idx_playlist_active_group_name_name_item` -- `(active, group_name, name, item_key)` -- covers filtered+sorted catalog queries
- `idx_playlist_active_name_item` -- `(active, name, item_key)` -- covers unfiltered catalog listing
- `idx_playlist_active_key_name_item` -- `(active, channel_key, name, item_key)` -- covers channel_key lookups with sort

### published_channels

Published HDHomeRun channels exposed to DVR clients.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `channel_id` | INTEGER | PRIMARY KEY AUTOINCREMENT | Internal channel ID |
| `channel_class` | TEXT | NOT NULL DEFAULT 'traditional' | `traditional` or `dynamic_generated` |
| `channel_key` | TEXT | | Links to `playlist_items.channel_key` |
| `guide_number` | TEXT | NOT NULL | HDHomeRun guide number (e.g., "1001") |
| `guide_name` | TEXT | NOT NULL | Display name in guide |
| `order_index` | INTEGER | NOT NULL | Sort position |
| `enabled` | INTEGER | NOT NULL DEFAULT 1 | Whether channel appears in lineup |
| `dynamic_query_id` | INTEGER | | FK to `dynamic_channel_queries.query_id` |
| `dynamic_item_key` | TEXT | | Seed item for dynamic channel |
| `dynamic_source_identity` | TEXT | NOT NULL DEFAULT '' | Dedup identity for dynamic sources |
| `dynamic_sources_enabled` | INTEGER | NOT NULL DEFAULT 0 | Whether dynamic source management is active |
| `dynamic_group_name` | TEXT | NOT NULL DEFAULT '' | Legacy single group filter |
| `dynamic_group_names_json` | TEXT | NOT NULL DEFAULT '[]' | Multi-group filter (JSON array) |
| `dynamic_search_query` | TEXT | NOT NULL DEFAULT '' | Catalog search filter |
| `dynamic_search_regex` | INTEGER | NOT NULL DEFAULT 0 | Whether search uses regex mode |
| `created_at` | INTEGER | NOT NULL | Unix epoch |
| `updated_at` | INTEGER | NOT NULL | Unix epoch |

**Indexes:**

- `idx_published_channels_guide_number` -- UNIQUE `(guide_number)`
- `idx_published_channels_order` -- UNIQUE `(order_index)`
- `idx_published_channels_channel_key` -- `(channel_key)`
- `idx_published_channels_dynamic_query` -- `(dynamic_query_id)`
- `idx_published_channels_dynamic_query_item` -- UNIQUE partial `(dynamic_query_id, dynamic_item_key)` WHERE both non-null/non-empty
- `idx_published_channels_dynamic_query_source_identity` -- UNIQUE partial `(dynamic_query_id, dynamic_source_identity)` WHERE both non-null/non-empty

### channel_sources

Ordered failover sources attached to published channels.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `source_id` | INTEGER | PRIMARY KEY AUTOINCREMENT | |
| `channel_id` | INTEGER | NOT NULL, FK -> `published_channels` ON DELETE CASCADE | |
| `item_key` | TEXT | NOT NULL, FK -> `playlist_items` ON DELETE RESTRICT | |
| `priority_index` | INTEGER | NOT NULL | Failover priority (0 = highest) |
| `enabled` | INTEGER | NOT NULL DEFAULT 1 | |
| `association_type` | TEXT | NOT NULL DEFAULT 'manual' | `manual`, `channel_key`, `dynamic_query`, or `dynamic_channel_item` |
| `last_ok_at` | INTEGER | | Last successful stream timestamp |
| `last_fail_at` | INTEGER | | Last failure timestamp |
| `last_fail_reason` | TEXT | | Error description |
| `success_count` | INTEGER | NOT NULL DEFAULT 0 | |
| `fail_count` | INTEGER | NOT NULL DEFAULT 0 | |
| `cooldown_until` | INTEGER | NOT NULL DEFAULT 0 | Backoff timestamp |
| `last_probe_at` | INTEGER | NOT NULL DEFAULT 0 | Last stream probe timestamp |
| `profile_width` | INTEGER | NOT NULL DEFAULT 0 | Detected video width |
| `profile_height` | INTEGER | NOT NULL DEFAULT 0 | Detected video height |
| `profile_fps` | REAL | NOT NULL DEFAULT 0 | Detected frame rate |
| `profile_video_codec` | TEXT | NOT NULL DEFAULT '' | e.g., `h264` |
| `profile_audio_codec` | TEXT | NOT NULL DEFAULT '' | e.g., `aac` |
| `profile_bitrate_bps` | INTEGER | NOT NULL DEFAULT 0 | Measured bitrate |
| `created_at` | INTEGER | NOT NULL | |
| `updated_at` | INTEGER | NOT NULL | |

**Constraints:** UNIQUE `(channel_id, item_key)`, UNIQUE `(channel_id, priority_index)`

**Indexes:**

- `idx_channel_sources_channel` -- `(channel_id)`
- `idx_channel_sources_cooldown` -- `(cooldown_until)`

### dynamic_channel_queries

Saved catalog filter queries that auto-generate channels.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `query_id` | INTEGER | PRIMARY KEY AUTOINCREMENT | |
| `enabled` | INTEGER | NOT NULL DEFAULT 1 | |
| `name` | TEXT | NOT NULL DEFAULT '' | Human-readable label |
| `group_name` | TEXT | NOT NULL DEFAULT '' | Legacy single group filter |
| `group_names_json` | TEXT | NOT NULL DEFAULT '[]' | Multi-group filter (JSON array) |
| `search_query` | TEXT | NOT NULL DEFAULT '' | Catalog search text |
| `search_regex` | INTEGER | NOT NULL DEFAULT 0 | Regex search mode flag |
| `order_index` | INTEGER | NOT NULL | Sort position |
| `last_count` | INTEGER | NOT NULL DEFAULT 0 | Channels from last sync |
| `truncated_by` | INTEGER | NOT NULL DEFAULT 0 | Items truncated by slot limits |
| `next_slot_cursor` | INTEGER | NOT NULL DEFAULT 0 | Guide number allocation cursor (max 999) |
| `created_at` | INTEGER | NOT NULL | |
| `updated_at` | INTEGER | NOT NULL | |

**Indexes:**

- `idx_dynamic_channel_queries_order` -- UNIQUE `(order_index)`

### stream_metrics

Cached stream analysis results from FFprobe.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `item_key` | TEXT | PRIMARY KEY | Logical ref to `playlist_items.item_key` (no DDL FK) |
| `analyzed_at` | INTEGER | NOT NULL | Unix epoch of analysis |
| `width` | INTEGER | | Video width |
| `height` | INTEGER | | Video height |
| `fps` | REAL | | Frame rate |
| `bitrate_bps` | INTEGER | | Measured bitrate |
| `variant_bps` | INTEGER | | HLS variant declared bitrate |
| `video_codec` | TEXT | | e.g., `h264`, `hevc` |
| `audio_codec` | TEXT | | e.g., `aac`, `mp3` |
| `score_hint` | REAL | | Computed quality score |
| `error` | TEXT | | Error message if analysis failed |

### job_runs

Execution history for scheduled and manual jobs.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `run_id` | INTEGER | PRIMARY KEY AUTOINCREMENT | |
| `job_name` | TEXT | NOT NULL | e.g., `playlist_sync`, `auto_prioritize` |
| `triggered_by` | TEXT | NOT NULL | `scheduler`, `api`, etc. |
| `started_at` | INTEGER | NOT NULL | Unix epoch |
| `finished_at` | INTEGER | | Unix epoch (NULL while running) |
| `status` | TEXT | NOT NULL | `running`, `success`, `error`, `canceled` |
| `progress_cur` | INTEGER | NOT NULL DEFAULT 0 | Current progress count |
| `progress_max` | INTEGER | NOT NULL DEFAULT 0 | Total expected count |
| `summary` | TEXT | | Human-readable result summary |
| `error` | TEXT | | Error details if failed |

**Indexes:**

- `idx_job_runs_name_time` -- `(job_name, started_at DESC)`
- `idx_job_runs_started_run_id_desc` -- `(started_at DESC, run_id DESC)` -- unfiltered history
- `idx_job_runs_name_started_run_id_desc` -- `(job_name, started_at DESC, run_id DESC)` -- filtered by job name

### dvr_instances

DVR server configuration (singleton pattern enforced via `singleton_key`).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | INTEGER | PRIMARY KEY AUTOINCREMENT | |
| `singleton_key` | INTEGER | NOT NULL DEFAULT 1, UNIQUE | Enforces single row |
| `provider` | TEXT | NOT NULL | `channels` or `jellyfin` |
| `active_providers` | TEXT | NOT NULL DEFAULT '' | Comma-separated active providers |
| `base_url` | TEXT | NOT NULL | Primary DVR URL |
| `channels_base_url` | TEXT | NOT NULL DEFAULT '' | Channels DVR specific URL |
| `jellyfin_base_url` | TEXT | NOT NULL DEFAULT '' | Jellyfin specific URL |
| `default_lineup_id` | TEXT | | Default lineup for sync |
| `sync_enabled` | INTEGER | NOT NULL DEFAULT 0 | |
| `sync_cron` | TEXT | | Cron expression for auto-sync |
| `sync_mode` | TEXT | NOT NULL DEFAULT 'configured_only' | `configured_only` or `mirror_device` |
| `pre_sync_refresh_devices` | INTEGER | NOT NULL DEFAULT 0 | Refresh devices before sync |
| `jellyfin_api_token` | TEXT | NOT NULL DEFAULT '' | |
| `jellyfin_tuner_host_id` | TEXT | NOT NULL DEFAULT '' | |
| `updated_at` | INTEGER | NOT NULL | |

### published_channel_dvr_map

Maps published channels to DVR lineup entries.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `channel_id` | INTEGER | NOT NULL, FK -> `published_channels` ON DELETE CASCADE | |
| `dvr_instance_id` | INTEGER | NOT NULL, FK -> `dvr_instances` ON DELETE CASCADE | |
| `dvr_lineup_id` | TEXT | NOT NULL | Lineup identifier on DVR |
| `dvr_lineup_channel` | TEXT | NOT NULL | Channel number in lineup |
| `dvr_station_ref` | TEXT | | Station reference ID |
| `dvr_callsign_hint` | TEXT | | Callsign hint for guide matching |

**Primary Key:** `(channel_id, dvr_instance_id)`

**Indexes:**

- `idx_channel_dvr_map_instance_lineup` -- `(dvr_instance_id, dvr_lineup_id)`

### favorites (Legacy)

Retained for backward compatibility. Rows are migrated to `published_channels` + `channel_sources` at startup via `migrateLegacyFavorites`. Not used for new data.

> **API Consumer Note**: The favorites compatibility API (`AddFavorite`,
> `RemoveFavorite`, `ListFavorites`, `ReorderFavorites` in
> `favorites.go`) actually operates on `published_channels` +
> `channel_sources` tables, not on the legacy `favorites` table. The
> legacy table is only read once during migration; all runtime favorites
> operations are implemented as channel/source mutations against the
> current schema.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `fav_id` | INTEGER | PRIMARY KEY AUTOINCREMENT | |
| `item_key` | TEXT | NOT NULL, FK -> `playlist_items` ON DELETE CASCADE | |
| `order_index` | INTEGER | NOT NULL, UNIQUE | |
| `guide_number` | TEXT | NOT NULL, UNIQUE | |
| `guide_name` | TEXT | NOT NULL | |
| `enabled` | INTEGER | NOT NULL DEFAULT 1 | |
| `created_at` | INTEGER | NOT NULL | |
| `updated_at` | INTEGER | NOT NULL | |

## Migration System

Migrations use Go's `embed.FS` to bundle SQL files from `migrations/*.sql`. On startup, `Store.migrate()`:

1. Reads all `.sql` files from the embedded `migrations/` directory
2. Sorts filenames lexicographically (so `001_init.sql` runs before `002_jobs_metrics.sql`)
3. Executes each file's SQL against the database
4. All DDL uses `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`, making migrations idempotent

After numbered migrations, programmatic schema extensions run:

- **`ensureFailoverSchema`**: Adds columns (`channel_key`, `tvg_name`, `first_seen_at`, `last_seen_at`, `active`) to `playlist_items`, creates `dynamic_channel_queries`, `published_channels`, and `channel_sources` tables, backfills data, and creates indexes
- **`ensureMetricsSchema`**: Ensures `stream_metrics` table has `video_codec` and `audio_codec` columns
- **`ensureDVRSchema`**: Ensures `dvr_instances` and `published_channel_dvr_map` tables have all required columns, enforces singleton, creates indexes
- **`migrateLegacyFavorites`**: One-time migration of `favorites` rows to `published_channels` + `channel_sources`

### addColumnIfMissing

The helper `addColumnIfMissing(table, column, alterSQL)` uses `PRAGMA table_info()` to check whether a column exists before executing `ALTER TABLE ... ADD COLUMN`. This makes column additions idempotent and safe to re-run.

## Non-Destructive Refresh

Playlist refresh (`upsertPlaylistItemsStream`) follows a non-destructive pattern:

```
BEGIN TRANSACTION
  1. For each item in the incoming stream:
     - UPSERT into playlist_items with ON CONFLICT(item_key) DO UPDATE
     - Set active=1, update last_seen_at to current refresh mark
  2. Mark all items NOT seen in this refresh as inactive:
     UPDATE playlist_items SET active=0
       WHERE last_seen_at <> ? AND active <> 0
COMMIT
```

Key properties:

- **Never deletes rows**: Items that disappear from a playlist are marked `active=0`, preserving history and foreign key references from `channel_sources`
- **Upsert semantics**: Existing items get their metadata updated; new items are inserted
- **Atomic refresh**: The entire refresh runs in a single transaction -- either all items are updated or none are
- **Timestamp-based marking**: Uses a nanosecond-precision refresh mark to distinguish "seen in this refresh" from "not seen"

## Query Patterns

### Catalog Search

The `ListItems` / `ListCatalogItems` methods support two search modes:

**Token mode** (default): Input is parsed into include and exclude terms. Supports OR/pipe disjunction:

- `"sports news"` -- both terms must match (AND)
- `"-replay"` or `"!replay"` -- excludes items containing "replay"
- `"sports | news"` -- either group can match (OR)
- Max 12 terms across max 6 disjunct groups, 64 runes per term
- Each term becomes `LOWER(name) LIKE ? ESCAPE '!'` with `%term%` pattern

**Regex mode** (`search_regex=true`): Input compiled as Go regexp with `(?i)` prefix for case-insensitive matching. Executed via a custom SQLite scalar function `catalog_regex_match(name, pattern)` registered at init time. Compiled patterns are cached (LRU, capacity 256). Max 256 runes.

#### Regex Engine Internals

The custom scalar function `catalog_regex_match` is registered at
package init via `modernsqlite.MustRegisterDeterministicScalarFunction`
(`catalog_search_regex.go`). Key implementation details:

- **Pattern injection**: `buildCatalogRegexPattern` prepends `(?i)` to
  the user-supplied pattern for case-insensitive matching. Pattern
  length is bounded to `maxCatalogRegexPatternRunes` (256 runes).
- **LRU cache**: `compiledCatalogRegexCache` (capacity 256) avoids
  recompilation of recently-used patterns. Cache eviction uses a
  circular-index strategy (`nextEvic`) rather than true LRU tracking.
- **Deterministic**: the function is registered as deterministic, so
  SQLite may cache results within a single query when the same pattern
  is used across multiple rows.

### Paged Listings

Pagination is endpoint/method specific:

- Catalog/channels/sources/DVR-mapping list APIs use `LIMIT ? OFFSET ?` and pair
  list queries with a `COUNT(*)` total query.
- Job-run history (`ListRuns` and `GET /api/admin/jobs`) uses `LIMIT ? OFFSET ?`
  without a total-count query.
- Default limits vary by endpoint (for example: catalog `100`, channels/sources
  `200`, job runs `50`).

Results are ordered deterministically: `ORDER BY group_name ASC, name ASC, item_key ASC` for catalog items; `ORDER BY order_index ASC` for channels.

### Streaming Upsert

Playlist refresh uses `UpsertPlaylistItemsStream`, which accepts an `ItemStream` callback function. Items are processed one at a time within a single prepared statement inside a transaction, keeping memory usage bounded regardless of playlist size.

### Catalog Filter Iteration (Keyset Pagination)

`IterateActiveItemKeysByCatalogFilter` uses keyset pagination (not OFFSET) for
efficient iteration over large result sets. Keyset pagination is used here
(instead of standard `LIMIT/OFFSET`) because dynamic source reconciliation can
iterate tens of thousands of catalog rows: `OFFSET`-based pagination would
require SQLite to materialize and discard all skipped rows on every page, while
keyset pagination uses the active catalog covering indexes
(`idx_playlist_active_name_item` or `idx_playlist_active_group_name_name_item`,
depending on group filters) to seek directly to the resume point.

```sql
WHERE ... AND (name > ? OR (name = ? AND item_key > ?))
ORDER BY name ASC, item_key ASC
LIMIT ?
```

Default page size is 512 rows. This is used by dynamic source synchronization.

## Entity Relationships

```
 +----------------+       +---------------------+       +------------------+
 | playlist_items |       | published_channels  |       | dvr_instances    |
 |----------------|       |---------------------|       |------------------|
 | item_key (PK)  |<--+   | channel_id (PK)     |<--+   | id (PK)          |
 | channel_key    |   |   | channel_class       |   |   | singleton_key    |
 | name           |   |   | channel_key --------+-->|   | provider         |
 | group_name     |   |   | guide_number        |   |   | base_url         |
 | stream_url     |   |   | guide_name          |   |   | sync_enabled     |
 | tvg_id         |   |   | order_index         |   |   | sync_mode        |
 | active         |   |   | enabled             |   |   | ...              |
 | ...            |   |   | dynamic_query_id ---+-->|   +------------------+
 +----------------+   |   | ...                 |   |          |
        |             |   +---------------------+   |          |
        |             |           |                  |          |
        |             |           | 1                |          |
        |             |           |                  |          |
        |             |           v N                |          |
        |             |   +------------------+       |          |
        |             +---| channel_sources  |       |          |
        |                 |------------------|       |          |
        |                 | source_id (PK)   |       |          |
        +<---FK-----------| item_key         |       |          |
                          | channel_id  -----+-------+          |
                          | priority_index   |                  |
                          | association_type |                  |
                          | enabled          |                  |
                          | last_ok_at       |                  |
                          | fail_count       |                  |
                          | profile_*        |                  |
                          +------------------+                  |
                                                                |
 +----------------------------+       +-------------------------+
 | dynamic_channel_queries    |       | published_channel_      |
 |----------------------------|       | dvr_map                 |
 | query_id (PK)              |       |-------------------------|
 | name                       |       | channel_id (FK, PK)     |
 | group_name                 |       | dvr_instance_id (FK,PK) |
 | search_query               |       | dvr_lineup_id           |
 | order_index                |       | dvr_lineup_channel      |
 | last_count                 |       | dvr_station_ref         |
 | ...                        |       +-------------------------+
 +----------------------------+

 +------------------+       +------------------+
 | stream_metrics   |       | settings         |
 |------------------|       |------------------|
 | item_key (PK)    |       | key (PK)         |
 | analyzed_at      |       | value            |
 | width, height    |       +------------------+
 | fps, bitrate_bps |
 | video/audio_codec|       +------------------+
 | score_hint       |       | job_runs         |
 | error            |       |------------------|
 +------------------+       | run_id (PK)      |
                            | job_name         |
                            | status           |
                            | started_at       |
                            | finished_at      |
                            | progress_cur/max |
                            | summary, error   |
                            +------------------+
```

**Reference type legend:**

- **DDL FK (not runtime-enforced)** — A `REFERENCES` clause exists in the `CREATE TABLE` DDL, declaring intent (CASCADE/RESTRICT), but the constraint is not enforced because `PRAGMA foreign_keys` is OFF at runtime.
- **Logical reference** — The column references another table by convention; there is no `REFERENCES` clause in the DDL.

Key relationships:

- `channel_sources.channel_id` -> `published_channels.channel_id` — DDL FK (CASCADE delete, not runtime-enforced)
- `channel_sources.item_key` -> `playlist_items.item_key` — DDL FK (RESTRICT delete, not runtime-enforced)
- `published_channel_dvr_map.channel_id` -> `published_channels.channel_id` — DDL FK (CASCADE delete, not runtime-enforced)
- `published_channel_dvr_map.dvr_instance_id` -> `dvr_instances.id` — DDL FK (CASCADE delete, not runtime-enforced)
- `published_channels.dynamic_query_id` -> `dynamic_channel_queries.query_id` — Logical reference (no DDL FK)
- `stream_metrics.item_key` -> `playlist_items.item_key` — Logical reference (no DDL FK)

## Settings Persistence

The `settings` table stores all application configuration as key-value text pairs. Settings use a dotted-path naming convention organized by subsystem.

**Operations** (`settings.go`):

- `GetSetting(key)` -- single value lookup
- `SetSetting(key, value)` -- upsert single value
- `SetSettings(map)` -- batch upsert in one transaction
- `ListSettings(prefix)` -- list with optional prefix filter (uses `LIKE prefix%`)

**Setting categories:**

| Prefix | Purpose |
|--------|---------|
| `identity.*` | Device identity (friendly_name, device_id, device_auth) |
| `playlist.*` | Playlist URL |
| `jobs.*` | Scheduler configuration (timezone, sync enables/crons) |
| `analyzer.*` | FFprobe tuning and auto-prioritize scope |

## Concurrency

The store uses a **single-connection model** (`SetMaxOpenConns(1)`) which serializes all database access through one connection. Combined with **WAL journal mode**, this provides:

- **Write serialization**: All writes are serialized through the single connection, eliminating write contention
- **No lock contention**: Since there's only one connection, there are no lock conflicts between concurrent goroutines -- they queue at the Go `database/sql` layer
- **WAL benefits**: Even with a single writer, WAL mode provides crash recovery and allows the OS to handle read caching efficiently
- **Busy timeout**: The 5-second `busy_timeout` pragma handles edge cases where the WAL checkpoint or OS-level locks cause brief delays

All multi-step write operations (playlist refresh, legacy migration, DVR singleton enforcement, batch settings update, TVG name backfill) use explicit transactions to ensure atomicity.

## Migration & Backup Checklist

- **Stop the service cleanly before backing up.** A clean shutdown checkpoints the WAL, so the database is a single file.
- **Copy the DB file** (and `-wal`/`-shm` files if present). If WAL/SHM files exist alongside the main DB, copy all three atomically — the DB may be inconsistent without them.
- **Migrations run automatically at startup.** Embedded SQL files (`migrations/*.sql`) execute in lexicographic order, followed by programmatic schema extensions (`ensureFailoverSchema`, `ensureMetricsSchema`, `ensureDVRSchema`, `migrateLegacyFavorites`). No manual migration commands are needed.
- **All DDL is idempotent.** `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, and `addColumnIfMissing` (via `PRAGMA table_info`) ensure re-running migrations is safe.
- **Verify WAL is checkpointed on clean shutdown.** If `-wal` and `-shm` files are absent after stopping the service, the checkpoint completed normally.

## Index & Query Performance Notes

### Index summary by table

| Table | Index | Columns | Type |
|-------|-------|---------|------|
| `playlist_items` | `idx_playlist_channel_key` | `(channel_key)` | regular |
| | `idx_playlist_group` | `(group_name)` | regular |
| | `idx_playlist_name` | `(name)` | regular |
| | `idx_playlist_active` | `(active)` | regular |
| | `idx_playlist_active_group_name_name_item` | `(active, group_name, name, item_key)` | covering — filtered+sorted catalog |
| | `idx_playlist_active_name_item` | `(active, name, item_key)` | covering — unfiltered catalog |
| | `idx_playlist_active_key_name_item` | `(active, channel_key, name, item_key)` | covering — channel_key lookups |
| `published_channels` | `idx_published_channels_guide_number` | `(guide_number)` | UNIQUE |
| | `idx_published_channels_order` | `(order_index)` | UNIQUE |
| | `idx_published_channels_channel_key` | `(channel_key)` | regular |
| | `idx_published_channels_dynamic_query` | `(dynamic_query_id)` | regular |
| | `idx_published_channels_dynamic_query_item` | `(dynamic_query_id, dynamic_item_key)` | UNIQUE partial |
| | `idx_published_channels_dynamic_query_source_identity` | `(dynamic_query_id, dynamic_source_identity)` | UNIQUE partial |
| `channel_sources` | `idx_channel_sources_channel` | `(channel_id)` | regular |
| | `idx_channel_sources_cooldown` | `(cooldown_until)` | regular |
| `dynamic_channel_queries` | `idx_dynamic_channel_queries_order` | `(order_index)` | UNIQUE |
| `job_runs` | `idx_job_runs_name_time` | `(job_name, started_at DESC)` | regular |
| | `idx_job_runs_started_run_id_desc` | `(started_at DESC, run_id DESC)` | regular |
| | `idx_job_runs_name_started_run_id_desc` | `(job_name, started_at DESC, run_id DESC)` | regular |
| `dvr_instances` | `idx_dvr_instances_singleton_key` | `(singleton_key)` | UNIQUE |
| `published_channel_dvr_map` | `idx_channel_dvr_map_instance_lineup` | `(dvr_instance_id, dvr_lineup_id)` | regular |
| `favorites` | `idx_favorites_order` | `(order_index)` | UNIQUE |
| | `idx_favorites_guidenum` | `(guide_number)` | UNIQUE |
| | `idx_favorites_item_key` | `(item_key)` | UNIQUE |

### Search performance characteristics

- **Token mode** (default): Each term generates a `LOWER(name) LIKE '%term%' ESCAPE '!'` clause. Substring `LIKE` with a leading wildcard cannot use B-tree indexes, so each term requires a filtered scan of active rows. Multiple AND terms narrow results progressively but the first term still scans.
- **Regex mode**: Uses a custom SQLite scalar function (`catalog_regex_match`). This always performs a full table scan of active rows — there is no index acceleration for regex evaluation.
- **High-offset pagination**: `LIMIT ? OFFSET ?` queries with large offsets require SQLite to materialize and skip rows, which can spill to a temp B-tree on large result sets. The `IterateActiveItemKeysByCatalogFilter` method avoids this via keyset pagination.
- **Single-connection serialization**: All queries (reads and writes) flow through one `database/sql` connection. Writes never contend with each other, but reads queue behind an in-progress write transaction (e.g., playlist sync). The 5 s `busy_timeout` handles brief WAL checkpoint delays.

## Active vs Legacy Table Reference

| Table | Status | Write profile |
|-------|--------|---------------|
| `playlist_items` | **active** | High-write during playlist sync (full upsert + mark-inactive per refresh) |
| `published_channels` | **active** | Moderate — channel creation, reorder, dynamic sync |
| `channel_sources` | **active** | Moderate — source add/remove, failover stats updates |
| `dynamic_channel_queries` | **active** | Low — query CRUD |
| `stream_metrics` | **active** | Moderate — written during stream analysis jobs |
| `dvr_instances` | **active** | Low — singleton config updates |
| `published_channel_dvr_map` | **active** | Low — DVR lineup sync |
| `settings` | **active** | Low — config upserts |
| `job_runs` | **active** | High-write — row per job execution, progress updates during runs |
| `favorites` | **legacy/deprecated** | No new writes — rows migrated to `published_channels` + `channel_sources` at startup via `migrateLegacyFavorites`; table retained for backward-compatible schema opens |
