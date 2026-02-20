# Catalog Pipeline

This document describes the three subsystems that form the M3U-to-channels
data pipeline: parsing (`internal/m3u`), playlist management
(`internal/playlist`), and source reconciliation (`internal/reconcile`).

Source files:

- `internal/m3u/parser.go` — M3U format parser and key generation
- `internal/playlist/types.go` — shared types (`Item`, `ItemStream`, `Group`, `Query`)
- `internal/playlist/manager.go` — HTTP fetch + parse wrapper
- `internal/playlist/refresh.go` — fetch + persist orchestrator with streaming support
- `internal/reconcile/reconcile.go` — channel source reconciliation engine

## End-to-End Data Flow

```
                       ┌────────────────────────────┐
                       │   Upstream M3U Playlist     │
                       └─────────────┬──────────────┘
                                     │ HTTP GET
                                     v
                       ┌────────────────────────────┐
                       │  playlist.Manager           │
                       │  (fetch + status check)     │
                       └─────────────┬──────────────┘
                                     │ io.Reader
                                     v
                       ┌────────────────────────────┐
                       │  m3u.ParseEach              │
                       │  (line scanner, #EXTINF     │
                       │   attribute extraction,     │
                       │   key generation)           │
                       └─────────────┬──────────────┘
                                     │ m3u.Item stream
                                     v
                       ┌────────────────────────────┐
                       │  playlist.Refresher         │
                       │  (streaming or batch        │
                       │   catalog upsert)           │
                       └─────────────┬──────────────┘
                                     │ catalog rows in SQLite
                                     v
                       ┌────────────────────────────┐
                       │  reconcile.Service          │
                       │  ┌──────────────────────┐  │
                       │  │ SyncDynamicChannel-   │  │
                       │  │ Blocks (10000+ guide) │  │
                       │  └──────────┬───────────┘  │
                       │             v              │
                       │  ┌──────────────────────┐  │
                       │  │ Per-channel reconcile │  │
                       │  │ (static or dynamic)   │  │
                       │  └──────────────────────┘  │
                       └─────────────┬──────────────┘
                                     │
                       ┌─────────────┴──────────────┐
                       v                            v
              published_channels            channel_sources
              (guide numbers)               (ordered failover)
```

Trigger sources:

- Startup one-shot sync (`cmd/hdhriptv/main.go`)
- Scheduled cron trigger via `internal/scheduler/scheduler.go`
- Manual trigger: `POST /api/admin/jobs/playlist-sync/run` (automation API)

The playlist sync job (`internal/jobs/playlist_sync.go`) orchestrates the
full pipeline: refresh catalog, reconcile sources, then optionally reload
DVR lineup.

The job requires `playlist.url` to be configured (via the `PLAYLIST_URL`
environment variable or persisted settings). When the setting is missing or
blank, the job returns an error immediately.

## M3U Parser (`internal/m3u`)

### Format Support

The parser processes standard M3U/M3U8 files with `#EXTINF` metadata lines.
Lines are scanned with a `bufio.Scanner` configured with a 64 KB initial
buffer and a 2 MB maximum line length.

Processing rules:

- Blank lines are skipped.
- `#EXTINF:` lines are parsed for attributes and the display name.
- All other `#`-prefixed lines (e.g. `#EXTM3U`, `#EXTVLCOPT`) are ignored.
- A non-comment, non-blank line following a `#EXTINF` is treated as the
  stream URL.
- URLs that appear without a preceding `#EXTINF` are skipped.

### Attribute Extraction

`#EXTINF` attributes are extracted via regex matching of `key="value"` pairs.
Attribute keys are lowercased on extraction to normalize casing differences
across providers. The display name is the text after the last comma on the
`#EXTINF` line.

Well-known attributes used downstream:

| Attribute      | Field     | Usage                                |
|----------------|-----------|--------------------------------------|
| `tvg-id`       | `TVGID`   | EPG identifier, key generation input |
| `tvg-logo`     | `TVGLogo` | Logo URL for UI display              |
| `group-title`  | `Group`   | Playlist group/category name         |

The full attribute map is preserved in `Attrs` and persisted as
`attrs_json` in the catalog for downstream consumers.

### Dual-Key Identity System

Every parsed entry receives two deterministic keys that never change for a
given input combination:

#### channel_key (channel-level grouping)

Groups multiple streams that represent the same logical channel. Used for
static-mode source matching and duplicate detection.

| Condition            | Format                           |
|----------------------|----------------------------------|
| `tvg-id` present     | `tvg:<lowered_tvg_id>`           |
| `tvg-id` absent      | `name:<normalized_name>`         |

Name normalization: lowercase, collapse whitespace, trim.

#### item_key (stream-level identity)

Uniquely identifies a specific stream URL within the catalog. Two items
sharing the same `channel_key` will have different `item_key` values when
their stream URLs differ.

| Condition            | Format                                             |
|----------------------|----------------------------------------------------|
| `tvg-id` present     | `src:<tvg_id_lower>:<sha1(normalized_url)[:12]>`   |
| `tvg-id` absent      | `src:<sha1(name_lower + "\n" + normalized_url)[:16]>` |

The 12-character vs 16-character hex prefix length encodes whether the
original entry had a `tvg-id` attribute, and provides collision resistance
within each category.

### URL Normalization

Before hashing, stream URLs are normalized by `normalizedURLForKey`:

1. Parse the URL. If the URL has a valid scheme and host, strip query
   parameters and fragment, then reconstruct as `scheme://host/path`.
2. If unparseable, fall back to the whitespace-trimmed raw value.

This ensures that URL variations differing only in query parameters or
fragments produce the same `item_key`, giving stable identity across
provider URL rotation.

### Streaming vs Batch API

| Function    | Signature                                            | Usage                     |
|-------------|------------------------------------------------------|---------------------------|
| `Parse`     | `(io.Reader) -> ([]Item, error)`                     | Collects all items in memory |
| `ParseEach` | `(io.Reader, func(Item) error) -> (int, error)`      | Emits items incrementally |

`Parse` is implemented in terms of `ParseEach`. The streaming API keeps
memory usage bounded regardless of playlist size — only one `Item` is live
at a time. The `Attrs` map is defensively cloned via `cloneItem` before
emission to prevent mutation of shared state between callback invocations.

## Playlist Management (`internal/playlist`)

### Types

`playlist.Item` extends the parsed `m3u.Item` with persistence-oriented
fields:

| Field         | Type              | Description                              |
|---------------|-------------------|------------------------------------------|
| `ItemKey`     | `string`          | Deterministic stream identity            |
| `ChannelKey`  | `string`          | Channel-level grouping key               |
| `Name`        | `string`          | Display name from `#EXTINF`              |
| `Group`       | `string`          | Group/category from `group-title`        |
| `StreamURL`   | `string`          | Stream URL                               |
| `TVGID`       | `string`          | EPG identifier                           |
| `TVGLogo`     | `string`          | Logo URL                                 |
| `Attrs`       | `map[string]string` | Full M3U attribute map                 |
| `FirstSeenAt` | `int64`           | Unix nanos when first appeared           |
| `LastSeenAt`  | `int64`           | Unix nanos of most recent refresh        |
| `Active`      | `bool`            | Present in latest refresh                |

`ItemStream` is a callback-based streaming type:

```go
type ItemStream func(yield func(Item) error) error
```

`Query` controls catalog filtering and pagination with group, search,
regex, limit, and offset fields.

### Manager

`playlist.Manager` wraps HTTP fetch and M3U parsing into a single
operation. Configuration:

- HTTP client with a default 30-second timeout.
- URL is validated (non-empty, whitespace-trimmed).
- Response status must be 2xx; non-success status codes cause an
  immediate error.

Provides both batch (`FetchAndParse`) and streaming (`FetchAndParseEach`)
APIs. The streaming variant converts `m3u.Item` to `playlist.Item` inline.
Callback errors are propagated through a sentinel type to distinguish sink
errors from parse errors.

### Refresher

`playlist.Refresher` coordinates the full fetch-parse-persist cycle
with a mutex-based single-refresh lock to prevent concurrent refresh
operations.

#### Streaming vs Batch Mode Detection

The refresher auto-detects the optimal pipeline at runtime:

```
if fetcher implements StreamingFetcher
   AND store implements StreamingCatalogStore:
     → streaming pipeline (bounded memory)
else:
     → batch pipeline (all items in memory)
```

**Streaming pipeline**: Items flow from HTTP response through the M3U
parser directly into the catalog store's streaming upsert within a single
database transaction. Memory usage is O(1) per item regardless of playlist
size.

**Batch pipeline**: All items are collected into a slice, then persisted
in one call. Memory usage is O(n) where n is the number of playlist
entries. This path exists for backward compatibility with store
implementations that do not support streaming upsert.

#### Refresh-Mark Semantics

Each refresh cycle generates a `refreshMark` timestamp (`time.Now().UnixNano()`).
During upsert, every matched item's `last_seen_at` is set to this mark and
`active` is set to `1`. After all items are upserted, the store runs:

```sql
UPDATE playlist_items SET active = 0 WHERE last_seen_at <> ? AND active <> 0
```

This deactivates any item not seen in the current refresh (its `last_seen_at`
will be from a prior cycle). Deactivation is non-destructive — the row and
all historical fields (`first_seen_at`, attributes, etc.) are preserved.
Downstream consumers use the `active` flag to distinguish current catalog
entries from stale ones.

## Source Reconciliation (`internal/reconcile`)

Reconciliation runs as the second stage of playlist sync, after catalog
refresh. It ensures that published channel source lists reflect the
current catalog state.

### Reconciliation Flow

```
Reconcile(ctx, onProgress)
  │
  ├── 1. Materialize dynamic blocks
  │      Add/remove dynamic block channels
  │      in published_channels (guide range 10000+)
  │
  ├── 2. List and filter channels
  │      Select the reconcilable subset
  │
  └── 3. Reconcile each channel
         ├── Static path
         │   Match channel_key → active catalog items → append new sources
         │
         └── Dynamic path
             Run catalog filter queries → sync via SyncDynamicSources
```

### Reconcilability Criteria

A channel is reconcilable when all of these are true:

| Channel Type                       | Requirement                           |
|------------------------------------|---------------------------------------|
| `channel_class=dynamic_generated`  | **Never** reconcilable (skipped)      |
| Dynamic rule enabled               | `search_query` must be non-empty      |
| Static (no dynamic rule)           | `channel_key` must be non-empty       |

In the default SQLite-backed implementation, `channels.List()` already
returns only traditional channels. Dynamic-generated rows are handled in
`SyncDynamicChannelBlocks` and are excluded from per-channel source reconcile.

### Static Channel Reconciliation

For channels without an enabled dynamic rule:

1. Query the catalog for all active `item_key` values matching the
   channel's `channel_key`.
2. Load existing sources on the channel.
3. For each catalog item not already attached as a source, call
   `AddSource` with `allowCrossChannel=false`.
4. Items that cause `ErrAssociationMismatch` (cross-channel key conflict)
   are silently skipped.

Static reconciliation is append-only — it never removes existing sources.

### Dynamic Channel Reconciliation

For channels with `DynamicRule.Enabled = true`:

1. Build a `dynamicCatalogFilterKey` from the rule's `group_names`,
   `search_query`, and `search_regex` flag.
2. Choose the sync strategy based on runtime conditions:

| Strategy         | When Used                                      | Memory Profile |
|------------------|------------------------------------------------|----------------|
| **Paged mode**   | Runs when paged mode is enabled, store interfaces support paging, and the rule is used by one channel | O(page_size) per page |
| **Shared-rule cache** | Multiple channels share the same dynamic rule | O(matches), cached and reused |
| **Legacy mode**  | Paged mode disabled or interfaces unavailable  | O(matches) per channel |

3. Invoke `SyncDynamicSources` (or `SyncDynamicSourcesByCatalogFilter`
   for paged mode) which:
   - Adds missing `dynamic_query`-type source associations for matched
     catalog items.
   - Removes `dynamic_query` associations that no longer match.
   - Preserves all `manual` source associations regardless of match state.

### Dynamic Rule Caching

When multiple channels share the same dynamic rule (identical group
names, search query, and regex flag), the reconciler caches the matched
item key list after the first evaluation and reuses it for subsequent
channels. This avoids repeated full catalog filter scans for popular
shared rules.

The cache key is a composite of `GroupNames` (joined with `\x1f`
separator), `SearchQuery` (trimmed), and `SearchRegex` flag. Cache
entries are only retained for rules used by more than one channel.

### Match Limit Guard

Dynamic rule reconciliation enforces a maximum match count
(`dynamicRuleMatchLimit`, defaults to `DynamicGuideBlockMaxLen`). If the
number of catalog items matching a dynamic rule exceeds this limit, the
reconciliation fails for that channel with a descriptive error. This
prevents overly broad rules from creating excessive source associations
or channel entries. A value `<= 0` disables this guard.

### Progress Reporting

`Reconcile` accepts an `onProgress` callback that is invoked after each
channel is processed, receiving `(current, total)` counts. The playlist
sync job uses this with throttled persistence (every 5 channels or every
1 second) to update the job run's progress counters without excessive
database writes.

### Result Counters

The `Result` struct tracks reconciliation outcomes:

| Counter                    | Description                                    |
|----------------------------|------------------------------------------------|
| `ChannelsTotal`            | Total channels seen                            |
| `ChannelsProcessed`        | Channels successfully reconciled               |
| `ChannelsSkipped`          | Non-reconcilable rows plus static-path `ErrAssociationMismatch` skips |
| `SourcesAdded`             | New source associations created (both modes)   |
| `SourcesAlreadySeen`       | Existing sources unchanged (both modes)        |
| `DynamicBlocksProcessed`   | Dynamic block queries evaluated                |
| `DynamicBlocksEnabled`     | Enabled dynamic block queries                  |
| `DynamicChannelsAdded`     | Channels added by dynamic blocks               |
| `DynamicChannelsUpdated`   | Channels updated by dynamic blocks             |
| `DynamicChannelsRetained`  | Channels unchanged by dynamic blocks           |
| `DynamicChannelsRemoved`   | Channels removed by dynamic blocks             |
| `DynamicChannelsTruncated` | Channels truncated at block match cap          |
| `DynamicChannelsProcessed` | Dynamic-rule channels source-synced            |
| `DynamicSourcesAdded`      | Sources added via dynamic rules                |
| `DynamicSourcesRemoved`    | Sources removed via dynamic rules              |
