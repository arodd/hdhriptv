# Channels & Favorites

This document describes the channels business logic layer and the legacy
favorites subsystem. All channel orchestration flows through
`internal/channels/service.go`, with dynamic group and block helpers in
`internal/channels/dynamic_groups.go` and `internal/channels/dynamic_blocks.go`.
The favorites subsystem lives in `internal/favorites/service.go`.

## Overview

The channels package is the central service layer used by stream handlers,
admin API routes, reconciliation jobs, and DVR integration. It mediates all
published-channel and source-mapping operations through a `Store` interface
that defines 30+ persistence methods.

Source files:

- `internal/channels/service.go` -- `Service` type, `Store` interface, domain
  types, sentinel errors, input normalization
- `internal/channels/dynamic_groups.go` -- `NormalizeGroupNames`,
  `GroupNameAlias`
- `internal/channels/dynamic_blocks.go` -- channel class constants, guide
  number allocation, `DynamicChannelQuery` types
- `internal/favorites/service.go` -- legacy `Service` type, `Store` interface,
  `Favorite` type

---

## Channel Classes

Every published channel has a `channel_class` that determines its guide range,
API visibility, and lifecycle management.

| Class | Constant | Guide Range | API Surface |
|-------|----------|-------------|-------------|
| `traditional` | `ChannelClassTraditional` | `100`–`9999` | `GET /api/channels`, `/ui/channels` |
| `dynamic_generated` | `ChannelClassDynamicGenerated` | `10000`+ | `GET /api/dynamic-channels/{queryID}/channels`, `/ui/dynamic-channels/{queryID}` |

Traditional channels are created manually (or via dynamic source rules) and
occupy contiguous guide numbers starting at `100`. Dynamic generated channels
are materialized by dynamic block queries into reserved guide ranges starting
at `10000`.

`GET /api/channels` is scoped to `channel_class=traditional` only. Generated
dynamic rows are surfaced under per-block endpoints.

---

## Service Type

`channels.Service` wraps a `Store` implementation and a `startGuideNumber`
(default `TraditionalGuideStart = 100`).

```go
type Service struct {
    store            Store
    startGuideNumber int
}
```

### Consumers

The service is used by:

- **Stream handlers** (`internal/stream/handler.go`) -- `GetByGuideNumber`
  for tune resolution, `ListSources` for failover ordering
- **Admin routes** (`internal/http/admin_routes.go`) -- full CRUD for
  channels, sources, dynamic queries
- **Reconciliation** (`internal/reconcile/reconcile.go`) -- `SyncDynamicSources`,
  `SyncDynamicSourcesByCatalogFilter`, `SyncDynamicChannelBlocks`
- **DVR integration** (`internal/dvr/service.go`) -- channel listing for
  lineup mapping
- **Auto-prioritize** (`internal/jobs/auto_prioritize.go`) -- bulk source
  loading, reorder, health-based scoring

---

## Store Interface

The `Store` interface defines the persistence boundary. All methods accept a
`context.Context` and return errors from the sentinel set described below.

Key method groups:

| Group | Methods | Purpose |
|-------|---------|---------|
| Channel CRUD | `CreateChannelFromItem`, `DeleteChannel`, `UpdateChannel`, `ListChannels`, `ListChannelsPaged`, `ListLineupChannels`, `ReorderChannels`, `GetChannelByGuideNumber` | Traditional channel lifecycle plus lineup/tune lookup |
| Source CRUD | `AddSource`, `DeleteSource`, `UpdateSource`, `ListSources`, `ListSourcesPaged`, `GetSource`, `ReorderSources` | Source attachment and ordering |
| Source health | `MarkSourceFailure`, `MarkSourceSuccess`, `UpdateSourceProfile`, `ClearSourceHealth`, `ClearAllSourceHealth` | Health tracking and cooldown |
| Dynamic sources | `SyncDynamicSources` | Item-key-based dynamic source sync |
| Dynamic blocks | `SyncDynamicChannelBlocks`, `ListDynamicChannelQueries`, `ListDynamicChannelQueriesPaged`, `GetDynamicChannelQuery`, `CreateDynamicChannelQuery`, `UpdateDynamicChannelQuery`, `DeleteDynamicChannelQuery`, `ListDynamicGeneratedChannelsPaged`, `ReorderDynamicGeneratedChannels` | Block query lifecycle and generated channel management |
| Suggestions | `ListDuplicateSuggestions` | Duplicate catalog grouping |

### Capability Detection Interfaces

Two optional interfaces extend the base `Store` contract when the backing
implementation supports them:

- **`bulkSourceLister`** -- `ListSourcesByChannelIDs(ctx, channelIDs, enabledOnly)`.
  Used by auto-prioritize to batch-load sources across channels in one query.
  `Service.ListSourcesByChannelIDs` checks for this interface at runtime and
  falls back to per-channel `ListSources` calls if unavailable.

- **`dynamicCatalogFilterSyncStore`** -- `SyncDynamicSourcesByCatalogFilter(ctx, channelID, groupNames, searchQuery, searchRegex, pageSize, maxMatches)`.
  Used by reconciliation to stream catalog-filter matches in bounded pages
  instead of loading all matching item keys into memory. Falls back to
  `SyncDynamicSources` with a pre-fetched key list when unavailable.

---

## Source Management

### Association Types

Each source has an `association_type` that controls its lifecycle during
dynamic sync:

| Type | Constant | Behavior |
|------|----------|----------|
| `channel_key` | `"channel_key"` | Auto-assigned when a source item's `channel_key` matches the published channel's `channel_key`. Preserved unless promoted by dynamic reconciliation. |
| `manual` | default | Created by operator action. Preserved during dynamic sync -- never automatically removed. |
| `dynamic_query` | `"dynamic_query"` | Created by dynamic source reconciliation. Automatically removed when no longer matched by the channel's dynamic rule. |
| `dynamic_channel_item` | `"dynamic_channel_item"` | Used for sources attached to generated channels materialized by dynamic channel blocks. Managed by block sync, not by traditional per-channel dynamic source sync. |

When a dynamic sync runs, only `dynamic_query` associations are candidates
for removal. `manual` and `channel_key` associations are preserved by default.
If a `channel_key` association matches a dynamic rule's criteria, reconciliation
promotes it to `dynamic_query`.

### Source CRUD

- `AddSource(channelID, itemKey, allowCrossChannel)` -- attaches a catalog
  item as a source. Matching channel keys are stored as
  `association_type="channel_key"`. If keys differ, pass
  `allowCrossChannel=true` to add as `manual`; otherwise
  `ErrAssociationMismatch` is returned.
- `DeleteSource(channelID, sourceID)` -- removes a source association.
- `UpdateSource(channelID, sourceID, enabled)` -- toggles source enabled state.
- `ReorderSources(channelID, sourceIDs)` -- sets explicit failover priority
  ordering.

### Health Tracking

Source health state drives failover ordering during stream startup and
recovery:

- `MarkSourceFailure(sourceID, reason, failedAt)` -- increments `fail_count`,
  records `last_fail_at` and `last_fail_reason`, applies cooldown from the
  fail ladder (`10s`, `30s`, `2m`, `10m`, `1h` cap).
- `MarkSourceSuccess(sourceID, succeededAt)` -- resets `fail_count` to 0,
  clears `last_fail_at`, `last_fail_reason`, and `cooldown_until`. A
  successful startup immediately restores the source to full availability.
- `UpdateSourceProfile(sourceID, profile)` -- persists stream profile metadata
  (resolution, FPS, codecs, bitrate) from probe analysis.
- `ClearSourceHealth(channelID)` / `ClearAllSourceHealth()` -- resets all
  health/cooldown fields (`success_count`, `fail_count`, `last_ok_at`,
  `last_fail_at`, `last_fail_reason`, `cooldown_until`). Exposed via admin
  API as `POST /api/channels/{channelID}/sources/health/clear` and
  `POST /api/channels/sources/health/clear`.

---

## Dynamic Source Sync

Dynamic source sync reconciles a channel's source list against catalog
matches defined by its `DynamicSourceRule`.

### Item-Key-Based Sync

`SyncDynamicSources(channelID, matchedItemKeys)` accepts a pre-computed
list of matching item keys and reconciles:

1. Adds `dynamic_query` sources for matched items not already present.
2. Removes `dynamic_query` sources for items no longer in the match set.
3. Retains all `manual` associations unchanged.
4. Promotes matching `channel_key` associations to `dynamic_query`.
5. Deduplicates and trims whitespace from input keys.

Returns a `DynamicSourceSyncResult` with `Added`, `Removed`, and `Retained`
counts.

### Catalog-Filter-Based Sync

`SyncDynamicSourcesByCatalogFilter(channelID, groupNames, searchQuery, searchRegex, pageSize, maxMatches)`
delegates to the store's paged implementation when available. This approach
streams catalog matches in pages of `pageSize` (default 512) items using
keyset pagination, avoiding loading the full match set into memory.

The method normalizes `groupNames` via `NormalizeGroupNames` and trims
`searchQuery` before delegating. It returns both the sync result and the total
match count.

### Reconciliation Trigger Points

Dynamic source sync is triggered from two paths:

1. **Playlist reconciliation** (`internal/reconcile/reconcile.go`) -- runs
   during playlist sync jobs for all dynamic-rule-enabled channels.
2. **Immediate sync** (`internal/http/admin_routes.go`) -- enqueues
   asynchronous reconciliation after channel create/update when
   `dynamic_rule` is enabled. The admin route layer keeps a per-channel
   coalescing queue: rapid updates merge, newer rules supersede in-flight
   syncs, and disable/delete transitions cancel pending work.

---

## Dynamic Block Guide Number Allocation

Dynamic generated channels occupy reserved guide ranges starting at `10000`.

### Block Layout

Each dynamic block query is assigned an `order_index` (0-based). Guide
numbers are allocated deterministically:

```
block_start = 10000 + (order_index * 1000)
```

Generated channels within a block receive guide numbers from `block_start`
to `block_start + 999`, capped at 1000 entries per block
(`DynamicGuideBlockMaxLen`).

### Constants

| Name | Value | Purpose |
|------|-------|---------|
| `TraditionalGuideStart` | `100` | First traditional guide number |
| `TraditionalGuideEnd` | `9999` | Last traditional guide number |
| `DynamicGuideStart` | `10000` | First dynamic block guide number |
| `DynamicGuideBlockSize` | `1000` | Guide range reserved per block |
| `DynamicGuideBlockMaxLen` | `1000` | Max generated channels per block |
| `dynamicChannelOrderBase` | `1000000` | Internal `order_index` offset for dynamic channels |

### Allocation Functions

- `DynamicGuideBlockStart(orderIndex)` -- returns the first guide number for
  a block. Validates against `maxSignedInt()` overflow.
- `DynamicGuideNumber(orderIndex, position)` -- returns the guide number for
  a specific position within a block. `position` must be `[0, 999]`.
- `DynamicChannelOrderIndex(orderIndex, position)` -- returns the internal
  `order_index` for persistence. Uses `dynamicChannelOrderBase` offset so
  dynamic channel ordering does not collide with traditional channels.

All three functions return errors on overflow or out-of-range inputs.

### Reorder Behavior

`ReorderDynamicGeneratedChannels(queryID, channelIDs)` reassigns guide
numbers deterministically within the block range. After successful
materialization or reorder, admin routes trigger `DVRService.ReloadLineup`
so DVR providers can pick up updated guide numbers.

---

## Group Name Normalization

`NormalizeGroupNames(groupName, groupNames)` produces a canonical group
filter list from legacy single-group and multi-group inputs.

### Algorithm

1. If `groupNames` is non-empty, use it as the candidate set. Otherwise,
   use the single `groupName`.
2. Trim whitespace from each candidate.
3. Deduplicate case-insensitively (first occurrence wins).
4. Sort deterministically by lowercase key.

Returns `nil` when all candidates are empty.

### GroupNameAlias

`GroupNameAlias(groupNames)` returns the first normalized group name as a
legacy compatibility alias. This bridges the transition from single-group
`group_name` to multi-group `group_names` payloads. When both fields are
present in API requests, normalized `group_names` semantics apply and
`group_name` is treated as an alias of the first entry.

---

## Dynamic Rule Input Normalization

`normalizeDynamicRuleInput(rule)` validates and normalizes a
`DynamicSourceRule` before persistence:

1. Normalizes group names via `NormalizeGroupNames`.
2. Sets `GroupName` alias via `GroupNameAlias`.
3. Trims whitespace from `SearchQuery`.
4. If `Enabled=true`, requires a non-empty `SearchQuery` (returns error
   otherwise).

Similar normalization runs for dynamic channel query create/update inputs
(`normalizeDynamicChannelQueryCreateInput`, `normalizeDynamicChannelQueryUpdateInput`).

---

## Sentinel Errors

| Error | When Returned | Typical HTTP Status |
|-------|---------------|---------------------|
| `ErrChannelNotFound` | Channel ID does not exist | `404` |
| `ErrSourceNotFound` | Source ID does not exist for the given channel | `404` |
| `ErrItemNotFound` | Catalog `item_key` not found in `playlist_items` | `404` |
| `ErrSourceOrderDrift` | Source set changed between read and reorder (count mismatch or missing id) | `400` |
| `ErrDynamicQueryNotFound` | Dynamic block query ID does not exist | `404` |
| `ErrAssociationMismatch` | Source `channel_key` does not match channel's `channel_key` and `allowCrossChannel` is false | `409` |

These errors are checked by admin route handlers to map service-layer
failures to appropriate HTTP responses.

---

## DuplicateSuggestions

`DuplicateSuggestions(minItems, searchQuery, limit, offset)` groups catalog
items by `channel_key` to surface duplicate streams that could be merged
into multi-source channels. Exposed via `GET /api/suggestions/duplicates`.

- `minItems` is clamped to `>= 2` in the service; the admin API additionally
  caps it at `100`.
- `searchQuery` is case-insensitive across `channel_key` and `tvg_id`.
- Returns `[]DuplicateGroup` where each group contains the shared
  `channel_key`, count, and individual `DuplicateItem` entries.

---

## Favorites (Legacy)

The `internal/favorites/` package implements the original favorites system
that predates the published channels model. It is retained for backward
compatibility; at startup, `migrateLegacyFavorites` moves existing rows from
the `favorites` table into `published_channels` + `channel_sources`.

### Favorite Struct

```go
type Favorite struct {
    FavID       int64
    ItemKey     string
    OrderIndex  int
    GuideNumber string
    GuideName   string
    Enabled     bool
    StreamURL   string
    TVGLogo     string
    GroupName   string
}
```

A favorite is a curated channel entry that appears in HDHomeRun lineup
responses. Each favorite references a catalog item via `ItemKey`.

### Guide Number Assignment

Guide numbers start at `startGuideNumber = 100` and are assigned
sequentially. Add and remove operations renumber remaining entries to keep
guide numbers contiguous.

### Service API

| Method | Signature | Description |
|--------|-----------|-------------|
| `Add` | `Add(ctx, itemKey) (Favorite, error)` | Creates a favorite from a catalog item key. Trims and validates the key. |
| `Remove` | `Remove(ctx, favID) error` | Deletes a favorite by ID. Remaining entries are renumbered. |
| `List` | `List(ctx) ([]Favorite, error)` | Returns all favorites (enabled and disabled). |
| `ListEnabled` | `ListEnabled(ctx) ([]Favorite, error)` | Returns only enabled favorites. |
| `Reorder` | `Reorder(ctx, favIDs) error` | Reorders favorites. Guide numbers are reassigned starting at 100. |

### Store Interface

```go
type Store interface {
    AddFavorite(ctx, itemKey, startGuideNumber) (Favorite, error)
    RemoveFavorite(ctx, favID, startGuideNumber) error
    ListFavorites(ctx, enabledOnly) ([]Favorite, error)
    ReorderFavorites(ctx, favIDs, startGuideNumber) error
}
```

The store methods operate on `published_channels` + `channel_sources` (not
the legacy `favorites` table). The interface naming retains the original
"favorite" terminology for API compatibility.

### Error Semantics

- `ErrItemNotFound` -- the specified `item_key` does not exist in the
  catalog. Distinct from `channels.ErrItemNotFound` (same semantics,
  separate package-level declaration).
- `ErrNotFound` -- the specified `fav_id` does not map to an existing
  published channel.

---

## Domain Types

### Channel

```go
type Channel struct {
    ChannelID      int64
    ChannelClass   string            // "traditional" or "dynamic_generated"
    ChannelKey     string
    GuideNumber    string
    GuideName      string
    OrderIndex     int
    Enabled        bool
    DynamicQueryID int64             // FK to dynamic_channel_queries
    DynamicItemKey string            // seed item for dynamic channel
    DynamicRule    DynamicSourceRule
    SourceTotal    int               // total associated sources
    SourceEnabled  int               // enabled associated sources
    SourceDynamic  int               // association_type=dynamic_query
    SourceManual   int               // association_type!=dynamic_query
}
```

### Source

```go
type Source struct {
    SourceID          int64
    ChannelID         int64
    ItemKey           string
    TVGName           string
    StreamURL         string
    PriorityIndex     int       // failover priority (0 = highest)
    Enabled           bool
    AssociationType   string    // "channel_key", "manual", "dynamic_query", or "dynamic_channel_item"
    LastOKAt          int64
    LastFailAt        int64
    LastFailReason    string
    SuccessCount      int
    FailCount         int
    CooldownUntil     int64
    LastProbeAt       int64
    ProfileWidth      int
    ProfileHeight     int
    ProfileFPS        float64
    ProfileVideoCodec string
    ProfileAudioCodec string
    ProfileBitrateBPS int64
}
```

### DynamicSourceRule

```go
type DynamicSourceRule struct {
    Enabled     bool
    GroupName   string      // legacy single-group compat alias
    GroupNames  []string    // preferred multi-group filter
    SearchQuery string      // catalog search filter
    SearchRegex bool        // regex mode flag
}
```

### DynamicChannelQuery

```go
type DynamicChannelQuery struct {
    QueryID        int64
    Enabled        bool
    Name           string
    GroupName      string
    GroupNames      []string
    SearchQuery    string
    SearchRegex    bool
    NextSlotCursor int      // allocation cursor for new matches
    OrderIndex     int
    LastCount      int      // channels from last sync
    TruncatedBy    int      // items truncated by slot limits
    CreatedAt      int64
    UpdatedAt      int64
}
```

---

## File Reference

| File | Description |
|------|-------------|
| `internal/channels/service.go` | Service type, Store interface, domain types, CRUD methods, sentinel errors, input normalization |
| `internal/channels/dynamic_groups.go` | Group name normalization, legacy alias |
| `internal/channels/dynamic_blocks.go` | Channel class constants, guide allocation functions, dynamic query types |
| `internal/favorites/service.go` | Legacy favorites service, Favorite type, Store interface |
