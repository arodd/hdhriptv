# DVR Integration

The DVR integration subsystem pushes channel-to-station mappings from hdhriptv to external DVR applications and pulls existing mappings back. It supports multiple DVR providers and can operate on a scheduled cron or on-demand via the admin API.

Source: `internal/dvr/`

## Supported Providers

| Provider    | Type constant | Default base URL              | Capabilities |
|-------------|---------------|-------------------------------|--------------|
| Channels DVR | `channels`   | empty (must be configured)    | Full: lineups, device channels, station listing, custom mapping read/write, guide refresh, device scan |
| Jellyfin     | `jellyfin`   | `http://jellyfin.lan:8096`    | Lineup reload only (re-post tuner host to trigger channel refresh) |

`InstanceConfig.Provider` is the primary provider field used by sync/mapping workflows; current API/UI contracts enforce this as `channels` only.
An `ActiveProviders` list enables multi-provider fan-out for playlist-sync-triggered reloads.
`Service.Sync()` and reverse-sync paths use the primary provider only; `ActiveProviders` fan-out is specific to post-playlist-sync reload.
For Channels, `ActiveProviders` includes `channels` only when a valid `channels_base_url` is configured.
This can surface in `/ui/dvr` as the `Channels DVR` active-provider checkbox
not persisting while `channels_base_url` is empty/invalid.

## Provider Interfaces

Provider contracts are capability-based (`provider.go`):

```go
type ProviderBase interface {
    Type() ProviderType
}

type LineupReloadProvider interface {
    ProviderBase
    ListLineups(ctx context.Context) ([]DVRLineup, error)
    ReloadDeviceLineup(ctx context.Context, deviceID string) error
    RefreshGuideStations(ctx context.Context) error
    RedownloadGuideLineup(ctx context.Context, lineupID string) error
}

type MappingProvider interface {
    ProviderBase
    ListDeviceChannels(ctx context.Context) (map[string]DVRDeviceChannel, error)
    ListLineupStations(ctx context.Context, lineupID string) ([]DVRStation, error)
    GetCustomMapping(ctx context.Context, lineupID string) (map[string]string, error)
    PutCustomMapping(ctx context.Context, lineupID string, patch map[string]string) error
    RefreshDevices(ctx context.Context) error
}
```

Factory behavior:
- `newLineupReloadProvider(...)` is used for lineup-reload paths (`ReloadLineup`, playlist-sync reload fan-out, lineup listing).
- `newMappingProvider(...)` is used for mapping paths (`Sync`, `ReverseSync`, `ReverseSyncChannel`, `TestConnection`).
- There is no union-provider compatibility adapter; call sites resolve the
  exact capability they require.

Provider/base-url/active-provider normalization is centralized in
`internal/dvr/config_normalize.go`:
- `NormalizeInstanceConfig(...)` for update payload normalization against current config
- `NormalizeStoredInstance(...)` for read-back/store normalization
- `NormalizeForProvider(...)` for provider-scoped views used during provider construction

`instanceForProvider()` is a thin compatibility wrapper around `NormalizeForProvider(...)`.

## Channels DVR Provider

`ChannelsProvider` (`channels_provider.go`) communicates with the Channels DVR HTTP API.

### API Endpoints Used

| Operation | Method | Path |
|-----------|--------|------|
| List device channels | GET | `/dvr/guide/channels` |
| List lineup stations | GET | `/dvr/guide/stations/{lineupID}` |
| Get custom mapping | GET | `/dvr/guide/stations/{lineupID}/custom` |
| Put custom mapping | PUT | `/dvr/guide/stations/{lineupID}` |
| List lineups (primary) | GET | `/dvr/lineups` |
| List lineups (fallback) | GET | `/devices` |
| Reload device lineup | POST | `/devices/{deviceID}/channels` |
| Refresh guide stations | PUT | `/dvr/guide/stations` |
| Redownload lineup guide | PUT | `/dvr/lineups/{lineupID}` |
| Refresh devices (primary) | PUT | `/dvr/scanner/scan` |
| Refresh devices (fallback) | POST | `/devices` |

### Custom Mapping

The custom mapping is a `map[string]string` keyed by device channel key, with station ref as the value. During forward sync, hdhriptv computes the desired mapping and PUTs it in batches of 75 (`syncBatchSize`).

### Lineup Discovery

`ListLineups` tries `/dvr/lineups` first. If that fails or returns empty, it falls back to `/devices`. Both responses are walked recursively by `collectLineups()`, which extracts any value that looks like a lineup ID (must contain at least one hyphen, length <= 96, allow `[A-Za-z0-9_.-]`, and include at least one letter).

### Reload Behavior

`ReloadDeviceLineup` POSTs to `/devices/{deviceID}/channels`. The Channels DVR server sometimes returns HTTP 400 with the refreshed channel list as the body (possibly base64-encoded). The provider detects this pattern via `isReloadLineupChannelListResponse()` and treats it as success.

## Jellyfin Provider

`JellyfinProvider` (`jellyfin_provider.go`) targets Jellyfin's Live TV subsystem. It supports a narrower set of operations focused on triggering lineup refresh.

### Authentication

All requests include an `X-Emby-Token` header set to `InstanceConfig.JellyfinAPIToken`. Requests fail with `ErrDVRSyncConfig` if the token is empty.

### Lineup Reload

`ReloadDeviceLineup` performs these steps:

1. Fetch live TV config from `GET /System/Configuration/livetv`
2. Select the matching tuner host via `selectTunerHost()`
3. Re-POST the tuner host object to `POST /LiveTv/TunerHosts` to trigger a channel refresh
4. Best-effort query of `GET /ScheduledTasks` to check the `RefreshGuide` task state

### Tuner Host Selection

`selectTunerHost()` uses this priority:

1. If `JellyfinTunerHostID` is set, match by tuner host `Id` field exactly
2. Filter to tuner hosts with `Type == "hdhomerun"`
3. If exactly one matches the HDHR device ID, use it
4. If multiple match, return an error suggesting `jellyfin_tuner_host_id` override
5. If none match by device ID but exactly one HDHomeRun host exists, use it
6. Otherwise error

### Unsupported Operations

Jellyfin intentionally does not implement `MappingProvider`. Mapping-specific
factory requests (`newMappingProvider(...)`) return `ErrUnsupportedProvider`.
`ListLineups` also returns `ErrUnsupportedProvider` because Jellyfin has no
lineup-list endpoint equivalent to Channels DVR. `RedownloadGuideLineup` is a
no-op for Jellyfin because there is no lineup-scoped endpoint equivalent to
Channels DVR's `/dvr/lineups/{lineupID}`.

## IncludeDynamic Parameter

The `IncludeDynamic` boolean controls whether dynamic-generated channels
(guide range `10000+`, `channel_class=dynamic_generated`) are included
in sync operations. It appears on both `SyncRequest` and
`ReverseSyncRequest` and defaults to `false`.

When `false` (default), only traditional channels (`100-9999`) participate
in forward and reverse sync. Set `IncludeDynamic=true` only when you
intentionally want dynamic block-generated channels included in DVR
lineup mapping.

The parameter propagates to `ListChannelsForDVRSync(ctx, instanceID,
enabledOnly, includeDynamic)` which filters `published_channels` by
`channel_class`.

## Lineup Caching

`Service.ListLineups(ctx, refresh)` returns cached lineup data when
`refresh=false` and a previous fetch succeeded. The cache is stored
in-memory on the `Service` struct:

- `cachedLineups []DVRLineup` — the last fetched lineup list.
- `cachedLineupsAt int64` — Unix epoch timestamp of the last fetch.

When `refresh=true` or the cache is empty, a fresh provider call is
made and the cache is updated atomically under a write lock. The cache
has no TTL — it persists until the next explicit refresh or process
restart.

The `GET /api/admin/dvr` response includes `cached_lineups_at` so
consumers can assess staleness. The `GET /api/admin/dvr/lineups?refresh=1`
endpoint forces a provider refresh.

## Forward Sync (Push)

Forward sync pushes hdhriptv channel mappings to the currently selected provider (`InstanceConfig.Provider`). Invoked via `Service.Sync()`.

### Sync Modes

| Mode | Constant | Behavior |
|------|----------|----------|
| Configured Only | `configured_only` | Only update device keys that have a configured mapping in hdhriptv. Existing provider mappings for unconfigured channels are left untouched. |
| Mirror Device | `mirror_device` | Update all device keys. Keys without a configured mapping are cleared (station ref set to empty string), effectively mirroring hdhriptv as the source of truth. |

### Sync Flow

1. Acquire `syncRunLock` (mutex with `TryLock`; returns `ErrSyncAlreadyRunning` if held)
2. Reload the device lineup on the provider (`ReloadDeviceLineup`)
3. Optionally refresh devices if `PreSyncRefreshDevices` is enabled
4. Fetch all device channels from the provider and filter by HDHR device ID
5. Build a `tunerNumber -> deviceKey` index from filtered channels
6. Load configured channel mappings from the database (enabled, non-dynamic unless `IncludeDynamic`)
7. Group mappings by lineup ID (falling back to `DefaultLineupID`)
8. Build a per-lineup sync plan:
   - Fetch lineup stations and current custom mapping from the provider
   - Resolve each channel's station ref using `resolveStationRef()`
   - Compute a patch diff against the current provider mapping
   - Compute per-lineup/global counters and warning summaries
9. Apply the sync plan:
   - Apply the patch in batches of 75 via `PutCustomMapping`
   - Persist resolved station refs back to the local database
   - In `dry_run`, skip provider/store writes but still emit patch preview and counters
10. Record the result in `lastSync` for API visibility

### Station Ref Resolution

`resolveStationRef()` resolves a channel mapping to a concrete station ref:

1. If a cached `DVRStationRef` exists and is valid in the lineup, use it
2. If cached ref points to a different lineup channel than configured, fall through to lineup channel lookup
3. Look up by `DVRLineupChannel` in the lineup's station list
4. If multiple stations share the same lineup channel, try matching by `DVRCallsignHint`
5. If still ambiguous, use the first station ref (sorted lexicographically) and emit a warning

### Batched Patches

Patches are split into chunks of 75 entries (`syncBatchSize`) by `chunkPatch()` before being sent to the provider. Keys are sorted for deterministic ordering.

## Reverse Sync (Pull)

Reverse sync pulls mappings from the currently selected provider into hdhriptv. Invoked via `Service.ReverseSync()` (bulk) or `Service.ReverseSyncChannel()` (single channel).

### Flow

1. Acquire `syncRunLock`
2. Determine the target lineup ID from: request parameter, channel mapping, `DefaultLineupID`, or inference from existing mappings
3. Fetch device channels and filter by HDHR device ID
4. Fetch lineup stations and custom mapping from the provider
5. For each candidate channel:
   - Find the device key via the guide number
   - Look up the station ref from the provider's custom mapping
   - Look up the station in the lineup to get lineup channel and callsign
   - Compare against the existing local mapping
   - If changed and not dry-run, persist via `UpsertChannelDVRMapping`
6. Return counts: imported, unchanged, missing tuner, missing mapping, missing station ref

### Lineup ID Inference

When no lineup ID is provided and `DefaultLineupID` is unset, `inferLineupIDFromMappings()` scans existing channel mappings. If all mappings reference the same lineup, that ID is used. If multiple lineup IDs are present, the request fails with an error listing the candidates.

## Per-Channel DVR Mapping

Each published channel can have a DVR mapping stored in `published_channel_dvr_map`:

```go
type ChannelMapping struct {
    ChannelID        int64   // FK to published_channels
    GuideNumber      string  // from published_channels (read-only in mapping context)
    GuideName        string  // from published_channels (read-only in mapping context)
    Enabled          bool    // from published_channels
    DVRInstanceID    int64   // FK to dvr_instances
    DVRLineupID      string  // e.g. "USA-OTA-90210"
    DVRLineupChannel string  // channel number within the lineup
    DVRStationRef    string  // provider station identifier (cached/resolved)
    DVRCallsignHint  string  // disambiguation hint for ambiguous lineup channels
}
```

The composite key is `(channel_id, dvr_instance_id)`. Updates go through `Service.UpdateChannelMapping()`, which:

- Falls back to `DefaultLineupID` if no lineup ID is provided
- Clears the mapping (deletes the row) if both lineup channel and station ref are empty
- Detects stale cached station refs: if identity fields (lineup ID, lineup channel, callsign hint) changed but the caller echoed the old station ref, the ref is cleared so forward sync re-resolves it

## Post-Playlist-Sync Reload

`ReloadLineupForPlaylistSyncOutcome()` is called after playlist sync completes to refresh DVR provider lineups. It fans out across all `ActiveProviders`:

1. For each active provider, build a provider instance
2. For Channels providers, check skip conditions via `channelsReloadSkipReason()`:
   - Missing or invalid Channels base URL
   - Skips are non-fatal and recorded as reasons in the return value
3. For Jellyfin providers, check skip conditions via `jellyfinReloadSkipReason()`:
   - Missing or invalid base URL
   - Missing API token
   - Skips are non-fatal and recorded as reasons in the return value
4. Call `reloadLineup()` which triggers:
   - `ReloadDeviceLineup`
   - `RefreshGuideStations`
   - `RedownloadGuideLineup` for configured/discovered lineup IDs
5. Provider-local build/reload errors are aggregated and do **not** abort fan-out:
   - healthy providers still run in the same pass
   - failed providers are recorded in provider-scoped failure metadata

`ReloadLineupForPlaylistSyncOutcome()` returns `ReloadOutcome` with
`reloaded`, `skipped`, `failed`, `status`, provider-scoped skip metadata,
and provider-scoped failure metadata.

## Config Persistence

DVR configuration is stored as a singleton row in `dvr_instances` (keyed by `singleton_key = 1`).

Singleton lifecycle:
- startup schema/ensure paths normalize duplicate rows and explicitly ensure the singleton row exists
- `GetDVRInstance()` is a pure read (`SELECT`) and does not create rows as a side effect
- a missing singleton row at runtime is treated as an initialization invariant violation and returned as an error

### Instance Config Fields

| Field | Description |
|-------|-------------|
| `provider` | Primary provider type (`channels` or `jellyfin`) |
| `active_providers` | CSV of providers for multi-provider fan-out |
| `base_url` | Resolved base URL for the active provider |
| `channels_base_url` | Channels DVR server URL |
| `jellyfin_base_url` | Jellyfin server URL |
| `default_lineup_id` | Fallback lineup ID when channel mappings omit it |
| `sync_enabled` | Whether scheduled sync is active |
| `sync_cron` | Cron expression (default: `*/30 * * * *`) |
| `sync_mode` | `configured_only` or `mirror_device` |
| `pre_sync_refresh_devices` | Trigger device scan before sync |
| `jellyfin_api_token` | Jellyfin API token (redacted in API responses) |
| `jellyfin_tuner_host_id` | Override for Jellyfin tuner host selection |

### Rollback on Scheduler Failure

When updating DVR config via the admin API (`handlePutDVR`), the handler:

1. Validates the cron expression before persisting
2. Persists the new config via `UpdateConfig`
3. Attempts to update the job scheduler
4. If the scheduler update fails, rolls back the DVR config to the previous state via `restoreDVRConfig()`

## Error Handling

### Sentinel Errors

| Error | Condition |
|-------|-----------|
| `ErrDVRSyncConfig` | Configuration problem (missing device ID, lineup ID, tuner host, API token) |
| `ErrSyncAlreadyRunning` | `syncRunLock` is held by another goroutine |
| `ErrUnsupportedProvider` | Unknown provider type or unsupported operation on a provider |

### Connection Test

`TestConnection()` resolves a `MappingProvider` from current config and calls
`ListDeviceChannels`. It returns reachability status, total device channel
count, and the count filtered to the configured HDHR device ID.

### Sync Warnings

Sync operations accumulate warnings (capped at 200 via `maxSyncWarnings`) for non-fatal issues:

- Missing tuner channels for configured guide numbers
- Ambiguous lineup channel matches
- Cached station ref mismatches
- Channels with mappings but no lineup ID
- Multiple device keys for the same tuner number

### Unresolved Counts

`SyncResult.UnresolvedCount` tracks channels that have a DVR mapping but could not be resolved to a station ref during forward sync. This indicates configuration issues that need user attention (wrong lineup channel, missing station in lineup, or no lineup ID).

## Provider Capability Matrix

| Capability | Channels DVR | Jellyfin |
|------------|:------------:|:--------:|
| Lineup discovery (`ListLineups`) | Supported | Not supported |
| Forward sync (push mappings) | Supported | Not supported |
| Reverse sync (pull mappings) | Supported | Not supported |
| Per-channel reverse sync | Supported | Not supported |
| Bulk patch (`PutCustomMapping`) | Supported (75 per batch) | Not supported |
| Reload device lineup | Supported | Supported |
| Refresh guide stations | Supported | N/A (no-op; refresh triggered by tuner host re-post) |
| Custom mapping read/write | Supported | Not supported |
| Station-ref matching | Supported | Not supported |
| Connection test (`ListDeviceChannels`) | Supported | Not supported |
| Refresh devices (scanner scan) | Supported | Not supported |

Jellyfin implements only lineup-reload capabilities. Sync/reverse-sync/test
paths resolve `MappingProvider` and therefore reject Jellyfin with
`ErrUnsupportedProvider` before any mapping API call is attempted.

## Mapping Conflict Precedence

### Per-Channel Mapping vs Default Lineup

When a per-channel DVR mapping specifies a `dvr_lineup_id` and the instance config also has a `default_lineup_id`, the per-channel value wins. The default is only used as a fallback when the channel mapping's lineup ID is empty.

**Example:** Channel 5.1 has `dvr_lineup_id = "USA-OTA-90210"` and the instance config has `default_lineup_id = "USA-CABLE-10001"`. Forward sync groups channel 5.1 under lineup `USA-OTA-90210`, ignoring the default.

### Ambiguous Station Ref Resolution

When multiple stations in a lineup share the same `lineup_channel` value, resolution follows this order:

1. **Callsign hint match** — if `dvr_callsign_hint` is set, the station whose `callSign` matches (case-insensitive) is selected
2. **Lexicographic fallback** — if no callsign match or no hint is configured, the station ref that sorts first lexicographically is used, and a warning is emitted

**Example:** Lineup `USA-OTA-90210` has two stations on lineup channel `7.1`: station ref `S-12345` (callsign `KABC`) and `S-67890` (callsign `KABCDT`). If the channel mapping has `dvr_callsign_hint = "KABC"`, station `S-12345` is selected. Without a hint, `S-12345` is still selected (sorts first), but a warning is logged.

### Mirror Device Mode with Unmapped Channels

In `mirror_device` sync mode, every device channel key known to the provider is evaluated. Device keys that have a configured mapping in hdhriptv are set to the resolved station ref. Device keys without a configured mapping have their station ref set to an empty string, effectively clearing the provider-side mapping.

**Example:** The provider reports 150 device channels. hdhriptv has mappings for 100 of them. Forward sync in `mirror_device` mode updates the 100 mapped keys and clears the remaining 50. In `configured_only` mode, those 50 would be left untouched.

### Stale Cached Station Ref Detection

When updating a channel mapping via the API, if identity fields (`dvr_lineup_id`, `dvr_lineup_channel`, `dvr_callsign_hint`) changed but the caller echoed the old cached `dvr_station_ref`, the station ref is cleared. This forces forward sync to re-resolve from the updated lineup/channel configuration rather than using a now-incorrect cached value.

## Retry and Timeout Behavior

### Internal Timeout Budget

All DVR sync and reverse-sync API requests run in a **detached context** (`context.WithoutCancel`) with an internal timeout of **2 minutes** (`defaultDVRSyncTimeout`). This means the sync operation continues even if the originating HTTP client disconnects. DVR config mutations (PUT) use a shorter **30-second** timeout (`defaultDVRMutationTimeout`).

Individual HTTP calls to provider APIs use a **10-second** per-request timeout set on the `http.Client`.

### Channels DVR Call Path

**Forward sync** makes multiple sequential provider calls:

1. `ReloadDeviceLineup` — POST to `/devices/{deviceID}/channels`
2. Optional `RefreshDevices` — PUT to `/dvr/scanner/scan` (fallback: POST `/devices`)
3. `ListDeviceChannels` — GET `/dvr/guide/channels`
4. Per lineup: `ListLineupStations` + `GetCustomMapping` (two GET calls)
5. Per lineup: `PutCustomMapping` in batches of 75 entries

**Batch patch behavior:** patches are split into chunks of 75 key-value pairs (`syncBatchSize`) with deterministic key ordering. Each chunk is a separate PUT request. If any batch fails with an HTTP error, the entire sync operation aborts immediately — already-applied batches are **not** rolled back.

**Lineup discovery** tries `/dvr/lineups` first. On failure or empty response, it falls back to `/devices`. Both are walked recursively by `collectLineups()`.

**Reload quirk:** the Channels DVR reload endpoint sometimes returns HTTP 400 with the refreshed channel list as the response body (possibly base64-encoded). The provider detects this pattern and treats it as success.

### Jellyfin Call Path

**Lineup reload** performs a three-step flow:

1. `GET /System/Configuration/livetv` — fetch live TV config to find tuner hosts
2. `POST /LiveTv/TunerHosts` — re-post the matched tuner host object to trigger channel refresh
3. `GET /ScheduledTasks` — best-effort observability check for `RefreshGuide` task state (failure is silently ignored)

The tuner host is selected via `selectTunerHost()` priority (see Tuner Host Selection above). All Jellyfin requests require `X-Emby-Token` authentication; missing token errors are returned as `ErrDVRSyncConfig`.

### Partial Failure Semantics

| Scenario | Behavior |
|----------|----------|
| Batch N of M fails (Channels DVR) | Batches 1..N-1 are already applied; sync returns error; no rollback |
| Provider unreachable | Sync fails immediately with HTTP client error |
| `syncRunLock` held | Returns `ErrSyncAlreadyRunning` (HTTP 409 Conflict) |
| Jellyfin `GET /ScheduledTasks` fails | Silently ignored; reload is still considered successful |
| Multi-provider reload: one provider fails | Error returned immediately; remaining providers are not attempted |
| Multi-provider reload: Channels skip (missing/invalid base URL) | Non-fatal skip; other providers proceed; skip reasons returned |
| Multi-provider reload: Jellyfin skip (missing token/URL) | Non-fatal skip; other providers proceed; skip reasons returned |

When forward sync returns an error, the API returns that error response; partial counters are not emitted as a successful `SyncResult` payload.

## Admin API Endpoints

| Method | Path | Handler | Description |
|--------|------|---------|-------------|
| GET | `/api/admin/dvr` | `handleGetDVR` | Get DVR config state (token redacted) |
| PUT | `/api/admin/dvr` | `handlePutDVR` | Update DVR config with scheduler rollback |
| POST | `/api/admin/dvr/test` | `handleTestDVR` | Test provider connectivity |
| GET | `/api/admin/dvr/lineups` | `handleDVRLineups` | List lineups (optional `?refresh=true`) |
| POST | `/api/admin/dvr/sync` | `handleDVRSync` | Trigger forward sync |
| POST | `/api/admin/dvr/reverse-sync` | `handleDVRReverseSync` | Trigger bulk reverse sync |
| GET | `/api/channels/dvr` | `handleListChannelDVRMappings` | List channel mappings (paginated) |
| GET | `/api/channels/{channelID}/dvr` | `handleGetChannelDVRMapping` | Get single channel mapping |
| PUT | `/api/channels/{channelID}/dvr` | `handlePutChannelDVRMapping` | Update single channel mapping |
| POST | `/api/channels/{channelID}/dvr/reverse-sync` | `handleChannelDVRReverseSync` | Reverse sync single channel |
