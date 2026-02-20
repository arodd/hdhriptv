# Architecture

This document describes the runtime structure of `hdhriptv`, how major
data paths move through the system, and exactly where public/admin routes are
implemented in code.

## Runtime Composition

`cmd/hdhriptv/main.go` wires all subsystems and starts:

- HTTP server(s) for HDHR endpoints, admin UI/API, `/healthz`, and optional
  `/metrics`
- UDP discovery server on port `65001`
- optional UPnP/SSDP responder on UDP `1900`
- automation scheduler and asynchronous job runner
- optional background source prober

Core subsystem ownership:

- Playlist ingest and catalog persistence
  - `internal/playlist/manager.go`
  - `internal/playlist/refresh.go`
  - `internal/m3u/parser.go`
  - `internal/store/sqlite/catalog_index.go`
- Published channels and source graph
  - `internal/channels/service.go`
  - `internal/store/sqlite/channels.go`
- Stream session manager and tuner leasing
  - `internal/stream/handler.go`
  - `internal/stream/shared_session.go`
  - `internal/stream/pump.go`
  - `internal/stream/ring.go`
  - `internal/stream/tuners.go`
  - `internal/stream/ffmpeg.go`
- Automation jobs and schedules
  - `internal/jobs/runner.go`
  - `internal/jobs/playlist_sync.go`
  - `internal/jobs/auto_prioritize.go`
  - `internal/scheduler/scheduler.go`
  - `internal/store/sqlite/job_runs.go`
  - `internal/store/sqlite/metrics.go`
- DVR integration
  - `internal/dvr/service.go`
  - `internal/dvr/channels_provider.go`
  - `internal/dvr/jellyfin_provider.go`
  - `internal/store/sqlite/dvr.go`
- HTTP surfaces
  - Public HDHR: `internal/hdhr/http_handlers.go`
  - UDP discovery: `internal/hdhr/discovery/udp.go`
  - UPnP/SSDP discovery: `internal/hdhr/upnp/server.go` and `internal/hdhr/upnp/protocol.go`
  - Admin: `internal/http/admin_routes.go`
  - Dynamic channel immediate-sync orchestration: `internal/http/admin_routes.go` (coalesced/cancelable worker loop per channel)
  - Automation admin handlers: `internal/http/admin_automation.go`
  - DVR admin handlers: `internal/http/admin_dvr.go`
  - Middleware: `internal/http/middleware/*.go`

Persistence is SQLite (`internal/store/sqlite/store.go`) with migrations under
`internal/store/sqlite/migrations/`.

## Key Data Flows

### 1. Playlist Sync Flow

1. Trigger source:
   - startup one-shot sync in `cmd/hdhriptv/main.go`
   - manual trigger: `POST /api/admin/jobs/playlist-sync/run`
   - scheduled trigger via `internal/scheduler/scheduler.go`
2. Job runner starts persisted run in `internal/jobs/runner.go`.
3. `internal/jobs/playlist_sync.go` executes:
   - refresh catalog via `internal/playlist/refresh.go`
   - reconcile channel sources via `internal/reconcile/reconcile.go`
   - optional post-sync DVR lineup reload hook (`DVRLineupReloader`), with
     active-provider fan-out and provider-aware skip semantics for incomplete
     Jellyfin config
4. Catalog refresh upserts active rows and marks unseen rows inactive in
   `internal/store/sqlite/catalog_index.go`.
   - IOERR diagnostics are enriched in `internal/store/sqlite/error_diag.go`,
     `internal/store/sqlite/ioerr_diag.go`, and
     `internal/store/sqlite/op_trace.go`:
     - one-shot `sqlite_ioerr_diag_bundle` captures runtime pragma/db-file stats
     - optional `sqlite_ioerr_trace_dump` emits a rate-limited operation timeline
       from an in-memory fixed-size trace ring.
5. Reconcile appends/synchronizes channel sources using `channels.Service`.

### 2. Stream Tune Flow (`/auto/...`)

1. `internal/stream/handler.go` resolves guide number and channel metadata.
2. Global tune backoff is checked before creating a new shared session.
3. `SessionManager` in `internal/stream/shared_session.go` creates/reuses
   one shared runtime session per channel.
4. Session acquires tuner lease (`internal/stream/tuners.go`), selects a source
   with cooldown-aware ordering, and starts producer (`internal/stream/ffmpeg.go`
   and `internal/stream/producer.go`).
5. Pump publishes chunks into ring buffer (`internal/stream/pump.go` and
   `internal/stream/ring.go`) and subscribers stream the same shared bytes.
6. Source health, recovery cycle telemetry, and stall handling are updated in
   shared session logic.

### 3. Auto-prioritize Flow

1. Trigger source:
   - manual trigger: `POST /api/admin/jobs/auto-prioritize/run`
   - scheduler callback
2. `internal/jobs/auto_prioritize.go`:
   - collects enabled channel sources
   - reuses cached metrics where fresh (`stream_metrics`)
   - probes pending sources via analyzer (`internal/analyzer/ffmpeg.go`)
   - computes normalized per-channel ranking and reorders source priority
3. Run status/progress/summary are persisted in `job_runs`.

### 4. DVR Forward Sync Flow (hdhriptv -> provider)

1. Trigger source:
   - manual trigger: `POST /api/admin/dvr/sync`
   - scheduler callback for `dvr_lineup_sync`
   - HTTP-triggered runs are detached from request cancellation and execute
     under an internal timeout budget (`AdminHandler.dvrSyncTimeout`,
     default `2m`)
2. `internal/dvr/service.go` loads configured mappings and provider lineup data.
3. Service resolves station references per channel and applies provider patch
   (dry run supported).
4. Resolved station references are persisted back to mapping cache.
5. Provider scope note:
   - `channels` provider supports full forward sync operations.
   - `jellyfin` provider mode focuses on lineup refresh orchestration and
     returns explicit unsupported-provider errors for custom-mapping paths.

### 5. DVR Reverse Sync Flow (provider -> hdhriptv)

1. Trigger source:
   - global: `POST /api/admin/dvr/reverse-sync`
   - per-channel: `POST /api/channels/{channelID}/dvr/reverse-sync`
   - HTTP-triggered runs are detached from request cancellation and execute
     under an internal timeout budget (`AdminHandler.dvrSyncTimeout`,
     default `2m`)
2. `internal/dvr/service.go` loads provider custom mapping and lineup stations.
3. Service maps tuner/channel keys back into channel DVR mapping rows.
4. For station entries missing lineup channel, station-ref-only mappings are
   preserved and warning counts are returned.

### 6. Dynamic Channel Immediate-Sync Flow

1. Trigger source:
   - channel create/update requests carrying `dynamic_rule`
   - `POST /api/channels`
   - `PATCH /api/channels/{channelID}`
2. `internal/http/admin_routes.go` normalizes the channel update response first,
   then queues background sync work for eligible rules (`enabled=true` and
   non-empty `search_query`).
   - `search_query` uses the same token semantics as `/api/items?q=...`:
     OR-disjunct separators (`|` or standalone `OR`) with include terms and
     exclusion tokens prefixed with `-` or `!`. Queries with no OR separator
     preserve legacy include/exclude AND behavior.
   - token-mode parser limits are runtime-configurable through
     `catalog-search-max-terms`, `catalog-search-max-disjuncts`, and
     `catalog-search-max-term-runes` (`CATALOG_SEARCH_MAX_*` env aliases).
   - truncation remains non-fatal; additive `search_warning` response metadata
     reports effective limits and applied/dropped token counts.
3. Queue behavior is per-channel and versioned:
   - in-flight runs can be superseded by newer updates
   - rapid updates are coalesced to the latest request
   - disable/delete transitions cancel pending and active sync runs
   - execution is detached from HTTP request cancellation; queued/running sync
     work continues after client disconnect unless canceled by rule-disable/delete
     transitions or the per-run timeout budget
4. Sync execution path:
   - list matching active catalog item keys via
     `catalog.ListActiveItemKeysByCatalogFilter(...)`
   - apply source reconciliation via `channels.Service.SyncDynamicSources(...)`
5. Reconciliation behavior:
   - adds missing `dynamic_query` sources for matched items
   - removes no-longer-matched `dynamic_query` sources
   - preserves manual source associations
   - promotes matching `channel_key` associations to `dynamic_query`

### 7. Dynamic Block Materialization Flow (`10000+`)

1. Trigger sources:
   - playlist reconcile (`internal/reconcile/reconcile.go`)
   - dynamic block CRUD (`/api/dynamic-channels*`) through immediate background sync
2. `channels.Service.SyncDynamicChannelBlocks(...)` materializes each enabled
   query into generated rows in `published_channels` with
   `channel_class=dynamic_generated`.
3. Generated rows are allocated in per-block ranges:
   - `block_start = 10000 + (order_index * 1000)`
   - generated rows are capped at `1000` per block
4. Reorder operations (`PATCH /api/dynamic-channels/{queryID}/channels/reorder`)
   persist deterministic guide-number reassignment within a block.
5. After successful materialization/reorder changes, admin routes trigger
   `DVRService.ReloadLineup(...)` as a best-effort post-change action so
   provider-side lineup views can pick up updated `10000+` guides.

## Route Implementation Map

All admin routes are registered in `internal/http/admin_routes.go`.
Automation routes are conditionally registered when automation dependencies are
wired, and DVR routes are conditionally registered when DVR service is wired.

### Public HDHR and Ops Routes

| Method | Path | Handler | Implementation |
| --- | --- | --- | --- |
| `GET` | `/discover.json` | `Handler.DiscoverJSON` | `internal/hdhr/http_handlers.go` |
| `GET` | `/lineup.json` | `Handler.LineupJSON` | `internal/hdhr/http_handlers.go` |
| `GET` | `/lineup.m3u` | `Handler.LineupM3U` | `internal/hdhr/http_handlers.go` |
| `GET` | `/lineup.xml` | `Handler.LineupXML` | `internal/hdhr/http_handlers.go` |
| `GET` | `/lineup_status.json` | `Handler.LineupStatusJSON` | `internal/hdhr/http_handlers.go` |
| `GET` | `/lineup.html` | `Handler.LineupHTML` | `internal/hdhr/http_handlers.go` |
| `GET` | `/upnp/device.xml` | `Handler.DeviceDescriptionXML` | `internal/hdhr/http_handlers.go` |
| `GET` | `/device.xml` | `Handler.DeviceDescriptionXML` | `internal/hdhr/http_handlers.go` |
| `GET` | `/upnp/scpd/connection-manager.xml` | `Handler.ConnectionManagerSCPDXML` | `internal/hdhr/upnp_control.go` |
| `GET` | `/upnp/scpd/content-directory.xml` | `Handler.ContentDirectorySCPDXML` | `internal/hdhr/upnp_control.go` |
| `POST` | `/upnp/control/connection-manager` | `Handler.ConnectionManagerControl` | `internal/hdhr/upnp_control.go` |
| `POST` | `/upnp/control/content-directory` | `Handler.ContentDirectoryControl` | `internal/hdhr/upnp_control.go` |
| `GET` | `/auto/{guide}` | `stream.Handler.ServeHTTP` | `internal/stream/handler.go` |
| `GET` | `/` | inline redirect (`/ui/`) | `cmd/hdhriptv/main.go` |
| `GET` | `/healthz` | inline handler | `cmd/hdhriptv/main.go` |
| `GET` | `/metrics` | promhttp handler (optional) | `cmd/hdhriptv/main.go` |
| `UDP` | `:65001` discovery | `Server.Serve` | `internal/hdhr/discovery/udp.go` |
| `UDP` | `:1900` SSDP (`UPNP_ENABLED=true`) | `Server.Serve` | `internal/hdhr/upnp/server.go` |

Behavior notes:

- Stream routing accepts `/auto/v{guide}` and `/auto/{guide}`; the handler
  normalizes a leading `v` before guide-number lookup.
- UPnP SSDP responder (when enabled) answers `M-SEARCH` for `ssdp:all`,
  `upnp:rootdevice`, `uuid:<derived DeviceID UDN>`,
  `urn:schemas-upnp-org:device:MediaServer:1`, and
  `urn:schemas-upnp-org:device:Basic:1`,
  `urn:schemas-atsc.org:device:primaryDevice:1.0`,
  `urn:schemas-upnp-org:service:ConnectionManager:1`, and
  `urn:schemas-upnp-org:service:ContentDirectory:1`.
- UPnP parser accepts both SSDP `M-SEARCH` request-line variants used in the wild:
  `HTTP/1.1` and legacy `HTTP/1.0`.
- UPnP control surfaces intentionally expose a bounded read-only action subset:
  `ConnectionManager` (`GetProtocolInfo`, `GetCurrentConnectionIDs`,
  `GetCurrentConnectionInfo`) and `ContentDirectory`
  (`GetSearchCapabilities`, `GetSortCapabilities`, `GetSystemUpdateID`,
  `Browse`).
- `stream.Handler.ServeHTTP` applies the global tune-backoff gate only when a
  request would create a new shared session/source startup.
  - If a session for that channel is already active or pending, additional
    subscribers bypass tune backoff and can join immediately.
  - Rejected tune attempts return HTTP `503` with a `Retry-After` header and
    log `reason=global_tune_backoff`.

### Admin UI Routes

| Method | Path | Handler | Implementation |
| --- | --- | --- | --- |
| `GET` | `/ui/` | `handleUIRoot` | `internal/http/admin_routes.go` |
| `GET` | `/ui/catalog` | `handleUICatalog` | `internal/http/admin_routes.go` |
| `GET` | `/ui/channels` | `handleUIChannels` | `internal/http/admin_routes.go` |
| `GET` | `/ui/channels/{channelID}` | `handleUIChannelDetail` | `internal/http/admin_routes.go` |
| `GET` | `/ui/dynamic-channels/{queryID}` | `handleUIDynamicChannelDetail` | `internal/http/admin_routes.go` |
| `GET` | `/ui/merge` | `handleUIMerge` | `internal/http/admin_routes.go` |
| `GET` | `/ui/tuners` | `handleUITuners` | `internal/http/admin_routes.go` |
| `GET` | `/ui/automation` | `handleUIAutomation` | `internal/http/admin_routes.go` |
| `GET` | `/ui/dvr` | `handleUIDVR` | `internal/http/admin_routes.go` |

UI behavior notes:

- `/ui/catalog` uses a toolbar-driven workflow for high-volume source assignment:
  - multi-group filter chips backed by `/api/groups`
  - target-channel rapid-add mode (row actions switch to `Add Source`)
  - toolbar-driven dynamic channel creation from current filter context
- `/ui/channels` row metadata highlights dynamic-rule status and per-channel source composition (`enabled/total`, dynamic-managed count, manual-managed count).
- `/ui/tuners` includes a bottom `Shared Session History` master-detail panel populated from `/api/admin/tuners` `session_history` data, with status/recovery/error filters, deterministic selection, tabbed detail panes (`Summary`, `Sources`, `Subscribers`, `Recovery`), and truncation-state messaging.

### Admin API Routes: Catalog, Channels, Sources, Tuner Status

| Method | Path | Handler | Implementation |
| --- | --- | --- | --- |
| `GET` | `/api/groups` | `handleGroups` | `internal/http/admin_routes.go` |
| `GET` | `/api/items` | `handleItems` | `internal/http/admin_routes.go` |
| `GET` | `/api/channels` | `handleChannels` | `internal/http/admin_routes.go` |
| `POST` | `/api/channels` | `handleCreateChannel` | `internal/http/admin_routes.go` |
| `PATCH` | `/api/channels/reorder` | `handleReorderChannels` | `internal/http/admin_routes.go` |
| `PATCH` | `/api/channels/{channelID}` | `handleUpdateChannel` | `internal/http/admin_routes.go` |
| `DELETE` | `/api/channels/{channelID}` | `handleDeleteChannel` | `internal/http/admin_routes.go` |
| `GET` | `/api/dynamic-channels` | `handleDynamicChannelQueries` | `internal/http/admin_routes.go` |
| `POST` | `/api/dynamic-channels` | `handleCreateDynamicChannelQuery` | `internal/http/admin_routes.go` |
| `GET` | `/api/dynamic-channels/{queryID}` | `handleGetDynamicChannelQuery` | `internal/http/admin_routes.go` |
| `PATCH` | `/api/dynamic-channels/{queryID}` | `handleUpdateDynamicChannelQuery` | `internal/http/admin_routes.go` |
| `DELETE` | `/api/dynamic-channels/{queryID}` | `handleDeleteDynamicChannelQuery` | `internal/http/admin_routes.go` |
| `GET` | `/api/dynamic-channels/{queryID}/channels` | `handleDynamicGeneratedChannels` | `internal/http/admin_routes.go` |
| `PATCH` | `/api/dynamic-channels/{queryID}/channels/reorder` | `handleReorderDynamicGeneratedChannels` | `internal/http/admin_routes.go` |
| `GET` | `/api/channels/{channelID}/sources` | `handleSources` | `internal/http/admin_routes.go` |
| `POST` | `/api/channels/{channelID}/sources` | `handleAddSource` | `internal/http/admin_routes.go` |
| `POST` | `/api/channels/{channelID}/sources/health/clear` | `handleClearSourceHealth` | `internal/http/admin_routes.go` |
| `PATCH` | `/api/channels/{channelID}/sources/reorder` | `handleReorderSources` | `internal/http/admin_routes.go` |
| `PATCH` | `/api/channels/{channelID}/sources/{sourceID}` | `handleUpdateSource` | `internal/http/admin_routes.go` |
| `DELETE` | `/api/channels/{channelID}/sources/{sourceID}` | `handleDeleteSource` | `internal/http/admin_routes.go` |
| `POST` | `/api/channels/sources/health/clear` | `handleClearAllSourceHealth` | `internal/http/admin_routes.go` |
| `GET` | `/api/suggestions/duplicates` | `handleDuplicateSuggestions` | `internal/http/admin_routes.go` |
| `GET` | `/api/admin/tuners` | `handleTunerStatus` | `internal/http/admin_routes.go` |
| `POST` | `/api/admin/tuners/recovery` | `handleTriggerTunerRecovery` | `internal/http/admin_routes.go` |

Behavior notes:

- Admin mutation handlers that decode JSON through `decodeJSON(...)` enforce
  strict JSON parsing:
  - unknown fields are rejected (`json.Decoder.DisallowUnknownFields`)
  - trailing data after the first JSON value is rejected
  - malformed/invalid JSON returns HTTP `400`
  - oversized bodies are rejected with HTTP `413` via `http.MaxBytesReader`
- `handleItems` (`GET /api/items`) accepts optional `group`/`group_names` and `q` filters plus
  `limit`/`offset` pagination hints.
  - group filter semantics:
    - repeated `group` parameters are accepted (`?group=News&group=Sports`)
    - comma-separated values are accepted (`?group=News,Sports`)
    - `group_names` is accepted as a compatibility alias
    - empty/omitted group filters mean all groups
  - `q` supports case-insensitive OR-of-AND include/exclude matching:
    - include: `fox`
    - exclude: `-spanish` or `!spanish`
    - disjunct separators: `|` or standalone `OR` keyword
    - within each disjunct, terms are AND-combined; across disjuncts, clauses are OR-combined
    - exclusion-only queries are allowed
    - queries without OR separators preserve legacy include/exclude AND behavior
  - optional `q_regex` boolean is accepted (`1/0`, `true/false`, `yes/no`, `on/off`) and defaults to `false`.
    - `q_regex=false` keeps token/LIKE matching semantics.
    - `q_regex=true` evaluates `q` as one case-insensitive regex pattern against the full item name string.
    - regex mode bypasses token operators (`|`/`OR`, `-term`/`!term`); those operators apply only in token mode.
    - invalid regex patterns or overlong regex patterns fail request validation with HTTP `400`.
  - responses include additive `search_warning` metadata with:
    - `mode`, `truncated`
    - effective limits: `max_terms`, `max_disjuncts`, `max_term_runes`
    - counters: `terms_applied`, `terms_dropped`, `disjuncts_applied`,
      `disjuncts_dropped`, `term_rune_truncations`
  - token-mode over-limit inputs return `200` and are visibility-reported via
    `search_warning.truncated=true` (queries are not hard-rejected).
  - `limit` defaults to `100`, clamps to a hard max of `1000`, and values `<1`
    normalize back to `100`.
  - `offset` defaults to `0`; negative values normalize to `0`.
  - Non-integer `limit`/`offset` values fall back to defaults (they do not
    return HTTP `400`).
- `handleChannels` (`GET /api/channels`) and `handleSources`
  (`GET /api/channels/{channelID}/sources`) use strict pagination parsing.
  - `limit` defaults to `200` when omitted.
  - explicit `limit=0` is normalized to the same bounded default (`200`).
  - hard caps apply (`1000` for channels, `2000` for per-channel sources).
  - negative or non-integer `limit`/`offset` values return HTTP `400`.
- `GET /api/channels` is scoped to traditional channels (`channel_class=traditional`);
  generated dynamic rows are surfaced under `/api/dynamic-channels/{queryID}/channels`.
- `GET /api/channels` responses include per-channel source summary fields:
  - `source_total`
  - `source_enabled`
  - `source_dynamic` (`association_type=dynamic_query`)
  - `source_manual` (`association_type!=dynamic_query`)
- `handleClearSourceHealth` (`POST /api/channels/{channelID}/sources/health/clear`)
  and `handleClearAllSourceHealth` (`POST /api/channels/sources/health/clear`)
  reset persisted source-health counters/cooldown fields and return
  `{"cleared":<count>}`.
- `handleDuplicateSuggestions` (`GET /api/suggestions/duplicates`) normalizes
  query inputs before delegating to channel duplicate grouping.
  - `min` defaults to `2`, clamps to `[2, 100]`.
  - `q` is case-insensitive across `channel_key` and `tvg_id`.
  - legacy `tvg_id` query fallback is accepted when `q` is omitted.
  - response echoes normalized `min` and `q`.
- `handleTunerStatus` (`GET /api/admin/tuners`) returns both live tuner/session
  rows and bounded shared-session history:
  - `session_history` is newest-first and includes active + recently closed sessions
    tracked in-memory during process lifetime.
  - `session_history_limit` reports current retention capacity (default `256`).
  - `session_history_truncated_count` reports the total number of oldest
    history entries evicted due to retention.
  - each `session_history` entry includes per-session timeline guardrails:
    `source_history_limit`, `source_history_truncated_count`,
    `subscriber_history_limit`, and
    `subscriber_history_truncated_count`.
  - history source URLs (`session_history[*].sources[*].stream_url`) are
    sanitized with the same credential/query redaction applied to live status
    source URLs.
- `handleTriggerTunerRecovery` (`POST /api/admin/tuners/recovery`) accepts a
  strict JSON body with `channel_id` (required, `>0`) and optional `reason`.
  - when `reason` is omitted/blank, it defaults to `ui_manual_trigger`.
  - successful requests return HTTP `200` with `{"accepted":true,...}`.
  - returns HTTP `503` when tuner status/recovery support is not configured.
  - returns HTTP `404` when the channel has no active shared session and HTTP
    `409` when a manual recovery request is already pending for that session.
- `POST /api/channels` and `PATCH /api/channels/{channelID}` accept optional
  `dynamic_rule` payloads and return promptly; dynamic source sync runs
  asynchronously in a background worker managed by `admin_routes.go`.
  - `dynamic_rule` supports preferred `group_names` multi-group payloads and
    legacy `group_name` compatibility aliasing.
  - `dynamic_rule.search_query` follows the same OR-capable include/exclude
    semantics as `GET /api/items` when `search_regex=false` (`|`/`OR` plus
    `-term`/`!term`; no OR separator keeps legacy AND behavior).
  - `dynamic_rule.search_regex` is an optional boolean toggle (defaults `false`)
    and is persisted with the rule payload.
    - when enabled, matching evaluates `search_query` as one case-insensitive
      regex pattern against the full item name string.
    - token operators apply only when regex mode is disabled.
    - invalid regex inputs are rejected before persistence.
  - when both are provided, normalized `group_names` semantics apply and
    `group_name` is treated as an alias of the first normalized entry.
  - create/update responses include additive `search_warning` metadata for
    `dynamic_rule.search_query` using the same schema as `/api/items`.
  - queued/running dynamic sync execution is request-detached and bounded by
    `AdminHandler.dynamicSyncTimeout` before timeout cancellation.
- Dynamic block CRUD handlers queue request-detached immediate block sync work
  (`enqueueDynamicBlockSync`/`runDynamicBlockSyncLoop`) with coalescing and
  cancellation of stale runs; successful changed runs trigger best-effort DVR
  lineup reload through `DVRService.ReloadLineup(...)`.
  - dynamic block create/update/read/list responses include per-query additive
    `search_warning` metadata so persisted token-mode truncation remains
    operator-visible in control-plane reads.
- Regex-mode UI toggles are exposed in the catalog search toolbar, channel
  dynamic-rule editor, dynamic block list/create surface, and dynamic block
  detail editor so operators can switch between token and regex evaluation
  without changing query grammar.
  - those UI surfaces also render `search_warning` truncation summaries for
    token-mode over-limit queries.

### Admin API Routes: Automation and Jobs

| Method | Path | Handler | Implementation |
| --- | --- | --- | --- |
| `GET` | `/api/admin/automation` | `handleGetAutomation` | `internal/http/admin_automation.go` |
| `PUT` | `/api/admin/automation` | `handlePutAutomation` | `internal/http/admin_automation.go` |
| `POST` | `/api/admin/jobs/playlist-sync/run` | `handleRunPlaylistSync` | `internal/http/admin_automation.go` |
| `POST` | `/api/admin/jobs/auto-prioritize/run` | `handleRunAutoPrioritize` | `internal/http/admin_automation.go` |
| `POST` | `/api/admin/jobs/auto-prioritize/cache/clear` | `handleClearAutoPrioritizeCache` | `internal/http/admin_automation.go` |
| `GET` | `/api/admin/jobs/{runID}` | `handleGetJobRun` | `internal/http/admin_automation.go` |
| `GET` | `/api/admin/jobs` | `handleListJobRuns` | `internal/http/admin_automation.go` |

Behavior notes:

- `handlePutAutomation` acquires `AdminHandler.adminConfigMutationMu` (defined in
  `internal/http/admin_routes.go`) so automation and DVR config writes are
  serialized through one mutation critical section.
- `handlePutAutomation` applies partial updates:
  - top-level keys: `playlist_url`, `timezone`
  - schedule objects: `playlist_sync`, `auto_prioritize` (`enabled`, `cron_spec`)
  - analyzer keys: `probe_timeout_ms`, `analyzeduration_us`, `probesize_bytes`,
    `bitrate_mode`, `sample_seconds`, `enabled_only`, `top_n_per_channel`
- Schedule update normalization in `parseScheduleUpdate(...)` requires
  `cron_spec` when a schedule resolves to `enabled=true` and allows disabling a
  schedule without validating/storing a cron expression.
- Analyzer input validation in `handlePutAutomation` enforces:
  - positive values for `probe_timeout_ms`, `analyzeduration_us`,
    `probesize_bytes`, and `sample_seconds`
  - `bitrate_mode` allowlist of `metadata`, `sample`, or
    `metadata_then_sample`
  - `top_n_per_channel >= 0`
- `handlePutAutomation` validates cron values only when the target schedule is
  enabled, writes settings, then applies runtime scheduler state via
  `Scheduler.LoadFromSettings(...)`.
- If scheduler apply fails, `handlePutAutomation` restores prior settings via a
  snapshot/rollback path (`snapshotAutomationSettings` ->
  `restoreAutomationSettings`) before returning an error.
- `handleClearAutoPrioritizeCache` calls
  `AutomationSettingsStore.DeleteAllStreamMetrics(...)` and returns
  `{"deleted":<count>}` with the removed cache row count.
- `startJobRun` wraps `Runner.Start(...)` with `context.WithoutCancel(...)`,
  so manual trigger request cancellation does not cancel a queued/running job.
- `handleListJobRuns` validates `name` against the allowlist
  (`playlist_sync`, `auto_prioritize`, `dvr_lineup_sync`) and normalizes query
  paging (`limit` default `50`, clamped `1..500`; `offset` clamped to `>= 0`).
  - non-integer `limit`/`offset` inputs fall back to defaults (`50`/`0`).
  - non-allowlisted `name` values return HTTP `400`.
  - response echoes normalized `name`, `limit`, and `offset`.

### Admin API Routes: DVR

| Method | Path | Handler | Implementation |
| --- | --- | --- | --- |
| `GET` | `/api/admin/dvr` | `handleGetDVR` | `internal/http/admin_dvr.go` |
| `PUT` | `/api/admin/dvr` | `handlePutDVR` | `internal/http/admin_dvr.go` |
| `POST` | `/api/admin/dvr/test` | `handleTestDVR` | `internal/http/admin_dvr.go` |
| `GET` | `/api/admin/dvr/lineups` | `handleDVRLineups` | `internal/http/admin_dvr.go` |
| `POST` | `/api/admin/dvr/sync` | `handleDVRSync` | `internal/http/admin_dvr.go` |
| `POST` | `/api/admin/dvr/reverse-sync` | `handleDVRReverseSync` | `internal/http/admin_dvr.go` |
| `GET` | `/api/channels/dvr` | `handleListChannelDVRMappings` | `internal/http/admin_dvr.go` |
| `GET` | `/api/channels/{channelID}/dvr` | `handleGetChannelDVRMapping` | `internal/http/admin_dvr.go` |
| `PUT` | `/api/channels/{channelID}/dvr` | `handlePutChannelDVRMapping` | `internal/http/admin_dvr.go` |
| `POST` | `/api/channels/{channelID}/dvr/reverse-sync` | `handleChannelDVRReverseSync` | `internal/http/admin_dvr.go` |

Behavior notes:

- `GET /api/channels/dvr` supports optional query filtering:
  - `enabled_only` (`1`, `true`, `yes`, `on`) scopes results to enabled channels.
  - `include_dynamic` (`1`, `true`, `yes`, `on`) includes generated dynamic rows.
  - `limit` and `offset` provide strict bounded pagination.
    - `limit` defaults to `200`; explicit `limit=0` normalizes to `200`.
    - `limit` clamps to `1000`.
    - `offset` defaults to `0`.
    - negative or non-integer `limit`/`offset` values return HTTP `400`.
  - default behavior excludes `channel_class=dynamic_generated` rows.
- `handleListChannelDVRMappings` returns paged payload metadata:
  `mappings`, `total`, `limit`, `offset`, `enabled_only`, and `include_dynamic`.
- `handleDVRLineups` (`GET /api/admin/dvr/lineups`) accepts optional `refresh`
  query parsing with truthy values (`1`, `true`, `yes`, `on`) and echoes the
  applied boolean in the response payload as `refresh`.
- `handleDVRSync`, `handleDVRReverseSync`, and `handleChannelDVRReverseSync`
  decode optional JSON payloads via `decodeOptionalJSON(...)`.
  - Empty bodies are accepted, including chunked requests with unknown content
    length and no payload bytes.
  - Non-empty payloads still use strict `decodeJSON(...)` parsing, so unknown
    fields and trailing JSON return HTTP `400`.
  - Malformed JSON still returns HTTP `400`.
  - Defaults apply when body content is omitted (for example `dry_run=false`,
    `include_dynamic=false`).
- `handlePutDVR` also uses `AdminHandler.adminConfigMutationMu`, so DVR updates
  cannot interleave with automation updates.
- `handlePutDVR` validates enabled sync cron before persisting config, then
  calls `DVRScheduler.UpdateJobSchedule(...)` for `jobs.JobDVRLineupSync`.
- If scheduler apply fails after config persistence, `handlePutDVR` restores the
  prior DVR config (`restoreDVRConfig`) and returns an error describing whether
  rollback succeeded.
- DVR config response redaction:
  - `jellyfin_api_token` is write-only and redacted from
    `GET /api/admin/dvr` and `PUT /api/admin/dvr` responses.
  - response payloads expose `jellyfin_api_token_configured=true|false`.
- Provider-selection and config fields accepted by `handlePutDVR`:
  - `provider` (primary provider for sync/mapping/test workflows)
  - `active_providers` (post-playlist-sync reload fan-out target set)
  - per-provider base URLs:
    - `channels_base_url`
    - `jellyfin_base_url`
  - legacy `base_url` (maps to selected primary provider for compatibility)
  - optional `jellyfin_api_token` (header auth token)
  - optional `jellyfin_tuner_host_id` (host targeting override)
