# API and Endpoint Reference

## Network and Discovery

### Required Ports

| Protocol | Port | Purpose |
| --- | --- | --- |
| TCP | `5004` (default) | Main HTTP API, lineup, streams, admin UI/API |
| UDP | `65001` | HDHomeRun discovery listener |
| UDP | `1900` (optional) | UPnP/SSDP discovery responder (`M-SEARCH` + optional `NOTIFY`) |
| TCP | `80` (optional) | Legacy client compatibility listener |

### Discovery Behavior

- UDP discovery replies are emitted only for compatible tuner discovery requests.
- Discovery responses include:
  - `DeviceID`
  - `DeviceAuth`
  - tuner count
  - `BaseURL`
  - `LineupURL`
- If `HTTP_ADDR_LEGACY` is set, discovery advertises that legacy port.
- `BaseURL` host is selected from the local route toward the requesting client.
- If `UPNP_ENABLED=true`, the service also listens for SSDP `M-SEARCH` on UDP `1900` and responds for:
  - `ssdp:all`
  - `upnp:rootdevice`
  - `uuid:<derived from DeviceID>`
  - `urn:schemas-upnp-org:device:MediaServer:1`
  - `urn:schemas-upnp-org:device:Basic:1`
  - `urn:schemas-atsc.org:device:primaryDevice:1.0` (HDHomeRun-oriented compatibility target)
  - `urn:schemas-upnp-org:service:ConnectionManager:1`
  - `urn:schemas-upnp-org:service:ContentDirectory:1`
- SSDP parser accepts both `M-SEARCH * HTTP/1.1` and legacy `M-SEARCH * HTTP/1.0` request lines.
- UPnP responses advertise `LOCATION: http://<host>:<port>/upnp/device.xml` using the same host/port selection behavior as HDHR discovery.
- `GET /device.xml` is a compatibility alias to the same UPnP device description payload.
- `GET /upnp/scpd/connection-manager.xml` and `GET /upnp/scpd/content-directory.xml` return live SCPD XML (no UI redirect behavior).
- `POST /upnp/control/connection-manager` and `POST /upnp/control/content-directory` expose a bounded read-only SOAP action subset for interoperability:
  - `ConnectionManager`: `GetProtocolInfo`, `GetCurrentConnectionIDs`, `GetCurrentConnectionInfo`
  - `ContentDirectory`: `GetSearchCapabilities`, `GetSortCapabilities`, `GetSystemUpdateID`, `Browse`

### mDNS / Avahi

mDNS is optional and not required for core compatibility.
See `deploy/avahi/README.md` for optional `.local` helper setup.

## Public Discovery and Streaming Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/discover.json` | Device metadata for discovery |
| `GET` | `/lineup.json` | Published-channel lineup JSON |
| `GET` | `/lineup.xml` | Published-channel lineup XML |
| `GET` | `/lineup.m3u` | Published-channel lineup M3U |
| `GET` | `/lineup_status.json` | Scan status compatibility stub |
| `GET` | `/lineup.html` | Redirects to `/ui/` |
| `GET` | `/upnp/device.xml` | UPnP root device-description XML (`LOCATION` target for SSDP) |
| `GET` | `/device.xml` | Compatibility alias for `/upnp/device.xml` |
| `GET` | `/upnp/scpd/connection-manager.xml` | UPnP `ConnectionManager` SCPD XML |
| `GET` | `/upnp/scpd/content-directory.xml` | UPnP `ContentDirectory` SCPD XML |
| `POST` | `/upnp/control/connection-manager` | SOAP control endpoint (`ConnectionManager` action subset) |
| `POST` | `/upnp/control/content-directory` | SOAP control endpoint (`ContentDirectory` action subset, including read-only `Browse`) |
| `GET` | `/auto/v{GuideNumber}` | Stream channel by guide number |
| `GET` | `/healthz` | Health check |
| `GET` | `/metrics` | Prometheus metrics (when enabled) |

Notes:

- Stream routing also accepts `/auto/{guide}` and normalizes a leading `v`.
- Lineup URLs are built from enabled published channels only.
- HDHomeRun lineup endpoints (`/lineup.json`, `/lineup.xml`, `/lineup.m3u`) accept `?show=demo` and return an empty lineup for compatibility with HDHomeRun demo probing behavior.
- HDHomeRun endpoints set `Connection: close` on responses to match physical-device behavior. Clients that poll metadata endpoints frequently should expect one TCP connection per request instead of keepalive reuse.
- Unsupported or invalid UPnP SOAP actions return protocol-valid SOAP faults (instead of HTTP redirects).

## Admin UI and Admin API

All routes below are protected by Basic Auth when `ADMIN_AUTH` is configured.

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/ui/` | Redirects to `/ui/catalog` |
| `GET` | `/ui/catalog` | Catalog browsing with toolbar-driven dynamic channel creation, multi-group filtering, and rapid source-add target-channel mode |
| `GET` | `/ui/channels` | Split channel control plane: traditional channels (`100-9999`) with reorder/DVR mapping controls, plus dynamic channel block management (`10000+`) |
| `GET` | `/ui/channels/{channelID}` | Channel detail and source management |
| `GET` | `/ui/dynamic-channels/{queryID}` | Dynamic block detail: block config + generated-channel sorting controls |
| `GET` | `/ui/merge` | Duplicate suggestions grouped by `channel_key` |
| `GET` | `/ui/tuners` | Live tuner/session status, tuned source, connected subscribers, per-session recovery trigger actions, plus a history master-detail console with status/recovery filters and tabbed session diagnostics |
| `GET` | `/ui/automation` | Automation settings, schedules, and manual job triggers |
| `GET` | `/ui/dvr` | DVR provider configuration, mapping, and sync actions |
| `GET` | `/api/groups` | Paged playlist groups metadata (optional count suppression) |
| `GET` | `/api/items` | Filtered catalog items |
| `GET` | `/api/channels` | List traditional published channels (`100-9999`) including per-channel source summary fields (`source_total`, `source_enabled`, `source_dynamic`, `source_manual`) |
| `POST` | `/api/channels` | Create published channel from `item_key` with optional `dynamic_rule` |
| `PATCH` | `/api/channels/{channelID}` | Update channel (`guide_name`, `enabled`, optional `dynamic_rule`) |
| `PATCH` | `/api/channels/reorder` | Reorder complete channel list (`204 No Content` on success); successful reorder enqueues a coalesced DVR lineup reload (`debounce=60s`, `max_wait=300s`) |
| `DELETE` | `/api/channels/{channelID}` | Delete channel |
| `GET` | `/api/dynamic-channels` | List dynamic channel blocks (paged metadata response) |
| `POST` | `/api/dynamic-channels` | Create dynamic channel block |
| `GET` | `/api/dynamic-channels/{queryID}` | Read one dynamic channel block |
| `PATCH` | `/api/dynamic-channels/{queryID}` | Update dynamic channel block config |
| `DELETE` | `/api/dynamic-channels/{queryID}` | Delete dynamic channel block |
| `GET` | `/api/dynamic-channels/{queryID}/channels` | List generated channels for one dynamic block |
| `PATCH` | `/api/dynamic-channels/{queryID}/channels/reorder` | Reorder generated channels within one dynamic block (`204 No Content` on success); successful reorder enqueues a coalesced DVR lineup reload (`debounce=60s`, `max_wait=300s`) |
| `GET` | `/api/channels/{channelID}/sources` | List channel sources |
| `POST` | `/api/channels/{channelID}/sources` | Add source by `item_key` |
| `POST` | `/api/channels/{channelID}/sources/health/clear` | Clear health/cooldown state for one channel's sources |
| `PATCH` | `/api/channels/{channelID}/sources/{sourceID}` | Update source (`enabled`) |
| `PATCH` | `/api/channels/{channelID}/sources/reorder` | Reorder channel sources (`204 No Content` on success) |
| `DELETE` | `/api/channels/{channelID}/sources/{sourceID}` | Delete source |
| `POST` | `/api/channels/sources/health/clear` | Clear health/cooldown state for all channel sources |
| `GET` | `/api/suggestions/duplicates?min=2&q=cnn.us&limit=100&offset=0` | Paged duplicate catalog suggestions grouped by `channel_key` (optional case-insensitive search across `channel_key` and `tvg_id`) |
| `GET` | `/api/admin/tuners` | Runtime tuner/session snapshot including shared-session subscriber mappings, bounded `session_history` timelines, process-lifetime `drain_wait` / `probe_close` telemetry counters, and optional reverse-DNS client host resolution via `resolve_ip` |
| `POST` | `/api/admin/tuners/recovery` | Trigger manual shared-session recovery for an active channel (`channel_id`, optional `reason`) |
| `GET` | `/api/admin/automation` | Current automation state (`playlist_url`, schedule config, analyzer settings, next/last run) |
| `PUT` | `/api/admin/automation` | Update automation schedule/timezone/analyzer and playlist URL settings |
| `POST` | `/api/admin/jobs/playlist-sync/run` | Trigger async playlist sync run |
| `POST` | `/api/admin/jobs/auto-prioritize/run` | Trigger async auto-prioritize run |
| `POST` | `/api/admin/jobs/auto-prioritize/cache/clear` | Clear cached stream metrics used by auto-prioritize |
| `GET` | `/api/admin/jobs/{runID}` | Fetch one job run (`running`, `success`, `error`, `canceled`) |
| `GET` | `/api/admin/jobs?name=dvr_lineup_sync&limit=20&offset=0` | List recent job runs by optional name filter (`playlist_sync`, `auto_prioritize`, `dvr_lineup_sync`) |
| `GET` | `/api/admin/dvr` | Current DVR integration config, cached lineups, and last sync summary |
| `PUT` | `/api/admin/dvr` | Update DVR config (`active_providers`, per-provider base URLs, default lineup, and sync schedule/mode). Primary provider is channels-only for sync/mapping workflows (`provider` must resolve to `channels`; non-channels values are rejected). Jellyfin supports optional `jellyfin_tuner_host_id` and write-only `jellyfin_api_token` inputs for post-sync reload fan-out. Legacy `base_url` remains supported as a channels base URL alias. |
| `POST` | `/api/admin/dvr/test` | Verify DVR provider connectivity and device-channel visibility |
| `GET` | `/api/admin/dvr/lineups?refresh=1` | List DVR lineups (optional provider refresh) |
| `POST` | `/api/admin/dvr/sync` | Run forward sync (hdhriptv mapping to DVR custom lineup patch). Optional JSON body (`dry_run`, optional `include_dynamic`); empty body is accepted and defaults to `dry_run=false`, `include_dynamic=false`. Execution is detached from initiating request cancellation and bounded by an internal timeout budget (`2m` default). |
| `POST` | `/api/admin/dvr/reverse-sync` | Run reverse sync (provider custom mapping into hdhriptv channel mappings). Optional JSON body (`dry_run`, optional `lineup_id`, optional `include_dynamic`); empty body is accepted. Execution is detached from initiating request cancellation and bounded by an internal timeout budget (`2m` default). |
| `GET` | `/api/channels/dvr` | Paged read of per-channel DVR mappings (used by `/ui/channels`; supports optional `enabled_only=1`, optional `include_dynamic=1`, plus `limit`/`offset`; dynamic generated channels are excluded by default) |
| `GET` | `/api/channels/{channelID}/dvr` | Read one channel's DVR mapping |
| `PUT` | `/api/channels/{channelID}/dvr` | Update one channel's DVR lineup/channel/station-ref mapping |
| `POST` | `/api/channels/{channelID}/dvr/reverse-sync` | Reverse-sync one channel from DVR mapping state. Optional JSON body (`dry_run`, optional `lineup_id`); empty body is accepted. Execution is detached from initiating request cancellation and bounded by an internal timeout budget (`2m` default). |

## API Behavior Details

### Admin Mutation JSON Parsing

Admin mutation routes that decode JSON bodies use strict single-object parsing:

- unknown JSON fields are rejected with HTTP `400`
- trailing JSON content after the first object is rejected with HTTP `400`
- oversized bodies are rejected with HTTP `413` using `ADMIN_JSON_BODY_LIMIT_BYTES`
- optional-body DVR sync routes (`/api/admin/dvr/sync`, `/api/admin/dvr/reverse-sync`, `/api/channels/{channelID}/dvr/reverse-sync`) still enforce these rules when a non-empty body is provided

### Catalog Items (`GET /api/items`)

`GET /api/items` supports `group`, `group_names`, `q`, optional `q_regex`, `limit`, and `offset` query parameters:

- `group`/`group_names` and `q` are optional filters.
- Group filter semantics:
  - repeated `group` parameters are supported (`?group=News&group=Sports`)
  - comma-separated values are supported (`?group=News,Sports`)
  - `group_names` is accepted as a compatibility alias
  - empty/omitted group filters mean all catalog groups
- `q` token semantics are case-insensitive substring match with OR-of-AND support:
  - include token: `fox`
  - exclude token: `-spanish` or `!spanish`
  - disjunct separators: `|` or standalone `OR` keyword (case-insensitive)
  - within each disjunct, include and exclude terms are AND-combined
  - across disjuncts, clauses are OR-combined
  - exclusion-only queries are allowed
  - queries without OR separators keep legacy include/exclude AND behavior unchanged
- `q_regex` accepts boolean values (`1/0`, `true/false`, `yes/no`, `on/off`) and defaults to `false`.
  - when `q_regex=false` (default), token/LIKE behavior is unchanged.
  - when `q_regex=true`, `q` is evaluated as one case-insensitive regex pattern against the full item name string (including spaces/punctuation).
  - regex mode does not apply token `|`/`OR` or `-term`/`!term` query-language operators; use raw regex syntax instead.
  - invalid regex patterns and overlong regex patterns are rejected with HTTP `400`.
- `/api/items` response includes additive `search_warning` metadata:
  - `mode` (`token` or `regex`)
  - `truncated` (`bool`)
  - effective limits (`max_terms`, `max_disjuncts`, `max_term_runes`)
  - applied/dropped counters (`terms_applied`, `terms_dropped`, `disjuncts_applied`, `disjuncts_dropped`)
  - rune truncation counter (`term_rune_truncations`)
  - token-mode over-limit queries return `200` with `search_warning.truncated=true` (visibility-first behavior).
- UI regex toggles are available on:
  - `/ui/catalog` search toolbar
  - `/ui/channels` dynamic block create/quick-edit flows
  - `/ui/channels/{channelID}` dynamic rule editor
  - `/ui/dynamic-channels/{queryID}` block detail editor
- `limit` defaults to `100`, values above `1000` are clamped to `1000`, and values `<1` are normalized back to `100`.
- `offset` defaults to `0`; negative values are normalized to `0`.
- Non-integer `limit`/`offset` values are treated as defaults (`100`/`0`) rather than returning HTTP `400`.

### Tuner Status (`GET /api/admin/tuners`)

- `/api/admin/tuners` and `/ui/tuners` intentionally redact `source_stream_url` values. The status payload preserves scheme/host/path but strips URL userinfo, query, and fragment fields.
- `/api/admin/tuners` supports optional `resolve_ip` boolean query semantics:
  - accepted true values: `1`, `true`, `yes`, `on`
  - accepted false values: `0`, `false`, `no`, `off`
  - default is `false`
  - malformed values return HTTP `400`
  - when `resolve_ip=true`, the response populates `client_host` for:
    - `client_streams[*]`
    - `session_history[*].subscribers[*]`
  - reverse lookups run per unique IP and are memoized within a single response payload.
  - resolved hostnames (and lookup failures) are also memoized across requests via an in-process short TTL cache (`~2m`).
  - each lookup is bounded by a per-lookup timeout (`2s`) to avoid request-path stalls from long DNS waits.
  - the full resolve phase is bounded by a total request timeout budget (`8s`).
  - lookups still execute sequentially in request scope; large numbers of unique client IPs can increase endpoint latency.
  - reverse lookup failures are non-fatal and keep `client_host` empty for that address.
- `/api/admin/tuners` includes bounded history fields: `session_history` (newest-first), `session_history_limit`, and `session_history_truncated_count`.
  - `session_history` combines active shared sessions plus recently closed sessions retained in-memory for diagnostics.
  - each history session includes lifecycle/recovery aggregates (`opened_at`, optional `closed_at`, `active`, `terminal_status`, `peak_subscribers`, `recovery_cycle_count`, `same_source_reselect_count`) and nested `sources` / `subscribers` timelines.
  - `session_history.sources[*].stream_url` uses the same sanitization policy as live `source_stream_url` fields.
  - `session_history.subscribers[*]` captures `connected_at`, optional `closed_at`, and `close_reason` so disconnect timing can be correlated with upstream/client logs.
  - `session_history_limit` reports active retention capacity (default `256`), while `session_history_truncated_count` reports how many oldest entries were evicted since process start.
  - each history entry also includes per-session timeline retention/truncation metadata: `source_history_limit`, `source_history_truncated_count`, `subscriber_history_limit`, and `subscriber_history_truncated_count`.
- `/ui/tuners` renders a bottom `Shared Session History` master-detail panel sourced from `session_history`, with deterministic row selection, status/errors/recovery filters, tabbed detail panes (`Summary`, `Sources`, `Subscribers`, `Recovery`), and a truncation banner when evictions have occurred.
- `/api/admin/tuners` snapshot fields `recovery_keepalive_mode`, `recovery_keepalive_fallback_count`, and `recovery_keepalive_fallback_reason` report active keepalive mode and fallback history for each shared session.
- `/api/admin/tuners` also exposes keepalive pacing/backlog telemetry for recovery windows: `recovery_keepalive_started_at`, `recovery_keepalive_stopped_at`, `recovery_keepalive_duration`, `recovery_keepalive_bytes`, `recovery_keepalive_chunks`, `recovery_keepalive_rate_bytes_per_second`, `recovery_keepalive_expected_rate_bytes_per_second`, optional `recovery_keepalive_realtime_multiplier` (when profile bitrate is known), and guardrail fields `recovery_keepalive_guardrail_count` / `recovery_keepalive_guardrail_reason`.
- When `RECOVERY_FILLER_MODE=slate_av` and source profile dimensions are normalized, a debug log event `shared session slate AV recovery filler profile normalized` records `original_resolution`, `normalized_resolution`, and bounded `normalization_reason` tokens.
- `POST /api/admin/tuners/recovery` triggers the same in-session recovery flow used by stall detection. Request body: `{"channel_id":<id>,"reason":"optional"}`; omitted/blank `reason` defaults to `ui_manual_trigger`.
- `POST /api/admin/tuners/recovery` requires at least one active subscriber on the target shared session; idle-grace sessions with zero subscribers return `404` (`shared session not found`) and do not start recovery churn.

### Paging Behavior

- `/api/groups`, `/api/channels`, `/api/dynamic-channels`, `/api/channels/{channelID}/sources`, and `/api/dynamic-channels/{queryID}/channels` accept `limit` + `offset` query params and return paging metadata (`total`, `limit`, `offset`) in responses.
- Group/channel/dynamic-query/source/generated-channel list endpoints default to `limit=200` when omitted. `limit=0` is normalized to the same bounded default (`200`) instead of triggering unbounded/all-results reads.
- Hard caps are applied when `limit` is too large (`/api/groups`, `/api/channels`, `/api/dynamic-channels`, and `/api/dynamic-channels/{queryID}/channels` max `1000`, `/api/channels/{channelID}/sources` max `2000`).
- For those paged endpoints, `limit` and `offset` must be integers `>= 0`; negative or non-integer values return HTTP `400` (unlike `/api/items`, which normalizes non-integer values to defaults).

### Groups (`GET /api/groups`)

- `GET /api/groups` supports optional `include_counts` boolean query semantics:
  - accepted true values: `1`, `true`, `yes`, `on`
  - accepted false values: `0`, `false`, `no`, `off`
  - default is `true`
  - when `include_counts=false`, group entries only include `name` (count metadata is omitted), which is useful for autocomplete consumers
  - malformed `include_counts` values return HTTP `400`

### Source Health Clear

- `POST /api/channels/{channelID}/sources/health/clear` and `POST /api/channels/sources/health/clear` reset persisted source health/cooldown fields (`success_count`, `fail_count`, `last_ok_at`, `last_fail_at`, `last_fail_reason`, `cooldown_until`) and return `{"cleared":<count>}`.

### Duplicate Suggestions (`GET /api/suggestions/duplicates`)

- `min` defaults to `2`, clamps to `[2, 100]`.
- `q` performs case-insensitive matching across `channel_key` and `tvg_id`.
- `limit` defaults to `100`; `limit=0` also normalizes to `100`; values above `500` are clamped.
- `offset` defaults to `0`.
- `limit` and `offset` must be integers `>= 0`; negative or non-integer values return HTTP `400`.
- Legacy `tvg_id` query fallback is accepted when `q` is omitted.
- Response payload includes `total`, `limit`, and `offset`, and echoes normalized `min` and `q` values.

### DVR Channel Mappings (`GET /api/channels/dvr`)

- `GET /api/channels/dvr` accepts optional:
  - `enabled_only` (`1`, `true`, `yes`, `on`) to return only enabled-channel mappings.
  - `include_dynamic` (`1`, `true`, `yes`, `on`) to include dynamic generated channels.
  - `limit`/`offset` paging controls with strict integer parsing.
    - `limit` defaults to `200`; explicit `limit=0` normalizes to the same bounded default.
    - `limit` clamps at `1000`.
    - `offset` defaults to `0`.
    - negative or non-integer `limit`/`offset` values return HTTP `400`.
  - default behavior excludes dynamic generated channels.
  Response payload includes `mappings`, `total`, `limit`, `offset`, and echoes applied filters as `enabled_only` and `include_dynamic`.

### DVR Lineups (`GET /api/admin/dvr/lineups`)

- `GET /api/admin/dvr/lineups` accepts optional `refresh` (`1`, `true`, `yes`, `on`) to force a provider lineup refresh; response payload echoes the applied boolean as `refresh`.

### DVR Sync Endpoints

DVR sync routes parse optional JSON payloads using empty-body-tolerant decoding:

- `POST /api/admin/dvr/sync`
- `POST /api/admin/dvr/reverse-sync`
- `POST /api/channels/{channelID}/dvr/reverse-sync`
- Empty body requests (including `Transfer-Encoding: chunked` with no payload bytes) are accepted and use default request values.
- Malformed JSON payloads return HTTP `400`.
- `include_dynamic` defaults to `false` on sync and reverse-sync APIs.
- execution is detached from request cancellation after handler admission, and each run is bounded by an internal timeout budget (`2m` default).

### Channel Create/Update Dynamic Rules

- `POST /api/channels` and `PATCH /api/channels/{channelID}` accept an optional `dynamic_rule` object:
  - `enabled` (`bool`)
  - `group_name` (`string`, optional compatibility alias)
  - `group_names` (`[]string`, optional preferred multi-group filter contract)
  - `search_query` (`string`, required when `enabled=true`; token semantics match `GET /api/items?q=...` when `search_regex=false`)
  - `search_regex` (`bool`, optional; defaults to `false`)
    - when `true`, `search_query` is treated as one case-insensitive regex pattern matched against the full item name.
    - token operators (`|`/`OR`, `-term`/`!term`) are only applied when `search_regex=false`.
    - invalid regex inputs are rejected with HTTP `400` before persistence.
  - create/update responses include additive `search_warning` metadata for `dynamic_rule.search_query`, using the same warning fields as `/api/items`.

### Dynamic Channel Blocks

- `POST /api/dynamic-channels` and `PATCH /api/dynamic-channels/{queryID}` accept the same search filter contract:
  - `search_query` (`string`, optional)
  - `search_regex` (`bool`, optional; defaults to `false`)
    - regex-mode validation semantics match `/api/items` and channel dynamic-rule validation.
  - create/update/read/list responses include additive `search_warning` metadata per query row, so token-mode truncation is visible for persisted dynamic block queries.
- Channel create/update responses include normalized `dynamic_rule` values. Dynamic-rule sync executes asynchronously in the background, so these requests are not blocked on catalog/source reconciliation.
- `dynamic_rule.group_name` remains supported for legacy clients. When both fields are present, normalized `group_names` semantics are used and `group_name` is treated as a compatibility alias of the first normalized `group_names` entry.
- `GET /api/channels` exposes per-channel observability fields:
  - `source_total`: total associated sources
  - `source_enabled`: enabled associated sources
  - `source_dynamic`: associations managed by dynamic query reconciliation (`association_type=dynamic_query`)
  - `source_manual`: associations managed outside dynamic query reconciliation (`association_type!=dynamic_query`)
- Dynamic-rule background sync is detached from request cancellation and runs under an internal timeout budget per sync cycle; client disconnects after create/update do not cancel already-queued immediate sync work.
- When a newer enabled `dynamic_rule` update supersedes an in-flight immediate sync, the stale run is canceled/preempted and only the latest queued rule is applied.
- Dynamic channel blocks reserve guide ranges in blocks of `1000` starting at `10000`:
  - `block_start = 10000 + (order_index * 1000)`
  - generated channels are capped at `1000` entries per block
  - generated-channel reorder APIs reassign guide numbers deterministically within the block
  - successful dynamic-block materialization/reorder changes enqueue a DVR lineup reload (coalesced queue, trailing edge, `debounce=60s`, `max_wait=300s`) so provider-side lineup views converge without reload churn.
- DVR lineup reload queue behavior for reorder/materialization mutations:
  - all lineup-changing admin mutation paths enqueue through one shared queue (`/api/channels/reorder`, dynamic block materialization syncs, and `/api/dynamic-channels/{queryID}/channels/reorder`)
  - enqueue is trailing-edge debounced by `60s`
  - every new enqueue extends the due time to `now + 60s`, capped at `first_enqueue + 300s`
  - enqueue during an in-flight reload schedules exactly one follow-up debounced run
  - queue execution is detached from request cancellation and uses an internal timeout budget (`30s` default)

### Job Runs (`GET /api/admin/jobs`)

- `GET /api/admin/jobs` accepts optional `name`, `limit`, and `offset` query parameters:
  - `name` must be empty or one of `playlist_sync`, `auto_prioritize`, `dvr_lineup_sync`; other values return HTTP `400`
  - `limit` defaults to `50`, clamps to `[1, 500]`; non-integer values fall back to the default
  - `offset` defaults to `0` and negative values are clamped to `0`; non-integer values fall back to the default
  - Response payload echoes normalized `name`, `limit`, and `offset` values.

### Automation (`PUT /api/admin/automation`)

- `PUT /api/admin/automation` accepts partial JSON updates; omitted fields are left unchanged:
  - top-level: `playlist_url`, `timezone`
  - schedule objects: `playlist_sync` and `auto_prioritize` with optional `enabled` and `cron_spec`
  - analyzer object: `probe_timeout_ms`, `analyzeduration_us`, `probesize_bytes`, `bitrate_mode`, `sample_seconds`, `enabled_only`, `top_n_per_channel`
  - if a schedule resolves to `enabled=true`, `cron_spec` is required
  - analyzer validation rules:
    - `probe_timeout_ms`, `analyzeduration_us`, `probesize_bytes`, and `sample_seconds` must be greater than `0`
    - `bitrate_mode` must be `metadata`, `sample`, or `metadata_then_sample`
    - `top_n_per_channel` must be `>= 0`
- `POST /api/admin/jobs/auto-prioritize/cache/clear` returns the number of deleted cached metric rows as `{"deleted":<count>}`.
- Manual job triggers (`POST /api/admin/jobs/playlist-sync/run`, `POST /api/admin/jobs/auto-prioritize/run`) are detached from request cancellation after enqueue; once `202` is returned, client disconnects do not cancel the run.
- `PUT /api/admin/automation` and `PUT /api/admin/dvr` are serialized under a shared admin-config mutation lock to prevent concurrent rollback clobber between automation and DVR config updates.
- `PUT /api/admin/automation` validates cron only for enabled schedules, then applies persistence/runtime reload/rollback under a detached `30s` mutation context (`context.WithTimeout(context.WithoutCancel(...))`) so client disconnects do not cancel in-flight apply or rollback work. If runtime apply fails, prior automation settings are restored before returning an error.
- `PUT /api/admin/dvr` validates enabled sync cron before persisting config. If scheduler apply fails after persistence, the previous DVR config is restored and the response reports rollback outcome.

## Example API Usage

```bash
# List catalog items
curl -u admin:change-me "http://127.0.0.1:5004/api/items?limit=20"

# Create a published channel from a catalog item
curl -u admin:change-me \
  -H "Content-Type: application/json" \
  -d '{"item_key":"src:news:primary"}' \
  http://127.0.0.1:5004/api/channels

# Add a source with explicit cross-channel override
curl -u admin:change-me \
  -H "Content-Type: application/json" \
  -d '{"item_key":"src:backup:secondary","allow_cross_channel":true}' \
  http://127.0.0.1:5004/api/channels/1001/sources

# Create a channel with an enabled dynamic rule
curl -u admin:change-me \
  -H "Content-Type: application/json" \
  -d '{"item_key":"src:news:primary","guide_name":"US News","dynamic_rule":{"enabled":true,"group_name":"US News","search_query":"news"}}' \
  http://127.0.0.1:5004/api/channels

# Disable dynamic rule and cancel in-flight immediate sync
curl -u admin:change-me \
  -X PATCH \
  -H "Content-Type: application/json" \
  -d '{"dynamic_rule":{"enabled":false}}' \
  http://127.0.0.1:5004/api/channels/1001
```

### Automation

```bash
# Inspect automation state
curl -u admin:change-me http://127.0.0.1:5004/api/admin/automation

# Update schedules/timezone and analyzer probe settings
curl -u admin:change-me \
  -X PUT \
  -H "Content-Type: application/json" \
  -d '{"timezone":"America/Chicago","playlist_sync":{"enabled":true,"cron_spec":"*/30 * * * *"},"auto_prioritize":{"enabled":false},"analyzer":{"bitrate_mode":"metadata_then_sample","sample_seconds":3,"enabled_only":true,"top_n_per_channel":0}}' \
  http://127.0.0.1:5004/api/admin/automation

# Trigger playlist sync job
curl -u admin:change-me -X POST \
  http://127.0.0.1:5004/api/admin/jobs/playlist-sync/run

# Clear auto-prioritize metrics cache
curl -u admin:change-me -X POST \
  http://127.0.0.1:5004/api/admin/jobs/auto-prioritize/cache/clear
```

### Example Responses

```json
{
  "run_id": 42,
  "status": "queued"
}
```

```json
{
  "run_id": 42,
  "job_name": "playlist_sync",
  "triggered_by": "manual",
  "status": "running",
  "progress_cur": 4,
  "progress_max": 27
}
```

### DVR Sync

```bash
# Run forward DVR sync with defaults (no JSON body)
curl -u admin:change-me -X POST \
  http://127.0.0.1:5004/api/admin/dvr/sync

# Run a dry-run forward DVR sync
curl -u admin:change-me \
  -H "Content-Type: application/json" \
  -d '{"dry_run":true}' \
  http://127.0.0.1:5004/api/admin/dvr/sync

# Import a single channel mapping from DVR
curl -u admin:change-me -X POST \
  -H "Content-Type: application/json" \
  -d '{"dry_run":false,"lineup_id":"USA-MN12345-X"}' \
  http://127.0.0.1:5004/api/channels/123/dvr/reverse-sync
```

Example DVR sync response:

```json
{
  "dry_run": true,
  "sync_mode": "configured_only",
  "updated_count": 3,
  "cleared_count": 0,
  "unchanged_count": 14,
  "unresolved_count": 1,
  "warnings": [
    "lineup=USA-MN12345-X station_ref=97047 has empty lineup channel; using station_ref-only mapping"
  ]
}
```

## Auth Behavior

- `ADMIN_AUTH` empty: `/ui/*` and `/api/*` are open.
- `ADMIN_AUTH` malformed (not `user:pass`): `/ui/*` and `/api/*` return HTTP `500`.

## Common Response Codes

- `401`: missing/invalid admin credentials
- `404`: channel/source/item not found
- `429`: rate limit exceeded
- `502`: upstream or ffmpeg stream failure
- `503`: all tuners are busy, or channel tune backoff is active
