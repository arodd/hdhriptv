# hdhriptv

`hdhriptv` emulates a HDHomeRun device backed by an IPTV playlist.
It ingests M3U channels, lets you curate published channels with ordered source
failover, and publishes those channels through HDHomeRun-compatible discovery,
lineup, and stream endpoints.

## What It Does

- Fetches and parses an M3U playlist.
- Stores catalog (including inactive historical rows), published channels, and source mappings in SQLite.
- Serves an admin UI and JSON admin APIs to manage channels and sources.
- Exposes HDHomeRun-compatible endpoints:
  - `/discover.json`
  - `/lineup.json`
  - `/lineup.xml`
  - `/lineup.m3u`
  - `/lineup_status.json`
- Streams channels from `/auto/v<GuideNumber>` using:
  - direct proxy mode
  - `ffmpeg-copy` remux mode
  - `ffmpeg-transcode` compatibility mode
- Responds to HDHomeRun UDP discovery on port `65001`.
- Optionally responds to UPnP/SSDP discovery (`M-SEARCH`) on UDP `1900`, advertising `/upnp/device.xml`.
- Supports optional legacy HTTP listener (commonly `:80`) for compatibility.

## Documentation Map

- Architecture and route map: `docs/ARCHITECTURE.md`
- Systemd deployment: `deploy/systemd/README.md`
- Avahi/mDNS helper setup: `deploy/avahi/README.md`
- Manual compatibility validation: `deploy/testing/compatibility-checklist.md`

## Quick Start

### Prerequisites

- Go `1.25.4` or newer (see `go.mod`)
- Network reachability from client devices to this service
- `ffmpeg` installed if using:
  - `STREAM_MODE=ffmpeg-copy` (default)
  - `STREAM_MODE=ffmpeg-transcode`
- No `ffmpeg` required if using `STREAM_MODE=direct`

### Run Locally

1. Build:

```bash
go build -o hdhriptv ./cmd/hdhriptv
```

2. Run with minimum practical settings:

```bash
PLAYLIST_URL="https://example.com/playlist.m3u" \
ADMIN_AUTH="admin:change-me" \
./hdhriptv
```

3. Verify service health and discovery:

```bash
curl -s http://127.0.0.1:5004/healthz
curl -s http://127.0.0.1:5004/discover.json
curl -s http://127.0.0.1:5004/lineup.json
```

4. Open `http://127.0.0.1:5004/ui/catalog` and publish channels from catalog items.
   `lineup.json` is channel-only and remains empty until at least one channel is published.

### Run With Docker

Build image:

```bash
docker build -t hdhriptv:local .
```

Run container:

```bash
docker run --rm -it \
  -p 5004:5004/tcp \
  -p 65001:65001/udp \
  -p 1900:1900/udp \
  -v hdhriptv-data:/data \
  -e PLAYLIST_URL="https://example.com/playlist.m3u" \
  -e ADMIN_AUTH="admin:change-me" \
  -e UPNP_ENABLED=true \
  hdhriptv:local
```

Notes:

- The image includes `ffmpeg`.
- The runtime image uses `alpine:3.21` and installs `ffmpeg` from Alpine's `community` repository.
- SQLite state persists in `/data` when a volume is mounted.
- Add `-p 80:80/tcp -e HTTP_ADDR_LEGACY=:80` if you need legacy client compatibility.

Build and publish the same multi-arch image flow used in GitLab CI from your local machine (run `docker login` first):

```bash
make release-local REGISTRY_IMAGE=registry.example.com/org/hdhriptv IMAGE_TAG=latest
```

## Configuration Reference

Flags and environment variables are equivalent except where explicitly labeled
as env-only or internal-only. Flag values override environment variables.

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--playlist-url` | `PLAYLIST_URL` | empty | No | If empty, no automatic refresh runs. Existing DB data may still be served. |
| `--db-path` | `DB_PATH` | `./hdhr-iptv.db` | No | SQLite file location. |
| `--log-dir` | `LOG_DIR` | current working directory | No | Directory for startup timestamped log files (`hdhriptv-YYYYMMDD-HHMMSS.log`). |
| `--http-addr` | `HTTP_ADDR` | `:5004` | No | Primary HTTP listener. |
| `--http-addr-legacy` | `HTTP_ADDR_LEGACY` | empty | No | Optional second HTTP listener, often `:80`. |
| `--upnp-enabled` | `UPNP_ENABLED` | `true` | No | Enable UPnP/SSDP responder on UDP `1900` (`M-SEARCH` + optional `NOTIFY`). |
| `--upnp-addr` | `UPNP_ADDR` | `:1900` | No | UPnP/SSDP UDP listen address. |
| `--upnp-notify-interval` | `UPNP_NOTIFY_INTERVAL` | `5m` | No | Periodic `NOTIFY ssdp:alive` interval. Set `0` to disable periodic announcements; shutdown still emits `ssdp:byebye` when notifications are enabled. |
| `--upnp-max-age` | `UPNP_MAX_AGE` | `30m` | No | `CACHE-CONTROL: max-age` value used in SSDP responses and `NOTIFY` announcements. |
| `--upnp-content-directory-update-id-cache-ttl` | `UPNP_CONTENT_DIRECTORY_UPDATE_ID_CACHE_TTL` | `1s` | No | TTL for cached UPnP `ContentDirectory` `GetSystemUpdateID` values. Increase to reduce repeated lineup reads under frequent SOAP polling. |
| `--tuner-count` | `TUNER_COUNT` | `2` | No | Max concurrent stream sessions. Must be `>= 1`. |
| `--friendly-name` | `FRIENDLY_NAME` | `HDHR IPTV` | No | Displayed in discover payloads and DVR UIs. Persisted in DB and reused across restarts unless explicitly set at startup. |
| `--device-id` | `DEVICE_ID` | random 8 hex chars | No | Auto-generated once and persisted in DB when unset; reused across restarts while DB persists. |
| `--device-auth` | `DEVICE_AUTH` | random token | No | Auto-generated once and persisted in DB when unset; reused across restarts while DB persists. |
| `--refresh-schedule` | `REFRESH_SCHEDULE` | empty | No | Cron expression for playlist sync (`5-field` or optional-seconds `6-field`). When set, it updates persisted `jobs.playlist_sync.cron` and enables playlist sync scheduling. |
| `--refresh-interval` | `REFRESH_INTERVAL` | empty | No | Deprecated compatibility flag/env. Duration is converted to `--refresh-schedule` when representable (for example `30m` -> `*/30 * * * *`). |
| `--ffmpeg-path` | `FFMPEG_PATH` | `ffmpeg` | No | Executable used for ffmpeg stream modes. |
| `--stream-mode` | `STREAM_MODE` | `ffmpeg-copy` | No | `direct`, `ffmpeg-copy`, `ffmpeg-transcode`. |
| `--startup-timeout` | `STARTUP_TIMEOUT` | `6s` | No | Per-source startup timeout during tune failover. |
| `--startup-random-access-recovery-only` | `STARTUP_RANDOM_ACCESS_RECOVERY_ONLY` | `true` | No | When `true`, startup random-access enforcement is applied only during recovery cycles; initial startup no longer waits for a random-access cutover marker under `slate_av` + ffmpeg modes. |
| `--min-probe-bytes` | `MIN_PROBE_BYTES` | `940` | No | Startup bytes required before stream commit (`>= 1`). |
| `--max-failovers` | `MAX_FAILOVERS` | `3` | No | Number of fallback attempts after primary source (`0` means try all enabled sources). |
| `--failover-total-timeout` | `FAILOVER_TOTAL_TIMEOUT` | `32s` | No | Total tune-time budget across startup failover attempts. |
| `--upstream-overlimit-cooldown` | `UPSTREAM_OVERLIMIT_COOLDOWN` | `3s` | No | Per-provider-scope cooldown applied after upstream startup `429 Too Many Requests` responses before retrying that same provider scope. |
| `--ffmpeg-reconnect-enabled` | `FFMPEG_RECONNECT_ENABLED` | `false` | No | Enables ffmpeg input reconnect flags (`-reconnect*`) for ffmpeg stream modes. |
| `--ffmpeg-reconnect-delay-max` | `FFMPEG_RECONNECT_DELAY_MAX` | `3s` | No | Max ffmpeg reconnect delay (`-reconnect_delay_max`, rounded up to seconds). |
| `--ffmpeg-reconnect-max-retries` | `FFMPEG_RECONNECT_MAX_RETRIES` | `1` | No | Maximum ffmpeg reconnect attempts (`-reconnect_max_retries`). |
| `--ffmpeg-reconnect-http-errors` | `FFMPEG_RECONNECT_HTTP_ERRORS` | empty | No | Value used for `-reconnect_on_http_error` in ffmpeg stream modes. Empty disables this specific ffmpeg input option. |
| `--ffmpeg-startup-probesize-bytes` | `FFMPEG_STARTUP_PROBESIZE_BYTES` | `1000000` | No | FFmpeg input `-probesize` used during startup stream detection in ffmpeg modes. Values below `128000` are normalized up to this floor to avoid startup stream-detection regressions. |
| `--ffmpeg-startup-analyzeduration` | `FFMPEG_STARTUP_ANALYZEDURATION` | `1.5s` | No | FFmpeg input `-analyzeduration` used during startup stream detection in ffmpeg modes. Values below `1s` are normalized up to this floor to avoid startup stream-detection regressions. |
| `--ffmpeg-copy-regenerate-timestamps` | `FFMPEG_COPY_REGENERATE_TIMESTAMPS` | `true` | No | Enables ffmpeg-copy timestamp regeneration (`-fflags +genpts`) to smooth sources with missing/non-monotonic timestamps. |
| `--producer-readrate` | `PRODUCER_READRATE` | `1` | No | FFmpeg producer pacing (`-readrate`). Shared sessions use this before buffering. |
| `--producer-initial-burst` | `PRODUCER_INITIAL_BURST` | `1` | No | FFmpeg producer initial burst seconds (`-readrate_initial_burst`). |
| `--buffer-chunk-bytes` | `BUFFER_CHUNK_BYTES` | `65536` | No | Shared-session chunk publish threshold in bytes (`>= 1`). |
| `--buffer-publish-flush-interval` | `BUFFER_PUBLISH_FLUSH_INTERVAL` | `100ms` | No | Shared-session timer flush interval for partial chunks. |
| `--buffer-ts-align-188` | `BUFFER_TS_ALIGN_188` | `false` | No | Align non-final chunk publishes to MPEG-TS packet boundaries (188-byte multiples). |
| `--stall-detect` | `STALL_DETECT` | `4s` | No | Shared-session no-publish threshold before recovery starts. |
| `--stall-hard-deadline` | `STALL_HARD_DEADLINE` | `32s` | No | Recovery deadline after stall detection before session closes. |
| `--stall-policy` | `STALL_POLICY` | `failover_source` | No | `failover_source`, `restart_same`, `close_session`. In `failover_source`, recovery automatically falls back to restart-like same-source retries when no startup-eligible alternates remain (alternate source ID with non-empty URL and no active cooldown). |
| `--stall-max-failovers-per-stall` | `STALL_MAX_FAILOVERS_PER_STALL` | `3` | No | Maximum startup failovers attempted per detected stall. |
| `--cycle-failure-min-health` | `CYCLE_FAILURE_MIN_HEALTH` | `20s` | No | Minimum healthy uptime for recovery-selected sources before cycle-close failures are persisted to source health (`0` disables this guard). |
| `--recovery-filler-enabled` | `RECOVERY_FILLER_ENABLED` | `true` | No | Enable recovery keepalive during shared-session recovery windows. |
| `--recovery-filler-mode` | `RECOVERY_FILLER_MODE` | `slate_av` | No | Recovery keepalive mode: `null` (PID `0x1FFF` null packets), `psi` (PAT/PMT packets), or `slate_av` (decodable A/V filler). In `slate_av`, odd/missing profile dimensions are normalized to codec-safe even values before filler startup. |
| `--recovery-filler-interval` | `RECOVERY_FILLER_INTERVAL` | `200ms` | No | Keepalive interval for packet modes (`null`/`psi`) while recovery is active (`> 0`). |
| `--recovery-filler-text` | `RECOVERY_FILLER_TEXT` | `Channel recovering...` | No | Overlay text rendered by `slate_av` recovery filler mode. |
| `--recovery-filler-enable-audio` | `RECOVERY_FILLER_ENABLE_AUDIO` | `true` | No | Include silent AAC audio track in `slate_av` filler to improve client decoder continuity. |
| `--subscriber-join-lag-bytes` | `SUBSCRIBER_JOIN_LAG_BYTES` | `2097152` | No | New subscriber join lag window in bytes (ring tail cushion). |
| `--subscriber-slow-client-policy` | `SUBSCRIBER_SLOW_CLIENT_POLICY` | `disconnect` | No | `disconnect` or `skip` when client falls behind ring tail. |
| `--subscriber-max-blocked-write` | `SUBSCRIBER_MAX_BLOCKED_WRITE` | `6s` | No | Per-chunk write deadline for shared subscribers (`0` disables deadline). |
| `--session-idle-timeout` | `SESSION_IDLE_TIMEOUT` | `5s` | No | Shared channel session teardown delay after last subscriber leaves. |
| `--session-drain-timeout` | `SESSION_DRAIN_TIMEOUT` | `2s` | No | Timeout budget used by shared-session bounded reader closes during startup aborts/recovery teardown. |
| `--session-max-subscribers` | `SESSION_MAX_SUBSCRIBERS` | `0` | No | Max subscribers per shared channel session (`0` for unlimited). |
| `--session-history-limit` | `SESSION_HISTORY_LIMIT` | `0` | No | Shared-session lifecycle history retention cap (`0` uses runtime default). |
| `--session-source-history-limit` | `SESSION_SOURCE_HISTORY_LIMIT` | `0` | No | Per-session source-history retention cap (`0` falls back to `session-history-limit` and runtime guardrails). |
| `--session-subscriber-history-limit` | `SESSION_SUBSCRIBER_HISTORY_LIMIT` | `0` | No | Per-session subscriber-history retention cap (`0` falls back to `session-history-limit` and runtime guardrails). |
| `--source-health-drain-timeout` | `SOURCE_HEALTH_DRAIN_TIMEOUT` | `0s` | No | Source-health persistence drain budget during session teardown (`0` uses runtime default `250ms`). |
| `n/a (env-only)` | `CLOSE_WITH_TIMEOUT_WORKER_BUDGET` | `16` | No | Global `closeWithTimeout` worker budget (`1..256`). Raise only when sustained close suppression indicates shutdown-path saturation. |
| `--preempt-settle-delay` | `PREEMPT_SETTLE_DELAY` | `500ms` | No | Delay before refilling from full tuner usage back to full usage (for example `2->1->2`), and before reusing a preempted slot. Helps upstream providers with lagging session teardown accounting. |
| `--auto-prioritize-probe-tune-delay` | `AUTO_PRIORITIZE_PROBE_TUNE_DELAY` | `1s` | No | Delay between automated probe tunes (auto-prioritize and background source probing) to reduce upstream session overlap. |
| `--auto-prioritize-workers` | `AUTO_PRIORITIZE_WORKERS` | `2` | No | Auto-prioritize worker policy: fixed positive worker count, or `auto` to use current available tuner slots. |
| `--probe-interval` | `PROBE_INTERVAL` | `0` | No | Background source probe interval (`0` disables probing). |
| `--probe-timeout` | `PROBE_TIMEOUT` | `3s` | No | Per-source timeout for background probes (`<= probe-interval` when enabled). |
| `--admin-auth` | `ADMIN_AUTH` | empty | No | Format must be `user:pass`. If empty, no auth on `/ui/*` and `/api/*`. |
| `--admin-json-body-limit-bytes` | `ADMIN_JSON_BODY_LIMIT_BYTES` | `1048576` | No | Maximum JSON body size accepted by admin mutation endpoints (`> 0`). Oversized bodies are rejected with HTTP `413`. |
| `--request-timeout` | `REQUEST_TIMEOUT` | `15s` | No | Timeout for non-stream routes. `0` disables. |
| `--rate-limit-rps` | `RATE_LIMIT_RPS` | `8` | No | Per-client IP token bucket. `0` disables rate limiting. |
| `--rate-limit-burst` | `RATE_LIMIT_BURST` | `32` | No | Must be `>= 1` when rate limiting is enabled. |
| `--rate-limit-max-clients` | `RATE_LIMIT_MAX_CLIENTS` | `4096` | No | Max distinct client-IP limiter entries kept in memory (`0` disables cap). Oldest entries are evicted first when cap is reached. |
| `--rate-limit-trusted-proxies` | `RATE_LIMIT_TRUSTED_PROXIES` | empty | No | Comma-separated trusted proxy CIDR/IP entries. Forwarded client headers are honored only when the immediate peer IP is trusted. Header precedence: `Forwarded`, then `X-Forwarded-For`, then `X-Real-IP`. Chain headers are resolved right-to-left by peeling trusted hops from the right and selecting the first non-trusted IP; malformed chains (or all-trusted chains) fall back to the peer IP. |
| `--tune-backoff-max-tunes` | `TUNE_BACKOFF_MAX_TUNES` | `8` | No | Per-channel startup-failure threshold across `/auto/...` before channel tune cooldown activates. `0` disables tune backoff protection. |
| `--tune-backoff-interval` | `TUNE_BACKOFF_INTERVAL` | `1m` | No | Rolling window used with `tune-backoff-max-tunes` to count per-channel startup failures. |
| `--tune-backoff-cooldown` | `TUNE_BACKOFF_COOLDOWN` | `20s` | No | Per-channel cooldown window applied after tune backoff threshold is exceeded. |
| `--catalog-search-max-terms` | `CATALOG_SEARCH_MAX_TERMS` | `12` | No | Maximum token-mode include/exclude terms retained per query (`1..256`). Additional terms are truncated and reported through `search_warning`. |
| `--catalog-search-max-disjuncts` | `CATALOG_SEARCH_MAX_DISJUNCTS` | `6` | No | Maximum token-mode OR disjunct clauses retained per query (`1..128`). Additional disjuncts are truncated and reported through `search_warning`. |
| `--catalog-search-max-term-runes` | `CATALOG_SEARCH_MAX_TERM_RUNES` | `64` | No | Maximum runes retained per token in token mode (`1..256`). Per-token rune clipping is reported through `search_warning.term_rune_truncations`. |
| `--enable-metrics` | `ENABLE_METRICS` | `false` | No | Enables `/metrics` Prometheus endpoint. |
| `--log-level` | `LOG_LEVEL` | `info` | No | `debug`, `info`, `warn`, `error`. |

### Internal Stream Tuning (Programmatic Only)

These options are available to embedders that construct stream internals
directly. They are not currently exposed as CLI flags or environment variables.

| Internal Option | Default | Notes |
| --- | --- | --- |
| `ProberConfig.ProbeCloseQueueDepth` | `8` | Capacity of the background-prober probe-session close queue. `<=0` normalizes to the default. Queue-full and inline fallback behavior is observable via `/api/admin/tuners` `probe_close.inline_count` and `probe_close.queue_full_count`. |

### SQLite IOERR Diagnostic Toggles (Environment Only)

These advanced toggles are environment-only (no CLI flag) and are intended for
incident windows where deeper SQLite write-path diagnostics are needed.

| Environment Variable | Default | Notes |
| --- | --- | --- |
| `HDHRIPTV_SQLITE_IOERR_CHECKPOINT_PROBE` | `false` | Enables one-shot `PRAGMA wal_checkpoint(PASSIVE)` capture inside `sqlite_ioerr_diag_bundle` events. Keep disabled unless actively diagnosing WAL state. |
| `HDHRIPTV_SQLITE_IOERR_TRACE_ENABLED` | `false` | Enables in-memory DB operation trace ring used for IOERR dump context (`sqlite_ioerr_trace_dump`). |
| `HDHRIPTV_SQLITE_IOERR_TRACE_RING_SIZE` | `256` | Fixed trace-ring entry count (`16`..`16384`). Larger values retain more pre-failure history but use more memory. |
| `HDHRIPTV_SQLITE_IOERR_TRACE_DUMP_LIMIT` | `64` | Maximum trace entries emitted per IOERR dump (`>=1`, capped to ring size). |
| `HDHRIPTV_SQLITE_IOERR_TRACE_DUMP_INTERVAL` | `30s` | Minimum interval between IOERR trace dumps. Set `0` to disable dump rate limiting during short debugging windows. |

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

## Endpoint Reference

### Public Discovery and Streaming Endpoints

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
- Unsupported or invalid UPnP SOAP actions return protocol-valid SOAP faults (instead of HTTP redirects).

### Admin UI and Admin API

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
| `PATCH` | `/api/channels/reorder` | Reorder complete channel list (`204 No Content` on success) |
| `DELETE` | `/api/channels/{channelID}` | Delete channel |
| `GET` | `/api/dynamic-channels` | List dynamic channel blocks (paged metadata response) |
| `POST` | `/api/dynamic-channels` | Create dynamic channel block |
| `GET` | `/api/dynamic-channels/{queryID}` | Read one dynamic channel block |
| `PATCH` | `/api/dynamic-channels/{queryID}` | Update dynamic channel block config |
| `DELETE` | `/api/dynamic-channels/{queryID}` | Delete dynamic channel block |
| `GET` | `/api/dynamic-channels/{queryID}/channels` | List generated channels for one dynamic block |
| `PATCH` | `/api/dynamic-channels/{queryID}/channels/reorder` | Reorder generated channels within one dynamic block (`204 No Content` on success) |
| `GET` | `/api/channels/{channelID}/sources` | List channel sources |
| `POST` | `/api/channels/{channelID}/sources` | Add source by `item_key` |
| `POST` | `/api/channels/{channelID}/sources/health/clear` | Clear health/cooldown state for one channel's sources |
| `PATCH` | `/api/channels/{channelID}/sources/{sourceID}` | Update source (`enabled`) |
| `PATCH` | `/api/channels/{channelID}/sources/reorder` | Reorder channel sources (`204 No Content` on success) |
| `DELETE` | `/api/channels/{channelID}/sources/{sourceID}` | Delete source |
| `POST` | `/api/channels/sources/health/clear` | Clear health/cooldown state for all channel sources |
| `GET` | `/api/suggestions/duplicates?min=2&q=cnn.us&limit=100&offset=0` | Paged duplicate catalog suggestions grouped by `channel_key` (optional case-insensitive search across `channel_key` and `tvg_id`) |
| `GET` | `/api/admin/tuners` | Runtime tuner/session snapshot including shared-session subscriber mappings, bounded `session_history` timelines, and process-lifetime `drain_wait` / `probe_close` telemetry counters |
| `POST` | `/api/admin/tuners/recovery` | Trigger manual shared-session recovery for an active channel (`channel_id`, optional `reason`) |
| `GET` | `/api/admin/automation` | Current automation state (`playlist_url`, schedule config, analyzer settings, next/last run) |
| `PUT` | `/api/admin/automation` | Update automation schedule/timezone/analyzer and playlist URL settings |
| `POST` | `/api/admin/jobs/playlist-sync/run` | Trigger async playlist sync run |
| `POST` | `/api/admin/jobs/auto-prioritize/run` | Trigger async auto-prioritize run |
| `POST` | `/api/admin/jobs/auto-prioritize/cache/clear` | Clear cached stream metrics used by auto-prioritize |
| `GET` | `/api/admin/jobs/{runID}` | Fetch one job run (`running`, `success`, `error`, `canceled`) |
| `GET` | `/api/admin/jobs?name=dvr_lineup_sync&limit=20&offset=0` | List recent job runs by optional name filter (`playlist_sync`, `auto_prioritize`, `dvr_lineup_sync`) |
| `GET` | `/api/admin/dvr` | Current DVR integration config, cached lineups, and last sync summary |
| `PUT` | `/api/admin/dvr` | Update DVR primary provider, `active_providers`, per-provider base URLs (`channels_base_url`, `jellyfin_base_url`), default lineup, and sync schedule/mode. Jellyfin supports optional `jellyfin_tuner_host_id` and write-only `jellyfin_api_token` inputs. Legacy `base_url` remains supported as a primary-provider alias. |
| `POST` | `/api/admin/dvr/test` | Verify DVR provider connectivity and device-channel visibility |
| `GET` | `/api/admin/dvr/lineups?refresh=1` | List DVR lineups (optional provider refresh) |
| `POST` | `/api/admin/dvr/sync` | Run forward sync (hdhriptv mapping to DVR custom lineup patch). Optional JSON body (`dry_run`, optional `include_dynamic`); empty body is accepted and defaults to `dry_run=false`, `include_dynamic=false`. Execution is detached from initiating request cancellation and bounded by an internal timeout budget (`2m` default). |
| `POST` | `/api/admin/dvr/reverse-sync` | Run reverse sync (provider custom mapping into hdhriptv channel mappings). Optional JSON body (`dry_run`, optional `lineup_id`, optional `include_dynamic`); empty body is accepted. Execution is detached from initiating request cancellation and bounded by an internal timeout budget (`2m` default). |
| `GET` | `/api/channels/dvr` | Paged read of per-channel DVR mappings (used by `/ui/channels`; supports optional `enabled_only=1`, optional `include_dynamic=1`, plus `limit`/`offset`; dynamic generated channels are excluded by default) |
| `GET` | `/api/channels/{channelID}/dvr` | Read one channel's DVR mapping |
| `PUT` | `/api/channels/{channelID}/dvr` | Update one channel's DVR lineup/channel/station-ref mapping |
| `POST` | `/api/channels/{channelID}/dvr/reverse-sync` | Reverse-sync one channel from DVR mapping state. Optional JSON body (`dry_run`, optional `lineup_id`); empty body is accepted. Execution is detached from initiating request cancellation and bounded by an internal timeout budget (`2m` default). |

Notes:

- Admin mutation routes that decode JSON bodies use strict single-object
  parsing:
  - unknown JSON fields are rejected with HTTP `400`
  - trailing JSON content after the first object is rejected with HTTP `400`
  - oversized bodies are rejected with HTTP `413` using
    `ADMIN_JSON_BODY_LIMIT_BYTES`
  - optional-body DVR sync routes (`/api/admin/dvr/sync`,
    `/api/admin/dvr/reverse-sync`, `/api/channels/{channelID}/dvr/reverse-sync`)
    still enforce these rules when a non-empty body is provided
- `GET /api/items` supports `group`, `group_names`, `q`, optional `q_regex`, `limit`, and `offset` query parameters:
  - `group`/`group_names` and `q` are optional filters.
  - group filter semantics:
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
    - exclusion-only queries are allowed.
    - queries without OR separators keep legacy include/exclude AND behavior unchanged.
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
- `/api/admin/tuners` and `/ui/tuners` intentionally redact `source_stream_url` values. The status payload preserves scheme/host/path but strips URL userinfo, query, and fragment fields.
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
- `/api/groups`, `/api/channels`, `/api/dynamic-channels`, `/api/channels/{channelID}/sources`, and `/api/dynamic-channels/{queryID}/channels` accept `limit` + `offset` query params and return paging metadata (`total`, `limit`, `offset`) in responses.
- Group/channel/dynamic-query/source/generated-channel list endpoints default to `limit=200` when omitted. `limit=0` is normalized to the same bounded default (`200`) instead of triggering unbounded/all-results reads.
- Hard caps are applied when `limit` is too large (`/api/groups`, `/api/channels`, `/api/dynamic-channels`, and `/api/dynamic-channels/{queryID}/channels` max `1000`, `/api/channels/{channelID}/sources` max `2000`).
- For those paged endpoints, `limit` and `offset` must be integers `>= 0`; negative or non-integer values return HTTP `400` (unlike `/api/items`, which normalizes non-integer values to defaults).
- `GET /api/groups` supports optional `include_counts` boolean query semantics:
  - accepted true values: `1`, `true`, `yes`, `on`
  - accepted false values: `0`, `false`, `no`, `off`
  - default is `true`
  - when `include_counts=false`, group entries only include `name` (count metadata is omitted), which is useful for autocomplete consumers
  - malformed `include_counts` values return HTTP `400`
- `POST /api/channels/{channelID}/sources/health/clear` and `POST /api/channels/sources/health/clear` reset persisted source health/cooldown fields (`success_count`, `fail_count`, `last_ok_at`, `last_fail_at`, `last_fail_reason`, `cooldown_until`) and return `{"cleared":<count>}`.
- `GET /api/suggestions/duplicates` normalizes query behavior:
  - `min` defaults to `2`, clamps to `[2, 100]`.
  - `q` performs case-insensitive matching across `channel_key` and `tvg_id`.
  - `limit` defaults to `100`; `limit=0` also normalizes to `100`; values above `500` are clamped.
  - `offset` defaults to `0`.
  - `limit` and `offset` must be integers `>= 0`; negative or non-integer values return HTTP `400`.
  - Legacy `tvg_id` query fallback is accepted when `q` is omitted.
  - Response payload includes `total`, `limit`, and `offset`, and echoes normalized `min` and `q` values.
- `GET /api/channels/dvr` accepts optional:
  - `enabled_only` (`1`, `true`, `yes`, `on`) to return only enabled-channel mappings.
  - `include_dynamic` (`1`, `true`, `yes`, `on`) to include dynamic generated channels.
  - `limit`/`offset` paging controls with strict integer parsing.
    - `limit` defaults to `200`; explicit `limit=0` normalizes to the same bounded default.
    - `limit` clamps at `1000`.
    - `offset` defaults to `0`.
    - negative or non-integer `limit`/`offset` values return HTTP `400`.
  - default behavior excludes dynamic generated channels.
  Response payload includes `mappings`, `total`, `limit`, `offset`, and echoes
  applied filters as `enabled_only` and `include_dynamic`.
- `GET /api/admin/dvr/lineups` accepts optional `refresh` (`1`, `true`, `yes`, `on`) to force a provider lineup refresh; response payload echoes the applied boolean as `refresh`.
- DVR sync routes parse optional JSON payloads using empty-body-tolerant decoding:
  - `POST /api/admin/dvr/sync`
  - `POST /api/admin/dvr/reverse-sync`
  - `POST /api/channels/{channelID}/dvr/reverse-sync`
  - Empty body requests (including `Transfer-Encoding: chunked` with no payload bytes) are accepted and use default request values.
  - Malformed JSON payloads return HTTP `400`.
  - `include_dynamic` defaults to `false` on sync and reverse-sync APIs.
  - execution is detached from request cancellation after handler admission, and each run is bounded by an internal timeout budget (`2m` default).
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
  - successful dynamic-block materialization/reorder changes trigger a DVR lineup reload so provider-side lineup views can pick up guide changes.
- `GET /api/admin/jobs` accepts optional `name`, `limit`, and `offset` query parameters:
  - `name` must be empty or one of `playlist_sync`, `auto_prioritize`, `dvr_lineup_sync`; other values return HTTP `400`
  - `limit` defaults to `50`, clamps to `[1, 500]`; non-integer values fall back to the default
  - `offset` defaults to `0` and negative values are clamped to `0`; non-integer values fall back to the default
  - Response payload echoes normalized `name`, `limit`, and `offset` values.
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

Example API usage:

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

Automation example:

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

Example responses:

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

DVR example:

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

Auth behavior:

- `ADMIN_AUTH` empty: `/ui/*` and `/api/*` are open.
- `ADMIN_AUTH` malformed (not `user:pass`): `/ui/*` and `/api/*` return HTTP `500`.

## Playlist and Channel Lifecycle

- On startup, if `playlist.url` is configured (via persisted settings or `PLAYLIST_URL`), the service performs an initial playlist sync job.
- Startup initial playlist sync is scheduled only after the primary HTTP listener answers `/healthz` readiness probes to avoid startup-window DVR reload races against an unopened listener.
- Startup sync uses bounded retry/backoff only for transient Jellyfin lineup reload failures (`reload dvr lineup after playlist sync` + Jellyfin provider + transient network/5xx signatures): up to `4` attempts, exponential backoff from `1s` capped at `8s`, within a `3m` overall retry budget.
- Non-transient startup sync failures abort immediately without retries.
- Startup sync emits structured log records (`msg="initial playlist sync phase"`) with `initial_sync_phase=scheduled_after_listener_start|completed|failed` and includes attempt/duration metadata on completion.
- Periodic refresh uses the automation schedule in `jobs.playlist_sync.cron`.
- `--refresh-schedule` / `REFRESH_SCHEDULE` and the UI/API automation settings update the same persisted schedule keys.
- Refresh is serialized by an internal lock (one refresh at a time).
- Playlist upsert behavior:
  - upserts existing entries
  - inserts new entries
  - marks stale entries inactive instead of deleting them
- Published channel behavior:
  - channels are ordered explicitly
  - guide numbers are contiguous and start at `100`
  - add/remove/reorder operations renumber guide numbers to keep them dense
  - `/ui/catalog` supports a rapid source-add workflow:
    - choose one target channel once in the toolbar
    - row actions switch to `Add Source` for rapid attachment
    - clear target selection to return to row-level `Create Channel` actions
    - dynamic channel creation is toolbar-driven from current filter context
  - dynamic channel blocks materialize generated channels into reserved guide ranges (`10000+`) and are managed separately from traditional channel ordering
  - each channel may have multiple ordered sources (`priority_index`)
  - each channel also has a `dynamic_rule`:
    - if enabled, matching catalog items are synchronized into channel sources asynchronously
    - when disabled, pending/in-flight dynamic sync is canceled
    - newer enabled updates also preempt/cancel stale in-flight sync runs so broad scans converge to the latest rule quickly
    - `dynamic_rule.search_query` is required when enabled
    - `dynamic_rule.search_query` uses the same OR-capable include/exclude semantics as `/api/items` (`|`/`OR` plus `-term`/`!term`; no OR separator preserves legacy AND behavior)
    - only `dynamic_query`-managed associations are automatically removed when no longer matched; manual associations are preserved
  - non-matching source attachments require explicit override (`allow_cross_channel`)
  - lineup responses include only enabled channels
  - source health cooldown uses a bounded fail ladder (`10s`, `30s`, `2m`, `10m`, `1h` cap)
  - successful source startup clears cooldown and resets failure state (`fail_count`, `last_fail_at`, `last_fail_reason`)

## Operations Workflow

### Playlist Sync vs Auto-prioritize

- Run playlist sync when catalog content changed, channel sources need to be reconciled, or lineup entries seem stale.
- Run auto-prioritize when channel source order quality needs to be recalculated from fresh probes.
- Use playlist sync first, then auto-prioritize, after major playlist/provider updates.
- Schedule playlist sync more frequently than auto-prioritize in most deployments because sync is correctness-focused and analyze/reorder is probe-heavy.
- If providers enforce strict concurrent session caps, reduce auto-prioritize frequency and verify `AUTO_PRIORITIZE_PROBE_TUNE_DELAY`.

### DVR Forward Sync vs Reverse Sync

- Use forward sync (`POST /api/admin/dvr/sync`) to push hdhriptv channel mapping intent into DVR custom lineup mapping.
- Use reverse sync (`POST /api/admin/dvr/reverse-sync`) to pull provider-side custom mapping back into hdhriptv.
- Use per-channel reverse sync (`POST /api/channels/{channelID}/dvr/reverse-sync`) to import a single channel without touching the rest.
- By default, DVR mapping/sync workflows target traditional channels only; set `include_dynamic=true` on sync APIs only when you intentionally want dynamic generated channels included.
- In `configured_only` mode, forward sync updates only configured channels.
- In `mirror_device` mode, forward sync also clears unmatched provider mappings to mirror current hdhriptv state.
- Forward/reverse sync endpoints are intended for the `channels` DVR provider.
  Jellyfin provider mode is for lineup refresh orchestration (`ReloadLineup`)
  and does not implement custom mapping APIs.

### Jellyfin DVR Provider Notes

- Configure primary provider workflows with `provider`, and configure
  post-playlist-sync reload fan-out with `active_providers`.
- Use per-provider base URLs:
  - `channels_base_url` for Channels DVR.
  - `jellyfin_base_url` for Jellyfin API root.
  - legacy `base_url` still maps to the selected primary provider.
- Jellyfin post-sync reload requires both `jellyfin_base_url` and
  `jellyfin_api_token`.
- hdhriptv Jellyfin requests use header auth only:
  `X-Emby-Token: <token>`.
  `api_key` query-token auth is not used by the provider implementation.
- `GET /api/admin/dvr` and `PUT /api/admin/dvr` responses redact
  `jellyfin_api_token` (write-only semantics) and expose
  `jellyfin_api_token_configured=true|false`.
- Optional `jellyfin_tuner_host_id` can pin refresh targeting when multiple
  Jellyfin HDHomeRun tuner hosts exist.
- Jellyfin lineup refresh flow after HDHR changes:
  `GET /System/Configuration/livetv` -> `POST /LiveTv/TunerHosts` ->
  best-effort `GET /ScheduledTasks?isHidden=false` observability probe.
- Playlist-sync-triggered DVR reload uses provider-aware gating for each active
  provider (`active_providers`):
  - Channels reload runs for active `channels`.
  - Jellyfin reload runs for active `jellyfin` only when
    `jellyfin_base_url` and `jellyfin_api_token` are configured.
  - Mixed outcomes are reported as `dvr_lineup_reload_status=partial`.
  - Skipped provider reasons are encoded in
    `dvr_lineup_reload_skip_reason` (for example
    `jellyfin:missing_jellyfin_api_token`).
  - Job summaries include
    `dvr_lineup_reload_status=<disabled|reloaded|partial|skipped>`
    and `dvr_lineup_reload_skip_reason=<reason>`.

### Job Status Interpretation

- Query `GET /api/admin/jobs/{runID}` or `GET /api/admin/jobs?name=...`.
- Supported `name` filters are `playlist_sync`, `auto_prioritize`, and `dvr_lineup_sync`.
- `running` means job execution has started and may update `progress_cur` and `progress_max`.
- `success` means terminal completion without errors.
- `error` means terminal completion with failure details in `error`.
- `canceled` means terminal cancellation, usually due to shutdown or context cancellation.
- A second trigger while the same job is active returns HTTP `409`.
- If a run remains `running` without progress changes for an extended period, correlate with logs (`job started`, `job finished`, and subsystem-specific warnings/errors).

## Stream Modes and Tradeoffs

| Mode | `ffmpeg` Required | CPU Use | Compatibility | Notes |
| --- | --- | --- | --- | --- |
| `direct` | No | Lowest | Lowest/variable | Byte-for-byte upstream proxy; codec/container unchanged. |
| `ffmpeg-copy` | Yes | Low | High | Remux to MPEG-TS without transcoding. Good default. |
| `ffmpeg-transcode` | Yes | Highest | Highest | Transcodes to H.264/AAC MPEG-TS for difficult clients. |

Shared-session behavior:

- Streaming is shared per published channel: multiple viewers of the same guide number reuse one upstream producer session.
- Tuner usage is per active channel session, not per viewer.
- Shared sessions use a size-or-time chunk pump and flush each chunk to subscribers to reduce no-data gaps.
- Stall recovery is policy-driven (`STALL_POLICY`); default behavior fails over to alternate sources (`failover_source`), with optional same-source retry (`restart_same`) or immediate session close (`close_session`) when configured.
- In `failover_source`, when no startup-eligible alternates are available in the current recovery pass, recovery automatically downgrades to restart-like same-source retry behavior (`restart_same` parity) while recovery filler keepalive remains active until startup succeeds or recovery deadline is reached.
- Repeated same-source `source_eof` recoveries are paced with bounded inter-cycle backoff (`250ms` -> `2s`) so retry loops do not burn recovery-cycle budget in millisecond bursts. Recovery burst accounting is time-aware (`recovery_burst_budget_count` / `recovery_burst_pace_window`), and repetitive `shared session recovery triggered` warnings are coalesced (`recovery_trigger_logs_coalesced`).

## Observability and Hardening

- `GET /healthz` returns `{"status":"ok"}` with HTTP `200`.
- `GET /metrics` available when `ENABLE_METRICS=true`.
- Per-client IP rate limiting via token bucket:
  - configured by `RATE_LIMIT_RPS`, `RATE_LIMIT_BURST`, and `RATE_LIMIT_MAX_CLIENTS`
  - for reverse-proxy deployments, set `RATE_LIMIT_TRUSTED_PROXIES` so client identity is derived from trusted forwarded headers instead of collapsing all traffic to the proxy IP
  - `Forwarded`/`X-Forwarded-For` chains are interpreted from right to left; trusted proxy hops are stripped from the right and the first non-trusted hop is used as the limiter key
  - returns HTTP `429` when exceeded
  - stale client entries are incrementally evicted and limiter-map cardinality can be capped to avoid unbounded growth
  - `/healthz` is exempt
- Request timeout middleware:
  - controlled by `REQUEST_TIMEOUT`
  - applies to non-stream endpoints
  - `/auto/...` streaming is exempt
- Admin mutation JSON size hardening:
  - controlled by `ADMIN_JSON_BODY_LIMIT_BYTES`
  - applies to admin mutation endpoints that decode JSON bodies
  - oversized bodies are rejected with HTTP `413 Request Entity Too Large`
- Channel tune startup-failure backoff:
  - controlled by `TUNE_BACKOFF_MAX_TUNES`, `TUNE_BACKOFF_INTERVAL`, and `TUNE_BACKOFF_COOLDOWN`
  - applies only to requests that would create a new shared session/source startup
  - counts startup-cycle outcomes only (startup leader path), so concurrent joiners sharing the same startup do not overcount failures or overclear successes
  - counts startup failures only; successful startups clear outstanding failure budget
  - scope is per channel, so one failing channel does not throttle unrelated channel startups
  - existing active shared sessions can continue accepting additional subscribers during cooldown
  - returns HTTP `503` with `Retry-After` while backoff is active
- Server read hardening:
  - `ReadTimeout=30s`
  - `ReadHeaderTimeout=5s`
  - `IdleTimeout=120s`

Common response codes:

- `401`: missing/invalid admin credentials
- `404`: channel/source/item not found
- `429`: rate limit exceeded
- `502`: upstream or ffmpeg stream failure
- `503`: all tuners are busy, or channel tune backoff is active

### Operational Log Events

Key info-level events emitted by the service:

- Stream/session lifecycle: `shared session created`, `shared session reused`, `shared session ready`, `shared session subscriber connected`, `shared session subscriber disconnected`, `shared session canceled while idle`, `stream tune rejected`, `stream tune failed`, `stream subscriber started`, `stream subscriber ended`, `stream subscriber canceled`, `stream subscriber disconnected due to lag`, `stream subscriber ended with error`.
- Recovery diagnostics include burst and pacing fields on `shared session recovery triggered` / `shared session recovery cycle budget exhausted` (`recovery_burst_count`, `recovery_burst_budget_count`, `recovery_burst_pace_window`, and `recovery_trigger_logs_coalesced` when repeated warnings are rate-limited).
- Tuner lifecycle: `tuner lease acquired`, `tuner lease reused`, `tuner lease released`, `tuner probe preempted`, `tuner idle-client preempted`.
- Admin mutation lifecycle: `admin channel created`, `admin channel updated`, `admin channels reordered`, `admin channel deleted`, `admin source added`, `admin source updated`, `admin sources reordered`, `admin source deleted`, `admin source health cleared`, `admin all source health cleared`, `admin automation updated`, `admin automation timezone updated`, `admin automation schedule updated`, `admin automation settings updated`, `admin manual job run started`, `admin auto-prioritize cache cleared`, `admin dvr config updated`, `admin dvr schedule updated`, `admin dvr sync requested`, `admin dvr sync completed`, `admin dvr reverse-sync requested`, `admin dvr reverse-sync completed`, `admin channel dvr reverse-sync requested`, `admin channel dvr reverse-sync completed`, `admin channel dvr mapping updated`.
- Dynamic channel immediate-sync lifecycle: `admin dynamic channel immediate sync queued`, `admin dynamic channel immediate sync started`, `admin dynamic channel immediate sync completed`, `admin dynamic channel immediate sync canceled`, `admin dynamic channel immediate sync skipped stale run`, `admin dynamic channel immediate sync failed`.
- Dynamic block materialization lifecycle: `admin dynamic block sync queued`, `admin dynamic block immediate sync started`, `admin dynamic block immediate sync completed`, `admin dynamic block immediate sync canceled`, `admin dynamic block immediate sync failed`, `admin dynamic generated channels reordered`.
- Dynamic block DVR lineup reload lifecycle: `admin dynamic block dvr lineup reload completed`, `admin dynamic block dvr lineup reload failed`.
- Jobs/scheduler/playlist lifecycle: `job started`, `job finished`, `job panic recovered`, `job run persistence failed`, `scheduler loaded schedules`, `scheduler schedule updated`, `scheduler timezone updated`, `playlist refresh started`, `playlist refresh finished`.
- SQLite IOERR diagnostics lifecycle: `sqlite_ioerr_diag_bundle` (one-shot pragma/db-file snapshot) and `sqlite_ioerr_trace_dump` (rate-limited in-memory DB operation timeline).
- Discovery lifecycle: `discovery response sent` (debug-level).
- Recovery filler normalization (debug-level): `shared session slate AV recovery filler profile normalized` with `original_resolution`, `normalized_resolution`, and `normalization_reason`.

Common correlation fields on these events include:

- `channel_id`, `guide_number`, `guide_name`
- `tuner_id`, `source_id`, `source_item_key`
- `subscriber_id`, `client_addr`, `remote_addr`
- `run_id`, `job_name`, `triggered_by`
- `result`, `reason`, `duration`

## Persistence, Backup, and Restore

- Catalog, channels, source mappings, and persisted device identity are stored in SQLite at `DB_PATH`.
- Default path is `./hdhr-iptv.db`.
- In Docker, default DB path is `/data/hdhr-iptv.db`; mount `/data` to persist.
- Backups:
  - stop service cleanly
  - copy DB file to backup storage
- Restore:
  - stop service
  - replace DB file
  - start service

Operational recommendation:

- If `DB_PATH` is persistent, identity remains stable across restarts automatically.
- Set explicit `DEVICE_ID` and `DEVICE_AUTH` when you need fixed identity across
  fresh databases, DB replacements/restores, or multiple deployments.

## Troubleshooting

### Discovery Does Not Find Device

- Confirm UDP `65001` is open between client and server.
- Verify service is running and listening.
- Validate client and server are on reachable subnets/VLANs.
- Try manual endpoint: `http://<server-ip>:5004/discover.json`.

### DVR Finds Device But No Channels

- `lineup.json` is published-channel only.
- Publish at least one channel in `/ui/catalog` or via `/api/channels`.
- Confirm playlist refresh succeeded and catalog has items.

### Streams Fail To Start

- HTTP `503`: all tuners are in use. Increase `TUNER_COUNT` or reduce concurrent playback.
- HTTP `502`: upstream URL unavailable or ffmpeg process failed.
- In ffmpeg modes, validate `FFMPEG_PATH` and local ffmpeg installation.
- For `ffmpeg-copy` startup-timeout errors, increase `STARTUP_TIMEOUT` and/or tune startup detection (`FFMPEG_STARTUP_PROBESIZE_BYTES`, `FFMPEG_STARTUP_ANALYZEDURATION`) so ffmpeg emits initial bytes before failover deadline.
- If initial startup frequently times out on random-access gating but recovery continuity still needs strict cutover behavior, keep `STARTUP_RANDOM_ACCESS_RECOVERY_ONLY=true` (the default) so random-access enforcement applies only during recovery cycles.
- Startup expects both video and audio components. If startup inventory reports an explicit component-incomplete state (`video_only` or `audio_only`), startup is rejected. Inspect diagnostics in logs and `/api/admin/tuners` (`source_startup_component_state`, `source_startup_video_streams`, `source_startup_audio_streams`). For random-access startup, compare `source_startup_probe_raw_bytes` vs `source_startup_probe_trimmed_bytes` and monitor `source_startup_probe_cutover_offset`/`source_startup_probe_dropped_bytes` to see how much pre-IDR data is discarded before stream handoff.
- High random-access cutover (>=75% dropped from startup probe) emits `shared session startup probe cutover warning` log events with bounded coalescing metadata (`source_startup_probe_cutover_warn_logs_coalesced`) to reduce repetitive log spam under rapid recovery churn.
- The runtime automatically retries once with relaxed startup probe/analyze settings (`source_startup_retry_relaxed_probe=true`) when startup detection initially appears component-incomplete. Startup is accepted only when inventory reaches `video_audio`; if the relaxed retry fails or still reports incomplete inventory, startup fails and failover continues.
- If DVR logs show disconnects after no data for several seconds, reduce `BUFFER_PUBLISH_FLUSH_INTERVAL`, confirm producer pacing (`PRODUCER_READRATE=1`), and confirm recovery keepalive remains enabled (`RECOVERY_FILLER_ENABLED=true`, default). For picky clients, try `RECOVERY_FILLER_MODE=slate_av` (decodable filler) or `RECOVERY_FILLER_MODE=psi` before `null`.
- For `RECOVERY_FILLER_MODE=slate_av`, odd source resolutions (for example `853x480`) are normalized to even dimensions for `libx264`/`yuv420p` encoder safety. Use debug logs to confirm normalization and watch `/api/admin/tuners` keepalive fallback counters if the session still degrades to `psi`/`null`.
- If recovery resumes cleanly but playback is far behind live edge, inspect `/api/admin/tuners` keepalive telemetry:
  - sustained high `recovery_keepalive_rate_bytes_per_second` relative to `recovery_keepalive_expected_rate_bytes_per_second`,
  - `recovery_keepalive_realtime_multiplier` significantly above `1.0` when profile bitrate is known,
  - non-zero `recovery_keepalive_guardrail_count` indicating safety fallback was required.

### Playlist Sync SQLite IOERR (Before-Restart Capture)

When playlist sync fails with SQLite IOERR codes (for example `SQLITE_IOERR_WRITE`
/ extended code `778`), capture diagnostics before restarting the process.

1. Keep the process running and preserve current logs.
2. Collect the first `sqlite_ioerr_diag_bundle` event for the failing run:
   - includes `phase`, `item_index`/`item_total`, `run_id`, sqlite code/name, `db.Stats()`, runtime pragmas, and DB/WAL/SHM file metadata.
3. If trace ring diagnostics are enabled, collect `sqlite_ioerr_trace_dump`:
   - provides the bounded pre-failure DB operation timeline (`op`, `phase`, duration, error classification/code).
4. Correlate timestamps with host and storage telemetry (kernel, filesystem, volume/backend metrics) before restart.
5. Restart only after capture is complete.

Rapid rollback switches (if diagnostic verbosity needs to be reduced immediately):

- Disable trace ring: `HDHRIPTV_SQLITE_IOERR_TRACE_ENABLED=false`.
- Reduce trace dump volume: lower `HDHRIPTV_SQLITE_IOERR_TRACE_DUMP_LIMIT` and/or increase `HDHRIPTV_SQLITE_IOERR_TRACE_DUMP_INTERVAL`.
- Disable optional checkpoint probing: `HDHRIPTV_SQLITE_IOERR_CHECKPOINT_PROBE=false`.

### Dynamic Rule Updates Do Not Apply Expected Sources

- Verify `dynamic_rule.enabled=true` and a non-empty `dynamic_rule.search_query`.
- Confirm target catalog rows are active and match both optional `group_name` and `search_query` (same behavior as `GET /api/items` filtering, including `-term`/`!term` exclusion support).
- Check logs for dynamic sync lifecycle events (`queued`, `started`, `completed`, `failed`, `canceled`, `skipped stale run`) and correlate by `channel_id`.
- `canceled` events include `cancel_reason` (`superseded`, `disabled_or_deleted`, `state_removed`, or `canceled`) to distinguish expected preemption from unexpected failures.
- If you disable a rule or delete a channel while sync is running, cancellation is expected and stale results are intentionally discarded.

### Automation Cron and Timezone Validation

- Invalid cron updates return HTTP `400` from automation or DVR config endpoints when a schedule is enabled.
- Disabling a schedule does not require cron validation; you can disable first and fix cron later.
- Scheduler timezone updates require a valid IANA timezone string; blank values return HTTP `400`.
- If persisted timezone is invalid at load time, scheduler falls back to `UTC` and logs a warning (`invalid scheduler timezone; falling back to UTC`).

### Automation and DVR Config Apply / Rollback Behavior

- `PUT /api/admin/automation` snapshots prior scheduler-related settings before applying updates and performs apply/rollback work under a detached `30s` timeout budget. If `Scheduler.LoadFromSettings(...)` fails, the handler restores the prior values and returns HTTP `500`.
- `PUT /api/admin/dvr` updates persisted DVR config first, then applies scheduler changes for `dvr_lineup_sync`. If schedule apply fails, the previous DVR config is restored before the error response is sent.
- If rollback itself fails, the error response includes both the apply failure and rollback failure details; immediately re-read config state from `GET /api/admin/automation` and `GET /api/admin/dvr` before retrying writes.

### Job Runs Stay Running, Error, or Conflict

- HTTP `409` when triggering a run means that job (or another job under global job lock) is already running.
- Inspect run state with `GET /api/admin/jobs/{runID}` and check `status`, `progress_cur`, `progress_max`, `summary`, and `error`.
- `status=error` with empty progress usually indicates early validation/config errors (for example missing playlist URL or provider access failure).
- `status=canceled` is expected on shutdown or canceled request contexts.
- If a run appears stalled in `running`, correlate with log events: `job started`, `job finished`, `playlist refresh*`, `admin dvr sync*`, and upstream/provider error messages.

### DVR Sync Mapping Edge Cases

- If lineup entries expose `station_ref` without `lineup_channel`, sync can still succeed using station-ref-only mapping.
- Station-ref-only behavior is surfaced as warnings in sync responses and should not be treated as automatic failure.
- Forward sync unresolved counts (`unresolved_count`) usually indicate missing tuner-number matches or lineup station lookup mismatches.
- Reverse sync missing counts (`missing_tuner_count`, `missing_mapping_count`, `missing_station_ref_count`) identify which side lacks matching metadata.

### Channels Page DVR Mapping Refresh and Rate Limiting

- `/ui/channels` uses paged bulk mapping fetch (`GET /api/channels/dvr` with `limit`/`offset`) to reduce per-row request fan-out and avoid one unbounded mapping payload.
- If DVR mappings render empty after refresh, check for HTTP `429` responses and adjust `RATE_LIMIT_RPS`/`RATE_LIMIT_BURST` for your admin workload.
- Confirm the DVR backend is reachable with `POST /api/admin/dvr/test` before treating blank mappings as UI-only issues.
- Use browser devtools network logs to confirm mapping payload content and response codes when diagnosing render gaps.

### Reverse Proxy Rate-Limit Bucketing

- If all users behind a reverse proxy appear to share one limiter bucket (frequent cross-user `429`), set `RATE_LIMIT_TRUSTED_PROXIES` (or `--rate-limit-trusted-proxies`) to the proxy CIDR/IP.
- Include only proxy hops you operate and trust to sanitize forwarded headers.
- Header precedence for trusted peers is `Forwarded`, then `X-Forwarded-For`, then `X-Real-IP`.
- `Forwarded` and `X-Forwarded-For` values are resolved right-to-left by peeling trusted hops from the right; malformed chains or all-trusted chains fall back to `RemoteAddr`.

### Auth Problems

- HTTP `401`: credentials are missing or wrong.
- HTTP `500` on all admin routes: `ADMIN_AUTH` format is invalid; use `user:pass`.

### Clients Ignore Advertised Port

- Enable legacy listener for compatibility:
  - set `HTTP_ADDR_LEGACY=:80`
  - expose/open TCP `80`

## Testing and Validation

### Automated Tests

```bash
go test ./...
```

For stream package tests, prefer shared fast timing defaults from `internal/stream/test_helpers_test.go` and event-driven progress checks over fixed `time.Sleep(...)` delays in high-frequency paths.

### Manual Smoke Checks

```bash
curl -s http://127.0.0.1:5004/healthz
curl -s http://127.0.0.1:5004/discover.json
curl -s http://127.0.0.1:5004/lineup_status.json
```

### Client Compatibility Checklist

Run `deploy/testing/compatibility-checklist.md` against real clients on your LAN:

- Plex
- Emby
- Jellyfin
- VLC

### Manual Recovery Continuity Test (`ffmpeg-copy` + `slate_av`)

Run the persisted recovery harness:

```bash
./deploy/testing/recovery-slate-av-ffmpeg-copy.sh
```

Expected output:

- `result=PASS`
- a summary artifact path (for example `/tmp/hdhr-slate-av-ffmpeg-copy-test.XXXXXX/summary.txt`)

The harness starts temporary local primary/backup MPEG-TS sources, forces source failover, plays the channel via `cvlc`, and verifies recovery continuity signals in server/client logs.

For ffplay-focused continuity + boundary-signal checks (headless/no-display):

```bash
./deploy/testing/recovery-slate-av-ffmpeg-copy-ffplay.sh
```

The ffplay harness captures `/api/admin/tuners` while playback is active and reports:

- `has_boundary_signal_set` (`disc,cc,pcr,psi_version` observed in transition telemetry)
- `has_boundary_stitch_applied` (whether PCR stitch path applied)
- `boundary_signal_values` / `boundary_signal_skips`
- keepalive pacing/backlog fields (`keepalive_bytes`, `keepalive_chunks`, `keepalive_rate_bytes_per_second`, `keepalive_expected_rate_bytes_per_second`, `keepalive_realtime_multiplier`)
- guardrail safety fields (`has_keepalive_guardrail_trigger`, `keepalive_guardrail_count`, `keepalive_guardrail_reason`)
- continuity/decode warning counts from `ffplay`

Set `STRICT_CONTINUITY=1` to fail the run when ffplay continuity warnings are detected.
Set `KEEPALIVE_MAX_REALTIME_MULTIPLIER` (default `2.5`) to tune the harness fail threshold for pacing regressions when bitrate-based realtime multiplier telemetry is available.

## Deployment Notes

- Dockerfile: `Dockerfile`
- Systemd unit and setup guide: `deploy/systemd/README.md`
- Optional Avahi/mDNS helper files: `deploy/avahi/README.md`
