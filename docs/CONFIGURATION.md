# Configuration Reference

Flags and environment variables are equivalent except where explicitly labeled
as env-only or internal-only. Flag values override environment variables.

## Core Settings

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--playlist-url` | `PLAYLIST_URL` | empty | No | If empty, no automatic refresh runs. Existing DB data may still be served. |
| `--db-path` | `DB_PATH` | `./hdhr-iptv.db` | No | SQLite file location. |
| `--log-dir` | `LOG_DIR` | current working directory | No | Directory for startup timestamped log files (`hdhriptv-YYYYMMDD-HHMMSS.log`). |
| `--http-addr` | `HTTP_ADDR` | `:5004` | No | Primary HTTP listener. |
| `--http-addr-legacy` | `HTTP_ADDR_LEGACY` | empty | No | Optional second HTTP listener, often `:80`. |
| `--tuner-count` | `TUNER_COUNT` | `2` | No | Max concurrent stream sessions. Must be `>= 1`. |
| `--friendly-name` | `FRIENDLY_NAME` | `HDHR IPTV` | No | Displayed in discover payloads and DVR UIs. Persisted in DB and reused across restarts unless explicitly set at startup. |
| `--device-id` | `DEVICE_ID` | random 8 hex chars | No | Auto-generated once and persisted in DB when unset; reused across restarts while DB persists. |
| `--device-auth` | `DEVICE_AUTH` | random token | No | Auto-generated once and persisted in DB when unset; reused across restarts while DB persists. |
| `--admin-auth` | `ADMIN_AUTH` | empty | No | Format must be `user:pass`. If empty, no auth on `/ui/*` and `/api/*`. |
| `--log-level` | `LOG_LEVEL` | `info` | No | `debug`, `info`, `warn`, `error`. |

## UPnP/SSDP

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--upnp-enabled` | `UPNP_ENABLED` | `true` | No | Enable UPnP/SSDP responder on UDP `1900` (`M-SEARCH` + optional `NOTIFY`). |
| `--upnp-addr` | `UPNP_ADDR` | `:1900` | No | UPnP/SSDP UDP listen address. |
| `--upnp-notify-interval` | `UPNP_NOTIFY_INTERVAL` | `5m` | No | Periodic `NOTIFY ssdp:alive` interval. Set `0` to disable periodic announcements; shutdown still emits `ssdp:byebye` when notifications are enabled. |
| `--upnp-max-age` | `UPNP_MAX_AGE` | `30m` | No | `CACHE-CONTROL: max-age` value used in SSDP responses and `NOTIFY` announcements. |
| `--upnp-content-directory-update-id-cache-ttl` | `UPNP_CONTENT_DIRECTORY_UPDATE_ID_CACHE_TTL` | `1s` | No | TTL for cached UPnP `ContentDirectory` `GetSystemUpdateID` values. Increase to reduce repeated lineup reads under frequent SOAP polling. |

## Scheduling and Automation

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--refresh-schedule` | `REFRESH_SCHEDULE` | empty | No | Cron expression for playlist sync (`5-field` or optional-seconds `6-field`). When set, it updates persisted `jobs.playlist_sync.cron` and enables playlist sync scheduling. |
| `--refresh-interval` | `REFRESH_INTERVAL` | empty | No | Deprecated compatibility flag/env. Duration is converted to `--refresh-schedule` when representable (for example `30m` -> `*/30 * * * *`). |

## Stream Mode

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--ffmpeg-path` | `FFMPEG_PATH` | `ffmpeg` | No | Executable used for ffmpeg stream modes. |
| `--stream-mode` | `STREAM_MODE` | `ffmpeg-copy` | No | `direct`, `ffmpeg-copy`, `ffmpeg-transcode`. |

## Startup and Failover

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--startup-timeout` | `STARTUP_TIMEOUT` | `6s` | No | Per-source startup timeout during tune failover. |
| `--startup-random-access-recovery-only` | `STARTUP_RANDOM_ACCESS_RECOVERY_ONLY` | `true` | No | When `true`, startup random-access enforcement is applied only during recovery cycles; initial startup no longer waits for a random-access cutover marker under `slate_av` + ffmpeg modes. |
| `--min-probe-bytes` | `MIN_PROBE_BYTES` | `940` | No | Startup bytes required before stream commit (`>= 1`). |
| `--max-failovers` | `MAX_FAILOVERS` | `3` | No | Number of fallback attempts after primary source (`0` means try all enabled sources). |
| `--failover-total-timeout` | `FAILOVER_TOTAL_TIMEOUT` | `32s` | No | Total tune-time budget across startup failover attempts. |
| `--upstream-overlimit-cooldown` | `UPSTREAM_OVERLIMIT_COOLDOWN` | `3s` | No | Per-provider-scope cooldown applied after upstream startup `429 Too Many Requests` responses before retrying that same provider scope. |

## FFmpeg Input Tuning

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--ffmpeg-reconnect-enabled` | `FFMPEG_RECONNECT_ENABLED` | `false` | No | Enables ffmpeg input reconnect flags (`-reconnect*`) for ffmpeg stream modes. |
| `--ffmpeg-reconnect-delay-max` | `FFMPEG_RECONNECT_DELAY_MAX` | `3s` | No | Max ffmpeg reconnect delay (`-reconnect_delay_max`, rounded up to seconds). |
| `--ffmpeg-reconnect-max-retries` | `FFMPEG_RECONNECT_MAX_RETRIES` | `1` | No | Maximum ffmpeg reconnect attempts (`-reconnect_max_retries`). |
| `--ffmpeg-reconnect-http-errors` | `FFMPEG_RECONNECT_HTTP_ERRORS` | empty | No | Value used for `-reconnect_on_http_error` in ffmpeg stream modes. Empty disables this specific ffmpeg input option. |
| `--ffmpeg-startup-probesize-bytes` | `FFMPEG_STARTUP_PROBESIZE_BYTES` | `1000000` | No | FFmpeg input `-probesize` used during startup stream detection in ffmpeg modes. Values below `128000` are normalized up to this floor to avoid startup stream-detection regressions. |
| `--ffmpeg-startup-analyzeduration` | `FFMPEG_STARTUP_ANALYZEDURATION` | `1.5s` | No | FFmpeg input `-analyzeduration` used during startup stream detection in ffmpeg modes. Values below `1s` are normalized up to this floor to avoid startup stream-detection regressions. |
| `--ffmpeg-copy-regenerate-timestamps` | `FFMPEG_COPY_REGENERATE_TIMESTAMPS` | `true` | No | Enables ffmpeg-copy timestamp regeneration (`-fflags +genpts`) to smooth sources with missing/non-monotonic timestamps. |
| `--producer-readrate` | `PRODUCER_READRATE` | `1` | No | FFmpeg producer pacing (`-readrate`). Shared sessions use this before buffering. |
| `--producer-readrate-catchup` | `PRODUCER_READRATE_CATCHUP` | `1` | No | FFmpeg producer catch-up pacing (`-readrate_catchup`) used when input falls behind realtime. Must be greater than or equal to `--producer-readrate`. |
| `--producer-initial-burst` | `PRODUCER_INITIAL_BURST` | `1` | No | FFmpeg producer initial burst seconds (`-readrate_initial_burst`). |

## Shared Session Buffering

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--buffer-chunk-bytes` | `BUFFER_CHUNK_BYTES` | `65536` | No | Shared-session chunk publish threshold in bytes (`>= 1`). |
| `--buffer-publish-flush-interval` | `BUFFER_PUBLISH_FLUSH_INTERVAL` | `100ms` | No | Shared-session timer flush interval for partial chunks. |
| `--buffer-ts-align-188` | `BUFFER_TS_ALIGN_188` | `false` | No | Align non-final chunk publishes to MPEG-TS packet boundaries (188-byte multiples). |

## Stall Detection and Recovery

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--stall-detect` | `STALL_DETECT` | `4s` | No | Shared-session no-publish threshold before recovery starts. |
| `--stall-hard-deadline` | `STALL_HARD_DEADLINE` | `32s` | No | Recovery deadline after stall detection before session closes. |
| `--stall-policy` | `STALL_POLICY` | `failover_source` | No | `failover_source`, `restart_same`, `close_session`. In `failover_source`, recovery automatically falls back to restart-like same-source retries when no startup-eligible alternates remain (alternate source ID with non-empty URL and no active cooldown). |
| `--stall-max-failovers-per-stall` | `STALL_MAX_FAILOVERS_PER_STALL` | `3` | No | Maximum startup failovers attempted per detected stall. |
| `--cycle-failure-min-health` | `CYCLE_FAILURE_MIN_HEALTH` | `20s` | No | Minimum healthy uptime for recovery-selected sources before cycle-close failures are persisted to source health (`0` disables this guard). |

## Recovery Filler

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--recovery-filler-enabled` | `RECOVERY_FILLER_ENABLED` | `true` | No | Enable recovery keepalive during shared-session recovery windows. |
| `--recovery-filler-mode` | `RECOVERY_FILLER_MODE` | `slate_av` | No | Recovery keepalive mode: `null` (PID `0x1FFF` null packets), `psi` (PAT/PMT packets), or `slate_av` (decodable A/V filler). In `slate_av`, odd/missing profile dimensions are normalized to codec-safe even values before filler startup. |
| `--recovery-filler-interval` | `RECOVERY_FILLER_INTERVAL` | `200ms` | No | Keepalive interval for packet modes (`null`/`psi`) while recovery is active (`> 0`). |
| `--recovery-filler-text` | `RECOVERY_FILLER_TEXT` | `Channel recovering...` | No | Overlay text rendered by `slate_av` recovery filler mode. |
| `--recovery-filler-enable-audio` | `RECOVERY_FILLER_ENABLE_AUDIO` | `true` | No | Include silent AAC audio track in `slate_av` filler to improve client decoder continuity. |

## Subscriber Settings

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--subscriber-join-lag-bytes` | `SUBSCRIBER_JOIN_LAG_BYTES` | `2097152` | No | New subscriber join lag window in bytes (ring tail cushion). |
| `--subscriber-slow-client-policy` | `SUBSCRIBER_SLOW_CLIENT_POLICY` | `disconnect` | No | `disconnect` or `skip` when client falls behind ring tail. |
| `--subscriber-max-blocked-write` | `SUBSCRIBER_MAX_BLOCKED_WRITE` | `6s` | No | Per-chunk write deadline for shared subscribers (`0` disables deadline). |

## Session Lifecycle

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--session-idle-timeout` | `SESSION_IDLE_TIMEOUT` | `5s` | No | Shared channel session teardown delay after last subscriber leaves. |
| `--session-drain-timeout` | `SESSION_DRAIN_TIMEOUT` | `2s` | No | Timeout budget used by shared-session bounded reader closes during startup aborts/recovery teardown. |
| `--session-max-subscribers` | `SESSION_MAX_SUBSCRIBERS` | `0` | No | Max subscribers per shared channel session (`0` for unlimited). |
| `--session-history-limit` | `SESSION_HISTORY_LIMIT` | `0` | No | Shared-session lifecycle history retention cap (`0` uses runtime default). |
| `--session-source-history-limit` | `SESSION_SOURCE_HISTORY_LIMIT` | `0` | No | Per-session source-history retention cap (`0` falls back to `session-history-limit` and runtime guardrails). |
| `--session-subscriber-history-limit` | `SESSION_SUBSCRIBER_HISTORY_LIMIT` | `0` | No | Per-session subscriber-history retention cap (`0` falls back to `session-history-limit` and runtime guardrails). |
| `--source-health-drain-timeout` | `SOURCE_HEALTH_DRAIN_TIMEOUT` | `0s` | No | Source-health persistence drain budget during session teardown (`0` uses runtime default `250ms`). |
| `n/a (env-only)` | `CLOSE_WITH_TIMEOUT_WORKER_BUDGET` | `16` | No | Global `closeWithTimeout` worker budget (`1..256`). Raise only when sustained close suppression indicates shutdown-path saturation. |
| `--preempt-settle-delay` | `PREEMPT_SETTLE_DELAY` | `500ms` | No | Delay before refilling from full tuner usage back to full usage (for example `2->1->2`), and before reusing a preempted slot. Helps upstream providers with lagging session teardown accounting. |

## Source Probing and Auto-Prioritize

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--auto-prioritize-probe-tune-delay` | `AUTO_PRIORITIZE_PROBE_TUNE_DELAY` | `1s` | No | Delay between automated probe tunes (auto-prioritize and background source probing) to reduce upstream session overlap. |
| `--auto-prioritize-workers` | `AUTO_PRIORITIZE_WORKERS` | `2` | No | Auto-prioritize worker policy: fixed positive worker count, or `auto` to use current available tuner slots. |
| `--probe-interval` | `PROBE_INTERVAL` | `0` | No | Background source probe interval (`0` disables probing). |
| `--probe-timeout` | `PROBE_TIMEOUT` | `3s` | No | Per-source timeout for background probes (`<= probe-interval` when enabled). |

## Rate Limiting and Hardening

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--admin-json-body-limit-bytes` | `ADMIN_JSON_BODY_LIMIT_BYTES` | `1048576` | No | Maximum JSON body size accepted by admin mutation endpoints (`> 0`). Oversized bodies are rejected with HTTP `413`. |
| `--request-timeout` | `REQUEST_TIMEOUT` | `15s` | No | Timeout for non-stream routes. `0` disables. |
| `--rate-limit-rps` | `RATE_LIMIT_RPS` | `8` | No | Per-client IP token bucket. `0` disables rate limiting. |
| `--rate-limit-burst` | `RATE_LIMIT_BURST` | `32` | No | Must be `>= 1` when rate limiting is enabled. |
| `--rate-limit-max-clients` | `RATE_LIMIT_MAX_CLIENTS` | `4096` | No | Max distinct client-IP limiter entries kept in memory (`0` disables cap). Oldest entries are evicted first when cap is reached. |
| `--rate-limit-trusted-proxies` | `RATE_LIMIT_TRUSTED_PROXIES` | empty | No | Comma-separated trusted proxy CIDR/IP entries. Forwarded client headers are honored only when the immediate peer IP is trusted. Header precedence: `Forwarded`, then `X-Forwarded-For`, then `X-Real-IP`. Chain headers are resolved right-to-left by peeling trusted hops from the right and selecting the first non-trusted IP; malformed chains (or all-trusted chains) fall back to the peer IP. |

## Tune Backoff Protection

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--tune-backoff-max-tunes` | `TUNE_BACKOFF_MAX_TUNES` | `8` | No | Per-channel startup-failure threshold across `/auto/...` before channel tune cooldown activates. `0` disables tune backoff protection. |
| `--tune-backoff-interval` | `TUNE_BACKOFF_INTERVAL` | `1m` | No | Rolling window used with `tune-backoff-max-tunes` to count per-channel startup failures. |
| `--tune-backoff-cooldown` | `TUNE_BACKOFF_COOLDOWN` | `20s` | No | Per-channel cooldown window applied after tune backoff threshold is exceeded. |

## Catalog Search Limits

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--catalog-search-max-terms` | `CATALOG_SEARCH_MAX_TERMS` | `12` | No | Maximum token-mode include/exclude terms retained per query (`1..256`). Additional terms are truncated and reported through `search_warning`. |
| `--catalog-search-max-disjuncts` | `CATALOG_SEARCH_MAX_DISJUNCTS` | `6` | No | Maximum token-mode OR disjunct clauses retained per query (`1..128`). Additional disjuncts are truncated and reported through `search_warning`. |
| `--catalog-search-max-term-runes` | `CATALOG_SEARCH_MAX_TERM_RUNES` | `64` | No | Maximum runes retained per token in token mode (`1..256`). Per-token rune clipping is reported through `search_warning.term_rune_truncations`. |

## Metrics

| Flag | Environment Variable | Default | Required | Notes |
| --- | --- | --- | --- | --- |
| `--enable-metrics` | `ENABLE_METRICS` | `false` | No | Enables `/metrics` Prometheus endpoint. |

## Internal Stream Tuning (Programmatic Only)

These options are available to embedders that construct stream internals
directly. They are not currently exposed as CLI flags or environment variables.

| Internal Option | Default | Notes |
| --- | --- | --- |
| `ProberConfig.ProbeCloseQueueDepth` | `8` | Capacity of the background-prober probe-session close queue. `<=0` normalizes to the default. Queue-full and inline fallback behavior is observable via `/api/admin/tuners` `probe_close.inline_count` and `probe_close.queue_full_count`. |

## SQLite IOERR Diagnostic Toggles (Environment Only)

These advanced toggles are environment-only (no CLI flag) and are intended for
incident windows where deeper SQLite write-path diagnostics are needed.

| Environment Variable | Default | Notes |
| --- | --- | --- |
| `HDHRIPTV_SQLITE_IOERR_CHECKPOINT_PROBE` | `false` | Enables one-shot `PRAGMA wal_checkpoint(PASSIVE)` capture inside `sqlite_ioerr_diag_bundle` events. Keep disabled unless actively diagnosing WAL state. |
| `HDHRIPTV_SQLITE_IOERR_TRACE_ENABLED` | `false` | Enables in-memory DB operation trace ring used for IOERR dump context (`sqlite_ioerr_trace_dump`). |
| `HDHRIPTV_SQLITE_IOERR_TRACE_RING_SIZE` | `256` | Fixed trace-ring entry count (`16`..`16384`). Larger values retain more pre-failure history but use more memory. |
| `HDHRIPTV_SQLITE_IOERR_TRACE_DUMP_LIMIT` | `64` | Maximum trace entries emitted per IOERR dump (`>=1`, capped to ring size). |
| `HDHRIPTV_SQLITE_IOERR_TRACE_DUMP_INTERVAL` | `30s` | Minimum interval between IOERR trace dumps. Set `0` to disable dump rate limiting during short debugging windows. |
