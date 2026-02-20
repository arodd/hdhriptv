# Operations

This document covers cross-cutting operational concerns for developers and
operators running `hdhriptv`. It complements the README's user-facing
configuration reference and troubleshooting sections with internal
architecture details, behavioral nuances, and diagnostic guidance.

## Logging Architecture

All logging is initialized by `internal/logging/logging.go`. The system
produces structured `slog.TextHandler` output in `key=value` format (not
JSON).

### Fan-out Handler

Every log record is dispatched to three simultaneous destinations through a
`fanoutHandler`:

| Destination | Level Range | Handler |
|-------------|-------------|---------|
| **stdout** | `DEBUG` .. `INFO` | `levelRangeHandler` with upper bound |
| **stderr** | `WARN` .. `ERROR` | `levelRangeHandler` with no upper bound |
| **log file** | configured level+ | `slog.TextHandler` (same `LevelVar` filter) |

The `levelRangeHandler` wraps a standard `slog.Handler` and adds min/max
level filtering. The stdout handler has `hasMax=true` (capped at `INFO`), so
`WARN` and `ERROR` records never appear on stdout. The stderr handler has
`hasMax=false`, so it accepts everything from `WARN` upward. The file
handler uses the same `LevelVar` filter as the stdout/stderr handlers, so it
respects the configured log level.

### Container and systemd implications

Container runtimes and systemd capture stdout and stderr as separate
streams. When using `journalctl`, entries from stderr appear with priority
`err`/`warning` while stdout entries appear with priority `info`/`debug`.
Log aggregation pipelines that merge both streams into one will see
interleaved output — the `key=value` format includes a `level=` field for
disambiguation.

### Log file lifecycle

- **Naming**: `hdhriptv-YYYYMMDD-HHMMSS.log` using the process startup
  timestamp.
- **Directory**: `LOG_DIR` is auto-created with `MkdirAll` mode `0o755`.
- **File permissions**: `0o644` (owner read/write, group/other read).
- **No rotation**: Each process startup creates a new file; old log files
  accumulate and are never automatically removed. Operators should configure
  external rotation (e.g., `logrotate`, cron cleanup) to manage disk
  usage.
- **Close**: The log file is closed when the process exits (deferred in
  `main.go`).

### Runtime level variable

The configured level is stored in a `slog.LevelVar`, which supports
atomic updates. While the service does not currently expose a runtime
level-change API, the infrastructure is in place for potential future use.

### Startup phase timing instrumentation

Startup emits structured timing checkpoints with:

- `msg="startup phase complete"`
- `phase=<phase_name>`
- `duration=<elapsed_duration>`

Current phase names are:

- `sqlite_runtime_pragmas`
- `sqlite_migrate`
- `sqlite_open_total`
- `identity_settings_resolve`
- `automation_overrides_sync`
- `dvr_schedule_sync`
- `scheduler_load_from_settings`

These records are intended for startup latency baselining and regression
tracking in log pipelines.

### Initial playlist sync startup events

When a playlist URL is configured, startup emits a dedicated initial-sync phase
event stream:

- `msg="initial playlist sync phase"`
- `initial_sync_phase=scheduled_after_listener_start|completed|failed`

Operational behavior:

- The initial sync is deferred until HTTP listener readiness succeeds
  (`/healthz`) so provider reloads do not run before listeners are accepting
  requests.
- Startup retry/backoff is scoped to transient Jellyfin lineup reload failures
  only.
- Retry policy is bounded: up to `4` attempts, exponential backoff from `1s`
  capped at `8s`, within a `3m` retry budget.
- Non-transient failures bypass retries and surface immediately.
- After startup sync reconciliation, dynamic generated guide names are refreshed
  so dynamic channel lineup names stay aligned with current playlist metadata.

Useful fields:

- `attempt` on `completed` phase events (`initial_sync_phase=completed`).
- `attempt`, `max_attempts`, and `backoff` on retry warnings (`msg="initial playlist sync transient jellyfin lineup reload failure; retrying"`). Retry warnings are separate log records and do not carry an `initial_sync_phase` value.
- `duration` on both `completed` and `failed` phase events, including readiness-gate failures.
- `error` on `failed` events.

## Stream Prometheus Metrics

`internal/stream` exports stream telemetry metrics on the standard
Prometheus `/metrics` endpoint.

### Shared-session lag, write-pressure, and source-ingress pause

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `stream_slow_skip_events_total` | counter | none | Total skip-policy lag events (slow subscriber fell behind and skipped forward). |
| `stream_slow_skip_lag_chunks` | histogram | none | Lag depth distribution (chunks) when skip-policy events happen. |
| `stream_slow_skip_lag_bytes` | histogram | none | Estimated lag depth distribution (bytes) when skip-policy events happen. |
| `stream_subscriber_write_deadline_timeouts_total` | counter | none | Subscriber writes that hit write deadlines (`os.ErrDeadlineExceeded`/timeout-classified errors). |
| `stream_subscriber_write_short_writes_total` | counter | none | Subscriber writes that returned `io.ErrShortWrite`. |
| `stream_subscriber_write_blocked_seconds` | histogram | none | Time spent blocked in subscriber `ResponseWriter.Write` calls. |
| `stream_source_read_pause_events_total` | counter | `reason` | Source read pauses >= 1s (minimum threshold) grouped by finalize reason. |
| `stream_source_read_pause_seconds` | histogram | `reason` | Duration distribution for source read pauses >= 1s grouped by finalize reason. |

`stream_source_read_pause_*{reason=...}` values:

- `recovered`: reads resumed and the pause closed normally.
- `pump_exit`: pump/run cycle exited while pause tracking was active.
- `ctx_cancel`: session context cancellation finalized an active pause.

### Bounded-close lifecycle telemetry

| Metric | Type | Labels | Purpose |
|--------|------|--------|---------|
| `stream_close_with_timeout_started_total` | counter | none | Number of bounded close attempts started. |
| `stream_close_with_timeout_retried_total` | counter | none | Number of deferred close attempts re-run from retry queue. |
| `stream_close_with_timeout_suppressed_total` | counter | none | Number of close attempts suppressed due to in-flight/budget limits. |
| `stream_close_with_timeout_suppressed_duplicate_total` | counter | none | Suppressed attempts due to duplicate close ownership. |
| `stream_close_with_timeout_suppressed_budget_total` | counter | none | Suppressed attempts due to global worker-budget saturation. |
| `stream_close_with_timeout_dropped_total` | counter | none | Deferred close retries dropped after retry queue overflow. |
| `stream_close_with_timeout_timeouts_total` | counter | none | Close attempts that exceeded bounded timeout. |
| `stream_close_with_timeout_late_completions_total` | counter | none | Timed-out closes that later completed before abandon deadline. |
| `stream_close_with_timeout_late_abandoned_total` | counter | none | Timed-out closes still blocked after abandon deadline. |
| `stream_close_with_timeout_release_underflow_total` | counter | none | Internal worker-slot release underflow guardrail hits. |

## Stream Profile Probing

`internal/stream/profile_probe.go` provides stream metadata detection used
by recovery filler resolution matching and tuner status diagnostics.

### streamProfile type

```go
type streamProfile struct {
    Width           int
    Height          int
    FrameRate       float64
    VideoCodec      string
    AudioCodec      string
    AudioSampleRate int
    AudioChannels   int
    BitrateBPS      int64
}
```

### probeStreamProfile

Invokes `ffprobe` with JSON output (`-of json -show_streams -show_format`)
against a stream URL. Key parameters:

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `ffprobePath` | `ffprobe` | Executable path |
| `timeout` | 4 s | Context deadline for the probe |
| `analyzeduration` | 1,500,000 us | ffprobe analysis window |
| `probesize` | 1,000,000 bytes | ffprobe input read limit |

The function:

1. Runs ffprobe and captures JSON output.
2. Selects the first video and first audio stream from the result
   (`selectProfileStreams`).
3. Parses frame rate from `avg_frame_rate` (falling back to `r_frame_rate`),
   handling both decimal and fractional (`N/D`) formats.
4. Resolves bitrate using a priority chain: video `bit_rate` >
   format `bit_rate` > stream tag `variant_bitrate`
   (`firstPositiveInt64`).

### Recovery filler resolution matching

When `RECOVERY_FILLER_MODE=slate_av`, the recovery filler uses the probed
profile to match the slate resolution, frame rate, and audio parameters to
the active source. If the profile has odd dimensions (e.g., `853x480`), they
are normalized to even values for `libx264`/`yuv420p` encoder safety. If no
profile is available, defaults are 1280x720 @ 29.97 fps, 48 kHz stereo.

### estimateCurrentBitrateBPS

Computes a running bitrate estimate from bytes pushed since source
selection:

```
bps = (bytesPushed - sourceBytesBaseline) * 8 / elapsed_seconds
```

This estimate is exposed in tuner status as `current_bitrate_bps` for
telemetry purposes only. Keepalive pacing guardrail calculations use the
separate `recoveryKeepaliveExpectedRate` field.

### Profile persistence

Probed profiles are persisted to the database via `UpdateSourceProfile` so
that admin diagnostics/history retain last-seen stream characteristics across
process restarts. Current live shared-session filler sizing uses the active
session's in-memory probe profile. The auto-prioritize analyzer
(`internal/analyzer/ffmpeg.go`) uses a similar but separate probing
subsystem for source quality ranking.

## URL Sanitization

`internal/stream/url_sanitize.go` implements credential and token redaction
for all user-visible URL output.

### SanitizeURLForLog

Parses the URL, then strips:

| Component | Action |
|-----------|--------|
| `User` (userinfo) | Removed (`parsed.User = nil`) |
| `RawQuery` | Removed (`parsed.RawQuery = ""`) |
| `Fragment` | Removed (`parsed.Fragment = ""`) |
| Scheme | Preserved |
| Host | Preserved |
| Path | Preserved |

If `url.Parse` fails, a fallback path (`fallbackSanitizeStreamURL`)
manually strips query/fragment and removes userinfo by finding `@` in the
authority portion.

### Where sanitization is applied

- **Log output**: All stream URLs logged during session lifecycle use
  `sanitizeStreamURLForLog`.
- **Tuner status API** (`/api/admin/tuners`): Live `source_stream_url`
  fields and all `session_history[*].sources[*].stream_url` entries use
  `sanitizeStreamURLForStatus` (same redaction policy).
- **Client stream status**: The `ClientStreamStatus` source URL is sanitized
  when building the snapshot.

The sanitization functions are intentionally aliased
(`sanitizeStreamURLForLog` and `sanitizeStreamURLForStatus` both delegate
to `SanitizeURLForLog`) so the redaction policy is consistent across all
surfaces and can be changed in one place.

## Tuner Status Diagnostics

`internal/stream/status.go` provides the structured status snapshot served
by `GET /api/admin/tuners` and rendered in `/ui/tuners`.

### Core types

| Type | Purpose |
|------|---------|
| `TunerStatusSnapshot` | Top-level response: tuner count, in-use/idle counts, churn summary, tuner list, client streams, session history |
| `TunerStatus` | One active tuner lease with linked shared-session state (source, recovery, keepalive, subscriber details) |
| `ClientStreamStatus` | One connected subscriber mapped to its backing tuner session |
| `ChurnSummary` | Aggregated recovery/reselection counters across all active shared sessions |
| `SharedSessionHistory` | Lifecycle record for one shared session (active or recently closed) |

### deriveTunerState

Maps tuner kind and subscriber count to a human-readable state string:

| Kind | Subscribers | Has session | State |
|------|-------------|-------------|-------|
| `probe` | any | any | `probe` |
| `client` | > 0 | any | `active_subscribers` |
| `client` | 0 | yes | `idle_grace_no_subscribers` |
| `client` | 0 | no | `allocating_session` |
| other | > 0 | any | `active_subscribers` |
| other | 0 | any | `unknown` |

### SharedSessionHistory

In-memory retention of active and recently closed session lifecycle records.
Key operational details:

- **Retention**: 256 entries (configurable via
  `--session-history-limit` / `SESSION_HISTORY_LIMIT`; default `256`).
- **Eviction**: Oldest entries are removed when the retention cap is
  reached. `session_history_truncated_count` in the API response tracks how
  many entries have been evicted since process start.
- **Per-session timeline bounds**: Each entry exposes
  `source_history_limit` / `source_history_truncated_count` and
  `subscriber_history_limit` / `subscriber_history_truncated_count` so
  operators can distinguish overall session-entry eviction from in-session
  source/subscriber timeline trimming.
- **Content**: Each entry includes `opened_at`, optional `closed_at`,
  `active` flag, `terminal_status`, `peak_subscribers`,
  `recovery_cycle_count`, `same_source_reselect_count`, and nested
  `sources` / `subscribers` timelines.
- **URL sanitization**: All `sources[*].stream_url` values in history
  entries are sanitized before inclusion in the snapshot.
- **Lifetime**: History is in-memory only and is lost on process restart.

### ChurnSummary

Aggregated from all active `SharedSessionStats`:

- `recovering_session_count`: sessions that have entered recovery in the
  current process lifetime (non-zero `recovery_cycle` or non-empty
  `recovery_reason`), not strictly "currently recovering".
- `sessions_with_reselect_count`: sessions where
  `same_source_reselect_count > 0`.
- `sessions_over_reselect_threshold`: sessions exceeding the alert threshold
  (default `3`).
- `total_recovery_cycles`, `total_same_source_reselect_count`: sums across
  all sessions.
- `max_same_source_reselect_count` with `max_reselect_channel_id` /
  `max_reselect_guide_number`: identifies the worst-churning channel.

## Graceful Shutdown Lifecycle

Shutdown is orchestrated in `cmd/hdhriptv/main.go` and follows a strict
ordering to ensure in-flight work drains before dependencies are closed.

### Signal handling

The process listens for `SIGINT` and `SIGTERM` via
`signal.NotifyContext`. When received, the context is canceled and
shutdown begins.

### Shutdown sequence

1. HTTP servers call `Shutdown` with a 10 s timeout. If graceful shutdown
   times out, active connections are force-closed.
2. UDP discovery server is closed.
3. UPnP/SSDP server is closed (if enabled).
4. Background source prober is closed. This closes the prober session-close
   queue and blocks until queued probe session closes have drained.
5. Stream handler is closed via `CloseWithContext(shutdownCtx)`. This cancels
   all active shared sessions, waits for session goroutines to drain within
   the remaining 10 s shutdown deadline, releases tuner leases, and ensures
   async source-health persistence completes. If the deadline expires before
   full convergence, shutdown logs a warning and continues teardown.
6. Admin handler is closed. This closes the `closeCh` channel to signal
   background workers, acquires `dynamicSyncMu` and
   `dynamicBlockSyncMu` to establish a happens-before barrier with
   enqueue paths, then calls `workerWg.Wait()` to block until all
   background workers finish.
7. Main goroutine calls `wg.Wait()` to wait for listener goroutines.
8. Job runner is closed (deferred). This waits for in-flight `FinishRun`
   persistence.
9. Store is closed (deferred). This closes the SQLite connection.

### AdminHandler Close pattern

The `AdminHandler` uses a `closeCh` / `workerWg` pattern for background
worker lifecycle:

- `closeCh` is a `chan struct{}` closed exactly once via `closeOnce.Do`.
  Background workers derive their context from this channel via
  `workerContext()`.
- `workerWg` tracks active background goroutines (dynamic channel sync,
  dynamic block sync). Each enqueue path calls `workerWg.Add(1)` under its
  respective mutex, and each worker defers `workerWg.Done()`.
- `Close()` closes the channel, then acquires both sync mutexes to
  establish a happens-before barrier — after the lock/unlock pairs, no new
  `Add(1)` can occur because enqueue methods check `closeCh` under the
  mutex.
- `Close()` then calls `workerWg.Wait()` to block until all background
  workers finish.

### Ordering constraints

- **Stream handler must close before admin handler**: Stream sessions may
  interact with channel/source state that admin sync workers also touch.
- **Admin handler must close before store**: Background sync workers
  perform database operations.
- **Job runner must close before store**: The runner's `FinishRun` writes
  terminal job state to the database.
- **Store closes last**: All subsystems that perform database I/O must drain
  before `store.Close()`.

## Log Format and Aggregation

All log output uses `slog.TextHandler`, which produces `key=value` pairs:

```
time=2026-02-17T10:30:15.123Z level=INFO msg="shared session created" channel_id=42 guide_number=101
```

### Implications for log aggregation

- **Not JSON**: Pipelines expecting JSON-structured logs will need a
  `key=value` parser (e.g., logfmt).
- **Consistent field names**: Correlation fields like `channel_id`,
  `guide_number`, `tuner_id`, `source_id`, `subscriber_id`, `run_id`,
  `job_name` appear consistently across related events.
- **Level field**: Always present as `level=DEBUG|INFO|WARN|ERROR`.
- **Timestamp**: Always present as `time=` in RFC 3339 format.
- **Multi-stream**: stdout carries `DEBUG`/`INFO`; stderr carries
  `WARN`/`ERROR`. The log file respects the configured log level. When
  aggregating from containers, prefer the log file or merge both streams.

## Disk and Resource Considerations

### Log files

Each process startup creates a new log file. Over time, log files
accumulate without automatic cleanup. In long-running deployments, configure
external rotation or periodic deletion of old `hdhriptv-*.log` files.

### SQLite database

The database at `DB_PATH` uses WAL mode. The main `.db` file, `-wal`, and
`-shm` files should all reside on the same filesystem. Backup procedures
should capture all three files (or stop the service first for a clean
single-file backup).

### Session history memory

The in-memory session history retains up to 256 entries per process
lifetime. Each entry includes source and subscriber timelines, but per-session
timeline limits (`source_history_limit` / `subscriber_history_limit`) prevent
active sessions from growing those nested arrays without bound. If trimming
occurs, `source_history_truncated_count` and
`subscriber_history_truncated_count` expose how many oldest per-session rows
were evicted.

### Recovery filler processes

`slate_av` recovery filler spawns an ffmpeg subprocess during each recovery
window. In deployments with many concurrent recovering sessions, this means
multiple ffmpeg processes may run simultaneously, each producing realtime
MPEG-TS output. The keepalive guardrail (2.5x expected bitrate for 1.5 s)
limits runaway output, and the fallback chain (`slate_av` -> `psi` ->
`null`) provides graceful degradation if ffmpeg fails.

When `RECOVERY_FILLER_MODE=slate_av`, ffmpeg drawtext rendering expects a
Sans-compatible font. The project Docker image installs `ttf-dejavu` to
provide that dependency. For custom images/hosts, install `ttf-dejavu` (or an
equivalent Sans font package) to avoid fallback failures like `Cannot find a
valid font for the family Sans`.
