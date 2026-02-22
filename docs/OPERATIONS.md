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

Transient error signatures treated as retryable during startup sync:

- response status `5xx` (server-side errors)
- `connection refused`
- `connection reset`
- `network is unreachable`
- `broken pipe`
- `deadline exceeded`
- `timed out`
- `timeout`
- `temporarily unavailable`
- `no route to host`
- `eof`

Useful fields:

- `attempt` on `completed` phase events (`initial_sync_phase=completed`).
- `attempt`, `max_attempts`, and `backoff` on retry warnings (`msg="initial playlist sync transient jellyfin lineup reload failure; retrying"`). Retry warnings are separate log records and do not carry an `initial_sync_phase` value.
- `duration` on both `completed` and `failed` phase events, including readiness-gate failures.
- `error` on `failed` events.

## Public Mirror Publish Runbook

`make publish-github` mirrors internal `main` to the public repo as squash
commits. It is intended for controlled release publishing, not day-to-day
development pushes.

### Inputs and variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `INTERNAL_REMOTE` | `origin` | Internal source-of-truth remote. |
| `INTERNAL_REPO_URL` | `git@gitlab.lan:arodd/hdhriptv.git` | Canonical URL enforced for `INTERNAL_REMOTE`. |
| `PUBLIC_REMOTE` | `github` | Public publishing remote. |
| `PUBLIC_REPO_URL` | `git@github.com:arodd/hdhriptv.git` | Canonical URL enforced for `PUBLIC_REMOTE`. |
| `SYNC_BRANCH` | `main` | Branch to publish. |
| `PUBLIC_SYNC_TAG` | `public-sync/latest` | Marker tag on internal remote tracking last published internal commit. |
| `PUBLISH_GITHUB_COMMIT_MESSAGE` | empty | Optional custom squash commit subject. |

### Squash-sync lifecycle

1. Fetch internal/public refs and verify local `SYNC_BRANCH` exactly matches
   `INTERNAL_REMOTE/SYNC_BRANCH`.
2. Read `PUBLIC_SYNC_TAG` from the internal remote to determine the last
   published internal commit.
3. If marker tag equals current internal tip, exit with no-op.
4. If marker tag is present but not an ancestor of current internal tip,
   fail safe and require operator intervention.
5. If `PUBLIC_REMOTE/SYNC_BRANCH` tree already equals internal tip tree, update
   only `PUBLIC_SYNC_TAG` on internal remote (recovery/idempotency path).
6. Otherwise create a squash commit from the internal tip tree:
   - initial publish: commit has no parent,
   - incremental publish: commit parents the current public tip.
7. Push squash commit to `PUBLIC_REMOTE/SYNC_BRANCH`.
8. Move and push `PUBLIC_SYNC_TAG` on internal remote to the published internal
   tip.

### Recovery procedures

- **Local/internal divergence rejection**
  - Symptom: `Refusing to publish: local main is out of sync...`
  - Action: reconcile local checkout with internal remote (`pull --ff-only` or
    reset to internal tip as appropriate), then rerun publish.
- **Marker ancestor violation**
  - Symptom: marker tag is not an ancestor of internal tip.
  - Action: audit marker history and fix the tag manually on internal remote
    before rerunning.
- **Public push succeeded, marker push failed**
  - Symptom: publish command fails during marker tag push after public branch
    moved.
  - Action: rerun `make publish-github` after fixing internal remote/tag push
    permissions. The command detects tree equality and updates marker only,
    avoiding duplicate public squash commits.

## GitHub Binary Release + GitLab Tag Runbook

`make release-github-sync-tag` is the release path when binaries are published
on GitHub, while GitLab only receives a matching release tag (no GitLab release
object or assets).

### Inputs and variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `RELEASE_TAG` | empty (required) | Release tag name (`vX.Y.Z`). |
| `RELEASE_TITLE` | empty | GitHub release title; defaults to `RELEASE_TAG`. |
| `RELEASE_NOTES_FILE` | empty | Optional notes file for GitHub release create/edit. |
| `RELEASE_DIST_DIR` | `dist` | Output directory for built binaries and checksums. |
| `GITHUB_RELEASE_REPO` | empty | Optional `owner/repo` override for release publishing. |
| `INTERNAL_REMOTE` | `origin` | Internal source-of-truth remote (GitLab). |
| `INTERNAL_REPO_URL` | `git@gitlab.lan:arodd/hdhriptv.git` | Canonical URL for `INTERNAL_REMOTE`. |
| `PUBLIC_REMOTE` | `github` | Public publishing remote (GitHub). |
| `PUBLIC_REPO_URL` | `git@github.com:arodd/hdhriptv.git` | Canonical URL for `PUBLIC_REMOTE`. |
| `SYNC_BRANCH` | `main` | Branch to mirror from internal to public before release tagging. |
| `PUBLISH_GITHUB_COMMIT_MESSAGE` | empty | Optional squash commit subject override used by `make publish-github`. |

### Lifecycle

1. Verify clean tracked working tree and local/internal branch parity.
2. Build release binaries and checksum file:
   - `dist/hdhriptv-linux-amd64`
   - `dist/hdhriptv-linux-arm64`
   - `dist/SHA256SUMS`
3. Run `make publish-github` to synchronize public mirror commit.
4. Verify internal/public branch tree hashes match.
5. Push `RELEASE_TAG` to internal remote at internal tip and to public remote
   at public tip.
6. Create/update GitHub release and upload binaries/checksums.

### Recovery procedures

- **Remote tag mismatch (safe-stop)**
  - Symptom: command reports existing `RELEASE_TAG` on a remote points to a
    different commit than expected.
  - Action: inspect release history, then retag manually (or choose a new
    release tag) before rerunning.
- **Release partially published to GitHub**
  - Symptom: release exists but assets are incomplete/outdated.
  - Action: rerun `make release-github-sync-tag`; asset upload uses clobber and
    converges to current local build outputs.
- **Public mirror sync failure**
  - Symptom: underlying `make publish-github` step fails.
  - Action: resolve mirror/marker issue using the publish runbook above, then
    rerun the release command.

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

## Monitoring and Alerting Guidance

Metrics are most useful when tied to clear operator actions. The baseline
below gives a practical starting point for dashboards and alerts; tune values
to your traffic profile after collecting at least several days of baseline.

### Recommended dashboard panels

| Panel | Example query | Why it matters |
|-------|---------------|----------------|
| Source read-pause rate by reason | `sum by (reason) (rate(stream_source_read_pause_events_total[5m]))` | Distinguishes upstream starvation (`recovered`) from expected shutdown/cancel churn (`ctx_cancel`, `pump_exit`). |
| Source read-pause duration p95 | `histogram_quantile(0.95, sum by (le, reason) (rate(stream_source_read_pause_seconds_bucket[5m])))` | Shows whether pauses are brief blips or sustained stalls likely to drain DVR client buffers. |
| Slow-skip event rate | `sum(rate(stream_slow_skip_events_total[5m]))` | Indicates subscribers repeatedly falling behind the publish window. |
| Write-pressure timeout/short-write rate | `sum(rate(stream_subscriber_write_deadline_timeouts_total[5m]))` and `sum(rate(stream_subscriber_write_short_writes_total[5m]))` | Detects downstream client/network write pressure before widespread disconnects. |
| Bounded-close timeout/suppression | `sum(rate(stream_close_with_timeout_timeouts_total[5m]))` and `sum(rate(stream_close_with_timeout_suppressed_total[5m]))` | Surfaces shutdown/cleanup pressure and worker-budget saturation. |
| Late-abandoned / release-underflow counters | `increase(stream_close_with_timeout_late_abandoned_total[15m])`, `increase(stream_close_with_timeout_release_underflow_total[15m])` | Flags close-path invariants and potentially stuck close operations. |

### Suggested alert thresholds

| Severity | Condition | Action |
|----------|-----------|--------|
| Page | `increase(stream_close_with_timeout_late_abandoned_total[10m]) > 0` | Investigate immediately: at least one close stayed blocked past abandon window; correlate with `shared session slate AV close error` logs and stream/tuner churn. |
| Page | `increase(stream_close_with_timeout_release_underflow_total[10m]) > 0` | Treat as invariant violation; inspect recent close suppression/timeout logs and deploy health before user impact expands. |
| Warning | `sum(rate(stream_source_read_pause_events_total{reason="recovered"}[5m])) > 0.1` for `15m` | Upstream starvation is recurring; inspect provider/source health and failover behavior. |
| Warning | `sum(rate(stream_subscriber_write_deadline_timeouts_total[5m])) > 0` for `10m` | Downstream write pressure is active; review subscriber network paths and lag policy settings. |
| Warning | `sum(rate(stream_slow_skip_events_total[5m])) > 0` for `10m` | Slow-client lag compensation is frequently engaged; verify buffer and subscriber lag settings. |

### Source read-pause metric migration note

`stream_source_read_pause_events_total` and
`stream_source_read_pause_seconds` are now reason-labeled vectors.

- For total event rate across all reasons, use:
  `sum(rate(stream_source_read_pause_events_total[5m]))`
- For per-reason breakdown, use:
  `sum by (reason) (rate(stream_source_read_pause_events_total[5m]))`
- For duration percentiles by reason, aggregate histogram buckets by both
  `le` and `reason` before `histogram_quantile(...)`.

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

## Shared Session Streaming Behavior

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

Key warn-level close-path events:

- `closeWithTimeout worker slot release underflow`: internal close worker-slot accounting invariant warning. Correlate `close_release_underflow` with `close_timeouts`, `close_late_completions`, and `close_late_abandoned`.
- `closeWithTimeout suppression observed`: close retry suppression under worker-budget pressure. Correlate suppression reason/counters with retry queue depth and close timeout churn.
- `shared session slate AV close error`: bounded close failure while shutting down recovery filler readers; inspect `close_error_type` and accompanying `close_*` counters.

See `docs/STREAMING.md` bounded-close telemetry guidance for detailed triage and remediation.

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

### Backup strategies

Use one of the two supported approaches:

1. Cold backup (simplest and safest):
   - stop the service cleanly
   - copy the SQLite DB file to backup storage
   - start the service
2. Online backup (service remains running):
   - use SQLite's online backup API (for example via `sqlite3` CLI `.backup`)
   - example:

```bash
sqlite3 /data/hdhr-iptv.db ".backup '/backups/hdhr-iptv-$(date +%Y%m%d-%H%M%S).db'"
```

Prefer the online backup API for live systems. Avoid plain file copies of a
busy database unless you are using a filesystem snapshot mechanism that can
capture all related SQLite files atomically.

### WAL and atomicity notes

- hdhriptv runs SQLite with WAL mode enabled in normal operation.
- With WAL mode, recently committed data may reside in `hdhr-iptv.db-wal`
  until checkpointed; copying only `hdhr-iptv.db` while writes continue can
  produce incomplete backups.
- The SQLite backup API reads a transaction-consistent view and safely captures
  WAL-backed state.
- If you must restore from filesystem-level copies, keep `.db`, `.db-wal`, and
  `.db-shm` files from the same snapshot together.

### Restore procedure

1. Stop the service.
2. Restore the backup DB file to `DB_PATH`.
3. Remove stale sidecars from previous runs if present (`*.db-wal`, `*.db-shm`)
   unless you intentionally restored matching sidecar files from the same backup set.
4. Start the service.
5. Run integrity checks:

```bash
sqlite3 /data/hdhr-iptv.db "PRAGMA integrity_check;"
sqlite3 /data/hdhr-iptv.db "PRAGMA foreign_key_check;"
```

`integrity_check` should return `ok`; `foreign_key_check` should return no rows.

### Backup frequency guidance

- Choose recovery point objective (RPO) based on tolerance for channel/source
  metadata loss between backups.
- As a practical baseline, run backups at least twice as often as the playlist
  refresh schedule (for example, every `15m` if playlist refresh runs every `30m`).
- Increase frequency when performing bulk channel/source edits or automation
  rollouts that mutate mapping state rapidly.

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
