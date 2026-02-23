# Automation

This document describes the job runner, scheduled jobs, and reconciliation
subsystem that keep channel data current without manual intervention.

## Overview

The automation layer has three job types, each registered under a constant name:

| Job Name           | Constant                    | Default Cron    | Default Enabled |
|--------------------|-----------------------------|-----------------|-----------------|
| `playlist_sync`    | `jobs.JobPlaylistSync`      | `*/30 * * * *`  | yes             |
| `auto_prioritize`  | `jobs.JobAutoPrioritize`    | `30 3 * * *`    | no              |
| `dvr_lineup_sync`  | `jobs.JobDVRLineupSync`     | `*/30 * * * *`  | no              |

Runner-managed jobs are triggered two ways:

- **Schedule** (`triggered_by = "schedule"`) — fired by the cron scheduler.
- **Manual** (`triggered_by = "manual"`) — used by automation run endpoints for
  `playlist_sync` and `auto_prioritize`.

`dvr_lineup_sync` is currently started by the scheduler callback path; manual
DVR sync uses `/api/admin/dvr/sync` and does not create a `job_runs` row.

Source files:

- `internal/jobs/runner.go` — run lifecycle, locking, panic recovery
- `internal/jobs/store.go` — `Run` model and `Store` interface
- `internal/jobs/context.go` — `RunMetadata` context propagation
- `internal/jobs/metrics.go` — `StreamMetric` cache model
- `internal/jobs/playlist_sync.go` — playlist sync job
- `internal/jobs/auto_prioritize.go` — auto-prioritize job
- `internal/scheduler/scheduler.go` — cron engine wrapper
- `internal/reconcile/reconcile.go` — channel source reconciliation
- `internal/analyzer/ffmpeg.go` — ffprobe/ffmpeg stream analyzer

## Job Runner

`jobs.Runner` (`internal/jobs/runner.go`) coordinates asynchronous job
execution with a global overlap lock and persisted run state.

### Global Lock

By default `globalLock` is `true`. When enabled, `tryLock` rejects any new
job start if **any** other job is already running (not just the same job name).
This means at most one job runs at a time across all job types. The lock can
be toggled via `SetGlobalLock(enabled)`.

Per-name locking is always active: two concurrent runs of the same job name
are never allowed regardless of the global lock setting.

### Start and Execution

`Runner.Start(ctx, jobName, triggeredBy, fn)` performs:

1. `tryLock(jobName)` — acquires per-name and optional global lock under `mu`.
   On success, increments `wg` to track the goroutine.
2. `store.CreateRun(...)` — persists a new run row with `status = "running"`.
3. Creates a cancellable context via `context.WithCancelCause`.
4. Launches `go r.run(...)` which calls `fn(ctx, runCtx)`.

### RunContext

`RunContext` is the handle passed to every `JobFunc`. It exposes:

- `RunID() int64` — persisted run identifier.
- `SetProgress(ctx, cur, max)` — persist progress counters to the store.
- `IncrementProgress(ctx, delta)` — convenience for cur += delta.
- `SetSummary(ctx, summary)` — persist a summary string.
- `Snapshot() (cur, max, summary)` — read in-memory progress without I/O.

Progress updates are guarded by a `finalizing` flag: once the deferred
cleanup begins, further updates return `ErrRunFinalized`.

### Run Metadata Context

`WithRunMetadata(ctx, runID, jobName, triggeredBy)` attaches a `RunMetadata`
value to the context, allowing downstream code to correlate log entries and
store operations back to the originating job run via
`RunMetadataFromContext(ctx)`.

### Panic Recovery and Shutdown

The deferred block in `run()`:

1. Recovers panics — sets `status = "error"` with the panic message.
2. Calls `beginFinalization()` on `RunContext` — blocks further progress
   updates and captures final `(cur, max, summary)`.
3. Releases the run cancel and per-name lock **before** persisting terminal
   state so that `IsRunning()` cannot outlive a non-running persisted status.
4. Calls `store.FinishRun(...)` with the final status, error text, summary,
   and finished timestamp.

`Runner.Close()` prevents new runs (`closed = true`), cancels all active
runs with `ErrRunnerClosed`, and blocks on `wg.Wait()` until every in-flight
goroutine finishes its `FinishRun` persistence.

### Run Statuses

Defined in `internal/jobs/store.go`:

| Status       | Meaning                                           |
|--------------|---------------------------------------------------|
| `running`    | Currently executing                               |
| `success`    | Completed without error                           |
| `error`      | Failed (including recovered panics)               |
| `canceled`   | Context was cancelled or deadline exceeded        |

### Store Interface

The `Store` interface (`internal/jobs/store.go`) defines persistence:

```go
type Store interface {
    CreateRun(ctx, jobName, triggeredBy, startedAt) (int64, error)
    UpdateRunProgress(ctx, runID, progressCur, progressMax, summary) error
    FinishRun(ctx, runID, status, errText, summary, finishedAt) error
    GetRun(ctx, runID) (Run, error)
    ListRuns(ctx, jobName, limit, offset) ([]Run, error)
}
```

The `Run` struct includes `ProgressCur`, `ProgressMax`, `Summary`, and
optional `AnalysisErrorBuckets` parsed from auto-prioritize summary text.

## Playlist Sync Job

`PlaylistSyncJob` (`internal/jobs/playlist_sync.go`) refreshes the channel
catalog from the upstream M3U playlist and reconciles channel sources.

### Pipeline

1. **Read playlist URL** — from settings store key `playlist.url`.
   Fails immediately if not configured.
2. **Refresh** — calls `PlaylistRefresher.Refresh(ctx, url)` which fetches,
   parses, and upserts catalog entries. Returns the count of refreshed items.
3. **Reconcile** — calls `PlaylistReconciler.Reconcile(ctx, onProgress)`.
   Progress is reported per-channel with throttled persistence (every 5
   channels or every 1 second, whichever comes first).
4. **DVR lineup reload** (optional) — if a `DVRLineupReloader` is configured
   via `SetPostSyncLineupReloader`, it is called after successful
   refresh+reconcile through `ReloadLineupForPlaylistSyncOutcome`, which
   returns typed reload status/skip metadata (`dvr.ReloadOutcome`).

### Progress Throttling

Both playlist sync and auto-prioritize use `progressPersistThrottle` to
reduce database write pressure. The throttle fires when either condition
is met:

- At least `persistEvery` channels have been processed since the last write.
- At least `persistInterval` has elapsed since the last write.
- The progress counter has reached `progressMax` (always persist on completion).

For playlist sync: `persistEvery = 5`, `persistInterval = 1s`.

### Summary Format

On completion the run summary is set to a key=value string:

```
playlist refreshed items=N; channels processed=X/Y; added_sources=A;
existing_sources=E; dynamic_blocks=B enabled=E added=A updated=U
retained=R removed=R truncated=T; dynamic_channels=C; dynamic_added=A;
dynamic_removed=R; dvr_lineup_reloaded=bool; dvr_lineup_reload_status=S;
dvr_lineup_reload_skip_reason=R
```

## Auto-Prioritize Job

`AutoPrioritizeJob` (`internal/jobs/auto_prioritize.go`) probes stream
sources with ffprobe, scores them by quality, and reorders each channel's
source list so the highest-quality source is tried first during streaming.

### Pipeline

1. **Resolve analysis scope** — reads settings:
   - `analyzer.autoprioritize.enabled_only` (default `true`) — only analyze
     enabled sources.
   - `analyzer.autoprioritize.top_n_per_channel` (default `0` = unlimited,
     max `100`) — limit analysis to the top N sources per channel by current
     sort order.

2. **Load channels and sources** — lists all enabled channels, bulk-loads
   sources via `ListSourcesByChannelIDs`. Falls back to per-channel
   `ListSources` if bulk load encounters mutation drift errors.

3. **Build analysis queue** — for each source, checks the metrics cache:
   - Cache hit (fresh) — reuse cached `StreamMetric`. Freshness thresholds:
     successful metrics are fresh for `SuccessFreshness` (default 24h),
     errored metrics retry after `ErrorRetry` (default 30min).
   - Cache miss or stale — queue an `analysisTask` for probing.
   - Empty stream URL — record error metric immediately, skip probing.

4. **Resolve worker count** — determines concurrent probe workers:
   - `"fixed"` mode: uses `FixedWorkers` (1–64), capped by available tuner
     slots when tuner capacity is configured.
   - `"auto"` mode (default): uses `TunerCount - InUseCount` when tuner
     capacity is configured, otherwise falls back to `DefaultWorkers`
     (default 4, max 64).
   - If the resolved worker count (after applying tuner/mode constraints)
     is zero and tasks exist, the job fails with a descriptive error.

5. **Analyze pending** — runs a concurrent worker pool:
   - Workers read from a shared `taskCh` channel.
   - Each worker acquires a probe lease from `TunerUsage.AcquireProbe` when
     tuner-aware mode is active.
   - On `stream.ErrNoTunersAvailable`, performs up to 3 probe-slot acquire
     attempts with exponential backoff. Delays start at 250 ms and double per
     attempt, capped at 2 s.
   - On HTTP 429 errors, retries once after `HTTP429Backoff` (default 60s).
   - On `ErrProbePreempted`, sets a fatal error and cancels all workers.
   - After each completed non-retried probe attempt with a tuner lease, waits
     `ProbeTuneDelay` before processing the next task.

6. **Cache and persist** — upserts analyzed `StreamMetric` entries and
   updates `SourceProfileUpdate` on each source record associated with that item key
   (last probe timestamp, resolution, FPS, codecs, bitrate).

7. **Score and reorder** — for each channel, calls `orderSourcesByScore`:
   - Computes a quality score and applies a health penalty.
   - Reorders sources via `ReorderSources` if the order changed.
   - Tolerates mutation drift errors (channel/source not found, source set
     drift) by recording skip telemetry instead of failing.

### Scoring Algorithm

Quality score for each source is the sum of three normalized components:

```
qualityScore = resNorm + fpsNorm + brNorm
```

Where:
- `resNorm = (width * height) / maxResolution` — resolution normalized
  against the highest resolution source in the channel.
- `fpsNorm = fps / maxFPS` — frame rate normalized against the highest FPS
  in the channel.
- `brNorm = bitrate / maxBitrate` — bitrate normalized against the highest
  bitrate in the channel. Uses `BitrateBPS`; falls back to `VariantBPS`
  when `BitrateBPS` is zero.

A source is scorable only when `Error` is empty and `Width > 0`,
`Height > 0`, and `FPS > 0`.

### Health Penalty

The final score is reduced by a health penalty based on recent streaming
failures:

```
finalScore = qualityScore - healthPenalty
```

Health penalty components (cumulative, capped at 2.95):

| Condition                              | Penalty |
|----------------------------------------|---------|
| `FailCount == 1`                       | +0.40   |
| `FailCount == 2`                       | +0.85   |
| `FailCount == 3`                       | +1.30   |
| `FailCount >= 4`                       | +1.90   |
| `CooldownUntil > now`                  | +0.90   |
| `LastFailAt` within 5 min              | +0.75   |
| `LastFailAt` within 30 min             | +0.50   |
| `LastFailAt` within 2 hours            | +0.25   |
| `LastFailAt` older than 2 hours        | +0.10   |
| `LastFailAt > LastOKAt` (last was fail)| +0.35   |

Sources with `FailCount >= 2` are forcibly demoted (sorted after all
non-demoted sources regardless of score). No penalty is applied when
`FailCount == 0` and `LastFailAt <= LastOKAt` (the source has recovered).

### Sort Tiebreaking

After scoring, sources are sorted by:

1. Non-demoted sources before demoted sources.
2. Higher score first (within 1e-9 tolerance).
3. More recent `LastOKAt` first.
4. Lower `SourceID` first (stable tiebreak).

Disabled sources are appended after all enabled sources.

### StreamMetric Cache

`StreamMetric` (`internal/jobs/metrics.go`) stores cached probe results:

| Field        | Type    | Description                            |
|--------------|---------|----------------------------------------|
| `ItemKey`    | string  | Catalog source identifier              |
| `AnalyzedAt` | int64   | Unix timestamp of last analysis        |
| `Width`      | int     | Video width in pixels                  |
| `Height`     | int     | Video height in pixels                 |
| `FPS`        | float64 | Frames per second                      |
| `VideoCodec` | string  | e.g. "h264", "hevc"                    |
| `AudioCodec` | string  | e.g. "aac", "mp3"                      |
| `BitrateBPS` | int64   | Measured or metadata bitrate           |
| `VariantBPS` | int64   | HLS variant bitrate from stream tags   |
| `ScoreHint`  | float64 | Reserved quality score hint (field exists but is not currently populated by the analyzer pipeline) |
| `Error`      | string  | Probe error message (empty on success) |

Default freshness: `DefaultMetricsFreshness = 24h`,
`DefaultErrorRetry = 30min`.

### Analysis Error Classification

Probe errors are bucketed for summary reporting:

| Bucket                    | Match Pattern                         |
|---------------------------|---------------------------------------|
| `http_NNN`                | "server returned NNN" (regex)         |
| `decode_ffprobe_json`     | "decode ffprobe json"                 |
| `ffprobe_no_video_streams`| "ffprobe returned no video streams"   |
| `stream_url_empty`        | "stream url is empty"                 |
| `probe_slot_unavailable`  | "probe slot unavailable"              |
| `timeout`                 | "deadline exceeded" / "timed out" / "timeout" |
| `dns_no_such_host`        | "no such host"                        |
| `connection_refused`      | "connection refused"                  |
| `ffmpeg_sample_failed`    | "ffmpeg sample failed"                |
| `ffprobe_failed`          | "ffprobe failed"                      |
| `other`                   | Everything else                       |

### Skip Reason Classification

When auto-prioritize encounters mutation drift (channels or sources
modified concurrently), it records a skip reason instead of failing:

| Bucket                                   | Condition                                  |
|------------------------------------------|--------------------------------------------|
| `source_load_channel_not_found`          | Channel deleted between list and load      |
| `source_load_source_not_found`           | Source deleted between list and load       |
| `source_load_source_set_drift`           | Source set changed during bulk load        |
| `reorder_channel_not_found`              | Channel deleted before reorder             |
| `reorder_source_not_found`               | Source deleted before reorder              |
| `reorder_source_set_drift`               | Source set changed between score and reorder |

### Auto-Prioritize Summary Format

On completion the run summary is set to a key=value string:

```
channels=N analyzed=N cache_hits=N reordered=N skipped_channels=N
analysis_errors=N analysis_error_buckets=bucket1:N,bucket2:N
skip_reason_buckets=bucket1:N,bucket2:N enabled_only=bool
top_n_per_channel=N limited_channels=N
```

### Playlist Types Reference

The playlist subsystem types used by catalog refresh are documented in
[CATALOG-PIPELINE.md](CATALOG-PIPELINE.md) under "Playlist Management":
`Item`, `ItemStream`, `Group`, and `Query`.

## Stream Analyzer

`analyzer.FFmpegAnalyzer` (`internal/analyzer/ffmpeg.go`) wraps ffprobe and
optionally ffmpeg to extract stream quality metrics.

### Configuration

| Field              | Default        | Description                        |
|--------------------|----------------|------------------------------------|
| `FFprobePath`      | `"ffprobe"`    | Path to ffprobe binary             |
| `FFmpegPath`       | `"ffmpeg"`     | Path to ffmpeg binary              |
| `ProbeTimeout`     | 7s             | Per-probe context timeout          |
| `AnalyzeDurationUS`| 1,500,000      | ffprobe `-analyzeduration` (us)    |
| `ProbeSizeBytes`   | 1,000,000      | ffprobe `-probesize` (bytes)       |
| `BitrateMode`      | `"metadata_then_sample"` | Bitrate measurement strategy |
| `SampleSeconds`    | 3              | ffmpeg sample duration (seconds)   |

### Bitrate Modes

- `metadata` — use only ffprobe metadata (stream/format/variant bitrate).
- `sample` — always run an ffmpeg sample to measure bitrate.
- `metadata_then_sample` (default) — use metadata if positive; fall back to
  ffmpeg sample otherwise.

### Probe Flow

1. Run ffprobe with JSON output, requesting stream codec/resolution/framerate
   and format bitrate.
2. Parse the first video stream for `Width`, `Height`, `FPS`, `VideoCodec`.
   Extract the first audio stream's `AudioCodec`.
3. Bitrate priority: `stream.bit_rate` > `format.bit_rate` > `stream.tags.variant_bitrate`.
4. If bitrate mode requires sampling, run ffmpeg for `SampleSeconds`,
   capture output byte count, and compute `bitrate = (bytes * 8) / elapsed`.

## Scheduler

`scheduler.Service` (`internal/scheduler/scheduler.go`) wraps
`github.com/robfig/cron/v3` to provide settings-backed cron scheduling with
hot-reload.

### Cron Parser

The parser supports the optional-seconds 6-field format plus descriptors:

```
Second (optional) | Minute | Hour | Dom | Month | Dow | Descriptor
```

This allows both standard 5-field specs (`*/30 * * * *`) and 6-field specs
with a leading seconds field (`0 */30 * * * *`).

### Timezone

Default timezone is `America/Chicago`. Configurable via setting
`jobs.timezone`. On invalid timezone, falls back to UTC with a warning.

The timezone affects when cron expressions fire. It can be updated at runtime
via `UpdateTimezone(ctx, timezone)` which persists the setting and reloads
all schedules.

### Settings Keys

| Setting Key                     | Default         | Description               |
|---------------------------------|-----------------|---------------------------|
| `jobs.timezone`                 | America/Chicago | Scheduler timezone        |
| `jobs.playlist_sync.enabled`    | true            | Enable playlist sync cron |
| `jobs.playlist_sync.cron`       | `*/30 * * * *`  | Playlist sync schedule    |
| `jobs.auto_prioritize.enabled`  | false           | Enable auto-prioritize    |
| `jobs.auto_prioritize.cron`     | `30 3 * * *`    | Auto-prioritize schedule  |
| `jobs.dvr_lineup_sync.enabled`  | false           | Enable DVR lineup sync    |
| `jobs.dvr_lineup_sync.cron`     | `*/30 * * * *`  | DVR lineup sync schedule  |

### Hot-Reload

`LoadFromSettings(ctx)` rebuilds the entire cron engine:

1. Reads timezone and per-job enabled/cron settings from the store.
2. Validates cron specs for enabled jobs.
3. Creates a new `cron.Cron` engine with the resolved timezone.
4. Registers all enabled jobs with their callbacks.
5. Stops the old engine (waits for completion), swaps in the new engine.
6. If the scheduler was previously started, starts the new engine.

`UpdateJobSchedule(ctx, jobName, enabled, cronSpec)` provides single-job
hot-update: validates, persists to settings, removes the old cron entry,
and adds the new one — all under the lifecycle mutex.

### Lifecycle

- `RegisterJob(jobName, callback)` — registers a callback before starting.
- `Start()` — starts the cron engine.
- `Stop()` — cancels the run context, stops the engine, returns a done context.
- `ListSchedules(ctx)` — returns all job schedules with their next-run times.
- `NextRun(jobName)` — returns the next scheduled fire time for one job.

## Reconciliation

`reconcile.Service` (`internal/reconcile/reconcile.go`) synchronizes
published channel source lists against the current catalog state.

### Static Channels

For channels without dynamic rules, reconciliation:

1. Lists active item keys from the catalog matching the channel's
   `ChannelKey`.
2. Lists existing sources on the channel.
3. Adds any catalog items not already present via `AddSource`.
4. Skips items that cause `ErrAssociationMismatch` (cross-channel conflicts).

### Dynamic Channels

Channels with `DynamicRule.Enabled = true` use catalog filter queries:

1. **Dynamic channel block sync** — `SyncDynamicChannelBlocks` materializes
   block-level channel additions/removals/updates. Truncation is logged when
   matches exceed `DynamicGuideBlockMaxLen`.

2. **Per-channel dynamic source sync** — for each reconcilable dynamic
   channel:
   - Builds a `dynamicCatalogFilterKey` from the rule's group names, search
     query, and regex flag.
   - **Paged mode** (when enabled and the rule is used by only one channel):
     delegates to `SyncDynamicSourcesByCatalogFilter` which iterates the
     catalog in pages of 512 items.
   - **Shared-rule cache mode** (when multiple channels share the same
     dynamic rule): caches the full item key list and reuses it for each
     channel to avoid repeated full catalog scans.
   - **Legacy mode** (paged mode disabled): fetches all matching item keys
     via `ListActiveItemKeysByCatalogFilter`, then calls
     `SyncDynamicSources`.

3. Match limit enforcement — if the matched item count exceeds
   `dynamicRuleMatchLimit` (default `DynamicGuideBlockMaxLen`), the
   reconciliation fails with an error for that channel. This prevents
   overly broad rules from creating excessive source associations.

### Reconciliation Result

The `Result` struct tracks:

| Field                      | Description                                |
|----------------------------|--------------------------------------------|
| `ChannelsTotal`            | Total channel count                        |
| `ChannelsProcessed`        | Channels successfully reconciled           |
| `ChannelsSkipped`          | Non-reconcilable or conflict-skipped       |
| `SourcesAdded`             | New source associations created            |
| `SourcesAlreadySeen`       | Existing sources unchanged                 |
| `DynamicBlocksProcessed`   | Dynamic block queries evaluated            |
| `DynamicBlocksEnabled`     | Enabled dynamic block queries              |
| `DynamicChannelsAdded`     | Channels added by dynamic blocks           |
| `DynamicChannelsUpdated`   | Channels updated by dynamic blocks         |
| `DynamicChannelsRetained`  | Channels unchanged by dynamic blocks       |
| `DynamicChannelsRemoved`   | Channels removed by dynamic blocks         |
| `DynamicChannelsTruncated` | Channels truncated at match cap            |
| `DynamicChannelsProcessed` | Dynamic-rule channels source-synced        |
| `DynamicSourcesAdded`      | Sources added via dynamic rules            |
| `DynamicSourcesRemoved`    | Sources removed via dynamic rules          |

## Configuration Reference

### Analyzer Settings

| Setting Key                                 | Type   | Default | Description                                |
|---------------------------------------------|--------|---------|--------------------------------------------|
| `analyzer.probe.timeout_ms`                 | int    | 7000    | ffprobe/ffmpeg per-probe timeout           |
| `analyzer.probe.analyzeduration_us`         | int64  | 1500000 | ffprobe `-analyzeduration` value           |
| `analyzer.probe.probesize_bytes`            | int64  | 1000000 | ffprobe `-probesize` value                 |
| `analyzer.bitrate_mode`                     | string | `metadata_then_sample` | Bitrate strategy (`metadata`, `sample`, `metadata_then_sample`) |
| `analyzer.sample_seconds`                   | int    | 3       | ffmpeg sample duration when sampling bitrate |
| `analyzer.autoprioritize.enabled_only`      | bool   | true    | Only analyze enabled sources               |
| `analyzer.autoprioritize.top_n_per_channel` | int    | 0       | Limit sources per channel (0 = all, max 100) |

### Auto-Prioritize Options

| Option             | Default   | Description                                    |
|--------------------|-----------|------------------------------------------------|
| `SuccessFreshness` | 24h       | How long a successful probe stays cached       |
| `ErrorRetry`       | 30min     | How long before retrying a failed probe        |
| `DefaultWorkers`   | 4         | Worker count when tuner-unaware                |
| `WorkerMode`       | `"auto"`  | `"auto"` or `"fixed"`                          |
| `FixedWorkers`     | —         | Required when WorkerMode is `"fixed"`          |
| `TunerCount`       | 0         | Total tuner capacity (0 = tuner-unaware)       |
| `ProbeTuneDelay`   | 0         | Cooldown after releasing a probe lease         |
| `HTTP429Backoff`   | 60s       | Wait time after an HTTP 429 response           |

## Operational Patterns

### When to Run Each Job

- **Playlist sync** — run frequently (every 15–30 minutes) to keep the
  catalog current with upstream playlist changes. This is the only job
  enabled by default.

- **Auto-prioritize** — run during low-usage hours (default: 3:30 AM).
  Probing streams consumes tuner slots and network bandwidth. Running it
  overnight minimizes impact on active viewers. Consider enabling only if
  you have multiple sources per channel and want automatic quality-based
  ordering.

- **DVR lineup sync** — run at the same cadence as playlist sync when
  a DVR integration is configured. Keeps the DVR provider's channel
  lineup in sync with the published channel list. Note: dvr\_lineup\_sync
  scheduling is configured via the DVR admin UI and API, not through
  the Automation page (see [DVR-INTEGRATION.md](DVR-INTEGRATION.md)
  for configuration details).

### Scheduling Recommendations

- Set the timezone to your local timezone so cron expressions behave
  intuitively.
- For auto-prioritize with tuner-aware mode, ensure `TunerCount` matches
  your actual tuner configuration so the worker pool respects active stream
  capacity.
- After changing playlist URL or channel configuration, trigger a manual
  playlist sync to apply changes immediately rather than waiting for the
  next scheduled run.
- Monitor job run summaries for elevated error counts — a high
  `analysis_errors` count in auto-prioritize may indicate network issues
  or stale stream URLs.

### Troubleshooting

#### Global Job Lock Contention

The job runner uses a global overlap lock (`globalLock = true` by default),
meaning only one job can run at a time across all job types. When a job
start is rejected because another job is already running, the admin API
returns **HTTP 409 Conflict**.

To diagnose:

- Check `GET /api/admin/jobs?limit=5` for a run with `status: "running"`.
- Wait for the running job to finish before retrying. Scheduled triggers
  that collide with an in-flight job are skipped with a warning log event;
  no `job_runs` row is created for the skipped invocation, so it will not
  appear in job history.
- If a job appears stuck (running for an unexpectedly long time), check
  server logs for panics or deadlocks. The runner recovers from panics
  and marks the run as `"error"`, but a blocked network call may hang
  until its context deadline.

#### HTTP 429 Cooldown During Auto-Prioritize

Upstream IPTV providers may return HTTP 429 (Too Many Requests) when
probe workers hit rate limits. When a probe receives a 429 response,
the worker retries once after `HTTP429Backoff` (default 60 seconds).

To reduce 429 pressure:

- **Increase `ProbeTuneDelay`** — adds a cooldown (in milliseconds)
  after each probe releases its tuner lease, spacing out requests to
  the upstream provider. Even a small value (e.g. 500ms–2s) can
  significantly reduce rate-limit hits.
- **Lower the worker count** — in `"fixed"` worker mode, reduce
  `FixedWorkers` so fewer concurrent probes hit the provider. In
  `"auto"` mode, the worker count is already capped by available tuner
  slots.
- **Schedule during off-peak hours** — the default cron (`30 3 * * *`)
  targets low-usage periods. Avoid overlapping with times when other
  automation or clients are actively streaming.

#### Safe Scheduling Defaults Under Load

When playlist sync and auto-prioritize are both enabled, their schedules
should be staggered to avoid contention:

- **Playlist sync** runs frequently (every 15–30 minutes). Keep this
  as the higher-frequency job since it is lightweight.
- **Auto-prioritize** is resource-intensive (probes consume tuner slots
  and network bandwidth). Schedule it at most once or twice per day,
  ideally during off-peak hours.
- Because the global lock prevents overlap, a long-running auto-prioritize
  job will cause scheduled playlist syncs to be skipped until it
  finishes. Set playlist sync frequency no higher than needed to avoid
  excessive skipped runs.
- Be aware of provider session caps — if your IPTV provider limits
  concurrent connections, ensure `TunerCount` reflects this limit so
  auto-prioritize workers do not exhaust all available sessions and
  block live viewer streams.

## Job Idempotency Contracts

All jobs are safe to re-run at any time. The global lock prevents concurrent
execution, so a duplicate trigger returns `ErrAlreadyRunning` (HTTP 409)
rather than causing data corruption.

| Job                | Idempotent | Semantics                                                                 |
|--------------------|------------|---------------------------------------------------------------------------|
| `playlist_sync`    | Yes        | Upsert semantics on catalog refresh; reconcile adds missing sources only. Re-running produces the same catalog state for a given upstream playlist. |
| `auto_prioritize`  | Yes        | Overwrites quality scores and source order based on current probe results. Metrics cache uses upsert; source reorder is a full replacement of the ordering. |
| `dvr_lineup_sync`  | Yes        | Applies an idempotent patch to the DVR provider lineup. Re-running with no upstream changes produces no mutations.                                  |

## Partial Failure Semantics

The playlist sync pipeline executes four sequential stages. A failure at
any stage is terminal — subsequent stages are skipped.

| Stage | Operation                | On Failure                                                                                      |
|-------|--------------------------|--------------------------------------------------------------------------------------------------|
| 1     | Read playlist URL setting | Job errors immediately. No catalog or source changes.                                           |
| 2     | Refresh (fetch + upsert) | Items already upserted before the error persist in the catalog. Remaining items are not processed. |
| 3     | Reconcile sources        | Catalog is updated (from stage 2) but channel source mappings for un-processed channels are stale. |
| 4     | DVR lineup reload        | Catalog and sources are updated but the DVR provider is not notified. Reported as partial in the run summary (`dvr_lineup_reload_status`). |

The auto-prioritize job has analogous staged behavior: if analysis
completes but reordering fails for a specific channel, the failure is
classified as mutation drift and recorded in skip telemetry rather than
aborting the entire job. Fatal errors (e.g. `ErrProbePreempted`) cancel
all workers immediately.

## DST / Timezone Behavior

The scheduler uses `robfig/cron/v3` with `cron.WithLocation(location)`,
so all cron expressions are evaluated in the configured timezone.

**Default timezone:** `America/Chicago` (setting key: `jobs.timezone`).
Change it at runtime via `PUT /api/admin/automation` (include `"timezone": "..."` in the
JSON body), which persists the value and hot-reloads all schedules.

**Spring-forward example:** A cron spec of `0 2 * * *` in
`America/Chicago` during the spring DST transition (2:00 AM jumps to
3:00 AM) causes the job to fire at 3:00 AM CDT — the first wall-clock
instant at or after the skipped target time.

**Fall-back example:** During the fall-back transition (2:00 AM occurs
twice), the cron fires on the **first** occurrence of the target time
(i.e. before clocks are set back). It does not fire a second time.

If the configured timezone is invalid (e.g. typo in the setting value),
the scheduler logs a warning and falls back to UTC.
