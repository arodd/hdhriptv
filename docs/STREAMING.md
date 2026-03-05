# Streaming Pipeline Architecture

This document describes the MPEG-TS streaming pipeline that delivers live TV
channels from upstream sources to HTTP clients. All code lives under
`internal/stream/`.

## Overview

The streaming system implements a **shared session model**: one upstream
connection per channel is fanned out to multiple HTTP subscribers through an
in-memory ring buffer.

```
                            ┌──────────────┐
                            │   Upstream   │
                            │   Source     │
                            └──────┬───────┘
                                   │ MPEG-TS
                            ┌──────▼───────┐
                            │  Producer    │
                            │ (ffmpeg/     │
                            │  direct)     │
                            └──────┬───────┘
                                   │ io.ReadCloser
                            ┌──────▼───────┐
                            │    Pump      │
                            │ (chunk +     │
                            │  flush)      │
                            └──────┬───────┘
                                   │ PublishChunk()
                            ┌──────▼───────┐
                            │  ChunkRing   │
                            │ (bounded     │
                            │  ring buf)   │
                            └──┬───┬───┬───┘
                               │   │   │  ReadFrom() / WaitForChange()
                            ┌──▼┐ ┌▼─┐ ┌▼──┐
                            │S1 │ │S2│ │S3 │  HTTP Subscribers
                            └───┘ └──┘ └───┘
```

Key components:

| Component | File | Role |
|-----------|------|------|
| `Handler` | `handler.go` | HTTP endpoint; resolves guide number → channel → session |
| `Pool` | `tuners.go` | Finite tuner slot pool; acquire/release/preempt |
| `VirtualTunerManager` | `virtual_tuners.go` | Per-source pool routing; aggregate stats |
| `SessionManager` | `shared_session.go` | Per-channel session lifecycle coordinator |
| `sharedRuntimeSession` | `shared_session.go` | Active session: source, pump, ring, subscribers |
| `Pump` | `pump.go` | Reads producer output, publishes sized/timed chunks |
| `ChunkRing` | `ring.go` | Bounded in-memory ring buffer with sync.Cond waits |
| `FFmpegProducer` | `producer.go` | Standalone ffmpeg process for upstream ingestion |
| `BackgroundProber` | `prober.go` | Periodic source health checking |
| `tuneBackoffGate` | `tune_backoff.go` | Per-channel startup throttling |
| `recentSourceHealth` | `recent_health.go` | In-memory health overlay for source ordering |

---

## Tuner Pool

The `Pool` (`tuners.go`) manages a fixed number of concurrent tuner slots
using a buffered channel (`free chan int`). Each slot is identified by an
integer ID.

### Slot management

- `NewPool(count)` creates `count` slots numbered `0..count-1` and pushes them
  all into the `free` channel.
- `AcquireClient()` takes a slot for a client stream session.
- `AcquireProbe()` takes a slot for a background probe (with a cancel func
  that allows preemption).
- `Lease.Release()` returns the slot (once-guarded).

### Preemption

When no free slots exist and a client requests a tune:

1. **Probe preemption** — the oldest probe session is canceled via its
   `CancelCauseFunc` with `ErrProbePreempted`. The system waits up to
   `clientPreemptWait` (2 s) + settle delay for the slot to return.
2. **Idle-client preemption** — if no probes are available, the oldest idle
   client session (registered via `markClientPreemptible`) is preempted. The
   preempt function returns `true` if the session was truly idle and can be
   reclaimed.

### Settle delays

After a preemption-triggered release, the reclaimed slot observes a
configurable settle delay (`SetPreemptSettleDelay`) before it can be reused.
Two settle mechanisms exist:

- **Per-slot settle** (`settleNotBefore[id]`): delays reuse of the specific
  reclaimed slot.
- **Capacity-refill settle** (`fullSettleUntil`): delays the acquire that would
  bring the pool back to full capacity, preventing rapid slot churn when the
  pool transitions from full to one-below-full.

Both are enforced by `waitForSlotSettle` and `waitForCapacityRefillSettle`
during acquire.

---

## Virtual Tuner Manager (Multi-Source)

When multiple playlist sources are configured, a `VirtualTunerManager`
(`virtual_tuners.go`) replaces the single global `Pool` to provide
per-source tuner capacity isolation.

### Architecture

Each enabled playlist source with `tuner_count > 0` gets its own `Pool`
instance. The `VirtualTunerManager` wraps all per-source pools and presents
a unified `tunerUsage` interface to the stream handler:

```
VirtualTunerManager
├── Pool (Source 1: "Primary", 4 tuners, baseID=0)
├── Pool (Source 2: "Backup A", 2 tuners, baseID=4)
└── Pool (Source 3: "Backup B", 2 tuners, baseID=6)
```

Global tuner IDs are assigned contiguously across pools via `baseID` offsets
so each tuner has a unique global identifier.

### Source-Aware Lease Acquisition

- `AcquireClientForSource(ctx, sourceID, guideNumber, clientAddr)` routes
  the lease request to the correct per-source pool. Returns a `Lease` with
  `PlaylistSourceID`, `PlaylistSourceName`, and `VirtualTunerSlot`.
  - At exact source-pool capacity, client acquires are still attempted when
    the pool has reclaimable preemptible leases (probe or idle-client). This
    preserves the configured per-source cap while allowing like-for-like
    replacement via pool-local preemption.
- `AcquireProbeForSource(ctx, sourceID, label, cancel)` routes probe leases
  to the source's pool.
- When `sourceID=0`, leases are routed to the default source (source_id=1 if
  present, otherwise the first entry).

### Cross-Source Failover

During session recovery, when a failover candidate comes from a different
playlist source than the current session:

1. The current source's tuner lease is released.
2. A new lease is acquired from the failover target's source pool.
3. The session continues with the new source and pool assignment.

This implements virtual tuner migration without exposing new consumer
endpoints.

### Preemption Scope

Preemption rules are pool-local:

- Probe preemption only targets probes within the **same source pool**.
- Idle-client preemption only targets idle clients within the **same source
  pool**.
- Capacity from unrelated source pools is never stolen.
- During source candidate evaluation, a full pool with reclaimable
  preemptible leases is treated as startup-eligible for that source; a full
  pool without reclaimable preemptible leases is treated as unavailable.

### Metrics

`stream_virtual_tuner_utilization_ratio` (Prometheus gauge, label
`playlist_source`): tracks the in-use/capacity ratio per source pool.
Updated on every lease acquisition and release.

### Lifecycle

`VirtualTunerManager.Close()` drains all per-source pools. It is called
during shutdown **after** `streamHandler.CloseWithContext()` has drained
all active sessions and released their leases.

---

## Stream Modes

Three streaming modes are supported, configured via `Config.Mode`:

| Mode | Description |
|------|-------------|
| `direct` | Raw HTTP GET; body is relayed directly as MPEG-TS. No ffmpeg. |
| `ffmpeg-copy` | ffmpeg remuxes upstream to MPEG-TS with `-c copy` (no transcoding). Default mode. |
| `ffmpeg-transcode` | ffmpeg transcodes video to H.264 (`libx264 -preset veryfast -tune zerolatency`) and audio to AAC. |

Mode selection happens in `startSourceSessionWithContexts()` (`ffmpeg.go:2048`).

### ffmpeg arguments (ffmpeg modes)

Built by `ffmpegArgs()` (`ffmpeg.go:1839`):

- `-readrate` controls producer pacing (default `1.0`).
- `-readrate_catchup` sets temporary catch-up pacing when ffmpeg falls behind
  realtime (default `1.75`); falls back gracefully if the ffmpeg build does not
  support this option.
- `-readrate_initial_burst` sends an initial burst (default 10 s); falls back
  gracefully if the ffmpeg build does not support this option.
- `-probesize` and `-analyzeduration` control MPEG-TS stream detection window
  (defaults: 1 MB / 1500 ms). On incomplete detection, a retry with relaxed
  parameters is attempted (2 MB / 3 s).
- Optional `-buffer_size` input sizing (`FFMPEG_INPUT_BUFFER_SIZE`) for
  ffmpeg stream modes. This is primarily effective for `rist://`, `rtp://`,
  `rtsp://`, and `udp://` inputs (typically less common in many M3U playlists).
  Most playlist sources are `http://`/`https://` MPEG-TS or HLS (`.m3u8`), and
  those inputs often reject this option. In mixed-protocol playlists, enabling
  this can cause an initial startup failure on unsupported inputs before
  fallback retries once without `-buffer_size`, which adds startup delay on
  those attempts.
- Optional `-reconnect*` flags for ffmpeg-level reconnection.
- In `ffmpeg-copy` mode, `-fflags +genpts` is enabled by default so ffmpeg can
  regenerate timestamps when upstream PTS/DTS continuity is weak. This can be
  disabled with `FFMPEG_COPY_REGENERATE_TIMESTAMPS=false` when needed.
- `FFMPEG_DISCARD_CORRUPT=true` adds `-fflags +discardcorrupt` and combines
  with `+genpts` in `ffmpeg-copy` mode when both features are enabled.

---

## Startup Pipeline

When a client tunes to `/auto/v{guideNumber}`:

```
  HTTP request
      │
      ▼
  Handler.ServeHTTP()              handler.go
      │
      ├─ normalizeGuideNumber()    extract guide number from URL
      ├─ channels.GetByGuideNumber() → Channel
      ├─ tuneBackoff.allow()       check per-channel backoff
      │
      ▼
  SessionManager.subscribe()       shared_session.go:896
      │
      ├─ getOrCreateSession()      find existing or create new
      │   │
      │   ├─ [existing] → reuse session
      │   ├─ [creating] → wait on done channel
      │   └─ [new] →
      │       ├─ tuners.AcquireClient()   acquire tuner slot
      │       ├─ create sharedRuntimeSession
      │       ├─ launch session.run() goroutine
      │       └─ signal done channel
      │
      ├─ session.addSubscriber()   allocate subscriber ID + start seq
      └─ session.waitReady()       block until source is up
```

### Inside `session.run()` (`shared_session.go:1548`)

```
  run()
    │
    ├─ startInitialSource()
    │   ├─ loadSourceCandidates()      list + order + apply recent health
    │   ├─ limitSourcesByFailovers()   cap to maxFailovers+1 sources
    │   └─ startSourceWithCandidates() iterate candidates:
    │       │
    │       ├─ startSourceSession()    ffmpeg.go — start direct/ffmpeg
    │       │   │
    │       │   ├─ HTTP GET or ffmpeg exec
    │       │   ├─ readStartupProbeWithRandomAccess()
    │       │   │   ├─ read minProbeBytes (default 940)
    │       │   │   ├─ scan for TS packet alignment (0x47 sync)
    │       │   │   ├─ parse PAT → PMT → stream inventory
    │       │   │   ├─ scan for random-access point (H.264 SPS+PPS+IDR
    │       │   │   │   or HEVC VPS+SPS+PPS+IDR)
    │       │   │   └─ trim probe to cutover offset
    │       │   │
    │       │   └─ startupInventoryRequiresVideoAudio()
    │       │       accept only video_audio;
    │       │       reject video_only/audio_only/undetected/unknown
    │       │
    │       └─ markReady()   signal waitReady() callers
    │
    └─ loop: runCycle() → recovery → runCycle() → ...
```

### Startup probe details

The startup probe (`readStartupProbeWithRandomAccess` in `ffmpeg.go:975`)
reads initial bytes from the upstream and performs MPEG-TS analysis:

1. **TS alignment** — scans for `0x47` sync bytes at 188-byte intervals,
   requiring 2-3 consecutive aligned packets for confidence.
2. **Stream inventory** — parses PAT (table ID `0x00`) to discover PMT PIDs.
   PMT (table ID `0x02`) is then parsed to identify video and audio elementary
   streams with their codecs (H.264, HEVC, AAC, AC3, etc.). If PMT is not
   found, falls back to PES stream ID heuristics.
3. **Random-access detection** — reassembles video PES payloads per PID and
   scans for Annex B NAL unit start codes. H.264 requires SPS (NAL type 7)
   and PPS (NAL type 8) preceding an IDR (NAL type 5). HEVC requires VPS
   (32), SPS (33), and PPS (34) preceding IDR_W_RADL (19) or IDR_N_LP (20).
4. **Probe trimming** — trims the output to the cutover offset (the TS
   packet containing the earliest parameter set), dropping leading bytes
   that precede the random-access point.

`readStartupProbeWithRandomAccess()` always reads startup bytes and inventory,
but random-access cutover scanning/trimming is only enforced when
`requireRandomAccess=true` (recovery startup, and optionally initial startup in
ffmpeg modes when `STARTUP_RANDOM_ACCESS_RECOVERY_ONLY=false`).

Detached startup-probe `Read(...)` workers are protected by a fixed global
budget (`16`). When blocked transports saturate this budget, additional startup
attempts wait for an available slot until their startup timeout/cancel fires,
which prevents runaway goroutine growth under repeated bad upstream attempts.

---

## Source Ordering

Source selection uses a health-aware ordering algorithm
(`orderSourcesByAvailability` in `handler.go:475`).

Sources are partitioned into **ready** (not cooling down) and **cooling**
groups. Within each group, `sortSourcesForStartup` applies a stable sort:

1. Prefer sources without a recent failure (within `startupRecentFailureWindow`
   = 20 min).
2. Lower `FailCount` wins.
3. Among recently failed sources, prefer the less-recent failure timestamp.
4. Higher `SuccessCount` wins (more historical successes).
5. Lower `PriorityIndex` wins (operator-defined ordering).
6. Lower `SourceID` as final tiebreaker.

Ready sources always appear before cooling sources. If all sources are cooling,
they are still returned (sorted) so the system can attempt them.

---

## Shared Session Lifecycle

### Creation

`getOrCreateSession()` uses a two-phase create pattern:

1. Check if an existing `sharedRuntimeSession` exists for the channel ID.
2. If another goroutine is already creating one (`creating` map), wait on its
   `done` channel.
3. Otherwise, register a `sessionCreateWait`, acquire a tuner lease, build the
   session, and launch `session.run()` in a background goroutine.

### Ready state

The session starts in a "not ready" state. Once `startInitialSource()`
successfully connects to an upstream source, `markReady(nil)` signals the
`readyCh` channel. Subscribers blocked in `waitReady()` proceed.

If startup fails, `markReady(err)` propagates the error to all waiting
subscribers.

### Subscribers

- `addSubscriber()` assigns a monotonic subscriber ID and records the client
  address. The ring's `StartSeqByLagBytes()` determines the initial read
  sequence (configurable via `SubscriberJoinLagBytes`, default 8 MB).
- `removeSubscriber()` deletes the subscriber entry and, if no subscribers
  remain, starts the idle timeout timer.

### Idle timeout

When the last subscriber disconnects, an idle timer fires after
`SessionIdleTimeout` (default 5 s). If no new subscriber arrives before
expiry, the session context is canceled, which tears down the pump, closes the
upstream source, and releases the tuner lease.

### Close

`SessionManager.Close()` cancels all active sessions and waits for their
goroutines (including source-health persistence) to drain. This must complete
before the database store is closed.

During teardown, source-health persistence draining is intentionally bounded by
`SourceHealthDrainTimeout` (default `250ms`) so shutdown cannot block
indefinitely on slow persistence paths.

- `SourceHealthDrainTimeout < 0` is clamped to `0`.
- `SourceHealthDrainTimeout == 0` means "use default `250ms`" (not unbounded).
- Current behavior does not expose an "infinite/unbounded drain" mode.

Per-session source/subscriber history timeline limits are normalized with this
fallback chain:

1. Use explicit per-timeline config (`SessionSourceHistoryLimit` or
   `SessionSubscriberHistoryLimit`) when `> 0`.
2. Otherwise, fall back to `SessionHistoryLimit` when `> 0`.
3. Otherwise, fall back to timeline defaults (`256`).
4. Clamp to timeline guardrails (`16..4096`).

### Drain synchronization (`WaitForDrain`)

`SessionManager.WaitForDrain(ctx)` provides a deterministic "no active or
pending session work remains" barrier for tests and shutdown orchestration.
The wait condition includes both active runtime sessions (`sessions`) and
in-flight session creators (`creating`).

- **Late waiter fast path:** once the manager is drained, the drain signal
  remains closed, so waiters arriving after drain return immediately.
- **Signal reuse across lifecycle churn:** when a new session/session-create
  starts after drain, the manager swaps to a fresh open signal so future
  waiters block again until the next drain.
- **Concurrent waiter broadcast:** all waiters observe the same signal and are
  released together when drain completes.
- **Nil receiver / nil context semantics:** `(*SessionManager)(nil).WaitForDrain(...)`
  returns `nil`; passing `nil` context falls back to `context.Background()`
  and can block until the next drain signal transition.
- **Error return contract:** `WaitForDrain(...)` returns `nil` when drain
  completes, and returns the context error (`context.Canceled` or
  `context.DeadlineExceeded`) when the caller-provided context ends first.

Wait-for-drain observability:

- Each `WaitForDrain(...)` completion emits either `stream drain wait completed`
  or `stream drain wait failed` with per-event fields:
  `manager_id`, `drain_result` (`ok` or `error`), and
  `drain_wait_duration_us`.
- Process-lifetime drain counters are exposed via `/api/admin/tuners` in
  `drain_wait.{ok,error,wait_duration_us,wait_duration_ms}` so operators can
  monitor aggregate drain success/error volume and cumulative wait time without
  log scraping. `wait_duration_us` and `wait_duration_ms` are monotonic
  process-lifetime accumulators, not "last drain" gauges.
- Process-lifetime background-prober close fallback counters are exposed via
  `/api/admin/tuners` in
  `probe_close.{inline_count,queue_full_count}`. `inline_count` tracks all
  inline fallbacks, while `queue_full_count` is the subset caused by close
  queue saturation.
- Prometheus exports shared-session slow-skip lag metrics so skip-policy
  behavior can be separated from upstream stall/failover noise:
  - `stream_slow_skip_events_total`
  - `stream_slow_skip_lag_chunks` (histogram)
  - `stream_slow_skip_lag_bytes` (histogram)
- Per-session slow-skip totals are exposed via `/api/admin/tuners` and
  session-history snapshots in
  `slow_skip_{events_total,lag_chunks_total,lag_bytes_total,max_lag_chunks}`.
- Runbook references may call out this peak field as
  `stream_slow_skip_max_lag_chunks`; in the tuner/status JSON payload it is
  emitted as `slow_skip_max_lag_chunks`.
- Prometheus exports shared-session subscriber write-pressure metrics to
  separate writer backpressure from ring lag behavior:
  - `stream_subscriber_write_deadline_unsupported_total`
  - `stream_subscriber_write_deadline_timeouts_total`
  - `stream_subscriber_write_short_writes_total`
  - `stream_subscriber_write_blocked_seconds` (histogram)
- Prometheus also exports source-ingress pause metrics so short upstream
  starvation windows (below `stall_detect`) can be correlated with client
  cache run-down:
  - `stream_source_read_pause_events_total{reason=...}`
  - `stream_source_read_pause_seconds{reason=...}` (histogram)
  - `stream_startup_probe_read_worker_waits_total`
  - `stream_startup_probe_read_worker_acquire_timeouts_total`
  - `reason` values:
    - `recovered`: pause ended because source reads resumed.
    - `pump_exit`: cycle ended while a pause was still active.
    - `ctx_cancel`: session context canceled while a pause was still active.
- Operational guidance: sustained growth in `drain_wait.error` usually means
  shutdown/drain windows are too short or a session close path is stuck.
  Treat multi-second growth in `drain_wait.wait_duration_ms` during normal
  operation as a lifecycle regression signal worth alerting on.

---

## Ring Buffer

`ChunkRing` (`ring.go`) is a bounded, sequence-numbered, in-memory ring
buffer.

### Structure

```
  ┌─────────────────────────────────────────────┐
  │  chunks[]    [slot0] [slot1] ... [slotN-1]  │
  │  slotRefs[]  [ ref ] [ ref ] ... [  ref  ]  │
  │                                             │
  │  start ──► oldest chunk index               │
  │  count ──► number of live chunks            │
  │  nextSeq ─► next sequence number to assign  │
  │  bufferedBytes ─► total live data bytes     │
  └─────────────────────────────────────────────┘
```

### Key parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `maxChunks` | Derived by `ringCapacityForLag()` | Shared sessions size this from `BufferChunkBytes` + `SubscriberJoinLagBytes` (clamped to 32..32768); with defaults this is 16733 |
| `maxBytes` | Derived by `ringByteBudgetForLag()` | Shared sessions use `SubscriberJoinLagBytes + 16*BufferChunkBytes`; with defaults this is 3145728 bytes |
| `startupChunkHint` | `BufferChunkBytes` (default 64 KiB) | Shared sessions prewarm slot buffers using the configured chunk size hint |

### PublishChunk

1. Optionally prewarms slot buffers on first call (contiguous slab allocation).
2. Evicts oldest chunks until byte budget and slot count limits are met.
3. Deep-copies data into the slot (reusing the evicted slot's backing array
   when the ref count is zero and capacity is within
   `maxRetainedChunkDataCapacity` = 256 KB).
4. Increments `nextSeq` and `waitSeq`, then broadcasts via `sync.Cond`.

### ReadFrom (zero-copy reads)

`ReadFrom(seq)` returns chunks starting at the requested sequence. Chunk
`Data` slices point directly into the ring's slot arrays. Each returned slot's
ref count is atomically incremented to prevent the slot from being reused while
a subscriber holds a reference.

The caller **must** call `ReadResult.Release()` to decrement ref counts.

If `seq` is behind the oldest available sequence, `Behind=true` is returned.

### WaitForChange

`WaitForChange(ctx, waitSeq)` blocks on a `sync.Cond` until `waitSeq`
changes (a new chunk is published or the ring closes). Context cancellation
wakes the waiter via `context.AfterFunc`.

---

## Chunk Pump

`Pump` (`pump.go`) copies producer bytes into chunk publications using a
**size-or-time** flush strategy.

### Read loop

A dedicated goroutine (`readLoop`) reads from the source `io.ReadCloser`
into pooled buffers (`pumpReadBuffer`, default 32 KB) and sends read events
to the main loop.

### Publish logic

The main loop accumulates bytes into an internal chunk buffer (default
capacity 64 KB + 188 bytes). Publication happens when:

1. **Size threshold** — the buffer reaches `chunkBytes` (default 64 KB).
2. **Time threshold** — `flushInterval` (default 20 ms) elapses since the
   last publish and the buffer is non-empty.
3. **EOF/error** — remaining bytes are flushed as a final chunk.

### TS alignment

When `TSAlign188` is enabled, `publishLength()` truncates the publish size
down to the nearest 188-byte (MPEG-TS packet size) boundary. This ensures
every published chunk starts and ends on a TS packet boundary.

---

## Stall Detection & Recovery

### Watchdog

`runCycle()` (`shared_session.go:1808`) runs the pump in a goroutine and
monitors it with a ticker (default interval 250 ms, clamped to
`stallDetect / 2`).

On each tick:

1. Check `pump.Stats().LastPublishAt`.
2. If `time.Since(lastPublishAt) >= stallDetect` (default 4 s) and there are
   subscribers, set `stallDetected = true` and cancel the pump context.
3. Manual recovery requests arrive via `manualRecoveryCh` and cancel the
   pump context, entering the same recovery path as stall detection.

### Stall policies

| Policy | Constant | Behavior |
|--------|----------|----------|
| `failover_source` | `stallPolicyFailoverSource` | Attempt alternate sources (default) |
| `restart_same` | `stallPolicyRestartSame` | Restart the same source |
| `close_session` | `stallPolicyCloseSession` | Terminate the session |

### Recovery cycle

After a stall or source error:

1. **Short-lived failure classification** — if the source ran for less than
   `cycleFailureMinHealth` (default 20 s), a transient penalty is recorded.
   Repeat short-lived failures escalate cooldown duration (1 s base, 12 s max,
   reset after 45 s of stability).
2. **Source failure recording** — `recordSourceFailure()` stages the failure
   into `recentSourceHealth` and asynchronously persists it to the database.
3. **Cycle pacing** — `waitForRecoveryCyclePacing()` applies backoff between
   rapid recovery attempts.
4. **Cycle budget** — recovery bursts are capped by a budget (calculated as
   `clamp((stallMaxFailoversPerStall + 1) * recoveryCycleBudgetMultiplier, 12, 48)`).
   If the budget is exhausted within the reset window (4–20 s), the session
   terminates.
5. **Hard deadline** — `stallHardDeadline` (default 32 s) bounds the total
   time spent recovering.
6. **Source re-selection** — `startRecoverySource()` reloads and re-orders
   sources, potentially selecting an alternate source. Candidate eligibility
   factors include source cooldown, stream URL presence, and source-pool
   tuner availability (including full-but-reclaimable preemptible capacity in
   the same source pool).
7. If recovery succeeds, the new pump cycle begins.

### Burst pacing

Recovery bursts that exceed budget are paced with sleep windows of
`recoveryCycleBudgetPaceMinWindow` (50 ms) to
`recoveryCycleBudgetPaceMaxWindow` (1 s). This prevents runaway recovery
loops from overwhelming upstream providers.

---

## Recovery Filler

When `RecoveryFillerEnabled` is `true`, the system injects filler content into
the ring buffer during recovery to prevent downstream client stalls.

### Modes

| Mode | Constant | Description |
|------|----------|-------------|
| `null` | `recoveryFillerModeNull` | Null TS packets (minimal overhead) |
| `psi` | `recoveryFillerModePSI` | PAT/PMT-only PSI tables |
| `slate_av` | `recoveryFillerModeSlateAV` | Full A/V slate generated by ffmpeg (default) |

### slate_av mode

`recovery_filler.go` generates a text-overlay slate using ffmpeg:

- Video: `lavfi` color source with `drawtext` filter displaying configurable
  text (default: `"Channel recovering..."`).
- Resolution/framerate matched to the stream's detected profile (defaults:
  1280x720 @ 29.97 fps).
- Audio: optional silent audio track (48 kHz stereo AAC).
- Output: MPEG-TS at realtime pacing (`-readrate 1`).

### Pacing telemetry

The keepalive publisher tracks bytes/chunks emitted and computes a realtime
multiplier against the expected bitrate. A guardrail terminates the filler if
output exceeds `recoveryKeepaliveGuardrailMultiplier` (2.5x) of the expected
rate for `recoveryKeepaliveGuardrailSustain` (1.5 s), preventing runaway
output.

The filler interval (`RecoveryFillerInterval`, default 200 ms) controls how
often keepalive chunks are published.

---

## Source Health & Cooldown

### Fail ladder

`sourceBackoffDurationForRecentHealth()` (`recent_health.go:187`) applies
exponential cooldown based on consecutive `FailCount`:

| FailCount | Cooldown |
|-----------|----------|
| 1 | 10 seconds |
| 2 | 30 seconds |
| 3 | 2 minutes |
| 4 | 10 minutes |
| 5+ | 1 hour |

### Success clearing

A successful startup (`MarkSourceSuccess`) resets `FailCount` to 0, clears
`LastFailAt`, `LastFailReason`, and `CooldownUntil`. This allows a source to
immediately return to the top of the ordering.

### In-memory health overlay

`recentSourceHealth` (`recent_health.go`) maintains an in-memory event log
per source (capped at 8 pending events). These events are overlaid onto
database-sourced `channels.Source` records via `apply()` before ordering,
ensuring that health state from the current session is reflected immediately
without waiting for database persistence round-trips.

### Provider 429 cooldown

When an upstream source returns HTTP 429 (Too Many Requests),
`isLikelyUpstreamOverlimitStatus()` identifies it as an overlimit condition.
The `SessionManager` tracks per-scope cooldown state
(`providerCooldownByScope`) with configurable `UpstreamOverlimitCooldown`
duration, preventing repeated requests to rate-limited providers.

#### Scope derivation

Cooldown state is keyed by a **scope string** derived by
`providerOverlimitScopeKey(sourceID, streamURL)` with this priority:

| Priority | Key Format | When Used |
|----------|-----------|-----------|
| 1 | `"<hostname>"` or `"<hostname>:<port>"` | Stream URL has a parseable hostname (per-provider semantics) |
| 2 | `"source:<sourceID>"` | URL has no parseable hostname but sourceID > 0 (per-source semantics) |
| 3 | `"global"` | Fallback when neither URL hostname nor sourceID is available |

Scope keys are lowercased and trimmed. The map is bounded to
`providerOverlimitScopeStateLimit` (256) entries; when the limit is
exceeded, the least-recently-used expired scope is evicted first,
then the least-recently-touched scope if all are still active.

During source startup, if a candidate's provider scope has an active
cooldown, the session blocks (via `waitForProviderOverlimitCooldown`)
until the cooldown expires or the startup deadline is reached. The
probe attempt occurs only after the cooldown has expired or the
deadline has been reached.

---

## Stream Profile Probing

`profile_probe.go` provides lightweight ffprobe-based stream profiling
used by the streaming subsystem (distinct from the full analyzer used by
auto-prioritize).

The `streamProfile` struct captures:

| Field             | Type    | Description                       |
|-------------------|---------|-----------------------------------|
| `Width`           | int     | Video width in pixels             |
| `Height`          | int     | Video height in pixels            |
| `FrameRate`       | float64 | Detected frame rate               |
| `VideoCodec`      | string  | e.g. `h264`, `hevc`              |
| `AudioCodec`      | string  | e.g. `aac`, `ac3`               |
| `AudioSampleRate` | int     | Audio sample rate (Hz)            |
| `AudioChannels`   | int     | Audio channel count               |
| `BitrateBPS`      | int64   | Bitrate (video > format > variant priority) |

`probeStreamProfile()` runs ffprobe with JSON output, a 4-second default
timeout, and `analyzeduration`/`probesize` tuned for quick detection.
Results feed two downstream consumers:

- **Recovery filler** (`recovery_filler.go`): uses `Width`, `Height`,
  `FrameRate`, `AudioSampleRate`, and `AudioChannels` to match the
  `slate_av` filler resolution and audio layout to the source profile.
- **Source profile persistence**: profile fields are persisted to
  `channel_sources` (`profile_width`, `profile_height`, `profile_fps`,
  `profile_video_codec`, `profile_audio_codec`, `profile_bitrate_bps`) for
  admin diagnostics/history and persistence visibility. Current live
  `slate_av` sizing uses the active session's in-memory probe profile.
  Auto-prioritize scoring uses its own metrics cache (`metricsByItem`
  from the `stream_metrics` table), not these persisted profile fields.

---

## FFmpegProducer Lifecycle

`FFmpegProducer` (`producer.go`) wraps a standalone ffmpeg process.
It implements the `Producer` interface: `Start(ctx) -> (io.ReadCloser, error)`.

### Process management

1. **Start**: `exec.CommandContext(ctx, ffmpegPath, args...)` launches
   ffmpeg. Stdout is piped back as the `io.ReadCloser`; stderr is
   captured into a buffer for error diagnostics.
2. **Read**: callers read MPEG-TS bytes from the stdout pipe.
3. **Close**: the `producerReadCloser.Close()` method (once-guarded):
   - Sends `SIGKILL` to the ffmpeg process (`cmd.Process.Kill()`).
   - Closes the stdout pipe.
   - Calls `cmd.Wait()` to reap the process.
   - If `Wait()` returns an error and stderr is non-empty, the stderr
     content is attached to the error for diagnostic context.

Context cancellation (from stall detection, recovery, or session teardown)
propagates through `exec.CommandContext` and triggers process termination.

---

## Tune Backoff

`tuneBackoffGate` (`tune_backoff.go`) throttles per-channel startup attempts
to prevent rapid retune storms.

### Configuration

| Parameter | Description |
|-----------|-------------|
| `TuneBackoffMaxTunes` | Maximum failures allowed within the interval |
| `TuneBackoffInterval` | Sliding window for counting failures |
| `TuneBackoffCooldown` | Lockout duration after exceeding the limit |

### Mechanism

- **Per-channel scope** — each channel ID has its own sliding window of
  failure timestamps.
- **`allow(channelID)`** — checks if the channel is in cooldown. Returns a
  `tuneBackoffDecision` with `Allowed`, `RetryAfter`, and failure stats.
- **`recordFailure(channelID)`** — appends a failure timestamp; if
  `len(failures) >= limit`, arms cooldown and clears the window.
- **`recordSuccess(channelID)`** — clears the failure window.
- **Scope cap** — at most `defaultTuneBackoffScopeCap` (256) channels are
  tracked; idle scopes are pruned by LRU eviction.

The `Handler` checks tune backoff before subscribing and returns
HTTP 503 with a `Retry-After` header when backoff is active.

---

## Background Prober

`BackgroundProber` (`prober.go`) periodically checks source startup readiness
to pre-populate health state.

### Behavior

- Runs on a configurable `ProbeInterval` ticker.
- Each tick iterates all enabled channels, selects the top-priority non-cooling
  source per channel, and attempts a startup probe.
- Uses `AcquireProbe()` to obtain a tuner slot (preemptible by client
  streams).
- On success: calls `MarkSourceSuccess()` on the source.
- On failure: calls `MarkSourceFailure()` with the startup failure reason.
- Skips channels where all sources are in cooldown.
- A configurable `ProbeTuneDelay` inserts a pause between consecutive probes
  when a tuner lease was acquired, preventing rapid-fire upstream requests.

### Lifecycle and shutdown

- `Run(ctx)` only drives periodic probe ticks; it does not own worker shutdown.
- Callers must invoke `Close()` during process shutdown.
- `Close()` closes the internal probe-session close queue and waits for the
  close worker to finish draining queued `session.close()` work.
- If the close queue is unavailable or full, the prober falls back to inline
  `session.close()` execution so probe sessions are still torn down.
- Probe-session close queue depth defaults to `8` and can be adjusted through
  `ProberConfig.ProbeCloseQueueDepth` when embedding stream internals
  programmatically. This knob is currently internal-only (no CLI flag or env
  var).

### Probe source selection

`selectProbeSource()` reuses `orderSourcesByAvailability()` and picks the
first non-cooling source. If all sources are cooling, the channel is skipped
entirely.

---

## Subscriber Management

### Subscription flow

1. `SessionSubscription` holds the ring read cursor (`nextSeq`), subscriber
   ID, slow-client policy, and max blocked write duration.
2. `Stream()` loops: `ReadFrom(nextSeq)` → write chunks → `Flush()` →
   advance `nextSeq`. When no chunks are available, `WaitForChange()` blocks.

### Slow client policies

| Policy | Constant | Behavior |
|--------|----------|----------|
| `disconnect` | `slowClientPolicyDisconnect` | Return `ErrSlowClientLagged` with diagnostic details (default) |
| `skip` | `slowClientPolicySkip` | Jump `nextSeq` to oldest available sequence |

A subscriber is "behind" when `ReadFrom()` returns `Behind=true`, meaning its
requested sequence has been evicted from the ring.

### Write deadlines

`writeChunk()` sets a write deadline on the `http.ResponseController` equal
to `SubscriberMaxBlockedWrite` (default 6 s). If the client cannot accept
data within this window, the write fails and the subscriber is disconnected.

If the underlying connection does not support `SetWriteDeadline` (e.g.
certain hijacked connections or non-TCP transports), `writeChunk()` falls back
to a best-effort write without a deadline and records explicit telemetry
(`stream_subscriber_write_deadline_unsupported_total`) so operators can detect
this degraded path.

### Join lag

`SubscriberJoinLagBytes` (default 8 MB) controls how far back from the live
tail a new subscriber starts reading. `ChunkRing.StartSeqByLagBytes()` walks
backward through buffered chunks until the byte budget is exhausted.

### Max subscribers

`SessionMaxSubscribers` caps the number of concurrent subscribers per shared
session. Attempts beyond this limit receive `ErrSessionMaxSubscribers`
(HTTP 503).

---

## File Reference

| File | Description |
|------|-------------|
| `handler.go` | HTTP handler, source ordering, guide number normalization |
| `tuners.go` | Tuner pool, lease management, preemption, settle delays |
| `ffmpeg.go` | Direct/ffmpeg stream startup, MPEG-TS probe, NAL parsing |
| `producer.go` | FFmpegProducer (standalone ffmpeg process wrapper) |
| `pump.go` | Chunk pump with size-or-time publishing and TS alignment |
| `ring.go` | ChunkRing bounded buffer with ref-counted zero-copy reads |
| `shared_session.go` | Session manager, shared session lifecycle, stall detection, recovery |
| `recovery_filler.go` | Recovery keepalive content generation (null/PSI/slate_av) |
| `prober.go` | Background source health probing |
| `recent_health.go` | In-memory source health overlay with fail ladder |
| `tune_backoff.go` | Per-channel tune throttling gate |
| `status.go` | Tuner/session status snapshots for admin diagnostics |

---

## Tuning Playbook

This section provides operational guidance for tuning the streaming pipeline.
All environment variables listed below have equivalent CLI flags (replace
`_` with `-` and lowercase, e.g. `STALL_DETECT` → `--stall-detect`).

### What to Tune First

Ordered priority list for stall-heavy or unstable environments:

1. **`STALL_DETECT`** (default `4s`) — Time without new bytes before a stall
   is declared. If your upstream sources have bursty or high-latency delivery,
   increase this to avoid false-positive stalls. Start with `6s`–`8s` and
   observe `stall_count` in the tuner status API.

2. **`STALL_HARD_DEADLINE`** (default `32s`) — Absolute maximum time to wait
   for data before forcing recovery regardless of policy. Lower only if you
   need faster failover; raise if upstream sources are known to have long
   buffering gaps.

3. **`RECOVERY_FILLER_MODE`** (default `slate_av`) — What subscribers receive
   during recovery. Options: `null` (silence/black), `psi` (PAT/PMT only),
   `slate_av` (synthetic video+audio slate). Use `null` if clients tolerate
   brief signal loss; use `slate_av` if clients disconnect on empty streams.

4. **`RECOVERY_FILLER_ENABLED`** (default `true`) — Master switch for recovery
   filler packets. Disable only if all clients handle signal gaps gracefully.

5. **`TUNER_COUNT`** (default `2`) — Number of concurrent tuner slots. Set to
   match your expected concurrent channel demand. Each active channel
   (including background probes) consumes one slot.

6. **`STARTUP_TIMEOUT`** (default `12s`) — How long to wait for the first bytes
   from an upstream source before declaring startup failure. Increase for slow
   providers or high-latency networks (e.g. `15s`–`20s`).

7. **`STALL_POLICY`** (default `failover_source`) — Action on stall:
   `failover_source` tries the next source, `restart_same` retries the current
   source, `close_session` terminates the session. Use `restart_same` only
   when a single source is expected to recover on reconnect.

8. **`STALL_MAX_FAILOVERS_PER_STALL`** (default `3`) — Maximum source failover
   attempts per stall event. Increase if you have many sources per channel.

9. **`UPSTREAM_OVERLIMIT_COOLDOWN`** (default `3s`) — Pause duration after
   receiving HTTP 429 from upstream before retrying. Increase if your provider
   enforces strict rate limits.

10. **`TUNE_BACKOFF_MAX_TUNES`** (default `8`) — Per-channel startup failure
    threshold within the backoff interval. If a channel fails this many times
    within `TUNE_BACKOFF_INTERVAL`, new tunes are rejected with 503 for the
    duration of `TUNE_BACKOFF_COOLDOWN`. Set to `0` to disable.

### Lag/write tuning knobs for 4K stutter runs

Use these knobs together when clients stay connected but pause/resume
frequently. Start from defaults, change one knob at a time, and compare
`slow_skip_*`, `stream_subscriber_write_*`, and user-visible stutter counts.

| Knob | Default | Practical tuning range | Primary effect |
|---|---|---|---|
| `SUBSCRIBER_SLOW_CLIENT_POLICY` | `disconnect` | `disconnect` or `skip` | `disconnect` makes lag explicit and drops clients immediately; `skip` keeps sessions alive by dropping missed chunks. |
| `SUBSCRIBER_JOIN_LAG_BYTES` | `8 MB` | `8 MB` to `64 MB` for 4K diagnostics | Larger values increase lag cushion before a subscriber falls behind ring tail. |
| `BUFFER_CHUNK_BYTES` | `64 KiB` | `32 KiB` to `128 KiB` | Smaller chunks improve skip granularity; larger chunks reduce publish bookkeeping overhead. |
| `BUFFER_PUBLISH_FLUSH_INTERVAL` | `20ms` | `20ms` to `120ms` | Lower values reduce latency/jitter at the cost of more flush/publish churn. |
| `SUBSCRIBER_MAX_BLOCKED_WRITE` | `6s` | `2s` to `12s` | Upper bound for a single subscriber write before teardown; higher values tolerate slower clients but can hide pressure longer. |

Use `SUBSCRIBER_SLOW_CLIENT_POLICY=disconnect` as a short diagnostic run when
you need to confirm whether stutter is caused by subscriber lag versus upstream
starvation. Restore `skip` if uninterrupted playback is preferred over strict
lag disconnects.

### Memory envelope quick estimate

For shared sessions, ring byte budget is sized from:

`ring_byte_budget ~= SUBSCRIBER_JOIN_LAG_BYTES + 16 * BUFFER_CHUNK_BYTES`

Approximate resident ring memory for `N` active sessions:

`total_ring_memory ~= N * ring_byte_budget` (plus allocator/metadata overhead).

Quick examples:

| `SUBSCRIBER_JOIN_LAG_BYTES` | `BUFFER_CHUNK_BYTES` | Per-session ring budget | 4 active sessions | 8 active sessions |
|---|---|---|---|---|
| `8 MB` | `64 KiB` | `9 MB` | `36 MB` | `72 MB` |
| `32 MB` | `64 KiB` | `33 MB` | `132 MB` | `264 MB` |
| `64 MB` | `64 KiB` | `65 MB` | `260 MB` | `520 MB` |

Plan 20-30% additional headroom for runtime object overhead, HTTP buffers, and
Go heap growth when running multi-session 4K tests.

### Telemetry-to-Action Table

Use the tuner status API (`GET /api/admin/tuners`) and structured log output to identify
issues. The table below maps observable symptoms to likely causes and the
config knob to adjust.

| Symptom (metric / log) | Likely Cause | Config Knob | Default |
|---|---|---|---|
| High `recovery_cycle` count per session | Upstream instability or stall threshold too low | `STALL_DETECT`, `STALL_HARD_DEADLINE` | `4s`, `32s` |
| Subscriber disconnects during recovery | No keepalive filler or wrong filler mode | `RECOVERY_FILLER_ENABLED`, `RECOVERY_FILLER_MODE` | `true`, `slate_av` |
| Frequent 503 "channel tune backoff active" | Repeated channel startup failures triggering backoff gate | `TUNE_BACKOFF_MAX_TUNES`, `TUNE_BACKOFF_INTERVAL`, `STARTUP_TIMEOUT` | `8`, `1m`, `12s` |
| Log: `provider overlimit cooldown` | HTTP 429 from upstream provider | `UPSTREAM_OVERLIMIT_COOLDOWN` | `3s` |
| Log: `stream subscriber disconnected due to lag` | Slow client falling behind ring buffer | `SUBSCRIBER_SLOW_CLIENT_POLICY`, `SUBSCRIBER_MAX_BLOCKED_WRITE` | `disconnect`, `6s` |
| High `slow_skip_events_total` or repeated `shared session subscriber lag skip` logs | Clients frequently fall behind but skip policy keeps sessions alive by dropping chunks | `SUBSCRIBER_SLOW_CLIENT_POLICY`, `SUBSCRIBER_JOIN_LAG_BYTES`, `SUBSCRIBER_MAX_BLOCKED_WRITE` | `disconnect`, `8 MB`, `6s` |
| High `same_source_reselect_count` | Single-source channels cycling on the same source | `STALL_POLICY`, `STALL_MAX_FAILOVERS_PER_STALL` | `failover_source`, `3` |
| Startup failures / `reason: deadline` | Source too slow to deliver probe bytes in time | `STARTUP_TIMEOUT`, `MIN_PROBE_BYTES` | `12s`, `940` |
| `recovery_keepalive_guardrail_count` incrementing | Filler output exceeding realtime pacing envelope | `RECOVERY_FILLER_MODE`, `RECOVERY_FILLER_INTERVAL` (null/psi only) | `slate_av`, `200ms` |
| All tuners busy / `ErrNoTunersAvailable` | Concurrent demand exceeds tuner pool | `TUNER_COUNT` | `2` |
| Sessions closing immediately after idle | Idle timeout too aggressive for bursty viewing | `SESSION_IDLE_TIMEOUT` | `5s` |

### Common Symptom → Cause → Fix

**1. Channels constantly cycling through recovery**

- **Symptom**: `recovery_cycle` climbs rapidly; logs show repeated
  `stall_detected=true` entries.
- **Cause**: `STALL_DETECT` is shorter than the upstream source's natural
  delivery gaps (e.g. some IPTV providers buffer 5–8 s between bursts).
- **Fix**: Increase `STALL_DETECT` to `6s`–`10s`. If the upstream is
  genuinely unstable, verify `STALL_POLICY=failover_source` and ensure
  multiple sources are configured per channel.

**2. Clients disconnect during channel recovery**

- **Symptom**: Subscriber count drops to zero during recovery windows; client
  apps report "stream ended".
- **Cause**: Recovery filler is disabled or set to `null`/`psi`, and the client
  interprets the data gap as end-of-stream.
- **Fix**: Set `RECOVERY_FILLER_ENABLED=true` and
  `RECOVERY_FILLER_MODE=slate_av`. Optionally set
  `RECOVERY_FILLER_ENABLE_AUDIO=true` (default) to include a silent audio
  track in the slate.

**3. New tune requests rejected with 503**

- **Symptom**: HTTP 503 responses with body "channel tune backoff active;
  retry later" and `Retry-After` header.
- **Cause**: The channel exceeded `TUNE_BACKOFF_MAX_TUNES` startup failures
  within `TUNE_BACKOFF_INTERVAL`. The backoff gate is protecting against
  retry storms.
- **Fix**: First, investigate why startups are failing (check
  `STARTUP_TIMEOUT`, upstream availability, source health). If the failures
  are transient, increase `TUNE_BACKOFF_MAX_TUNES` or decrease
  `TUNE_BACKOFF_COOLDOWN` (default `20s`).

**4. Upstream 429 rate limiting causing outages**

- **Symptom**: Logs show `provider overlimit cooldown` with HTTP 429 status
  codes; channels stall during cooldown wait.
- **Cause**: Too many concurrent requests to the upstream provider, or
  recovery cycles are hitting the provider faster than its rate limit allows.
- **Fix**: Increase `UPSTREAM_OVERLIMIT_COOLDOWN` (e.g. `5s`–`10s`) to give
  the provider time to recover. Reduce `TUNER_COUNT` if the provider cannot
  handle the concurrency. Consider `STALL_DETECT` increases to reduce
  recovery frequency.

**5. Slow clients causing subscriber disconnects**

- **Symptom**: Logs show `stream subscriber disconnected due to lag`;
  specific client IPs appear repeatedly.
- **Cause**: Client cannot consume data fast enough and falls behind the ring
  buffer. The default `disconnect` policy terminates the subscriber.
- **Fix**: If you prefer to keep slow clients connected, set
  `SUBSCRIBER_SLOW_CLIENT_POLICY=skip` (drops chunks the client missed
  rather than disconnecting). Increase `SUBSCRIBER_MAX_BLOCKED_WRITE`
  (default `6s`) to give clients more time per chunk write. Increase
  `SUBSCRIBER_JOIN_LAG_BYTES` (default `8 MB`) to widen the ring buffer
  window.

### Bounded Close Telemetry (`closeWithTimeout`)

Stream cleanup uses a shared bounded close-worker subsystem to avoid unbounded
goroutine growth when upstream `Close()` calls block. The subsystem maintains a
global worker budget (default `16`, overridable via
`CLOSE_WITH_TIMEOUT_WORKER_BUDGET=1..256`) and telemetry counters:

- `Started`: close workers that started immediately.
- `Retried`: deferred close attempts started from the retry queue.
- `Suppressed`: close attempts suppressed by duplicate in-flight ownership or
  budget pressure.
- `SuppressedDuplicate`: subset of `Suppressed` attributed to duplicate
  in-flight dedupe ownership.
- `SuppressedBudget`: subset of `Suppressed` attributed to worker-budget
  saturation.
- `Dropped`: deferred close attempts dropped when retry queue is full.
- `Queued`: current retry queue depth.
- `Timeouts`: close workers that exceeded timeout budget.
- `LateCompletions`: timed-out closes that finished asynchronously later.
- `LateAbandoned`: post-timeout close waiters that exceeded the late-abandon
  budget and force-cleared in-flight dedupe ownership.
- `ReleaseUnderflow`: attempted worker-slot releases when no worker token was
  held (internal accounting invariant warning).

Deferred retries signal the drain loop immediately through an internal notify
channel. As a backstop, the loop also polls for queued retries every `250ms`
(`closeWithTimeoutRetryPollInterval`) so deferred closes can still drain if a
notify event is missed.

Timed-out close workers keep their slot while waiting for late completion, but
only up to a hard `30s` hold window. After that window, the subsystem
force-releases the worker slot so unrelated closers can proceed. A second hard
`2m` late-abandon window (`closeWithTimeoutLateCompletionAbandonTimeout`) then
waits for completion before force-clearing per-closer dedupe ownership and
incrementing `LateAbandoned`.

`closeSlateAVRecoveryReaderWithTimeout(...)` shares these counters for slate AV
recovery-reader shutdown. Error paths emit
`shared session slate AV close error` with `close_error_type=timeout|non_timeout`
plus the same `close_*` telemetry fields (`close_late_abandoned`,
`close_release_underflow`, `close_suppressed_duplicate`,
`close_suppressed_budget`, and related counters) to aid operator triage.
Shared-session drain close timeout defaults to `2s` and is configurable via
`SESSION_DRAIN_TIMEOUT` / `--session-drain-timeout`.
Non-timeout warnings are coalesced per session on a 1-second window; when the
window reopens, the next warning includes
`close_non_timeout_logs_coalesced=<suppressed_count>`. Timeout warnings bypass
this coalescing intentionally so every timeout event is visible.
Under sustained budget pressure, runtime also emits
`closeWithTimeout suppression observed` warning logs on the first suppression
event and every 64th suppression thereafter with `close_worker_budget`,
`close_retry_queue_budget`, and current `close_*` counter values.

For startup non-2xx diagnostics, `readBodyPreviewBounded(...)` now waits up to
`previewCancelFlushWait` (`10ms`) after cancellation so buffered preview bytes
can flush before returning. This adds a small bounded cancel-path delay while
preserving caller-owned response-body close semantics.

Operator actions:

- If `close_suppressed` or `close_queued` climbs steadily, close worker budget
  pressure is building. Check for blocked upstream `Close()` calls or high
  churn startup failures that are forcing repeated cleanup.
- If `close_retried` increases while `close_queued` stays near zero, deferred
  closes are draining successfully after temporary budget pressure. If both
  `close_retried` and `close_queued` climb together, pressure is sustained and
  close workers are not catching up fast enough.
- If `close_timeouts` and `close_late_completions` both rise, shutdown calls
  are frequently exceeding the timeout budget and completing only after detach.
  Inspect upstream providers/transports for slow close behavior and look for
  related socket/resource exhaustion.
- If `close_late_abandoned` is non-zero, some `Close()` calls exceeded both the
  `30s` worker-slot hold and the `2m` late-abandon wait. Treat this as a stuck
  upstream close path and investigate transport/provider teardown immediately.
- If logs emit `closeWithTimeout worker slot release underflow`, inspect
  `close_release_underflow` together with `close_timeouts` and
  `close_late_completions` to scope whether timeout churn is contributing to
  worker-slot accounting drift. This warning indicates an internal invariant
  violation and should be treated as urgent for follow-up.
- If `close_dropped` is non-zero, deferred close retries exceeded queue
  capacity. Treat this as an urgent signal of sustained close-path saturation
  and investigate immediately.

---

## Known Limitations

> Last validated: 2026-03-04

This document no longer maintains a static "active remediation" backlog.
Streaming hardening work is tracked in internal coordination TODO artifacts and
is promoted to `CHANGELOG.md` once shipped. Use the telemetry, log event, and
operator-action guidance above for current production diagnosis and triage.
