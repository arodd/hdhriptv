# Stall Recovery Subsystem

The recovery subsystem detects upstream source failures, manages automatic failover between sources, and keeps DVR clients connected during recovery windows by emitting keepalive filler data.

## Overview

DVR clients like Channels DVR disconnect from a stream when they stop receiving data for approximately 6 seconds. Without recovery, any upstream source interruption (network stall, provider outage, source EOF) would immediately terminate every subscriber's recording or live viewing session.

The recovery subsystem addresses this by:

1. Detecting stalls via a watchdog timer that monitors the last-publish timestamp
2. Selecting an alternate or restarted source within a configurable deadline
3. Emitting keepalive filler bytes to subscribers during the recovery window so clients do not time out

All recovery logic lives in the shared session layer (`shared_session.go`). Each `sharedRuntimeSession` runs a main loop (`run()`) that calls `runCycle()` per source. When a cycle ends (stall, EOF, or error), recovery begins.

## Stall Detection

The stall watchdog runs inside `runCycle()` as a polling ticker. It checks `pump.Stats().LastPublishAt` against wall-clock time.

**Detection logic** (lines 1808-1893 of `shared_session.go`):
- A ticker fires every 250ms (or `stallDetect/2` if `stallDetect` is smaller, floored at 50ms)
- On each tick, if `time.Since(lastPublishAt) >= stallDetect` and no stall has been detected yet, the stall flag is set and the pump context is canceled
- Detection only fires when at least one subscriber is connected
- Manual recovery requests are also received on this same select loop via `manualRecoveryCh`

When a stall is detected, `stallCount` is incremented and the pump's context is canceled, causing the current reader to drain and the cycle to end with an error.

## Recovery Policies

The `STALL_POLICY` setting determines the recovery strategy. Three modes are supported:

### `failover_source` (default)

Tries alternate sources first, preferring sources that are not currently on cooldown. Sources are ordered by availability (ready sources before cooling-down sources), recent failure history, fail count, success count, operator priority, and source ID.

When no alternate sources are eligible for startup (all on cooldown or only one source exists), `failover_source` automatically **downgrades to `restart_same`** behavior, retrying the current source. This is logged with reason `no_alternate_sources` and a `restart_same_fallback` tag.

Short-lived recovery penalties are tracked per-source. Sources that repeatedly fail quickly during recovery receive transient cooldown penalties (base 1s, max 12s, reset window 45s, state TTL 2m) to avoid thrashing.

### `restart_same`

Always retries the current source. No alternate source selection occurs.

### `close_session`

Immediately closes the session on any source failure. No recovery is attempted. All subscribers are disconnected.

## Recovery Cycle Budget

Each burst of rapid recovery cycles is tracked by a budget system that prevents infinite recovery loops from consuming resources.

**Budget calculation:**
- `limit = clamp((stallMaxFailoversPerStall + 1) * 8, 12, 48)`
  - With the default `STALL_MAX_FAILOVERS_PER_STALL=3`: limit = clamp((3+1)*8, 12, 48) = 32
- `resetWindow = clamp(stallDetect*2 + startupTimeout, 4s, 20s)`
  - With defaults (4s + 6s = 14s): resetWindow = 14s
- `paceWindow = clamp(min(250ms, stallDetect?, startupTimeout?), 50ms, 1s)`
  - `stallDetect` and `startupTimeout` are only included in the min comparison when their value is > 0; if either is zero or unset, it is excluded and does not pull the window down
  - With defaults (both > 0): paceWindow = min(250ms, 4s, 6s) = 250ms

**How it works:**
- The burst counter increments on each recovery cycle
- If `sinceLastSourceSelect >= resetWindow`, the burst counter resets to 1
- Budget accounting is time-aware: `budgetCount = min(burstCount, elapsed/paceWindow + 1)`
- When `budgetCount > limit`, the session closes with "recovery cycle budget exhausted"

This prevents rapid-fire recovery loops while still allowing a generous number of retries over time.

## Burst Pacing (Source-EOF Backoff)

When recovery reason is `source_eof` and the same source is reselected, an exponential backoff delay is applied before the next recovery attempt:

| Attempt | Backoff  |
|---------|----------|
| 1       | 250ms    |
| 2       | 500ms    |
| 3       | 1s       |
| 4+      | 2s (cap) |

The backoff is calculated by `recoveryRetryBackoff()` and applied via `waitForRecoveryCyclePacing()`. Time already spent since the last source selection is subtracted from the target delay, so fast re-EOFs wait longer while slow failures may skip the wait entirely.

**Log coalescing:** Repeated recovery trigger logs for the same source and reason within a 1-second window are coalesced. The suppressed count is emitted on the next distinct log entry.

## Hard Deadline

The `STALL_HARD_DEADLINE` (default 32s) sets the maximum wall-clock time allowed for a single recovery attempt. When recovery begins:

```go
recoverCtx, cancel = context.WithTimeout(s.ctx, stallHardDeadline)
```

If the deadline expires before a new source is established, the recovery context is canceled. The failover total timeout is also capped to this deadline during stall recovery.

When the hard deadline is exceeded, the session is closed and all subscribers are disconnected.

## Recovery Filler / Keepalive

When `RECOVERY_FILLER_ENABLED=true` (default), keepalive data is emitted to subscribers during the recovery window to prevent DVR client timeouts.

### Keepalive Modes

#### `null`

Emits PID 0x1FFF null MPEG-TS packets at the configured interval. These are padding packets that MPEG-TS demuxers discard. Minimal CPU and bandwidth overhead.

#### `psi`

Emits PAT (Program Association Table) and PMT (Program Map Table) packets at the configured interval. These signal-level packets maintain the MPEG-TS program structure without contributing decodable media. Slightly more useful than null packets for keeping demuxers synchronized.

PAT/PMT continuity counters and version numbers are tracked per-session to ensure correct incrementing across recovery windows.

#### `slate_av` (default)

Generates a full decodable H.264 video + AAC audio MPEG-TS stream via an ffmpeg subprocess. The video shows a configurable text overlay (default: "Channel recovering...") and the audio is silent.

**Profile normalization:** The slate matches the current source's resolution, frame rate, and audio parameters. Odd-dimension values are rounded to even numbers (H.264 encoding requires even dimensions). If no profile is known, defaults to 1280x720 @ 29.97fps, 48kHz stereo audio.

**Guardrails:** The keepalive output rate is monitored against the expected bitrate:
- Expected rate = `profileBitrateBPS / 8` bytes per second when the probed profile bitrate is available, falling back to `1.5 Mbps / 8` when unavailable
- After a 750ms warmup period, if the output rate exceeds `expectedRate * 2.5` for a sustained 1500ms, the keepalive is terminated with `errRecoveryKeepaliveOverproduction`
- This prevents runaway ffmpeg processes from flooding subscribers with data faster than realtime

**Startup and restart:** The slate AV producer is allowed one automatic restart if it exits unexpectedly. After two failures, it falls through the fallback chain.

### Fallback Chain

If the configured mode fails, keepalive degrades through a chain:

```
slate_av  -->  psi  -->  null
```

If `psi` is configured and fails:

```
psi  -->  null
```

If `null` is configured, it runs directly with no fallback.

Each fallback is logged and tracked in telemetry (`recovery_keepalive_fallback_count`, `recovery_keepalive_fallback_reason`).

### Filler Interval

For `null` and `psi` modes, the `RECOVERY_FILLER_INTERVAL` (default 200ms) controls the ticker period between keepalive chunk emissions. The `slate_av` mode uses its own ffmpeg-paced output instead.

### Transition Boundaries

When switching between live and keepalive (and back), transition boundary markers are published to the ring buffer:
- `live_to_keepalive`: emitted when recovery keepalive starts
- `keepalive_to_live`: emitted when keepalive stops for any reason, including successful source establishment **and** terminal recovery failure paths (budget exhaustion, hard deadline, no subscribers). The stop callback runs unconditionally before the recovery error is checked (`shared_session.go` lines 1792–1794), so consumers should not assume this marker means a new source is ready — only that keepalive emission has ended

## Comprehensive Timer Reference

| Timer / Constant | Default | Config Flag / Env Var | Purpose |
|---|---|---|---|
| `STALL_DETECT` | 4s | `--stall-detect` / `STALL_DETECT` | No-publish duration before stall detection triggers recovery |
| `STALL_HARD_DEADLINE` | 32s | `--stall-hard-deadline` / `STALL_HARD_DEADLINE` | Maximum time for entire recovery attempt before session close |
| `STARTUP_TIMEOUT` | 6s | `--startup-timeout` / `STARTUP_TIMEOUT` | Per-source startup timeout (connecting + probe bytes) |
| `FAILOVER_TOTAL_TIMEOUT` | 32s | `--failover-total-timeout` / `FAILOVER_TOTAL_TIMEOUT` | Total budget for iterating through source candidates during startup/recovery |
| `MAX_FAILOVERS` | 3 | `--max-failovers` / `MAX_FAILOVERS` | Maximum fallback source attempts after primary (0 = try all) |
| `STALL_MAX_FAILOVERS_PER_STALL` | 3 | `--stall-max-failovers-per-stall` / `STALL_MAX_FAILOVERS_PER_STALL` | Maximum failover attempts per detected stall event |
| `CYCLE_FAILURE_MIN_HEALTH` | 20s | `--cycle-failure-min-health` / `CYCLE_FAILURE_MIN_HEALTH` | Minimum source healthy uptime before recovery failures are persisted to health tracking |
| `RECOVERY_FILLER_INTERVAL` | 200ms | `--recovery-filler-interval` / `RECOVERY_FILLER_INTERVAL` | Interval between null/psi keepalive chunk emissions |
| `RECOVERY_FILLER_ENABLED` | true | `--recovery-filler-enabled` / `RECOVERY_FILLER_ENABLED` | Enable keepalive filler during recovery |
| `RECOVERY_FILLER_MODE` | `slate_av` | `--recovery-filler-mode` / `RECOVERY_FILLER_MODE` | Keepalive mode: `null`, `psi`, or `slate_av` |
| `RECOVERY_FILLER_TEXT` | "Channel recovering..." | `--recovery-filler-text` / `RECOVERY_FILLER_TEXT` | Text overlay for slate_av filler |
| `RECOVERY_FILLER_ENABLE_AUDIO` | true | `--recovery-filler-enable-audio` / `RECOVERY_FILLER_ENABLE_AUDIO` | Include silent AAC audio in slate_av filler |
| `UPSTREAM_OVERLIMIT_COOLDOWN` | 3s | `--upstream-overlimit-cooldown` / `UPSTREAM_OVERLIMIT_COOLDOWN` | Per-provider-scope cooldown after HTTP 429 upstream failures |
| `SESSION_IDLE_TIMEOUT` | 5s | `--session-idle-timeout` / `SESSION_IDLE_TIMEOUT` | Session teardown delay after last subscriber leaves |
| `SUBSCRIBER_MAX_BLOCKED_WRITE` | 6s | `--subscriber-max-blocked-write` / `SUBSCRIBER_MAX_BLOCKED_WRITE` | Max blocked write duration per chunk for slow subscribers |
| `TUNE_BACKOFF_MAX_TUNES` | 8 | `--tune-backoff-max-tunes` / `TUNE_BACKOFF_MAX_TUNES` | Per-channel startup failure threshold before cooldown (0 disables) |
| `TUNE_BACKOFF_INTERVAL` | 1m | `--tune-backoff-interval` / `TUNE_BACKOFF_INTERVAL` | Rolling window for counting per-channel startup failures |
| `TUNE_BACKOFF_COOLDOWN` | 20s | `--tune-backoff-cooldown` / `TUNE_BACKOFF_COOLDOWN` | Cooldown duration applied when tune backoff threshold is exceeded |
| `PREEMPT_SETTLE_DELAY` | 500ms | `--preempt-settle-delay` / `PREEMPT_SETTLE_DELAY` | Delay before reusing preempted tuner slots |
| `STARTUP_RANDOM_ACCESS_RECOVERY_ONLY` | true | `--startup-random-access-recovery-only` / `STARTUP_RANDOM_ACCESS_RECOVERY_ONLY` | Enforce random-access startup gating only during recovery (not initial startup) |

### Hardcoded Timing Constants

| Constant | Value | Location | Purpose |
|---|---|---|---|
| Stall check ticker | 250ms | `shared_session.go` `runCycle` | Polling interval for stall watchdog (adjusted to `stallDetect/2` if smaller, floor 50ms) |
| `recoveryRetryBackoff` base | 250ms | `shared_session.go` | Initial backoff for source-EOF same-source recovery pacing |
| `recoveryRetryBackoff` cap | 2s | `shared_session.go` | Maximum backoff for source-EOF same-source recovery pacing |
| `recoveryCycleBudgetMultiplier` | 8 | `shared_session.go` | Multiplier applied to per-stall attempt limit for burst budget |
| `recoveryCycleBudgetMin` | 12 | `shared_session.go` | Minimum recovery cycle burst budget |
| `recoveryCycleBudgetMax` | 48 | `shared_session.go` | Maximum recovery cycle burst budget |
| `recoveryCycleBudgetResetMinWindow` | 4s | `shared_session.go` | Minimum window before burst counter resets |
| `recoveryCycleBudgetResetMaxWindow` | 20s | `shared_session.go` | Maximum window before burst counter resets |
| `recoveryCycleBudgetPaceMinWindow` | 50ms | `shared_session.go` | Minimum time-aware pacing window for budget counting |
| `recoveryCycleBudgetPaceMaxWindow` | 1s | `shared_session.go` | Maximum time-aware pacing window for budget counting |
| `recoveryTriggerLogCoalesceWindow` | 1s | `shared_session.go` | Window for coalescing repeated recovery trigger log entries |
| `recoveryKeepaliveExpectedBitrateMin` | 1.5 Mbps | `shared_session.go` | Fallback expected bitrate when probed profile bitrate is unavailable |
| `recoveryKeepaliveGuardrailMultiplier` | 2.5x | `shared_session.go` | Rate multiplier threshold before guardrail triggers |
| `recoveryKeepaliveGuardrailWarmup` | 750ms | `shared_session.go` | Warmup period before guardrail monitoring activates |
| `recoveryKeepaliveGuardrailSustain` | 1500ms | `shared_session.go` | Sustained overrate duration required before guardrail fires |
| `recoverySubscriberGuardPollInterval` | 50ms | `shared_session.go` | Polling interval for subscriber-count guard during recovery waits |
| `recoveryAlternateMinAttemptWindow` | 3s | `shared_session.go` | Minimum time window allocated to try an alternate source |
| `recoveryAlternateMaxAttemptWindow` | 8s | `shared_session.go` | Maximum time window allocated to try an alternate source |
| `recoveryAlternateFallbackReserve` | 500ms | `shared_session.go` | Time reserved for falling back to current source after alternate attempts |
| `recoveryAlternateMinStartupAttempts` | 3 | `shared_session.go` | Minimum alternate sources to attempt during failover recovery |
| `recoveryTransientPenaltyBase` | 1s | `shared_session.go` | Base cooldown for short-lived recovery failure penalty |
| `recoveryTransientPenaltyMax` | 12s | `shared_session.go` | Maximum cooldown for short-lived recovery failure penalty |
| `recoveryTransientPenaltyResetWindow` | 45s | `shared_session.go` | Window after which short-lived penalty counter resets |
| `recoveryTransientPenaltyStateTTL` | 2m | `shared_session.go` | TTL for short-lived penalty tracking state per source |
| `sameSourceReselectAlertThreshold` | 3 | `shared_session.go` | Reselect count before churn alert appears in tuner status |
| `profileProbeRecoveryDelayMin` | 100ms | `shared_session.go` | Minimum delay before profile probe after recovery source selection |
| `profileProbeRecoveryDelayMax` | 5s | `shared_session.go` | Maximum delay before profile probe after recovery source selection |
| `profileProbeRestartCooldownMin` | 100ms | `shared_session.go` | Minimum cooldown between profile probe restarts |
| `profileProbeRestartCooldownMax` | 2s | `shared_session.go` | Maximum cooldown between profile probe restarts |
| `clientPreemptWait` | 2s | `tuners.go` | Timeout waiting for preempted tuner to release |
| Default filler resolution | 1280x720 | `recovery_filler.go` | Default slate_av resolution when no source profile is known |
| Default filler frame rate | 29.97 fps | `recovery_filler.go` | Default slate_av frame rate |
| Default filler audio | 48kHz stereo | `recovery_filler.go` | Default slate_av audio parameters |

## State Transitions

```
                         +-----------+
                         |  NORMAL   |
                         | (pumping) |
                         +-----+-----+
                               |
             stall detected /  |  \ source EOF / error /
             manual trigger    |   \ no subscribers
                               |    \
                               |     +---> [session closes gracefully]
                               |
                      +--------v---------+
                      | STALL DETECTED   |
                      | (pump canceled)  |
                      +--------+---------+
                               |
          policy check         |
          +--------------------+--------------------+
          |                    |                    |
  close_session         failover_source      restart_same
     |                     |                    |
     v                     v                    v
  [session          +------+-------+    +-------+------+
   closes]          | RECOVERING   |    | RECOVERING   |
                    | (failover)   |    | (restart)    |
                    +------+-------+    +-------+------+
                           |                    |
          keepalive filler | (concurrent)       | keepalive filler
          emitting during  |                    | emitting during
          source startup   |                    | source startup
                           |                    |
             +-------------+--------------------+
             |                                  |
      success (new source)            failure / deadline exceeded
             |                                  |
             v                                  v
      +------+------+                  +--------+--------+
      |   NORMAL    |                  | budget exceeded  |
      |  (pumping)  |                  | or hard deadline |
      +-------------+                  +--------+--------+
                                                |
                                                v
                                        [session closes]
```

**Within the RECOVERING state:**

```
  RECOVERING
      |
      +---> begin recovery cycle (increment counter)
      |
      +---> check stall policy
      |         |
      |    failover_source:
      |         +---> order candidates by availability
      |         +---> apply short-lived recovery penalties
      |         +---> try alternates (within attempt window)
      |         +---> if no alternates: fallback to restart_same
      |
      +---> start keepalive heartbeat (concurrent)
      |         |
      |         +---> slate_av / psi / null (with fallback chain)
      |
      +---> wait for recovery cycle pacing (if source_eof + same source)
      |
      +---> track recovery burst budget
      |         |
      |         +---> if budget exceeded: close session
      |
      +---> apply hard deadline context timeout
      |
      +---> start recovery source (with failover budget)
      |
      +---> stop keepalive heartbeat
      |
      +---> if success: return to NORMAL with new reader
      +---> if failure: close session
```

## Manual Recovery Trigger

A manual recovery can be triggered via the admin API:

```
POST /api/admin/tuners/recovery
```

**Requirements:**
- The session must be active with at least one connected subscriber
- No recovery must already be pending (the manual recovery channel is buffered with size 1)
- If the session is idle/absent (for example, no active subscribers), `ErrSessionNotFound` is returned

**Behavior:**
- The reason string is normalized to lowercase with spaces replaced by underscores
- For `POST /api/admin/tuners/recovery`, an empty reason defaults to `ui_manual_trigger`
  (the lower-level session helper defaults to `manual_trigger` when invoked directly)
- The trigger is delivered via `manualRecoveryCh` and detected in the `runCycle` select loop
- A manual trigger increments the stall counter and cancels the pump, just like a real stall
- Manual recovery does **not** mark the current source as failed (it is a synthetic signal)

The recovery reason in telemetry will show the normalized reason string (e.g., `ui_manual_trigger`).

## Source Health During Recovery

The `CYCLE_FAILURE_MIN_HEALTH` guard (default 20s) controls whether recovery-cycle source failures are persisted to the source health tracking database.

**Logic in `shouldTrackCycleFailure()`:**
- If `cycleFailureMinHealth` is 0 (disabled), all failures are always persisted
- If the source was selected during a `recovery_cycle_*` and has been healthy for less than `cycleFailureMinHealth`, the failure is **not persisted**
- This prevents recovery "churn" from permanently penalizing sources that failed only because they were tested briefly during recovery

Sources selected during initial startup always have their failures persisted regardless of uptime.

The direct cycle-ending failure (the source that was actively streaming when the stall occurred) is always recorded. Only recovery-attempt startup failures are subject to the minimum health gate.

## Telemetry and Observability

### Recovery Fields in Tuner Status (`/api/admin/tuners`)

| Field | Description |
|---|---|
| `stall_count` | Total stall detections for this session |
| `recovery_cycle` | Current/last recovery cycle number |
| `recovery_reason` | Reason for current/last recovery (`stall`, `source_eof`, `ui_manual_trigger`, `upstream_429`, etc.) |
| `recovery_transition_mode` | Configured transition strategy |
| `recovery_transition_effective_mode` | Actual transition strategy in use (may differ after fallback) |
| `recovery_transition_signals_applied` | Comma-separated signals applied during transition |
| `recovery_transition_signal_skips` | Comma-separated signals skipped during transition |
| `recovery_transition_fallback_count` | Number of transition strategy fallbacks |
| `recovery_transition_fallback_reason` | Reason for last transition fallback |
| `recovery_transition_stitch_applied` | Whether PCR/continuity stitching was applied at transition |
| `recovery_keepalive_mode` | Active keepalive mode (`null`, `psi`, `slate_av`, or empty) |
| `recovery_keepalive_fallback_count` | Number of keepalive mode fallbacks |
| `recovery_keepalive_fallback_reason` | Reason for last keepalive fallback |
| `recovery_keepalive_started_at` | When keepalive emission began |
| `recovery_keepalive_stopped_at` | When keepalive emission ended |
| `recovery_keepalive_duration` | Duration of keepalive window |
| `recovery_keepalive_bytes` | Total keepalive bytes emitted |
| `recovery_keepalive_chunks` | Total keepalive chunks emitted |
| `recovery_keepalive_rate_bytes_per_second` | Observed output rate |
| `recovery_keepalive_expected_rate_bytes_per_second` | Expected output rate (`profile_bitrate_bps / 8`, or `1.5 Mbps / 8` fallback) |
| `recovery_keepalive_realtime_multiplier` | Ratio of observed to expected rate |
| `recovery_keepalive_guardrail_count` | Number of guardrail triggers |
| `recovery_keepalive_guardrail_reason` | Reason for last guardrail trigger |
| `source_select_count` | Total source selections for this session |
| `same_source_reselect_count` | Consecutive times the same source was reselected |
| `last_source_selected_at` | Timestamp of last source selection |
| `last_source_select_reason` | Reason for last source selection (e.g., `recovery_cycle_1:stall`) |

### Churn Summary (aggregated across sessions)

| Field | Description |
|---|---|
| `recovering_session_count` | Sessions that have entered recovery in the current runtime (`recovery_cycle > 0` or non-empty `recovery_reason`) |
| `sessions_with_reselect_count` | Sessions with any same-source reselections |
| `sessions_over_reselect_threshold` | Sessions exceeding the reselect alert threshold (3) |
| `total_recovery_cycles` | Sum of recovery cycles across all sessions |
| `total_same_source_reselect_count` | Sum of same-source reselections across all sessions |

### Key Log Events

| Log Message | Level | When |
|---|---|---|
| `shared session recovery triggered` | WARN | Recovery cycle begins |
| `shared session recovery cycle budget exhausted` | WARN | Burst budget exceeded, session closing |
| `shared session cycle failure not persisted before healthy threshold` | INFO | Source failure suppressed by `CYCLE_FAILURE_MIN_HEALTH` |
| `shared session recovery keepalive started` | INFO | Keepalive filler begins emitting |
| `shared session recovery keepalive stopped` | INFO | Keepalive filler ends (includes rate/duration stats) |
| `shared session recovery keepalive fallback` | WARN | Keepalive mode degraded (e.g., slate_av to psi) |
| `shared session slate AV keepalive exited; restarting once` | WARN | Slate AV process exited unexpectedly, attempting restart |
| `shared session manual recovery requested` | INFO | Manual trigger received via API |
| `shared session skipped recovery while idle without subscribers` | INFO | Recovery skipped because no subscribers are connected |
| `shared session recovery keepalive overproduction guardrail triggered` | WARN | Output rate exceeded guardrail threshold |
| `shared session recovery transition fallback` | WARN | Transition strategy fallback applied |

## Known Limitations / Active Remediation

> Last validated: 2026-02-17

The following open issues affect the recovery subsystem and are tracked in dedicated TODO files:

- `TODO-stream-shared-session-recovery-stale-last-publish-false-stall-loop.md` — Recovery cycles can self-trigger `stall` from stale pump `LastPublishAt` before first publish of the new source cycle
- `TODO-stream-shared-session-trigger-recovery-control-plane-consistency.md` — Accepted recovery triggers can route to stale session owners, producing false `not found`, `already pending`, or stale success outcomes
- `TODO-stream-shared-session-manual-recovery-lifecycle-consistency.md` — Accepted manual recovery requests can be silently dropped, stale-acked, replayed, or misclassified as `source_eof`
- `TODO-stream-recovery-restart-same-disabled-current-source-retry-bypass.md` — `restart_same` and fallback-to-current paths can retry disabled sources absent from enabled candidate queries
- `TODO-stream-recovery-keepalive-terminal-failure-false-live-transition-boundary.md` — Terminal recovery failure still emits `keepalive_to_live` boundary signaling even when no replacement live source is selected
- `TODO-stream-recovery-alternate-startup-idle-cancel-stale-penalty-reattach-race.md` — Idle-guard startup cancellations can record alternate short-lived penalties when subscribers reattach before post-attempt lifecycle guards run
- `TODO-stream-source-profile-persistence-and-recovery-inference-convergence.md` — Stale `LastProbeAt` clobber and profile persist retry gaps can leave persisted profile diagnostics stale until later successful probes
- `TODO-stream-blocking-close-startup-abort-lifecycle-convergence.md` — Blocked close/read/wait paths can pin recovery lifecycle convergence across direct, ffmpeg, prober, startup, and pump flows

### Top 3 Current Risks

**1. False stall loop** (failure mode: stall)
Stale prior-cycle `LastPublishAt` timestamps can trigger immediate false stalls before any data flows in new recovery cycles, causing rapid recovery churn and premature session termination.
- `TODO-stream-shared-session-recovery-stale-last-publish-false-stall-loop.md`

**2. Control-plane consistency** (failure mode: control-plane)
Recovery trigger routing can target stale session owners, and manual recovery requests can be silently dropped or misclassified, leaving operators unable to reliably intervene during recovery events.
- `TODO-stream-shared-session-trigger-recovery-control-plane-consistency.md`
- `TODO-stream-shared-session-manual-recovery-lifecycle-consistency.md`

**3. Source selection drift** (failure mode: persistence)
Disabled sources can be retried via `restart_same` fallback, and stale alternate penalties can accumulate from lifecycle race windows.
- `TODO-stream-recovery-restart-same-disabled-current-source-retry-bypass.md`
- `TODO-stream-recovery-alternate-startup-idle-cancel-stale-penalty-reattach-race.md`

## Troubleshooting

### Diagnosing Recovery Churn

Recovery churn occurs when a session repeatedly cycles through recovery without establishing a stable source. Signs to look for:

**In logs:**
- Repeated `shared session recovery triggered` WARN entries in rapid succession for the same channel
- `shared session recovery cycle budget exhausted` indicates the session hit the burst limit and was closed
- High `stall_count` values accumulating over a short period
- `shared session recovery keepalive fallback` entries showing keepalive mode degradation (e.g., `slate_av` falling back to `psi` or `null`)

**In `/api/admin/tuners`:**
- `same_source_reselect_count` exceeding 3 (the `sameSourceReselectAlertThreshold`) signals thrashing on one source
- `sessions_over_reselect_threshold` in the churn summary indicates system-wide churn
- `recovery_cycle` values climbing rapidly relative to `stall_count` — each stall should ideally produce only a few recovery cycles
- `recovery_keepalive_guardrail_count > 0` may indicate the keepalive itself is misbehaving

### Safe Timer Defaults Under Load

The default timer values are tuned for typical single-tuner residential setups. Under higher load (many concurrent sessions, shared upstream providers), consider:

| Timer | Default | Under Load Guidance |
|---|---|---|
| `STALL_DETECT` | 4s | Increase to 6–8s to avoid false stall detection from transient upstream congestion. Values below 3s can cause false positives on busy systems. |
| `STALL_HARD_DEADLINE` | 32s | Generally safe as-is. Reduce to 16–20s only if fast session teardown is preferred over prolonged recovery attempts. |
| `STARTUP_TIMEOUT` | 6s | Increase to 10–12s if upstream sources are slow to respond (e.g., remote/CDN sources). |
| `FAILOVER_TOTAL_TIMEOUT` | 32s | Keep aligned with or below `STALL_HARD_DEADLINE`. Reducing this limits how many alternate sources can be tried. |
| `SESSION_IDLE_TIMEOUT` | 5s | Increase to 10–15s under load to reduce session teardown/recreation churn when subscribers briefly disconnect. |
| `UPSTREAM_OVERLIMIT_COOLDOWN` | 3s | Increase to 5–10s if providers enforce strict rate limits (HTTP 429). |

### When to Adjust STALL_DETECT vs STALL_HARD_DEADLINE vs Recovery Filler Mode

**Adjust `STALL_DETECT` when:**
- False stall detections appear in logs (source is healthy but briefly slow) — increase the value
- Stalls take too long to detect and subscribers drop before recovery starts — decrease the value (but not below 3s)
- Upstream sources have variable bitrate or bursty delivery patterns — increase to accommodate natural gaps

**Adjust `STALL_HARD_DEADLINE` when:**
- Recovery takes too long and subscribers give up waiting — decrease to fail fast
- Sources are slow to start (remote CDNs, cold caches) and recovery is closing sessions that would have recovered — increase to allow more time
- As a rule of thumb, `STALL_HARD_DEADLINE` should be at least 4x `STALL_DETECT` to allow meaningful recovery attempts

**Adjust recovery filler mode when:**
- `slate_av` is consuming too much CPU — switch to `psi` or `null` for lower overhead
- DVR clients are disconnecting during recovery despite keepalive being active — try `slate_av` which provides decodable frames that keep clients more reliably connected
- Slate AV is failing to start (check ffmpeg availability/path/permissions and filter errors) — `psi` is a reliable lightweight fallback
- You need minimal resource usage and clients tolerate brief gaps — use `null` mode

### Telemetry-to-Action Mapping

Use this table to map observed telemetry signals to the appropriate configuration adjustment:

| Signal (field or log event) | Indicates | Config Knob to Adjust |
|---|---|---|
| High `stall_count` with stable sources | `STALL_DETECT` is too sensitive | Increase `STALL_DETECT` (e.g., 6–8s) |
| `recovery_cycle` climbing much faster than `stall_count` | Rapid recovery churn within each stall | Check source availability; consider increasing `STALL_HARD_DEADLINE` or `STARTUP_TIMEOUT` |
| `same_source_reselect_count` climbing | No viable alternates or `failover_source` keeps falling back to same source | Add more sources to the channel lineup; consider `failover_source` policy if only one source exists |
| `recovery_keepalive_guardrail_count > 0` | Keepalive output rate exceeded expected bitrate threshold | Check source profile bitrate accuracy; if slate_av is overproducing, switch to `psi` or `null` filler mode |
| `recovery_keepalive_fallback_count > 0` | Configured keepalive mode failed and degraded | Check `recovery_keepalive_fallback_reason`; for `slate_av` failures, verify ffmpeg availability/path and process stderr context (dimensions are normalized automatically) |
| `sessions_over_reselect_threshold` rising | System-wide source churn across multiple sessions | Review upstream provider stability; increase `UPSTREAM_OVERLIMIT_COOLDOWN` if 429s are involved |
| `shared session recovery cycle budget exhausted` log | Session hit burst limit and was terminated | Upstream source is unstable; investigate source health or increase `STALL_DETECT` to reduce false triggers |
| `shared session cycle failure not persisted` log | Recovery churn is being filtered from health tracking | Expected when `CYCLE_FAILURE_MIN_HEALTH` is working; no action needed unless source health data seems incomplete |

### Escalation Decision Tree: Choosing a Stall Policy

Use this decision tree to choose between `failover_source`, `restart_same`, and `close_session` based on your environment:

```
Is the channel backed by multiple independent sources?
├── YES: Use `failover_source` (default)
│   │
│   ├── Are upstream sources generally reliable (rare stalls)?
│   │   └── YES: Default settings work well.
│   │       Tune STALL_DETECT up (6–8s) only if false stalls appear.
│   │
│   └── Are upstream sources unreliable (frequent stalls/EOFs)?
│       └── YES: Keep `failover_source`. Consider:
│           - Increase STALL_HARD_DEADLINE to allow more failover time
│           - Increase STARTUP_TIMEOUT if sources are slow to respond
│           - Monitor `same_source_reselect_count` — if high,
│             add more sources or check source health
│
└── NO (single source per channel):
    │
    ├── Is the source generally reliable (rare, transient failures)?
    │   └── YES: Use `restart_same`
    │       - `failover_source` also works (auto-downgrades to
    │         restart_same when no alternates exist), but
    │         `restart_same` avoids the alternate-search overhead
    │
    ├── Is the source unreliable (frequent failures)?
    │   └── Consider whether clients can tolerate disconnects:
    │       ├── NO (recordings must not break):
    │       │   └── Use `restart_same` with generous timeouts
    │       │       - Increase STALL_HARD_DEADLINE (e.g., 45–60s)
    │       │       - Enable slate_av filler to keep clients connected
    │       └── YES (live viewing, brief gaps acceptable):
    │           └── Use `close_session` for fast cleanup
    │               - Avoids prolonged recovery that wastes resources
    │               - Clients reconnect and get a fresh session
    │
    └── Do you want immediate teardown on any failure?
        └── YES: Use `close_session`
            - No recovery attempted; all subscribers disconnected
            - Appropriate when upstream issues require manual
              intervention and automatic retry would only mask problems
```

**Key considerations:**
- `failover_source` is the safest default — it tries alternates first and automatically falls back to `restart_same` when no alternates are available
- `restart_same` is preferred for single-source channels where the source is expected to recover quickly (transient network issues, brief provider interruptions)
- `close_session` is appropriate when recovery is not useful (upstream requires manual intervention, or fast teardown is preferred over prolonged filler emission)
