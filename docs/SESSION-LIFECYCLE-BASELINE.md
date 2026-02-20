# Shared-session lifecycle baseline evidence workflow (Phase B0)

This document defines the practical baseline workflow for `Bug Triage: Shared-session
Lifecycle Convergence` (Phase B0).

## Purpose

Before fix-phase edits, this phase collects reproducible evidence for:

- startup/readiness latency
- finish-to-release or finish-to-subscriber-unblock latency
- goroutine and thread growth under lifecycle stress paths
- history/snapshot growth behavior during churn

The evidence is intended to make later `B2` and `B3` measurements comparable.

## Artifacts

- Script: `project/scripts/session-lifecycle-baseline.sh`
- Default artifact directory:
  - `project/tmp/session-lifecycle-baseline/`
- Primary outputs per phase:
  - `ready-latency.log`
  - `finish-release.log`
  - `worker-growth.log`
  - `history-snapshot-growth.log`
- Supporting outputs:
  - `commands.txt`
  - `metadata.txt`
  - `artifacts.txt`
  - `*.threads.csv` files when thread sampling is enabled

## How to run

The script defaults to plan mode (dry-run), so it never executes tests unless
explicitly disabled.

```bash
cd /home/austin/git/hdhriptv
./project/scripts/session-lifecycle-baseline.sh
```

To run collection:

```bash
cd /home/austin/git/hdhriptv
SESSION_LIFECYCLE_DRY_RUN=0 ./project/scripts/session-lifecycle-baseline.sh collect
```

Optional overrides:

- `SESSION_LIFECYCLE_PACKAGES`: package list (default: `./internal/stream ./internal/http ./internal/store/sqlite`)
- `SESSION_LIFECYCLE_OUTDIR`: output directory
- `SESSION_LIFECYCLE_SAMPLE_INTERVAL`: thread sample cadence in seconds
- `SESSION_LIFECYCLE_THREAD_SAMPLING`: `1` enables thread samples, `0` disables
- `SESSION_LIFECYCLE_READY_PATTERN`, `SESSION_LIFECYCLE_FINISH_PATTERN`,
  `SESSION_LIFECYCLE_WORKER_PATTERN`, `SESSION_LIFECYCLE_HISTORY_PATTERN`

### Example with custom output directory

```bash
SESSION_LIFECYCLE_DRY_RUN=0 \
SESSION_LIFECYCLE_OUTDIR=./tmp/lifecycle-baseline-run \
./project/scripts/session-lifecycle-baseline.sh collect
```

## Evidence interpretation (minimum checks)

These checks are intentionally explicit and repeatable:

- `ready-latency.log`
  - confirm startup/readiness-focused tests were exercised
  - record run duration and any test failures for baseline comparison
- `finish-release.log`
  - confirm finish/recovery/shutdown paths are represented in test logs
- `worker-growth.log`
  - run with `-race` for a stronger stress envelope
  - compare goroutine/thread pressure qualitatively in adjacent artifacts
- `history-snapshot-growth.log`
  - include lifecycle snapshot and history tests
  - capture overlap or duplicate states before fix-phase work

For thread files, a practical per-phase check:

- `phase-name.threads.csv` should include timestamped thread counts while phase command
  runs.
- capture the baseline peak and final thread count for comparison against later runs.

## Integration with outstanding todo

Once the script/docs are in place, Phase B0 is considered evidence-ready.

- `outstanding-todo.md` entry for B0 now points to this workflow for Phase B0 evidence.
- `TODO-stream-shared-session-lifecycle-consolidation.md` is the canonical planner note
  for cross-issue convergence sequencing.

## Notes

- The script stores command output in raw logs. No numeric acceptance gates are
  enforced in script mode; comparison across runs is intentionally manual.
- Use the same `SESSION_LIFECYCLE_*` overrides for B0 and later B3 reruns
  so before/after runs are directly comparable.
