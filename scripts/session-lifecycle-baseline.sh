#!/usr/bin/env bash
#
# session-lifecycle-baseline.sh
#
# Collects deterministic evidence artifacts for Bug Triage: Shared-session
# Lifecycle Convergence Phase B0. The script is intentionally conservative:
# it prints planned commands first and does not execute by default.
#
# Usage:
#   ./project/scripts/session-lifecycle-baseline.sh
#       (print planned commands only)
#   SESSION_LIFECYCLE_DRY_RUN=0 ./project/scripts/session-lifecycle-baseline.sh collect
#
# Optional environment:
# - SESSION_LIFECYCLE_OUTDIR: output directory for logs (default: ./project/tmp/session-lifecycle-baseline)
# - SESSION_LIFECYCLE_PACKAGES: go test package list for baseline runs
# - SESSION_LIFECYCLE_SAMPLE_INTERVAL: seconds between thread samples (default: 0.25)
# - SESSION_LIFECYCLE_THREAD_SAMPLING: enable OS thread samples (1|0, default: 1)
# - SESSION_LIFECYCLE_READY_PATTERN: go test -run pattern for startup/readiness latency probes
# - SESSION_LIFECYCLE_FINISH_PATTERN: go test -run pattern for finish-to-release probes
# - SESSION_LIFECYCLE_WORKER_PATTERN: go test -run pattern for worker/goroutine stress probes
# - SESSION_LIFECYCLE_HISTORY_PATTERN: go test -run pattern for snapshot/history regressions
# - SESSION_LIFECYCLE_DRY_RUN: print commands without executing (default: 1)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MODE="${1:-plan}"
shift || true

DEFAULT_OUTDIR="$PROJECT_ROOT/tmp/session-lifecycle-baseline"
OUTDIR="${SESSION_LIFECYCLE_OUTDIR:-$DEFAULT_OUTDIR}"
READY_PATTERN="${SESSION_LIFECYCLE_READY_PATTERN:-Test.*(Ready|ready|Startup|startup|markReady|mark_ready)}"
FINISH_PATTERN="${SESSION_LIFECYCLE_FINISH_PATTERN:-Test.*(finish|close|shutdown|disconnect|release|ring\\.Close)}"
WORKER_PATTERN="${SESSION_LIFECYCLE_WORKER_PATTERN:-Test.*(worker|goroutine|Detached|closeWorker|close.*worker|Async)}"
HISTORY_PATTERN="${SESSION_LIFECYCLE_HISTORY_PATTERN:-Test.*(History|Snapshot|HasActiveOrPendingSession|TunerStatus)}"
PACKAGES="${SESSION_LIFECYCLE_PACKAGES:-./internal/stream ./internal/http ./internal/store/sqlite}"
SAMPLE_INTERVAL="${SESSION_LIFECYCLE_SAMPLE_INTERVAL:-0.25}"
THREAD_SAMPLING="${SESSION_LIFECYCLE_THREAD_SAMPLING:-1}"
DRY_RUN="${SESSION_LIFECYCLE_DRY_RUN:-1}"
GO_TEST_TIMEOUT="${SESSION_LIFECYCLE_GO_TEST_TIMEOUT:-2m}"
FAILURES=0
THREAD_SAMPLE_FILES=()

usage() {
  cat <<'EOF'
Session-lifecycle baseline evidence collector.

Default mode prints planned commands only. To run collection:

  SESSION_LIFECYCLE_DRY_RUN=0 ./project/scripts/session-lifecycle-baseline.sh collect

Optional flags:
  --out DIR
      Override output directory.
  --packages "pkgs"
      Go package list for test runs (quoted, space-separated list).
  --dry-run 0|1
      Force dry-run behavior explicitly.
  --sample-interval SEC
      Seconds between OS thread samples.
  --thread-sampling 0|1
      Enable/disable thread sampling for each phase.

Common environment-based overrides are read from
SESSION_LIFECYCLE_* variables listed in the script header.
EOF
}

parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --out)
        OUTDIR="$2"
        shift 2
        ;;
      --packages)
        PACKAGES="$2"
        shift 2
        ;;
      --dry-run)
        DRY_RUN="$2"
        shift 2
        ;;
      --sample-interval)
        SAMPLE_INTERVAL="$2"
        shift 2
        ;;
      --thread-sampling)
        THREAD_SAMPLING="$2"
        shift 2
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        echo "error: unknown argument: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

log_meta() {
  local -r meta_file="$OUTDIR/metadata.txt"
  {
    echo "generated_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "project_root=$PROJECT_ROOT"
    echo "mode=$MODE"
    echo "outdir=$OUTDIR"
    echo "packages=$PACKAGES"
    echo "ready_pattern=$READY_PATTERN"
    echo "finish_pattern=$FINISH_PATTERN"
    echo "worker_pattern=$WORKER_PATTERN"
    echo "history_pattern=$HISTORY_PATTERN"
    echo "thread_sampling=$THREAD_SAMPLING"
    echo "sample_interval=$SAMPLE_INTERVAL"
    echo "go_test_timeout=$GO_TEST_TIMEOUT"
    echo "dry_run=$DRY_RUN"
    go version
  } > "$meta_file"
}

log_command() {
  local -r phase="$1"
  shift
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$OUTDIR/commands.log"
}

append_threads() {
  local phase="$1"
  local pid="$2"
  local thread_file="$OUTDIR/${phase}.threads.csv"
  if [ "$THREAD_SAMPLING" != "1" ]; then
    return
  fi

  echo "timestamp,pid,threads" >> "$thread_file"
  THREAD_SAMPLE_FILES+=("$thread_file")

  while kill -0 "$pid" 2>/dev/null; do
    local threads
    threads="$(ps -p "$pid" -o nlwp= 2>/dev/null | tr -d ' ' || true)"
    if [ -z "$threads" ]; then
      threads="NA"
    fi
    printf '%s,%s,%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$pid" "$threads" >> "$thread_file"
    sleep "$SAMPLE_INTERVAL"
  done
}

run_phase() {
  local -r phase="$1"
  shift
  local cmd=("$@")
  local log_file="$OUTDIR/${phase}.log"

  {
    echo "phase=$phase"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'command='
    printf '%q ' "${cmd[@]}"
    echo
  } > "$log_file"

  log_command "$phase" "${cmd[@]}"

  if [ "$DRY_RUN" = "1" ]; then
    echo "DRY-RUN skip $phase: would run: ${cmd[*]}" | tee -a "$OUTDIR/plan.txt"
    return 0
  fi

  local phase_status="ok"
  (
    cd "$PROJECT_ROOT"
    "${cmd[@]}"
  ) >> "$log_file" 2>&1 &
  local pid="$!"
  append_threads "$phase" "$pid"
  local rc=0
  wait "$pid" || rc=$?

  if [ "$rc" -ne 0 ]; then
    phase_status="fail"
    FAILURES=$((FAILURES + 1))
  fi

  {
    echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "status=$phase_status"
    echo "exit_code=$rc"
  } >> "$log_file"

  return 0
}

collect_baseline() {
  mkdir -p "$OUTDIR"
  log_meta
  printf '# %s\n' "Session-lifecycle baseline collection at $OUTDIR" > "$OUTDIR/README.txt"
  echo "collecting baseline into $OUTDIR" | tee "$OUTDIR/plan.txt"

  run_phase "ready-latency" \
    go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$READY_PATTERN" -json $PACKAGES
  run_phase "finish-release" \
    go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$FINISH_PATTERN" -json $PACKAGES
  run_phase "worker-growth" \
    go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$WORKER_PATTERN" -json -race $PACKAGES
  run_phase "history-snapshot-growth" \
    go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$HISTORY_PATTERN" -json $PACKAGES

  echo "ready-latency.log / finish-release.log / worker-growth.log / history-snapshot-growth.log" \
    > "$OUTDIR/artifacts.txt"
  for sample_file in "${THREAD_SAMPLE_FILES[@]}"; do
    echo "$(basename "$sample_file")" >> "$OUTDIR/artifacts.txt"
  done
  cat "$OUTDIR/commands.log" >> "$OUTDIR/artifacts.txt"

  if [ "$FAILURES" -gt 0 ]; then
    echo "failed_phases=${FAILURES}" > "$OUTDIR/failure.txt"
    echo "COLLECTION_FAILED: ${FAILURES} command(s) exited non-zero." >&2
    exit 1
  fi

  echo "COLLECTION_OK: captured baseline logs and samples under $OUTDIR"
}

plan_only() {
  mkdir -p "$OUTDIR"
  log_meta
  cat <<EOF > "$OUTDIR/commands.txt"
go test -count=1 -timeout $GO_TEST_TIMEOUT -run '$READY_PATTERN' -json $PACKAGES
go test -count=1 -timeout $GO_TEST_TIMEOUT -run '$FINISH_PATTERN' -json $PACKAGES
go test -count=1 -timeout $GO_TEST_TIMEOUT -run '$WORKER_PATTERN' -json -race $PACKAGES
go test -count=1 -timeout $GO_TEST_TIMEOUT -run '$HISTORY_PATTERN' -json $PACKAGES
EOF
  cat <<'EOF' > "$OUTDIR/README.txt"
Baseline plan generated without execution (SESSION_LIFECYCLE_DRY_RUN=1).
Run with SESSION_LIFECYCLE_DRY_RUN=0 to execute collection.
EOF
  echo "PLAN_ONLY: wrote command plan to $OUTDIR/commands.txt"
}

main() {
  parse_args "$@"

  if [ "$MODE" = "collect" ]; then
    collect_baseline
    exit 0
  fi

  if [ "$MODE" = "plan" ] || [ "$MODE" = "-h" ] || [ "$MODE" = "--help" ]; then
    plan_only
    exit 0
  fi

  echo "error: unknown mode '$MODE' (expected plan|collect)" >&2
  usage
  exit 1
}

main "$@"
