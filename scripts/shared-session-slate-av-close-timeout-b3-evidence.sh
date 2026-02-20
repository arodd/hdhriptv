#!/usr/bin/env bash

set -euo pipefail

# shared-session-slate-av-close-timeout-b3-evidence.sh
#
# Collect reproducible evidence for shared-session slate_av close-timeout B3 validation.
#
# Usage:
#   ./project/scripts/shared-session-slate-av-close-timeout-b3-evidence.sh
#       (plan-only output)
#   SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_DRY_RUN=0 \
#       ./project/scripts/shared-session-slate-av-close-timeout-b3-evidence.sh collect
#
# Environment:
# - SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_OUTDIR: output directory (default: ./project/tmp/shared-session-slate-av-close-timeout-b3)
# - SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_PACKAGES: package list (default: ./internal/stream)
# - SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_DRY_RUN: 1 plan-only, 0 execute (default: 1)
# - SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_GO_TEST_TIMEOUT: go test timeout (default: 2m)
# - SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_TARGET_PATTERN: -run pattern for targeted phase

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MODE="${1:-plan}"
shift || true

OUTDIR="${SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_OUTDIR:-$PROJECT_ROOT/tmp/shared-session-slate-av-close-timeout-b3}"
PACKAGES="${SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_PACKAGES:-./internal/stream}"
DRY_RUN="${SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_DRY_RUN:-1}"
GO_TEST_TIMEOUT="${SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_GO_TEST_TIMEOUT:-2m}"
TARGET_PATTERN="${SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_TARGET_PATTERN:-Test(SharedSessionRecoveryHeartbeatSlateAV|CloseSlateAVRecoveryReaderWithTimeout)}"
FAILURES=0

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
      --timeout)
        GO_TEST_TIMEOUT="$2"
        shift 2
        ;;
      --target-pattern)
        TARGET_PATTERN="$2"
        shift 2
        ;;
      --help|-h)
        cat <<'EOF_HELP'
Usage: ./project/scripts/shared-session-slate-av-close-timeout-b3-evidence.sh [plan|collect] [--out DIR] [--packages 'pkgs'] [--dry-run 0|1] [--timeout DURATION] [--target-pattern REGEX]
EOF_HELP
        exit 0
        ;;
      *)
        echo "error: unknown argument: $1" >&2
        exit 1
        ;;
    esac
  done
}

write_metadata() {
  cat <<EOF_META > "$OUTDIR/metadata.txt"
generated_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
mode=$MODE
outdir=$OUTDIR
packages=$PACKAGES
target_pattern=$TARGET_PATTERN
go_test_timeout=$GO_TEST_TIMEOUT
dry_run=$DRY_RUN
EOF_META
  go version >> "$OUTDIR/metadata.txt"
}

append_command_log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$OUTDIR/commands.log"
}

run_phase() {
  local phase="$1"
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

  append_command_log "$phase" "${cmd[@]}"

  if [ "$DRY_RUN" = "1" ]; then
    echo "DRY-RUN skip $phase: would run: ${cmd[*]}" | tee -a "$OUTDIR/plan.txt"
    return
  fi

  local rc=0
  (
    cd "$PROJECT_ROOT"
    "${cmd[@]}"
  ) >> "$log_file" 2>&1 || rc=$?

  echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$log_file"
  echo "status=$([[ $rc -eq 0 ]] && echo ok || echo fail)" >> "$log_file"
  echo "exit_code=$rc" >> "$log_file"

  if [ "$rc" -ne 0 ]; then
    FAILURES=$((FAILURES + 1))
  fi
}

run_plan_only() {
  mkdir -p "$OUTDIR"
  write_metadata
  cat <<EOF_CMDS > "$OUTDIR/commands.txt"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$TARGET_PATTERN" "$PACKAGES"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
go test -race -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
EOF_CMDS
  cat <<'EOF_README' > "$OUTDIR/README.txt"
Shared-session slate_av close-timeout B3 evidence collection plan generated in dry-run mode.
Run with SHARED_SESSION_SLATE_AV_CLOSE_TIMEOUT_B3_DRY_RUN=0 and the 'collect' mode to execute.
EOF_README
  echo "PLAN_ONLY: command plan written to $OUTDIR/commands.txt"
}

run_collect() {
  mkdir -p "$OUTDIR"
  write_metadata
  touch "$OUTDIR/commands.log"

  cat <<EOF_CMDS > "$OUTDIR/commands.txt"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$TARGET_PATTERN" "$PACKAGES"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
go test -race -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
EOF_CMDS

  run_phase "targeted-slate-av-close-timeout" \
    go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$TARGET_PATTERN" "$PACKAGES"
  run_phase "stream-full" \
    go test -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
  run_phase "stream-race" \
    go test -race -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"

  cat <<EOF_ARTIFACTS > "$OUTDIR/artifacts.txt"
targeted-slate-av-close-timeout.log
stream-full.log
stream-race.log
commands.txt
metadata.txt
EOF_ARTIFACTS

  if [ "$FAILURES" -gt 0 ]; then
    echo "failed_phases=$FAILURES" > "$OUTDIR/failure.txt"
    echo "COLLECTION_FAILED: ${FAILURES} phase(s) failed." >&2
    exit 1
  fi

  echo "COLLECTION_OK: captured evidence under $OUTDIR"
}

main() {
  parse_args "$@"
  case "$MODE" in
    collect)
      run_collect
      ;;
    plan|*)
      run_plan_only
      ;;
  esac
}

main "$@"
