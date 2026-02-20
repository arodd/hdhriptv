#!/usr/bin/env bash
#
# background-prober-close-worker-b3-evidence.sh
#
# Collects reproducible evidence for Background Prober close-worker B3 validation.
#
# Usage:
#   ./project/scripts/background-prober-close-worker-b3-evidence.sh
#       (plan-only output)
#   BACKGROUND_PROBER_CLOSE_B3_DRY_RUN=0 ./project/scripts/background-prober-close-worker-b3-evidence.sh collect
#
# Optional environment:
# - BACKGROUND_PROBER_CLOSE_B3_OUTDIR: output directory (default: ./project/tmp/background-prober-close-worker-b3)
# - BACKGROUND_PROBER_CLOSE_B3_PACKAGES: go test package list (default: ./internal/stream)
# - BACKGROUND_PROBER_CLOSE_B3_TARGET_PATTERN: target regex for go test -run (default: TestBackgroundProber.*(Close|ProbeOnce).*)
# - BACKGROUND_PROBER_CLOSE_B3_DRY_RUN: 1 plan-only, 0 execute (default: 1)
# - BACKGROUND_PROBER_CLOSE_B3_GO_TEST_TIMEOUT: go test timeout (default: 2m)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MODE="${1:-plan}"
shift || true

OUTDIR="${BACKGROUND_PROBER_CLOSE_B3_OUTDIR:-$PROJECT_ROOT/tmp/background-prober-close-worker-b3}"
PACKAGES="${BACKGROUND_PROBER_CLOSE_B3_PACKAGES:-./internal/stream}"
TARGET_PATTERN="${BACKGROUND_PROBER_CLOSE_B3_TARGET_PATTERN:-TestBackgroundProber.*(Close|ProbeOnce).*}"
DRY_RUN="${BACKGROUND_PROBER_CLOSE_B3_DRY_RUN:-1}"
GO_TEST_TIMEOUT="${BACKGROUND_PROBER_CLOSE_B3_GO_TEST_TIMEOUT:-2m}"

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
      --target-pattern)
        TARGET_PATTERN="$2"
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
      --help|-h)
        usage
        exit 0
        ;;
      *)
        echo "error: unknown argument: $1" >&2
        exit 1
        ;;
    esac
  done
}

usage() {
  cat <<'EOF'
Background prober close-worker B3 evidence collector.

Usage:
  ./project/scripts/background-prober-close-worker-b3-evidence.sh [plan|collect]

Examples:
  ./project/scripts/background-prober-close-worker-b3-evidence.sh
  BACKGROUND_PROBER_CLOSE_B3_DRY_RUN=0 ./project/scripts/background-prober-close-worker-b3-evidence.sh collect
EOF
}

log_metadata() {
  cat <<EOF > "$OUTDIR/metadata.txt"
generated_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
mode=$MODE
outdir=$OUTDIR
packages=$PACKAGES
target_pattern=$TARGET_PATTERN
go_test_timeout=$GO_TEST_TIMEOUT
dry_run=$DRY_RUN
EOF
  go version >> "$OUTDIR/metadata.txt"
}

log_command() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$OUTDIR/commands.log"
}

run_phase() {
  local phase="$1"
  shift
  local command=("$@")
  local log_file="$OUTDIR/${phase}.log"

  {
    echo "phase=$phase"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'command='
    printf '%q ' "${command[@]}"
    echo
  } > "$log_file"

  log_command "$phase" "${command[@]}"

  if [ "$DRY_RUN" = "1" ]; then
    echo "DRY-RUN skip $phase: would run: ${command[*]}" | tee -a "$OUTDIR/plan.txt"
    return 0
  fi

  local rc=0
  (
    cd "$PROJECT_ROOT"
    "${command[@]}"
  ) >> "$log_file" 2>&1 || rc=$?
  {
    echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "status=$([[ $rc -eq 0 ]] && echo ok || echo fail)"
    echo "exit_code=$rc"
  } >> "$log_file"

  if [ "$rc" -ne 0 ]; then
    FAILURES=$((FAILURES + 1))
  fi
}

plan_only() {
  mkdir -p "$OUTDIR"
  log_metadata
  cat <<EOF > "$OUTDIR/commands.txt"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$TARGET_PATTERN" "$PACKAGES"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
go test -race -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
EOF
  cat <<'EOF' > "$OUTDIR/README.txt"
Background prober close-worker B3 evidence plan generated without execution.
Run with BACKGROUND_PROBER_CLOSE_B3_DRY_RUN=0 and the 'collect' mode to execute.
EOF
  echo "PLAN_ONLY: wrote command plan to $OUTDIR/commands.txt"
}

collect_only() {
  mkdir -p "$OUTDIR"
  log_metadata
  touch "$OUTDIR/commands.log"
  cat <<EOF > "$OUTDIR/commands.txt"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$TARGET_PATTERN" "$PACKAGES"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
go test -race -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
EOF

  run_phase "targeted-prober-close-worker" \
    go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$TARGET_PATTERN" "$PACKAGES"
  run_phase "stream-full" \
    go test -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
  run_phase "stream-race" \
    go test -race -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"

  cat <<EOF > "$OUTDIR/artifacts.txt"
targeted-prober-close-worker.log
stream-full.log
stream-race.log
commands.txt
metadata.txt
EOF

  if [ "$FAILURES" -gt 0 ]; then
    echo "failed_phases=$FAILURES" > "$OUTDIR/failure.txt"
    echo "COLLECTION_FAILED: ${FAILURES} phase(s) exited non-zero." >&2
    exit 1
  fi

  echo "COLLECTION_OK: captured evidence under $OUTDIR"
}

main() {
  parse_args "$@"

  case "$MODE" in
    collect)
      collect_only
      ;;
    plan|*)
      plan_only
      ;;
  esac
}

main "$@"
