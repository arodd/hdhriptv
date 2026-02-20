#!/usr/bin/env bash
#
# direct-startup-late-response-b3-evidence.sh
#
# Collects reproducible evidence for direct-startup late-response B3 validation.
#
# Usage:
#   ./project/scripts/direct-startup-late-response-b3-evidence.sh
#       (print planned commands only)
#   DIRECT_STARTUP_LATE_RESPONSE_B3_DRY_RUN=0 ./project/scripts/direct-startup-late-response-b3-evidence.sh collect
#
# Environment:
# - DIRECT_STARTUP_LATE_RESPONSE_B3_OUTDIR: output directory for artifacts (default: ./project/tmp/direct-startup-late-response-b3)
# - DIRECT_STARTUP_LATE_RESPONSE_B3_PACKAGES: packages list (default: ./internal/stream)
# - DIRECT_STARTUP_LATE_RESPONSE_B3_DRY_RUN: 1 plan-only, 0 execute (default: 1)
# - DIRECT_STARTUP_LATE_RESPONSE_B3_GO_TEST_TIMEOUT: timeout string for go test commands (default: 2m)
# - DIRECT_STARTUP_LATE_RESPONSE_B3_TARGET_PATTERN: -run regex for targeted B3 probe tests
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MODE="${1:-plan}"
shift || true

OUTDIR="${DIRECT_STARTUP_LATE_RESPONSE_B3_OUTDIR:-$PROJECT_ROOT/tmp/direct-startup-late-response-b3}"
PACKAGES="${DIRECT_STARTUP_LATE_RESPONSE_B3_PACKAGES:-./internal/stream}"
DRY_RUN="${DIRECT_STARTUP_LATE_RESPONSE_B3_DRY_RUN:-1}"
GO_TEST_TIMEOUT="${DIRECT_STARTUP_LATE_RESPONSE_B3_GO_TEST_TIMEOUT:-2m}"
TARGET_PATTERN="${DIRECT_STARTUP_LATE_RESPONSE_B3_TARGET_PATTERN:-TestStartDirect.*(LateResponse|Timeout|Cancel).*}"
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
      --help|-h)
        echo "Usage: $0 [plan|collect] [--out DIR] [--packages 'pkgs'] [--dry-run 0|1] [--timeout DURATION]"
        exit 0
        ;;
      *)
        echo "error: unknown argument: $1" >&2
        exit 1
        ;;
    esac
  done
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
    return
  fi

  local rc=0
  (
    cd "$PROJECT_ROOT"
    "${cmd[@]}"
  ) >> "$log_file" 2>&1 || rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "phase=$phase status=fail exit_code=$rc" >> "$log_file"
    FAILURES=$((FAILURES + 1))
  else
    echo "phase=$phase status=ok exit_code=0" >> "$log_file"
  fi
}

run_plan_only() {
  mkdir -p "$OUTDIR"
  log_metadata
  cat <<EOF > "$OUTDIR/commands.txt"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$TARGET_PATTERN" "$PACKAGES"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
go test -race -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
EOF
  cat <<'EOF' > "$OUTDIR/README.txt"
Direct-startup late-response B3 evidence collection plan generated in dry-run mode.
Run with DIRECT_STARTUP_LATE_RESPONSE_B3_DRY_RUN=0 and the 'collect' mode to execute.
EOF
  echo "PLAN_ONLY: wrote command plan to $OUTDIR/commands.txt"
}

run_collect() {
  mkdir -p "$OUTDIR"
  log_metadata
  touch "$OUTDIR/commands.log"
  cat <<EOF > "$OUTDIR/commands.txt"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$TARGET_PATTERN" "$PACKAGES"
go test -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
go test -race -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
EOF

  run_phase "targeted-late-response" \
    go test -count=1 -timeout "$GO_TEST_TIMEOUT" -run "$TARGET_PATTERN" "$PACKAGES"
  run_phase "stream-full" \
    go test -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"
  run_phase "stream-race" \
    go test -race -count=1 -timeout "$GO_TEST_TIMEOUT" "$PACKAGES"

  {
    echo "targeted-late-response.log"
    echo "stream-full.log"
    echo "stream-race.log"
  } > "$OUTDIR/artifacts.txt"

  if [ "$FAILURES" -gt 0 ]; then
    echo "failed_phases=$FAILURES" > "$OUTDIR/failure.txt"
    echo "COLLECTION_FAILED: ${FAILURES} phase(s) exited non-zero." >&2
    exit 1
  fi

  echo "COLLECTION_OK: captured evidence under $OUTDIR"
  echo "Artifacts listed in $OUTDIR/artifacts.txt"
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
