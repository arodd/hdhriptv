#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run an end-to-end shared-session recovery continuity check using:
- stream mode: ffmpeg-copy
- recovery keepalive mode: slate_av
- client: ffplay (no video output)

This harness starts:
1) two local HTTP MPEG-TS sources (primary finite + backup longer) derived from a real sample TS file,
2) a temporary playlist HTTP server,
3) a temporary hdhriptv instance,
4) a ffplay session against /auto/v<GuideNumber>.

Pass criteria:
- shared-session recovery keepalive starts in mode=slate_av,
- failover occurs from source_id=1 to source_id=2,
- no fatal stream-open/session-stop errors occur,
- ffplay does not report a fatal open error,
- recorder capture exits successfully and emits a TS artifact,
- recorded video packet timestamps are monotonic (no PTS/DTS backtracks),
- transition boundary signal set (`disc,cc,pcr,psi_version`) is observed via `/api/admin/tuners`.
- if `STRICT_CONTINUITY=1`, ffplay continuity warnings fail the run.

Usage:
  ./deploy/testing/recovery-slate-av-ffmpeg-copy-ffplay.sh

Optional env overrides:
  RUN_DIR=/tmp/my-run-dir
  KEEP_RUN_DIR=1
  HDHR_PORT=56014
  PRIMARY_PORT=35201
  BACKUP_PORT=35202
  PLAYLIST_PORT=35203
  PRIMARY_DURATION_SECONDS=8
  BACKUP_DURATION_SECONDS=45
  SAMPLE_TS=/path/to/sample.ts
  FFPLAY_TIMEOUT=40s
  FFPLAY_LOGLEVEL=info
  FFPLAY_AUDIO_DRIVER=dummy
  SOURCE_PACE_CHUNK_BYTES=1316
  SOURCE_PACE_SLEEP=0.003
  STARTUP_TIMEOUT=12s
  SUBSCRIBER_MAX_BLOCKED_WRITE=15s
  STRICT_CONTINUITY=0
  KEEPALIVE_MAX_REALTIME_MULTIPLIER=2.5
  RECORD_DURATION_SECONDS=24
  RECORDER_TIMEOUT=60s
  CLEANUP_PORT_WAIT_RETRIES=40
  CLEANUP_PORT_WAIT_SLEEP_SECONDS=0.1

Output artifacts:
  $RUN_DIR/hdhr.out
  $RUN_DIR/ffplay.log
  $RUN_DIR/recording-ffmpeg.log
  $RUN_DIR/recorded.ts
  $RUN_DIR/recorded-video-packets.json
  $RUN_DIR/recorded-video-pts-values.txt
  $RUN_DIR/recorded-video-pts-monotonic.txt
  $RUN_DIR/ffplay.log.norm
  $RUN_DIR/ffplay-continuity-lines.txt
  $RUN_DIR/recovery-lines.txt
  $RUN_DIR/tuner-status.json
  $RUN_DIR/summary.txt
  $RUN_DIR/cleanup-status.txt
  $RUN_DIR/cleanup-leaked-processes.txt
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

port_is_free() {
  local port="$1"
  python3 - "$port" <<'PY'
import socket
import sys

port = int(sys.argv[1])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    s.bind(("127.0.0.1", port))
except OSError:
    sys.exit(1)
finally:
    s.close()
sys.exit(0)
PY
}

assert_port_free() {
  local port="$1"
  if ! port_is_free "${port}"; then
    echo "port is already in use on 127.0.0.1: $port" >&2
    exit 2
  fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ROOT_DIR="$(cd "${PROJECT_DIR}/.." && pwd)"

require_cmd ffmpeg
require_cmd ffplay
require_cmd ffprobe
require_cmd jq
require_cmd curl
require_cmd rg
require_cmd python3
require_cmd go
require_cmd timeout
require_cmd setsid

RUN_DIR="${RUN_DIR:-$(mktemp -d /tmp/hdhr-slate-av-ffmpeg-copy-ffplay-test.XXXXXX)}"
KEEP_RUN_DIR="${KEEP_RUN_DIR:-1}"
HDHR_PORT="${HDHR_PORT:-56014}"
PRIMARY_PORT="${PRIMARY_PORT:-35201}"
BACKUP_PORT="${BACKUP_PORT:-35202}"
PLAYLIST_PORT="${PLAYLIST_PORT:-35203}"
PRIMARY_DURATION_SECONDS="${PRIMARY_DURATION_SECONDS:-8}"
BACKUP_DURATION_SECONDS="${BACKUP_DURATION_SECONDS:-45}"
SAMPLE_TS="${SAMPLE_TS:-${ROOT_DIR}/sample_1280x720_surfing_with_audio.ts}"
FFPLAY_TIMEOUT="${FFPLAY_TIMEOUT:-40s}"
FFPLAY_LOGLEVEL="${FFPLAY_LOGLEVEL:-info}"
FFPLAY_AUDIO_DRIVER="${FFPLAY_AUDIO_DRIVER:-dummy}"
SOURCE_PACE_CHUNK_BYTES="${SOURCE_PACE_CHUNK_BYTES:-1316}"
SOURCE_PACE_SLEEP="${SOURCE_PACE_SLEEP:-0.003}"
STARTUP_TIMEOUT="${STARTUP_TIMEOUT:-12s}"
SUBSCRIBER_MAX_BLOCKED_WRITE="${SUBSCRIBER_MAX_BLOCKED_WRITE:-15s}"
STRICT_CONTINUITY="${STRICT_CONTINUITY:-0}"
KEEPALIVE_MAX_REALTIME_MULTIPLIER="${KEEPALIVE_MAX_REALTIME_MULTIPLIER:-2.5}"
RECORD_DURATION_SECONDS="${RECORD_DURATION_SECONDS:-24}"
RECORDER_TIMEOUT="${RECORDER_TIMEOUT:-60s}"
CLEANUP_PORT_WAIT_RETRIES="${CLEANUP_PORT_WAIT_RETRIES:-40}"
CLEANUP_PORT_WAIT_SLEEP_SECONDS="${CLEANUP_PORT_WAIT_SLEEP_SECONDS:-0.1}"

if [[ ! -f "${SAMPLE_TS}" ]]; then
  echo "sample TS file not found: ${SAMPLE_TS}" >&2
  echo "set SAMPLE_TS to a valid MPEG-TS fixture path and retry" >&2
  exit 2
fi

assert_port_free "${HDHR_PORT}"
assert_port_free "${PRIMARY_PORT}"
assert_port_free "${BACKUP_PORT}"
assert_port_free "${PLAYLIST_PORT}"

mkdir -p "${RUN_DIR}"
echo "run_dir=${RUN_DIR}"

declare -a PIDS=()
CLEANUP_DONE=0
CLEANUP_PORTS_RELEASED="no"
CLEANUP_PROCESS_LEAK_COUNT="0"
CLEANUP_LEAK_DETECTED="no"
CLEANUP_LEAK_REPORT="${RUN_DIR}/cleanup-leaked-processes.txt"
CLEANUP_STATUS_REPORT="${RUN_DIR}/cleanup-status.txt"
HDHR_BIN="${RUN_DIR}/hdhriptv-harness"

wait_for_port_release() {
  local port="$1"
  local attempt
  for attempt in $(seq 1 "${CLEANUP_PORT_WAIT_RETRIES}"); do
    if port_is_free "${port}"; then
      return 0
    fi
    sleep "${CLEANUP_PORT_WAIT_SLEEP_SECONDS}"
  done
  return 1
}

stop_managed_processes() {
  local pid
  local any_alive
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      kill -TERM -- "-${pid}" 2>/dev/null || true
    fi
  done

  for _ in $(seq 1 40); do
    any_alive=0
    for pid in "${PIDS[@]:-}"; do
      if kill -0 "${pid}" 2>/dev/null; then
        any_alive=1
        break
      fi
    done
    if [[ "${any_alive}" == "0" ]]; then
      break
    fi
    sleep 0.1
  done

  for pid in "${PIDS[@]:-}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      kill -KILL -- "-${pid}" 2>/dev/null || true
    fi
    wait "${pid}" 2>/dev/null || true
  done
}

record_cleanup_status() {
  {
    echo "cleanup_done=yes"
    echo "cleanup_ports_released=${CLEANUP_PORTS_RELEASED}"
    echo "cleanup_process_leak_count=${CLEANUP_PROCESS_LEAK_COUNT}"
    echo "cleanup_leak_detected=${CLEANUP_LEAK_DETECTED}"
    echo "cleanup_leaked_processes_file=${CLEANUP_LEAK_REPORT}"
    if [[ -s "${CLEANUP_LEAK_REPORT}" ]]; then
      echo
      echo "== leaked-processes =="
      sed -n '1,200p' "${CLEANUP_LEAK_REPORT}"
    fi
  } >"${CLEANUP_STATUS_REPORT}"
}

run_cleanup() {
  if [[ "${CLEANUP_DONE}" == "1" ]]; then
    if [[ "${CLEANUP_LEAK_DETECTED}" == "yes" ]]; then
      return 1
    fi
    return 0
  fi
  CLEANUP_DONE=1

  local had_errexit=0
  if [[ $- == *e* ]]; then
    had_errexit=1
    set +e
  fi

  stop_managed_processes

  CLEANUP_PORTS_RELEASED="yes"
  local port
  for port in "${HDHR_PORT}" "${PRIMARY_PORT}" "${BACKUP_PORT}" "${PLAYLIST_PORT}"; do
    if ! wait_for_port_release "${port}"; then
      CLEANUP_PORTS_RELEASED="no"
    fi
  done

  ps -eo pid=,args= \
    | awk -v run_dir="${RUN_DIR}" -v playlist_port="${PLAYLIST_PORT}" '
      index($0, run_dir "/hdhriptv-harness") ||
      index($0, run_dir "/pace_server.py") ||
      (index($0, "http.server " playlist_port) && index($0, run_dir)) { print }
    ' >"${CLEANUP_LEAK_REPORT}"
  CLEANUP_PROCESS_LEAK_COUNT="$(wc -l <"${CLEANUP_LEAK_REPORT}" | awk '{print $1}')"

  CLEANUP_LEAK_DETECTED="no"
  if [[ "${CLEANUP_PORTS_RELEASED}" != "yes" || "${CLEANUP_PROCESS_LEAK_COUNT}" != "0" ]]; then
    CLEANUP_LEAK_DETECTED="yes"
  fi

  record_cleanup_status

  if [[ "${had_errexit}" == "1" ]]; then
    set -e
  fi

  if [[ "${CLEANUP_LEAK_DETECTED}" == "yes" ]]; then
    echo "cleanup leak detected; see ${CLEANUP_STATUS_REPORT}" >&2
    return 1
  fi
  return 0
}

cleanup_trap() {
  local code=$?
  trap - EXIT INT TERM

  if ! run_cleanup; then
    code=1
  fi
  if [[ "${KEEP_RUN_DIR}" == "0" && "${code}" -eq 0 ]]; then
    rm -rf "${RUN_DIR}"
  fi
  exit "${code}"
}
trap cleanup_trap EXIT INT TERM

cat >"${RUN_DIR}/pace_server.py" <<'PY'
#!/usr/bin/env python3
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TS_FILE = os.environ["TS_FILE"]
PORT = int(os.environ["PORT"])
CHUNK = int(os.environ.get("CHUNK", "1316"))
SLEEP = float(os.environ.get("SLEEP", "0.003"))


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/live.ts":
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "video/MP2T")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        try:
            with open(TS_FILE, "rb") as f:
                while True:
                    chunk = f.read(CHUNK)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    self.wfile.flush()
                    time.sleep(SLEEP)
        except (BrokenPipeError, ConnectionResetError):
            return

    def log_message(self, _fmt, *_args):
        return


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
PY
chmod +x "${RUN_DIR}/pace_server.py"

ffmpeg -hide_banner -loglevel error -y \
  -stream_loop -1 -i "${SAMPLE_TS}" \
  -t "${PRIMARY_DURATION_SECONDS}" \
  -map 0 \
  -c copy \
  -f mpegts "${RUN_DIR}/primary.ts"

ffmpeg -hide_banner -loglevel error -y \
  -stream_loop -1 -i "${SAMPLE_TS}" \
  -t "${BACKUP_DURATION_SECONDS}" \
  -map 0 \
  -c copy \
  -f mpegts "${RUN_DIR}/backup.ts"

cat >"${RUN_DIR}/playlist.m3u" <<EOF
#EXTM3U
#EXTINF:-1 tvg-id="slate.test.primary" group-title="Test",Slate Failover Primary
http://127.0.0.1:${PRIMARY_PORT}/live.ts
#EXTINF:-1 tvg-id="slate.test.backup" group-title="Test",Slate Failover Backup
http://127.0.0.1:${BACKUP_PORT}/live.ts
EOF

(
  cd "${PROJECT_DIR}"
  go build -o "${HDHR_BIN}" ./cmd/hdhriptv
)

TS_FILE="${RUN_DIR}/primary.ts" PORT="${PRIMARY_PORT}" CHUNK="${SOURCE_PACE_CHUNK_BYTES}" SLEEP="${SOURCE_PACE_SLEEP}" \
  setsid python3 "${RUN_DIR}/pace_server.py" >"${RUN_DIR}/source1.log" 2>&1 &
PIDS+=("$!")
TS_FILE="${RUN_DIR}/backup.ts" PORT="${BACKUP_PORT}" CHUNK="${SOURCE_PACE_CHUNK_BYTES}" SLEEP="${SOURCE_PACE_SLEEP}" \
  setsid python3 "${RUN_DIR}/pace_server.py" >"${RUN_DIR}/source2.log" 2>&1 &
PIDS+=("$!")
setsid python3 -m http.server "${PLAYLIST_PORT}" --bind 127.0.0.1 --directory "${RUN_DIR}" >"${RUN_DIR}/playlist_server.log" 2>&1 &
PIDS+=("$!")

setsid env \
  DB_PATH="${RUN_DIR}/hdhr.db" \
  LOG_DIR="${RUN_DIR}" \
  HTTP_ADDR=":${HDHR_PORT}" \
  PLAYLIST_URL="http://127.0.0.1:${PLAYLIST_PORT}/playlist.m3u" \
  STREAM_MODE="ffmpeg-copy" \
  FFMPEG_RECONNECT_ENABLED="false" \
  STARTUP_TIMEOUT="${STARTUP_TIMEOUT}" \
  FAILOVER_TOTAL_TIMEOUT="20s" \
  MAX_FAILOVERS="0" \
  STALL_POLICY="failover_source" \
  STALL_DETECT="1s" \
  STALL_HARD_DEADLINE="12s" \
  STALL_MAX_FAILOVERS_PER_STALL="3" \
  SUBSCRIBER_MAX_BLOCKED_WRITE="${SUBSCRIBER_MAX_BLOCKED_WRITE}" \
  RECOVERY_FILLER_ENABLED="true" \
  RECOVERY_FILLER_MODE="slate_av" \
  RECOVERY_FILLER_TEXT="Slate AV recovery continuity test" \
  RECOVERY_FILLER_ENABLE_AUDIO="true" \
  TUNER_COUNT="1" \
  "${HDHR_BIN}" >"${RUN_DIR}/hdhr.out" 2>&1 &
PIDS+=("$!")

for _ in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:${HDHR_PORT}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done
curl -fsS "http://127.0.0.1:${HDHR_PORT}/healthz" >/dev/null

curl -fsS -X POST "http://127.0.0.1:${HDHR_PORT}/api/admin/jobs/playlist-sync/run" >"${RUN_DIR}/playlist-sync-trigger.json" || true

ITEMS_JSON=""
for _ in $(seq 1 40); do
  ITEMS_JSON="$(curl -fsS "http://127.0.0.1:${HDHR_PORT}/api/items?limit=100")"
  count="$(printf '%s' "${ITEMS_JSON}" | jq -r '.items | length')"
  if [[ "${count}" -ge 2 ]]; then
    break
  fi
  sleep 0.25
done
printf '%s' "${ITEMS_JSON}" >"${RUN_DIR}/items.json"

PRIMARY_KEY="$(printf '%s' "${ITEMS_JSON}" | jq -r '.items[] | select(.item_key | startswith("src:slate.test.primary:")) | .item_key' | head -n1)"
BACKUP_KEY="$(printf '%s' "${ITEMS_JSON}" | jq -r '.items[] | select(.item_key | startswith("src:slate.test.backup:")) | .item_key' | head -n1)"
if [[ -z "${PRIMARY_KEY}" || "${PRIMARY_KEY}" == "null" || -z "${BACKUP_KEY}" || "${BACKUP_KEY}" == "null" ]]; then
  echo "failed to resolve source item keys from /api/items; see ${RUN_DIR}/items.json" >&2
  exit 1
fi

CREATE_RESP="$(
  curl -fsS -X POST \
    -H 'Content-Type: application/json' \
    -d "{\"item_key\":\"${PRIMARY_KEY}\"}" \
    "http://127.0.0.1:${HDHR_PORT}/api/channels"
)"
printf '%s' "${CREATE_RESP}" >"${RUN_DIR}/create-channel.json"

CHANNEL_ID="$(printf '%s' "${CREATE_RESP}" | jq -r '.channel_id')"
GUIDE_NUMBER="$(printf '%s' "${CREATE_RESP}" | jq -r '.guide_number')"
if [[ -z "${CHANNEL_ID}" || "${CHANNEL_ID}" == "null" || -z "${GUIDE_NUMBER}" || "${GUIDE_NUMBER}" == "null" ]]; then
  echo "failed to create channel; see ${RUN_DIR}/create-channel.json" >&2
  exit 1
fi

curl -fsS -X POST \
  -H 'Content-Type: application/json' \
  -d "{\"item_key\":\"${BACKUP_KEY}\",\"allow_cross_channel\":true}" \
  "http://127.0.0.1:${HDHR_PORT}/api/channels/${CHANNEL_ID}/sources" >"${RUN_DIR}/add-source.json"

declare -a FFPLAY_ARGS=(
  -hide_banner
  -loglevel "${FFPLAY_LOGLEVEL}"
  -stats
  -nodisp
  -autoexit
  -f mpegts
)
FFPLAY_ARGS+=("http://127.0.0.1:${HDHR_PORT}/auto/v${GUIDE_NUMBER}")
RECORDED_TS="${RUN_DIR}/recorded.ts"
RECORDER_LOG="${RUN_DIR}/recording-ffmpeg.log"

set +e
setsid timeout --signal=INT "${FFPLAY_TIMEOUT}" \
  env SDL_AUDIODRIVER="${FFPLAY_AUDIO_DRIVER}" SDL_VIDEODRIVER="${SDL_VIDEODRIVER:-dummy}" \
  ffplay "${FFPLAY_ARGS[@]}" >"${RUN_DIR}/ffplay.log" 2>&1 &
FFPLAY_PID=$!
PIDS+=("${FFPLAY_PID}")

setsid timeout --signal=INT "${RECORDER_TIMEOUT}" \
  ffmpeg -hide_banner -loglevel error -y \
  -t "${RECORD_DURATION_SECONDS}" \
  -i "http://127.0.0.1:${HDHR_PORT}/auto/v${GUIDE_NUMBER}" \
  -map 0 \
  -c copy \
  -f mpegts "${RECORDED_TS}" >"${RECORDER_LOG}" 2>&1 &
RECORDER_PID=$!
PIDS+=("${RECORDER_PID}")

TUNER_STATUS_JSON="${RUN_DIR}/tuner-status.json"
TUNER_STATUS_TMP="${RUN_DIR}/tuner-status.tmp.json"
rm -f "${TUNER_STATUS_JSON}" "${TUNER_STATUS_TMP}"
for _ in $(seq 1 160); do
  if ! kill -0 "${FFPLAY_PID}" 2>/dev/null; then
    break
  fi
  if curl -fsS "http://127.0.0.1:${HDHR_PORT}/api/admin/tuners" >"${TUNER_STATUS_TMP}" 2>/dev/null; then
    mv "${TUNER_STATUS_TMP}" "${TUNER_STATUS_JSON}"
    if jq -e 'any(.tuners[]?; ((.recovery_transition_signals_applied // "") | length) > 0)' "${TUNER_STATUS_JSON}" >/dev/null 2>&1; then
      break
    fi
  fi
  sleep 0.25
done

wait "${FFPLAY_PID}"
FFPLAY_EXIT=$?
wait "${RECORDER_PID}"
RECORDER_EXIT=$?
set -e

sleep 1

if [[ ! -s "${TUNER_STATUS_JSON}" ]]; then
  curl -fsS "http://127.0.0.1:${HDHR_PORT}/api/admin/tuners" >"${TUNER_STATUS_JSON}" || true
fi

tr '\r' '\n' <"${RUN_DIR}/ffplay.log" >"${RUN_DIR}/ffplay.log.norm"

rg -n "shared session selected source|shared session startup probe cutover warning|shared session recovery keepalive started|shared session recovery keepalive transition boundary published|shared stream session stopped with error|shared session subscriber disconnected|stream subscriber canceled" "${RUN_DIR}/hdhr.out" >"${RUN_DIR}/recovery-lines.txt" || true
FFPLAY_PLAYBACK_LOG="${RUN_DIR}/ffplay.log.playback.norm"
if awk '/^Input #0, mpegts/ {capture=1} capture {print}' "${RUN_DIR}/ffplay.log.norm" >"${FFPLAY_PLAYBACK_LOG}" && [[ -s "${FFPLAY_PLAYBACK_LOG}" ]]; then
  FFPLAY_CONTINUITY_SCAN_LOG="${FFPLAY_PLAYBACK_LOG}"
  FFPLAY_CONTINUITY_SCAN_SCOPE="post_input_probe"
else
  FFPLAY_CONTINUITY_SCAN_LOG="${RUN_DIR}/ffplay.log.norm"
  FFPLAY_CONTINUITY_SCAN_SCOPE="full_log_fallback"
fi
rg -n "(?i)(Continuity check failed|TS discontinuity|TS duplicate|Packet corrupt|PES packet size mismatch|non monotonically increasing dts|DTS .* invalid dropping|decode_slice_header error|Error decoding the extradata|non-existing PPS|sps_id .* out of range|missing picture in access unit|buffer deadlock prevented)" "${FFPLAY_CONTINUITY_SCAN_LOG}" >"${RUN_DIR}/ffplay-continuity-lines.txt" || true

HAS_KEEPALIVE_SLATE_AV="$(
  rg -n "shared session recovery keepalive started.*mode=slate_av" "${RUN_DIR}/hdhr.out" >/dev/null && echo yes || echo no
)"
HAS_FAILOVER_SOURCE1_TO_SOURCE2="$(
  awk '/shared session selected source/ && /source_id=1/ {s1=1} /shared session selected source/ && /source_id=2/ {s2=1} END {if (s1 && s2) print "yes"; else print "no"}' "${RUN_DIR}/hdhr.out"
)"
HAS_FATAL_FFPLAY_OPEN_ERROR="$(
  rg -n "Failed to open file|Connection refused|Server returned (4|5)[0-9]{2}|HTTP error|No such file or directory|Protocol not found|Connection timed out" "${RUN_DIR}/ffplay.log.norm" >/dev/null && echo yes || echo no
)"
HAS_SESSION_STOP_ERROR="$(
  rg -n "shared stream session stopped with error" "${RUN_DIR}/hdhr.out" >/dev/null && echo yes || echo no
)"
HAS_SESSION_CLOSED_DISCONNECT="$(
  rg -n "shared session subscriber disconnected.*reason=session_closed" "${RUN_DIR}/hdhr.out" >/dev/null && echo yes || echo no
)"
HAS_STREAM_WRITE_ERROR="$(
  rg -n "stream subscriber ended with error|reason=stream_write_error" "${RUN_DIR}/hdhr.out" >/dev/null && echo yes || echo no
)"
CONTINUITY_ISSUE_COUNT="$(
  wc -l <"${RUN_DIR}/ffplay-continuity-lines.txt" | awk '{print $1}'
)"
HAS_CONTINUITY_ISSUES="no"
if [[ "${CONTINUITY_ISSUE_COUNT}" != "0" ]]; then
  HAS_CONTINUITY_ISSUES="yes"
fi

case "${RECORDER_EXIT}" in
  0) RECORDER_EXIT_OK=yes ;;
  *) RECORDER_EXIT_OK=no ;;
esac

HAS_RECORDING_CAPTURE="no"
RECORDING_BYTES="0"
if [[ -s "${RECORDED_TS}" ]]; then
  HAS_RECORDING_CAPTURE="yes"
  RECORDING_BYTES="$(wc -c <"${RECORDED_TS}" | awk '{print $1}')"
fi

VIDEO_PACKET_JSON="${RUN_DIR}/recorded-video-packets.json"
VIDEO_PTS_VALUES="${RUN_DIR}/recorded-video-pts-values.txt"
VIDEO_PTS_MONOTONIC_REPORT="${RUN_DIR}/recorded-video-pts-monotonic.txt"
VIDEO_PTS_PACKET_COUNT="0"
VIDEO_PTS_BACKTRACK_COUNT="0"
VIDEO_PTS_FIRST=""
VIDEO_PTS_LAST=""
HAS_RECORDING_VIDEO_PTS="no"
HAS_MONOTONIC_VIDEO_PTS="no"

if [[ "${HAS_RECORDING_CAPTURE}" == "yes" ]]; then
  if ffprobe -hide_banner -loglevel error \
    -select_streams v:0 \
    -show_packets \
    -show_entries packet=pts_time,dts_time \
    -of json \
    "${RECORDED_TS}" >"${VIDEO_PACKET_JSON}" 2>"${RUN_DIR}/recorded-video-packets.stderr"; then
    # Packet order can include B-frame PTS reordering; prefer DTS for
    # monotonicity checks and fall back to PTS when DTS is absent.
    jq -r '.packets[]? | (.dts_time // .pts_time // empty)' "${VIDEO_PACKET_JSON}" >"${VIDEO_PTS_VALUES}" || true
    PTS_SUMMARY="$(
      python3 - "${VIDEO_PTS_VALUES}" "${VIDEO_PTS_MONOTONIC_REPORT}" <<'PY'
import decimal
import sys

pts_path = sys.argv[1]
report_path = sys.argv[2]

count = 0
backtracks = 0
first = None
last = None
prev = None

with open(pts_path, "r", encoding="utf-8") as fh:
    for raw in fh:
        value = raw.strip()
        if not value:
            continue
        try:
            pts = decimal.Decimal(value)
        except decimal.InvalidOperation:
            continue
        if first is None:
            first = pts
        if prev is not None and pts < prev:
            backtracks += 1
        prev = pts
        last = pts
        count += 1

first_str = "" if first is None else format(first, "f")
last_str = "" if last is None else format(last, "f")
status = "pass" if count > 0 and backtracks == 0 else "fail"

with open(report_path, "w", encoding="utf-8") as fh:
    fh.write(f"status={status}\n")
    fh.write(f"packet_count={count}\n")
    fh.write(f"backtrack_count={backtracks}\n")
    fh.write(f"first_pts={first_str}\n")
    fh.write(f"last_pts={last_str}\n")

print(f"{count}|{backtracks}|{first_str}|{last_str}")
PY
    )"
    IFS='|' read -r VIDEO_PTS_PACKET_COUNT VIDEO_PTS_BACKTRACK_COUNT VIDEO_PTS_FIRST VIDEO_PTS_LAST <<<"${PTS_SUMMARY}"
    if [[ "${VIDEO_PTS_PACKET_COUNT}" != "0" ]]; then
      HAS_RECORDING_VIDEO_PTS="yes"
      if [[ "${VIDEO_PTS_BACKTRACK_COUNT}" == "0" ]]; then
        HAS_MONOTONIC_VIDEO_PTS="yes"
      fi
    fi
  else
    : >"${VIDEO_PACKET_JSON}"
    : >"${VIDEO_PTS_VALUES}"
    {
      echo "status=fail"
      echo "packet_count=0"
      echo "backtrack_count=0"
      echo "first_pts="
      echo "last_pts="
      echo "error=ffprobe_failed"
    } >"${VIDEO_PTS_MONOTONIC_REPORT}"
  fi
else
  : >"${VIDEO_PACKET_JSON}"
  : >"${VIDEO_PTS_VALUES}"
  {
    echo "status=fail"
    echo "packet_count=0"
    echo "backtrack_count=0"
    echo "first_pts="
    echo "last_pts="
    echo "error=no_recording"
  } >"${VIDEO_PTS_MONOTONIC_REPORT}"
fi

HAS_BOUNDARY_SIGNAL_SET="$(
  jq -r '
    if any(.tuners[]?;
      ((.recovery_transition_signals_applied // "") | split(",")) as $signals
      | (["disc","cc","pcr","psi_version"] | all(. as $required | ($signals | index($required))))
    )
    then "yes" else "no" end
  ' "${TUNER_STATUS_JSON}" 2>/dev/null || echo no
)"
HAS_BOUNDARY_STITCH_APPLIED="$(
  jq -r 'if any(.tuners[]?; (.recovery_transition_stitch_applied // false) == true) then "yes" else "no" end' "${TUNER_STATUS_JSON}" 2>/dev/null || echo no
)"
BOUNDARY_SIGNAL_VALUES="$(
  jq -r '[.tuners[]?.recovery_transition_signals_applied | select(. != null and . != "")] | unique | join(";")' "${TUNER_STATUS_JSON}" 2>/dev/null || true
)"
BOUNDARY_SIGNAL_SKIPS="$(
  jq -r '[.tuners[]?.recovery_transition_signal_skips | select(. != null and . != "")] | unique | join(";")' "${TUNER_STATUS_JSON}" 2>/dev/null || true
)"
BOUNDARY_STITCH_VALUES="$(
  jq -r '[.tuners[]?.recovery_transition_stitch_applied | tostring] | unique | join(",")' "${TUNER_STATUS_JSON}" 2>/dev/null || true
)"
KEEPALIVE_STARTED_AT_VALUES="$(
  jq -r '[.tuners[]?.recovery_keepalive_started_at | select(. != null and . != "")] | unique | join(";")' "${TUNER_STATUS_JSON}" 2>/dev/null || true
)"
KEEPALIVE_STOPPED_AT_VALUES="$(
  jq -r '[.tuners[]?.recovery_keepalive_stopped_at | select(. != null and . != "")] | unique | join(";")' "${TUNER_STATUS_JSON}" 2>/dev/null || true
)"
KEEPALIVE_DURATION_VALUES="$(
  jq -r '[.tuners[]?.recovery_keepalive_duration | select(. != null and . != "")] | unique | join(";")' "${TUNER_STATUS_JSON}" 2>/dev/null || true
)"
KEEPALIVE_BYTES="$(
  jq -r '[.tuners[]?.recovery_keepalive_bytes // 0] | max // 0' "${TUNER_STATUS_JSON}" 2>/dev/null || echo 0
)"
KEEPALIVE_CHUNKS="$(
  jq -r '[.tuners[]?.recovery_keepalive_chunks // 0] | max // 0' "${TUNER_STATUS_JSON}" 2>/dev/null || echo 0
)"
KEEPALIVE_RATE_BYTES_PER_SECOND="$(
  jq -r '[.tuners[]?.recovery_keepalive_rate_bytes_per_second // 0] | max // 0' "${TUNER_STATUS_JSON}" 2>/dev/null || echo 0
)"
KEEPALIVE_EXPECTED_RATE_BYTES_PER_SECOND="$(
  jq -r '[.tuners[]?.recovery_keepalive_expected_rate_bytes_per_second // 0] | max // 0' "${TUNER_STATUS_JSON}" 2>/dev/null || echo 0
)"
KEEPALIVE_REALTIME_MULTIPLIER="$(
  jq -r '[.tuners[]?.recovery_keepalive_realtime_multiplier // 0] | max // 0' "${TUNER_STATUS_JSON}" 2>/dev/null || echo 0
)"
KEEPALIVE_GUARDRAIL_COUNT="$(
  jq -r '[.tuners[]?.recovery_keepalive_guardrail_count // 0] | max // 0' "${TUNER_STATUS_JSON}" 2>/dev/null || echo 0
)"
KEEPALIVE_GUARDRAIL_REASON="$(
  jq -r '[.tuners[]?.recovery_keepalive_guardrail_reason | select(. != null and . != "")] | unique | join(";")' "${TUNER_STATUS_JSON}" 2>/dev/null || true
)"
STARTUP_PROBE_RAW_BYTES="$(
  jq -r '[.tuners[]?.source_startup_probe_raw_bytes // 0] | max // 0' "${TUNER_STATUS_JSON}" 2>/dev/null || echo 0
)"
STARTUP_PROBE_TRIMMED_BYTES="$(
  jq -r '[.tuners[]?.source_startup_probe_trimmed_bytes // 0] | max // 0' "${TUNER_STATUS_JSON}" 2>/dev/null || echo 0
)"
STARTUP_PROBE_CUTOVER_OFFSET="$(
  jq -r '[.tuners[]?.source_startup_probe_cutover_offset // 0] | max // 0' "${TUNER_STATUS_JSON}" 2>/dev/null || echo 0
)"
STARTUP_PROBE_DROPPED_BYTES="$(
  jq -r '[.tuners[]?.source_startup_probe_dropped_bytes // 0] | max // 0' "${TUNER_STATUS_JSON}" 2>/dev/null || echo 0
)"
STARTUP_PROBE_CUTOVER_PERCENT="$(
  python3 - "${STARTUP_PROBE_RAW_BYTES}" "${STARTUP_PROBE_DROPPED_BYTES}" <<'PY'
import math
import sys

try:
    raw = float(sys.argv[1])
except Exception:
    raw = 0.0
try:
    dropped = float(sys.argv[2])
except Exception:
    dropped = 0.0

if not math.isfinite(raw) or raw <= 0:
    print("0")
else:
    if not math.isfinite(dropped) or dropped <= 0:
        print("0")
    else:
        pct = max(0.0, min(100.0, (dropped / raw) * 100.0))
        print(f"{pct:.1f}")
PY
)"
HAS_STARTUP_HIGH_CUTOVER="no"
if python3 - "${STARTUP_PROBE_CUTOVER_PERCENT}" <<'PY'
import math
import sys

try:
    pct = float(sys.argv[1])
except Exception:
    pct = 0.0

if not math.isfinite(pct):
    pct = 0.0
sys.exit(0 if pct >= 75.0 else 1)
PY
then
  HAS_STARTUP_HIGH_CUTOVER="yes"
fi
STARTUP_CUTOVER_WARNING_COUNT="$(
  (rg -n "shared session startup probe cutover warning" "${RUN_DIR}/hdhr.out" || true) | wc -l | awk '{print $1}'
)"
KEEPALIVE_RATE_RATIO="$(
  python3 - "${KEEPALIVE_RATE_BYTES_PER_SECOND}" "${KEEPALIVE_EXPECTED_RATE_BYTES_PER_SECOND}" <<'PY'
import math
import sys

try:
    rate = float(sys.argv[1])
except Exception:
    rate = 0.0
try:
    expected = float(sys.argv[2])
except Exception:
    expected = 0.0

if not math.isfinite(rate):
    rate = 0.0
if not math.isfinite(expected):
    expected = 0.0

if expected <= 0:
    print("0")
else:
    print(f"{(rate / expected):.3f}")
PY
)"
HAS_KEEPALIVE_OVER_MULTIPLIER="$(
  python3 - "${KEEPALIVE_REALTIME_MULTIPLIER}" "${KEEPALIVE_MAX_REALTIME_MULTIPLIER}" <<'PY'
import math
import sys

try:
    multiplier = float(sys.argv[1])
except Exception:
    multiplier = 0.0
try:
    limit = float(sys.argv[2])
except Exception:
    limit = 0.0

if not math.isfinite(multiplier):
    multiplier = 0.0
if not math.isfinite(limit):
    limit = 0.0

if limit > 0 and multiplier > limit:
    print("yes")
else:
    print("no")
PY
)"
HAS_KEEPALIVE_GUARDRAIL_TRIGGER="no"
if [[ "${KEEPALIVE_GUARDRAIL_COUNT}" != "0" ]]; then
  HAS_KEEPALIVE_GUARDRAIL_TRIGGER="yes"
fi

case "${FFPLAY_EXIT}" in
  0|124|130) FFPLAY_EXIT_OK=yes ;;
  *) FFPLAY_EXIT_OK=no ;;
esac

RESULT="PASS"
if [[ "${HAS_KEEPALIVE_SLATE_AV}" != "yes" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_FAILOVER_SOURCE1_TO_SOURCE2}" != "yes" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_FATAL_FFPLAY_OPEN_ERROR}" != "no" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_SESSION_STOP_ERROR}" != "no" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_SESSION_CLOSED_DISCONNECT}" != "no" ]]; then
  RESULT="FAIL"
fi
if [[ "${FFPLAY_EXIT_OK}" != "yes" ]]; then
  RESULT="FAIL"
fi
if [[ "${RECORDER_EXIT_OK}" != "yes" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_RECORDING_CAPTURE}" != "yes" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_RECORDING_VIDEO_PTS}" != "yes" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_MONOTONIC_VIDEO_PTS}" != "yes" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_BOUNDARY_SIGNAL_SET}" != "yes" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_KEEPALIVE_GUARDRAIL_TRIGGER}" != "no" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_KEEPALIVE_OVER_MULTIPLIER}" != "no" ]]; then
  RESULT="FAIL"
fi
case "${STRICT_CONTINUITY,,}" in
  1|true|yes|on)
    if [[ "${HAS_CONTINUITY_ISSUES}" != "no" ]]; then
      RESULT="FAIL"
    fi
    ;;
esac

if ! run_cleanup; then
  RESULT="FAIL"
fi

{
  echo "result=${RESULT}"
  echo "run_dir=${RUN_DIR}"
  echo "stream_mode=ffmpeg-copy"
  echo "recovery_filler_mode=slate_av"
  echo "sample_ts=${SAMPLE_TS}"
  echo "ffplay_exit=${FFPLAY_EXIT}"
  echo "ffplay_exit_ok=${FFPLAY_EXIT_OK}"
  echo "ffplay_loglevel=${FFPLAY_LOGLEVEL}"
  echo "ffplay_audio_driver=${FFPLAY_AUDIO_DRIVER}"
  echo "recorder_exit=${RECORDER_EXIT}"
  echo "recorder_exit_ok=${RECORDER_EXIT_OK}"
  echo "record_duration_seconds=${RECORD_DURATION_SECONDS}"
  echo "recorder_timeout=${RECORDER_TIMEOUT}"
  echo "recording_bytes=${RECORDING_BYTES}"
  echo "has_recording_capture=${HAS_RECORDING_CAPTURE}"
  echo "has_recording_video_pts=${HAS_RECORDING_VIDEO_PTS}"
  echo "has_monotonic_video_pts=${HAS_MONOTONIC_VIDEO_PTS}"
  echo "video_pts_packet_count=${VIDEO_PTS_PACKET_COUNT}"
  echo "video_pts_backtrack_count=${VIDEO_PTS_BACKTRACK_COUNT}"
  echo "video_pts_first=${VIDEO_PTS_FIRST}"
  echo "video_pts_last=${VIDEO_PTS_LAST}"
  echo "source_pace_chunk_bytes=${SOURCE_PACE_CHUNK_BYTES}"
  echo "source_pace_sleep=${SOURCE_PACE_SLEEP}"
  echo "startup_timeout=${STARTUP_TIMEOUT}"
  echo "ffplay_continuity_scan_scope=${FFPLAY_CONTINUITY_SCAN_SCOPE}"
  echo "subscriber_max_blocked_write=${SUBSCRIBER_MAX_BLOCKED_WRITE}"
  echo "strict_continuity=${STRICT_CONTINUITY}"
  echo "cleanup_status_report=${CLEANUP_STATUS_REPORT}"
  echo "cleanup_leak_detected=${CLEANUP_LEAK_DETECTED}"
  echo "cleanup_ports_released=${CLEANUP_PORTS_RELEASED}"
  echo "cleanup_process_leak_count=${CLEANUP_PROCESS_LEAK_COUNT}"
  echo "has_keepalive_slate_av=${HAS_KEEPALIVE_SLATE_AV}"
  echo "has_failover_source1_to_source2=${HAS_FAILOVER_SOURCE1_TO_SOURCE2}"
  echo "has_fatal_ffplay_open_error=${HAS_FATAL_FFPLAY_OPEN_ERROR}"
  echo "has_session_stop_error=${HAS_SESSION_STOP_ERROR}"
  echo "has_session_closed_disconnect=${HAS_SESSION_CLOSED_DISCONNECT}"
  echo "has_stream_write_error=${HAS_STREAM_WRITE_ERROR}"
  echo "has_continuity_issues=${HAS_CONTINUITY_ISSUES}"
  echo "continuity_issue_count=${CONTINUITY_ISSUE_COUNT}"
  echo "has_boundary_signal_set=${HAS_BOUNDARY_SIGNAL_SET}"
  echo "has_boundary_stitch_applied=${HAS_BOUNDARY_STITCH_APPLIED}"
  echo "boundary_signal_values=${BOUNDARY_SIGNAL_VALUES}"
  echo "boundary_signal_skips=${BOUNDARY_SIGNAL_SKIPS}"
  echo "boundary_stitch_values=${BOUNDARY_STITCH_VALUES}"
  echo "keepalive_started_at_values=${KEEPALIVE_STARTED_AT_VALUES}"
  echo "keepalive_stopped_at_values=${KEEPALIVE_STOPPED_AT_VALUES}"
  echo "keepalive_duration_values=${KEEPALIVE_DURATION_VALUES}"
  echo "keepalive_bytes=${KEEPALIVE_BYTES}"
  echo "keepalive_chunks=${KEEPALIVE_CHUNKS}"
  echo "keepalive_rate_bytes_per_second=${KEEPALIVE_RATE_BYTES_PER_SECOND}"
  echo "keepalive_expected_rate_bytes_per_second=${KEEPALIVE_EXPECTED_RATE_BYTES_PER_SECOND}"
  echo "keepalive_rate_ratio=${KEEPALIVE_RATE_RATIO}"
  echo "keepalive_realtime_multiplier=${KEEPALIVE_REALTIME_MULTIPLIER}"
  echo "keepalive_max_realtime_multiplier=${KEEPALIVE_MAX_REALTIME_MULTIPLIER}"
  echo "has_keepalive_over_multiplier=${HAS_KEEPALIVE_OVER_MULTIPLIER}"
  echo "keepalive_guardrail_count=${KEEPALIVE_GUARDRAIL_COUNT}"
  echo "has_keepalive_guardrail_trigger=${HAS_KEEPALIVE_GUARDRAIL_TRIGGER}"
  echo "keepalive_guardrail_reason=${KEEPALIVE_GUARDRAIL_REASON}"
  echo "startup_probe_raw_bytes=${STARTUP_PROBE_RAW_BYTES}"
  echo "startup_probe_trimmed_bytes=${STARTUP_PROBE_TRIMMED_BYTES}"
  echo "startup_probe_cutover_offset=${STARTUP_PROBE_CUTOVER_OFFSET}"
  echo "startup_probe_dropped_bytes=${STARTUP_PROBE_DROPPED_BYTES}"
  echo "startup_probe_cutover_percent=${STARTUP_PROBE_CUTOVER_PERCENT}"
  echo "has_startup_high_cutover=${HAS_STARTUP_HIGH_CUTOVER}"
  echo "startup_cutover_warning_count=${STARTUP_CUTOVER_WARNING_COUNT}"
  echo
  echo "== recovery-lines =="
  sed -n '1,200p' "${RUN_DIR}/recovery-lines.txt"
  echo
  echo "== tuner-status =="
  sed -n '1,220p' "${TUNER_STATUS_JSON}"
  echo
  echo "== ffplay-continuity-lines =="
  sed -n '1,200p' "${RUN_DIR}/ffplay-continuity-lines.txt"
  echo
  echo "== recorded-video-pts-monotonic =="
  sed -n '1,120p' "${VIDEO_PTS_MONOTONIC_REPORT}"
  echo
  echo "== recording-ffmpeg-tail =="
  tail -n 80 "${RECORDER_LOG}" || true
  echo
  echo "== ffplay-tail =="
  tail -n 120 "${RUN_DIR}/ffplay.log.norm"
  echo
  echo "== cleanup-status =="
  sed -n '1,120p' "${CLEANUP_STATUS_REPORT}"
} >"${RUN_DIR}/summary.txt"

echo "result=${RESULT}"
echo "summary=${RUN_DIR}/summary.txt"
echo "run_dir=${RUN_DIR}"
echo "cleanup_status=${CLEANUP_STATUS_REPORT}"

if [[ "${RESULT}" != "PASS" ]]; then
  exit 1
fi
