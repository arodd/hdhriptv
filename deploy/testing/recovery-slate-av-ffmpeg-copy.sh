#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run an end-to-end shared-session recovery continuity check using:
- stream mode: ffmpeg-copy
- recovery keepalive mode: slate_av
- client: cvlc

This harness starts:
1) two local HTTP MPEG-TS sources (primary finite + backup longer) derived from a real sample TS file,
2) a temporary playlist HTTP server,
3) a temporary hdhriptv instance,
4) a cvlc playback session against /auto/v<GuideNumber>.

Pass criteria:
- shared-session recovery keepalive starts in mode=slate_av,
- failover occurs from source_id=1 to source_id=2,
- no fatal stream-open/session-stop errors occur,
- cvlc does not report a fatal open error.

Usage:
  ./deploy/testing/recovery-slate-av-ffmpeg-copy.sh

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
  CVLC_TIMEOUT=40s
  CVLC_NETWORK_CACHING_MS=300

Output artifacts:
  $RUN_DIR/hdhr.out
  $RUN_DIR/cvlc.log
  $RUN_DIR/recovery-lines.txt
  $RUN_DIR/summary.txt
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

assert_port_free() {
  local port="$1"
  if ! python3 - "$port" <<'PY'
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
  then
    echo "port is already in use on 127.0.0.1: $port" >&2
    exit 2
  fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ROOT_DIR="$(cd "${PROJECT_DIR}/.." && pwd)"

require_cmd ffmpeg
require_cmd cvlc
require_cmd jq
require_cmd curl
require_cmd rg
require_cmd python3
require_cmd go
require_cmd timeout

RUN_DIR="${RUN_DIR:-$(mktemp -d /tmp/hdhr-slate-av-ffmpeg-copy-test.XXXXXX)}"
KEEP_RUN_DIR="${KEEP_RUN_DIR:-1}"
HDHR_PORT="${HDHR_PORT:-56014}"
PRIMARY_PORT="${PRIMARY_PORT:-35201}"
BACKUP_PORT="${BACKUP_PORT:-35202}"
PLAYLIST_PORT="${PLAYLIST_PORT:-35203}"
PRIMARY_DURATION_SECONDS="${PRIMARY_DURATION_SECONDS:-8}"
BACKUP_DURATION_SECONDS="${BACKUP_DURATION_SECONDS:-45}"
SAMPLE_TS="${SAMPLE_TS:-${ROOT_DIR}/sample_1280x720_surfing_with_audio.ts}"
CVLC_TIMEOUT="${CVLC_TIMEOUT:-40s}"
CVLC_NETWORK_CACHING_MS="${CVLC_NETWORK_CACHING_MS:-300}"

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
cleanup() {
  local code=$?
  for p in "${PIDS[@]:-}"; do
    if kill -0 "${p}" 2>/dev/null; then
      kill "${p}" 2>/dev/null || true
      wait "${p}" 2>/dev/null || true
    fi
  done
  if [[ "${KEEP_RUN_DIR}" == "0" && "${code}" -eq 0 ]]; then
    rm -rf "${RUN_DIR}"
  fi
}
trap cleanup EXIT INT TERM

cat >"${RUN_DIR}/pace_server.py" <<'PY'
#!/usr/bin/env python3
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TS_FILE = os.environ["TS_FILE"]
PORT = int(os.environ["PORT"])
CHUNK = int(os.environ.get("CHUNK", "1316"))
SLEEP = float(os.environ.get("SLEEP", "0.01"))


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

TS_FILE="${RUN_DIR}/primary.ts" PORT="${PRIMARY_PORT}" python3 "${RUN_DIR}/pace_server.py" >"${RUN_DIR}/source1.log" 2>&1 &
PIDS+=("$!")
TS_FILE="${RUN_DIR}/backup.ts" PORT="${BACKUP_PORT}" python3 "${RUN_DIR}/pace_server.py" >"${RUN_DIR}/source2.log" 2>&1 &
PIDS+=("$!")
python3 -m http.server "${PLAYLIST_PORT}" --bind 127.0.0.1 --directory "${RUN_DIR}" >"${RUN_DIR}/playlist_server.log" 2>&1 &
PIDS+=("$!")

(
  cd "${PROJECT_DIR}"
  DB_PATH="${RUN_DIR}/hdhr.db" \
  LOG_DIR="${RUN_DIR}" \
  HTTP_ADDR=":${HDHR_PORT}" \
  PLAYLIST_URL="http://127.0.0.1:${PLAYLIST_PORT}/playlist.m3u" \
  STREAM_MODE="ffmpeg-copy" \
  FFMPEG_RECONNECT_ENABLED="false" \
  STARTUP_TIMEOUT="5s" \
  FAILOVER_TOTAL_TIMEOUT="20s" \
  MAX_FAILOVERS="0" \
  STALL_POLICY="failover_source" \
  STALL_DETECT="1s" \
  STALL_HARD_DEADLINE="12s" \
  STALL_MAX_FAILOVERS_PER_STALL="3" \
  RECOVERY_FILLER_ENABLED="true" \
  RECOVERY_FILLER_MODE="slate_av" \
  RECOVERY_FILLER_TEXT="Slate AV recovery continuity test" \
  RECOVERY_FILLER_ENABLE_AUDIO="true" \
  TUNER_COUNT="1" \
  go run ./cmd/hdhriptv/main.go >"${RUN_DIR}/hdhr.out" 2>&1
) &
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

set +e
timeout --signal=INT "${CVLC_TIMEOUT}" \
  cvlc -I dummy --play-and-exit \
    --network-caching="${CVLC_NETWORK_CACHING_MS}" \
    --vout=dummy \
    --aout=dummy \
    "http://127.0.0.1:${HDHR_PORT}/auto/v${GUIDE_NUMBER}" >"${RUN_DIR}/cvlc.log" 2>&1
CVLC_EXIT=$?
set -e

sleep 1

rg -n "shared session selected source|shared session recovery keepalive started|shared session recovery keepalive transition boundary published|shared stream session stopped with error|shared session subscriber disconnected|stream subscriber canceled" "${RUN_DIR}/hdhr.out" >"${RUN_DIR}/recovery-lines.txt" || true

HAS_KEEPALIVE_SLATE_AV="$(
  rg -n "shared session recovery keepalive started.*mode=slate_av" "${RUN_DIR}/hdhr.out" >/dev/null && echo yes || echo no
)"
HAS_FAILOVER_SOURCE1_TO_SOURCE2="$(
  awk '/shared session selected source/ && /source_id=1/ {s1=1} /shared session selected source/ && /source_id=2/ {s2=1} END {if (s1 && s2) print "yes"; else print "no"}' "${RUN_DIR}/hdhr.out"
)"
HAS_FATAL_CVLC_OPEN_ERROR="$(
  rg -n "Your input can.t be opened|VLC is unable to open the MRL|HTTP 503 error" "${RUN_DIR}/cvlc.log" >/dev/null && echo yes || echo no
)"
HAS_SESSION_STOP_ERROR="$(
  rg -n "shared stream session stopped with error" "${RUN_DIR}/hdhr.out" >/dev/null && echo yes || echo no
)"
HAS_SESSION_CLOSED_DISCONNECT="$(
  rg -n "shared session subscriber disconnected.*reason=session_closed" "${RUN_DIR}/hdhr.out" >/dev/null && echo yes || echo no
)"

case "${CVLC_EXIT}" in
  0|124|130) CVLC_EXIT_OK=yes ;;
  *) CVLC_EXIT_OK=no ;;
esac

RESULT="PASS"
if [[ "${HAS_KEEPALIVE_SLATE_AV}" != "yes" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_FAILOVER_SOURCE1_TO_SOURCE2}" != "yes" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_FATAL_CVLC_OPEN_ERROR}" != "no" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_SESSION_STOP_ERROR}" != "no" ]]; then
  RESULT="FAIL"
fi
if [[ "${HAS_SESSION_CLOSED_DISCONNECT}" != "no" ]]; then
  RESULT="FAIL"
fi
if [[ "${CVLC_EXIT_OK}" != "yes" ]]; then
  RESULT="FAIL"
fi

{
  echo "result=${RESULT}"
  echo "run_dir=${RUN_DIR}"
  echo "stream_mode=ffmpeg-copy"
  echo "recovery_filler_mode=slate_av"
  echo "sample_ts=${SAMPLE_TS}"
  echo "cvlc_exit=${CVLC_EXIT}"
  echo "cvlc_exit_ok=${CVLC_EXIT_OK}"
  echo "has_keepalive_slate_av=${HAS_KEEPALIVE_SLATE_AV}"
  echo "has_failover_source1_to_source2=${HAS_FAILOVER_SOURCE1_TO_SOURCE2}"
  echo "has_fatal_cvlc_open_error=${HAS_FATAL_CVLC_OPEN_ERROR}"
  echo "has_session_stop_error=${HAS_SESSION_STOP_ERROR}"
  echo "has_session_closed_disconnect=${HAS_SESSION_CLOSED_DISCONNECT}"
  echo
  echo "== recovery-lines =="
  sed -n '1,200p' "${RUN_DIR}/recovery-lines.txt"
  echo
  echo "== cvlc-tail =="
  tail -n 80 "${RUN_DIR}/cvlc.log"
} >"${RUN_DIR}/summary.txt"

echo "result=${RESULT}"
echo "summary=${RUN_DIR}/summary.txt"
echo "run_dir=${RUN_DIR}"

if [[ "${RESULT}" != "PASS" ]]; then
  exit 1
fi
