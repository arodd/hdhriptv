# hdhriptv

**Turn any M3U/IPTV playlist into a HDHomeRun tuner that Plex, Jellyfin, Emby, and other DVR software discover automatically.**

## What is hdhriptv?

hdhriptv is a lightweight server that makes your IPTV playlist look like a physical [HDHomeRun](https://www.silicondust.com/) TV tuner on your network. HDHomeRun is the most widely supported network TV tuner standard -- nearly every DVR application can detect and use one automatically. hdhriptv takes advantage of that by speaking the same protocol, so you can plug your IPTV channels into the DVR workflow you already use without any special client-side setup.

Point hdhriptv at an M3U playlist, curate the channels you want to publish through a built-in admin UI, and your DVR will discover it automatically -- just like a real tuner. From there, your DVR handles the rest -- guide data, recordings, live TV, and multi-device playback -- all using channels from your IPTV subscription.

hdhriptv is written in Go with an embedded SQLite database -- no external database required. It runs as a single binary, in Docker (multi-arch: amd64 and arm64), or as a systemd service. The only external dependency is ffmpeg, needed for the default `ffmpeg-copy` stream mode. If you use `direct` proxy mode, no ffmpeg is required. The Docker image includes ffmpeg out of the box.

## Key Features

### Channel Management
- Import channels from any M3U/IPTV playlist
- Curate which channels to publish through a web-based admin UI
- Attach multiple sources per channel with ordered failover priority
- Dynamic channel rules that automatically sync matching catalog items into channel sources
- Dynamic channel blocks for bulk channel creation from filter queries
- Merge/duplicate detection to identify overlapping catalog entries

### Streaming
- Three stream modes: direct proxy, ffmpeg remux (copy), and ffmpeg transcode
- Shared sessions -- multiple viewers on the same channel share one upstream connection
- Automatic stall detection and source failover with configurable recovery policies
- Recovery filler (null packets, PSI, or decodable A/V slate) keeps clients connected during failover
- Configurable tuner count to match upstream provider limits

### Discovery and Compatibility
- HDHomeRun UDP discovery (port 65001) -- detected automatically by DVR software
- UPnP/SSDP discovery with full device description and ContentDirectory browsing
- Standard lineup endpoints: `/discover.json`, `/lineup.json`, `/lineup.xml`, `/lineup.m3u`
- Optional legacy HTTP listener on port 80 for clients that ignore advertised ports
- Optional mDNS/Avahi integration for .local hostname resolution

### DVR Integration

Every HDHomeRun-compatible app discovers hdhriptv automatically. For deeper integration, Channels DVR and Jellyfin get dedicated sync and refresh workflows:

- Forward sync: push hdhriptv channel mappings into your DVR's custom lineup
- Reverse sync: pull DVR-side mappings back into hdhriptv
- Per-channel DVR mapping control with lineup and station-ref support
- Jellyfin-native lineup refresh after playlist updates
- Channels DVR custom mapping API support

### Automation
- Scheduled playlist refresh via cron expressions
- Auto-prioritize: probe sources and reorder by quality/availability
- Source health tracking with cooldown ladders for failing sources
- Background source probing at configurable intervals
- All jobs observable through the admin API with progress and status tracking

### Admin and Observability
- Web UI for catalog browsing, channel management, tuner status, and automation
- Rapid source-add workflow for quickly building channel lineups
- Live tuner and session dashboard with recovery trigger controls
- Session history console with filterable diagnostics
- Full JSON API for scripting and integration
- Health check endpoint (`/healthz`) for load balancers and orchestrators
- Optional Prometheus metrics endpoint (`/metrics`)
- Per-client IP rate limiting with trusted proxy support

## Compatibility

hdhriptv works with any software that supports HDHomeRun tuners. It is tested with the following DVR and media platforms:

| Platform | Discovery | Live TV | DVR/Recording | Notes |
|----------|-----------|---------|---------------|-------|
| **Plex** | Automatic (HDHomeRun scan) | Yes | Yes | Also supports manual IP entry |
| **Channels DVR** | Automatic (HDHomeRun scan) | Yes | Yes | Deepest integration: forward/reverse sync, custom lineup mapping |
| **Jellyfin** | Automatic (HDHomeRun tuner) | Yes | Yes | Native lineup refresh integration; use port 80 if detection fails |
| **Emby** | Automatic (HDHomeRun tuner) | Yes | Yes | Use port 80 if detection fails |
| **VLC** | Direct URL | Yes | -- | Open any `/auto/v{channel}` URL directly |
| **UPnP apps** | SSDP/ContentDirectory | Browse/Play | -- | BubbleUPnP, Kodi UPnP browser, and similar (read-only browse) |

**Stream mode compatibility:** `ffmpeg-copy` (the default) provides the best balance of compatibility and efficiency. Use `ffmpeg-transcode` if clients have trouble with your source codec/container format. Use `direct` for lowest overhead when sources already produce compatible MPEG-TS output.

## Quick Start

### Prerequisites

- Go `1.25.4` or newer (see `go.mod`)
- Network reachability from client devices to this service
- `ffmpeg` installed if using:
  - `STREAM_MODE=ffmpeg-copy` (default)
  - `STREAM_MODE=ffmpeg-transcode`
- No `ffmpeg` required if using `STREAM_MODE=direct`

### Run Locally (Linux and Mac)

You can either download a prebuilt release binary for Linux
(`hdhriptv-linux-amd64` / `hdhriptv-linux-arm64`) or macOS
(`hdhriptv-darwin-amd64` / `hdhriptv-darwin-arm64`) from:
<https://github.com/arodd/hdhriptv/releases>

1. Download the release binary for your platform from the release page above.

2. Make the downloaded binary executable:

```bash
chmod +x ./hdhriptv-*
```

3. Or build locally from source instead:

```bash
go build -o hdhriptv ./cmd/hdhriptv
```

4. Run with minimum practical settings (replace `./hdhriptv` with your downloaded filename if not building locally):

```bash
PLAYLIST_URL="https://example.com/playlist.m3u" \
ADMIN_AUTH="admin:change-me" \
./hdhriptv
```

5. Verify service health and discovery:

```bash
curl -s http://127.0.0.1:5004/healthz
curl -s http://127.0.0.1:5004/discover.json
curl -s http://127.0.0.1:5004/lineup.json
```

6. Open `http://127.0.0.1:5004/ui/catalog` and publish channels from catalog items.
   `lineup.json` is channel-only and remains empty until at least one channel is published.

### Run With Docker

By default, use the public Docker Hub image:

```bash
docker pull arodd/hdhriptv:latest
```

Run container:

```bash
docker run --rm -it \
  -p 5004:5004/tcp \
  -p 65001:65001/udp \
  -p 1900:1900/udp \
  -v hdhriptv-data:/data \
  -e PLAYLIST_URL="https://example.com/playlist.m3u" \
  -e ADMIN_AUTH="admin:change-me" \
  -e UPNP_ENABLED=true \
  arodd/hdhriptv:latest
```

Notes:

- The image includes `ffmpeg`.
- The runtime image uses `alpine:3.23` and installs `ffmpeg` from Alpine's `community` repository.
- Multi-arch image available (amd64, arm64).
- SQLite state persists in `/data` when a volume is mounted.
- Add `-p 80:80/tcp -e HTTP_ADDR_LEGACY=:80` if you need legacy client compatibility.

Optionally, build and run from the local Dockerfile instead:

```bash
docker build -t hdhriptv:local .
docker run --rm -it \
  -p 5004:5004/tcp \
  -p 65001:65001/udp \
  -p 1900:1900/udp \
  -v hdhriptv-data:/data \
  -e PLAYLIST_URL="https://example.com/playlist.m3u" \
  -e ADMIN_AUTH="admin:change-me" \
  -e UPNP_ENABLED=true \
  hdhriptv:local
```

Build and publish the same multi-arch image flow used in GitLab CI from your local machine (run `docker login` first):

```bash
make release-local REGISTRY_IMAGE=registry.example.com/org/hdhriptv IMAGE_TAG=latest
```

### Run on Windows

1. Download the Windows binary release (`hdhriptv-windows-amd64.exe`).

2. Install `ffmpeg` and `ffprobe` (Windows builds):
   - Gyan release builds: <https://www.gyan.dev/ffmpeg/builds/#release-builds>
   - BtbN release builds: <https://github.com/BtbN/FFmpeg-Builds/releases>

3. Extract the build and point both flags to executable paths (not just the `bin` directory):
   - `--ffmpeg-path C:\Users\<you>\ffmpeg\bin\ffmpeg.exe`
   - `--ffprobe-path C:\Users\<you>\ffmpeg\bin\ffprobe.exe`

4. Common startup pattern:

```powershell
.\hdhriptv.exe `
  --playlist-url https://example.com/playlist `
  --ffmpeg-path C:\users\person\ffmpeg\bin\ffmpeg.exe `
  --ffprobe-path C:\users\person\ffmpeg\bin\ffprobe.exe `
  --http-addr-legacy :80 `
  --friendly-name "HDHRIPTV Windows"
```

## Stream Modes and Tradeoffs

| Mode | `ffmpeg` Required | CPU Use | Compatibility | Notes |
| --- | --- | --- | --- | --- |
| `direct` | No | Lowest | Lowest/variable | Byte-for-byte upstream proxy; codec/container unchanged. |
| `ffmpeg-copy` | Yes | Low | High | Remux to MPEG-TS without transcoding. Good default. |
| `ffmpeg-transcode` | Yes | Highest | Highest | Transcodes to H.264/AAC MPEG-TS for difficult clients. |

## Configuration Reference

The most common settings are listed below. For the full reference including all flags, internal stream tuning, and SQLite diagnostic toggles, see [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

| Flag | Environment Variable | Default | Notes |
| --- | --- | --- | --- |
| `--playlist-url` | `PLAYLIST_URL` | empty | M3U playlist URL. If empty, no automatic refresh runs. |
| `--db-path` | `DB_PATH` | `./hdhr-iptv.db` | SQLite file location. |
| `--http-addr` | `HTTP_ADDR` | `:5004` | Primary HTTP listener. |
| `--http-addr-legacy` | `HTTP_ADDR_LEGACY` | empty | Optional second listener, often `:80`. |
| `--tuner-count` | `TUNER_COUNT` | `2` | Max concurrent stream sessions. |
| `--friendly-name` | `FRIENDLY_NAME` | `HDHR IPTV` | Displayed in discover payloads and DVR UIs. |
| `--stream-mode` | `STREAM_MODE` | `ffmpeg-copy` | `direct`, `ffmpeg-copy`, `ffmpeg-transcode`. |
| `--ffmpeg-path` | `FFMPEG_PATH` | `ffmpeg` | Executable used for ffmpeg stream modes. |
| `--ffprobe-path` | `FFPROBE_PATH` | `ffprobe` | Executable used by auto-prioritize analysis and shared-session stream profile probes. |
| `--admin-auth` | `ADMIN_AUTH` | empty | `user:pass` format. If empty, admin routes are open. |
| `--refresh-schedule` | `REFRESH_SCHEDULE` | empty | Cron expression for playlist sync. |
| `--upnp-enabled` | `UPNP_ENABLED` | `true` | Enable UPnP/SSDP discovery on UDP `1900`. |
| `--stall-policy` | `STALL_POLICY` | `failover_source` | `failover_source`, `restart_same`, `close_session`. |
| `--recovery-filler-mode` | `RECOVERY_FILLER_MODE` | `slate_av` | `null`, `psi`, or `slate_av` (decodable A/V filler). |
| `--enable-metrics` | `ENABLE_METRICS` | `false` | Enables `/metrics` Prometheus endpoint. |
| `--log-level` | `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. |

## Documentation

| Document | Description |
| --- | --- |
| [docs/API.md](docs/API.md) | API and endpoint reference |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Runtime architecture, route map, and subsystem wiring |
| [docs/AUTOMATION.md](docs/AUTOMATION.md) | Job runner, scheduler, and background automation behavior |
| [docs/CATALOG-PIPELINE.md](docs/CATALOG-PIPELINE.md) | M3U ingest, normalization, and reconcile pipeline details |
| [docs/CHANNELS.md](docs/CHANNELS.md) | Channel/favorites service model and dynamic block flows |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Full flag and environment variable reference |
| [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) | Testing, CI, and GitHub mirror publish workflow |
| [docs/DATA-MODEL.md](docs/DATA-MODEL.md) | SQLite data model, keys, and persistence semantics |
| [docs/DVR-INTEGRATION.md](docs/DVR-INTEGRATION.md) | Channels DVR and Jellyfin sync workflows and edge cases |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Operations, troubleshooting, and advanced topics |
| [docs/RECOVERY.md](docs/RECOVERY.md) | Stall detection, failover, and recovery filler behavior |
| [docs/SESSION-LIFECYCLE-BASELINE.md](docs/SESSION-LIFECYCLE-BASELINE.md) | Shared-session lifecycle baseline evidence workflow |
| [docs/STREAMING.md](docs/STREAMING.md) | Shared-session streaming pipeline internals and tuning |
| [docs/UI-COMPONENTS.md](docs/UI-COMPONENTS.md) | Admin UI pages, templates, and frontend architecture |
| [deploy/systemd/README.md](deploy/systemd/README.md) | Systemd deployment |
| [deploy/avahi/README.md](deploy/avahi/README.md) | Avahi/mDNS setup |
| [deploy/testing/compatibility-checklist.md](deploy/testing/compatibility-checklist.md) | Client compatibility testing |

## Deployment Notes

- **Docker**: See Quick Start above. Multi-arch image (amd64, arm64). SQLite state persists in `/data`.
- **Systemd**: See [deploy/systemd/README.md](deploy/systemd/README.md) for unit file and setup guide.
- **Avahi/mDNS**: See [deploy/avahi/README.md](deploy/avahi/README.md) for optional `.local` hostname resolution.
- **Ports**: TCP `5004` (main), UDP `65001` (discovery), optionally UDP `1900` (UPnP) and TCP `80` (legacy).

## Testing

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for test commands, smoke checks, client compatibility testing, and the GitHub mirror publish workflow.
