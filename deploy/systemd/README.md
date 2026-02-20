# systemd deployment

This folder contains a sample unit file for running `hdhriptv` on Linux hosts
with persistent state under `/var/lib/hdhriptv`.

## Install

1. Create a service account and state directory:

```bash
sudo useradd --system --home /var/lib/hdhriptv --shell /usr/sbin/nologin hdhriptv
sudo install -d -o hdhriptv -g hdhriptv /var/lib/hdhriptv
```

2. Copy artifacts:

```bash
sudo install -m 0755 ./hdhriptv /usr/local/bin/hdhriptv
sudo install -m 0644 ./deploy/systemd/hdhriptv.service /etc/systemd/system/hdhriptv.service
```

3. Create `/etc/default/hdhriptv`:

```bash
PLAYLIST_URL=https://example.com/playlist.m3u
DB_PATH=/var/lib/hdhriptv/hdhr-iptv.db
HTTP_ADDR=:5004
HTTP_ADDR_LEGACY=:80
TUNER_COUNT=2
FRIENDLY_NAME=HDHR IPTV
ADMIN_AUTH=admin:replace-me
REFRESH_SCHEDULE="*/30 * * * *"
STREAM_MODE=ffmpeg-copy
FFMPEG_PATH=ffmpeg
REQUEST_TIMEOUT=15s
RATE_LIMIT_RPS=8
RATE_LIMIT_BURST=32
ENABLE_METRICS=true
LOG_LEVEL=info
```

4. Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now hdhriptv
sudo systemctl status hdhriptv
```

## Notes

- Keep `HTTP_ADDR_LEGACY=:80` for clients that incorrectly assume port 80.
- Open UDP `65001` and TCP `5004` (and optionally `80`) on your LAN firewall.
- If you use Avahi, also install files from `deploy/avahi`.
- With persistent `DB_PATH`, device identity is stored in SQLite and reused after restarts.
