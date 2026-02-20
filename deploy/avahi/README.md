# Avahi / mDNS Deployment

These files are optional helpers for publishing the emulator on `.local` networks.
Core HDHomeRun compatibility still comes from UDP discovery on port `65001`.

## Files

- `hdhriptv.service`: `_http._tcp` service announcement for Avahi.
- `avahi.hosts.example`: optional host aliases (`hdhomerun.local`, `<deviceid>.local`, `hdhr-<deviceid>.local`).

## Linux setup

1. Install Avahi if it is not already installed.
2. Copy `hdhriptv.service` to `/etc/avahi/services/hdhriptv.service`.
3. Optionally copy/edit `avahi.hosts.example` into `/etc/avahi/hosts`.
4. Restart Avahi:

```bash
sudo systemctl restart avahi-daemon
```

## Notes

- Keep the advertised service port aligned with your actual HTTP port.
- If you run with `--http-addr-legacy :80`, update `hdhriptv.service` port to `80`.
- `.local` resolution depends on multicast DNS support on your network and client devices.
