# Grafana Dashboard Bundle

This directory contains a ready-to-import Grafana dashboard for runtime and release health verification of `hdhriptv` using both Prometheus and Loki.

## Files

- `hdhriptv-release-health-dashboard.json`
  - Dashboard title: `HDHR IPTV - Release Health`
  - Dashboard UID: `hdhriptv-release-health`

## Prerequisites

- A Prometheus datasource in Grafana that can query `up{job="hdhriptv"}`.
- A Loki datasource in Grafana that can query `{nomad_job="hdhriptv"}`.

## Import

1. In Grafana, go to **Dashboards -> New -> Import**.
2. Upload `hdhriptv-release-health-dashboard.json`.
3. Map datasource variables during import:
   - `PROM_DS` -> your Prometheus datasource
   - `LOKI_DS` -> your Loki datasource
4. Save the dashboard.

## Notes

- Default time range is `now-6h` and auto-refresh is `30s`.
- The dashboard includes filters for:
  - `instance` (Prometheus target instance)
  - `host` (Loki host label; defaults to `All`)
- The top stat row is intended for release verification windows (fresh deploy check).
