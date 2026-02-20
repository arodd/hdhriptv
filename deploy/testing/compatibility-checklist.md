# HDHomeRun client compatibility checklist

Use this checklist to validate behavior after deployment changes.

## Preconditions

- Service is reachable from client VLAN/subnet.
- UDP port `65001` is open between client and server.
- UDP port `1900` is open between client and server when `UPNP_ENABLED=true`.
- TCP port `5004` is open and responding.
- Optional compatibility port `80` is available if your client ignores advertised ports.
- At least one favorite exists and has a playable stream URL.

## Plex (HDHomeRun tuner)

1. In Plex DVR setup, scan for HDHomeRun devices with auto-discovery.
2. Verify this emulator appears with expected friendly name and tuner count.
3. Run lineup scan and confirm favorite channels are detected.
4. Play at least two channels to validate tuner allocation and stream compatibility.
5. Repeat setup using manual IP entry (`http://<server-ip>:5004/discover.json`).

Record result: `[ ] pass  [ ] fail`
Notes:

## Emby and Jellyfin

1. Add an HDHomeRun tuner source and run device detection.
2. Confirm lineup import succeeds and channels map to expected names/numbers.
3. Test live playback for at least one SD and one HD stream.
4. If detection fails, retry with legacy HTTP endpoint enabled on port `80`.

Record result: `[ ] pass  [ ] fail`
Notes:

## VLC direct playback

1. Fetch `lineup.json` and pick a channel URL (`/auto/v<GuideNumber>`).
2. Open that URL directly in VLC.
3. Confirm playback starts and continues for at least 60 seconds.

Record result: `[ ] pass  [ ] fail`
Notes:

## UPnP/SSDP discovery sanity check

1. Fetch the UPnP device description directly and confirm XML is returned:
   - `curl -s http://<server-ip>:5004/upnp/device.xml`
   - Optional alias check: `curl -s http://<server-ip>:5004/device.xml`
2. Confirm device XML includes expected identity and service fields:
   - `friendlyName`, `UDN`, `modelName`
   - `serviceType=urn:schemas-upnp-org:service:ConnectionManager:1`
   - `serviceType=urn:schemas-upnp-org:service:ContentDirectory:1`
   - matching `SCPDURL`/`controlURL` entries under `/upnp/scpd/*` and `/upnp/control/*`
3. Send an SSDP probe from a host on the same subnet:
   - `printf 'M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: 1\r\nST: ssdp:all\r\n\r\n' | nc -u -w 1 <server-ip> 1900`
4. Send a direct service-level probe:
   - `printf 'M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: 1\r\nST: urn:schemas-upnp-org:service:ContentDirectory:1\r\n\r\n' | nc -u -w 1 <server-ip> 1900`
5. Send an HDHomeRun-oriented compatibility probe:
   - `printf 'M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: 1\r\nST: urn:schemas-atsc.org:device:primaryDevice:1.0\r\n\r\n' | nc -u -w 1 <server-ip> 1900`
6. Confirm SSDP responses include:
   - `HTTP/1.1 200 OK`
   - `LOCATION: http://<server-ip>:5004/upnp/device.xml` (or equivalent reachable host)
   - stable `USN` values with `uuid:` prefix and supported `ST` values
   - explicit response coverage for `urn:schemas-upnp-org:service:ContentDirectory:1`.
7. Validate legacy request-line compatibility used by older control points:
   - `printf 'M-SEARCH * HTTP/1.0\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: 1\r\nST: upnp:rootdevice\r\n\r\n' | nc -u -w 1 <server-ip> 1900`
8. Fetch SCPD endpoints and confirm XML is returned without UI redirects:
   - `curl -si http://<server-ip>:5004/upnp/scpd/connection-manager.xml`
   - `curl -si http://<server-ip>:5004/upnp/scpd/content-directory.xml`
9. Validate SOAP control actions:
   - `GetSystemUpdateID` success (`<Id>1</Id>`):
     - `curl -si -X POST http://<server-ip>:5004/upnp/control/content-directory -H 'Content-Type: text/xml; charset="utf-8"' -H 'SOAPACTION: "urn:schemas-upnp-org:service:ContentDirectory:1#GetSystemUpdateID"' --data-binary '<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><u:GetSystemUpdateID xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1"/></s:Body></s:Envelope>'`
   - `Browse` success returns DIDL with root/channels/channel object IDs:
     - `curl -si -X POST http://<server-ip>:5004/upnp/control/content-directory -H 'Content-Type: text/xml; charset="utf-8"' -H 'SOAPACTION: "urn:schemas-upnp-org:service:ContentDirectory:1#Browse"' --data-binary '<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><u:Browse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1"><ObjectID>channels</ObjectID><BrowseFlag>BrowseDirectChildren</BrowseFlag><Filter>*</Filter><StartingIndex>0</StartingIndex><RequestedCount>5</RequestedCount><SortCriteria></SortCriteria></u:Browse></s:Body></s:Envelope>'`
   - invalid action returns SOAP fault with UPnP error code:
     - `curl -si -X POST http://<server-ip>:5004/upnp/control/connection-manager -H 'Content-Type: text/xml; charset="utf-8"' -H 'SOAPACTION: "urn:schemas-upnp-org:service:ConnectionManager:1#UnknownAction"' --data-binary '<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><u:UnknownAction xmlns:u="urn:schemas-upnp-org:service:ConnectionManager:1"/></s:Body></s:Envelope>'`

Record result: `[ ] pass  [ ] fail`
Notes:

## Strict UPnP control-point apps (manual)

Validate at least two control-point apps that require `ContentDirectory` probing
before device listing (for example: BubbleUPnP on Android, Kodi UPnP browser,
VLC network discovery, or equivalent tooling available on your LAN).

1. App #1: ____________________
   - Device listing succeeds via UPnP discovery: `[ ] pass  [ ] fail`
   - Browse/channel listing succeeds: `[ ] pass  [ ] fail`
   - Playback launch from UPnP browse succeeds: `[ ] pass  [ ] fail`
2. App #2: ____________________
   - Device listing succeeds via UPnP discovery: `[ ] pass  [ ] fail`
   - Browse/channel listing succeeds: `[ ] pass  [ ] fail`
   - Playback launch from UPnP browse succeeds: `[ ] pass  [ ] fail`

Notes:

## Recovery failover continuity (`ffmpeg-copy` + `slate_av`)

1. Run `./deploy/testing/recovery-slate-av-ffmpeg-copy.sh`.
2. Confirm the script prints `result=PASS`.
3. Open the generated `summary.txt` path and verify all checks are green, especially:
   - `has_keepalive_slate_av=yes`
   - `has_failover_source1_to_source2=yes`
   - `has_session_stop_error=no`
   - `has_fatal_cvlc_open_error=no`

Record result: `[ ] pass  [ ] fail`
Notes:

## Observability and hardening checks

1. `GET /healthz` returns `{"status":"ok"}` with status 200.
2. If metrics are enabled, `GET /metrics` returns Prometheus text exposition.
3. Rate limiting: issue rapid repeated requests from one client and expect HTTP 429 responses.
4. Timeout behavior: confirm non-stream endpoints terminate long-running requests at configured timeout.
5. Streaming path (`/auto/...`) is not cut off by metadata timeout middleware.

Record result: `[ ] pass  [ ] fail`
Notes:

## Known gotchas

- Some clients ignore the port in advertised `BaseURL` and still connect to port `80`.
- `.local` hostnames require working mDNS/Avahi and may fail across VLAN boundaries.
