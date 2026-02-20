package upnp

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/hdhr"
)

func TestParseMSearchRequestAcceptsMixedCaseAndWhitespace(t *testing.T) {
	raw := strings.Join([]string{
		"M-SEARCH * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		`MAN:   "ssdp:discover"`,
		"MX: 9",
		"ST: urn:schemas-upnp-org:device:mediaserver:1",
		"",
		"",
	}, "\r\n")

	req, err := parseMSearchRequest([]byte(raw))
	if err != nil {
		t.Fatalf("parseMSearchRequest() error = %v", err)
	}
	if req.ST != hdhr.UPnPDeviceTypeMediaServer {
		t.Fatalf("ST = %q, want %q", req.ST, hdhr.UPnPDeviceTypeMediaServer)
	}
	if req.MX != maxMXSeconds {
		t.Fatalf("MX = %d, want %d (capped)", req.MX, maxMXSeconds)
	}
}

func TestParseMSearchRequestRejectsMissingDiscoverToken(t *testing.T) {
	raw := strings.Join([]string{
		"M-SEARCH * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		`MAN: "not-discover"`,
		"MX: 1",
		"ST: ssdp:all",
		"",
		"",
	}, "\r\n")

	if _, err := parseMSearchRequest([]byte(raw)); err == nil {
		t.Fatal("expected parse error for invalid MAN token")
	}
}

func TestParseMSearchRequestDefaultsMXWhenInvalid(t *testing.T) {
	raw := strings.Join([]string{
		"M-SEARCH * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		`MAN: "ssdp:discover"`,
		"MX: nope",
		"ST: upnp:rootdevice",
		"",
		"",
	}, "\r\n")

	req, err := parseMSearchRequest([]byte(raw))
	if err != nil {
		t.Fatalf("parseMSearchRequest() error = %v", err)
	}
	if req.MX != 1 {
		t.Fatalf("MX = %d, want default 1", req.MX)
	}
}

func TestParseMSearchRequestAcceptsHTTP10StartLine(t *testing.T) {
	raw := strings.Join([]string{
		"M-SEARCH * HTTP/1.0",
		"HOST: 239.255.255.250:1900",
		`MAN: "ssdp:discover"`,
		"MX: 1",
		"ST: upnp:rootdevice",
		"",
		"",
	}, "\r\n")

	req, err := parseMSearchRequest([]byte(raw))
	if err != nil {
		t.Fatalf("parseMSearchRequest() error = %v", err)
	}
	if req.ST != hdhr.UPnPSearchTargetRootDevice {
		t.Fatalf("ST = %q, want %q", req.ST, hdhr.UPnPSearchTargetRootDevice)
	}
}

func TestParseMSearchRequestTreatsNotifyAsExpectedTraffic(t *testing.T) {
	raw := strings.Join([]string{
		"NOTIFY * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		"NTS: ssdp:alive",
		"NT: upnp:rootdevice",
		"USN: uuid:12345678-1234-5678-1234-567812345678::upnp:rootdevice",
		"",
		"",
	}, "\r\n")

	if _, err := parseMSearchRequest([]byte(raw)); !errors.Is(err, errIgnoreNotifyAnnouncement) {
		t.Fatalf("parseMSearchRequest() err = %v, want errIgnoreNotifyAnnouncement", err)
	}
}

func TestResponseTargetsForSearchAll(t *testing.T) {
	udn := "uuid:12345678-1234-5678-1234-567812345678"
	targets := responseTargetsForSearch(ssdpSearchTargetAll, udn)
	if len(targets) != 7 {
		t.Fatalf("len(targets) = %d, want 7", len(targets))
	}
	if targets[0].ST != hdhr.UPnPSearchTargetRootDevice {
		t.Fatalf("targets[0].ST = %q, want %q", targets[0].ST, hdhr.UPnPSearchTargetRootDevice)
	}
	if targets[1].ST != udn {
		t.Fatalf("targets[1].ST = %q, want %q", targets[1].ST, udn)
	}
	if targets[2].ST != hdhr.UPnPDeviceTypeMediaServer {
		t.Fatalf("targets[2].ST = %q, want %q", targets[2].ST, hdhr.UPnPDeviceTypeMediaServer)
	}
	if targets[3].ST != hdhr.UPnPDeviceTypeBasic {
		t.Fatalf("targets[3].ST = %q, want %q", targets[3].ST, hdhr.UPnPDeviceTypeBasic)
	}
	if targets[4].ST != hdhr.UPnPServiceTypeConnection {
		t.Fatalf("targets[4].ST = %q, want %q", targets[4].ST, hdhr.UPnPServiceTypeConnection)
	}
	if targets[5].ST != hdhr.UPnPServiceTypeContentDir {
		t.Fatalf("targets[5].ST = %q, want %q", targets[5].ST, hdhr.UPnPServiceTypeContentDir)
	}
	if targets[6].ST != hdhr.UPnPDeviceTypeATSCPrimary {
		t.Fatalf("targets[6].ST = %q, want %q", targets[6].ST, hdhr.UPnPDeviceTypeATSCPrimary)
	}
}

func TestResponseTargetsForSearchUUIDAndUnsupported(t *testing.T) {
	udn := hdhr.DeviceUDN("ABCD1234")

	targets := responseTargetsForSearch(strings.ToUpper(udn), udn)
	if len(targets) != 1 {
		t.Fatalf("len(targets) = %d, want 1", len(targets))
	}
	if targets[0].ST != udn {
		t.Fatalf("targets[0].ST = %q, want %q", targets[0].ST, udn)
	}
	if targets[0].USN != udn {
		t.Fatalf("targets[0].USN = %q, want %q", targets[0].USN, udn)
	}

	unsupported := responseTargetsForSearch("urn:schemas-upnp-org:device:InternetGatewayDevice:1", udn)
	if len(unsupported) != 0 {
		t.Fatalf("len(unsupported) = %d, want 0", len(unsupported))
	}
}

func TestUSNForTargetVariants(t *testing.T) {
	udn := "uuid:12345678-1234-5678-1234-567812345678"

	rootUSN := usnForTarget(udn, hdhr.UPnPSearchTargetRootDevice)
	if rootUSN != udn+"::"+hdhr.UPnPSearchTargetRootDevice {
		t.Fatalf("root USN = %q, want %q", rootUSN, udn+"::"+hdhr.UPnPSearchTargetRootDevice)
	}

	uuidUSN := usnForTarget(udn, udn)
	if uuidUSN != udn {
		t.Fatalf("uuid USN = %q, want %q", uuidUSN, udn)
	}

	mediaUSN := usnForTarget(udn, hdhr.UPnPDeviceTypeMediaServer)
	if mediaUSN != udn+"::"+hdhr.UPnPDeviceTypeMediaServer {
		t.Fatalf("media USN = %q, want %q", mediaUSN, udn+"::"+hdhr.UPnPDeviceTypeMediaServer)
	}

	contentUSN := usnForTarget(udn, hdhr.UPnPServiceTypeContentDir)
	if contentUSN != udn+"::"+hdhr.UPnPServiceTypeContentDir {
		t.Fatalf("content USN = %q, want %q", contentUSN, udn+"::"+hdhr.UPnPServiceTypeContentDir)
	}
}

func TestResponseTargetsForSearchContentDirectoryService(t *testing.T) {
	udn := hdhr.DeviceUDN("ABCD1234")
	targets := responseTargetsForSearch(hdhr.UPnPServiceTypeContentDir, udn)
	if len(targets) != 1 {
		t.Fatalf("len(targets) = %d, want 1", len(targets))
	}
	if targets[0].ST != hdhr.UPnPServiceTypeContentDir {
		t.Fatalf("targets[0].ST = %q, want %q", targets[0].ST, hdhr.UPnPServiceTypeContentDir)
	}
	if targets[0].USN != udn+"::"+hdhr.UPnPServiceTypeContentDir {
		t.Fatalf("targets[0].USN = %q, want %q", targets[0].USN, udn+"::"+hdhr.UPnPServiceTypeContentDir)
	}
}

func TestResponseTargetsForSearchATSCPrimaryDevice(t *testing.T) {
	udn := hdhr.DeviceUDN("ABCD1234")
	targets := responseTargetsForSearch(hdhr.UPnPDeviceTypeATSCPrimary, udn)
	if len(targets) != 1 {
		t.Fatalf("len(targets) = %d, want 1", len(targets))
	}
	if targets[0].ST != hdhr.UPnPDeviceTypeATSCPrimary {
		t.Fatalf("targets[0].ST = %q, want %q", targets[0].ST, hdhr.UPnPDeviceTypeATSCPrimary)
	}
	if targets[0].USN != udn+"::"+hdhr.UPnPDeviceTypeATSCPrimary {
		t.Fatalf("targets[0].USN = %q, want %q", targets[0].USN, udn+"::"+hdhr.UPnPDeviceTypeATSCPrimary)
	}
}

func TestNormalizeSearchTargetSupportsLooseVersionForms(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "quoted media server without explicit version",
			raw:  `"urn:schemas-upnp-org:device:MediaServer"`,
			want: hdhr.UPnPDeviceTypeMediaServer,
		},
		{
			name: "content directory v2 request maps to canonical service",
			raw:  "urn:schemas-upnp-org:service:ContentDirectory:2",
			want: hdhr.UPnPServiceTypeContentDir,
		},
		{
			name: "connection manager with minor version maps to canonical service",
			raw:  "urn:schemas-upnp-org:service:ConnectionManager:1.0",
			want: hdhr.UPnPServiceTypeConnection,
		},
		{
			name: "atsc primary with major-only version maps to canonical target",
			raw:  "urn:schemas-atsc.org:device:primaryDevice:1",
			want: hdhr.UPnPDeviceTypeATSCPrimary,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeSearchTarget(tc.raw)
			if got != tc.want {
				t.Fatalf("normalizeSearchTarget(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestBuildMSearchResponseIncludesRequiredHeaders(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	payload := buildMSearchResponse(ssdpResponse{
		Location: "http://192.0.2.10:5004/upnp/device.xml",
		ST:       hdhr.UPnPSearchTargetRootDevice,
		USN:      "uuid:demo::upnp:rootdevice",
		MaxAge:   15 * time.Minute,
	}, now)

	headers := parseSSDPHeaders(strings.Split(payload, "\r\n")[1:])
	if headers["cache-control"] != "max-age=900" {
		t.Fatalf("CACHE-CONTROL = %q, want max-age=900", headers["cache-control"])
	}
	if headers["date"] == "" {
		t.Fatal("DATE header is missing")
	}
	if headers["location"] != "http://192.0.2.10:5004/upnp/device.xml" {
		t.Fatalf("LOCATION = %q, want location URL", headers["location"])
	}
	if headers["st"] != hdhr.UPnPSearchTargetRootDevice {
		t.Fatalf("ST = %q, want %q", headers["st"], hdhr.UPnPSearchTargetRootDevice)
	}
	if headers["usn"] != "uuid:demo::upnp:rootdevice" {
		t.Fatalf("USN = %q, want uuid:demo::upnp:rootdevice", headers["usn"])
	}
}
