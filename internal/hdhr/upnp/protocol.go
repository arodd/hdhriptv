package upnp

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/hdhr"
)

const (
	ssdpMulticastAddr = "239.255.255.250:1900"
	defaultListenAddr = ":1900"

	// Contract choices for the initial compatibility profile:
	// - answer ssdp:all with one response per allowlisted target
	// - answer direct target probes for rootdevice, uuid, advertised device types,
	//   and advertised service types
	// - include required SSDP response headers and deterministic USN derivation
	// - keep MX handling bounded; max accepted MX is capped to 5 seconds
	ssdpSearchTargetAll = "ssdp:all"
	ssdpHeaderServer    = "hdhriptv/1.0 UPnP/1.1 HDHR-IPTV/1.0"
	maxMXSeconds        = 5
)

var supportedSearchTargets = []string{
	hdhr.UPnPSearchTargetRootDevice,
	hdhr.UPnPDeviceTypeMediaServer,
	hdhr.UPnPDeviceTypeBasic,
	hdhr.UPnPServiceTypeConnection,
	hdhr.UPnPServiceTypeContentDir,
	hdhr.UPnPDeviceTypeATSCPrimary,
}

type msearchRequest struct {
	ST string
	MX int
}

type searchResponseTarget struct {
	ST  string
	USN string
}

type ssdpResponse struct {
	Location string
	ST       string
	USN      string
	MaxAge   time.Duration
}

type ssdpNotify struct {
	Location string
	NT       string
	NTS      string
	USN      string
	MaxAge   time.Duration
}

func parseMSearchRequest(datagram []byte) (msearchRequest, error) {
	lines := strings.Split(string(datagram), "\n")
	if len(lines) == 0 {
		return msearchRequest{}, fmt.Errorf("empty ssdp request")
	}

	startLine := strings.TrimSpace(lines[0])
	if isSupportedNotifyStartLine(startLine) {
		return msearchRequest{}, errIgnoreNotifyAnnouncement
	}
	if !isSupportedMSearchStartLine(startLine) {
		return msearchRequest{}, fmt.Errorf("unsupported request line %q", startLine)
	}

	headers := parseSSDPHeaders(lines[1:])
	man := strings.ToLower(strings.TrimSpace(headers["man"]))
	if !strings.Contains(man, "ssdp:discover") {
		return msearchRequest{}, fmt.Errorf("missing ssdp discover MAN token")
	}

	st := normalizeSearchTarget(headers["st"])
	if st == "" {
		return msearchRequest{}, fmt.Errorf("missing ST header")
	}

	mx := 1
	if rawMX, ok := headers["mx"]; ok {
		parsedMX, err := strconv.Atoi(strings.TrimSpace(rawMX))
		if err == nil {
			if parsedMX < 0 {
				parsedMX = 0
			}
			if parsedMX > maxMXSeconds {
				parsedMX = maxMXSeconds
			}
			mx = parsedMX
		}
	}

	return msearchRequest{
		ST: st,
		MX: mx,
	}, nil
}

func isSupportedMSearchStartLine(line string) bool {
	if strings.EqualFold(line, "M-SEARCH * HTTP/1.1") {
		return true
	}
	return strings.EqualFold(line, "M-SEARCH * HTTP/1.0")
}

func isSupportedNotifyStartLine(line string) bool {
	if strings.EqualFold(line, "NOTIFY * HTTP/1.1") {
		return true
	}
	return strings.EqualFold(line, "NOTIFY * HTTP/1.0")
}

func parseSSDPHeaders(lines []string) map[string]string {
	headers := make(map[string]string, len(lines))
	for _, rawLine := range lines {
		line := strings.TrimSpace(strings.TrimSuffix(rawLine, "\r"))
		if line == "" {
			break
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			continue
		}
		headers[key] = strings.TrimSpace(value)
	}
	return headers
}

func normalizeSearchTarget(raw string) string {
	value := strings.Trim(strings.TrimSpace(raw), `"`)
	if value == "" {
		return ""
	}

	lowerValue := strings.ToLower(value)
	switch lowerValue {
	case ssdpSearchTargetAll:
		return ssdpSearchTargetAll
	case strings.ToLower(hdhr.UPnPSearchTargetRootDevice):
		return hdhr.UPnPSearchTargetRootDevice
	case strings.ToLower(hdhr.UPnPDeviceTypeMediaServer):
		return hdhr.UPnPDeviceTypeMediaServer
	case strings.ToLower(hdhr.UPnPDeviceTypeBasic):
		return hdhr.UPnPDeviceTypeBasic
	case strings.ToLower(hdhr.UPnPDeviceTypeATSCPrimary):
		return hdhr.UPnPDeviceTypeATSCPrimary
	case strings.ToLower(hdhr.UPnPServiceTypeConnection):
		return hdhr.UPnPServiceTypeConnection
	case strings.ToLower(hdhr.UPnPServiceTypeContentDir):
		return hdhr.UPnPServiceTypeContentDir
	default:
		if target, ok := normalizeVersionedSearchTarget(
			lowerValue,
			"urn:schemas-upnp-org:device:mediaserver",
			hdhr.UPnPDeviceTypeMediaServer,
		); ok {
			return target
		}
		if target, ok := normalizeVersionedSearchTarget(
			lowerValue,
			"urn:schemas-upnp-org:device:basic",
			hdhr.UPnPDeviceTypeBasic,
		); ok {
			return target
		}
		if target, ok := normalizeVersionedSearchTarget(
			lowerValue,
			"urn:schemas-upnp-org:service:connectionmanager",
			hdhr.UPnPServiceTypeConnection,
		); ok {
			return target
		}
		if target, ok := normalizeVersionedSearchTarget(
			lowerValue,
			"urn:schemas-upnp-org:service:contentdirectory",
			hdhr.UPnPServiceTypeContentDir,
		); ok {
			return target
		}
		if target, ok := normalizeVersionedSearchTarget(
			lowerValue,
			"urn:schemas-atsc.org:device:primarydevice",
			hdhr.UPnPDeviceTypeATSCPrimary,
		); ok {
			return target
		}
		if strings.HasPrefix(lowerValue, "uuid:") {
			return lowerValue
		}
		return value
	}
}

// normalizeVersionedSearchTarget handles common non-strict UPnP `ST` variants:
// - missing explicit version suffix (for example "...:MediaServer")
// - version suffixes beyond v1 (for example "...:MediaServer:2")
// - minor notation (for example "...:primaryDevice:1.0")
func normalizeVersionedSearchTarget(value, base, canonical string) (string, bool) {
	if !strings.HasPrefix(value, base) {
		return "", false
	}
	suffix := strings.TrimPrefix(value, base)
	if suffix == "" {
		return canonical, true
	}
	if !strings.HasPrefix(suffix, ":") {
		return "", false
	}
	majorPart := strings.TrimPrefix(suffix, ":")
	if majorPart == "" {
		return "", false
	}
	if dotIdx := strings.Index(majorPart, "."); dotIdx >= 0 {
		majorPart = majorPart[:dotIdx]
	}
	major, err := strconv.Atoi(majorPart)
	if err != nil || major < 1 {
		return "", false
	}
	return canonical, true
}

func responseTargetsForSearch(st, udn string) []searchResponseTarget {
	st = normalizeSearchTarget(st)
	udn = strings.ToLower(strings.TrimSpace(udn))
	if udn == "" {
		return nil
	}

	all := []string{
		supportedSearchTargets[0],
		udn,
		supportedSearchTargets[1],
		supportedSearchTargets[2],
		supportedSearchTargets[3],
		supportedSearchTargets[4],
		supportedSearchTargets[5],
	}

	if strings.EqualFold(st, ssdpSearchTargetAll) {
		targets := make([]searchResponseTarget, 0, len(all))
		for _, target := range all {
			targets = append(targets, searchResponseTarget{
				ST:  target,
				USN: usnForTarget(udn, target),
			})
		}
		return targets
	}

	for _, target := range all {
		if strings.EqualFold(st, target) {
			return []searchResponseTarget{{
				ST:  target,
				USN: usnForTarget(udn, target),
			}}
		}
	}
	return nil
}

func usnForTarget(udn, st string) string {
	udn = strings.ToLower(strings.TrimSpace(udn))
	switch normalizeSearchTarget(st) {
	case hdhr.UPnPSearchTargetRootDevice:
		return udn + "::" + hdhr.UPnPSearchTargetRootDevice
	default:
		if strings.EqualFold(st, udn) {
			return udn
		}
		return udn + "::" + st
	}
}

func buildMSearchResponse(resp ssdpResponse, now time.Time) string {
	maxAge := int(resp.MaxAge / time.Second)
	if maxAge < 1 {
		maxAge = 1
	}

	return strings.Join([]string{
		"HTTP/1.1 200 OK",
		fmt.Sprintf("CACHE-CONTROL: max-age=%d", maxAge),
		fmt.Sprintf("DATE: %s", now.UTC().Format(http.TimeFormat)),
		"EXT:",
		fmt.Sprintf("LOCATION: %s", strings.TrimSpace(resp.Location)),
		fmt.Sprintf("SERVER: %s", ssdpHeaderServer),
		fmt.Sprintf("ST: %s", strings.TrimSpace(resp.ST)),
		fmt.Sprintf("USN: %s", strings.TrimSpace(resp.USN)),
		"",
		"",
	}, "\r\n")
}

func buildNotifyMessage(msg ssdpNotify, now time.Time) string {
	maxAge := int(msg.MaxAge / time.Second)
	if maxAge < 1 {
		maxAge = 1
	}

	return strings.Join([]string{
		"NOTIFY * HTTP/1.1",
		fmt.Sprintf("HOST: %s", ssdpMulticastAddr),
		fmt.Sprintf("CACHE-CONTROL: max-age=%d", maxAge),
		fmt.Sprintf("DATE: %s", now.UTC().Format(http.TimeFormat)),
		fmt.Sprintf("LOCATION: %s", strings.TrimSpace(msg.Location)),
		fmt.Sprintf("SERVER: %s", ssdpHeaderServer),
		fmt.Sprintf("NT: %s", strings.TrimSpace(msg.NT)),
		fmt.Sprintf("NTS: %s", strings.TrimSpace(msg.NTS)),
		fmt.Sprintf("USN: %s", strings.TrimSpace(msg.USN)),
		"",
		"",
	}, "\r\n")
}
