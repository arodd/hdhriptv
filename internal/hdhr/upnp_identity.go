package hdhr

import (
	"crypto/sha1"
	"fmt"
	"strings"
)

// DeviceUDN derives a deterministic UPnP uuid:... identifier from DeviceID.
func DeviceUDN(deviceID string) string {
	normalized := strings.ToUpper(strings.TrimSpace(deviceID))
	if normalized == "" {
		normalized = "00000000"
	}

	sum := sha1.Sum([]byte("hdhriptv:upnp:udn:" + normalized))
	uuidBytes := make([]byte, 16)
	copy(uuidBytes, sum[:16])

	// RFC 4122 variant/version bit layout with deterministic (v5-like) digest bytes.
	uuidBytes[6] = (uuidBytes[6] & 0x0f) | 0x50
	uuidBytes[8] = (uuidBytes[8] & 0x3f) | 0x80

	return fmt.Sprintf(
		"uuid:%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		uuidBytes[0], uuidBytes[1], uuidBytes[2], uuidBytes[3],
		uuidBytes[4], uuidBytes[5],
		uuidBytes[6], uuidBytes[7],
		uuidBytes[8], uuidBytes[9],
		uuidBytes[10], uuidBytes[11], uuidBytes[12], uuidBytes[13], uuidBytes[14], uuidBytes[15],
	)
}
