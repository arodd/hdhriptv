package stream

import (
	"strconv"
	"strings"
)

const playlistSourceMetricUnknown = "unknown"

func playlistSourceMetricLabel(sourceID int64, sourceName string) string {
	trimmedName := strings.TrimSpace(sourceName)
	if sourceID <= 0 {
		if trimmedName == "" {
			return playlistSourceMetricUnknown
		}
		return trimmedName
	}
	if trimmedName == "" {
		return strconv.FormatInt(sourceID, 10)
	}
	return strconv.FormatInt(sourceID, 10) + ":" + trimmedName
}
