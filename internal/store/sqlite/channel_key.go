package sqlite

import "strings"

const normalizedChannelKeySQLExpr = `CASE
	WHEN LOWER(TRIM(COALESCE(channel_key, ''))) LIKE 'tvg:%' THEN 'tvg:' || LOWER(TRIM(SUBSTR(TRIM(COALESCE(channel_key, '')), 5)))
	WHEN LOWER(TRIM(COALESCE(channel_key, ''))) LIKE 'name:%' THEN 'name:' || LOWER(TRIM(SUBSTR(TRIM(COALESCE(channel_key, '')), 6)))
	ELSE TRIM(COALESCE(channel_key, ''))
END`

func normalizeChannelKey(raw string) string {
	key := strings.TrimSpace(raw)
	if key == "" {
		return ""
	}

	lower := strings.ToLower(key)
	if strings.HasPrefix(lower, "tvg:") {
		return "tvg:" + strings.ToLower(strings.TrimSpace(key[4:]))
	}
	if strings.HasPrefix(lower, "name:") {
		return "name:" + strings.ToLower(strings.TrimSpace(key[5:]))
	}
	return key
}

func channelKeysEqual(a, b string) bool {
	return normalizeChannelKey(a) == normalizeChannelKey(b)
}
