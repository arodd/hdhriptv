package playlist

import (
	"net/url"
	"strings"
)

// CanonicalPlaylistSourceName returns the normalized comparison key used for
// source-name uniqueness checks across config, API, and store mutation paths.
func CanonicalPlaylistSourceName(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

// CanonicalPlaylistSourceURL returns the normalized comparison key used for
// source URL uniqueness checks across config, API, and store mutation paths.
//
// Canonicalization rules:
// - trim surrounding whitespace
// - lowercase scheme and host
// - preserve path/query/fragment semantics
// - if parsing fails, fall back to lowercased raw string
func CanonicalPlaylistSourceURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return strings.ToLower(trimmed)
	}
	parsed.Scheme = strings.ToLower(strings.TrimSpace(parsed.Scheme))
	parsed.Host = strings.ToLower(strings.TrimSpace(parsed.Host))
	return strings.TrimSpace(parsed.String())
}
