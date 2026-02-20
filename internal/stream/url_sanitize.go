package stream

import (
	"net/url"
	"strings"
)

// SanitizeURLForLog keeps URL routing shape while removing credentials and
// token-bearing components from log output.
func SanitizeURLForLog(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	parsed, err := url.Parse(raw)
	if err == nil {
		parsed.User = nil
		parsed.RawQuery = ""
		parsed.Fragment = ""
		sanitized := strings.TrimSpace(parsed.String())
		if sanitized != "" {
			return sanitized
		}
	}

	return fallbackSanitizeStreamURL(raw)
}

// sanitizeStreamURLForLog keeps URL routing shape while removing credentials
// and token-bearing components from log output.
func sanitizeStreamURLForLog(raw string) string {
	return SanitizeURLForLog(raw)
}

// sanitizeStreamURLForStatus applies the same redaction policy used for logs
// to admin status payloads.
func sanitizeStreamURLForStatus(raw string) string {
	return sanitizeStreamURLForLog(raw)
}

func fallbackSanitizeStreamURL(raw string) string {
	withoutFragment := raw
	if idx := strings.IndexByte(withoutFragment, '#'); idx >= 0 {
		withoutFragment = withoutFragment[:idx]
	}
	withoutQuery := withoutFragment
	if idx := strings.IndexByte(withoutQuery, '?'); idx >= 0 {
		withoutQuery = withoutQuery[:idx]
	}

	if schemeIdx := strings.Index(withoutQuery, "://"); schemeIdx >= 0 {
		prefix := withoutQuery[:schemeIdx+3]
		remainder := withoutQuery[schemeIdx+3:]
		authority := remainder
		path := ""
		if slashIdx := strings.IndexByte(remainder, '/'); slashIdx >= 0 {
			authority = remainder[:slashIdx]
			path = remainder[slashIdx:]
		}
		if atIdx := strings.LastIndexByte(authority, '@'); atIdx >= 0 {
			authority = authority[atIdx+1:]
		}
		return prefix + authority + path
	}

	if atIdx := strings.LastIndexByte(withoutQuery, '@'); atIdx >= 0 {
		return withoutQuery[atIdx+1:]
	}
	return withoutQuery
}
