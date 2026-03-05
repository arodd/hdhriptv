package m3u

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

var attrRe = regexp.MustCompile(`([A-Za-z0-9_-]+)="([^"]*)"`)

// Item represents a parsed M3U entry before catalog normalization.
type Item struct {
	ItemKey    string
	ChannelKey string
	Name       string
	Group      string
	StreamURL  string
	TVGID      string
	TVGLogo    string
	Attrs      map[string]string
}

func Parse(r io.Reader) ([]Item, error) {
	items := make([]Item, 0)
	_, err := ParseEach(r, func(item Item) error {
		items = append(items, item)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// ParseEach parses M3U items and emits each normalized item incrementally.
func ParseEach(r io.Reader, onItem func(Item) error) (int, error) {
	if onItem == nil {
		return 0, fmt.Errorf("item callback is required")
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

	count := 0
	var pending *Item

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "#EXTINF:") {
			attrs, name := parseExtinf(line)
			pending = &Item{
				Name:    name,
				Group:   strings.TrimSpace(attrs["group-title"]),
				TVGID:   strings.TrimSpace(attrs["tvg-id"]),
				TVGLogo: strings.TrimSpace(attrs["tvg-logo"]),
				Attrs:   attrs,
			}
			continue
		}

		if strings.HasPrefix(line, "#") {
			continue
		}

		if pending == nil {
			continue
		}

		pending.StreamURL = strings.TrimSpace(line)
		pending.ChannelKey = channelKey(*pending)
		pending.ItemKey = itemKey(*pending)
		if err := onItem(cloneItem(*pending)); err != nil {
			return count, err
		}
		count++
		pending = nil
	}

	if err := scanner.Err(); err != nil {
		return count, err
	}

	return count, nil
}

func parseExtinf(line string) (map[string]string, string) {
	attrs := make(map[string]string)

	for _, match := range attrRe.FindAllStringSubmatch(line, -1) {
		if len(match) != 3 {
			continue
		}
		attrs[strings.ToLower(match[1])] = strings.TrimSpace(match[2])
	}

	name := ""
	if idx := strings.LastIndex(line, ","); idx >= 0 && idx+1 < len(line) {
		name = strings.TrimSpace(line[idx+1:])
	}

	return attrs, name
}

func channelKey(it Item) string {
	if tvgID := normalizedTVGID(it.TVGID); tvgID != "" {
		return "tvg:" + tvgID
	}
	return "name:" + normalizeName(it.Name)
}

func itemKey(it Item) string {
	normalizedURL := normalizedURLForKey(it.StreamURL)
	if tvgID := normalizedTVGID(it.TVGID); tvgID != "" {
		h := sha1.Sum([]byte(normalizedURL))
		return "src:" + tvgID + ":" + hex.EncodeToString(h[:])[:12]
	}
	h := sha1.Sum([]byte(normalizeName(it.Name) + "\n" + normalizedURL))
	return "src:" + hex.EncodeToString(h[:])[:16]
}

func normalizedTVGID(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func normalizedURLForKey(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return normalize(raw)
	}
	u.Fragment = ""

	base := u.Scheme + "://" + u.Host + u.Path
	query := u.Query()
	if len(query) == 0 {
		return base
	}

	filtered := make(url.Values, len(query))
	for key, values := range query {
		key = strings.TrimSpace(key)
		if key == "" || isVolatileQueryKey(key) {
			continue
		}
		copied := make([]string, 0, len(values))
		for _, value := range values {
			copied = append(copied, strings.TrimSpace(value))
		}
		sort.Strings(copied)
		filtered[key] = copied
	}

	if len(filtered) == 0 {
		return base
	}
	return base + "?" + filtered.Encode()
}

func isVolatileQueryKey(raw string) bool {
	normalized := normalizeQueryKey(raw)
	if normalized == "" {
		return false
	}

	switch normalized {
	case "token",
		"authtoken",
		"signature",
		"sig",
		"expires",
		"expire",
		"exp",
		"e",
		"session",
		"sessionid",
		"sid",
		"hdnts",
		"wmsauthsign",
		"jwt":
		return true
	}

	// Playlist providers commonly rotate auth/session-style query keys with
	// different prefixes. Treat those families as volatile identity noise.
	return strings.Contains(normalized, "token") ||
		strings.Contains(normalized, "auth") ||
		strings.Contains(normalized, "signature") ||
		strings.Contains(normalized, "session") ||
		strings.Contains(normalized, "expires")
}

func normalizeQueryKey(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}
	replacer := strings.NewReplacer("-", "", "_", "", ".", "")
	return replacer.Replace(raw)
}

func normalizeName(v string) string {
	return strings.ToLower(strings.Join(strings.Fields(normalize(v)), " "))
}

func normalize(v string) string {
	v = strings.ReplaceAll(v, "\r\n", "\n")
	v = strings.ReplaceAll(v, "\r", "\n")
	return strings.TrimSpace(v)
}

func cloneItem(in Item) Item {
	out := in
	out.Attrs = make(map[string]string, len(in.Attrs))
	for k, v := range in.Attrs {
		out.Attrs[k] = v
	}
	return out
}
