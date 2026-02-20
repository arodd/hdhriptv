package channels

import (
	"sort"
	"strings"
)

// NormalizeGroupNames trims, deduplicates, and deterministically orders
// dynamic filter group selectors. Legacy single-group callers can pass
// groupName with an empty groupNames slice.
func NormalizeGroupNames(groupName string, groupNames []string) []string {
	candidates := make([]string, 0, len(groupNames)+1)
	if len(groupNames) > 0 {
		candidates = append(candidates, groupNames...)
	} else {
		candidates = append(candidates, groupName)
	}

	type groupEntry struct {
		Value string
		Key   string
	}
	deduped := make(map[string]groupEntry, len(candidates))
	for _, raw := range candidates {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, exists := deduped[key]; exists {
			continue
		}
		deduped[key] = groupEntry{Value: trimmed, Key: key}
	}
	if len(deduped) == 0 {
		return nil
	}

	ordered := make([]groupEntry, 0, len(deduped))
	for _, entry := range deduped {
		ordered = append(ordered, entry)
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Key == ordered[j].Key {
			return ordered[i].Value < ordered[j].Value
		}
		return ordered[i].Key < ordered[j].Key
	})

	out := make([]string, 0, len(ordered))
	for _, entry := range ordered {
		out = append(out, entry.Value)
	}
	return out
}

// GroupNameAlias returns the legacy single-group alias for compatibility.
func GroupNameAlias(groupNames []string) string {
	if len(groupNames) == 0 {
		return ""
	}
	return strings.TrimSpace(groupNames[0])
}
