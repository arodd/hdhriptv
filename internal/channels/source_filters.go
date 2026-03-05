package channels

import (
	"fmt"
	"sort"
	"strings"
)

// ValidateSourceIDs reports an input validation error when source selectors
// contain non-positive values.
func ValidateSourceIDs(sourceIDs []int64, field string) error {
	field = strings.TrimSpace(field)
	if field == "" {
		field = "source_ids"
	}
	for _, sourceID := range sourceIDs {
		if sourceID <= 0 {
			return fmt.Errorf("%s must contain only positive integers", field)
		}
	}
	return nil
}

// NormalizeSourceIDs deduplicates and deterministically orders source selectors.
// Non-positive source IDs are ignored.
func NormalizeSourceIDs(sourceIDs []int64) []int64 {
	if len(sourceIDs) == 0 {
		return []int64{}
	}

	deduped := make(map[int64]struct{}, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		if sourceID <= 0 {
			continue
		}
		deduped[sourceID] = struct{}{}
	}
	if len(deduped) == 0 {
		return []int64{}
	}

	out := make([]int64, 0, len(deduped))
	for sourceID := range deduped {
		out = append(out, sourceID)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}
