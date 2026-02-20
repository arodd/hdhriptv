package jobs

import (
	"strconv"
	"strings"
)

const analysisErrorBucketsSummaryKey = "analysis_error_buckets="
const autoPrioritizeSkipReasonBucketsSummaryKey = "skip_reason_buckets="

// ParseAnalysisErrorBuckets extracts categorized auto-prioritize error counts
// from a run summary string. It returns nil when no parseable buckets exist.
func ParseAnalysisErrorBuckets(summary string) map[string]int {
	return parseSummaryBuckets(summary, analysisErrorBucketsSummaryKey)
}

// ParseAutoPrioritizeSkipReasonBuckets extracts per-reason skipped channel counts
// from an auto-prioritize run summary string.
func ParseAutoPrioritizeSkipReasonBuckets(summary string) map[string]int {
	return parseSummaryBuckets(summary, autoPrioritizeSkipReasonBucketsSummaryKey)
}

func parseSummaryBuckets(summary, key string) map[string]int {
	summary = strings.TrimSpace(summary)
	if summary == "" {
		return nil
	}

	idx := strings.Index(summary, key)
	if idx < 0 {
		return nil
	}

	raw := summary[idx+len(key):]
	if cut := strings.IndexByte(raw, ' '); cut >= 0 {
		raw = raw[:cut]
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "none") {
		return nil
	}

	buckets := make(map[string]int)
	parts := strings.Split(raw, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		nameValue := strings.SplitN(part, ":", 2)
		if len(nameValue) != 2 {
			continue
		}
		name := strings.TrimSpace(nameValue[0])
		if name == "" {
			continue
		}
		count, err := strconv.Atoi(strings.TrimSpace(nameValue[1]))
		if err != nil || count <= 0 {
			continue
		}
		buckets[name] = count
	}

	if len(buckets) == 0 {
		return nil
	}
	return buckets
}
