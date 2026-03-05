package version

import (
	"regexp"
	"strings"
)

const (
	defaultVersion   = "v0.0.0-dev"
	defaultCommit    = "unknown"
	defaultBuildTime = "unknown"
)

var (
	// Version should be set by build ldflags.
	// Release example: v1.0.6
	// Dev example: v1.0.6-dev+abc123def456
	Version = defaultVersion

	// Commit should be set by build ldflags (short or full git SHA).
	Commit = defaultCommit

	// BuildTime should be set by build ldflags using UTC RFC3339.
	BuildTime = defaultBuildTime
)

var plainSemverRE = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)

type Info struct {
	Version   string
	Source    string
	Commit    string
	BuildTime string
}

// Current returns normalized runtime build metadata.
func Current() Info {
	normalizedVersion := normalizeVersion(Version)
	return Info{
		Version:   normalizedVersion,
		Source:    classifySource(normalizedVersion),
		Commit:    normalizeCommit(Commit),
		BuildTime: normalizeBuildTime(BuildTime),
	}
}

func normalizeVersion(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" || strings.EqualFold(value, "dev") {
		return defaultVersion
	}
	if plainSemverRE.MatchString(value) {
		return "v" + value
	}
	return value
}

func classifySource(version string) string {
	normalized := strings.ToLower(strings.TrimSpace(version))
	if strings.Contains(normalized, "-dev") {
		return "dev"
	}
	if strings.HasPrefix(normalized, "v") {
		return "release"
	}
	return "custom"
}

func normalizeCommit(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" || strings.EqualFold(value, defaultCommit) {
		return defaultCommit
	}
	if len(value) > 12 {
		return value[:12]
	}
	return value
}

func normalizeBuildTime(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return defaultBuildTime
	}
	return value
}
