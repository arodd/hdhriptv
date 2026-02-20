package sqlite

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"unicode/utf8"

	modernsqlite "modernc.org/sqlite"
)

const (
	catalogSearchRegexClause    = "catalog_regex_match(name, ?) = 1"
	maxCatalogRegexPatternRunes = 256
	catalogRegexCacheSize       = 256
)

var catalogRegexCache = newCompiledCatalogRegexCache(catalogRegexCacheSize)

func init() {
	modernsqlite.MustRegisterDeterministicScalarFunction(
		"catalog_regex_match",
		2,
		func(_ *modernsqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			if len(args) < 2 {
				return int64(0), nil
			}

			name := driverValueString(args[0])
			pattern := driverValueString(args[1])
			if strings.TrimSpace(pattern) == "" {
				return int64(0), nil
			}

			compiled, err := compileCatalogRegexPattern(pattern)
			if err != nil {
				return nil, fmt.Errorf("compile catalog regex pattern: %w", err)
			}
			if compiled.MatchString(name) {
				return int64(1), nil
			}
			return int64(0), nil
		},
	)
}

type compiledCatalogRegexCache struct {
	mu       sync.Mutex
	patterns map[string]*regexp.Regexp
	order    []string
	nextEvic int
}

func newCompiledCatalogRegexCache(capacity int) *compiledCatalogRegexCache {
	if capacity < 1 {
		capacity = 1
	}
	return &compiledCatalogRegexCache{
		patterns: make(map[string]*regexp.Regexp, capacity),
		order:    make([]string, 0, capacity),
	}
}

func (c *compiledCatalogRegexCache) getOrCompile(pattern string) (*regexp.Regexp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if compiled, ok := c.patterns[pattern]; ok {
		return compiled, nil
	}

	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	if len(c.order) < cap(c.order) {
		c.order = append(c.order, pattern)
	} else {
		evict := c.order[c.nextEvic]
		delete(c.patterns, evict)
		c.order[c.nextEvic] = pattern
		c.nextEvic = (c.nextEvic + 1) % len(c.order)
	}
	c.patterns[pattern] = compiled

	return compiled, nil
}

func driverValueString(value driver.Value) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func compileCatalogRegexPattern(pattern string) (*regexp.Regexp, error) {
	return catalogRegexCache.getOrCompile(pattern)
}

func buildCatalogRegexPattern(raw string) (string, error) {
	pattern := strings.TrimSpace(raw)
	if pattern == "" {
		return "", nil
	}
	if utf8.RuneCountInString(pattern) > maxCatalogRegexPatternRunes {
		return "", fmt.Errorf("invalid regex pattern %q: pattern length exceeds %d runes", pattern, maxCatalogRegexPatternRunes)
	}
	// Keep regex mode case-insensitive by default for parity with token mode.
	patternWithFlags := "(?i)" + pattern
	if _, err := compileCatalogRegexPattern(patternWithFlags); err != nil {
		return "", fmt.Errorf("invalid regex pattern %q: %w", pattern, err)
	}
	return patternWithFlags, nil
}

func validateCatalogSearchRegexQuery(raw string) error {
	_, err := buildCatalogRegexPattern(raw)
	return err
}
