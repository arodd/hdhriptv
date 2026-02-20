package stream

import (
	"sort"
	"sync"
	"time"
)

type tuneBackoffDecision struct {
	Allowed          bool
	RetryAfter       time.Duration
	BackoffUntil     time.Time
	FailuresInWindow int
	Limit            int
	Interval         time.Duration
	ScopeKey         int64
}

type tuneBackoffScope struct {
	failures    []time.Time
	coolUntil   time.Time
	lastTouched time.Time
}

type tuneBackoffGate struct {
	mu sync.Mutex

	limit    int
	interval time.Duration
	cooldown time.Duration
	scopeCap int
	scopes   map[int64]*tuneBackoffScope
}

const defaultTuneBackoffScopeCap = 256

func newTuneBackoffGate(limit int, interval, cooldown time.Duration) *tuneBackoffGate {
	if limit <= 0 || interval <= 0 || cooldown <= 0 {
		return nil
	}
	return &tuneBackoffGate{
		limit:    limit,
		interval: interval,
		cooldown: cooldown,
		scopeCap: defaultTuneBackoffScopeCap,
		scopes:   make(map[int64]*tuneBackoffScope),
	}
}

func (g *tuneBackoffGate) allow(scopeKey int64, now time.Time) tuneBackoffDecision {
	if g == nil {
		return tuneBackoffDecision{Allowed: true}
	}

	scopeKey = normalizeTuneBackoffScopeKey(scopeKey)
	now = now.UTC()

	g.mu.Lock()
	defer g.mu.Unlock()

	g.pruneIdleScopesLocked(now)

	scope := g.scopeLocked(scopeKey, now)
	g.pruneFailuresLocked(scope, now)

	if scope.coolUntil.After(now) {
		decision := tuneBackoffDecision{
			Allowed:          false,
			RetryAfter:       scope.coolUntil.Sub(now),
			BackoffUntil:     scope.coolUntil,
			FailuresInWindow: len(scope.failures),
			Limit:            g.limit,
			Interval:         g.interval,
			ScopeKey:         scopeKey,
		}
		g.enforceScopeCapLocked(now, scopeKey)
		return decision
	}

	g.dropScopeIfIdleLocked(scopeKey, scope, now)
	g.enforceScopeCapLocked(now, scopeKey)
	return tuneBackoffDecision{
		Allowed:          true,
		FailuresInWindow: len(scope.failures),
		Limit:            g.limit,
		Interval:         g.interval,
		ScopeKey:         scopeKey,
	}
}

func (g *tuneBackoffGate) recordFailure(scopeKey int64, now time.Time) tuneBackoffDecision {
	if g == nil {
		return tuneBackoffDecision{Allowed: true}
	}

	scopeKey = normalizeTuneBackoffScopeKey(scopeKey)
	now = now.UTC()

	g.mu.Lock()
	defer g.mu.Unlock()

	g.pruneIdleScopesLocked(now)

	scope := g.scopeLocked(scopeKey, now)
	g.pruneFailuresLocked(scope, now)

	scope.failures = append(scope.failures, now)
	if len(scope.failures) >= g.limit {
		scope.coolUntil = now.Add(g.cooldown)
		scope.failures = scope.failures[:0]
		decision := tuneBackoffDecision{
			Allowed:      false,
			RetryAfter:   g.cooldown,
			BackoffUntil: scope.coolUntil,
			Limit:        g.limit,
			Interval:     g.interval,
			ScopeKey:     scopeKey,
		}
		g.enforceScopeCapLocked(now, scopeKey)
		return decision
	}

	g.enforceScopeCapLocked(now, scopeKey)
	return tuneBackoffDecision{
		Allowed:          true,
		FailuresInWindow: len(scope.failures),
		Limit:            g.limit,
		Interval:         g.interval,
		ScopeKey:         scopeKey,
	}
}

func (g *tuneBackoffGate) recordSuccess(scopeKey int64, now time.Time) {
	if g == nil {
		return
	}

	scopeKey = normalizeTuneBackoffScopeKey(scopeKey)
	now = now.UTC()

	g.mu.Lock()
	defer g.mu.Unlock()

	g.pruneIdleScopesLocked(now)

	scope := g.scopeLocked(scopeKey, now)
	if len(scope.failures) > 0 {
		scope.failures = scope.failures[:0]
	}
	if !scope.coolUntil.After(now) {
		scope.coolUntil = time.Time{}
	}
	g.dropScopeIfIdleLocked(scopeKey, scope, now)
	g.enforceScopeCapLocked(now, scopeKey)
}

func normalizeTuneBackoffScopeKey(scopeKey int64) int64 {
	if scopeKey <= 0 {
		return 0
	}
	return scopeKey
}

func (g *tuneBackoffGate) scopeLocked(scopeKey int64, now time.Time) *tuneBackoffScope {
	scope := g.scopes[scopeKey]
	if scope == nil {
		scope = &tuneBackoffScope{}
		g.scopes[scopeKey] = scope
	}
	scope.lastTouched = now
	return scope
}

func (g *tuneBackoffGate) pruneFailuresLocked(scope *tuneBackoffScope, now time.Time) {
	if scope == nil || len(scope.failures) == 0 {
		return
	}

	cutoff := now.Add(-g.interval)
	trim := 0
	for trim < len(scope.failures) && scope.failures[trim].Before(cutoff) {
		trim++
	}
	if trim == 0 {
		return
	}
	copy(scope.failures, scope.failures[trim:])
	scope.failures = scope.failures[:len(scope.failures)-trim]
}

func (g *tuneBackoffGate) dropScopeIfIdleLocked(scopeKey int64, scope *tuneBackoffScope, now time.Time) {
	if g.scopeIsIdleLocked(scope, now) {
		delete(g.scopes, scopeKey)
	}
}

func (g *tuneBackoffGate) scopeIsIdleLocked(scope *tuneBackoffScope, now time.Time) bool {
	if scope == nil {
		return false
	}
	if len(scope.failures) > 0 {
		return false
	}
	if scope.coolUntil.After(now) {
		return false
	}
	return true
}

func (g *tuneBackoffGate) pruneIdleScopesLocked(now time.Time) {
	for key, scope := range g.scopes {
		g.pruneFailuresLocked(scope, now)
		if g.scopeIsIdleLocked(scope, now) {
			delete(g.scopes, key)
		}
	}
}

type tuneBackoffScopeEviction struct {
	key        int64
	inCooldown bool
	lastTouch  time.Time
}

func (g *tuneBackoffGate) enforceScopeCapLocked(now time.Time, preserveScopeKey int64) {
	if g.scopeCap <= 0 || len(g.scopes) <= g.scopeCap {
		return
	}

	candidates := make([]tuneBackoffScopeEviction, 0, len(g.scopes))
	for key, scope := range g.scopes {
		if key == preserveScopeKey {
			continue
		}
		candidates = append(candidates, tuneBackoffScopeEviction{
			key:        key,
			inCooldown: scope.coolUntil.After(now),
			lastTouch:  scope.lastTouched,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].inCooldown != candidates[j].inCooldown {
			return !candidates[i].inCooldown && candidates[j].inCooldown
		}
		if !candidates[i].lastTouch.Equal(candidates[j].lastTouch) {
			return candidates[i].lastTouch.Before(candidates[j].lastTouch)
		}
		return candidates[i].key < candidates[j].key
	})

	for _, candidate := range candidates {
		if len(g.scopes) <= g.scopeCap {
			return
		}
		delete(g.scopes, candidate.key)
	}
}
