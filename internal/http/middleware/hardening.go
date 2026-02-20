package middleware

import (
	"container/list"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	defaultRateLimitIdleTTL      = 10 * time.Minute
	defaultRateLimitCleanupBatch = 64
	defaultRateLimitMaxClients   = 4096
	defaultRateLimitStatsLogFreq = 30 * time.Second
)

// Timeout enforces a request timeout for non-exempt paths.
func Timeout(timeout time.Duration, exemptPathPrefixes ...string) func(http.Handler) http.Handler {
	if timeout <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}

	prefixes := normalizePathPrefixes(exemptPathPrefixes)

	return func(next http.Handler) http.Handler {
		timeoutHandler := http.TimeoutHandler(next, timeout, "request timed out")
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if hasExemptPathPrefix(r.URL.Path, prefixes) {
				next.ServeHTTP(w, r)
				return
			}
			timeoutHandler.ServeHTTP(w, r)
		})
	}
}

type ipLimiter struct {
	key      string
	limiter  *rate.Limiter
	lastSeen time.Time
	elem     *list.Element
}

// RateLimitByIPConfig controls per-client limiter cache behavior.
type RateLimitByIPConfig struct {
	IdleTTL            time.Duration
	MaxClients         int
	CleanupBatch       int
	ExemptPathPrefixes []string
	TrustedProxyCIDRs  []string
}

func (c RateLimitByIPConfig) normalized() RateLimitByIPConfig {
	if c.IdleTTL <= 0 {
		c.IdleTTL = defaultRateLimitIdleTTL
	}
	if c.CleanupBatch < 1 {
		c.CleanupBatch = defaultRateLimitCleanupBatch
	}
	if c.MaxClients < 0 {
		c.MaxClients = 0
	}
	c.TrustedProxyCIDRs = normalizeEntries(c.TrustedProxyCIDRs)
	return c
}

type ipLimiterStore struct {
	mu           sync.Mutex
	clients      map[string]*ipLimiter
	lru          list.List
	rps          rate.Limit
	burst        int
	idleTTL      time.Duration
	maxClients   int
	cleanupBatch int
	highWater    int
	staleEvicted uint64
	capEvicted   uint64
	lastStatsLog time.Time
	pendingStats rateLimitStatsSnapshot
	hasPending   bool
	logStatsFn   func(rateLimitStatsSnapshot)
}

type rateLimitStatsSnapshot struct {
	clients      int
	maxClients   int
	highWater    int
	staleEvicted uint64
	capEvicted   uint64
}

func newIPLimiterStore(rps float64, burst int, cfg RateLimitByIPConfig) *ipLimiterStore {
	store := &ipLimiterStore{
		clients:      make(map[string]*ipLimiter),
		rps:          rate.Limit(rps),
		burst:        burst,
		idleTTL:      cfg.IdleTTL,
		maxClients:   cfg.MaxClients,
		cleanupBatch: cfg.CleanupBatch,
	}
	store.logStatsFn = logRateLimitStats
	return store
}

func (s *ipLimiterStore) limiterFor(key string, now time.Time) *rate.Limiter {
	s.mu.Lock()

	s.evictStaleLocked(now, s.cleanupBatch)

	if existing, ok := s.clients[key]; ok {
		existing.lastSeen = now
		s.lru.MoveToBack(existing.elem)
		pending, shouldLog := s.takePendingStatsLocked()
		logStats := s.logStatsFn
		s.mu.Unlock()
		if shouldLog && logStats != nil {
			logStats(pending)
		}
		return existing.limiter
	}

	s.ensureCapacityLocked(now)

	created := &ipLimiter{
		key:      key,
		limiter:  rate.NewLimiter(s.rps, s.burst),
		lastSeen: now,
	}
	created.elem = s.lru.PushBack(created)
	s.clients[key] = created
	if len(s.clients) > s.highWater {
		s.highWater = len(s.clients)
		s.maybeQueueStatsLogLocked(now)
	}
	pending, shouldLog := s.takePendingStatsLocked()
	logStats := s.logStatsFn
	s.mu.Unlock()
	if shouldLog && logStats != nil {
		logStats(pending)
	}
	return created.limiter
}

func (s *ipLimiterStore) ensureCapacityLocked(now time.Time) {
	if s.maxClients < 1 {
		return
	}
	for len(s.clients) >= s.maxClients {
		if removed := s.evictStaleLocked(now, s.cleanupBatch); removed > 0 {
			continue
		}
		if !s.evictOldestLocked(now) {
			return
		}
	}
}

func (s *ipLimiterStore) evictStaleLocked(now time.Time, max int) int {
	if max < 1 {
		max = 1
	}

	removed := 0
	for removed < max {
		oldestElem := s.lru.Front()
		if oldestElem == nil {
			break
		}
		oldest, _ := oldestElem.Value.(*ipLimiter)
		if oldest == nil {
			s.lru.Remove(oldestElem)
			continue
		}
		if now.Sub(oldest.lastSeen) < s.idleTTL {
			break
		}
		s.removeEntryLocked(oldest)
		s.staleEvicted++
		removed++
	}
	if removed > 0 {
		s.maybeQueueStatsLogLocked(now)
	}
	return removed
}

func (s *ipLimiterStore) evictOldestLocked(now time.Time) bool {
	oldestElem := s.lru.Front()
	if oldestElem == nil {
		return false
	}
	oldest, _ := oldestElem.Value.(*ipLimiter)
	if oldest == nil {
		s.lru.Remove(oldestElem)
		return true
	}
	s.removeEntryLocked(oldest)
	s.capEvicted++
	s.maybeQueueStatsLogLocked(now)
	return true
}

func (s *ipLimiterStore) removeEntryLocked(entry *ipLimiter) {
	if entry == nil {
		return
	}
	delete(s.clients, entry.key)
	if entry.elem != nil {
		s.lru.Remove(entry.elem)
		entry.elem = nil
	}
}

func (s *ipLimiterStore) maybeQueueStatsLogLocked(now time.Time) {
	if !s.lastStatsLog.IsZero() && now.Sub(s.lastStatsLog) < defaultRateLimitStatsLogFreq {
		return
	}
	s.lastStatsLog = now
	s.pendingStats = rateLimitStatsSnapshot{
		clients:      len(s.clients),
		maxClients:   s.maxClients,
		highWater:    s.highWater,
		staleEvicted: s.staleEvicted,
		capEvicted:   s.capEvicted,
	}
	s.hasPending = true
}

func (s *ipLimiterStore) takePendingStatsLocked() (rateLimitStatsSnapshot, bool) {
	if !s.hasPending {
		return rateLimitStatsSnapshot{}, false
	}
	pending := s.pendingStats
	s.pendingStats = rateLimitStatsSnapshot{}
	s.hasPending = false
	return pending, true
}

func logRateLimitStats(stats rateLimitStatsSnapshot) {
	slog.Default().Info("rate-limit client map stats",
		"clients", stats.clients,
		"max_clients", stats.maxClients,
		"high_water", stats.highWater,
		"stale_evictions", stats.staleEvicted,
		"capacity_evictions", stats.capEvicted,
	)
}

// RateLimitByIP applies token-bucket rate limiting keyed by client IP.
func RateLimitByIP(rps float64, burst int, idleTTL time.Duration, exemptPathPrefixes ...string) func(http.Handler) http.Handler {
	return RateLimitByIPWithConfig(rps, burst, RateLimitByIPConfig{
		IdleTTL:            idleTTL,
		MaxClients:         defaultRateLimitMaxClients,
		ExemptPathPrefixes: exemptPathPrefixes,
	})
}

// RateLimitByIPWithConfig applies token-bucket rate limiting keyed by client IP
// with bounded stale cleanup and optional client cardinality cap.
func RateLimitByIPWithConfig(rps float64, burst int, cfg RateLimitByIPConfig) func(http.Handler) http.Handler {
	if rps <= 0 || burst < 1 {
		return func(next http.Handler) http.Handler { return next }
	}
	cfg = cfg.normalized()
	prefixes := normalizePathPrefixes(cfg.ExemptPathPrefixes)
	store := newIPLimiterStore(rps, burst, cfg)
	resolver := newClientIdentityResolver(cfg.TrustedProxyCIDRs)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if hasExemptPathPrefix(r.URL.Path, prefixes) {
				next.ServeHTTP(w, r)
				return
			}

			key := resolver.rateLimitKey(r)
			limiter := store.limiterFor(key, time.Now())
			allowed := limiter.Allow()

			if !allowed {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func normalizePathPrefixes(prefixes []string) []string {
	return normalizeEntries(prefixes)
}

func normalizeEntries(values []string) []string {
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}
	return normalized
}

func hasExemptPathPrefix(path string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func clientIPKey(remoteAddr string) string {
	remoteAddr = strings.TrimSpace(remoteAddr)
	if remoteAddr == "" {
		return "unknown"
	}

	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return "unknown"
	}
	return host
}

type clientIdentityResolver struct {
	trustedProxyNets []*net.IPNet
}

func newClientIdentityResolver(cidrs []string) clientIdentityResolver {
	nets := make([]*net.IPNet, 0, len(cidrs))
	for _, raw := range cidrs {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		if _, network, err := net.ParseCIDR(entry); err == nil {
			nets = append(nets, network)
			continue
		}
		ip := net.ParseIP(entry)
		if ip != nil {
			maskBits := 128
			if v4 := ip.To4(); v4 != nil {
				ip = v4
				maskBits = 32
			}
			nets = append(nets, &net.IPNet{
				IP:   ip,
				Mask: net.CIDRMask(maskBits, maskBits),
			})
			continue
		}
		slog.Default().Warn("ignoring invalid rate-limit trusted proxy entry", "entry", entry)
	}
	return clientIdentityResolver{trustedProxyNets: nets}
}

func (r clientIdentityResolver) rateLimitKey(req *http.Request) string {
	fallback := clientIPKey(req.RemoteAddr)
	if len(r.trustedProxyNets) == 0 {
		return fallback
	}

	peerIP := parseRemoteAddrIP(req.RemoteAddr)
	if !r.isTrustedPeer(peerIP) {
		return fallback
	}

	if values := req.Header.Values("Forwarded"); len(values) > 0 {
		chain, ok := parseForwardedHeader(values)
		if !ok {
			return fallback
		}
		if ip := r.resolveForwardedChain(peerIP, chain); ip != nil {
			return ip.String()
		}
		return fallback
	}
	if value := strings.TrimSpace(req.Header.Get("X-Forwarded-For")); value != "" {
		chain, ok := parseXForwardedForHeader(value)
		if !ok {
			return fallback
		}
		if ip := r.resolveForwardedChain(peerIP, chain); ip != nil {
			return ip.String()
		}
		return fallback
	}
	if value := strings.TrimSpace(req.Header.Get("X-Real-IP")); value != "" {
		if ip := parseForwardedForValue(value); ip != nil {
			return ip.String()
		}
		return fallback
	}
	return fallback
}

func (r clientIdentityResolver) resolveForwardedChain(peerIP net.IP, chain []net.IP) net.IP {
	if !r.isTrustedPeer(peerIP) || len(chain) == 0 {
		return nil
	}
	for i := len(chain) - 1; i >= 0; i-- {
		if r.isTrustedPeer(chain[i]) {
			continue
		}
		return chain[i]
	}
	return nil
}

func (r clientIdentityResolver) isTrustedPeer(peerIP net.IP) bool {
	if peerIP == nil {
		return false
	}
	for _, network := range r.trustedProxyNets {
		if network.Contains(peerIP) {
			return true
		}
	}
	return false
}

func parseRemoteAddrIP(remoteAddr string) net.IP {
	remoteAddr = strings.TrimSpace(remoteAddr)
	if remoteAddr == "" {
		return nil
	}

	host := remoteAddr
	if parsedHost, _, err := net.SplitHostPort(remoteAddr); err == nil {
		host = strings.TrimSpace(parsedHost)
	}
	return parseIPLiteral(host)
}

func parseForwardedHeader(values []string) ([]net.IP, bool) {
	chain := make([]net.IP, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return nil, false
		}
		entries := strings.Split(value, ",")
		for _, entry := range entries {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				return nil, false
			}
			params := strings.Split(entry, ";")
			foundFor := false
			for _, param := range params {
				key, rawValue, ok := strings.Cut(param, "=")
				if !ok {
					continue
				}
				if !strings.EqualFold(strings.TrimSpace(key), "for") {
					continue
				}
				foundFor = true
				ip := parseForwardedForValue(rawValue)
				if ip == nil {
					return nil, false
				}
				chain = append(chain, ip)
				break
			}
			if !foundFor {
				return nil, false
			}
		}
	}
	if len(chain) == 0 {
		return nil, false
	}
	return chain, true
}

func parseXForwardedForHeader(value string) ([]net.IP, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, false
	}
	entries := strings.Split(value, ",")
	chain := make([]net.IP, 0, len(entries))
	for _, entry := range entries {
		ip := parseForwardedForValue(entry)
		if ip == nil {
			return nil, false
		}
		chain = append(chain, ip)
	}
	return chain, len(chain) > 0
}

func parseForwardedForValue(raw string) net.IP {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}

	value = strings.Trim(value, "\"")
	if strings.EqualFold(value, "unknown") || strings.HasPrefix(value, "_") {
		return nil
	}

	if strings.HasPrefix(value, "[") {
		if host, _, err := net.SplitHostPort(value); err == nil {
			return parseIPLiteral(host)
		}
		value = strings.TrimSuffix(strings.TrimPrefix(value, "["), "]")
		return parseIPLiteral(value)
	}

	if host, _, err := net.SplitHostPort(value); err == nil {
		if ip := parseIPLiteral(host); ip != nil {
			return ip
		}
	}

	return parseIPLiteral(value)
}

func parseIPLiteral(raw string) net.IP {
	raw = strings.TrimSpace(raw)
	raw = strings.Trim(raw, "\"")
	if raw == "" {
		return nil
	}

	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		raw = strings.TrimPrefix(strings.TrimSuffix(raw, "]"), "[")
	}
	if zoneSep := strings.Index(raw, "%"); zoneSep >= 0 {
		raw = raw[:zoneSep]
	}

	return net.ParseIP(raw)
}
