package middleware

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestTimeoutAppliesToNonExemptPath(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(40 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	})

	handler := Timeout(10*time.Millisecond, "/auto/")(base)
	req := httptest.NewRequest(http.MethodGet, "http://example.local/api/items", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("expected timeout response body to be non-empty")
	}
}

func TestTimeoutSkipsExemptPath(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(25 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	})

	handler := Timeout(10*time.Millisecond, "/auto/")(base)
	req := httptest.NewRequest(http.MethodGet, "http://example.local/auto/v101", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestRateLimitByIP(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := RateLimitByIP(1, 1, time.Minute)(base)

	req1 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req1.RemoteAddr = "192.168.1.10:12000"
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusNoContent {
		t.Fatalf("first request status = %d, want %d", rec1.Code, http.StatusNoContent)
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req2.RemoteAddr = "192.168.1.10:12001"
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want %d", rec2.Code, http.StatusTooManyRequests)
	}

	req3 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req3.RemoteAddr = "192.168.1.11:12002"
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusNoContent {
		t.Fatalf("third request status = %d, want %d", rec3.Code, http.StatusNoContent)
	}
}

func TestRateLimitByIPDisabled(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := RateLimitByIP(0, 0, time.Minute)(base)

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
		req.RemoteAddr = "192.168.1.10:12000"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("request %d status = %d, want %d", i+1, rec.Code, http.StatusNoContent)
		}
	}
}

func TestRateLimitByIPEvictsIdleClientEntries(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := RateLimitByIP(1, 1, 20*time.Millisecond)(base)

	req1 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req1.RemoteAddr = "192.168.1.20:12000"
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusNoContent {
		t.Fatalf("first request status = %d, want %d", rec1.Code, http.StatusNoContent)
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req2.RemoteAddr = "192.168.1.20:12001"
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want %d", rec2.Code, http.StatusTooManyRequests)
	}

	time.Sleep(35 * time.Millisecond)

	req3 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req3.RemoteAddr = "192.168.1.20:12002"
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusNoContent {
		t.Fatalf("third request status = %d, want %d (expected stale limiter eviction)", rec3.Code, http.StatusNoContent)
	}
}

func TestRateLimitByIPWithConfigMaxClientsEvictsLeastRecentlySeen(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := RateLimitByIPWithConfig(1, 1, RateLimitByIPConfig{
		IdleTTL:      time.Minute,
		MaxClients:   2,
		CleanupBatch: 4,
	})(base)

	reqA1 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	reqA1.RemoteAddr = "192.168.1.10:12000"
	recA1 := httptest.NewRecorder()
	handler.ServeHTTP(recA1, reqA1)
	if recA1.Code != http.StatusNoContent {
		t.Fatalf("A request 1 status = %d, want %d", recA1.Code, http.StatusNoContent)
	}

	reqA2 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	reqA2.RemoteAddr = "192.168.1.10:12001"
	recA2 := httptest.NewRecorder()
	handler.ServeHTTP(recA2, reqA2)
	if recA2.Code != http.StatusTooManyRequests {
		t.Fatalf("A request 2 status = %d, want %d", recA2.Code, http.StatusTooManyRequests)
	}

	reqB := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	reqB.RemoteAddr = "192.168.1.11:12000"
	recB := httptest.NewRecorder()
	handler.ServeHTTP(recB, reqB)
	if recB.Code != http.StatusNoContent {
		t.Fatalf("B request status = %d, want %d", recB.Code, http.StatusNoContent)
	}

	reqC := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	reqC.RemoteAddr = "192.168.1.12:12000"
	recC := httptest.NewRecorder()
	handler.ServeHTTP(recC, reqC)
	if recC.Code != http.StatusNoContent {
		t.Fatalf("C request status = %d, want %d", recC.Code, http.StatusNoContent)
	}

	reqA3 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	reqA3.RemoteAddr = "192.168.1.10:12002"
	recA3 := httptest.NewRecorder()
	handler.ServeHTTP(recA3, reqA3)
	if recA3.Code != http.StatusNoContent {
		t.Fatalf("A request 3 status = %d, want %d after cap eviction", recA3.Code, http.StatusNoContent)
	}
}

func TestRateLimitByIPTrustedProxyUsesForwardedClientIP(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := RateLimitByIPWithConfig(1, 1, RateLimitByIPConfig{
		IdleTTL:           time.Minute,
		MaxClients:        32,
		TrustedProxyCIDRs: []string{"10.0.0.0/8"},
	})(base)

	reqA1 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	reqA1.RemoteAddr = "10.1.2.3:12000"
	reqA1.Header.Set("X-Forwarded-For", "198.51.100.10")
	recA1 := httptest.NewRecorder()
	handler.ServeHTTP(recA1, reqA1)
	if recA1.Code != http.StatusNoContent {
		t.Fatalf("A request 1 status = %d, want %d", recA1.Code, http.StatusNoContent)
	}

	reqB1 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	reqB1.RemoteAddr = "10.1.2.3:12001"
	reqB1.Header.Set("X-Forwarded-For", "198.51.100.11")
	recB1 := httptest.NewRecorder()
	handler.ServeHTTP(recB1, reqB1)
	if recB1.Code != http.StatusNoContent {
		t.Fatalf("B request status = %d, want %d", recB1.Code, http.StatusNoContent)
	}

	reqA2 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	reqA2.RemoteAddr = "10.1.2.3:12002"
	reqA2.Header.Set("X-Forwarded-For", "198.51.100.10")
	recA2 := httptest.NewRecorder()
	handler.ServeHTTP(recA2, reqA2)
	if recA2.Code != http.StatusTooManyRequests {
		t.Fatalf("A request 2 status = %d, want %d", recA2.Code, http.StatusTooManyRequests)
	}
}

func TestRateLimitByIPTrustedProxyPrefersForwardedHeader(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := RateLimitByIPWithConfig(1, 1, RateLimitByIPConfig{
		IdleTTL:           time.Minute,
		MaxClients:        32,
		TrustedProxyCIDRs: []string{"10.0.0.0/8"},
	})(base)

	req1 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req1.RemoteAddr = "10.9.8.7:12000"
	req1.Header.Set("Forwarded", "for=203.0.113.10")
	req1.Header.Set("X-Forwarded-For", "198.51.100.70")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusNoContent {
		t.Fatalf("request 1 status = %d, want %d", rec1.Code, http.StatusNoContent)
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req2.RemoteAddr = "10.9.8.7:12001"
	req2.Header.Set("Forwarded", "for=203.0.113.10")
	req2.Header.Set("X-Forwarded-For", "198.51.100.71")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("request 2 status = %d, want %d", rec2.Code, http.StatusTooManyRequests)
	}
}

func TestRateLimitByIPTrustedProxyXFFSpoofedLeftMostValueIgnored(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := RateLimitByIPWithConfig(1, 1, RateLimitByIPConfig{
		IdleTTL:           time.Minute,
		MaxClients:        32,
		TrustedProxyCIDRs: []string{"10.0.0.0/8", "203.0.113.0/24"},
	})(base)

	req1 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req1.RemoteAddr = "10.9.8.7:12000"
	req1.Header.Set("X-Forwarded-For", "198.51.100.200, 198.51.100.10, 203.0.113.9")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusNoContent {
		t.Fatalf("request 1 status = %d, want %d", rec1.Code, http.StatusNoContent)
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req2.RemoteAddr = "10.9.8.7:12001"
	req2.Header.Set("X-Forwarded-For", "198.51.100.201, 198.51.100.10, 203.0.113.9")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("request 2 status = %d, want %d", rec2.Code, http.StatusTooManyRequests)
	}
}

func TestRateLimitByIPTrustedProxyForwardedSpoofedLeftMostValueIgnored(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := RateLimitByIPWithConfig(1, 1, RateLimitByIPConfig{
		IdleTTL:           time.Minute,
		MaxClients:        32,
		TrustedProxyCIDRs: []string{"10.0.0.0/8", "203.0.113.0/24"},
	})(base)

	req1 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req1.RemoteAddr = "10.9.8.7:12000"
	req1.Header.Set("Forwarded", "for=198.51.100.200;proto=https, for=198.51.100.10, for=203.0.113.9")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusNoContent {
		t.Fatalf("request 1 status = %d, want %d", rec1.Code, http.StatusNoContent)
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req2.RemoteAddr = "10.9.8.7:12001"
	req2.Header.Set("Forwarded", "for=198.51.100.201;proto=https, for=198.51.100.10, for=203.0.113.9")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("request 2 status = %d, want %d", rec2.Code, http.StatusTooManyRequests)
	}
}

func TestRateLimitByIPIgnoresForwardedHeadersFromUntrustedPeer(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := RateLimitByIPWithConfig(1, 1, RateLimitByIPConfig{
		IdleTTL:           time.Minute,
		MaxClients:        32,
		TrustedProxyCIDRs: []string{"10.0.0.0/8"},
	})(base)

	req1 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req1.RemoteAddr = "192.0.2.10:12000"
	req1.Header.Set("Forwarded", "for=203.0.113.21")
	req1.Header.Set("X-Forwarded-For", "198.51.100.80")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusNoContent {
		t.Fatalf("request 1 status = %d, want %d", rec1.Code, http.StatusNoContent)
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req2.RemoteAddr = "192.0.2.10:12001"
	req2.Header.Set("Forwarded", "for=203.0.113.22")
	req2.Header.Set("X-Forwarded-For", "198.51.100.81")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("request 2 status = %d, want %d", rec2.Code, http.StatusTooManyRequests)
	}
}

func TestRateLimitByIPTrustedProxyMalformedForwardedHeaderFallsBackToRemoteAddr(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := RateLimitByIPWithConfig(1, 1, RateLimitByIPConfig{
		IdleTTL:           time.Minute,
		MaxClients:        32,
		TrustedProxyCIDRs: []string{"10.0.0.0/8"},
	})(base)

	req1 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req1.RemoteAddr = "10.1.1.30:12000"
	req1.Header.Set("Forwarded", "for=invalid-ip")
	req1.Header.Set("X-Forwarded-For", "198.51.100.50")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusNoContent {
		t.Fatalf("request 1 status = %d, want %d", rec1.Code, http.StatusNoContent)
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req2.RemoteAddr = "10.1.1.30:12001"
	req2.Header.Set("Forwarded", "for=still-invalid")
	req2.Header.Set("X-Forwarded-For", "198.51.100.51")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("request 2 status = %d, want %d", rec2.Code, http.StatusTooManyRequests)
	}
}

func TestRateLimitByIPTrustedProxyMalformedHeadersFallbackToRemoteAddr(t *testing.T) {
	base := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := RateLimitByIPWithConfig(1, 1, RateLimitByIPConfig{
		IdleTTL:           time.Minute,
		MaxClients:        32,
		TrustedProxyCIDRs: []string{"10.0.0.0/8"},
	})(base)

	req1 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req1.RemoteAddr = "10.1.1.20:12000"
	req1.Header.Set("Forwarded", "for=not-an-ip")
	req1.Header.Set("X-Forwarded-For", "still-not-an-ip")
	req1.Header.Set("X-Real-IP", "also-not-an-ip")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusNoContent {
		t.Fatalf("request 1 status = %d, want %d", rec1.Code, http.StatusNoContent)
	}

	req2 := httptest.NewRequest(http.MethodGet, "http://example.local/lineup.json", nil)
	req2.RemoteAddr = "10.1.1.20:12001"
	req2.Header.Set("Forwarded", "for=invalid")
	req2.Header.Set("X-Forwarded-For", "invalid-too")
	req2.Header.Set("X-Real-IP", "nope")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("request 2 status = %d, want %d", rec2.Code, http.StatusTooManyRequests)
	}
}

func TestIPLimiterStoreStatsLoggerRunsOutsideMutex(t *testing.T) {
	store := newIPLimiterStore(10, 2, RateLimitByIPConfig{
		IdleTTL:      time.Minute,
		MaxClients:   16,
		CleanupBatch: 4,
	})

	var logged bool
	var sawLocked bool
	store.logStatsFn = func(rateLimitStatsSnapshot) {
		logged = true
		if !store.mu.TryLock() {
			sawLocked = true
			return
		}
		store.mu.Unlock()
	}

	_ = store.limiterFor("198.51.100.10", time.Unix(100, 0))
	if !logged {
		t.Fatal("expected stats logger to run")
	}
	if sawLocked {
		t.Fatal("stats logger ran while limiter mutex was locked")
	}
}

func TestIPLimiterStoreSlowStatsLoggerDoesNotBlockConcurrentLimiterLookup(t *testing.T) {
	store := newIPLimiterStore(10, 2, RateLimitByIPConfig{
		IdleTTL:      time.Minute,
		MaxClients:   16,
		CleanupBatch: 4,
	})

	started := make(chan struct{})
	release := make(chan struct{})
	var logCalls int32
	store.logStatsFn = func(rateLimitStatsSnapshot) {
		if atomic.AddInt32(&logCalls, 1) == 1 {
			close(started)
			<-release
		}
	}

	firstDone := make(chan struct{})
	go func() {
		_ = store.limiterFor("198.51.100.20", time.Unix(200, 0))
		close(firstDone)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first stats log callback")
	}

	secondDone := make(chan struct{})
	go func() {
		_ = store.limiterFor("198.51.100.21", time.Unix(201, 0))
		close(secondDone)
	}()

	select {
	case <-secondDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("concurrent limiter lookup blocked behind slow stats logger")
	}

	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first limiter lookup to return after releasing logger")
	}
}

func TestIPLimiterStoreStatsLoggerRespectsCadence(t *testing.T) {
	store := newIPLimiterStore(10, 2, RateLimitByIPConfig{
		IdleTTL:      time.Minute,
		MaxClients:   16,
		CleanupBatch: 4,
	})

	var logCalls int32
	store.logStatsFn = func(rateLimitStatsSnapshot) {
		atomic.AddInt32(&logCalls, 1)
	}

	base := time.Unix(300, 0)
	_ = store.limiterFor("198.51.100.30", base)
	_ = store.limiterFor("198.51.100.31", base.Add(time.Second))
	if got := atomic.LoadInt32(&logCalls); got != 1 {
		t.Fatalf("stats log calls within cadence window = %d, want 1", got)
	}

	_ = store.limiterFor("198.51.100.32", base.Add(defaultRateLimitStatsLogFreq+time.Second))
	if got := atomic.LoadInt32(&logCalls); got != 2 {
		t.Fatalf("stats log calls after cadence window = %d, want 2", got)
	}
}
