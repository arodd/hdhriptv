package discovery

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestServerRespondsToDiscoverRequest(t *testing.T) {
	srv, err := NewServer(Config{
		ListenAddr: "127.0.0.1:0",
		DeviceID:   "1234ABCD",
		DeviceAuth: "auth-token",
		TunerCount: 2,
		HTTPAddr:   ":5004",
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		_ = srv.Close()

		select {
		case serveErr := <-errCh:
			if serveErr != nil {
				t.Fatalf("Serve() error = %v", serveErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Serve() did not exit")
		}
	})

	addr, ok := srv.Addr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("Addr() type = %T, want *net.UDPAddr", srv.Addr())
	}

	client, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatalf("DialUDP() error = %v", err)
	}
	defer client.Close()

	requestFrame, err := Packet{
		Type: typeDiscoverReq,
		Tags: []TLV{
			{Tag: tagDeviceType, Value: uint32Bytes(deviceTypeTuner)},
			{Tag: tagDeviceID, Value: uint32Bytes(deviceIDWildcard)},
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("Marshal(request) error = %v", err)
	}

	if _, err := client.Write(requestFrame); err != nil {
		t.Fatalf("Write(request) error = %v", err)
	}

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	respBuf := make([]byte, 2048)
	n, _, err := client.ReadFromUDP(respBuf)
	if err != nil {
		t.Fatalf("ReadFromUDP(response) error = %v", err)
	}

	resp, err := ParsePacket(respBuf[:n])
	if err != nil {
		t.Fatalf("ParsePacket(response) error = %v", err)
	}
	if resp.Type != typeDiscoverRpy {
		t.Fatalf("response type = %d, want %d", resp.Type, typeDiscoverRpy)
	}

	tagValues := make(map[byte][]byte, len(resp.Tags))
	for _, tag := range resp.Tags {
		tagValues[tag.Tag] = tag.Value
	}

	if got := binary.BigEndian.Uint32(tagValues[tagDeviceID]); got != 0x1234ABCD {
		t.Fatalf("device id = %#x, want %#x", got, uint32(0x1234ABCD))
	}
	if got := tagValues[tagTunerCount][0]; got != 2 {
		t.Fatalf("tuner count = %d, want 2", got)
	}

	baseURL := string(tagValues[tagBaseURL])
	if !strings.Contains(baseURL, ":5004") {
		t.Fatalf("base URL = %q, want advertised port 5004", baseURL)
	}

	lineupURL := string(tagValues[tagLineupURL])
	if lineupURL != baseURL+"/lineup.json" {
		t.Fatalf("lineup URL = %q, want %q", lineupURL, baseURL+"/lineup.json")
	}

	if got := string(tagValues[tagDeviceAuth]); got != "auth-token" {
		t.Fatalf("device auth = %q, want auth-token", got)
	}
}

func TestNewServerCapsLargeTunerCountForDiscoveryEncoding(t *testing.T) {
	srv, err := NewServer(Config{
		ListenAddr: "127.0.0.1:0",
		DeviceID:   "1234ABCD",
		TunerCount: 512,
		HTTPAddr:   ":5004",
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v, want nil with capped discovery count", err)
	}
	defer srv.Close()

	if got := srv.tunerCount.Load(); got != 255 {
		t.Fatalf("server tunerCount = %d, want 255 cap for discovery encoding", got)
	}
}

func TestServerSetDiscoveryAdvertisedTunerCountCapsAndStores(t *testing.T) {
	srv, err := NewServer(Config{
		ListenAddr: "127.0.0.1:0",
		DeviceID:   "1234ABCD",
		TunerCount: 2,
		HTTPAddr:   ":5004",
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	defer srv.Close()

	srv.SetDiscoveryAdvertisedTunerCount(370)
	if got := srv.tunerCount.Load(); got != 255 {
		t.Fatalf("tunerCount after large runtime update = %d, want 255 cap", got)
	}

	srv.SetDiscoveryAdvertisedTunerCount(0)
	if got := srv.tunerCount.Load(); got != 1 {
		t.Fatalf("tunerCount after zero runtime update = %d, want floor 1", got)
	}

	srv.SetDiscoveryAdvertisedTunerCount(17)
	if got := srv.tunerCount.Load(); got != 17 {
		t.Fatalf("tunerCount after nominal runtime update = %d, want 17", got)
	}
}

func TestServerIgnoresMismatchedDeviceIDRequest(t *testing.T) {
	srv, err := NewServer(Config{
		ListenAddr: "127.0.0.1:0",
		DeviceID:   "1234ABCD",
		TunerCount: 2,
		HTTPAddr:   ":5004",
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ctx)
	}()
	defer func() {
		cancel()
		_ = srv.Close()
		select {
		case serveErr := <-errCh:
			if serveErr != nil {
				t.Fatalf("Serve() error = %v", serveErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Serve() did not exit")
		}
	}()

	addr, ok := srv.Addr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("Addr() type = %T, want *net.UDPAddr", srv.Addr())
	}

	client, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatalf("DialUDP() error = %v", err)
	}
	defer client.Close()

	requestFrame, err := Packet{
		Type: typeDiscoverReq,
		Tags: []TLV{
			{Tag: tagDeviceType, Value: uint32Bytes(deviceTypeTuner)},
			{Tag: tagDeviceID, Value: uint32Bytes(0xDEADBEEF)},
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("Marshal(request) error = %v", err)
	}

	if _, err := client.Write(requestFrame); err != nil {
		t.Fatalf("Write(request) error = %v", err)
	}

	_ = client.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 2048)
	_, _, err = client.ReadFromUDP(buf)
	if err == nil {
		t.Fatal("ReadFromUDP() expected timeout, got response")
	}

	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("ReadFromUDP() error = %v, want timeout", err)
	}
}

func TestLocalHostForRemoteCachesResolverResult(t *testing.T) {
	t.Parallel()

	remote := &net.UDPAddr{IP: net.ParseIP("198.51.100.11"), Port: 65001}
	resolvedLocal := &net.UDPAddr{IP: net.ParseIP("192.0.2.25"), Port: 0}
	var resolverCalls int

	srv := &Server{
		hostCacheTTL: 30 * time.Second,
		hostCache:    make(map[string]routeHostCacheEntry),
		localAddrResolver: func(gotRemote *net.UDPAddr) (*net.UDPAddr, error) {
			if !gotRemote.IP.Equal(remote.IP) {
				t.Fatalf("resolver remote IP = %v, want %v", gotRemote.IP, remote.IP)
			}
			resolverCalls++
			return resolvedLocal, nil
		},
		interfaceAddrs: func() ([]net.Addr, error) {
			t.Fatal("interfaceAddrs fallback should not be invoked when resolver succeeds")
			return nil, nil
		},
	}

	first := srv.localHostForRemote(remote)
	second := srv.localHostForRemote(remote)
	if first != "192.0.2.25" || second != "192.0.2.25" {
		t.Fatalf("cached host = %q/%q, want 192.0.2.25", first, second)
	}
	if resolverCalls != 1 {
		t.Fatalf("resolver call count = %d, want 1", resolverCalls)
	}
}

func TestLocalHostForRemoteResolverFailurePrefersNonLoopbackFallback(t *testing.T) {
	t.Parallel()

	srv := &Server{
		hostCacheTTL: 30 * time.Second,
		hostCache:    make(map[string]routeHostCacheEntry),
		localAddrResolver: func(remote *net.UDPAddr) (*net.UDPAddr, error) {
			return nil, errors.New("route lookup unavailable")
		},
		interfaceAddrs: func() ([]net.Addr, error) {
			return []net.Addr{
				&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
				&net.IPNet{IP: net.ParseIP("192.168.10.55"), Mask: net.CIDRMask(24, 32)},
			}, nil
		},
	}

	host := srv.localHostForRemote(&net.UDPAddr{IP: net.ParseIP("10.0.0.10"), Port: 65001})
	if host != "192.168.10.55" {
		t.Fatalf("fallback host = %q, want 192.168.10.55", host)
	}
	if strings.HasPrefix(host, "127.") {
		t.Fatalf("fallback host should avoid localhost for wildcard LAN deployment, got %q", host)
	}
}

func TestLocalHostForRemoteCachePreservesPerRemoteResolution(t *testing.T) {
	t.Parallel()

	remoteA := &net.UDPAddr{IP: net.ParseIP("203.0.113.10"), Port: 65001}
	remoteB := &net.UDPAddr{IP: net.ParseIP("203.0.113.20"), Port: 65001}

	var (
		mu            sync.Mutex
		resolverCalls = make(map[string]int)
	)

	srv := &Server{
		hostCacheTTL: 30 * time.Second,
		hostCache:    make(map[string]routeHostCacheEntry),
		localAddrResolver: func(remote *net.UDPAddr) (*net.UDPAddr, error) {
			key := remote.IP.String()
			mu.Lock()
			resolverCalls[key]++
			mu.Unlock()

			switch key {
			case remoteA.IP.String():
				return &net.UDPAddr{IP: net.ParseIP("10.0.0.5")}, nil
			case remoteB.IP.String():
				return &net.UDPAddr{IP: net.ParseIP("10.0.1.5")}, nil
			default:
				return nil, errors.New("unexpected remote")
			}
		},
		interfaceAddrs: func() ([]net.Addr, error) {
			t.Fatal("interfaceAddrs fallback should not be invoked when resolver succeeds")
			return nil, nil
		},
	}

	if host := srv.localHostForRemote(remoteA); host != "10.0.0.5" {
		t.Fatalf("remote A host = %q, want 10.0.0.5", host)
	}
	if host := srv.localHostForRemote(remoteB); host != "10.0.1.5" {
		t.Fatalf("remote B host = %q, want 10.0.1.5", host)
	}
	if host := srv.localHostForRemote(remoteA); host != "10.0.0.5" {
		t.Fatalf("remote A cached host = %q, want 10.0.0.5", host)
	}
	if host := srv.localHostForRemote(remoteB); host != "10.0.1.5" {
		t.Fatalf("remote B cached host = %q, want 10.0.1.5", host)
	}

	mu.Lock()
	defer mu.Unlock()
	if resolverCalls[remoteA.IP.String()] != 1 {
		t.Fatalf("remote A resolver calls = %d, want 1", resolverCalls[remoteA.IP.String()])
	}
	if resolverCalls[remoteB.IP.String()] != 1 {
		t.Fatalf("remote B resolver calls = %d, want 1", resolverCalls[remoteB.IP.String()])
	}
}

func TestStoreHostCacheEnforcesMaxEntries(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	srv := &Server{
		hostCacheTTL: 30 * time.Second,
		hostCache:    make(map[string]routeHostCacheEntry),
	}

	for i := 0; i < maxRouteHostCacheEntries+128; i++ {
		key := fmt.Sprintf("remote:%04d", i)
		host := fmt.Sprintf("192.0.2.%d", (i%250)+1)
		srv.storeHostCache(key, host, now)

		if got := len(srv.hostCache); got > maxRouteHostCacheEntries {
			t.Fatalf("cache len = %d, want <= %d", got, maxRouteHostCacheEntries)
		}
	}

	if got := len(srv.hostCache); got != maxRouteHostCacheEntries {
		t.Fatalf("cache len = %d, want %d", got, maxRouteHostCacheEntries)
	}
}

func TestStoreHostCachePrunesExpiredEntriesWhenOverCap(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	srv := &Server{
		hostCacheTTL: 30 * time.Second,
		hostCache:    make(map[string]routeHostCacheEntry),
	}

	srv.hostCache["expired"] = routeHostCacheEntry{
		host:      "198.51.100.10",
		expiresAt: now.Add(-1 * time.Second),
	}
	for i := 0; i < maxRouteHostCacheEntries; i++ {
		key := fmt.Sprintf("fresh:%04d", i)
		srv.hostCache[key] = routeHostCacheEntry{
			host:      fmt.Sprintf("192.0.2.%d", (i%250)+1),
			expiresAt: now.Add(30 * time.Second),
		}
	}

	srv.storeHostCache("incoming", "203.0.113.7", now)

	if _, ok := srv.hostCache["expired"]; ok {
		t.Fatal("expired entry should be pruned")
	}
	if _, ok := srv.hostCache["incoming"]; !ok {
		t.Fatal("incoming entry should be retained")
	}
	if got := len(srv.hostCache); got != maxRouteHostCacheEntries {
		t.Fatalf("cache len = %d, want %d", got, maxRouteHostCacheEntries)
	}
}

func TestStoreHostCacheOverwriteDoesNotEvictAtCapacity(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	srv := &Server{
		hostCacheTTL: 30 * time.Second,
		hostCache:    make(map[string]routeHostCacheEntry),
	}

	keys := make([]string, 0, maxRouteHostCacheEntries)
	for i := 0; i < maxRouteHostCacheEntries; i++ {
		key := fmt.Sprintf("key:%04d", i)
		srv.hostCache[key] = routeHostCacheEntry{
			host:      fmt.Sprintf("192.0.2.%d", (i%250)+1),
			expiresAt: now.Add(30 * time.Second),
		}
		keys = append(keys, key)
	}

	existingKey := keys[0]
	srv.storeHostCache(existingKey, "203.0.113.99", now)

	if got := len(srv.hostCache); got != maxRouteHostCacheEntries {
		t.Fatalf("cache len = %d, want %d", got, maxRouteHostCacheEntries)
	}
	for _, key := range keys {
		if _, ok := srv.hostCache[key]; !ok {
			t.Fatalf("expected key %q to remain after overwrite", key)
		}
	}
	if got := srv.hostCache[existingKey].host; got != "203.0.113.99" {
		t.Fatalf("overwritten host = %q, want %q", got, "203.0.113.99")
	}
}
