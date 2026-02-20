package discovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	errDiscoveryIgnoredNonTunerRequest = errors.New("discovery request does not target tuner devices")
	errDiscoveryIgnoredDeviceIDFilter  = errors.New("discovery request filtered by device id")
)

const (
	defaultRouteHostCacheTTL = 30 * time.Second
	maxRouteHostCacheEntries = 512
)

type localAddrResolver func(remoteAddr *net.UDPAddr) (*net.UDPAddr, error)
type interfaceAddrsProvider func() ([]net.Addr, error)

type routeHostCacheEntry struct {
	host      string
	expiresAt time.Time
}

type routeHostCacheEvictionCandidate struct {
	key       string
	expiresAt time.Time
}

// Config controls the UDP discovery responder.
type Config struct {
	ListenAddr     string
	DeviceID       string
	DeviceAuth     string
	TunerCount     int
	HTTPAddr       string
	LegacyHTTPAddr string
	Logger         *slog.Logger
}

// Server handles HDHomeRun UDP discovery requests.
type Server struct {
	conn *net.UDPConn

	logger         *slog.Logger
	deviceID       uint32
	deviceAuth     string
	tunerCount     uint8
	advertisedPort int

	localAddrResolver   localAddrResolver
	interfaceAddrs      interfaceAddrsProvider
	hostCacheTTL        time.Duration
	hostCacheMu         sync.Mutex
	hostCache           map[string]routeHostCacheEntry
	boundHost           string
	boundHostIsLoopback bool
}

func NewServer(cfg Config) (*Server, error) {
	listenAddr := strings.TrimSpace(cfg.ListenAddr)
	if listenAddr == "" {
		listenAddr = fmt.Sprintf(":%d", DiscoverPort)
	}

	deviceIDHex := strings.TrimSpace(cfg.DeviceID)
	if deviceIDHex == "" {
		return nil, errors.New("device-id is required")
	}
	deviceID, err := strconv.ParseUint(deviceIDHex, 16, 32)
	if err != nil {
		return nil, fmt.Errorf("parse device-id %q: %w", deviceIDHex, err)
	}

	if cfg.TunerCount < 1 {
		return nil, errors.New("tuner-count must be at least 1")
	}
	if cfg.TunerCount > 255 {
		return nil, errors.New("tuner-count must be <= 255 for discovery tag encoding")
	}

	port, err := parsePortFromAddr(cfg.HTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("parse http-addr: %w", err)
	}
	if strings.TrimSpace(cfg.LegacyHTTPAddr) != "" {
		legacyPort, err := parsePortFromAddr(cfg.LegacyHTTPAddr)
		if err != nil {
			return nil, fmt.Errorf("parse http-addr-legacy: %w", err)
		}
		port = legacyPort
	}

	udpAddr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve listen address %q: %w", listenAddr, err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("listen udp on %q: %w", listenAddr, err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	boundHost, boundHostIsLoopback := localHostFromAddr(conn.LocalAddr())

	return &Server{
		conn:                conn,
		logger:              logger,
		deviceID:            uint32(deviceID),
		deviceAuth:          strings.TrimSpace(cfg.DeviceAuth),
		tunerCount:          uint8(cfg.TunerCount),
		advertisedPort:      port,
		localAddrResolver:   dialLocalAddrForRemote,
		interfaceAddrs:      net.InterfaceAddrs,
		hostCacheTTL:        defaultRouteHostCacheTTL,
		hostCache:           make(map[string]routeHostCacheEntry),
		boundHost:           boundHost,
		boundHostIsLoopback: boundHostIsLoopback,
	}, nil
}

func (s *Server) Addr() net.Addr {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.conn.LocalAddr()
}

func (s *Server) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

func (s *Server) Serve(ctx context.Context) error {
	if s == nil || s.conn == nil {
		return errors.New("discovery server is not initialized")
	}

	buf := make([]byte, 2048)

	for {
		if err := s.conn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}

		n, remoteAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if ctx.Err() != nil {
					return nil
				}
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read udp datagram: %w", err)
		}

		if err := s.handleDatagram(buf[:n], remoteAddr); err != nil {
			reason := discoveryReasonBucket(err)
			s.logger.Debug(
				"ignored udp datagram",
				"remote_addr", remoteAddr.String(),
				"reason", reason,
				"error", err,
			)
		}
	}
}

func (s *Server) handleDatagram(frame []byte, remoteAddr *net.UDPAddr) error {
	query, err := ParseDiscoverRequest(frame)
	if err != nil {
		return fmt.Errorf("parse discover request: %w", err)
	}

	if !query.WantsTuner() || !query.WantsDeviceID(s.deviceID) {
		if !query.WantsTuner() {
			return errDiscoveryIgnoredNonTunerRequest
		}
		return errDiscoveryIgnoredDeviceIDFilter
	}

	baseURL := s.baseURLForRemote(remoteAddr)
	lineupURL := baseURL + "/lineup.json"

	respFrame, err := BuildDiscoverResponseFrame(DiscoverResponse{
		DeviceID:   s.deviceID,
		TunerCount: s.tunerCount,
		BaseURL:    baseURL,
		LineupURL:  lineupURL,
		DeviceAuth: s.deviceAuth,
	})
	if err != nil {
		return fmt.Errorf("build discover response: %w", err)
	}

	if _, err := s.conn.WriteToUDP(respFrame, remoteAddr); err != nil {
		return fmt.Errorf("write discover response: %w", err)
	}
	s.logger.Debug(
		"discovery response sent",
		"remote_addr", remoteAddr.String(),
		"base_url", baseURL,
		"lineup_url", lineupURL,
	)

	return nil
}

func discoveryReasonBucket(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errDiscoveryIgnoredNonTunerRequest):
		return "non_tuner_request"
	case errors.Is(err, errDiscoveryIgnoredDeviceIDFilter):
		return "device_id_filter_mismatch"
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(text, "parse discover request"):
		return "malformed_request"
	case strings.Contains(text, "write discover response"):
		return "write_failed"
	case strings.Contains(text, "build discover response"):
		return "response_build_failed"
	default:
		return "other"
	}
}

func (s *Server) baseURLForRemote(remoteAddr *net.UDPAddr) string {
	host := s.localHostForRemote(remoteAddr)
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return fmt.Sprintf("http://%s:%d", host, s.advertisedPort)
}

func (s *Server) localHostForRemote(remoteAddr *net.UDPAddr) string {
	if host := strings.TrimSpace(s.boundHost); host != "" {
		return host
	}

	cacheKey := routeHostCacheKey(remoteAddr)
	now := time.Now()
	if host, ok := s.lookupHostCache(cacheKey, now); ok {
		return host
	}

	host := s.resolveRouteHost(remoteAddr)
	if host == "" {
		host = s.fallbackHost(remoteAddr)
	}
	if host == "" {
		host = "127.0.0.1"
	}

	s.storeHostCache(cacheKey, host, now)
	return host
}

func (s *Server) resolveRouteHost(remoteAddr *net.UDPAddr) string {
	resolver := s.localAddrResolver
	if resolver == nil {
		resolver = dialLocalAddrForRemote
	}

	localAddr, err := resolver(remoteAddr)
	if err != nil || localAddr == nil {
		return ""
	}

	ip := canonicalIP(localAddr.IP)
	if ip == nil || ip.IsUnspecified() {
		return ""
	}

	if ip.IsLoopback() && !s.boundHostIsLoopback {
		if remoteAddr == nil || remoteAddr.IP == nil || !remoteAddr.IP.IsLoopback() {
			return ""
		}
	}

	return ip.String()
}

func (s *Server) fallbackHost(remoteAddr *net.UDPAddr) string {
	remoteIsLoopback := remoteAddr != nil && remoteAddr.IP != nil && remoteAddr.IP.IsLoopback()
	preferIPv6 := remoteAddr != nil && remoteAddr.IP != nil && remoteAddr.IP.To4() == nil

	nonLoopback, loopback := s.interfaceHosts(preferIPv6)
	if nonLoopback != "" {
		return nonLoopback
	}

	if s.boundHostIsLoopback && s.boundHost != "" {
		return s.boundHost
	}
	if remoteIsLoopback && loopback != "" {
		return loopback
	}
	if loopback != "" {
		return loopback
	}
	return ""
}

func (s *Server) interfaceHosts(preferIPv6 bool) (nonLoopback string, loopback string) {
	addrsFn := s.interfaceAddrs
	if addrsFn == nil {
		addrsFn = net.InterfaceAddrs
	}

	addrs, err := addrsFn()
	if err != nil {
		return "", ""
	}

	var fallbackNonLoopback string
	var fallbackLoopback string
	for _, addr := range addrs {
		ip := ipFromAddr(addr)
		if ip == nil || ip.IsUnspecified() {
			continue
		}

		host := ip.String()
		if ip.IsLoopback() {
			if ipMatchesFamily(ip, preferIPv6) {
				if loopback == "" {
					loopback = host
				}
			} else if fallbackLoopback == "" {
				fallbackLoopback = host
			}
			continue
		}
		if !ip.IsGlobalUnicast() {
			continue
		}

		if ipMatchesFamily(ip, preferIPv6) {
			if nonLoopback == "" {
				nonLoopback = host
			}
		} else if fallbackNonLoopback == "" {
			fallbackNonLoopback = host
		}
	}

	if nonLoopback == "" {
		nonLoopback = fallbackNonLoopback
	}
	if loopback == "" {
		loopback = fallbackLoopback
	}
	return nonLoopback, loopback
}

func (s *Server) lookupHostCache(key string, now time.Time) (string, bool) {
	ttl := s.hostCacheTTL
	if ttl <= 0 {
		return "", false
	}

	s.hostCacheMu.Lock()
	defer s.hostCacheMu.Unlock()
	entry, ok := s.hostCache[key]
	if !ok {
		return "", false
	}
	if now.After(entry.expiresAt) {
		delete(s.hostCache, key)
		return "", false
	}
	return entry.host, true
}

func (s *Server) storeHostCache(key, host string, now time.Time) {
	ttl := s.hostCacheTTL
	if ttl <= 0 || strings.TrimSpace(host) == "" {
		return
	}

	s.hostCacheMu.Lock()
	defer s.hostCacheMu.Unlock()

	if s.hostCache == nil {
		s.hostCache = make(map[string]routeHostCacheEntry)
	}
	s.hostCache[key] = routeHostCacheEntry{
		host:      host,
		expiresAt: now.Add(ttl),
	}

	if len(s.hostCache) <= maxRouteHostCacheEntries {
		return
	}
	s.pruneExpiredHostCacheLocked(now)
	s.evictHostCacheLocked()
}

func (s *Server) pruneExpiredHostCacheLocked(now time.Time) {
	for cachedKey, cached := range s.hostCache {
		if now.After(cached.expiresAt) {
			delete(s.hostCache, cachedKey)
		}
	}
}

func (s *Server) evictHostCacheLocked() {
	over := len(s.hostCache) - maxRouteHostCacheEntries
	if over <= 0 {
		return
	}

	candidates := make([]routeHostCacheEvictionCandidate, 0, len(s.hostCache))
	for key, entry := range s.hostCache {
		candidates = append(candidates, routeHostCacheEvictionCandidate{
			key:       key,
			expiresAt: entry.expiresAt,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].expiresAt.Equal(candidates[j].expiresAt) {
			return candidates[i].key < candidates[j].key
		}
		return candidates[i].expiresAt.Before(candidates[j].expiresAt)
	})

	for i := 0; i < over && i < len(candidates); i++ {
		delete(s.hostCache, candidates[i].key)
	}
}

func dialLocalAddrForRemote(remoteAddr *net.UDPAddr) (*net.UDPAddr, error) {
	if remoteAddr == nil || remoteAddr.IP == nil {
		return nil, errors.New("remote address is empty")
	}

	dialAddr := &net.UDPAddr{
		IP:   canonicalIP(remoteAddr.IP),
		Port: remoteAddr.Port,
		Zone: remoteAddr.Zone,
	}

	conn, err := net.DialUDP("udp", nil, dialAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	localAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || localAddr.IP == nil {
		return nil, errors.New("local address is not udp")
	}

	return localAddr, nil
}

func localHostFromAddr(addr net.Addr) (host string, isLoopback bool) {
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok || udpAddr == nil {
		return "", false
	}
	ip := canonicalIP(udpAddr.IP)
	if ip == nil || ip.IsUnspecified() {
		return "", false
	}
	return ip.String(), ip.IsLoopback()
}

func routeHostCacheKey(remoteAddr *net.UDPAddr) string {
	if remoteAddr == nil || remoteAddr.IP == nil {
		return "remote:<nil>"
	}
	ip := canonicalIP(remoteAddr.IP)
	if ip == nil {
		return "remote:<nil>"
	}
	if remoteAddr.Zone != "" {
		return ip.String() + "%" + remoteAddr.Zone
	}
	return ip.String()
}

func ipFromAddr(addr net.Addr) net.IP {
	switch typed := addr.(type) {
	case *net.IPAddr:
		return canonicalIP(typed.IP)
	case *net.IPNet:
		return canonicalIP(typed.IP)
	default:
		return nil
	}
}

func ipMatchesFamily(ip net.IP, preferIPv6 bool) bool {
	if preferIPv6 {
		return ip.To4() == nil
	}
	return ip.To4() != nil
}

func canonicalIP(ip net.IP) net.IP {
	if ip == nil {
		return nil
	}
	if v4 := ip.To4(); v4 != nil {
		return v4
	}
	return ip.To16()
}

func parsePortFromAddr(addr string) (int, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return 0, errors.New("address is empty")
	}

	if strings.HasPrefix(addr, ":") {
		port, err := strconv.Atoi(strings.TrimPrefix(addr, ":"))
		if err != nil {
			return 0, fmt.Errorf("parse port from %q: %w", addr, err)
		}
		if port <= 0 || port > 65535 {
			return 0, fmt.Errorf("port out of range in %q", addr)
		}
		return port, nil
	}

	if numeric, err := strconv.Atoi(addr); err == nil {
		if numeric <= 0 || numeric > 65535 {
			return 0, fmt.Errorf("port out of range in %q", addr)
		}
		return numeric, nil
	}

	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, fmt.Errorf("split host/port %q: %w", addr, err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf("parse port %q: %w", portStr, err)
	}
	if port <= 0 || port > 65535 {
		return 0, fmt.Errorf("port out of range in %q", addr)
	}
	return port, nil
}
