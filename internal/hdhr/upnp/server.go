package upnp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arodd/hdhriptv/internal/hdhr"
)

var (
	errIgnoreUnsupportedSearch = errors.New("unsupported SSDP search target")
	errIgnoreNotifyAnnouncement = errors.New("ssdp notify announcement")
)

type localAddrResolver func(remoteAddr *net.UDPAddr) (*net.UDPAddr, error)
type interfaceAddrsProvider func() ([]net.Addr, error)

// Config controls the UPnP/SSDP discovery responder.
type Config struct {
	ListenAddr     string
	DeviceID       string
	FriendlyName   string
	HTTPAddr       string
	LegacyHTTPAddr string
	NotifyInterval time.Duration
	MaxAge         time.Duration
	Logger         *slog.Logger
}

// Server handles SSDP M-SEARCH requests and optional NOTIFY lifecycle.
type Server struct {
	conn *net.UDPConn

	logger         *slog.Logger
	friendlyName   string
	udn            string
	maxAge         time.Duration
	notifyInterval time.Duration
	advertisedPort int

	localAddrResolver localAddrResolver
	interfaceAddrs    interfaceAddrsProvider
	boundHost         string
	boundHostLoopback bool
}

func NewServer(cfg Config) (*Server, error) {
	listenAddr := strings.TrimSpace(cfg.ListenAddr)
	if listenAddr == "" {
		listenAddr = defaultListenAddr
	}

	deviceID := strings.TrimSpace(cfg.DeviceID)
	if deviceID == "" {
		return nil, errors.New("device-id is required")
	}
	udn := hdhr.DeviceUDN(deviceID)

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

	udpAddr, err := net.ResolveUDPAddr("udp4", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve upnp listen address %q: %w", listenAddr, err)
	}
	conn, err := listenUPnP(udpAddr)
	if err != nil {
		return nil, fmt.Errorf("listen upnp udp on %q: %w", listenAddr, err)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	maxAge := cfg.MaxAge
	if maxAge <= 0 {
		maxAge = 30 * time.Minute
	}
	notifyInterval := cfg.NotifyInterval
	if notifyInterval < 0 {
		notifyInterval = 0
	}

	boundHost, boundLoopback := localHostFromAddr(conn.LocalAddr())
	return &Server{
		conn:              conn,
		logger:            logger,
		friendlyName:      strings.TrimSpace(cfg.FriendlyName),
		udn:               udn,
		maxAge:            maxAge,
		notifyInterval:    notifyInterval,
		advertisedPort:    port,
		localAddrResolver: dialLocalAddrForRemote,
		interfaceAddrs:    net.InterfaceAddrs,
		boundHost:         boundHost,
		boundHostLoopback: boundLoopback,
	}, nil
}

func listenUPnP(listenAddr *net.UDPAddr) (*net.UDPConn, error) {
	if listenAddr == nil {
		return nil, errors.New("listen address is required")
	}

	multicastAddr := &net.UDPAddr{
		IP:   net.ParseIP("239.255.255.250"),
		Port: listenAddr.Port,
	}
	conn, err := net.ListenMulticastUDP("udp4", nil, multicastAddr)
	if err == nil {
		return conn, nil
	}

	return net.ListenUDP("udp4", listenAddr)
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
		return errors.New("upnp server is not initialized")
	}

	var (
		notifyStop chan struct{}
		notifyWG   sync.WaitGroup
	)
	if s.notifyInterval > 0 {
		_ = s.sendNotify("ssdp:alive")
		notifyStop = make(chan struct{})
		notifyWG.Add(1)
		go func() {
			defer notifyWG.Done()
			ticker := time.NewTicker(s.notifyInterval)
			defer ticker.Stop()
			for {
				select {
				case <-notifyStop:
					return
				case <-ctx.Done():
					return
				case <-ticker.C:
					_ = s.sendNotify("ssdp:alive")
				}
			}
		}()
	}
	defer func() {
		if notifyStop != nil {
			close(notifyStop)
			notifyWG.Wait()
			_ = s.sendNotify("ssdp:byebye")
		}
	}()

	buf := make([]byte, 4096)
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
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read ssdp datagram: %w", err)
		}

		if err := s.handleDatagram(buf[:n], remoteAddr); err != nil {
			if shouldSuppressIgnoredDatagramLog(err) {
				continue
			}
			s.logger.Debug(
				"ignored ssdp datagram",
				"remote_addr", remoteAddr.String(),
				"error", err,
			)
		}
	}
}

func shouldSuppressIgnoredDatagramLog(err error) bool {
	return errors.Is(err, errIgnoreUnsupportedSearch) || errors.Is(err, errIgnoreNotifyAnnouncement)
}

func (s *Server) handleDatagram(frame []byte, remoteAddr *net.UDPAddr) error {
	request, err := parseMSearchRequest(frame)
	if err != nil {
		return err
	}

	targets := responseTargetsForSearch(request.ST, s.udn)
	if len(targets) == 0 {
		return errIgnoreUnsupportedSearch
	}

	location := s.locationForRemote(remoteAddr)
	now := time.Now()
	for _, target := range targets {
		payload := buildMSearchResponse(ssdpResponse{
			Location: location,
			ST:       target.ST,
			USN:      target.USN,
			MaxAge:   s.maxAge,
		}, now)
		if _, err := s.conn.WriteToUDP([]byte(payload), remoteAddr); err != nil {
			return fmt.Errorf("write ssdp response: %w", err)
		}
	}

	s.logger.Debug(
		"ssdp response sent",
		"remote_addr", remoteAddr.String(),
		"st", request.ST,
		"responses", len(targets),
		"location", location,
		"friendly_name", s.friendlyName,
	)
	return nil
}

func (s *Server) sendNotify(nts string) error {
	if nts != "ssdp:alive" && nts != "ssdp:byebye" {
		return fmt.Errorf("unsupported NTS %q", nts)
	}

	destAddr, err := net.ResolveUDPAddr("udp4", ssdpMulticastAddr)
	if err != nil {
		return fmt.Errorf("resolve ssdp multicast addr: %w", err)
	}

	location := s.locationForNotify()
	now := time.Now()
	targets := responseTargetsForSearch(ssdpSearchTargetAll, s.udn)
	for _, target := range targets {
		payload := buildNotifyMessage(ssdpNotify{
			Location: location,
			NT:       target.ST,
			NTS:      nts,
			USN:      target.USN,
			MaxAge:   s.maxAge,
		}, now)
		if _, err := s.conn.WriteToUDP([]byte(payload), destAddr); err != nil {
			return fmt.Errorf("write ssdp notify: %w", err)
		}
	}

	s.logger.Debug(
		"ssdp notify sent",
		"nts", nts,
		"nt_count", len(targets),
		"location", location,
		"friendly_name", s.friendlyName,
	)
	return nil
}

func (s *Server) locationForRemote(remoteAddr *net.UDPAddr) string {
	host := s.hostForRemote(remoteAddr)
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return fmt.Sprintf("http://%s:%d%s", host, s.advertisedPort, hdhr.UPnPDescriptionPath)
}

func (s *Server) locationForNotify() string {
	host := s.hostForRemote(nil)
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return fmt.Sprintf("http://%s:%d%s", host, s.advertisedPort, hdhr.UPnPDescriptionPath)
}

func (s *Server) hostForRemote(remoteAddr *net.UDPAddr) string {
	if host := strings.TrimSpace(s.boundHost); host != "" {
		return host
	}
	if host := s.resolveRouteHost(remoteAddr); host != "" {
		return host
	}
	if host := s.fallbackHost(remoteAddr); host != "" {
		return host
	}
	return "127.0.0.1"
}

func (s *Server) resolveRouteHost(remoteAddr *net.UDPAddr) string {
	if remoteAddr == nil || remoteAddr.IP == nil {
		return ""
	}

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
	if ip.IsLoopback() && !s.boundHostLoopback {
		if remoteAddr.IP == nil || !remoteAddr.IP.IsLoopback() {
			return ""
		}
	}
	return ip.String()
}

func (s *Server) fallbackHost(remoteAddr *net.UDPAddr) string {
	preferIPv6 := remoteAddr != nil && remoteAddr.IP != nil && remoteAddr.IP.To4() == nil
	nonLoopback, loopback := s.interfaceHosts(preferIPv6)
	if nonLoopback != "" {
		return nonLoopback
	}
	if s.boundHostLoopback && s.boundHost != "" {
		return s.boundHost
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

func dialLocalAddrForRemote(remoteAddr *net.UDPAddr) (*net.UDPAddr, error) {
	if remoteAddr == nil || remoteAddr.IP == nil {
		return nil, errors.New("remote address is empty")
	}

	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{
		IP:   canonicalIP(remoteAddr.IP),
		Port: remoteAddr.Port,
		Zone: remoteAddr.Zone,
	})
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
