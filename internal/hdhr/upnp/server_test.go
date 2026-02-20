package upnp

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/hdhr"
)

func TestServerRespondsToMSearchAll(t *testing.T) {
	srv, err := NewServer(Config{
		ListenAddr: "127.0.0.1:0",
		DeviceID:   "1234ABCD",
		HTTPAddr:   ":5004",
		MaxAge:     30 * time.Minute,
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

	request := strings.Join([]string{
		"M-SEARCH * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		`MAN: "ssdp:discover"`,
		"MX: 1",
		"ST: ssdp:all",
		"",
		"",
	}, "\r\n")

	if _, err := client.Write([]byte(request)); err != nil {
		t.Fatalf("Write(request) error = %v", err)
	}

	buf := make([]byte, 4096)
	seenTargets := make(map[string]bool)
	deadline := time.Now().Add(750 * time.Millisecond)
	for {
		_ = client.SetReadDeadline(deadline)
		n, _, err := client.ReadFromUDP(buf)
		if err != nil {
			netErr, ok := err.(net.Error)
			if ok && netErr.Timeout() {
				break
			}
			t.Fatalf("ReadFromUDP(response) error = %v", err)
		}

		resp := string(buf[:n])
		if !strings.HasPrefix(resp, "HTTP/1.1 200 OK\r\n") {
			t.Fatalf("response prefix = %q, want HTTP/1.1 200 OK", resp)
		}

		headers := parseSSDPHeaders(strings.Split(resp, "\r\n")[1:])
		st := headers["st"]
		if st == "" {
			t.Fatalf("response missing ST header: %q", resp)
		}
		seenTargets[st] = true
		if !strings.Contains(headers["location"], "/upnp/device.xml") {
			t.Fatalf("LOCATION = %q, want /upnp/device.xml", headers["location"])
		}
		if !strings.Contains(headers["location"], ":5004") {
			t.Fatalf("LOCATION = %q, want :5004", headers["location"])
		}
		if !strings.HasPrefix(headers["usn"], "uuid:") {
			t.Fatalf("USN = %q, want uuid-prefixed value", headers["usn"])
		}
		if headers["cache-control"] != "max-age=1800" {
			t.Fatalf("CACHE-CONTROL = %q, want max-age=1800", headers["cache-control"])
		}
	}

	for _, target := range []string{
		hdhr.UPnPSearchTargetRootDevice,
		hdhr.DeviceUDN("1234ABCD"),
		hdhr.UPnPDeviceTypeMediaServer,
		hdhr.UPnPDeviceTypeBasic,
		hdhr.UPnPServiceTypeConnection,
		hdhr.UPnPServiceTypeContentDir,
		hdhr.UPnPDeviceTypeATSCPrimary,
	} {
		if !seenTargets[target] {
			t.Fatalf("missing SSDP response for ST %q (seen=%v)", target, seenTargets)
		}
	}
}

func TestServerIgnoresUnsupportedSearchTarget(t *testing.T) {
	srv, err := NewServer(Config{
		ListenAddr: "127.0.0.1:0",
		DeviceID:   "1234ABCD",
		HTTPAddr:   ":5004",
		MaxAge:     30 * time.Minute,
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

	request := strings.Join([]string{
		"M-SEARCH * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		`MAN: "ssdp:discover"`,
		"MX: 1",
		"ST: urn:schemas-upnp-org:device:InternetGatewayDevice:1",
		"",
		"",
	}, "\r\n")

	if _, err := client.Write([]byte(request)); err != nil {
		t.Fatalf("Write(request) error = %v", err)
	}

	buf := make([]byte, 512)
	_ = client.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, _, err := client.ReadFromUDP(buf); err == nil {
		t.Fatal("expected no response for unsupported ST")
	} else {
		netErr, ok := err.(net.Error)
		if !ok || !netErr.Timeout() {
			t.Fatalf("ReadFromUDP() error = %v, want timeout", err)
		}
	}
}

func TestServerIgnoresNotifyAnnouncements(t *testing.T) {
	srv, err := NewServer(Config{
		ListenAddr: "127.0.0.1:0",
		DeviceID:   "1234ABCD",
		HTTPAddr:   ":5004",
		MaxAge:     30 * time.Minute,
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

	request := strings.Join([]string{
		"NOTIFY * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		"CACHE-CONTROL: max-age=1800",
		"LOCATION: http://192.0.2.10:8000/device.xml",
		"NTS: ssdp:alive",
		"NT: upnp:rootdevice",
		"USN: uuid:12345678-1234-5678-1234-567812345678::upnp:rootdevice",
		"",
		"",
	}, "\r\n")

	if _, err := client.Write([]byte(request)); err != nil {
		t.Fatalf("Write(request) error = %v", err)
	}

	buf := make([]byte, 512)
	_ = client.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, _, err := client.ReadFromUDP(buf); err == nil {
		t.Fatal("expected no response for NOTIFY announcements")
	} else {
		netErr, ok := err.(net.Error)
		if !ok || !netErr.Timeout() {
			t.Fatalf("ReadFromUDP() error = %v, want timeout", err)
		}
	}
}

func TestServerRespondsToContentDirectorySearchTarget(t *testing.T) {
	srv, err := NewServer(Config{
		ListenAddr: "127.0.0.1:0",
		DeviceID:   "1234ABCD",
		HTTPAddr:   ":5004",
		MaxAge:     30 * time.Minute,
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

	request := strings.Join([]string{
		"M-SEARCH * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		`MAN: "ssdp:discover"`,
		"MX: 1",
		"ST: urn:schemas-upnp-org:service:ContentDirectory:1",
		"",
		"",
	}, "\r\n")

	if _, err := client.Write([]byte(request)); err != nil {
		t.Fatalf("Write(request) error = %v", err)
	}

	buf := make([]byte, 1024)
	responseCount := 0
	deadline := time.Now().Add(500 * time.Millisecond)
	var responseHeaders map[string]string
	for {
		_ = client.SetReadDeadline(deadline)
		n, _, err := client.ReadFromUDP(buf)
		if err != nil {
			netErr, ok := err.(net.Error)
			if ok && netErr.Timeout() {
				break
			}
			t.Fatalf("ReadFromUDP(response) error = %v", err)
		}

		responseCount++
		responseHeaders = parseSSDPHeaders(strings.Split(string(buf[:n]), "\r\n")[1:])
	}

	if responseCount != 1 {
		t.Fatalf("response count = %d, want 1", responseCount)
	}
	if responseHeaders["st"] != hdhr.UPnPServiceTypeContentDir {
		t.Fatalf("ST = %q, want %q", responseHeaders["st"], hdhr.UPnPServiceTypeContentDir)
	}

	udn := hdhr.DeviceUDN("1234ABCD")
	wantUSN := udn + "::" + hdhr.UPnPServiceTypeContentDir
	if responseHeaders["usn"] != wantUSN {
		t.Fatalf("USN = %q, want %q", responseHeaders["usn"], wantUSN)
	}
	if !strings.Contains(responseHeaders["location"], "/upnp/device.xml") {
		t.Fatalf("LOCATION = %q, want /upnp/device.xml", responseHeaders["location"])
	}
}

func TestServerRespondsToATSCPrimarySearchTarget(t *testing.T) {
	srv, err := NewServer(Config{
		ListenAddr: "127.0.0.1:0",
		DeviceID:   "1234ABCD",
		HTTPAddr:   ":5004",
		MaxAge:     30 * time.Minute,
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

	request := strings.Join([]string{
		"M-SEARCH * HTTP/1.1",
		"HOST: 239.255.255.250:1900",
		`MAN: "ssdp:discover"`,
		"MX: 1",
		"ST: urn:schemas-atsc.org:device:primaryDevice:1.0",
		"",
		"",
	}, "\r\n")

	if _, err := client.Write([]byte(request)); err != nil {
		t.Fatalf("Write(request) error = %v", err)
	}

	buf := make([]byte, 1024)
	responseCount := 0
	deadline := time.Now().Add(500 * time.Millisecond)
	var responseHeaders map[string]string
	for {
		_ = client.SetReadDeadline(deadline)
		n, _, err := client.ReadFromUDP(buf)
		if err != nil {
			netErr, ok := err.(net.Error)
			if ok && netErr.Timeout() {
				break
			}
			t.Fatalf("ReadFromUDP(response) error = %v", err)
		}

		responseCount++
		responseHeaders = parseSSDPHeaders(strings.Split(string(buf[:n]), "\r\n")[1:])
	}

	if responseCount != 1 {
		t.Fatalf("response count = %d, want 1", responseCount)
	}
	if responseHeaders["st"] != hdhr.UPnPDeviceTypeATSCPrimary {
		t.Fatalf("ST = %q, want %q", responseHeaders["st"], hdhr.UPnPDeviceTypeATSCPrimary)
	}

	udn := hdhr.DeviceUDN("1234ABCD")
	wantUSN := udn + "::" + hdhr.UPnPDeviceTypeATSCPrimary
	if responseHeaders["usn"] != wantUSN {
		t.Fatalf("USN = %q, want %q", responseHeaders["usn"], wantUSN)
	}
}

func TestServerRespondsToHTTP10MSearch(t *testing.T) {
	srv, err := NewServer(Config{
		ListenAddr: "127.0.0.1:0",
		DeviceID:   "1234ABCD",
		HTTPAddr:   ":5004",
		MaxAge:     30 * time.Minute,
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

	request := strings.Join([]string{
		"M-SEARCH * HTTP/1.0",
		"HOST: 239.255.255.250:1900",
		`MAN: "ssdp:discover"`,
		"MX: 1",
		"ST: upnp:rootdevice",
		"",
		"",
	}, "\r\n")

	if _, err := client.Write([]byte(request)); err != nil {
		t.Fatalf("Write(request) error = %v", err)
	}

	buf := make([]byte, 1024)
	_ = client.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, _, err := client.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("ReadFromUDP(response) error = %v", err)
	}
	headers := parseSSDPHeaders(strings.Split(string(buf[:n]), "\r\n")[1:])
	if headers["st"] != hdhr.UPnPSearchTargetRootDevice {
		t.Fatalf("ST = %q, want %q", headers["st"], hdhr.UPnPSearchTargetRootDevice)
	}
}
