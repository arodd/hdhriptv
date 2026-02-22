package hdhr

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

func TestSCPDEndpointsExposeExpectedActions(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{})

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	connectionReq := httptest.NewRequest(http.MethodGet, "http://example.local"+UPnPConnectionSCPDPath, nil)
	connectionRes := httptest.NewRecorder()
	mux.ServeHTTP(connectionRes, connectionReq)
	if connectionRes.Code != http.StatusOK {
		t.Fatalf("connection manager scpd status = %d, want 200", connectionRes.Code)
	}
	if got := connectionRes.Header().Get("Content-Type"); got != "application/xml" {
		t.Fatalf("connection manager scpd content type = %q, want application/xml", got)
	}

	var connectionSCPD scpdDocument
	if err := xml.Unmarshal(connectionRes.Body.Bytes(), &connectionSCPD); err != nil {
		t.Fatalf("decode connection manager scpd xml: %v", err)
	}
	if connectionSCPD.XMLNS != "urn:schemas-upnp-org:service-1-0" {
		t.Fatalf("connection manager scpd xmlns = %q, want urn:schemas-upnp-org:service-1-0", connectionSCPD.XMLNS)
	}
	for _, actionName := range []string{"GetProtocolInfo", "GetCurrentConnectionIDs", "GetCurrentConnectionInfo"} {
		if !scpdHasAction(connectionSCPD, actionName) {
			t.Fatalf("connection manager scpd missing %s action: %s", actionName, connectionRes.Body.String())
		}
	}

	contentReq := httptest.NewRequest(http.MethodGet, "http://example.local"+UPnPContentDirectorySCPDPath, nil)
	contentRes := httptest.NewRecorder()
	mux.ServeHTTP(contentRes, contentReq)
	if contentRes.Code != http.StatusOK {
		t.Fatalf("content directory scpd status = %d, want 200", contentRes.Code)
	}
	if got := contentRes.Header().Get("Content-Type"); got != "application/xml" {
		t.Fatalf("content directory scpd content type = %q, want application/xml", got)
	}

	var contentSCPD scpdDocument
	if err := xml.Unmarshal(contentRes.Body.Bytes(), &contentSCPD); err != nil {
		t.Fatalf("decode content directory scpd xml: %v", err)
	}
	if contentSCPD.XMLNS != "urn:schemas-upnp-org:service-1-0" {
		t.Fatalf("content directory scpd xmlns = %q, want urn:schemas-upnp-org:service-1-0", contentSCPD.XMLNS)
	}
	for _, actionName := range []string{"GetSearchCapabilities", "GetSortCapabilities", "GetSystemUpdateID", "Browse"} {
		if !scpdHasAction(contentSCPD, actionName) {
			t.Fatalf("content directory scpd missing %s action: %s", actionName, contentRes.Body.String())
		}
	}
}

func TestConnectionManagerControlGetProtocolInfo(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{})

	reqBody := `<?xml version="1.0" encoding="utf-8"?>` +
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">` +
		`<s:Body><u:GetProtocolInfo xmlns:u="urn:schemas-upnp-org:service:ConnectionManager:1"></u:GetProtocolInfo></s:Body>` +
		`</s:Envelope>`

	req := httptest.NewRequest(http.MethodPost, "http://example.local"+UPnPConnectionControlPath, strings.NewReader(reqBody))
	req.Header.Set("SOAPACTION", `"urn:schemas-upnp-org:service:ConnectionManager:1#GetProtocolInfo"`)
	res := httptest.NewRecorder()
	h.ConnectionManagerControl(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", res.Code, http.StatusOK)
	}
	if !strings.Contains(res.Body.String(), "<Source>http-get:*:video/mpeg:*</Source>") {
		t.Fatalf("response missing Source protocol info: %s", res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "<Sink></Sink>") {
		t.Fatalf("response missing Sink field: %s", res.Body.String())
	}
}

func TestConnectionManagerControlInvalidActionReturnsSOAPFault(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{})

	reqBody := `<?xml version="1.0" encoding="utf-8"?>` +
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">` +
		`<s:Body><u:UnknownAction xmlns:u="urn:schemas-upnp-org:service:ConnectionManager:1"></u:UnknownAction></s:Body>` +
		`</s:Envelope>`
	req := httptest.NewRequest(http.MethodPost, "http://example.local"+UPnPConnectionControlPath, strings.NewReader(reqBody))
	req.Header.Set("SOAPACTION", `"urn:schemas-upnp-org:service:ConnectionManager:1#UnknownAction"`)
	res := httptest.NewRecorder()
	h.ConnectionManagerControl(res, req)

	if res.Code != http.StatusInternalServerError {
		t.Fatalf("status code = %d, want %d", res.Code, http.StatusInternalServerError)
	}
	if !strings.Contains(res.Body.String(), "<errorCode>401</errorCode>") {
		t.Fatalf("fault body missing invalid-action error code: %s", res.Body.String())
	}
}

func TestContentDirectoryBrowseDirectChildrenSupportsBoundedPaging(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{
		items: []channels.Channel{
			{ChannelID: 11, GuideNumber: "101", GuideName: "Alpha", Enabled: true},
			{ChannelID: 12, GuideNumber: "102", GuideName: "Bravo", Enabled: true},
		},
	})

	reqBody := `<?xml version="1.0" encoding="utf-8"?>` +
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">` +
		`<s:Body><u:Browse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">` +
		`<ObjectID>channels</ObjectID>` +
		`<BrowseFlag>BrowseDirectChildren</BrowseFlag>` +
		`<Filter>*</Filter>` +
		`<StartingIndex>1</StartingIndex>` +
		`<RequestedCount>1</RequestedCount>` +
		`<SortCriteria></SortCriteria>` +
		`</u:Browse></s:Body></s:Envelope>`
	req := httptest.NewRequest(http.MethodPost, "http://example.local"+UPnPContentDirectoryControlPath, strings.NewReader(reqBody))
	req.Host = "example.local:5004"
	req.Header.Set("SOAPACTION", `"urn:schemas-upnp-org:service:ContentDirectory:1#Browse"`)
	res := httptest.NewRecorder()
	h.ContentDirectoryControl(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", res.Code, http.StatusOK)
	}
	if !strings.Contains(res.Body.String(), "<NumberReturned>1</NumberReturned>") {
		t.Fatalf("response missing NumberReturned=1: %s", res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "<TotalMatches>2</TotalMatches>") {
		t.Fatalf("response missing TotalMatches=2: %s", res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "channel:12") {
		t.Fatalf("response missing expected paged object id channel:12: %s", res.Body.String())
	}
	if strings.Contains(res.Body.String(), "channel:11") {
		t.Fatalf("response unexpectedly included channel:11 in paged window: %s", res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "http://example.local:5004/auto/v102") {
		t.Fatalf("response missing expected playback url: %s", res.Body.String())
	}
}

func TestContentDirectoryBrowseUnknownObjectReturnsSOAPFault(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{})

	reqBody := `<?xml version="1.0" encoding="utf-8"?>` +
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">` +
		`<s:Body><u:Browse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">` +
		`<ObjectID>does-not-exist</ObjectID>` +
		`<BrowseFlag>BrowseMetadata</BrowseFlag>` +
		`<Filter>*</Filter>` +
		`<StartingIndex>0</StartingIndex>` +
		`<RequestedCount>1</RequestedCount>` +
		`<SortCriteria></SortCriteria>` +
		`</u:Browse></s:Body></s:Envelope>`
	req := httptest.NewRequest(http.MethodPost, "http://example.local"+UPnPContentDirectoryControlPath, strings.NewReader(reqBody))
	req.Header.Set("SOAPACTION", `"urn:schemas-upnp-org:service:ContentDirectory:1#Browse"`)
	res := httptest.NewRecorder()
	h.ContentDirectoryControl(res, req)

	if res.Code != http.StatusInternalServerError {
		t.Fatalf("status code = %d, want %d", res.Code, http.StatusInternalServerError)
	}
	if !strings.Contains(res.Body.String(), "<errorCode>701</errorCode>") {
		t.Fatalf("fault body missing no-such-object error code: %s", res.Body.String())
	}
}

func TestContentDirectoryControlGetSystemUpdateID(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{})

	reqBody := `<?xml version="1.0" encoding="utf-8"?>` +
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">` +
		`<s:Body><u:GetSystemUpdateID xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1"></u:GetSystemUpdateID></s:Body>` +
		`</s:Envelope>`
	req := httptest.NewRequest(http.MethodPost, "http://example.local"+UPnPContentDirectoryControlPath, strings.NewReader(reqBody))
	req.Header.Set("SOAPACTION", `"urn:schemas-upnp-org:service:ContentDirectory:1#GetSystemUpdateID"`)
	res := httptest.NewRecorder()
	h.ContentDirectoryControl(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", res.Code, http.StatusOK)
	}
	if !strings.Contains(res.Body.String(), "<Id>1</Id>") {
		t.Fatalf("response missing system update id: %s", res.Body.String())
	}
}

func TestContentDirectoryControlGetSystemUpdateIDChangesWhenLineupChanges(t *testing.T) {
	provider := &mutableChannelsProvider{
		items: []channels.Channel{
			{ChannelID: 11, GuideNumber: "101", GuideName: "Alpha", Enabled: true},
		},
	}
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, provider)
	h.contentDirectoryUpdateIDCacheTTL = 0

	firstID := getSystemUpdateIDForTest(t, h)
	if firstID <= 0 {
		t.Fatalf("first system update id = %d, want > 0", firstID)
	}

	provider.items[0].GuideName = "Alpha Renamed"
	secondID := getSystemUpdateIDForTest(t, h)
	if secondID <= 0 {
		t.Fatalf("second system update id = %d, want > 0", secondID)
	}
	if secondID == firstID {
		t.Fatalf("system update id unchanged across lineup rename: first=%d second=%d", firstID, secondID)
	}
}

func TestContentDirectoryControlGetSystemUpdateIDListEnabledErrorReturnsSOAPFault(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, failingChannelsProvider{err: errors.New("list-enabled failed")})

	reqBody := `<?xml version="1.0" encoding="utf-8"?>` +
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">` +
		`<s:Body><u:GetSystemUpdateID xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1"></u:GetSystemUpdateID></s:Body>` +
		`</s:Envelope>`
	req := httptest.NewRequest(http.MethodPost, "http://example.local"+UPnPContentDirectoryControlPath, strings.NewReader(reqBody))
	req.Header.Set("SOAPACTION", `"urn:schemas-upnp-org:service:ContentDirectory:1#GetSystemUpdateID"`)
	res := httptest.NewRecorder()
	h.ContentDirectoryControl(res, req)

	if res.Code != http.StatusInternalServerError {
		t.Fatalf("status code = %d, want %d", res.Code, http.StatusInternalServerError)
	}
	if !strings.Contains(res.Body.String(), "<errorCode>501</errorCode>") {
		t.Fatalf("fault body missing action-failed error code: %s", res.Body.String())
	}
}

func TestCurrentContentDirectoryUpdateIDUsesShortTTLCache(t *testing.T) {
	provider := &countingChannelsProvider{
		items: []channels.Channel{
			{ChannelID: 11, GuideNumber: "101", GuideName: "Alpha", Enabled: true},
		},
	}
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, provider)
	h.contentDirectoryUpdateIDCacheTTL = 5 * time.Millisecond

	firstID, err := h.currentContentDirectoryUpdateID(context.Background(), "http://example.local:5004")
	if err != nil {
		t.Fatalf("currentContentDirectoryUpdateID(first) error = %v", err)
	}
	if calls := provider.callsSnapshot(); calls != 1 {
		t.Fatalf("ListEnabled calls after first lookup = %d, want 1", calls)
	}

	provider.updateGuideName("Alpha Renamed")
	cachedID, err := h.currentContentDirectoryUpdateID(context.Background(), "http://example.local:5004")
	if err != nil {
		t.Fatalf("currentContentDirectoryUpdateID(cached) error = %v", err)
	}
	if calls := provider.callsSnapshot(); calls != 1 {
		t.Fatalf("ListEnabled calls after cached lookup = %d, want 1", calls)
	}
	if cachedID != firstID {
		t.Fatalf("cached update id = %d, want %d while cache TTL active", cachedID, firstID)
	}

	time.Sleep(12 * time.Millisecond)

	refreshedID, err := h.currentContentDirectoryUpdateID(context.Background(), "http://example.local:5004")
	if err != nil {
		t.Fatalf("currentContentDirectoryUpdateID(refreshed) error = %v", err)
	}
	if calls := provider.callsSnapshot(); calls != 2 {
		t.Fatalf("ListEnabled calls after cache expiry lookup = %d, want 2", calls)
	}
	if refreshedID == firstID {
		t.Fatalf("refreshed update id = %d, want changed value after lineup rename", refreshedID)
	}
}

func TestCurrentContentDirectoryUpdateIDCoalescesConcurrentRefreshes(t *testing.T) {
	provider := &blockingChannelsProvider{
		items: []channels.Channel{
			{ChannelID: 11, GuideNumber: "101", GuideName: "Alpha", Enabled: true},
		},
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, provider)
	h.contentDirectoryUpdateIDCacheTTL = 5 * time.Millisecond

	const callers = 12
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(callers)

	updateIDs := make([]int, callers)
	errorsByCall := make([]error, callers)
	for i := 0; i < callers; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			updateID, err := h.currentContentDirectoryUpdateID(context.Background(), "http://example.local:5004")
			updateIDs[idx] = updateID
			errorsByCall[idx] = err
		}(i)
	}

	close(start)

	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first ListEnabled call")
	}

	// While the first refresh is blocked, concurrent callers should wait rather than fan out.
	deadline := time.Now().Add(75 * time.Millisecond)
	for time.Now().Before(deadline) {
		if calls := provider.callsSnapshot(); calls > 1 {
			t.Fatalf("ListEnabled fan-out calls = %d, want 1 while refresh is in-flight", calls)
		}
		time.Sleep(time.Millisecond)
	}

	close(provider.release)
	wg.Wait()

	if calls := provider.callsSnapshot(); calls != 1 {
		t.Fatalf("ListEnabled total calls = %d, want 1 after coalesced refresh", calls)
	}

	firstID := updateIDs[0]
	if firstID <= 0 {
		t.Fatalf("update id = %d, want > 0", firstID)
	}
	for i := range errorsByCall {
		if errorsByCall[i] != nil {
			t.Fatalf("currentContentDirectoryUpdateID call %d error = %v", i, errorsByCall[i])
		}
		if updateIDs[i] != firstID {
			t.Fatalf("update id mismatch at call %d: got %d want %d", i, updateIDs[i], firstID)
		}
	}
}

func TestCurrentContentDirectoryUpdateIDFollowerRetriesAfterLeaderCancellation(t *testing.T) {
	provider := &leaderCanceledThenSuccessProvider{
		items: []channels.Channel{
			{ChannelID: 11, GuideNumber: "101", GuideName: "Alpha", Enabled: true},
		},
		firstCallStarted: make(chan struct{}, 1),
		firstCallRelease: make(chan struct{}),
		secondCallStart:  make(chan struct{}, 1),
	}
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, provider)
	h.contentDirectoryUpdateIDCacheTTL = 0

	type refreshResult struct {
		updateID int
		err      error
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()

	leaderResultCh := make(chan refreshResult, 1)
	go func() {
		updateID, err := h.currentContentDirectoryUpdateID(leaderCtx, "http://example.local:5004")
		leaderResultCh <- refreshResult{updateID: updateID, err: err}
	}()

	select {
	case <-provider.firstCallStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first refresh call to start")
	}

	followerEntered := make(chan struct{})
	followerResultCh := make(chan refreshResult, 1)
	go func() {
		close(followerEntered)
		updateID, err := h.currentContentDirectoryUpdateID(context.Background(), "http://example.local:5004")
		followerResultCh <- refreshResult{updateID: updateID, err: err}
	}()

	<-followerEntered

	waitStart := time.Now()
	for time.Since(waitStart) < 50*time.Millisecond {
		if calls := provider.callsSnapshot(); calls > 1 {
			t.Fatalf("ListEnabled calls before leader cancel = %d, want 1 (follower should be waiting)", calls)
		}
		time.Sleep(time.Millisecond)
	}

	cancelLeader()
	close(provider.firstCallRelease)

	select {
	case <-provider.secondCallStart:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for follower retry refresh")
	}

	select {
	case followerResult := <-followerResultCh:
		if followerResult.err != nil {
			t.Fatalf("follower currentContentDirectoryUpdateID error = %v, want nil after retry", followerResult.err)
		}
		if followerResult.updateID <= 0 {
			t.Fatalf("follower update id = %d, want > 0", followerResult.updateID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for follower result")
	}

	select {
	case leaderResult := <-leaderResultCh:
		if !errors.Is(leaderResult.err, context.Canceled) {
			t.Fatalf("leader error = %v, want context canceled", leaderResult.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for leader result")
	}

	if calls := provider.callsSnapshot(); calls != 2 {
		t.Fatalf("ListEnabled calls = %d, want 2 (leader canceled + follower retry)", calls)
	}
}

func TestCurrentContentDirectoryUpdateIDFollowerCancellationWhileWaiting(t *testing.T) {
	provider := &blockingChannelsProvider{
		items: []channels.Channel{
			{ChannelID: 11, GuideNumber: "101", GuideName: "Alpha", Enabled: true},
		},
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, provider)
	h.contentDirectoryUpdateIDCacheTTL = 0

	type refreshResult struct {
		updateID int
		err      error
	}

	leaderResultCh := make(chan refreshResult, 1)
	go func() {
		updateID, err := h.currentContentDirectoryUpdateID(context.Background(), "http://example.local:5004")
		leaderResultCh <- refreshResult{updateID: updateID, err: err}
	}()

	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for leader refresh call")
	}

	followerCtx, cancelFollower := context.WithCancel(context.Background())
	defer cancelFollower()

	followerEntered := make(chan struct{})
	followerErrCh := make(chan error, 1)
	go func() {
		close(followerEntered)
		_, err := h.currentContentDirectoryUpdateID(followerCtx, "http://example.local:5004")
		followerErrCh <- err
	}()

	<-followerEntered

	waitStart := time.Now()
	for time.Since(waitStart) < 50*time.Millisecond {
		if calls := provider.callsSnapshot(); calls > 1 {
			t.Fatalf("ListEnabled calls before follower cancel = %d, want 1", calls)
		}
		time.Sleep(time.Millisecond)
	}

	cancelFollower()

	select {
	case err := <-followerErrCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("follower error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for follower cancellation result")
	}

	if calls := provider.callsSnapshot(); calls != 1 {
		t.Fatalf("ListEnabled calls after follower cancel = %d, want 1", calls)
	}

	close(provider.release)

	select {
	case leaderResult := <-leaderResultCh:
		if leaderResult.err != nil {
			t.Fatalf("leader error = %v, want nil", leaderResult.err)
		}
		if leaderResult.updateID <= 0 {
			t.Fatalf("leader update id = %d, want > 0", leaderResult.updateID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for leader result")
	}

	if calls := provider.callsSnapshot(); calls != 1 {
		t.Fatalf("ListEnabled total calls = %d, want 1", calls)
	}
}

func TestCurrentContentDirectoryUpdateIDPropagatesLeaderErrorToFollowers(t *testing.T) {
	expectedErr := errors.New("list-enabled failed")
	provider := &blockingChannelsProvider{
		err:     expectedErr,
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, provider)
	h.contentDirectoryUpdateIDCacheTTL = 0

	const callers = 8
	releaseBarrier := make(chan struct{})
	var invokeWG sync.WaitGroup
	invokeWG.Add(callers)
	var wg sync.WaitGroup
	wg.Add(callers)

	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		go func(idx int) {
			defer wg.Done()
			<-releaseBarrier
			invokeWG.Done()
			_, err := h.currentContentDirectoryUpdateID(context.Background(), "http://example.local:5004")
			errs[idx] = err
		}(i)
	}

	close(releaseBarrier)
	invokeWG.Wait()

	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for leader refresh call")
	}

	waitStart := time.Now()
	for time.Since(waitStart) < 50*time.Millisecond {
		if calls := provider.callsSnapshot(); calls > 1 {
			t.Fatalf("ListEnabled fan-out calls before leader release = %d, want 1", calls)
		}
		time.Sleep(time.Millisecond)
	}

	close(provider.release)
	wg.Wait()

	if calls := provider.callsSnapshot(); calls != 1 {
		t.Fatalf("ListEnabled calls = %d, want 1", calls)
	}

	for i, err := range errs {
		if !errors.Is(err, expectedErr) {
			t.Fatalf("call %d error = %v, want %v", i, err, expectedErr)
		}
	}
}

func TestContentDirectoryUpdateIDDeterministicOrderSensitiveAndFallback(t *testing.T) {
	if got := contentDirectoryUpdateID(nil); got != contentDirectoryDefaultUpdateID {
		t.Fatalf("contentDirectoryUpdateID(nil) = %d, want %d", got, contentDirectoryDefaultUpdateID)
	}

	items := []contentDirectoryItem{
		{ObjectID: "channel:11", GuideNumber: "101", GuideName: "Alpha"},
		{ObjectID: "channel:12", GuideNumber: "102", GuideName: "Bravo"},
	}
	first := contentDirectoryUpdateID(items)
	second := contentDirectoryUpdateID(items)
	if first != second {
		t.Fatalf("contentDirectoryUpdateID() deterministic mismatch: first=%d second=%d", first, second)
	}

	reordered := []contentDirectoryItem{
		{ObjectID: "channel:12", GuideNumber: "102", GuideName: "Bravo"},
		{ObjectID: "channel:11", GuideNumber: "101", GuideName: "Alpha"},
	}
	reorderedID := contentDirectoryUpdateID(reordered)
	if reorderedID == first {
		t.Fatalf("contentDirectoryUpdateID() order-insensitive hash: original=%d reordered=%d", first, reorderedID)
	}

	if got := contentDirectoryUpdateIDFromHashSum(0); got != contentDirectoryDefaultUpdateID {
		t.Fatalf("contentDirectoryUpdateIDFromHashSum(0) = %d, want %d", got, contentDirectoryDefaultUpdateID)
	}
	if got := contentDirectoryUpdateIDFromHashSum(0x80000000); got != contentDirectoryDefaultUpdateID {
		t.Fatalf("contentDirectoryUpdateIDFromHashSum(0x80000000) = %d, want %d", got, contentDirectoryDefaultUpdateID)
	}
}

func TestContentDirectoryBrowseMetadataUsesStableChannelObjectID(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{
		items: []channels.Channel{
			{ChannelID: 77, GuideNumber: "777", GuideName: "Sample", Enabled: true},
		},
	})

	reqBody := `<?xml version="1.0" encoding="utf-8"?>` +
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">` +
		`<s:Body><u:Browse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">` +
		`<ObjectID>channel:77</ObjectID>` +
		`<BrowseFlag>BrowseMetadata</BrowseFlag>` +
		`<Filter>*</Filter>` +
		`<StartingIndex>0</StartingIndex>` +
		`<RequestedCount>1</RequestedCount>` +
		`<SortCriteria></SortCriteria>` +
		`</u:Browse></s:Body></s:Envelope>`
	req := httptest.NewRequest(http.MethodPost, "http://example.local"+UPnPContentDirectoryControlPath, strings.NewReader(reqBody))
	req.Host = "example.local:5004"
	req.Header.Set("SOAPACTION", `"urn:schemas-upnp-org:service:ContentDirectory:1#Browse"`)
	res := httptest.NewRecorder()
	h.ContentDirectoryControl(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", res.Code, http.StatusOK)
	}
	if !strings.Contains(res.Body.String(), "<NumberReturned>1</NumberReturned>") {
		t.Fatalf("response missing NumberReturned=1: %s", res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "<TotalMatches>1</TotalMatches>") {
		t.Fatalf("response missing TotalMatches=1: %s", res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "channel:77") {
		t.Fatalf("response missing expected channel object id: %s", res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "http://example.local:5004/auto/v777") {
		t.Fatalf("response missing expected playback url: %s", res.Body.String())
	}
}

func TestContentDirectoryBrowseInvalidBrowseFlagReturnsSOAPFault(t *testing.T) {
	h := NewHandler(Config{
		FriendlyName: "HDHR IPTV",
		DeviceID:     "1234ABCD",
		DeviceAuth:   "token",
		TunerCount:   2,
	}, fakeChannelsProvider{})

	reqBody := `<?xml version="1.0" encoding="utf-8"?>` +
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">` +
		`<s:Body><u:Browse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1">` +
		`<ObjectID>0</ObjectID>` +
		`<BrowseFlag>InvalidBrowseFlag</BrowseFlag>` +
		`<Filter>*</Filter>` +
		`<StartingIndex>0</StartingIndex>` +
		`<RequestedCount>1</RequestedCount>` +
		`<SortCriteria></SortCriteria>` +
		`</u:Browse></s:Body></s:Envelope>`
	req := httptest.NewRequest(http.MethodPost, "http://example.local"+UPnPContentDirectoryControlPath, strings.NewReader(reqBody))
	req.Header.Set("SOAPACTION", `"urn:schemas-upnp-org:service:ContentDirectory:1#Browse"`)
	res := httptest.NewRecorder()
	h.ContentDirectoryControl(res, req)

	if res.Code != http.StatusInternalServerError {
		t.Fatalf("status code = %d, want %d", res.Code, http.StatusInternalServerError)
	}
	if !strings.Contains(res.Body.String(), "<errorCode>402</errorCode>") {
		t.Fatalf("fault body missing invalid-args error code: %s", res.Body.String())
	}
}

type mutableChannelsProvider struct {
	mu    sync.Mutex
	items []channels.Channel
}

func (m *mutableChannelsProvider) ListEnabled(context.Context) ([]channels.Channel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]channels.Channel, len(m.items))
	copy(out, m.items)
	return out, nil
}

type failingChannelsProvider struct {
	err error
}

func (p failingChannelsProvider) ListEnabled(context.Context) ([]channels.Channel, error) {
	if p.err == nil {
		return nil, errors.New("channels provider failed")
	}
	return nil, p.err
}

type countingChannelsProvider struct {
	mu    sync.Mutex
	items []channels.Channel
	calls int
}

func (p *countingChannelsProvider) ListEnabled(context.Context) ([]channels.Channel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.calls++
	out := make([]channels.Channel, len(p.items))
	copy(out, p.items)
	return out, nil
}

func (p *countingChannelsProvider) callsSnapshot() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *countingChannelsProvider) updateGuideName(guideName string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.items) == 0 {
		return
	}
	p.items[0].GuideName = guideName
}

type blockingChannelsProvider struct {
	mu      sync.Mutex
	items   []channels.Channel
	err     error
	calls   int
	started chan struct{}
	release chan struct{}
}

func (p *blockingChannelsProvider) ListEnabled(context.Context) ([]channels.Channel, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()

	select {
	case p.started <- struct{}{}:
	default:
	}

	<-p.release

	if p.err != nil {
		return nil, p.err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]channels.Channel, len(p.items))
	copy(out, p.items)
	return out, nil
}

func (p *blockingChannelsProvider) callsSnapshot() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

type leaderCanceledThenSuccessProvider struct {
	mu sync.Mutex

	items []channels.Channel
	calls int

	firstCallStarted chan struct{}
	firstCallRelease chan struct{}
	secondCallStart  chan struct{}
}

func (p *leaderCanceledThenSuccessProvider) ListEnabled(ctx context.Context) ([]channels.Channel, error) {
	p.mu.Lock()
	p.calls++
	callNumber := p.calls
	p.mu.Unlock()

	if callNumber == 1 {
		select {
		case p.firstCallStarted <- struct{}{}:
		default:
		}
		<-p.firstCallRelease
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if callNumber == 2 {
		select {
		case p.secondCallStart <- struct{}{}:
		default:
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]channels.Channel, len(p.items))
	copy(out, p.items)
	return out, nil
}

func (p *leaderCanceledThenSuccessProvider) callsSnapshot() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func getSystemUpdateIDForTest(t testing.TB, h *Handler) int {
	t.Helper()

	reqBody := `<?xml version="1.0" encoding="utf-8"?>` +
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">` +
		`<s:Body><u:GetSystemUpdateID xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1"></u:GetSystemUpdateID></s:Body>` +
		`</s:Envelope>`
	req := httptest.NewRequest(http.MethodPost, "http://example.local"+UPnPContentDirectoryControlPath, strings.NewReader(reqBody))
	req.Header.Set("SOAPACTION", `"urn:schemas-upnp-org:service:ContentDirectory:1#GetSystemUpdateID"`)
	res := httptest.NewRecorder()
	h.ContentDirectoryControl(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", res.Code, http.StatusOK)
	}
	body := res.Body.String()
	start := strings.Index(body, "<Id>")
	end := strings.Index(body, "</Id>")
	if start < 0 || end <= start+len("<Id>") {
		t.Fatalf("response missing Id element: %s", body)
	}
	idValue := body[start+len("<Id>") : end]
	id, err := strconv.Atoi(strings.TrimSpace(idValue))
	if err != nil {
		t.Fatalf("parse Id=%q: %v", idValue, err)
	}
	return id
}

func scpdHasAction(doc scpdDocument, actionName string) bool {
	for _, action := range doc.Actions {
		if action.Name == actionName {
			return true
		}
	}
	return false
}
