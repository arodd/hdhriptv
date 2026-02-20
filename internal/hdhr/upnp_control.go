package hdhr

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/arodd/hdhriptv/internal/channels"
)

const (
	maxSOAPRequestBytes                     = 1 << 20
	contentDirectoryMaxBrowseCount          = 200
	contentDirectoryRootID                  = "0"
	contentDirectoryChannelsID              = "channels"
	contentDirectoryDefaultUpdateID         = 1
	defaultContentDirectoryUpdateIDCacheTTL = time.Second
)

var (
	connectionManagerProtocolInfo = "http-get:*:video/mpeg:*"
)

type soapControlRequest struct {
	ServiceType string
	ActionName  string
	Args        map[string]string
}

type soapEnvelopeRequest struct {
	Body soapBodyRequest `xml:"Body"`
}

type soapBodyRequest struct {
	Action soapActionRequest `xml:",any"`
}

type soapActionRequest struct {
	XMLName xml.Name
	Args    []soapActionArg `xml:",any"`
}

type soapActionArg struct {
	XMLName xml.Name
	Value   string `xml:",chardata"`
}

type soapResponseArg struct {
	Name  string
	Value string
}

type contentDirectoryBrowseResponse struct {
	Result         string
	NumberReturned int
	TotalMatches   int
	UpdateID       int
}

type scpdDocument struct {
	XMLName           xml.Name            `xml:"scpd"`
	XMLNS             string              `xml:"xmlns,attr"`
	Spec              UPnPSpecVersion     `xml:"specVersion"`
	Actions           []scpdAction        `xml:"actionList>action"`
	ServiceStateTable []scpdStateVariable `xml:"serviceStateTable>stateVariable"`
}

type scpdAction struct {
	Name      string         `xml:"name"`
	Arguments []scpdArgument `xml:"argumentList>argument,omitempty"`
}

type scpdArgument struct {
	Name                 string `xml:"name"`
	Direction            string `xml:"direction"`
	RelatedStateVariable string `xml:"relatedStateVariable"`
}

type scpdStateVariable struct {
	SendEvents    string   `xml:"sendEvents,attr"`
	Name          string   `xml:"name"`
	DataType      string   `xml:"dataType"`
	DefaultValue  string   `xml:"defaultValue,omitempty"`
	AllowedValues []string `xml:"allowedValueList>allowedValue,omitempty"`
}

type didlLite struct {
	XMLName    xml.Name        `xml:"DIDL-Lite"`
	XMLNS      string          `xml:"xmlns,attr"`
	XMLNSDC    string          `xml:"xmlns:dc,attr"`
	XMLNSUPnP  string          `xml:"xmlns:upnp,attr"`
	Containers []didlContainer `xml:"container,omitempty"`
	Items      []didlItem      `xml:"item,omitempty"`
}

type didlContainer struct {
	ID         string `xml:"id,attr"`
	ParentID   string `xml:"parentID,attr"`
	Restricted int    `xml:"restricted,attr"`
	ChildCount int    `xml:"childCount,attr,omitempty"`
	Title      string `xml:"dc:title"`
	Class      string `xml:"upnp:class"`
}

type didlItem struct {
	ID           string       `xml:"id,attr"`
	ParentID     string       `xml:"parentID,attr"`
	Restricted   int          `xml:"restricted,attr"`
	Title        string       `xml:"dc:title"`
	Class        string       `xml:"upnp:class"`
	ChannelNr    string       `xml:"upnp:channelNr,omitempty"`
	ResourceInfo didlResource `xml:"res"`
}

type didlResource struct {
	ProtocolInfo string `xml:"protocolInfo,attr"`
	URL          string `xml:",chardata"`
}

type contentDirectoryItem struct {
	ObjectID    string
	GuideNumber string
	GuideName   string
	PlaybackURL string
}

func (h *Handler) ConnectionManagerSCPDXML(w http.ResponseWriter, _ *http.Request) {
	writeXMLPayload(w, connectionManagerSCPD())
}

func (h *Handler) ContentDirectorySCPDXML(w http.ResponseWriter, _ *http.Request) {
	writeXMLPayload(w, contentDirectorySCPD())
}

func (h *Handler) ConnectionManagerControl(w http.ResponseWriter, r *http.Request) {
	request, err := parseSOAPControlRequest(r)
	if err != nil {
		writeSOAPFault(w, 402, "Invalid Args")
		return
	}
	if request.ServiceType != "" && !strings.EqualFold(request.ServiceType, UPnPServiceTypeConnection) {
		writeSOAPFault(w, 401, "Invalid Action")
		return
	}

	switch request.ActionName {
	case "GetProtocolInfo":
		writeSOAPActionResponse(w, UPnPServiceTypeConnection, "GetProtocolInfo", []soapResponseArg{
			{Name: "Source", Value: connectionManagerProtocolInfo},
			{Name: "Sink", Value: ""},
		})
	case "GetCurrentConnectionIDs":
		writeSOAPActionResponse(w, UPnPServiceTypeConnection, "GetCurrentConnectionIDs", []soapResponseArg{
			{Name: "ConnectionIDs", Value: "0"},
		})
	case "GetCurrentConnectionInfo":
		connectionID := strings.TrimSpace(request.Args["ConnectionID"])
		if connectionID == "" {
			connectionID = "0"
		}
		if connectionID != "0" {
			writeSOAPFault(w, 706, "No Such Connection")
			return
		}
		writeSOAPActionResponse(w, UPnPServiceTypeConnection, "GetCurrentConnectionInfo", []soapResponseArg{
			{Name: "RcsID", Value: "-1"},
			{Name: "AVTransportID", Value: "-1"},
			{Name: "ProtocolInfo", Value: connectionManagerProtocolInfo},
			{Name: "PeerConnectionManager", Value: ""},
			{Name: "PeerConnectionID", Value: "-1"},
			{Name: "Direction", Value: "Output"},
			{Name: "Status", Value: "OK"},
		})
	default:
		writeSOAPFault(w, 401, "Invalid Action")
	}
}

func (h *Handler) ContentDirectoryControl(w http.ResponseWriter, r *http.Request) {
	request, err := parseSOAPControlRequest(r)
	if err != nil {
		writeSOAPFault(w, 402, "Invalid Args")
		return
	}
	if request.ServiceType != "" && !strings.EqualFold(request.ServiceType, UPnPServiceTypeContentDir) {
		writeSOAPFault(w, 401, "Invalid Action")
		return
	}

	switch request.ActionName {
	case "GetSearchCapabilities":
		writeSOAPActionResponse(w, UPnPServiceTypeContentDir, "GetSearchCapabilities", []soapResponseArg{
			{Name: "SearchCaps", Value: "dc:title,upnp:class"},
		})
	case "GetSortCapabilities":
		writeSOAPActionResponse(w, UPnPServiceTypeContentDir, "GetSortCapabilities", []soapResponseArg{
			{Name: "SortCaps", Value: "dc:title"},
		})
	case "GetSystemUpdateID":
		updateID, updateErr := h.currentContentDirectoryUpdateID(r.Context(), BaseURL(r))
		if updateErr != nil {
			writeSOAPFault(w, 501, "Action Failed")
			return
		}
		writeSOAPActionResponse(w, UPnPServiceTypeContentDir, "GetSystemUpdateID", []soapResponseArg{
			{Name: "Id", Value: strconv.Itoa(updateID)},
		})
	case "Browse":
		response, faultCode, faultDescription, browseErr := h.browseContentDirectory(r.Context(), BaseURL(r), request.Args)
		if browseErr != nil {
			writeSOAPFault(w, 501, "Action Failed")
			return
		}
		if faultCode != 0 {
			writeSOAPFault(w, faultCode, faultDescription)
			return
		}
		writeSOAPActionResponse(w, UPnPServiceTypeContentDir, "Browse", []soapResponseArg{
			{Name: "Result", Value: response.Result},
			{Name: "NumberReturned", Value: strconv.Itoa(response.NumberReturned)},
			{Name: "TotalMatches", Value: strconv.Itoa(response.TotalMatches)},
			{Name: "UpdateID", Value: strconv.Itoa(response.UpdateID)},
		})
	default:
		writeSOAPFault(w, 401, "Invalid Action")
	}
}

func parseSOAPControlRequest(r *http.Request) (soapControlRequest, error) {
	if r == nil {
		return soapControlRequest{}, fmt.Errorf("request is required")
	}
	if r.Body != nil {
		defer r.Body.Close()
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, maxSOAPRequestBytes))
	if err != nil {
		return soapControlRequest{}, fmt.Errorf("read SOAP body: %w", err)
	}

	var envelope soapEnvelopeRequest
	if err := xml.Unmarshal(bodyBytes, &envelope); err != nil {
		return soapControlRequest{}, fmt.Errorf("decode SOAP body: %w", err)
	}

	serviceType := strings.TrimSpace(envelope.Body.Action.XMLName.Space)
	actionName := strings.TrimSpace(envelope.Body.Action.XMLName.Local)
	headerServiceType, headerAction := parseSOAPActionHeader(r.Header.Get("SOAPACTION"))

	if headerAction != "" {
		if actionName == "" {
			actionName = headerAction
		} else if !strings.EqualFold(actionName, headerAction) {
			return soapControlRequest{}, fmt.Errorf("SOAPAction action mismatch")
		}
	}
	if headerServiceType != "" {
		if serviceType == "" {
			serviceType = headerServiceType
		} else if !strings.EqualFold(serviceType, headerServiceType) {
			return soapControlRequest{}, fmt.Errorf("SOAPAction service mismatch")
		}
	}
	if actionName == "" {
		return soapControlRequest{}, fmt.Errorf("missing SOAP action")
	}

	args := make(map[string]string, len(envelope.Body.Action.Args))
	for _, arg := range envelope.Body.Action.Args {
		name := strings.TrimSpace(arg.XMLName.Local)
		if name == "" {
			continue
		}
		args[name] = strings.TrimSpace(arg.Value)
	}

	return soapControlRequest{
		ServiceType: serviceType,
		ActionName:  actionName,
		Args:        args,
	}, nil
}

func parseSOAPActionHeader(raw string) (serviceType string, actionName string) {
	value := strings.TrimSpace(raw)
	value = strings.Trim(value, "\"'")
	if value == "" {
		return "", ""
	}

	prefix, suffix, ok := strings.Cut(value, "#")
	if !ok {
		return "", strings.TrimSpace(value)
	}
	return strings.TrimSpace(prefix), strings.TrimSpace(suffix)
}

func writeSOAPActionResponse(w http.ResponseWriter, serviceType string, action string, args []soapResponseArg) {
	var builder strings.Builder
	builder.WriteString(xml.Header)
	builder.WriteString(`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">`)
	builder.WriteString(`<s:Body>`)
	builder.WriteString(`<u:`)
	builder.WriteString(action)
	builder.WriteString(`Response xmlns:u="`)
	builder.WriteString(xmlEscape(serviceType))
	builder.WriteString(`">`)
	for _, arg := range args {
		builder.WriteString(`<`)
		builder.WriteString(arg.Name)
		builder.WriteString(`>`)
		builder.WriteString(xmlEscape(arg.Value))
		builder.WriteString(`</`)
		builder.WriteString(arg.Name)
		builder.WriteString(`>`)
	}
	builder.WriteString(`</u:`)
	builder.WriteString(action)
	builder.WriteString(`Response>`)
	builder.WriteString(`</s:Body></s:Envelope>`)

	w.Header().Set("Content-Type", `text/xml; charset="utf-8"`)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(builder.String()))
}

func writeSOAPFault(w http.ResponseWriter, code int, description string) {
	if strings.TrimSpace(description) == "" {
		description = "Action Failed"
	}

	var builder strings.Builder
	builder.WriteString(xml.Header)
	builder.WriteString(`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">`)
	builder.WriteString(`<s:Body><s:Fault>`)
	builder.WriteString(`<faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring>`)
	builder.WriteString(`<detail><UPnPError xmlns="urn:schemas-upnp-org:control-1-0">`)
	builder.WriteString(`<errorCode>`)
	builder.WriteString(strconv.Itoa(code))
	builder.WriteString(`</errorCode><errorDescription>`)
	builder.WriteString(xmlEscape(description))
	builder.WriteString(`</errorDescription></UPnPError></detail>`)
	builder.WriteString(`</s:Fault></s:Body></s:Envelope>`)

	w.Header().Set("Content-Type", `text/xml; charset="utf-8"`)
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write([]byte(builder.String()))
}

func (h *Handler) browseContentDirectory(
	ctx context.Context,
	baseURL string,
	args map[string]string,
) (contentDirectoryBrowseResponse, int, string, error) {
	objectID := strings.TrimSpace(args["ObjectID"])
	if objectID == "" {
		objectID = contentDirectoryRootID
	}
	browseFlag := strings.TrimSpace(args["BrowseFlag"])
	if browseFlag != "BrowseMetadata" && browseFlag != "BrowseDirectChildren" {
		return contentDirectoryBrowseResponse{}, 402, "Invalid Args", nil
	}

	startingIndex, err := parseNonNegativeArg(args["StartingIndex"])
	if err != nil {
		return contentDirectoryBrowseResponse{}, 402, "Invalid Args", nil
	}
	requestedCount, err := parseNonNegativeArg(args["RequestedCount"])
	if err != nil {
		return contentDirectoryBrowseResponse{}, 402, "Invalid Args", nil
	}
	if requestedCount == 0 || requestedCount > contentDirectoryMaxBrowseCount {
		requestedCount = contentDirectoryMaxBrowseCount
	}

	items, err := h.contentDirectoryItems(ctx, baseURL)
	if err != nil {
		return contentDirectoryBrowseResponse{}, 0, "", err
	}
	updateID := contentDirectoryUpdateID(items)

	switch objectID {
	case contentDirectoryRootID:
		if browseFlag == "BrowseMetadata" {
			result, buildErr := buildDIDL([]didlContainer{{
				ID:         contentDirectoryRootID,
				ParentID:   "-1",
				Restricted: 1,
				ChildCount: 1,
				Title:      h.friendlyName,
				Class:      "object.container",
			}}, nil)
			if buildErr != nil {
				return contentDirectoryBrowseResponse{}, 0, "", buildErr
			}
			return contentDirectoryBrowseResponse{
				Result:         result,
				NumberReturned: 1,
				TotalMatches:   1,
				UpdateID:       updateID,
			}, 0, "", nil
		}

		result, buildErr := buildDIDL([]didlContainer{{
			ID:         contentDirectoryChannelsID,
			ParentID:   contentDirectoryRootID,
			Restricted: 1,
			ChildCount: len(items),
			Title:      "Channels",
			Class:      "object.container",
		}}, nil)
		if buildErr != nil {
			return contentDirectoryBrowseResponse{}, 0, "", buildErr
		}
		return contentDirectoryBrowseResponse{
			Result:         result,
			NumberReturned: 1,
			TotalMatches:   1,
			UpdateID:       updateID,
		}, 0, "", nil

	case contentDirectoryChannelsID:
		if browseFlag == "BrowseMetadata" {
			result, buildErr := buildDIDL([]didlContainer{{
				ID:         contentDirectoryChannelsID,
				ParentID:   contentDirectoryRootID,
				Restricted: 1,
				ChildCount: len(items),
				Title:      "Channels",
				Class:      "object.container",
			}}, nil)
			if buildErr != nil {
				return contentDirectoryBrowseResponse{}, 0, "", buildErr
			}
			return contentDirectoryBrowseResponse{
				Result:         result,
				NumberReturned: 1,
				TotalMatches:   1,
				UpdateID:       updateID,
			}, 0, "", nil
		}

		total := len(items)
		if startingIndex > total {
			startingIndex = total
		}
		end := startingIndex + requestedCount
		if end > total {
			end = total
		}
		pagedItems := items[startingIndex:end]

		didlItems := make([]didlItem, 0, len(pagedItems))
		for _, item := range pagedItems {
			didlItems = append(didlItems, didlItem{
				ID:         item.ObjectID,
				ParentID:   contentDirectoryChannelsID,
				Restricted: 1,
				Title:      item.GuideName,
				Class:      "object.item.videoItem",
				ChannelNr:  item.GuideNumber,
				ResourceInfo: didlResource{
					ProtocolInfo: connectionManagerProtocolInfo,
					URL:          item.PlaybackURL,
				},
			})
		}
		result, buildErr := buildDIDL(nil, didlItems)
		if buildErr != nil {
			return contentDirectoryBrowseResponse{}, 0, "", buildErr
		}
		return contentDirectoryBrowseResponse{
			Result:         result,
			NumberReturned: len(didlItems),
			TotalMatches:   total,
			UpdateID:       updateID,
		}, 0, "", nil
	}

	channelObjectID, ok := parseContentDirectoryChannelObjectID(objectID)
	if !ok {
		return contentDirectoryBrowseResponse{}, 701, "No Such Object", nil
	}
	if browseFlag == "BrowseDirectChildren" {
		return contentDirectoryBrowseResponse{}, 710, "No Such Container", nil
	}

	for _, item := range items {
		if item.ObjectID != channelObjectID {
			continue
		}
		result, buildErr := buildDIDL(nil, []didlItem{{
			ID:         item.ObjectID,
			ParentID:   contentDirectoryChannelsID,
			Restricted: 1,
			Title:      item.GuideName,
			Class:      "object.item.videoItem",
			ChannelNr:  item.GuideNumber,
			ResourceInfo: didlResource{
				ProtocolInfo: connectionManagerProtocolInfo,
				URL:          item.PlaybackURL,
			},
		}})
		if buildErr != nil {
			return contentDirectoryBrowseResponse{}, 0, "", buildErr
		}
		return contentDirectoryBrowseResponse{
			Result:         result,
			NumberReturned: 1,
			TotalMatches:   1,
			UpdateID:       updateID,
		}, 0, "", nil
	}

	return contentDirectoryBrowseResponse{}, 701, "No Such Object", nil
}

func (h *Handler) contentDirectoryItems(ctx context.Context, baseURL string) ([]contentDirectoryItem, error) {
	if h == nil || h.channelsProvider == nil {
		return nil, fmt.Errorf("channels provider is not configured")
	}

	lineupChannels, err := h.channelsProvider.ListEnabled(ctx)
	if err != nil {
		return nil, err
	}
	return toContentDirectoryItems(baseURL, lineupChannels), nil
}

func (h *Handler) currentContentDirectoryUpdateID(ctx context.Context, baseURL string) (int, error) {
	if h == nil {
		return 0, fmt.Errorf("channels provider is not configured")
	}
	if cachedUpdateID, ok := h.cachedContentDirectoryUpdateID(); ok {
		return cachedUpdateID, nil
	}

	h.contentDirectoryUpdateIDRefreshMu.Lock()
	wait := h.contentDirectoryUpdateIDRefreshWait
	if wait == nil {
		wait = make(chan struct{})
		h.contentDirectoryUpdateIDRefreshWait = wait
		h.contentDirectoryUpdateIDRefreshMu.Unlock()

		updateID, err := h.refreshContentDirectoryUpdateID(ctx, baseURL)

		h.contentDirectoryUpdateIDRefreshMu.Lock()
		h.contentDirectoryUpdateIDRefreshValue = updateID
		h.contentDirectoryUpdateIDRefreshErr = err
		close(wait)
		h.contentDirectoryUpdateIDRefreshWait = nil
		h.contentDirectoryUpdateIDRefreshMu.Unlock()
		return updateID, err
	}
	h.contentDirectoryUpdateIDRefreshMu.Unlock()

	select {
	case <-wait:
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	h.contentDirectoryUpdateIDRefreshMu.Lock()
	updateID := h.contentDirectoryUpdateIDRefreshValue
	err := h.contentDirectoryUpdateIDRefreshErr
	h.contentDirectoryUpdateIDRefreshMu.Unlock()
	return updateID, err
}

func (h *Handler) refreshContentDirectoryUpdateID(ctx context.Context, baseURL string) (int, error) {
	items, err := h.contentDirectoryItems(ctx, baseURL)
	if err != nil {
		return 0, err
	}
	updateID := contentDirectoryUpdateID(items)
	h.storeContentDirectoryUpdateID(updateID)
	return updateID, nil
}

func (h *Handler) cachedContentDirectoryUpdateID() (int, bool) {
	if h == nil {
		return 0, false
	}
	if h.contentDirectoryUpdateIDCacheTTL <= 0 {
		return 0, false
	}

	now := time.Now().UTC()
	h.contentDirectoryUpdateIDCacheMu.Lock()
	defer h.contentDirectoryUpdateIDCacheMu.Unlock()

	if h.contentDirectoryUpdateIDCacheAt.IsZero() {
		return 0, false
	}
	if now.Sub(h.contentDirectoryUpdateIDCacheAt) > h.contentDirectoryUpdateIDCacheTTL {
		return 0, false
	}
	return h.contentDirectoryUpdateIDCacheValue, true
}

func (h *Handler) storeContentDirectoryUpdateID(updateID int) {
	if h == nil {
		return
	}
	if h.contentDirectoryUpdateIDCacheTTL <= 0 {
		return
	}

	h.contentDirectoryUpdateIDCacheMu.Lock()
	h.contentDirectoryUpdateIDCacheValue = updateID
	h.contentDirectoryUpdateIDCacheAt = time.Now().UTC()
	h.contentDirectoryUpdateIDCacheMu.Unlock()
}

func contentDirectoryUpdateID(items []contentDirectoryItem) int {
	if len(items) == 0 {
		return contentDirectoryDefaultUpdateID
	}

	hash := fnv.New32a()
	_, _ = hash.Write([]byte("content-directory:v1\n"))
	for _, item := range items {
		_, _ = hash.Write([]byte(item.ObjectID))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(item.GuideNumber))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(item.GuideName))
		_, _ = hash.Write([]byte{'\n'})
	}

	return contentDirectoryUpdateIDFromHashSum(hash.Sum32())
}

func contentDirectoryUpdateIDFromHashSum(hashSum uint32) int {
	updateID := int(hashSum & 0x7fffffff)
	if updateID <= 0 {
		return contentDirectoryDefaultUpdateID
	}
	return updateID
}

func parseNonNegativeArg(raw string) (int, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	if parsed < 0 {
		return 0, fmt.Errorf("value must be non-negative")
	}
	return parsed, nil
}

func contentDirectoryChannelObjectID(channelID int64) string {
	return fmt.Sprintf("channel:%d", channelID)
}

func parseContentDirectoryChannelObjectID(objectID string) (string, bool) {
	value := strings.TrimSpace(objectID)
	if !strings.HasPrefix(value, "channel:") {
		return "", false
	}
	idValue := strings.TrimPrefix(value, "channel:")
	if strings.TrimSpace(idValue) == "" {
		return "", false
	}
	if _, err := strconv.ParseInt(idValue, 10, 64); err != nil {
		return "", false
	}
	return value, true
}

func buildDIDL(containers []didlContainer, items []didlItem) (string, error) {
	payload := didlLite{
		XMLNS:      "urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/",
		XMLNSDC:    "http://purl.org/dc/elements/1.1/",
		XMLNSUPnP:  "urn:schemas-upnp-org:metadata-1-0/upnp/",
		Containers: containers,
		Items:      items,
	}

	var buf bytes.Buffer
	enc := xml.NewEncoder(&buf)
	if err := enc.Encode(payload); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func connectionManagerSCPD() scpdDocument {
	return scpdDocument{
		XMLNS: "urn:schemas-upnp-org:service-1-0",
		Spec:  UPnPSpecVersion{Major: 1, Minor: 0},
		Actions: []scpdAction{
			{
				Name: "GetProtocolInfo",
				Arguments: []scpdArgument{
					{Name: "Source", Direction: "out", RelatedStateVariable: "SourceProtocolInfo"},
					{Name: "Sink", Direction: "out", RelatedStateVariable: "SinkProtocolInfo"},
				},
			},
			{
				Name: "GetCurrentConnectionIDs",
				Arguments: []scpdArgument{
					{Name: "ConnectionIDs", Direction: "out", RelatedStateVariable: "CurrentConnectionIDs"},
				},
			},
			{
				Name: "GetCurrentConnectionInfo",
				Arguments: []scpdArgument{
					{Name: "ConnectionID", Direction: "in", RelatedStateVariable: "A_ARG_TYPE_ConnectionID"},
					{Name: "RcsID", Direction: "out", RelatedStateVariable: "A_ARG_TYPE_RcsID"},
					{Name: "AVTransportID", Direction: "out", RelatedStateVariable: "A_ARG_TYPE_AVTransportID"},
					{Name: "ProtocolInfo", Direction: "out", RelatedStateVariable: "A_ARG_TYPE_ProtocolInfo"},
					{Name: "PeerConnectionManager", Direction: "out", RelatedStateVariable: "A_ARG_TYPE_ConnectionManager"},
					{Name: "PeerConnectionID", Direction: "out", RelatedStateVariable: "A_ARG_TYPE_ConnectionID"},
					{Name: "Direction", Direction: "out", RelatedStateVariable: "A_ARG_TYPE_Direction"},
					{Name: "Status", Direction: "out", RelatedStateVariable: "A_ARG_TYPE_ConnectionStatus"},
				},
			},
		},
		ServiceStateTable: []scpdStateVariable{
			{SendEvents: "no", Name: "SourceProtocolInfo", DataType: "string"},
			{SendEvents: "no", Name: "SinkProtocolInfo", DataType: "string"},
			{SendEvents: "no", Name: "CurrentConnectionIDs", DataType: "string"},
			{SendEvents: "no", Name: "A_ARG_TYPE_ConnectionID", DataType: "i4"},
			{SendEvents: "no", Name: "A_ARG_TYPE_RcsID", DataType: "i4", DefaultValue: "-1"},
			{SendEvents: "no", Name: "A_ARG_TYPE_AVTransportID", DataType: "i4", DefaultValue: "-1"},
			{SendEvents: "no", Name: "A_ARG_TYPE_ProtocolInfo", DataType: "string"},
			{SendEvents: "no", Name: "A_ARG_TYPE_ConnectionManager", DataType: "string"},
			{SendEvents: "no", Name: "A_ARG_TYPE_Direction", DataType: "string", AllowedValues: []string{"Input", "Output"}},
			{SendEvents: "no", Name: "A_ARG_TYPE_ConnectionStatus", DataType: "string", AllowedValues: []string{"OK", "Unknown"}},
		},
	}
}

func contentDirectorySCPD() scpdDocument {
	return scpdDocument{
		XMLNS: "urn:schemas-upnp-org:service-1-0",
		Spec:  UPnPSpecVersion{Major: 1, Minor: 0},
		Actions: []scpdAction{
			{
				Name: "GetSearchCapabilities",
				Arguments: []scpdArgument{
					{Name: "SearchCaps", Direction: "out", RelatedStateVariable: "SearchCapabilities"},
				},
			},
			{
				Name: "GetSortCapabilities",
				Arguments: []scpdArgument{
					{Name: "SortCaps", Direction: "out", RelatedStateVariable: "SortCapabilities"},
				},
			},
			{
				Name: "GetSystemUpdateID",
				Arguments: []scpdArgument{
					{Name: "Id", Direction: "out", RelatedStateVariable: "SystemUpdateID"},
				},
			},
			{
				Name: "Browse",
				Arguments: []scpdArgument{
					{Name: "ObjectID", Direction: "in", RelatedStateVariable: "A_ARG_TYPE_ObjectID"},
					{Name: "BrowseFlag", Direction: "in", RelatedStateVariable: "A_ARG_TYPE_BrowseFlag"},
					{Name: "Filter", Direction: "in", RelatedStateVariable: "A_ARG_TYPE_Filter"},
					{Name: "StartingIndex", Direction: "in", RelatedStateVariable: "A_ARG_TYPE_Index"},
					{Name: "RequestedCount", Direction: "in", RelatedStateVariable: "A_ARG_TYPE_Count"},
					{Name: "SortCriteria", Direction: "in", RelatedStateVariable: "A_ARG_TYPE_SortCriteria"},
					{Name: "Result", Direction: "out", RelatedStateVariable: "A_ARG_TYPE_Result"},
					{Name: "NumberReturned", Direction: "out", RelatedStateVariable: "A_ARG_TYPE_Count"},
					{Name: "TotalMatches", Direction: "out", RelatedStateVariable: "A_ARG_TYPE_Count"},
					{Name: "UpdateID", Direction: "out", RelatedStateVariable: "A_ARG_TYPE_UpdateID"},
				},
			},
		},
		ServiceStateTable: []scpdStateVariable{
			{SendEvents: "yes", Name: "SearchCapabilities", DataType: "string"},
			{SendEvents: "yes", Name: "SortCapabilities", DataType: "string"},
			{SendEvents: "yes", Name: "SystemUpdateID", DataType: "ui4", DefaultValue: strconv.Itoa(contentDirectoryDefaultUpdateID)},
			{SendEvents: "no", Name: "A_ARG_TYPE_ObjectID", DataType: "string"},
			{SendEvents: "no", Name: "A_ARG_TYPE_BrowseFlag", DataType: "string", AllowedValues: []string{"BrowseMetadata", "BrowseDirectChildren"}},
			{SendEvents: "no", Name: "A_ARG_TYPE_Filter", DataType: "string"},
			{SendEvents: "no", Name: "A_ARG_TYPE_Index", DataType: "ui4"},
			{SendEvents: "no", Name: "A_ARG_TYPE_Count", DataType: "ui4"},
			{SendEvents: "no", Name: "A_ARG_TYPE_SortCriteria", DataType: "string"},
			{SendEvents: "no", Name: "A_ARG_TYPE_Result", DataType: "string"},
			{SendEvents: "no", Name: "A_ARG_TYPE_UpdateID", DataType: "ui4"},
		},
	}
}

func writeXMLPayload(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(payload)
}

func xmlEscape(value string) string {
	var buf bytes.Buffer
	_ = xml.EscapeText(&buf, []byte(value))
	return buf.String()
}

func toContentDirectoryItems(baseURL string, lineupChannels []channels.Channel) []contentDirectoryItem {
	items := make([]contentDirectoryItem, 0, len(lineupChannels))
	for _, channel := range lineupChannels {
		items = append(items, contentDirectoryItem{
			ObjectID:    contentDirectoryChannelObjectID(channel.ChannelID),
			GuideNumber: channel.GuideNumber,
			GuideName:   channel.GuideName,
			PlaybackURL: baseURL + "/auto/v" + channel.GuideNumber,
		})
	}
	return items
}
