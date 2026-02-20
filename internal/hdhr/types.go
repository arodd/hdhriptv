package hdhr

import "encoding/xml"

// DiscoverResponse describes emulator metadata exposed at /discover.json.
type DiscoverResponse struct {
	FriendlyName    string `json:"FriendlyName"`
	DeviceID        string `json:"DeviceID"`
	DeviceAuth      string `json:"DeviceAuth"`
	BaseURL         string `json:"BaseURL"`
	LineupURL       string `json:"LineupURL"`
	ModelNumber     string `json:"ModelNumber"`
	FirmwareName    string `json:"FirmwareName"`
	FirmwareVersion string `json:"FirmwareVersion"`
	TunerCount      int    `json:"TunerCount"`
}

// LineupEntry is a channel record in lineup responses.
type LineupEntry struct {
	GuideNumber string `json:"GuideNumber" xml:"GuideNumber"`
	GuideName   string `json:"GuideName" xml:"GuideName"`
	URL         string `json:"URL" xml:"URL"`
	Tags        string `json:"Tags,omitempty" xml:"Tags,omitempty"`
}

// LineupXMLResponse is the /lineup.xml payload.
type LineupXMLResponse struct {
	XMLName xml.Name      `xml:"Lineup"`
	Items   []LineupEntry `xml:"Program"`
}

// LineupStatusResponse is a compatibility stub for scan status polling.
type LineupStatusResponse struct {
	ScanInProgress int      `json:"ScanInProgress"`
	ScanPossible   int      `json:"ScanPossible"`
	Source         string   `json:"Source"`
	SourceList     []string `json:"SourceList"`
}
