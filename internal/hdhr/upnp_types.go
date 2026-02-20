package hdhr

import "encoding/xml"

const (
	// UPnPDescriptionPath is the primary device XML location advertised in SSDP LOCATION.
	UPnPDescriptionPath = "/upnp/device.xml"
	// UPnPDescriptionAliasPath is a compatibility alias used by some controllers.
	UPnPDescriptionAliasPath = "/device.xml"

	// SSDP/UPnP search targets supported by the compatibility profile.
	UPnPSearchTargetRootDevice = "upnp:rootdevice"
	UPnPDeviceTypeMediaServer  = "urn:schemas-upnp-org:device:MediaServer:1"
	UPnPDeviceTypeBasic        = "urn:schemas-upnp-org:device:Basic:1"
	// UPnPDeviceTypeATSCPrimary is an HDHomeRun-oriented compatibility target used by some control points.
	UPnPDeviceTypeATSCPrimary = "urn:schemas-atsc.org:device:primaryDevice:1.0"
	UPnPServiceTypeConnection = "urn:schemas-upnp-org:service:ConnectionManager:1"
	UPnPServiceTypeContentDir = "urn:schemas-upnp-org:service:ContentDirectory:1"

	UPnPConnectionSCPDPath          = "/upnp/scpd/connection-manager.xml"
	UPnPContentDirectorySCPDPath    = "/upnp/scpd/content-directory.xml"
	UPnPConnectionControlPath       = "/upnp/control/connection-manager"
	UPnPContentDirectoryControlPath = "/upnp/control/content-directory"
	UPnPConnectionEventPath         = "/upnp/event/connection-manager"
	UPnPContentDirectoryEventPath   = "/upnp/event/content-directory"
)

type UPnPRootDescription struct {
	XMLName xml.Name              `xml:"root"`
	XMLNS   string                `xml:"xmlns,attr"`
	Spec    UPnPSpecVersion       `xml:"specVersion"`
	URLBase string                `xml:"URLBase,omitempty"`
	Device  UPnPDeviceDescription `xml:"device"`
}

type UPnPSpecVersion struct {
	Major int `xml:"major"`
	Minor int `xml:"minor"`
}

type UPnPDeviceDescription struct {
	DeviceType      string                   `xml:"deviceType"`
	FriendlyName    string                   `xml:"friendlyName"`
	Manufacturer    string                   `xml:"manufacturer"`
	ModelName       string                   `xml:"modelName"`
	ModelNumber     string                   `xml:"modelNumber"`
	SerialNumber    string                   `xml:"serialNumber,omitempty"`
	UDN             string                   `xml:"UDN"`
	PresentationURL string                   `xml:"presentationURL,omitempty"`
	ServiceList     []UPnPServiceDescription `xml:"serviceList>service,omitempty"`
}

type UPnPServiceDescription struct {
	ServiceType string `xml:"serviceType"`
	ServiceID   string `xml:"serviceId"`
	SCPDURL     string `xml:"SCPDURL"`
	ControlURL  string `xml:"controlURL"`
	EventSubURL string `xml:"eventSubURL"`
}
