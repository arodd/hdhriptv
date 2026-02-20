package discovery

import (
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
)

func TestPacketRoundTripWithTwoByteVarLength(t *testing.T) {
	largeValue := make([]byte, 130)
	for i := range largeValue {
		largeValue[i] = 'a'
	}

	in := Packet{
		Type: typeDiscoverReq,
		Tags: []TLV{
			{Tag: tagDeviceType, Value: uint32Bytes(deviceTypeTuner)},
			{Tag: tagBaseURL, Value: largeValue},
		},
	}

	raw, err := in.Marshal()
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	out, err := ParsePacket(raw)
	if err != nil {
		t.Fatalf("ParsePacket() error = %v", err)
	}

	if !reflect.DeepEqual(out, in) {
		t.Fatalf("round-trip packet mismatch:\nout = %#v\nin  = %#v", out, in)
	}
}

func TestParsePacketRejectsBadCRC(t *testing.T) {
	raw, err := Packet{
		Type: typeDiscoverReq,
		Tags: []TLV{{Tag: tagDeviceType, Value: uint32Bytes(deviceTypeTuner)}},
	}.Marshal()
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	raw[len(raw)-1] ^= 0xFF

	if _, err := ParsePacket(raw); err == nil || !strings.Contains(err.Error(), "crc mismatch") {
		t.Fatalf("ParsePacket() error = %v, want crc mismatch", err)
	}
}

func TestParseDiscoverRequest(t *testing.T) {
	raw, err := Packet{
		Type: typeDiscoverReq,
		Tags: []TLV{
			{Tag: tagMultiType, Value: append(uint32Bytes(0x00000005), uint32Bytes(deviceTypeTuner)...)},
			{Tag: tagDeviceID, Value: uint32Bytes(0x1234ABCD)},
		},
	}.Marshal()
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	query, err := ParseDiscoverRequest(raw)
	if err != nil {
		t.Fatalf("ParseDiscoverRequest() error = %v", err)
	}

	if len(query.DeviceTypes) != 2 {
		t.Fatalf("len(DeviceTypes) = %d, want 2", len(query.DeviceTypes))
	}
	if query.DeviceID != 0x1234ABCD {
		t.Fatalf("DeviceID = %#x, want %#x", query.DeviceID, uint32(0x1234ABCD))
	}
	if !query.WantsTuner() {
		t.Fatal("WantsTuner() = false, want true")
	}
	if !query.WantsDeviceID(0x1234ABCD) {
		t.Fatal("WantsDeviceID() = false, want true")
	}
}

func TestBuildDiscoverResponseFrame(t *testing.T) {
	raw, err := BuildDiscoverResponseFrame(DiscoverResponse{
		DeviceID:   0xABCD1234,
		TunerCount: 2,
		BaseURL:    "http://192.168.1.20:5004",
		LineupURL:  "http://192.168.1.20:5004/lineup.json",
		DeviceAuth: "token123",
	})
	if err != nil {
		t.Fatalf("BuildDiscoverResponseFrame() error = %v", err)
	}

	pkt, err := ParsePacket(raw)
	if err != nil {
		t.Fatalf("ParsePacket() error = %v", err)
	}
	if pkt.Type != typeDiscoverRpy {
		t.Fatalf("packet type = %d, want %d", pkt.Type, typeDiscoverRpy)
	}

	tagValues := make(map[byte][]byte, len(pkt.Tags))
	for _, tag := range pkt.Tags {
		tagValues[tag.Tag] = tag.Value
	}

	if got := binary.BigEndian.Uint32(tagValues[tagDeviceID]); got != 0xABCD1234 {
		t.Fatalf("device id tag = %#x, want %#x", got, uint32(0xABCD1234))
	}
	if got := tagValues[tagTunerCount][0]; got != 2 {
		t.Fatalf("tuner count tag = %d, want 2", got)
	}
	if got := string(tagValues[tagBaseURL]); got != "http://192.168.1.20:5004" {
		t.Fatalf("base url tag = %q, want %q", got, "http://192.168.1.20:5004")
	}
}
