package discovery

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
)

const (
	DiscoverPort = 65001

	typeDiscoverReq = 0x0002
	typeDiscoverRpy = 0x0003

	tagDeviceType   = 0x01
	tagDeviceID     = 0x02
	tagTunerCount   = 0x10
	tagLineupURL    = 0x27
	tagBaseURL      = 0x2A
	tagDeviceAuth   = 0x2B
	tagMultiType    = 0x2D
	deviceTypeTuner = 0x00000001

	deviceTypeWildcard = 0xFFFFFFFF
	deviceIDWildcard   = 0xFFFFFFFF
)

// TLV is a discovery packet field.
type TLV struct {
	Tag   byte
	Value []byte
}

// Packet is an HDHomeRun discovery protocol frame.
type Packet struct {
	Type uint16
	Tags []TLV
}

// DiscoverQuery captures request filters from a discovery query.
type DiscoverQuery struct {
	DeviceTypes []uint32
	DeviceID    uint32
}

// DiscoverResponse controls fields emitted by a discovery reply packet.
type DiscoverResponse struct {
	DeviceID   uint32
	TunerCount uint8
	BaseURL    string
	LineupURL  string
	DeviceAuth string
}

func ParsePacket(frame []byte) (Packet, error) {
	if len(frame) < 8 {
		return Packet{}, errors.New("frame is too short")
	}

	payloadLen := int(binary.BigEndian.Uint16(frame[2:4]))
	totalLen := 4 + payloadLen + 4
	if totalLen > len(frame) {
		return Packet{}, fmt.Errorf("payload length %d exceeds frame length %d", payloadLen, len(frame))
	}
	if totalLen < len(frame) {
		frame = frame[:totalLen]
	}

	expectedCRC := binary.LittleEndian.Uint32(frame[4+payloadLen:])
	actualCRC := crc32.ChecksumIEEE(frame[:4+payloadLen])
	if expectedCRC != actualCRC {
		return Packet{}, fmt.Errorf("crc mismatch")
	}

	payload := frame[4 : 4+payloadLen]
	tags := make([]TLV, 0, 8)
	for pos := 0; pos < len(payload); {
		if pos+1 > len(payload) {
			return Packet{}, errors.New("unexpected end of payload while reading tag")
		}
		tag := payload[pos]
		pos++

		length, consumed, err := readVarLength(payload[pos:])
		if err != nil {
			return Packet{}, fmt.Errorf("decode tag %d length: %w", tag, err)
		}
		pos += consumed
		if pos+length > len(payload) {
			return Packet{}, errors.New("tag value exceeds payload boundary")
		}

		value := make([]byte, length)
		copy(value, payload[pos:pos+length])
		pos += length

		tags = append(tags, TLV{Tag: tag, Value: value})
	}

	return Packet{
		Type: binary.BigEndian.Uint16(frame[:2]),
		Tags: tags,
	}, nil
}

func (p Packet) Marshal() ([]byte, error) {
	payload := make([]byte, 0, 256)

	for _, tag := range p.Tags {
		payload = append(payload, tag.Tag)

		var err error
		payload, err = appendVarLength(payload, len(tag.Value))
		if err != nil {
			return nil, fmt.Errorf("encode tag %d length: %w", tag.Tag, err)
		}

		payload = append(payload, tag.Value...)
	}

	if len(payload) > 0xFFFF {
		return nil, fmt.Errorf("payload too large: %d bytes", len(payload))
	}

	frame := make([]byte, 4+len(payload)+4)
	binary.BigEndian.PutUint16(frame[0:2], p.Type)
	binary.BigEndian.PutUint16(frame[2:4], uint16(len(payload)))
	copy(frame[4:], payload)

	crc := crc32.ChecksumIEEE(frame[:4+len(payload)])
	binary.LittleEndian.PutUint32(frame[4+len(payload):], crc)
	return frame, nil
}

func ParseDiscoverRequest(frame []byte) (DiscoverQuery, error) {
	pkt, err := ParsePacket(frame)
	if err != nil {
		return DiscoverQuery{}, err
	}
	if pkt.Type != typeDiscoverReq {
		return DiscoverQuery{}, fmt.Errorf("packet type %d is not discover request", pkt.Type)
	}

	query := DiscoverQuery{
		DeviceID: deviceIDWildcard,
	}

	for _, tag := range pkt.Tags {
		switch tag.Tag {
		case tagDeviceType:
			if len(tag.Value) != 4 {
				continue
			}
			query.DeviceTypes = append(query.DeviceTypes, binary.BigEndian.Uint32(tag.Value))
		case tagMultiType:
			if len(tag.Value)%4 != 0 {
				continue
			}
			for i := 0; i < len(tag.Value); i += 4 {
				query.DeviceTypes = append(query.DeviceTypes, binary.BigEndian.Uint32(tag.Value[i:i+4]))
			}
		case tagDeviceID:
			if len(tag.Value) != 4 {
				continue
			}
			query.DeviceID = binary.BigEndian.Uint32(tag.Value)
		default:
		}
	}

	return query, nil
}

func (q DiscoverQuery) WantsTuner() bool {
	if len(q.DeviceTypes) == 0 {
		return true
	}
	for _, deviceType := range q.DeviceTypes {
		if deviceType == deviceTypeWildcard || deviceType == deviceTypeTuner {
			return true
		}
	}
	return false
}

func (q DiscoverQuery) WantsDeviceID(deviceID uint32) bool {
	return q.DeviceID == deviceIDWildcard || q.DeviceID == deviceID
}

func BuildDiscoverResponseFrame(resp DiscoverResponse) ([]byte, error) {
	tags := []TLV{
		{Tag: tagDeviceType, Value: uint32Bytes(deviceTypeTuner)},
		{Tag: tagDeviceID, Value: uint32Bytes(resp.DeviceID)},
		{Tag: tagTunerCount, Value: []byte{resp.TunerCount}},
	}

	baseURL := strings.TrimSpace(resp.BaseURL)
	if baseURL != "" {
		tags = append(tags, TLV{
			Tag:   tagBaseURL,
			Value: []byte(baseURL),
		})
	}

	lineupURL := strings.TrimSpace(resp.LineupURL)
	if lineupURL != "" {
		tags = append(tags, TLV{
			Tag:   tagLineupURL,
			Value: []byte(lineupURL),
		})
	}

	deviceAuth := strings.TrimSpace(resp.DeviceAuth)
	if deviceAuth != "" {
		tags = append(tags, TLV{
			Tag:   tagDeviceAuth,
			Value: []byte(deviceAuth),
		})
	}

	return Packet{
		Type: typeDiscoverRpy,
		Tags: tags,
	}.Marshal()
}

func readVarLength(in []byte) (length int, consumed int, err error) {
	if len(in) == 0 {
		return 0, 0, errors.New("missing varlen field")
	}

	first := int(in[0])
	if first&0x80 == 0 {
		return first, 1, nil
	}
	if len(in) < 2 {
		return 0, 0, errors.New("truncated two-byte varlen")
	}

	length = (first & 0x7F) | (int(in[1]) << 7)
	return length, 2, nil
}

func appendVarLength(dst []byte, length int) ([]byte, error) {
	if length < 0 {
		return nil, errors.New("negative length")
	}
	if length <= 0x7F {
		return append(dst, byte(length)), nil
	}
	if length > 0x7FFF {
		return nil, fmt.Errorf("length %d exceeds protocol varlen max 32767", length)
	}

	return append(dst, byte((length&0x7F)|0x80), byte(length>>7)), nil
}

func uint32Bytes(v uint32) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, v)
	return out
}
