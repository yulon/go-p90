package p90

import (
	"time"

	"github.com/google/uuid"
)

// packet types
const (
	ptUnreliableData byte = iota
	ptData
	ptReceiveds
	ptRequests
	ptClose
	ptHowAreYou
	ptStreamData
)

var isReliablePT = []bool{
	false, // ptUnreliableData
	true,  // ptData
	false, // ptReceiveds
	false, // ptRequests
	true,  // ptClose
	true,  // ptHowAreYou
	true,  // ptStreamData
}

var MagicNumber byte = 0x90

type packetHeader struct {
	Checksum  byte
	ConID     uuid.UUID
	ID        uint64
	Type      byte
	SendTime  int64
	SendCount byte
}

type receivedPacketInfo struct {
	ID        uint64
	SendTime  int64
	SendCount byte
	RecvTime  int64
}

var CalcPacketHeaderHash = func(h *packetHeader) byte {
	var n byte
	for _, b := range h.ConID {
		n += b
	}
	n += byte(h.ID)
	n += byte(h.ID >> 8)
	n += byte(h.ID >> 16)
	n += byte(h.ID >> 24)
	n += byte(h.ID >> 32)
	n += byte(h.ID >> 40)
	n += byte(h.ID >> 48)
	n += byte(h.ID >> 56)
	n += h.Type
	n += byte(h.SendTime)
	n += byte(h.SendTime >> 8)
	n += byte(h.SendTime >> 16)
	n += byte(h.SendTime >> 24)
	n += byte(h.SendTime >> 32)
	n += byte(h.SendTime >> 40)
	n += byte(h.SendTime >> 48)
	n += byte(h.SendTime >> 56)
	n += h.SendCount
	return n
}

var PacketHeaderIsValid = func(h *packetHeader) bool {
	return CalcPacketHeaderHash(h)+h.Checksum == MagicNumber
}

var CalcPacketHeaderChecksum = func(h *packetHeader) byte {
	return MagicNumber - CalcPacketHeaderHash(h)
}

const DefaultRTT = 266 * time.Millisecond
