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

type Header struct {
	Checksum byte
	ConID    uuid.UUID
	PktCount uint64
	PktType  byte
}

var CalcHeaderValue = func(h *Header) byte {
	var n byte
	for _, b := range h.ConID {
		n += b
	}
	n += byte(h.PktCount)
	n += byte(h.PktCount >> 8)
	n += byte(h.PktCount >> 16)
	n += byte(h.PktCount >> 24)
	n += byte(h.PktCount >> 32)
	n += byte(h.PktCount >> 40)
	n += byte(h.PktCount >> 48)
	n += byte(h.PktCount >> 56)
	n += h.PktType
	return n
}

var HeaderIsValid = func(h *Header) bool {
	return CalcHeaderValue(h)+h.Checksum == MagicNumber
}

var GenHeaderChecksum = func(h *Header) byte {
	return MagicNumber - CalcHeaderValue(h)
}

const DefaultRTT = 266 * time.Millisecond
