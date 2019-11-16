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

const MagicNumber byte = 0x90

type header struct {
	MagNum   byte
	ConID    uuid.UUID
	PktCount uint64
	PktType  byte
}

const DefaultRTT = 266 * time.Millisecond
