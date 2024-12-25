package p90

import (
	"time"

	"github.com/google/uuid"
)

// packet types
const (
	ptUnreliableData byte = iota
	ptData
	ptGots
	ptClose
	ptHowAreYou
	ptStreamData
)

var isReliablePT = []bool{
	false, // ptUnreliableData
	true,  // ptData
	false, // ptGots
	true,  // ptClose
	true,  // ptHowAreYou
	true,  // ptStreamData
}

var MagicNumber byte = 0x90

type gotPkt struct {
	ID         uint64
	SendCount  byte
	GotElapsed int64
}

type replyingCtx struct {
	gotPkt
	gotTm int64
}

func (rc *replyingCtx) Index() uint64 {
	return rc.ID
}

func (rc *replyingCtx) updateGotElapsed(now int64) {
	rc.GotElapsed = now - rc.gotTm
}

type packetHeader struct {
	Checksum     byte
	ConID        uuid.UUID
	ID           uint64
	Type         byte
	SendCount    byte
	GotDenseLast gotPkt
}

var CalcPacketHash = func(p []byte) (h byte) {
	for i := 1; i < len(p); i++ {
		h += p[i]
	}
	return
}

var PacketIsValid = func(p []byte) bool {
	return p[0]+CalcPacketHash(p) == MagicNumber
}

var CalcPacketChecksum = func(p []byte) byte {
	return MagicNumber - CalcPacketHash(p)
}

const DefaultRTT = 266 * time.Millisecond
