package p90

const mgcNum uint16 = 0x9050

type baseHeader struct {
	MgcNum uint16
	Hash   uint32
	Type   uint8
}

//var baseHeaderSz int = binary.Size(baseHeader{})
const baseHeaderSz int = 7

type typeLessBaseHeader struct {
	MgcNum uint16
	Hash   uint32
}

const typeLessBaseHeaderSz int = 6

const typeByteIndex int = 6

const (
	basicPacket byte = iota
	reliablePacket
	reliablePacketResponse
)

type basicPacketHeader struct{}

type reliablePacketHeader struct {
	ReliablePacketID uint32
}

type reliablePacketResponseHeader struct {
	ReceivedReliablePacketID   uint32
	ReceivedReliablePacketHash uint32
}
