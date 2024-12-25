package p90

const mgcNumValue uint16 = 0x9050

type baseHeader struct {
	MgcNum uint16
	Type   uint8
	Size   uint16
}

const baseHeaderSz int = 5

const typeIndex int = 2

const hashSz int = 2

const pktSzMin int = baseHeaderSz + hashSz

const (
	basicPacket byte = iota
	reliablePacket
	reliablePacketResponse
)

type basicPacketHeader struct{}

type reliablePacketHeader struct {
	ReliablePacketID uint32
}

const reliablePacketHeaderSz int = 4

type reliablePacketResponseHeader struct {
	ReceivedReliablePacketID   uint32
	ReceivedReliablePacketHash uint16
}

const reliablePacketResponseHeaderSz int = 6
