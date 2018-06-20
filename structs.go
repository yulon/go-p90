package p90

const mgcNum uint16 = 0x9050

type baseHeader struct {
	mgcNum uint16
	hash   uint32
	typ    uint8
}

//var baseHeaderSz int = binary.Size(baseHeader{})
const baseHeaderSz int = 7

type typeLessBaseHeader struct {
	MgcNum uint16
	Hash   uint32
}

const typeLessBaseHeaderSz int = 6

const (
	basicPacket byte = iota
	reliablePacket
	reliablePacketResponse
	softPacket
	hardPacket
	hardPacketsRequest
	latestHardPacketCountRequest
	latestHardPacketCountResponse
)

type basicPacketHeader struct{}

type softPacketHeader struct {
	LatestHardPacketC uint32
}

type hardPacketHeader struct {
	HardPacketC uint32
}

type hardPacketsRequestHeader struct {
	HardPacketCList []uint32
}

type reliablePacketHeader struct {
	ReliablePacketID uint64
}

type reliablePacketResponseHeader struct {
	ReceivedReliablePacketID uint64
}

type latestHardPacketCountRequestHeader reliablePacketHeader

type latestHardPacketCountResponseHeader struct {
	ReceivedReliablePacketID uint64
	LatestHardPacketC        uint32
}
