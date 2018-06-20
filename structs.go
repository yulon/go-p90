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
	mgcNum uint16
	hash   uint32
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
	latestHardPacketC uint32
}

type hardPacketHeader struct {
	hardPacketC uint32
}

type hardPacketsRequestHeader struct {
	hardPacketCList []uint32
}

type reliablePacketHeader struct {
	reliablePacketID uint64
}

type reliablePacketResponseHeader struct {
	receivedReliablePacketID uint64
}

type latestHardPacketCountRequestHeader reliablePacketHeader

type latestHardPacketCountResponseHeader struct {
	receivedReliablePacketID uint64
	latestHardPacketC        uint32
}
