package p90

import "time"

const (
	pktUnreliableData byte = iota
	pktData
	pktReceiveds
	pktRequests
	pktClosed
	pktHowAreYou
	pktStreamData
)

var isReliableType = []bool{
	false, // pktUnreliableData
	true,  // pktData
	false, // pktReceiveds
	false, // pktRequests
	true,  // pktClosed
	true,  // pktHowAreYou
	true,  // pktStreamData
}

const MagicNumber byte = 0x90

const resendPktsSizeMax = 1024 * 1024

const DefaultRTT = 266 * time.Millisecond
