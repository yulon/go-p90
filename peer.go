package p90

import (
	"bytes"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type reliablePacketCache struct {
	ts          time.Time
	hash        uint16
	pkt         []byte
	isResponsed bool
}

type connContext struct {
	rpIDCount uint32

	rpMap       map[uint32]*reliablePacketCache
	hasRtnsLoop bool
	rtnsGrtMtx  sync.Mutex

	rvRpMap sync.Map // map[rpID]interface{}
	rtt     int64
}

func newConnContext() *connContext {
	return &connContext{rpMap: map[uint32]*reliablePacketCache{}, hasRtnsLoop: false, rtt: int64(time.Second)}
}

type Receiver func(conn *Conn, data []byte)

type Peer struct {
	udpConn    *net.UDPConn
	connCtxMap sync.Map // map[string]*connContext

	receiver Receiver

	closed chan bool
}

func newPeer(udpAddr *net.UDPAddr, receiver Receiver) (*Peer, error) {
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	peer := &Peer{udpConn, sync.Map{}, receiver, make(chan bool, 1)}
	go peer.listen()
	return peer, nil
}

func NewPeer(addr string, receiver Receiver) (*Peer, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	return newPeer(udpAddr, receiver)
}

func Listen(addr string, receiver Receiver) error {
	peer, err := NewPeer(addr, receiver)
	if err != nil {
		return err
	}
	peer.WaitClose()
	return nil
}

type Conn struct {
	peer    *Peer
	addr    *net.UDPAddr
	addrStr string
	ctx     *connContext
}

func (peer *Peer) dial(addr *net.UDPAddr) *Conn {
	return &Conn{peer, addr, "", nil}
}

func (peer *Peer) Dial(addr string) (*Conn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	return peer.dial(udpAddr), nil
}

func (c *Conn) Addr() string {
	if len(c.addrStr) == 0 {
		c.addrStr = c.addr.String()
	}
	return c.addrStr
}

func (c *Conn) getOrNewCtx() {
	c.ctx = newConnContext()
	v, loaded := c.peer.connCtxMap.LoadOrStore(c.addr.String(), c.ctx)
	if loaded {
		c.ctx = v.(*connContext)
	}
}

func (c *Conn) getCtx() bool {
	v, loaded := c.peer.connCtxMap.Load(c.addr.String())
	if loaded {
		c.ctx = v.(*connContext)
		return true
	}
	return false
}

func bkdrHash(data []byte) uint16 {
	var seed uint64 = 131
	var hash uint64 = 1
	for i := 0; i < len(data); i++ {
		hash = hash*seed + uint64(data[i])
	}
	return uint16(hash % 65535)
}

func makeBaseHeader(typ uint8, secHeaderAndBodySz int) *bytes.Buffer {
	pktBuf := bytes.NewBuffer([]byte{})
	binary.Write(pktBuf, binary.LittleEndian, baseHeader{
		mgcNumValue,
		typ,
		uint16(baseHeaderSz + secHeaderAndBodySz + hashSz),
	})
	return pktBuf
}

func writeHash(pktBuf *bytes.Buffer) {
	binary.Write(pktBuf, binary.LittleEndian, bkdrHash(pktBuf.Bytes()[typeIndex:]))
}

func writeHashAndRet(pktBuf *bytes.Buffer) uint16 {
	hash := bkdrHash(pktBuf.Bytes()[typeIndex:])
	binary.Write(pktBuf, binary.LittleEndian, hash)
	return hash
}

func (c *Conn) Send(data []byte) {
	pktBuf := makeBaseHeader(basicPacket, len(data))

	pktBuf.Write(data)

	writeHash(pktBuf)
	c.peer.udpConn.WriteToUDP(pktBuf.Bytes(), c.addr)
}

func (c *Conn) ReliableSend(data []byte) {
	ts := time.Now()

	pktBuf := makeBaseHeader(reliablePacket, reliablePacketHeaderSz+len(data))

	if c.ctx == nil {
		c.getOrNewCtx()
	}

	id := atomic.AddUint32(&c.ctx.rpIDCount, 1)

	binary.Write(pktBuf, binary.LittleEndian, reliablePacketHeader{
		id,
	})
	pktBuf.Write(data)

	hash :=
		writeHashAndRet(pktBuf)
	pkt := pktBuf.Bytes()

	c.ctx.rtnsGrtMtx.Lock()

	c.peer.udpConn.WriteToUDP(pkt, c.addr)

	rpCache := &reliablePacketCache{ts, hash, pkt, false}

	c.ctx.rpMap[id] = rpCache

	if c.ctx.hasRtnsLoop {
		c.ctx.rtnsGrtMtx.Unlock()
		return
	}
	c.ctx.hasRtnsLoop = true

	go func() {
		for {
			var rpCache *reliablePacketCache

			c.ctx.rtnsGrtMtx.Lock()

			for _, rpCache = range c.ctx.rpMap {
				break
			}
			if rpCache == nil {
				c.ctx.hasRtnsLoop = false
				c.ctx.rtnsGrtMtx.Unlock()
				return
			}

			c.ctx.rtnsGrtMtx.Unlock()

			dur := time.Duration(atomic.LoadInt64(&c.ctx.rtt))
			var sleeped time.Duration
			for sleeped < time.Minute {
				dur *= 2
				time.Sleep(dur)
				sleeped += dur

				c.ctx.rtnsGrtMtx.Lock()
				if rpCache.isResponsed {
					c.ctx.rtnsGrtMtx.Unlock()
					break
				}
				c.ctx.rtnsGrtMtx.Unlock()

				c.peer.udpConn.WriteToUDP(rpCache.pkt, c.addr)
			}
		}
	}()

	c.ctx.rtnsGrtMtx.Unlock()
}

func (peer *Peer) respondRP(addr *net.UDPAddr, receivedRpID uint32, receivedRpHash uint16) {
	pktBuf := makeBaseHeader(reliablePacketResponse, reliablePacketResponseHeaderSz)

	binary.Write(pktBuf, binary.LittleEndian, reliablePacketResponseHeader{
		receivedRpID,
		receivedRpHash,
	})

	writeHash(pktBuf)
	peer.udpConn.WriteToUDP(pktBuf.Bytes(), addr)
}

func (peer *Peer) listen() {
	buf := make([]byte, 512)
	for {
		udpPktSz, addr, err := peer.udpConn.ReadFromUDP(buf)
		if err != nil || udpPktSz < pktSzMin {
			continue
		}

		var bHeader baseHeader
		binary.Read(bytes.NewReader(buf), binary.LittleEndian, &bHeader)
		if bHeader.MgcNum != mgcNumValue || int(bHeader.Size) > udpPktSz {
			continue
		}

		pktSz := int(bHeader.Size)
		pkt := make([]byte, pktSz)
		copy(pkt, buf)

		go func() {
			var hash uint16
			dataEnd := pktSz - hashSz
			binary.Read(bytes.NewReader(pkt[dataEnd:]), binary.LittleEndian, &hash)
			if hash != bkdrHash(pkt[typeIndex:dataEnd]) {
				return
			}

			switch pkt[typeIndex] {

			case basicPacket:
				if pktSz == pktSzMin {
					return
				}

				if peer.receiver == nil {
					return
				}
				peer.receiver(peer.dial(addr), pkt[baseHeaderSz:dataEnd])

			case reliablePacket:
				var rpHeader reliablePacketHeader
				headersSz := baseHeaderSz + reliablePacketHeaderSz

				if pktSz < headersSz {
					return
				}

				binary.Read(bytes.NewReader(pkt[baseHeaderSz:]), binary.LittleEndian, &rpHeader)

				peer.respondRP(addr, rpHeader.ReliablePacketID, hash)

				c := peer.dial(addr)
				c.getOrNewCtx()

				_, loaded := c.ctx.rvRpMap.LoadOrStore(rpHeader.ReliablePacketID, true)
				if loaded {
					return
				}

				if pktSz == headersSz+hashSz {
					return
				}

				if peer.receiver == nil {
					return
				}
				peer.receiver(c, pkt[baseHeaderSz:dataEnd])

			case reliablePacketResponse:
				var rprHeader reliablePacketResponseHeader
				headersSz := baseHeaderSz + reliablePacketResponseHeaderSz

				if pktSz < headersSz {
					return
				}

				binary.Read(bytes.NewReader(pkt[baseHeaderSz:]), binary.LittleEndian, &rprHeader)

				c := peer.dial(addr)
				if !c.getCtx() {
					return
				}

				c.ctx.rtnsGrtMtx.Lock()

				rpCache, loaded := c.ctx.rpMap[rprHeader.ReceivedReliablePacketID]
				if !loaded {
					c.ctx.rtnsGrtMtx.Unlock()
					return
				}

				if rpCache.hash != rprHeader.ReceivedReliablePacketHash {
					c.ctx.rtnsGrtMtx.Unlock()
					return
				}

				rpCache.isResponsed = true
				delete(c.ctx.rpMap, rprHeader.ReceivedReliablePacketID)

				atomic.StoreInt64(&c.ctx.rtt, int64(time.Now().Sub(rpCache.ts)))

				c.ctx.rtnsGrtMtx.Unlock()
			}
		}()
	}
	peer.closed <- true
}

func (peer *Peer) WaitClose() {
	<-peer.closed
	peer.closed <- true
}
