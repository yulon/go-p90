package p90

import (
	"bytes"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type rpInfo struct {
	hash        uint16
	ts          time.Time
	isResponsed uint32
}

type conn struct {
	rpIDCount uint32
	rpMap     sync.Map // map[rpID]*rpInfo
	rvRpMap   sync.Map // map[rpID]interface{}
	rtt       int64
}

func newConn() *conn {
	return &conn{rtt: int64(time.Second)}
}

type Session struct {
	udpConn  *net.UDPConn
	connMap  sync.Map // map[string]*conn
	receiver func(addr *net.UDPAddr, data []byte)
}

func NewSessionFromUDP(udpAddr *net.UDPAddr, receiver func(addr *net.UDPAddr, data []byte)) (*Session, error) {
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	return &Session{udpConn, sync.Map{}, receiver}, nil
}

func NewSession(addr string, receiver func(addr *net.UDPAddr, data []byte)) (*Session, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	return NewSessionFromUDP(udpAddr, receiver)
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

func (s *Session) Send(addr *net.UDPAddr, data []byte) {
	pktBuf := makeBaseHeader(basicPacket, len(data))

	pktBuf.Write(data)

	writeHash(pktBuf)
	s.udpConn.WriteToUDP(pktBuf.Bytes(), addr)
}

func (s *Session) ReliableSend(addr *net.UDPAddr, data []byte) {
	ts := time.Now()

	pktBuf := makeBaseHeader(reliablePacket, reliablePacketHeaderSz+len(data))

	c := newConn()
	v, loaded := s.connMap.LoadOrStore(addr.String(), c)
	if loaded {
		c = v.(*conn)
	}

	id := atomic.AddUint32(&c.rpIDCount, 1)

	binary.Write(pktBuf, binary.LittleEndian, reliablePacketHeader{
		id,
	})
	pktBuf.Write(data)

	hash := writeHashAndRet(pktBuf)

	rpi := &rpInfo{hash, ts, 0}
	c.rpMap.Store(id, rpi)

	pkt := pktBuf.Bytes()
	s.udpConn.WriteToUDP(pkt, addr)

	go func() {
		dur := time.Duration(atomic.LoadInt64(&c.rtt))
		var sleeped time.Duration
		for sleeped < time.Minute {
			dur *= 2
			time.Sleep(dur)
			sleeped += dur

			if atomic.LoadUint32(&rpi.isResponsed) == 1 {
				return
			}
			s.udpConn.WriteToUDP(pkt, addr)
		}
		c.rpMap.Delete(id)
	}()
}

func (s *Session) sendRpResp(addr *net.UDPAddr, receivedRpID uint32, receivedRpHash uint16) {
	pktBuf := makeBaseHeader(reliablePacketResponse, reliablePacketResponseHeaderSz)

	binary.Write(pktBuf, binary.LittleEndian, reliablePacketResponseHeader{
		receivedRpID,
		receivedRpHash,
	})

	writeHash(pktBuf)
	s.udpConn.WriteToUDP(pktBuf.Bytes(), addr)
}

func (s *Session) Listen() {
	buf := make([]byte, 512)
	for {
		udpPktSz, addr, err := s.udpConn.ReadFromUDP(buf)
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
			var c *conn

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
				s.receiver(addr, pkt[baseHeaderSz:dataEnd])

			case reliablePacket:
				var rpHeader reliablePacketHeader
				headersSz := baseHeaderSz + reliablePacketHeaderSz

				if pktSz < headersSz {
					return
				}

				binary.Read(bytes.NewReader(pkt[baseHeaderSz:]), binary.LittleEndian, &rpHeader)

				s.sendRpResp(addr, rpHeader.ReliablePacketID, hash)

				c = newConn()
				v, loaded := s.connMap.LoadOrStore(addr.String(), c)
				if loaded {
					c = v.(*conn)
				}

				v, loaded = c.rvRpMap.LoadOrStore(rpHeader.ReliablePacketID, true)
				if loaded {
					return
				}

				if pktSz == headersSz+hashSz {
					return
				}
				s.receiver(addr, pkt[baseHeaderSz:dataEnd])

			case reliablePacketResponse:
				var rprHeader reliablePacketResponseHeader
				headersSz := baseHeaderSz + reliablePacketResponseHeaderSz

				if pktSz < headersSz {
					return
				}

				binary.Read(bytes.NewReader(pkt[baseHeaderSz:]), binary.LittleEndian, &rprHeader)

				v, loaded := s.connMap.Load(addr.String())
				if !loaded {
					return
				}
				c = v.(*conn)

				v, loaded = c.rpMap.Load(rprHeader.ReceivedReliablePacketID)
				if !loaded {
					return
				}
				rpi := v.(*rpInfo)

				if rpi.hash != rprHeader.ReceivedReliablePacketHash {
					return
				}

				c.rpMap.Delete(rprHeader.ReceivedReliablePacketID)

				atomic.StoreUint32(&rpi.isResponsed, 1)

				atomic.StoreInt64(&c.rtt, int64(time.Now().Sub(rpi.ts)))
			}
		}()
	}
}
