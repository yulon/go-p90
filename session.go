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
	hash        uint32
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

func bkdr(data []byte) uint32 {
	var seed uint32 = 131
	var hash uint32 = 1
	for i := 0; i < len(data); i++ {
		hash = hash*seed + uint32(data[i])
	}
	return hash & 0x7FFFFFFF
}

func validBlockSz(data []byte, h uint32) int {
	var seed uint32 = 131
	var hash uint32 = 1
	for i := 0; i < len(data) && i < 521; i++ {
		hash = hash*seed + uint32(data[i])
		if hash&0x7FFFFFFF == h {
			return i + 1
		}
	}
	return 0
}

func (s *Session) sendHashedValidBlock(addr *net.UDPAddr, hash uint32, validBlock []byte) {
	pktBuf := bytes.NewBuffer([]byte{})

	binary.Write(pktBuf, binary.LittleEndian, typeLessBaseHeader{
		mgcNum,
		hash,
	})

	pktBuf.Write(validBlock)

	s.udpConn.WriteToUDP(pktBuf.Bytes(), addr)
}

func (s *Session) sendValidBlock(addr *net.UDPAddr, validBlock []byte) {
	s.sendHashedValidBlock(addr, bkdr(validBlock), validBlock)
}

func (s *Session) Send(addr *net.UDPAddr, data []byte) {
	validBlockBuf := bytes.NewBuffer([]byte{})
	validBlockBuf.WriteByte(basicPacket)
	validBlockBuf.Write(data)

	s.sendValidBlock(addr, validBlockBuf.Bytes())
}

func (s *Session) ReliableSend(addr *net.UDPAddr, data []byte) {
	ts := time.Now()

	validBlockBuf := bytes.NewBuffer([]byte{})
	validBlockBuf.WriteByte(reliablePacket)

	c := newConn()
	v, loaded := s.connMap.LoadOrStore(addr.String(), c)
	if loaded {
		c = v.(*conn)
	}

	id := atomic.AddUint32(&c.rpIDCount, 1)

	binary.Write(validBlockBuf, binary.LittleEndian, reliablePacketHeader{
		id,
	})

	validBlockBuf.Write(data)

	validBlock := validBlockBuf.Bytes()
	validBlockHash := bkdr(validBlock)

	rpi := &rpInfo{validBlockHash, ts, 0}
	c.rpMap.Store(id, rpi)

	s.sendHashedValidBlock(addr, validBlockHash, validBlock)

	go func() {
		dur := time.Duration(atomic.LoadInt64(&c.rtt))
		var sleeped time.Duration
		for sleeped < time.Minute {
			dur *= 2
			time.Sleep(dur)
			sleeped += dur

			isResponsed := atomic.LoadUint32(&rpi.isResponsed)
			if isResponsed == 1 {
				return
			}
			s.sendHashedValidBlock(addr, validBlockHash, validBlock)
		}
		c.rpMap.Delete(id)
	}()
}

func (s *Session) sendRpResp(addr *net.UDPAddr, receivedRpID uint32) {
	validBlockBuf := bytes.NewBuffer([]byte{})
	validBlockBuf.WriteByte(reliablePacketResponse)

	validBlock := validBlockBuf.Bytes()
	validBlockHash := bkdr(validBlock)

	binary.Write(validBlockBuf, binary.LittleEndian, reliablePacketResponseHeader{
		receivedRpID,
		validBlockHash,
	})

	s.sendHashedValidBlock(addr, validBlockHash, validBlock)
}

func (s *Session) Listen() {
	cache := make([]byte, 1024)
	for {
		udpPktSz, addr, err := s.udpConn.ReadFromUDP(cache)
		if err != nil || udpPktSz < baseHeaderSz {
			continue
		}

		var tlBaseHeader typeLessBaseHeader

		ok := false

		for i := 0; i < udpPktSz-baseHeaderSz; i++ {
			buf := bytes.NewReader(cache[i:])
			binary.Read(buf, binary.LittleEndian, &tlBaseHeader)

			if tlBaseHeader.MgcNum == mgcNum {
				ok = true
				break
			}
		}

		if !ok {
			continue
		}

		udpPkt := make([]byte, udpPktSz)
		copy(udpPkt, cache)

		go func() {
			var c *conn

			pktSz := validBlockSz(udpPkt[typeByteIndex:], tlBaseHeader.Hash)
			if pktSz == 0 {
				return
			}
			pktSz += typeLessBaseHeaderSz

			switch udpPkt[typeByteIndex] {

			case basicPacket:
				if pktSz == baseHeaderSz {
					return
				}
				validBlock := udpPkt[baseHeaderSz:pktSz]
				s.receiver(addr, validBlock)

			case reliablePacket:
				var secHeader reliablePacketHeader
				secHeaderSz := binary.Size(secHeader)
				HeaderSz := baseHeaderSz + secHeaderSz

				if pktSz < HeaderSz {
					return
				}

				validBlock := udpPkt[baseHeaderSz:pktSz]
				buf := bytes.NewReader(validBlock)
				binary.Read(buf, binary.LittleEndian, &secHeader)

				s.sendRpResp(addr, secHeader.ReliablePacketID)

				c = newConn()
				v, loaded := s.connMap.LoadOrStore(addr.String(), c)
				if loaded {
					c = v.(*conn)
				}

				v, loaded = c.rvRpMap.LoadOrStore(secHeader.ReliablePacketID, true)
				if loaded {
					return
				}

				if pktSz == HeaderSz {
					return
				}
				s.receiver(addr, validBlock[secHeaderSz:])

			case reliablePacketResponse:
				var secHeader reliablePacketResponseHeader
				secHeaderSz := binary.Size(secHeader)
				HeaderSz := baseHeaderSz + secHeaderSz

				if pktSz < HeaderSz {
					return
				}

				validBlock := udpPkt[baseHeaderSz:pktSz]
				buf := bytes.NewReader(validBlock)
				binary.Read(buf, binary.LittleEndian, &secHeader)

				v, loaded := s.connMap.Load(addr.String())
				if !loaded {
					return
				}
				c = v.(*conn)

				v, loaded = c.rpMap.Load(secHeader.ReceivedReliablePacketID)
				if !loaded {
					return
				}
				rpi := v.(*rpInfo)

				if rpi.hash != secHeader.ReceivedReliablePacketHash {
					return
				}

				c.rpMap.Delete(secHeader.ReceivedReliablePacketID)

				atomic.StoreUint32(&rpi.isResponsed, 1)

				atomic.StoreInt64(&c.rtt, int64(time.Now().Sub(rpi.ts)))
			}
		}()
	}
}
