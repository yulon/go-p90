package p90

import (
	"bytes"
	"encoding/binary"
	"net"
	"sync/atomic"
	"time"
)

func writePacketBody(buf *bytes.Buffer, others ...interface{}) {
	for _, other := range others {
		switch other.(type) {
		case nil:
		case []byte:
			buf.Write(other.([]byte))
		default:
			binary.Write(buf, binary.LittleEndian, other)
		}
	}
}

func makePacketBody(others ...interface{}) []byte {
	buf := bytes.NewBuffer(nil)
	writePacketBody(buf, others...)
	return buf.Bytes()
}

func writePacket(buf *bytes.Buffer, h *packetHeader, others ...interface{}) {
	h.Checksum = CalcPacketHeaderChecksum(h)
	binary.Write(buf, binary.LittleEndian, h)
	writePacketBody(buf, others...)
}

func makePacket(h *packetHeader, others ...interface{}) []byte {
	buf := bytes.NewBuffer(nil)
	writePacket(buf, h, others...)
	return buf.Bytes()
}

var ipv4Localhost = net.ParseIP("127.0.0.1")

func NewLocalUDPAddr(port int, remoteUDPAddr *net.UDPAddr) *net.UDPAddr {
	localUDPAddr := &net.UDPAddr{
		Port: port,
	}
	if remoteUDPAddr.IP.Equal(ipv4Localhost) {
		localUDPAddr.IP = ipv4Localhost
	} else {
		localUDPAddr.IP = net.IPv4zero
	}
	return localUDPAddr
}

type atomicTime struct {
	val int64
}

func newAtomicTime(t time.Time) *atomicTime {
	return &atomicTime{t.Unix()}
}

func (at *atomicTime) Set(t time.Time) {
	atomic.StoreInt64(&at.val, t.Unix())
}

func (at *atomicTime) Get() time.Time {
	return time.Unix(atomic.LoadInt64(&at.val), 0)
}

type atomicDur struct {
	val int64
}

func newAtomicDur(d time.Duration) *atomicDur {
	return &atomicDur{int64(d)}
}

func (ad *atomicDur) Set(d time.Duration) {
	atomic.StoreInt64(&ad.val, int64(d))
}

func (ad *atomicDur) Get() time.Duration {
	return time.Duration(atomic.LoadInt64(&ad.val))
}

var endOfTimeTick = make(chan time.Time, 1)

func getTick(tick <-chan time.Time) <-chan time.Time {
	if tick == nil {
		return endOfTimeTick
	}
	return tick
}
