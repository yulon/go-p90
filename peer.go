package p90

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Peer struct {
	mtx       sync.Mutex
	locLnr    net.PacketConn
	conMap    sync.Map
	acptCh    chan *Conn
	wasClosed bool
}

func (pr *Peer) opErr(op string, srcErr error) error {
	if srcErr == nil {
		return nil
	}
	return &net.OpError{Op: op, Net: "p90", Source: pr.Addr(), Addr: nil, Err: srcErr}
}

var errPeerWasClosed = errors.New("peer was closed")

func (pr *Peer) writeTo(b []byte, addr net.Addr) (int, error) {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if pr.wasClosed {
		return 0, errPeerWasClosed
	}
	return pr.locLnr.WriteTo(b, addr)
}

func (pr *Peer) tryRespCloseTo(addr net.Addr, h *Header) bool {
	if h.PktType != ptClose {
		return false
	}
	h.PktType = ptReceiveds
	h.PktCount = 0
	pr.writeTo(makePacket(h), addr)
	return true
}

func (pr *Peer) bypassRecv(from net.Addr, to net.PacketConn, h *Header, p []byte) {
	r := bytes.NewBuffer(nil)
	r.Write(p)
	err := binary.Read(r, binary.LittleEndian, h)

	if err != nil || !HeaderIsValid(h) || int(h.PktType) >= len(isReliablePT) {
		return
	}

	var con *Conn
	v, ok := pr.conMap.Load(h.ConID)
	if ok {
		con = v.(*Conn)
	} else {
		if pr.tryRespCloseTo(from, h) {
			return
		}
		if pr.acptCh == nil {
			return
		}
		con = newConn(h.ConID, pr, from, false)
		actual, loaded := pr.conMap.LoadOrStore(h.ConID, con)
		if loaded {
			con = actual.(*Conn)
		} else {
			err := pr.putAcpt(con)
			if err != nil {
				con.close(err)
				return
			}
		}
	}
	if con.handleRecv(from, h, r) != nil {
		pr.tryRespCloseTo(from, h)
	}
}

func listen(pktCon net.PacketConn, isUnique bool) (*Peer, error) {
	pr := &Peer{
		locLnr: pktCon,
	}
	if !isUnique {
		pr.acptCh = make(chan *Conn, 1)
	}
	go func() {
		var h Header
		b := make([]byte, 4096)
		for {
			sz, addr, err := pr.locLnr.ReadFrom(b)
			if err != nil {
				return
			}
			pr.bypassRecv(addr, pr.locLnr, &h, b[:sz])
		}
	}()
	go func() {
		for {
			dur := 90 * time.Second

			pr.conMap.Range(func(_, v interface{}) bool {
				con := v.(*Conn)
				d := con.handleRTO()
				if d > 0 && d < dur {
					dur = d
				}
				return true
			})

			time.Sleep(dur)
		}
	}()
	return pr, nil
}

func ListenPacketConn(pktCon net.PacketConn) (*Peer, error) {
	return listen(pktCon, false)
}

func ListenUDP(udpAddr *net.UDPAddr) (*Peer, error) {
	udpLnr, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	return ListenPacketConn(udpLnr)
}

func Listen(addrStr string) (*Peer, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addrStr)
	if err != nil {
		return nil, err
	}
	return ListenUDP(udpAddr)
}

func (pr *Peer) dial(addr net.Addr) (*Conn, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	con := newConn(id, pr, addr, true)
	pr.conMap.LoadOrStore(id, con)
	return con, nil
}

func (pr *Peer) DialAddr(addr net.Addr) (*Conn, error) {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if pr.wasClosed {
		return nil, pr.opErr("DialAddr", errPeerWasClosed)
	}
	return pr.dial(addr)
}

func (pr *Peer) Dial(addrStr string) (*Conn, error) {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if pr.wasClosed {
		return nil, pr.opErr("Dial", errPeerWasClosed)
	}
	addr, err := net.ResolveUDPAddr(pr.locLnr.LocalAddr().Network(), addrStr)
	if err != nil {
		return nil, err
	}
	return pr.dial(addr)
}

func DialAddr(locPktCon net.PacketConn, rmtAddr net.Addr) (*Conn, error) {
	pr, err := listen(locPktCon, true)
	if err != nil {
		return nil, err
	}
	con, err := pr.DialAddr(rmtAddr)
	if err != nil {
		return nil, err
	}
	return con, err
}

func DialUDP(udpAddr *net.UDPAddr) (*Conn, error) {
	udpLnr, err := net.ListenUDP("udp", NewLocalUDPAddr(0, udpAddr))
	if err != nil {
		return nil, err
	}
	return DialAddr(udpLnr, udpAddr)
}

func Dial(addr string) (*Conn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	return DialUDP(udpAddr)
}

func (pr *Peer) putAcpt(con *Conn) error {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if pr.wasClosed {
		return pr.opErr("putAcpt", errPeerWasClosed)
	}
	pr.acptCh <- con
	return nil
}

func (pr *Peer) AcceptP90() (*Conn, error) {
	return <-pr.acptCh, nil
}

func (pr *Peer) Accept() (net.Conn, error) {
	return pr.AcceptP90()
}

func (pr *Peer) Addr() net.Addr {
	return pr.locLnr.LocalAddr()
}

func (pr *Peer) Close() error {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if pr.wasClosed {
		return pr.opErr("Close", errPeerWasClosed)
	}
	pr.wasClosed = true

	if pr.acptCh != nil {
		close(pr.acptCh)
	}
	pr.locLnr.Close()
	return nil
}

func (pr *Peer) Range(f func(con *Conn) bool) {
	pr.conMap.Range(func(_, v interface{}) bool {
		return f(v.(*Conn))
	})
}
