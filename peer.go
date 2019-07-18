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

func (pr *Peer) newErr(errStr string) error {
	return errors.New("p90 peer (" + pr.Addr().String() + ") " + errStr)
}

func (pr *Peer) newWasClosedErr() error {
	return pr.newErr("was closed.")
}

func (pr *Peer) writeTo(b []byte, addr net.Addr) (int, error) {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if pr.wasClosed {
		return 0, pr.newWasClosedErr()
	}
	return pr.locLnr.WriteTo(b, addr)
}

func (pr *Peer) bypassRecvPacket(from net.Addr, to net.PacketConn, h *header, p []byte) {
	r := bytes.NewBuffer(p)
	err := binary.Read(r, binary.LittleEndian, h)

	if err != nil || h.MagNum != MagicNumber || int(h.Type) >= len(isReliableType) {
		return
	}

	var con *Conn
	v, ok := pr.conMap.Load(h.ConID)
	if ok {
		con = v.(*Conn)
		con.updateRecvInfo(from)
	} else {
		if pr.acptCh == nil {
			return
		}

		con = newConn(h.ConID, pr, from)
		con.updateRecvInfo(from)

		actual, loaded := pr.conMap.LoadOrStore(h.ConID, con)
		if loaded {
			con = actual.(*Conn)
			con.updateRecvInfo(from)
		} else {
			err = pr.putAcpt(con)
			if err != nil {
				con.closeUS(err)
				return
			}
		}
	}
	con.handleRecvPacket(from, to, h, r)
	return
}

func listen(pktCon net.PacketConn, isUnique bool) (*Peer, error) {
	pr := &Peer{
		locLnr: pktCon,
	}
	if !isUnique {
		pr.acptCh = make(chan *Conn, 1)
	}
	go func() {
		var h header
		b := make([]byte, 1280)
		for {
			sz, addr, err := pktCon.ReadFrom(b)
			if err != nil {
				return
			}
			pr.bypassRecvPacket(addr, pktCon, &h, b[:sz])
		}
	}()
	go func() {
		for {
			dur := 90 * time.Second

			now := time.Now()
			pr.conMap.Range(func(_, v interface{}) bool {
				con := v.(*Conn)
				con.mtx.Lock()
				defer con.mtx.Unlock()

				diff := now.Sub(con.lastRecvTime)
				if diff > con.recvTimeout {
					con.closeUS(con.newTimeoutErr(false))
					return true
				}
				if diff < dur {
					dur = diff
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

func (pr *Peer) dialAddrUS(addr net.Addr) (*Conn, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	return newConn(id, pr, addr), nil
}

func (pr *Peer) DialAddr(addr net.Addr) (*Conn, error) {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if pr.wasClosed {
		return nil, pr.newWasClosedErr()
	}
	return pr.dialAddrUS(addr)
}

func (pr *Peer) Dial(addrStr string) (*Conn, error) {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if pr.wasClosed {
		return nil, pr.newWasClosedErr()
	}
	addr, err := ResolveAddr(pr.locLnr.LocalAddr().Network(), addrStr)
	if err != nil {
		return nil, err
	}
	return pr.dialAddrUS(addr)
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
		return pr.newWasClosedErr()
	}
	pr.acptCh <- con
	return nil
}

func (pr *Peer) AcceptGatling() (*Conn, error) {
	con, ok := <-pr.acptCh
	if !ok || con.IsClose() {
		return nil, pr.newWasClosedErr()
	}
	return con, nil
}

func (pr *Peer) Accept() (net.Conn, error) {
	return pr.AcceptGatling()
}

func (pr *Peer) Addr() net.Addr {
	return pr.locLnr.LocalAddr()
}

func (pr *Peer) Close() error {
	pr.mtx.Lock()
	defer pr.mtx.Unlock()

	if pr.wasClosed {
		return pr.newWasClosedErr()
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