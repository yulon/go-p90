package p90

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type recvData struct {
	data   []byte
	doneCh chan bool
}

// conn state
const (
	csNormal byte = iota
	csClosing
	csClosed
)

type Conn struct {
	id uuid.UUID

	locPr   *Peer
	rmtAddr net.Addr
	mtx     sync.Mutex

	state byte

	handshakeTimeout *atomicDur
	isHandshaked     uintptr

	wTimeout *atomicDur
	rTimeout *atomicDur

	nowRTT  *atomicDur
	nowRTSC uintptr
	minRTT  *atomicDur

	wLastTime *atomicTime
	rLastTime *atomicTime

	isKeepAlived  bool
	isWaitingFine uintptr

	wPktCount uint64

	cWnd        []*sendPktCtx
	cWndSize    int
	cWndSizeMax int
	cWndOnPush  *sync.Cond
	cWndOnPop   *sync.Cond
	cWndErr     error

	sentPktSorter    *sorter
	someoneSentPktID uint64
	someonePktSentTS time.Time

	rPktSorter *sorter

	rDataBuf dataReadBuffer

	wStrmCount uint64
	wStrmMtx   sync.Mutex
	rStrmBuf   streamReadBuffer
}

func newConn(id uuid.UUID, locPr *Peer, rmtAddr net.Addr, isKeepAlived bool) *Conn {
	now := time.Now()
	con := &Conn{
		id:               id,
		locPr:            locPr,
		rmtAddr:          rmtAddr,
		nowRTT:           newAtomicDur(DefaultRTT),
		nowRTSC:          1,
		minRTT:           newAtomicDur(DefaultRTT),
		handshakeTimeout: newAtomicDur(10 * time.Second),
		wTimeout:         newAtomicDur(30 * time.Second),
		rTimeout:         newAtomicDur(90 * time.Second),
		rLastTime:        newAtomicTime(now),
		wLastTime:        newAtomicTime(now),
		isKeepAlived:     isKeepAlived,
		cWndSizeMax:      2048 * 1024,
		sentPktSorter:    newSorter(nil, nil),
		rPktSorter:       newSorter(nil, nil),
	}
	con.cWndOnPush = sync.NewCond(&con.mtx)
	con.cWndOnPop = sync.NewCond(&con.mtx)
	go con.loopHandleWTO()
	return con
}

func (con *Conn) opErr(op string, srcErr error) error {
	if srcErr == nil {
		return nil
	}
	return &net.OpError{Op: op, Net: "p90", Source: con.LocalAddr(), Addr: con.rmtAddr, Err: srcErr}
}

var errWasClosed = errors.New("was closed")
var errClosingByLocal = errors.New("closing by local")
var errClosedByLocal = errors.New("closed by local")
var errClosedByRemote = errors.New("closed by remote")

func (con *Conn) close(reason error) error {
	if con.state == csClosed {
		panic(con.state)
	}
	con.state = csClosed

	con.locPr.conMap.Delete(con.id)
	if con.locPr.acptCh == nil {
		con.locPr.Close()
	}

	con.cWndErr = reason
	con.cWndOnPop.Broadcast()
	con.cWndOnPush.Signal()

	con.rDataBuf.Close(reason)
	con.rStrmBuf.Close(reason)
	return nil
}

func (con *Conn) checkCloseErr() error {
	if con.state == csClosed {
		return errWasClosed
	}
	if con.state == csClosing {
		return errClosingByLocal
	}
	return nil
}

func (con *Conn) Close() error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.state == csClosed {
		return con.opErr("Close", errWasClosed)
	}
	if con.state == csClosing {
		err := con.flush(true)
		if err != nil && err != errClosedByLocal {
			return con.opErr("Close", err)
		}
		return nil
	}
	con.state = csClosing
	err := con.flush(true)
	if err != nil {
		return con.opErr("Close", err)
	}
	err = con.send(ptClose)
	if err != nil {
		return con.opErr("Close", err)
	}
	err = con.flush(true)
	if err != nil {
		return con.opErr("Close", err)
	}
	return con.close(errClosedByLocal)
}

func (con *Conn) handleRecv(from net.Addr, h *Header, r *bytes.Buffer) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.state == csClosed {
		return errWasClosed
	}

	con.rLastTime.Set(time.Now())
	con.rmtAddr = from
	if atomic.LoadUintptr(&con.isWaitingFine) == 1 {
		atomic.StoreUintptr(&con.isWaitingFine, 0)
	}
	if atomic.LoadUintptr(&con.isHandshaked) == 0 {
		atomic.StoreUintptr(&con.isHandshaked, 1)
	}

	if isReliablePT[h.PktType] && h.PktCount > 0 {
		err := con.send(ptReceiveds, h.PktCount)
		if err != nil {
			return err
		}
		if !con.rPktSorter.TryAdd(h.PktCount, nil) {
			return nil
		}
	}

	switch h.PktType {

	case ptData:
		fallthrough
	case ptUnreliableData:
		con.rDataBuf.Put(r.Bytes())

	case ptReceiveds:
		pktIDs := make([]uint64, r.Len()/8)
		binary.Read(r, binary.LittleEndian, &pktIDs)
		err := con.cleanCWnd(pktIDs...)
		if err != nil {
			con.close(err)
			return err
		}

	case ptRequests:

	case ptClose:
		err := con.close(errClosedByRemote)
		if err != nil {
			return err
		}

	case ptStreamData:
		var strmPktIx uint64
		binary.Read(r, binary.LittleEndian, &strmPktIx)
		con.rStrmBuf.Put(strmPktIx, r.Bytes())
	}

	return nil
}

func (con *Conn) Recv() ([]byte, error) {
	pkt, err := con.rDataBuf.Get()
	return pkt, con.opErr("Recv", err)
}

func (con *Conn) Read(b []byte) (int, error) {
	n, err := con.rStrmBuf.Read(b)
	return n, con.opErr("Read", err)
}

const dur3sec = 3 * time.Second

func (con *Conn) handleRTO() time.Duration {
	now := time.Now()

	rto := con.rTimeout.Get()
	ela := now.Sub(con.rLastTime.Get())
	dur := rto - ela
	if dur > 0 {
		if con.isKeepAlived {
			wto := con.wTimeout.Get()
			if dur <= wto && atomic.LoadUintptr(&con.isHandshaked) == 1 && atomic.SwapUintptr(&con.isWaitingFine, 1) == 0 {
				con.mtx.Lock()
				defer con.mtx.Unlock()
				if con.state == csNormal {
					con.send(ptHowAreYou)
				}
				return dur
			}
			return dur - wto
		}
		return dur
	}

	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.state < csClosed {
		con.close(errors.New("receiving timeout"))
	}
	return -1
}

func (con *Conn) write(pkt []byte) error {
	con.wLastTime.Set(time.Now())
	_, err := con.locPr.writeTo(pkt, con.rmtAddr)
	return err
}

type sendPktCtx struct {
	id         uint64
	pkt        []byte
	wFirstTime time.Time
	wLastTime  time.Time
	wCount     uintptr
}

func (con *Conn) send(typ byte, others ...interface{}) error {
	if con.state == csClosed {
		panic(con.state)
	}

	isReliable := isReliablePT[typ]

	if isReliable {
		con.wPktCount++
	}

	h := Header{
		Checksum: MagicNumber,
		ConID:    con.id,
		PktCount: con.wPktCount,
		PktType:  typ,
	}

	pkt := makePacket(&h, others...)

	err := con.write(pkt)
	if err != nil {
		return err
	}

	if !isReliable {
		return nil
	}

	spc := &sendPktCtx{id: h.PktCount, pkt: pkt, wFirstTime: con.wLastTime.Get(), wCount: 1}

	err = con.flush(false)
	if err != nil {
		return err
	}

	if con.sentPktSorter.Has(h.PktCount) {
		if h.PktCount == con.someoneSentPktID {
			con.updateRTI(con.someonePktSentTS.Sub(spc.wFirstTime), spc.wCount)
			con.someoneSentPktID = 0
		}
		return nil
	}

	con.cWnd = append(con.cWnd, spc)
	con.cWndSize += len(spc.pkt)
	con.cWndOnPush.Signal()
	return nil
}

func (con *Conn) isNeedFlushNow(isWantFlushAll bool) bool {
	if isWantFlushAll {
		return con.cWndSize > 0
	}
	return con.cWndSize >= con.cWndSizeMax
}

func (con *Conn) flush(isWantFlushAll bool) error {
	for {
		if con.state == csClosed {
			panic(con.state)
		}
		if !con.isNeedFlushNow(isWantFlushAll) {
			return nil
		}
		con.cWndOnPop.Wait()
		if con.cWndErr != nil {
			return con.cWndErr
		}
	}
}

func (con *Conn) resend(spc *sendPktCtx) error {
	spc.wCount++
	err := con.write(spc.pkt)
	spc.wLastTime = con.wLastTime.Get()
	return err
}

func (con *Conn) handleWTO() (time.Duration, error) {
	if con.cWndSize == 0 {
		return -1, nil
	}

	wTimeout := con.wTimeout
	if atomic.LoadUintptr(&con.isHandshaked) == 0 && con.handshakeTimeout.Get() < con.wTimeout.Get() {
		wTimeout = con.handshakeTimeout
	}

	durMax := con.minRTT.Get()
	durMax += durMax / 8
	dur := durMax

	now := time.Now()
	for _, spc := range con.cWnd {
		if now.Sub(spc.wFirstTime) > wTimeout.Get() {
			if atomic.LoadUintptr(&con.isHandshaked) == 0 {
				return -1, errors.New("handshaking timeout")
			}
			return -1, errors.New("sending a packet timeout")
		}
		left := durMax - now.Sub(spc.wLastTime)
		if left > 0 {
			if left < dur {
				dur = left
			}
			continue
		} else if durMax < dur {
			dur = durMax
		}
		err := con.resend(spc)
		if err != nil {
			return -1, err
		}
	}
	return dur, nil
}

func (con *Conn) loopHandleWTO() {
	con.mtx.Lock()
	defer con.mtx.Unlock()
	for {
		if con.state == csClosed {
			return
		}
		dur, err := con.handleWTO()
		if err != nil {
			con.close(err)
			return
		}
		if dur < 0 {
			con.cWndOnPush.Wait()
			continue
		}
		con.mtx.Unlock()
		time.Sleep(dur)
		con.mtx.Lock()
	}
}

func (con *Conn) updateRTI(rtt time.Duration, wCount uintptr) {
	con.nowRTT.Set(rtt)
	minRTT := con.minRTT.Get()
	if rtt < minRTT {
		con.minRTT.Set(rtt)
	}
	atomic.SwapUintptr(&con.nowRTSC, wCount)
}

func (con *Conn) cleanCWnd(pktIDs ...uint64) error {
	now := time.Now()
	isSent := false
	for _, pktID := range pktIDs {
		if !con.sentPktSorter.TryAdd(pktID, nil) {
			continue
		}
		isSent = true
		isRm := false
		for i, spc := range con.cWnd {
			if spc.id == pktID {
				con.updateRTI(now.Sub(spc.wFirstTime), spc.wCount)

				con.cWndSize -= len(spc.pkt)

				if i == 0 {
					con.cWnd = con.cWnd[1:]
					break
				}
				if i == len(con.cWnd)-1 {
					con.cWnd = con.cWnd[:i]
					break
				}
				cWndRights := make([]*sendPktCtx, len(con.cWnd[i+1:]))
				copy(cWndRights, con.cWnd[i+1:])
				con.cWnd = con.cWnd[:len(con.cWnd)-1]
				copy(con.cWnd[i:], cWndRights)
				break
			}
		}
		if !isRm && con.someoneSentPktID == 0 {
			con.someoneSentPktID = pktID
			con.someonePktSentTS = now
		}
	}
	if isSent {
		con.cWndOnPop.Broadcast()
	}
	return nil
}

func (con *Conn) UnreliableSend(data []byte) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err == nil {
		err = con.send(ptUnreliableData, data)
	}
	return con.opErr("UnreliableSend", err)
}

func (con *Conn) Send(data []byte) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err == nil {
		err = con.send(ptData, data)
	}
	return con.opErr("Send", err)
}

func (con *Conn) Write(b []byte) (int, error) {
	con.wStrmMtx.Lock()
	defer con.wStrmMtx.Unlock()

	sz := 0
	for {
		if len(b) == 0 {
			return sz, nil
		}
		var data []byte
		if len(b) > 1024 {
			data = b[:1024]
			b = b[1024:]
		} else {
			data = b
			b = nil
		}
		con.wStrmCount++

		con.mtx.Lock()

		err := con.checkCloseErr()
		if err != nil {
			con.mtx.Unlock()
			return 0, con.opErr("Write", err)
		}
		err = con.send(ptStreamData, con.wStrmCount, data)
		if err != nil {
			con.mtx.Unlock()
			return 0, con.opErr("Write", err)
		}

		con.mtx.Unlock()

		sz += len(data)
	}
}

func (con *Conn) LocalAddr() net.Addr {
	return con.locPr.Addr()
}

func (con *Conn) RemoteAddr() net.Addr {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.rmtAddr
}

func (con *Conn) ID() uuid.UUID {
	return con.id
}

func (con *Conn) RTT() time.Duration {
	return con.nowRTT.Get()
}

func (con *Conn) PacketLoss() float32 {
	return float32(1) - float32(1)/float32(atomic.LoadUintptr(&con.nowRTSC))
}

func (con *Conn) LastReadTime() time.Time {
	return con.rLastTime.Get()
}

func (con *Conn) LastWriteTime() time.Time {
	return con.wLastTime.Get()
}

func (con *Conn) LastActivityTime() time.Time {
	rt := con.rLastTime.Get()
	st := con.wLastTime.Get()
	if rt.Sub(st) > 0 {
		return rt
	}
	return st
}

func (con *Conn) SetWriteTimeout(dur time.Duration) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return con.opErr("SetWriteTimeout", err)
	}

	con.wTimeout.Set(dur)
	return nil
}

func (con *Conn) SetReadTimeout(dur time.Duration) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return con.opErr("SetReadTimeout", err)
	}

	con.rTimeout.Set(dur)
	return nil
}

func (con *Conn) SetWriteDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return con.opErr("SetWriteDeadline", err)
	}

	con.wTimeout.Set(t.Sub(time.Now()))
	return nil
}

func (con *Conn) SetReadDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return con.opErr("SetReadDeadline", err)
	}

	con.rTimeout.Set(t.Sub(time.Now()))
	return nil
}

func (con *Conn) SetDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return con.opErr("SetDeadline", err)
	}

	dur := t.Sub(time.Now())
	con.wTimeout.Set(dur)
	con.rTimeout.Set(dur)
	return nil
}
