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

	wTimeout    *atomicDur
	idleTimeout *atomicDur

	rtt               *atomicDur
	rtsc              uintptr
	minRTT            *atomicDur
	resendTimeoutBase time.Duration
	rttIncCnt         byte
	minIncRTT         time.Duration

	wLastTime *atomicTime
	rLastTime *atomicTime

	isKeepAlived  bool
	isWaitingFine uintptr

	lastPktID uint64

	cWnd        []*sendingPkt
	cWndSize    int64
	cWndSizeMax int64
	cWndOnPush  *sync.Cond
	cWndOnPop   *sync.Cond
	cWndErr     error

	sentPkts *sorter
	repPkts  *sorter

	rDataBuf dataReadBuffer

	wStrmCount uint64
	wStrmMtx   sync.Mutex
	rStrmBuf   streamReadBuffer
}

func newConn(id uuid.UUID, locPr *Peer, rmtAddr net.Addr, isKeepAlived bool) *Conn {
	now := time.Now()
	con := &Conn{
		id:                id,
		locPr:             locPr,
		rmtAddr:           rmtAddr,
		rtt:               newAtomicDur(DefaultRTT),
		rtsc:              1,
		minRTT:            newAtomicDur(DefaultRTT),
		resendTimeoutBase: DefaultRTT,
		handshakeTimeout:  newAtomicDur(100 * time.Second),
		wTimeout:          newAtomicDur(10 * time.Second),
		idleTimeout:       newAtomicDur(90 * time.Second),
		isKeepAlived:      isKeepAlived,
		rLastTime:         newAtomicTime(now),
		wLastTime:         newAtomicTime(now),
		cWndSizeMax:       2048 * 1024,
		sentPkts:          newSorter(nil),
		repPkts:           newSorter(nil),
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

func (con *Conn) handleRecv(from net.Addr, h *packetHeader, r *bytes.Buffer) error {
	now := time.Now().UnixMilli()

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

	if isReliablePT[h.Type] && h.ID > 0 {
		rpi := &receivedPacketInfo{
			h.ID,
			h.SendTime,
			h.SendCount,
			now,
		}

		nc := con.repPkts.NonContinuous()
		n := len(nc)
		if n == 0 {
			err := con.send(ptReceiveds, rpi)
			if err != nil {
				return err
			}
		} else {
			if n > 32 {
				n = 32
			}
			n++

			rpis := make([]interface{}, 1, n)
			rpis[0] = rpi
			for _, data := range nc {
				rpis = append(rpis, data.val.(*receivedPacketInfo))
				if len(rpis) >= n {
					break
				}
			}

			err := con.send(ptReceiveds, rpis...)
			if err != nil {
				return err
			}
		}

		if !con.repPkts.TryAdd(h.ID, rpi) {
			return nil
		}
	}

	switch h.Type {

	case ptData:
		fallthrough
	case ptUnreliableData:
		con.rDataBuf.Put(r.Bytes())

	case ptReceiveds:
		rpis := make([]receivedPacketInfo, r.Len()/25)
		binary.Read(r, binary.LittleEndian, &rpis)
		err := con.cleanCWnd(now, h.SendTime, rpis...)
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

var errDataSizeOverflow = errors.New("data size overflow")

func (con *Conn) ReadPacket(b []byte) (int, error) {
	pkt, err := con.rDataBuf.Get()
	if err != nil {
		return 0, con.opErr("ReadPacket", err)
	}
	n := len(pkt)
	if len(b) < n {
		return 0, con.opErr("ReadPacket", errDataSizeOverflow)
	}
	copy(b, pkt)
	return n, nil
}

func (con *Conn) Read(b []byte) (int, error) {
	n, err := con.rStrmBuf.Read(b)
	if err != nil {
		return 0, con.opErr("Read", err)
	}
	return n, nil
}

const dur3sec = 3 * time.Second

var errRecvTimeout = errors.New("receiving timeout")

func (con *Conn) handleRTO() time.Duration {
	now := time.Now()

	if con.isKeepAlived { // TODO: move to handleWTO
		wto := con.wTimeout.Get()
		if atomic.LoadUintptr(&con.isHandshaked) == 1 && atomic.SwapUintptr(&con.isWaitingFine, 1) == 0 {
			con.mtx.Lock()
			defer con.mtx.Unlock()
			if con.state == csNormal {
				con.send(ptHowAreYou)
			}
		}
		return wto + wto/2
	}

	ito := con.idleTimeout.Get()
	if ito == 0 {
		return -1
	}
	rEla := now.Sub(con.rLastTime.Get())
	wEla := now.Sub(con.wLastTime.Get())
	rRem := ito - rEla
	wRem := ito - wEla
	if rRem > 0 {
		if wRem > 0 {
			if wRem < rRem {
				return wRem
			}
			return rRem
		}
	}

	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.state < csClosed {
		con.close(errRecvTimeout)
	}
	return -1
}

var errResendMaximum = errors.New("resend count has been maximum")

func (con *Conn) writePkt(h *packetHeader, now time.Time, body []byte) (int, error) {
	if h.SendCount == 255 {
		return 0, errResendMaximum
	}

	if now.IsZero() {
		now = time.Now()
	}

	con.wLastTime.Set(now)

	h.SendTime = now.UnixMilli()
	h.SendCount++

	return con.locPr.writeTo(makePacket(h, body), con.rmtAddr)
}

type sendingPkt struct {
	h          *packetHeader
	body       []byte
	sz         int
	wFirstTime time.Time
	wLastTime  time.Time
}

func (con *Conn) send(typ byte, others ...interface{}) error {
	if con.state == csClosed {
		panic(con.state)
	}

	isReliable := isReliablePT[typ]

	if isReliable {
		con.lastPktID++
	}

	now := time.Now()

	h := &packetHeader{
		Checksum:  MagicNumber,
		ConID:     con.id,
		ID:        con.lastPktID,
		Type:      typ,
		SendCount: 0,
	}

	body := makePacketBody(others...)

	sz, err := con.writePkt(h, now, body)
	if err != nil {
		return err
	}

	if !isReliable {
		return nil
	}

	sp := &sendingPkt{h: h, body: body, sz: sz, wFirstTime: now, wLastTime: now}

	err = con.flush(false)
	if err != nil {
		return err
	}

	con.cWndSize += int64(sz)

	if con.sentPkts.Has(h.ID) {
		return nil
	}

	con.cWnd = append(con.cWnd, sp)
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

func (con *Conn) resend(sp *sendingPkt) error {
	now := time.Now()
	_, err := con.writePkt(sp.h, now, sp.body)
	if err == nil {
		sp.wLastTime = now
	}
	return err
}

var errHandshakingTimeout = errors.New("handshaking timeout")
var errSendingTimeout = errors.New("sending a packet timeout")

func (con *Conn) handleWTO() (time.Duration, error) {
	if con.cWndSize == 0 {
		return -1, nil
	}

	wTimeout := con.wTimeout
	if atomic.LoadUintptr(&con.isHandshaked) == 0 && con.handshakeTimeout.Get() < con.wTimeout.Get() {
		wTimeout = con.handshakeTimeout
	}

	resendTimeout := con.resendTimeoutBase + con.resendTimeoutBase/8
	dur := resendTimeout

	now := time.Now()
	for _, sp := range con.cWnd {
		if now.Sub(sp.wFirstTime) > wTimeout.Get() {
			if atomic.LoadUintptr(&con.isHandshaked) == 0 {
				return -1, errHandshakingTimeout
			}
			return -1, errSendingTimeout
		}
		rem := resendTimeout - now.Sub(sp.wLastTime)
		if rem > 0 {
			if rem < dur {
				dur = rem
			}
			continue
		}
		err := con.resend(sp)
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

func (con *Conn) updateRTInfo(rtt time.Duration, wCount uintptr) {
	con.rtt.Set(rtt)

	if rtt < con.minRTT.Get() {
		con.minRTT.Set(rtt)
	}

	atomic.SwapUintptr(&con.rtsc, wCount)

	if con.resendTimeoutBase >= rtt {
		con.resendTimeoutBase = rtt
		if con.rttIncCnt > 0 {
			con.rttIncCnt = 0
			con.minIncRTT = 0
		}
		return
	}

	if con.minIncRTT == 0 || rtt < con.minIncRTT {
		con.minIncRTT = rtt
	}
	con.rttIncCnt++
	if con.rttIncCnt < 10 {
		return
	}
	con.rttIncCnt = 0
	con.resendTimeoutBase = con.minIncRTT
	con.minIncRTT = 0
}

func (con *Conn) cleanCWnd(now, repTime int64, rpis ...receivedPacketInfo) error {
	isSent := false
	for _, rpi := range rpis {
		if !con.sentPkts.TryAdd(rpi.ID, nil) {
			continue
		}
		isSent = true
		for i, sp := range con.cWnd {
			if sp.h.ID == rpi.ID {
				con.updateRTInfo(time.Duration(now-rpi.SendTime-(repTime-rpi.RecvTime))*time.Millisecond, uintptr(rpi.SendCount))

				con.cWndSize -= int64(sp.sz)

				if i == 0 {
					con.cWnd = con.cWnd[1:]
					break
				}
				if i == len(con.cWnd)-1 {
					con.cWnd = con.cWnd[:i]
					break
				}
				cWndRights := make([]*sendingPkt, len(con.cWnd[i+1:]))
				copy(cWndRights, con.cWnd[i+1:])
				con.cWnd = con.cWnd[:len(con.cWnd)-1]
				copy(con.cWnd[i:], cWndRights)
				break
			}
		}
	}
	if isSent {
		con.cWndOnPop.Broadcast()
	}
	return nil
}

func (con *Conn) WriteUnreliablePacket(data []byte) (int, error) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return 0, con.opErr("WritePacket", err)
	}
	err = con.send(ptUnreliableData, data)
	if err != nil {
		return 0, con.opErr("WritePacket", err)
	}
	return len(data), nil
}

func (con *Conn) WritePacket(data []byte) (int, error) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return 0, con.opErr("WritePacket", err)
	}
	err = con.send(ptData, data)
	if err != nil {
		return 0, con.opErr("WritePacket", err)
	}
	return len(data), nil
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
	return con.rtt.Get()
}

func (con *Conn) PacketLoss() float32 {
	return float32(1) - float32(1)/float32(atomic.LoadUintptr(&con.rtsc))
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

func (con *Conn) SetReadTimeout(dur time.Duration) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return con.opErr("SetReadTimeout", err)
	}

	// con.rTimeout.Set(dur)
	return nil
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

func (con *Conn) SetIdleTimeout(dur time.Duration) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return con.opErr("SetIdleTimeout", err)
	}

	con.idleTimeout.Set(dur)
	return nil
}

func (con *Conn) SetReadDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return con.opErr("SetReadDeadline", err)
	}

	if t.IsZero() {
		//con.rTimeout.Set(0)
		return nil
	}
	//con.rTimeout.Set(t.Sub(time.Now()))
	return nil
}

func (con *Conn) SetWriteDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return con.opErr("SetWriteDeadline", err)
	}

	if t.IsZero() {
		con.wTimeout.Set(0)
		return nil
	}
	con.wTimeout.Set(t.Sub(time.Now()))
	return nil
}

func (con *Conn) SetDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	err := con.checkCloseErr()
	if err != nil {
		return con.opErr("SetDeadline", err)
	}

	if t.IsZero() {
		con.wTimeout.Set(0)
		//con.rTimeout.Set(0)
		return nil
	}
	dur := t.Sub(time.Now())
	con.wTimeout.Set(dur)
	//con.rTimeout.Set(dur)
	return nil
}

type unreliablePacketer struct {
	*Conn
}

func (upktr *unreliablePacketer) Write(b []byte) (int, error) {
	return upktr.WriteUnreliablePacket(b)
}

func (upktr *unreliablePacketer) Read(b []byte) (int, error) {
	return upktr.ReadPacket(b)
}

func (con *Conn) UnreliablePacketer() net.Conn {
	return &unreliablePacketer{con}
}

type packeter struct {
	*Conn
}

func (pktr *packeter) Write(b []byte) (int, error) {
	return pktr.WritePacket(b)
}

func (pktr *packeter) Read(b []byte) (int, error) {
	return pktr.ReadPacket(b)
}

func (con *Conn) Packeter() net.Conn {
	return &packeter{con}
}
