package p90

import (
	"bytes"
	"container/list"
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
	csNormal uintptr = iota
	csStoping
	csClosing
	csClosed
)

type Conn struct {
	id uuid.UUID

	locPr   *Peer
	rmtAddr net.Addr
	wMtx    sync.Mutex

	state atomic.Uintptr

	handshakeTimeout *atomicDur
	isHandshaked     atomic.Uintptr

	wTimeout    *atomicDur
	idleTimeout *atomicDur

	rtt    *atomicDur
	rtsc   atomic.Uintptr
	minRTT *atomicDur
	rtts   []time.Duration
	rttSum time.Duration
	rttAvg time.Duration

	wLastTime *atomicTime
	rLastTime *atomicTime

	isKeepAlived  atomic.Uintptr
	isWaitingFine atomic.Uintptr

	lastPktID atomic.Uint64

	cWndSizeMax atomic.Int64

	sendCh chan *sendingCtx
	recvCh chan *recvPkt
	stopCh chan error

	rDataBuf dataReadBuffer

	wStrmCount uint64
	wStrmMtx   sync.Mutex
	rStrmBuf   streamReadBuffer

	closedErrCh chan error
}

func newConn(id uuid.UUID, locPr *Peer, rmtAddr net.Addr, isKeepAlived bool) *Conn {
	now := time.Now()
	con := &Conn{
		id:               id,
		locPr:            locPr,
		rmtAddr:          rmtAddr,
		rtt:              newAtomicDur(DefaultRTT),
		minRTT:           newAtomicDur(DefaultRTT),
		rttAvg:           DefaultRTT,
		handshakeTimeout: newAtomicDur(100 * time.Second),
		wTimeout:         newAtomicDur(10 * time.Second),
		idleTimeout:      newAtomicDur(90 * time.Second),
		rLastTime:        newAtomicTime(now),
		wLastTime:        newAtomicTime(now),
	}
	con.rtsc.Store(1)
	if isKeepAlived {
		con.isKeepAlived.Store(1)
	}
	con.cWndSizeMax.Store(2048 * 1024)
	con.sendCh = make(chan *sendingCtx)
	con.recvCh = make(chan *recvPkt)
	con.stopCh = make(chan error)
	con.closedErrCh = make(chan error, 1)
	go con.handleIO()
	return con
}

func (con *Conn) opErr(op string, srcErr error) error {
	if srcErr == nil {
		return nil
	}
	return &net.OpError{Op: op, Net: "p90", Source: con.LocalAddr(), Addr: con.RemoteAddr(), Err: srcErr}
}

var errWasClosed = errors.New("was closed")
var errClosingByLocal = errors.New("closing by local")
var errClosedByLocal = errors.New("closed by local")
var errClosedByRemote = errors.New("closed by remote")

func (con *Conn) close(reason error) error {
	if con.state.Swap(csClosed) == csClosed {
		return con.closedErr()
	}

	con.locPr.conMap.Delete(con.id)
	if con.locPr.acptCh == nil {
		con.locPr.Close()
	}

	con.rDataBuf.Close(reason)
	con.rStrmBuf.Close(reason)

	con.closedErrCh <- reason

	return nil
}

func (con *Conn) stop(reason error) error {
	if !con.state.CompareAndSwap(csNormal, csStoping) {
		return con.closedErr()
	}

	con.stopCh <- reason

	return nil
}

func (con *Conn) closedErr() error {
	err := <-con.closedErrCh
	con.closedErrCh <- err
	return err
}

func (con *Conn) checkErr() error {
	if con.state.Load() > csNormal {
		return con.closedErr()
	}
	return nil
}

func (con *Conn) Close() error {
	err := con.stop(errClosedByLocal)
	if err != nil {
		return con.opErr("Close", err)
	}
	err = con.closedErr()
	if err != errClosedByLocal {
		return con.opErr("Close", err)
	}
	return nil
}

type recvPkt struct {
	from net.Addr
	h    packetHeader
	body *bytes.Buffer
}

func (con *Conn) handleRecv(cWndPkts *list.List, sentPkts *sorter, recvPkts *sorter, rp *recvPkt) (int64, error) {
	now := time.Now().UnixMilli()

	con.rLastTime.Set(time.Now())
	con.isWaitingFine.CompareAndSwap(1, 0)
	con.isHandshaked.CompareAndSwap(0, 1)

	con.wMtx.Lock()
	con.rmtAddr = rp.from
	con.wMtx.Unlock()

	if isReliablePT[rp.h.Type] && rp.h.ID > 0 {
		curRPI := &receivedPacketInfo{
			rp.h.ID,
			rp.h.SendTime,
			rp.h.SendCount,
			now,
		}
		isNew := recvPkts.TryAdd(rp.h.ID, curRPI)

		nc := recvPkts.Discretes()
		ncn := nc.Len()
		if ncn > 30 {
			ncn = 30
		}
		body := make([]interface{}, 2, ncn+2)

		body[0] = recvPkts.ContinuousLastIndex()
		body[1] = curRPI

		for it := nc.Front(); it != nil; it = it.Next() {
			data := it.Value.(*indexedData)
			if len(body) >= ncn {
				break
			}
			rpi := data.val.(*receivedPacketInfo)
			if rpi == curRPI {
				continue
			}
			body = append(body, rpi)
		}

		err := con.sendOnce(ptReceiveds, body...)
		if err != nil {
			return 0, err
		}

		if !isNew {
			return 0, nil
		}
	}

	switch rp.h.Type {

	case ptData:
		fallthrough
	case ptUnreliableData:
		con.rDataBuf.Put(rp.body.Bytes())

	case ptReceiveds:
		var continuousLastID uint64
		binary.Read(rp.body, binary.LittleEndian, &continuousLastID)

		rpis := make([]receivedPacketInfo, rp.body.Len()/25)
		binary.Read(rp.body, binary.LittleEndian, &rpis)

		return con.unsends(cWndPkts, sentPkts, now, rp.h.SendTime, continuousLastID, rpis...), nil

	case ptRequests:

	case ptClose:
		con.close(errClosedByRemote)
		return 0, nil

	case ptStreamData:
		var strmPktIx uint64
		binary.Read(rp.body, binary.LittleEndian, &strmPktIx)
		con.rStrmBuf.Put(strmPktIx, rp.body.Bytes())
	}

	return 0, nil
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

	if con.isKeepAlived.Load() != 0 { // TODO: move to resends()
		wto := con.wTimeout.Get()
		if con.isHandshaked.Load() == 1 && con.isWaitingFine.Swap(1) == 0 {
			if con.state.Load() == csNormal {
				con.sendOnce(ptHowAreYou)
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

	con.close(errRecvTimeout)
	return -1
}

var errResendMaximum = errors.New("send count has been maximum")

func (con *Conn) sendHeaderAndBody(now time.Time, h *packetHeader, body []byte) (int, error) {
	if h.SendCount == 255 {
		return 0, errResendMaximum
	}

	if now.IsZero() {
		now = time.Now()
	}

	con.wLastTime.Set(now)

	h.SendTime = now.UnixMilli()
	h.SendCount++

	con.wMtx.Lock()
	defer con.wMtx.Unlock()

	return con.locPr.writeTo(makePacket(h, body), con.rmtAddr)
}

type sendingCtx struct {
	h          *packetHeader
	body       []byte
	wFirstTime time.Time
	wLastTime  time.Time
}

var packetHeaderSz = binary.Size(packetHeader{})

func (sc *sendingCtx) Size() int {
	return packetHeaderSz + len(sc.body)
}

func (con *Conn) newHeader(typ byte) *packetHeader {
	ph := &packetHeader{
		Checksum:  MagicNumber,
		ConID:     con.id,
		Type:      typ,
		SendCount: 0,
	}
	if isReliablePT[typ] {
		ph.ID = con.lastPktID.Add(1)
	} else {
		ph.ID = con.lastPktID.Load()
	}
	return ph
}

func (con *Conn) newSend(typ byte, others ...interface{}) *sendingCtx {
	return &sendingCtx{h: con.newHeader(typ), body: makePacketBody(others...)}
}

func (con *Conn) addSend(typ byte, others ...interface{}) error {
	con.sendCh <- con.newSend(typ, others...)
	return nil
}

func (con *Conn) send(sc *sendingCtx) error {
	now := time.Now()
	if sc.wFirstTime.IsZero() {
		sc.wFirstTime = now
	}
	sc.wLastTime = now
	_, err := con.sendHeaderAndBody(now, sc.h, sc.body)
	return err
}

func (con *Conn) sendOnce(typ byte, others ...interface{}) error {
	_, err := con.sendHeaderAndBody(time.Now(), con.newHeader(typ), makePacketBody(others...))
	return err
}

var errHandshakingTimeout = errors.New("handshaking timeout")
var errSendingTimeout = errors.New("sending a packet timeout")

func (con *Conn) resends(cWndPkts *list.List) (time.Duration, error) {
	if cWndPkts.Len() == 0 {
		return -1, nil
	}

	wTimeout := con.wTimeout
	if con.isHandshaked.Load() == 0 && con.handshakeTimeout.Get() < con.wTimeout.Get() {
		wTimeout = con.handshakeTimeout
	}

	rttAvg := con.rttAvg
	if rttAvg == 0 {
		rttAvg = DefaultRTT
	}
	resendTimeout := rttAvg + rttAvg/8
	dur := resendTimeout

	now := time.Now()
	for it := cWndPkts.Front(); it != nil; it = it.Next() {
		sc := it.Value.(*sendingCtx)
		if sc.wFirstTime.IsZero() {
			panic(sc)
		}
		if now.Sub(sc.wFirstTime) > wTimeout.Get() {
			if con.isHandshaked.Load() == 0 {
				return -1, errHandshakingTimeout
			}
			return -1, errSendingTimeout
		}
		if sc.wLastTime.IsZero() {
			panic(sc)
		}
		rem := resendTimeout - now.Sub(sc.wLastTime)
		if rem > 0 {
			if rem < dur {
				dur = rem
			}
			continue
		}
		err := con.send(sc)
		if err != nil {
			return -1, err
		}
	}
	return dur, nil
}

func (con *Conn) handleIO() {
	cWndPkts := list.New()
	cWndSize := int64(0)

	sentPkts := newSorter(nil)
	recvPkts := newSorter(nil)

	wakeTk := time.NewTicker(time.Hour)

	var stopErr, closedErr error

	for {
		if con.state.Load() == csClosed {
			select {
			case <-con.sendCh:
			case <-con.recvCh:
			case <-con.stopCh:
			default:
				return
			}
			continue
		}

		var sc *sendingCtx
		var rp *recvPkt
		trySendClose := false

		if cWndSize < con.cWndSizeMax.Load() {
			select {
			case sc = <-con.sendCh:
			case rp = <-con.recvCh:
			case stopErr = <-con.stopCh:
				trySendClose = true
			case closedErr = <-con.closedErrCh:
			case <-wakeTk.C:
			}
		} else {
			select {
			case rp = <-con.recvCh:
			case stopErr = <-con.stopCh:
				trySendClose = true
			case closedErr = <-con.closedErrCh:
			case <-wakeTk.C:
			}
		}

		if closedErr != nil {
			con.closedErrCh <- closedErr
			continue
		}

		if rp != nil {
			popSz, err := con.handleRecv(cWndPkts, sentPkts, recvPkts, rp)
			if err != nil {
				con.close(err)
				continue
			}
			if popSz == 0 {
				continue
			}
			cWndSize -= popSz
			if cWndPkts.Len() > 0 {
				continue
			}
			if stopErr == nil {
				continue
			}
			if con.state.Load() == csClosing {
				con.close(stopErr)
				continue
			}
			trySendClose = true
		}

		if trySendClose {
			if cWndPkts.Len() > 0 || !con.state.CompareAndSwap(csStoping, csClosing) {
				continue
			}
			sc = con.newSend(ptClose)
		}

		if sc != nil {
			if con.state.Load() == csClosing && sc.h.Type != ptClose {
				continue
			}
			err := con.send(sc)
			if err != nil {
				con.close(err)
				continue
			}
			if !isReliablePT[sc.h.Type] {
				continue
			}
			cWndSize += int64(len(sc.body))
			cWndPkts.PushBack(sc)
		}

		dur, err := con.resends(cWndPkts)
		if err != nil {
			con.close(err)
			continue
		}
		if dur < 0 {
			wakeTk.Reset(time.Hour)
			continue
		}

		wakeTk.Reset(dur)
	}
}

func (con *Conn) updateRTInfo(rtt time.Duration, wCount uintptr) {
	con.rtt.Set(rtt)

	if rtt < con.minRTT.Get() {
		con.minRTT.Set(rtt)
	}

	con.rtsc.Store(wCount)

	if len(con.rtts) < 10 {
		con.rtts = append(con.rtts, rtt)
	} else {
		con.rttSum -= con.rtts[0]
		for i := 1; i < 10; i++ {
			con.rtts[i-1] = con.rtts[i]
		}
		con.rtts[9] = rtt
	}
	con.rttSum += rtt
	con.rttAvg = con.rttSum / time.Duration(len(con.rtts))
}

type receivedInfo struct {
	recvRepTime      int64
	repTime          int64
	continuousLastID uint64
	rpis             []receivedPacketInfo
}

func (con *Conn) unsends(cWndPkts *list.List, sentPkts *sorter, recvRepTime int64, repTime int64, continuousLastID uint64, rpis ...receivedPacketInfo) int64 {
	popSz := int64(0)

	var lastRPI *receivedPacketInfo

	for _, rpi := range rpis {
		if !sentPkts.TryAdd(rpi.ID, nil) {
			continue
		}
		for it := cWndPkts.Front(); it != nil; it = it.Next() {
			sc := it.Value.(*sendingCtx)
			if sc.h.ID == rpi.ID {
				if lastRPI == nil || lastRPI.ID < rpi.ID {
					lastRPI = &rpi
				}

				popSz += int64(sc.Size())

				cWndPkts.Remove(it)
				break
			}
		}
	}

	if lastRPI != nil {
		con.updateRTInfo(time.Duration(recvRepTime-lastRPI.SendTime-(repTime-lastRPI.RecvTime))*time.Millisecond, uintptr(lastRPI.SendCount))
	}

	if sentPkts.TryApplyContinuousLastIndex(continuousLastID) {
		for it := cWndPkts.Front(); it != nil; {
			sc := it.Value.(*sendingCtx)
			if sc.h.ID > continuousLastID {
				break
			}

			popSz += int64(sc.Size())

			next := it.Next()
			cWndPkts.Remove(it)
			it = next
		}
	}

	return popSz
}

func (con *Conn) WritePacketOnce(data []byte) (int, error) {
	err := con.checkErr()
	if err != nil {
		return 0, con.opErr("WritePacketOnce", err)
	}
	err = con.addSend(ptUnreliableData, data)
	if err != nil {
		return 0, con.opErr("WritePacketOnce", err)
	}
	return len(data), nil
}

func (con *Conn) WritePacket(data []byte) (int, error) {
	err := con.checkErr()
	if err != nil {
		return 0, con.opErr("WritePacket", err)
	}
	err = con.addSend(ptData, data)
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

		err := con.checkErr()
		if err != nil {
			return 0, con.opErr("Write", err)
		}
		err = con.addSend(ptStreamData, con.wStrmCount, data)
		if err != nil {
			return 0, con.opErr("Write", err)
		}

		sz += len(data)
	}
}

func (con *Conn) LocalAddr() net.Addr {
	return con.locPr.Addr()
}

func (con *Conn) RemoteAddr() net.Addr {
	con.wMtx.Lock()
	defer con.wMtx.Unlock()

	return con.rmtAddr
}

func (con *Conn) ID() uuid.UUID {
	return con.id
}

func (con *Conn) RTT() time.Duration {
	return con.rtt.Get()
}

func (con *Conn) PacketLoss() float32 {
	return float32(1) - float32(1)/float32(con.rtsc.Load())
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
	err := con.checkErr()
	if err != nil {
		return con.opErr("SetReadTimeout", err)
	}

	// con.rTimeout.Set(dur)
	return nil
}

func (con *Conn) SetWriteTimeout(dur time.Duration) error {
	err := con.checkErr()
	if err != nil {
		return con.opErr("SetWriteTimeout", err)
	}

	con.wTimeout.Set(dur)
	return nil
}

func (con *Conn) SetIdleTimeout(dur time.Duration) error {
	err := con.checkErr()
	if err != nil {
		return con.opErr("SetIdleTimeout", err)
	}

	con.idleTimeout.Set(dur)
	return nil
}

func (con *Conn) SetReadDeadline(t time.Time) error {
	err := con.checkErr()
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
	err := con.checkErr()
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
	err := con.checkErr()
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
	return upktr.WritePacketOnce(b)
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
