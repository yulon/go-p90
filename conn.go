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

	isHandshaked bool

	idleTimeout atomicDur

	rtt          atomicDur
	rtsc         atomic.Uintptr
	minRTT       atomicDur
	rtts         []time.Duration
	rttSum       time.Duration
	rttAvg       atomicDur
	rttFromPktID uint64

	wLastTime atomicTime
	rLastTime atomicTime

	isKeepAlived bool

	lastPktID atomic.Uint64

	cWndSizeMax atomic.Int64

	gots *sorter

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
		id:           id,
		locPr:        locPr,
		rmtAddr:      rmtAddr,
		idleTimeout:  newAtomicDur(time.Minute),
		rtt:          newAtomicDur(DefaultRTT),
		minRTT:       newAtomicDur(DefaultRTT),
		rttAvg:       newAtomicDur(DefaultRTT),
		rLastTime:    newAtomicTime(now),
		wLastTime:    newAtomicTime(now),
		isKeepAlived: isKeepAlived,
		gots:         newSorter(nil),
	}
	con.rtsc.Store(1)
	con.cWndSizeMax.Store(1024)
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

func (con *Conn) handleRecv(cWnd *list.List, sents *sorter, lstRepMoreTm *int64, rp *recvPkt) (fstRecv bool, sentN int64, err error) {
	now := time.Now().UnixMilli()

	con.rLastTime.Set(time.Now())

	if !con.isHandshaked {
		con.isHandshaked = true
		fstRecv = true
	}

	con.wMtx.Lock()
	con.rmtAddr = rp.from
	con.wMtx.Unlock()

	if isReliablePT[rp.h.Type] && rp.h.ID > 0 {
		curRC := &replyingCtx{
			gotPkt{
				rp.h.ID,
				rp.h.SendCount,
				0,
			},
			now,
		}

		isNew := con.gots.TryAdd(curRC)
		rcs := con.gots.Nondense()
		var body []interface{}

		/*if !isNew {
			for it := rcs.Front(); it != nil; it = it.Next() {
				rc := it.Value.(*replyingCtx)

				if rc.Index() == curRC.Index() {
					rc.updateGotElapsed(now)
					curRC = rc
					break
				}
			}
		}
		for it := rcs.Front(); it != nil && len(body) < 3; it = it.Next() {
			rc := it.Value.(*replyingCtx)

			if rc.Index() == curRC.Index() {
				continue
			}

			rc.updateGotElapsed(now)
			body = append(body, rc)
		}*/

		if isNew {
			if curRC.ID > con.gots.DenseLastIndex() {
				body = append(body, curRC)
			}
		} else {
			repMore := false
			if now-*lstRepMoreTm > int64(con.rtt.Get()/time.Millisecond) {
				*lstRepMoreTm = now
				repMore = true
			}
			if curRC.ID <= con.gots.DenseLastIndex() {
				if repMore {
					for it := rcs.Front(); it != nil && len(body) < 32; it = it.Next() {
						rc := it.Value.(*replyingCtx)
						rc.updateGotElapsed(now)
						body = append(body, rc)
					}
				}
			} else {
				for it := rcs.Back(); it != nil; it = it.Prev() {
					rc := it.Value.(*replyingCtx)
					if rc.Index() != curRC.Index() {
						continue
					}

					rc.updateGotElapsed(now)
					body = append(body, rc)

					if !repMore {
						break
					}

					for jt := it.Prev(); jt != nil && len(body) < 32; jt = jt.Prev() {
						rc := jt.Value.(*replyingCtx)
						rc.updateGotElapsed(now)
						body = append(body, rc)
					}

					for jt := it.Next(); jt != nil && len(body) < 32; jt = jt.Next() {
						rc := jt.Value.(*replyingCtx)
						rc.updateGotElapsed(now)
						body = append(body, rc)
					}
					break
				}
			}
		}

		err = con.sendOnce(ptGots, body...)
		if err != nil {
			return
		}

		if !isNew {
			return
		}
	}

	var rmtGotNondense []gotPkt

	switch rp.h.Type {

	case ptData:
		fallthrough
	case ptUnreliableData:
		con.rDataBuf.Put(rp.body.Bytes())

	case ptGots:
		rmtGotNondense = make([]gotPkt, rp.body.Len()/25)
		binary.Read(rp.body, binary.LittleEndian, &rmtGotNondense)

	case ptClose:
		con.close(errClosedByRemote)
		return

	case ptStreamData:
		var strmPktIx uint64
		binary.Read(rp.body, binary.LittleEndian, &strmPktIx)
		con.rStrmBuf.Put(strmPktIx, rp.body.Bytes())
	}

	sentN = con.sents(cWnd, sents, now, &rp.h.GotDenseLast, rmtGotNondense)

	return
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

func (con *Conn) checkAlive() (time.Duration, error) {
	now := time.Now()

	ito := con.idleTimeout.Get()
	if ito == 0 {
		return -1, nil
	}

	var dur time.Duration

	rt := con.rLastTime.Get()
	if !rt.IsZero() {
		dur = now.Sub(rt)
	}

	wt := con.wLastTime.Get()
	if !wt.IsZero() {
		wDur := now.Sub(wt)
		if wDur < dur {
			dur = wDur
		}
	}

	if dur == 0 {
		return -1, nil
	}

	rem := ito - dur
	if rem <= 0 {
		return -1, errRecvTimeout
	}

	if !con.isHandshaked || con.state.Load() != csNormal || !con.isKeepAlived {
		return rem, nil
	}

	wto := con.wTimeout(ito)
	rem -= wto
	if rem > 0 {
		return rem, nil
	}
	con.sendOnce(ptHowAreYou)
	return -1, nil
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

	h.SendCount++

	ml := con.gots.DenseLast()
	if ml == nil {
		h.GotDenseLast.ID = 0
	} else {
		h.GotDenseLast = ml.(*replyingCtx).gotPkt
	}

	con.wMtx.Lock()
	defer con.wMtx.Unlock()

	return con.locPr.writeTo(makePacket(h, body), con.rmtAddr)
}

type sendingCtx struct {
	h      *packetHeader
	body   []byte
	wTimes []time.Time
}

var packetHeaderSz = binary.Size(packetHeader{})

func (sc *sendingCtx) Size() int {
	return packetHeaderSz + len(sc.body)
}

func (con *Conn) newHeader(typ byte) *packetHeader {
	ph := &packetHeader{
		ConID: con.id,
		Type:  typ,
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
	sc.wTimes = append(sc.wTimes, now)
	_, err := con.sendHeaderAndBody(now, sc.h, sc.body)
	return err
}

func (con *Conn) sendOnce(typ byte, others ...interface{}) error {
	_, err := con.sendHeaderAndBody(time.Now(), con.newHeader(typ), makePacketBody(others...))
	return err
}

var errHandshakingTimeout = errors.New("handshaking timeout")
var errSendingTimeout = errors.New("sending a packet timeout")

func (con *Conn) resends(cWnd *list.List) (time.Duration, error) {
	if cWnd.Len() == 0 {
		return -1, nil
	}

	rttRef := con.rtt.Get()
	if rttRef == 0 {
		rttRef = DefaultRTT
	}
	resendTimeout := rttRef + rttRef/8
	nextDur := resendTimeout

	now := time.Now()
	for it := cWnd.Front(); it != nil; it = it.Next() {
		sc := it.Value.(*sendingCtx)
		if len(sc.wTimes) == 0 {
			panic(sc)
		}
		if now.Sub(sc.wTimes[0]) > con.GetWriteTimeout() {
			if !con.isHandshaked {
				return -1, errHandshakingTimeout
			}
			return -1, errSendingTimeout
		}
		rem := resendTimeout - now.Sub(sc.wTimes[len(sc.wTimes)-1])
		if rem > 0 {
			if rem < nextDur {
				nextDur = rem
			}
			continue
		}
		err := con.send(sc)
		if err != nil {
			return -1, err
		}
	}
	return nextDur, nil
}

func (con *Conn) handleIO() {
	cWnd := list.New()
	//cWndSize := int64(0)

	sents := newSorter(nil)
	var lstRepMoreTm int64

	resendTk := time.NewTicker(time.Hour)
	checkAliveTk := time.NewTicker(time.Hour)

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
		onResend := false
		onCheckAlive := false

		if cWnd.Len() < int(con.cWndSizeMax.Load()) {
			select {
			case sc = <-con.sendCh:
			case rp = <-con.recvCh:
			case stopErr = <-con.stopCh:
				trySendClose = true
			case closedErr = <-con.closedErrCh:
			case <-resendTk.C:
				onResend = true
			case <-checkAliveTk.C:
				onCheckAlive = true
			}
		} else {
			select {
			case rp = <-con.recvCh:
			case stopErr = <-con.stopCh:
				trySendClose = true
			case closedErr = <-con.closedErrCh:
			case <-resendTk.C:
				onResend = true
			case <-checkAliveTk.C:
				onCheckAlive = true
			}
		}

		if closedErr != nil {
			con.closedErrCh <- closedErr
			continue
		}

		if rp != nil {
			_, sentN, err := con.handleRecv(cWnd, sents, &lstRepMoreTm, rp)
			if err != nil {
				con.close(err)
				continue
			}

			ito := con.idleTimeout.Get()
			if ito > 0 {
				if con.isKeepAlived {
					ito -= con.wTimeout(ito)
				}
				checkAliveTk.Reset(ito)
			}

			if sentN == 0 {
				continue
			}
			//cWndSize -= popSz
			if cWnd.Len() > 0 {
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
			if cWnd.Len() > 0 || !con.state.CompareAndSwap(csStoping, csClosing) {
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
			//cWndSize += int64(len(sc.body))
			cWnd.PushBack(sc)
			onResend = true
		}

		if onResend {
			dur, err := con.resends(cWnd)
			if err != nil {
				con.close(err)
				continue
			}
			if dur < 0 {
				resendTk.Reset(time.Hour)
				continue
			}
			resendTk.Reset(dur)
			continue
		}

		if onCheckAlive {
			dur, err := con.checkAlive()
			if err != nil {
				con.close(err)
				continue
			}
			if dur < 0 {
				checkAliveTk.Reset(time.Hour)
				continue
			}
			checkAliveTk.Reset(dur)
			continue
		}
	}
}

func (con *Conn) updateRTInfo(fromPktID uint64, rtt time.Duration, wCount uintptr) {
	if fromPktID <= con.rttFromPktID {
		return
	}
	con.rttFromPktID = fromPktID

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
	con.rttAvg.Set(con.rttSum / time.Duration(len(con.rtts)))
}

func (con *Conn) sents(cWnd *list.List, sents *sorter, now int64, denseLast *gotPkt, nondense []gotPkt) int64 {
	sentN := int64(0)

	var lstGP *gotPkt
	var lstSC *sendingCtx

	if denseLast != nil && denseLast.ID > 0 && sents.TryChangeDenseLast(indexed(denseLast.ID)) {
		for it := cWnd.Front(); it != nil; {
			sc := it.Value.(*sendingCtx)
			if sc.h.ID > denseLast.ID {
				break
			}

			sentN++

			next := it.Next()
			cWnd.Remove(it)
			it = next

			if sc.h.ID == denseLast.ID {
				lstGP = denseLast
				lstSC = sc
				break
			}
		}
	}

	for _, gp := range nondense {
		if !sents.TryAdd(indexed(gp.ID)) {
			continue
		}
		for it := cWnd.Front(); it != nil; it = it.Next() {
			sc := it.Value.(*sendingCtx)
			if sc.h.ID > gp.ID {
				break
			}
			if sc.h.ID != gp.ID {
				continue
			}

			if lstGP == nil || lstGP.ID < gp.ID {
				lstGP = &gotPkt{gp.ID, gp.SendCount, gp.GotElapsed}
				lstSC = sc
			}

			wt := time.Duration(now-sc.wTimes[0].UnixMilli()*2) * time.Millisecond
			if wt > con.GetWriteTimeout() {
				con.SetWriteTimeout(wt)
			}

			sentN++

			cWnd.Remove(it)
			break
		}
	}

	if lstSC != nil && lstGP.SendCount > 0 && int(lstGP.SendCount) <= len(lstSC.wTimes) {
		con.updateRTInfo(lstGP.ID, time.Duration(now-lstSC.wTimes[lstGP.SendCount-1].UnixMilli()-lstGP.GotElapsed)*time.Millisecond, uintptr(lstGP.SendCount))
	}

	return sentN
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

func (con *Conn) SetIdleTimeout(dur time.Duration) {
	con.idleTimeout.Set(dur)
}

func (con *Conn) GetIdleTimeout() time.Duration {
	return con.idleTimeout.Get()
}

func (con *Conn) SetWriteTimeout(dur time.Duration) {
	con.idleTimeout.Set((dur*2 - con.rttAvg.Get()) * 2)
}

func (con *Conn) wTimeout(ito time.Duration) time.Duration {
	return (ito/2 + con.rttAvg.Get()) / 2
}

func (con *Conn) GetWriteTimeout() time.Duration {
	return con.wTimeout(con.idleTimeout.Get())
}

func (con *Conn) SetReadDeadline(t time.Time) error {
	err := con.checkErr()
	if err != nil {
		return con.opErr("SetReadDeadline", err)
	}
	// TODO
	return nil
}

func (con *Conn) SetWriteDeadline(t time.Time) error {
	err := con.checkErr()
	if err != nil {
		return con.opErr("SetWriteDeadline", err)
	}
	// TODO
	return nil
}

func (con *Conn) SetDeadline(t time.Time) error {
	err := con.checkErr()
	if err != nil {
		return con.opErr("SetDeadline", err)
	}
	// TODO
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
