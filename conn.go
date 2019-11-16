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
	csFlushing
	csClosing
	csClosed
)

type Conn struct {
	id uuid.UUID

	locPr      *Peer
	rmtAddr    net.Addr
	rmtAddrMtx sync.Mutex

	handshakeTimeout *atomicDur
	isHandshaked     bool

	wTimeout     *atomicDur
	wTimeoutTick <-chan time.Time

	rTimeout     *atomicDur
	rTimeoutTick <-chan time.Time

	nowRTT              *atomicDur
	nowSentPktSendCount uintptr
	minRTT              *atomicDur

	wLastTime *atomicTime
	rLastTime *atomicTime

	wPktCount uint64

	cWnd        []*sendPktCtx
	cWndSize    int
	cWndSizeMax int

	sentPktSorter *sorter

	rPktSorter *sorter

	rDataBuf dataReadBuffer

	wStrmCount uint64
	wStrmMtx   sync.Mutex
	rStrmBuf   streamReadBuffer

	wBufOut chan *bytes.Buffer
	rBufOut chan *bytes.Buffer
	opIn    chan interface{}
	errOut  chan error
}

func newConn(id uuid.UUID, locPr *Peer, rmtAddr net.Addr) *Conn {
	now := time.Now()

	con := &Conn{
		id:               id,
		locPr:            locPr,
		rmtAddr:          rmtAddr,
		nowRTT:           newAtomicDur(DefaultRTT),
		minRTT:           newAtomicDur(DefaultRTT),
		handshakeTimeout: newAtomicDur(10 * time.Second),
		wTimeout:         newAtomicDur(30 * time.Second),
		rTimeout:         newAtomicDur(60 * time.Second),
		rLastTime:        newAtomicTime(now),
		wLastTime:        newAtomicTime(now),
		cWndSizeMax:      2048 * 1024,
		sentPktSorter:    newSorter(nil, nil),
		rPktSorter:       newSorter(nil, nil),
		rBufOut:          make(chan *bytes.Buffer, 1),
		wBufOut:          make(chan *bytes.Buffer, 1),
		opIn:             make(chan interface{}, 1),
		errOut:           make(chan error, 1),
	}

	con.rBufOut <- bytes.NewBuffer(nil)
	con.wBufOut <- bytes.NewBuffer(nil)

	go con.handleOps()

	return con
}

func (con *Conn) opErr(op string, srcErr error) error {
	if srcErr == nil {
		return nil
	}
	return &net.OpError{Op: op, Net: "p90", Source: con.LocalAddr(), Addr: con.rmtAddr, Err: srcErr}
}

func (con *Conn) setRemoteAddr(addr net.Addr) {
	con.rmtAddrMtx.Lock()
	defer con.rmtAddrMtx.Unlock()

	con.rmtAddr = addr
}

func (con *Conn) getBuf(out chan *bytes.Buffer) (*bytes.Buffer, error) {
	select {
	case buf := <-out:
		return buf, nil
	case err := <-con.errOut:
		con.errOut <- err
		return nil, err
	}
}

func (con *Conn) putBuf(out chan *bytes.Buffer, buf *bytes.Buffer) {
	buf.Reset()
	out <- buf
}

func (con *Conn) getRBuf() (*bytes.Buffer, error) {
	return con.getBuf(con.rBufOut)
}

func (con *Conn) putRBuf(buf *bytes.Buffer) {
	con.putBuf(con.rBufOut, buf)
}

func (con *Conn) getWBuf() (*bytes.Buffer, error) {
	return con.getBuf(con.wBufOut)
}

func (con *Conn) putWBuf(buf *bytes.Buffer) {
	con.putBuf(con.wBufOut, buf)
}

var errWasClosed = errors.New("was closed")

var errClosedByLocal = errors.New("closed by local")

func (con *Conn) close(reason error) {
	con.locPr.conMap.Delete(con.id)
	if con.locPr.acptCh == nil {
		con.locPr.Close()
	}
	con.errOut <- reason
	con.rDataBuf.Close(reason)
	con.rStrmBuf.Close(reason)
}

type closeInfo struct {
	wBuf *bytes.Buffer
}

func (con *Conn) handleClose(ci closeInfo) error {
	err := con.flush()
	if err != nil {
		return err
	}
	err = con.send(ptClose)
	if err != nil {
		return err
	}
	err = con.flush()
	if err != nil {
		return err
	}
	return errClosedByLocal
}

func (con *Conn) addCloseOp() error {
	wBuf, err := con.getWBuf()
	if err != nil {
		return err
	}
	con.opIn <- closeInfo{
		wBuf: wBuf,
	}
	return nil
}

func (con *Conn) Close() error {
	err := con.addCloseOp()
	if err == nil {
		err = <-con.errOut
		con.errOut <- err
	}
	if err != errClosedByLocal {
		return err
	}
	return nil
}

type recvInfo struct {
	from     net.Addr
	h        header
	rDataBuf *bytes.Buffer
}

var errClosedByRemote = errors.New("closed by remote")

func (con *Conn) handleRecv(ri recvInfo) error {
	con.rLastTime.Set(time.Now())

	con.setRemoteAddr(ri.from)

	if !con.isHandshaked {
		con.isHandshaked = true
	}

	if con.rTimeoutTick == nil {
		con.rTimeoutTick = time.Tick(con.rTimeout.Get())
	}

	if isReliablePT[ri.h.PktType] && ri.h.PktCount > 0 {
		err := con.send(ptReceiveds, ri.h.PktCount)
		if err != nil {
			return err
		}
		if !con.rPktSorter.TryAdd(ri.h.PktCount, nil) {
			con.putRBuf(ri.rDataBuf)
			return nil
		}
	}

	switch ri.h.PktType {

	case ptData:
		fallthrough
	case ptUnreliableData:
		con.rDataBuf.Put(ri.rDataBuf.Bytes())

	case ptReceiveds:
		pktIDs := make([]uint64, ri.rDataBuf.Len()/8)
		binary.Read(ri.rDataBuf, binary.LittleEndian, &pktIDs)
		err := con.cleanCWnd(pktIDs...)
		if err != nil {
			return err
		}

	case ptRequests:

	case ptClose:
		return errClosedByRemote

	case ptStreamData:
		var strmPktIx uint64
		binary.Read(ri.rDataBuf, binary.LittleEndian, &strmPktIx)
		con.rStrmBuf.Put(strmPktIx, ri.rDataBuf.Bytes())
	}

	con.putRBuf(ri.rDataBuf)
	return nil
}

func (con *Conn) addRecvOp(from net.Addr, h *header, body []byte) error {
	rDataBuf, err := con.getRBuf()
	if err != nil {
		return err
	}
	rDataBuf.Write(body)
	con.opIn <- recvInfo{
		from:     from,
		h:        *h,
		rDataBuf: rDataBuf,
	}
	return nil
}

func (con *Conn) Recv() (pkt []byte, err error) {
	pkt, err = con.rDataBuf.Get()
	if err != nil {
		err = con.opErr("Recv", err)
	}
	return
}

func (con *Conn) Read(b []byte) (n int, err error) {
	n, err = con.rStrmBuf.Read(b)
	if err != nil {
		err = con.opErr("Read", err)
	}
	return
}

func (con *Conn) handleRTimeout() error {
	rTimeout := con.rTimeout.Get()
	now := time.Now()
	ela := now.Sub(con.rLastTime.Get())
	if ela < rTimeout {
		con.rTimeoutTick = time.Tick(rTimeout - ela)
		return nil
	}
	con.send(ptHowAreYou)
	con.rTimeoutTick = nil
	return nil
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
	isReliable := isReliablePT[typ]

	if isReliable {
		con.wPktCount++
	}

	h := header{
		MagNum:   MagicNumber,
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

	for con.cWndSize >= con.cWndSizeMax {
		err := con.handleOp()
		if err != nil {
			return err
		}
	}

	ctx := &sendPktCtx{id: h.PktCount, pkt: pkt, wFirstTime: con.wLastTime.Get()}

	con.cWnd = append(con.cWnd, ctx)
	con.cWndSize += len(ctx.pkt)

	if con.wTimeoutTick != nil {
		return nil
	}
	con.wTimeoutTick = time.Tick(DefaultRTT)
	return nil
}

func (con *Conn) flush() error {
	for con.cWndSize > 0 {
		err := con.handleOp()
		if err != nil {
			return err
		}
	}
	return nil
}

func (con *Conn) resend(spc *sendPktCtx) error {
	spc.wCount++
	err := con.write(spc.pkt)
	spc.wLastTime = con.wLastTime.Get()
	return err
}

func (con *Conn) handleWTimeout() error {
	if con.cWndSize == 0 {
		con.wTimeoutTick = nil
		return nil
	}

	wTimeout := con.wTimeout
	if !con.isHandshaked && con.handshakeTimeout.Get() < con.wTimeout.Get() {
		wTimeout = con.handshakeTimeout
	}

	durMax := con.minRTT.Get()
	durMax += durMax / 4
	dur := durMax
	now := time.Now()

	for _, spc := range con.cWnd {
		if now.Sub(spc.wFirstTime) > wTimeout.Get() {
			con.wTimeoutTick = nil
			if !con.isHandshaked {
				return errors.New("handshaking timeout")
			}
			return errors.New("sending a packet timeout")
		}
		ela := now.Sub(spc.wLastTime)
		if ela < durMax {
			rem := durMax - ela
			if rem < dur {
				dur = rem
			}
			continue
		}
		err := con.resend(spc)
		if err != nil {
			con.wTimeoutTick = nil
			return err
		}
	}
	con.wTimeoutTick = time.Tick(dur + dur/10)
	return nil
}

func (con *Conn) cleanCWnd(pktIDs ...uint64) error {
	now := time.Now()
	isSent := false
	for _, pktID := range pktIDs {
		if !con.sentPktSorter.TryAdd(pktID, nil) {
			continue
		}
		isSent = true
		for i, spc := range con.cWnd {
			if spc.id == pktID {
				nowRTT := now.Sub(spc.wFirstTime)
				con.nowRTT.Set(nowRTT)

				minRTT := con.minRTT.Get()
				if nowRTT < minRTT {
					con.minRTT.Set(nowRTT)
				}

				atomic.SwapUintptr(&con.nowSentPktSendCount, spc.wCount)

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
	}
	if !isSent {
		return nil
	}
	if con.cWndSize == 0 {
		con.wTimeoutTick = nil
		return nil
	}
	return nil
}

type sendInfo struct {
	typ byte
	buf *bytes.Buffer
}

func (con *Conn) handleSend(inf sendInfo) error {
	err := con.send(inf.typ, inf.buf.Bytes())
	if err == nil {
		con.putWBuf(inf.buf)
	}
	return err
}

func (con *Conn) addSendOp(typ byte, others ...interface{}) error {
	wBuf, err := con.getWBuf()
	if err != nil {
		return err
	}
	writeData(wBuf, others...)
	con.opIn <- sendInfo{typ: typ, buf: wBuf}
	return nil
}

func (con *Conn) UnreliableSend(data []byte) error {
	return con.opErr("UnreliableSend", con.addSendOp(ptUnreliableData, data))
}

func (con *Conn) Send(data []byte) error {
	return con.opErr("Send", con.addSendOp(ptData, data))
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
		err := con.addSendOp(ptStreamData, con.wStrmCount, data)
		if err != nil {
			return 0, con.opErr("Write", err)
		}
		sz += len(data)
	}
}

func (con *Conn) handleOp() error {
	select {
	case op := <-con.opIn:
		switch op.(type) {
		case recvInfo:
			return con.handleRecv(op.(recvInfo))
		case sendInfo:
			return con.handleSend(op.(sendInfo))
		case closeInfo:
			return con.handleClose(op.(closeInfo))
		default:
			panic(op)
		}
	case <-getTick(con.wTimeoutTick):
		return con.handleWTimeout()
	case <-getTick(con.rTimeoutTick):
		return con.handleRTimeout()
	}
}

func (con *Conn) handleOps() {
	for {
		err := con.handleOp()
		if err != nil {
			con.close(err)
			return
		}
	}
}

func (con *Conn) LocalAddr() net.Addr {
	return con.locPr.Addr()
}

func (con *Conn) RemoteAddr() net.Addr {
	con.rmtAddrMtx.Lock()
	defer con.rmtAddrMtx.Unlock()

	return con.rmtAddr
}

func (con *Conn) ID() uuid.UUID {
	return con.id
}

func (con *Conn) RTT() time.Duration {
	return con.nowRTT.Get()
}

func (con *Conn) PacketLoss() float32 {
	return float32(1) - float32(1)/float32(atomic.LoadUintptr(&con.nowSentPktSendCount))
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

func (con *Conn) SetWriteTimeout(dur time.Duration) {
	con.wTimeout.Set(dur)
}

func (con *Conn) SetReadTimeout(dur time.Duration) {
	con.rTimeout.Set(dur)
}

func (con *Conn) SetReadDeadline(t time.Time) error {
	con.rTimeout.Set(t.Sub(time.Now()))
	return nil
}

func (con *Conn) SetWriteDeadline(t time.Time) error {
	con.wTimeout.Set(t.Sub(time.Now()))
	return nil
}

func (con *Conn) SetDeadline(t time.Time) error {
	dur := t.Sub(time.Now())
	con.rTimeout.Set(dur)
	con.wTimeout.Set(dur)
	return nil
}
