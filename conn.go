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

type Conn struct {
	mtx sync.Mutex

	id uuid.UUID

	locPr           *Peer
	locPrIsUnique   bool
	isJoinedInLocPr bool

	rmtAddr net.Addr

	nowRTT              time.Duration
	nowSentPktSendCount int
	minRTT              time.Duration

	lastSendTime  time.Time
	lastRecvTime  time.Time
	isWaitingFine uint32

	handshakeTimeout time.Duration
	isHandshaked     bool

	sendTimeout time.Duration
	recvTimeout time.Duration

	sendPktIDCount uint64

	resendPkts                  []*resendPktCtx
	resendPktsSize              int
	resendPktErr                error
	pktTimeoutResenderIsRunning bool

	sentPktSorter    *sorter
	onSentPktCond    *sync.Cond
	WaitSentPktCount uint64

	sentPktIDBaseCache       uint64
	sentPktIDBaseRepeatCount int

	someoneSentPktID uint64
	someonePktSentTS time.Time

	recvPktSorter *sorter

	recvPktBuf packetBuffer

	wStrmPktCount uint64
	wStrmMtx      sync.Mutex

	rStrmBuf streamPacketBuffer

	closeState byte
}

func newConn(id uuid.UUID, locPr *Peer, rmtAddr net.Addr) *Conn {
	con := &Conn{
		id:                          id,
		locPr:                       locPr,
		rmtAddr:                     rmtAddr,
		nowRTT:                      DefaultRTT,
		minRTT:                      DefaultRTT,
		isWaitingFine:               0,
		handshakeTimeout:            10 * time.Second,
		sendTimeout:                 30 * time.Second,
		recvTimeout:                 90 * time.Second,
		sentPktSorter:               newSorter(nil, nil),
		pktTimeoutResenderIsRunning: false,
		recvPktSorter:               newSorter(&sync.Mutex{}, nil),
	}
	return con
}

func (con *Conn) opErr(op string, srcErr error) error {
	return &net.OpError{Op: op, Net: "p90", Source: con.LocalAddr(), Addr: con.rmtAddr, Err: srcErr}
}

func (con *Conn) newOpErr(op, srcErrStr string) error {
	return con.opErr(op, errors.New(srcErrStr))
}

func (con *Conn) newOpErrWasClosed(op string) error {
	return con.newOpErr(op, "was closed")
}

func (con *Conn) closeUS(recvErr error) error {
	if con.closeState > 1 {
		return nil
	}
	if recvErr == nil {
		recvErr = errors.New("closed by local")
		if con.closeState > 0 {
			for con.closeState == 1 {
				con.flushUS()
			}
			return nil
		}
		con.closeState = 1
		err := con.flushUS()
		if err != nil {
			return err
		}
		err = con.sendUS(pktClosed)
		if err != nil {
			return err
		}
		err = con.flushUS()
		if err != nil {
			return err
		}
	}

	con.locPr.conMap.Delete(con.id)
	if con.locPr.acptCh == nil {
		con.locPr.Close()
	}

	con.resendPktErr = recvErr
	if con.WaitSentPktCount > 0 {
		con.onSentPktCond.Broadcast()
	}

	con.recvPktBuf.Close(recvErr)
	con.rStrmBuf.Close(recvErr)

	con.closeState = 2
	return nil
}

func (con *Conn) close(err error) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.closeUS(err)
}

func (con *Conn) Close() error {
	return con.close(nil)
}

func (con *Conn) IsClose() bool {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.closeState > 0
}

func (con *Conn) updateRecvInfo(from net.Addr) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.lastRecvTime = time.Now()
	con.rmtAddr = from

	if con.isHandshaked {
		return
	}
	con.isHandshaked = true

	if con.isJoinedInLocPr {
		return
	}
	con.isJoinedInLocPr = true
	con.lastSendTime = con.lastRecvTime
}

var errClosedByRemote = errors.New("closed by remote")

func (con *Conn) handleRecvPacket(from net.Addr, to net.PacketConn, h *header, r *bytes.Buffer) {
	if isReliableType[h.Type] && h.PktID > 0 {
		con.send(pktReceiveds, h.PktID)
		if !con.recvPktSorter.TryAdd(h.PktID, nil) {
			return
		}
	}

	switch h.Type {

	case pktData:
		fallthrough
	case pktUnreliableData:
		con.recvPktBuf.Put(r.Bytes())

	case pktReceiveds:
		pktIDs := make([]uint64, r.Len()/8)
		binary.Read(r, binary.LittleEndian, &pktIDs)
		con.unresend(pktIDs...)

	case pktRequests:

	case pktClosed:
		con.close(errClosedByRemote)

	case pktHowAreYou:
		atomic.StoreUint32(&con.isWaitingFine, 0)

	case pktStreamData:
		var strmPktIx uint64
		binary.Read(r, binary.LittleEndian, &strmPktIx)
		con.rStrmBuf.Put(strmPktIx, r.Bytes())
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
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.nowRTT
}

func (con *Conn) PacketLoss() float32 {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return float32(1) - float32(1)/float32(con.nowSentPktSendCount)
}

func (con *Conn) LastReadTime() time.Time {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.lastRecvTime
}

func (con *Conn) LastWriteTime() time.Time {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.lastSendTime
}

func (con *Conn) LastActivityTime() time.Time {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.lastRecvTime.Sub(con.lastSendTime) > 0 {
		return con.lastRecvTime
	}
	return con.lastSendTime
}

func (con *Conn) SetWriteTimeout(dur time.Duration) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.sendTimeout = dur
}

func (con *Conn) SetReadTimeout(dur time.Duration) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	con.recvTimeout = dur
}

func (con *Conn) SetReadDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.closeState > 0 {
		return con.newOpErrWasClosed("SetReadDeadline")
	}
	con.recvTimeout = t.Sub(time.Now())
	return nil
}

func (con *Conn) SetWriteDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.closeState > 0 {
		return con.newOpErrWasClosed("SetWriteDeadline")
	}
	con.sendTimeout = t.Sub(time.Now())
	return nil
}

func (con *Conn) SetDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.closeState > 0 {
		return con.newOpErrWasClosed("SetDeadline")
	}
	dur := t.Sub(time.Now())
	con.recvTimeout = dur
	con.sendTimeout = dur
	return nil
}

func (con *Conn) writeUS(b []byte, count int) (int, error) {
	if con.closeState > 1 {
		return 0, con.newOpErrWasClosed("writeUS")
	}

	con.lastSendTime = time.Now()

	if !con.isJoinedInLocPr {
		con.isJoinedInLocPr = true
		con.lastRecvTime = con.lastSendTime
		con.locPr.conMap.Store(con.id, con)
	}

	sz, err := con.locPr.writeTo(b, con.rmtAddr)
	if err != nil {
		con.closeUS(err)
		return 0, err
	}
	return sz, nil
}

func (con *Conn) write(b []byte, count int) (int, error) {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.writeUS(b, count)
}

func (con *Conn) sendUS(typ byte, others ...interface{}) error {
	isReliable := isReliableType[typ]

	if isReliable {
		con.sendPktIDCount++
	}

	h := header{
		MagNum: MagicNumber,
		ConID:  con.id,
		PktID:  con.sendPktIDCount,
		Type:   typ,
	}
	p := makePacket(&h, others...)

	if !isReliable {
		_, err := con.writeUS(p, 1)
		return err
	}

	rspc := &resendPktCtx{
		id: h.PktID,
		p:  p,
	}

	err := con.resendUS(rspc, 1)
	rspc.firstSendTime = rspc.lastSendTime
	if err != nil {
		return err
	}

	for {
		if con.closeState > 1 {
			if con.resendPktErr != nil {
				return con.opErr("sendUS", con.resendPktErr)
			}
			return con.newOpErrWasClosed("sendUS")
		}

		if con.resendPktsSize < resendPktsSizeMax {
			break
		}

		if con.onSentPktCond == nil {
			con.onSentPktCond = sync.NewCond(&con.mtx)
		}
		con.WaitSentPktCount++
		con.onSentPktCond.Wait()
		con.WaitSentPktCount--

		if con.sentPktSorter.Has(rspc.id) {
			if rspc.id == con.someoneSentPktID {
				con.updateRTTAndSPSCUS(con.someonePktSentTS.Sub(rspc.firstSendTime), 1)
				con.someoneSentPktID = 0
			}
			return nil
		}
	}

	con.resendPkts = append(con.resendPkts, rspc)
	con.resendPktsSize += len(p)

	if con.pktTimeoutResenderIsRunning {
		return nil
	}

	go func() {
		for {
			con.mtx.Lock()

			if len(con.resendPkts) == 0 {
				con.pktTimeoutResenderIsRunning = false
				con.mtx.Unlock()
				return
			}

			dur := con.minRTT

			sendTimeout := con.sendTimeout
			if !con.isHandshaked && con.handshakeTimeout < con.sendTimeout {
				sendTimeout = con.handshakeTimeout
			}
			now := time.Now()
			for _, rspc := range con.resendPkts {
				if now.Sub(rspc.firstSendTime) > sendTimeout {
					if !con.isHandshaked {
						con.closeUS(errors.New("handshaking timeout"))
					} else {
						con.closeUS(errors.New("sending a packet timeout"))
					}
					con.mtx.Unlock()
					return
				}
				diff := now.Sub(rspc.lastSendTime)
				if diff < con.minRTT {
					if diff < dur {
						dur = diff
					}
					continue
				}
				err := con.resendUS(rspc, 1)
				if err != nil {
					con.mtx.Unlock()
					return
				}
			}

			con.mtx.Unlock()

			time.Sleep(dur + dur/10)
		}
	}()

	con.pktTimeoutResenderIsRunning = true

	return nil
}

func (con *Conn) send(typ byte, others ...interface{}) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.sendUS(typ, others...)
}

type resendPktCtx struct {
	id            uint64
	p             []byte
	firstSendTime time.Time
	lastSendTime  time.Time
	sendCount     int
}

func (con *Conn) resendUS(rspc *resendPktCtx, count int) error {
	rspc.sendCount++
	_, err := con.writeUS(rspc.p, count)
	rspc.lastSendTime = con.lastSendTime
	return err
}

func (con *Conn) resend(rspc *resendPktCtx, count int) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.resendUS(rspc, count)
}

func (con *Conn) flushUS() error {
	for {
		if con.closeState > 1 {
			if con.resendPktErr != nil {
				return con.resendPktErr
			}
			return con.newOpErrWasClosed("flushUS")
		}
		if len(con.resendPkts) == 0 {
			return nil
		}
		if con.onSentPktCond == nil {
			con.onSentPktCond = sync.NewCond(&con.mtx)
		}
		con.WaitSentPktCount++
		con.onSentPktCond.Wait()
		con.WaitSentPktCount--
	}
}

func (con *Conn) Flush() error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	return con.flushUS()
}

func (con *Conn) UnreliableSend(data []byte) error {
	return con.send(pktUnreliableData, data)
}

func (con *Conn) Send(data []byte) error {
	return con.send(pktData, data)
}

func (con *Conn) updateRTTAndSPSCUS(rtt time.Duration, sentPktSendCount int) {
	con.nowRTT = rtt
	con.nowSentPktSendCount = sentPktSendCount
	if rtt < con.minRTT {
		con.minRTT = rtt
	}
}

func (con *Conn) unresend(pktIDs ...uint64) {
	now := time.Now()

	con.mtx.Lock()

	isSent := false
	for _, pktID := range pktIDs {
		if !con.sentPktSorter.TryAdd(pktID, nil) {
			continue
		}
		isSent = true
		isCached := false
		for i, rspc := range con.resendPkts {
			if rspc.id == pktID {
				nowRTT := now.Sub(rspc.firstSendTime)
				con.updateRTTAndSPSCUS(nowRTT, rspc.sendCount)

				con.resendPktsSize -= len(rspc.p)

				if i == 0 {
					con.resendPkts = con.resendPkts[1:]
					break
				}
				if i == len(con.resendPkts)-1 {
					con.resendPkts = con.resendPkts[:i]
					break
				}
				resendPktRights := make([]*resendPktCtx, len(con.resendPkts[i+1:]))
				copy(resendPktRights, con.resendPkts[i+1:])
				con.resendPkts = con.resendPkts[:len(con.resendPkts)-1]
				copy(con.resendPkts[i:], resendPktRights)

				isCached = true
				break
			}
		}
		if !isCached && con.someoneSentPktID == 0 {
			con.someoneSentPktID = pktID
			con.someonePktSentTS = now
		}
	}
	if isSent {
		if len(con.resendPkts) > 0 {
			if con.sentPktIDBaseCache == con.sentPktSorter.ContinuousLastID() {
				con.sentPktIDBaseRepeatCount++
				if con.sentPktIDBaseRepeatCount > 2 {
					con.sentPktIDBaseRepeatCount = 0

					err := con.resendUS(con.resendPkts[0], 1)
					if err != nil {
						return
					}
				}
			} else {
				con.sentPktIDBaseCache = con.sentPktSorter.ContinuousLastID()
				con.sentPktIDBaseRepeatCount = 0
			}
		}
		if con.WaitSentPktCount > 0 {
			con.mtx.Unlock()
			con.onSentPktCond.Broadcast()
			return
		}
	}

	con.mtx.Unlock()
}

func (con *Conn) Recv() (pkt []byte, err error) {
	pkt, err = con.recvPktBuf.Get()
	if err != nil {
		err = con.opErr("Recv", err)
	}
	return
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
		con.wStrmPktCount++
		err := con.send(pktStreamData, con.wStrmPktCount, data)
		if err != nil {
			return 0, err
		}
		sz += len(data)
	}
}

func (con *Conn) Read(b []byte) (n int, err error) {
	n, err = con.rStrmBuf.Read(b)
	if err != nil {
		err = con.opErr("Read", err)
	}
	return
}
