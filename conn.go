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

	lastSendTime time.Time
	lastRecvTime time.Time

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

	recvPktCache [][]byte
	recvPktErr   error
	recvPktMtx   sync.Mutex
	recvPktCond  *sync.Cond

	wStrmPktCount uint64
	wStrmMtx      sync.Mutex

	rStrmSorter *sorter
	rStrmPkts   [][]byte
	rStrmBuf    *bytes.Buffer
	rStrmErr    error
	rStrmMtx    sync.Mutex
	rStrmCond   *sync.Cond

	closeState byte
}

func newConn(id uuid.UUID, locPr *Peer, rmtAddr net.Addr) *Conn {
	con := &Conn{
		id:                          id,
		locPr:                       locPr,
		rmtAddr:                     rmtAddr,
		nowRTT:                      DefaultRTT,
		minRTT:                      DefaultRTT,
		handshakeTimeout:            10 * time.Second,
		sendTimeout:                 30 * time.Second,
		recvTimeout:                 90 * time.Second,
		sentPktSorter:               newSorter(nil, nil),
		pktTimeoutResenderIsRunning: false,
		recvPktSorter:               newSorter(&sync.Mutex{}, nil),
	}
	return con
}

func (con *Conn) newErr(errStr string) error {
	return errors.New("p90 connection (" + con.LocalAddr().String() + "->" + con.rmtAddr.String() + ") " + errStr)
}

func (con *Conn) newTimeoutErr(onWrite bool) error {
	if !con.isHandshaked {
		return con.newErr("handshaking timeout.")
	}
	if onWrite {
		return con.newErr("sending someone packet timeout.")
	}
	return con.newErr("receiving timeout.")
}

func (con *Conn) newWasClosedErr() error {
	return con.newErr("was closed.")
}

func (con *Conn) closeUS(recvErr error) error {
	if con.closeState > 1 {
		return nil
	}
	if recvErr == nil {
		recvErr = con.newErr("closed by local.")
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

	con.putRecvPkt(nil, recvErr)
	con.putReadStrmPkt(0, nil, recvErr)

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
		con.putRecvPkt(r.Bytes(), nil)

	case pktReceiveds:
		pktIDs := make([]uint64, r.Len()/8)
		binary.Read(r, binary.LittleEndian, &pktIDs)
		con.unresend(pktIDs...)

	case pktRequests:

	case pktClosed:
		con.close(con.newErr("closed by remote."))

	case pktStreamData:
		var strmPktIx uint64
		binary.Read(r, binary.LittleEndian, &strmPktIx)
		con.putReadStrmPkt(strmPktIx, r.Bytes(), nil)
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
		return con.newWasClosedErr()
	}
	con.recvTimeout = t.Sub(time.Now())
	return nil
}

func (con *Conn) SetWriteDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.closeState > 0 {
		return con.newWasClosedErr()
	}
	con.sendTimeout = t.Sub(time.Now())
	return nil
}

func (con *Conn) SetDeadline(t time.Time) error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if con.closeState > 0 {
		return con.newWasClosedErr()
	}
	dur := t.Sub(time.Now())
	con.recvTimeout = dur
	con.sendTimeout = dur
	return nil
}

func (con *Conn) writeUS(b []byte, count int) (int, error) {
	if con.closeState > 1 {
		return 0, con.newWasClosedErr()
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
				return con.resendPktErr
			}
			return con.newWasClosedErr()
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
					con.closeUS(con.newTimeoutErr(true))
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
			return con.newWasClosedErr()
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

func (con *Conn) Pace() error {
	con.mtx.Lock()
	defer con.mtx.Unlock()

	if time.Now().Sub(con.lastSendTime.Add(con.minRTT)) <= 0 {
		return con.newErr("pacing interval too brief.")
	}
	return con.sendUS(pktHeartbeat)
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

func (con *Conn) putRecvPkt(data []byte, err error) {
	con.recvPktMtx.Lock()
	if err == nil {
		dataCpy := make([]byte, len(data))
		copy(dataCpy, data)
		con.recvPktCache = append(con.recvPktCache, dataCpy)
	} else {
		con.recvPktErr = err
	}
	if con.recvPktCond != nil {
		con.recvPktMtx.Unlock()
		con.recvPktCond.Broadcast()
		return
	}
	con.recvPktMtx.Unlock()
}

func (con *Conn) Recv() ([]byte, error) {
	con.recvPktMtx.Lock()
	defer con.recvPktMtx.Unlock()

	for {
		if len(con.recvPktCache) > 0 {
			data := con.recvPktCache[0]
			con.recvPktCache = con.recvPktCache[1:]
			return data, nil
		} else if con.recvPktErr != nil {
			return nil, con.recvPktErr
		}
		if con.recvPktCond == nil {
			con.recvPktCond = sync.NewCond(&con.recvPktMtx)
		}
		con.recvPktCond.Wait()
	}
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

func (con *Conn) putReadStrmPkt(ix uint64, p []byte, err error) {
	con.rStrmMtx.Lock()
	if err == nil {
		if con.rStrmBuf == nil {
			con.rStrmBuf = bytes.NewBuffer([]byte{})
			con.rStrmSorter = newSorter(nil, func(datas []indexedData) {
				for _, data := range datas {
					con.rStrmBuf.Write(data.val.([]byte))
				}
			})
		}
		if con.rStrmSorter.TryAdd(ix, p) == false {
			con.rStrmMtx.Unlock()
			panic("putReadStrmPkt: duplicate index")
		}
	} else {
		con.rStrmErr = err
	}
	if con.rStrmCond != nil {
		con.rStrmMtx.Unlock()
		con.rStrmCond.Broadcast()
		return
	}
	con.rStrmMtx.Unlock()
}

func (con *Conn) Read(b []byte) (int, error) {
	con.rStrmMtx.Lock()
	defer con.rStrmMtx.Unlock()

	for {
		if con.rStrmBuf != nil && con.rStrmBuf.Len() > 0 {
			sz, err := con.rStrmBuf.Read(b)
			if err != nil {
				panic(err)
			}
			return sz, nil
		} else if con.rStrmErr != nil {
			return 0, con.rStrmErr
		}
		if con.rStrmCond == nil {
			con.rStrmCond = sync.NewCond(&con.rStrmMtx)
		}
		con.rStrmCond.Wait()
	}
}
