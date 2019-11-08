package p90

import (
	"bytes"
	"sync"
)

type packetBufferBase struct {
	err  error
	mtx  sync.Mutex
	cond *sync.Cond
}

func (pbb *packetBufferBase) wBegin() {
	pbb.mtx.Lock()
}

func (pbb *packetBufferBase) wDone() {
	if pbb.cond != nil {
		pbb.mtx.Unlock()
		pbb.cond.Broadcast()
		return
	}
	pbb.mtx.Unlock()
}

func (pbb *packetBufferBase) rBegin() {
	pbb.mtx.Lock()
}

func (pbb *packetBufferBase) rDone() {
	pbb.mtx.Unlock()
}

func (pbb *packetBufferBase) waitForWrite() {
	if pbb.cond == nil {
		pbb.cond = sync.NewCond(&pbb.mtx)
	}
	pbb.cond.Wait()
}

func (pbb *packetBufferBase) Close(err error) {
	pbb.wBegin()
	defer pbb.wDone()

	pbb.err = err
}

type packetBuffer struct {
	packetBufferBase
	pkts [][]byte
}

func (pb *packetBuffer) Put(pkt []byte) {
	pb.wBegin()
	defer pb.wDone()

	dataCpy := make([]byte, len(pkt))
	copy(dataCpy, pkt)
	pb.pkts = append(pb.pkts, dataCpy)
}

func (pb *packetBuffer) Get() ([]byte, error) {
	pb.rBegin()
	defer pb.rDone()

	for {
		if len(pb.pkts) > 0 {
			data := pb.pkts[0]
			pb.pkts = pb.pkts[1:]
			return data, nil
		} else if pb.err != nil {
			return nil, pb.err
		}
		pb.waitForWrite()
	}
}

type streamPacketBuffer struct {
	packetBufferBase
	sor *sorter
	buf *bytes.Buffer
}

func (spb *streamPacketBuffer) Put(ix uint64, pkt []byte) {
	spb.wBegin()
	defer spb.wDone()

	if spb.buf == nil {
		spb.buf = bytes.NewBuffer([]byte{})
		spb.sor = newSorter(nil, func(datas []indexedData) {
			for _, data := range datas {
				spb.buf.Write(data.val.([]byte))
			}
		})
	}
	if spb.sor.TryAdd(ix, pkt) == false {
		panic("duplicated index")
	}
}

func (spb *streamPacketBuffer) Read(data []byte) (int, error) {
	spb.rBegin()
	defer spb.rDone()

	for {
		if spb.buf != nil && spb.buf.Len() > 0 {
			sz, err := spb.buf.Read(data)
			if err != nil {
				panic(err)
			}
			return sz, nil
		} else if spb.err != nil {
			return 0, spb.err
		}
		spb.waitForWrite()
	}
}
