package p90

import (
	"bytes"
	"sync"
)

type readBufferBase struct {
	err  error
	mtx  sync.Mutex
	cond *sync.Cond
}

func (pbb *readBufferBase) wBegin() {
	pbb.mtx.Lock()
}

func (pbb *readBufferBase) wDone() {
	if pbb.cond != nil {
		pbb.mtx.Unlock()
		pbb.cond.Broadcast()
		return
	}
	pbb.mtx.Unlock()
}

func (pbb *readBufferBase) rBegin() {
	pbb.mtx.Lock()
}

func (pbb *readBufferBase) rDone() {
	pbb.mtx.Unlock()
}

func (pbb *readBufferBase) waitForWrite() {
	if pbb.cond == nil {
		pbb.cond = sync.NewCond(&pbb.mtx)
	}
	pbb.cond.Wait()
}

func (pbb *readBufferBase) Close(err error) {
	pbb.wBegin()
	defer pbb.wDone()

	pbb.err = err
}

type dataReadBuffer struct {
	readBufferBase
	pkts [][]byte
}

func (pb *dataReadBuffer) Put(pkt []byte) {
	pb.wBegin()
	defer pb.wDone()

	dataCpy := make([]byte, len(pkt))
	copy(dataCpy, pkt)
	pb.pkts = append(pb.pkts, dataCpy)
}

func (pb *dataReadBuffer) Get() ([]byte, error) {
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

type streamReadBuffer struct {
	readBufferBase
	sor *sorter
	buf *bytes.Buffer
}

func (spb *streamReadBuffer) Put(ix uint64, pkt []byte) {
	spb.wBegin()
	defer spb.wDone()

	if spb.buf == nil {
		spb.buf = bytes.NewBuffer(nil)
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

func (spb *streamReadBuffer) Read(data []byte) (int, error) {
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
