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

func (rbb *readBufferBase) wBegin() bool {
	rbb.mtx.Lock()
	if rbb.err == nil {
		return true
	}
	rbb.mtx.Unlock()
	return false
}

func (rbb *readBufferBase) wDone() {
	if rbb.cond != nil {
		rbb.mtx.Unlock()
		rbb.cond.Broadcast()
		return
	}
	rbb.mtx.Unlock()
}

func (rbb *readBufferBase) rBegin() {
	rbb.mtx.Lock()
}

func (rbb *readBufferBase) rDone() {
	rbb.mtx.Unlock()
}

func (rbb *readBufferBase) waitForWrite() {
	if rbb.cond == nil {
		rbb.cond = sync.NewCond(&rbb.mtx)
	}
	rbb.cond.Wait()
}

func (rbb *readBufferBase) Close(err error) {
	if !rbb.wBegin() {
		return
	}
	defer rbb.wDone()

	rbb.err = err
}

type dataReadBuffer struct {
	readBufferBase
	pkts [][]byte
}

func (drb *dataReadBuffer) Put(pkt []byte) {
	if !drb.wBegin() {
		return
	}
	defer drb.wDone()

	dataCpy := make([]byte, len(pkt))
	copy(dataCpy, pkt)
	drb.pkts = append(drb.pkts, dataCpy)
}

func (drb *dataReadBuffer) Get() ([]byte, error) {
	drb.rBegin()
	defer drb.rDone()

	for {
		if len(drb.pkts) > 0 {
			data := drb.pkts[0]
			drb.pkts = drb.pkts[1:]
			return data, nil
		} else if drb.err != nil {
			return nil, drb.err
		}
		drb.waitForWrite()
	}
}

type streamReadBuffer struct {
	readBufferBase
	sor *sorter
	buf *bytes.Buffer
}

func (srb *streamReadBuffer) Put(ix uint64, pkt []byte) {
	if !srb.wBegin() {
		return
	}
	defer srb.wDone()

	if srb.buf == nil {
		srb.buf = bytes.NewBuffer(nil)
		srb.sor = newSorter(func(datas []indexer) {
			for _, data := range datas {
				srb.buf.Write(data.(*indexedData).data)
			}
		})
	}
	if !srb.sor.TryAdd(newIndexedData(ix, pkt)) {
		panic("duplicated index")
	}
}

func (srb *streamReadBuffer) Read(data []byte) (int, error) {
	srb.rBegin()
	defer srb.rDone()

	for {
		if srb.buf != nil && srb.buf.Len() > 0 {
			sz, err := srb.buf.Read(data)
			if err != nil {
				panic(err)
			}
			return sz, nil
		} else if srb.err != nil {
			return 0, srb.err
		}
		srb.waitForWrite()
	}
}
