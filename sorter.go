package p90

import "sync"

type indexedData struct {
	ix  uint64
	val interface{}
}

func newIndexedData(ix uint64, val interface{}) indexedData {
	switch val.(type) {
	case []byte:
		dataSrc := val.([]byte)
		dataCpy := make([]byte, len(dataSrc))
		copy(dataCpy, dataSrc)
		return indexedData{ix, dataCpy}
	}
	return indexedData{ix, val}
}

type sorter struct {
	continuousLastID uint64
	discretes        []indexedData
	onAppend         func([]indexedData)
	mtx              *sync.Mutex
}

func newSorter(mtx *sync.Mutex, onAppend func([]indexedData)) *sorter {
	return &sorter{onAppend: onAppend, mtx: mtx}
}

func (str *sorter) TryAdd(ix uint64, val interface{}) bool {
	if str.mtx != nil {
		str.mtx.Lock()
		defer str.mtx.Unlock()
	}

	if ix <= str.continuousLastID {
		return false
	}

	isInst := false
	for i, data := range str.discretes {
		if ix == data.ix {
			return false
		}
		if ix < data.ix {
			a := str.discretes[:i]
			b := []indexedData{newIndexedData(ix, val)}
			c := str.discretes[i:]
			str.discretes = make([]indexedData, len(a)+len(b)+len(c))
			copy(str.discretes, a)
			copy(str.discretes[len(a):], b)
			copy(str.discretes[len(a)+len(b):], c)
			isInst = true
			break
		}
	}
	if !isInst {
		if len(str.discretes) == 0 {
			str.discretes = []indexedData{newIndexedData(ix, val)}
		} else if ix > str.discretes[len(str.discretes)-1].ix {
			str.discretes = append(str.discretes, newIndexedData(ix, val))
		}
	}

	sz := len(str.discretes)
	if sz == 0 || str.discretes[0].ix-str.continuousLastID > 1 {
		return true
	}
	i := 1
	for ; i < sz && str.discretes[i].ix-str.discretes[i-1].ix == 1; i++ {
	}
	if str.onAppend != nil {
		str.onAppend(str.discretes[:i])
	}
	str.continuousLastID = str.discretes[i-1].ix
	str.discretes = str.discretes[i:]
	return true
}

func (str *sorter) Has(ix uint64) bool {
	if str.mtx != nil {
		str.mtx.Lock()
		defer str.mtx.Unlock()
	}

	if ix <= str.continuousLastID {
		return true
	}
	for _, data := range str.discretes {
		if ix == data.ix {
			return true
		}
	}
	return false
}

func (str *sorter) ContinuousLastID() uint64 {
	if str.mtx != nil {
		str.mtx.Lock()
		defer str.mtx.Unlock()
	}

	return str.continuousLastID
}
