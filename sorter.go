package p90

import (
	"container/list"
)

type indexedData struct {
	ix  uint64
	val interface{}
}

func newIndexedData(ix uint64, val interface{}) *indexedData {
	switch val.(type) {
	case []byte:
		dataSrc := val.([]byte)
		dataCpy := make([]byte, len(dataSrc))
		copy(dataCpy, dataSrc)
		return &indexedData{ix, dataCpy}
	}
	return &indexedData{ix, val}
}

type sorter struct {
	continuousLastIx uint64
	discretes        *list.List
	onMerge          func([]*indexedData)
}

func newSorter(onMerge func([]*indexedData)) *sorter {
	return &sorter{onMerge: onMerge, discretes: list.New()}
}

func (str *sorter) TryAdd(ix uint64, val interface{}) bool {
	if ix <= str.continuousLastIx {
		return false
	}

	isInst := false
	for it := str.discretes.Front(); it != nil; it = it.Next() {
		data := it.Value.(*indexedData)
		if ix == data.ix {
			return false
		}
		if ix < data.ix {
			str.discretes.InsertBefore(newIndexedData(ix, val), it)
			isInst = true
			break
		}
	}
	if !isInst && (str.discretes.Len() == 0 || ix > str.discretes.Back().Value.(*indexedData).ix) {
		str.discretes.PushBack(newIndexedData(ix, val))
	}

	n := str.discretes.Len()
	if n == 0 {
		return true
	}
	frontIx := str.discretes.Front().Value.(*indexedData).ix
	frontDiff := frontIx - str.continuousLastIx
	if frontDiff > 1 {
		return true
	}
	if frontDiff == 0 {
		panic(frontDiff)
	}

	newFront := str.discretes.Front().Next()
	c := 1
	for ; newFront != nil; newFront = newFront.Next() {
		data := newFront.Value.(*indexedData)
		prevData := newFront.Prev().Value.(*indexedData)
		if data.ix-prevData.ix > 1 {
			break
		}
		c++
	}

	if newFront != nil {
		str.continuousLastIx = newFront.Prev().Value.(*indexedData).ix
	} else {
		str.continuousLastIx = str.discretes.Back().Value.(*indexedData).ix
	}

	var merges []*indexedData
	for it := str.discretes.Front(); it != nil; {
		data := it.Value.(*indexedData)
		if data.ix > str.continuousLastIx {
			break
		}
		if str.onMerge != nil {
			if merges == nil {
				merges = make([]*indexedData, 0, c)
			}
			merges = append(merges, data)
		}
		next := it.Next()
		str.discretes.Remove(it)
		it = next
	}
	if len(merges) > 0 {
		str.onMerge(merges)
	}
	return true
}

func (str *sorter) Has(ix uint64) bool {
	if ix <= str.continuousLastIx {
		return true
	}
	for it := str.discretes.Front(); it != nil; it = it.Next() {
		data := it.Value.(*indexedData)
		if ix == data.ix {
			return true
		}
	}
	return false
}

func (str *sorter) Discretes() *list.List {
	return str.discretes
}

func (str *sorter) ContinuousLastIndex() uint64 {
	return str.continuousLastIx
}

func (str *sorter) TryApplyContinuousLastIndex(ix uint64) bool {
	if ix <= str.continuousLastIx {
		return false
	}
	str.continuousLastIx = ix

	merges := make([]*indexedData, 0)
	for it := str.discretes.Front(); it != nil; {
		data := it.Value.(*indexedData)
		if data.ix > ix && data.ix-ix > 1 {
			break
		}
		if str.onMerge != nil {
			merges = append(merges, data)
		}
		next := it.Next()
		str.discretes.Remove(it)
		it = next
	}
	if len(merges) > 0 {
		str.onMerge(merges)
	}
	return true
}
