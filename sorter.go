package p90

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
	continuousLastIx uint64
	discretes        []indexedData
	onAppend         func([]indexedData)
}

func newSorter(onAppend func([]indexedData)) *sorter {
	return &sorter{onAppend: onAppend}
}

func (str *sorter) TryAdd(ix uint64, val interface{}) bool {
	if ix <= str.continuousLastIx {
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
	if sz == 0 || str.discretes[0].ix-str.continuousLastIx > 1 {
		return true
	}
	i := 1
	for ; i < sz && str.discretes[i].ix-str.discretes[i-1].ix == 1; i++ {
	}
	if str.onAppend != nil {
		str.onAppend(str.discretes[:i])
	}
	str.continuousLastIx = str.discretes[i-1].ix
	str.discretes = str.discretes[i:]
	return true
}

func (str *sorter) Has(ix uint64) bool {
	if ix <= str.continuousLastIx {
		return true
	}
	for _, data := range str.discretes {
		if ix == data.ix {
			return true
		}
	}
	return false
}

func (str *sorter) Discretes() []indexedData {
	return str.discretes
}

func (str *sorter) ContinuousLastIndex() uint64 {
	return str.continuousLastIx
}

func (str *sorter) TryApplyContinuousLastIndex(ix uint64) bool {
	if ix <= str.continuousLastIx {
		return false
	}
	for i, data := range str.discretes {
		if data.ix > ix && data.ix-ix > 1 {
			if str.onAppend != nil && i > 0 {
				str.onAppend(str.discretes[:i])
			}
			str.discretes = str.discretes[:i]
			break
		}
	}
	str.continuousLastIx = ix
	return true
}
