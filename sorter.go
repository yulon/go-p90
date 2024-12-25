package p90

import (
	"container/list"
)

type indexer interface {
	Index() uint64
}

type sorter struct {
	denseLast indexer
	nondense  *list.List
	onDense   func([]indexer)
}

func newSorter(onDense func([]indexer)) *sorter {
	return &sorter{nondense: list.New(), onDense: onDense}
}

func (s *sorter) TryAdd(val indexer) bool {
	if s.denseLast != nil && val.Index() <= s.denseLast.Index() {
		return false
	}

	isInst := false
	for it := s.nondense.Front(); it != nil; it = it.Next() {
		added := it.Value.(indexer)
		if val.Index() == added.Index() {
			return false
		}
		if val.Index() < added.Index() {
			s.nondense.InsertBefore(val, it)
			isInst = true
			break
		}
	}
	if !isInst && (s.nondense.Len() == 0 || val.Index() > s.nondense.Back().Value.(indexer).Index()) {
		s.nondense.PushBack(val)
	}

	if s.nondense.Len() == 0 {
		return true
	}

	var dense []indexer
	for {
		it := s.nondense.Front()
		if it == nil {
			break
		}
		cur := it.Value.(indexer)

		var mbi uint64
		if s.denseLast != nil {
			mbi = s.denseLast.Index()
		} else {
			mbi = 0
		}

		if cur.Index()-mbi > 1 {
			break
		}

		s.nondense.Remove(it)
		s.denseLast = cur
		if s.onDense != nil {
			dense = append(dense, cur)
		}
	}
	if len(dense) > 0 {
		s.onDense(dense)
	}
	return true
}

func (s *sorter) Has(ix uint64) bool {
	if s.denseLast != nil && ix <= s.denseLast.Index() {
		return true
	}
	for it := s.nondense.Front(); it != nil; it = it.Next() {
		added := it.Value.(indexer)
		if ix == added.Index() {
			return true
		}
	}
	return false
}

func (s *sorter) Nondense() *list.List {
	return s.nondense
}

func (s *sorter) DenseLast() indexer {
	return s.denseLast
}

func (s *sorter) DenseLastIndex() uint64 {
	if s.denseLast == nil {
		return 0
	}
	return s.denseLast.Index()
}

func (s *sorter) TryChangeDenseLast(ixr indexer) bool {
	if s.denseLast != nil && ixr.Index() <= s.denseLast.Index() {
		return false
	}
	s.denseLast = ixr

	var dense []indexer
	for it := s.nondense.Front(); it != nil; {
		added := it.Value.(indexer)
		if added.Index() > ixr.Index() {
			if added.Index()-ixr.Index() > 1 {
				break
			}
			s.denseLast = added
		}
		if s.onDense != nil {
			dense = append(dense, added)
		}
		next := it.Next()
		s.nondense.Remove(it)
		it = next
	}
	if len(dense) > 0 {
		s.onDense(dense)
	}
	return true
}

type indexed uint64

func (ix indexed) Index() uint64 {
	return uint64(ix)
}

type indexedData struct {
	ix   uint64
	data []byte
}

func (id *indexedData) Index() uint64 {
	return id.ix
}

func newIndexedData(ix uint64, data []byte) indexer {
	dataCpy := make([]byte, len(data))
	copy(dataCpy, data)
	return &indexedData{ix, dataCpy}
}
