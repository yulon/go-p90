package p90

import (
	"fmt"
	"testing"
)

func TestSorter(*testing.T) {
	str := newSorter(func(datas []*indexedData) {
		for i, v := range datas {
			if int(v.ix) != i+1 {
				panic(fmt.Sprintf("%d != %d", int(v.ix), i+1))
			}
		}
	})
	if str.TryAdd(8, nil) == false {
		panic("str.TryAdd onec")
	}
	if str.TryAdd(8, nil) == true {
		panic("str.TryAdd twice")
	}
	str.TryAdd(3, nil)
	str.TryAdd(2, nil)
	str.TryAdd(4, nil)
	str.TryAdd(1, nil)
}
