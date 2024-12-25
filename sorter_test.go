package p90

import (
	"fmt"
	"testing"
)

func TestSorter(*testing.T) {
	str := newSorter(func(datas []indexer) {
		for i, v := range datas {
			if int(v.Index()) != i+1 {
				panic(fmt.Sprintf("%d != %d", int(v.Index()), i+1))
			}
		}
	})
	if str.TryAdd(indexed(8)) == false {
		panic("str.TryAdd onec")
	}
	if str.TryAdd(indexed(8)) == true {
		panic("str.TryAdd twice")
	}
	str.TryAdd(indexed(3))
	str.TryAdd(indexed(2))
	str.TryAdd(indexed(4))
	str.TryAdd(indexed(1))
}
