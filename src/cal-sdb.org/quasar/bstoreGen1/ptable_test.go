package bstoreGen1

import (
	"testing"
	"math/rand"
)

func TestPTable_1(t *testing.T) {
	table, _, ptr := mapMeta("test_ptable.db", 128)
	vals := make([]uint64,128)
	for i:=0;i<128;i++ {
		vals[i] = uint64(rand.Int63())
	}
	for i:=0; i<128;i++ {
		table[i] = vals[i]
	}
	ClosePTable(ptr)
	table2, _, _ := mapMeta("test_ptable.db", 128)
	for i:=0; i<128;i++ {
		if table2[i] != vals[i] {
			t.Log("Value mismatch!")
			t.FailNow()
		}
	}
}
