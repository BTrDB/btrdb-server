package bstore

import (
	"testing"
	"math/rand"
	"time"
	)

func init() {
	sd := time.Now().Unix()
	log.Debug(">>>> USING %v AS SEED <<<<<", sd)
	rand.Seed(sd)
}

func Test_DeCompose(t *testing.T){
	for i := 0 ; i < 10000000; i++ {
		x := rand.Float64()
		packed_m, packed_e := decompose(x)
		rv := recompose(packed_m, packed_e)
		if rv != x {
			t.Errorf("Number did not convert: +v", x);
		}
	}
	for i := 0 ; i < 10000000; i++ {
		x := rand.Float64()
		packed_m, packed_e := decompose(x)
		rv := recompose(packed_m, packed_e)
		if rv != x {
			t.Errorf("Number did not convert: +v", x);
		}
	}
}

func Test_Pack1(t *testing.T){
	tst := func(x uint64) int {
		b := make([]byte, 9)
		ln := writeUnsignedHuff(b, x)
		for i:=ln;i<9;i++ {
			if b[i] != 0 {
				t.Errorf("Unexpected non-null byte")
			}
		}
		xr := readUnsignedHuff(b)
		if xr != x {
			t.Errorf("Number did not match:", x, xr)
		}
		return ln
	}
	//First test around the boundaries
	var order uint64
	for order = 0; order < 64; order++ {
		for offset := -4; offset < 4; offset ++ {
			x := uint64((1<<order) + offset)
			tst(x)
		}
	}
	
	//Now test that the huff boundaries have the write number of chars
	bcheck := []struct {
		n uint64
		exp int
	} {
		{ (1<<7) -1, 1},
		{ (1<<7)  , 2},
		{ (1<<14) -1, 2},
		{ (1<<14)  , 3},
		{ (1<<20) -1, 3},
		{ (1<<20)  , 4},
		{ (1<<28) -1, 4},
		{ (1<<28)  , 5},
		{ (1<<36) -1 , 5},
		{ (1<<36)  , 6},
		{ (1<<42) -1 , 6},
		{ (1<<42)  , 7},
		{ (1<<50)  -1, 7},
		{ (1<<50)  , 8},
		{ (1<<58)  -1, 8},
		{ (1<<58)  , 9},
		{ 0xFFFFFFFFFFFFFFFF , 9},
	}
	for _,ob := range(bcheck) {
		l := tst(ob.n)
		if l != ob.exp {
			t.Errorf("Did not get expected number of bytes out test=",ob,l)
		}
	}
	
	//Check the big number
	tst(0xFFFFFFFFFFFFFFFF)
	
	//Check the small number
	tst(0)
	
	//Check random numbers
	for i := 0 ; i < 1000000; i++ {
		x := uint64(rand.Int63())
		tst(x)
	}
}

func Test_Pack2(t *testing.T){
	//Unsigned numbers are probably covered ok, lets try a few signed numbers
	//Check random numbers
	tst := func(x int64) int {
		b := make([]byte, 9)
		ln := writeSignedHuff(b, x)
		for i:=ln;i<9;i++ {
			if b[i] != 0 {
				t.Errorf("Unexpected non-null byte")
			}
		}
		xr := readSignedHuff(b)
		if xr != x {
			t.Errorf("Number did not match:", x, xr)
		}
		return ln
	}
	for i := 0 ; i < 10000000; i++ {
		x := rand.Int63()
		tst(x)
	}
	tst(-1)
	tst(-0x7FFFFFFFFFFFFFFF)
	tst(0x7FFFFFFFFFFFFFFF)
}