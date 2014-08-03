package qtree

import (
	"testing"
	"math/rand"
	"log"
	"../bstore"
)

func GenBrk(avg uint64, spread uint64) chan uint64{
	rv := make(chan uint64)
	go func() {
		for {
			num := int64(avg)
			num -= int64(spread/2)
			num += rand.Int63n(int64(spread))
			rv <- uint64(num)
		}
	} ()
	return rv
}
func TestQT2_A(t *testing.T){
	rand.Seed(12)
	gs := int64(20+rand.Intn(10))*365*DAY
	ge := int64(30+rand.Intn(10))*365*DAY
	freq := uint64(rand.Intn(10))*HOUR
	varn := uint64(30*MINUTE)
	tdat := GenData(gs,ge, freq, varn, 
		func(_ int64) float64 {return rand.Float64()})
	log.Printf("generated %v records",len(tdat))
	tr, uuid := MakeWTree()
	log.Printf("geneated tree %v",tr.gen.Uuid().String())
	tr.Commit()
	
	idx := uint64(0)
	brks := GenBrk(100,50)
	loops := GenBrk(4,4)
	for ;idx<uint64(len(tdat)); {
		tr := LoadWTree(uuid)
		loop := <- loops
		for i:= uint64(0); i<loop; i++ {
			brk := <- brks
			if idx+brk >= uint64(len(tdat)) {
				brk = uint64(len(tdat)) - idx
			}
			if brk == 0 {
				continue
			}
			tr.InsertValues(tdat[idx:idx+brk])
			idx += brk
		}
		tr.Commit()
	}
	
	rtr, err := NewReadQTree(_bs, uuid, bstore.LatestGeneration) 
	if err != nil {
		log.Panic(err)
	}
	rval, err := rtr.ReadStandardValuesBlock(gs, ge+int64(2*varn))
	if err != nil {
		log.Panic(err)
	}
	log.Printf("wrote %v, read %v", len(tdat), len(rval))
	CompareData(tdat, rval)
}

