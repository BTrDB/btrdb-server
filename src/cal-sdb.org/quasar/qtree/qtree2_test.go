package qtree

import (
	"testing"
	"math/rand"
	"log"
	"fmt"
	"time"
	bstore "cal-sdb.org/quasar/bstoreGen1"
)

func init() {
	sd := time.Now().Unix()
	fmt.Printf(">>>> USING %v AS SEED <<<<<", sd)
	rand.Seed(sd)
}
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

//TODO PW test at range with no data
func TestQT2_PW2(t *testing.T){
	log.Printf("Inserting data 0-4096")
	te := int64(4096)
	tdat := GenData(0, 4096, 1, 0, func(_ int64) float64 {return rand.Float64()})
	if int64(len(tdat)) != te {
		log.Panic("GenDat messed up a bit")
	}
	tr, uuid := MakeWTree()
	tr.InsertValues(tdat)
	tr.Commit()
	var err error
	tr, err = NewReadQTree(_bs, uuid, bstore.LatestGeneration)
	if err != nil {
		t.Error(err)
	}
	
	moddat := make([]StatRecord, len(tdat))
	for i,v := range tdat {
		moddat[i] = StatRecord {
			Time:v.Time,
			Count:1,
			Min:v.Val,
			Mean:v.Val,
			Max:v.Val,
		}
	}
	expected_qty := 4096
	for pwi:=uint8(0); pwi<63;pwi++ {
		qrydat, err := tr.QueryStatisticalValuesBlock(-(16<<56), 48<<56 , pwi)
		if err != nil {
			log.Panic(err)
		}
		//log.Printf("for pwi %v, we got len %v",pwi, len(qrydat))
		if len(qrydat) != expected_qty {
			log.Printf("qdat: %v",qrydat)
			log.Printf("expected %v, got %v",expected_qty, len(qrydat))
			t.FailNow()
		}
		if expected_qty != 1 {
			expected_qty >>= 1
		}
	}
}
func TestQT2_PW(t *testing.T){
	log.Printf("Inserting data 0-4096")
	te := int64(4096)
	tdat := GenData(0, 4096, 1, 0, func(_ int64) float64 {return rand.Float64()})
	if int64(len(tdat)) != te {
		log.Panic("GenDat messed up a bit")
	}
	tr, uuid := MakeWTree()
	tr.InsertValues(tdat)
	tr.Commit()
	var err error
	tr, err = NewReadQTree(_bs, uuid, bstore.LatestGeneration)
	if err != nil {
		t.Error(err)
	}
	
	moddat := make([]StatRecord, len(tdat))
	for i,v := range tdat {
		moddat[i] = StatRecord {
			Time:v.Time,
			Count:1,
			Min:v.Val,
			Mean:v.Val,
			Max:v.Val,
		}
	}
	for pwi:=uint8(0); pwi<12;pwi++ {
		qrydat, err := tr.QueryStatisticalValuesBlock(0, te, pwi)
		if err != nil {
			log.Panic(err)
		}
		if int64(len(qrydat)) != te >> pwi {
			t.Log("len of qrydat mismatch %v vs %v", len(qrydat), te>>pwi)
			log.Printf("qry dat %+v", qrydat)
			t.FailNow()
		} else {
			t.Log("LEN MATCH %v",len(qrydat))
		}
		min := func (a float64, b float64) float64{
			if a<b {return a}
			return b
		}
		max := func (a float64, b float64) float64{
			if a>b {return a}
			return b
		}
		moddat2 := make([]StatRecord, len(moddat)/2)
		for i:=0; i < len(moddat)/2; i++ {
			nmean := moddat[2*i].Mean*float64(moddat[2*i].Count) +
					 moddat[2*i+1].Mean*float64(moddat[2*i+1].Count)
			nmean /= float64(moddat[2*i].Count + moddat[2*i+1].Count)
			
			moddat2[i] = StatRecord {
				Time:moddat[2*i].Time,
				Count:moddat[2*i].Count + moddat[2*i+1].Count,
				Min: min(moddat[2*i].Min, moddat[2*i+1].Min),
				Mean: nmean,
			    Max: max(moddat[2*i].Max, moddat[2*i+1].Max),
			}
		}
	}
}
func TestQT2_A(t *testing.T){
	gs := int64(20+rand.Intn(10))*365*DAY
	ge := int64(30+rand.Intn(10))*365*DAY
	freq := uint64(rand.Intn(10))*HOUR + 1
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

func TestQT2_Nearest(t *testing.T) {
	vals := []Record { 
		{int64(1<<56), 1 },
		{int64(2<<56), 2 },
		{int64(3<<56), 3 },
	}
	tr, uuid := MakeWTree()
	tr.InsertValues(vals)
	tr.Commit()
	rtr, err := NewReadQTree(_bs, uuid, bstore.LatestGeneration)
	if err != nil {
		log.Panic(err)
	}
	tparams := []struct{
		time int64
		backwards bool
		expectOk bool
		val float64
		} {
		{(2<<56)+1, true, true, 2},
		{(2<<56), true, true, 1},
		{(2<<56), false, true, 2},
		{(2<<56)+1, false, true, 3},
		{0, false, true, 1},
		{4<<56, true, true, 3},
		{0, true, false, -1},
		{4<<56, false, false, -1},
	}
	for i, v := range tparams {
		rv, err := rtr.FindNearestValue(v.time, v.backwards)
		if v.expectOk {
			if err != nil || rv.Val != v.val {
				t.Fatal("subtest [%v] = %+v", i, v)
			}
		} else {
			if err != ErrNoSuchPoint {
				t.Fatal("subtest [%v] = %+v", i, v)
			}
		}
	}
}



