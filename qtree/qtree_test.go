package qtree

import (
	bstore "github.com/SoftwareDefinedBuildings/quasar/bstoreGen1"
	"log"
	"testing"
	"math/rand"
	"code.google.com/p/go-uuid/uuid"
)



var _bs *bstore.BlockStore = nil
func mBS() {
	if _bs == nil {
		nbs, err := bstore.NewBlockStore("localhost", 5000, "/srv/quasartestdb/")
		if err != nil {
			log.Panic(err)
		}
		_bs = nbs
	}
}

func TestTreeSWrite(t *testing.T) {
	//t.SkipNow()
	mBS()
	testuuid := uuid.NewRandom()
	
	tr, err := NewWriteQTree(_bs, testuuid)
	if err != nil {
		t.Error(err)
	}
	records := []Record{ Record{1,1}, Record{2,2}, Record{3,3} }
	tr.InsertValues(records)
	tr.Commit()
	
	tr, err = NewReadQTree(_bs, testuuid, bstore.LatestGeneration)
	if err != nil {
		log.Panic(err)
	}
	recordc := make(chan Record)
	errc := make(chan error)
	log.Printf("beginning chan select")
	rv := make([]Record, 0, 5)
	go tr.ReadStandardValuesCI(recordc, errc, -1, 8)
	for {
		select {
			case r, r_c := <- recordc:
				rv = append(rv, r)
				if (!r_c) { break }
			case err, err_c := <- errc:
				if err != nil {
					t.Error(err)
					return
				}
				if !err_c {
					return
				}
		}
	}
	
	for i, v := range(rv) {
		if v != records[i] {
			t.Fail()
		}
	}
}

func LoadWTree(id uuid.UUID) *QTree {
	mBS()
	tr, err := NewWriteQTree(_bs, id)
	if err != nil {
		log.Panic(err)
	}
	return tr
}
func GenData(s int64, e int64, avgTimeBetweenSamples uint64,
			 spread uint64, dat func (int64) float64) []Record {
 	if avgTimeBetweenSamples == 0 {
 		panic("lolwut")
 	}
 	if e <= s {
 		panic ("s<=e")
 	}
 	log.Printf("e %v s %v avt %v", s, e, avgTimeBetweenSamples)
 	p3 := uint64((e-s))/avgTimeBetweenSamples + 100
 	log.Printf("p3: ",p3)
	rv := make([]Record, 0, p3)
	r := Record{}
	for t := s; t < e; {
		r.Time = t
		r.Val = dat(t)
		rv = append(rv, r)
		nt := t + int64(avgTimeBetweenSamples)
		if spread != 0 {
			nt -= int64(spread/2)
			nt += rand.Int63n(int64(spread))
		}
		if nt > t {
			t = nt
		}
	}
	return rv
}
		
func MakeWTree() (*QTree, uuid.UUID) {
	id := uuid.NewRandom()
	mBS()
	tr, err := NewWriteQTree(_bs, id)
	if err != nil {
		log.Panic(err)
	}
	return tr, id
}
func CompareData(lhs []Record, rhs []Record) {
	if len(lhs) != len(rhs) {
		log.Panic("lhs != rhs len")
	}
	for i, v := range lhs {
		if rhs[i] != v {
			log.Panic("data differs")
		}
	}
}
func TestTreeSWriteLarge(t *testing.T) {
	mBS()
	testuuid := uuid.NewRandom()
	tr, err := NewWriteQTree(_bs, testuuid)
	log.Printf("Generated tree %v",testuuid.String())
	if err != nil {
		t.Error(err)
	}
	log.Printf("Generating dummy records")
	records := GenData(0, 40*DAY, HOUR, 2*MINUTE, func(t int64) float64 {
			return float64(t)})
	log.Printf("We generated %v records", len(records))
	
	tr.InsertValues(records)
	tr.Commit()
	
	tr, err = NewReadQTree(_bs, testuuid, bstore.LatestGeneration)
	if err != nil {
		log.Panic(err)
	}
	rrec, err := tr.ReadStandardValuesBlock(0, 40*DAY+2*MINUTE)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("We read %v records",len(rrec))
	if len(rrec) != len(records) {
		t.FailNow()
	}
	for i:=0; i < len(rrec); i++ {
		if records[i].Time != rrec[i].Time ||
		   records[i].Val != rrec[i].Val {
		   	t.FailNow()
		   }
		//log.Printf("[%5d] w=%v r=%v d=%v", i, records[i].Time, rrec[i].Time, 
		//	int64(records[i].Time- rrec[i].Time))
	}
	
}

func BenchmarkMultiSWrite(b *testing.B) {
	mBS()
	testuuid := uuid.NewRandom()
	log.Printf("MultiSWrite is using %v", testuuid.String())
	log.Printf("Generating dummy records")
	records := GenData(0, 1*DAY, SECOND, 100*MILLISECOND, func(t int64) float64 {
			return float64(t)})
	log.Printf("We generated %v records, randomizing a copy", len(records))
	rec_copy_orig := make([]Record,len(records))
	perm := rand.Perm(len(records))
	for i, v := range perm {
	    rec_copy_orig[v] = records[i]
	}
	b.ResetTimer()
	for iter:=0; iter <b.N; iter ++ {
		rec_copy := make([]Record,len(rec_copy_orig))
		copy(rec_copy, rec_copy_orig)
		iperstage := 4000
		idx := 0
		for {
			tr, err := NewWriteQTree(_bs, testuuid)
			if err != nil {
				b.Error(err)
			}
			end := idx+iperstage
			if end > len(rec_copy) {
				end = len(rec_copy)
			}
			tr.InsertValues(rec_copy[idx: end])
			tr.Commit()
			idx = end
			if idx == len(rec_copy) {
				break
			}
		}
		/*
		//Read back the records
		tr, err := NewReadQTree(_bs, testuuid, bstore.LatestGeneration)
		if err != nil {
			log.Panic(err)
		}
		rrec, err := tr.ReadStandardValuesBlock(0, 40*DAY+2*MINUTE)
		if err != nil {
			log.Panic(err)
		}
		*/
	}
}
func TestTreeMultiSWrite(t *testing.T) {
	mBS()
	testuuid := uuid.NewRandom()
	log.Printf("MultiSWrite is going into %v", testuuid.String())
	log.Printf("Generating dummy records")
	records := GenData(0, 1*HOUR, 1*MINUTE, 2*SECOND, func(t int64) float64 {
			return float64(t)})
	log.Printf("We generated %v records, randomizing a copy", len(records))
	rec_copy := make([]Record,len(records))
	perm := rand.Perm(len(records))
	for i, v := range perm {
	    rec_copy[v] = records[i]
	}
	iperstage := 30
	idx := 0
	for {
		tr, err := NewWriteQTree(_bs, testuuid)
		if err != nil {
			t.Error(err)
		}
		end := idx+iperstage
		if end > len(rec_copy) {
			end = len(rec_copy)
		}
		tr.InsertValues(rec_copy[idx: end])
		tr.root.PrintCounts(2)
		tr.Commit()
		idx = end
		if idx == len(rec_copy) {
			break
		}
	}
	
	//Read back the records
	tr, err := NewReadQTree(_bs, testuuid, bstore.LatestGeneration)
	if err != nil {
		log.Panic(err)
	}
	rrec, err := tr.ReadStandardValuesBlock(0, 40*DAY+2*MINUTE)
	if err != nil {
		log.Panic(err)
	}
	//Verify we have the same number (for now)
	log.Printf("wrote %v, read %v", len(records), len(rrec))
	tr.root.PrintCounts(0)
	if len(records) != len(rrec) {
		t.FailNow()
	}
}
