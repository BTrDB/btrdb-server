package btrdb

import (
	"fmt"
	_ "log"
	"math/rand"
	"testing"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/SoftwareDefinedBuildings/btrdb/qtree"
)

const MICROSECOND = 1000
const MILLISECOND = 1000 * MICROSECOND
const SECOND = 1000 * MILLISECOND
const MINUTE = 60 * SECOND
const HOUR = 60 * MINUTE
const DAY = 24 * HOUR

/*
func TestMultInsert(t *testing.T) {
	testuuid := uuid.NewRandom()
	cfg := &DefaultQuasarConfig
	cfg.BlockPath = "/srv/quasartestdb"
	q, err := NewQuasar(cfg)
	if err != nil {
		log.Panic(err)
	}
	vals := []qtree.Record{{10, 10}, {20, 20}}
	q.InsertValues(testuuid, vals)
	q.InsertValues(testuuid, vals)
}
*/
func init() {
	sd := time.Now().Unix()
	fmt.Printf(">>>> USING %v AS SEED <<<<<", sd)
	rand.Seed(sd)
}

/*
var _bs *bstore.BlockStore = nil

func mBS() {
	if _bs == nil {
		nbs, err := bstore.NewBlockStore("localhost", 0, "/srv/quasartestdb/")
		if err != nil {
			log.Panic(err)
		}
		_bs = nbs
	}
}
func GenBrk(avg uint64, spread uint64) chan uint64 {
	rv := make(chan uint64)
	go func() {
		for {
			num := int64(avg)
			num -= int64(spread / 2)
			num += rand.Int63n(int64(spread))
			rv <- uint64(num)
		}
	}()
	return rv
}
func GenData(s int64, e int64, avgTimeBetweenSamples uint64,
	spread uint64, dat func(int64) float64) []qtree.Record {
	if avgTimeBetweenSamples == 0 {
		panic("lolwut")
	}
	if e <= s {
		panic("s<=e")
	}
	log.Printf("e %v s %v avt %v", s, e, avgTimeBetweenSamples)
	p3 := uint64((e-s))/avgTimeBetweenSamples + 100
	log.Printf("p3: ", p3)
	rv := make([]qtree.Record, 0, p3)
	r := qtree.Record{}
	for t := s; t < e; {
		r.Time = t
		r.Val = dat(t)
		rv = append(rv, r)
		nt := t + int64(avgTimeBetweenSamples)
		if spread != 0 {
			nt -= int64(spread / 2)
			nt += rand.Int63n(int64(spread))
		}
		if nt > t {
			t = nt
		}
	}
	return rv
}
func MakeWTree() (*qtree.QTree, uuid.UUID) {
	id := uuid.NewRandom()
	mBS()
	tr, err := qtree.NewWriteQTree(_bs, id)
	if err != nil {
		log.Panic(err)
	}
	return tr, id
}
*/
func CompareData(lhs []qtree.Record, rhs []qtree.Record) {
	if len(lhs) != len(rhs) {
		log.Panicf("lhs != rhs len %d vs %d\n", len(lhs), len(rhs))
	}
	for i, v := range lhs {
		if rhs[i] != v {
			log.Panic("data differs")
		}
	}
}

/*
func LoadWTree(id uuid.UUID) *qtree.QTree {
	mBS()
	tr, err := qtree.NewWriteQTree(_bs, id)
	if err != nil {
		log.Panic(err)
	}
	return tr
}

//This flushes, for now
func TestInsertFlush(t *testing.T) {
	gs := int64(23) * 365 * DAY
	ge := int64(25) * 365 * DAY
	freq := uint64(100 * MINUTE)
	varn := uint64(10 * MINUTE)
	tdat := GenData(gs, ge, freq, varn,
		func(_ int64) float64 { return rand.Float64() })
	log.Printf("generated %v records", len(tdat))

	cfg := &DefaultQuasarConfig
	cfg.BlockPath = "/srv/quasartestdb"
	q, err := NewQuasar(cfg)
	if err != nil {
		log.Panic(err)
	}

	id := uuid.NewRandom()
	log.Printf("Generating uuid=%s", id)
	brk := GenBrk(100, 50)
	idx := 0
	for idx < len(tdat) {
		time.Sleep(100 * time.Millisecond)
		ln := int(<-brk)
		end := idx + ln
		if end > len(tdat) {
			end = len(tdat)
		}
		q.InsertValues(id, tdat[idx:end])
		q.Flush(id)
		idx += ln
	}

	q.Flush(id)

	dat, gen, err := q.QueryValues(id, gs, ge, LatestGeneration)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Test gen was: %v", gen)
	CompareData(dat, tdat)

}
*/
func TestArbWindow(t *testing.T) {
	Params := map[string]string{
		"mongoserver": "localhost",
		"provider":    "file",
		"cachesize":   "16000",
		"collection":  "testdb",
		"dbpath":      "/srv/testqdb/",
	}
	cfg := QuasarConfig{
		DatablockCacheSize:           uint64(0),
		TransactionCoalesceEnable:    true,
		TransactionCoalesceInterval:  uint64(5000),
		TransactionCoalesceEarlyTrip: uint64(16000),
		Params: Params,
	}
	q, err := NewQuasar(&cfg)
	if err != nil {
		log.Panicf("error: ", err)
	}
	startt := 0
	deltat := 1000000000
	tnum := 50000
	tdat := make([]qtree.Record, tnum)
	id := uuid.NewRandom()
	for i := 0; i < tnum; i++ {
		tdat[i].Time = int64(startt) + int64(deltat*i)
		tdat[i].Val = float64(i)
	}
	q.InsertValues(id, tdat)
	for i := 0; i < tnum; i++ {
		tdat[i].Time = int64(startt) + int64(deltat*i) + int64(tnum*2*deltat)
		tdat[i].Val = float64(i)
	}
	q.InsertValues(id, tdat)
	q.Flush(id)
	time.Sleep(2 * time.Second)
	log.Info("Stream: %+v\n", id)
	var rstart int64 = int64(startt) - int64(4000*deltat)
	var rend int64 = int64(startt + deltat*250000 + 5000000000)
	rvalc, _ := q.QueryWindow(id, rstart, rend, LatestGeneration, uint64(deltat)*700, 0)
	for {
		v, ok := <-rvalc
		log.Info("reading: %+v", v)
		if !ok {
			panic("eof")
		}
		/*exp := float64(v.Time+v.Time+int64(deltat)) / float64(deltat) / 2.0
		if math.Abs(v.Mean-exp) > 0.00001 {
			log.Panicf("got bad %+v\n expected mean: ", v, exp)
		}*/
	}
}

/*
func TestUnlinkBlocks(t *testing.T) {

	gs := int64(24) * 365 * DAY
	ge := int64(25) * 365 * DAY
	freq := uint64(300 * MINUTE)
	varn := uint64(10 * MINUTE)
	tdat := GenData(gs, ge, freq, varn,
		func(_ int64) float64 { return rand.Float64() })
	log.Printf("generated %v records", len(tdat))

	cfg := &DefaultQuasarConfig
	cfg.BlockPath = "/srv/quasartestdb"
	q, err := NewQuasar(cfg)
	if err != nil {
		log.Panic(err)
	}

	{
		alloced, free, strange, leaked := q.bs.InspectBlocks()
		log.Printf("BEFORE SUMMARY:")
		log.Printf("ALLOCED: %d", alloced)
		log.Printf("FREE   : %d", free)
		log.Printf("STRANGE: %d", strange)
		log.Printf("LEAKED : %d", leaked)
		log.Printf("USAGE  : %.2f %%\n", float64(alloced)/float64(alloced+free)*100)
	}
	id := uuid.NewRandom()
	log.Printf("Generating uuid=%s", id)
	brk := GenBrk(100, 50)
	idx := 0
	for idx < len(tdat) {
		time.Sleep(1 * time.Second)
		ln := int(<-brk)
		end := idx + ln
		if end > len(tdat) {
			end = len(tdat)
		}
		q.InsertValues(id, tdat[idx:end])
		idx += ln
	}
	//Allow for coalescence
	time.Sleep(10 * time.Second)

	{
		alloced, free, strange, leaked := q.bs.InspectBlocks()
		log.Printf("AFTER SUMMARY:")
		log.Printf("ALLOCED: %d", alloced)
		log.Printf("FREE   : %d", free)
		log.Printf("STRANGE: %d", strange)
		log.Printf("LEAKED : %d", leaked)
		log.Printf("USAGE  : %.2f %%\n", float64(alloced)/float64(alloced+free)*100)
	}
	{
		dat, gen, err := q.QueryValues(id, gs, ge, LatestGeneration)
		if err != nil {
			log.Panic(err)
		}
		log.Printf("Test gen was: %v", gen)
		CompareData(dat, tdat)
		err = q.UnlinkBlocks([]uuid.UUID{id}, []uint64{0}, []uint64{gen - 1})
		if err != nil {
			log.Panic(err)
		}
	}

	{
		dat, gen, err := q.QueryValues(id, gs, ge, LatestGeneration)
		if err != nil {
			log.Panic(err)
		}
		log.Printf("Test gen was: %v", gen)
		CompareData(dat, tdat)
	}

	{
		alloced, free, strange, leaked := q.bs.InspectBlocks()
		log.Printf("AFTER2 SUMMARY:")
		log.Printf("ALLOCED: %d", alloced)
		log.Printf("FREE   : %d", free)
		log.Printf("STRANGE: %d", strange)
		log.Printf("LEAKED : %d", leaked)
		log.Printf("USAGE  : %.2f %%\n", float64(alloced)/float64(alloced+free)*100)
	}
}
func TestCompleteDelete(t *testing.T) {
	gs := int64(24) * 365 * DAY
	ge := int64(25) * 365 * DAY
	freq := uint64(300 * MINUTE)
	varn := uint64(10 * MINUTE)
	tdat := GenData(gs, ge, freq, varn,
		func(_ int64) float64 { return rand.Float64() })
	log.Printf("generated %v records", len(tdat))
	id := uuid.NewRandom()
	cfg := &DefaultQuasarConfig
	cfg.BlockPath = "/srv/quasartestdb"
	q, err := NewQuasar(cfg)
	if err != nil {
		log.Panic(err)
	}
	{
		q.InsertValues(id, tdat)
		q.Flush(id)
	}
	{
		dat, _, err := q.QueryValues(id, gs, ge, LatestGeneration)
		if err != nil {
			log.Panic(err)
		}
		CompareData(dat, tdat)
	}
	{
		q.DeleteRange(id, gs, ge+1)
		dat, _, err := q.QueryValues(id, gs, ge, LatestGeneration)
		if err != nil {
			log.Panic(err)
		}
		if len(dat) != 0 {
			t.Log("dat length wrong")
			t.Fail()
		}
	}
	{
		q.InsertValues(id, tdat)
		q.Flush(id)
	}
	{
		dat, _, err := q.QueryValues(id, gs, ge, LatestGeneration)
		if err != nil {
			log.Panic(err)
		}
		CompareData(dat, tdat)
	}

}
func TestUnlinkBlocks2(t *testing.T) {

	gs := int64(24) * 365 * DAY
	ge := int64(25) * 365 * DAY
	freq := uint64(300 * MINUTE)
	varn := uint64(10 * MINUTE)
	tdat := GenData(gs, ge, freq, varn,
		func(_ int64) float64 { return rand.Float64() })
	log.Printf("generated %v records", len(tdat))

	cfg := &DefaultQuasarConfig
	cfg.BlockPath = "/srv/quasartestdb"
	q, err := NewQuasar(cfg)
	if err != nil {
		log.Panic(err)
	}

	{
		alloced, free, strange, leaked := q.bs.InspectBlocks()
		log.Printf("BEFORE SUMMARY:")
		log.Printf("ALLOCED: %d", alloced)
		log.Printf("FREE   : %d", free)
		log.Printf("STRANGE: %d", strange)
		log.Printf("LEAKED : %d", leaked)
		log.Printf("USAGE  : %.2f %%\n", float64(alloced)/float64(alloced+free)*100)
	}
	id := uuid.NewRandom()
	log.Printf("Generating uuid=%s", id)
	brk := GenBrk(100, 50)
	idx := 0
	for idx < len(tdat) {
		time.Sleep(1 * time.Second)
		ln := int(<-brk)
		end := idx + ln
		if end > len(tdat) {
			end = len(tdat)
		}
		q.InsertValues(id, tdat[idx:end])
		idx += ln
	}
	//Allow for coalescence
	time.Sleep(10 * time.Second)
	{
		alloced, free, strange, leaked := q.bs.InspectBlocks()
		log.Printf("BEFORE DELETE:")
		log.Printf("ALLOCED: %d", alloced)
		log.Printf("FREE   : %d", free)
		log.Printf("STRANGE: %d", strange)
		log.Printf("LEAKED : %d", leaked)
		log.Printf("USAGE  : %.2f %%\n", float64(alloced)/float64(alloced+free)*100)
	}
	{
		err := q.DeleteRange(id, tdat[1].Time, ge)
		if err != nil {
			t.Error(err)
		}
	}
	{
		q.InsertValues(id, []qtree.Record{{0, 100}})
		q.Flush(id)
	}
	{
		alloced, free, strange, leaked := q.bs.InspectBlocks()
		log.Printf("AFTER DELETE:")
		log.Printf("ALLOCED: %d", alloced)
		log.Printf("FREE   : %d", free)
		log.Printf("STRANGE: %d", strange)
		log.Printf("LEAKED : %d", leaked)
		log.Printf("USAGE  : %.2f %%\n", float64(alloced)/float64(alloced+free)*100)
	}
	{
		_, gen, err := q.QueryValues(id, gs, ge, LatestGeneration)
		if err != nil {
			log.Panic(err)
		}
		err = q.UnlinkBlocks([]uuid.UUID{id}, []uint64{0}, []uint64{gen})
		if err != nil {
			log.Panic(err)
		}
	}

	{
		alloced, free, strange, leaked := q.bs.InspectBlocks()
		log.Printf("AFTER FREE:")
		log.Printf("ALLOCED: %d", alloced)
		log.Printf("FREE   : %d", free)
		log.Printf("STRANGE: %d", strange)
		log.Printf("LEAKED : %d", leaked)
		log.Printf("USAGE  : %.2f %%\n", float64(alloced)/float64(alloced+free)*100)
	}
}
*/
