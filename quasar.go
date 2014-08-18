package quasar

import (
	bstore "github.com/SoftwareDefinedBuildings/quasar/bstoreGen1"
	"github.com/SoftwareDefinedBuildings/quasar/qtree"
	"log"
	"sync"
	"time"
	"code.google.com/p/go-uuid/uuid"
	lg "code.google.com/p/log4go"
)

type openTree struct {
	store	[]qtree.Record
	id      uuid.UUID
	sigEC	chan bool
}

const MinimumTime = -(16<<56)
const MaximumTime = (48<<56)
const LatestGeneration = bstore.LatestGeneration


type Quasar struct {
	cfg QuasarConfig
	bs  *bstore.BlockStore

	//Transaction coalescence
	globlock     sync.Mutex
	treelocks  map[[16]byte]*sync.Mutex
	openTrees map[[16]byte]*openTree
}

func newOpenTree(id uuid.UUID) *openTree {
	return &openTree{
		id:    id,
	}
}

type QuasarConfig struct {
	//Measured in the number of datablocks
	//So 1000 is 8 MB cache
	DatablockCacheSize uint64

	//This enables the grouping of value inserts
	//with a commit every Interval millis
	//If the number of stored values exceeds
	//EarlyTrip
	TransactionCoalesceEnable    bool
	TransactionCoalesceInterval  uint64
	TransactionCoalesceEarlyTrip uint64

	//The mongo database is used to store superblocks
	//in the current version.
	//btoreEmu actually stores the datablocks there too
	MongoURI string
	
	//The path for the dblock store
	BlockPath string
}

var DefaultQuasarConfig QuasarConfig = QuasarConfig{
	DatablockCacheSize:          	65526, //512MB
	TransactionCoalesceEnable:   	true,
	TransactionCoalesceInterval: 	5000,
	TransactionCoalesceEarlyTrip: 	16384,
	MongoURI:                    	"localhost",
	BlockPath:						"/srv/quasar/",
}

func NewQuasar(cfg *QuasarConfig) (*Quasar, error) {
	bs, err := bstore.NewBlockStore(cfg.MongoURI, cfg.DatablockCacheSize, cfg.BlockPath)
	if err != nil {
		return nil, err
	}
	rv := &Quasar{
		cfg:       *cfg,
		bs:        bs,
		openTrees: make(map[[16]byte]*openTree, 128),
		treelocks: make(map[[16]byte]*sync.Mutex, 128),
	}
	return rv, nil
}

func (q *Quasar) getTree(id uuid.UUID) (*openTree, *sync.Mutex) {
	mk := bstore.UUIDToMapKey(id)
	q.globlock.Lock()
	ot, ok := q.openTrees[mk]
	if !ok {
		ot := newOpenTree(id)
		mtx := &sync.Mutex{}
		q.openTrees[mk] = ot
		q.treelocks[mk] = mtx
		q.globlock.Unlock()
		return ot, mtx
	}
	mtx, ok := q.treelocks[mk]
	if !ok {
		lg.Crashf("This should not happen")
	}
	q.globlock.Unlock()
	return ot, mtx
}

func (t *openTree) commit(q *Quasar) {
	if len(t.store) == 0 {
		//This might happen with a race in the timeout commit
		return
	}
	tr, err := qtree.NewWriteQTree(q.bs, t.id)
	if err != nil {
		log.Panic(err)
	}
	if err := tr.InsertValues(t.store); err != nil {
		log.Panic(err)
	}
	tr.Commit()
	t.store = nil
}
func (q *Quasar) InsertValues(id uuid.UUID, r []qtree.Record) {
	tr, mtx := q.getTree(id)
	mtx.Lock()
	if tr == nil {
		lg.Crashf("This should not happen")
	}
	if tr.store == nil {
		//Empty store
		tr.store = make([]qtree.Record,0,len(r)*2)
		tr.sigEC = make(chan bool, 1)
		//Also spawn the coalesce timeout goroutine
		go func(abrt chan bool) {
			tmt := time.After(time.Duration(q.cfg.TransactionCoalesceInterval) * time.Millisecond)
			select {
				case <- tmt :
					//do coalesce
					mtx.Lock()
					//In case we early tripped between waiting for lock and getting it, commit will return ok
					lg.Debug("Coalesce timeout %v",id.String())
					tr.commit(q)
					mtx.Unlock()
				case <- abrt :
				return
			}
		}(tr.sigEC)
	}
	tr.store = append(tr.store, r...)
	if uint64(len(tr.store)) >= q.cfg.TransactionCoalesceEarlyTrip {
		tr.sigEC <- true
		lg.Debug("Coalesce early trip %v",id.String())
		tr.commit(q)
	}
	mtx.Unlock()
}
func (q *Quasar) Flush(id uuid.UUID) error {
	tr, mtx := q.getTree(id)
	mtx.Lock()
	if len(tr.store) != 0 {
		tr.sigEC <- true
		tr.commit(q)
	}
	mtx.Unlock()
	return nil
}
/*
func (q *Quasar) InsertValues3(id uuid.UUID, r []qtree.Record) {
	mk := bstore.UUIDToMapKey(id)
	q.tlock.Lock()
	albert:
	ot, ok := q.openTrees[mk]
	if !ok {
		ot = newOpenTree(id)
		q.openTrees[mk] = ot
		go func () {
			time.Sleep(time.Duration(q.cfg.TransactionCoalesceInterval) * time.Millisecond)
			q.tlock.Lock()
			if !ot.expired {
				log.Printf("Coalesce timeout")
				//It is still running
				ot.expired = true
				//delete(q.openTrees, mk)
				ot.exmtx.Lock()
				q.tlock.Unlock()
				//XTAG: I'm worried about what happens when we get multiple of these
				//timeout commits pending...
				ot.Commit(q)
				ot.exmtx.Unlock()
			} else {
				//It was early comitted
				q.tlock.Unlock()
			}
		}()
	}
	if ot.expired {
		ot.exmtx.Lock()
		//If we obtain the lock, the transaction is done
		ot.exmtx.Unlock()
		delete(q.openTrees, mk)
		goto albert
	}
	ot.store = append(ot.store, r...)
	if len(ot.store) >= int(q.cfg.TransactionCoalesceEarlyTrip) {
		log.Printf("Coalesce early trip")
		ot.expired = true
		delete(q.openTrees, mk)
		q.tlock.Unlock()
		//So we do this synchronously as a way of exerting backpressure
		//otherwise we could get two of these commits happening
		//at the same time
		ot.Commit(q)
	} else {
		q.tlock.Unlock()
	}
}
func (q *Quasar) Flush(id uuid.UUID) error {
	mk := bstore.UUIDToMapKey(id)
	q.tlock.Lock()
	ot, ok := q.openTrees[mk]
	if !ok {
		q.tlock.Unlock()
		return qtree.ErrNoSuchStream
	}
	if ot.expired {
		q.tlock.Unlock()
		return nil
	}
	ot.expired = true
	delete(q.openTrees, mk)
	q.tlock.Unlock()
	ot.Commit(q)
	return nil
}
*/

//This function is threadsafe
/*
func (q *Quasar) InsertValuesBroken(id uuid.UUID, r []qtree.Record) {
	//Check if we have a coalesced commit waiting
	mk := bstore.UUIDToMapKey(id)
	q.tlock.Lock()
	ot, ok := q.openTrees[mk]
	if !ok {
		ot = newOpenTree(id)
		q.openTrees[mk] = ot
		go func() {
			time.Sleep(time.Duration(q.cfg.TransactionCoalesceInterval) * time.Microsecond)
			q.tlock.Lock()
			ot.mtx.Lock()
			if !ot.comitted {
				delete(q.openTrees, mk)
				q.tlock.Unlock()
				ot.Commit(q)
				//OT is now orphaned, no need to free mutex
			}
			//If it was committed, then its already being freed from the map
			q.tlock.Unlock()
		}()
	}
	ot.mtx.Lock()
	q.tlock.Unlock()
	if ot.comitted {
		log.Panic("I'm pretty sure this can't happen")
	}
	ot.store = append(ot.store, r...)
	if len(ot.store) >= int(q.cfg.TransactionCoalesceEarlyTrip) {
		ot.Commit(q)
	}
	ot.mtx.Unlock()
}
*/

//These functions are the API. TODO add all the bounds checking on PW, and sanity on start/end
func (q *Quasar) QueryValues(id uuid.UUID, start int64, end int64, gen uint64) ([]qtree.Record, uint64, error) {
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, 0, err
	}
	rv, err := tr.ReadStandardValuesBlock(start, end)
	return rv, tr.Generation(), err
}

func (q *Quasar) QueryStatisticalValues(id uuid.UUID, start int64, end int64,
	gen uint64, pointwidth uint8) ([]qtree.StatRecord, uint64, error) {
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, 0, err
	}
	rv, err := tr.QueryStatisticalValuesBlock(start, end, pointwidth)
	if err != nil {
		return nil, 0, err
	}
	return rv, tr.Generation(), nil
}

func (q *Quasar) QueryGeneration(id uuid.UUID) (uint64, error) {
	sb := q.bs.LoadSuperblock(id, bstore.LatestGeneration)
	if sb == nil {
		return 0, qtree.ErrNoSuchStream
	}
	return sb.Gen(), nil
}

func (q *Quasar) QueryNearestValue(id uuid.UUID, time int64, backwards bool, gen uint64) (qtree.Record, uint64, error) {
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return qtree.Record{}, 0, err
	}
	rv, err := tr.FindNearestValue(time, backwards)
	return rv, tr.Generation(), err
}
type ChangedRange struct {
	Start int64
	End int64
}

//Threshold is the number of points below which you stop trying to resolve the range further.
//So for example 10000 would mean that if a changed range had only 10000 points in it, you
//wouldn't care about splitting it up into smaller ranges. A threshold of 0 means go all the way
//to the leaves, which is slower
func (q *Quasar) QueryChangedRanges(id uuid.UUID, startgen uint64, endgen uint64, threshold uint64) ([]ChangedRange, uint64, error ){
	tr, err := qtree.NewReadQTree(q.bs, id, endgen)
	if err != nil {
		return nil, 0, err
	}
	
	rv := make([]ChangedRange, 1024)
	rch := tr.FindChangedSince(startgen, threshold)
	for {
		var lr *ChangedRange = nil
		select {
		case cr, ok := <-rch:
			if !ok {
				break
			}
			if !cr.Valid {
				lg.Crashf("Didn't think this could happen")
			}
			//Coalesce
			if lr != nil && cr.Start == lr.End+1 {
				lr.End = cr.End
			} else {
				if lr != nil {
					rv = append(rv, *lr)
				}
				lr = &ChangedRange{Start:cr.Start, End: cr.End}
			}
		}
	}
	return rv, tr.Generation(), nil
}

func (q *Quasar) UnlinkBlocks(ids []uuid.UUID, start []uint64, end []uint64) error {
	ulc := make([]bstore.UnlinkCriteriaNew, 0, len(ids))
	for i:=0; i < len(ids); i++ {
		log.Printf("Scanning generations for id %d",i)
		//Verify that the end generation exists
		sb := q.bs.LoadSuperblock(ids[i], end[i])
		if sb == nil {
			log.Printf("No such generation, skipping")
			continue
		}
	
		if start[i] != 0 {
			log.Panic("Only support start=0 for now")
		}
		e_sb := q.bs.LoadSuperblock(ids[i], end[i])
		log.Printf("End superblock MIBID was: %v",e_sb.MIBID())
		
		q.bs.UnlinkGenerations(ids[i], start[i], end[i])
		ulc = append(ulc,bstore.UnlinkCriteriaNew{Uuid: []byte(ids[i]), StartGen: start[i], EndGen: end[i] })
	}
	log.Printf("Got referenced addrs")
	
	//So my theory is that an unlink can free every node with a mibid greater than SB, and less than EB as long 
	//as it is not referenced by either SB or EB
	//For this implementation where SB == 0, thats everything with a MIBID less than EB and not referenced by
	//EB
	unlink_count := q.bs.UnlinkBlocks(ulc)
	log.Printf("Unlinked %d blocks",unlink_count)
	return nil
}

func (q *Quasar) DeleteRange(id uuid.UUID, start int64, end int64) error {
	tr, mtx := q.getTree(id)
	mtx.Lock()
	if len(tr.store) != 0 {
		tr.sigEC <- true
		tr.commit(q)
	}
	wtr, err := qtree.NewWriteQTree(q.bs, id)
	if err != nil {
		log.Panic(err)
	}
	err = wtr.DeleteRange(start, end)
	if err != nil {
		log.Panic(err)
	}
	wtr.Commit()
	mtx.Unlock()
	return nil
}

//Returns alloced, free, strange, leaked
func (q *Quasar) InspectBlocks() (uint64, uint64, uint64, uint64) {
	return q.bs.InspectBlocks()
}

func (q *Quasar) UnlinkLeaks() uint64 {
	return q.bs.UnlinkLeaks()
}



