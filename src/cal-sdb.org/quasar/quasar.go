package quasar

import (
	bstore "cal-sdb.org/quasar/bstoreGen1"
	"cal-sdb.org/quasar/qtree"
	"log"
	"sync"
	"time"
	"code.google.com/p/go-uuid/uuid"
)

type openTree struct {
	expired  bool
	store	 []qtree.Record
	id       uuid.UUID
	exmtx	 sync.Mutex
}

const MinimumTime = -(16<<56)
const MaximumTime = (48<<56)
const LatestGeneration = bstore.LatestGeneration

//This must be called with the OT locked
func (t *openTree) Commit(q *Quasar) {
	tr, err := qtree.NewWriteQTree(q.bs, t.id)
	if err != nil {
		log.Panic(err)
	}
	tr.InsertValues(t.store)
	tr.Commit()
}

type Quasar struct {
	cfg QuasarConfig
	bs  *bstore.BlockStore

	//Transaction coalescence
	tlock     sync.Mutex
	openTrees map[[16]byte]*openTree
}

func newOpenTree(id uuid.UUID) *openTree {
	return &openTree{
		store: make([]qtree.Record, 0, 256),
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
	}
	return rv, nil
}

func (q *Quasar) InsertValues(id uuid.UUID, r []qtree.Record) {
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
func (q *Quasar) InsertValues2(id uuid.UUID, r []qtree.Record) {
	tr, err := qtree.NewWriteQTree(q.bs, id)
	if err != nil {
		log.Panic(err)
	}
	tr.InsertValues(r)
	tr.Commit()
}
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

func (q *Quasar) UnlinkBlocks(ids []uuid.UUID, start []uint64, end []uint64) error {
	end_refset := make(map[uint64]bool, 1024000)
	ulc := make([]bstore.UnlinkCriteria, 0, len(ids))
	for i:=0; i < len(ids); i++ {
		log.Printf("Scanning references for id %d",i)
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
		e_tree, err := qtree.NewReadQTree(q.bs, ids[i], end[i])
		if err != nil {
			log.Panic(err)
		}
		
		q.bs.UnlinkGenerations(ids[i], start[i], end[i])

		log.Printf("Generating referenced addrs")
		rchan := e_tree.GetAllReferencedVAddrs()
		//for i, v := range e_tree.
		idx := 0
		for {
			val, ok := <- rchan
			if idx % 8192 == 0 {
				log.Printf("Got referenced addr #%d", idx)
			}
			idx += 1
			if !ok {
				break
			}
			end_refset[val] = true
		}
		ulc = append(ulc,bstore.UnlinkCriteria{Uuid: []byte(ids[i]), StartMibid:0, EndMibid:e_sb.MIBID()})
	}
	log.Printf("Got referenced addrs")
	
	//So my theory is that an unlink can free every node with a mibid greater than SB, and less than EB as long 
	//as it is not referenced by either SB or EB
	//For this implementation where SB == 0, thats everything with a MIBID less than EB and not referenced by
	//EB
	unlink_count := q.bs.UnlinkBlocks(ulc, end_refset)
	log.Printf("Unlinked %d blocks",unlink_count)
	return nil
}

//Returns alloced, free, strange, leaked
func (q *Quasar) InspectBlocks() (uint64, uint64, uint64, uint64) {
	return q.bs.InspectBlocks()
}

func (q *Quasar) UnlinkLeaks() uint64 {
	return q.bs.UnlinkLeaks()
}



