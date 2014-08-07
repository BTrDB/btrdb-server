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
	comitted bool
	mtx      sync.Mutex
	store    []qtree.Record
	id       uuid.UUID
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
	t.comitted = true
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
	//with a commit every Interval microseconds
	//If the number of stored values exceeds
	//EarlyTrip
	TransactionCoalesceEnable    bool
	TransactionCoalesceInterval  uint64
	TransactionCoalesceEarlyTrip uint64

	//The mongo database is used to store superblocks
	//in the current version.
	//btoreEmu actually stores the datablocks there too
	MongoURI string
}

var DefaultQuasarConfig QuasarConfig = QuasarConfig{
	DatablockCacheSize:          65526, //512MB
	TransactionCoalesceEnable:   true,
	TransactionCoalesceInterval: 1000000,
	MongoURI:                    "localhost",
}

func NewQuasar(cfg *QuasarConfig) (*Quasar, error) {
	bs, err := bstore.NewBlockStore(cfg.MongoURI, cfg.DatablockCacheSize)
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
	tr, err := qtree.NewWriteQTree(q.bs, id)
	if err != nil {
		log.Panic(err)
	}
	tr.InsertValues(r)
	tr.Commit()
}
//This function is threadsafe
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
