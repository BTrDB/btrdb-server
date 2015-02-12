package quasar

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/SoftwareDefinedBuildings/quasar/internal/bstore"
	"github.com/SoftwareDefinedBuildings/quasar/qtree"
	"sync"
	"time"
	"github.com/op/go-logging"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

type openTree struct {
	store []qtree.Record
	id    uuid.UUID
	sigEC chan bool
}

const MinimumTime = -(16 << 56)
const MaximumTime = (48 << 56)
const LatestGeneration = bstore.LatestGeneration

type Quasar struct {
	cfg QuasarConfig
	bs  *bstore.BlockStore

	//Transaction coalescence
	globlock  sync.Mutex
	treelocks map[[16]byte]*sync.Mutex
	openTrees map[[16]byte]*openTree
}

func newOpenTree(id uuid.UUID) *openTree {
	return &openTree{
		id: id,
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

	Params map[string]string
}

// Return true if there are uncommited results to be written to disk
// Should only be used during shutdown as it hogs the glock
func (q *Quasar) IsPending() bool {
	isPend := false
	q.globlock.Lock()
	for uuid, ot := range q.openTrees {
		q.treelocks[uuid].Lock()
		if len(ot.store) != 0 {
			isPend = true
			q.treelocks[uuid].Unlock()
			break;
		}
		q.treelocks[uuid].Unlock()
	}
	q.globlock.Unlock()
	return isPend
}

func NewQuasar(cfg *QuasarConfig) (*Quasar, error) {
	bs, err := bstore.NewBlockStore(cfg.Params)
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
		log.Panicf("This should not happen")
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
		log.Error("BAD INSERT: ", err)
	}
	tr.Commit()
	t.store = nil
}
func (q *Quasar) InsertValues(id uuid.UUID, r []qtree.Record) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("BAD INSERT: ", r)
		}
	}()
	tr, mtx := q.getTree(id)
	mtx.Lock()
	if tr == nil {
		log.Panicf("This should not happen")
	}
	if tr.store == nil {
		//Empty store
		tr.store = make([]qtree.Record, 0, len(r)*2)
		tr.sigEC = make(chan bool, 1)
		//Also spawn the coalesce timeout goroutine
		go func(abrt chan bool) {
			tmt := time.After(time.Duration(q.cfg.TransactionCoalesceInterval) * time.Millisecond)
			select {
			case <-tmt:
				//do coalesce
				mtx.Lock()
				//In case we early tripped between waiting for lock and getting it, commit will return ok
				log.Debug("Coalesce timeout %v", id.String())
				tr.commit(q)
				mtx.Unlock()
			case <-abrt:
				return
			}
		}(tr.sigEC)
	}
	tr.store = append(tr.store, r...)
	if uint64(len(tr.store)) >= q.cfg.TransactionCoalesceEarlyTrip {
		tr.sigEC <- true
		log.Debug("Coalesce early trip %v", id.String())
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
	start &^= ((1<<pointwidth)-1)
	end &^= ~((1<<pointwidth)-1)
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
func (q *Quasar) QueryStatisticalValuesStream(id uuid.UUID, start int64, end int64,
	gen uint64, pointwidth uint8) (chan qtree.StatRecord, chan error, uint64) {
	start &^= ((1<<pointwidth)-1)
	end &^= ~((1<<pointwidth)-1)
	rvv := make(chan qtree.StatRecord, 1024)
	rve := make(chan error)
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, nil, 0
	}
	go tr.QueryStatisticalValues(rvv, rve, start, end, pointwidth)
	return rvv, rve, tr.Generation()
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
	End   int64
}

//Threshold is the number of points below which you stop trying to resolve the range further.
//So for example 10000 would mean that if a changed range had only 10000 points in it, you
//wouldn't care about splitting it up into smaller ranges. A threshold of 0 means go all the way
//to the leaves, which is slower
func (q *Quasar) QueryChangedRanges(id uuid.UUID, startgen uint64, endgen uint64, threshold uint64) ([]ChangedRange, uint64, error) {
	tr, err := qtree.NewReadQTree(q.bs, id, endgen)
	if err != nil {
		log.Debug("Error on QCR open tree")
		return nil, 0, err
	}
	rv := make([]ChangedRange, 0, 1024)
	rch := tr.FindChangedSince(startgen, threshold)
	var lr *ChangedRange = nil
	for {

		select {
		case cr, ok := <-rch:
			if !ok {
				//This is the end.
				//Do we have an unsaved LR?
				if lr != nil {
					rv = append(rv, *lr)
				}
				return rv, tr.Generation(), nil
			}
			if !cr.Valid {
				log.Panicf("Didn't think this could happen")
			}
			//Coalesce
			if lr != nil && cr.Start == lr.End {
				lr.End = cr.End
			} else {
				if lr != nil {
					rv = append(rv, *lr)
				}
				lr = &ChangedRange{Start: cr.Start, End: cr.End}
			}
		}
	}
	return rv, tr.Generation(), nil
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
