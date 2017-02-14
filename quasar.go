package btrdb

import (
	"fmt"
	"math"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/SoftwareDefinedBuildings/btrdb/bte"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/bprovider"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/bstore"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/configprovider"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/rez"
	"github.com/SoftwareDefinedBuildings/btrdb/qtree"
	"github.com/op/go-logging"
	"github.com/pborman/uuid"
)

var lg *logging.Logger

func init() {
	lg = logging.MustGetLogger("log")
}

type openTree struct {
	store []qtree.Record
	id    uuid.UUID
	sigEC chan bool
	res   *rez.Resource
}

const MinimumTime = -(16 << 56)
const MaximumTime = (48 << 56)
const LatestGeneration = bstore.LatestGeneration

type Quasar struct {
	cfg configprovider.Configuration
	bs  *bstore.BlockStore

	//Transaction coalescence
	globlock  sync.Mutex
	treelocks map[[16]byte]*sync.Mutex
	openTrees map[[16]byte]*openTree

	rez *rez.RezManager
}

func (q *Quasar) Rez() *rez.RezManager {
	return q.rez
}
func (q *Quasar) newOpenTree(ctx context.Context, id uuid.UUID) (*openTree, bte.BTE) {
	res, err := q.rez.Get(ctx, rez.OpenTrees)
	if err != nil {
		return nil, err
	}
	if q.bs.StreamExists(id) {
		return &openTree{
			id:  id,
			res: res,
		}, nil
	}
	return nil, bte.Err(bte.NoSuchStream, "Create stream before inserting")
}

func (q *Quasar) GetClusterConfiguration() configprovider.ClusterConfiguration {
	if !q.cfg.ClusterEnabled() {
		panic("Clustering is not enabled")
	}
	return q.cfg.(configprovider.ClusterConfiguration)
}

// Return true if there are uncommited results to be written to disk
// Should only be used during shutdown as it hogs the glock
//XTAG func (q *Quasar) IsPending() bool {
//XTAG 	isPend := false
//XTAG 	q.globlock.Lock()
//XTAG 	for uuid, ot := range q.openTrees {
//XTAG 		q.treelocks[uuid].Lock()
//XTAG 		if len(ot.store) != 0 {
//XTAG 			isPend = true
//XTAG 			q.treelocks[uuid].Unlock()
//XTAG 			break
//XTAG 		}
//XTAG 		q.treelocks[uuid].Unlock()
//XTAG 	}
//XTAG 	q.globlock.Unlock()
//XTAG 	return isPend
//XTAG }

func NewQuasar(cfg configprovider.Configuration) (*Quasar, error) {
	bs, err := bstore.NewBlockStore(cfg)
	if err != nil {
		return nil, err
	}
	rm := rez.NewResourceManager(cfg.(rez.TunableProvider))
	rm.CreateResourcePool(rez.OpenTrees,
		rez.NopNew, rez.NopDel)
	rv := &Quasar{
		cfg:       cfg,
		rez:       rm,
		bs:        bs,
		openTrees: make(map[[16]byte]*openTree, 128),
		treelocks: make(map[[16]byte]*sync.Mutex, 128),
	}
	return rv, nil
}

//Like get tree but don't open it if it is not open
func (q *Quasar) tryGetTree(ctx context.Context, id uuid.UUID) (*openTree, *sync.Mutex, bte.BTE) {
	mk := bstore.UUIDToMapKey(id)
	q.globlock.Lock()
	ot, ok := q.openTrees[mk]
	if !ok {
		q.globlock.Unlock()
		return nil, nil, nil
	}
	mtx, ok := q.treelocks[mk]
	if !ok {
		lg.Panicf("This should not happen")
	}
	q.globlock.Unlock()
	return ot, mtx, nil
}
func (q *Quasar) getTree(ctx context.Context, id uuid.UUID) (*openTree, *sync.Mutex, bte.BTE) {
	mk := bstore.UUIDToMapKey(id)
	q.globlock.Lock()
	ot, ok := q.openTrees[mk]
	if !ok {
		ot, err := q.newOpenTree(ctx, id)
		if err != nil {
			q.globlock.Unlock()
			return nil, nil, err
		}
		mtx := &sync.Mutex{}
		q.openTrees[mk] = ot
		q.treelocks[mk] = mtx
		q.globlock.Unlock()
		return ot, mtx, nil
	}
	mtx, ok := q.treelocks[mk]
	if !ok {
		lg.Panicf("This should not happen")
	}
	q.globlock.Unlock()
	return ot, mtx, nil
}

func (t *openTree) commit(q *Quasar) {
	if len(t.store) == 0 {
		//This might happen with a race in the timeout commit
		fmt.Println("no store in commit")
		return
	}
	tr, err := qtree.NewWriteQTree(q.bs, t.id)
	if err != nil {
		lg.Panicf("oh dear: %v", err)
	}
	if err := tr.InsertValues(t.store); err != nil {
		lg.Panicf("we should not allow this: %v", err)
	}
	tr.Commit()
	t.store = nil
}
func (q *Quasar) StorageProvider() bprovider.StorageProvider {
	return q.bs.StorageProvider()
}

func (q *Quasar) InsertValues(ctx context.Context, id uuid.UUID, r []qtree.Record) bte.BTE {
	if ctx.Err() != nil {
		return bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
		return bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")
	}

	for _, rec := range r {
		if rec.Time < MinimumTime || rec.Time >= MaximumTime {
			return bte.Err(bte.InvalidTimeRange, "insert contains points outside valid time interval")
		}
		if math.IsNaN(rec.Val) {
			return bte.Err(bte.BadValue, "insert contains NaN values")
		}
		if math.IsInf(rec.Val, 0) {
			return bte.Err(bte.BadValue, "insert contains Inf values")
		}
	}
	tr, mtx, err := q.getTree(ctx, id)
	if err != nil {
		return err
	}
	//Empty insert is valid, but does nothing
	if len(r) == 0 {
		return nil
	}
	mtx.Lock()
	if tr == nil {
		lg.Panicf("This should not happen")
	}
	if tr.store == nil {
		//Empty store
		tr.store = make([]qtree.Record, 0, len(r)*2)
		tr.sigEC = make(chan bool, 1)
		//Also spawn the coalesce timeout goroutine
		go func(abrt chan bool) {
			tmt := time.After(time.Duration(q.cfg.CoalesceMaxInterval()) * time.Millisecond)
			select {
			case <-tmt:
				//do coalesce
				mtx.Lock()
				//In case we early tripped between waiting for lock and getting it, commit will return ok
				//lg.Debug("Coalesce timeout %v", id.String())
				tr.commit(q)
				mtx.Unlock()
			case <-abrt:
				return
			}
		}(tr.sigEC)
	}
	tr.store = append(tr.store, r...)
	if len(tr.store) >= q.cfg.CoalesceMaxPoints() {
		tr.sigEC <- true
		//lg.Debug("Coalesce early trip %v", id.String())
		tr.commit(q)
	}
	mtx.Unlock()
	return nil
}

func (q *Quasar) Flush(ctx context.Context, id uuid.UUID) bte.BTE {
	if ctx.Err() != nil {
		return bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
		return bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")
	}
	tr, mtx, err := q.tryGetTree(ctx, id)
	if err != nil {
		return err
	}
	if tr == nil {
		return nil
	}
	mtx.Lock()
	if len(tr.store) != 0 {
		tr.sigEC <- true
		tr.commit(q)
		fmt.Printf("Commit done %+v\n", id)
	} else {
		fmt.Printf("no store\n")
	}
	mtx.Unlock()
	return nil
}

func (q *Quasar) InitiateShutdown() chan struct{} {
	rv := make(chan struct{})
	go func() {
		lg.Warningf("Attempting to lock core mutex for shutdown")
		q.globlock.Lock()
		total := len(q.openTrees)
		lg.Warningf("Mutex acquired, there are %d trees to flush", total)
		idx := 0
		for uu, tr := range q.openTrees {
			idx++
			if len(tr.store) != 0 {
				tr.sigEC <- true
				tr.commit(q)
				lg.Warningf("Flushed %x (%d/%d)", uu, idx, total)
			} else {
				lg.Warningf("Clean %x (%d/%d)", uu, idx, total)
			}
		}
		close(rv)
	}()
	return rv
}

func (q *Quasar) QueryValuesStream(ctx context.Context, id uuid.UUID, start int64, end int64, gen uint64) (chan qtree.Record, chan bte.BTE, uint64) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.ErrW(bte.ContextError, "context error", ctx.Err())), 0
	}
	if start < MinimumTime || end >= MaximumTime {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "time range out of bounds")), 0
	}
	res, err := q.rez.Get(ctx, rez.OpenReadTrees)
	if err != nil {
		return nil, bte.Chan(err), 0
	}
	defer res.Release()
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, bte.Chan(err), 0
	}
	recordc, errc := tr.ReadStandardValuesCI(ctx, start, end)
	return recordc, errc, tr.Generation()
}

func (q *Quasar) QueryStatisticalValuesStream(ctx context.Context, id uuid.UUID, start int64, end int64,
	gen uint64, pointwidth uint8) (chan qtree.StatRecord, chan bte.BTE, uint64) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.ErrW(bte.ContextError, "context error", ctx.Err())), 0
	}
	if pointwidth > 63 {
		return nil, bte.Chan(bte.Err(bte.InvalidPointWidth, "pointwidth invalid")), 0
	}
	if start < MinimumTime || end >= MaximumTime {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "time range out of bounds")), 0
	}
	start &^= ((1 << pointwidth) - 1)
	end &^= ((1 << pointwidth) - 1)
	res, err := q.rez.Get(ctx, rez.OpenReadTrees)
	if err != nil {
		return nil, bte.Chan(err), 0
	}
	defer res.Release()
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, bte.Chan(err), 0
	}
	rvv, rve := tr.QueryStatisticalValues(ctx, start, end, pointwidth)
	return rvv, rve, tr.Generation()
}

func (q *Quasar) QueryWindow(ctx context.Context, id uuid.UUID, start int64, end int64,
	gen uint64, width uint64, depth uint8) (chan qtree.StatRecord, chan bte.BTE, uint64) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.ErrW(bte.ContextError, "context error", ctx.Err())), 0
	}
	if depth > 63 {
		return nil, bte.Chan(bte.Err(bte.InvalidPointWidth, "window depth invalid")), 0
	}
	if start < MinimumTime || end >= MaximumTime {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "time range out of bounds")), 0
	}
	res, err := q.rez.Get(ctx, rez.OpenReadTrees)
	if err != nil {
		return nil, bte.Chan(err), 0
	}
	defer res.Release()
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, bte.Chan(err), 0
	}
	rvv, rve := tr.QueryWindow(ctx, start, end, width, depth)
	return rvv, rve, tr.Generation()
}

func (q *Quasar) QueryGeneration(ctx context.Context, id uuid.UUID) (uint64, bte.BTE) {
	if ctx.Err() != nil {
		return 0, bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	sb := q.bs.LoadSuperblock(id, bstore.LatestGeneration)
	if sb == nil {
		return 0, bte.Err(bte.NoSuchStream, "stream not found")
	}
	return sb.Gen(), nil
}

func (q *Quasar) QueryNearestValue(ctx context.Context, id uuid.UUID, time int64, backwards bool, gen uint64) (qtree.Record, bte.BTE, uint64) {
	if ctx.Err() != nil {
		return qtree.Record{}, bte.ErrW(bte.ContextError, "context error", ctx.Err()), 0
	}
	if time < MinimumTime || time > MaximumTime {
		return qtree.Record{}, bte.Err(bte.InvalidTimeRange, "nearest time out of range"), 0
	}
	res, err := q.rez.Get(ctx, rez.OpenReadTrees)
	if err != nil {
		return qtree.Record{}, err, 0
	}
	defer res.Release()
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return qtree.Record{}, err, 0
	}

	rv, rve := tr.FindNearestValue(ctx, time, backwards)
	return rv, rve, tr.Generation()
}

type ChangedRange struct {
	Start int64
	End   int64
}

//Resolution is how far down the tree to go when working out which blocks have changed. Higher resolutions are faster
//but will give you back coarser results.
func (q *Quasar) QueryChangedRanges(ctx context.Context, id uuid.UUID, startgen uint64, endgen uint64, resolution uint8) (chan ChangedRange, chan bte.BTE, uint64) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.ErrW(bte.ContextError, "context error", ctx.Err())), 0
	}
	if resolution > 63 {
		return nil, bte.Chan(bte.Err(bte.InvalidPointWidth, "changed ranges resolution invalid")), 0
	}
	//0 is a reserved generation, so is 1, which means "before first"
	if startgen == 0 {
		startgen = 1
	}
	res, err := q.rez.Get(ctx, rez.OpenReadTrees)
	if err != nil {
		return nil, bte.Chan(err), 0
	}
	defer res.Release()
	tr, err := qtree.NewReadQTree(q.bs, id, endgen)
	if err != nil {
		lg.Debug("Error on QCR open tree")
		return nil, bte.Chan(err), 0
	}
	nctx, cancel := context.WithCancel(ctx)
	rv := make(chan ChangedRange, 100)
	rve := make(chan bte.BTE, 10)
	rch, rche := tr.FindChangedSince(nctx, startgen, resolution)
	var lr *ChangedRange = nil
	go func() {
		for {
			select {
			case err, ok := <-rche:
				if ok {
					cancel()
					rve <- err
					return
				}
			case cr, ok := <-rch:
				if !ok {
					//This is the end.
					//Do we have an unsaved LR?
					if lr != nil {
						rv <- *lr
					}
					close(rv)
					cancel()
					return
				}
				if !cr.Valid {
					lg.Panicf("Didn't think this could happen")
				}
				//Coalesce
				if lr != nil && cr.Start == lr.End {
					lr.End = cr.End
				} else {
					if lr != nil {
						rv <- *lr
					}
					lr = &ChangedRange{Start: cr.Start, End: cr.End}
				}
			}
		}
	}()
	return rv, rve, tr.Generation()
}

func (q *Quasar) DeleteRange(ctx context.Context, id uuid.UUID, start int64, end int64) bte.BTE {
	if ctx.Err() != nil {
		return bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
		return bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")
	}
	if start < MinimumTime || end >= MaximumTime {
		return bte.Err(bte.InvalidTimeRange, "delete time range out of bounds")
	}
	tr, mtx, err := q.getTree(ctx, id)
	if err != nil {
		return err
	}
	mtx.Lock()
	defer mtx.Unlock()
	if len(tr.store) != 0 {
		tr.sigEC <- true
		tr.commit(q)
	}
	wtr, err := qtree.NewWriteQTree(q.bs, id)
	if err != nil {
		return err
	}
	err2 := wtr.DeleteRange(start, end)
	if err2 != nil {
		lg.Panic(err2)
	}
	wtr.Commit()
	return nil
}
