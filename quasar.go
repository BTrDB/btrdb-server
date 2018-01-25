package btrdb

import (
	"fmt"
	"math"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/bprovider"
	"github.com/BTrDB/btrdb-server/internal/bstore"
	"github.com/BTrDB/btrdb-server/internal/configprovider"
	"github.com/BTrDB/btrdb-server/internal/jprovider"
	"github.com/BTrDB/btrdb-server/internal/mprovider"
	"github.com/BTrDB/btrdb-server/internal/rez"
	"github.com/BTrDB/btrdb-server/qtree"
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
	mp  mprovider.MProvider

	pqm *PQM
	jp  jprovider.JournalProvider
}

type pqmAdapter struct {
	q *Quasar
}

func (ad *pqmAdapter) JP() jprovider.JournalProvider {
	return ad.q.jp
}

func (ad *pqmAdapter) OurNotifiedRange(ctx context.Context) configprovider.MashRange {
	return ad.q.cfg.(configprovider.ClusterConfiguration).OurNotifiedRange()
}

//Unthrottled
func (ad *pqmAdapter) WritePrimaryStorage(ctx context.Context, id uuid.UUID, r []qtree.Record) (major uint64, err bte.BTE) {
	panic("ni")
	//return ad.q.primaryInsertValues(ctx, id, r)
}

//Appropriate locks will be held
func (ad *pqmAdapter) StreamMajorVersion(ctx context.Context, id uuid.UUID) (uint64, bte.BTE) {
	return ad.q.loadMajorVersion(ctx, id)
}

func (q *Quasar) backgroundScannerLoop() {
	for {
		time.Sleep(1 * time.Minute)
		uuz, err := q.mp.ListToDelete(context.Background())
		if err != nil {
			lg.Panicf("cannot initiate background scan: %v", err)
		}
		if len(uuz) != 0 {
			lg.Infof("identified %d streams for background deletion", len(uuz))
		} else {
			continue
		}
		q.StorageProvider().BackgroundCleanup(uuz)
		err = q.mp.ClearToDelete(context.Background(), uuz)
		if err != nil {
			lg.Panicf("could not complete background scan: %v", err)
		}
	}
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
	} else {
		res.Release()
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
	ccfg := cfg.(configprovider.ClusterConfiguration)
	mp := mprovider.NewEtcdMetadataProvider(cfg.ClusterPrefix(), ccfg.GetEtcdClient())
	rm := rez.NewResourceManager(cfg.(rez.TunableProvider))
	rm.CreateResourcePool(rez.OpenTrees,
		rez.NopNew, rez.NopDel)
	rm.CreateResourcePool(rez.OpenReadTrees,
		rez.NopNew, rez.NopDel)
	rm.CreateResourcePool(rez.ConcurrentOp,
		rez.NopNew, rez.NopDel)
	rv := &Quasar{
		cfg:       cfg,
		rez:       rm,
		bs:        bs,
		openTrees: make(map[[16]byte]*openTree, 128),
		treelocks: make(map[[16]byte]*sync.Mutex, 128),
		mp:        mp,
	}
	go rv.backgroundScannerLoop()
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
	q.globlock.Unlock()
	if !ok {
		lg.Panicf("This should not happen")
	}
	mtx.Lock()
	if len(ot.store) != 0 {
		if ot.res == nil {
			panic("nil res try get tree")
		}
	} else {
		//Store is empty
		if ot.res == nil {
			res, err := q.rez.Get(ctx, rez.OpenTrees)
			if err != nil {
				mtx.Unlock()
				return nil, nil, err
			}
			ot.res = res
		} else {
			//A tree with an empty store can have a resource
			//panic("empty store with resource?")
		}
	}
	return ot, mtx, nil
}
func (q *Quasar) getTree(ctx context.Context, id uuid.UUID) (*openTree, *sync.Mutex, bte.BTE) {
	mk := bstore.UUIDToMapKey(id)
	q.globlock.Lock()
	ot, ok := q.openTrees[mk]
	if !ok {
		q.globlock.Unlock()
		//This will get a resource too
		ot, err := q.newOpenTree(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		q.globlock.Lock()

		_, sniped := q.openTrees[mk]
		if sniped {
			q.globlock.Unlock()
			ot.res.Release()
			lg.Warningf("SNIPED ON OPEN TREE")
			return q.getTree(ctx, id)
		}
		mtx := &sync.Mutex{}
		mtx.Lock()
		q.openTrees[mk] = ot
		q.treelocks[mk] = mtx
		q.globlock.Unlock()
		return ot, mtx, nil
	}
	mtx, ok := q.treelocks[mk]
	q.globlock.Unlock()
	if !ok {
		lg.Panicf("This should not happen")
	}
	mtx.Lock()
	//do NOT release this lock, caller will release
	//defer mtx.Unlock()
	if len(ot.store) != 0 {
		if ot.res == nil {
			panic("nil res try get tree")
		}
	} else {
		//Store is empty
		if ot.res == nil {
			res, err := q.rez.Get(ctx, rez.OpenTrees)
			if err != nil {
				mtx.Unlock()
				return nil, nil, err
			}
			ot.res = res
		}
	}
	return ot, mtx, nil
}

func (t *openTree) commit(ctx context.Context, q *Quasar) {
	//The tree lock must be held when this is called
	if len(t.store) == 0 {
		//This might happen with a race in the timeout commit
		if t.res != nil {
			t.res.Release()
			t.res = nil
		}
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
	if t.res == nil {
		panic("nil tree tres")
	}
	t.res.Release()
	t.res = nil
}
func (q *Quasar) StorageProvider() bprovider.StorageProvider {
	return q.bs.StorageProvider()
}

func (q *Quasar) InsertValues(ctx context.Context, id uuid.UUID, r []qtree.Record) (maj, min uint64, err bte.BTE) {
	if ctx.Err() != nil {
		return 0, 0, bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
		return 0, 0, bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")
	}

	for _, rec := range r {
		//This is >= max-1 because inserting at max-1 is odd in that it is not
		//queryably because it is exclusive and the maximum parameter to queries.
		if rec.Time < MinimumTime || rec.Time >= (MaximumTime-1) {
			return 0, 0, bte.Err(bte.InvalidTimeRange, "insert contains points outside valid time interval")
		}
		if math.IsNaN(rec.Val) {
			return 0, 0, bte.Err(bte.BadValue, "insert contains NaN values")
		}
		if math.IsInf(rec.Val, 0) {
			return 0, 0, bte.Err(bte.BadValue, "insert contains Inf values")
		}
	}
	if len(r) == 0 {
		//Return the current version
		panic("need to support this")
	}

	tr, err := qtree.NewWriteQTree(q.bs, id)
	if err != nil {
		lg.Panicf("oh dear: %v", err)
	}
	if err := tr.InsertValues(r); err != nil {
		lg.Panicf("we should not allow this: %v", err)
	}
	err = tr.Commit()
	if err != nil {
		return 0, 0, err
	}
	return tr.Generation(), 0, nil
}

func (q *Quasar) Flush(ctx context.Context, id uuid.UUID) bte.BTE {
	if ctx.Err() != nil {
		return bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
		return bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")
	}
	//TODO v49 we don't have stuffs yet
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
				tr.commit(context.Background(), q)
				lg.Warningf("Flushed %x (%d/%d)", uu, idx, total)
			} else {
				lg.Warningf("Clean %x (%d/%d)", uu, idx, total)
			}
		}
		close(rv)
	}()
	return rv
}

func (q *Quasar) QueryValuesStream(ctx context.Context, id uuid.UUID, start int64, end int64, gen uint64) (chan qtree.Record, chan bte.BTE, uint64, uint64) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.ErrW(bte.ContextError, "context error", ctx.Err())), 0, 0
	}
	if start < MinimumTime || end >= MaximumTime {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "time range out of bounds")), 0, 0
	}
	if start >= end {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "start time >= end time")), 0, 0
	}
	if gen == LatestGeneration {
		if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
			return nil, bte.Chan(bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")), 0, 0
		}
	}
	res, err := q.rez.Get(ctx, rez.OpenReadTrees)
	if err != nil {
		return nil, bte.Chan(err), 0, 0
	}
	defer res.Release()
	tr, err := qtree.NewReadQTree(q.bs, id, gen)
	if err != nil {
		return nil, bte.Chan(err), 0, 0
	}
	recordc, errc := tr.ReadStandardValuesCI(ctx, start, end)
	if gen == LatestGeneration {
		return q.pqm.MergeQueryValuesStream(ctx, id, start, end, recordc, errc)
	}
	return recordc, errc, tr.Generation(), 0
}

func (q *Quasar) QueryStatisticalValuesStream(ctx context.Context, id uuid.UUID, start int64, end int64,
	gen uint64, pointwidth uint8) (chan qtree.StatRecord, chan bte.BTE, uint64) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.ErrW(bte.ContextError, "context error", ctx.Err())), 0
	}
	if pointwidth > 63 {
		return nil, bte.Chan(bte.Err(bte.InvalidPointWidth, "pointwidth invalid")), 0
	}
	start &^= ((1 << pointwidth) - 1)
	end &^= ((1 << pointwidth) - 1)
	//Make end equal to the last nanosecond in the interval
	end += (1 << pointwidth) - 1
	if start < MinimumTime || end >= MaximumTime {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "time range out of bounds")), 0
	}
	if start >= end {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "start time >= end time")), 0
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
	if width > 1<<63 {
		return nil, bte.Chan(bte.Err(bte.InvalidPointWidth, "window width is absurd")), 0
	}
	//Round end down to multiple of width
	over := (end - start) % int64(width)
	end -= over
	if start < MinimumTime || end >= MaximumTime {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "time range out of bounds")), 0
	}
	if start >= end {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "start time >= end time")), 0
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

// func (q *Quasar) QueryGeneration(ctx context.Context, id uuid.UUID) (uint64, bte.BTE) {
// 	if ctx.Err() != nil {
// 		return 0, bte.ErrW(bte.ContextError, "context error", ctx.Err())
// 	}
// 	sb := q.bs.LoadSuperblock(id, bstore.LatestGeneration)
// 	if sb == nil {
// 		return 0, bte.Err(bte.NoSuchStream, "stream not found")
// 	}
// 	return sb.Gen(), nil
// }

func (q *Quasar) QueryNearestValue(ctx context.Context, id uuid.UUID, time int64, backwards bool, gen uint64) (qtree.Record, bte.BTE, uint64) {
	if ctx.Err() != nil {
		return qtree.Record{}, bte.ErrW(bte.ContextError, "context error", ctx.Err()), 0
	}
	if time < MinimumTime || time >= MaximumTime {
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
func (q *Quasar) QueryChangedRanges(ctx context.Context, id uuid.UUID, startgen uint64, endgen uint64, resolution uint8) (chan ChangedRange, chan bte.BTE, uint64, uint64) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.ErrW(bte.ContextError, "context error", ctx.Err())), 0, 0
	}
	if resolution > 63 {
		return nil, bte.Chan(bte.Err(bte.InvalidPointWidth, "changed ranges resolution invalid")), 0, 0
	}

	//TODO v49 if the endgen is latest call merge on pqm
	if endgen == LatestGeneration {
		if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
			return nil, bte.Chan(bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")), 0, 0
		}
	}
	//0 is a reserved generation, so is 1, which means "before first"
	if startgen == 0 {
		startgen = 1
	}
	if startgen > endgen {
		return nil, bte.Chan(bte.Err(bte.InvalidVersions, "start version after end version")), 0, 0
	}
	res, err := q.rez.Get(ctx, rez.OpenReadTrees)
	if err != nil {
		return nil, bte.Chan(err), 0, 0
	}
	defer res.Release()
	tr, err := qtree.NewReadQTree(q.bs, id, endgen)
	if err != nil {
		lg.Debug("Error on QCR open tree")
		return nil, bte.Chan(err), 0, 0
	}
	nctx, cancel := context.WithCancel(ctx)
	mergeres := []ChangedRange{}
	minorver := uint64(0)
	if endgen == LatestGeneration {
		mergeres, err, _, minorver = q.pqm.GetChangedRanges(nctx, id, resolution)
		if err != nil {
			return nil, bte.Chan(err), 0, 0
		}
	}

	_ = tr
	cancel()
	_ = mergeres
	_ = minorver
	/*
		rv := make(chan ChangedRange, 100)
		rve := make(chan bte.BTE, 10)
		rch, rche := tr.FindChangedSince(nctx, startgen, resolution)
		var lr *ChangedRange = nil
		nxtcr := func(cr *qtree.ChangedRange) {
			if cr == nil {
				if lr != nil {
					rv <- *lr
				}
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
		go func() {
		nextupstream:
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
						if len(mergeres) == 0 {
							for _, m := range mergeres {
								nxtcr(m)
							}
						}
						nxtcr(nil)
						close(rv)
						cancel()
					}
				nextmerge:
					for len(mergeres) > 0 {
						mr := mergeres[0]
						//case A cr is before mr
						if cr.End < mr.Start {
							nxtcr(cr)
							continue nextupstream
						}
						//case B cr is after mr
						if cr.Start > mr.End {
							nxtcr(mr)
							mergeres = mergeres[1:]
							continue nextmerge
						}
						//case C cr intersects mr
						if mr.Start < cr.Start {
							cr.Start = mr.Start
						}
						if mr.End > cr.End {
							cr.End = mr.End
						}
						nxtcr(cr)
						mergeres = mergeres[1:]
						continue nextupstream
					}
				}
			}
		}()
		return rv, rve, tr.Generation()*/
	panic("ni")
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
	if start >= end {
		return bte.Err(bte.InvalidTimeRange, "start time >= end time")
	}
	tr, mtx, err := q.getTree(ctx, id)
	if err != nil {
		return err
	}
	defer mtx.Unlock()
	if len(tr.store) != 0 {
		tr.sigEC <- true
		tr.commit(ctx, q)
	} else {
		if tr.res != nil {
			tr.res.Release()
			tr.res = nil
		}
	}
	res, err := q.rez.Get(ctx, rez.OpenTrees)
	if err != nil {
		return err
	}
	defer res.Release()
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

// Sets the stream annotations. An entry with a nil string implies delete
func (q *Quasar) SetStreamAnnotations(ctx context.Context, uuid []byte, aver uint64, changes map[string]*string) bte.BTE {
	return q.mp.SetStreamAnnotations(ctx, uuid, aver, changes)
}

// Get a stream annotations and tags
func (q *Quasar) GetStreamDescriptor(ctx context.Context, uuid []byte) (res *mprovider.LookupResult, err bte.BTE) {
	return q.mp.GetStreamInfo(ctx, uuid)
}

// Get a stream annotations and tags
func (q *Quasar) GetStreamVersion(ctx context.Context, uuid []byte) (major, minor uint64, err bte.BTE) {
	if !q.GetClusterConfiguration().WeHoldWriteLockFor(uuid) {
		return 0, 0, bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")
	}
	return q.pqm.QueryVersion(ctx, uuid)
}

func (q *Quasar) loadMajorVersion(ctx context.Context, uuid []byte) (ver uint64, err bte.BTE) {
	//TODO use superblock cache or otherwise fix this
	ver = q.StorageProvider().GetStreamVersion(uuid)
	if ver == 0 {
		//There is a chance the stream exists but has not been written to.
		//GetStreamDescriptor will return an error if that is not the case, just
		//pass that on
		_, err = q.GetStreamDescriptor(ctx, uuid)
		return
	}
	return ver, nil
}

// CreateStream makes a stream with the given uuid, collection and tags. Returns
// an error if the uuid already exists.
func (q *Quasar) CreateStream(ctx context.Context, uuid []byte, collection string, tags map[string]string, annotations map[string]string) bte.BTE {
	err := q.mp.CreateStream(ctx, uuid, collection, tags, annotations)
	//Technically this is a race. If we crash between these two ops, the stream will 'exist' but be unusable.
	//I think that is acceptable for now
	if err != nil {
		return err
	}
	q.StorageProvider().SetStreamVersion(uuid, bprovider.SpecialVersionCreated)
	return nil
}

// DeleteStream tombstones a stream
func (q *Quasar) ObliterateStream(ctx context.Context, id []byte) bte.BTE {
	//Ensure that there are no open trees for this stream, and delete it under the stream lock and global lock?
	if ctx.Err() != nil {
		return bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
		return bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")
	}

	//Try get it and flush it
	tr, mtx, err := q.tryGetTree(ctx, id)
	if err != nil {
		return err
	}
	if tr != nil {
		if mtx == nil {
			panic("wut")
		}
		defer mtx.Unlock()
		if len(tr.store) != 0 {
			tr.sigEC <- true
			tr.commit(ctx, q)
		} else {
			//It could be nil res because it is zero store
			if tr.res != nil {
				tr.res.Release()
				tr.res = nil
			}
		}
	}

	q.bs.FlushSuperblockFromCache(id)

	//Ok it has been flushed (or did not exist)
	e := q.mp.DeleteStream(ctx, id)
	if e != nil {
		return e
	}
	q.StorageProvider().ObliterateStreamMetadata(id)
	return nil
}

// ListCollections returns a list of collections beginning with prefix (which may be "")
// and starting from the given string. If number is > 0, only that many results
// will be returned. More can be obtained by re-calling ListCollections with
// a given startingFrom and number.
func (q *Quasar) ListCollections(ctx context.Context, prefix string, startingFrom string, limit uint64) ([]string, bte.BTE) {
	return q.mp.ListCollections(ctx, prefix, startingFrom, limit)
}

// Return back all streams in all collections beginning with collection (or exactly equal if prefix is false)
// provided they have the given tags and annotations, where a nil entry in the map means has the tag but the value is irrelevant
func (q *Quasar) LookupStreams(ctx context.Context, collection string, isCollectionPrefix bool, tags map[string]*string, annotations map[string]*string) (chan *mprovider.LookupResult, chan bte.BTE) {
	return q.mp.LookupStreams(ctx, collection, isCollectionPrefix, tags, annotations)
}
