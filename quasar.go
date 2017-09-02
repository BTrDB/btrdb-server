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
	"github.com/SoftwareDefinedBuildings/btrdb/internal/mprovider"
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
	mp  mprovider.MProvider
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

func (q *Quasar) InsertValues(ctx context.Context, id uuid.UUID, r []qtree.Record) bte.BTE {
	if ctx.Err() != nil {
		return bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
		return bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")
	}

	for _, rec := range r {
		//This is >= max-1 because inserting at max-1 is odd in that it is not
		//queryably because it is exclusive and the maximum parameter to queries.
		if rec.Time < MinimumTime || rec.Time >= (MaximumTime-1) {
			return bte.Err(bte.InvalidTimeRange, "insert contains points outside valid time interval")
		}
		if math.IsNaN(rec.Val) {
			return bte.Err(bte.BadValue, "insert contains NaN values")
		}
		if math.IsInf(rec.Val, 0) {
			return bte.Err(bte.BadValue, "insert contains Inf values")
		}
	}
	//The resource may or may not be be non-nil (mtx is not held)
	tr, mtx, err := q.getTree(ctx, id)
	//mtx is locked if err == nil
	if err != nil {
		return err
	}
	//Empty insert is valid, but does nothing
	if len(r) == 0 {
		mtx.Unlock()
		return nil
	}
	defer mtx.Unlock()

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
				tr.commit(context.Background(), q)
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
		tr.commit(ctx, q)
	}
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
	defer mtx.Unlock()
	if tr == nil {
		return nil
	}
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

func (q *Quasar) QueryValuesStream(ctx context.Context, id uuid.UUID, start int64, end int64, gen uint64) (chan qtree.Record, chan bte.BTE, uint64) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.ErrW(bte.ContextError, "context error", ctx.Err())), 0
	}
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
	if startgen > endgen {
		return nil, bte.Chan(bte.Err(bte.InvalidVersions, "start version after end version")), 0
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
func (q *Quasar) GetStreamVersion(ctx context.Context, uuid []byte) (ver uint64, err bte.BTE) {
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
