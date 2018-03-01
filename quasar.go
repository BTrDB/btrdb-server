package btrdb

import (
	"math"
	"sync"
	"time"

	"context"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/bprovider"
	"github.com/BTrDB/btrdb-server/internal/bstore"
	"github.com/BTrDB/btrdb-server/internal/cephprovider"
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

func (ad *pqmAdapter) WritePrimaryStorage(ctx context.Context, id uuid.UUID, r []qtree.Record) (major uint64, err bte.BTE) {
	return ad.q.writePrimaryStorage(ctx, id, r)
}

func (as *pqmAdapter) CP() configprovider.ClusterConfiguration {
	return as.q.GetClusterConfiguration()
}

//Unthrottled
func (q *Quasar) writePrimaryStorage(ctx context.Context, id uuid.UUID, r []qtree.Record) (major uint64, err bte.BTE) {
	if ctx.Err() != nil {
		return 0, bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	// This code path is used by journal recovery which writes uuids that
	// WeHoldWriteLockFor thinks are not for us (until active mash advances)
	// if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
	// 	return 0, bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")
	// }

	for _, rec := range r {
		//This is >= max-1 because inserting at max-1 is odd in that it is not
		//queryably because it is exclusive and the maximum parameter to queries.
		if rec.Time < MinimumTime || rec.Time >= (MaximumTime-1) {
			return 0, bte.Err(bte.InvalidTimeRange, "insert contains points outside valid time interval")
		}
		if math.IsNaN(rec.Val) {
			return 0, bte.Err(bte.BadValue, "insert contains NaN values")
		}
		if math.IsInf(rec.Val, 0) {
			return 0, bte.Err(bte.BadValue, "insert contains Inf values")
		}
	}
	if len(r) == 0 {
		return q.loadMajorVersion(ctx, id)
	}

	tr, err := qtree.NewWriteQTree(q.bs, id)
	if err != nil {
		return 0, err
	}
	if err := tr.InsertValues(r); err != nil {
		return 0, err
	}
	err = tr.Commit()
	if err != nil {
		return 0, err
	}
	return tr.Generation(), nil
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

func (q *Quasar) GetClusterConfiguration() configprovider.ClusterConfiguration {
	if !q.cfg.ClusterEnabled() {
		panic("Clustering is not enabled")
	}
	return q.cfg.(configprovider.ClusterConfiguration)
}

func NewQuasar(cfg configprovider.Configuration) (*Quasar, error) {
	rm := rez.NewResourceManager(cfg.(rez.TunableProvider))
	bs, err := bstore.NewBlockStore(cfg, rm)
	if err != nil {
		return nil, err
	}
	ccfg := cfg.(configprovider.ClusterConfiguration)
	mp := mprovider.NewEtcdMetadataProvider(cfg.ClusterPrefix(), ccfg.GetEtcdClient())
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

	jp, err := cephprovider.NewJournalProvider(cfg, ccfg)
	if err != nil {
		return nil, err
	}
	rv.jp = jp
	pqm := NewPQM(&pqmAdapter{q: rv})
	rv.pqm = pqm
	ccfg.BeginClusterDaemons()
	go rv.backgroundScannerLoop()
	return rv, nil
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
		return q.pqm.QueryVersion(ctx, id)
	}

	return q.pqm.Insert(ctx, id, r)
}

func (q *Quasar) Flush(ctx context.Context, id uuid.UUID) (uint64, uint64, bte.BTE) {
	if ctx.Err() != nil {
		return 0, 0, bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
		return 0, 0, bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")
	}
	return q.pqm.Flush(ctx, id)
}

func (q *Quasar) InitiateShutdown() chan struct{} {
	return q.pqm.InitiateShutdown()
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
	tr, err := qtree.NewReadQTree(ctx, q.bs, id, gen)
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
	gen uint64, pointwidth uint8) (chan qtree.StatRecord, chan bte.BTE, uint64, uint64) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.ErrW(bte.ContextError, "context error", ctx.Err())), 0, 0
	}
	if pointwidth > 63 {
		return nil, bte.Chan(bte.Err(bte.InvalidPointWidth, "pointwidth invalid")), 0, 0
	}
	if gen == LatestGeneration {
		if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
			return nil, bte.Chan(bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")), 0, 0
		}
	}
	start &^= ((1 << pointwidth) - 1)
	end &^= ((1 << pointwidth) - 1)
	//Make end equal to the last nanosecond in the interval
	end += (1 << pointwidth) - 1
	if start < MinimumTime || end >= MaximumTime {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "time range out of bounds")), 0, 0
	}
	if start >= end {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "start time >= end time")), 0, 0
	}
	res, err := q.rez.Get(ctx, rez.OpenReadTrees)
	if err != nil {
		return nil, bte.Chan(err), 0, 0
	}
	defer res.Release()
	tr, err := qtree.NewReadQTree(ctx, q.bs, id, gen)
	if err != nil {
		return nil, bte.Chan(err), 0, 0
	}
	rvv, rve := tr.QueryStatisticalValues(ctx, start, end, pointwidth)
	return rvv, rve, tr.Generation(), 0
}

func (q *Quasar) QueryWindow(ctx context.Context, id uuid.UUID, start int64, end int64,
	gen uint64, width uint64, depth uint8) (chan qtree.StatRecord, chan bte.BTE, uint64, uint64) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.ErrW(bte.ContextError, "context error", ctx.Err())), 0, 0
	}
	if depth > 63 {
		return nil, bte.Chan(bte.Err(bte.InvalidPointWidth, "window depth invalid")), 0, 0
	}
	if width > 1<<63 {
		return nil, bte.Chan(bte.Err(bte.InvalidPointWidth, "window width is absurd")), 0, 0
	}
	if gen == LatestGeneration {
		if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
			return nil, bte.Chan(bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")), 0, 0
		}
	}
	//Round end down to multiple of width
	over := (end - start) % int64(width)
	end -= over
	if start < MinimumTime || end >= MaximumTime {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "time range out of bounds")), 0, 0
	}
	if start >= end {
		return nil, bte.Chan(bte.Err(bte.InvalidTimeRange, "start time >= end time")), 0, 0
	}

	res, err := q.rez.Get(ctx, rez.OpenReadTrees)
	if err != nil {
		return nil, bte.Chan(err), 0, 0
	}
	defer res.Release()
	tr, err := qtree.NewReadQTree(ctx, q.bs, id, gen)
	if err != nil {
		return nil, bte.Chan(err), 0, 0
	}
	rvv, rve := tr.QueryWindow(ctx, start, end, width, depth)
	return rvv, rve, tr.Generation(), 0
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

func (q *Quasar) QueryNearestValue(ctx context.Context, id uuid.UUID, time int64, backwards bool, gen uint64) (qtree.Record, bte.BTE, uint64, uint64) {
	if ctx.Err() != nil {
		return qtree.Record{}, bte.ErrW(bte.ContextError, "context error", ctx.Err()), 0, 0
	}
	if time < MinimumTime || time >= MaximumTime {
		return qtree.Record{}, bte.Err(bte.InvalidTimeRange, "nearest time out of range"), 0, 0
	}
	if gen == LatestGeneration {
		if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
			return qtree.Record{}, bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream"), 0, 0
		}
	}
	res, err := q.rez.Get(ctx, rez.OpenReadTrees)
	if err != nil {
		return qtree.Record{}, err, 0, 0
	}
	defer res.Release()
	tr, err := qtree.NewReadQTree(ctx, q.bs, id, gen)
	if err != nil {
		return qtree.Record{}, err, 0, 0
	}

	rv, rve := tr.FindNearestValue(ctx, time, backwards)
	if rve != nil {
		return qtree.Record{}, rve, 0, 0
	}
	if gen == LatestGeneration {
		return q.pqm.MergeNearestValue(ctx, id, time, backwards, rv)
	}
	return rv, rve, tr.Generation(), 0
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
	tr, err := qtree.NewReadQTree(ctx, q.bs, id, endgen)
	if err != nil {
		lg.Debug("Error on QCR open tree")
		return nil, bte.Chan(err), 0, 0
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
	//TODO we should actually query minor version data too
	return rv, rve, tr.Generation(), 0
}

func (q *Quasar) DeleteRange(ctx context.Context, id uuid.UUID, start int64, end int64) (uint64, uint64, bte.BTE) {
	if ctx.Err() != nil {
		return 0, 0, bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	if !q.GetClusterConfiguration().WeHoldWriteLockFor(id) {
		return 0, 0, bte.Err(bte.WrongEndpoint, "This is the wrong endpoint for this stream")
	}
	if start < MinimumTime || end >= MaximumTime {
		return 0, 0, bte.Err(bte.InvalidTimeRange, "delete time range out of bounds")
	}
	if start >= end {
		return 0, 0, bte.Err(bte.InvalidTimeRange, "start time >= end time")
	}
	_, _, err := q.pqm.Flush(ctx, id)

	res, err := q.rez.Get(ctx, rez.OpenTrees)
	if err != nil {
		return 0, 0, err
	}
	defer res.Release()
	wtr, err := qtree.NewWriteQTree(q.bs, id)
	if err != nil {
		return 0, 0, err
	}
	err = wtr.DeleteRange(start, end)
	if err != nil {
		return 0, 0, err
	}
	err = wtr.Commit()
	if err != nil {
		return 0, 0, err
	}
	return wtr.Generation(), 0, nil
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

func (q *Quasar) loadMajorVersion(ctx context.Context, uu []byte) (ver uint64, err bte.BTE) {
	//Lets assume the majority of these calls are happening on a node holding
	//the write lock. It is faster to query the actual superblock and therein
	//hit the sb cache than it is to directly query the version
	sb, err := q.bs.LoadSuperblock(ctx, uuid.UUID(uu), bstore.LatestGeneration)
	if err != nil {
		return 0, err
	}
	ver = sb.Gen()
	if ver == 0 {
		//There is a chance the stream exists but has not been written to.
		//GetStreamDescriptor will return an error if that is not the case, just
		//pass that on
		_, err = q.GetStreamDescriptor(ctx, uu)
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
	_, _, err := q.pqm.Flush(ctx, id)
	if err != nil {
		return err
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
