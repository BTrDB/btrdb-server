package bstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/bprovider"
	"github.com/BTrDB/btrdb-server/internal/cephprovider"
	"github.com/BTrDB/btrdb-server/internal/configprovider"
	"github.com/BTrDB/btrdb-server/internal/rez"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pborman/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

var pmLAS prometheus.Histogram
var pmBlockCacheMisses prometheus.Counter
var pmBlockCacheHits prometheus.Counter
var pmSuperblockMisses prometheus.Counter
var pmSuperblockHits prometheus.Counter
var pmCacheOccupancy prometheus.Gauge

func init() {
	pmLAS = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "btrdb",
		Name:       "link_and_store",
		Help:       "Milliseconds spent doing Link And Store to ceph",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	prometheus.MustRegister(pmLAS)
	pmBlockCacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "btrdb",
		Name:      "bcache_miss",
		Help:      "The number of block cache misses that have occurred",
	})
	prometheus.MustRegister(pmBlockCacheMisses)
	pmBlockCacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "btrdb",
		Name:      "bcache_hit",
		Help:      "The number of block cache hits that have occurred",
	})
	prometheus.MustRegister(pmBlockCacheHits)
	pmSuperblockMisses = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "btrdb",
		Name:      "sbcache_miss",
		Help:      "The number of superblock cache misses that have occurred",
	})
	prometheus.MustRegister(pmSuperblockMisses)
	pmSuperblockHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "btrdb",
		Name:      "sbcache_hit",
		Help:      "The number of superblock cache hits that have occurred",
	})
	prometheus.MustRegister(pmSuperblockHits)
	pmCacheOccupancy = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "btrdb",
		Name:      "bcache_occupancy",
		Help:      "The percentage of the block cache in use",
	})
	prometheus.MustRegister(pmCacheOccupancy)
}

const LatestGeneration = uint64(^(uint64(0)))

func UUIDToMapKey(id uuid.UUID) [16]byte {
	rv := [16]byte{}
	copy(rv[:], id)
	return rv
}

type BlockStore struct {
	_wlocks map[[16]byte]*sync.Mutex
	glock   sync.RWMutex

	cachemap map[uint64]*CacheItem
	cacheold *CacheItem
	cachenew *CacheItem
	cachemtx sync.Mutex
	cachelen uint64
	cachemax uint64

	cachemiss uint64
	cachehit  uint64

	store bprovider.StorageProvider
	alloc chan uint64
	cfg   configprovider.Configuration
	ccfg  configprovider.ClusterConfiguration

	sbcache     map[[16]byte]*sbcachet
	sbmu        sync.Mutex
	sbcachehit  uint64
	sbcachemiss uint64

	laschan    chan *LASMetric
	lasdropped uint64

	evict_replaced_blocks bool

	rm *rez.RezManager
}

var block_buf_pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, DBSIZE+5)
	},
}

var ErrDatablockNotFound = errors.New("Coreblock not found")
var ErrGenerationNotFound = errors.New("Generation not found")

/* A generation stores all the information acquired during a write pass.
 * A superblock contains all the information required to navigate a tree.
 */
type Generation struct {
	Cur_SB     *Superblock
	New_SB     *Superblock
	cblocks    []*Coreblock
	vblocks    []*Vectorblock
	blockstore *BlockStore
	flushed    bool
	replaced   []uint64
}

func (g *Generation) HintEvictReplaced(addr uint64) {
	g.replaced = append(g.replaced, addr)
}
func (g *Generation) UpdateRootAddr(addr uint64) {
	g.New_SB.root = addr
}
func (g *Generation) Uuid() *uuid.UUID {
	return &g.Cur_SB.uuid
}

func (g *Generation) Number() uint64 {
	return g.New_SB.gen
}

// func (bs *BlockStore) UnlinkGenerations(id uuid.UUID, sgen uint64, egen uint64) error {
// 	iter := bs.db.C("superblocks").Find(bson.M{"uuid": id.String(), "gen": bson.M{"$gte": sgen, "$lt": egen}, "unlinked": false}).Iter()
// 	rs := fake_sblock{}
// 	for iter.Next(&rs) {
// 		rs.Unlinked = true
// 		_, err := bs.db.C("superblocks").Upsert(bson.M{"uuid": id.String(), "gen": rs.Gen}, rs)
// 		if err != nil {
// 			lg.Panic(err)
// 		}
// 	}
// 	return nil
// }
func NewBlockStore(cfg configprovider.Configuration, rm *rez.RezManager) (*BlockStore, error) {
	bs := BlockStore{}
	bs.cfg = cfg
	bs.laschan = make(chan *LASMetric, 1000)
	bs.ccfg, _ = cfg.(configprovider.ClusterConfiguration)
	bs._wlocks = make(map[[16]byte]*sync.Mutex)
	bs.sbcache = make(map[[16]byte]*sbcachet, SUPERBLOCK_CACHE_SIZE)
	bs.alloc = make(chan uint64, 256)
	bs.rm = rm
	bs.ccfg.WatchMASHChange(func(flushComplete chan struct{}, activeRange configprovider.MashRange, proposedRange configprovider.MashRange) {
		bs.NotifyWriteLockLost()
		close(flushComplete)
	})
	//TODO maybe this shuld not be hardcoded?
	//False has been the default for a long time
	bs.evict_replaced_blocks = false
	go func() {
		relocation_addr := uint64(RELOCATION_BASE)
		for {
			bs.alloc <- relocation_addr
			relocation_addr += 1
			if relocation_addr < RELOCATION_BASE {
				relocation_addr = RELOCATION_BASE
			}
		}
	}()
	go bs.lasmetricloop()
	if cfg.ClusterEnabled() {
		bs.store = new(cephprovider.CephStorageProvider)
	} else {
		panic("we no longer support the file storage engine")
	}
	bs.store.Initialize(cfg, rm)
	cachesz := cfg.BlockCache()
	bs.initCache(uint64(cachesz))
	return &bs, nil
}

// This is called if our write lock changes. Need to invalidate caches
func (bs *BlockStore) NotifyWriteLockLost() {
	bs.sbmu.Lock()
	bs.sbcache = make(map[[16]byte]*sbcachet)
	bs.sbmu.Unlock()
}

func (bs *BlockStore) lasmetricloop() {
	lastemit := time.Now()
	buf := make([]*LASMetric, 0, 1000)
	for {
		for m := range bs.laschan {
			pmLAS.Observe(float64(m.sort+m.lock+m.vb+m.cb+m.unlock) / 1000)
			buf = append(buf, m)
			if time.Now().Sub(lastemit) > 2*time.Second {
				//emit the las information
				_total := make([]int, len(buf))
				_sort := make([]int, len(buf))
				_lock := make([]int, len(buf))
				_vb := make([]int, len(buf))
				_cb := make([]int, len(buf))
				_unlock := make([]int, len(buf))
				for idx, e := range buf {
					_total[idx] = e.sort + e.lock + e.vb + e.cb + e.unlock
					_sort[idx] = e.sort
					_lock[idx] = e.lock
					_vb[idx] = e.vb
					_cb[idx] = e.cb
					_unlock[idx] = e.unlock
				}
				sort.Ints(_total)
				sort.Ints(_sort)
				sort.Ints(_lock)
				sort.Ints(_vb)
				sort.Ints(_cb)
				sort.Ints(_unlock)
				lg.Infof("[las totalmax=%d,totalmed=%d,sortmax=%d,sortmed=%d,lockmax=%d,lockmed=%d,vbmax=%d,vbmed=%d,cbmax=%d,cbmed=%d,unlockmax=%d,unlockmed=%d,rate=%f]",
					_total[len(_total)-1], _total[len(_total)/2],
					_sort[len(_sort)-1], _sort[len(_sort)/2],
					_lock[len(_lock)-1], _lock[len(_lock)/2],
					_vb[len(_vb)-1], _vb[len(_vb)/2],
					_cb[len(_cb)-1], _cb[len(_cb)/2],
					_unlock[len(_lock)-1], _unlock[len(_unlock)/2],
					float64(len(buf))/float64(time.Now().Sub(lastemit)/time.Second))
				buf = buf[:0]

				lastemit = time.Now()
				if bs.lasdropped > 0 {
					fmt.Printf("LAS DROPPED %d", bs.lasdropped)
					bs.lasdropped = 0
				}
			}
		}
	}
}

func (bs *BlockStore) StreamExists(ctx context.Context, id uuid.UUID) (bool, bte.BTE) {
	if e := bte.CtxE(ctx); e != nil {
		return false, e
	}
	cachedSB := bs.LoadSuperblockFromCache(id)
	if cachedSB != nil {
		return true, nil
	}
	latestGen, err := bs.store.GetStreamVersion(ctx, id)
	if err != nil {
		return false, bte.ErrW(bte.CephError, "error getting stream version", err)
	}
	if latestGen > 0 {
		return true, nil
	}
	return false, nil
}

func (bs *BlockStore) StorageProvider() bprovider.StorageProvider {
	return bs.store
}

/*
 * This obtains a generation, blocking if necessary
 */
func (bs *BlockStore) ObtainGeneration(ctx context.Context, id uuid.UUID) (*Generation, bte.BTE) {
	if e := bte.CtxE(ctx); e != nil {
		return nil, e
	}
	mk := UUIDToMapKey(id)
	bs.glock.Lock()
	mtx, ok := bs._wlocks[mk]
	if !ok {
		//Mutex doesn't exist so is unlocked
		mtx = new(sync.Mutex)
		mtx.Lock()
		bs._wlocks[mk] = mtx
	} else {
		mtx.Lock()
	}
	bs.glock.Unlock()

	gen := &Generation{
		cblocks: make([]*Coreblock, 0, 8192),
		vblocks: make([]*Vectorblock, 0, 8192),
	}
	//We need a generation. Lets check the cache
	var err bte.BTE
	gen.Cur_SB, err = bs.LoadSuperblock(ctx, id, LatestGeneration)
	if err != nil {
		return nil, err
	}
	if gen.Cur_SB == nil {
		// Stream doesn't exist, error
		return nil, bte.Err(bte.NoSuchStream, "Stream does not exist")
		//previously we just made the stream like this:
		//gen.Cur_SB = NewSuperblock(id)
	}

	gen.New_SB = gen.Cur_SB.CloneInc()
	gen.blockstore = bs
	return gen, nil
}

//The returned address map is primarily for unit testing
func (gen *Generation) Commit() (map[uint64]uint64, bte.BTE) {
	//TODO v49 we could return errors from ceph here
	if gen.flushed {
		return nil, bte.Err(bte.InvariantFailure, "Already committed")
	}
	sp := opentracing.StartSpan("LinkAndStore")
	address_map := LinkAndStore([]byte(*gen.Uuid()), gen.blockstore, gen.blockstore.store, gen.vblocks, gen.cblocks)
	sp.Finish()
	rootaddr, ok := address_map[gen.New_SB.root]
	if !ok {
		lg.Panic("Could not obtain root address")
	}
	gen.New_SB.root = rootaddr

	//lg.Infof("rawlp[%s %s=%d,%s=%d,%s=%d]", "las", "latus", uint64(dt/time.Microsecond), "cblocks", len(gen.cblocks), "vblocks", len(gen.vblocks))
	//log.Info("(LAS %4dus %dc%dv) ins blk u=%v gen=%v root=0x%016x",
	//	uint64(dt/time.Microsecond), len(gen.cblocks), len(gen.vblocks), gen.Uuid().String(), gen.Number(), rootaddr)
	/*if len(gen.vblocks) > 100 {
		total := 0
		for _, v:= range gen.vblocks {
			total += int(v.Len)
		}
		log.Critical("Triggered vblock examination: %v blocks, %v points, %v avg", len(gen.vblocks), total, total/len(gen.vblocks))
	}*/
	gen.vblocks = nil
	gen.cblocks = nil
	sp = opentracing.StartSpan("WriteSuperblock")
	gen.blockstore.store.WriteSuperBlock(gen.New_SB.uuid, gen.New_SB.gen, gen.New_SB.Serialize())
	gen.blockstore.store.SetStreamVersion(gen.New_SB.uuid, gen.New_SB.gen)
	gen.blockstore.PutSuperblockInCache(gen.New_SB)
	sp.Finish()
	gen.flushed = true
	gen.blockstore.glock.RLock()
	gen.blockstore._wlocks[UUIDToMapKey(*gen.Uuid())].Unlock()
	gen.blockstore.glock.RUnlock()

	//Also evict replaced blocks
	if gen.blockstore.evict_replaced_blocks {
		for _, block := range gen.replaced {
			gen.blockstore.cacheEvictAddr(block)
		}
	}
	return address_map, nil
}

func (bs *BlockStore) allocateBlock() uint64 {
	relocation_address := <-bs.alloc
	return relocation_address
}

func (gen *Generation) AllocateCoreblock() *Coreblock {
	cblock := &Coreblock{}
	cblock.Identifier = gen.blockstore.allocateBlock()
	cblock.Generation = gen.Number()
	gen.cblocks = append(gen.cblocks, cblock)
	return cblock
}

func (gen *Generation) AllocateVectorblock() *Vectorblock {
	vblock := &Vectorblock{}
	vblock.Identifier = gen.blockstore.allocateBlock()
	vblock.Generation = gen.Number()
	gen.vblocks = append(gen.vblocks, vblock)
	return vblock
}

func (bs *BlockStore) FreeCoreblock(cb **Coreblock) {
	*cb = nil
}

func (bs *BlockStore) FreeVectorblock(vb **Vectorblock) {
	*vb = nil
}

func (bs *BlockStore) ReadDatablock(ctx context.Context, uuid uuid.UUID, addr uint64, impl_Generation uint64, impl_Pointwidth uint8, impl_StartTime int64) (Datablock, bte.BTE) {
	if e := bte.CtxE(ctx); e != nil {
		return nil, e
	}
	//Try hit the cache first
	db := bs.cacheGet(addr)
	if db != nil {
		return db, nil
	}
	sp := opentracing.StartSpan("ReadDatablock")
	syncbuf := block_buf_pool.Get().([]byte)
	trimbuf, err := bs.store.Read(ctx, []byte(uuid), addr, syncbuf)
	sp.Finish()
	if err != nil {
		return nil, bte.ErrW(bte.CephError, "could not read datablock", err)
	}
	sp = opentracing.StartSpan("DecodeDatablock")
	defer sp.Finish()
	switch DatablockGetBufferType(trimbuf) {
	case Core:
		rv := &Coreblock{}
		rv.Deserialize(trimbuf)
		block_buf_pool.Put(syncbuf)
		rv.Identifier = addr
		rv.Generation = impl_Generation
		rv.PointWidth = impl_Pointwidth
		rv.StartTime = impl_StartTime
		bs.cachePut(addr, rv)
		return rv, nil
	case Vector:
		rv := &Vectorblock{}
		rv.Deserialize(trimbuf)
		block_buf_pool.Put(syncbuf)
		rv.Identifier = addr
		rv.Generation = impl_Generation
		rv.PointWidth = impl_Pointwidth
		rv.StartTime = impl_StartTime
		bs.cachePut(addr, rv)
		return rv, nil
	}
	//This is quite bad, so panic instead of error
	lg.Panic("Strange datablock type")
	return nil, nil
}

func (bs *BlockStore) LoadSuperblock(ctx context.Context, id uuid.UUID, generation uint64) (*Superblock, bte.BTE) {
	if e := bte.CtxE(ctx); e != nil {
		return nil, e
	}
	if generation == LatestGeneration {
		cachedSB := bs.LoadSuperblockFromCache(id)
		if cachedSB != nil {
			atomic.AddUint64(&bs.sbcachehit, 1)
			pmSuperblockHits.Inc()
			return cachedSB, nil
		}
	}
	pmSuperblockMisses.Inc()
	atomic.AddUint64(&bs.sbcachemiss, 1)
	latestGen, err := bs.store.GetStreamVersion(ctx, id)
	if err != nil {
		return nil, bte.ErrW(bte.CephError, "could not get stream version", err)
	}
	if latestGen < bprovider.SpecialVersionCreated {
		return nil, nil
	}
	if latestGen == bprovider.SpecialVersionCreated {
		return NewSuperblock(id), nil
	}
	//Ok it exists and is not new
	if generation == LatestGeneration {
		generation = latestGen
	}
	if generation > latestGen {
		return nil, nil
	}
	sp := opentracing.StartSpan("ReadSuperblock")
	defer sp.Finish()
	buff := make([]byte, 16)
	sbarr, err := bs.store.ReadSuperBlock(ctx, id, generation, buff)
	if err != nil {
		return nil, bte.ErrW(bte.CephError, "could not read superblock", err)
	}
	if sbarr == nil {
		lg.Panicf("Your database is corrupt, superblock %d for stream %s should exist (but doesn't)", generation, id.String())
	}
	sb := DeserializeSuperblock(id, generation, sbarr)
	return sb, nil
}

func CreateDatabase(cfg configprovider.Configuration, overwrite bool) {
	if cfg.ClusterEnabled() {
		cp := new(cephprovider.CephStorageProvider)
		err := cp.CreateDatabase(cfg, overwrite)
		if err != nil {
			lg.Critical("Error on create: %v", err)
			os.Exit(1)
		}
	} else {
		panic("we no longer support the file storage engine")
		/*
			if err := os.MkdirAll(cfg.StorageFilepath(), 0755); err != nil {
				lg.Panic(err)
			}
			fp := new(fileprovider.FileStorageProvider)
			err := fp.CreateDatabase(cfg)
			if err != nil {
				lg.Critical("Error on create: %v", err)
				os.Exit(1)
			}
		*/
	}
}
