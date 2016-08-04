package bstore

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SoftwareDefinedBuildings/btrdb/internal/bprovider"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/cephprovider"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/configprovider"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/fileprovider"
	"github.com/pborman/uuid"
)

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
func NewBlockStore(cfg configprovider.Configuration) (*BlockStore, error) {
	bs := BlockStore{}
	bs.cfg = cfg
	bs.laschan = make(chan *LASMetric, 1000)
	bs.ccfg, _ = cfg.(configprovider.ClusterConfiguration)
	bs._wlocks = make(map[[16]byte]*sync.Mutex)
	bs.sbcache = make(map[[16]byte]*sbcachet, SUPERBLOCK_CACHE_SIZE)
	bs.alloc = make(chan uint64, 256)
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
		bs.store = new(fileprovider.FileStorageProvider)
	}

	bs.store.Initialize(cfg)
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
				lg.Infof("rawlp[las totalmax=%d,totalmed=%d,sortmax=%d,sortmed=%d,lockmax=%d,lockmed=%d,vbmax=%d,vbmed=%d,cbmax=%d,cbmed=%d,unlockmax=%d,unlockmed=%d,rate=%d]",
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
				}
			}
		}
	}
}

/*
 * This obtains a generation, blocking if necessary
 */
func (bs *BlockStore) ObtainGeneration(id uuid.UUID) *Generation {
	//The first thing we do is obtain a write lock on the UUID, as a generation
	//represents a lock
	if bs.ccfg != nil && !bs.ccfg.WeHoldWriteLockFor(id) {
		lg.Panicf("We do not have the write lock for %s", id.String())
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
	gen.Cur_SB = bs.LoadSuperblock(id, LatestGeneration)
	if gen.Cur_SB == nil {
		// ok, stream doesn't exist, just make one
		gen.Cur_SB = NewSuperblock(id)
	}

	gen.New_SB = gen.Cur_SB.CloneInc()
	gen.blockstore = bs
	return gen
}

//The returned address map is primarily for unit testing
func (gen *Generation) Commit() (map[uint64]uint64, error) {
	if gen.flushed {
		return nil, errors.New("Already Flushed")
	}

	address_map := LinkAndStore([]byte(*gen.Uuid()), gen.blockstore, gen.blockstore.store, gen.vblocks, gen.cblocks)
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

	gen.blockstore.store.WriteSuperBlock(gen.New_SB.uuid, gen.New_SB.gen, gen.New_SB.Serialize())
	gen.blockstore.store.SetStreamVersion(gen.New_SB.uuid, gen.New_SB.gen)
	gen.blockstore.PutSuperblockInCache(gen.New_SB)
	gen.flushed = true
	gen.blockstore.glock.RLock()
	gen.blockstore._wlocks[UUIDToMapKey(*gen.Uuid())].Unlock()
	gen.blockstore.glock.RUnlock()
	return address_map, nil
}

func (bs *BlockStore) allocateBlock() uint64 {
	relocation_address := <-bs.alloc
	return relocation_address
}

/**
 * The real function is supposed to allocate an address for the data
 * block, reserving it on disk, and then give back the data block that
 * can be filled in
 * This stub makes up an address, and mongo pretends its real
 */
func (gen *Generation) AllocateCoreblock() (*Coreblock, error) {
	cblock := &Coreblock{}
	cblock.Identifier = gen.blockstore.allocateBlock()
	cblock.Generation = gen.Number()
	gen.cblocks = append(gen.cblocks, cblock)
	return cblock, nil
}

func (gen *Generation) AllocateVectorblock() (*Vectorblock, error) {
	vblock := &Vectorblock{}
	vblock.Identifier = gen.blockstore.allocateBlock()
	vblock.Generation = gen.Number()
	gen.vblocks = append(gen.vblocks, vblock)
	return vblock, nil
}

func (bs *BlockStore) FreeCoreblock(cb **Coreblock) {
	*cb = nil
}

func (bs *BlockStore) FreeVectorblock(vb **Vectorblock) {
	*vb = nil
}

func (bs *BlockStore) ReadDatablock(uuid uuid.UUID, addr uint64, impl_Generation uint64, impl_Pointwidth uint8, impl_StartTime int64) Datablock {
	//Try hit the cache first
	db := bs.cacheGet(addr)
	if db != nil {
		return db
	}
	syncbuf := block_buf_pool.Get().([]byte)
	trimbuf := bs.store.Read([]byte(uuid), addr, syncbuf)
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
		return rv
	case Vector:
		rv := &Vectorblock{}
		rv.Deserialize(trimbuf)
		block_buf_pool.Put(syncbuf)
		rv.Identifier = addr
		rv.Generation = impl_Generation
		rv.PointWidth = impl_Pointwidth
		rv.StartTime = impl_StartTime
		bs.cachePut(addr, rv)
		return rv
	}
	lg.Panic("Strange datablock type")
	return nil
}

func (bs *BlockStore) LoadSuperblock(id uuid.UUID, generation uint64) *Superblock {
	if generation == LatestGeneration {
		cachedSB := bs.LoadSuperblockFromCache(id)
		if cachedSB != nil {
			atomic.AddUint64(&bs.sbcachehit, 1)
			return cachedSB
		}
	}
	atomic.AddUint64(&bs.sbcachemiss, 1)
	latestGen := bs.store.GetStreamVersion(id)
	if latestGen == 0 {
		return nil
	}
	if generation == LatestGeneration {
		generation = latestGen
	}
	if generation > latestGen {
		return nil
	}

	buff := make([]byte, 16)
	sbarr := bs.store.ReadSuperBlock(id, generation, buff)
	if sbarr == nil {
		lg.Panicf("Your database is corrupt, superblock %d for stream %s should exist (but doesn't)", generation, id.String())
	}
	sb := DeserializeSuperblock(id, generation, sbarr)
	return sb
}

func CreateDatabase(cfg configprovider.Configuration) {
	if cfg.ClusterEnabled() {
		cp := new(cephprovider.CephStorageProvider)
		err := cp.CreateDatabase(cfg)
		if err != nil {
			lg.Critical("Error on create: %v", err)
			os.Exit(1)
		}
	} else {
		if err := os.MkdirAll(cfg.StorageFilepath(), 0755); err != nil {
			lg.Panic(err)
		}
		fp := new(fileprovider.FileStorageProvider)
		err := fp.CreateDatabase(cfg)
		if err != nil {
			lg.Critical("Error on create: %v", err)
			os.Exit(1)
		}
	}
}
