package cephprovider

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BTrDB/btrdb-server/internal/bprovider"
	"github.com/BTrDB/btrdb-server/internal/configprovider"
	"github.com/BTrDB/btrdb-server/internal/rez"
	"github.com/ceph/go-ceph/rados"
	logging "github.com/op/go-logging"
)

var lg *logging.Logger

func init() {
	lg = logging.MustGetLogger("log")
}

const NUM_HOT_HANDLES = 128
const NUM_COLD_HANDLES = 128

//We know we won't get any addresses here, because this is the relocation base as well
const METADATA_BASE = 0xFF00000000000000

const INITIAL_COLD_BASE_ADDRESS = 0
const INITIAL_HOT_BASE_ADDRESS = 0x8000000000000000

//4096 blocks per addr lock
const ADDR_LOCK_SIZE = 0x1000000000
const ADDR_OBJ_SIZE = 0x0001000000

//Just over the DBSIZE
const MAX_EXPECTED_OBJECT_SIZE = 20485

//The number of RADOS blocks to cache (up to 16MB each, probably only 1.6MB each)
const RADOS_CACHE_SIZE = 512

const OFFSET_MASK = 0xFFFFFF

//This is how many uuid/address pairs we will keep to facilitate appending to segments
//instead of creating new ones.
const WORTH_CACHING = OFFSET_MASK - MAX_EXPECTED_OBJECT_SIZE
const SEGCACHE_SIZE = 100 * 1024

// 1MB for write cache, I doubt we will ever hit this tbh
const WCACHE_SIZE = 1 << 20

// Makes 16MB for 16B sblocks
const SBLOCK_CHUNK_SHIFT = 20
const SBLOCK_CHUNK_MASK = 0xFFFFF
const SBLOCKS_PER_CHUNK = 1 << SBLOCK_CHUNK_SHIFT
const SBLOCK_SIZE = 16

var provided_rh int64

func IsAddressHot(addr uint64) bool {
	return addr >= INITIAL_HOT_BASE_ADDRESS
}

func UUIDSliceToArr(id []byte) [16]byte {
	rv := [16]byte{}
	copy(rv[:], id)
	return rv
}

type CephSegment struct {
	ishot       bool
	h           *rados.IOContext
	rez         *rez.Resource
	sp          *CephStorageProvider
	ptr         uint64
	naddr       uint64
	base        uint64 //Not the same as the provider's base
	uid         [16]byte
	wcache      []byte
	wcache_base uint64
}

type chunkreqindex struct {
	UUID [16]byte
	Addr uint64
}

type CephStorageProvider struct {
	conn *rados.Conn
	/*
		rh           []*rados.IOContext

		rhidx        chan int
		rhidx_ret    chan int
		rh_avail     []bool
		wh           []*rados.IOContext
		whidx        chan int
		whidx_ret    chan int
		wh_avail     []bool*/

	hot_ptr    uint64
	hot_alloc  chan uint64
	cold_ptr   uint64
	cold_alloc chan uint64

	hot_segaddrcache  map[[16]byte]uint64
	hot_segcachelock  sync.Mutex
	cold_segaddrcache map[[16]byte]uint64
	cold_segcachelock sync.Mutex

	chunklock sync.Mutex
	chunkgate map[chunkreqindex][]chan []byte

	rcache *CephCache

	dataPool    string
	hotPool     string
	journalPool string

	hot_handle_q  chan *rados.IOContext
	cold_handle_q chan *rados.IOContext

	cfg configprovider.Configuration

	annotationMu sync.Mutex

	rm *rez.RezManager
}

// func (sp *CephStorageProvider) getHotHandle(ctx context.Context) (*rados.IOContext, error) {
// 	hnd, err := sp.rm.Get(ctx, rez.CephHotHandle)
// 	if err != nil {
// 		return hnd.(
// 	}
// 	return <-sp.hot_handle_q
// }
// func (sp *CephStorageProvider) returnHotHandle(c *rados.IOContext) {
// 	sp.hot_handle_q <- c
// }
// func (sp *CephStorageProvider) getColdHandle() *rados.IOContext {
// 	return <-sp.cold_handle_q
// }
func (sp *CephStorageProvider) getHandle(ctx context.Context, ishot bool) (*rez.Resource, *rados.IOContext, error) {
	var h *rez.Resource
	var err error
	if ishot {
		h, err = sp.rm.Get(ctx, rez.CephHotHandle)
	} else {
		h, err = sp.rm.Get(ctx, rez.CephColdHandle)
	}
	if err != nil {
		return nil, nil, err
	}
	return h, h.Val().(*rados.IOContext), nil
}

// func (sp *CephStorageProvider) returnColdHandle(c *rados.IOContext) {
// 	sp.cold_handle_q <- c
// }
func (sp *CephStorageProvider) initializeHotHandles() {
	sp.rm.CreateResourcePool(rez.CephHotHandle, func() interface{} {
		ioctx, err := sp.conn.OpenIOContext(sp.hotPool)
		if err != nil {
			panic(err)
		}
		return ioctx
	}, func(v interface{}) {
		v.(*rados.IOContext).Destroy()
	})
	//
	// sp.hot_handle_q = make(chan *rados.IOContext, NUM_HOT_HANDLES)
	// for i := 0; i < NUM_HOT_HANDLES; i++ {
	// 	h, err := sp.conn.OpenIOContext(sp.hotPool)
	// 	if err != nil {
	// 		lg.Panicf("Could not open ceph hot handle: %v", err)
	// 	}
	// 	sp.hot_handle_q <- h
	// }
}

func (sp *CephStorageProvider) initializeColdHandles() {
	sp.rm.CreateResourcePool(rez.CephColdHandle, func() interface{} {
		ioctx, err := sp.conn.OpenIOContext(sp.dataPool)
		if err != nil {
			panic(err)
		}
		return ioctx
	}, func(v interface{}) {
		v.(*rados.IOContext).Destroy()
	})

	// sp.cold_handle_q = make(chan *rados.IOContext, NUM_COLD_HANDLES)
	// for i := 0; i < NUM_COLD_HANDLES; i++ {
	// 	h, err := sp.conn.OpenIOContext(sp.dataPool)
	// 	if err != nil {
	// 		lg.Panicf("Could not open ceph cold handle: %v", err)
	// 	}
	// 	sp.cold_handle_q <- h
	// }
}

//Returns the address of the first free word in the segment when it was locked
func (seg *CephSegment) BaseAddress() uint64 {
	return seg.base
}

//Unlocks the segment for the StorageProvider to give to other consumers
//Implies a flush
func (seg *CephSegment) Unlock() {
	seg.flushWrite()
	seg.rez.Release()
	if seg.ishot {
		if (seg.naddr & OFFSET_MASK) < WORTH_CACHING {
			seg.sp.hot_segcachelock.Lock()
			seg.sp.hotPruneSegCache()
			seg.sp.hot_segaddrcache[seg.uid] = seg.naddr
			seg.sp.hot_segcachelock.Unlock()
		}

	} else {
		if (seg.naddr & OFFSET_MASK) < WORTH_CACHING {
			seg.sp.cold_segcachelock.Lock()
			seg.sp.coldPruneSegCache()
			seg.sp.cold_segaddrcache[seg.uid] = seg.naddr
			seg.sp.cold_segcachelock.Unlock()
		}
	}
}

func (seg *CephSegment) flushWrite() {
	if len(seg.wcache) == 0 {
		return
	}
	address := seg.wcache_base
	aa := address >> 24
	oid := fmt.Sprintf("%032x%010x", seg.uid, aa)
	offset := address & OFFSET_MASK
	err := seg.h.Write(oid, seg.wcache, offset)
	if err != nil {
		panic(fmt.Errorf("ceph write error: %v", err))
	}
	for i := 0; i < len(seg.wcache); i += R_CHUNKSIZE {
		seg.sp.rcache.cacheInvalidate((uint64(i) + seg.wcache_base) & R_ADDRMASK)
	}
	seg.wcache = make([]byte, 0, WCACHE_SIZE)
	seg.wcache_base = seg.naddr
}

var totalbytes int64

//Writes a slice to the segment, returns immediately
//Returns nil if op is OK, otherwise ErrNoSpace or ErrInvalidArgument
//It is up to the implementer to work out how to report no space immediately
//The uint64 is the address to be used for the next write
func (seg *CephSegment) Write(uuid []byte, address uint64, data []byte) (uint64, error) {
	atomic.AddInt64(&totalbytes, int64(len(data)))
	//We don't put written blocks into the cache, because those will be
	//in the dblock cache much higher up.
	if address != seg.naddr {
		lg.Panic("Non-sequential write")
	}

	if len(seg.wcache)+len(data)+2 > cap(seg.wcache) {
		seg.flushWrite()
	}

	base := len(seg.wcache)
	seg.wcache = seg.wcache[:base+2]
	seg.wcache[base] = byte(len(data))
	seg.wcache[base+1] = byte(len(data) >> 8)
	seg.wcache = append(seg.wcache, data...)

	naddr := address + uint64(len(data)+2)

	//We cannot go past the end of the allocation anymore because it would break the read cache
	if ((naddr + MAX_EXPECTED_OBJECT_SIZE + 2) >> 24) != (address >> 24) {
		//We are gonna need a new object addr
		if seg.ishot {
			naddr = <-seg.sp.hot_alloc
		} else {
			naddr = <-seg.sp.cold_alloc
		}
		seg.naddr = naddr
		seg.flushWrite()
		return naddr, nil
	}
	seg.naddr = naddr

	return naddr, nil
}

//Block until all writes are complete. Note this does not imply a flush of the underlying files.
func (seg *CephSegment) Flush() {
	//Not sure we need to do stuff here, we can do it in unlock
}

//Must be called with the cache lock held
func (sp *CephStorageProvider) coldPruneSegCache() {
	//This is extremely rare, so its best to handle it simply
	//If we drop the cache, we will get one shortsized object per stream,
	//and it won't necessarily be _very_ short.
	if len(sp.cold_segaddrcache) >= SEGCACHE_SIZE {
		sp.cold_segaddrcache = make(map[[16]byte]uint64, SEGCACHE_SIZE)
	}
}

//Must be called with the cache lock held
func (sp *CephStorageProvider) hotPruneSegCache() {
	//This is extremely rare, so its best to handle it simply
	//If we drop the cache, we will get one shortsized object per stream,
	//and it won't necessarily be _very_ short.
	if len(sp.hot_segaddrcache) >= SEGCACHE_SIZE {
		sp.hot_segaddrcache = make(map[[16]byte]uint64, SEGCACHE_SIZE)
	}
}

/*
func (sp *CephStorageProvider) provideReadHandles() {
	for {
		//Read all returned read handles
	ldretfir:
		for {
			select {
			case fi := <-sp.rhidx_ret:
				sp.rh_avail[fi] = true
			default:
				break ldretfir
			}
		}

		found := false
		for i := 0; i < NUM_RHANDLES; i++ {
			if sp.rh_avail[i] {
				sp.rhidx <- i
				provided_rh += 1
				sp.rh_avail[i] = false
				found = true
			}
		}
		//If we didn't find one, do a blocking read
		if !found {
			idx := <-sp.rhidx_ret
			sp.rh_avail[idx] = true
		}
	}
}

func (sp *CephStorageProvider) provideWriteHandles() {
	for {
		//Read all returned write handles
	ldretfiw:
		for {
			select {
			case fi := <-sp.whidx_ret:
				sp.wh_avail[fi] = true
			default:
				break ldretfiw
			}
		}

		found := false
		for i := 0; i < NUM_WHANDLES; i++ {
			if sp.wh_avail[i] {
				sp.whidx <- i
				sp.wh_avail[i] = false
				found = true
			}
		}
		//If we didn't find one, do a blocking read
		if !found {
			idx := <-sp.whidx_ret
			sp.wh_avail[idx] = true
		}
	}
}
*/
func (sp *CephStorageProvider) hotProvideAllocs() {
	base := sp.hot_ptr
	ioctx, err := sp.conn.OpenIOContext(sp.hotPool)
	if err != nil {
		panic(err)
	}
	for {
		sp.hot_alloc <- sp.hot_ptr
		sp.hot_ptr += ADDR_OBJ_SIZE
		if sp.hot_ptr >= base+ADDR_LOCK_SIZE {
			sp.hot_ptr = sp.hotObtainBaseAddress(ioctx)
			base = sp.hot_ptr
		}
	}
}

func (sp *CephStorageProvider) coldProvideAllocs() {
	base := sp.cold_ptr
	ioctx, err := sp.conn.OpenIOContext(sp.dataPool)
	if err != nil {
		panic(err)
	}
	for {
		sp.cold_alloc <- sp.cold_ptr
		sp.cold_ptr += ADDR_OBJ_SIZE
		if sp.cold_ptr >= base+ADDR_LOCK_SIZE {
			sp.cold_ptr = sp.coldObtainBaseAddress(ioctx)
			base = sp.cold_ptr
		}
	}
}

func (sp *CephStorageProvider) coldObtainBaseAddress(h *rados.IOContext) uint64 {
	addr := make([]byte, 8)
	h.LockExclusive("cold_allocator", "cold_alloc_lock", "cold_main", "cold_alloc", 10*time.Second, nil)
	c, err := h.Read("cold_allocator", addr, 0)
	if err != nil || c != 8 {
		h.Unlock("cold_allocator", "cold_alloc_lock", "cold_main")
		return 0
	}
	le := binary.LittleEndian.Uint64(addr)
	ne := le + ADDR_LOCK_SIZE
	if ne >= INITIAL_HOT_BASE_ADDRESS {
		panic("wtf how did we run out of cold address space")
	}
	binary.LittleEndian.PutUint64(addr, ne)
	err = h.WriteFull("cold_allocator", addr)
	if err != nil {
		panic("could not writeback the cold allocator object")
	}
	h.Unlock("cold_allocator", "cold_alloc_lock", "cold_main")
	return le
}

func (sp *CephStorageProvider) hotObtainBaseAddress(h *rados.IOContext) uint64 {
	addr := make([]byte, 8)
	h.LockExclusive("hot_allocator", "hot_alloc_lock", "hot_main", "hot_alloc", 10*time.Second, nil)
	c, err := h.Read("hot_allocator", addr, 0)
	if err != nil || c != 8 {
		h.Unlock("hot_allocator", "hot_alloc_lock", "hot_main")
		return 0
	}
	le := binary.LittleEndian.Uint64(addr)
	ne := le + ADDR_LOCK_SIZE
	if ne >= METADATA_BASE {
		panic("wtf how did we run out of hot address space")
	}
	binary.LittleEndian.PutUint64(addr, ne)
	err = h.WriteFull("hot_allocator", addr)
	if err != nil {
		panic("could not writeback the hot allocator object")
	}
	h.Unlock("hot_allocator", "hot_alloc_lock", "hot_main")
	return le
}

//Called at startup of a normal run
func (sp *CephStorageProvider) Initialize(cfg configprovider.Configuration, rm *rez.RezManager) {
	//Allocate caches
	// go func() {
	// 	for {
	// 		time.Sleep(10 * time.Second)
	// 		lg.Infof("rawlp[%s %s=%d,%s=%d]", "cachegood", "actual", atomic.LoadInt64(&actualread), "used", atomic.LoadInt64(&readused))
	// 	}
	// }()
	sp.cfg = cfg
	sp.rcache = &CephCache{}
	cachesz := cfg.RadosReadCache()
	if cachesz < 40 {
		cachesz = 40 //one per read handle: 40MB
	}
	sp.rcache.initCache(uint64(cachesz))
	conn, err := rados.NewConn()
	if err != nil {
		lg.Panicf("Could not initialize ceph storage: %v", err)
	}
	err = conn.ReadConfigFile(cfg.StorageCephConf())
	if err != nil {
		lg.Panicf("Could not read ceph config: %v", err)
	}
	err = conn.Connect()
	if err != nil {
		lg.Panicf("Could not initialize ceph storage: %v", err)
	}
	sp.conn = conn
	sp.dataPool = cfg.StorageCephDataPool()
	sp.hotPool = cfg.StorageCephHotPool()
	sp.journalPool = cfg.StorageCephJournalPool()
	sp.rm = rm
	sp.hot_alloc = make(chan uint64, 128)
	sp.hot_segaddrcache = make(map[[16]byte]uint64, SEGCACHE_SIZE)
	sp.cold_alloc = make(chan uint64, 128)
	sp.cold_segaddrcache = make(map[[16]byte]uint64, SEGCACHE_SIZE)
	sp.chunkgate = make(map[chunkreqindex][]chan []byte)

	sp.initializeHotHandles()
	sp.initializeColdHandles()
	/*
		for i := 0; i < NUM_RHANDLES; i++ {
			sp.rh_avail[i] = true
			h, err := conn.OpenIOContext(sp.dataPool)
			if err != nil {
				lg.Panicf("Could not open CEPH: %v", err)
			}
			sp.rh[i] = h
		}

		for i := 0; i < NUM_WHANDLES; i++ {
			sp.wh_avail[i] = true
			h, err := conn.OpenIOContext(sp.dataPool)
			if err != nil {
				lg.Panicf("Could not open CEPH: %v", err)
			}
			sp.wh[i] = h
		}

		//Start serving read handles
		go sp.provideReadHandles()
		go sp.provideWriteHandles()
	*/

	coldrez, coldh, err := sp.getHandle(context.Background(), false)
	if err != nil {
		panic(err)
	}
	//Obtain base address
	sp.cold_ptr = sp.coldObtainBaseAddress(coldh)
	if sp.cold_ptr == 0 {
		lg.Panic("Could not read allocator for cold pool! Has the DB been created properly?")
	}
	lg.Infof("Base address in cold pool obtained as 0x%016x", sp.cold_ptr)
	coldrez.Release()

	hotrez, hoth, err := sp.getHandle(context.Background(), true)
	if err != nil {
		panic(err)
	}
	sp.hot_ptr = sp.hotObtainBaseAddress(hoth)
	if sp.hot_ptr == 0 {
		lg.Panic("Could not read allocator for hot pool! Has the DB been created properly?")
	}
	lg.Infof("Base address in hot pool obtained as 0x%016x", sp.hot_ptr)
	hotrez.Release()

	go sp.coldProvideAllocs()
	go sp.hotProvideAllocs()
}

//Called to create the database for the first time
//This doesn't lock, but nobody else would be trying to do the same thing at
//the same time, so...
func (sp *CephStorageProvider) CreateDatabase(cfg configprovider.Configuration, overwrite bool) error {
	coldpool := cfg.StorageCephDataPool()
	hotpool := cfg.StorageCephHotPool()
	cephconf := cfg.StorageCephConf()
	conn, err := rados.NewConn()
	if err != nil {
		panic(err)
	}
	err = conn.ReadConfigFile(cephconf)
	if err != nil {
		lg.Panicf("Could not read ceph config: %v", err)
	}
	fmt.Printf("reading ceph config: %s hotpool=%s coldpool=%s\n", cephconf, hotpool, coldpool)
	err = conn.Connect()
	if err != nil {
		lg.Panicf("Could not initialize ceph storage (likely a ceph.conf error): %v", err)
	}
	fmt.Printf("connection OK, opening cold IO context\n")
	coldh, err := conn.OpenIOContext(coldpool)
	if err != nil {
		lg.Panicf("Could not create the ceph allocator context for the cold pool: %v", err)
	}
	fmt.Printf("cold OK, opening hot IO context\n")
	hoth, err := conn.OpenIOContext(hotpool)
	if err != nil {
		lg.Panicf("Could not create the ceph allocator context for the hot pool: %v", err)
	}
	fmt.Printf("checking for cold allocator\n")
	coldoid := "cold_allocator"
	cstatres, err := coldh.Stat(coldoid)
	if !overwrite && (cstatres.Size != 0 || err != rados.RadosErrorNotFound) {
		fmt.Printf("Not initializing cold pool: allocator already there\n")
	} else {
		//Check if there is a legacy allocator
		fmt.Printf("checking for legacy allocator\n")
		legacyoid := "allocator"
		lstatres, _ := coldh.Stat(legacyoid)
		data := make([]byte, 8)
		if lstatres.Size == 8 {
			if coldpool != hotpool {
				fmt.Printf("migrating mandatory hot objects\n")
				migrateMandatoryHotObjects(coldh, hoth)
			}
			fmt.Printf("[MIGRATE] porting legacy allocator\n")
			count, rerr := coldh.Read(legacyoid, data, 0)
			if count != 8 || rerr != nil {
				lg.Panicf("could not read legacy allocator")
			}
		} else {
			fmt.Printf("Creating blank cold allocator\n")
			addr := uint64(INITIAL_COLD_BASE_ADDRESS + ADDR_LOCK_SIZE)
			binary.LittleEndian.PutUint64(data, addr)
		}
		fmt.Printf("Initializing cold pool\n")
		err = coldh.WriteFull("cold_allocator", data)
		if err != nil {
			lg.Panicf("Could not create the ceph cold allocator handle: %v", err)
		}
	}
	coldh.Destroy()

	hotoid := "hot_allocator"
	hstatres, err := hoth.Stat(hotoid)
	if !overwrite && (hstatres.Size != 0 || err != rados.RadosErrorNotFound) {
		fmt.Printf("Not initializing cold pool: allocator already there\n")
	} else {
		fmt.Printf("Initializing hot pool\n")
		addr := uint64(INITIAL_HOT_BASE_ADDRESS + ADDR_LOCK_SIZE)
		baddr := make([]byte, 8)
		binary.LittleEndian.PutUint64(baddr, addr)
		err = hoth.WriteFull("hot_allocator", baddr)
		if err != nil {
			lg.Panicf("Could not create the ceph hot allocator handle: %v", err)
		}
	}
	hoth.Destroy()
	return nil
}

func migrateMandatoryHotObjects(cold *rados.IOContext, hot *rados.IOContext) {
	fmt.Printf("[MIGRATE] DETECTED AN UPGRADE TO TIERED STORAGE\n")
	fmt.Printf("[MIGRATE] We need to scan the cold pool for objects that\n")
	fmt.Printf("[MIGRATE] need to move to the hot pool.\n")
	scanned := uint64(0)
	moved := uint64(0)
	bufsz := 17 * 1024 * 1024
	buf := make([]byte, bufsz)
	lfunc := func(oid string) {
		buf = buf[:bufsz]
		scanned++
		if strings.HasPrefix(oid, "sb") {
			nread, err := cold.Read(oid, buf, 0)
			if err != nil {
				lg.Panicf("Failed to read object for migration: %v", err)
			}
			if nread == bufsz {
				lg.Panicf("Unexpected massive object for migration: %d", nread)
			}
			if nread != 0 {
				buf = buf[:nread]
				err = hot.WriteFull(oid, buf)
				if err != nil {
					lg.Panicf("Failed to write object for migration: %v", err)
				}
			}
			moved++
		}
		if strings.HasPrefix(oid, "meta") {
			nread, err := cold.GetXattr(oid, "version", buf)
			if err != nil {
				lg.Panicf("Failed to read object xattr for migration: %v", err)
			}
			err = hot.SetXattr(oid, "version", buf[:nread])
			if err != nil {
				lg.Panicf("Failed to set object xattr for migration: %v", err)
			}
			moved++
		}
		if scanned%1000 == 0 {
			fmt.Printf("[MIGRATE] %d k objects scanned, %d objects migrated\n", scanned/1000, moved)
		}
	}
	err := cold.ListObjects(lfunc)
	if err != nil {
		lg.Panicf("Failed to scan objects for migration: %v", err)
	}
	fmt.Printf("[MIGRATE] object migration complete %d k objects scanned, %d objects migrated.\n", scanned/1000, moved)
}

// Lock a segment, or block until a segment can be locked
// Returns a Segment struct
// Implicit unchecked assumption: you cannot lock more than one segment
// for a given uuid (without unlocking them in between). It will break
// segcache
func (sp *CephStorageProvider) lockSegment(uuid []byte, ishot bool) bprovider.Segment {
	rv := new(CephSegment)
	rv.sp = sp
	rv.ishot = ishot
	rezh, h, err := sp.getHandle(context.Background(), ishot)
	if err != nil {
		panic(err)
	}
	rv.rez = rezh
	rv.h = h
	if ishot {
		rv.ptr = <-sp.hot_alloc
	} else {
		rv.ptr = <-sp.cold_alloc
	}
	rv.uid = UUIDSliceToArr(uuid)
	rv.wcache = make([]byte, 0, WCACHE_SIZE)
	var cached_ptr uint64
	var ok bool
	if ishot {
		sp.hot_segcachelock.Lock()
		cached_ptr, ok = sp.hot_segaddrcache[rv.uid]
		if ok {
			delete(sp.hot_segaddrcache, rv.uid)
		}
		sp.hot_segcachelock.Unlock()
	} else {
		sp.cold_segcachelock.Lock()
		cached_ptr, ok = sp.cold_segaddrcache[rv.uid]
		if ok {
			delete(sp.cold_segaddrcache, rv.uid)
		}
		sp.cold_segcachelock.Unlock()
	}
	if ok {
		rv.base = cached_ptr
		rv.naddr = rv.base
	} else {
		rv.base = rv.ptr
		rv.naddr = rv.base
	}
	rv.wcache_base = rv.naddr
	return rv
}

func (sp *CephStorageProvider) LockCoreSegment(uuid []byte) bprovider.Segment {
	return sp.lockSegment(uuid, true)
}

func (sp *CephStorageProvider) LockVectorSegment(uuid []byte) bprovider.Segment {
	return sp.lockSegment(uuid, false)
}

func (sp *CephStorageProvider) rawObtainChunk(uuid []byte, address uint64) []byte {
	ishot := IsAddressHot(address)
	chunk := sp.rcache.cacheGet(address)
	if chunk == nil {
		chunk = sp.rcache.getBlank()
		rhnd, hnd, err := sp.getHandle(context.Background(), ishot)
		if err != nil {
			panic(err)
		}
		aa := address >> 24
		oid := fmt.Sprintf("%032x%010x", uuid, aa)
		offset := address & OFFSET_MASK
		rc, err := hnd.Read(oid, chunk, offset)
		if err != nil {
			lg.Panicf("ceph error: %v", err)
		}
		chunk = chunk[0:rc]
		rhnd.Release()
		sp.rcache.cachePut(address, chunk)
	}
	return chunk
}

func (sp *CephStorageProvider) obtainChunk(uuid []byte, address uint64) []byte {
	chunk := sp.rcache.cacheGet(address)
	if chunk != nil {
		return chunk
	}
	index := chunkreqindex{UUID: UUIDSliceToArr(uuid), Addr: address}
	rvc := make(chan []byte, 1)
	sp.chunklock.Lock()
	slc, ok := sp.chunkgate[index]
	if ok {
		sp.chunkgate[index] = append(slc, rvc)
		sp.chunklock.Unlock()
	} else {
		sp.chunkgate[index] = []chan []byte{rvc}
		sp.chunklock.Unlock()
		go func() {
			bslice := sp.rawObtainChunk(uuid, address)
			sp.chunklock.Lock()
			slc, ok := sp.chunkgate[index]
			if !ok {
				panic("inconsistency!!")
			}
			for _, chn := range slc {
				chn <- bslice
			}
			delete(sp.chunkgate, index)
			sp.chunklock.Unlock()
		}()
	}
	rv := <-rvc
	return rv
}

// Read the blob into the given buffer: direct read

// func (sp *CephStorageProvider) Read(uuid []byte, address uint64, buffer []byte) []byte {
// 	rhidx := sp.GetRH()
// 	aa := address >> 24
// 	oid := fmt.Sprintf("%032x%010x", uuid, aa)
// 	offset := address & OFFSET_MASK
// 	buffer = buffer[:MAX_EXPECTED_OBJECT_SIZE]
// 	rc, err := sp.rh[rhidx].Read(oid, buffer, offset)
// 	if err != nil {
// 		panic(fmt.Errorf("nread error %v", err))
// 	}
// 	ln := int(buffer[0]) + (int(buffer[1]) << 8)
// 	if int(rc) < ln+2 {
// 		//TODO this can happen, it is better to just go back a few superblocks
// 		lg.Panic("Short read")
// 	}
// 	sp.rhidx_ret <- rhidx
// 	return buffer[2 : ln+2]
// }

// Read the blob into the given buffer
func (sp *CephStorageProvider) Read(ctx context.Context, uuid []byte, address uint64, buffer []byte) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	//Get the first chunk for this object:
	chunk1 := sp.obtainChunk(uuid, address&R_ADDRMASK)[address&R_OFFSETMASK:]
	var chunk2 []byte
	var ln int

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if len(chunk1) < 2 {
		//not even long enough for the prefix, must be one byte in the first chunk, one in teh second
		chunk2 = sp.obtainChunk(uuid, (address+R_CHUNKSIZE)&R_ADDRMASK)
		ln = int(chunk1[0]) + (int(chunk2[0]) << 8)
		chunk2 = chunk2[1:]
		chunk1 = chunk1[1:]
	} else {
		ln = int(chunk1[0]) + (int(chunk1[1]) << 8)
		chunk1 = chunk1[2:]
	}

	if (ln) > MAX_EXPECTED_OBJECT_SIZE {
		lg.Panic("WTUF: ", ln)
	}

	copied := 0
	if len(chunk1) > 0 {
		//We need some bytes from chunk1
		end := ln
		if len(chunk1) < ln {
			end = len(chunk1)
		}
		copied = copy(buffer, chunk1[:end])
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if copied < ln {
		//We need some bytes from chunk2
		if chunk2 == nil {
			chunk2 = sp.obtainChunk(uuid, (address+R_CHUNKSIZE)&R_ADDRMASK)
		}
		copy(buffer[copied:], chunk2[:ln-copied])

	}
	if ln < 2 {
		lg.Panic("This is unexpected")
	}
	return buffer[:ln], nil

}

// Read the given version of superblock into the buffer.
// mebbeh we want to cache this?
func (sp *CephStorageProvider) ReadSuperBlock(ctx context.Context, uuid []byte, version uint64, buffer []byte) ([]byte, error) {
	chunk := version >> SBLOCK_CHUNK_SHIFT
	offset := (version & SBLOCK_CHUNK_MASK) * SBLOCK_SIZE
	oid := fmt.Sprintf("sb%032x%011x", uuid, chunk)
	rez, h, err := sp.getHandle(ctx, true)
	if err != nil {
		return nil, err
	}
	br, err := h.Read(oid, buffer, offset)
	if br != SBLOCK_SIZE || err != nil {
		lg.Panicf("unexpected sb read rv: %v %v offset=%v oid=%s version=%d bl=%d", br, err, offset, oid, version, len(buffer))
	}
	rez.Release()
	return buffer, nil
}

// Writes a superblock of the given version
func (sp *CephStorageProvider) WriteSuperBlock(uuid []byte, version uint64, buffer []byte) {
	chunk := version >> SBLOCK_CHUNK_SHIFT
	offset := (version & SBLOCK_CHUNK_MASK) * SBLOCK_SIZE
	oid := fmt.Sprintf("sb%032x%011x", uuid, chunk)
	rez, h, err := sp.getHandle(context.Background(), true)
	if err != nil {
		panic(err)
	}
	err = h.Write(oid, buffer, offset)
	if err != nil {
		lg.Panicf("unexpected sb write rv: %v", err)
	}
	rez.Release()
}

// Sets the version of a stream. If it is in the past, it is essentially a rollback,
// and although no space is freed, the consecutive version numbers can be reused
// note to self: you must make sure not to call ReadSuperBlock on versions higher
// than you get from GetStreamVersion because they might succeed
func (sp *CephStorageProvider) SetStreamVersion(uuid []byte, version uint64) {
	oid := fmt.Sprintf("meta%032x", uuid)
	rez, h, err := sp.getHandle(context.Background(), true)
	if err != nil {
		panic(err)
	}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, version)
	err = h.SetXattr(oid, "version", data)
	if err != nil {
		lg.Panicf("ceph error: %v", err)
	}
	rez.Release()
}

// Gets the version of a stream. Returns 0 if none exists.
func (sp *CephStorageProvider) GetStreamVersion(ctx context.Context, uuid []byte) (uint64, error) {
	oid := fmt.Sprintf("meta%032x", uuid)
	rez, h, err := sp.getHandle(context.Background(), true)
	if err != nil {
		return 0, err
	}
	data := make([]byte, 8)
	bc, err := h.GetXattr(oid, "version", data)
	if err == rados.RadosErrorNotFound {
		rez.Release()
		return 0, nil
	}
	if err != nil || bc != 8 {
		lg.Panicf("weird ceph error getting xattrs: %v", err)
	}
	rez.Release()
	ver := binary.LittleEndian.Uint64(data)
	return ver, nil
}

func (sp *CephStorageProvider) ObliterateStreamMetadata(uuid []byte) {
	oid := fmt.Sprintf("meta%032x", uuid)
	rez, h, err := sp.getHandle(context.Background(), true)
	err = h.Delete(oid)
	if err != nil && err != rados.RadosErrorNotFound {
		lg.Panicf("weird ceph error obliterating meta: %v", err)
	}
	rez.Release()
	return
}
