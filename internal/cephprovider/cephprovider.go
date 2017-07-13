package cephprovider

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SoftwareDefinedBuildings/btrdb/internal/bprovider"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/configprovider"
	"github.com/ceph/go-ceph/rados"
	logging "github.com/op/go-logging"
)

var lg *logging.Logger

func init() {
	lg = logging.MustGetLogger("log")
}

const NUM_RHANDLES = 128
const NUM_WHANDLES = 128

//We know we won't get any addresses here, because this is the relocation base as well
const METADATA_BASE = 0xFF00000000000000

//4096 blocks per addr lock
const ADDR_LOCK_SIZE = 0x1000000000
const ADDR_OBJ_SIZE = 0x0001000000

//Just over the DBSIZE
const MAX_EXPECTED_OBJECT_SIZE = 20485

//The number of RADOS blocks to cache (up to 16MB each, probably only 1.6MB each)
const RADOS_CACHE_SIZE = NUM_RHANDLES * 2

const OFFSET_MASK = 0xFFFFFF

//This is how many uuid/address pairs we will keep to facilitate appending to segments
//instead of creating new ones.
const WORTH_CACHING = OFFSET_MASK - MAX_EXPECTED_OBJECT_SIZE
const SEGCACHE_SIZE = 1024

// 1MB for write cache, I doubt we will ever hit this tbh
const WCACHE_SIZE = 1 << 20

// Makes 16MB for 16B sblocks
const SBLOCK_CHUNK_SHIFT = 20
const SBLOCK_CHUNK_MASK = 0xFFFFF
const SBLOCKS_PER_CHUNK = 1 << SBLOCK_CHUNK_SHIFT
const SBLOCK_SIZE = 16

var provided_rh int64

func UUIDSliceToArr(id []byte) [16]byte {
	rv := [16]byte{}
	copy(rv[:], id)
	return rv
}

type CephSegment struct {
	h           *rados.IOContext
	sp          *CephStorageProvider
	ptr         uint64
	naddr       uint64
	base        uint64 //Not the same as the provider's base
	uid         [16]byte
	wcache      []byte
	wcache_base uint64
	hi          int //write handle index
}

type chunkreqindex struct {
	UUID [16]byte
	Addr uint64
}

type CephStorageProvider struct {
	rh           []*rados.IOContext
	conn         *rados.Conn
	rhidx        chan int
	rhidx_ret    chan int
	rh_avail     []bool
	wh           []*rados.IOContext
	whidx        chan int
	whidx_ret    chan int
	wh_avail     []bool
	ptr          uint64
	alloc        chan uint64
	segaddrcache map[[16]byte]uint64
	segcachelock sync.Mutex

	chunklock sync.Mutex
	chunkgate map[chunkreqindex][]chan []byte

	rcache *CephCache

	dataPool string
	hotPool  string

	cfg configprovider.Configuration

	annotationMu sync.Mutex
}

//Returns the address of the first free word in the segment when it was locked
func (seg *CephSegment) BaseAddress() uint64 {
	return seg.base
}

//Unlocks the segment for the StorageProvider to give to other consumers
//Implies a flush
func (seg *CephSegment) Unlock() {
	seg.flushWrite()
	seg.sp.whidx_ret <- seg.hi
	if (seg.naddr & OFFSET_MASK) < WORTH_CACHING {
		seg.sp.segcachelock.Lock()
		seg.sp.pruneSegCache()
		seg.sp.segaddrcache[seg.uid] = seg.naddr
		seg.sp.segcachelock.Unlock()
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
	seg.h.Write(oid, seg.wcache, offset)

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

	//OLD NOTE:
	//Note that it is ok for an object to "go past the end of the allocation". Naddr could be one byte before
	//the end of the allocation for example. This is not a problem as we never address anything except the
	//start of an object. This is why we do not add the object max size here
	//NEW NOTE:
	//We cannot go past the end of the allocation anymore because it would break the read cache
	if ((naddr + MAX_EXPECTED_OBJECT_SIZE + 2) >> 24) != (address >> 24) {
		//We are gonna need a new object addr
		naddr = <-seg.sp.alloc
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
func (sp *CephStorageProvider) pruneSegCache() {
	//This is extremely rare, so its best to handle it simply
	//If we drop the cache, we will get one shortsized object per stream,
	//and it won't necessarily be _very_ short.
	if len(sp.segaddrcache) >= SEGCACHE_SIZE {
		sp.segaddrcache = make(map[[16]byte]uint64, SEGCACHE_SIZE)
	}
}

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

func (sp *CephStorageProvider) provideAllocs() {
	base := sp.ptr
	for {
		sp.alloc <- sp.ptr
		sp.ptr += ADDR_OBJ_SIZE
		if sp.ptr >= base+ADDR_LOCK_SIZE {
			sp.ptr = sp.obtainBaseAddress()
			base = sp.ptr
		}
	}
}

func (sp *CephStorageProvider) GetRH() int {
	h := <-sp.rhidx
	return h
}
func (sp *CephStorageProvider) obtainBaseAddress() uint64 {
	addr := make([]byte, 8)
	hi := <-sp.rhidx
	h := sp.rh[hi]
	h.LockExclusive("allocator", "alloc_lock", "main", "alloc", 5*time.Second, nil)
	c, err := h.Read("allocator", addr, 0)
	if err != nil || c != 8 {
		h.Unlock("allocator", "alloc_lock", "main")
		sp.rhidx_ret <- hi
		return 0
	}
	le := binary.LittleEndian.Uint64(addr)
	ne := le + ADDR_LOCK_SIZE
	binary.LittleEndian.PutUint64(addr, ne)
	err = h.WriteFull("allocator", addr)
	if err != nil {
		panic("b")
	}
	h.Unlock("allocator", "alloc_lock", "main")
	sp.rhidx_ret <- hi
	return le
}

//Called at startup of a normal run
func (sp *CephStorageProvider) Initialize(cfg configprovider.Configuration) {
	//Allocate caches
	go func() {
		for {
			time.Sleep(10 * time.Second)
			lg.Infof("rawlp[%s %s=%d,%s=%d]", "cachegood", "actual", atomic.LoadInt64(&actualread), "used", atomic.LoadInt64(&readused))
		}
	}()
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

	sp.rh = make([]*rados.IOContext, NUM_RHANDLES)
	sp.rh_avail = make([]bool, NUM_RHANDLES)
	sp.rhidx = make(chan int, NUM_RHANDLES+1)
	sp.rhidx_ret = make(chan int, NUM_RHANDLES+1)
	sp.wh = make([]*rados.IOContext, NUM_RHANDLES)
	sp.wh_avail = make([]bool, NUM_WHANDLES)
	sp.whidx = make(chan int, NUM_WHANDLES+1)
	sp.whidx_ret = make(chan int, NUM_WHANDLES+1)
	sp.alloc = make(chan uint64, 128)
	sp.segaddrcache = make(map[[16]byte]uint64, SEGCACHE_SIZE)
	sp.chunkgate = make(map[chunkreqindex][]chan []byte)

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
	//Obtain base address
	sp.ptr = sp.obtainBaseAddress()
	if sp.ptr == 0 {
		lg.Panic("Could not read allocator! DB not created properly?")
	}
	lg.Infof("Base address obtained as 0x%016x", sp.ptr)

	//Start providing address allocations
	go sp.provideAllocs()

}

//Called to create the database for the first time
//This doesn't lock, but nobody else would be trying to do the same thing at
//the same time, so...
func (sp *CephStorageProvider) CreateDatabase(cfg configprovider.Configuration) error {
	cephpool := cfg.StorageCephDataPool()
	cephconf := cfg.StorageCephConf()
	conn, err := rados.NewConn()
	if err != nil {
		panic(err)
	}
	err = conn.ReadConfigFile(cephconf)
	if err != nil {
		lg.Panicf("Could not read ceph config: %v", err)
	}
	fmt.Printf("reading ceph config: %s pool %s ", cephconf, cephpool)
	err = conn.Connect()
	if err != nil {
		lg.Panicf("Could not initialize ceph storage (likely a ceph.conf error): %v", err)
	}

	h, err := conn.OpenIOContext(cephpool)
	if err != nil {
		lg.Panicf("Could not create the ceph allocator context: %v", err)
	}
	addr := uint64(0x1000000)
	baddr := make([]byte, 8)
	binary.LittleEndian.PutUint64(baddr, addr)
	err = h.WriteFull("allocator", baddr)
	if err != nil {
		lg.Panicf("Could not create the ceph allocator handle: %v", err)
	}
	h.Destroy()
	return nil
}

// Lock a segment, or block until a segment can be locked
// Returns a Segment struct
// Implicit unchecked assumption: you cannot lock more than one segment
// for a given uuid (without unlocking them in between). It will break
// segcache
func (sp *CephStorageProvider) LockSegment(uuid []byte) bprovider.Segment {
	rv := new(CephSegment)
	rv.sp = sp
	rv.hi = <-sp.whidx
	rv.h = sp.wh[rv.hi]
	rv.ptr = <-sp.alloc
	rv.uid = UUIDSliceToArr(uuid)
	rv.wcache = make([]byte, 0, WCACHE_SIZE)
	sp.segcachelock.Lock()
	cached_ptr, ok := sp.segaddrcache[rv.uid]
	if ok {
		delete(sp.segaddrcache, rv.uid)
	}
	sp.segcachelock.Unlock()
	//ok = false
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

func (sp *CephStorageProvider) rawObtainChunk(uuid []byte, address uint64) []byte {
	chunk := sp.rcache.cacheGet(address)
	if chunk == nil {
		chunk = sp.rcache.getBlank()
		rhidx := sp.GetRH()
		aa := address >> 24
		oid := fmt.Sprintf("%032x%010x", uuid, aa)
		offset := address & OFFSET_MASK
		rc, err := sp.rh[rhidx].Read(oid, chunk, offset)
		atomic.AddInt64(&actualread, int64(rc))
		if err != nil {
			lg.Panicf("ceph error: %v", err)
		}
		chunk = chunk[0:rc]
		sp.rhidx_ret <- rhidx
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

var exl_lock sync.Mutex

// Read the blob into the given buffer
func (sp *CephStorageProvider) Read(uuid []byte, address uint64, buffer []byte) []byte {
	//Get the first chunk for this object:
	chunk1 := sp.obtainChunk(uuid, address&R_ADDRMASK)[address&R_OFFSETMASK:]
	var chunk2 []byte
	var ln int

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
	exl_lock.Lock()
	_, ok := excludemap[address]
	if !ok {
		excludemap[address] = true
		readused += int64(ln)
	}
	exl_lock.Unlock()
	return buffer[:ln]

}

// Read the given version of superblock into the buffer.
// mebbeh we want to cache this?
func (sp *CephStorageProvider) ReadSuperBlock(uuid []byte, version uint64, buffer []byte) []byte {
	chunk := version >> SBLOCK_CHUNK_SHIFT
	offset := (version & SBLOCK_CHUNK_MASK) * SBLOCK_SIZE
	oid := fmt.Sprintf("sb%032x%011x", uuid, chunk)
	hi := sp.GetRH()
	h := sp.rh[hi]
	br, err := h.Read(oid, buffer, offset)
	if br != SBLOCK_SIZE || err != nil {
		lg.Panicf("unexpected sb read rv: %v %v offset=%v oid=%s version=%d bl=%d", br, err, offset, oid, version, len(buffer))
	}
	sp.rhidx_ret <- hi
	return buffer
}

// Writes a superblock of the given version
func (sp *CephStorageProvider) WriteSuperBlock(uuid []byte, version uint64, buffer []byte) {
	chunk := version >> SBLOCK_CHUNK_SHIFT
	offset := (version & SBLOCK_CHUNK_MASK) * SBLOCK_SIZE
	oid := fmt.Sprintf("sb%032x%011x", uuid, chunk)
	hi := <-sp.whidx
	h := sp.wh[hi]
	err := h.Write(oid, buffer, offset)
	if err != nil {
		lg.Panicf("unexpected sb write rv: %v", err)
	}
	sp.whidx_ret <- hi
}

// Sets the version of a stream. If it is in the past, it is essentially a rollback,
// and although no space is freed, the consecutive version numbers can be reused
// note to self: you must make sure not to call ReadSuperBlock on versions higher
// than you get from GetStreamVersion because they might succeed
func (sp *CephStorageProvider) SetStreamVersion(uuid []byte, version uint64) {
	oid := fmt.Sprintf("meta%032x", uuid)
	hi := sp.GetRH()
	h := sp.rh[hi]
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, version)
	err := h.SetXattr(oid, "version", data)
	if err != nil {
		lg.Panicf("ceph error: %v", err)
	}
	sp.rhidx_ret <- hi
}

// Gets the version of a stream. Returns 0 if none exists.
func (sp *CephStorageProvider) GetStreamVersion(uuid []byte) uint64 {
	oid := fmt.Sprintf("meta%032x", uuid)
	hi := sp.GetRH()
	h := sp.rh[hi]

	data := make([]byte, 8)
	bc, err := h.GetXattr(oid, "version", data)
	if err == rados.RadosErrorNotFound {
		sp.rhidx_ret <- hi
		return 0
	}
	if err != nil || bc != 8 {
		lg.Panicf("weird ceph error getting xattrs: %v", err)
	}
	sp.rhidx_ret <- hi
	ver := binary.LittleEndian.Uint64(data)
	return ver
}
