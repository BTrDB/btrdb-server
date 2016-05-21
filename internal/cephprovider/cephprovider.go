package cephprovider

// #cgo LDFLAGS: -lrados
// #include "cephprovider.h"
// #include <stdlib.h>
import "C"

import (
	"strconv"
	"sync"
	"unsafe"

	"github.com/SoftwareDefinedBuildings/btrdb/internal/bprovider"
	"github.com/op/go-logging"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

const NUM_RHANDLES = 200

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
const R_CHUNKSIZE = 1 << 20

//This is how many uuid/address pairs we will keep to facilitate appending to segments
//instead of creating new ones.
const WORTH_CACHING = OFFSET_MASK - MAX_EXPECTED_OBJECT_SIZE
const SEGCACHE_SIZE = 1024

// 1MB for write cache, I doubt we will ever hit this tbh
const WCACHE_SIZE = 1 << 20

func UUIDSliceToArr(id []byte) [16]byte {
	rv := [16]byte{}
	copy(rv[:], id)
	return rv
}

type CephSegment struct {
	h           C.phandle_t
	sp          *CephStorageProvider
	ptr         uint64
	naddr       uint64
	base        uint64 //Not the same as the provider's base
	warrs       [][]byte
	uid         [16]byte
	wcache      []byte
	wcache_base uint64
}

type chunkreqindex struct {
	UUID [16]byte
	Addr uint64
}

type CephStorageProvider struct {
	rh           []C.phandle_t
	rhidx        chan int
	rhidx_ret    chan int
	rh_avail     []bool
	ptr          uint64
	alloc        chan uint64
	segaddrcache map[[16]byte]uint64
	segcachelock sync.Mutex

	chunklock sync.Mutex
	chunkgate map[chunkreqindex][]chan []byte

	rcache *CephCache
}

//Returns the address of the first free word in the segment when it was locked
func (seg *CephSegment) BaseAddress() uint64 {
	return seg.base
}

//Unlocks the segment for the StorageProvider to give to other consumers
//Implies a flush
func (seg *CephSegment) Unlock() {
	seg.flushWrite()
	_, err := C.handle_close(seg.h)
	if err != nil {
		log.Panicf("CGO ERROR: %v", err)
	}
	seg.warrs = nil
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
	C.handle_write(seg.h, (*C.uint8_t)(unsafe.Pointer(&seg.uid[0])), C.uint64_t(seg.wcache_base),
		(*C.char)(unsafe.Pointer(&seg.wcache[0])), C.int(len(seg.wcache)), 0)

	for i := 0; i < len(seg.wcache); i += R_CHUNKSIZE {
		seg.sp.rcache.cacheInvalidate((uint64(i) + seg.wcache_base) & R_ADDRMASK)
	}
	//The C code does not finish immediately, so we need to keep a reference to the old
	//wcache array until the segment is unlocked
	seg.warrs = append(seg.warrs, seg.wcache)
	seg.wcache = make([]byte, 0, WCACHE_SIZE)
	seg.wcache_base = seg.naddr

}

//Writes a slice to the segment, returns immediately
//Returns nil if op is OK, otherwise ErrNoSpace or ErrInvalidArgument
//It is up to the implementer to work out how to report no space immediately
//The uint64 is the address to be used for the next write
func (seg *CephSegment) Write(uuid []byte, address uint64, data []byte) (uint64, error) {
	//We don't put written blocks into the cache, because those will be
	//in the dblock cache much higher up.
	if address != seg.naddr {
		log.Panic("Non-sequential write")
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
	ldretfi:
		for {
			select {
			case fi := <-sp.rhidx_ret:
				sp.rh_avail[fi] = true
			default:
				break ldretfi
			}
		}

		found := false
		for i := 0; i < NUM_RHANDLES; i++ {
			if sp.rh_avail[i] {
				sp.rhidx <- i
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

func (sp *CephStorageProvider) obtainBaseAddress() uint64 {
	h, err := C.handle_create()
	if err != nil {
		log.Panicf("CGO ERROR: %v", err)
	}
	addr, err := C.handle_obtainrange(h)
	if err != nil {
		log.Panicf("CGO ERROR: %v", err)
	}
	return uint64(addr)
}

//Called at startup of a normal run
func (sp *CephStorageProvider) Initialize(opts map[string]string) {
	//Allocate caches
	sp.rcache = &CephCache{}
	cachesz, _ := strconv.Atoi(opts["cephrcache"])
	if cachesz < 40 {
		cachesz = 40 //one per read handle: 40MB
	}
	sp.rcache.initCache(uint64(cachesz))

	cephconf := C.CString(opts["cephconf"])
	cephpool := C.CString(opts["cephpool"])
	_, err := C.initialize_provider(cephconf, cephpool)
	if err != nil {
		log.Panicf("CGO ERROR: %v", err)
	}
	C.free(unsafe.Pointer(cephconf))
	C.free(unsafe.Pointer(cephpool))

	sp.rh = make([]C.phandle_t, NUM_RHANDLES)
	sp.rh_avail = make([]bool, NUM_RHANDLES)
	sp.rhidx = make(chan int, NUM_RHANDLES+1)
	sp.rhidx_ret = make(chan int, NUM_RHANDLES+1)
	sp.alloc = make(chan uint64, 128)
	sp.segaddrcache = make(map[[16]byte]uint64, SEGCACHE_SIZE)
	sp.chunkgate = make(map[chunkreqindex][]chan []byte)

	for i := 0; i < NUM_RHANDLES; i++ {
		sp.rh_avail[i] = true
		h, err := C.handle_create()
		if err != nil {
			log.Panicf("CGO ERROR: %v", err)
		}
		sp.rh[i] = h
	}

	//Obtain base address
	sp.ptr = sp.obtainBaseAddress()
	if sp.ptr == 0 {
		log.Panic("Could not read allocator! DB not created properly?")
	}
	log.Infof("Base address obtained as 0x%016x", sp.ptr)

	//Start serving read handles
	go sp.provideReadHandles()

	//Start providing address allocations
	go sp.provideAllocs()

}

//Called to create the database for the first time
func (sp *CephStorageProvider) CreateDatabase(opts map[string]string) error {
	cephconf := C.CString(opts["cephconf"])
	cephpool := C.CString(opts["cephpool"])
	_, err := C.initialize_provider(cephconf, cephpool)
	if err != nil {
		log.Panicf("CGO ERROR: %v", err)
	}
	C.free(unsafe.Pointer(cephconf))
	C.free(unsafe.Pointer(cephpool))
	h, err := C.handle_create()
	if err != nil {
		log.Panicf("CGO ERROR: %v", err)
	}
	C.handle_init_allocator(h)
	_, err = C.handle_close(h)
	if err != nil {
		log.Panicf("CGO ERROR: %v", err)
	}
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
	h, err := C.handle_create()
	if err != nil {
		log.Panicf("CGO ERROR: %v", err)
	}
	rv.h = h
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
	//Although I don't know this for sure, I am concerned that when we pass the write array pointer to C
	//the Go GC may free it before C is done. I prevent this by pinning all the written arrays, which get
	//deref'd after the segment is unlocked
	rv.warrs = make([][]byte, 0, 64)
	return rv
}

func (sp *CephStorageProvider) rawObtainChunk(uuid []byte, address uint64) []byte {
	chunk := sp.rcache.cacheGet(address)
	if chunk == nil {
		chunk = sp.rcache.getBlank()
		rhidx := <-sp.rhidx
		rc, err := C.handle_read(sp.rh[rhidx], (*C.uint8_t)(unsafe.Pointer(&uuid[0])), C.uint64_t(address), (*C.char)(unsafe.Pointer(&chunk[0])), R_CHUNKSIZE)
		if err != nil {
			log.Panicf("CGO ERROR: %v", err)
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
/*
func (sp *CephStorageProvider) Read(uuid []byte, address uint64, buffer []byte) []byte {

	//Get a read handle
	rhidx := <-sp.rhidx
	if len(buffer) < MAX_EXPECTED_OBJECT_SIZE {
		log.Panic("That doesn't seem safe")
	}
	rc, err := C.handle_read(sp.rh[rhidx], (*C.uint8_t)(unsafe.Pointer(&uuid[0])), C.uint64_t(address), (*C.char)(unsafe.Pointer(&buffer[0])), MAX_EXPECTED_OBJECT_SIZE)
	if err != nil {
		log.Panicf("CGO ERROR: %v", err)
	}
	sp.rhidx_ret <- rhidx
	ln := int(buffer[0]) + (int(buffer[1]) << 8)
	if int(rc) < ln+2 {
		//TODO this can happen, it is better to just go back a few superblocks
		log.Panic("Short read")
	}
	return buffer[2 : ln+2]
}*/

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
		log.Panic("WTUF: ", ln)
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
		log.Panic("This is unexpected")
	}
	return buffer[:ln]

}
