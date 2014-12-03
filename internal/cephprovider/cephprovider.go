package cephprovider

// #cgo LDFLAGS: -lrados
// #include "cephprovider.h"
import "C"

import (
	"github.com/op/go-logging"
	"unsafe"
	"github.com/SoftwareDefinedBuildings/quasar/internal/bprovider"
	)
var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

//I'm going for one per core on a decent server
const NUM_RHANDLES = 40

//We know we won't get any addresses here, because this is the relocation base as well
const METADATA_BASE = 0xFF00000000000000

//4096 blocks per addr lock
const ADDR_LOCK_SIZE = 0x1000000000
const ADDR_OBJ_SIZE  = 0x0001000000

//Just over the DBSIZE
const MAX_EXPECTED_OBJECT_SIZE = 20485

//The number of RADOS blocks to cache (up to 16MB each, probably only 1.6MB each)
const RADOS_CACHE_SIZE = NUM_RHANDLES*2

const OFFSET_MASK = 0xFFFFFF

type CephSegment struct {
	h	 C.phandle_t
	sp	 *CephStorageProvider
	ptr	 uint64
	base uint64
	warrs [][]byte
}

type CephStorageProvider struct {
	rh 			[]C.phandle_t 
	rhidx 		chan int
	rhidx_ret	chan int
	rh_avail	[]bool
	ptr			uint64
	alloc		chan uint64
}

//Returns the address of the first free word in the segment when it was locked
func (seg *CephSegment) BaseAddress() uint64 {
	return seg.base
}

//Unlocks the segment for the StorageProvider to give to other consumers
//Implies a flush
func (seg *CephSegment) Unlock() {
	_, err := C.handle_close(seg.h)
	if err != nil {
		log.Panic("CGO ERROR: %v",err)
	}
	seg.warrs = nil
}

//Writes a slice to the segment, returns immediately
//Returns nil if op is OK, otherwise ErrNoSpace or ErrInvalidArgument
//It is up to the implementer to work out how to report no space immediately
//The uint64 is the address to be used for the next write
func (seg *CephSegment) Write(address uint64, data []byte) (uint64, error) {
	//We don't put written blocks into the cache, because those will be 
	//in the dblock cache much higher up.
	seg.warrs = append(seg.warrs, data)
	szbytes := make([]byte,2, 2+len(data))
	szbytes[0] = byte(len(data))
	szbytes[1] = byte(len(data)>>8)
	szbytes = append(szbytes, data...)
	C.handle_write(seg.h, C.uint64_t(address), (*C.char)(unsafe.Pointer(&szbytes[0])), C.int(len(szbytes)), 0)
	naddr := address + uint64(len(szbytes))

	if ((naddr + MAX_EXPECTED_OBJECT_SIZE) >> 24) != (address >> 24) {
		//We are gonna need a new object addr
		naddr = <- seg.sp.alloc
	}
	return naddr, nil
}

//Block until all writes are complete. Note this does not imply a flush of the underlying files.
func (seg *CephSegment) Flush() {
	//Not sure we need to do stuff here, we can do it in unlock
}


func (sp *CephStorageProvider) provideReadHandles() {
	for {
		//Read all returned read handles
		ldretfi:
		for {
			select {
				case fi := <- sp.rhidx_ret:
					sp.rh_avail[fi] = true
				default:
					break ldretfi
			}
		}
		
		found  := false
		for i:=0; i<NUM_RHANDLES;i++ {
			if sp.rh_avail[i] {
				sp.rhidx <- i
				found = true
			}
		}
		//If we didn't find one, do a blocking read
		if !found {
			idx := <- sp.rhidx_ret
			sp.rh_avail[idx] = true
		}
	}
}

func (sp *CephStorageProvider) provideAllocs() {
	base := sp.ptr
	for {
		sp.alloc <- sp.ptr
		sp.ptr += ADDR_OBJ_SIZE
		if sp.ptr >= base + ADDR_LOCK_SIZE {
			sp.ptr = sp.obtainBaseAddress()
			base = sp.ptr
		}
	}
}

func (sp *CephStorageProvider) obtainBaseAddress() uint64 {
	h, err := C.handle_create()
	if err != nil {
		log.Panic("CGO ERROR: %v",err)
	}
	addr, err := C.handle_obtainrange(h)
	if err != nil {
		log.Panic("CGO ERROR: %v",err)
	}
	return uint64(addr)
}
//Called at startup of a normal run
func (sp *CephStorageProvider) Initialize(opts map[string]string) {
	_, err := C.initialize_provider()
	if err != nil {
		log.Panic("CGO ERROR: %v",err)
	}
	
	sp.rh 		 = make([]C.phandle_t, NUM_RHANDLES)
	sp.rh_avail  = make([]bool, NUM_RHANDLES)
	sp.rhidx 	 = make(chan int, NUM_RHANDLES+1)
	sp.rhidx_ret = make(chan int, NUM_RHANDLES+1)
	sp.alloc     = make(chan uint64, 128)
	
	for i:=0;i<NUM_RHANDLES;i++ {
		sp.rh_avail[i] = true
		h, err := C.handle_create()
		if err != nil {
			log.Panic("CGO ERROR: %v",err)
		}
		sp.rh[i] = h
	}
	
	//Obtain base address
	sp.ptr = sp.obtainBaseAddress()
	log.Info("Base address obtained as 0x%016x", sp.ptr)
	
	//Start serving read handles
	go sp.provideReadHandles()
	
	//Start providing address allocations
	go sp.provideAllocs()
	
	
}

//Called to create the database for the first time
func (sp *CephStorageProvider) CreateDatabase(opts map[string]string) error {
	_, err := C.initialize_provider()
	if err != nil {
		log.Panic("CGO ERROR: %v",err)
	}
	h, err := C.handle_create()
	if err != nil {
		log.Panic("CGO ERROR: %v",err)
	}
	C.handle_init_allocator(h)
	_, err = C.handle_close(h)
	if err != nil {
		log.Panic("CGO ERROR: %v",err)
	}
	return nil
}

// Lock a segment, or block until a segment can be locked
// Returns a Segment struct
func (sp *CephStorageProvider) LockSegment() bprovider.Segment {
	rv := new(CephSegment)
	rv.sp = sp
	h, err := C.handle_create()
	if err != nil {
		log.Panic("CGO ERROR: %v",err)
	}
	rv.h = h
	rv.ptr = <- sp.alloc
	rv.base = rv.ptr
	//Although I don't know this for sure, I am concerned that when we pass the write array pointer to C
	//the Go GC may free it before C is done. I prevent this by pinning all the written arrays, which get
	//deref'd after the segment is unlocked
	rv.warrs = make([][]byte,0,64)
	return rv
}

// Read the blob into the given buffer
func (sp *CephStorageProvider) Read(address uint64, buffer []byte) ([]byte) {
	//Check if this address is in the buffer
	/*cached, ok := sp.cache[address >> 24]
	if ok {
		var ln int
		ln = int(cached[address & OFFSET_MASK]) + (int(cached[(address & OFFSET_MASK) + 1]) << 8 )
		copy(buffer, cached[(address & OFFSET_MASK)+2:(address & OFFSET_MASK)+2+ln])
		return
	}*/
	//Get a read handle
	rhidx := <- sp.rhidx
	if len(buffer) < MAX_EXPECTED_OBJECT_SIZE {
		log.Panic("That doesn't seem safe")
	}
	rc, err := C.handle_read(sp.rh[rhidx], C.uint64_t(address), (*C.char)(unsafe.Pointer(&buffer[0])), MAX_EXPECTED_OBJECT_SIZE)
	if err != nil {
		log.Panic("CGO ERROR: %v",err)
	}
	ln := int(buffer[0]) + (int(buffer[1]) << 8)
	if int(rc) < ln+2 {
		log.Panic("Short read")
	}
	return buffer[2:ln+2]
}


