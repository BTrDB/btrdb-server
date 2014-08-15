package bstoreGen1

import (
	"fmt"
	"os"
	"log"
	"syscall"
	"unsafe"
	"reflect"
	"encoding/binary"
	"code.google.com/p/go-uuid/uuid"
	lg "code.google.com/p/log4go"
)

const RECORDSIZE = 8
const RESERVEHDR = 8192
func mapMeta(path string) ([]uint64, *BSMetadata, []byte, uint64) {
	if unsafe.Sizeof(int(0)) != 8 {
		log.Panic("Your go implementation has 32 bit ints")
	}	
	ptablef, err := os.OpenFile(path, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		log.Printf("ERROR: page table did not exist")
		log.Printf("Expected: %s", path)
		log.Printf("Run quasar -makedb <size_in_gb> to generate")
		os.Exit(1)
		//ptablef, err = os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0666)
		//ptablef.Truncate(int64(numrecords*RECORDSIZE) + RESERVEHDR)
		//if serr := ptablef.Sync(); serr != nil {
		//	log.Panic(serr)
		//}
	} else if err != nil {
		log.Panic(err)
	}
	disklen, err := ptablef.Seek(0, os.SEEK_END)
	if err != nil {
		log.Panic(err)
	}
	numrecords := (disklen - RESERVEHDR)
	if numrecords & (RECORDSIZE-1) != 0 {
		log.Panicf("Strange number of records %d", numrecords)
	}
	numrecords /= RECORDSIZE 
	numrecords /= 2
	//if disklen != int64(numrecords*RECORDSIZE + RESERVEHDR) {
	//	log.Panicf("WARNING: page table len %v doesn't match metadata %v", 
	//		disklen, numrecords*RECORDSIZE + RESERVEHDR)
	//}
	dat, err := syscall.Mmap(int(ptablef.Fd()), 0, int(disklen),
		syscall.PROT_READ | syscall.PROT_WRITE, 
		syscall.MAP_SHARED)
	if err != nil {
		log.Panicf("Could not map page table: %v",err)
	}
	page_array := []uint64{}
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&page_array))
	sh.Data = uintptr(unsafe.Pointer(&dat[RESERVEHDR/8]))
	sh.Len = int(numrecords*2)
	sh.Cap = sh.Len
	bsmeta := (*BSMetadata)(unsafe.Pointer(&dat[0]))
	return page_array, bsmeta, dat, uint64(numrecords)
}
//This is mostly for testing
func ClosePTable(dat []byte) {
	if err := syscall.Munmap(dat); err != nil {
		log.Panic(err)
	}
}
type allocation struct {
	vaddr	uint64
	paddr 	uint64
}

const (
	ALLOCATED 	  = 1
	WRITTEN_BACK  = 2
)

//Encoding of a vaddr 
// [1: flags] [1: resvd] [1: file] [5: paddr]
const PADDR_MASK = 0x0000FFFFFFFFFFFF
const FILE_ADDR_MASK   = 0xFFFFFFFFFF
const FLAGS_SHIFT = 56
const FILE_SHIFT = 40

func UUIDtoIdHint(id []byte) uint32 {
	return binary.LittleEndian.Uint32(id[12:])
}
func (bs *BlockStore) flagWriteBack(vaddr uint64, id uuid.UUID) {
	idb := uint64(UUIDtoIdHint(id))
	bs.vtable[vaddr<<1 + 1] = idb << 32
	//log.Printf("Flaggin writeback on 0x%016x", vaddr)
	bs.vtable[vaddr<<1] |= (WRITTEN_BACK << FLAGS_SHIFT)
}
func (bs *BlockStore) FlagUnreferenced(vaddr uint64, id []byte, gen uint64) {
	hint := bs.vtable[vaddr<<1 + 1]
	bid := UUIDtoIdHint(id)
	lg.Debug("flagging unreferenced %016x gen=%v",vaddr, gen)
	if hint != uint64(bid) << 32 {
		lg.Crashf("read hint at %016x does not correspond with expected %016x vs expected %016x (gen=%v)", vaddr, hint, uint64(bid) << 32 , gen)
	}
	//This saturates the generation
	wb := uint32(gen)
	if gen >= (1<<32) {
		wb = 1<<32-1
	}
	bs.vtable[vaddr<<1 + 1] = (uint64(bid)<<32) + uint64(wb)
}
func (bs *BlockStore) initMetadata() {
	ptablepath := fmt.Sprintf("%s/metadata.db",bs.basepath)
	//Lol? We need to somehow store the desired PTSize somewhere
	//Or make it grow dynamically and derive from file size?
	page_array, bsmeta, mmp, sz := mapMeta(ptablepath)
	bs.vtable = page_array
	bs.ptable_ptr = mmp
	bs.meta = bsmeta
	bs.ptsize = sz/2 // because the entries are in pairs
	
	if bs.meta.Version == 0 {
		*bs.meta = defaultBSMeta
	}
	
	bs.alloc = make(chan allocation)
	go func() {
		if bs.meta.valloc_ptr >= bs.ptsize {
			log.Printf("WARNING: VALLOC PTR was invalid")
			bs.meta.valloc_ptr = 1
		}
		lastalloc :=  bs.meta.valloc_ptr
		for {
			alloc, written := bs.VaddrFlags(bs.meta.valloc_ptr)
			if alloc {
				//Maybe its a leaked block?
				if !written {
					log.Printf("Leaked / unwritten block: v=0x%016x",bs.meta.valloc_ptr)
					//We cannot actually safely use this, as it might be alloced to
					//a block in memory and not yet written back
					//bs.alloc <- allocation{bs.meta.valloc_ptr, paddr}
					//lastalloc = bs.meta.valloc_ptr
				} else {
					//Its a normal block thats allocated
				}
			} else {
				//This is a free block
				bs.vtable[bs.meta.valloc_ptr << 1] |= (ALLOCATED << FLAGS_SHIFT)
				bs.alloc <- allocation{bs.meta.valloc_ptr, bs.vtable[bs.meta.valloc_ptr << 1]}
				lastalloc = bs.meta.valloc_ptr
			}
			bs.meta.valloc_ptr++
			if bs.meta.valloc_ptr >= bs.ptsize {
				log.Printf("WARNING: VALLOC PTR WRAP!")
				bs.meta.valloc_ptr = 1
			}
			if lastalloc == bs.meta.valloc_ptr {
				log.Panic("Ran out of pt virtual addresses")
			}
		}
	}()
}

func (bs *BlockStore) virtToPhysical(address uint64) uint64 {
	if address >= bs.ptsize {
		log.Panicf("Block virtual address SIGSEGV (0x%08x)", address)
	}
	paddr := bs.vtable[address << 1] & PADDR_MASK
	//log.Printf("resolved virtual address %08x as %08x", address, paddr)
	return paddr
}

func (bs *BlockStore) TotalBlocks() uint64 {
	return bs.ptsize
}

//Returns alloced, free, strange, leaked
func (bs *BlockStore) InspectBlocks() (uint64, uint64, uint64, uint64) {
	var _leaked, _alloced, _free, _strange uint64
	for i := uint64(1); i < bs.ptsize; i++ {
		if i % (32*1024) == 0 {
			log.Printf("Inspecting block %d", i)	
		}
		//log.Printf("%016x %016x",i,bs.ptable[i])
		flags := bs.vtable[i<<1] >> FLAGS_SHIFT
		if flags & ALLOCATED != 0 {
			_alloced ++
			if flags & WRITTEN_BACK == 0 {
				_leaked ++
			}
		} else {
			_free++
		}
		if bs.vtable[i<<1] & PADDR_MASK == 0 {
			log.Printf("VADDR 0x%08x has no PADDR", i)
			_strange++
		}
	}
	return _alloced, _free, _strange, _leaked
}

func (bs *BlockStore) VaddrFlags(vaddr uint64) (allocated bool, written bool) {
	flags := bs.vtable[vaddr<<1] >> FLAGS_SHIFT
	allocated = flags & ALLOCATED != 0
	written = flags & WRITTEN_BACK != 0
	return
}
func (bs *BlockStore) VaddrHint(vaddr uint64) (idhint uint32, genhint uint32) {
	idhint = uint32(bs.vtable[vaddr<<1+1] >> 32)
	genhint = uint32(bs.vtable[vaddr<<1+1])
	return
}
func (bs *BlockStore) UnlinkVaddr(vaddr uint64) {
	bs.vtable[vaddr<<1] &= PADDR_MASK
}

func CreateDatabase(numBlocks uint64, basepath string) {
	if numBlocks % FNUM != 0 {
		log.Panicf("Database size needs to be a multiple of %d", FNUM)
	}
	if numBlocks / FNUM % 128 != 0 {
		log.Panicf("Database size needs to be a multiple of %d", FNUM*128)
	}
	//Size of each db file
	fsize := (numBlocks / FNUM) * DBSIZE * 2
	psize := numBlocks*RECORDSIZE*2 + RESERVEHDR 
	
	//Now we need to create the page table
	ptablepath := fmt.Sprintf("%s/metadata.db", basepath)
	f, err := os.OpenFile(ptablepath, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0666)
	if err != nil {
		log.Printf(err.Error())
		os.Exit(1)
	}
	if err := f.Truncate(int64(psize)); err != nil {
		log.Panic(err)
	}
	if err := f.Sync(); err != nil {
		log.Panic(err)
	}
	if err := f.Close(); err != nil {
		log.Panic(err)
	}
	
	for fi := 0; fi < FNUM; fi++ {
		fname := fmt.Sprintf("%s/blockstore.%02x.db", basepath, fi)
		log.Printf("Creating %s", fname)
		f, err := os.OpenFile(fname, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0666)
		if err != nil {
			log.Printf(err.Error())
			os.Exit(1)
		}
		if err := f.Truncate(int64(fsize)); err != nil {
			log.Panic(err)
		}
		if err := f.Sync(); err != nil {
			log.Panic(err)
		}
		if err := f.Close(); err != nil {
			log.Panic(err)
		}
	}
	
	ptable, meta, _, sz := mapMeta(ptablepath)
	if sz*RECORDSIZE*2 != psize-RESERVEHDR {
		log.Panic("Size mismatch")
	}
	
	var file_offset uint64 = 0
	bases := make([]uint64, FNUM)
	fidx := 0
	//Now we perform our virtual address to physical address mapping
	for vaddr := uint64(0); vaddr < sz; vaddr++ {
		paddr := (uint64(fidx) << FILE_SHIFT) + bases[fidx] + file_offset
		ptable[vaddr<<1] = paddr
		
		file_offset += 1
		if file_offset == 128 {
			bases[fidx] += 128
			file_offset = 0
			fidx += 1
			if fidx == FNUM {
				fidx = 0
			}
		}
	}
	*meta = defaultBSMeta
}

