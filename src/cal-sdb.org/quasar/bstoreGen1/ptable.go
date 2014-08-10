package bstoreGen1

import (
	"fmt"
	"os"
	"log"
	"syscall"
	"unsafe"
	"reflect"
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
	sh.Len = int(numrecords)
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
	ALLOCATED 	  = 1 << iota
	WRITTEN_BACK
)

//Encoding of a vaddr 
// [1: flags] [1: resvd] [1: file] [5: paddr]
const PADDR_MASK = 0x0000FFFFFFFFFFFF
const FILE_ADDR_MASK   = 0xFFFFFFFFFF
const FLAGS_SHIFT = 56
const FILE_SHIFT = 48

func (bs *BlockStore) flagWriteBack(vaddr uint64) {
	bs.ptable[vaddr] |= WRITTEN_BACK
}

func (bs *BlockStore) initMetadata() {
	ptablepath := fmt.Sprintf("%s/metadata.db",bs.basepath)
	//Lol? We need to somehow store the desired PTSize somewhere
	//Or make it grow dynamically and derive from file size?
	page_array, bsmeta, mmp, sz := mapMeta(ptablepath)
	bs.ptable = page_array
	bs.ptable_ptr = mmp
	bs.meta = bsmeta
	bs.ptsize = sz
	
	if bs.meta.Version == 0 {
		*bs.meta = defaultBSMeta
	}
	
	bs.alloc = make(chan allocation, 1)
	go func() {
		lastalloc :=  uint64(0)
		for {
			flags := bs.ptable[bs.meta.valloc_ptr] >> FLAGS_SHIFT
			paddr := bs.ptable[bs.meta.valloc_ptr] & PADDR_MASK
			if flags & ALLOCATED != 0{
				//Maybe its a leaked block?
				if !(flags & WRITTEN_BACK != 0) {
					log.Printf("Leaked block: v=0x%08x",bs.meta.valloc_ptr)
					bs.alloc <- allocation{bs.meta.valloc_ptr, paddr}
					lastalloc = bs.meta.valloc_ptr
				} else {
					//Its a normal block thats allocated
				}
			} else {
				//This is a free block
				bs.ptable[bs.meta.valloc_ptr] |= (ALLOCATED << FLAGS_SHIFT)
				bs.alloc <- allocation{bs.meta.valloc_ptr, paddr}
				lastalloc = bs.meta.valloc_ptr
			}
			bs.meta.valloc_ptr++
			if bs.meta.valloc_ptr == bs.ptsize {
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
	return bs.ptable[address]
}

func CreateDatabase(numBlocks uint64, basepath string) {
	if numBlocks % FNUM != 0 {
		log.Panicf("Database size needs to be a multiple of %d", FNUM)
	}
	if numBlocks / FNUM % 128 != 0 {
		log.Panicf("Database size needs to be a multiple of %d", FNUM*128)
	}
	//Size of each db file
	fsize := (numBlocks / FNUM) * DBSIZE
	psize := numBlocks*RECORDSIZE + RESERVEHDR
	
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
	if sz*RECORDSIZE != psize-RESERVEHDR {
		log.Panic("Size mismatch")
	}
	
	var file_offset uint64 = 0
	bases := make([]uint64, FNUM)
	fidx := 0
	//Now we perform our virtual address to physical address mapping
	for vaddr := uint64(0); vaddr < sz; vaddr++ {
		paddr := (uint64(fidx) << FILE_SHIFT) + bases[fidx] + file_offset
		ptable[vaddr] = paddr
		
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
