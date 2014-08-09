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
func mapMeta(path string, numrecords uint64) ([]uint64, *BSMetadata, []byte,) {
	if unsafe.Sizeof(int(0)) != 8 {
		log.Panic("Your go implementation has 32 bit ints")
	}	
	ptablef, err := os.OpenFile(path, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		log.Printf("WARNING: page table did not exist")
		ptablef, err = os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0666)
		ptablef.Truncate(int64(numrecords*RECORDSIZE) + RESERVEHDR)
		if serr := ptablef.Sync(); serr != nil {
			log.Panic(serr)
		}
	} else if err != nil {
		log.Panic(err)
	}
	disklen, err := ptablef.Seek(0, os.SEEK_END)
	if err != nil {
		log.Panic(err)
	}
	if disklen != int64(numrecords*RECORDSIZE + RESERVEHDR) {
		log.Panicf("WARNING: page table len %v doesn't match metadata %v", 
			disklen, numrecords*RECORDSIZE + RESERVEHDR)
	}
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
	return page_array, bsmeta, dat
}
//This is mostly for testing
func ClosePTable(dat []byte) {
	if err := syscall.Munmap(dat); err != nil {
		log.Panic(err)
	}
}
func (bs *BlockStore) initMetadata() {
	ptablepath := fmt.Sprintf("%s/metadata.db",bs.basepath)
	//Lol? We need to somehow store the desired PTSize somewhere
	//Or make it grow dynamically and derive from file size?
	page_array, bsmeta, mmp := mapMeta(ptablepath, defaultBSMeta.PTSize)
	bs.ptable = page_array
	bs.ptable_ptr = mmp
	bs.meta = bsmeta
	
	if bs.meta.Version == 0 {
		*bs.meta = defaultBSMeta
	}
	
	bs.vaddr = make(chan uint64, 10)
	go func() {
		lastalloc := uint64(0)
		for {
			if bs.ptable[bs.meta.valloc_ptr] == 0 {
				bs.vaddr <- bs.meta.valloc_ptr
				lastalloc = bs.meta.valloc_ptr
			}
			bs.meta.valloc_ptr++
			if bs.meta.valloc_ptr == bs.meta.PTSize {
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
	if address >= bs.meta.PTSize {
		log.Panicf("Block virtual address SIGSEGV (0x%08x)", address)
	}
	return bs.ptable[address]
}
