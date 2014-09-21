package bstoreGen1

import (
	"code.google.com/p/go-uuid/uuid"
	lg "code.google.com/p/log4go"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

//const RECORDSIZE = 8
const ENTRYSIZE = 16
const RESERVEHDR = 8192

type allocation struct {
	vaddr uint64
	paddr uint64
}

const (
	ALLOCATED    = 1
	WRITTEN_BACK = 2
)

//The metadata file reserves 8TB of memory address space.
//This corresponds to 4PB of data space. As we seem to be getting
//a maximum of 50x compress ratio on zfs, this corresponds to a
//maximum dataset size of 82TB which is probably ok, as that's
//bigger than my biggest server.
const METADATA_MEMORY_MAP = 8 * 1024 * 1024 * 1024 * 1024

//Encoding of a vaddr
// [1: flags] [2: file] [5: paddr]
// where paddr is the block index, so should be multiplied by the block size
// to get the byte address. This yields about 1 Terablock per file, with a max
// of 65535 files, which is roughly 65 Petablocks of address space. We are ok.
const PADDR_MASK = 0x00FFFFFFFFFFFFFF
const FILE_ADDR_MASK = 0xFFFFFFFFFF
const FLAGS_SHIFT = 56
const FILE_SHIFT = 40

//This maps the metadata file into memory, returning:
//the byte slice
//the uint64 slice with len set to the number of backed elements(*2).
//and cap set to the reserved memory segment ceiling. Note that
//you cannot access elements past len without calling realloc_vtable()
func mapMetaFile(path string) ([]byte, []uint64, *BSMetadata) {
	if unsafe.Sizeof(int(0)) != 8 {
		lg.Crashf("Your go implementation has 32 bit ints")
	}
	ptablef, err := os.OpenFile(path, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		fmt.Printf("ERROR: page table did not exist")
		fmt.Printf("Expected: %s", path)
		fmt.Printf("Run qserver -makedb <size_in_gb> to generate")
		os.Exit(1)
	} else if err != nil {
		lg.Crashf("Could not open file: %v", err)
	}
	disklen, err := ptablef.Seek(0, os.SEEK_END)
	if err != nil {
		lg.Crashf("Could not seek file: %v", err)
	}
	recordbytes := (disklen - RESERVEHDR)
	if recordbytes&(ENTRYSIZE-1) != 0 {
		lg.Crashf("Strange number of record bytes %d", recordbytes)
	}
	numrecords := recordbytes / ENTRYSIZE

	//if disklen != int64(numrecords*RECORDSIZE + RESERVEHDR) {
	//	log.Panicf("WARNING: page table len %v doesn't match metadata %v",
	//		disklen, numrecords*RECORDSIZE + RESERVEHDR)
	//}
	dat, err := syscall.Mmap(int(ptablef.Fd()), 0, METADATA_MEMORY_MAP,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)
	if err != nil {
		lg.Crashf("Could not map page table: %v", err)
	}
	ptable := []uint64{}
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&ptable))
	sh.Data = uintptr(unsafe.Pointer(&dat[RESERVEHDR/8]))
	sh.Len = int(numrecords * 2)
	sh.Cap = METADATA_MEMORY_MAP / 8
	bsmeta := (*BSMetadata)(unsafe.Pointer(&dat[0]))
	return dat, ptable, bsmeta
}

//This increases the size of the underlying table, and extends
//the vtable slice to cover the newly provisioned space. This only
//works because we reserved the address range to start with.
func (bs *BlockStore) realloc_ptable(newElementCount uint64) {
	if int(newElementCount*2) > cap(bs.vtable) {
		lg.Crashf("Cannot realloc past reserved range")
	}
	if int(newElementCount*2) < len(bs.vtable) {
		lg.Crashf("Realloc count smaller than existing")
	}
	ptablepath := fmt.Sprintf("%s/metadata.db", bs.basepath)
	f, err := os.OpenFile(ptablepath, os.O_RDWR, 0666)
	if err != nil {
		lg.Crashf("realloc fail", err.Error())
	}
	if err := f.Truncate(int64(newElementCount * 16)); err != nil {
		lg.Crashf("truncate fail", err)
	}
	if err := f.Sync(); err != nil {
		lg.Crashf("sync fail", err)
	}
	if err := f.Close(); err != nil {
		lg.Crashf("close fail", err)
	}
	bs.vtable = bs.vtable[:newElementCount*2]
}

//Return part of the UUID as a 32 bit hint
func UUIDtoIdHint(id []byte) uint32 {
	return binary.LittleEndian.Uint32(id[12:])
}

//Flag a vaddr as written back (not leaked)
func (bs *BlockStore) flagWriteBack(vaddr uint64, id uuid.UUID) {
	idb := uint64(UUIDtoIdHint(id))
	bs.vtable[vaddr<<1+1] = idb << 32
	//log.Printf("Flaggin writeback on 0x%016x", vaddr)
	bs.vtable[vaddr<<1] |= (WRITTEN_BACK << FLAGS_SHIFT)
}

//Flag a vaddr as being unreferenced by a certain generation
func (bs *BlockStore) FlagUnreferenced(vaddr uint64, id []byte, gen uint64) {
	hint := bs.vtable[vaddr<<1+1]
	bid := UUIDtoIdHint(id)
	//lg.Debug("flagging unreferenced %016x gen=%v",vaddr, gen)
	if hint != uint64(bid)<<32 {
		lg.Warn("read hint at %016x does not correspond with expected %016x vs expected %016x (gen=%v)", vaddr, hint, uint64(bid)<<32, gen)
		//This can happen if we shut down in the middle of a commit, so its ok ? XTAG really?
	}
	//This saturates the generation
	wb := uint32(gen)
	if gen >= (1 << 32) {
		wb = 1<<32 - 1
	}
	bs.vtable[vaddr<<1+1] = (uint64(bid) << 32) + uint64(wb)
}
func (bs *BlockStore) initMetadata() {
	ptablepath := fmt.Sprintf("%s/metadata.db", bs.basepath)
	//Lol? We need to somehow store the desired PTSize somewhere
	//Or make it grow dynamically and derive from file size?
	mmp, page_array, bsmeta := mapMetaFile(ptablepath)
	bs.vtable = page_array
	bs.ptable_ptr = mmp
	bs.meta = bsmeta

	if bs.meta.Version == 0 {
		*bs.meta = defaultBSMeta
	}

	bs.alloc = make(chan allocation, 16)
	go func() {
		if bs.meta.valloc_ptr >= uint64(len(bs.vtable)/2) {
			lg.Warn("WARNING: VALLOC PTR was invalid")
			bs.meta.valloc_ptr = 1
		}
		lastalloc := bs.meta.valloc_ptr
		for {
			alloc, written := bs.VaddrFlags(bs.meta.valloc_ptr)
			if alloc {
				//Maybe its a leaked block?
				if !written {
					lg.Info("Leaked / unwritten block: v=0x%016x", bs.meta.valloc_ptr)
					//We cannot actually safely use this, as it might be alloced to
					//a block in memory and not yet written back
				}
			} else {
				//This is a free block
				bs.vtable[bs.meta.valloc_ptr<<1] |= (ALLOCATED << FLAGS_SHIFT)
				bs.alloc <- allocation{bs.meta.valloc_ptr, bs.vtable[bs.meta.valloc_ptr<<1]}
				lastalloc = bs.meta.valloc_ptr
			}
			bs.meta.valloc_ptr++
			if bs.meta.valloc_ptr >= uint64(len(bs.vtable)/2) {
				lg.Warn("WARNING: VALLOC PTR WRAP!")
				bs.meta.valloc_ptr = 1
			}
			if lastalloc == bs.meta.valloc_ptr {
				lg.Warn("Allocating more database capacity")
				bs.NEEDMOARDATABASE()
				lg.Warn("Complete")
			}
		}
	}()
}

func (bs *BlockStore) virtToPhysical(address uint64) uint64 {
	if address >= uint64(len(bs.vtable)/2) {
		lg.Crashf("Block virtual address SIGSEGV (0x%08x)", address)
	}
	paddr := bs.vtable[address<<1] & PADDR_MASK
	//log.Printf("resolved virtual address %08x as %08x", address, paddr)
	return paddr
}

func (bs *BlockStore) TotalBlocks() uint64 {
	return uint64(len(bs.vtable) / 2)
}

//Returns alloced, free, strange, leaked
func (bs *BlockStore) InspectBlocks() (uint64, uint64, uint64, uint64) {
	var _leaked, _alloced, _free, _strange uint64
	for i := uint64(1); i < uint64(len(bs.vtable)/2); i++ {
		if i%(32*1024) == 0 {
			lg.Info("Inspecting block %d / %d", i, len(bs.vtable)/2)
		}
		//log.Printf("%016x %016x",i,bs.ptable[i])
		flags := bs.vtable[i<<1] >> FLAGS_SHIFT
		if flags&ALLOCATED != 0 {
			_alloced++
			if flags&WRITTEN_BACK == 0 {
				_leaked++
			}
		} else {
			_free++
		}
		if bs.vtable[i<<1]&PADDR_MASK == 0 {
			lg.Info("VADDR 0x%08x has no PADDR", i)
			_strange++
		}
	}
	return _alloced, _free, _strange, _leaked
}

func (bs *BlockStore) VaddrFlags(vaddr uint64) (allocated bool, written bool) {
	flags := bs.vtable[vaddr<<1] >> FLAGS_SHIFT
	allocated = flags&ALLOCATED != 0
	written = flags&WRITTEN_BACK != 0
	return
}
func (bs *BlockStore) VaddrHint(vaddr uint64) (idhint uint32, genhint uint32) {
	idhint = uint32(bs.vtable[vaddr<<1+1] >> 32)
	genhint = uint32(bs.vtable[vaddr<<1+1])
	return
}
func (bs *BlockStore) UnlinkVaddr(vaddr uint64) {
	bs.vtable[vaddr<<1] &= PADDR_MASK
	bs.vtable[vaddr<<1+1] = 0
}

//This is the primary function for extending the database
//It will add 128MibiBlock more data space (1TB data), which hopefully your underlying
//storage can take. Don't call this concurrently plox
func (bs *BlockStore) NEEDMOARDATABASE() {
	//Get current size
	curblocks := uint64(len(bs.vtable) / 2)
	newblocks := curblocks + 128*1024*1024
	expected_filesz := int64(curblocks * DBSIZE / FNUM)
	new_filesz := int64(newblocks * DBSIZE / FNUM)

	for fi := 0; fi < FNUM; fi++ {
		fname := fmt.Sprintf("%s/blockstore.%02x.db", bs.basepath, fi)
		f, err := os.OpenFile(fname, os.O_RDWR, 0666)
		if err != nil {
			lg.Crashf("Could not open DB file %s:", fname, err.Error())
		}
		curlen, err := f.Seek(0, os.SEEK_END)
		if err != nil {
			lg.Crashf("Could not seek file: %v", err)
		}
		if curlen != expected_filesz {
			lg.Crashf("File is not expected size! %v vs %v", curlen, expected_filesz)
		}
		if err := f.Truncate(new_filesz); err != nil {
			lg.Crashf("truncate fail", err)
		}
		if err := f.Sync(); err != nil {
			lg.Crashf("sync fail", err)
		}
		if err := f.Close(); err != nil {
			lg.Crashf("close fail", err)
		}
	}
	var file_offset uint64 = 0
	bases := make([]uint64, FNUM)
	base_offset := curblocks / FNUM
	fidx := 0
	current_vaddr := len(bs.vtable) / 2
	bs.realloc_ptable(newblocks)
	new_vaddr := len(bs.vtable) / 2
	//Now we perform our virtual address to physical address mapping
	for vaddr := current_vaddr; vaddr < new_vaddr; vaddr++ {
		paddr := (uint64(fidx) << FILE_SHIFT) + bases[fidx] + file_offset + base_offset
		bs.vtable[vaddr<<1] = paddr

		//We slice between files every 128 blocks
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
}
func CreateDatabase(numBlocks uint64, basepath string) {
	if numBlocks%FNUM != 0 {
		lg.Crashf("Database size needs to be a multiple of %d", FNUM)
	}
	if numBlocks/FNUM%128 != 0 {
		lg.Crashf("Database size needs to be a multiple of %d", FNUM*128)
	}
	//Size of each db file
	fsize := (numBlocks / FNUM) * DBSIZE
	psize := numBlocks*ENTRYSIZE + RESERVEHDR

	//Now we need to create the page table
	ptablepath := fmt.Sprintf("%s/metadata.db", basepath)
	f, err := os.OpenFile(ptablepath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		lg.Crashf("metadata open error %v", err)
	}
	if err := f.Truncate(int64(psize)); err != nil {
		lg.Crashf("truncate error %v", err)
	}
	if err := f.Sync(); err != nil {
		lg.Crashf("sync error %v", err)
	}
	if err := f.Close(); err != nil {
		lg.Crashf("close error %v", err)
	}

	for fi := 0; fi < FNUM; fi++ {
		fname := fmt.Sprintf("%s/blockstore.%02x.db", basepath, fi)
		lg.Info("Creating %s", fname)
		f, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			lg.Crashf("Could not open file", err)
		}
		if err := f.Truncate(int64(fsize)); err != nil {
			lg.Crashf("truncate fail", err)
		}
		if err := f.Sync(); err != nil {
			lg.Crashf("sync fail", err)
		}
		if err := f.Close(); err != nil {
			lg.Crashf("close fail", err)
		}
	}

	_, ptable, meta := mapMetaFile(ptablepath)
	sz := uint64(len(ptable) / 2)
	if sz*ENTRYSIZE != psize-RESERVEHDR {
		lg.Crashf("Size mismatch")
	}

	var file_offset uint64 = 0
	bases := make([]uint64, FNUM)
	fidx := 0
	//Now we perform our virtual address to physical address mapping
	for vaddr := uint64(0); vaddr < sz; vaddr++ {
		paddr := (uint64(fidx) << FILE_SHIFT) + bases[fidx] + file_offset
		ptable[vaddr<<1] = paddr

		//We slice between files every 128 blocks
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
