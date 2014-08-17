package bstoreGen1

import (
	"bytes"
	"code.google.com/p/go-uuid/uuid"
	"errors"
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"os"
	"sync"
	lg "code.google.com/p/log4go"
)

const LatestGeneration = uint64(^(uint64(0)))

const KFACTOR = 64
const PWFACTOR = uint8(6) //1<<6 == 64
const VSIZE = 256
const FNUM = 8
const BALLOC_INC = 4096 //How many blocks to prealloc
const MIBID_INC = 32768 //How many unique identifiers to use between metadata flushes
func UUIDToMapKey(id uuid.UUID) [16]byte {
	rv := [16]byte{}
	copy(rv[:], id)
	return rv
}
func init() {
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Lshortfile)
}

type BSMetadata struct {
	Version uint64

	//The size of the page table in blocks
	MIBID uint64
	//The current virtual memory address. Eventually we need to replace
	//this with an allocator
	valloc_ptr uint64
}

var defaultBSMeta = BSMetadata{
	Version:    1,
	MIBID:      1,
	valloc_ptr: 1,
	//PTSize		: 2*1024*1024*1024, //16TB worth of blocks
}

type BlockStore struct {
	ses     *mgo.Session
	db      *mgo.Database
	_wlocks map[[16]byte]*sync.Mutex
	glock   sync.RWMutex

	fidx chan int
	//maxblock	[]uint64
	//nxtblock	[]uint64
	dbf      []*os.File
	blockmtx []sync.Mutex

	basepath   string
	meta       *BSMetadata
	vtable     []uint64
	ptable_ptr []byte //The underlying data for the ptable
	cMIBID     chan uint64
	vaddr      chan uint64

	cachemap map[uint64]*CacheItem
	cacheold *CacheItem
	cachenew *CacheItem
	cachemtx sync.Mutex
	cachelen uint64
	cachemax uint64

	alloc  chan allocation
	ptsize uint64
}

var block_buf_pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, DBSIZE)
	},
}

var ErrDatablockNotFound = errors.New("Coreblock not found")
var ErrGenerationNotFound = errors.New("Generation not found")

/* A generation stores all the information acquired during a write pass.
 * A superblock contains all the information required to navigate a tree.
 */
type Generation struct {
	Cur_SB       *Superblock
	New_SB       *Superblock
	cblocks      []*Coreblock
	vblocks      []*Vectorblock
	blockstore   *BlockStore
	unref_vaddrs []uint64
	flushed      bool
}

func (g *Generation) UpdateRootAddr(addr uint64) {
	//log.Printf("updateaddr called (%v)",addr)
	g.New_SB.root = addr
}
func (g *Generation) Uuid() *uuid.UUID {
	return &g.Cur_SB.uuid
}

func (g *Generation) Number() uint64 {
	lg.Debug("Num called %p %v",g, g.New_SB.gen)
	return g.New_SB.gen
}

func (g *Generation) UnreferenceBlock(vaddr uint64) {
	g.unref_vaddrs = append(g.unref_vaddrs, vaddr)
}

func (bs *BlockStore) UnlinkGenerations(id uuid.UUID, sgen uint64, egen uint64) error {
	iter := bs.db.C("superblocks").Find(bson.M{"uuid": id.String(), "gen": bson.M{"$gte": sgen, "$lt": egen}, "unlinked": false}).Iter()
	rs := fake_sblock{}
	for iter.Next(&rs) {
		rs.Unlinked = true
		_, err := bs.db.C("superblocks").Upsert(bson.M{"uuid": id.String(), "gen": rs.Gen}, rs)
		if err != nil {
			log.Panic(err)
		}
	}
	return nil
}
func NewBlockStore(targetserv string, cachesize uint64, dbpath string) (*BlockStore, error) {
	bs := BlockStore{}
	ses, err := mgo.Dial(targetserv)
	if err != nil {
		return nil, err
	}
	bs.ses = ses
	bs.db = ses.DB("quasar")
	bs._wlocks = make(map[[16]byte]*sync.Mutex)
	bs.basepath = dbpath
	if err := os.MkdirAll(bs.basepath, 0755); err != nil {
		log.Panic(err)
	}
	//bs.maxblock = make([]uint64, FNUM)
	//bs.nxtblock = make([]uint64, FNUM)
	bs.dbf = make([]*os.File, FNUM)
	bs.blockmtx = make([]sync.Mutex, FNUM)
	bs.initCache(cachesize)
	bs.fidx = make(chan int, 32)
	go func() {
		idx := 0
		for {
			for k := 0; k < 64; k++ {
				bs.fidx <- idx
			}
			idx += 1
			if idx == FNUM {
				idx = 0
			}
		}
	}()
	bs.initMetadata()
	bs.cMIBID = make(chan uint64, 10)
	go func() {
		for {
			bs.cMIBID <- bs.meta.MIBID
			bs.meta.MIBID++
		}
	}()
	for fi := 0; fi < FNUM; fi++ {
		fname := fmt.Sprintf("%s/blockstore.%02x.db", dbpath, fi)
		f, err := os.OpenFile(fname, os.O_RDWR, 0666)
		if err != nil {
			log.Printf("Problem with blockstore DB: ", err)
			log.Panic(err)
		}
		l, err := f.Seek(0, os.SEEK_END)
		if err != nil {
			log.Panic(err)
		}
		if l&(DBSIZE-1) != 0 {
			log.Printf("dbsize is: %v", l)
			log.Panic("DB is weird size")
		}
		bs.dbf[fi] = f
		f.Sync()
	}
	return &bs, nil
}

/*
 * This obtains a generation, blocking if necessary
 */
func (bs *BlockStore) ObtainGeneration(id uuid.UUID) *Generation {
	//The first thing we do is obtain a write lock on the UUID, as a generation
	//represents a lock
	mk := UUIDToMapKey(id)
	bs.glock.RLock()
	mtx, ok := bs._wlocks[mk]
	bs.glock.RUnlock()
	if !ok {
		//Mutex doesn't exist so is unlocked
		mtx := new(sync.Mutex)
		mtx.Lock()
		bs.glock.Lock()
		bs._wlocks[mk] = mtx
		bs.glock.Unlock()
	} else {
		mtx.Lock()
	}

	gen := &Generation{
		cblocks: make([]*Coreblock, 0, 8192),
		vblocks: make([]*Vectorblock, 0, 8192),
		unref_vaddrs: make([]uint64, 0, 8192),
	}
	//We need a generation. Lets see if one is on disk
	qry := bs.db.C("superblocks").Find(bson.M{"uuid": id.String()})
	rs := fake_sblock{}
	qerr := qry.Sort("-gen").One(&rs)
	if qerr == mgo.ErrNotFound {
		log.Printf("no superblock found for %v", id.String())
		//Ok just create a new superblock/generation
		gen.Cur_SB = NewSuperblock(id)
	} else if qerr != nil {
		//Well thats more serious
		log.Panic(qerr)
	} else {
		//Ok we have a superblock, pop the gen
		log.Printf("Found a superblock for %v", id.String())
		sb := Superblock{
			uuid: id,
			root: rs.Root,
			gen:  rs.Gen,
		}
		gen.Cur_SB = &sb
	}

	gen.New_SB = gen.Cur_SB.Clone()
	gen.New_SB.gen = gen.Cur_SB.gen + 1
	gen.New_SB.mibid = <-bs.cMIBID
	gen.blockstore = bs
	return gen
}



func (gen *Generation) Commit() error {
	if gen.flushed {
		return errors.New("Already Flushed")
	}
	reqf := make(map[int]bool)
	for _, cb := range gen.cblocks {
		raddr := gen.blockstore.virtToPhysical(cb.This_addr)
		reqf[int(raddr>>48)] = true
		gen.blockstore.writeCoreblockAndFree(cb)
	}
	gen.cblocks = nil
	for _, vb := range gen.vblocks {
		raddr := gen.blockstore.virtToPhysical(vb.This_addr)
		reqf[int(raddr>>48)] = true
		gen.blockstore.writeVectorblockAndFree(vb)
	}
	gen.vblocks = nil
	for _, ur := range gen.unref_vaddrs {
		gen.blockstore.FlagUnreferenced(ur, []byte(*gen.Uuid()), gen.Number())
	}
	gen.unref_vaddrs = nil
	for fi := range reqf {
		gen.blockstore.datablockBarrier(fi)
	}
	
	log.Printf("inserting supeblock u=%v gen=%v root=%v", gen.Uuid().String(), gen.Number(), gen.New_SB.root)
	//Ok we cannot directly write a superblock to the DB here
	fsb := fake_sblock{
		Uuid:  gen.New_SB.uuid.String(),
		Gen:   gen.New_SB.gen,
		Root:  gen.New_SB.root,
		MIBID: gen.New_SB.mibid,
	}
	if err := gen.blockstore.db.C("superblocks").Insert(fsb); err != nil {
		log.Panic(err)
	}
	gen.flushed = true
	gen.blockstore.glock.RLock()
	//log.Printf("bs is %v, wlocks is %v", gen.blockstore, gen.blockstore._wlocks)
	gen.blockstore._wlocks[UUIDToMapKey(*gen.Uuid())].Unlock()
	gen.blockstore.glock.RUnlock()
	return nil
}

func (bs *BlockStore) datablockBarrier(fi int) {
	//Gonuts group says that I don't need to call Sync()

	//Block until all datablocks have finished writing
	/*bs.blockmtx[fi].Lock()
	err := bs.dbf[fi].Sync()
	if err != nil {
		log.Panic(err)
	}
	bs.blockmtx[fi].Unlock()*/
	//bs.ses.Fsync(false)
}

func (bs *BlockStore) allocateBlock() uint64 {
	allocation := <-bs.alloc
	//Encode the fidx in the top 16 bits
	return allocation.vaddr
}

/**
 * The real function is supposed to allocate an address for the data
 * block, reserving it on disk, and then give back the data block that
 * can be filled in
 * This stub makes up an address, and mongo pretends its real
 */
func (gen *Generation) AllocateCoreblock() (*Coreblock, error) {
	cblock := &Coreblock{}
	cblock.This_addr = gen.blockstore.allocateBlock()
	cblock.Generation = gen.Number()
	copy(cblock.UUID[:], []byte(*gen.Uuid()))
	cblock.MIBID = <-gen.blockstore.cMIBID
	gen.cblocks = append(gen.cblocks, cblock)
	return cblock, nil
}

func (gen *Generation) AllocateVectorblock() (*Vectorblock, error) {
	vblock := &Vectorblock{}
	vblock.This_addr = gen.blockstore.allocateBlock()
	vblock.Generation = gen.Number()
	copy(vblock.UUID[:], []byte(*gen.Uuid()))
	vblock.MIBID = <-gen.blockstore.cMIBID
	gen.vblocks = append(gen.vblocks, vblock)
	return vblock, nil
}

func (bs *BlockStore) FreeCoreblock(cb **Coreblock) {
	*cb = nil
}

func (bs *BlockStore) FreeVectorblock(vb **Vectorblock) {
	*vb = nil
}

func (bs *BlockStore) DEBUG_DELETE_UUID(id uuid.UUID) {
	log.Printf("DEBUG removing uuid '%v' from database", id.String())
	_, err := bs.db.C("superblocks").RemoveAll(bson.M{"uuid": id.String()})
	if err != nil && err != mgo.ErrNotFound {
		log.Panic(err)
	}
	if err == mgo.ErrNotFound {
		log.Printf("Quey did not find supeblock to delete")
	} else {
		log.Printf("err was nik")
	}
	//bs.datablockBarrier()
}

func (bs *BlockStore) writeDBlock(vaddr uint64, contents []byte, id []byte) error {
	addr := bs.virtToPhysical(vaddr)
	//log.Printf("Got physical address: %08x",addr)
	fileidx := (addr >> FILE_SHIFT) & 0xFF
	addr &= FILE_ADDR_MASK
	bs.blockmtx[fileidx].Lock()
	_, err := bs.dbf[fileidx].WriteAt(contents, int64(addr*DBSIZE))
	bs.blockmtx[fileidx].Unlock()
	bs.flagWriteBack(vaddr, id)
	return err
}

func (bs *BlockStore) readDBlock(vaddr uint64, buf []byte) error {
	addr := bs.virtToPhysical(vaddr)
	fileidx := (addr >> FILE_SHIFT) & 0xFF
	addr &= FILE_ADDR_MASK
	bs.blockmtx[fileidx].Lock()
	_, err := bs.dbf[fileidx].ReadAt(buf, int64(addr*DBSIZE))
	bs.blockmtx[fileidx].Unlock()
	return err
}

/**
 * The real function is meant to now write back the contents
 * of the data block to the address. This just uses the address
 * as a key
 */
func (bs *BlockStore) writeCoreblockAndFree(cb *Coreblock) error {
	bs.cachePut(cb.This_addr, cb)
	syncbuf := block_buf_pool.Get().([]byte)
	cb.Serialize(syncbuf)
	ierr := bs.writeDBlock(cb.This_addr, syncbuf, cb.UUID[:])
	if ierr != nil {
		log.Panic(ierr)
	}
	block_buf_pool.Put(syncbuf)
	return nil
}

func (bs *BlockStore) writeVectorblockAndFree(vb *Vectorblock) error {
	bs.cachePut(vb.This_addr, vb)
	syncbuf := block_buf_pool.Get().([]byte)
	vb.Serialize(syncbuf)
	ierr := bs.writeDBlock(vb.This_addr, syncbuf, vb.UUID[:])
	if ierr != nil {
		log.Panic(ierr)
	}
	block_buf_pool.Put(syncbuf)
	return nil
}

func (bs *BlockStore) ReadDatablock(addr uint64) Datablock {
	//Try hit the cache first
	db := bs.cacheGet(addr)
	if db != nil {
		return db
	}
	syncbuf := block_buf_pool.Get().([]byte)
	err := bs.readDBlock(addr, syncbuf)
	if err != nil {
		log.Panic(err)
	}
	switch DatablockGetBufferType(syncbuf) {
	case Core:
		rv := &Coreblock{}
		rv.Deserialize(syncbuf)
		block_buf_pool.Put(syncbuf)
		bs.cachePut(addr, rv)
		return rv
	case Vector:
		rv := &Vectorblock{}
		rv.Deserialize(syncbuf)
		block_buf_pool.Put(syncbuf)
		bs.cachePut(addr, rv)
		return rv
	}
	log.Panic("Strange datablock type")
	return nil
}

type fake_sblock struct {
	Uuid     string
	Gen      uint64
	Root     uint64
	MIBID    uint64
	Unlinked bool
}

func (bs *BlockStore) LoadSuperblock(id uuid.UUID, generation uint64) *Superblock {
	var sb = fake_sblock{}
	if generation == LatestGeneration {
		log.Printf("loading superblock uuid=%v (lgen)", id.String())
		qry := bs.db.C("superblocks").Find(bson.M{"uuid": id.String()})
		if err := qry.Sort("-gen").One(&sb); err != nil {
			if err == mgo.ErrNotFound {
				log.Printf("sb notfound!")
				return nil
			} else {
				log.Panic(err)
			}
		}
	} else {
		qry := bs.db.C("superblocks").Find(bson.M{"uuid": id.String(), "gen": generation})
		if err := qry.One(&sb); err != nil {
			if err == mgo.ErrNotFound {
				return nil
			} else {
				log.Panic(err)
			}
		}
	}
	rv := Superblock{
		uuid:     id,
		gen:      sb.Gen,
		root:     sb.Root,
		unlinked: sb.Unlinked,
		mibid:    sb.MIBID,
	}
	return &rv
}

type UnlinkCriteria struct {
	Uuid       []byte
	StartMibid uint64
	EndMibid   uint64
}

//Nobody better go doing anything with the rest of the system while we do this
//Read as: this is an offline operation. do NOT even have mutating thoughts about the trees...
func (bs *BlockStore) UnlinkBlocksOld(criteria []UnlinkCriteria, except map[uint64]bool) uint64 {
	unlinked := uint64(0)
	for vaddr := uint64(0); vaddr < bs.ptsize; vaddr++ {
		if vaddr%32768 == 0 {
			log.Printf("Scanning vaddr 0x%016x", vaddr)
		}
		allocd, written := bs.VaddrFlags(vaddr)
		if allocd && written {
			dblock := bs.ReadDatablock(vaddr)
			for _, cr := range criteria {
				if bytes.Equal(dblock.GetUUID(), cr.Uuid) {
					mibid := dblock.GetMIBID()
					if mibid >= cr.StartMibid && mibid < cr.EndMibid {
						//MIBID matches, unlink
						bs.UnlinkVaddr(vaddr)
						//bs.ptable[vaddr] &= PADDR_MASK
						unlinked++
					}
					break
				}
			}
		}
	}
	return unlinked
}

type UnlinkCriteriaNew struct {
	Uuid       []byte
	StartGen 	uint64
	EndGen   	uint64
}

func (bs *BlockStore) UnlinkBlocks(criteria []UnlinkCriteriaNew) uint64 {
	unlinked := uint64(0)
	bids := make([]uint32, len(criteria))
	for i := 0; i < len(criteria); i++ {
		bids[i] = UUIDtoIdHint(criteria[i].Uuid)
	}
	for vaddr := uint64(0); vaddr < bs.ptsize; vaddr++ {
		if vaddr%32768 == 0 {
			log.Printf("Scanning vaddr 0x%016x", vaddr)
		}
		allocd, written := bs.VaddrFlags(vaddr)
		idhint, genhint := bs.VaddrHint(vaddr)
		if allocd && written && genhint != 0{
			for i := 0; i < len(criteria); i++ {
				if idhint == bids[i] {
					//UUID possibly matches
					//TODO deal with >32 bit generations by using the saturation property
					if criteria[i].StartGen <= uint64(genhint) && criteria[i].EndGen > uint64(genhint) {
						//The generation probably matches
						//Read block and double check the uuid
						dblock := bs.ReadDatablock(vaddr)
						if bytes.Equal(dblock.GetUUID(), criteria[i].Uuid) {
							log.Printf("Unlinking block: genhint %v, uuid match", genhint)
							bs.UnlinkVaddr(vaddr)
							unlinked++		
							allocd, written := bs.VaddrFlags(vaddr)
							log.Printf("now reads: %v %v",allocd, written)
							goto next
						}
						
					}
				}
			}
		}
		next:
	}
	return unlinked
}

func (bs *BlockStore) UnlinkLeaks() uint64 {
	freed := uint64(0)
	for vaddr := uint64(0); vaddr < bs.ptsize; vaddr++ {
		allocd, written := bs.VaddrFlags(vaddr)
		if allocd && !written {
			bs.UnlinkVaddr(vaddr)
			freed++
		}
	}
	return freed
}
