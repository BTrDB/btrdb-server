package bstore

import (
	"errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"sync"
)

const LatestGeneration = uint64(^(uint64(0)))
type UUID [16]byte
type BlockStore struct {
	ses    *mgo.Session
	db     *mgo.Database
	_gens   map[UUID]*Generation
	_wlocks map[UUID]*sync.Mutex
	glock  sync.RWMutex
}

var block_buf_pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, DBSIZE)
	},
}

var core_pool = sync.Pool{
	New: func() interface{} {
		return new(Coreblock)
	},
}
var vector_pool = sync.Pool {
	New: func() interface{} {
		return new(Vectorblock)
	},
}

var ErrDatablockNotFound = errors.New("Coreblock not found")

/* A generation stores all the information acquired during a write pass.
 * A superblock contains all the information required to navigate a tree.
 */
type Generation struct {
	Gen 		uint64
	uuid		UUID
	Cur_SB  	*Superblock
	New_SB		*Superblock
	cblocks    	[]*Coreblock
	vblocks    	[]*Vectorblock
	blockstore 	*BlockStore
	flushed    	bool
}

/*
func (bs *BlockStore) gens(uuid UUID) (*Generation, bool) {
	bs.glock.RLock()
	defer bs.glock.RUnlock()
	rv, ok := bs._gens[uuid]
	return rv, ok
}

func (bs *BlockStore) wlocks(uuid UUID) (*
*/

func NewBlockStore (targetserv string) (*BlockStore, error) {
	bs := BlockStore{}
	ses, err := mgo.Dial(targetserv)
	if err != nil {
		return nil, err
	}
	bs.ses = ses
	bs.db = ses.DB("quasaar")
	bs._wlocks = make(map[UUID]*sync.Mutex)
	bs._gens = make(map[UUID]*Generation)
	return &bs, nil
}

/*
 * This obtains a generation, blocking if necessary
 */
func (bs *BlockStore) ObtainGeneration(uuid UUID) *Generation {
	log.Printf("obtaining generation")
	//The first thing we do is obtain a write lock on the UUID, as a generation
	//represents a lock
	bs.glock.RLock()
	mtx, ok := bs._wlocks[uuid]
	bs.glock.RUnlock()
	if !ok {
		//Mutex doesn't exist so is unlocked
		mtx := new(sync.Mutex)
		mtx.Lock()
		bs.glock.Lock()
		bs._wlocks[uuid] = mtx
		bs.glock.Unlock()
	} else {
		mtx.Lock()
	}
	

	//If we have the generation cached it should be flushed
	bs.glock.RLock()
	gen, ok := bs._gens[uuid]
	bs.glock.RUnlock()
	if ok && !gen.flushed {
		//This should never happen
		log.Panic("Lock granted with unflushed generation")
	}

	if ok {
		//ok we have the current gen number, lets set the new one
		log.Printf("reusing current gen")
		gen.Gen++
		gen.flushed = false
	} else {
		log.Printf("creating new gen")
		gen = &Generation{
			cblocks: make([]*Coreblock, 0, 32),
			vblocks: make([]*Vectorblock, 0, 32),
		}
		//We need a generation. Lets see if one is on disk
		qry := bs.db.C("superblocks").Find(bson.M{"uuid": uuid[:]})
		rs := Superblock{}
		qerr := qry.Sort("-gen").One(&rs)
		if qerr == mgo.ErrNotFound {
			log.Printf("no superblock found for UUID")
			//Ok just create a new superblock/generation
			gen.Cur_SB = &rs
			gen.Cur_SB = NewSuperblock(uuid)
			gen.Gen = 1
			
			//Put it in the DB
			if err := bs.db.C("superblocks").Insert(rs); err != nil {
				log.Panic(err)
			}
		} else if qerr != nil {
			//Well thats more serious
			log.Panic(qerr)
		} else {
			//Ok we have a superblock, pop the gen
			log.Printf("found a superblock")
			gen.Gen = rs.gen + 1
			gen.Cur_SB = &rs
		}
	}
	gen.New_SB = gen.Cur_SB.Clone()
	gen.New_SB.gen = gen.Cur_SB.gen + 1
	gen.blockstore = bs
	bs.glock.Lock()
	bs._gens[uuid] = gen
	bs.glock.Unlock()
	return gen
}

func (gen *Generation) Commit() error {
	if gen.flushed {
		return errors.New("Already Flushed")
	}
	for _, cb := range gen.cblocks {
		gen.blockstore.writeCoreblockAndFree(cb)
	}
	gen.cblocks = nil
	for _, vb := range gen.vblocks {
		gen.blockstore.writeVectorblockAndFree(vb)
	}
	gen.vblocks = nil
	gen.blockstore.datablockBarrier()
	if err := gen.blockstore.db.C("superblocks").Insert(gen.New_SB); err != nil {
		log.Panic(err)
	}
	gen.flushed = true
	gen.blockstore.glock.RLock()
	gen.blockstore._wlocks[gen.uuid].Unlock()
	gen.blockstore.glock.RUnlock()
	return nil
}

func (bs *BlockStore) datablockBarrier() {
	//Block until all datablocks have finished writing
	bs.ses.Fsync(false)
}

func (bs *BlockStore) allocateBlock() uint64 {
	//TODO the real system will have an allocator that makes
	//unique 64 bit addresses. We don't have tha yet, so we
	//try make one from the mongo ID and timestamp
	id := bson.NewObjectId()
	cnt := id.Counter()
	tm := uint64(id.Time().Unix() & 0xFFFFFFFF)
	addr := (tm << 32) + (uint64(cnt) & 0xFFFFFFFF)
	addr &= 0x7FFFFFFFFFFFFFFF
	return addr
}
/**
 * The real function is supposed to allocate an address for the data
 * block, reserving it on disk, and then give back the data block that
 * can be filled in
 * This stub makes up an address, and mongo pretends its real
 */
func (gen *Generation) AllocateCoreblock() (*Coreblock, error) {
	cblock := core_pool.Get().(*Coreblock)
	cblock.This_addr = gen.blockstore.allocateBlock()
	cblock.Generation = gen.Gen
	gen.cblocks = append(gen.cblocks, cblock)
	return cblock, nil
}

func (gen *Generation) AllocateVectorblock() (*Vectorblock, error) {
	vblock := vector_pool.Get().(*Vectorblock)
	vblock.This_addr = gen.blockstore.allocateBlock()
	vblock.Generation = gen.Gen
	gen.vblocks = append(gen.vblocks, vblock)
	return vblock, nil
}

func (bs *BlockStore) FreeCoreblock(cb *Coreblock) {
	core_pool.Put(cb)
}

func (bs *BlockStore) FreeVectorblock(vb *Vectorblock) {
	vector_pool.Put(vb)
}
type fake_dblock_t struct {
	Addr uint64
	Data []byte
}

/**
 * The real function is meant to now write back the contents
 * of the data block to the address. This just uses the address
 * as a key
 */
func (bs *BlockStore) writeCoreblockAndFree(cb *Coreblock) error {
	syncbuf := block_buf_pool.Get().([]byte)
	cb.Serialize(syncbuf)
	ierr := bs.db.C("dblocks").Insert(fake_dblock_t{cb.This_addr, syncbuf[:]})
	if ierr != nil {
		log.Panic(ierr)
	}
	block_buf_pool.Put(syncbuf)
	core_pool.Put(cb)
	return nil
}

func (bs *BlockStore) writeVectorblockAndFree(vb *Vectorblock) error {
	syncbuf := block_buf_pool.Get().([]byte)
	vb.Serialize(syncbuf)
	ierr := bs.db.C("dblocks").Insert(fake_dblock_t{vb.This_addr, syncbuf[:]})
	if ierr != nil {
		log.Panic(ierr)
	}
	block_buf_pool.Put(syncbuf)
	vector_pool.Put(vb)
	return nil
}

func (bs *BlockStore) ReadDatablock(addr uint64) (Datablock, error) {
	//rv := db_block_pool.Get().(*Coreblock)
	frecord := fake_dblock_t{}
	qry := bs.db.C("dblocks").Find(bson.M{"addr": addr})
	qerr := qry.One(&frecord)
	if qerr == mgo.ErrNotFound {
		return nil, ErrDatablockNotFound
	} else if qerr != nil {
		log.Panic(qerr)
	}
	switch DatablockGetBufferType(frecord.Data) {
	case Core:
		rv := core_pool.Get().(*Coreblock)
		rv.Deserialize(frecord.Data)
		return rv, nil
	case Vector:
		rv := vector_pool.Get().(*Vectorblock)
		rv.Deserialize(frecord.Data)
		return rv, nil
	}
	log.Panic("Strange datablock type")
	return nil, nil
}

func (bs *BlockStore) LoadSuperblock(uuid UUID, generation uint64) (*Superblock) {
	var sb = Superblock{}
	if generation == LatestGeneration {
		qry := bs.db.C("supeblocks").Find(bson.M{"uuid":uuid[:]})
		if err := qry.Sort("-gen").One(&sb); err != nil {
			if err == mgo.ErrNotFound {
				return nil
			} else {
				log.Panic(err)
			}
		}
	} else {
		qry := bs.db.C("superblocks").Find(bson.M{"uuid":uuid[:],"gen":generation})
		if err := qry.One(&sb); err != nil {
			if err == mgo.ErrNotFound {
				return nil
			} else {
				log.Panic(err)
			}
		}		
	}
	return &sb
}
