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
	gens   map[UUID]*Generation
	wlocks map[UUID]*sync.Mutex
	glock  sync.Mutex
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
	dblocks    	[]*Coreblock
	blockstore 	*BlockStore
	flushed    	bool
}

/* Initialise the block store
 */
func (bs *BlockStore) Init(targetserv string) error {
	ses, err := mgo.Dial(targetserv)
	if err != nil {
		return err
	}
	bs.ses = ses
	bs.db = ses.DB("quasaar")
	bs.wlocks = make(map[UUID]*sync.Mutex)
	return nil
}

/*
 * This obtains a generation, blocking if necessary
 */
func (bs *BlockStore) ObtainGeneration(uuid UUID) *Generation {

	//The first thing we do is obtain a write lock on the UUID, as a generation
	//represents a lock
	bs.glock.Lock()
	mtx, ok := bs.wlocks[uuid]
	if !ok {
		//Mutex doesn't exist so is unlocked
		mtx := new(sync.Mutex)
		mtx.Lock()
		bs.wlocks[uuid] = mtx
	} else {
		mtx.Lock()
	}
	bs.glock.Unlock()

	//If we have the generation cached it should be flushed
	gen, ok := bs.gens[uuid]
	if ok && !gen.flushed {
		//This should never happen
		log.Panic("Lock granted with unflushed generation")
	}

	if ok {
		//ok we have the current gen number, lets set the new one
		gen.Gen++
		gen.flushed = false
	} else {
		gen = new(Generation)
		//We need a generation. Lets see if one is on disk
		qry := bs.db.C("superblocks").Find(bson.M{"uuid": uuid[:]})
		rs := Superblock{}
		qerr := qry.Sort("-gen").One(&rs)
		if qerr == mgo.ErrNotFound {
			//Ok just create a new superblock/generation
			gen.Cur_SB = &rs
			gen.Cur_SB.Create(uuid)
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
			gen.Gen = rs.gen + 1
			gen.Cur_SB = &rs
		}
	}
	gen.New_SB = gen.Cur_SB.Clone()
	gen.New_SB.gen = gen.Cur_SB.gen + 1
	gen.blockstore = bs
	return gen
}

func (gen *Generation) Commit() error {
	if gen.flushed {
		return errors.New("Already Flushed")
	}
	for _, db := range gen.dblocks {
		gen.blockstore.WriteCoreblockAndFree(db)
	}
	gen.blockstore.DatablockBarrier()
	if err := gen.blockstore.db.C("superblocks").Insert(gen.New_SB); err != nil {
		log.Panic(err)
	}
	gen.flushed = true
	gen.blockstore.wlocks[gen.uuid].Unlock()
	return nil
}

func (bs *BlockStore) DatablockBarrier() {
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
func (bs *BlockStore) AllocateCoreblock() (*Coreblock, error) {
	cblock := core_pool.Get().(*Coreblock)
	cblock.This_addr = bs.allocateBlock()
	return cblock, nil
}

func (bs *BlockStore) AllocateVectorBlock() (*Vectorblock, error) {
	vblock := vector_pool.Get().(*Vectorblock)
	vblock.This_addr = bs.allocateBlock()
	return vblock, nil
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
func (bs *BlockStore) WriteCoreblockAndFree(cb *Coreblock) error {
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
