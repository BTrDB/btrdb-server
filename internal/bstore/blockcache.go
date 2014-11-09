package bstore

import (
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type CacheItem struct {
	val   Datablock
	vaddr uint64
	newer *CacheItem
	older *CacheItem
}

//Creates the maps
func (bs *BlockStore) initCache(size uint64) {
	bs.cachemax = size
	bs.cachemap = make(map[uint64]*CacheItem, size)
}

//This function must be called with the mutex held
func (bs *BlockStore) cachePromote(i *CacheItem) {
	if bs.cachenew == i {
		//Already at front
		return
	}
	if i.newer != nil {
		i.newer.older = i.older
	}
	if i.older != nil {
		i.older.newer = i.newer
	}
	if bs.cacheold == i && i.newer != nil {
		//This was the tail of a list longer than 1
		bs.cacheold = i.newer
	} else if bs.cacheold == nil {
		//This was/is the only item in the list
		bs.cacheold = i
	}

	i.newer = nil
	i.older = bs.cachenew
	if bs.cachenew != nil {
		bs.cachenew.newer = i
	}
	bs.cachenew = i
}

func (bs *BlockStore) cachePut(vaddr uint64, item Datablock) {
	if bs.cachemax == 0 {
		return
	}
	bs.cachemtx.Lock()
	i, ok := bs.cachemap[vaddr]
	if ok {
		bs.cachePromote(i)
	} else {
		i = &CacheItem{
			val:   item,
			vaddr: vaddr,
		}
		bs.cachemap[vaddr] = i
		bs.cachePromote(i)
		bs.cachelen++
		bs.cacheCheckCap()
	}
	bs.cachemtx.Unlock()
}

func (bs *BlockStore) cacheGet(vaddr uint64) Datablock {
	if bs.cachemax == 0 {
		return nil
	}
	bs.cachemtx.Lock()
	rv, ok := bs.cachemap[vaddr]
	if ok {
		bs.cachePromote(rv)
	}
	bs.cachemtx.Unlock()
	if ok {
		return rv.val
	} else {
		return nil
	}
}

//This must be called with the mutex held
func (bs *BlockStore) cacheCheckCap() {
	for bs.cachelen > bs.cachemax {
		i := bs.cacheold
		delete(bs.cachemap, i.vaddr)
		if i.newer != nil {
			i.newer.older = nil
		}
		bs.cacheold = i.newer
		bs.cachelen--
	}
}
