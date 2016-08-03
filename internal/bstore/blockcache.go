package bstore

import (
	"time"
)

type CacheItem struct {
	val   Datablock
	vaddr uint64
	newer *CacheItem
	older *CacheItem
}

func (bs *BlockStore) initCache(size uint64) {
	bs.cachemax = size
	bs.cachemap = make(map[uint64]*CacheItem, size)
	go func() {
		for {
			lg.Infof("Cachestats: %d misses, %d hits, %.2f %%",
				bs.cachemiss, bs.cachehit, (float64(bs.cachehit*100) / float64(bs.cachemiss+bs.cachehit)))
			time.Sleep(5 * time.Second)
		}
	}()
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
		bs.cachemiss++
		return nil
	}
	bs.cachemtx.Lock()
	rv, ok := bs.cachemap[vaddr]
	if ok {
		bs.cachePromote(rv)
	}
	bs.cachemtx.Unlock()
	if ok {
		bs.cachehit++
		return rv.val
	} else {
		bs.cachemiss++
		return nil
	}
}

//debug function
func (bs *BlockStore) walkCache() {
	fw := 0
	bw := 0
	it := bs.cachenew
	for {
		if it == nil {
			break
		}
		fw++
		if it.older == nil {
			lg.Info("fw walked to end, compare %p/%p", it, bs.cacheold)
		}
		it = it.older
	}
	it = bs.cacheold
	for {
		if it == nil {
			break
		}
		bw++
		if it.newer == nil {
			lg.Info("bw walked to end, compare %p/%p", it, bs.cachenew)
		}
		it = it.newer
	}
	lg.Info("Walked cache fw=%v, bw=%v, map=%v", fw, bw, len(bs.cachemap))
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
