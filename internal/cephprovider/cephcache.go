// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package cephprovider

import (
	"sync"
	//"runtime"
)

//This is the size of the readahead/behind for caching. It rounds down so it is
//sometimes readahead sometimes readbehind
const R_CHUNKSIZE = 1 << 17
const R_ADDRMASK = ^(uint64(R_CHUNKSIZE) - 1)
const R_OFFSETMASK = (uint64(R_CHUNKSIZE) - 1)

type CephCache struct {
	cachemap  map[uint64]*CacheItem
	cachemiss uint64
	cachehit  uint64
	cacheold  *CacheItem
	cachenew  *CacheItem
	cachemtx  sync.Mutex
	cachelen  uint64
	cachemax  uint64
	cacheinv  uint64
	pool      *sync.Pool
}
type CacheItem struct {
	val   []byte
	addr  uint64
	newer *CacheItem
	older *CacheItem
}

func (cc *CephCache) dropCache() {
	cc.cachemtx.Lock()
	cc.cachemap = make(map[uint64]*CacheItem, cc.cachemax)
	cc.cachemtx.Unlock()
}

func (cc *CephCache) initCache(size uint64) {
	cc.cachemax = size
	cc.cachemap = make(map[uint64]*CacheItem, size)
	cc.pool = &sync.Pool{
		New: func() interface{} {
			return make([]byte, R_CHUNKSIZE)
		},
	}

	// go func() {
	// 	for {
	// 		lg.Infof("Ceph BlockCache: %d invs %d misses, %d hits, %.2f %%",
	// 			cc.cacheinv, cc.cachemiss, cc.cachehit, (float64(cc.cachehit*100) / float64(cc.cachemiss+cc.cachehit)))
	// 		time.Sleep(5 * time.Second)
	// 	}
	// }()
}

//This function must be called with the mutex held
func (cc *CephCache) cachePromote(i *CacheItem) {
	if cc.cachenew == i {
		//Already at front
		return
	}
	if i.newer != nil {
		i.newer.older = i.older
	}
	if i.older != nil {
		i.older.newer = i.newer
	}
	if cc.cacheold == i && i.newer != nil {
		//This was the tail of a list longer than 1
		cc.cacheold = i.newer
	} else if cc.cacheold == nil {
		//This was/is the only item in the list
		cc.cacheold = i
	}

	i.newer = nil
	i.older = cc.cachenew
	if cc.cachenew != nil {
		cc.cachenew.newer = i
	}
	cc.cachenew = i
}

func (cc *CephCache) cachePut(addr uint64, item []byte) {
	if cc.cachemax == 0 {
		return
	}
	cc.cachemtx.Lock()
	i, ok := cc.cachemap[addr]
	if ok {
		cc.cachePromote(i)
	} else {
		i = &CacheItem{
			val:  item,
			addr: addr,
		}
		cc.cachemap[addr] = i
		cc.cachePromote(i)
		cc.cachelen++
		cc.cacheCheckCap()
	}
	cc.cachemtx.Unlock()
}

func (cc *CephCache) getBlank() []byte {
	rv := cc.pool.Get().([]byte)
	rv = rv[0:R_CHUNKSIZE]

	return rv
}

func (cc *CephCache) cacheGet(addr uint64) []byte {
	if cc.cachemax == 0 {
		pmCacheMiss.Inc()
		cc.cachemiss++
		return nil
	}
	cc.cachemtx.Lock()
	rv, ok := cc.cachemap[addr]
	if ok {
		cc.cachePromote(rv)
	}
	cc.cachemtx.Unlock()
	if ok {
		pmCacheHit.Inc()
		cc.cachehit++
		return rv.val
	} else {
		pmCacheMiss.Inc()
		cc.cachemiss++
		return nil
	}
}

//This is rare and only happens if the block cache is too small
func (cc *CephCache) cacheInvalidate(addr uint64) {
	if cc.cachemax == 0 {
		return
	}
	cc.cachemtx.Lock()
	i, ok := cc.cachemap[addr]
	if ok {
		if i.newer != nil {
			i.newer.older = i.older
		}
		if i.older != nil {
			i.older.newer = i.newer
		}
		if cc.cacheold == i {
			//This was the tail of a list longer than 1
			cc.cacheold = i.newer
		}
		if cc.cachenew == i {
			cc.cachenew = i.older
		}
		cc.cachelen--
		cc.cacheinv++
		pmCacheInvalidate.Inc()
		delete(cc.cachemap, addr)
	}
	cc.cachemtx.Unlock()
}

//This must be called with the mutex held
func (cc *CephCache) cacheCheckCap() {
	for cc.cachelen > cc.cachemax {
		i := cc.cacheold

		delete(cc.cachemap, i.addr)
		if i.newer != nil {
			i.newer.older = nil
		}
		cc.cacheold = i.newer
		cc.cachelen--
	}
}
