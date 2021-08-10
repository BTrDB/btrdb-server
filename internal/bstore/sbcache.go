// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package bstore

import "github.com/pborman/uuid"

// Like roughly 1MB
const SUPERBLOCK_CACHE_SIZE = 32768

// If the superblock cache exceeds the max above, randomly delete this many
// elements from it
const SUPERBLOCK_PRUNE_SIZE = 1024

type sbcachet struct {
	root     uint64
	walltime int64
	gen      uint64
}

func (bs *BlockStore) LoadSuperblockFromCache(uu uuid.UUID) *Superblock {
	if bs.ccfg != nil && !bs.ccfg.WeHoldWriteLockFor(uu) {
		//We are in cluster mode and don't hold the write lock for this uuid
		//so load the superblock from ceph
		return nil
	}
	bs.sbmu.Lock()
	e, ok := bs.sbcache[UUIDToMapKey(uu)]
	bs.sbmu.Unlock()
	if ok {
		return &Superblock{
			gen:      e.gen,
			uuid:     uu,
			root:     e.root,
			walltime: e.walltime,
		}
	}
	return nil
}

func (bs *BlockStore) PutSuperblockInCache(s *Superblock) {
	// Don't cache superblocks we don't hold the write lock for, we can't
	// trust them
	if bs.ccfg != nil && !bs.ccfg.WeHoldWriteLockFor(s.uuid) {
		//We are in cluster mode and don't hold the write lock for this uuid
		//so load the superblock from ceph
		return
	}
	bs.sbmu.Lock()
	if len(bs.sbcache) >= SUPERBLOCK_CACHE_SIZE {
		i := 0
		for k, _ := range bs.sbcache {
			delete(bs.sbcache, k)
			i += 1
			if i >= SUPERBLOCK_PRUNE_SIZE {
				break
			}
		}
	}
	bs.sbcache[UUIDToMapKey(s.uuid)] = &sbcachet{root: s.root, walltime: s.walltime, gen: s.gen}
	bs.sbmu.Unlock()
}

func (bs *BlockStore) FlushSuperblockFromCache(uu uuid.UUID) {
	bs.sbmu.Lock()
	delete(bs.sbcache, UUIDToMapKey(uu))
	bs.sbmu.Unlock()
}
