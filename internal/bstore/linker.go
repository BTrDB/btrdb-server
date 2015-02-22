package bstore

import (
	"github.com/SoftwareDefinedBuildings/quasar/internal/bprovider"
	"sort"
	"sync"
)

var ser_buf_pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, DBSIZE)
	},
}

type pCBArr []*Coreblock

func (dca pCBArr) Len() int {
	return len(dca)
}

func (dca pCBArr) Swap(i, j int) {
	dca[i], dca[j] = dca[j], dca[i]
}

func (dca pCBArr) Less(i, j int) bool {
	return dca[i].PointWidth < dca[j].PointWidth
}

func LinkAndStore(uuid []byte, bs *BlockStore, bp bprovider.StorageProvider, vblocks []*Vectorblock, cblocks []*Coreblock) map[uint64]uint64 {
	loaned_sercbufs := make([][]byte, len(cblocks))
	loaned_servbufs := make([][]byte, len(vblocks))

	//First sort the vblock array (time before lock costs less)
	sort.Sort(pCBArr(cblocks))

	//Then lets lock a segment
	seg := bp.LockSegment(uuid)

	backpatch := make(map[uint64]uint64, len(cblocks)+len(vblocks)+1)
	backpatch[0] = 0 //Null address is still null

	ptr := seg.BaseAddress()

	//First step is to write all the vector blocks, order is not important
	for i := 0; i < len(vblocks); i++ {
		vb := vblocks[i]

		//Store relocation for cb backpatch
		backpatch[vb.Identifier] = ptr
		
		//Update the block. VB should now look as if it were read from disk
		vb.Identifier = ptr
		//So we can cache it
		bs.cachePut(ptr, vb)

		//Now write it
		serbuf := ser_buf_pool.Get().([]byte)
		cutdown := vb.Serialize(serbuf)
		loaned_servbufs[i] = serbuf
		nptr, err := seg.Write(uuid, ptr, cutdown)
		if err != nil {
			log.Panicf("Got error on segment write: %v", err)
		}
		ptr = nptr
	}

	//Now we need to write the coreblocks out
	for i := 0; i < len(cblocks); i++ {
		cb := cblocks[i]

		//Relocate and backpatch
		for k := 0; k < KFACTOR; k++ {
			if cb.Addr[k] < RELOCATION_BASE {
				continue
			}
			nval, ok := backpatch[cb.Addr[k]]
			if !ok {
				log.Panicf("Failed to backpatch! (trying to find addr 0x%016x)", cb.Addr[k])
			}
			cb.Addr[k] = nval
		}
		backpatch[cb.Identifier] = ptr
		cb.Identifier = ptr
		bs.cachePut(ptr, cb)

		serbuf := ser_buf_pool.Get().([]byte)
		cutdown := cb.Serialize(serbuf)
		loaned_sercbufs[i] = serbuf
		nptr, err := seg.Write(uuid, ptr, cutdown)
		if err != nil {
			log.Panicf("Got error on segment write: %v", err)
		}
		ptr = nptr
	}
	seg.Unlock()
	//Return buffers to pool
	for _, v := range loaned_sercbufs {
		ser_buf_pool.Put(v)
	}
	for _, v := range loaned_servbufs {
		ser_buf_pool.Put(v)
	}
	return backpatch
}
