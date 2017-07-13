package bstore

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SoftwareDefinedBuildings/btrdb/internal/bprovider"
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

type LASMetric struct {
	sort   int
	lock   int
	vb     int
	cb     int
	unlock int
	numc   int
	numv   int
}

func (bs *BlockStore) LASMetrics(m *LASMetric) {
	select {
	case bs.laschan <- m:
	default:
		atomic.AddUint64(&bs.lasdropped, 1)
	}

}
func LinkAndStore(uuid []byte, bs *BlockStore, bp bprovider.StorageProvider, vblocks []*Vectorblock, cblocks []*Coreblock) map[uint64]uint64 {
	ta := time.Now()
	loaned_sercbufs := make([][]byte, len(cblocks))
	loaned_servbufs := make([][]byte, len(vblocks))

	//First sort the vblock array (time before lock costs less)
	sort.Sort(pCBArr(cblocks))
	tb := time.Now()
	//Then lets lock a segment
	vseg := bp.LockVectorSegment(uuid)
	cseg := bp.LockCoreSegment(uuid)
	tc := time.Now()
	backpatch := make(map[uint64]uint64, len(cblocks)+len(vblocks)+1)
	backpatch[0] = 0 //Null address is still null

	vptr := vseg.BaseAddress()
	cptr := cseg.BaseAddress()

	//First step is to write all the vector blocks, order is not important
	for i := 0; i < len(vblocks); i++ {
		vb := vblocks[i]

		//Store relocation for cb backpatch
		backpatch[vb.Identifier] = vptr

		//Update the block. VB should now look as if it were read from disk
		vb.Identifier = vptr
		//So we can cache it
		bs.cachePut(vptr, vb)

		//Now write it
		serbuf := ser_buf_pool.Get().([]byte)
		cutdown := vb.Serialize(serbuf)
		loaned_servbufs[i] = serbuf
		nptr, err := vseg.Write(uuid, vptr, cutdown)
		if err != nil {
			log.Panicf("Got error on segment write: %v", err)
		}
		vptr = nptr
	}
	td := time.Now()

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
		backpatch[cb.Identifier] = cptr
		cb.Identifier = cptr
		bs.cachePut(cptr, cb)

		serbuf := ser_buf_pool.Get().([]byte)
		cutdown := cb.Serialize(serbuf)
		loaned_sercbufs[i] = serbuf
		nptr, err := cseg.Write(uuid, cptr, cutdown)
		if err != nil {
			log.Panicf("Got error on segment write: %v", err)
		}
		cptr = nptr
	}
	te := time.Now()
	vseg.Unlock()
	cseg.Unlock()
	//Return buffers to pool
	for _, v := range loaned_sercbufs {
		ser_buf_pool.Put(v)
	}
	for _, v := range loaned_servbufs {
		ser_buf_pool.Put(v)
	}
	tf := time.Now()
	bs.LASMetrics(&LASMetric{
		sort:   int(tb.Sub(ta) / time.Microsecond),
		lock:   int(tc.Sub(tb) / time.Microsecond),
		vb:     int(td.Sub(tc) / time.Microsecond),
		cb:     int(te.Sub(td) / time.Microsecond),
		unlock: int(tf.Sub(te) / time.Microsecond),
		numc:   len(cblocks),
		numv:   len(vblocks)})
	return backpatch
}
