package bstore

import (
	"sync"
	"github.com/SoftwareDefinedBuildings/quasar/internal/bprovider"
	"sort"
)

var ser_buf_pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, DBSIZE)
	},
}

type pDBArr []Datablock
func (dba pDBArr) Len() int {
	return len(dba)
}

func (dba pDBArr) Swap(i, j int) {
	dba[i], dba[j] = dba[j], dba[i]
}

func (dba pDBArr) Less(i, j int) bool {
	if dba[i].GetDatablockType() == Vector {
		if dba[j].GetDatablockType() == Core {
			return true
		} else {
			return false
		}
	} else {
		if dba[j].GetDatablockType() == Vector {
			return false
		}
		return dba[i].(*Coreblock).PointWidth < dba[j].(*Coreblock).PointWidth
	}
}

func LinkAndStore(bp bprovider.StorageProvider, dblocks []Datablock) {
	loaned_serbufs := make([][]byte, len(dblocks))

	//First sort the array (time before lock costs less)
	sort.Sort(pDBArr(dblocks))
	
	//Then lets lock a segment
	seg := bp.LockSegment()

	backpatch := make(map[uint64]uint64,len(dblocks))
	backpatch[0] = 0 //Null address is still null
	
	ptr := seg.BaseAddress()
	
	//First step is to write all the datablocks, order is not important
	var i int
	for i=0; i<len(dblocks); i++ {
		if dblocks[i].GetDatablockType() != Vector {
			break
		}
		
		vb := dblocks[i].(*Vectorblock)
		
		//Store relocation for cb backpatch
		backpatch[vb.Identifier] = ptr
		
		//Now write it
		serbuf := ser_buf_pool.Get().([]byte)
		cutdown := vb.Serialize(serbuf)
		loaned_serbufs[i] = serbuf
		nptr, err := seg.Write(ptr, cutdown)
		if err != nil {
			log.Panicf("Got error on segment write: %v", err)
		}
		ptr = nptr
	}
	
	//Now we need to write the coreblocks out
	for ;i<len(dblocks);i++ {
		cb := dblocks[i].(*Coreblock)
		
		//Relocate and backpatch
		for k:=0;k<KFACTOR;k++ {
			nval, ok := backpatch[cb.Addr[k]]
			if !ok {
				log.Panicf("Failed to backpatch!")
			}
			cb.Addr[k] = nval
		}
		backpatch[cb.Identifier] = ptr
		
		serbuf := ser_buf_pool.Get().([]byte)
		cutdown := cb.Serialize(serbuf)
		loaned_serbufs[i] = serbuf
		nptr, err := seg.Write(ptr, cutdown)
		if err != nil {
			log.Panicf("Got error on segment write: %v", err)
		}
		ptr = nptr
	}
	
	seg.Unlock()
}