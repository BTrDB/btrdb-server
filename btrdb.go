// +build ignore

package btrdb

import (
	"context"
	"sync"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/jprovider"
	"github.com/BTrDB/btrdb-server/qtree"
	"github.com/pborman/uuid"
)


/*
per stream struct
lock global, get stream struct, release global
stream structs are never freed, never change pointers

* we want double buffers so that streams of small inserts always see low latency

* insert is synchronous, either to journal or to primary storage
* if insert returns error, the data (superblock) MUST not be written

get priamry storage handle
write content to journal
either pass it to write to primary OR append it to buffer
release primary storage handle

*/


const MaxPQMBufferSize = 1024

type Record qtree.Record

//This structure must not contain pointers
//to improve GC performance. Using a preallocated
//buffer also reduces allocations on insert
type MuxEntry struct {
	PointBuf []Record
	Length   int
}

type PQM struct {
	mu    sync.Mutex
	buffers map[[16]byte]MuxEntry

	jp jprovider.JournalProvider
}

type primaryStorageHandle struct {
	done chan struct {}
}
type primaryStorageHandleRequest struct {
	ctx context.Context
	cancel func ()
	ch chan *primaryStorageHandle
}
func (q *Quasar) waitForPrimaryStorageHandle(ctx context.Context, writing bool) chan *primaryStorageHandle {
	subctx, cancel := context.WithCancel(ctx)
	rv := make(chan *primaryStorageHandle, 2)
	bhreq := &bufferHandleReq{
		ctx:    subctx,
		rng:    rng,
		size:   size,
		cancel: cancel,
		ch:     rv,
	}
	target := jp.bufferq
	if priority {
		target = jp.bufferpriq
	}
	//Try and put the request in the queue.
	//If we fail to do so before the context times out
	//Then generate an error response and return it
	select {
	case target <- bhreq:
		return rv
	case <-ctx.Done():
		cancel()
		rv <- &bufferHandle{
			Err:  ctx.Err(),
			Done: nil,
		}
		return rv
	}
}
func (q *Quasar) primaryStorageInsert(ctx context.Context, id uuid.UUID, r []Record) bte.BTE {
  panic("ni")
}
func (q *Quasar) InsertValues(ctx context.Context, id uuid.UUID, r []Record) bte.BTE {

	//if the slice is already too big
	//?
	q.pqm.mu.Lock()
	defer q.pqm.mu.Unlock()
	var aid [16]byte
  copy(aid[:], id[:])
  newBufferSize := q.pqm.mu.buffers[aid].length + len(r)
  if newBufferSize >= MaxPQMBufferSize {
    insertion := make([]Record, newBufferSize)
    copy(insertion[:], r)
    copy(insertion[len(r):, q.pqm.mu.buffers[aid].PointBuf[:q.pqm.mu.buffers[aid].length])
    err := q.primaryStorageInsert(context.Background(), id, insertion)
  }
}


//
// func (q *Quasar) Flush(ctx context.Context, id uuid.UUID) bte.BTE {
// 	//rewrite
// }
//
//
// func (q *Quasar) QueryValuesStream(ctx context.Context, id uuid.UUID, start int64, end int64, gen uint64) (chan qtree.Record, chan bte.BTE, uint64) {
// 	//do old impl
//   //mix in the mux
// }
//
// func (q *Quasar) QueryStatisticalValuesStream(ctx context.Context, id uuid.UUID, start int64, end int64,
// 	gen uint64, pointwidth uint8) (chan qtree.StatRecord, chan bte.BTE, uint64) {
// 	//do old impl
//   //mix in the mux
// }
//
// func (q *Quasar) QueryWindow(ctx context.Context, id uuid.UUID, start int64, end int64,
// 	gen uint64, width uint64, depth uint8) (chan qtree.StatRecord, chan bte.BTE, uint64) {
// 	//do old impl
//   //mix in the mux
// }
//
// func (q *Quasar) QueryGeneration(ctx context.Context, id uuid.UUID) (uint64, bte.BTE) {
// 	//TODO what is the difference between this andversion?
// }
//
// func (q *Quasar) QueryNearestValue(ctx context.Context, id uuid.UUID, time int64, backwards bool, gen uint64) (qtree.Record, bte.BTE, uint64) {
// 	//Do old impl
//   //Check if any result in the buffer lies between the result and the given time
//   //and switch to that if required
// }
//
//
// //Resolution is how far down the tree to go when working out which blocks have changed. Higher resolutions are faster
// //but will give you back coarser results.
// func (q *Quasar) QueryChangedRanges(ctx context.Context, id uuid.UUID, startgen uint64, endgen uint64, resolution uint8) (chan ChangedRange, chan bte.BTE, uint64) {
// 	//Do old impl
//   //For every result in the buffer add a resolution sized window to the result
//   //Ensuring you are deduplicated
// }
//
// func (q *Quasar) DeleteRange(ctx context.Context, id uuid.UUID, start int64, end int64) bte.BTE {
// 	//Flush
//   //Do old impl
// }
//
// // Get a stream annotations and tags
// func (q *Quasar) GetStreamVersion(ctx context.Context, uuid []byte) (major uint64, minor uint64, err bte.BTE) {
// 	//Get major version
//   //Get length of stream cache as minor version
// }
//
// // DeleteStream tombstones a stream
// func (q *Quasar) ObliterateStream(ctx context.Context, id []byte) bte.BTE {
// 	//remove all entries from cache
//   //do old impl
// }
