package cephprovider

import (
	"container/heap"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/configprovider"
	"github.com/BTrDB/btrdb-server/internal/jprovider"
	"github.com/ceph/go-ceph/rados"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
)

/*
The journal provider works on a notion of buffers you can write in to.
You ask for a buffer, put your data in it and then it give it back
in exchange for a microversion number. You can wait for that number or
cause a flush to happen
*/

//Replaced during testing to add a delay to each write
var writehook = func() {}

const CJournalProviderNamespace = "journalprovider"

//Keeping RADOS objects small is a good idea
const MaxObjectSize = 16 * 1024 * 1024

var pmCacheHit prometheus.Counter
var pmCacheMiss prometheus.Counter
var pmCacheInvalidate prometheus.Counter

var pmCanFreeCP prometheus.Gauge
var pmBeenFreedCP prometheus.Gauge
var pmMaxFreeCP prometheus.Gauge
var pmFreeListLength prometheus.Gauge

func init() {

	pmCacheHit = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "btrdb",
		Subsystem: "cephstore",
		Name:      "cachehit",
		Help:      "The number of hits off the ceph cache",
	})
	prometheus.MustRegister(pmCacheHit)

	pmCacheMiss = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "btrdb",
		Subsystem: "cephstore",
		Name:      "cachemiss",
		Help:      "The number of misses off the ceph cache",
	})
	prometheus.MustRegister(pmCacheMiss)

	pmCacheInvalidate = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "btrdb",
		Subsystem: "cephstore",
		Name:      "cacheinv",
		Help:      "The number of cache invalidations that have occurred",
	})
	prometheus.MustRegister(pmCacheInvalidate)

	pmCanFreeCP = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "btrdb",
		Subsystem: "journal",
		Name:      "canfree",
		Help:      "The highest checkpoint that can be freed",
	})
	prometheus.MustRegister(pmCanFreeCP)

	pmBeenFreedCP = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "btrdb",
		Subsystem: "journal",
		Name:      "beenfreed",
		Help:      "The highest checkpoint that has been freed",
	})
	prometheus.MustRegister(pmBeenFreedCP)

	pmMaxFreeCP = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "btrdb",
		Subsystem: "journal",
		Name:      "maxfree",
		Help:      "The highest checkpoint that is written and waiting to be freed",
	})
	prometheus.MustRegister(pmMaxFreeCP)

	pmFreeListLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "btrdb",
		Subsystem: "journal",
		Name:      "fll",
		Help:      "The length of the journal free list",
	})
	prometheus.MustRegister(pmFreeListLength)
}
func NewJournalProvider(cfg configprovider.Configuration, ccfg configprovider.ClusterConfiguration) (jprovider.JournalProvider, bte.BTE) {
	conn, err := rados.NewConn()
	if err != nil {
		lg.Panicf("Could not initialize ceph storage: %v", err)
	}
	err = conn.ReadConfigFile(cfg.StorageCephConf())
	if err != nil {
		lg.Panicf("Could not read ceph config: %v", err)
	}
	err = conn.Connect()
	if err != nil {
		lg.Panicf("Could not initialize ceph storage: %v", err)
	}
	return newJournalProvider(ccfg.NodeName(), conn, cfg.StorageCephJournalPool())
}

//Constructs a new journal provider
func newJournalProvider(ournodename string, conn *rados.Conn, pool string) (jprovider.JournalProvider, bte.BTE) {
	wbioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		return nil, bte.ErrW(bte.CephError, "could not open ioctx", err)
	}
	wbioctx.SetNamespace(CJournalProviderNamespace)

	rbioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		return nil, bte.ErrW(bte.CephError, "could not open ioctx", err)
	}
	rbioctx.SetNamespace(CJournalProviderNamespace)

	data := make([]byte, 8)
	nread, err := wbioctx.Read("node/"+ournodename, data, 0)
	if nread != 0 || err != rados.RadosErrorNotFound {
		return nil, bte.ErrF(bte.NodeExisted, "Node %s has existed before", ournodename)
	}
	err = wbioctx.WriteFull("node/"+ournodename, data)
	writehook()
	if err != nil {
		return nil, bte.ErrW(bte.CephError, "could not write node cookie", err)
	}
	rv := &CJournalProvider{
		conn:            conn,
		pool:            pool,
		wbioctx:         wbioctx,
		rbioctx:         rbioctx,
		nodename:        ournodename,
		writebacks:      make(chan writeBackRequest, 10),
		bufferpriq:      make(chan *bufferHandleReq, 10),
		bufferq:         make(chan *bufferHandleReq, 10),
		writtencpchange: make(chan struct{}),
		currentBuffer:   nil,
		canFreeCP:       1, //not inclusive
		beenFreedCP:     1, //also not inclusive
		freelist:        CheckpointHeap{},
		pendingcp:       make(map[cprange]struct{}),
	}
	go rv.freeCheckpoints()
	go rv.printFreeSegmentList()
	go rv.startBufferQAdmission(context.Background())
	for i := 0; i < 10; i++ {
		go rv.startWritebackWorker(context.Background())
	}
	return rv, nil
}

func (jp *CJournalProvider) freeCheckpoints() {
	for {
		time.Sleep(5 * time.Second)
		jp.freelistmu.Lock()
		canFreeCP := jp.canFreeCP
		jp.freelistmu.Unlock()
		if canFreeCP > jp.beenFreedCP {
			lg.Infof("[JRN] performing actual free up to CP=%d", canFreeCP)
			jp.rbmu.Lock()
			jp.releaseJournalEntriesLockHeld(context.Background(), jp.nodename, jprovider.Checkpoint(canFreeCP), &configprovider.FullMashRange)
			jp.rbmu.Unlock()
			jp.beenFreedCP = canFreeCP
			pmBeenFreedCP.Set(float64(jp.beenFreedCP))
		}
	}
}
func (jp *CJournalProvider) printFreeSegmentList() {
	for {
		time.Sleep(5 * time.Second)
		lg.Infof("[JRN] CanFreeCP=%d BeenFreedCP=%d MaxFreeCP=%d FLL=%d", jp.canFreeCP, jp.beenFreedCP, jp.maxFreeCP, jp.freelist.Len())
	}
}
func (jp *CJournalProvider) ReleaseDisjointCheckpoint(ctx context.Context, cp jprovider.Checkpoint) bte.BTE {
	jp.freelistmu.Lock()
	jp.addFreedCheckpointToList(uint64(cp))
	for jp.freelist.Len() > 0 && jp.freelist.Peek() == jp.canFreeCP {
		heap.Pop(&jp.freelist)
		jp.canFreeCP++
	}
	pmCanFreeCP.Set(float64(jp.canFreeCP))
	jp.freelistmu.Unlock()
	return nil
}

//We also need some other metadata that we can use to learn
//what checkpoint number to start from
type CJournalProvider struct {
	conn       *rados.Conn
	pool       string
	wbioctx    *rados.IOContext
	rbioctx    *rados.IOContext
	rbmu       sync.Mutex
	freelistmu sync.Mutex

	//This is the NEXT checkpoint to be written
	writtencp       uint64
	writtencpmu     sync.Mutex
	writtencpchange chan struct{}

	//These are cps that are complete but can't be folded
	//in to writtencp because others before them are outstanding
	pendingcp map[cprange]struct{}

	nodename string

	writebacks chan writeBackRequest

	//Requests to insert into the buffer
	bufferq chan *bufferHandleReq
	//Requests to flip buffers
	bufferpriq    chan *bufferHandleReq
	currentBuffer *bufferEntry

	//The last CP passed to Release proper
	canFreeCP   uint64
	beenFreedCP uint64
	maxFreeCP   uint64
	//maxOfferedCP uint64

	freelist CheckpointHeap
}

//The lock must be held
func (jp *CJournalProvider) addFreedCheckpointToList(cp uint64) bte.BTE {
	if cp > jp.maxFreeCP {
		jp.maxFreeCP = cp
		pmMaxFreeCP.Set(float64(cp))
	}
	heap.Push(&jp.freelist, cp)
	return nil
}

//This captures the parameters to a ceph write
type writeBackRequest struct {
	ObjName string
	//This data is owned by the recipient of the request
	Data         []byte
	Offset       uint64
	StartCP      uint64
	EndCPNonIncl uint64
}

//This is a buffer to be written to
type bufferEntry struct {
	data    []byte
	objname string
	//Which portion of data has been queued for writeback
	bytesWritten uint64

	rng *configprovider.MashRange
	//The very first CP in the buffer
	startCP uint64
	//The CP of the next thing to be written (or first CP of next buffer)
	nextCP uint64
	//The value of nextCP when bytesWritten was last changed
	nextWriteCP uint64
}

//This is a bufferEntry with a channel to signify release of the lock
type bufferHandle struct {
	Err  error
	Buf  *bufferEntry
	Done chan struct{}
}

//This is a request to obtain a bufferHandle
type bufferHandleReq struct {
	ctx    context.Context
	rng    *configprovider.MashRange
	size   int
	cancel func()
	ch     chan *bufferHandle
}

//Range of checkpoints
type cprange struct {
	Start uint64
	End   uint64
}

type objName struct {
	NodeName           string
	StartingCheckpoint uint64
}
type jiterator struct {
	jp         *CJournalProvider
	objectlist []string
	on         *objName
	nn         string
	value      *CJrecord
	buffer     []byte
}

//Mark that the given range of checkpoints is now durable on disk
func (jp *CJournalProvider) markCPRangeWritten(startCP, endCP uint64) {
	//Ensure nobody reads the old writtencp while we are busy
	jp.writtencpmu.Lock()

	//We add ourselves to the pending CPs first in case we are
	//the missing puzzle piece that allows many pending cps to be
	//processed. You can get a deadlock otherwise

	//Add this range to pending CPs
	jp.pendingcp[cprange{Start: startCP, End: endCP}] = struct{}{}

	//Then process all pending CPs. Keep processing them while we
	//find pieces that fit
outer:
	for {
		for cprange, _ := range jp.pendingcp {
			if jp.writtencp == cprange.Start-1 {
				jp.writtencp = cprange.End - 1
				ch := jp.writtencpchange
				jp.writtencpchange = make(chan struct{})
				close(ch)
				delete(jp.pendingcp, cprange)
				continue outer
			}
		}
		break
	}

	jp.writtencpmu.Unlock()
}

//Returns a handle that will get a buffer that can be written to at some point, or a context error if the context times out
func (jp *CJournalProvider) waitForBufferHandle(ctx context.Context, rng *configprovider.MashRange, priority bool, size int) chan *bufferHandle {
	subctx, cancel := context.WithCancel(ctx)
	rv := make(chan *bufferHandle, 2)
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

//Listens for buffer requests, prioritizing ones in the priority queue
func (jp *CJournalProvider) startBufferQAdmission(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		var req *bufferHandleReq
		//Prioritize bufferpriq but if its empty
		//then just take the first new request
		select {
		case req = <-jp.bufferpriq:
		default:
			select {
			case req = <-jp.bufferpriq:
			case req = <-jp.bufferq:
			}
		}
		//If we are satisfying an expired request,
		//ignore it
		if req.ctx.Err() != nil {
			req.ch <- &bufferHandle{
				Err:  req.ctx.Err(),
				Done: nil,
			}
			continue
		}

		//Before we give control of buffer, verify the buffer has
		//the correct range and has enough space
		//This might be a priority request to write out the
		//the buffer, in which case it doesn't care so the size will
		//be zero
		if req.size != 0 {
			if jp.currentBuffer == nil ||
				!jp.currentBuffer.rng.Equal(req.rng) ||
				len(jp.currentBuffer.data)+req.size >= MaxObjectSize {
				if jp.currentBuffer != nil && !jp.currentBuffer.rng.Equal(req.rng) {
					//fmt.Printf("range switch\n")
				}
				if jp.currentBuffer != nil && len(jp.currentBuffer.data)+req.size >= MaxObjectSize {
					//fmt.Printf("len switch %d %d\n", len(jp.currentBuffer.data), jp.currentBuffer.bytesWritten)
				}
				err := jp.writeBackBufferWithFlip(ctx, req.rng)
				if err != nil {
					panic(err)
				}
				//We can continue as normal now
			}
		}
		//Give control of the buffer
		done := make(chan struct{})

		span := opentracing.StartSpan("PQBufferHeldA")
		req.ch <- &bufferHandle{
			Err:  nil,
			Buf:  jp.currentBuffer,
			Done: done,
		}

		//Wait for them to close channel to signal
		//buffer unlocked
		<-done
		span.Finish()
		//They will have done whatever they needed with the buffer
		// they will also have changed the nextCP in the buffer

		//The buffer will be flushed via bufferpriq if there is
		//a checkpoint wait request. If the buffer is getting big
		//we will flush it in the check above
	}
}

//Starts a worker that listens for write requests and fulfills them. Each
//has its own io context but uses a shared connection
func (jp *CJournalProvider) startWritebackWorker(ctx context.Context) {
	// conn, err := rados.NewConn()
	// if err != nil {
	// 	lg.Panicf("Could not initialize ceph storage: %v", err)
	// }
	// err = conn.ReadDefaultConfigFile()
	// if err != nil {
	// 	lg.Panicf("Could not read ceph config: %v", err)
	// }
	// err = conn.Connect()
	// if err != nil {
	// 	lg.Panicf("Could not initialize ceph storage: %v", err)
	// }

	ioctx, err := jp.conn.OpenIOContext(jp.pool)
	if err != nil {
		panic(err)
	}
	ioctx.SetNamespace(CJournalProviderNamespace)
	for {
		if ctx.Err() != nil {
			ioctx.Destroy()
			return
		}

		args := <-jp.writebacks
		if len(args.Data) == 0 {
			panic("data len is zero")
		}
		err := ioctx.Write(args.ObjName, args.Data, args.Offset)
		writehook()
		if err != nil {
			panic(err)
		}
		jp.markCPRangeWritten(args.StartCP, args.EndCPNonIncl)
	}
}

//Write the unwritten portions of the current buffer but do not start a new one
//Returns before buffer is on disk, you must call waitForBufferWritten
//The buffer must be owned by the caller
func (jp *CJournalProvider) writeBackBufferNoFlip(ctx context.Context) bte.BTE {
	if jp.currentBuffer != nil {
		err := jp.writeExistingBuffer(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

//Write the remaining portions of the current buffer and starts a new one
//Returns before buffer is on disk, you must call waitForBufferWritten
//The buffer must be owned by the caller
func (jp *CJournalProvider) writeBackBufferWithFlip(ctx context.Context, newRange *configprovider.MashRange) bte.BTE {
	if jp.currentBuffer != nil {
		err := jp.writeExistingBuffer(ctx)
		if err != nil {
			return err
		}
	}
	err := jp.beginNewObject(newRange)
	return err
}

func (jp *CJournalProvider) Barrier(ctx context.Context, cp jprovider.Checkpoint) bte.BTE {
	if ctx.Err() != nil {
		return bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}

	//Ensure the data before the barrier is written
	err := jp.WaitForCheckpoint(ctx, cp)
	if err != nil {
		return err
	}

	//Put in the barrier
	buf := jp.waitForBufferHandle(ctx, nil, true, 0)
	hnd := <-buf

	//We have the lock
	err = jp.writeBackBufferWithFlip(ctx, jp.currentBuffer.rng)
	if err != nil {
		panic(err)
	}
	//Now we have triggered a writeback, we know it will be done soon
	hnd.Done <- struct{}{}
	return nil
}

//Helper method used by writeBackBufferX. The buffer must be owned by the caller
func (jp *CJournalProvider) writeExistingBuffer(ctx context.Context) bte.BTE {
	if int(jp.currentBuffer.bytesWritten) == len(jp.currentBuffer.data) {
		return nil
	}
	span := opentracing.StartSpan("PQEnqWriteback")
	jp.writebacks <- writeBackRequest{
		ObjName:      jp.currentBuffer.objname,
		Data:         jp.currentBuffer.data[jp.currentBuffer.bytesWritten:],
		Offset:       jp.currentBuffer.bytesWritten,
		StartCP:      jp.currentBuffer.nextWriteCP,
		EndCPNonIncl: jp.currentBuffer.nextCP,
	}
	span.Finish()
	jp.currentBuffer.bytesWritten = uint64(len(jp.currentBuffer.data))
	jp.currentBuffer.nextWriteCP = jp.currentBuffer.nextCP

	return nil
}

//Called without the lock held, waits for the persisted checkpoint number to
//exceed or equal the given checkpoint
func (jp *CJournalProvider) waitForBufferWritten(ctx context.Context, equalCP uint64) bte.BTE {
	for {
		jp.writtencpmu.Lock()
		ch := jp.writtencpchange
		cp := jp.writtencp
		jp.writtencpmu.Unlock()
		if cp >= equalCP {
			return nil
		}
		select {
		case <-ch:
			continue
		case <-ctx.Done():
			return bte.ErrW(bte.ContextError, "context error", ctx.Err())
		}
	}
}

func (on *objName) String() string {
	return fmt.Sprintf("jo/%s/%016x", on.NodeName, on.StartingCheckpoint)
}

//Prepares a new object, called by writeBack functions
func (jp *CJournalProvider) beginNewObject(rng *configprovider.MashRange) bte.BTE {
	if jp.currentBuffer == nil {
		jp.currentBuffer = &bufferEntry{
			nextCP:      1,
			nextWriteCP: 1,
		}
	}
	if jp.currentBuffer.nextWriteCP != jp.currentBuffer.nextCP {
		panic("begin new object called before previous queued for write")
	}
	nextCP := jp.currentBuffer.nextCP

	on := objName{NodeName: jp.nodename, StartingCheckpoint: nextCP}
	ons := on.String()
	//The range that this object covers
	err := jp.wbioctx.SetXattr(ons, "range", rng.Pack())
	if err != nil {
		return bte.ErrW(bte.CephError, "could not set range xattr", err)
	}
	//The released ranges (sequences of 16 byte ranges)
	//Populate relrange with (0,0) as a released range
	err = jp.wbioctx.SetXattr(ons, "relrange", make([]byte, 16))
	if err != nil {
		return bte.ErrW(bte.CephError, "could not set relrange xattr", err)
	}

	jp.currentBuffer.objname = ons
	jp.currentBuffer.data = make([]byte, 0, MaxObjectSize)
	jp.currentBuffer.rng = rng
	jp.currentBuffer.startCP = nextCP
	jp.currentBuffer.nextCP = nextCP
	jp.currentBuffer.nextWriteCP = nextCP
	jp.currentBuffer.bytesWritten = 0
	return nil
}
func ParseObjectName(s string) *objName {
	parts := strings.SplitN(s, "/", -1)
	if parts[0] != "jo" {
		return nil
	}
	in, err := strconv.ParseInt(parts[2], 16, 64)
	if err != nil {
		return nil
	}
	return &objName{NodeName: parts[1], StartingCheckpoint: uint64(in)}
}

func (sp *CephStorageProvider) CreateJournalProvider(ournodename string) (jprovider.JournalProvider, bte.BTE) {
	return newJournalProvider(ournodename, sp.conn, sp.journalPool)
}
func (jp *CJournalProvider) Nodename() string {
	return jp.nodename
}
func (jp *CJournalProvider) Insert(ctx context.Context, rng *configprovider.MashRange, jr *jprovider.JournalRecord) (checkpoint jprovider.Checkpoint, err bte.BTE) {
	if jr == nil {
		return 0, bte.Err(bte.InvariantFailure, "cannot use a nil journal record")
	}
	if ctx.Err() != nil {
		return 0, bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	if rng == nil {
		return 0, bte.Err(bte.InvariantFailure, "cannot use a nil range")
	}
	//fmt.Printf("J queueing %2d points for %s\n", len(jr.Values), uuid.UUID(jr.UUID).String())
	cjr := &CJrecord{
		R: jr,
		//Maximum size int for Msgsize() guess
		C: 0xAAAAAAAAAAAAAAAA,
	}
	sz := cjr.Msgsize()
	span := opentracing.StartSpan("WaitForBufferInsert")
	buf := jp.waitForBufferHandle(ctx, rng, false, sz)
	hnd := <-buf
	if hnd.Err != nil {
		return 0, bte.ErrW(bte.JournalError, "could not obtain handle", hnd.Err)
	}
	span.Finish()
	cp := hnd.Buf.nextCP
	cjr.C = cp

	//TODO maybe marshal directly into buffer
	//TODO once we have data integrity checks better
	data, perr := cjr.MarshalMsg(nil)
	if perr != nil {
		panic(perr)
	}
	hnd.Buf.data = append(hnd.Buf.data, data...)
	hnd.Buf.nextCP++

	//Release the buffer
	hnd.Done <- struct{}{}
	// for {
	// 	curmax := atomic.LoadUint64(&jp.maxOfferedCP)
	// 	if cp > curmax {
	// 		if atomic.CompareAndSwapUint64(&jp.maxOfferedCP, curmax, cp) {
	// 			break
	// 		}
	// 	} else {
	// 		break
	// 	}
	// }
	return jprovider.Checkpoint(cp), nil
}
func (jp *CJournalProvider) WaitForCheckpoint(ctx context.Context, checkpoint jprovider.Checkpoint) bte.BTE {
	if ctx.Err() != nil {
		return bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}

	//Quickly check if we already have it
	jp.writtencpmu.Lock()
	cp := jp.writtencp
	jp.writtencpmu.Unlock()
	if cp >= uint64(checkpoint) {
		return nil
	}

	//TODO this could be just a wait, not a writeback? We could merge writes?
	buf := jp.waitForBufferHandle(ctx, nil, false, 0)
	hnd := <-buf
	if hnd.Err != nil {
		return bte.ErrW(bte.JournalError, "could not obtain buffer handle", hnd.Err)
	}
	//XXTODO check writtencp here as well
	jp.writtencpmu.Lock()
	cp = jp.writtencp
	jp.writtencpmu.Unlock()
	if cp >= uint64(checkpoint) {
		hnd.Done <- struct{}{}
		return nil
	}

	//We have the lock
	err := jp.writeBackBufferNoFlip(ctx)
	if err != nil {
		//TODO backpressure?
		//Mark the CP as likely to not complete?
		//Make the end node reinsert that data elsewhere
		panic(err)
	}
	//Now we have triggered a writeback, we know it will be done soon
	hnd.Done <- struct{}{}
	return jp.waitForBufferWritten(ctx, uint64(checkpoint))
}

//Used by a node taking control of a range, returns an iterator over all unreleased journals
func (jp *CJournalProvider) ObtainNodeJournals(ctx context.Context, nodename string) (jprovider.JournalIterator, bte.BTE) {
	if ctx.Err() != nil {
		return nil, bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	//We need to consume the whole iterator now and sort it
	//so we consume the checkpoints in order
	jp.rbmu.Lock()
	iter, err := jp.rbioctx.Iter()
	if err != nil {
		return nil, bte.ErrW(bte.CephError, "could not open iterator: ", err)
	}
	objectlist := []string{}
	for iter.Next() {
		name := iter.Value()
		objname := ParseObjectName(name)
		if objname == nil {
			continue
		}
		if objname.NodeName != nodename {
			continue
		}
		objectlist = append(objectlist, name)
	}
	iter.Close()

	jp.rbmu.Unlock()
	sort.Strings(objectlist)
	return &jiterator{
		jp:         jp,
		nn:         nodename,
		objectlist: objectlist,
	}, nil
}

//Load in the buffer for use by preparenextrecord
func (it *jiterator) loadrecordbuffer(obj string) {
	data := make([]byte, MaxObjectSize)
	it.jp.rbmu.Lock()
	nread, err := it.jp.rbioctx.Read(obj, data, 0)
	it.jp.rbmu.Unlock()
	if err != nil {
		panic(err)
	}
	it.buffer = data[:nread]
}

//Load in the next CJrecord
func (it *jiterator) preparenextrecord() bool {
	if len(it.buffer) == 0 {
		return false
	}
	r := CJrecord{}
	remaining, err := r.UnmarshalMsg(it.buffer)
	if err != nil {
		panic(err)
	}
	it.buffer = remaining
	it.value = &r
	return true
}

//Get the journal record
func (it *jiterator) Value() (*jprovider.JournalRecord, jprovider.Checkpoint, bte.BTE) {
	if it.value == nil || it.value.R == nil {
		return nil, 0, bte.Err(bte.InvariantFailure, "No iterator value")
	}
	return it.value.R, jprovider.Checkpoint(it.value.C), nil
}

//Go to the next journal record
func (it *jiterator) Next() bool {
	for !it.preparenextrecord() {
		//We need a new buffer
		if len(it.objectlist) == 0 {
			return false
		}
		it.loadrecordbuffer(it.objectlist[0])
		it.objectlist = it.objectlist[1:]
	}
	return true
}

const MaxDistinctRanges = 64

//for a consumed journal, mark a range as done. If the whole object is now released,
//delete it. This is not the same as markCPRangeWritten which is for the write path
//this is for the read path.
func (jp *CJournalProvider) markOrDeleteReleasedRangeLockHeld(objname string, rng *configprovider.MashRange) bte.BTE {
	buffer := make([]byte, 16*MaxDistinctRanges)
	nread, err := jp.rbioctx.GetXattr(objname, "relrange", buffer)
	if err != nil {
		return bte.ErrW(bte.CephError, "could not get relrange xattr", err)
	}
	fullrangeb := make([]byte, 20)
	nread2, err := jp.rbioctx.GetXattr(objname, "range", fullrangeb)
	if err != nil {
		return bte.ErrW(bte.CephError, "could not get range xattr", err)
	}
	if nread2 != 16 {
		return bte.ErrF(bte.CephError, "wrong number of read bytes %d", nread)
	}
	fullrange := configprovider.UnpackMashRange(fullrangeb[:16])

	counter := 0
	ranges := make(map[int]*configprovider.MashRange)
	ranges[counter] = rng
	counter++
	for i := 0; i < nread/16; i++ {
		r := configprovider.UnpackMashRange(buffer[i*16:])
		ranges[counter] = r
		counter++
	}

	change := true
	//O(n^3).. yaay
	//but we don't recover often so this is a rare
	//piece of code
findingchanges:
	for change {
		change = false
		for lhidx, lhs := range ranges {
			for rhidx, rhs := range ranges {
				if rhidx == lhidx {
					continue
				}
				touches, union := lhs.Union(rhs)
				if touches {
					change = true
					ranges[lhidx] = union
					delete(ranges, rhidx)

					continue findingchanges
				}
			}
		}
	}

	//now we have the collapsed list of ranges
	mustdelete := false
	if len(ranges) == 1 {
		var rng *configprovider.MashRange
		for _, r := range ranges {
			rng = r
			break
		}
		if rng.End >= fullrange.End && rng.Start <= fullrange.Start {
			mustdelete = true
		}
	}
	if len(ranges) > MaxDistinctRanges {
		panic(ranges)
	}

	if mustdelete {
		//delete the object
		err := jp.rbioctx.Delete(objname)
		if err != nil {
			return bte.ErrW(bte.CephError, "could not delete object", err)
		}
		return nil
	} else {
		//write out the new ranges
		newserial := make([]byte, len(ranges)*16)
		idx := 0
		for _, r := range ranges {
			packed := r.Pack()
			copy(newserial[idx*16:(idx+1)*16], packed)
			idx++
		}
		err := jp.rbioctx.SetXattr(objname, "relrange", newserial)
		if err != nil {
			return bte.ErrW(bte.CephError, "could not set relrange xattr", err)
		}
		return nil
	}
}

//Used by both the recovering nodes and the generating nodes
//Given that the same journal can be processed by two different nodes
//across different ranges, it is important that the provider only frees resources
//associated with old checkpoints if they have been released across the entire range
//of the journal. The checkpoint is EXCLUSIVE.
func (jp *CJournalProvider) ReleaseJournalEntries(ctx context.Context, nodename string, upto jprovider.Checkpoint, rng *configprovider.MashRange) bte.BTE {
	//TODO if we use a channel for locking we can acquire with context deadline case
	jp.rbmu.Lock()
	defer jp.rbmu.Unlock()
	return jp.releaseJournalEntriesLockHeld(ctx, nodename, upto, rng)
}
func (jp *CJournalProvider) releaseJournalEntriesLockHeld(ctx context.Context, nodename string, upto jprovider.Checkpoint, rng *configprovider.MashRange) bte.BTE {
	if ctx.Err() != nil {
		return bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	iter, err := jp.rbioctx.Iter()
	if err != nil {
		return bte.ErrW(bte.CephError, "iterator error", err)
	}
	//Get all the object names
	allnames := []string{}
	for iter.Next() {
		name := iter.Value()
		objname := ParseObjectName(name)
		if objname == nil {
			continue
		}
		if objname.NodeName != nodename {
			continue
		}
		allnames = append(allnames, name)
	}
	err = iter.Err()
	if err != nil {
		return bte.ErrW(bte.CephError, "iterator error", err)
	}
	iter.Close()
	//Now that we have a sorted list of names, and we know that the objects
	//don't overlap, we know that we can delete a journal object if the object
	//AFTER it has a starting checkpoint greater than or equal to UPTO
	sort.Strings(allnames)
	sort.Sort(sort.Reverse(sort.StringSlice(allnames)))

	//Traverse in reverse order
	var lastcp uint64
	for idx, name := range allnames {
		objname := ParseObjectName(name)
		if idx == 0 {
			//Cannot delete the first(last) object
			//as we don't know where it ends
			lastcp = objname.StartingCheckpoint
			continue
		}
		if lastcp <= uint64(upto) {
			err := jp.markOrDeleteReleasedRangeLockHeld(objname.String(), rng)
			if err != nil {
				return err
			}
		}
		lastcp = objname.StartingCheckpoint
	}

	return nil
}

func (jp *CJournalProvider) GetLatestCheckpoint() jprovider.Checkpoint {
	return jprovider.Checkpoint(atomic.LoadUint64(&jp.currentBuffer.nextCP))
}

func (jp *CJournalProvider) ForgetAboutNode(ctx context.Context, nodename string) bte.BTE {
	jp.rbmu.Lock()
	defer jp.rbmu.Unlock()
	return ForgetAboutNode(ctx, jp.rbioctx, nodename)
}

//A bit like forget about node, but still keep the node name tombstone
func (jp *CJournalProvider) ReleaseAllOurJournals(ctx context.Context) bte.BTE {
	jp.rbmu.Lock()
	defer jp.rbmu.Unlock()
	if ctx.Err() != nil {
		return bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	iter, err := jp.rbioctx.Iter()
	if err != nil {
		return bte.ErrW(bte.CephError, "iterator error", err)
	}
	for iter.Next() {
		name := iter.Value()
		objname := ParseObjectName(name)
		if objname == nil {
			continue
		}
		if objname.NodeName == jp.nodename {
			err := jp.rbioctx.Delete(name)
			if err != nil {
				return bte.ErrW(bte.CephError, "iterator error", err)
			}
		}
	}
	err = iter.Err()
	if err != nil {
		return bte.ErrW(bte.CephError, "iterator error", err)
	}
	iter.Close()
	return nil
}

//Delete all journals associated with a node and also remove the tombstone, allowing
//the same node name to be used again
func ForgetAboutNode(ctx context.Context, ioctx *rados.IOContext, nodename string) bte.BTE {
	if ctx.Err() != nil {
		return bte.ErrW(bte.ContextError, "context error", ctx.Err())
	}
	iter, err := ioctx.Iter()
	if err != nil {
		return bte.ErrW(bte.CephError, "iterator error", err)
	}
	for iter.Next() {
		name := iter.Value()
		objname := ParseObjectName(name)
		if objname == nil {
			continue
		}
		if objname.NodeName == nodename {
			err := ioctx.Delete(name)
			if err != nil {
				return bte.ErrW(bte.CephError, "iterator error", err)
			}
		}
	}
	err = iter.Err()
	if err != nil {
		return bte.ErrW(bte.CephError, "iterator error", err)
	}
	iter.Close()
	err = ioctx.Delete("node/" + nodename)
	if err != nil && err != rados.RadosErrorNotFound {
		return bte.ErrW(bte.CephError, "could not delete object", err)
	}
	return nil
}
