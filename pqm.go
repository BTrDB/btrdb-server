package btrdb

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/configprovider"
	"github.com/BTrDB/btrdb-server/internal/jprovider"
	"github.com/BTrDB/btrdb-server/qtree"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pborman/uuid"
)

type Record = qtree.Record

type StorageInterface interface {
	JP() jprovider.JournalProvider
	CP() configprovider.ClusterConfiguration

	//Unthrottled
	WritePrimaryStorage(ctx context.Context, id uuid.UUID, r []Record) (major uint64, err bte.BTE)
	//Appropriate locks will be held
	StreamMajorVersion(ctx context.Context, id uuid.UUID) (uint64, bte.BTE)
}

//This number should be >2000 for decent storage efficiency.
//If it is too large then recovery of journals can take a long time
const MaxPQMBufferSize = 32768

//TODO this should be more like a few hours
//Even a few minutes
const MaxPQMBufferAge = 8 * time.Hour

type PQM struct {
	si       StorageInterface
	globalMu sync.Mutex
	streams  map[[16]byte]*streamEntry

	//TODO replace with real scheme
	hackmu sync.Mutex
}

type streamEntry struct {
	mu sync.Mutex
	//The last commit
	majorVersion uint64
	buffer       []Record
	checkpoints  []jprovider.Checkpoint
	openTime     time.Time
}
type psHandle struct {
	pqm *PQM
}

func (psh *psHandle) Done() {
	psh.pqm.hackmu.Unlock()
}

func NewPQM(si StorageInterface) *PQM {
	rv := &PQM{
		si:      si,
		streams: make(map[[16]byte]*streamEntry),
	}
	go rv.scanForOldBuffers()
	si.CP().WatchMASHChange(rv.mashChange)
	return rv
}

func (pqm *PQM) mashChange(flushComplete chan struct{}, active configprovider.MashRange, proposed configprovider.MashRange) {
	//Flush all streams
	lg.Warningf("[MASHCHANGE] acquiring global lock for flush")
	pqm.globalMu.Lock()
	idx := 0
	wg := sync.WaitGroup{}
	wg.Add(len(pqm.streams))
	parallel := make(chan bool, 100)
	for id, st := range pqm.streams {
		idx++
		parallel <- true
		go func(idx int, id [16]byte, st *streamEntry) {
			lg.Warningf("[MASHCHANGE] locking stream %d/%d for flush", idx, len(pqm.streams))
			st.mu.Lock()
			pqm.flushLockHeld(context.Background(), uuid.UUID(id[:]), st)
			st.mu.Unlock()
			<-parallel
			wg.Done()
		}(idx, id, st)
	}
	wg.Wait()
	lg.Warningf("[MASHCHANGE] stream flush complete")
	jrnstart := time.Now()
	cs := pqm.si.CP().GetCachedClusterState()
	idx = 0
	for _, mbr := range cs.Members {
		idx++
		if mbr.IsIn() {
			lg.Warningf("[MASHCHANGE] skipping journals for member %d/%d [%s] (is in); jcheck_total=%s", idx, len(cs.Members), mbr.Nodename, time.Now().Sub(jrnstart))
			continue
		} else {
			lg.Warningf("[MASHCHANGE] checking journals for member %d/%d [%s]; jcheck_total=%s", idx, len(cs.Members), mbr.Nodename, time.Now().Sub(jrnstart))
		}
		strt := time.Now()
		pqm.mashChangeProcessJournals(mbr.Nodename, &proposed)
		lg.Warningf("[MASHCHANGE] journals complete member %d/%d thismember=%s; jcheck_total=%s", idx, len(cs.Members), time.Now().Sub(strt), time.Now().Sub(jrnstart))
	}
	lg.Warningf("[MASHCHANGE] this node is ready to move to the next MASH")
	close(flushComplete)
	pqm.globalMu.Unlock()
}

func (pqm *PQM) mashChangeProcessJournals(nodename string, rng *configprovider.MashRange) {
	if rng.Start == rng.End {
		lg.Warningf("[MASHCHANGE] [JRN/%s] skip all, zero range", nodename)
		return
	}
	procjrnctx, procjrncancel := context.WithCancel(context.Background())
	defer procjrncancel()
	var queued int64
	var skipped int64
	var recovered int64
	var outofrange int64
	go func() {
		for {
			time.Sleep(1 * time.Second)
			if procjrnctx.Err() != nil {
				return
			}
			lg.Warningf("[MASHCHANGE] [JRN/%s] skipping=%d notus=%d queued=%d recovered=%d (%.1f %%)", nodename, skipped, outofrange, queued, recovered, float64(recovered*100)/float64(queued))
		}
	}()
	iter, err := pqm.si.JP().ObtainNodeJournals(context.Background(), nodename)
	if err != nil {
		panic(err)
	}
	versioncache := make(map[[16]byte]uint64)
	toinsert := []*jprovider.JournalRecord{}
	var lastcp jprovider.Checkpoint
	for iter.Next() {
		jrn, cp, err := iter.Value()
		if err != nil {
			panic(err)
		}
		lastcp = cp

		if !rng.SuperSetOfUUID(jrn.UUID) {
			outofrange++
			//fmt.Printf("IGNORING JOURNAL (R) n=%s uu=%s mv=%d rmv=%d len=%d\n", nodename, uuid.UUID(jrn.UUID).String(), jrn.MajorVersion, maj, len(jrn.Times))
			continue
		}

		maj, ok := versioncache[uuid.UUID(jrn.UUID).Array()]
		if !ok {
			var err error
			maj, err = pqm.si.StreamMajorVersion(context.Background(), jrn.UUID)
			if err != nil {
				panic(err)
			}
			versioncache[uuid.UUID(jrn.UUID).Array()] = maj
		}

		//We need to accumulate the ones that need inserting into a list so that
		//we don't accidentally ignore multiple entries with same uu/version by incrementing
		//the stream version when isnerting
		if maj == jrn.MajorVersion {
			queued += int64(len(jrn.Values))
			//fmt.Printf("RECOVERING JOURNAL n=%s uu=%s mv=%d rmv=%d len=%d\n", nodename, uuid.UUID(jrn.UUID).String(), jrn.MajorVersion, maj, len(jrn.Times))
			toinsert = append(toinsert, jrn)
		} else {
			skipped++
			//fmt.Printf("IGNORING JOURNAL (V) n=%s uu=%s mv=%d rmv=%d len=%d\n", nodename, uuid.UUID(jrn.UUID).String(), jrn.MajorVersion, maj, len(jrn.Times))
		}
	}
	insertmap := make(map[[16]byte][]qtree.Record)
	for _, jrn := range toinsert {
		r := make([]qtree.Record, len(jrn.Times))
		for idx, _ := range jrn.Times {
			r[idx].Time = jrn.Times[idx]
			r[idx].Val = jrn.Values[idx]
		}
		insertmap[uuid.UUID(jrn.UUID).Array()] = append(insertmap[uuid.UUID(jrn.UUID).Array()], r...)
	}
	for uu, recs := range insertmap {
		_, err := pqm.si.WritePrimaryStorage(context.Background(), uu[:], recs)
		if err != nil {
			panic(err)
		}
		recovered += int64(len(recs))
		//fmt.Printf("RECOVERED %d POINTS FOR %s\n", len(recs), uuid.UUID(uu[:]).String())
	}
	if lastcp != 0 {
		err := pqm.si.JP().ReleaseJournalEntries(context.Background(), nodename, lastcp, rng)
		if err != nil {
			panic(err)
		}
	}
	lg.Warningf("[MASHCHANGE] [JRN/%s] COMPLETE queued=%d skipping=%d recovered=%d\n", nodename, queued, skipped, recovered)

}

func (pqm *PQM) scanForOldBuffers() {
	for {
		time.Sleep(2 * time.Minute)
		nw := time.Now()
		todo := []uuid.UUID{}
		then := time.Now()
		pqm.globalMu.Lock()
		for uu, st := range pqm.streams {
			st.mu.Lock()
			if len(st.checkpoints) > 0 && nw.Sub(st.openTime) > MaxPQMBufferAge {
				arr := uu
				todo = append(todo, uuid.UUID(arr[:]))
			}
			st.mu.Unlock()
		}
		pqm.globalMu.Unlock()
		lg.Infof("scanning for old PQM buffers took %.1f ms", float64(time.Now().Sub(then)/time.Microsecond)/1000.)
		for _, uu := range todo {
			_, _, err := pqm.Flush(context.Background(), uu)
			if err != nil {
				panic(err)
			}
		}
	}

}

//Flush all open buffers
func (pqm *PQM) InitiateShutdown() chan struct{} {
	rv := make(chan struct{})
	go func() {
		pqm.globalMu.Lock()

		idx := 0
		wg := sync.WaitGroup{}
		wg.Add(len(pqm.streams))
		parallel := make(chan bool, 100)
		for id, st := range pqm.streams {
			idx++
			parallel <- true
			go func(idx int, id [16]byte, st *streamEntry) {
				lg.Warningf("[SHUTDOWN] locking stream %d/%d for flush\n", idx, len(pqm.streams))
				st.mu.Lock()
				pqm.flushLockHeld(context.Background(), uuid.UUID(id[:]), st)
				<-parallel
				wg.Done()
			}(idx, id, st)
		}
		wg.Wait()
		pqm.si.JP().ReleaseAllOurJournals(context.Background())
		close(rv)
	}()
	return rv
}
func (pqm *PQM) flushLockHeld(ctx context.Context, id uuid.UUID, st *streamEntry) (maj uint64, min uint64, err bte.BTE) {
	if len(st.buffer) == 0 {
		return st.majorVersion, 0, nil
	}
	maj, err = pqm.si.WritePrimaryStorage(ctx, id, st.buffer)
	if err != nil {
		return 0, 0, err
	}
	for _, cp := range st.checkpoints {
		err := pqm.si.JP().ReleaseDisjointCheckpoint(ctx, cp)
		if err != nil {
			return 0, 0, err
		}
	}
	st.checkpoints = []jprovider.Checkpoint{}
	st.buffer = st.buffer[:0]
	st.majorVersion = maj
	return maj, 0, nil
}
func (pqm *PQM) Flush(ctx context.Context, id uuid.UUID) (maj uint64, min uint64, err bte.BTE) {
	pqm.globalMu.Lock()
	st, ok := pqm.streams[id.Array()]
	pqm.globalMu.Unlock()
	if ok {
		st.mu.Lock()
		defer st.mu.Unlock()
		return pqm.flushLockHeld(ctx, id, st)
	}
	maj, err = pqm.si.StreamMajorVersion(ctx, id)
	if err != nil {
		return 0, 0, err
	}
	return maj, 0, err
}
func (pqm *PQM) GetPSHandle(ctx context.Context) (*psHandle, bte.BTE) {
	rv := &psHandle{pqm: pqm}
	pqm.hackmu.Lock()
	return rv, nil
}

func (pqm *PQM) MergeNearestValue(ctx context.Context, id uuid.UUID, time int64,
	backwards bool, parentRec Record) (r Record, err bte.BTE, maj uint64, min uint64) {
	maj, min, buf, err := pqm.MuxContents(ctx, id)
	if err != nil {
		return qtree.Record{}, err, 0, 0
	}
	chosenrec := parentRec
	for _, bufrec := range buf {
		if backwards {
			if bufrec.Time > chosenrec.Time && chosenrec.Time < time {
				chosenrec = bufrec
			}
		} else {
			if bufrec.Time < chosenrec.Time && bufrec.Time >= time {
				chosenrec = bufrec
			}
		}
	}
	return chosenrec, nil, maj, min
}

func (pqm *PQM) QueryVersion(ctx context.Context, id uuid.UUID) (maj uint64, min uint64, err bte.BTE) {
	arrid := idSliceToArr(id)

	pqm.globalMu.Lock()
	streamEntry, ok := pqm.streams[arrid]
	pqm.globalMu.Unlock()
	if !ok {
		maj, err := pqm.si.StreamMajorVersion(ctx, id)
		if err != nil {
			return 0, 0, err
		}
		return maj, 0, nil
	}
	streamEntry.mu.Lock()
	rvmaj := streamEntry.majorVersion
	rvmin := uint64(len(streamEntry.buffer))
	streamEntry.mu.Unlock()
	return rvmaj, rvmin, nil
}

func (pqm *PQM) GetChangedRanges(ctx context.Context, id uuid.UUID, resolution uint8) ([]ChangedRange, bte.BTE, uint64, uint64) {
	maj, min, buf, err := pqm.MuxContents(ctx, id)
	if err != nil {
		panic(err)
	}
	if len(buf) == 0 {
		return nil, nil, maj, min
	}
	crz := make(map[int64]ChangedRange)
	//Parent will coalesce but we have to do it in order
	for _, e := range buf {
		start := e.Time
		start &= ^((1 << resolution) - 1)
		crz[start] = ChangedRange{
			Start: start,
			End:   start + 1<<resolution,
		}
	}
	crzslice := changedRangeSliceSorter{}
	for _, el := range crz {
		crzslice = append(crzslice, el)
	}
	sort.Sort(crzslice)
	return crzslice, nil, maj, min
}

type changedRangeSliceSorter []ChangedRange

func (crz changedRangeSliceSorter) Len() int {
	return len(crz)
}
func (crz changedRangeSliceSorter) Less(i, j int) bool {
	return crz[i].Start < crz[j].Start
}
func (crz changedRangeSliceSorter) Swap(i, j int) {
	crz[i], crz[j] = crz[j], crz[i]
}

func (pqm *PQM) MergedQueryWindow(ctx context.Context, id uuid.UUID, start int64, end int64,
	width uint64, parentSR chan qtree.StatRecord, parentCE chan bte.BTE) (chan qtree.StatRecord,
	chan bte.BTE, uint64, uint64) {
	maj, min, buf, err := pqm.MuxContents(ctx, id)
	if err != nil {
		panic(err)
	}
	if len(buf) == 0 {
		return parentSR, parentCE, maj, min
	}
	windows := CreateStatWindows(buf, start, start, end, width)
	rvsr, rvse := mergeStatisticalWindowChannels(parentSR, parentCE, windows)
	return rvsr, rvse, maj, min
}

func (pqm *PQM) MergeQueryStatisticalValuesStream(ctx context.Context, id uuid.UUID, start int64, end int64,
	pointwidth uint8, parentSR chan qtree.StatRecord, parentCE chan bte.BTE) (chan qtree.StatRecord,
	chan bte.BTE, uint64, uint64) {
	maj, min, buf, err := pqm.MuxContents(ctx, id)
	if err != nil {
		panic(err)
	}
	if len(buf) == 0 {
		return parentSR, parentCE, maj, min
	}
	windows := CreateStatWindows(buf, start, start & ^((1<<uint64(pointwidth))-1), end, 1<<pointwidth)
	rvsr, rvse := mergeStatisticalWindowChannels(parentSR, parentCE, windows)
	return rvsr, rvse, maj, min
}

func (pqm *PQM) MergeQueryValuesStream(ctx context.Context, id uuid.UUID, start int64, end int64,
	parentCR chan qtree.Record, parentCE chan bte.BTE) (chan qtree.Record, chan bte.BTE, uint64, uint64) {
	rv := make(chan qtree.Record, 1000)
	rve := make(chan bte.BTE, 10)
	maj, minor, contents, err := pqm.MuxContents(ctx, id)
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			select {
			case e := <-parentCE:
				rve <- e
				return
			case v, ok := <-parentCR:
				//If the parent is finished, emit all of the buffer
				if !ok {
					for _, cv := range contents {
						rv <- cv
					}
					close(rv)
					return
				}
				//Emit all records from teh buffer that are ahead of the parent
				for len(contents) > 0 && contents[0].Time < v.Time {
					rv <- contents[0]
					contents = contents[1:]
				}
				//Emit the parent
				rv <- v
			}
		}
	}()
	return rv, rve, maj, minor
}

//The global lock is held here
func (pqm *PQM) loadStreamEntry(ctx context.Context, arrid [16]byte) (*streamEntry, bte.BTE) {
	mv, err := pqm.si.StreamMajorVersion(ctx, uuid.UUID(arrid[:]))
	if err != nil {
		return nil, err
	}
	rv := streamEntry{
		majorVersion: mv,
		buffer:       make([]Record, 0, 1024),
	}
	rv.mu.Lock()
	pqm.streams[arrid] = &rv
	return &rv, nil
}

func (pqm *PQM) MuxContents(ctx context.Context, id uuid.UUID) (major, minor uint64, contents []Record, err bte.BTE) {
	arrid := idSliceToArr(id)

	pqm.globalMu.Lock()

	//Get the stream entry
	streamEntry, ok := pqm.streams[arrid]
	pqm.globalMu.Unlock()
	if !ok {
		maj, err := pqm.si.StreamMajorVersion(ctx, id)
		if err != nil {
			return 0, 0, nil, err
		}
		return maj, 0, nil, nil
	}
	streamEntry.mu.Lock()
	rv := make([]Record, len(streamEntry.buffer))
	copy(rv, streamEntry.buffer)
	rvmaj := streamEntry.majorVersion
	streamEntry.mu.Unlock()
	return rvmaj, uint64(len(rv)), rv, nil
}

func (pqm *PQM) Insert(ctx context.Context, id uuid.UUID, r []Record) (major, minor uint64, err bte.BTE) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "PQMInsert")
	defer span.Finish()

	arrid := idSliceToArr(id)
	//We want the superset of both active and proposed because
	//anyone processing the log will need to consider that whole range.
	//In reality we should have rejected everything not in the intersection
	//before here anyway.
	active, proposed := pqm.si.CP().OurRanges()
	okay, ourRange := active.Union(&proposed)
	if !okay {
		return 0, 0, bte.Err(bte.WrongEndpoint, "We live in tumultuous times")
	}

	//At this point, OurNotifiedRange is the range we are supposed to aspire to
	//so we need to
	//a) flush
	//b) check in the active mash for all nodes
	//c) get the journal entries for all those nodes
	//d) process all the journal entries that are VALID* and in the range
	//   valid means the journal entry version matches the stream
	// The caller does this check
	// ourRange := pqm.si.OurNotifiedRange(ctx)
	// if !ourRange.SuperSetOfUUID(id) {
	// 	return 0, 0, bte.Err(bte.WrongEndpoint, "we are not the server for that stream")
	// }

	//Get a PS handle

	//This is means to ensure that the path where all commits are going to primary
	//storage does not get overwhelmed. The path where things go via the journal
	//is taken care of by the journal's queues

	/*hnd, err := pqm.GetPSHandle(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer hnd.Done()*/

	pqm.globalMu.Lock()

	//Get the stream entry
	streamEntry, ok := pqm.streams[arrid]
	if !ok {
		var err bte.BTE
		//It comes back locked from this
		streamEntry, err = pqm.loadStreamEntry(ctx, arrid)
		pqm.globalMu.Unlock()
		if err != nil {
			return 0, 0, err
		}
	} else {
		pqm.globalMu.Unlock()
		streamEntry.mu.Lock()
	}
	doFullCommit := len(r)+len(streamEntry.buffer) >= MaxPQMBufferSize

	if !doFullCommit {
		tz := make([]int64, len(r))
		vz := make([]float64, len(r))
		for idx, v := range r {
			tz[idx] = v.Time
			vz[idx] = v.Val
		}
		//Now we have a handle, so we know we can write to primary storage if required
		//Insert into the journal
		jr := jprovider.JournalRecord{
			UUID:         id,
			MajorVersion: uint64(streamEntry.majorVersion),
			MicroVersion: uint32(len(streamEntry.buffer) + len(r)),
			Times:        tz,
			Values:       vz,
		}
		checkpoint, err := pqm.si.JP().Insert(ctx, ourRange, &jr)
		if err != nil {
			streamEntry.mu.Unlock()
			return 0, 0, err
		}
		//Record the time at which we opened this PQM buffer
		if len(streamEntry.checkpoints) == 0 {
			streamEntry.openTime = time.Now()
		}
		streamEntry.checkpoints = append(streamEntry.checkpoints, checkpoint)
		streamEntry.buffer = append(streamEntry.buffer, r...)
		streamEntry.mu.Unlock()
		err = pqm.si.JP().WaitForCheckpoint(ctx, checkpoint)
		if err != nil {
			return 0, 0, err
		}
		return streamEntry.majorVersion, uint64(len(streamEntry.buffer)), nil
	} //End do partial commit
	defer streamEntry.mu.Unlock()
	//we have to do a full commit
	//Don;t extend streamEntry buffer because we don't want duplicates
	//if we get a context error of some kind
	span3, ctx := opentracing.StartSpanFromContext(ctx, "WritePrimary")
	fullbuffer := make([]Record, len(streamEntry.buffer)+len(r))
	copy(fullbuffer[:len(streamEntry.buffer)], streamEntry.buffer)
	copy(fullbuffer[len(streamEntry.buffer):], r)
	majorv, err := pqm.si.WritePrimaryStorage(ctx, id, fullbuffer)
	if err != nil {
		return 0, 0, err
	}
	for _, cp := range streamEntry.checkpoints {
		err := pqm.si.JP().ReleaseDisjointCheckpoint(ctx, cp)
		if err != nil {
			return 0, 0, err
		}
	}
	streamEntry.checkpoints = []jprovider.Checkpoint{}
	streamEntry.buffer = streamEntry.buffer[:0]
	streamEntry.majorVersion = majorv
	span3.Finish()
	return majorv, 0, nil
}

func idSliceToArr(id []byte) [16]byte {
	var rv [16]byte
	copy(rv[:], id)
	return rv
}
