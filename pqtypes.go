package btrdb

import (
	"context"
	"sort"
	"sync"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/configprovider"
	"github.com/BTrDB/btrdb-server/internal/jprovider"
	"github.com/BTrDB/btrdb-server/qtree"
	"github.com/pborman/uuid"
)

type Record = qtree.Record

type StorageInterface interface {
	JP() jprovider.JournalProvider

	OurNotifiedRange(ctx context.Context) configprovider.MashRange

	//Unthrottled
	WritePrimaryStorage(ctx context.Context, id uuid.UUID, r []Record) (major uint64, err bte.BTE)
	//Appropriate locks will be held
	StreamMajorVersion(ctx context.Context, id uuid.UUID) (uint64, bte.BTE)
}

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
}
type psHandle struct {
	pqm *PQM
}

func (psh *psHandle) Done() {
	psh.pqm.hackmu.Unlock()
}

func NewPQM(si StorageInterface) *PQM {
	return &PQM{
		si: si,
	}
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
	/*
		go func(){
			for _ := range parentRec {

			}
		}()
	*/
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
	panic("ni")
}

func (pqm *PQM) MergeQueryStatisticalValuesStream(ctx context.Context, id uuid.UUID, start int64, end int64,
	pointwidth uint8, parentSR chan qtree.StatRecord, parentCE chan bte.BTE) (chan qtree.StatRecord,
	chan bte.BTE, uint64) {
	panic("ni")
}

func (pqm *PQM) MergeQueryValuesStream(ctx context.Context, id uuid.UUID, start int64, end int64,
	parentCR chan qtree.Record, parentCE chan bte.BTE) (chan qtree.Record, chan bte.BTE, uint64, uint64) {
	panic("ni")
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
	arrid := idSliceToArr(id)

	ourRange := pqm.si.OurNotifiedRange(ctx)
	if !ourRange.SuperSetOfUUID(id) {
		return 0, 0, bte.Err(bte.WrongEndpoint, "we are not the server for that stream")
	}

	//Get a PS handle
	hnd, err := pqm.GetPSHandle(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer hnd.Done()

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

	doFullCommit := false
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
		checkpoint, err := pqm.si.JP().Insert(ctx, &ourRange, &jr)
		if err != nil {
			return 0, 0, err
		}
		streamEntry.buffer = append(streamEntry.buffer, r...)
		err = pqm.si.JP().WaitForCheckpoint(ctx, checkpoint)
		if err != nil {
			return 0, 0, err
		}
		return streamEntry.majorVersion, uint64(len(streamEntry.buffer)), nil
	} //End do partial commit

	//we have to do a full commit
	//Don;t extend streamEntry buffer because we don't want duplicates
	//if we get a context error of some kind
	fullbuffer := make([]Record, len(streamEntry.buffer)+len(r))
	copy(fullbuffer[:len(streamEntry.buffer)], streamEntry.buffer)
	copy(fullbuffer[len(streamEntry.buffer):], r)
	majorv, err := pqm.si.WritePrimaryStorage(ctx, id, fullbuffer)
	if err != nil {
		return 0, 0, err
	}
	streamEntry.buffer = streamEntry.buffer[:0]
	streamEntry.majorVersion = majorv
	return majorv, 0, nil
}

func idSliceToArr(id []byte) [16]byte {
	var rv [16]byte
	copy(rv[:], id)
	return rv
}
