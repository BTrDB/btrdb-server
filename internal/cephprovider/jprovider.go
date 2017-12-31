package cephprovider

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/BTrDB/btrdb-server/internal/configprovider"
	"github.com/BTrDB/btrdb-server/internal/jprovider"
	"github.com/ceph/go-ceph/rados"
)

const CJournalProviderNamespace = "journalprovider"

const MaxObjectSize = 16 * 1024 * 1024

//The ceph provider needs to split the journal into relatively small objects (16MB)
//each object has a header containing
//Which range it covers
//Which ranges have been released
//Which checkpoint numbers it contains

//We also need some other metadata that we can use to learn
//what checkpoint number to start from
type CJournalProvider struct {
	sp    *CephStorageProvider
	ioctx *rados.IOContext
	//This is the NEXT checkpoint to be written
	cp       uint64
	nodename string

	currentObject           string
	currentObjectSize       uint64
	currentObjectCheckpoint uint64
	currentObjectRange      *configprovider.MashRange
	mu                      sync.Mutex
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

func (on *objName) String() string {
	return fmt.Sprintf("jo/%s/%016x", on.NodeName, on.StartingCheckpoint)
}

//Mutex must be held
func (jp *CJournalProvider) beginNewObject(rng *configprovider.MashRange) error {
	on := objName{NodeName: jp.nodename, StartingCheckpoint: jp.cp}
	ons := on.String()
	//The range that this object covers
	err := jp.ioctx.SetXattr(ons, "range", rng.Pack())
	if err != nil {
		return err
	}
	//The released ranges (sequences of 16 byte ranges)
	//Populate relrange with (0,0) as a released range
	err = jp.ioctx.SetXattr(ons, "relrange", make([]byte, 16))
	if err != nil {
		return err
	}
	jp.currentObject = ons
	jp.currentObjectSize = 0
	jp.currentObjectCheckpoint = jp.cp
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

func newJournalProvider(ournodename string, ioctx *rados.IOContext, sp *CephStorageProvider) (jprovider.JournalProvider, error) {
	ioctx.SetNamespace(CJournalProviderNamespace)
	data := make([]byte, 8)
	nread, err := ioctx.Read("node/"+ournodename, data, 0)
	if nread != 0 || err != rados.RadosErrorNotFound {
		return nil, fmt.Errorf("Node %s has existed before", ournodename)
	}
	err = ioctx.WriteFull("node/"+ournodename, data)
	if err != nil {
		return nil, err
	}
	rv := &CJournalProvider{
		sp:       sp,
		ioctx:    ioctx,
		nodename: ournodename,
		cp:       1,
	}
	return rv, nil
}
func (sp *CephStorageProvider) CreateJournalProvider(ournodename string) (jprovider.JournalProvider, error) {
	h, err := sp.conn.OpenIOContext(sp.hotPool)
	if err != nil {
		return nil, err
	}
	return newJournalProvider(ournodename, h, sp)
}

func (jp *CJournalProvider) Insert(ctx context.Context, rng *configprovider.MashRange, jr *jprovider.JournalRecord) (checkpoint jprovider.Checkpoint, err error) {
	jp.mu.Lock()
	defer jp.mu.Unlock()
	if jr == nil {
		return 0, fmt.Errorf("cannot use a nil journal record")
	}
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if rng == nil {
		return 0, fmt.Errorf("cannot use a nil range")
	}
	cjr := &CJrecord{
		R: jr,
		C: jp.cp,
	}
	data, err := cjr.MarshalMsg(nil)
	if err != nil {
		panic(err)
	}

	//If we have an existing journal object and it is not too big, write to the end of it
	//otherwise, create a new journal object
	//The range will not be equal if the current object does not exist
	needNew := !rng.Equal(jp.currentObjectRange) ||
		jp.currentObjectSize+uint64(len(data)) >= MaxObjectSize

	if needNew {
		err := jp.beginNewObject(rng)
		if err != nil {
			return 0, err
		}
	}
	err = jp.ioctx.Write(jp.currentObject, data, jp.currentObjectSize)
	if err != nil {
		panic(err)
	}
	jp.currentObjectSize += uint64(len(data))
	jp.cp += 1
	return jprovider.Checkpoint(cjr.C), nil
}
func (jp *CJournalProvider) WaitForCheckpoint(ctx context.Context, checkpoint jprovider.Checkpoint) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	//In the current implementation checkpoints are written immediately
	return nil
}

//Used by a node taking control of a range
func (jp *CJournalProvider) ObtainNodeJournals(ctx context.Context, nodename string) (jprovider.JournalIterator, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	//We need to consume the whole iterator now and sort it
	//so we consume the checkpoints in order
	iter, err := jp.ioctx.Iter()
	if err != nil {
		return nil, err
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
	sort.Strings(objectlist)
	return &jiterator{
		jp:         jp,
		nn:         nodename,
		objectlist: objectlist,
	}, nil
}

func (it *jiterator) loadrecordbuffer(obj string) {
	data := make([]byte, MaxObjectSize)
	nread, err := it.jp.ioctx.Read(obj, data, 0)
	if err != nil {
		panic(err)
	}
	it.buffer = data[:nread]
}

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
func (it *jiterator) Value() (*jprovider.JournalRecord, jprovider.Checkpoint, error) {
	if it.value == nil || it.value.R == nil {
		return nil, 0, fmt.Errorf("No value")
	}
	return it.value.R, jprovider.Checkpoint(it.value.C), nil
}

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

func (jp *CJournalProvider) markOrDeleteReleasedRange(objname string, rng *configprovider.MashRange) error {
	buffer := make([]byte, 16*MaxDistinctRanges)
	nread, err := jp.ioctx.GetXattr(objname, "relrange", buffer)
	if err != nil {
		return err
	}
	fullrangeb := make([]byte, 20)
	nread2, err := jp.ioctx.GetXattr(objname, "range", fullrangeb)
	if err != nil {
		return err
	}
	if nread2 != 16 {
		return fmt.Errorf("wrong number of read bytes %d", nread)
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
					//The only appropriate answer to an O(n^3) code snippet
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
		return jp.ioctx.Delete(objname)
	} else {
		//write out the new ranges
		newserial := make([]byte, len(ranges)*16)
		idx := 0
		for _, r := range ranges {
			packed := r.Pack()
			copy(newserial[idx*16:(idx+1)*16], packed)
			idx++
		}
		err := jp.ioctx.SetXattr(objname, "relrange", newserial)
		return err
	}
}

//Used by both the recovering nodes and the generating nodes
//Given that the same journal can be processed by two different nodes
//across different ranges, it is important that the provider only frees resources
//associated with old checkpoints if they have been released across the entire range
//of the journal. The checkpoint is INCLUSIVE.
func (jp *CJournalProvider) ReleaseJournalEntries(ctx context.Context, nodename string, upto jprovider.Checkpoint, rng *configprovider.MashRange) error {
	//TODO if we use a channel for locking we can acquire with context deadline case
	jp.mu.Lock()
	defer jp.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	iter, err := jp.ioctx.Iter()
	if err != nil {
		return err
	}
	for iter.Next() {
		name := iter.Value()
		objname := ParseObjectName(name)
		if objname == nil {
			continue
		}
		if objname.NodeName == nodename {
			err := jp.markOrDeleteReleasedRange(objname.String(), rng)
			if err != nil {
				return err
			}
		}
	}
	err = iter.Err()
	if err != nil {
		return err
	}
	iter.Close()
	return nil
}

func (jp *CJournalProvider) ForgetAboutNode(ctx context.Context, nodename string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	iter, err := jp.ioctx.Iter()
	if err != nil {
		return err
	}
	for iter.Next() {
		name := iter.Value()
		objname := ParseObjectName(name)
		if objname == nil {
			continue
		}
		if objname.NodeName == nodename {
			err := jp.ioctx.Delete(name)
			if err != nil {
				return err
			}
		}
	}
	err = iter.Err()
	if err != nil {
		return err
	}
	iter.Close()
	return jp.ioctx.Delete("node/" + nodename)
}
