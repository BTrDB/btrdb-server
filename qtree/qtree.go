package qtree

import (
	"errors"
	"fmt"
	"github.com/SoftwareDefinedBuildings/quasar/internal/bstore"
	"github.com/op/go-logging"
	"math"
	"sort"
)

var log *logging.Logger

func init() {
	log = logging.MustGetLogger("log")
}

//but we still have support for dates < 1970

var ErrNoSuchStream = errors.New("No such stream")
var ErrNotLeafNode = errors.New("Not a leaf node")
var ErrImmutableTree = errors.New("Tree is immutable")
var ErrIdxNotFound = errors.New("Index not found")
var ErrNoSuchPoint = errors.New("No such point")
var ErrBadInsert = errors.New("Bad insert")
var ErrBadDelete = errors.New("Bad delete")

//It is important to note that if backwards is true, then time is exclusive. So if
//a record exists with t=80 and t=100, and you query with t=100, backwards=true, you will get the t=80
//record. For forwards, time is inclusive.
func (n *QTreeNode) FindNearestValue(time int64, backwards bool) (Record, error) {
	if n.isLeaf {

		if n.vector_block.Len == 0 {
			log.Panicf("Not expecting this")
		}
		idx := -1
		for i := 0; i < int(n.vector_block.Len); i++ {
			if n.vector_block.Time[i] >= time {
				if !backwards {
					idx = i
				}
				break
			}
			if backwards {
				idx = i
			}
		}
		if idx == -1 {
			//If backwards that means first point is >
			//If forwards that means last point is <
			return Record{}, ErrNoSuchPoint
		}
		return Record{
			Time: n.vector_block.Time[idx],
			Val:  n.vector_block.Value[idx],
		}, nil
	} else {
		//We need to find which child with nonzero count is the best to satisfy the claim.
		idx := -1
		for i := 0; i < KFACTOR; i++ {
			if n.core_block.Count[i] == 0 {
				continue
			}
			if n.ChildStartTime(uint16(i)) >= time {
				if !backwards {
					idx = i
				}
				break
			}
			if backwards {
				idx = i
			}
		}
		if idx == -1 {
			return Record{}, ErrNoSuchPoint
		}
		//for backwards, idx points to the containing window
		//for forwards, idx points to the window after the containing window

		val, err := n.Child(uint16(idx)).FindNearestValue(time, backwards)

		//For both, we also need the window before this point
		if idx != 0 && n.core_block.Count[idx-1] != 0 { //The block containing the time is not empty

			//So if we are going forward, we need to do two queries, the window that CONTAINS the time, and the window
			//that FOLLOWS the time, because its possible for all the data points in the CONTAINS window to fall before
			//the time. For backwards we have the same thing but VAL above is the CONTAINS window, and we need to check
			//the BEFORE window
			other, oerr := n.Child(uint16(idx-1)).FindNearestValue(time, backwards)
			if oerr == ErrNoSuchPoint {
				//Oh well the standard window is the only option
				return val, err
			}

			if backwards {
				//The val is best
				if err == nil {
					return val, nil
				} else {
					return other, oerr
				}
			} else { //Other is best
				if oerr == nil {
					return other, nil
				} else {
					return val, err
				}
			}
		}

		return val, err
	}
}

//Holy shit these take a long time
func (n *QTree) GetAllReferencedVAddrs() chan uint64 {
	rchan := make(chan uint64, 1024)
	go func() {
		n.root.GetAllReferencedVAddrs(rchan)
		close(rchan)
	}()
	return rchan
}
func (n *QTreeNode) GetAllReferencedVAddrs(rchan chan uint64) {
	rchan <- n.ThisAddr()
	if !n.isLeaf {
		for c := uint16(0); c < KFACTOR; c++ {
			if ch := n.Child(c); ch != nil {
				ch.GetAllReferencedVAddrs(rchan)
			}
		}
	}
}

func (tr *QTree) FindChangedSinceSlice(gen uint64, threshold uint64) []ChangedRange {
	if tr.root == nil {
		return make([]ChangedRange, 0)
	}
	rv := make([]ChangedRange, 0, 1024)
	rch := tr.FindChangedSince(gen, threshold)
	var lr ChangedRange = ChangedRange{}
	for {

		select {
		case cr, ok := <-rch:
			if !ok {
				//This is the end.
				//Do we have an unsaved LR?
				if lr.Valid {
					rv = append(rv, lr)
				}
				log.Debug("Returning from FCSS")
				return rv
			}
			log.Debug("Got on channel")
			if !cr.Valid {
				log.Panicf("Didn't think this could happen")
			}
			//Coalesce
			if lr.Valid && cr.Start == lr.End {
				log.Debug("Coalescing")
				lr.End = cr.End
			} else {
				if lr.Valid {
					rv = append(rv, lr)
				}
				lr = cr
			}
		}
	}
	log.Debug("Returning from FCSS")
	return rv
}
func (tr *QTree) FindChangedSince(gen uint64, threshold uint64) chan ChangedRange {
	rv := make(chan ChangedRange, 1024)
	go func() {
		if tr.root == nil {
			close(rv)
			return
		}
		cr := tr.root.FindChangedSince(gen, rv, threshold, false)
		log.Debug("Returned from FCS")
		if cr.Valid {
			rv <- cr
		}
		close(rv)
	}()
	return rv
}

func (n *QTreeNode) DeleteRange(start int64, end int64) *QTreeNode {
	if n.isLeaf {
		widx, ridx := 0, 0
		//First check if this operation deletes all the entries or only some
		if n.vector_block.Len == 0 {
			log.Panicf("This shouldn't happen")
		}
		if start <= n.vector_block.Time[0] && end > n.vector_block.Time[n.vector_block.Len-1] {
			return nil
		}
		//Otherwise we need to copy the parts that still exist
		log.Debug("Calling uppatch loc1")
		newn, err := n.AssertNewUpPatch()
		if err != nil {
			log.Panicf("Could not up patch: %v", err)
		}
		n = newn
		for ridx < int(n.vector_block.Len) {
			//if n.vector_block.
			if n.vector_block.Time[ridx] < start || n.vector_block.Time[ridx] >= end {
				n.vector_block.Time[widx] = n.vector_block.Time[ridx]
				n.vector_block.Value[widx] = n.vector_block.Value[ridx]
				widx++
			}
			ridx++
		}
		n.vector_block.Len = uint16(widx)
		return n
	} else {
		if start <= n.StartTime() && end > n.EndTime() {
			//This node is being deleted in its entirety. As we are no longer using the dereferences, we can
			//prune the whole branch up here. Note that this _does_ leak references for all the children, but
			//we are no longer using them
			return nil
		}
		
		//We have at least one reading somewhere in here not being deleted
		sb := n.ClampBucket(start)
		eb := n.ClampBucket(end)
		
		//Check if there are nodes fully outside the range being deleted
		othernodes := false
		for i := uint16(0); i < sb; i++ {
			if n.core_block.Addr[i] != 0 {
				othernodes = true
				break
			}
		}
		for i := eb + 1; i < KFACTOR; i++ {
			if n.core_block.Addr[i] != 0 {
				othernodes = true
				break
			}
		}
		
		//Replace our children
		newchildren := make([]*QTreeNode, 64)
		nonnull := false
		for i := sb; i <= eb; i++ {
			ch := n.Child(i)
			if ch != nil {
				newchildren[i] = ch.DeleteRange(start, end)
				if newchildren[i] != nil {
					nonnull = true
					//The child might have done the uppatch
					if newchildren[i].Parent() != n {
						n = newchildren[i].Parent()
					}
				}
			}
		}
		
		if !nonnull && !othernodes {
			return nil
		} else {
			//This node is not completely empty
			newn, err := n.AssertNewUpPatch()
			if err != nil {
				log.Panicf("Could not up patch: %v", err)
			}
			n = newn
			//nil children unfref'd themselves, so we should be ok with just marking them as nil
			for i := sb; i <= eb; i++ {
				n.SetChild(i, newchildren[i])
			}
			return n
		}
	}
}

//TODO: consider deletes. I think that it will require checking if the generation of a core node is higher than all it's non-nil
//children. This implies that one or more children got deleted entirely, and then the node must report its entire time range as changed.
//it's not possible for a child to have an equal generation to the parent AND another node got deleted, as deletes and inserts do not
//get batched together. Also, if we know that a generation always corresponds to a contiguous range deletion (i.e we don't coalesce
//deletes) then we know that we couldn't have got a partial delete that bumped up a gen and masked another full delete.
func (n *QTreeNode) FindChangedSince(gen uint64, rchan chan ChangedRange, threshold uint64, youAreAboveT bool) ChangedRange {
	if n.isLeaf {
		if n.vector_block.Generation <= gen {
			return ChangedRange{} //Not valid
		}
		//log.Debug("Returning a leaf changed range")
		return ChangedRange{true, n.StartTime(), n.EndTime()}
	} else {
		//log.Debug("Entering FCS in core %v",n.TreePath())
		if n.core_block.Generation < gen {
			log.Debug("Exit 1: %v / %v", n.core_block.Generation, gen)
			return ChangedRange{} //Not valid
		}
		if youAreAboveT {
			log.Debug("Exit 2")
			return ChangedRange{true, n.StartTime(), n.EndTime()}
		}
		cr := ChangedRange{}
		maxchild := uint64(0)
		for k := 0; k < KFACTOR; k++ {
			if n.core_block.CGeneration[k] > maxchild {
				maxchild = n.core_block.CGeneration[k]
			}
		}
		if maxchild > n.Generation() {
			log.Panicf("Children are older than parent (this is bad) here: %s", n.TreePath())
		}

		for k := 0; k < KFACTOR; k++ {
			ch := n.Child(uint16(k))
			if ch == nil {
				log.Debug("Nil child(%v), gen %v", k, n.core_block.CGeneration[k])
				if n.core_block.CGeneration[k] >= gen {
					log.Debug("Found a new nil block cg")
					//A whole child was deleted here
					cstart := n.ChildStartTime(uint16(k))
					cend := n.ChildEndTime(uint16(k))
					if cr.Valid {
						if cstart == cr.End+1 {
							cr.End = cend
						} else {
							rchan <- cr
							cr = ChangedRange{End: cend, Start: cstart, Valid: true}
						}
					} else {
						cr = ChangedRange{End: cend, Start: cstart, Valid: true}
					}
				}
			} else {
				if ch.Generation() != n.core_block.CGeneration[k] {
					log.Panicf("Mismatch on generation hint")
				}
				cabove := threshold != 0 && ch.core_block.Count[k] <= threshold
				rcr := n.Child(uint16(k)).FindChangedSince(gen, rchan, threshold, cabove)
				if rcr.Valid {
					//log.Debug("Got valid range from child %+v", rcr)
					if cr.Valid {
						if rcr.Start == cr.End {
							//If the changed range is connected, just extend what we have
							//log.Debug("Extending what we have")
							cr.End = rcr.End
						} else {
							//Send out the prev. changed range
							log.Debug("Sending a range")
							rchan <- cr
							cr = rcr
						}
					} else {
						//log.Debug("Assigning to CR")
						cr = rcr
					}
				}
			}
		}
		//Note that we don't get 100% coalescence. The receiver on rchan should also check for coalescence.
		//we just do a bit to reduce traffic on the channel. One case is if we have two disjoint ranges in a
		//core, and the first is at the start. We send it on rchan even if it might be adjacent to the prev
		//sibling
		return cr //Which might be invalid if we got none from children (all islanded)
	}
	return ChangedRange{}
}

func (n *QTreeNode) Child(i uint16) *QTreeNode {
	//log.Debug("Child %v called on %v",i, n.TreePath())
	if n.isLeaf {
		log.Panicf("Child of leaf?")
	}
	if n.core_block.Addr[i] == 0 {
		return nil
	}
	if n.child_cache[i] != nil {
		return n.child_cache[i]
	}

	child, err := n.tr.LoadNode(n.core_block.Addr[i],
		n.core_block.CGeneration[i], n.ChildPW(), n.ChildStartTime(i))
	if err != nil {
		log.Debug("We are at %v", n.TreePath())
		log.Debug("We were trying to load child %v", i)
		log.Debug("With address %v", n.core_block.Addr[i])
		log.Panicf("%v", err)
	}
	child.parent = n
	n.child_cache[i] = child
	return child
}

//Like Child() but creates the node if it doesn't exist
func (n *QTreeNode) wchild(i uint16, isVector bool) *QTreeNode {
	if n.isLeaf {
		log.Panicf("Child of leaf?")
	}
	if n.tr.gen == nil {
		log.Panicf("Cannot use WChild on read only tree")
	}
	if n.PointWidth() == 0 {
		log.Panicf("Already at the bottom of the tree!")
	} else {
		//	log.Debug("ok %d", n.PointWidth())
	}
	if n.core_block.Addr[i] == 0 {
		//log.Debug("no existing child. spawning pw(%v)[%v] vector=%v", n.PointWidth(),i,isVector)
		var newn *QTreeNode
		var err error
		//log.Debug("child window is s=%v",n.ChildStartTime(i))
		if isVector {
			newn, err = n.tr.NewVectorNode(n.ChildStartTime(i), n.ChildPW())
		} else {
			newn, err = n.tr.NewCoreNode(n.ChildStartTime(i), n.ChildPW())
		}
		if err != nil {
			log.Panicf("%v", err)
		}
		newn.parent = n
		n.child_cache[i] = newn
		n.core_block.Addr[i] = newn.ThisAddr()
		return newn
	}
	if n.child_cache[i] != nil {
		return n.child_cache[i]
	}
	child, err := n.tr.LoadNode(n.core_block.Addr[i],
		n.core_block.CGeneration[i], n.ChildPW(), n.ChildStartTime(i))
	if err != nil {
		log.Panicf("%v", err)
	}
	child.parent = n
	n.child_cache[i] = child
	return child
}

//This function assumes that n is already new
func (n *QTreeNode) SetChild(idx uint16, c *QTreeNode) {
	if n.tr.gen == nil {
		log.Panicf("umm")
	}
	if n.isLeaf {
		log.Panicf("umm")
	}
	if !n.isNew {
		log.Panicf("uhuh lol?")
	}
	
	n.child_cache[idx] = c
	n.core_block.CGeneration[idx] = n.tr.Generation()
	if c == nil {
		n.core_block.Addr[idx] = 0
		n.core_block.Min[idx] = 0
		n.core_block.Max[idx] = 0
		n.core_block.Count[idx] = 0
		n.core_block.Mean[idx] = 0
	} else {
		c.parent = n
		if c.isLeaf {
			n.core_block.Addr[idx] = c.vector_block.Identifier
		} else {
			n.core_block.Addr[idx] = c.core_block.Identifier
		}
		//Note that a bunch of updates of the metrics inside the block need to
		//go here
		n.core_block.Min[idx] = c.OpMin()
		n.core_block.Max[idx] = c.OpMax()
		n.core_block.Count[idx], n.core_block.Mean[idx] = c.OpCountMean()
	}
}

//Here is where we would replace with fancy delta compression
func (n *QTreeNode) MergeIntoVector(r []Record) {
	if !n.isNew {
		log.Panicf("bro... cmon")
	}
	//There is a special case: this can be called to insert into an empty leaf
	//don't bother being smart then
	if n.vector_block.Len == 0 {
		for i := 0; i < len(r); i++ {
			n.vector_block.Time[i] = r[i].Time
			n.vector_block.Value[i] = r[i].Val
		}
		n.vector_block.Len = uint16(len(r))
		return
	}
	curtimes := n.vector_block.Time
	curvals := n.vector_block.Value
	iDst := 0
	iVec := 0
	iRec := 0
	if len(r) == 0 {
		panic("zero record insert")
	}
	if n.vector_block.Len == 0 {
		panic("zero sized leaf")
	}
	for {
		if iRec == len(r) {
			//Dump vector
			for iVec < int(n.vector_block.Len) {
				n.vector_block.Time[iDst] = curtimes[iVec]
				n.vector_block.Value[iDst] = curvals[iVec]
				iDst++
				iVec++
			}
			break
		}
		if iVec == int(n.vector_block.Len) {
			//Dump records
			for iRec < len(r) {
				n.vector_block.Time[iDst] = r[iRec].Time
				n.vector_block.Value[iDst] = r[iRec].Val
				iDst++
				iRec++
			}
			break
		}
		if r[iRec].Time < curtimes[iVec] {
			n.vector_block.Time[iDst] = r[iRec].Time
			n.vector_block.Value[iDst] = r[iRec].Val
			iRec++
			iDst++
		} else {
			n.vector_block.Time[iDst] = curtimes[iVec]
			n.vector_block.Value[iDst] = curvals[iVec]
			iVec++
			iDst++
		}
	}
	n.vector_block.Len += uint16(len(r))
}
func (n *QTreeNode) AssertNewUpPatch() (*QTreeNode, error) {
	if n.isNew {
		//We assume that all parents are already ok
		return n, nil
	}

	//Ok we need to clone
	newn, err := n.clone()
	if err != nil {
		log.Panicf("%v", err)
	}

	//Does our parent need to also uppatch?
	if n.Parent() == nil {
		//We don't have a parent. We better be root
		if n.PointWidth() != ROOTPW {
			log.Panicf("WTF")
		}
	} else {
		npar, err := n.Parent().AssertNewUpPatch()
		if err != nil {
			log.Panicf("sigh")
		}
		//The parent might have changed. Update it
		newn.parent = npar
		//Get the IDX from the old parent
		//TODO(mpa) this operation is actually really slow. Change how we do it
		idx, err := n.FindParentIndex()
		if err != nil {
			log.Panicf("Could not find parent idx")
		}
		//Downlink
		newn.Parent().SetChild(idx, newn)
	}
	return newn, nil
}

//We need to create a core node, insert all the vector data into it,
//and patch up the parent
func (n *QTreeNode) ConvertToCore(newvals []Record) *QTreeNode {
	//log.Critical("CTC call")
	newn, err := n.tr.NewCoreNode(n.StartTime(), n.PointWidth())
	if err != nil {
		log.Panicf("%v", err)
	}
	n.parent.AssertNewUpPatch()
	newn.parent = n.parent
	idx, err := n.FindParentIndex()
	newn.Parent().SetChild(idx, newn)
	valset := make([]Record, int(n.vector_block.Len)+len(newvals))
	for i := 0; i < int(n.vector_block.Len); i++ {
		valset[i] = Record{n.vector_block.Time[i],
			n.vector_block.Value[i]}

	}
	base := n.vector_block.Len
	for i := 0; i < len(newvals); i++ {
		valset[base] = newvals[i]
		base++
	}
	sort.Sort(RecordSlice(valset))
	newn.InsertValues(valset)

	return newn
}

/**
 * This function is for inserting a large chunk of data. It is required
 * that the data is sorted, so we do that here
 */
func (tr *QTree) InsertValues(records []Record) (e error) {
	if tr.gen == nil {
		return ErrBadInsert
	}
	proc_records := make([]Record, len(records))
	idx := 0
	for _, v := range records {
		if math.IsInf(v.Val, 0) || math.IsNaN(v.Val) {
			log.Debug("WARNING Got Inf/NaN insert value, dropping")
		} else if v.Time <= MinimumTime || v.Time >= MaximumTime {
			log.Debug("WARNING Got time out of range, dropping")
		} else {
			proc_records[idx] = v
			idx++
		}
	}
	proc_records = proc_records[:idx]
	if len(proc_records) == 0 {
		return ErrBadInsert
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered insertvalues panic", r)
			e = ErrBadInsert
		}
	}()
	sort.Sort(RecordSlice(proc_records))
	n, err := tr.root.InsertValues(proc_records)
	if err != nil {
		log.Panicf("Root insert values: %v", err)
	}

	tr.root = n
	tr.gen.UpdateRootAddr(n.ThisAddr())
	return nil
}

func (tr *QTree) DeleteRange(start int64, end int64) error {
	if tr.gen == nil {
		return ErrBadDelete
	}
	n := tr.root.DeleteRange(start, end)
	tr.root = n
	if n == nil {
		tr.gen.UpdateRootAddr(0)
	} else {
		tr.gen.UpdateRootAddr(n.ThisAddr())
	}
	return nil
}

/**
 * the process is:
 * call insertvalues - returns new QTreeNode.
 *   this must have: address, stats
 *   and it must have put whatever it touched in the generation
 * replace it in the child cache, change address + stats
 *   and return to parent
 */
func (n *QTreeNode) InsertValues(records []Record) (*QTreeNode, error) {
	//log.Debug("InsertValues called on pw(%v) with %v records @%08x",
	//	n.PointWidth(), len(records), n.ThisAddr())
	//log.Debug("IV ADDR: %s", n.TreePath())
	////First determine if any of the records are outside our window
	//This is debugging, it won't even work if the records aren't sorted
	if !n.isLeaf {
		if records[0].Time < n.StartTime() {
			//Actually I don't think we can be less than the start.
			log.Panicf("Bad window <")
		}
		if records[len(records)-1].Time >= n.StartTime()+((1<<n.PointWidth())*bstore.KFACTOR) {
			log.Debug("FE.")
			log.Debug("Node window s=%v e=%v", n.StartTime(),
				n.StartTime()+((1<<n.PointWidth())*bstore.KFACTOR))
			log.Debug("record time: %v", records[len(records)-1].Time)
			log.Panicf("Bad window >=")
		}
	}
	if n.isLeaf {
		//log.Debug("insertin values in leaf")
		if int(n.vector_block.Len)+len(records) > bstore.VSIZE && n.PointWidth() != 0{
			//log.Debug("need to convert leaf to a core");
			//log.Debug("because %v + %v",n.vector_block.Len, len(records))
			//log.Debug("Converting pw %v to core", n.PointWidth())
			n = n.ConvertToCore(records)
			return n, nil
		} else {
			if n.PointWidth() == 0 && int(n.vector_block.Len)+len(records) > bstore.VSIZE {
				truncidx := bstore.VSIZE - int(n.vector_block.Len)
				if truncidx == 0 {
					log.Critical("Truncating - full!!")
					return n, nil
				}
				log.Critical("Truncating insert due to PW0 overflow!!")
				records = records[:truncidx]
			}
			//log.Debug("inserting %d records into pw(%v) vector", len(records),n.PointWidth())
			newn, err := n.AssertNewUpPatch()
			if err != nil {
				log.Panicf("Uppatch failed: ", err)
			}
			n = newn
			n.MergeIntoVector(records)
			return n, nil
		}
	} else {
		//log.Debug("inserting valus in core")
		//We are a core node
		newn, err := n.AssertNewUpPatch()
		if err != nil {
			log.Panicf("ANUP: %v", err)
		}
		n := newn
		lidx := 0
		lbuckt := n.ClampBucket(records[0].Time)
		for idx := 1; idx < len(records); idx++ {
			r := records[idx]
			//log.Debug("iter: %v, %v", idx, r)
			buckt := n.ClampBucket(r.Time)
			if buckt != lbuckt {
				//log.Debug("records spanning bucket. flushing to child %v", lbuckt)
				//Next bucket has started
				childisleaf := idx-lidx < bstore.VSIZE
				if n.ChildPW() == 0 {
					childisleaf = true
				}
				newchild, err := n.wchild(lbuckt, childisleaf).InsertValues(records[lidx:idx])
				if err != nil {
					log.Panicf("%v", err)
				}
				n.SetChild(lbuckt, newchild) //This should set parent link too
				lidx = idx
				lbuckt = buckt
			}
		}
		//log.Debug("reched end of records. flushing to child %v", buckt)
		newchild, err := n.wchild(lbuckt, (len(records)-lidx) < bstore.VSIZE).InsertValues(records[lidx:])
		//log.Debug("Address of new child was %08x", newchild.ThisAddr())
		if err != nil {
			log.Panicf("%v", err)
		}
		n.SetChild(lbuckt, newchild)

		return n, nil
	}
}

var ErrBadTimeRange error = errors.New("Invalid time range")

//start is inclusive, end is exclusive. To query a specific nanosecond, query (n, n+1)
func (tr *QTree) ReadStandardValuesCI(rv chan Record, err chan error, start int64, end int64) {
	if tr.root != nil {
		tr.root.ReadStandardValuesCI(rv, err, start, end)
	}
	close(rv)
	close(err)
}

func (tr *QTree) ReadStandardValuesBlock(start int64, end int64) ([]Record, error) {
	rv := make([]Record, 0, 256)
	recordc := make(chan Record)
	errc := make(chan error)
	var err error
	busy := true
	go tr.ReadStandardValuesCI(recordc, errc, start, end)
	for busy {
		select {
		case e, _ := <-errc:
			if e != nil {
				err = e
				busy = false
			}
		case r, r_ok := <-recordc:
			if !r_ok {
				busy = false
			} else {
				rv = append(rv, r)
			}
		}
	}
	return rv, err
}

type StatRecord struct {
	Time  int64 //This is at the start of the record
	Count uint64
	Min   float64
	Mean  float64
	Max   float64
}

func (tr *QTree) QueryStatisticalValues(rv chan StatRecord, err chan error,
	start int64, end int64, pw uint8) {
	//Remember end is inclusive for QSV
	if tr.root != nil {
		tr.root.QueryStatisticalValues(rv, err, start, end, pw)
	}
	close(rv)
	close(err)
}

func (tr *QTree) QueryStatisticalValuesBlock(start int64, end int64, pw uint8) ([]StatRecord, error) {
	rv := make([]StatRecord, 0, 256)
	recordc := make(chan StatRecord)
	errc := make(chan error)
	var err error
	busy := true
	go tr.QueryStatisticalValues(recordc, errc, start, end, pw)
	for busy {
		select {
		case e, _ := <-errc:
			if e != nil {
				err = e
				busy = false
			}
		case r, r_ok := <-recordc:
			if !r_ok {
				busy = false
			} else {
				rv = append(rv, r)
			}
		}
	}
	return rv, err
}

func (n *QTreeNode) QueryStatisticalValues(rv chan StatRecord, err chan error,
	start int64, end int64, pw uint8) {
	if n.isLeaf {

		for idx := 0; idx < int(n.vector_block.Len); idx++ {
			if n.vector_block.Time[idx] < start {
				continue
			}
			if n.vector_block.Time[idx] >= end {
				break
			}
			b := n.ClampVBucket(n.vector_block.Time[idx], pw)
			count, min, mean, max := n.OpReduce(pw, uint64(b))
			if count != 0 {
				rv <- StatRecord{Time: n.ArbitraryStartTime(b, pw),
					Count: count,
					Min:   min,
					Mean:  mean,
					Max:   max,
				}
				//Skip over records in the vector that the PW included
				idx += int(count - 1)
			}

		}
		/*
			sb := n.ClampVBucket(start, pw)
			eb := n.ClampVBucket(end, pw)
			for b := sb; b <= eb; b++ {
				count, min, mean, max := n.OpReduce(pw, uint64(b))
				if count != 0 {
					rv <- StatRecord{Time: n.ArbitraryStartTime(b, pw),
						Count: count,
						Min:   min,
						Mean:  mean,
						Max:   max,
					}
				}
			}*/
	} else {
		//Ok we are at the correct level and we are a core
		sb := n.ClampBucket(start) //TODO check this function handles out of range
		eb := n.ClampBucket(end)
		recurse := pw <= n.PointWidth()
		if recurse {
			for b := sb; b <= eb; b++ {
				c := n.Child(b)
				if c != nil {
					c.QueryStatisticalValues(rv, err, start, end, pw)
					c.Free()
					n.child_cache[b] = nil
				}
			}
		} else {
			pwdelta := pw - n.PointWidth()
			sidx := sb >> pwdelta
			eidx := eb >> pwdelta
			for b := sidx; b <= eidx; b++ {
				count, min, mean, max := n.OpReduce(pw, uint64(b))
				if count != 0 {
					rv <- StatRecord{Time: n.ChildStartTime(b << pwdelta),
						Count: count,
						Min:   min,
						Mean:  mean,
						Max:   max,
					}
				}
			}
		}

	}
}

//Although we keep caches of datablocks in the bstore, we can't actually free them until
//they are unreferenced. This dropcache actually just makes sure they are unreferenced
func (n *QTreeNode) Free() {
	if n.isLeaf {
		n.tr.bs.FreeVectorblock(&n.vector_block)
	} else {
		for i, c := range n.child_cache {
			if c != nil {
				c.Free()
				n.child_cache[i] = nil
			}
		}
		n.tr.bs.FreeCoreblock(&n.core_block)
	}

}

func (n *QTreeNode) ReadStandardValuesCI(rv chan Record, err chan error,
	start int64, end int64) {
	if end <= start {
		err <- ErrBadTimeRange
		return
	}
	if n.isLeaf {
		//log.Debug("rsvci = leaf len(%v)", n.vector_block.Len)
		//Currently going under assumption that buckets are sorted
		//TODO replace with binary searches
		for i := 0; i < int(n.vector_block.Len); i++ {
			if n.vector_block.Time[i] >= start {
				if n.vector_block.Time[i] < end {
					rv <- Record{n.vector_block.Time[i], n.vector_block.Value[i]}
				} else {
					//Hitting a value past end means we are done with the query as a whole
					//we just need to clean up our memory now
					return
				}
			}
		}
	} else {
		//log.Debug("rsvci = core")

		//We are a core
		sbuck := uint16(0)
		if start > n.StartTime() {
			if start >= n.EndTime() {
				log.Panicf("hmmm")
			}
			sbuck = n.ClampBucket(start)
		}
		ebuck := uint16(bstore.KFACTOR)
		if end < n.EndTime() {
			if end < n.StartTime() {
				log.Panicf("hmm")
			}
			ebuck = n.ClampBucket(end) + 1
		}
		//log.Debug("rsvci s/e %v/%v",sbuck, ebuck)
		for buck := sbuck; buck < ebuck; buck++ {
			//log.Debug("walking over child %v", buck)
			c := n.Child(buck)
			if c != nil {
				//log.Debug("child existed")
				//log.Debug("rscvi descending from pw(%v) into [%v]", n.PointWidth(),buck)
				c.ReadStandardValuesCI(rv, err, start, end)
				c.Free()
				n.child_cache[buck] = nil
			} else {
				//log.Debug("child was nil")
			}
		}
	}
}

func (n *QTreeNode) PrintCounts(indent int) {
	spacer := ""
	for i := 0; i < indent; i++ {
		spacer += " "
	}
	if n.isLeaf {
		log.Debug("%sVECTOR <len=%v>", spacer, n.vector_block.Len)
		return
	}
	_ = n.Parent()
	pw := n.PointWidth()
	log.Debug("%sCORE(pw=%v)", spacer, pw)
	for i := 0; i < bstore.KFACTOR; i++ {
		if n.core_block.Addr[i] != 0 {
			c := n.Child(uint16(i))
			if c == nil {
				log.Panicf("Nil child with addr %v", n.core_block.Addr[i])
			}
			log.Debug("%s+ [idx=%v] <len=%v> time=(%v to %v)", spacer, i, n.core_block.Count[i], n.ChildStartTime(uint16(i)), n.ChildEndTime(uint16(i)))
			c.PrintCounts(indent + 2)
		}
	}
}
