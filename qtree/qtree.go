package qtree

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/SoftwareDefinedBuildings/btrdb/internal/bstore"
	"github.com/op/go-logging"
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

func (tr *QTree) FindChangedSinceSlice(gen uint64, resolution uint8) []ChangedRange {
	if tr.root == nil {
		return make([]ChangedRange, 0)
	}
	rv := make([]ChangedRange, 0, 1024)
	rch := tr.FindChangedSince(gen, resolution)
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
				return rv
			}
			if !cr.Valid {
				log.Panicf("Didn't think this could happen")
			}
			//Coalesce
			if lr.Valid && cr.Start == lr.End {
				lr.End = cr.End
			} else {
				if lr.Valid {
					rv = append(rv, lr)
				}
				lr = cr
			}
		}
	}
	return rv
}
func (tr *QTree) FindChangedSince(gen uint64, resolution uint8) chan ChangedRange {
	rv := make(chan ChangedRange, 1024)
	go func() {
		if tr.root == nil {
			close(rv)
			return
		}
		cr := tr.root.FindChangedSince(gen, rv, resolution)
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
//NOTE: We should return changes SINCE a generation, so strictly greater than.
func (n *QTreeNode) FindChangedSince(gen uint64, rchan chan ChangedRange, resolution uint8) ChangedRange {
	if n.isLeaf {
		if n.vector_block.Generation <= gen {
			//This can happen if the root is a leaf. Not sure if we allow that or not
			log.Error("Should not have executed here1")
			return ChangedRange{} //Not valid
		}
		//This is acceptable, the parent had no way of knowing we were a leaf
		return ChangedRange{true, n.StartTime(), n.EndTime()}
	} else {
		if n.core_block.Generation < gen {
			//Parent should not have called us, it knows our generation
			log.Error("Should not have executed here2")
			return ChangedRange{} //Not valid
		}
		/*if n.PointWidth() <= resolution {
			//Parent should not have called us, it knows our pointwidth
			log.Error("Should not have executed here3")
			return ChangedRange{true, n.StartTime(), n.EndTime()}
		}*/
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

		norecurse := n.PointWidth() <= resolution
		for k := 0; k < KFACTOR; k++ {
			if n.core_block.CGeneration[k] > gen {
				if n.core_block.Addr[k] == 0 || norecurse {
					//A whole child was deleted here or we don't want to recurse further
					cstart := n.ChildStartTime(uint16(k))
					cend := n.ChildEndTime(uint16(k))
					if cr.Valid {
						if cstart == cr.End {
							cr.End = cend
						} else {
							rchan <- cr
							cr = ChangedRange{End: cend, Start: cstart, Valid: true}
						}
					} else {
						cr = ChangedRange{End: cend, Start: cstart, Valid: true}
					}
				} else {
					//We have a child, we need to recurse, and it has a worthy generation:
					rcr := n.Child(uint16(k)).FindChangedSince(gen, rchan, resolution)
					if rcr.Valid {
						if cr.Valid {
							if rcr.Start == cr.End {
								//If the changed range is connected, just extend what we have
								cr.End = rcr.End
							} else {
								//Send out the prev. changed range
								rchan <- cr
								cr = rcr
							}
						} else {
							cr = rcr
						}
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

func (n *QTreeNode) PrebufferChild(i uint16) {
	if n.isLeaf {
		return
	}
	if n.core_block.Addr[i] == 0 {
		return
	}

	n.tr.LoadNode(n.core_block.Addr[i], n.core_block.CGeneration[i],
		n.ChildPW(), n.ChildStartTime(i))
}

func (n *QTreeNode) HasChild(i uint16) bool {
	if n.isLeaf {
		log.Panicf("Child of leaf?")
	}
	if n.core_block.Addr[i] == 0 {
		return false
	}
	return true
}
func (n *QTreeNode) Child(i uint16) *QTreeNode {
	//log.Debugf("Child %v called on %v",i, n.TreePath())
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
		log.Debugf("We are at %v", n.TreePath())
		log.Debugf("We were trying to load child %v", i)
		log.Debugf("With address %v", n.core_block.Addr[i])
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
		//	log.Debugf("ok %d", n.PointWidth())
	}
	if n.core_block.Addr[i] == 0 {
		//log.Debugf("no existing child. spawning pw(%v)[%v] vector=%v", n.PointWidth(),i,isVector)
		var newn *QTreeNode
		var err error
		//log.Debugf("child window is s=%v",n.ChildStartTime(i))
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
	//log.Debugf("InsertValues called on pw(%v) with %v records @%08x",
	//	n.PointWidth(), len(records), n.ThisAddr())
	//log.Debugf("IV ADDR: %s", n.TreePath())
	////First determine if any of the records are outside our window
	//This is debugging, it won't even work if the records aren't sorted
	if !n.isLeaf {
		if records[0].Time < n.StartTime() {
			//Actually I don't think we can be less than the start.
			log.Panicf("Bad window <")
		}
		if records[len(records)-1].Time >= n.StartTime()+((1<<n.PointWidth())*bstore.KFACTOR) {
			log.Debug("FE.")
			log.Debugf("Node window s=%v e=%v", n.StartTime(),
				n.StartTime()+((1<<n.PointWidth())*bstore.KFACTOR))
			log.Debugf("record time: %v", records[len(records)-1].Time)
			log.Panicf("Bad window >=")
		}
	}
	if n.isLeaf {
		//log.Debug("insertin values in leaf")
		if int(n.vector_block.Len)+len(records) > bstore.VSIZE && n.PointWidth() != 0 {
			//log.Debug("need to convert leaf to a core");
			//log.Debugf("because %v + %v",n.vector_block.Len, len(records))
			//log.Debugf("Converting pw %v to core", n.PointWidth())
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
			//log.Debugf("inserting %d records into pw(%v) vector", len(records),n.PointWidth())
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
			//log.Debugf("iter: %v, %v", idx, r)
			buckt := n.ClampBucket(r.Time)
			if buckt != lbuckt {
				//log.Debugf("records spanning bucket. flushing to child %v", lbuckt)
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
		//log.Debugf("reched end of records. flushing to child %v", buckt)
		newchild, err := n.wchild(lbuckt, (len(records)-lidx) < bstore.VSIZE).InsertValues(records[lidx:])
		//log.Debugf("Address of new child was %08x", newchild.ThisAddr())
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

type WindowContext struct {
	Time   int64
	Count  uint64
	Min    float64
	Total  float64
	Max    float64
	Active bool
	Done   bool
}

func (tr *QTree) QueryStatisticalValues(rv chan StatRecord, err chan error,
	start int64, end int64, pw uint8) {
	//Remember end is inclusive for QSV
	if tr.root != nil {
		tr.root.QueryStatisticalValues(rv, err, start, end, pw)
	}
	close(err)
}

func (tr *QTree) QueryStatisticalValuesBlock(start int64, end int64, pw uint8) ([]StatRecord, error) {
	rv := make([]StatRecord, 0, 256)
	recordc := make(chan StatRecord, 500)
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

type childpromise struct {
	RC  chan StatRecord
	Hnd *QTreeNode
	Idx uint16
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
		close(rv)
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
		recurse := pw < n.PointWidth()
		if recurse {
			//Parallel resolution of children
			//don't use n.Child() because its not threadsafe
			/*for b := sb; b <= eb; b++ {
				go n.PrebufferChild(b)
			}*/

			var childslices []childpromise
			for b := sb; b <= eb; b++ {
				c := n.Child(b)
				if c != nil {
					childrv := make(chan StatRecord, 500)
					go c.QueryStatisticalValues(childrv, err, start, end, pw)
					childslices = append(childslices, childpromise{childrv, c, b})
				}
			}
			for _, prom := range childslices {
				for v := range prom.RC {
					rv <- v
				}
				prom.Hnd.Free()
				n.child_cache[prom.Idx] = nil
			}
			close(rv)
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
			close(rv)
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
		//log.Debugf("rsvci = leaf len(%v)", n.vector_block.Len)
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
		//log.Debugf("rsvci s/e %v/%v",sbuck, ebuck)
		for buck := sbuck; buck < ebuck; buck++ {
			//log.Debugf("walking over child %v", buck)
			c := n.Child(buck)
			if c != nil {
				//log.Debug("child existed")
				//log.Debugf("rscvi descending from pw(%v) into [%v]", n.PointWidth(),buck)
				c.ReadStandardValuesCI(rv, err, start, end)
				c.Free()
				n.child_cache[buck] = nil
			} else {
				//log.Debug("child was nil")
			}
		}
	}
}

func (n *QTreeNode) updateWindowContextWholeChild(child uint16, ctx *WindowContext) {

	if (n.core_block.Max[child] > ctx.Max || ctx.Count == 0) && n.core_block.Count[child] != 0 {
		ctx.Max = n.core_block.Max[child]
	}
	if (n.core_block.Min[child] < ctx.Min || ctx.Count == 0) && n.core_block.Count[child] != 0 {
		ctx.Min = n.core_block.Min[child]
	}
	ctx.Total += n.core_block.Mean[child] * float64(n.core_block.Count[child])
	ctx.Count += n.core_block.Count[child]
}
func (n *QTreeNode) emitWindowContext(rv chan StatRecord, width uint64, ctx *WindowContext) {
	var mean float64
	if ctx.Count != 0 {
		mean = ctx.Total / float64(ctx.Count)
	}
	res := StatRecord{
		Count: ctx.Count,
		Min:   ctx.Min,
		Max:   ctx.Max,
		Mean:  mean,
		Time:  ctx.Time,
	}
	rv <- res
	ctx.Active = true
	ctx.Min = 0
	ctx.Total = 0
	ctx.Max = 0
	ctx.Count = 0
	ctx.Time += int64(width)
}

//QueryWindow queries this node for an arbitrary window of time. ctx must be initialized, especially with Time.
//Holes will be emitted as blank records
func (n *QTreeNode) QueryWindow(end int64, nxtstart *int64, width uint64, depth uint8, rv chan StatRecord, ctx *WindowContext) {
	if !n.isLeaf {
		//We are core
		var buckid uint16
		for buckid = 0; buckid < KFACTOR; buckid++ {
			//EndTime is actually start of next
			if n.ChildEndTime(buckid) <= *nxtstart {
				//This bucket is wholly contained in the 'current' window.
				//If we are not active, that does not matter
				if !ctx.Active {
					continue
				}
				//Update the current context
				n.updateWindowContextWholeChild(buckid, ctx)
			}
			if n.ChildEndTime(buckid) == *nxtstart {
				//We will have updated the context above, but we also now
				//need to emit, because there is nothing left in the context
				//We can cleanly emit and start new window without going into child
				//because the childEndTime exactly equals the next start
				//or because the child in question does not exist
				n.emitWindowContext(rv, width, ctx)
				//Check it wasn't the last
				if *nxtstart >= end {
					ctx.Done = true
					return
				}
				*nxtstart += int64(width)
				//At this point we have a new context, we can continue to next loop
				//iteration
				continue
			} else if n.ChildEndTime(buckid) > *nxtstart {
				//The bucket went past nxtstart, so we need to fragment
				if true /*XTAG replace with depth check*/ {
					//Now, we might want
					//They could be equal if next window started exactly after
					//this child. That would mean we don't recurse
					//As it turns out, we must recurse before emitting
					if n.HasChild(buckid) {
						n.Child(buckid).QueryWindow(end, nxtstart, width, depth, rv, ctx)
						if ctx.Done {
							return
						}
					} else {
						//We would have had a child that did the emit + restart for us
						//Possibly several times over, but there is a hole, so we need
						//to emulate all of that.
						if !ctx.Active {
							//Our current time is ctx.Time
							//We know that ChildEndTime is greater than nxttime
							//We can just set time to nxttime, and then continue with
							//the active loop
							ctx.Time = *nxtstart
							ctx.Active = true
							*nxtstart += int64(width)
						}
						//We are definitely active now
						//For every nxtstart less than this (missing) bucket's end time,
						//emit a window
						for *nxtstart <= n.ChildEndTime(buckid) {
							n.emitWindowContext(rv, width, ctx)
							if ctx.Time != *nxtstart {
								panic("LOLWUT")
							}
							*nxtstart += int64(width)
							if *nxtstart >= end {
								ctx.Done = true
								return
							}
						}
					}
				} //end depth check
			} //End bucket > nxtstart
		} //For loop over children
	} else {
		//We are leaf
		//There could be multiple windows within us
		var i uint16
		for i = 0; i < n.vector_block.Len; i++ {

			//We use this twice, pull it out
			add := func() {
				ctx.Total += n.vector_block.Value[i]
				if n.vector_block.Value[i] < ctx.Min || ctx.Count == 0 {
					ctx.Min = n.vector_block.Value[i]
				}
				if n.vector_block.Value[i] > ctx.Max || ctx.Count == 0 {
					ctx.Max = n.vector_block.Value[i]
				}
				ctx.Count++
			}

			//Check if part of active ctx
			if n.vector_block.Time[i] < *nxtstart {
				//This is part of the previous window
				if ctx.Active {
					add()
				} else {
					//This is before our active time
					continue
				}
			} else {
				//We have crossed a window boundary
				if ctx.Active {
					//We need to emit the window
					n.emitWindowContext(rv, width, ctx)
					//Check it wasn't the last
					if *nxtstart >= end {
						ctx.Done = true
						return
					}
					*nxtstart += int64(width)
				} else {
					//This is the first window
					ctx.Active = true
					*nxtstart += int64(width)
				}
				//If we are here, this point needs to be added to the context
				add()
			}
		}
	}
}

//QueryWindow queries for windows between start and end, with an explicit (arbitrary) width. End is exclusive
func (tr *QTree) QueryWindow(start int64, end int64, width uint64, depth uint8, rv chan StatRecord) {
	ctx := &WindowContext{Time: start}
	var nxtstart = start
	if tr.root != nil {
		tr.root.QueryWindow(end, &nxtstart, width, depth, rv, ctx)
	}
	close(rv)
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
