package qtree

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"context"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/bstore"
	"github.com/op/go-logging"
)

var lg *logging.Logger

const ChanBufferSize = 4096

func init() {
	lg = logging.MustGetLogger("log")
}

//but we still have support for dates < 1970

//It is important to note that if backwards is true, then time is exclusive. So if
//a record exists with t=80 and t=100, and you query with t=100, backwards=true, you will get the t=80
//record. For forwards, time is inclusive.
func (n *QTreeNode) FindNearestValue(ctx context.Context, time int64, backwards bool) (Record, bte.BTE) {
	if ctx.Err() != nil {
		return Record{}, bte.CtxE(ctx)
	}
	if n.isLeaf {
		if n.vector_block.Len == 0 {
			panic("zero vector len")
			//return Record{}, bte.Err(bte.InvariantFailure, "zero vector len")
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
			return Record{}, bte.Err(bte.NoSuchPoint, "no such point")
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
			return Record{}, bte.Err(bte.NoSuchPoint, "no such point")
		}
		//for backwards, idx points to the containing window
		//for forwards, idx points to the window after the containing window
		chld, err := n.Child(ctx, uint16(idx))
		if err != nil {
			return Record{}, err
		}
		val, err := chld.FindNearestValue(ctx, time, backwards)

		//For both, we also need the window before this point
		if idx != 0 && n.core_block.Count[idx-1] != 0 { //The block containing the time is not empty

			//So if we are going forward, we need to do two queries, the window that CONTAINS the time, and the window
			//that FOLLOWS the time, because its possible for all the data points in the CONTAINS window to fall before
			//the time. For backwards we have the same thing but VAL above is the CONTAINS window, and we need to check
			//the BEFORE window
			chld, cherr := n.Child(ctx, uint16(idx-1))
			if cherr != nil {
				return Record{}, cherr
			}
			other, oerr := chld.FindNearestValue(ctx, time, backwards)
			if oerr != nil {
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

func (tr *QTree) FindChangedSince(ctx context.Context, gen uint64, resolution uint8) (chan ChangedRange, chan bte.BTE) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.CtxE(ctx))
	}
	rv := make(chan ChangedRange, 1024)
	rve := make(chan bte.BTE, 1)
	go func() {
		defer close(rv)
		if tr.root == nil {
			cr := ChangedRange{
				Start: MinimumTime,
				End:   MaximumTime,
				Valid: true,
			}
			rv <- cr
			return
		}
		cr := tr.root.FindChangedSince(ctx, gen, rv, rve, resolution)
		if cr.Valid {
			rv <- cr
		}
	}()
	return rv, rve
}

func (n *QTreeNode) DeleteRange(start int64, end int64) *QTreeNode {
	if n.isLeaf {
		widx, ridx := 0, 0
		//First check if this operation deletes all the entries or only some
		if n.vector_block.Len == 0 {
			lg.Panicf("This shouldn't happen")
		}
		if start <= n.vector_block.Time[0] && end > n.vector_block.Time[n.vector_block.Len-1] {
			return nil
		}
		//Otherwise we need to copy the parts that still exist
		//lg.Debug("Calling uppatch loc1")
		newn, err := n.AssertNewUpPatch()
		if err != nil {
			lg.Panicf("Could not up patch: %v", err)
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
			ch, err := n.Child(context.Background(), i)
			if err != nil {
				panic(err)
			}
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
				lg.Panicf("Could not up patch: %v", err)
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
//NOTE4: we have this chan  + return value thing because we return the last range which is more probably going to be coalesced.
//			 the stuff in the chan will also need coalescence but not as much
func (n *QTreeNode) FindChangedSince(ctx context.Context, gen uint64, rchan chan ChangedRange, echan chan bte.BTE, resolution uint8) ChangedRange {
	if ctx.Err() != nil {
		echan <- bte.CtxE(ctx)
		return ChangedRange{}
	}
	if n.isLeaf {
		if n.vector_block.Generation <= gen {
			//This can happen if the root is a leaf. Not sure if we allow that or not
			echan <- bte.Err(bte.InvariantFailure, "Should not have executed here1")
			return ChangedRange{} //Not valid
		}
		//This is acceptable, the parent had no way of knowing we were a leaf
		return ChangedRange{true, n.StartTime(), n.EndTime()}
	} else {
		if n.core_block.Generation < gen {
			//Parent should not have called us, it knows our generation
			echan <- bte.Err(bte.InvariantFailure, "Should not have executed here2")
			return ChangedRange{} //Not valid
		}
		/*if n.PointWidth() <= resolution {
			//Parent should not have called us, it knows our pointwidth
			lg.Error("Should not have executed here3")
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
			echan <- bte.ErrF(bte.InvariantFailure, "Children are older than parent (this is bad) here: %s", n.TreePath())
			return ChangedRange{}
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
							//GUARDED CHAN
							select {
							case rchan <- cr:
							case <-ctx.Done():
								bte.ChkContextError(ctx, echan)
								return ChangedRange{}
							}
							cr = ChangedRange{End: cend, Start: cstart, Valid: true}
						}
					} else {
						cr = ChangedRange{End: cend, Start: cstart, Valid: true}
					}
				} else {
					//We have a child, we need to recurse, and it has a worthy generation:
					chld, err := n.Child(ctx, uint16(k))
					if err != nil {
						echan <- err
						return ChangedRange{}
					}
					rcr := chld.FindChangedSince(ctx, gen, rchan, echan, resolution)
					if rcr.Valid {
						if cr.Valid {
							if rcr.Start == cr.End {
								//If the changed range is connected, just extend what we have
								cr.End = rcr.End
							} else {
								//Send out the prev. changed range
								//GUARDED CHAN
								select {
								case rchan <- cr:
								case <-ctx.Done():
									bte.ChkContextError(ctx, echan)
									return ChangedRange{}
								}
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
}

//TODO ADD HINT func (n *QTreeNode) PrebufferChild(i uint16) {
//TODO ADD HINT 	if n.isLeaf {
//TODO ADD HINT 		return
//TODO ADD HINT 	}
//TODO ADD HINT 	if n.core_block.Addr[i] == 0 {
//TODO ADD HINT 		return
//TODO ADD HINT 	}
//TODO ADD HINT
//TODO ADD HINT 	n.tr.LoadNode(n.core_block.Addr[i], n.core_block.CGeneration[i],
//TODO ADD HINT 		n.ChildPW(), n.ChildStartTime(i))
//TODO ADD HINT }

func (n *QTreeNode) HasChild(i uint16) bool {
	if n.isLeaf {
		lg.Panicf("Child of leaf?")
	}
	if n.core_block.Addr[i] == 0 {
		return false
	}
	return true
}
func (n *QTreeNode) Child(ctx context.Context, i uint16) (*QTreeNode, bte.BTE) {
	//lg.Debug("Child %v called on %v",i, n.TreePath())
	if n.isLeaf {
		lg.Panicf("Child of leaf?")
	}
	if n.core_block.Addr[i] == 0 {
		return nil, nil
	}
	if n.child_cache[i] != nil {
		return n.child_cache[i], nil
	}

	child, err := n.tr.LoadNode(ctx, n.core_block.Addr[i],
		n.core_block.CGeneration[i], n.ChildPW(), n.ChildStartTime(i))
	if err != nil {
		return nil, err
	}
	child.parent = n
	n.child_cache[i] = child
	return child, nil
}

//Like Child() but creates the node if it doesn't exist
func (n *QTreeNode) wchild(i uint16, isVector bool) *QTreeNode {
	if n.isLeaf {
		lg.Panicf("Child of leaf?")
	}
	if n.tr.gen == nil {
		lg.Panicf("Cannot use WChild on read only tree")
	}
	if n.PointWidth() == 0 {
		//TODO this can be a user error, so try catch earlier
		lg.Panicf("Already at the bottom of the tree!")
	}
	if n.core_block.Addr[i] == 0 {
		//lg.Debug("no existing child. spawning pw(%v)[%v] vector=%v", n.PointWidth(),i,isVector)
		var newn *QTreeNode
		//lg.Debug("child window is s=%v",n.ChildStartTime(i))
		if isVector {
			newn = n.tr.NewVectorNode(n.ChildStartTime(i), n.ChildPW())
		} else {
			newn = n.tr.NewCoreNode(n.ChildStartTime(i), n.ChildPW())
		}
		newn.parent = n
		n.child_cache[i] = newn
		n.core_block.Addr[i] = newn.ThisAddr()
		return newn
	}
	if n.child_cache[i] != nil {
		return n.child_cache[i]
	}
	child, err := n.tr.LoadNode(context.Background(), n.core_block.Addr[i],
		n.core_block.CGeneration[i], n.ChildPW(), n.ChildStartTime(i))
	if err != nil {
		panic(err)
	}
	child.parent = n
	n.child_cache[i] = child
	return child
}

//This function assumes that n is already new
func (n *QTreeNode) SetChild(idx uint16, c *QTreeNode) {
	if n.tr.gen == nil {
		lg.Panicf("umm")
	}
	if n.isLeaf {
		lg.Panicf("umm")
	}
	if !n.isNew {
		lg.Panicf("uhuh lol?")
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
		lg.Panicf("bro... cmon")
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
	n.tr.gen.HintEvictReplaced(n.ThisAddr())
	//Ok we need to clone
	newn := n.clone()

	//Does our parent need to also uppatch?
	if n.Parent() == nil {
		//We don't have a parent. We better be root
		if n.PointWidth() != ROOTPW {
			lg.Panicf("WTF")
		}
	} else {
		npar, err := n.Parent().AssertNewUpPatch()
		if err != nil {
			lg.Panicf("sigh")
		}
		//The parent might have changed. Update it
		newn.parent = npar
		//Get the IDX from the old parent
		//TODO(mpa) this operation is actually really slow. Change how we do it
		idx := n.FindParentIndex()
		//Downlink
		newn.Parent().SetChild(idx, newn)
	}
	return newn, nil
}

//We need to create a core node, insert all the vector data into it,
//and patch up the parent
func (n *QTreeNode) ConvertToCore(newvals []Record) *QTreeNode {
	//lg.Critical("CTC call")
	if n.PointWidth() == 0 {
		panic("PW 0 CTC")
	}
	n.tr.gen.HintEvictReplaced(n.ThisAddr())
	newn := n.tr.NewCoreNode(n.StartTime(), n.PointWidth())
	n.parent.AssertNewUpPatch()
	newn.parent = n.parent
	idx := n.FindParentIndex()
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
func (tr *QTree) InsertValues(records []Record) (e bte.BTE) {
	if tr.gen == nil {
		panic("nil generation on tree?")
	}
	proc_records := make([]Record, len(records))
	idx := 0
	for _, v := range records {
		//v4 changed to panic because we should catch these earlier
		if math.IsInf(v.Val, 0) || math.IsNaN(v.Val) {
			lg.Panic("WARNING Got Inf/NaN insert value, dropping")
		} else if v.Time < MinimumTime || v.Time >= MaximumTime {
			lg.Panic("WARNING Got time out of range, dropping")
		} else {
			proc_records[idx] = v
			idx++
		}
	}
	proc_records = proc_records[:idx]
	if len(proc_records) == 0 {
		lg.Panic("Should have caught this earlier")
	}
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		fmt.Println("Recovered insertvalues panic", r)
	// 		e = ErrBadInsert
	// 	}
	// }()
	sort.Sort(RecordSlice(proc_records))
	n, err := tr.root.InsertValues(proc_records)
	if err != nil {
		return bte.ErrW(bte.InsertFailure, "insert failure", err)
	}

	tr.root = n
	tr.gen.UpdateRootAddr(n.ThisAddr())
	return nil
}

func (tr *QTree) DeleteRange(start int64, end int64) bte.BTE {
	if tr.gen == nil {
		lg.Panicf("nil gen?")
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
	//lg.Debug("InsertValues called on pw(%v) with %v records @%08x",
	//	n.PointWidth(), len(records), n.ThisAddr())
	//lg.Debug("IV ADDR: %s", n.TreePath())
	////First determine if any of the records are outside our window
	//This is debugging, it won't even work if the records aren't sorted
	if !n.isLeaf {
		if records[0].Time < n.StartTime() {
			//Actually I don't think we can be less than the start.
			lg.Panicf("Bad window <")
		}
		if records[len(records)-1].Time >= n.StartTime()+((1<<n.PointWidth())*bstore.KFACTOR) {
			lg.Debug("FE.")
			lg.Debug("Node window s=%v e=%v", n.StartTime(),
				n.StartTime()+((1<<n.PointWidth())*bstore.KFACTOR))
			lg.Debug("record time: %v", records[len(records)-1].Time)
			lg.Panicf("Bad window >=")
		}
	}
	if n.isLeaf {
		//lg.Debug("insertin values in leaf")
		//TODO i think this check is wrong, it is making a new child of pw 0 which
		//I do not think is valid?
		if int(n.vector_block.Len)+len(records) > bstore.VSIZE && n.PointWidth() > 0 {
			//lg.Debug("need to convert leaf to a core");
			//lg.Debug("because %v + %v",n.vector_block.Len, len(records))
			//lg.Debug("Converting pw %v to core", n.PointWidth())
			n = n.ConvertToCore(records)
			return n, nil
		} else {
			if n.PointWidth() == 0 && int(n.vector_block.Len)+len(records) > bstore.VSIZE {
				truncidx := bstore.VSIZE - int(n.vector_block.Len)
				if truncidx <= 0 {
					lg.Critical("Truncating insert due to duplicate timestamps (FIX YOUR DATA)!")
					return n, nil
				}
				lg.Critical("Truncating insert due to duplicate timestamps (FIX YOUR DATA)!!")
				records = records[:truncidx]
			}
			//lg.Debug("inserting %d records into pw(%v) vector", len(records),n.PointWidth())
			newn, err := n.AssertNewUpPatch()
			if err != nil {
				lg.Panicf("Uppatch failed: %v", err)
			}
			n = newn
			n.MergeIntoVector(records)
			return n, nil
		}
	} else {
		if n.PointWidth() == 0 {
			panic("should not have a core node with pw 0\n")
		}
		//lg.Debug("inserting valus in core")
		//We are a core node
		newn, err := n.AssertNewUpPatch()
		if err != nil {
			lg.Panicf("ANUP: %v", err)
		}
		n := newn
		lidx := 0
		lbuckt := n.ClampBucket(records[0].Time)
		for idx := 1; idx < len(records); idx++ {
			r := records[idx]
			//lg.Debug("iter: %v, %v", idx, r)
			buckt := n.ClampBucket(r.Time)
			if buckt != lbuckt {
				//lg.Debug("records spanning bucket. flushing to child %v", lbuckt)
				//Next bucket has started
				childisleaf := idx-lidx < bstore.VSIZE
				if n.ChildPW() == 0 {
					childisleaf = true
				}
				newchild, err := n.wchild(lbuckt, childisleaf).InsertValues(records[lidx:idx])
				if err != nil {
					lg.Panicf("%v", err)
				}
				n.SetChild(lbuckt, newchild) //This should set parent link too
				lidx = idx
				lbuckt = buckt
			}
		}
		//lg.Debug("reched end of records. flushing to child %v", buckt)
		childisleaf := (len(records) - lidx) < bstore.VSIZE
		if n.ChildPW() == 0 {
			childisleaf = true
		}
		newchild, err := n.wchild(lbuckt, childisleaf).InsertValues(records[lidx:])
		//lg.Debug("Address of new child was %08x", newchild.ThisAddr())
		if err != nil {
			lg.Panicf("%v", err)
		}
		n.SetChild(lbuckt, newchild)

		return n, nil
	}
}

var ErrBadTimeRange error = errors.New("Invalid time range")

//start is inclusive, end is exclusive. To query a specific nanosecond, query (n, n+1)
func (tr *QTree) ReadStandardValuesCI(ctx context.Context, start int64, end int64) (chan Record, chan bte.BTE) {
	rv := make(chan Record, ChanBufferSize)
	rve := make(chan bte.BTE, 10)
	if tr.root != nil {
		go func() {
			tr.root.ReadStandardValuesCI(ctx, rv, rve, start, end)
			close(rv)
		}()
	} else {
		//Tree is empty, thats ok
		close(rv)
	}
	return rv, rve
}

//NOSYNC func (tr *QTree) ReadStandardValuesBlock(start int64, end int64) ([]Record, error) {
//NOSYNC 	rv := make([]Record, 0, 256)
//NOSYNC 	recordc := make(chan Record)
//NOSYNC 	errc := make(chan error)
//NOSYNC 	var err error
//NOSYNC 	busy := true
//NOSYNC 	go tr.ReadStandardValuesCI(recordc, errc, start, end)
//NOSYNC 	for busy {
//NOSYNC 		select {
//NOSYNC 		case e, _ := <-errc:
//NOSYNC 			if e != nil {
//NOSYNC 				err = e
//NOSYNC 				busy = false
//NOSYNC 			}
//NOSYNC 		case r, r_ok := <-recordc:
//NOSYNC 			if !r_ok {
//NOSYNC 				busy = false
//NOSYNC 			} else {
//NOSYNC 				rv = append(rv, r)
//NOSYNC 			}
//NOSYNC 		}
//NOSYNC 	}
//NOSYNC 	return rv, err
//NOSYNC }

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

func (tr *QTree) QueryStatisticalValues(ctx context.Context, start int64, end int64, pw uint8) (chan StatRecord, chan bte.BTE) {
	if ctx.Err() != nil {
		return nil, bte.Chan(bte.CtxE(ctx))
	}
	rv := make(chan StatRecord, ChanBufferSize)
	rve := make(chan bte.BTE, 10)
	//Remember end is inclusive for QSV
	if tr.root != nil {
		go func() {
			tr.root.QueryStatisticalValues(ctx, rv, rve, start, end, pw)
			close(rv)
		}()
	} else {
		close(rv)
	}
	return rv, rve
}

//NOSYNC func (tr *QTree) QueryStatisticalValuesBlock(start int64, end int64, pw uint8) ([]StatRecord, error) {
//NOSYNC 	rv := make([]StatRecord, 0, 256)
//NOSYNC 	recordc := make(chan StatRecord, 500)
//NOSYNC 	errc := make(chan error)
//NOSYNC 	var err error
//NOSYNC 	busy := true
//NOSYNC 	go tr.QueryStatisticalValues(recordc, errc, start, end, pw)
//NOSYNC 	for busy {
//NOSYNC 		select {
//NOSYNC 		case e, _ := <-errc:
//NOSYNC 			if e != nil {
//NOSYNC 				err = e
//NOSYNC 				busy = false
//NOSYNC 			}
//NOSYNC 		case r, r_ok := <-recordc:
//NOSYNC 			if !r_ok {
//NOSYNC 				busy = false
//NOSYNC 			} else {
//NOSYNC 				rv = append(rv, r)
//NOSYNC 			}
//NOSYNC 		}
//NOSYNC 	}
//NOSYNC 	return rv, err
//NOSYNC }

// type childpromise struct {
// 	RC  chan StatRecord
// 	Hnd *QTreeNode
// 	Idx uint16
// }

func (n *QTreeNode) QueryStatisticalValues(ctx context.Context, rv chan StatRecord, err chan bte.BTE,
	start int64, end int64, pw uint8) {
	if bte.ChkContextError(ctx, err) {
		return
	}
	if n.isLeaf {
		for idx := 0; idx < int(n.vector_block.Len); idx++ {
			if n.vector_block.Time[idx] < start {
				continue
			}
			if n.vector_block.Time[idx] > end {
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
		//close(rv)
	} else {

		sb := n.ClampBucket(start) //TODO check this function handles out of range
		eb := n.ClampBucket(end)
		recurse := pw < n.PointWidth()
		if recurse {
			//Parallel resolution of children
			//don't use n.Child() because its not threadsafe
			/*for b := sb; b <= eb; b++ {
				go n.PrebufferChild(b)
			}*/
			//TODO I think this is a fucking terrible idea and I had no idea what
			//I was thinking. Please remove the fuck out of this.
			//	var childslices []childpromise
			for b := sb; b <= eb; b++ {
				if bte.ChkContextError(ctx, err) {
					return
				}
				c, cherr := n.Child(ctx, b)
				if cherr != nil {
					err <- cherr
					return
				}
				if c != nil {
					c.QueryStatisticalValues(ctx, rv, err, start, end, pw)
					c.Free()
					n.child_cache[b] = nil
				}
			}
		} else {
			//Ok we are at the correct level and we are a core
			pwdelta := pw - n.PointWidth()
			sidx := sb >> pwdelta
			eidx := eb >> pwdelta
			for b := sidx; b <= eidx; b++ {
				count, min, mean, max := n.OpReduce(pw, uint64(b))
				if count != 0 {
					v := StatRecord{Time: n.ChildStartTime(b << pwdelta),
						Count: count,
						Min:   min,
						Mean:  mean,
						Max:   max,
					}
					//GUARDED CHAN
					select {
					case rv <- v:
					case <-ctx.Done():
						bte.ChkContextError(ctx, err)
						return
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

func (n *QTreeNode) ReadStandardValuesCI(ctx context.Context, rv chan Record, err chan bte.BTE,
	start int64, end int64) {
	if end <= start {
		panic("end <= start")
		//return
	}
	if n.isLeaf {
		//lg.Debug("rsvci = leaf len(%v)", n.vector_block.Len)
		//Currently going under assumption that buckets are sorted
		//TODO replace with binary searches
		for i := 0; i < int(n.vector_block.Len); i++ {
			if n.vector_block.Time[i] >= start {
				if n.vector_block.Time[i] < end {
					v := Record{n.vector_block.Time[i], n.vector_block.Value[i]}
					//GUARDED CHAN
					select {
					case rv <- v:
					case <-ctx.Done():
						bte.ChkContextError(ctx, err)
						return
					}
				} else {
					//Hitting a value past end means we are done with the query as a whole
					//we just need to clean up our memory now
					return
				}
			}
		}
	} else {
		//lg.Debug("rsvci = core")

		//We are a core
		sbuck := uint16(0)
		if start > n.StartTime() {
			if start >= n.EndTime() {
				lg.Panicf("hmmm")
			}
			sbuck = n.ClampBucket(start)
		}
		ebuck := uint16(bstore.KFACTOR)
		if end < n.EndTime() {
			if end < n.StartTime() {
				lg.Panicf("hmm")
			}
			ebuck = n.ClampBucket(end) + 1
		}
		//lg.Debug("rsvci s/e %v/%v",sbuck, ebuck)
		for buck := sbuck; buck < ebuck; buck++ {
			//lg.Debug("walking over child %v", buck)
			c, cherr := n.Child(ctx, buck)
			if cherr != nil {
				err <- cherr
				return
			}
			if c != nil {
				//lg.Debug("child existed")
				//lg.Debug("rscvi descending from pw(%v) into [%v]", n.PointWidth(),buck)
				c.ReadStandardValuesCI(ctx, rv, err, start, end)
				c.Free()
				n.child_cache[buck] = nil
			}
		}
	}
}

func (n *QTreeNode) updateWindowContextWholeChild(child uint16, wctx *WindowContext) {
	if (n.core_block.Max[child] > wctx.Max || wctx.Count == 0) && n.core_block.Count[child] != 0 {
		wctx.Max = n.core_block.Max[child]
	}
	if (n.core_block.Min[child] < wctx.Min || wctx.Count == 0) && n.core_block.Count[child] != 0 {
		wctx.Min = n.core_block.Min[child]
	}
	wctx.Total += n.core_block.Mean[child] * float64(n.core_block.Count[child])
	wctx.Count += n.core_block.Count[child]
}
func (n *QTreeNode) emitWindowContext(ctx context.Context, rv chan StatRecord, width uint64, wctx *WindowContext, rve chan bte.BTE) {
	var mean float64
	if wctx.Count != 0 {
		mean = wctx.Total / float64(wctx.Count)
	}
	v := StatRecord{
		Count: wctx.Count,
		Min:   wctx.Min,
		Max:   wctx.Max,
		Mean:  mean,
		Time:  wctx.Time,
	}
	//GUARDED CHAN
	select {
	case rv <- v:
	case <-ctx.Done():
		bte.ChkContextError(ctx, rve)
		return
	}
	wctx.Active = true
	wctx.Min = 0
	wctx.Total = 0
	wctx.Max = 0
	wctx.Count = 0
	wctx.Time += int64(width)
}

//QueryWindow queries this node for an arbitrary window of time. ctx must be initialized, especially with Time.
//Holes will be emitted as blank records
func (n *QTreeNode) QueryWindow(ctx context.Context, end int64, nxtstart *int64, width uint64, depth uint8, rv chan StatRecord, rve chan bte.BTE, wctx *WindowContext) {
	if bte.ChkContextError(ctx, rve) {
		return
	}
	if !n.isLeaf {
		//We are core
		var buckid uint16
		for buckid = 0; buckid < KFACTOR; buckid++ {
			//EndTime is actually start of next
			if n.ChildEndTime(buckid) <= *nxtstart {
				//This bucket is wholly contained in the 'current' window.
				//If we are not active, that does not matter
				if !wctx.Active {
					continue
				}
				//Update the current context
				n.updateWindowContextWholeChild(buckid, wctx)
			}
			if n.ChildEndTime(buckid) == *nxtstart {
				//We will have updated the context above, but we also now
				//need to emit, because there is nothing left in the context
				//We can cleanly emit and start new window without going into child
				//because the childEndTime exactly equals the next start
				//or because the child in question does not exist
				n.emitWindowContext(ctx, rv, width, wctx, rve)
				if bte.ChkContextError(ctx, rve) {
					return
				}
				//Check it wasn't the last
				if *nxtstart >= end {
					wctx.Done = true
					return
				}
				if wctx.Time != *nxtstart {
					panic(fmt.Sprintf("please report this wctx.Time=%d nxtstart=%d\n", wctx.Time, *nxtstart))
				}
				*nxtstart += int64(width)
				//At this point we have a new context, we can continue to next loop
				//iteration
				continue
			} else if n.ChildEndTime(buckid) > *nxtstart {
				//The bucket went past nxtstart, so we need to fragment
				if n.HasChild(buckid) {
					if n.ChildPW() >= depth {
						chld, cherr := n.Child(ctx, buckid)
						if cherr != nil {
							rve <- cherr
							return
						}
						chld.QueryWindow(ctx, end, nxtstart, width, depth, rv, rve, wctx)
						if wctx.Done {
							return
						}
					} else {
						//So we are not allowed to recurse. Essentially this means we are
						//going to treat this bucket as if it perfectly fitted
						//in the window. If we are not active, the child would
						//have made us active. So do that
						if !wctx.Active {
							wctx.Active = true
							*nxtstart += int64(width)
						} else {
							n.updateWindowContextWholeChild(buckid, wctx)
							n.emitWindowContext(ctx, rv, width, wctx, rve)
							if bte.ChkContextError(ctx, rve) {
								return
							}
							if wctx.Time != *nxtstart {
								panic(fmt.Sprintf("please report this-C wctx.Time=%d nxtstart=%d\n", wctx.Time, *nxtstart))
							}
							*nxtstart += int64(width)
							if *nxtstart >= end {
								wctx.Done = true
								return
							}
						}
					}
				} else {
					//We would have had a child that did the emit + restart for us
					//Possibly several times over, but there is a hole, so we need
					//to emulate all of that.
					if !wctx.Active {
						//Our current time is wctx.Time
						//We know that ChildEndTime is greater than nxttime
						//We can just set time to nxttime, and then continue with
						//the active loop
						wctx.Time = *nxtstart
						wctx.Active = true
						*nxtstart += int64(width)
					}
					//We are definitely active now
					//For every nxtstart less than this (missing) bucket's end time,
					//emit a window
					for *nxtstart <= n.ChildEndTime(buckid) {
						n.emitWindowContext(ctx, rv, width, wctx, rve)
						if bte.ChkContextError(ctx, rve) {
							return
						}
						if wctx.Time != *nxtstart {
							panic(fmt.Sprintf("please report this-D wctx.Time=%d nxtstart=%d\n", wctx.Time, *nxtstart))
						}
						*nxtstart += int64(width)
						if *nxtstart >= end {
							wctx.Done = true
							return
						}
					}
				}
			} //End bucket > nxtstart
		} //For loop over children
	} else {
		//We are leaf
		//There could be multiple windows within us
		var i uint16
		for i = 0; i < n.vector_block.Len; i++ {

			//We use this twice, pull it out
			add := func() {
				wctx.Total += n.vector_block.Value[i]
				if n.vector_block.Value[i] < wctx.Min || wctx.Count == 0 {
					wctx.Min = n.vector_block.Value[i]
				}
				if n.vector_block.Value[i] > wctx.Max || wctx.Count == 0 {
					wctx.Max = n.vector_block.Value[i]
				}
				wctx.Count++
			}

			//Check if part of active wctx
			if n.vector_block.Time[i] < *nxtstart {
				//This is part of the previous window
				if wctx.Active {
					add()
				} else {
					//This is before our active time
					continue
				}
			} else {
				//We have crossed a window boundary
				if wctx.Active {
					//We need to emit the window
					n.emitWindowContext(ctx, rv, width, wctx, rve)
					if bte.ChkContextError(ctx, rve) {
						return
					}
					//Check it wasn't the last
					if *nxtstart >= end {
						wctx.Done = true
						return
					}
					*nxtstart += int64(width)
				} else {
					//This is the first window
					wctx.Active = true
					*nxtstart += int64(width)
				}
				//If we are here, this point needs to be added to the context
				add()
			}
		}
	}
}

//QueryWindow queries for windows between start and end, with an explicit (arbitrary) width. End is exclusive
func (tr *QTree) QueryWindow(ctx context.Context, start int64, end int64, width uint64, depth uint8) (chan StatRecord, chan bte.BTE) {
	wctx := &WindowContext{Time: start}
	var nxtstart = start
	rv := make(chan StatRecord, ChanBufferSize)
	rve := make(chan bte.BTE, 10)
	if tr.root != nil {
		go func() {
			tr.root.QueryWindow(ctx, end, &nxtstart, width, depth, rv, rve, wctx)
			close(rv)
		}()
	} else {
		go func() {
			for ; start+int64(width) < end; start += int64(width) {
				sr := StatRecord{
					Time:  start,
					Count: 0,
				}
				rv <- sr
			}
			close(rv)
		}()
	}
	return rv, rve
}
