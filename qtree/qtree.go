package qtree

import (
	"code.google.com/p/go-uuid/uuid"
	lg "code.google.com/p/log4go"
	"errors"
	"fmt"
	bstore "github.com/SoftwareDefinedBuildings/quasar/bstoreGen1"
	"math"
	"sort"
)

const PWFACTOR = bstore.PWFACTOR
const KFACTOR = bstore.KFACTOR
const MICROSECOND = 1000
const MILLISECOND = 1000 * MICROSECOND
const SECOND = 1000 * MILLISECOND
const MINUTE = 60 * SECOND
const HOUR = 60 * MINUTE
const DAY = 24 * HOUR
const ROOTPW = 56 //This makes each bucket at the root ~= 2.2 years
//so the root spans 146.23 years
const ROOTSTART = -1152921504606846976 //This makes the 16th bucket start at 1970 (0)
const MinimumTime = -(16 << 56)
const MaximumTime = (48 << 56)

//but we still have support for dates < 1970

var ErrNoSuchStream = errors.New("No such stream")
var ErrNotLeafNode = errors.New("Not a leaf node")
var ErrImmutableTree = errors.New("Tree is immutable")
var ErrIdxNotFound = errors.New("Index not found")
var ErrNoSuchPoint = errors.New("No such point")

type QTree struct {
	sb       *bstore.Superblock
	bs       *bstore.BlockStore
	gen      *bstore.Generation
	root     *QTreeNode
	commited bool
}

type Record struct {
	Time int64
	Val  float64
}

type RecordSlice []Record

func (s RecordSlice) Len() int {
	return len(s)
}

func (s RecordSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s RecordSlice) Less(i, j int) bool {
	return s[i].Time < s[j].Time
}

func (tr *QTree) GetReferencedAddrsDebug() map[uint64]bool {
	refset := make(map[uint64]bool, 1024000)

	rchan := tr.GetAllReferencedVAddrs()
	//for i, v := range e_tree.
	idx := 0
	for {
		val, ok := <-rchan
		if idx%8192 == 0 {
			lg.Trace("Got referenced addr #%d", idx)
		}
		idx += 1
		if !ok {
			break
		}
		refset[val] = true
	}
	return refset
}

func (tr *QTree) Commit() {
	if tr.commited {
		lg.Crashf("Tree alredy comitted")
	}
	if tr.gen == nil {
		lg.Crashf("Commit on non-write-tree")
	}

	tr.gen.Commit()
	tr.commited = true
	tr.gen = nil

}

/**
 * Load a quasar tree
 */
func NewReadQTree(bs *bstore.BlockStore, id uuid.UUID, generation uint64) (*QTree, error) {
	sb := bs.LoadSuperblock(id, generation)
	if sb == nil {
		return nil, ErrNoSuchStream
	}
	if sb.Root() == 0 {
		lg.Crashf("Was expectin nonzero root tbh")
	}
	rv := &QTree{sb: sb, bs: bs}
	rt, err := rv.LoadNode(sb.Root())
	if err != nil {
		lg.Crashf("%v", err)
		return nil, err
	}
	//lg.Debug("The start time for the root is %v",rt.StartTime())
	rv.root = rt
	return rv, nil
}

func NewWriteQTree(bs *bstore.BlockStore, id uuid.UUID) (*QTree, error) {
	gen := bs.ObtainGeneration(id)
	rv := &QTree{
		sb:  gen.New_SB,
		gen: gen,
		bs:  bs,
	}

	//If there is an existing root node, we need to load it so that it
	//has the correct values
	if rv.sb.Root() != 0 {
		rt, err := rv.LoadNode(rv.sb.Root())
		if err != nil {
			lg.Crashf("%v", err)
			return nil, err
		}
		rv.root = rt
	} else {
		rt, err := rv.NewCoreNode(ROOTSTART, ROOTPW)
		if err != nil {
			lg.Crashf("%v", err)
			return nil, err
		}
		rv.root = rt
	}

	return rv, nil
}

//It is important to note that if backwards is true, then time is exclusive. So if
//a record exists with t=80 and t=100, and you query with t=100, backwards=true, you will get the t=80
//record. For forwards, time is inclusive.
func (n *QTreeNode) FindNearestValue(time int64, backwards bool) (Record, error) {
	if n.isLeaf {

		if n.vector_block.Len == 0 {
			lg.Crashf("Not expecting this")
		}
		idx := -1
		for i := 0; i < n.vector_block.Len; i++ {
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

func (tr *QTree) FindChangedSince(gen uint64, threshold uint64) chan ChangedRange {
	rv := make(chan ChangedRange, 1024)
	go func() {
		cr := tr.root.FindChangedSince(gen, rv, threshold, false)
		rv <- cr
		close(rv)
	}()
	return rv
}

type ChangedRange struct {
	Valid bool
	Start int64
	End   int64
}

func (n *QTreeNode) FindChangedSince(gen uint64, rchan chan ChangedRange, threshold uint64, youAreAboveT bool) ChangedRange {
	if n.isLeaf {
		if n.vector_block.Generation <= gen {
			return ChangedRange{} //Not valid
		}
		return ChangedRange{true, n.StartTime(), n.EndTime()}
	} else {
		if n.core_block.Generation <= gen {
			return ChangedRange{} //Not valid
		}
		if youAreAboveT {
			return ChangedRange{true, n.StartTime(), n.EndTime()}
		}
		cr := ChangedRange{}
		for k := 0; k < KFACTOR; k++ {
			ch := n.Child(uint16(k))
			cabove := ch.core_block.Count[k] >= threshold
			rcr := n.Child(uint16(k)).FindChangedSince(gen, rchan, threshold, cabove)
			if rcr.Valid {
				if cr.Valid {
					if rcr.Start == cr.End+1 {
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
		//Note that we don't get 100% coalescence. The receiver on rchan should also check for coalescence.
		//we just do a bit to reduce traffic on the channel. One case is if we have two disjoint ranges in a
		//core, and the first is at the start. We send it on rchan even if it might be adjacent to the prev
		//sibling
		return cr //Which might be invalid if we got none from children (all islanded)
	}
	return ChangedRange{}
}

func (n *QTree) FindNearestValue(time int64, backwards bool) (Record, error) {
	return n.root.FindNearestValue(time, backwards)
}

type QTreeNode struct {
	tr           *QTree
	vector_block *bstore.Vectorblock
	core_block   *bstore.Coreblock
	isLeaf       bool
	child_cache  [bstore.KFACTOR]*QTreeNode
	parent       *QTreeNode
	isNew        bool
}

func (n *QTree) Generation() uint64 {
	if n.gen != nil {
		//Return the gen it will have after commit
		return n.gen.Number()
	} else {
		//Return it's current gen
		return n.sb.Gen()
	}
	return n.gen.Number()
}

func (n *QTreeNode) Child(i uint16) *QTreeNode {
	//lg.Debug("Child %v called on %v",i, n.TreePath())
	if n.isLeaf {
		lg.Crashf("Child of leaf?")
	}
	if n.core_block.Addr[i] == 0 {
		return nil
	}
	if n.child_cache[i] != nil {
		return n.child_cache[i]
	}

	child, err := n.tr.LoadNode(n.core_block.Addr[i])
	if err != nil {
		lg.Debug("We are at %v", n.TreePath())
		lg.Debug("We were trying to load child %v", i)
		lg.Debug("With address %v", n.core_block.Addr[i])
		lg.Crashf("%v", err)
	}
	child.parent = n
	n.child_cache[i] = child
	return child
}

func (n *QTreeNode) TreePath() string {
	rv := ""
	if n.isLeaf {
		rv += "V"
	} else {
		rv += "C"
	}
	dn := n
	for {
		par := dn.Parent()
		if par == nil {
			return rv
		}
		//Try locate the index of this node in the parent
		addr := dn.ThisAddr()
		found := false
		for i := 0; i < bstore.KFACTOR; i++ {
			if par.core_block.Addr[i] == addr {
				rv = fmt.Sprintf("(%v)[%v].", par.PointWidth(), i) + rv
				found = true
				break
			}
		}
		if !found {
			lg.Crashf("Could not find self address in parent")
		}
		dn = par
	}
}

func (n *QTreeNode) ChildPW() uint8 {
	if n.PointWidth() <= PWFACTOR {
		return 0
	} else {
		return n.PointWidth() - PWFACTOR
	}
}

//So this might be the only explanation of how PW really relates to time:
//If the node is core, the node's PW is the log of the amount of time that
//each child covers. So a pw of 8 means that each child covers 1<<8 nanoseconds
//If the node is a vector, the PW represents what the PW would be if it were
//a core. It does NOT represent the PW of the vector itself.
func (n *QTreeNode) WidthTime() int64 {
	return 1 << n.PointWidth()
}

func (n *QTreeNode) ArbitraryStartTime(idx uint64, pw uint8) int64 {
	return n.StartTime() + int64(idx*(1<<pw))
}

func (n *QTreeNode) ChildStartTime(idx uint16) int64 {
	return n.ArbitraryStartTime(uint64(idx), n.PointWidth())
}

func (n *QTreeNode) ThisAddr() uint64 {
	if n.isLeaf {
		return n.vector_block.This_addr
	} else {
		return n.core_block.This_addr
	}
}

//Like Child() but creates the node if it doesn't exist
func (n *QTreeNode) wchild(i uint16, isVector bool) *QTreeNode {
	if n.isLeaf {
		lg.Crashf("Child of leaf?")
	}
	if n.tr.gen == nil {
		lg.Crashf("Cannot use WChild on read only tree")
	}
	if n.PointWidth() == 0 {
		lg.Crashf("Already at the bottom of the tree!")
	} else {
		//	lg.Debug("ok %d", n.PointWidth())
	}
	if n.core_block.Addr[i] == 0 {
		//lg.Debug("no existing child. spawning pw(%v)[%v] vector=%v", n.PointWidth(),i,isVector)
		var newn *QTreeNode
		var err error
		//lg.Debug("child window is s=%v",n.ChildStartTime(i))
		if isVector {
			newn, err = n.tr.NewVectorNode(n.ChildStartTime(i), n.ChildPW())
		} else {
			newn, err = n.tr.NewCoreNode(n.ChildStartTime(i), n.ChildPW())
		}
		if err != nil {
			lg.Crashf("%v", err)
		}
		newn.parent = n
		n.child_cache[i] = newn
		n.core_block.Addr[i] = newn.ThisAddr()
		return newn
	}
	if n.child_cache[i] != nil {
		return n.child_cache[i]
	}
	child, err := n.tr.LoadNode(n.core_block.Addr[i])
	if err != nil {
		lg.Crashf("%v", err)
	}
	child.parent = n
	n.child_cache[i] = child
	return child
}

func (n *QTreeNode) Parent() *QTreeNode {
	return n.parent
}

//This function assumes that n is already new
func (n *QTreeNode) SetChild(idx uint16, c *QTreeNode) {
	if n.tr.gen == nil {
		lg.Crashf("umm")
	}
	if n.isLeaf {
		lg.Crashf("umm")
	}
	if !n.isNew {
		lg.Crashf("uhuh lol?")
	}
	n.child_cache[idx] = c
	c.parent = n
	if c.isLeaf {
		n.core_block.Addr[idx] = c.vector_block.This_addr
	} else {
		n.core_block.Addr[idx] = c.core_block.This_addr
	}
	//Note that a bunch of updates of the metrics inside the block need to
	//go here
	n.core_block.Min[idx] = c.OpMin()
	n.core_block.Max[idx] = c.OpMax()
	n.core_block.Count[idx], n.core_block.Mean[idx] = c.OpCountMean()
}

func (tr *QTree) LoadNode(addr uint64) (*QTreeNode, error) {
	//lg.Debug("loading node@%08x", addr)
	db := tr.bs.ReadDatablock(addr)
	n := &QTreeNode{tr: tr}
	switch db.GetDatablockType() {
	case bstore.Vector:
		n.vector_block = db.(*bstore.Vectorblock)
		n.isLeaf = true
	case bstore.Core:
		n.core_block = db.(*bstore.Coreblock)
		n.isLeaf = false
	default:
		lg.Crashf("What kind of type is this? %+v", db.GetDatablockType())
	}
	if n.ThisAddr() == 0 {
		lg.Crashf("Node has zero address")
	}
	return n, nil
}

func ClampTime(t int64, pw uint8) int64 {
	if pw == 0 {
		return t
	}
	//Protip... &^ is bitwise and not in golang... not XOR
	return t &^ ((1 << pw) - 1)

}

func (n *QTreeNode) ClampBucket(t int64) uint16 {
	if n.isLeaf {
		lg.Crashf("Not meant to use this on leaves")
	}
	if t < n.StartTime() {
		t = n.StartTime()
	}
	t -= n.StartTime()

	rv := (t >> n.PointWidth())
	if rv >= bstore.KFACTOR {
		rv = bstore.KFACTOR - 1
	}
	return uint16(rv)
}

//Unlike core nodes, vectors have infinitely many buckets. This
//function allows you to get a bucket idx for a time and an
//arbitrary point width
func (n *QTreeNode) ClampVBucket(t int64, pw uint8) uint64 {
	if !n.isLeaf {
		lg.Crashf("This is intended for vectors")
	}
	if t < n.StartTime() {
		t = n.StartTime()
	}
	t -= n.StartTime()
	if pw > n.Parent().PointWidth() {
		lg.Crashf("I can't do this dave")
	}
	idx := uint64(t) >> pw
	maxidx := uint64(n.Parent().WidthTime()) >> pw
	if idx >= maxidx {
		idx = maxidx - 1
	}
	return idx
}

func (tr *QTree) NewCoreNode(startTime int64, pointWidth uint8) (*QTreeNode, error) {
	if tr.gen == nil {
		return nil, ErrImmutableTree
	}
	cb, err := tr.gen.AllocateCoreblock()
	if err != nil {
		return nil, err
	}
	cb.PointWidth = pointWidth
	startTime = ClampTime(startTime, pointWidth)
	cb.StartTime = startTime
	rv := &QTreeNode{
		core_block: cb,
		tr:         tr,
		isNew:      true,
	}
	return rv, nil
}

func (tr *QTree) NewVectorNode(startTime int64, pointWidth uint8) (*QTreeNode, error) {
	if tr.gen == nil {
		return nil, ErrImmutableTree
	}
	vb, err := tr.gen.AllocateVectorblock()
	if err != nil {
		return nil, err
	}
	vb.PointWidth = pointWidth
	startTime = ClampTime(startTime, pointWidth)
	vb.StartTime = startTime
	rv := &QTreeNode{
		vector_block: vb,
		tr:           tr,
		isLeaf:       true,
		isNew:        true,
	}
	return rv, nil
}

func (n *QTreeNode) FindParentIndex() (uint16, error) {
	//Try locate the index of this node in the parent
	addr := n.ThisAddr()
	for i := uint16(0); i < bstore.KFACTOR; i++ {
		if n.Parent().core_block.Addr[i] == addr {
			return i, nil
		}
	}
	return bstore.KFACTOR, ErrIdxNotFound
}

func (n *QTreeNode) clone() (*QTreeNode, error) {
	var rv *QTreeNode
	var err error
	if !n.isLeaf {
		rv, err = n.tr.NewCoreNode(n.StartTime(), n.PointWidth())
		if err != nil {
			return nil, err
		}
		n.core_block.CopyInto(rv.core_block)
	} else {
		rv, err = n.tr.NewVectorNode(n.StartTime(), n.PointWidth())
		if err != nil {
			return nil, err
		}
		n.vector_block.CopyInto(rv.vector_block)
	}
	return rv, nil
}

//Here is where we would replace with fancy delta compression
func (n *QTreeNode) MergeIntoVector(r []Record) {
	if !n.isNew {
		lg.Crashf("bro... cmon")
	}
	//There is a special case: this can be called to insert into an empty leaf
	//don't bother being smart then
	if n.vector_block.Len == 0 {
		for i := 0; i < len(r); i++ {
			n.vector_block.Time[i] = r[i].Time
			n.vector_block.Value[i] = r[i].Val
		}
		n.vector_block.Len = len(r)
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
			for iVec < n.vector_block.Len {
				n.vector_block.Time[iDst] = curtimes[iVec]
				n.vector_block.Value[iDst] = curvals[iVec]
				iDst++
				iVec++
			}
			break
		}
		if iVec == n.vector_block.Len {
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
	n.vector_block.Len += len(r)
}
func (n *QTreeNode) AssertNewUpPatch() (*QTreeNode, error) {
	if n.isNew {
		//We assume that all parents are already ok
		return n, nil
	}

	//Ok we need to clone
	newn, err := n.clone()
	if err != nil {
		lg.Crashf("%v", err)
	}

	//This operation implies that the current generation will no longer
	//reference n, so we flag it as unreferenced to facilitate GC
	n.tr.gen.UnreferenceBlock(n.ThisAddr())

	//Does our parent need to also uppatch?
	if n.Parent() == nil {
		//We don't have a parent. We better be root
		if n.PointWidth() != ROOTPW {
			lg.Crashf("WTF")
		}
	} else {
		npar, err := n.Parent().AssertNewUpPatch()
		if err != nil {
			lg.Crashf("sigh")
		}
		//The parent might have changed. Update it
		newn.parent = npar
		//Get the IDX from the old parent
		//TODO(mpa) this operation is actually really slow. Change how we do it
		idx, err := n.FindParentIndex()
		if err != nil {
			lg.Crashf("Could not find parent idx")
		}
		//Downlink
		newn.Parent().SetChild(idx, newn)
	}
	return newn, nil
}

//We need to create a core node, insert all the vector data into it,
//and patch up the parent
func (n *QTreeNode) ConvertToCore(newvals []Record) *QTreeNode {
	newn, err := n.tr.NewCoreNode(n.StartTime(), n.PointWidth())
	if err != nil {
		lg.Crashf("%v", err)
	}
	n.parent.AssertNewUpPatch()
	newn.parent = n.parent
	idx, err := n.FindParentIndex()
	newn.Parent().SetChild(idx, newn)
	valset := make([]Record, n.vector_block.Len+len(newvals))
	for i := 0; i < n.vector_block.Len; i++ {
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

	//This operation implies that this vector is now unreferenced.
	n.tr.gen.UnreferenceBlock(n.ThisAddr())
	return newn
}
func (n *QTreeNode) PointWidth() uint8 {
	if n.isLeaf {
		return n.vector_block.PointWidth
	} else {
		return n.core_block.PointWidth
	}
}

func (n *QTreeNode) StartTime() int64 {
	if n.isLeaf {
		return n.vector_block.StartTime
	} else {
		return n.core_block.StartTime
	}
}

func (n *QTreeNode) EndTime() int64 {
	if n.isLeaf {
		//We do this because out point width might not be *KFACTOR as we might be
		//at the lowest level
		return n.StartTime() + (1 << n.Parent().PointWidth())
	} else {
		//A core node has multiple buckets
		return n.StartTime() + (1<<n.PointWidth())*bstore.KFACTOR
	}
}

var ErrBadInsert = errors.New("Bad insert")

/**
 * This function is for inserting a large chunk of data. It is required
 * that the data is sorted, so we do that here
 */
func (tr *QTree) InsertValues(records []Record) (e error) {
	proc_records := make([]Record, len(records))
	idx := 0
	for _, v := range records {
		if math.IsInf(v.Val, 0) || math.IsNaN(v.Val) {
			lg.Debug("WARNING Got Inf/NaN insert value, dropping")
		} else if v.Time <= MinimumTime || v.Time >= MaximumTime {
			lg.Debug("WARNING Got time out of range, dropping")
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
		lg.Crashf("Root insert values: %v", err)
	}

	tr.root = n
	tr.gen.UpdateRootAddr(n.ThisAddr())
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
			lg.Crashf("Bad window <")
		}
		if records[len(records)-1].Time >= n.StartTime()+((1<<n.PointWidth())*bstore.KFACTOR) {
			lg.Debug("FE.")
			lg.Debug("Node window s=%v e=%v", n.StartTime(),
				n.StartTime()+((1<<n.PointWidth())*bstore.KFACTOR))
			lg.Debug("record time: %v", records[len(records)-1].Time)
			lg.Crashf("Bad window >=")
		}
	}
	if n.isLeaf {
		//lg.Debug("insertin values in leaf")
		if n.vector_block.Len+len(records) > bstore.VSIZE {
			//lg.Debug("need to convert leaf to a core");
			//lg.Debug("because %v + %v",n.vector_block.Len, len(records))
			//BUG(MPA) we waste a leaf allocation here if the leaf block was new...
			if n.PointWidth() == 0 {
				lg.Crashf("Overflowed 0 pw vector")
			}
			n = n.ConvertToCore(records)
			return n, nil
		} else {
			//lg.Debug("inserting %d records into pw(%v) vector", len(records),n.PointWidth())
			newn, err := n.AssertNewUpPatch()
			if err != nil {
				lg.Crashf("Uppatch failed: ", err)
			}
			n = newn
			n.MergeIntoVector(records)
			return n, nil
			//BUG(MPA) the len field should just be an int then
		}
	} else {
		//lg.Debug("inserting valus in core")
		//We are a core node
		newn, err := n.AssertNewUpPatch()
		if err != nil {
			lg.Crashf("ANUP: %v", err)
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
				newchild, err := n.wchild(lbuckt, idx-lidx < bstore.VSIZE).InsertValues(records[lidx:idx])
				if err != nil {
					lg.Crashf("%v", err)
				}
				n.SetChild(lbuckt, newchild) //This should set parent link too
				lidx = idx
				lbuckt = buckt
			}
		}
		//lg.Debug("reched end of records. flushing to child %v", buckt)
		newchild, err := n.wchild(lbuckt, (len(records)-lidx) < bstore.VSIZE).InsertValues(records[lidx:])
		//lg.Debug("Address of new child was %08x", newchild.ThisAddr())
		if err != nil {
			lg.Crashf("%v", err)
		}
		n.SetChild(lbuckt, newchild)

		return n, nil
	}
}

var ErrBadTimeRange error = errors.New("Invalid time range")

//start is inclusive, end is exclusive. To query a specific nanosecond, query (n, n+1)
func (tr *QTree) ReadStandardValuesCI(rv chan Record, err chan error, start int64, end int64) {
	tr.root.ReadStandardValuesCI(rv, err, start, end)
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
	tr.root.QueryStatisticalValues(rv, err, start, end, pw)
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
		}
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
	//lg.Debug("Free called on %p",n)
	//BUG(MPA) we really really don't want to do this on a write tree...
	if n.tr.gen != nil {
		lg.Crashf("Haven't fixed the free on write tree bug yet")
	}
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
		//lg.Debug("rsvci = leaf len(%v)", n.vector_block.Len)
		//Currently going under assumption that buckets are sorted
		//TODO replace with binary searches
		for i := 0; i < n.vector_block.Len; i++ {
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
		//lg.Debug("rsvci = core")

		//We are a core
		sbuck := uint16(0)
		if start > n.StartTime() {
			if start >= n.EndTime() {
				lg.Crashf("hmmm")
			}
			sbuck = n.ClampBucket(start)
		}
		ebuck := uint16(bstore.KFACTOR)
		if end < n.EndTime() {
			if end < n.StartTime() {
				lg.Crashf("hmm")
			}
			ebuck = n.ClampBucket(end) + 1
		}
		//lg.Debug("rsvci s/e %v/%v",sbuck, ebuck)
		for buck := sbuck; buck < ebuck; buck++ {
			//lg.Debug("walking over child %v", buck)
			c := n.Child(buck)
			if c != nil {
				//lg.Debug("child existed")
				//lg.Debug("rscvi descending from pw(%v) into [%v]", n.PointWidth(),buck)
				c.ReadStandardValuesCI(rv, err, start, end)
				c.Free()
				n.child_cache[buck] = nil
			} else {
				//lg.Debug("child was nil")
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
		lg.Debug("%sVECTOR <%v>", spacer, n.vector_block.Len)
		return
	}
	_ = n.Parent()
	pw := n.PointWidth()
	lg.Debug("%sCORE(%v)", spacer, pw)
	for i := 0; i < bstore.KFACTOR; i++ {
		if n.core_block.Addr[i] != 0 {
			c := n.Child(uint16(i))
			if c == nil {
				lg.Crashf("Nil child with addr %v", n.core_block.Addr[i])
			}
			lg.Debug("%s+ [%v] <%v>", spacer, i, n.core_block.Count[i])
			c.PrintCounts(indent + 2)
		}
	}
}
