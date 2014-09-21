package qtree

import (
	"code.google.com/p/go-uuid/uuid"
	lg "code.google.com/p/log4go"
	"fmt"
	bstore "github.com/SoftwareDefinedBuildings/quasar/bstoreGen1"
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

type QTreeNode struct {
	tr           *QTree
	vector_block *bstore.Vectorblock
	core_block   *bstore.Coreblock
	isLeaf       bool
	child_cache  [bstore.KFACTOR]*QTreeNode
	parent       *QTreeNode
	isNew        bool
}

type RecordSlice []Record

type ChangedRange struct {
	Valid bool
	Start int64
	End   int64
}

func (s RecordSlice) Len() int {
	return len(s)
}

func (s RecordSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s RecordSlice) Less(i, j int) bool {
	return s[i].Time < s[j].Time
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

func (n *QTree) FindNearestValue(time int64, backwards bool) (Record, error) {
	if n.root == nil {
		return Record{}, ErrNoSuchPoint
	}
	return n.root.FindNearestValue(time, backwards)
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

/**
 * Load a quasar tree
 */
func NewReadQTree(bs *bstore.BlockStore, id uuid.UUID, generation uint64) (*QTree, error) {
	sb := bs.LoadSuperblock(id, generation)
	if sb == nil {
		return nil, ErrNoSuchStream
	}
	rv := &QTree{sb: sb, bs: bs}
	if sb.Root() != 0 {
		rt, err := rv.LoadNode(sb.Root())
		if err != nil {
			lg.Crashf("%v", err)
			return nil, err
		}
		//lg.Debug("The start time for the root is %v",rt.StartTime())
		rv.root = rt
	}
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

func (n *QTreeNode) Generation() uint64 {
	if n.isLeaf {
		return n.vector_block.Generation
	} else {
		return n.core_block.Generation
	}
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

func (n *QTreeNode) ArbitraryStartTime(idx uint64, pw uint8) int64 {
	return n.StartTime() + int64(idx*(1<<pw))
}

func (n *QTreeNode) ChildPW() uint8 {
	if n.PointWidth() <= PWFACTOR {
		return 0
	} else {
		return n.PointWidth() - PWFACTOR
	}
}

func (n *QTreeNode) ChildStartTime(idx uint16) int64 {
	return n.ArbitraryStartTime(uint64(idx), n.PointWidth())
}

func (n *QTreeNode) ChildEndTime(idx uint16) int64 {
	return n.ArbitraryStartTime(uint64(idx+1), n.PointWidth()) - 1
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

func (n *QTreeNode) Parent() *QTreeNode {
	return n.parent
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

func (n *QTreeNode) ThisAddr() uint64 {
	if n.isLeaf {
		return n.vector_block.This_addr
	} else {
		return n.core_block.This_addr
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

func ClampTime(t int64, pw uint8) int64 {
	if pw == 0 {
		return t
	}
	//Protip... &^ is bitwise and not in golang... not XOR
	return t &^ ((1 << pw) - 1)

}
