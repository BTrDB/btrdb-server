package qtree

import (
	"cal-sdb.org/bstore"
	"errors"
	"log"
	"sort"
)

const KFACTOR = bstore.KFACTOR
const PWFACTOR = bstore.PWFACTOR
const ROOTPW = 56 //This makes each bucket at the root ~= 2.2 years
//so the root spans 146.23 years
const ROOTSTART = -1152921504606846976 //This makes the 16th bucket start at 1970 (0)
//but we still have support for dates < 1970
var ErrNoSuchStream = errors.New("No such stream")
var ErrNotLeafNode = errors.New("Not a leaf node")
var ErrImmutableTree = errors.New("Tree is immutable")

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

func (tr *QTree) Commit() {
	if tr.commited {
		log.Panic("Tree alredy comitted")
	}
	if tr.gen == nil {
		log.Panic("Commit on non-write-tree")
	}

	tr.gen.Commit()
	tr.commited = true
	tr.gen = nil
}

/**
 * Load a quasar tree
 */
func NewReadQTree(bs *bstore.BlockStore, uuid bstore.UUID, generation uint64) (*QTree, error) {
	sb := bs.LoadSuperblock(uuid, generation)
	if sb == nil {
		return nil, ErrNoSuchStream
	}
	if sb.Root() == 0 {
		log.Panic("Was expectin nonzero root tbh")
	}
	rv := &QTree{sb: sb, bs: bs}
	rt, err := rv.LoadNode(sb.Root())
	if err != nil {
		log.Panic(err)
		return nil, err
	}
	rv.root = rt
	return rv, nil
}

func NewWriteQTree(bs *bstore.BlockStore, uuid bstore.UUID) (*QTree, error) {
	gen := bs.ObtainGeneration(uuid)
	rv := &QTree{
		sb:  gen.New_SB,
		gen: gen,
		bs:  bs,
	}
	//If there is an existing root node, we need to load it so that it
	//has the correct values
	if rv.sb.Root() != 0 {
		log.Printf("loading root node: %v", rv.sb.Root())
		rt, err := rv.LoadNode(rv.sb.Root())
		if err != nil {
			log.Panic(err)
			return nil, err
		}
		rv.root = rt
	} else {
		rt, err := rv.NewCoreNode(ROOTSTART, ROOTPW)
		if err != nil {
			log.Panic(err)
			return nil, err
		}
		rv.root = rt
	}

	return rv, nil
}

type QTreeNode struct {
	tr           *QTree
	vector_block *bstore.Vectorblock
	core_block   *bstore.Coreblock
	isLeaf       bool
	child_cache  [KFACTOR]*QTreeNode
	parent       *QTreeNode
	isNew        bool
}

/*
func (n *QTreeNode) DirectTValue(i uint32) (int64, float64, error) {
	if !n.isLeaf {
		return -1, 0, ErrNotLeafNode
	}
	return n.vector_block.Time[i], n.vector_block.Value[i], nil
}
func (n *QTreeNode) DirectValue(i uint32) (float64, error) {
	if !n.isLeaf {
		return 0, ErrNotLeafNode
	}
	return n.vector_block.Value[i], nil
}
func (n *QTreeNode) DirectTime(i uint32) (int64, error) {
	if !n.isLeaf {
		return -1, ErrNotLeafNode
	}
	return n.vector_block.Time[i], nil
}
*/

func (n *QTreeNode) Child(i uint16) *QTreeNode {
	log.Printf("Child called on %p",n)
	if n.isLeaf {
		log.Panic("Child of leaf?")
	}
	if n.core_block.Addr[i] == 0 {
		return nil
	}
	if n.child_cache[i] != nil {
		log.Printf("cachc hi")
		return n.child_cache[i]
	}
	
	child, err := n.tr.LoadNode(n.core_block.Addr[i])
	if err != nil {
		log.Panic(err)
	}
	child.parent = n
	n.child_cache[i] = child
	return child
}

func (n *QTreeNode) ChildPW() uint8 {
	if n.PointWidth() <= PWFACTOR {
		return 0
	} else {
		return n.PointWidth() - PWFACTOR
	}
}

func (n *QTreeNode) ChildStartTime(idx uint16) int64 {
	return n.StartTime() + int64(idx)*(1<<n.ChildPW())
}

func (n *QTreeNode) ThisAddr() uint64 {
	if n.isLeaf {
		return n.vector_block.This_addr
	} else {
		return n.core_block.This_addr
	}
}

//Like Child() but creates the node if it doesn't exist
func (n *QTreeNode) wchild(i uint16) *QTreeNode {
	if n.isLeaf {
		log.Panic("Child of leaf?")
	}
	if n.tr.gen == nil {
		log.Panic("Cannot use WChild on read only tree")
	}
	if n.core_block.Addr[i] == 0 {
		log.Printf("No existingchild. Spawning")
		newn, err := n.tr.NewVectorNode(n.ChildStartTime(i), n.ChildPW())
		if err != nil {
			log.Panic(err)
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
		log.Panic(err)
	}
	child.parent = n
	n.child_cache[i] = child
	return child
}

func (n *QTreeNode) Parent() *QTreeNode {
	return n.parent
}

func (n *QTreeNode) SetChild(idx uint16, c *QTreeNode) {
	if n.tr.gen == nil {
		log.Panic("umm")
	}
	if n.isLeaf {
		log.Panic("umm")
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
	log.Printf("loading node@%08x", addr)
	db, err := tr.bs.ReadDatablock(addr)
	if err != nil {
		return nil, err
	}
	n := &QTreeNode{tr: tr}
	switch db.GetDatablockType() {
	case bstore.Vector:
		n.vector_block = db.(*bstore.Vectorblock)
		n.isLeaf = true
	case bstore.Core:
		n.core_block = db.(*bstore.Coreblock)
		n.isLeaf = false
	default:
		log.Panicf("What kind of type is this? %+v", db.GetDatablockType())
	}
	return n, nil
}

func ClampTime(t int64, pw uint8) int64 {
	if pw == 0 {
		return t
	}
	return t &^ ((1 << pw) - 1)
}

func (n *QTreeNode) ClampBucket(t int64) uint16 {
	if n.isLeaf {
		log.Panic("Not meant to use this on leaves")
	}
	if t < n.StartTime() {
		t = n.StartTime()
	}
	t -= n.StartTime()
	
	rv := (t >> n.PointWidth())
	if rv >= KFACTOR {
		rv = KFACTOR - 1
	}
	return uint16(rv)
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

func (n *QTreeNode) Clone() (*QTreeNode, error) {
	var rv *QTreeNode
	var err error
	if !n.isLeaf {
		rv, err := n.tr.NewCoreNode(n.StartTime(), n.PointWidth())
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
		//A leaf is a single bucket
		return n.StartTime() + (1 << n.PointWidth())
	} else {
		//A core node has multiple buckets
		return n.StartTime() + (1<<n.PointWidth())*KFACTOR
	}

}

/**
 * This function is for inserting a large chunk of data. It is required
 * that the data is sorted, so we do that here
 */
func (tr *QTree) InsertValues(records []Record) {
	sort.Sort(RecordSlice(records))
	n, err := tr.root.InsertValues(records)
	if err != nil {
		log.Panic(err)
	}
	tr.root = n
	tr.gen.UpdateRootAddr(n.ThisAddr())
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
	log.Printf("InsertValues called %v",records)
	//First determine if any of the records are outside our window
	//This is debugging, it won't even work if the records aren't sorted
	if !n.isLeaf {
		if records[0].Time < n.StartTime() {
			//Actually I don't think we can be less than the start.
			log.Panic("Bad window <")
		}
		if records[len(records)-1].Time >= n.StartTime()+((1<<n.PointWidth())*KFACTOR) {
			log.Panic("Bad window >=")
		}
	}
	if n.isLeaf {
		log.Printf("insertin values in leaf")
		if n.vector_block.Len+len(records) > bstore.VSIZE {
			if n.isNew {
				log.Panic("A new leaf should never overflow")
			}
			if n.PointWidth() == 0 {
				log.Panic("Overflowed 0 pw vector")
			}
			node, err := n.tr.NewCoreNode(n.StartTime(), n.PointWidth())
			if err != nil {
				return nil, err
			}
			node.parent = n.parent
			node.InsertValues(records)
			n.tr.bs.FreeVectorblock(&n.vector_block)
			return node, nil
		} else {
			if !n.isNew {
				var err error
				log.Printf("n before %p, %p",n, n.core_block)
				n, err = n.Clone()
				log.Printf("n after %p, %p",n, n.core_block)
				if err != nil {
					log.Panic(err)
				}
			}
			base := n.vector_block.Len
			for _, r := range records {
				n.vector_block.Time[base] = r.Time
				n.vector_block.Value[base] = r.Val
				base++
			}
			n.vector_block.Len = base
			return n, nil
			//BUG(MPA) the len field should just be an int then
		}
	} else {
		log.Printf("inserting valus in core")
		//We are a core node
		if !n.isNew {
			log.Printf("cloning core node")
			var err error
			n, err = n.Clone()
			if err != nil {
				log.Panic(err)
			}
		}
		lidx := 0
		lbuckt := n.ClampBucket(records[0].Time)
		for idx := 1; idx < len(records); idx++ {
			r := records[idx]
			log.Printf("iter: %v, %v", idx, r)
			buckt := n.ClampBucket(r.Time)
			if buckt != lbuckt {
				log.Printf("records spanningbucket. flushin")
				//Next bucket has started
				newchild, err := n.wchild(lbuckt).InsertValues(records[lidx:idx])
				if err != nil {
					log.Panic(err)
				}
				n.SetChild(lbuckt, newchild) //This should set parent link too
				lidx = idx
				lbuckt = buckt
			}
			if idx == len(records)-1 {
				log.Printf("reched end of records. flushing")
				newchild, err := n.wchild(buckt).InsertValues(records[lidx : idx+1])
				log.Printf("Address of new child was %08x", newchild.ThisAddr())
				if err != nil {
					log.Panic(err)
				}
				n.SetChild(lbuckt, newchild)
			}
		}
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

//Although we keep caches of datablocks in the bstore, we can't actually free them until
//they are unreferenced. This dropcache actually just makes sure they are unreferenced
func (n *QTreeNode) Free() {
	//TODO drop all children + return your own datablock to the bstore cache
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

func (n *QTreeNode) ReadStandardValuesCI(rv chan Record, err chan error, start int64, end int64) {
	if end <= start {
		err <- ErrBadTimeRange
		return
	}
	if n.isLeaf {
		log.Printf("rsvci is leaf")
		//Currently going under assumption that buckets are sorted
		//TODO replace with binary searches
		for i := 0; i < n.vector_block.Len; i++ {
			if n.vector_block.Time[i] >= start {
				if n.vector_block.Time[i] < end {
					rv <- Record{n.vector_block.Time[i], n.vector_block.Value[i]}
				} else {
					//Hitting a value past end means we are done with the query as a whole
					//we just need to clean up our memory now
					close(rv)
					return
				}
			}
		}
	} else {
		log.Printf("rsvci is core")
		
		//We are a core
		sbuck := uint16(0)
		if start > n.StartTime() {
			if start >= n.EndTime() {
				log.Panic("hmmm")
			}
			sbuck = n.ClampBucket(start)
		}
		ebuck := uint16(KFACTOR + 1)
		if end < n.EndTime() {
			if end < n.StartTime() {
				log.Panic("hmm")
			}
			ebuck = n.ClampBucket(end) + 1
		}
		log.Printf("rsvci s/e %v/%v",sbuck, ebuck)
		for buck := sbuck; buck < ebuck; buck++ {
			log.Printf("walking over child %v", buck)
			c := n.Child(buck)
			if c != nil {
				log.Printf("child existed")
				c.ReadStandardValuesCI(rv, err, start, end)
				c.Free()
			} else {
				log.Printf("child was nil")
			}
		}
	}
}
