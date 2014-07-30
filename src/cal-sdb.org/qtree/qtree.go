package qtree

import (
	"cal-sdb.org/bstore"
	"errors"
	"log"
	"sort"
)

const KFACTOR = bstore.KFACTOR
const PWFACTOR = bstore.PWFACTOR

var ErrNoSuchStream = errors.New("No such stream")
var ErrNotLeafNode = errors.New("Not a leaf node")
var ErrImmutableTree = errors.New("Tree is immutable")

type QTree struct {
	sb   *bstore.Superblock
	bs   *bstore.BlockStore
	gen  *bstore.Generation
	root *QTreeNode
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

/**
 * Load a quasar tree
 */
func NewQTree(bs *bstore.BlockStore, uuid bstore.UUID, generation uint64) (*QTree, error) {
	sb := bs.LoadSuperblock(uuid, generation)
	if sb == nil {
		return nil, ErrNoSuchStream
	}
	return &QTree{
		sb: sb,
	}, nil
}

type QTreeNode struct {
	tr           *QTree
	vector_block *bstore.Vectorblock
	core_block   *bstore.Coreblock
	isLeaf       bool
	child_cache  [KFACTOR]*QTreeNode
	parent       *QTreeNode
	startTime    int64
	pointWidth   uint8
	isNew        bool
}

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

func (n *QTreeNode) Child(i uint16) *QTreeNode {
	if n.isLeaf {
		log.Panic("Child of leaf?")
	}
	if n.core_block.Addr[i] == 0 {
		return nil
	}
	if n.child_cache[i] != nil {
		return n.child_cache[i]
	}
	db, err := n.tr.bs.ReadDatablock(n.core_block.Addr[i])
	if err != nil {
		log.Panic(err)
	}
	child := LoadQTreeNode(n.tr, db)
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

func LoadQTreeNode(tr *QTree, db bstore.Datablock) *QTreeNode {
	n := &QTreeNode{tr: tr}
	switch db.GetDatablockType() {
	case bstore.Vector:
		n.vector_block = db.(*bstore.Vectorblock)
		n.isLeaf = true
		n.startTime = n.vector_block.Time[0]
		n.pointWidth = n.vector_block.PointWidth
	case bstore.Core:
		n.core_block = db.(*bstore.Coreblock)
		n.isLeaf = false
		n.startTime = n.core_block.StartTime
		n.pointWidth = n.core_block.PointWidth
	default:
		log.Panicf("What kind of type is this? %+v", db.GetDatablockType())
	}
	return n
}

func ClampTime(t int64, pw uint8) int64 {
	if pw == 0 {
		return t
	}
	return t &^ ((1 << pw) - 1)
}
func NewCoreQTreeNode(tr *QTree, startTime int64, pointWidth uint8) (*QTreeNode, error) {
	if tr.gen == nil {
		return nil, ErrImmutableTree
	}
	cb, err := tr.gen.AllocateCoreblock()
	if err != nil {
		return nil, err
	}
	cb.PointWidth = pointWidth
	startTime = ClampTime(startTime, pointWidth)
	rv := &QTreeNode{
		core_block: cb,
		tr:         tr,
		startTime:  startTime,
		pointWidth: pointWidth,
		isNew:      true,
	}
	return rv, nil
}

func NewVectorQTreeNode(tr *QTree, startTime int64, pointWidth uint8) (*QTreeNode, error) {
	if tr.gen == nil {
		return nil, ErrImmutableTree
	}
	vb, err := tr.gen.AllocateVectorblock()
	if err != nil {
		return nil, err
	}
	vb.PointWidth = pointWidth
	startTime = ClampTime(startTime, pointWidth)
	rv := &QTreeNode{
		vector_block: vb,
		tr:           tr,
		isLeaf:       true,
		startTime:    startTime,
		pointWidth:   pointWidth,
		isNew:        true,
	}
	return rv, nil
}

func (n *QTreeNode) Clone() (*QTreeNode, error) {
	var rv *QTreeNode
	var err error
	if n.isLeaf {
		rv, err := NewCoreQTreeNode(n.tr, n.startTime, n.pointWidth)
		if err != nil {
			return nil, err
		}
		n.core_block.CopyInto(rv.core_block)
	} else {
		rv, err = NewVectorQTreeNode(n.tr, n.startTime, n.pointWidth)
		if err != nil {
			return nil, err
		}
		n.vector_block.CopyInto(rv.vector_block)
	}
	return rv, nil
}

/**
 * This function is for inserting a large chunk of data. It is required
 * that the data is sorted, so we do that here
 */
func (n *QTree) InsertValues(records []Record) {
	sort.Sort(RecordSlice(records))
	n.root.InsertValues(records)
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
	//First determine if any of the records are outside our window
	//This is debugging
	if !n.isLeaf {
		if records[0].Time < n.startTime {
			//Actually I don't think we can be less than the start.
			log.Panic("Bad window <")
		}
		if records[len(records)-1].Time >= n.startTime+(1<<n.pointWidth) {
			log.Panic("Bad window >=")
		}
	}
	if n.isLeaf {
		if n.vector_block.Len+len(records) > bstore.VSIZE {
			if n.isNew {
				log.Panic("A new leaf should never overflow")
			}
			if n.pointWidth == 0 {
				log.Panic("Overflowed 0 pw vector")
			}
			node, err := NewCoreQTreeNode(n.tr, n.startTime, n.pointWidth)
			if err != nil {
				return nil, err
			}
			node.parent = n.parent
			node.InsertValues(records)
			n.tr.bs.FreeVectorblock(n.vector_block)
			n.vector_block = nil
			return node, nil
		} else {
			if !n.isNew {
				var err error
				n, err = n.Clone()
				if err != nil {
					log.Panic(err)
				}
			}
			base := n.vector_block.Len
			for _, r := range(records) {
				n.vector_block.Time[base] = r.Time
				n.vector_block.Value[base] = r.Val
				base++
			}
			n.vector_block.Len = base
			return n, nil
			//BUG(MPA) the len field should just be an int then
		}
	} else {
		//We are a core node
		if !n.isNew {
			var err error
			n, err = n.Clone()
			if err != nil {
				log.Panic(err)
			}
		}
		lidx := 0
		lbuckt := uint16((records[0].Time >> n.pointWidth) & ((1 << PWFACTOR) - 1))
		for idx, r := range records[1:] {
			buckt := uint16((r.Time >> n.pointWidth) & ((1 << PWFACTOR) - 1))
			if buckt != lbuckt {
				//Next bucket has started
				newchild, err := n.Child(lbuckt).InsertValues(records[lidx:idx])
				if err != nil {
					log.Panic(err)
				}
				n.SetChild(lbuckt, newchild) //This should set parent link too
				lidx = idx
				lbuckt = buckt
			}
			if idx == len(records)-1 {
				newchild, err := n.Child(buckt).InsertValues(records[lidx : idx+1])
				if err != nil {
					log.Panic(err)
				}
				n.SetChild(lbuckt, newchild)
			}
		}
		return n, nil
	}
}
