package qtree

import (
	"cal-sdb.org/bstore"
	"errors"
	"log"
)

const KFACTOR = bstore.KFACTOR

var ErrNoSuchStream = errors.New("No such stream")
var ErrNotLeafNode = errors.New("Not a leaf node")

type QTree struct {
	sb   *bstore.Superblock
	bs   *bstore.BlockStore
	gen  uint64
	root *QTreeNode
}

/**
 * Load a quasar tree
 */
func LoadQTree(bs *bstore.BlockStore, uuid bstore.UUID, generation uint64) (*QTree, error) {
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

//Errors
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
	child := new(QTreeNode).load(n.tr, db)
	child.parent = n
	n.child_cache[i] = child
	return child
}

func (n *QTreeNode) Parent() *QTreeNode {
	return n.parent
}

func (n *QTreeNode) load(tr *QTree, db bstore.Datablock) *QTreeNode {
	n.tr = tr
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
	return n
}
