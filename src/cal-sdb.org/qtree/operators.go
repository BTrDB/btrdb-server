package qtree

import (
	"cal-sdb.org/bstore"
)

func (n *QTreeNode) OpCountMean() (uint64, float64) {
	total := 0.0
	cnt := uint64(0)
	if n.isLeaf {
		for i:=0; i<n.vector_block.Len; i++ {
			total += n.vector_block.Value[i]
		}
		return uint64(n.vector_block.Len), total/float64(n.vector_block.Len)
	} else {
		for i:=0; i<bstore.KFACTOR; i++ {
			cnt += n.core_block.Count[i]
			total += n.core_block.Mean[i]*float64(cnt)
		}
		return cnt, total/float64(cnt)
	}
}

func (n *QTreeNode) OpMin() float64 {
	if n.isLeaf {
		min := n.vector_block.Value[0]
		for i:=0; i<n.vector_block.Len; i++ {
			if n.vector_block.Value[i] < min {
				min = n.vector_block.Value[i]
			}
		}
		return min
	} else {
		min := n.core_block.Min[0]
		for i:=0; i<len(n.core_block.Min); i++ {
			if n.core_block.Min[i] < min {
				min = n.core_block.Min[i]
			}
		}
		return min
	}
}

func (n *QTreeNode) OpMax() float64 {
		if n.isLeaf {
		max := n.vector_block.Value[0]
		for i:=0; i<n.vector_block.Len; i++ {
			if n.vector_block.Value[i] > max {
				max = n.vector_block.Value[i]
			}
		}
		return max
	} else {
		max := n.core_block.Max[0]
		for i:=0; i<len(n.core_block.Max); i++ {
			if n.core_block.Max[i] > max {
				max = n.core_block.Max[i]
			}
		}
		return max
	}
}