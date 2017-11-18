package qtree

import (
	"math"

	"github.com/BTrDB/btrdb-server/internal/bstore"
)

func (n *QTreeNode) OpCountMean() (uint64, float64) {
	total := 0.0
	cnt := uint64(0)
	if n.isLeaf {
		for i := 0; i < int(n.vector_block.Len); i++ {
			total += n.vector_block.Value[i]
		}
		return uint64(n.vector_block.Len), total / float64(n.vector_block.Len)
	} else {
		for i := 0; i < bstore.KFACTOR; i++ {
			if n.core_block.Count[i] == 0 {
				continue
			}
			cnt += n.core_block.Count[i]
			total += n.core_block.Mean[i] * float64(n.core_block.Count[i])
		}
		return cnt, total / float64(cnt)
	}
}

func (n *QTreeNode) OpMin() float64 {
	if n.isLeaf {
		min := n.vector_block.Value[0]
		for i := 0; i < int(n.vector_block.Len); i++ {
			if n.vector_block.Value[i] < min {
				min = n.vector_block.Value[i]
			}
		}
		return min
	} else {
		min := float64(0)
		minset := false
		for i := 0; i < len(n.core_block.Min); i++ {
			if n.core_block.Count[i] == 0 {
				continue
			}
			if !minset || n.core_block.Min[i] < min {
				min = n.core_block.Min[i]
				minset = true
			}
		}
		return min
	}
}

func (n *QTreeNode) OpMax() float64 {
	if n.isLeaf {
		max := n.vector_block.Value[0]
		for i := 0; i < int(n.vector_block.Len); i++ {
			if n.vector_block.Value[i] > max {
				max = n.vector_block.Value[i]
			}
		}
		return max
	} else {
		max := float64(0)
		maxset := false
		for i := 0; i < len(n.core_block.Max); i++ {
			if n.core_block.Count[i] == 0 {
				continue
			}
			if !maxset || n.core_block.Max[i] > max {
				max = n.core_block.Max[i]
				maxset = true
			}
		}
		return max
	}
}

/*

ok so here is the problem. If we call opreduce on a core node, then we can only deliver
pointwidths GREATER than our pointwidth and less than pointwidth + 6 right?
but as a leaf we can potentially deliver pointwidths down to 0...
*/
func (n *QTreeNode) OpReduce(pointwidth uint8, index uint64) (uint64, float64, float64, float64) {
	if !n.isLeaf && pointwidth < n.PointWidth() {
		lg.Panic("Bad pointwidth for core. See code comment")
	}
	if pointwidth > n.PointWidth()+PWFACTOR {
		lg.Panic("Can't guarantee this PW")
	}
	maxpw := n.PointWidth() + PWFACTOR
	pwdelta := pointwidth - n.PointWidth()
	width := int64(1) << pointwidth
	maxidx := 1 << (maxpw - pointwidth)
	if maxidx <= 0 || index >= uint64(maxidx) {
		lg.Critical("node is %s", n.TreePath())
		lg.Panic("bad index", maxidx, index)
	}
	sum := 0.0
	min := math.NaN()
	max := math.NaN()
	minset := false
	maxset := false
	count := uint64(0)
	if n.isLeaf {
		st := n.StartTime() + int64(index)*width
		et := st + width
		if n.vector_block.Len != 0 {
			for i := 0; i < int(n.vector_block.Len); i++ {
				if n.vector_block.Time[i] < st {
					continue
				}
				if n.vector_block.Time[i] >= et {
					break
				}
				v := n.vector_block.Value[i]
				sum += v
				if !minset || v < min {
					minset = true
					min = v
				}
				if !maxset || v > max {
					maxset = true
					max = v
				}
				count++
			}
		}
		return count, min, sum / float64(count), max
	} else {
		s := index << pwdelta
		e := (index + 1) << pwdelta
		for i := s; i < e; i++ {
			if n.core_block.Count[i] == 0 {
				continue
			}
			count += n.core_block.Count[i]
			sum += n.core_block.Mean[i] * float64(n.core_block.Count[i])
			if !minset || n.core_block.Min[i] < min {
				minset = true
				min = n.core_block.Min[i]
			}
			if !maxset || n.core_block.Max[i] > max {
				maxset = true
				max = n.core_block.Max[i]
			}
		}
		mean := sum / float64(count)
		return count, min, mean, max
	}
}
