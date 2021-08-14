// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package cephprovider

type CheckpointHeap []uint64

func (h CheckpointHeap) Len() int           { return len(h) }
func (h CheckpointHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h CheckpointHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *CheckpointHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(uint64))
}

func (h *CheckpointHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *CheckpointHeap) Peek() uint64 {
	return (*h)[0]
}
