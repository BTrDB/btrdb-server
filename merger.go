// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package btrdb

import (
	"sort"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/qtree"
)

func mergeChangedRanges(parent chan ChangedRange, parentCE chan bte.BTE,
	pqbuffer []ChangedRange) (chan ChangedRange, chan bte.BTE) {

	var left, right *ChangedRange
	pqbufferIndex := 0
	//Fill left and right with an element
	refill := func() bool {
		found := false
		if left == nil {
			cr, ok := <-parent
			if ok {
				left = &cr
				found = true
			}
		}
		if right == nil {
			if pqbufferIndex < len(pqbuffer) {
				right = &pqbuffer[pqbufferIndex]
				pqbufferIndex += 1
				found = true
			}
		}
		return found || (left != nil) || right != nil
	}

	rv := make(chan ChangedRange, 100)
	rve := make(chan bte.BTE, 100)

	intersect := func(l *ChangedRange, r *ChangedRange) (bool, *ChangedRange) {
		if l.End < r.Start || r.End < l.Start {
			return false, nil
		}
		// They intersect
		minStart := l.Start
		maxEnd := l.End
		if r.Start < minStart {
			minStart = r.Start
		}
		if r.End > maxEnd {
			maxEnd = r.End
		}
		rv := &ChangedRange{minStart, maxEnd}
		return true, rv
	}

	go func() {
		var cur *ChangedRange
		for {
			//Check for errors
			select {
			case err := <-parentCE:
				rve <- err
				return
			default:
			}
			//Refill left and right
			more := refill()
			if !more {
				if cur != nil {
					rv <- *cur
					//end
					close(rv)
					return
				}
			}
			//If no current element, fill it
			if cur == nil {
				if left != nil && right != nil {
					//Select first from left/right
					if left.Start < right.Start {
						cur = left
						left = nil
					} else {
						cur = right
						right = nil
					}
				} else if left != nil {
					//Must be left
					cur = left
					left = nil
				} else {
					//Must be right
					cur = right
					right = nil
				}
				//Refill left and right
				continue
			}

			//We have a current
			if left != nil {
				bIs, Is := intersect(cur, left)
				if bIs {
					cur = Is
					left = nil
					continue
				}
			}
			if right != nil {
				bIs, Is := intersect(cur, right)
				if bIs {
					cur = Is
					right = nil
					continue
				}
			}

			//Current does not intersect
			rv <- *cur
			cur = nil
			continue
		}
	}() //end go()
	return rv, rve
}

func mergeStatisticalWindowChannels(parent chan qtree.StatRecord, parentCE chan bte.BTE, pqbuffer []qtree.StatRecord) (chan qtree.StatRecord,
	chan bte.BTE) {
	rvc := make(chan qtree.StatRecord, 1000)
	rve := make(chan bte.BTE, 2)
	var parentValue qtree.StatRecord
	var parentError bte.BTE
	var parentExhausted bool
	var hasCached bool
	peekparent := func() (qtree.StatRecord, bool, bte.BTE) {
		if hasCached {
			return parentValue, parentExhausted, parentError
		}
		var ok bool
		select {
		case parentValue, ok = <-parent:
			if !ok {
				parentExhausted = true
			}
		case parentError = <-parentCE:
		}
		hasCached = true
		return parentValue, parentExhausted, parentError
	}
	popparent := func() {
		if parentExhausted {
			return
		}
		hasCached = false
	}

	go func() {
		for {
			pv, fin, pe := peekparent()
			if pe != nil {
				rve <- pe
				return
			}
			if fin {
				//No more parent values, just return pqbuffer
				if len(pqbuffer) > 0 {
					rvc <- pqbuffer[0]
					pqbuffer = pqbuffer[1:]
					continue
				} else {
					close(rvc)
					return
				}
			}
			//There is a parent value
			if len(pqbuffer) == 0 || pv.Time < pqbuffer[0].Time {
				//return parent unmodified
				rvc <- pv
				popparent()
			} else if pqbuffer[0].Time < pv.Time {
				//return pq unmodified
				rvc <- pqbuffer[0]
				pqbuffer = pqbuffer[1:]
			} else if pqbuffer[0].Time == pv.Time {
				//Need to merge
				v := pqbuffer[0]
				pqbuffer = pqbuffer[1:]
				if v.Count == 0 {
					v.Max = pv.Max
					v.Min = pv.Min
				}
				if pv.Max > v.Max {
					v.Max = pv.Max
				}
				if pv.Min < v.Min {
					v.Min = pv.Min
				}
				v.Mean = (v.Mean*float64(v.Count) + pv.Mean*float64(pv.Count)) / float64((v.Count + pv.Count))
				v.Count += pv.Count
				rvc <- v
				popparent()
			} else {
				panic("PQM done messed up")
			}
		}
	}()

	return rvc, rve
}

//For all points in rz >= tCutoffStart and < tEnd, align into windows of width w starting from tStart
//END IS INCLUSIVE, so you need to subtract one for unaligned windows
func CreateStatWindows(rz []qtree.Record, tCutoffStart int64, tStart int64, tEnd int64, w uint64) []qtree.StatRecord {
	wz := make(map[int64]qtree.StatRecord)
	for _, r := range rz {
		if r.Time < tCutoffStart {
			continue
		}
		if r.Time > tEnd {
			continue
		}
		windowIdx := (r.Time - tStart) / int64(w)
		ex := wz[windowIdx]
		ex.Time = windowIdx*int64(w) + tStart
		if ex.Count == 0 {
			ex.Min = r.Val
			ex.Max = r.Val
			ex.Mean = r.Val
			ex.Count = 1
		} else {
			if r.Val < ex.Min {
				ex.Min = r.Val
			}
			if r.Val > ex.Max {
				ex.Max = r.Val
			}
			ex.Mean = (ex.Mean*float64(ex.Count) + r.Val) / float64(ex.Count+1)
			ex.Count++
		}
		wz[windowIdx] = ex
	}
	rv := make([]qtree.StatRecord, 0, len(wz))
	for _, sr := range wz {
		rv = append(rv, sr)
	}
	sort.Sort(StatRecordSlice(rv))
	return rv
}

type StatRecordSlice []qtree.StatRecord

func (srs StatRecordSlice) Len() int {
	return len(srs)
}

func (srs StatRecordSlice) Less(i, j int) bool {
	return srs[i].Time < srs[j].Time
}
func (srs StatRecordSlice) Swap(i, j int) {
	srs[i], srs[j] = srs[j], srs[i]
}
