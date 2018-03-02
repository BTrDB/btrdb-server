package btrdb

import (
	"sort"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/qtree"
)

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
