package grpcinterface

import (
	"fmt"
	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/qtree"
)

type genericStream interface {
	getTime(i int) int64
	isOpen(i int) bool
	readPoint(i int) (bool, bte.BTE)
	writePoint(i int, row []string)
	writeEmptyPoint(i int, row []string)
	getHeaderRow(configs []*StreamCSVConfig, includeVersions bool) []string
	calculateColumnOffset(i int) int
}

type statBufferEntry struct {
	stac chan qtree.StatRecord
	errc chan bte.BTE
	ver  uint64
	pt   qtree.StatRecord
	open bool
}

type rawBufferEntry struct {
	rawc chan qtree.Record
	errc chan bte.BTE
	ver  uint64
	pt   qtree.Record
	open bool
}

type rawBuffer []rawBufferEntry

type statBuffer []statBufferEntry

func setTimeHeaders(row []string) {
	row[0] = "Timestamp (ns)"
	row[1] = "Human-Readable Time (UTC)"
}

func (sb statBuffer) calculateColumnOffset(i int) int {
	// Two columns for time, 4 for each point
	return 2 + (i * 4)
}

func (sb statBuffer) getTime(i int) int64 {
	return sb[i].pt.Time
}

func (sb statBuffer) isOpen(i int) bool {
	return sb[i].open
}

func (sb statBuffer) readPoint(i int) (bool, bte.BTE) {
	select {
	case pt, open := <-sb[i].stac:
		sb[i].pt = pt
		sb[i].open = open
		return open, nil

	case err := <-sb[i].errc:
		return false, err
	}
}

func (sb statBuffer) writePoint(i int, row []string) {
	offset := sb.calculateColumnOffset(i)
	row[offset] = fmt.Sprintf("%f", sb[i].pt.Min)
	row[offset+1] = fmt.Sprintf("%f", sb[i].pt.Mean)
	row[offset+2] = fmt.Sprintf("%f", sb[i].pt.Max)
	row[offset+3] = fmt.Sprintf("%d", sb[i].pt.Count)
}

func (sb statBuffer) writeEmptyPoint(i int, row []string) {
	offset := sb.calculateColumnOffset(i)
	row[offset] = ""
	row[offset+1] = ""
	row[offset+2] = ""
	row[offset+3] = ""
}

func (sb statBuffer) getHeaderRow(configs []*StreamCSVConfig, includeVersions bool) []string {
	numcols := sb.calculateColumnOffset(len(sb))
	row := make([]string, numcols, numcols)
	setTimeHeaders(row)
	versionStr := ""
	for i, config := range configs {
		offset := 2 + (i * 4)
		if includeVersions {
			versionStr = fmt.Sprintf(", ver. %d", sb[i].ver)
		}
		row[offset] = fmt.Sprintf("%s%s (Min)", config.Label, versionStr)
		row[offset+1] = fmt.Sprintf("%s%s (Mean)", config.Label, versionStr)
		row[offset+2] = fmt.Sprintf("%s%s (Max)", config.Label, versionStr)
		row[offset+3] = fmt.Sprintf("%s%s (Count)", config.Label, versionStr)
	}
	return row
}

func (rb rawBuffer) calculateColumnOffset(i int) int {
	return 2 + i
}

func (rb rawBuffer) getTime(i int) int64 {
	return rb[i].pt.Time
}

func (rb rawBuffer) isOpen(i int) bool {
	return rb[i].open
}

func (rb rawBuffer) readPoint(i int) (bool, bte.BTE) {
	select {
	case pt, open := <-rb[i].rawc:
		rb[i].pt = pt
		rb[i].open = open
		return open, nil

	case err := <-rb[i].errc:
		return false, err
	}
}

func (rb rawBuffer) writePoint(i int, row []string) {
	offset := rb.calculateColumnOffset(i)
	row[offset] = fmt.Sprintf("%f", rb[i].pt.Val)
}

func (rb rawBuffer) writeEmptyPoint(i int, row []string) {
	offset := rb.calculateColumnOffset(i)
	row[offset] = ""
}

func (rb rawBuffer) getHeaderRow(configs []*StreamCSVConfig, includeVersions bool) []string {
	numcols := rb.calculateColumnOffset(len(rb))
	row := make([]string, numcols, numcols)
	setTimeHeaders(row)
	versionStr := ""
	for i, config := range configs {
		offset := 2 + i
		if includeVersions {
			versionStr = fmt.Sprintf(", ver. %d", rb[i].ver)
		}
		row[offset] = fmt.Sprintf("%s%s", config.Label, versionStr)
	}
	return row
}
