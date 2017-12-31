package cephprovider

import "github.com/BTrDB/btrdb-server/internal/jprovider"

//go:generate msgp -tests=false

type CJrecord struct {
	R *jprovider.JournalRecord
	C uint64
}
