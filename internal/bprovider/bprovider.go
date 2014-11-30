package bprovider

import (
	"errors"
)
var ErrNoSpace 		 = errors.New("No more space")
var ErrInvalidArgument = errors.New("Invalid argument")

type Segment interface {
	//Returns the address of the first free word in the segment when it was locked
	BaseAddress() uint64
	
	//Unlocks the segment for the StorageProvider to give to other consumers
	//Implies a flush
	Unlock()
	
	//Writes a slice to the segment, returns immediately
	//Returns nil if op is OK, otherwise ErrNoSpace or ErrInvalidArgument
	//It is up to the implementer to work out how to report no space immediately
	//The uint64 is the address to be used for the next write
	Write(address uint64, data []byte) (uint64, error)
	
	//Block until all writes are complete
	Flush()
}
type StorageProvider interface {
	
	//Called at startup
	Initialize(opts map[string]string)
	
	// Lock a segment, or block until a segment can be locked
	// Returns a Segment struct
	LockSegment() Segment
}