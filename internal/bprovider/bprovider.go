package bprovider

//A blob provider implements a simple interface for storing blobs
//An address base gets locked in the form of a segment, and then an arbitrary number of
//blobs are written sequentially from that base, with each write call returning the address
//of the base of the next write. At the end, the segment is unlocked.
//For reading, the blob provider needs to work out its own framing, as it gets given
//a start address and must magically return the blob corresponding to that address
//The addresses have no special form*, other than being uint64s. It is up to the provider
//to encode whatever metadata it requires inside that uint64

//*I lied, addresses must not have the top byte as FF, those are reserved for relocation addresses

//In case it is not obvious, the challenge a bprovider faces is being able to hand out an address
//and support an arbitrary sized blob being written to that address. At the moment the max size of
//a blob can be determined by max(CBSIZE, VBSIZE) which is under 32k, but may be as little as 1k
//for well compressed blocks.

import (
	"errors"
)

var ErrNoSpace = errors.New("No more space")
var ErrInvalidArgument = errors.New("Invalid argument")
var ErrExists = errors.New("File exists")

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
	Write(uuid []byte, address uint64, data []byte) (uint64, error)

	//Block until all writes are complete. Note this does not imply a flush of the underlying files.
	Flush()
}
type StorageProvider interface {

	//Called at startup of a normal run
	Initialize(opts map[string]string)

	//Called to create the database for the first time
	//Note that initialize is not called before this function call
	//and you can assume the program will exit shortly after this
	//function call
	CreateDatabase(opts map[string]string) error

	// Lock a segment, or block until a segment can be locked
	// Returns a Segment struct
	LockSegment(uuid []byte) Segment

	// Read the blob into the given buffer
	Read(uuid []byte, address uint64, buffer []byte) []byte
}
