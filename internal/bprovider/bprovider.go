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

	"github.com/SoftwareDefinedBuildings/btrdb/internal/configprovider"
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

type Stream interface {
	//The UUID of the stream
	UUID() []byte
	//The collection name of the stream
	Collection() string
	//The stream's tags
	Tags() map[string]string
}

type StorageProvider interface {

	//Called at startup of a normal run
	Initialize(configprovider.Configuration)

	//Called to create the database for the first time
	//Note that initialize is not called before this function call
	//and you can assume the program will exit shortly after this
	//function call
	CreateDatabase(configprovider.Configuration) error

	// Lock a segment, or block until a segment can be locked
	// Returns a Segment struct
	LockSegment(uuid []byte) Segment

	// Read the blob into the given buffer
	Read(uuid []byte, address uint64, buffer []byte) []byte

	// Read the given version of superblock into the buffer.
	ReadSuperBlock(uuid []byte, version uint64, buffer []byte) []byte

	// Writes a superblock of the given version
	// TODO I think the storage will need to chunk this, because sb logs of gigabytes are possible
	WriteSuperBlock(uuid []byte, version uint64, buffer []byte)

	// Sets the version of a stream. If it is in the past, it is essentially a rollback,
	// and although no space is freed, the consecutive version numbers can be reused
	// note to self: you must make sure not to call ReadSuperBlock on versions higher
	// than you get from GetStreamVersion because they might succeed
	SetStreamVersion(uuid []byte, version uint64)

	// Gets the version of a stream. Returns 0 if none exists.
	GetStreamInfo(uuid []byte) (Stream, uint64)

	// CreateStream makes a stream with the given uuid, collection and tags. Returns
	// an error if the uuid already exists.
	CreateStream(uuid []byte, collection string, tags map[string]string) error

	// ListCollections returns a list of collections beginning with prefix (which may be "")
	// and starting from the given string. If number is > 0, only that many results
	// will be returned. More can be obtained by re-calling ListCollections with
	// a given startingFrom and number.
	ListCollections(prefix string, startingFrom string, number int64) ([]string, error)

	// ListStreams lists all the streams within a collection. If tags are specified
	// then streams are only returned if they have that tag, and the value equals
	// the value passed. If partial is false, zero or one streams will be returned.
	ListStreams(collection string, partial bool, tags map[string]string) ([]Stream, error)
}
