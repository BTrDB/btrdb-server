package fileprovider

import (
	"github.com/SoftwareDefinedBuildings/quasar/internal/bprovider"
)

type FileProviderSegment struct {
	//Returns the address of the first free word in the segment when it was locked
	func (seg *FileProviderSegment) BaseAddress() uint64 {
		
	}
	
	//Unlocks the segment for the StorageProvider to give to other consumers
	//Implies a flush
	func (seg *FileProviderSegment) Unlock() {
		
	}
	
	//Writes a slice to the segment, returns immediately
	//Returns nil if op is OK, otherwise ErrNoSpace or ErrInvalidArgument
	//It is up to the implementer to work out how to report no space immediately
	//The uint64 is the address to be used for the next write
	func (seg *FileProviderSegment) Write(address uint64, data []byte) (uint64, error) {
		
	}
	
	//Block until all writes are complete
	func (seg *FileProviderSegment) Flush() {
		
	}
}
type FileStorageProvider struct {
	
	//Called at startup
	func (sp *FileStorageProvider) Initialize(opts map[string]string) {
		
	}
	
	// Lock a segment, or block until a segment can be locked
	// Returns a Segment struct
	func (sp *FileStorageProvider) LockSegment() Segment {
		
	}
}