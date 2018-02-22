package jprovider

import (
	"context"

	"github.com/BTrDB/btrdb-server/bte"
	"github.com/BTrDB/btrdb-server/internal/configprovider"
)

type JournalMeta struct {
	cp Checkpoint
}
type Checkpoint uint64

type JournalIterator interface {
	Next() bool
	Value() (*JournalRecord, Checkpoint, bte.BTE)
}
type JournalProvider interface {
	//Used by a node that is inserting
	Insert(ctx context.Context, rng *configprovider.MashRange, jr *JournalRecord) (checkpoint Checkpoint, err bte.BTE)
	WaitForCheckpoint(ctx context.Context, checkpoint Checkpoint) bte.BTE

	//Used by a node taking control of a range
	//The context MUST be cancelled when done with the iterator
	//The iterator MUST return records in order
	ObtainNodeJournals(ctx context.Context, nodename string) (JournalIterator, bte.BTE)

	//Used by both the recovering nodes and the generating nodes
	//Given that the same journal can be processed by two different nodes
	//across different ranges, it is important that the provider only frees resources
	//associated with old checkpoints if they have been released across the entire range
	//of the journal. The checkpoint is INCLUSIVE.
	ReleaseJournalEntries(ctx context.Context, nodename string, upto Checkpoint, rng *configprovider.MashRange) bte.BTE

	//This is a niche operation and generally is only useful for tests. It ensures that any
	//new records are stored in a separate place from the old ones. A release journal entries
	//is only guaranteed to release space up to a barrier, the content after the barrier may
	//not be freed because it shares a file or object with other records after the upto
	//parameter
	Barrier(ctx context.Context, upto Checkpoint) bte.BTE

	//from is inclusive, upto is exclusive. The range is considered "this node" so the caller must ensure
	//that only appropriate checkpoints are released
	ReleaseDisjointCheckpoint(ctx context.Context, cp Checkpoint) bte.BTE

	//Get the current checkpoint for determining what an "old" checkpoint number is
	GetLatestCheckpoint() Checkpoint

	//If there is any information (such as journals) stored for this node name, forget it
	//It is not an error if it does not exist
	ForgetAboutNode(ctx context.Context, nodename string) bte.BTE
}
