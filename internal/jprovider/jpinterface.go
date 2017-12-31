package jprovider

import (
	"context"

	"github.com/BTrDB/btrdb-server/internal/configprovider"
)

type JournalMeta struct {
	cp Checkpoint
}
type Checkpoint uint64

type JournalIterator interface {
	Next() bool
	Value() (*JournalRecord, Checkpoint, error)
}
type JournalProvider interface {
	//Used by a node that is inserting
	Insert(ctx context.Context, rng *configprovider.MashRange, jr *JournalRecord) (checkpoint Checkpoint, err error)
	WaitForCheckpoint(ctx context.Context, checkpoint Checkpoint) error

	//Used by a node taking control of a range
	//The context MUST be cancelled when done with the iterator
	//The iterator MUST return records in order
	ObtainNodeJournals(ctx context.Context, nodename string) (JournalIterator, error)

	//Used by both the recovering nodes and the generating nodes
	//Given that the same journal can be processed by two different nodes
	//across different ranges, it is important that the provider only frees resources
	//associated with old checkpoints if they have been released across the entire range
	//of the journal. The checkpoint is INCLUSIVE.
	ReleaseJournalEntries(ctx context.Context, nodename string, upto Checkpoint, rng *configprovider.MashRange) error

	//If there is any information (such as journals) stored for this node name, forget it
	//It is not an error if it does not exist
	ForgetAboutNode(ctx context.Context, nodename string) error
}
