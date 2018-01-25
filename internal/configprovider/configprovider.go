package configprovider

import etcd "github.com/coreos/etcd/clientv3"

type Configuration interface {
	ClusterEnabled() bool
	ClusterPrefix() string
	ClusterEtcdEndpoints() []string
	StorageCephConf() string
	StorageFilepath() string
	StorageCephDataPool() string
	StorageCephHotPool() string
	HttpEnabled() bool
	HttpListen() string
	HttpAdvertise() []string
	GRPCEnabled() bool
	GRPCListen() string
	GRPCAdvertise() []string
	BlockCache() int
	RadosReadCache() int
	RadosWriteCache() int

	// Note that these are "live" and called in the hotpath, so buffer them
	CoalesceMaxPoints() int
	CoalesceMaxInterval() int
}

type ClusterConfiguration interface {
	// Returns true if we hold the write lock for the given uuid. Returns false
	// if we do not have the write lock, or we are trying to get rid of the write
	// lock
	NodeName() string
	WeHoldWriteLockFor(uuid []byte) bool

	OurRanges() (active MashRange, proposed MashRange)
	WatchMASHChange(w func(flushComplete chan struct{}, activeRange MashRange, proposedRange MashRange))

	PeerHTTPAdvertise(nodename string) ([]string, error)
	PeerGRPCAdvertise(nodename string) ([]string, error)
	GetCachedClusterState() *ClusterState
	//	MASHNumber() int64
	// Called when the node knows it is faulty (generally pre-panic). This
	// removes the delay that would normally accompany the lease expiry
	Fault(fz string, args ...interface{})

	// Get a tunable, and watch it for all future changes. Only meant to be
	// used by Rez
	WatchTunable(name string, onchange func(v string)) error

	GetEtcdClient() *etcd.Client

	BeginClusterDaemons()
}

// have some buffers
// write lock changes
//   notify mash watchers and give them channel
//   they close the channel when they all their buffers not in the new map (WeHoldWriteLockFor) are flushed
// cman increments mash number for node
