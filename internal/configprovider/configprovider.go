package configprovider

type Configuration interface {
	ClusterEnabled() bool
	ClusterPrefix() string
	ClusterEtcdEndpoints() []string
	StorageCephConf() string
	StorageFilepath() string
	StorageCephDataPool() string
	StorageCephHotPool() string
	HttpEnabled() bool
	HttpPort() int
	HttpAddress() string
	CapnpEnabled() bool
	CapnpPort() int
	CapnpAddress() string
	BlockCache() int
	RadosReadCache() int
	RadosWriteCache() int

	// Note that these are "live" and called in the hotpath, so buffer them
	CoalesceMaxPoints() int
	CoalesceMaxInterval() int

	MongoServer() string
	MongoCollection() string
}

type ClusterConfiguration interface {
}
