package configprovider

import gcfg "gopkg.in/gcfg.v1"

type FileConfig struct {
	Cluster struct {
		Prefix       string
		EtcdEndpoint []string
		Enabled      bool
	}
	Http struct {
		Port    int
		Address string
		Enabled bool
	}
	Capnp struct {
		Port    int
		Address string
		Enabled bool
	}
	Mongo struct {
		Server     string
		Collection string
	}
	Storage struct {
		Filepath     string
		CephDataPool string
		CephHotPool  string
		CephConf     string
	}
	Cache struct {
		BlockCache      int
		RadosWriteCache int
		RadosReadCache  int
	}
	Debug struct {
		Cpuprofile  bool
		Heapprofile bool
	}
	Coalescence struct {
		MaxPoints int
		Interval  int
	}
}

func LoadFileConfig(path string) (Configuration, error) {
	cfg := &FileConfig{}
	err := gcfg.ReadFileInto(cfg, path)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *FileConfig) ClusterEnabled() bool {
	return c.Cluster.Enabled
}
func (c *FileConfig) ClusterPrefix() string {
	return c.Cluster.Prefix
}
func (c *FileConfig) ClusterEtcdEndpoints() []string {
	return c.Cluster.EtcdEndpoint
}
func (c *FileConfig) StorageCephConf() string {
	return c.Storage.CephConf
}
func (c *FileConfig) StorageFilepath() string {
	return c.Storage.Filepath
}
func (c *FileConfig) StorageCephDataPool() string {
	return c.Storage.CephDataPool
}
func (c *FileConfig) StorageCephHotPool() string {
	return c.Storage.CephHotPool
}
func (c *FileConfig) HttpEnabled() bool {
	return c.Http.Enabled
}
func (c *FileConfig) HttpPort() int {
	return c.Http.Port
}
func (c *FileConfig) HttpAddress() string {
	return c.Http.Address
}
func (c *FileConfig) CapnpEnabled() bool {
	return c.Capnp.Enabled
}
func (c *FileConfig) CapnpPort() int {
	return c.Capnp.Port
}
func (c *FileConfig) CapnpAddress() string {
	return c.Capnp.Address
}
func (c *FileConfig) BlockCache() int {
	return c.Cache.BlockCache
}
func (c *FileConfig) RadosReadCache() int {
	return c.Cache.RadosReadCache
}
func (c *FileConfig) RadosWriteCache() int {
	return c.Cache.RadosWriteCache
}
func (c *FileConfig) CoalesceMaxPoints() int {
	return c.Coalescence.MaxPoints
}
func (c *FileConfig) CoalesceMaxInterval() int {
	return c.Coalescence.Interval
}
func (c *FileConfig) MongoServer() string {
	return c.Mongo.Server
}
func (c *FileConfig) MongoCollection() string {
	return c.Mongo.Collection
}
