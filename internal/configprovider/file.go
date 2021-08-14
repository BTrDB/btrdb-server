// Copyright (c) 2021 Michael Andersen
// Copyright (c) 2021 Regents of the University Of California
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

package configprovider

import (
	"strings"

	gcfg "gopkg.in/gcfg.v1"
)

type FileConfig struct {
	Cluster struct {
		Prefix       string
		EtcdEndpoint []string
		Enabled      bool
	}
	Http struct {
		Listen    string
		Advertise []string
		Enabled   bool
	}
	Grpc struct {
		Listen    string
		Advertise []string
		Enabled   bool
	}
	Storage struct {
		Filepath        string
		CephDataPool    string
		CephHotPool     string
		CephJournalPool string
		CephConf        string
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
func (c *FileConfig) StorageCephJournalPool() string {
	return c.Storage.CephJournalPool
}
func (c *FileConfig) HttpEnabled() bool {
	return c.Http.Enabled
}
func (c *FileConfig) HttpListen() string {
	return c.Http.Listen
}
func (c *FileConfig) HttpAdvertise() []string {
	rv := []string{}
	for _, x := range c.Http.Advertise {
		if x == "" {
			continue
		}
		el := strings.Split(x, ",")
		for _, e := range el {
			if e == "" {
				continue
			}
			rv = append(rv, e)
		}
	}
	return rv
}
func (c *FileConfig) GRPCEnabled() bool {
	return c.Grpc.Enabled
}
func (c *FileConfig) GRPCListen() string {
	return c.Grpc.Listen
}
func (c *FileConfig) GRPCAdvertise() []string {
	rv := []string{}
	for _, x := range c.Grpc.Advertise {
		if x == "" {
			continue
		}
		el := strings.Split(x, ",")
		for _, e := range el {
			if e == "" {
				continue
			}
			rv = append(rv, e)
		}
	}
	return rv
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
