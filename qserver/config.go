package main

import (
	"code.google.com/p/gcfg"
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Http struct {
		Port    *int
		Address *string
		Enabled bool
	}
	Capnp struct {
		Port    *int
		Address *string
		Enabled bool
	}
	Mongo struct {
		Server     *string
		Collection *string
	}
	Storage struct {
		Provider string
		Filepath *string
		Cephconf *string
		Cephpool *string
	}
	Cache struct {
		BlockCache      int
		RadosWriteCache *int
		RadosReadCache  *int
	}
	Debug struct {
		Cpuprofile  bool
		Heapprofile bool
	}
	Coalescence struct {
		Earlytrip *int
		Interval  *int
	}
}

var Configuration Config
var Params map[string]string

func loadConfig() {
	found := false
	err := gcfg.ReadFileInto(&Configuration, "./quasar.conf")
	if err != nil {
		fmt.Printf("Could not load configuration file './quasar.conf':\n%v\n", err)
	} else {
		found = true
	}

	if !found {
		err := gcfg.ReadFileInto(&Configuration, "/etc/quasar/quasar.conf")
		if err != nil {
			fmt.Printf("Could not load configuration file '/etc/quasar/quasar.conf':\n%v\n", err)
		} else {
			found = true
		}
	}

	if !found {
		fmt.Printf("Aborting: no configuration found!\n")
		os.Exit(1)
	}

	if Configuration.Mongo.Server == nil || *Configuration.Mongo.Server == "" {
		fmt.Printf("Aborting: configuration missing MongoDB server address\n")
		os.Exit(1)
	}
	if Configuration.Mongo.Collection == nil || *Configuration.Mongo.Collection == "" {
		fmt.Printf("Aborting: configuration missing MongoDB collection\n")
		os.Exit(1)
	}

	if Configuration.Storage.Provider == "file" {
		if Configuration.Storage.Filepath == nil {
			fmt.Printf("Aborting: using Files for storage, but no filepath specified\n")
			os.Exit(1)
		}
	} else if Configuration.Storage.Provider == "ceph" {
		if Configuration.Storage.Cephconf == nil {
			fmt.Printf("Aborting: using Ceph for storage, but no cephconf specified\n")
			os.Exit(1)
		}
		if Configuration.Storage.Cephpool == nil {
			fmt.Printf("Aborting: using Ceph for storage, but no cephpool specified\n")
			os.Exit(1)
		}
	} else {
		fmt.Printf("Aborting: unknown storage provider specified\n")
		os.Exit(1)
	}

	if Configuration.Cache.RadosWriteCache == nil {
		z := 0
		Configuration.Cache.RadosWriteCache = &z
	}
	if Configuration.Cache.RadosReadCache == nil {
		z := 0
		Configuration.Cache.RadosReadCache = &z
	}

	if Configuration.Http.Enabled && Configuration.Http.Port == nil {
		fmt.Printf("Aborting: http server enabled, but no port specified\n")
		os.Exit(1)
	}

	if Configuration.Http.Enabled && Configuration.Http.Address == nil {
		fmt.Printf("Aborting: http server enabled, but no address specified\n")
		os.Exit(1)
	}

	if Configuration.Capnp.Enabled && Configuration.Capnp.Port == nil {
		fmt.Printf("Aborting: capn proto server enabled, but no port specified\n")
		os.Exit(1)
	}

	if Configuration.Capnp.Enabled && Configuration.Capnp.Address == nil {
		fmt.Printf("Aborting: capn proto server enabled, but no address specified\n")
		os.Exit(1)
	}

	if Configuration.Coalescence.Earlytrip == nil {
		fmt.Printf("Aborting: transaction coalescence early trip object count not set\n")
		os.Exit(1)
	}

	if Configuration.Coalescence.Interval == nil {
		fmt.Printf("Aborting: transaction coalescence commit interval not set\n")
		os.Exit(1)
	}

	Params = map[string]string{
		"mongoserver": *Configuration.Mongo.Server,
		"provider":    Configuration.Storage.Provider,
		"cachesize":   strconv.FormatInt(int64(Configuration.Cache.BlockCache), 10),
		"collection":  *Configuration.Mongo.Collection,
	}
	if Configuration.Storage.Provider == "ceph" {
		Params["cephconf"] = *Configuration.Storage.Cephconf
		Params["cephpool"] = *Configuration.Storage.Cephpool
		Params["cephrcache"] = strconv.FormatInt(int64(*Configuration.Cache.RadosReadCache), 10)
		Params["cephwcache"] = strconv.FormatInt(int64(*Configuration.Cache.RadosWriteCache), 10)
	}
	if Configuration.Storage.Provider == "file" {
		Params["dbpath"] = *Configuration.Storage.Filepath
	}

	fmt.Printf("Configuration OK!\n")
}
