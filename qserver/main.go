package main

import (
	lg "code.google.com/p/log4go"
	"flag"
	"fmt"
	"github.com/SoftwareDefinedBuildings/quasar"
	"github.com/SoftwareDefinedBuildings/quasar/cpinterface"
	"github.com/SoftwareDefinedBuildings/quasar/httpinterface"
	"github.com/SoftwareDefinedBuildings/quasar/internal/bstore"
	"github.com/op/go-logging"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"
)

var log *logging.Logger

func init() {
	logging.SetFormatter(logging.MustStringFormatter("%{color}%{shortfile} ▶%{color:reset} %{message}"))
	log = logging.MustGetLogger("log")
}

/*
var serveHttp = flag.String("http", "", "Serve http requests from this address:port")
var serveCPNP = flag.String("cpnp", "localhost:4410", "Serve Capn Proto requests from this address:port")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")*/
var createDB = flag.Bool("makedb", false, "create a new database")

/*
var dbpath = flag.String("dbpath", "/srv/quasar", "path of databae")
var cachesz = flag.Uint64("cache", 2, "block MRU cache in GB")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")*/

func main() {
	loadConfig()
	flag.Parse()

	if Configuration.Debug.Cpuprofile {
		f, err := os.Create("profile.cpu")
		if err != nil {
			log.Panicf("Error creating CPU profile: %v", err)
		}
		f2, err := os.Create("profile.block")
		if err != nil {
			log.Panicf("Error creating Block profile: %v", err)
		}
		pprof.StartCPUProfile(f)
		runtime.SetBlockProfileRate(1)
		defer runtime.SetBlockProfileRate(0)
		defer pprof.Lookup("block").WriteTo(f2, 1)
		defer pprof.StopCPUProfile()
	}

	if *createDB {
		fmt.Printf("Creating a new database\n")
		bstore.CreateDatabase(Params)
		fmt.Printf("Done\n")
		os.Exit(0)
	}
	nCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nCPU)
	cfg := quasar.QuasarConfig{
		DatablockCacheSize:           uint64(Configuration.Cache.BlockCache),
		TransactionCoalesceEnable:    true,
		TransactionCoalesceInterval:  uint64(*Configuration.Coalescence.Interval),
		TransactionCoalesceEarlyTrip: uint64(*Configuration.Coalescence.Earlytrip),
		Params: Params,
	}
	q, err := quasar.NewQuasar(&cfg)
	if err != nil {
		lg.Crash(err)
	}

	if Configuration.Http.Enabled {
		go httpinterface.QuasarServeHTTP(q, *Configuration.Http.Address+":"+strconv.FormatInt(int64(*Configuration.Http.Port), 10))
	}
	if Configuration.Capnp.Enabled {
		go cpinterface.ServeCPNP(q, "tcp", *Configuration.Capnp.Address+":"+strconv.FormatInt(int64(*Configuration.Capnp.Port), 10))
	}
	idx := 0
	for {
		time.Sleep(5 * time.Second)
		log.Info("Still alive")
		idx++
		if idx*5/60 == 2 {
			if Configuration.Debug.Heapprofile {
				f, err := os.Create("profile.heap")
				if err != nil {
					log.Panicf("Could not create memory profile %v", err)
				}
				pprof.WriteHeapProfile(f)
				f.Close()
				return
			}
			if Configuration.Debug.Cpuprofile {
				return
			}
		}
	}
}
