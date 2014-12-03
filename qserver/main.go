package main

import (
	lg "code.google.com/p/log4go"
	"flag"
	_ "fmt"
	"github.com/SoftwareDefinedBuildings/quasar"
	"github.com/SoftwareDefinedBuildings/quasar/internal/bstore"
	"github.com/SoftwareDefinedBuildings/quasar/cpinterface"
	"github.com/SoftwareDefinedBuildings/quasar/httpinterface"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
	"github.com/op/go-logging"
	"code.google.com/p/gcfg"
)

var log *logging.Logger

func init() {
	logging.SetFormatter(logging.MustStringFormatter("%{color}%{time:15:04:05.000000}::%{shortfile}▶%{color:reset}%{message}"))
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
			log.Panicf("Error creating CPU profile: %v",err)
		}
		f2, err := os.Create("profile.block")
		if err != nil {
			log.Panicf("Error creating Block profile: %v",err)
		}
		pprof.StartCPUProfile(f)
		runtime.SetBlockProfileRate(1)
		defer runtime.SetBlockProfileRate(0)
		defer pprof.Lookup("block").WriteTo(f2, 1)
		defer pprof.StopCPUProfile()
	}
	
	if *createDB {
		fmt.Printf("Creating a new database")
		bstore.CreateDatabase(Params)
		fmt.Printf("Done")
		os.Exit(0)
	}
	nCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nCPU)
	cfg := quasar.DefaultQuasarConfig
	cfg.BlockPath = *dbpath
	cfg.DatablockCacheSize = (*cachesz * 1024 * 1024 * 1024) / bstore.DBSIZE
	q, err := quasar.NewQuasar(&cfg)
	if err != nil {
		lg.Crash(err)
	}

	if *serveHttp != "" {
		go httpinterface.QuasarServeHTTP(q, *serveHttp)
	}
	if *serveCPNP != "" {
		go cpinterface.ServeCPNP(q, "tcp", *serveCPNP)
	}
	idx := 0
	for {
		time.Sleep(5 * time.Second)
		lg.Info("Still alive")
		idx++
		if idx*5/60 == 2 {
			if *memprofile != "" {
				f, err := os.Create(*memprofile)
				if err != nil {
					lg.Crash(err)
				}
				pprof.WriteHeapProfile(f)
				f.Close()
				return
			}
			if *cpuprofile != "" {
				return
			}
		}
	}
}
