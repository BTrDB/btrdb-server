package main

import (
	lg "code.google.com/p/log4go"
	"flag"
	_ "fmt"
	"github.com/SoftwareDefinedBuildings/quasar"
	bstore "github.com/SoftwareDefinedBuildings/quasar/bstoreGen1"
	"github.com/SoftwareDefinedBuildings/quasar/cpinterface"
	"github.com/SoftwareDefinedBuildings/quasar/httpinterface"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

func init() {
	if _, err := os.Stat("logconfig.xml"); os.IsNotExist(err) {
		lg.Info("No logconfig.xml file exists, using default logging")
		return
	} else {
		lg.LoadConfiguration("logconfig.xml")
	}
}

var serveHttp = flag.String("http", "", "Serve http requests from this address:port")
var serveCPNP = flag.String("cpnp", "localhost:4410", "Serve Capn Proto requests from this address:port")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var createDB = flag.Uint64("makedb", 0, "create a new database")
var dbpath = flag.String("dbpath", "/srv/quasar", "path of databae")
var cachesz = flag.Uint64("cache", 2, "block MRU cache in GB")
var memprofile = flag.String("memprofile", "", "write memory profile to this file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			lg.Crash(err)
		}
		f2, err := os.Create("blockprofile.db")
		if err != nil {
			lg.Crash(err)
		}
		pprof.StartCPUProfile(f)
		runtime.SetBlockProfileRate(1)
		defer runtime.SetBlockProfileRate(0)
		defer pprof.Lookup("block").WriteTo(f2, 1)
		defer pprof.StopCPUProfile()
	}

	if *createDB != 0 {
		lg.Info("Creating a new database")
		bstore.CreateDatabase(*createDB*131072, *dbpath)
		//bstore.CreateDatabase(1024, *dbpath)
		lg.Info("Done")
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
