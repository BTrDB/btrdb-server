package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"
	
	"github.com/SoftwareDefinedBuildings/btrdb"
	"github.com/SoftwareDefinedBuildings/btrdb/cpinterface"
	"github.com/SoftwareDefinedBuildings/btrdb/httpinterface"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/bstore"
	"github.com/op/go-logging"
)

var log *logging.Logger

func init() {
	logging.SetFormatter(logging.MustStringFormatter("%{color}%{shortfile} ▶%{color:reset} %{message}"))
	log = logging.MustGetLogger("log")

}

var createDB = flag.Bool("makedb", false, "create a new database")

func main() {
	loadConfig()
	flag.Parse()

	go func() {
		for {
			time.Sleep(10 * time.Second)
			fmt.Println("Num goroutines: ", runtime.NumGoroutine())
		}
	}()
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
	cfg := btrdb.QuasarConfig{
		DatablockCacheSize:           uint64(Configuration.Cache.BlockCache),
		TransactionCoalesceEnable:    true,
		TransactionCoalesceInterval:  uint64(*Configuration.Coalescence.Interval),
		TransactionCoalesceEarlyTrip: uint64(*Configuration.Coalescence.Earlytrip),
		Params: Params,
	}
	q, err := btrdb.NewQuasar(&cfg)
	if err != nil {
		log.Panicf("error: ", err)
	}

	if Configuration.Http.Enabled {
		go httpinterface.QuasarServeHTTP(q, *Configuration.Http.Address+":"+strconv.FormatInt(int64(*Configuration.Http.Port), 10))
	}
	if Configuration.Capnp.Enabled {
		go cpinterface.ServeCPNP(q, "tcp", *Configuration.Capnp.Address+":"+strconv.FormatInt(int64(*Configuration.Capnp.Port), 10))
	}

	if Configuration.Debug.Heapprofile {
		go func() {
			idx := 0
			for {
				f, err := os.Create(fmt.Sprintf("profile.heap.%05d", idx))
				if err != nil {
					log.Panicf("Could not create memory profile %v", err)
				}
				idx = idx + 1
				pprof.WriteHeapProfile(f)
				f.Close()
				time.Sleep(30 * time.Second)
			}
		}()
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	for {
		time.Sleep(5 * time.Second)
		log.Info("Still alive")

		select {
		case _ = <-sigchan:
			log.Warning("Received Ctrl-C, waiting for graceful shutdown")
			time.Sleep(4 * time.Second) //Allow http some time
			log.Warning("Checking for pending inserts")
			for {
				if q.IsPending() {
					log.Warning("Pending inserts... waiting... ")
					time.Sleep(2 * time.Second)
				} else {
					log.Warning("No pending inserts")
					break
				}
			}
			if Configuration.Debug.Heapprofile {
				log.Warning("writing heap profile")
				f, err := os.Create("profile.heap.FIN")
				if err != nil {
					log.Panicf("Could not create memory profile %v", err)
				}
				pprof.WriteHeapProfile(f)
				f.Close()

			}
			return //end the program
		default:

		}
	}
}
