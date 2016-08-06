package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"time"

	"github.com/SoftwareDefinedBuildings/btrdb"
	"github.com/SoftwareDefinedBuildings/btrdb/cpinterface"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/bstore"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/configprovider"
	"github.com/op/go-logging"
)

var log *logging.Logger

func init() {
	logging.SetFormatter(logging.MustStringFormatter("%{color}%{shortfile} ▶%{color:reset} %{message}"))
	log = logging.MustGetLogger("log")

}

var createDB = flag.Bool("makedb", false, "create a new database")
var printVersion = flag.Bool("version", false, "print version and exit")

func main() {
	flag.Parse()
	if *printVersion {
		if btrdb.VersionString == "" {
			fmt.Println("3.x.x")
		} else {
			fmt.Println(btrdb.VersionString)
		}
		os.Exit(0)
	}
	log.Infof("Starting BTrDB version %s %s", btrdb.VersionString, btrdb.BuildDate)

	cfg, err1 := configprovider.LoadFileConfig("./btrdb.conf")
	if cfg == nil {
		var err2 error
		cfg, err2 = configprovider.LoadFileConfig("/etc/btrdb/btrdb.conf")
		if cfg == nil {
			fmt.Println("Could not locate configuration")
			fmt.Printf("Tried ./btrdb.conf : %s\n", err1)
			fmt.Printf("Tried /etc/btrdb/btrdb.conf : %s\n", err2)
			fmt.Printf("Unashamedly giving up\n")
			os.Exit(1)
		}
	}

	if cfg.ClusterEnabled() {
		hostname, err := os.Hostname()
		if err != nil {
			fmt.Println("Could not obtain hostname")
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		cfg, err = configprovider.LoadEtcdConfig(cfg, hostname)
		if err != nil {
			fmt.Println("Could not load cluster configuration")
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Println("CONFIG OKAY!")
	if *createDB {
		fmt.Printf("Creating a new database\n")
		bstore.CreateDatabase(cfg)
		fmt.Printf("Done\n")
		os.Exit(0)
	}

	q, err := btrdb.NewQuasar(cfg)
	if err != nil {
		log.Panicf("error: %v", err)
	}
	fmt.Println("QUASAR OKAY!")
	go func() {
		for {
			time.Sleep(1 * time.Second)
			log.Infof("Num goroutines: %d", runtime.NumGoroutine())
		}
	}()

	//if cfg.HttpEnabled() {
	//	go httpinterface.QuasarServeHTTP(q, cfg.HttpAddress()+":"+strconv.FormatInt(int64(cfg.HttpPort()), 10))
	//}
	if cfg.CapnpEnabled() {
		go cpinterface.ServeCPNP(q, "tcp", cfg.CapnpAddress()+":"+strconv.FormatInt(int64(cfg.CapnpPort()), 10))
	}

	// if Configuration.Debug.Heapprofile {
	// 	go func() {
	// 		idx := 0
	// 		for {
	// 			f, err := os.Create(fmt.Sprintf("profile.heap.%05d", idx))
	// 			if err != nil {
	// 				log.Panicf("Could not create memory profile %v", err)
	// 			}
	// 			idx = idx + 1
	// 			pprof.WriteHeapProfile(f)
	// 			f.Close()
	// 			time.Sleep(30 * time.Second)
	// 		}
	// 	}()
	// }

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
			//	for {
			//		if q.IsPending() {
			//			log.Warning("Pending inserts... waiting... ")
			//			time.Sleep(2 * time.Second)
			//		} else {
			//			log.Warning("No pending inserts")
			//			break
			//		}
			//	}
			// if Configuration.Debug.Heapprofile {
			// 	log.Warning("writing heap profile")
			// 	f, err := os.Create("profile.heap.FIN")
			// 	if err != nil {
			// 		log.Panicf("Could not create memory profile %v", err)
			// 	}
			// 	pprof.WriteHeapProfile(f)
			// 	f.Close()
			//
			// }
			return //end the program
		default:

		}
	}
}
