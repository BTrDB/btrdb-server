package main 

import (
	_ "fmt"
	"cal-sdb.org/quasar"
	"log"
	"flag"
	"cal-sdb.org/quasar/httpinterface"
	"cal-sdb.org/quasar/cpinterface"
	bstore "cal-sdb.org/quasar/bstoreGen1"
	"time"
	"runtime/pprof"
	"os"
	//"code.google.com/p/go-uuid/uuid"
)

var serveHttp = flag.String("http", "", "Serve requests from this address:port")
var serveCPNP = flag.String("cpnp", "localhost:4410", "Serve Capn Proto requests over this port")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var createDB = flag.Uint64("makedb",0, "create a new database")
var dbpath = flag.String("dbpath","/srv/quasar","path of databae")
    
func main() {
	flag.Parse()
	if *cpuprofile != "" {
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal(err)
        }
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }
	if *createDB != 0 {
		log.Printf("Creating new database")
		//bstore.CreateDatabase(*createDB*131072, *dbpath)
		bstore.CreateDatabase(1024, *dbpath)
		log.Printf("done")
		os.Exit(0)
	}
	cfg := quasar.DefaultQuasarConfig
	cfg.BlockPath = *dbpath
	q, err := quasar.NewQuasar(&cfg)
	if err != nil {
		log.Panic(err)
	}
	
	if *serveHttp != "" {
		go httpinterface.QuasarServeHTTP(q, *serveHttp)
	}
	if *serveCPNP != "" {
		go cpinterface.ServeCPNP(q, "tcp", *serveCPNP)
	}
	for {
		time.Sleep(5*time.Second)
		log.Printf("Still alive")
	}
}

