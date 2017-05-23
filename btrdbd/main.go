package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/SoftwareDefinedBuildings/btrdb"
	"github.com/SoftwareDefinedBuildings/btrdb/grpcinterface"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/bstore"
	"github.com/SoftwareDefinedBuildings/btrdb/internal/configprovider"
	"github.com/SoftwareDefinedBuildings/btrdb/version"
	"github.com/immesys/sysdigtracer"
	"github.com/op/go-logging"
	opentracing "github.com/opentracing/opentracing-go"
)

var log *logging.Logger

func init() {
	logging.SetBackend(logging.NewLogBackend(os.Stderr, "", 0))
	logging.SetFormatter(logging.MustStringFormatter("[%{level}]%{shortfile} > %{message}"))
	log = logging.MustGetLogger("log")

}

var createDB = flag.Bool("makedb", false, "create a new database")
var printVersion = flag.Bool("version", false, "print version and exit")

func main() {
	flag.Parse()
	if *printVersion {
		fmt.Println(version.VersionString)

		os.Exit(0)
	}
	log.Infof("Starting BTrDB version %s %s", version.VersionString, version.BuildDate)

	dotracer := os.Getenv("BTRDB_ENABLE_OVERWATCH")
	if dotracer != "" {
		// // create collector.
		// collector, err := zipkin.NewHTTPCollector("http://zipkin:9411/api/v1/spans")
		// if err != nil {
		// 	fmt.Printf("unable to create Zipkin HTTP collector: %+v", err)
		// 	os.Exit(-1)
		// }
		//
		// // create recorder.
		// recorder := zipkin.NewRecorder(collector, false, "0.0.0.0:4410", "btrdbd")
		//
		// // create tracer.
		// tracer, err := zipkin.NewTracer(
		// 	recorder,
		// 	zipkin.ClientServerSameSpan(true),
		// 	zipkin.TraceID128Bit(true),
		// )
		// if err != nil {
		// 	fmt.Printf("unable to create Zipkin tracer: %+v", err)
		// 	os.Exit(-1)
		// }
		tracer := sysdigtracer.New()
		//Cheers love! The cavalry's here!
		opentracing.SetGlobalTracer(tracer)
		fmt.Printf("TRACING ENABLED\n")
	} else {
		fmt.Printf("TRACING IS _NOT_ ENABLED\n")
	}
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
			time.Sleep(10 * time.Second)
			log.Infof("Num goroutines: %d", runtime.NumGoroutine())
		}
	}()

	//if cfg.HttpEnabled() {
	//	go httpinterface.QuasarServeHTTP(q, cfg.HttpAddress()+":"+strconv.FormatInt(int64(cfg.HttpPort()), 10))
	//}
	//	if cfg.CapnpEnabled() {
	//		go cpinterface.ServeCPNP(q, "tcp", cfg.CapnpAddress()+":"+strconv.FormatInt(int64(cfg.CapnpPort()), 10))
	//	}
	grpcHandle := grpcinterface.ServeGRPC(q, "0.0.0.0:4410")
	//go httpinterface.Run()
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

	//So the correct shutdown procedure is:
	// - out your node in the cluster
	//   - all write requests must finish (grpc must watch out notify too)
	//   - all caches must flush
	// - wait graceful shutdown of grpc (for read)
	// - exit

	sigchan := make(chan os.Signal, 3)
	signal.Notify(sigchan, os.Interrupt)

	for {
		select {
		case _ = <-sigchan:
			log.Warning("Received SIGINT, removing node from cluster")
			log.Warning("send SIGINT again to quit immediately")
			grpc := grpcHandle.InitiateShutdown()
			select {
			case _ = <-grpc:
				log.Warning("GRPC shutdown complete")
			case _ = <-sigchan:
				log.Warning("SIGINT RECEIVED, SKIPPING SAFE SHUTDOWN")
				return
			}
			/*http := httpinterface.InitiateShutdown()
			select {
			case _ = <-http:
				log.Warning("HTTP shutdown complete")
			case _ = <-sigchan:
				log.Warning("SIGINT RECEIVED, SKIPPING SAFE SHUTDOWN")
				return
			}*/
			qdone := q.InitiateShutdown()
			select {
			case _ = <-qdone:
				log.Warning("Core shutdown complete")
			case _ = <-sigchan:
				log.Warning("SIGINT RECEIVED, SKIPPING SAFE SHUTDOWN")
				return
			}
			log.Warning("Safe shutdown complete")
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

		}
	}
}
