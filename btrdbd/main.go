package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/BTrDB/btrdb-server"
	"github.com/BTrDB/btrdb-server/grpcinterface"
	"github.com/BTrDB/btrdb-server/internal/bstore"
	"github.com/BTrDB/btrdb-server/internal/configprovider"
	"github.com/BTrDB/btrdb-server/version"
	"github.com/immesys/sysdigtracer"
	"github.com/op/go-logging"
	opentracing "github.com/opentracing/opentracing-go"
)

var lg *logging.Logger

func init() {
	logging.SetBackend(logging.NewLogBackend(os.Stderr, "", 0))
	logging.SetFormatter(logging.MustStringFormatter("[%{level}]%{shortfile} > %{message}"))
	lg = logging.MustGetLogger("log")
}

var createDB = flag.Bool("makedb", false, "create a new database")
var ensureDB = flag.Bool("ensuredb", false, "initialize pools only if they are uninitialized")
var printVersion = flag.Bool("version", false, "print version and exit")

func main() {
	flag.Parse()
	if *printVersion {
		fmt.Println(version.VersionString)
		os.Exit(0)
	}
	lg.Infof("Starting BTrDB version %s %s", version.VersionString, version.BuildDate)

	dotracer := os.Getenv("BTRDB_ENABLE_OVERWATCH")
	if strings.ToLower(dotracer) == "yes" {
		tracer := sysdigtracer.New()
		//Cheers love! The cavalry's here!
		opentracing.SetGlobalTracer(tracer)
		lg.Infof("TRACING ENABLED")
	} else {
		lg.Infof("TRACING IS _NOT_ ENABLED")
	}

	go func() {
		for {
			span := opentracing.StartSpan("Dummy")
			time.Sleep(150 * time.Millisecond)
			span.Finish()
		}
	}()
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
		var err error
		cfg, err = configprovider.LoadEtcdConfig(cfg, "")
		if err != nil {
			fmt.Println("Could not load cluster configuration")
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
	}
	lg.Infof("CONFIG OKAY!")
	if *createDB {
		lg.Infof("Creating a new database")
		bstore.CreateDatabase(cfg, true)
		lg.Infof("Done")
		os.Exit(0)
	}
	if *ensureDB {
		lg.Infof("Ensuring database is initialized")
		bstore.CreateDatabase(cfg, false)
		lg.Infof("Done")
		os.Exit(0)
	}

	//This will begin the etcd cluster tasks
	q, err := btrdb.NewQuasar(cfg)
	if err != nil {
		lg.Panicf("error: %v", err)
	}
	lg.Infof("BTRDB OKAY!")

	go func() {
		for {
			time.Sleep(10 * time.Second)
			lg.Infof("Number of goroutines: %d", runtime.NumGoroutine())
		}
	}()

	grpcHandle := grpcinterface.ServeGRPC(q, cfg.GRPCListen())

	sigchan := make(chan os.Signal, 30)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case _ = <-sigchan:
			lg.Critical("Received SIGINT, removing node from cluster")
			lg.Critical("send SIGINT again to quit immediately")
			grpc := grpcHandle.InitiateShutdown()
			<-grpc
			lg.Critical("GRPC shutdown complete")
			qdone := q.InitiateShutdown()
			<-qdone
			lg.Critical("Safe shutdown complete")

			return //end the program

		}
	}
}
