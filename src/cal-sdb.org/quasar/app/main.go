package main 

import (
	_ "fmt"
	"cal-sdb.org/quasar"
	"log"
	"flag"
	"cal-sdb.org/quasar/httpinterface"
	"cal-sdb.org/quasar/cpinterface"
	"time"
	//"code.google.com/p/go-uuid/uuid"
)

var serveHttp = flag.String("http", "", "Serve requests from this address:port")
var serveCPNP = flag.String("cpnp", "localhost:4410", "Serve Capn Proto requests over this port")

func main() {
	q, err := quasar.NewQuasar(&quasar.DefaultQuasarConfig)
	if err != nil {
		log.Panic(err)
	}
	
	flag.Parse()
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

